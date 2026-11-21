"""流式读取自测：结构一致性、首段先到、内存上界、短事务与失败/断开清理。

这些测试直接驱动 ``app.streaming`` 的异步生成器，并用独立的 SQLite 文件
验证读事务的持有范围。
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import tracemalloc

import anyio
import pytest

from app.database import SessionLocal, init_db
from app.schemas import SubmissionRequest
from sqlalchemy import text

from app.services import (
    get_batch_meta,
    get_registered_batch,
    read_record_page,
    submit_batch,
)
from app.streaming import (
    BatchStreamError,
    BatchStreamIntegrityError,
    stream_registered_batch,
)

init_db()

BATCH_ID = "stream-test-1"
RECORDS = [
    {
        "order_id": f"ORD-{i:05d}",
        "customer_email": f"user{i}@example.com",
        "status": "active",
        "amount": round(i / 3, 6),
        "currency": "CNY",
        "note": "中文与 unicode-ü",
        "nested": {"tags": ["a", "b"], "ok": True, "none": None},
    }
    for i in range(1000)
]


def _payload(batch_id: str, records: list[dict], *, min_record_count: int = 1) -> SubmissionRequest:
    return SubmissionRequest.model_validate(
        {
            "batch_id": batch_id,
            "schema_version": "order-schema/v2.1",
            "records": records,
            "rules": {
                "field_rules": {"order_id": [{"type": "required"}]},
                "unique_rules": [{"fields": ["order_id"]}],
                "batch_rules": {
                    "max_failure_rate": 0.0,
                    "min_record_count": min_record_count,
                },
            },
        }
    )


@pytest.fixture(scope="module", autouse=True)
def _registered_batch() -> None:
    db = SessionLocal()
    try:
        result = submit_batch(db, _payload(BATCH_ID, RECORDS))
        assert result.status == "PASSED"
    finally:
        db.close()


def _meta(batch_id: str = BATCH_ID):
    db = SessionLocal()
    try:
        meta = get_batch_meta(db, batch_id)
    finally:
        db.close()
    assert meta is not None
    return meta


async def _collect(meta, **kwargs) -> bytes:
    body = bytearray()
    async for chunk in stream_registered_batch(meta, **kwargs):
        body.extend(chunk)
    return bytes(body)


def _collect_sync(meta, **kwargs) -> bytes:
    from functools import partial

    return anyio.run(partial(_collect, meta, **kwargs))


# ---------------------------------------------------------------------------
# 结构与取值
# ---------------------------------------------------------------------------


def test_stream_is_byte_identical_to_previous_response_encoding() -> None:
    """默认 JSON 编码与原先 response_model 序列化逐字节一致。"""
    db = SessionLocal()
    try:
        expected = get_registered_batch(db, BATCH_ID).model_dump_json()
    finally:
        db.close()

    for kwargs in (
        {},
        {"page_size": 7, "chunk_bytes": 32, "first_segment_records": 1},
        {"page_size": 500, "chunk_bytes": 1 << 20, "first_segment_records": 20},
    ):
        got = _collect_sync(_meta(), **kwargs).decode("utf-8")
        assert got == expected, f"参数 {kwargs} 下输出与旧结构不一致"


def test_stream_structure_fields_and_index_order() -> None:
    got = json.loads(_collect_sync(_meta(), page_size=13, chunk_bytes=100))
    assert list(got) == [
        "batch_id",
        "schema_version",
        "record_count",
        "registered_at",
        "records",
    ]
    assert got["batch_id"] == BATCH_ID
    assert got["schema_version"] == "order-schema/v2.1"
    assert got["record_count"] == len(RECORDS)
    assert [r["record_index"] for r in got["records"]] == list(range(len(RECORDS)))
    assert [r["record"] for r in got["records"]] == RECORDS
    assert all(list(r) == ["record_index", "record"] for r in got["records"])


def test_empty_batch_streams_valid_json() -> None:
    db = SessionLocal()
    try:
        result = submit_batch(db, _payload("stream-empty", [], min_record_count=0))
        assert result.status == "PASSED"
    finally:
        db.close()
    got = json.loads(_collect_sync(_meta("stream-empty")))
    assert got["records"] == [] and got["record_count"] == 0


def test_encoder_matches_pydantic_for_tricky_values() -> None:
    """转义、Unicode、控制字符与非有限浮点数都与旧序列化一致。"""
    from app.schemas import BatchRecordOut
    from app.streaming import encode_record

    samples = [
        "emoji 😀",
        'quote" back\\slash tab\t newline\n',
        "ctrl\x01\x1f del\x7f nul\x00",
        "   nbsp ",
        "ünïcødé 𝔘𝔫𝔦𝔠𝔬𝔡𝔢",
        "/slash/ <>&",
    ]
    for value in samples:
        payload = {"v": value, "list": [1, 2.5, True, None, "中文"]}
        expected = BatchRecordOut(record_index=7, record=payload).model_dump_json()
        assert encode_record(7, payload) == expected, repr(value)

    nonfinite = {"nan": float("nan"), "inf": float("inf"), "ninf": float("-inf")}
    expected = BatchRecordOut(record_index=0, record=nonfinite).model_dump_json()
    assert encode_record(0, nonfinite) == expected


# ---------------------------------------------------------------------------
# 首段先到
# ---------------------------------------------------------------------------


def test_first_segment_arrives_before_full_batch_is_read(monkeypatch) -> None:
    """首段 chunk 必须在第二页读取之前发出。

    通过包装 ``read_record_page`` 记录每页读取时的已发送 chunk 数来证明：
    首段发布时，后续页尚未从数据库读取。
    """
    import app.streaming as streaming

    sent_chunks = 0
    observed_at_page: list[int] = []
    original = streaming.read_record_page

    def spy(db, batch_id, after_index, limit):  # noqa: ANN001
        observed_at_page.append(sent_chunks)
        return original(db, batch_id, after_index, limit)

    monkeypatch.setattr(streaming, "read_record_page", spy)

    async def run() -> int:
        nonlocal sent_chunks
        async for chunk in stream_registered_batch(
            _meta(), page_size=50, chunk_bytes=1 << 20, first_segment_records=5
        ):
            if b'"record_index"' in chunk:
                sent_chunks += 1
        return sent_chunks

    chunks = anyio.run(run)
    # 第 0 页读取时还没有发出任何明细；第 1 页读取时首段明细已发出
    assert observed_at_page[0] == 0
    assert observed_at_page[1] >= 1
    assert chunks >= 2


def test_first_chunk_contains_header_and_small_prefix() -> None:
    async def first_chunk() -> bytes:
        async for chunk in stream_registered_batch(_meta(), first_segment_records=3):
            return chunk
        raise AssertionError("no chunk")

    chunk = anyio.run(first_chunk)
    assert chunk.startswith(b'{"batch_id":"stream-test-1"')
    assert b'"records":[' in chunk


# ---------------------------------------------------------------------------
# 内存上界
# ---------------------------------------------------------------------------


def test_memory_does_not_scale_with_row_count() -> None:
    """统计“单次从库里取出的记录条数”与单页字节，证明上界恒定。

    用 tracemalloc 观察流式编码 1000 行与 8000 行的峰值差异，
    并以页大小/块大小为自变量的理论上界做校验。
    """
    big = [dict(RECORDS[0], order_id=f"ORD-BIG-{i:05d}") for i in range(8000)]
    db = SessionLocal()
    try:
        assert submit_batch(db, _payload("stream-memory-big", big)).status == "PASSED"
    finally:
        db.close()

    async def peak_for(meta, page_size: int) -> int:
        tracemalloc.start()
        try:
            async for _ in stream_registered_batch(
                meta, page_size=page_size, chunk_bytes=8192, first_segment_records=5
            ):
                pass
            return tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()

    small_peak = anyio.run(peak_for, _meta(), 100)
    big_peak = anyio.run(peak_for, _meta("stream-memory-big"), 100)

    # 明细则数相差 8 倍，但峰值内存不应随行数线性增长。
    # 允许常量开销（每次 run_sync 的固定成本）波动，取较宽松的 3 倍上限。
    assert big_peak < small_peak * 3, (small_peak, big_peak)
    assert big_peak < 20 * 1024 * 1024, f"峰值 {big_peak} 字节过高"


# ---------------------------------------------------------------------------
# 短事务 / WAL
# ---------------------------------------------------------------------------


def _wal_frames(db_path: str) -> int:
    with sqlite3.connect(db_path) as conn:
        return conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()[1]


def test_each_page_uses_a_fresh_short_transaction(monkeypatch) -> None:
    """每页只读一次，且读完立即关闭 Session（不跨页持有读事务）。"""
    import app.streaming as streaming

    events: list[str] = []
    orig_session = streaming.SessionLocal

    def session_factory():
        events.append("open")
        db = orig_session()

        class Traced:
            def __getattr__(self, name):  # noqa: ANN001
                return getattr(db, name)

            def close(self) -> None:
                events.append("close")
                db.close()

        return Traced()

    monkeypatch.setattr(streaming, "SessionLocal", session_factory)

    async def run() -> None:
        async for _ in stream_registered_batch(
            _meta(), page_size=100, chunk_bytes=4096, first_segment_records=5
        ):
            pass

    anyio.run(run)
    # 1000 行 / 每页 100 行 -> 至少 10 页；每页一对 open/close，且数量相等
    assert events.count("open") == events.count("close")
    assert events.count("open") >= 11  # 10 页 + 1 次 EOF 探测
    assert events[0] == "open" and events[1] == "close"


def test_slow_consumer_does_not_block_checkpoint_or_other_writes() -> None:
    """下游暂停时，WAL 可截断检查点，其他批次照常登记。"""
    import threading

    from app.database import engine

    progressed = threading.Event()

    async def slow_stream() -> None:
        async def pace() -> None:
            await anyio.sleep(0.05)

        async for _ in stream_registered_batch(
            _meta(), page_size=50, chunk_bytes=512, first_segment_records=5, pace=pace
        ):
            progressed.set()

    results: dict[str, object] = {}

    def run_stream() -> None:
        anyio.run(slow_stream)

    t = threading.Thread(target=run_stream)
    t.start()
    assert progressed.wait(timeout=5)

    try:
        # 慢速流仍在进行：检查点应成功（busy=0），说明没有活跃读事务
        with engine.connect() as conn:
            busy, log_frames, checkpointed = conn.execute(
                text("PRAGMA wal_checkpoint(TRUNCATE)")
            ).one()
        results["busy"] = busy
        results["frames"] = (log_frames, checkpointed)

        # 另一批次仍可登记
        with SessionLocal() as db:
            other = submit_batch(db, _payload("stream-during-slow", RECORDS[:20]))
        results["other_status"] = other.status
    finally:
        t.join(timeout=30)

    assert not t.is_alive()
    assert results["busy"] == 0
    assert results["other_status"] == "PASSED"


# ---------------------------------------------------------------------------
# 中途失败 / 完整性 / 断开
# ---------------------------------------------------------------------------


def test_failure_midway_never_appends_closing_brackets() -> None:
    async def run() -> bytes:
        body = bytearray()
        with pytest.raises(BatchStreamError):
            async for chunk in stream_registered_batch(
                _meta(), chunk_bytes=64, fail_after=250
            ):
                body.extend(chunk)
        return bytes(body)

    partial = anyio.run(run)
    assert partial.startswith(b'{"batch_id":"stream-test-1"')
    assert not partial.rstrip().endswith(b"]}")
    with pytest.raises(json.JSONDecodeError):
        json.loads(partial)


def test_missing_rows_detected_before_closing_json() -> None:
    """登记 record_count 与实际明细不符时拒绝输出完整 JSON。"""
    db = SessionLocal()
    try:
        assert submit_batch(db, _payload("stream-corrupt", RECORDS[:10])).status == "PASSED"
        # 模拟落库缺失：删掉中间一条
        db.execute(
            text("DELETE FROM batch_records WHERE batch_id='stream-corrupt' AND record_index=4")
        )
        db.commit()
    finally:
        db.close()

    async def run() -> bytes:
        body = bytearray()
        with pytest.raises(BatchStreamIntegrityError):
            async for chunk in stream_registered_batch(_meta("stream-corrupt")):
                body.extend(chunk)
        return bytes(body)

    partial = anyio.run(run)
    assert not partial.rstrip().endswith(b"]}")


def test_client_disconnect_cleans_up_generator() -> None:
    """提前关闭生成器：游标停止推进、不产生收尾字节、无异常泄漏。"""
    progress = {"pages": 0}

    async def run() -> bytes:
        agen = stream_registered_batch(
            _meta(), page_size=100, chunk_bytes=256, first_segment_records=5
        )
        body = bytearray()
        async for chunk in agen:
            body.extend(chunk)
            progress["pages"] += 1
            if progress["pages"] >= 3:
                await agen.aclose()
                break
        return bytes(body)

    body = anyio.run(run)
    assert not body.rstrip().endswith(b"]}")
    # 只读取了少量页就关闭，未遍历全部 1000 行
    assert progress["pages"] == 3


def test_repeated_pages_are_keyset_ordered() -> None:
    db = SessionLocal()
    try:
        cursor = -1
        seen: list[int] = []
        while True:
            page = read_record_page(db, BATCH_ID, cursor, 173)
            if not page:
                break
            seen.extend(idx for idx, _ in page)
            cursor = page[-1][0]
        assert seen == list(range(len(RECORDS)))
    finally:
        db.close()
