"""已登记批次的流式读取与增量 JSON 编码。

设计目标
--------
1. **首段先到**：先输出批次头，再按页读取明细；首页记录数受
   ``stream_first_segment_records`` 限制，不必等整批读完。
2. **常量级内存**：每次只从 SQLite 取 ``stream_page_size`` 条，按记录逐条
   编码，攒到 ``stream_chunk_bytes`` 即发送并丢弃；内存上界由页大小和
   chunk 大小决定，与批次总行数无关。
3. **短读事务**：每页使用独立 Session，读完立即关闭（回滚并归还连接）。
   向下游 ``send`` 阻塞（慢速接收）期间不持有 SQLite 读事务，因此其他
   批次仍可登记，WAL 也能检查点回收。
4. **完整性**：输出前先校验记录下标从 0 连续递增且总数等于批次登记时的
   ``record_count``；任何读库/编码/校验错误都会抛出异常终止连接，绝不
   补上收尾的 ``]}``，客户端只会看到不完整响应而不会拿到“缺行的完整 JSON”。
5. **断开清理**：异步生成器被取消/关闭时通过 ``finally`` 释放资源；页读取
   在线程池中执行，取消会等待当前页读取结束并关闭其 Session。

输出结构与原先 ``RegisteredBatchOut`` 的默认 JSON 编码完全一致：
紧凑分隔符、``ensure_ascii=False``、``allow_nan=False``、字段顺序为
``batch_id, schema_version, record_count, registered_at, records[]``，
每条明细为 ``{"record_index": ..., "record": {...}}`` 且按 ``record_index`` 升序。
"""
from __future__ import annotations

import json
import math
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import anyio

from .config import settings
from .database import SessionLocal
from .services import BatchMeta, read_record_page

_JSON_KWARGS: dict[str, Any] = {
    "ensure_ascii": False,
    "allow_nan": False,
    "separators": (",", ":"),
}


class BatchStreamError(RuntimeError):
    """流式读取或编码失败；响应已开始时会中止连接。"""


class BatchStreamIntegrityError(BatchStreamError):
    """落库明细不完整/不连续，拒绝输出看似完整的 JSON。"""


# ---------------------------------------------------------------------------
# 运行指标（演示与自测用；进程内累计）
# ---------------------------------------------------------------------------

_metrics_lock = threading.Lock()
_metrics: dict[str, int] = {
    "streams_started": 0,
    "streams_completed": 0,
    "streams_aborted": 0,
    "streams_client_disconnected": 0,
    "pages_read": 0,
    "chunks_sent": 0,
    "records_emitted": 0,
    "peak_chunk_bytes": 0,
    "peak_page_records": 0,
}


def _bump(key: str, amount: int = 1) -> None:
    with _metrics_lock:
        _metrics[key] = _metrics.get(key, 0) + amount


def _observe_peak(key: str, value: int) -> None:
    with _metrics_lock:
        if value > _metrics.get(key, 0):
            _metrics[key] = value


def metrics_snapshot() -> dict[str, int]:
    with _metrics_lock:
        return dict(_metrics)


def reset_metrics() -> None:
    with _metrics_lock:
        for key in _metrics:
            _metrics[key] = 0


def encode_meta(meta: BatchMeta) -> str:
    """编码响应前缀（批次头与 ``records`` 数组开头）。"""
    return (
        '{"batch_id":'
        + _dumps(meta.batch_id)
        + ',"schema_version":'
        + _dumps(meta.schema_version)
        + ',"record_count":'
        + str(int(meta.record_count))
        + ',"registered_at":'
        + _dumps(meta.registered_at)
        + ',"records":['
    )


def _sanitize_nonfinite(value: Any) -> Any:
    """把 NaN/Infinity 递归替换为 None，与 Pydantic 的序列化行为一致。

    正常数据不会走到这里（``json.dumps`` 严格模式先成功），只有历史/极端
    数据含非有限浮点数时才需要，避免流式编码中途抛错而旧实现输出 ``null``。
    """
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _sanitize_nonfinite(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_nonfinite(v) for v in value]
    return value


def _dumps(value: Any) -> str:
    try:
        return json.dumps(value, **_JSON_KWARGS)
    except ValueError:
        return json.dumps(_sanitize_nonfinite(value), **_JSON_KWARGS)


def encode_record(record_index: int, record: dict[str, Any]) -> str:
    """编码单条明细，结构与 ``BatchRecordOut`` 一致。"""
    return (
        '{"record_index":'
        + str(int(record_index))
        + ',"record":'
        + _dumps(record)
        + "}"
    )


def _read_and_encode_page(
    batch_id: str, after_index: int, limit: int
) -> list[tuple[int, str]]:
    """在一个短事务内读取一页并编码；返回 ``(record_index, json_text)``。

    该函数在工作线程中执行。Session 在 ``finally`` 中关闭，保证页与页之间
    不遗留读事务；编码失败也会先关连接再把异常抛给异步侧。
    """
    db = SessionLocal()
    try:
        rows = read_record_page(db, batch_id, after_index, limit)
    finally:
        db.close()
    # 连接已归还后再做 CPU 编码，避免编码耗时延长读事务
    return [(idx, encode_record(idx, record)) for idx, record in rows]


Pace = Callable[[], Awaitable[None]]


async def stream_registered_batch(
    meta: BatchMeta,
    *,
    page_size: int | None = None,
    chunk_bytes: int | None = None,
    first_segment_records: int | None = None,
    pace: Pace | None = None,
    fail_after: int | None = None,
) -> AsyncIterator[bytes]:
    """按页读取并流式输出一个已登记批次。

    :param pace: 可选，每个 chunk 发送前的异步回调；演示“慢速接收”时注入。
    :param fail_after: 仅演示/测试用；输出指定条数后抛出 :class:`BatchStreamError`，
        用于验证中途失败不会产生完整的 JSON。
    """
    page_size = page_size or settings.stream_page_size
    chunk_bytes = chunk_bytes or settings.stream_chunk_bytes
    first_limit = min(
        page_size, first_segment_records or settings.stream_first_segment_records
    )
    first_limit = max(1, first_limit)

    # 在异步上下文中解析取消异常类型；不能等到 except 里调用，
    # 否则生成器在非异步上下文被 GC 关闭时会取不到当前异步库。
    cancelled_exc = anyio.get_cancelled_exc_class()

    _bump("streams_started")
    pending: list[str] = []
    try:
        # 先发批次头：即使整批很大、后续页读取较慢，首段字节也能尽快到达。
        _bump("chunks_sent")
        yield encode_meta(meta).encode("utf-8")

        expected_index = 0
        expected_total = int(meta.record_count)
        cursor = -1
        first_page = True
        emitted_records = 0
        pending_bytes = 0
        first_element = True

        def take_chunk() -> bytes:
            nonlocal first_element, pending_bytes
            body = ",".join(pending)
            pending.clear()
            pending_bytes = 0
            if first_element:
                first_element = False
                return body.encode("utf-8")
            # 上一 chunk 已输出过元素，chunk 之间需要补数组分隔逗号
            return ("," + body).encode("utf-8")

        async def flush() -> bytes:
            nonlocal first_page_flushed
            chunk = take_chunk()
            first_page_flushed = True
            _bump("chunks_sent")
            _observe_peak("peak_chunk_bytes", len(chunk))
            if pace is not None:
                await pace()
            return chunk

        first_page_flushed = False
        while True:
            limit = first_limit if first_page else page_size
            try:
                rows = await anyio.to_thread.run_sync(
                    _read_and_encode_page, meta.batch_id, cursor, limit
                )
            except BatchStreamError:
                raise
            except Exception as exc:  # noqa: BLE001 - 统一转为可识别的流错误
                raise BatchStreamError(
                    f"读取批次 {meta.batch_id} 明细失败（cursor={cursor}）"
                ) from exc

            was_first_page = first_page
            first_page = False
            if not rows:
                break
            _bump("pages_read")
            _observe_peak("peak_page_records", len(rows))

            for idx, text in rows:
                # 下标必须从 0 连续递增；缺失/乱序说明落库数据不完整
                if idx != expected_index:
                    raise BatchStreamIntegrityError(
                        f"批次 {meta.batch_id} 明细不连续：期望 record_index="
                        f"{expected_index}，实际 {idx}"
                    )
                if emitted_records >= expected_total:
                    raise BatchStreamIntegrityError(
                        f"批次 {meta.batch_id} 明细多于登记的 record_count={expected_total}"
                    )
                expected_index += 1
                emitted_records += 1
                pending.append(text)
                pending_bytes += len(text) + 1  # +1 为分隔逗号

                if pending_bytes >= chunk_bytes:
                    yield await flush()

                if fail_after is not None and emitted_records >= fail_after:
                    if pending:
                        yield await flush()
                    # 故意在收尾前中止，模拟中途失败
                    raise BatchStreamError(
                        f"演示注入失败：已输出 {emitted_records} 条后中止"
                    )

            # 首段（首页明细）立即发出，不等攒满 chunk，也不等后续页读完
            if was_first_page and not first_page_flushed and pending:
                yield await flush()

            _bump("records_emitted", len(rows))
            cursor = rows[-1][0]

        # 收尾前做完整性校验：少读一条都视为失败，不输出完整 JSON
        if emitted_records != expected_total:
            raise BatchStreamIntegrityError(
                f"批次 {meta.batch_id} 明细缺失：登记 record_count={expected_total}，"
                f"实际读取 {emitted_records}"
            )

        if pending:
            chunk = take_chunk()
            _bump("chunks_sent")
            _observe_peak("peak_chunk_bytes", len(chunk))
            if pace is not None:
                await pace()
            yield chunk

        if pace is not None:
            await pace()
        _bump("chunks_sent")
        yield b"]}"
        _bump("streams_completed")
    except (GeneratorExit, cancelled_exc):
        # 客户端断开 / 上层取消：生成器被关闭，按断开计数后重新抛出
        _bump("streams_client_disconnected")
        raise
    except BaseException:
        _bump("streams_aborted")
        raise
    finally:
        # 任何退出路径（正常、异常、客户端断开）都断开对缓冲的引用；
        # 每页的数据库连接已在工作线程内关闭，无需在此额外回滚。
        pending.clear()
        pending = []
