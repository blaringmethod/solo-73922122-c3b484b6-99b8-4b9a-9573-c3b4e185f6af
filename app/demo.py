"""演示路由（仅在 ``DQG_DEMO_ENABLED=true`` 时挂载）。

用于在 ``/docs`` 中直观演示：
- 服务端生成大批次并登记（无需先上传巨大请求体）；
- 慢速接收：每个 chunk 发送前等待，模拟下游背压；
- 中途失败：输出若干条后中止，验证不会交付“缺行的完整 JSON”；
- 流式读取期间 WAL 可检查点回收、其他批次可正常登记。
"""
from __future__ import annotations

import anyio
import time

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from .config import settings
from .database import SessionLocal
from .schemas import SubmissionRequest
from .services import get_batch_meta, submit_batch
from .streaming import metrics_snapshot, stream_registered_batch

router = APIRouter(prefix="/demo", tags=["demo"])

_MAX_DEMO_RECORDS = 200_000


def _large_payload(batch_id: str, records: int) -> SubmissionRequest:
    data = [
        {
            "order_id": f"ORD-{i:07d}",
            "customer_email": f"user{i}@example.com",
            "status": "active",
            "amount": (i % 10_000) / 4 + 0.25,
            "currency": "CNY",
        }
        for i in range(records)
    ]
    return SubmissionRequest.model_validate(
        {
            "batch_id": batch_id,
            "schema_version": "order-schema/v2.1",
            "records": data,
            "rules": {
                "field_rules": {
                    "order_id": [
                        {"type": "required"},
                        {"type": "regex", "pattern": "^ORD-\\d{7}$"},
                    ],
                    "customer_email": [{"type": "required"}, {"type": "max_length", "max_length": 128}],
                    "status": [{"type": "in", "values": ["active", "inactive", "pending"]}],
                    "amount": [{"type": "type", "value": "number"}, {"type": "min", "min": 0}],
                    "currency": [{"type": "in", "values": ["CNY", "USD", "EUR"]}],
                },
                "unique_rules": [{"fields": ["order_id"]}, {"fields": ["customer_email", "currency"]}],
                "batch_rules": {
                    "max_failure_rate": 0.0,
                    "min_record_count": 1,
                    "max_record_count": _MAX_DEMO_RECORDS,
                },
            },
        }
    )


@router.post(
    "/batches",
    summary="[演示] 生成并登记一个大批次",
    description=(
        "服务端按 `records` 生成合法订单明细并走完整校验/登记流程；"
        "同一 `records` 使用同一 batch_id，重复调用为幂等重放。"
    ),
)
def create_large_batch(
    records: int = Query(default=50_000, ge=1, le=_MAX_DEMO_RECORDS),
    batch_id: str | None = Query(default=None, max_length=128),
) -> dict:
    bid = batch_id or f"demo-large-{records}"
    req = _large_payload(bid, records)
    started = time.perf_counter()
    db = SessionLocal()
    try:
        result = submit_batch(db, req)
    finally:
        db.close()
    return {
        "batch_id": result.batch_id,
        "status": result.status,
        "registered": result.registered,
        "replayed": result.replayed,
        "record_count": records,
        "submit_seconds": round(time.perf_counter() - started, 3),
        "read_url": f"/batches/{result.batch_id}",
        "slow_stream_url": f"/demo/batches/{result.batch_id}/slow",
        "fail_stream_url": f"/demo/batches/{result.batch_id}/fail",
    }


def _meta_or_404(batch_id: str):
    db = SessionLocal()
    try:
        meta = get_batch_meta(db, batch_id)
    finally:
        db.close()
    if meta is None:
        raise HTTPException(status_code=404, detail=f"批次 {batch_id} 未登记或未通过质量闸门")
    return meta


@router.get(
    "/batches/{batch_id}/slow",
    summary="[演示] 慢速接收：每个 chunk 发送前等待 delay_ms",
    description=(
        "以与 GET /batches/{batch_id} 相同的流式路径输出，但每个 chunk 前休眠，"
        "模拟下游处理慢、连接背压。休眠期间服务端不持有 SQLite 读事务，"
        "可同时调用 POST /demo/batches 登记新批次、POST /demo/db/checkpoint 回收 WAL。"
    ),
)
def slow_stream(
    batch_id: str,
    delay_ms: int = Query(default=300, ge=0, le=10_000),
    chunk_bytes: int = Query(default=8 * 1024, ge=256, le=1_048_576),
    first_records: int = Query(default=5, ge=1, le=10_000),
) -> StreamingResponse:
    meta = _meta_or_404(batch_id)

    async def pace() -> None:
        if delay_ms:
            await anyio.sleep(delay_ms / 1000)

    return StreamingResponse(
        stream_registered_batch(
            meta,
            chunk_bytes=chunk_bytes,
            first_segment_records=first_records,
            pace=pace,
        ),
        media_type="application/json",
        headers={"X-Demo-Mode": "slow", "Cache-Control": "no-store"},
    )


@router.get(
    "/batches/{batch_id}/fail",
    summary="[演示] 中途失败：输出 after_records 条后中止连接",
    description=(
        "模拟读取/编码中途出错。响应头与部分明细已发出，但不会补上收尾的 `]}`；"
        "客户端会收到传输层错误，拿不到可解析的完整 JSON。"
    ),
)
def failing_stream(
    batch_id: str,
    after_records: int = Query(default=50, ge=1, le=_MAX_DEMO_RECORDS),
    delay_ms: int = Query(default=100, ge=0, le=10_000),
) -> StreamingResponse:
    meta = _meta_or_404(batch_id)

    async def pace() -> None:
        if delay_ms:
            await anyio.sleep(delay_ms / 1000)

    return StreamingResponse(
        stream_registered_batch(
            meta,
            chunk_bytes=512,
            first_segment_records=min(after_records, 5),
            pace=pace,
            fail_after=after_records,
        ),
        media_type="application/json",
        headers={"X-Demo-Mode": "fail", "Cache-Control": "no-store"},
    )


@router.get(
    "/batches/{batch_id}/head",
    summary="[演示] 只取响应最前面的字节（验证首段先到）",
)
def head_of_stream(
    batch_id: str,
    bytes_: int = Query(default=256, alias="bytes", ge=1, le=65_536),
) -> dict:
    meta = _meta_or_404(batch_id)
    collected = {"body": bytearray(), "records": 0, "chunks": []}
    started = time.perf_counter()

    async def _collect() -> None:
        agen = stream_registered_batch(
            meta, chunk_bytes=max(bytes_, 256), first_segment_records=5
        )
        try:
            async for chunk in agen:
                collected["body"].extend(chunk)
                collected["chunks"].append(len(chunk))
                collected["records"] += chunk.count(b'{"record_index":')
                if len(collected["body"]) >= bytes_:
                    break
        finally:
            # 客户端在 prefix 处停止读取：关闭生成器，释放页连接与缓冲
            await agen.aclose()

    anyio.run(_collect)
    body = collected["body"]
    elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
    return {
        "batch_id": batch_id,
        "requested_bytes": bytes_,
        "received_bytes": len(body),
        "chunk_sizes": collected["chunks"],
        "records_in_prefix": collected["records"],
        "elapsed_ms": elapsed_ms,
        "prefix": body[:bytes_].decode("utf-8", errors="replace"),
        "note": (
            "首个 chunk 即批次头+首页明细；只读了前缀就关闭流，"
            "未读取其余明细，说明首段无需等待整批。"
        ),
    }


@router.post(
    "/db/checkpoint",
    summary="[演示] 尝试 WAL TRUNCATE 检查点（慢速接收期间应能成功回收）",
    description="返回 `busy` 标志；busy=0 表示没有活跃读事务阻塞，WAL 已截断。",
)
def checkpoint_wal() -> dict:
    db = SessionLocal()
    try:
        db.execute(text("PRAGMA busy_timeout=1500"))
        busy, log_frames, checkpointed = db.execute(
            text("PRAGMA wal_checkpoint(TRUNCATE)")
        ).one()
        db.commit()
    finally:
        db.close()
    return {
        "busy": int(busy),
        "wal_log_frames": int(log_frames),
        "checkpointed_frames": int(checkpointed),
        "reclaimed": int(busy) == 0 and int(log_frames) == int(checkpointed),
    }


@router.get(
    "/metrics",
    summary="[演示] 流式读取运行指标（chunk/页/内存上界观测）",
)
def metrics() -> dict:
    return {
        "config": {
            "page_size": settings.stream_page_size,
            "chunk_bytes": settings.stream_chunk_bytes,
            "first_segment_records": settings.stream_first_segment_records,
        },
        "metrics": metrics_snapshot(),
    }
