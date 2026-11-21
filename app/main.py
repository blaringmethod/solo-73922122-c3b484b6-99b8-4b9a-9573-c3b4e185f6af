"""FastAPI 应用入口。"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import __version__
from .config import settings
from .database import init_db
from .routers import router


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Data Quality Gate API",
    description=(
        "数据质量闸门：接收带 schema 版本的批次数据与质量规则，"
        "执行字段约束、跨记录唯一性与批次级阈值校验，"
        "仅在校验通过时登记可供下游读取的批次版本。\n\n"
        "`GET /batches/{batch_id}` 采用**流式输出**：先返回批次头与首段明细，"
        "按页读取并以常量级内存编码，慢速下游不会长期占用 SQLite 读事务。"
    ),
    version=__version__,
    lifespan=lifespan,
)

app.include_router(router)

if settings.demo_enabled:
    from .demo import router as demo_router

    app.include_router(demo_router)
