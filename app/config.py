"""运行期配置，全部支持通过环境变量覆盖。"""
from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"环境变量 {name} 必须是整数，实际为 {raw!r}") from exc
    if value < minimum:
        raise ValueError(f"环境变量 {name} 必须 >= {minimum}，实际为 {value}")
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class Settings:
    # SQLite 数据库文件位置（容器内默认 /data/dqg.db）
    db_path: str = os.getenv("DQG_DB_PATH", str(BASE_DIR / "data" / "dqg.db"))
    # 规则目录（规则类型说明）
    catalog_path: str = os.getenv(
        "DQG_CATALOG_PATH", str(BASE_DIR / "data" / "rule_catalog.json")
    )
    # 示例批次数据
    sample_path: str = os.getenv(
        "DQG_SAMPLE_PATH", str(BASE_DIR / "data" / "sample_payloads.json")
    )

    # 流式读取：每页从 SQLite 读取的记录数（决定单次读事务长度与内存上界）
    stream_page_size: int = _env_int("DQG_STREAM_PAGE_SIZE", 500)
    # 流式输出：攒够多少字节后向下游发送一个 chunk
    stream_chunk_bytes: int = _env_int("DQG_STREAM_CHUNK_BYTES", 64 * 1024)
    # 首批 chunk 最多包含多少条明细，保证首段尽快到达（其余按页读取）
    stream_first_segment_records: int = _env_int("DQG_STREAM_FIRST_SEGMENT_RECORDS", 20)

    # 仅演示用：是否挂载 /demo 路由（大批次生成、慢速接收观测、故障注入）
    demo_enabled: bool = _env_bool("DQG_DEMO_ENABLED", False)

    @property
    def db_url(self) -> str:
        return f"sqlite:///{self.db_path}"


settings = Settings()
