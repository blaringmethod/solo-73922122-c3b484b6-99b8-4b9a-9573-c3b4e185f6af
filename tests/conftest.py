"""pytest 全局配置：在任何 app 模块导入前固定测试数据库路径。"""
from __future__ import annotations

import os
import tempfile

# 必须在导入 app.* 之前设置；各测试模块共享同一个临时库（批次 ID 互不冲突）
os.environ.setdefault(
    "DQG_DB_PATH", os.path.join(tempfile.mkdtemp(prefix="dqg-pytest-"), "test.db")
)
# 启用演示路由，便于在测试中覆盖大批次/慢速/失败注入路径
os.environ.setdefault("DQG_DEMO_ENABLED", "true")
