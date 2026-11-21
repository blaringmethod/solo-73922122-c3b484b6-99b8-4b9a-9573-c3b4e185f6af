"""演示路由自测：大批次生成、前缀读取、失败注入与 WAL 检查点。"""
from __future__ import annotations

import json

from fastapi.testclient import TestClient

from app.database import init_db
from app.main import app

init_db()
client = TestClient(app)


def test_demo_routes_are_mounted() -> None:
    paths = {r.path for r in app.routes}
    assert "/demo/batches" in paths
    assert "/demo/batches/{batch_id}/slow" in paths
    assert "/demo/batches/{batch_id}/fail" in paths
    assert "/demo/db/checkpoint" in paths


def test_demo_large_batch_and_prefix_read() -> None:
    r = client.post("/demo/batches", params={"records": 500, "batch_id": "demo-test-500"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "PASSED" and body["registered"] is True

    head = client.get(
        "/demo/batches/demo-test-500/head", params={"bytes": 200}
    ).json()
    assert head["records_in_prefix"] >= 1
    assert head["prefix"].startswith('{"batch_id":"demo-test-500"')
    assert head["prefix"].endswith('"records":[') or '"record_index":0' in head["prefix"]
    assert len(head["prefix"].encode("utf-8")) <= 200
    # 只读了前缀：收到的字节远小于整批编码后的体积，且首个 chunk 远小于整批
    full = client.get("/batches/demo-test-500").content
    assert head["received_bytes"] < len(full)
    assert head["chunk_sizes"][0] < len(full)

    # 幂等重放
    again = client.post(
        "/demo/batches", params={"records": 500, "batch_id": "demo-test-500"}
    ).json()
    assert again["replayed"] is True and again["status"] == "PASSED"


def test_demo_checkpoint_reports_reclaimed() -> None:
    r = client.post("/demo/db/checkpoint")
    assert r.status_code == 200
    body = r.json()
    assert body["busy"] == 0
    assert body["reclaimed"] is True
    assert body["wal_log_frames"] >= body["checkpointed_frames"]


def test_demo_metrics_after_stream_roundtrip() -> None:
    client.post("/demo/batches", params={"records": 50, "batch_id": "demo-test-50"})
    assert json.loads(client.get("/batches/demo-test-50").content)["record_count"] == 50
    metrics = client.get("/demo/metrics").json()
    assert metrics["metrics"]["streams_completed"] >= 1
    assert metrics["metrics"]["peak_page_records"] <= metrics["config"]["page_size"]
