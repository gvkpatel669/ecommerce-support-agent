from fastapi.testclient import TestClient

import app.main as main_mod


def test_chat_rejects_empty_payload():
    client = TestClient(main_mod.app)
    assert client.post("/chat", json={}).status_code == 422


def test_chat_rejects_oversized_message():
    client = TestClient(main_mod.app)
    resp = client.post("/chat", json={"message": "x" * 5000})
    assert resp.status_code == 422


def test_chat_runs_graph_off_event_loop(monkeypatch):
    import threading
    seen = {}

    class FakeGraph:
        def invoke(self, state):
            seen["thread"] = threading.current_thread().name
            return {"response": f"echo: {state['messages'][-1]['content']}"}

    monkeypatch.setattr(main_mod, "graph", FakeGraph())
    with TestClient(main_mod.app) as client:  # context manager runs the lifespan
        resp = client.post("/chat", json={"message": "hello"})
    assert resp.status_code == 200
    assert resp.json() == {"response": "echo: hello"}
    assert "MainThread" not in seen["thread"]  # ran in the threadpool, not on the event loop


def test_multipart_content_is_flattened(monkeypatch):
    monkeypatch.setattr(main_mod, "graph", type("G", (), {"invoke": lambda self, s: {"response": s["messages"][-1]["content"]}})())
    client = TestClient(main_mod.app)
    resp = client.post("/chat", json={"messages": [{"role": "user", "content": [{"type": "text", "text": "stock levels"}]}]})
    assert resp.status_code == 200
    assert resp.json()["response"] == "stock levels"


def test_oversized_body_rejected_before_parsing():
    client = TestClient(main_mod.app)
    resp = client.post("/chat", content=b"{" + b" " * (main_mod.MAX_BODY_BYTES + 10) + b"}", headers={"Content-Type": "application/json"})
    assert resp.status_code == 413


def test_lifespan_fails_fast_without_llm_key(monkeypatch):
    from pydantic import SecretStr
    import pytest
    monkeypatch.setattr(main_mod.settings, "LLM_API_KEY", SecretStr(""))
    with pytest.raises(RuntimeError):
        with TestClient(main_mod.app):
            pass


def test_chat_hides_internal_errors(monkeypatch):
    class BoomGraph:
        def invoke(self, state):
            raise RuntimeError("snowflake host secret-host.snowflakecomputing.com")

    monkeypatch.setattr(main_mod, "graph", BoomGraph())
    client = TestClient(main_mod.app)
    resp = client.post("/chat", json={"message": "hello"})
    assert resp.status_code == 500
    assert "snowflake" not in resp.text.lower()
    assert "request_id" in resp.json()


def test_api_key_enforced_when_configured(monkeypatch):
    from pydantic import SecretStr

    monkeypatch.setattr(main_mod.settings, "ECOMBOT_API_KEY", SecretStr("s3cret"))
    client = TestClient(main_mod.app)
    assert client.post("/chat", json={"message": "hi"}).status_code == 401
    monkeypatch.setattr(main_mod, "graph", type("G", (), {"invoke": lambda self, s: {"response": "ok"}})())
    assert client.post("/chat", json={"message": "hi"}, headers={"X-API-Key": "s3cret"}).status_code == 200
    assert client.post("/chat", json={"message": "hi"}, headers={"Authorization": "Bearer s3cret"}).status_code == 200
    assert client.post("/chat", json={"message": "hi"}, headers={"Authorization": "Bearer wrong"}).status_code == 401
