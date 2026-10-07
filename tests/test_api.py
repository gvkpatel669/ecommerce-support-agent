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
            seen["invoke_thread"] = threading.get_ident()
            return {"response": f"echo: {state['messages'][-1]['content']}"}

    async def record_loop_thread():  # async dependencies run on the event-loop thread
        seen["loop_thread"] = threading.get_ident()

    monkeypatch.setattr(main_mod, "graph", FakeGraph())
    main_mod.app.dependency_overrides[main_mod.require_api_key] = record_loop_thread
    try:
        with TestClient(main_mod.app) as client:  # context manager runs the lifespan
            resp = client.post("/chat", json={"message": "hello"})
    finally:
        main_mod.app.dependency_overrides.clear()
    assert resp.status_code == 200
    assert resp.json() == {"response": "echo: hello"}
    assert seen["invoke_thread"] != seen["loop_thread"]  # graph ran in the threadpool, not on the loop


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


def test_chunked_oversized_body_rejected():
    client = TestClient(main_mod.app)

    def chunks():
        for _ in range(main_mod.MAX_BODY_BYTES // 1024 + 2):
            yield b" " * 1024

    resp = client.post("/chat", content=chunks(), headers={"Content-Type": "application/json"})
    assert resp.status_code == 413


def test_non_ascii_api_key_is_401_not_500(monkeypatch):
    import pytest
    from fastapi import HTTPException
    from pydantic import SecretStr

    monkeypatch.setattr(main_mod.settings, "ECOMBOT_API_KEY", SecretStr("s3cret"))
    # hmac.compare_digest raises TypeError on non-ASCII str; the dependency must compare bytes.
    with pytest.raises(HTTPException) as exc:
        main_mod.require_api_key(x_api_key="s\u00e9cret", authorization=None)
    assert exc.value.status_code == 401


def test_multipart_with_null_text_is_422():
    client = TestClient(main_mod.app)
    resp = client.post("/chat", json={"messages": [{"role": "user", "content": [{"type": "text", "text": None}]}]})
    assert resp.status_code == 422


def test_chunked_body_within_limit_still_parses(monkeypatch):
    monkeypatch.setattr(main_mod, "graph", type("G", (), {"invoke": lambda self, s: {"response": "ok"}})())
    client = TestClient(main_mod.app)

    def chunks():
        yield b'{"message": '
        yield b'"hello"}'

    resp = client.post("/chat", content=chunks(), headers={"Content-Type": "application/json"})
    assert resp.status_code == 200


def test_multipart_non_string_text_is_422():
    client = TestClient(main_mod.app)
    resp = client.post("/chat", json={"messages": [{"role": "user", "content": [{"type": "text", "text": 5}]}]})
    assert resp.status_code == 422


def test_blank_string_content_is_422():
    client = TestClient(main_mod.app)
    resp = client.post("/chat", json={"message": "   "})
    assert resp.status_code == 422
