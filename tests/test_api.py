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
    class FakeGraph:
        def invoke(self, state):
            return {"response": f"echo: {state['messages'][-1]['content']}"}

    monkeypatch.setattr(main_mod, "graph", FakeGraph())
    client = TestClient(main_mod.app)
    resp = client.post("/chat", json={"message": "hello"})
    assert resp.status_code == 200
    assert resp.json() == {"response": "echo: hello"}


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
