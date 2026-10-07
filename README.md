# ecommerce-support-agent (ecombot)

A small FastAPI + LangGraph support agent that answers questions about sales, inventory, profit
and customers from the `ECOMM_DATA_LAKE.CONFORMED` Snowflake schema. It is the application under
test for AutoResearch in agent-ta.

## Run locally

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt -c constraints.txt
cp .env.example .env   # fill in LLM_* and SNOWFLAKE_* values
uvicorn app.main:app --port 8010
```

`GET /health` is liveness; `GET /ready` also pings Snowflake. `POST /chat` takes
`{"message": "..."}` or an OpenAI-style `{"messages": [...]}` and, when `ECOMBOT_API_KEY` is set,
requires the key as `X-API-Key` or `Authorization: Bearer <key>`. `LOG_LEVEL` (default INFO)
controls logging; `SNOWFLAKE_TIMEZONE` (default Asia/Kolkata) sets the session timezone used for
"today"/"this month" style windows.

## Seed data

```bash
SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=... python scripts/seed_snowflake.py
```

The seed appends and refuses to run when `DIM_CUSTOMER` already has rows; truncate the
`CONFORMED` tables first or pass `--force`. Orders are seeded for January–June 2026, so
"last 30 days" style questions will say which dates have data.

## Docker

Built and run from `agent-ta-backend/docker-compose.yml` as the `ecombot` and `ecombot-staging`
services.

## Tests

```bash
pip install -r requirements.txt -r requirements-dev.txt -c constraints.txt
python -m pytest -q tests && python -m pyflakes app tests scripts
```

Three bugs are planted on purpose as AutoResearch targets (see the header of
`scripts/seed_snowflake.py`); do not fix them.
