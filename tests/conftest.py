import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Environment overrides beat the repo .env, so tests never depend on local secrets/config.
os.environ.setdefault("LLM_API_KEY", "test-key")
os.environ["ECOMBOT_API_KEY"] = ""
os.environ.setdefault("SNOWFLAKE_ROLE", "")
