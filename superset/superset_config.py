import os
from urllib.parse import quote_plus

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change-this-in-production")

_db_pass = quote_plus(os.getenv("SUPERSET_DB_PASSWORD", "superset_secret"))
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://superset:{_db_pass}"
    f"@superset-db:5432/superset"
)

# Disable flask-compress when not behind a proxy (gunicorn handles this)
COMPRESS_REGISTER = False

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": "redis://superset-cache:6379/0",
}

DATA_CACHE_CONFIG = CACHE_CONFIG
