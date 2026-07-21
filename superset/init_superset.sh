#!/bin/bash
# superset/init_superset.sh
set -e

superset db upgrade

superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@crypto.local \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}"

superset init

# Register Snowflake connection
python3 - << PYEOF
import os
from urllib.parse import quote_plus
from superset.app import create_app

app = create_app()
with app.app_context():
    from superset.extensions import db
    from superset.models.core import Database

    acct = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    pwd  = os.getenv("SNOWFLAKE_PASSWORD")
    db_  = os.getenv("SNOWFLAKE_DATABASE", "CRYPTO_ANALYTICS")
    wh   = os.getenv("SNOWFLAKE_WAREHOUSE", "CRYPTO_PIPELINE_WH")
    role = os.getenv("SNOWFLAKE_ROLE", "CRYPTO_PIPELINE_ROLE")

    uri = (
        f"snowflake://{quote_plus(user)}:{quote_plus(pwd)}"
        f"@{acct}/{db_}/MARTS?warehouse={wh}&role={role}"
    )

    existing = db.session.query(Database).filter_by(database_name="Crypto Snowflake").first()
    if not existing:
        database = Database(
            database_name="Crypto Snowflake",
            sqlalchemy_uri=uri,
            expose_in_sqllab=True,
        )
        db.session.add(database)
        db.session.commit()
        print("Snowflake connection registered.")
    else:
        print("Snowflake connection already exists.")
PYEOF

echo "Superset init complete."

# Start gunicorn
exec gunicorn \
  --bind 0.0.0.0:8088 \
  --workers 4 \
  --worker-class gthread \
  --threads 4 \
  --timeout 120 \
  --limit-request-line 0 \
  --limit-request-field_size 0 \
  'superset.app:create_app()'