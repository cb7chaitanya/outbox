#!/usr/bin/env bash
# Runs once against the default admin database on first container start
# (docker-entrypoint-initdb.d convention). Creates one database per
# service so each keeps a separate schema/pool (spec section 6).
set -euo pipefail

for db in orders inventory payments fulfilment; do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-SQL
    SELECT 'CREATE DATABASE $db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db')\gexec
SQL
done
