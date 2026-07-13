#!/usr/bin/env bash
# =============================================================
# run_pipeline.sh
# Mandera Analytics — single script to bring up the full stack
# and trigger a pipeline run.
#
# Prerequisites (one-time manual steps, NOT automated here):
#   1. .env file exists with MONGO_URI, MONGO_DB, and
#      AIRFLOW__CORE__FERNET_KEY set (see README Quick Start).
#      These require secrets only you can generate.
#
# Usage:
#   bash run_pipeline.sh           # start + run (no rebuild)
#   bash run_pipeline.sh --build   # rebuild Airflow image first
#   bash run_pipeline.sh --fresh   # wipe volumes, rebuild, start, run
# =============================================================

set -euo pipefail

AIRFLOW_CONTAINER="mandera-airflow-webserver"
AIRFLOW_READY_TIMEOUT=120

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
abort() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

BUILD=false; FRESH=false
for arg in "$@"; do
  case "$arg" in
    --build) BUILD=true ;;
    --fresh) FRESH=true; BUILD=true ;;
  esac
done

# ── Preflight ───────────────────────────────────────────────
[[ -f ".env" ]] || abort ".env not found. Copy .env.example and fill in MONGO_URI, MONGO_DB, and AIRFLOW__CORE__FERNET_KEY."

FERNET_KEY=$(grep -E "^AIRFLOW__CORE__FERNET_KEY=" .env | cut -d= -f2- | tr -d '"' | tr -d "'" | tr -d '[:space:]')
[[ -n "$FERNET_KEY" ]] || abort "AIRFLOW__CORE__FERNET_KEY not set in .env. Generate:
  python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""

# ── Fresh wipe ──────────────────────────────────────────────
if [[ "$FRESH" == true ]]; then
  warn "--fresh: wiping all volumes. Press Ctrl+C within 5s to cancel."
  sleep 5
  docker compose down -v
  info "Volumes cleared."
fi

# ── Build ───────────────────────────────────────────────────
if [[ "$BUILD" == true ]]; then
  info "Building Airflow image ..."
  docker compose build airflow-webserver airflow-scheduler airflow-worker
fi

# ── Start stack ─────────────────────────────────────────────
info "Starting Docker services ..."
docker compose up -d

# ── Wait: postgres-init ─────────────────────────────────────
info "Waiting for schema setup (postgres-init) ..."
for i in $(seq 1 40); do
  STATUS=$(docker inspect --format='{{.State.Status}}' mandera-postgres-init 2>/dev/null || echo "pending")
  if [[ "$STATUS" == "exited" ]]; then
    EXIT_CODE=$(docker inspect --format='{{.State.ExitCode}}' mandera-postgres-init)
    [[ "$EXIT_CODE" == "0" ]] && { info "Schema ready."; break; } || \
      abort "postgres-init failed (exit $EXIT_CODE). Run: docker logs mandera-postgres-init"
  fi
  echo -n "."; sleep 3
done; echo ""

# ── Wait: Airflow webserver ─────────────────────────────────
info "Waiting for Airflow webserver (up to ${AIRFLOW_READY_TIMEOUT}s) ..."
ELAPSED=0
# Use full path — avoids host-vs-container PATH ambiguity that caused the
# "could not translate host name postgres" error: airflow commands MUST
# run inside the container where "postgres" resolves on mandera-net,
# not on the host where that hostname doesn't exist.
until docker exec "$AIRFLOW_CONTAINER" \
      /home/airflow/.local/bin/airflow version &>/dev/null; do
  sleep 5; ELAPSED=$((ELAPSED + 5)); echo -n "."
  [[ $ELAPSED -ge $AIRFLOW_READY_TIMEOUT ]] && \
    abort "Webserver not ready after ${AIRFLOW_READY_TIMEOUT}s. Check: docker logs $AIRFLOW_CONTAINER"
done; echo ""
sleep 5   # let the webserver fully settle before hitting the metadata DB
info "Airflow webserver ready."

# ── Init Airflow DB (inside container, idempotent) ──────────
info "Initialising Airflow metadata database ..."
docker exec "$AIRFLOW_CONTAINER" \
  /home/airflow/.local/bin/airflow db init 2>&1 | tail -3

# ── Create admin user (idempotent) ──────────────────────────
info "Creating admin user ..."
docker exec "$AIRFLOW_CONTAINER" \
  /home/airflow/.local/bin/airflow users create \
    --username admin --password admin \
    --firstname Mandera --lastname Admin \
    --role Admin --email admin@mandera.com 2>&1 | grep -v "^$" || true

# ── Unpause + trigger DAG ───────────────────────────────────
info "Unpausing mandera_batch_pipeline ..."
docker exec "$AIRFLOW_CONTAINER" \
  /home/airflow/.local/bin/airflow dags unpause mandera_batch_pipeline

info "Triggering pipeline run ..."
docker exec "$AIRFLOW_CONTAINER" \
  /home/airflow/.local/bin/airflow dags trigger mandera_batch_pipeline

# ── Done ────────────────────────────────────────────────────
echo ""
info "Pipeline triggered. Open:"
echo "  Airflow UI  →  http://localhost:8080"
echo "  MinIO       →  http://localhost:9001"
echo "  pgAdmin     →  http://localhost:5050"
echo ""
info "Watch the run:  docker logs -f mandera-airflow-worker"