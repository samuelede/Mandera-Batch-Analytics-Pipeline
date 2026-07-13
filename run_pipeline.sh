#!/usr/bin/env bash
# =============================================================
# run_pipeline.sh
# Mandera Analytics — single script to bring up the full stack
# and trigger a pipeline run.
#
# Prerequisites (one-time, manual — see README):
#   .env must exist with MONGO_URI, MONGO_DB, AIRFLOW__CORE__FERNET_KEY
#
#   IMPORTANT: do NOT put AIRFLOW__DATABASE__SQL_ALCHEMY_CONN or
#   AIRFLOW__CELERY__RESULT_BACKEND in .env. ${VAR} syntax does not
#   interpolate in .env files (it's sent as literal text), and the
#   host-side localhost:5433 values are wrong inside containers anyway.
#   docker-compose.yml sets both correctly using postgres:5432.
#
# Usage:
#   bash run_pipeline.sh           # start + run (no rebuild)
#   bash run_pipeline.sh --build   # rebuild Airflow image first
#   bash run_pipeline.sh --fresh   # wipe volumes, rebuild, start, run
# =============================================================

set -euo pipefail

AIRFLOW_CONTAINER="mandera-airflow-webserver"
# Use the bare command, NOT an absolute path like
# /home/airflow/.local/bin/airflow — Git Bash (MSYS) on Windows
# rewrites absolute Unix paths into Windows paths before Docker sees
# them, producing:
#   exec: "C:/Program Files/Git/home/airflow/.local/bin/airflow": no such file
# `docker exec <container> airflow` resolves against the CONTAINER's
# PATH, so the bare command is both correct and MSYS-safe.
AIRFLOW_CMD="airflow"
CONTAINER_UP_TIMEOUT=90    # wait for container process to exist
WEBSERVER_TIMEOUT=180      # wait for webserver to serve after db init

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
[[ -f ".env" ]] || abort ".env not found. Copy .env.example and fill in MONGO_URI, MONGO_DB, AIRFLOW__CORE__FERNET_KEY."

FERNET_KEY=$(grep -E "^AIRFLOW__CORE__FERNET_KEY=" .env | cut -d= -f2- | tr -d '"' | tr -d "'" | tr -d '[:space:]')
[[ -n "$FERNET_KEY" ]] || abort "AIRFLOW__CORE__FERNET_KEY not set in .env. Generate:
  python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""

# Guard against the .env interpolation trap that caused repeated
# "password authentication failed" auth loops at ~100% CPU.
if grep -qE "^AIRFLOW__(DATABASE__SQL_ALCHEMY_CONN|CELERY__RESULT_BACKEND)=" .env; then
  abort "Remove AIRFLOW__DATABASE__SQL_ALCHEMY_CONN and AIRFLOW__CELERY__RESULT_BACKEND from .env.
  \${VAR} does not interpolate in .env files — Airflow receives literal '\${POSTGRES_USER}'
  as the username and the auth fails in a tight retry loop.
  docker-compose.yml already sets both correctly using postgres:5432."
fi

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
    CODE=$(docker inspect --format='{{.State.ExitCode}}' mandera-postgres-init)
    [[ "$CODE" == "0" ]] && { info "Schema ready."; break; } || \
      abort "postgres-init failed (exit $CODE). Run: docker logs mandera-postgres-init"
  fi
  echo -n "."; sleep 3
done; echo ""

# ── Wait: container process exists (NOT full webserver readiness) ──
# The Airflow webserver CANNOT fully start until the metadata database
# is initialised — but `airflow db init` needs a running container to
# exec into. Waiting for full webserver readiness here would deadlock:
# webserver waits for db init, db init waits for webserver.
# So we only wait for the container's Python/Airflow CLI to be callable,
# then run db init, THEN wait for the webserver to actually serve.
info "Waiting for Airflow container to accept commands ..."
ELAPSED=0
until docker exec "$AIRFLOW_CONTAINER" "$AIRFLOW_CMD" version &>/dev/null; do
  sleep 5; ELAPSED=$((ELAPSED + 5)); echo -n "."
  [[ $ELAPSED -ge $CONTAINER_UP_TIMEOUT ]] && \
    abort "Airflow container not responding after ${CONTAINER_UP_TIMEOUT}s.
    Check: docker logs $AIRFLOW_CONTAINER"
done; echo ""
info "Airflow container is up."

# ── Init Airflow metadata DB (idempotent) ───────────────────
# Must run BEFORE waiting for webserver readiness — see note above.
info "Initialising Airflow metadata database ..."
docker exec "$AIRFLOW_CONTAINER" "$AIRFLOW_CMD" db init 2>&1 | tail -3
info "Metadata database initialised."

# ── Create admin user (idempotent) ──────────────────────────
info "Creating admin user ..."
docker exec "$AIRFLOW_CONTAINER" "$AIRFLOW_CMD" users create \
  --username admin --password admin \
  --firstname Mandera --lastname Admin \
  --role Admin --email admin@mandera.com 2>&1 | grep -vE "^$" || true

# ── Restart webserver so it picks up the initialised DB ──────
# The webserver has been crash-looping on "you need to initialize the
# database" until now. Restarting lets it start cleanly.
info "Restarting webserver to pick up initialised database ..."
docker compose restart airflow-webserver >/dev/null 2>&1

info "Waiting for webserver to serve (up to ${WEBSERVER_TIMEOUT}s) ..."
ELAPSED=0
until docker exec "$AIRFLOW_CONTAINER" "$AIRFLOW_CMD" dags list &>/dev/null; do
  sleep 5; ELAPSED=$((ELAPSED + 5)); echo -n "."
  [[ $ELAPSED -ge $WEBSERVER_TIMEOUT ]] && \
    abort "Webserver not serving after ${WEBSERVER_TIMEOUT}s. Check: docker logs $AIRFLOW_CONTAINER"
done; echo ""
info "Webserver ready."

# ── Unpause + trigger DAG ───────────────────────────────────
info "Unpausing mandera_batch_pipeline ..."
docker exec "$AIRFLOW_CONTAINER" "$AIRFLOW_CMD" dags unpause mandera_batch_pipeline

info "Triggering pipeline run ..."
docker exec "$AIRFLOW_CONTAINER" "$AIRFLOW_CMD" dags trigger mandera_batch_pipeline

# ── Detect actual host port (may not be 8080 if remapped) ───
PORT=$(docker inspect \
  --format='{{range $p, $c := .NetworkSettings.Ports}}{{if eq $p "8080/tcp"}}{{(index $c 0).HostPort}}{{end}}{{end}}' \
  "$AIRFLOW_CONTAINER" 2>/dev/null || echo "8080")
PORT=${PORT:-8080}

# ── Done ────────────────────────────────────────────────────
echo ""
info "Pipeline triggered successfully."
echo ""
echo "  Airflow UI  →  http://localhost:${PORT}"
echo "  MinIO       →  http://localhost:9001"
echo "  pgAdmin     →  http://localhost:5050"
echo ""
info "Watch the run:  docker logs -f mandera-airflow-worker"