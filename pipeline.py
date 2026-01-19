# pipeline.py
# Dagster orchestration for: scraper -> load -> dbt -> yolo
# Updated to:
# - always use the venv interpreter (sys.executable) instead of "python"
# - run subprocesses unbuffered (-u) so stderr/stdout show in Dagster UI
# - validate important paths you shared (images root, detections.csv location)
# - keep strict execution order via dependency tokens
# - keep failure hook returning HookExecutionResult (required by your Dagster version)
# - REGISTER schedules via Definitions so Dagster UI shows them

import os
import sys
import subprocess
from datetime import datetime, timezone
from typing import Optional, Sequence

from dagster import (
    DefaultScheduleStatus,
    Definitions,              # <-- NEW
    Failure,
    HookDefinition,
    HookExecutionResult,
    ScheduleDefinition,
    job,
    op,
)

# -------------------------
# Project paths
# -------------------------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

IMAGES_ROOT = os.path.join(PROJECT_ROOT, "data", "raw", "images")
RAW_CSV_ROOT = os.path.join(PROJECT_ROOT, "data", "raw", "csv")

YOLO_DETECTIONS_CSV = os.path.join(
    PROJECT_ROOT, "data", "processed", "yolo", "detections.csv")
DBT_DIR = os.path.join(PROJECT_ROOT, "medical_warehouse")


# -------------------------
# Subprocess runner
# -------------------------
def run_cmd(
    context,
    cmd: Sequence[str],
    cwd: Optional[str] = None,
    env: Optional[dict] = None,
) -> str:
    """Run a command, log stdout/stderr to Dagster, and raise dagster.Failure on errors."""
    cwd = cwd or PROJECT_ROOT
    env = env or os.environ.copy()

    context.log.info(f"Running: {' '.join(cmd)} (cwd={cwd})")

    p = subprocess.run(
        list(cmd),
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
        shell=False,
    )

    if p.stdout:
        context.log.info(p.stdout.strip())
    if p.stderr:
        if p.returncode == 0:
            context.log.warning(p.stderr.strip())
        else:
            context.log.error(p.stderr.strip())

    if p.returncode != 0:
        raise Failure(f"Command failed: {' '.join(cmd)}")

    return (p.stdout or "").strip()


# -------------------------
# Failure hook
# -------------------------
def _on_failure(hook_context, event_list):
    hook_context.log.error(
        f"[ALERT] Failure in op={hook_context.op.name} | run_id={hook_context.run_id}"
    )
    return HookExecutionResult(hook_name="failure_alert_hook", is_skipped=False)


failure_alert_hook = HookDefinition(
    name="failure_alert_hook", hook_fn=_on_failure)


# -------------------------
# Ops
# -------------------------
@op
def scrape_telegram_data(context) -> str:
    run_cmd(context, [sys.executable, "-u", os.path.join("src", "scraper.py")])

    run_date = datetime.now(timezone.utc).date().isoformat()
    context.log.info(f"Scrape finished. run_date={run_date}")
    return run_date


@op
def load_raw_to_postgres(context, run_date: str) -> str:
    run_cmd(context, [sys.executable, "-u",
            os.path.join("src", "load_raw_to_postgres.py")])

    token = f"loaded:{run_date}"
    context.log.info(f"Load finished. token={token}")
    return token


@op
def run_dbt_transformations(context, upstream_token: str) -> str:
    if not os.path.isdir(DBT_DIR):
        raise Failure(f"dbt project directory not found: {DBT_DIR}")

    run_cmd(context, ["dbt", "--version"], cwd=DBT_DIR)
    run_cmd(context, ["dbt", "deps"], cwd=DBT_DIR)
    run_cmd(context, ["dbt", "build"], cwd=DBT_DIR)

    token = f"dbt_done:{upstream_token}"
    context.log.info(f"dbt finished. token={token}")
    return token


@op
def run_yolo_enrichment(context, dbt_token: str) -> str:
    if not os.path.isdir(IMAGES_ROOT):
        raise Failure(f"Images root folder not found: {IMAGES_ROOT}")

    os.makedirs(os.path.dirname(YOLO_DETECTIONS_CSV), exist_ok=True)
    context.log.info(f"YOLO detections output path: {YOLO_DETECTIONS_CSV}")

    run_cmd(context, [sys.executable, "-u",
            os.path.join("src", "yolo_detect.py")])

    if not os.path.isfile(YOLO_DETECTIONS_CSV):
        raise Failure(
            "YOLO script finished but detections.csv was not found at expected path: "
            f"{YOLO_DETECTIONS_CSV}"
        )

    context.log.info(f"YOLO finished. upstream={dbt_token}")
    return f"yolo_done:{dbt_token}"


# -------------------------
# Job graph (strict order)
# -------------------------
@job(hooks={failure_alert_hook})
def shipping_data_product_job():
    run_date = scrape_telegram_data()
    load_token = load_raw_to_postgres(run_date)
    dbt_token = run_dbt_transformations(load_token)
    _ = run_yolo_enrichment(dbt_token)


# -------------------------
# Daily schedule (02:00 UTC)
# -------------------------
daily_schedule = ScheduleDefinition(
    name="daily_schedule",  # <-- explicit name helps UI clarity
    job=shipping_data_product_job,
    cron_schedule="0 2 * * *",
    default_status=DefaultScheduleStatus.STOPPED,  # enable manually in Dagster UI
)

# -------------------------
# IMPORTANT: Register job + schedule so Dagster UI can see them
# -------------------------
defs = Definitions(
    jobs=[shipping_data_product_job],
    schedules=[daily_schedule],
)
