import os
import subprocess
from datetime import datetime, timezone
from typing import Optional

from dagster import (
    Failure,
    HookDefinition,
    ScheduleDefinition,
    job,
    op,
)


# -------------------------
# Helpers
# -------------------------
def run_cmd(context, cmd: list[str], cwd: Optional[str] = None) -> str:
    """
    Run a command and stream useful logs to Dagster.
    Raises dagster.Failure on non-zero exit.
    Returns stdout as text (for optional downstream use).
    """
    cwd = cwd or os.getcwd()
    context.log.info(f"Running: {' '.join(cmd)} (cwd={cwd})")

    try:
        p = subprocess.run(
            cmd,
            cwd=cwd,
            check=True,
            text=True,
            capture_output=True,
            shell=False,
        )
        if p.stdout:
            context.log.info(p.stdout.strip())
        if p.stderr:
            # many tools write warnings to stderr even on success
            context.log.warning(p.stderr.strip())

        return (p.stdout or "").strip()

    except subprocess.CalledProcessError as e:
        stdout = (e.stdout or "").strip()
        stderr = (e.stderr or "").strip()

        if stdout:
            context.log.error(f"[stdout]\n{stdout}")
        if stderr:
            context.log.error(f"[stderr]\n{stderr}")

        raise Failure(f"Command failed: {' '.join(cmd)}") from e


# -------------------------
# Failure hook (FIXED SIGNATURE)
# -------------------------
def _on_failure(hook_context, event_list):
    # Minimal “alerting”: log an error with run/op context.
    # You can later extend this to email/Slack, etc.
    hook_context.log.error(
        f"[ALERT] Failure in op={hook_context.op.name} | run_id={hook_context.run_id}"
    )


failure_alert_hook = HookDefinition(
    name="failure_alert_hook", hook_fn=_on_failure)


# -------------------------
# Ops (with dependency tokens)
# -------------------------
@op
def scrape_telegram_data(context) -> str:
    """
    Runs the Telegram scraper.
    Returns a run_date token so downstream ops have an explicit dependency.
    """
    run_cmd(context, ["python", "src/scraper.py"])

    run_date = datetime.now(timezone.utc).date().isoformat()
    context.log.info(f"Scrape finished. run_date={run_date}")
    return run_date


@op
def load_raw_to_postgres(context, run_date: str) -> str:
    """
    Loads raw JSON into Postgres.
    Consumes run_date (dependency token) and returns a load token.
    """
    # If your loader accepts args, you can pass run_date, otherwise just run it.
    # Example (optional): ["python", "src/load_to_postgres.py", "--run-date", run_date]
    run_cmd(context, ["python", "src/load_to_postgres.py"])

    token = f"loaded:{run_date}"
    context.log.info(f"Load finished. token={token}")
    return token


@op
def run_dbt_transformations(context, upstream_token: str) -> str:
    """
    Executes dbt deps + dbt build in medical_warehouse.
    Consumes upstream_token to enforce order, returns dbt token.
    """
    dbt_dir = os.path.join(os.getcwd(), "medical_warehouse")

    # Helpful preflight logging
    run_cmd(context, ["dbt", "--version"])
    run_cmd(context, ["dbt", "deps"], cwd=dbt_dir)
    run_cmd(context, ["dbt", "build"], cwd=dbt_dir)

    token = f"dbt_done:{upstream_token}"
    context.log.info(f"dbt finished. token={token}")
    return token


@op
def run_yolo_enrichment(context, dbt_token: str) -> None:
    """
    Runs YOLO enrichment only after dbt succeeds.
    """
    run_cmd(context, ["python", "src/yolo_detect.py"])
    context.log.info(f"YOLO finished. upstream={dbt_token}")


# -------------------------
# Job graph (STRICT ORDER)
# -------------------------
@job(hooks={failure_alert_hook})
def shipping_data_product_job():
    run_date = scrape_telegram_data()
    load_token = load_raw_to_postgres(run_date)
    dbt_token = run_dbt_transformations(load_token)
    run_yolo_enrichment(dbt_token)


# -------------------------
# Schedule (daily 02:00 UTC)
# -------------------------
daily_schedule = ScheduleDefinition(
    job=shipping_data_product_job,
    cron_schedule="0 2 * * *",  # 02:00 UTC daily
    default_status="STOPPED",   # enable manually in Dagster UI when ready
)
