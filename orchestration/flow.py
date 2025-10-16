from prefect import flow, task, get_run_logger
import time

@task
def ingest():
    logger = get_run_logger()
    logger.info("Ingestion placeholder ⛳ (buraya dosya/BigQuery yükleme gelecek)")
    time.sleep(1)

@task
def run_dbt():
    logger = get_run_logger()
    logger.info("dbt transform placeholder 🧱 (buraya dbt komutu gelecek)")
    time.sleep(1)

@task
def log_metrics():
    logger = get_run_logger()
    logger.info("Log metrics placeholder 📝 (satır sayısı, süre vs.)")
    time.sleep(1)

@flow(name="elt_case_flow")
def main():
    logger = get_run_logger()
    import prefect
    logger.info(f"Prefect version: {prefect.__version__}")
    ingest()
    run_dbt()
    log_metrics()
    logger.info("Flow OK ✅")

if __name__ == "__main__":
    main()
