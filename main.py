import os
from dotenv import load_dotenv
from scraping import determine_pages, run_scraping_process
from db_controller import db_creator, create_dump
import schedule
import time


def scheduler(start_check: str, start_time: str, base_url: str, pages_total: int, num_processes: int,
              params: dict[str, str, str, str, str], table_name: str, default_db: str, path: str) -> None:
    """Schedules a DB check/creation, dump and scraping run"""
    schedule.every().day.at(start_check).do(db_creator, params, table_name, default_db)
    schedule.every().day.at(start_time).do(create_dump, params, path)
    job = schedule.every().day.at(start_time).do(run_scraping_process, base_url, pages_total, num_processes, table_name)
    next_run_time = job.next_run
    print(f"Next run time: {next_run_time}")

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    load_dotenv()

    db_params = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'dbname': os.getenv('POSTGRES_DB')
    }

    table = 'auto_ria'
    db_default = os.getenv('DB_DEFAULT')
    dump_path = os.getenv('DUMP_PATH')
    url = 'https://auto.ria.com/uk/car/used/'
    time_schedule = '00:00'
    time_check = '23:55'
    processes = 2

    total_pages = determine_pages(url)
    scheduler(time_check, time_schedule, url, total_pages, processes, db_params, table, db_default, dump_path)
