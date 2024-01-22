# auto-ria-scraper

A script that scrapes auto-ria used cars section each day at 00:00. Requirements are listed in the requirements.txt file.

A .env file with the following variables is required:
- DB_HOST - the host where the PostgreSQL will run;
- DB_PORT - the port where the PostgreSQL will run;
- POSTGRES_USER - PostgreSQL username;
- POSTGRES_PASSWORD - password;
- POSTGRES_DB - selected name of the DB where the output of the script will be saved;
- DB_DEFAULT - default db name (usually 'postgres') to create POSTGRES_DB;
- DUMP_PATH - a path to pg_dump file.

To run the script run `python main.py`
