import psycopg2
from psycopg2 import sql, pool
from psycopg2.extensions import cursor, connection
from datetime import datetime
import subprocess
import os


def check_database_existence(database_name: str, db_cursor: cursor) -> bool:
    """Checks if the DB exists"""
    try:
        db_cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = {}").format(sql.Identifier(database_name)))
        return db_cursor.fetchone() is not None

    except psycopg2.Error as e:
        print(f"Error checking database existence: {e}")
        return False


def check_table_existence(table_name: str, connect: connection) -> bool:
    """Checks if the table exists"""
    try:
        with connect.cursor() as db_cursor:
            db_cursor.execute(sql.SQL("SELECT 1 FROM information_schema.tables WHERE table_name = %s"), (table_name,))
            return db_cursor.fetchone() is not None

    except psycopg2.Error as e:
        print(f"Error checking table existence: {e}")
        return False


def create_table(table_name: str, connect: connection) -> bool:
    """Creates the table if previous check determines the DB does not exist"""
    with connect.cursor() as db_cursor:
        create_table_query = sql.SQL("""CREATE TABLE {} (
                                     id SERIAL PRIMARY KEY,
                                     URL VARCHAR(255),
                                     title VARCHAR(255),
                                     price_usd INT,
                                     odometer INT,
                                     username VARCHAR(255),
                                     phone_number BIGINT,
                                     image_url VARCHAR(255),
                                     images_count INT,
                                     car_number VARCHAR(20),
                                     car_vin VARCHAR(50),
                                     datetime_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP::TIMESTAMP(0)
                                     );""").format(sql.Identifier(table_name))
        db_cursor.execute(create_table_query)
        connect.commit()
        print(f"Table '{table_name}' created successfully.")
    return True


def insert_entry(values: tuple[str, str, int, int, str, str, str, int, str, str], table_name: str) -> bool:
    """Inserts an entry into the table"""
    db_params = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'dbname': os.getenv('POSTGRES_DB')
    }
    db_connection = psycopg2.pool.SimpleConnectionPool(minconn=1, maxconn=4, **db_params)
    conn = db_connection.getconn()
    check_query = sql.SQL("SELECT COUNT(*) FROM {} WHERE image_url = %s").format(sql.Identifier(table_name))
    with conn.cursor() as db_cursor:
        db_cursor.execute(check_query, (values[6],))
        count = db_cursor.fetchone()[0]

    if count == 0:
        insert_query = sql.SQL("""
            INSERT INTO {} (url, title, price_usd, odometer, username, 
                            phone_number, image_url, images_count, car_number, 
                            car_vin)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """).format(sql.Identifier(table_name))

        with conn.cursor() as db_cursor:
            db_cursor.execute(insert_query, values)
        conn.commit()
        print("Entry inserted successfully.")
    else:
        print("Entry with the same image_url already exists. Not inserting.")
    db_connection.putconn(conn)
    return True


def create_dump(params: dict[str, str, str, str, str], path: str) -> bool:
    """Creates a dump of the DB"""
    os.makedirs('./dumps', exist_ok=True)
    os.environ['PGPASSWORD'] = params['password']
    script_path = os.path.dirname(os.path.realpath(__file__))
    dump_dir = os.path.join(script_path, 'dumps')
    date = datetime.now().strftime("%d-%m-%Y")
    dump_name = f'{params["dbname"]}_dump_{date}.sql'
    pg_dump_command = [
        path,
        "-h", params['host'],
        "-p", str(params['port']),
        "-U", params['user'],
        "-d", params['dbname'],
        "-F", "c",
        "-f", os.path.join(dump_dir, dump_name)
    ]
    subprocess.run(pg_dump_command, check=True)
    return True


def db_creator(db_params: dict[str, str, str, str, str], table_name: str, db_default: str) -> bool:
    """Checks for the DB and table existence and creates them if they do not exist"""
    try:
        db_connection = psycopg2.connect(**db_params)
    except psycopg2.OperationalError:
        db_connection = psycopg2.connect(host=db_params['host'],
                                         database=db_default,
                                         user=db_params['user'],
                                         password=db_params['password'],
                                         port=db_params['port'])
        db_connection.autocommit = True
        with db_connection.cursor() as db_cursor:
            db_cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_params['dbname'])))
            print(f"DB '{db_params['dbname']}' created successfully.")
    finally:
        db_connection.close()

    db_connection = psycopg2.connect(**db_params)
    if not check_table_existence(table_name, db_connection):
        create_table(table_name, db_connection)
    else:
        print(f'Table {table_name} exists.')
    db_connection.close()

    return True
