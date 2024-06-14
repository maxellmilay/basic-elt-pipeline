import subprocess
import time

def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], 
                check=True, 
                capture_output=True, 
                text=True
            )

            if "accepting connections" in result.stdout:
                print('Successfully connected to Postgres')
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to Postgres: {e}")
            retries += 1
            print(f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    print("Max retries reached. Exiting")
    return False


# verify first if connection with source postgres was established
if not wait_for_postgres(host="source_postgres"):
    exit(1)

print("Starting ELT Script...")

source_config = {
    'dbname': 'source_db' ,
    'user': 'postgres',
    'password': '1539',
    'host': 'source_postgres'
}

dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'
]

subprocess_env = dict(PGPASSWORD=source_config['password'])

# Data dumping from source
try:
    result = subprocess.run(dump_command, env=subprocess_env, check=True, capture_output=True, text=True)
    print(result.stdout)
    print("Data dump completed successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error during pg_dump: {e.stderr}")
    exit(1)

destination_config = {
    'dbname': 'destination_db' ,
    'user': 'postgres',
    'password': '1539',
    'host': 'destination_postgres'
}

load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'
]

subprocess_env = dict(PGPASSWORD=destination_config['password'])

# Data loading into destination
try:
    result = subprocess.run(load_command, env=subprocess_env, check=True, capture_output=True, text=True)
    print(result.stdout)
    print("Data load completed successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error during psql load: {e.stderr}")
    exit(1)

print("Ending ELT Script...")