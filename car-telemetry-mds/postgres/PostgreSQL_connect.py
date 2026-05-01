import psycopg2

try:
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database="car_telemetry_dw",
        user="telemetry",
        password="telemetry_secret_2024"
    )
    print("Successfully connected to the database")
except Exception as error:
    print(f"Error connecting: {error}")