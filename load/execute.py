import os
import sys
import time
import psycopg2
from psycopg2 import sql
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pyspark.sql import SparkSession
from utility.utility import setup_logging, format_time


def create_spark_session():
    """Create and return a SparkSession."""
    return SparkSession.builder \
        .appName("SpotifyDataLoad") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()


def create_postgres_tables(logger, db_user, db_password):
    """Create PostgresQL tables if they don't exist using psycopg2."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=db_user,
            password=db_password,
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        logger.debug("Successfully connected to postgres database")

        create_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS master_table (
                track_id VARCHAR(50),
                track_name TEXT,
                track_popularity INTEGER,
                artist_id VARCHAR(50),
                artist_name TEXT,
                followers FLOAT,
                genres TEXT,
                artist_popularity INTEGER,
                danceability FLOAT,
                energy FLOAT,
                tempo FLOAT,
                related_ids TEXT[]
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS recommendations_exploded (
                id VARCHAR(50),
                related_id VARCHAR(50)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS artist_track (
                id VARCHAR(50),
                artist_id VARCHAR(50)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS track_metadata (
                id VARCHAR(50) PRIMARY KEY,
                name TEXT,
                popularity INTEGER,
                duration_ms INTEGER,
                danceability FLOAT,
                energy FLOAT,
                tempo FLOAT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS artist_metadata (
                id VARCHAR(50) PRIMARY KEY,
                name TEXT,
                followers FLOAT,
                popularity INTEGER
            );
            """
        ]

        for query in create_table_queries:
            cursor.execute(query)
        conn.commit()
        logger.info("PostgresQL tables created successfully")

    except Exception as e:
        logger.warning(f"Error creating tables: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def load_to_postgres(logger, spark, input_dir, db_user, db_password):
    """Load Parquet files to PostgresQL."""
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    tables = [
        ("stage2/master_table", "master_table"),
        ("stage3/recommendations_exploded", "recommendations_exploded"),
        ("stage3/artist_track", "artist_track"),
        ("stage3/track_metadata", "track_metadata"),
        ("stage3/artist_metadata", "artist_metadata")
    ]

    for parquet_subpath, table_name in tables:
        parquet_path = os.path.join(input_dir, parquet_subpath)
        try:
            if not os.path.exists(parquet_path):
                logger.info(f"Skipping {table_name}: {parquet_path} not found.")
                continue

            df = spark.read.parquet(parquet_path)
            mode = "append" if "master" in parquet_subpath else "overwrite"

            df.write \
                .mode(mode) \
                .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

            logger.info(f"Loaded {table_name} from {parquet_path} to PostgresQL")

        except Exception as e:
            logger.warning(f"Error loading {table_name}: {e}")


if __name__ == "__main__":
    try:
        logger = setup_logging("load.log")

        if len(sys.argv) != 4:
            print("Usage: python ETL/load/execute.py <input_dir> <db_user> <db_password>")
            sys.exit(1)

        input_dir = sys.argv[1]
        db_user = sys.argv[2]
        db_password = sys.argv[3]

        if not os.path.exists(input_dir):
            print(f"Error: Input directory {input_dir} does not exist")
            sys.exit(1)

        logger.info("Load stage started")
        start = time.time()

        spark = create_spark_session()
        create_postgres_tables(logger, db_user, db_password)
        load_to_postgres(logger, spark, input_dir, db_user, db_password)

        spark.stop()

        end = time.time()
        logger.info("Load stage completed")
        logger.info(f"Total time taken {format_time(end-start)}")

    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
