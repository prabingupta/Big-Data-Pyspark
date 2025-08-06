import sys
import os
import json
import time
from zipfile import ZipFile
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time


def extract_zip_file(zip_path, output_dir, logger):
    try:
        with ZipFile(zip_path, "r") as zip_file:
            zip_file.extractall(output_dir)
        logger.info(f"Extracted: {zip_path}")
    except Exception as e:
        logger.warning(f"Failed to extract {zip_path}: {e}")
        raise


def fix_json_dict(output_dir, logger):
    file_path = os.path.join(output_dir, "dict_artists.json")
    if not os.path.exists(file_path):
        msg = f"{file_path} not found!"
        logger.error(msg)
        raise FileNotFoundError(msg)

    output_file = os.path.join(output_dir, "fixed_da.json")
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        with open(output_file, "w", encoding="utf-8") as f_out:
            for key, value in data.items():
                record = {"id": key, "related_ids": value}
                json.dump(record, f_out, ensure_ascii=False)
                f_out.write("\n")
        logger.info(f"Fixed and saved JSON to: {output_file}")
        os.remove(file_path)
    except Exception as e:
        logger.warning(f"Error fixing JSON dict: {e}")
        raise


def create_spark_session():
    return SparkSession.builder \
        .appName("SpotifyDataTransform") \
        .getOrCreate()


def load_and_clean(spark, input_dir, output_dir, logger):
    artists_schema = T.StructType([
        T.StructField("id", T.StringType(), False),
        T.StructField("followers", T.FloatType(), True),
        T.StructField("genres", T.StringType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("popularity", T.IntegerType(), True),
    ])

    recommendations_schema = T.StructType([
        T.StructField("id", T.StringType(), False),
        T.StructField("related_ids", T.ArrayType(T.StringType()), True),
    ])

    tracks_schema = T.StructType([
        T.StructField("id", T.StringType(), False),
        T.StructField("name", T.StringType(), True),
        T.StructField("popularity", T.IntegerType(), True),
        T.StructField("duration_ms", T.IntegerType(), True),
        T.StructField("explicit", T.IntegerType(), True),
        T.StructField("artists", T.StringType(), True),
        T.StructField("id_artists", T.StringType(), True),
        T.StructField("release_date", T.StringType(), True),
        T.StructField("danceability", T.FloatType(), True),
        T.StructField("energy", T.FloatType(), True),
        T.StructField("key", T.IntegerType(), True),
        T.StructField("loudness", T.FloatType(), True),
        T.StructField("mode", T.IntegerType(), True),
        T.StructField("speechiness", T.FloatType(), True),
        T.StructField("acousticness", T.FloatType(), True),
        T.StructField("instrumentalness", T.FloatType(), True),
        T.StructField("liveness", T.FloatType(), True),
        T.StructField("valence", T.FloatType(), True),
        T.StructField("tempo", T.FloatType(), True),
        T.StructField("time_signature", T.IntegerType(), True)
    ])

    logger.info("Loading CSV and JSON files into Spark DataFrames...")

    artists_df = spark.read.schema(artists_schema).csv(os.path.join(input_dir, "artists.csv"), header=True)
    recommendations_df = spark.read.schema(recommendations_schema).json(os.path.join(input_dir, "fixed_da.json"))
    tracks_df = spark.read.schema(tracks_schema).csv(os.path.join(input_dir, "tracks.csv"), header=True)

    artists_df = artists_df.dropDuplicates(["id"]).filter(F.col("id").isNotNull())
    recommendations_df = recommendations_df.dropDuplicates(["id"]).filter(F.col("id").isNotNull())
    tracks_df = tracks_df.dropDuplicates(["id"]).filter(F.col("id").isNotNull())

    logger.info("Saving cleaned data as parquet files...")
    artists_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "artists"))
    recommendations_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "recommendations"))
    tracks_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "tracks"))

    logger.info("Stage 1: Cleaned data saved")
    return artists_df, recommendations_df, tracks_df


def create_master_table(output_dir, artists_df, recommendations_df, tracks_df, logger):
    tracks_df = tracks_df.withColumn("id_artists_json", F.regexp_replace(F.col("id_artists"), "'", "\""))
    tracks_df = tracks_df.withColumn("id_artists_array", F.from_json(F.col("id_artists_json"), T.ArrayType(T.StringType())))
    tracks_exploded = tracks_df.select("id", "name", "popularity", "id_artists_array") \
        .withColumn("artist_id", F.explode(F.col("id_artists_array")))

    master_df = tracks_exploded.join(artists_df, tracks_exploded.artist_id == artists_df.id, "left") \
        .select(
            tracks_exploded.id.alias("track_id"),
            tracks_exploded.name.alias("track_name"),
            tracks_exploded.popularity.alias("track_popularity"),
            artists_df.id.alias("artist_id"),
            artists_df.name.alias("artist_name"),
            artists_df.followers,
            artists_df.genres,
            artists_df.popularity.alias("artist_popularity")
        )

    master_df = master_df.join(recommendations_df, master_df.artist_id == recommendations_df.id, "left") \
        .select(
            master_df.track_id,
            master_df.track_name,
            master_df.track_popularity,
            master_df.artist_id,
            master_df.artist_name,
            master_df.followers,
            master_df.genres,
            master_df.artist_popularity,
            recommendations_df.related_ids
        )

    master_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage2", "master_table"))
    logger.info("Stage 2: Master table saved")


def create_query_tables(output_dir, artists_df, recommendations_df, tracks_df, logger):
    recommendations_exploded = recommendations_df.withColumn("related_id", F.explode(F.col("related_ids"))) \
        .select("id", "related_id")
    recommendations_exploded.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "recommendations_exploded"))

    tracks_df = tracks_df.withColumn("id_artists_json", F.regexp_replace(F.col("id_artists"), "'", "\""))
    tracks_exploded = tracks_df.withColumn("id_artists_array", F.from_json(F.col("id_artists_json"), T.ArrayType(T.StringType()))) \
        .withColumn("artist_id", F.explode(F.col("id_artists_array"))) \
        .select("id", "artist_id")
    tracks_exploded.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "artist_track"))

    tracks_metadata = tracks_df.select(
        "id", "name", "popularity", "duration_ms", "danceability", "energy", "tempo"
    )
    tracks_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "track_metadata"))

    artists_metadata = artists_df.select("id", "name", "followers", "popularity")
    artists_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "artist_metadata"))

    logger.info("Stage 3: Query-optimized tables saved")


if __name__ == "__main__":
    try:
        logger = setup_logging("transform.log")

        if len(sys.argv) != 3:
            print("Usage: python ETL/transform/execute.py <extract_output_dir> <transform_output_dir>")
            sys.exit(1)

        extract_output_dir = sys.argv[1]
        transform_output_dir = sys.argv[2]

        if not os.path.exists(extract_output_dir):
            logger.error(f"Extract output directory {extract_output_dir} does not exist")
            sys.exit(1)

        os.makedirs(transform_output_dir, exist_ok=True)

        logger.info("Transformation stage started")
        start = time.time()

        # Extraction phase: unzip and fix JSON
        base_path = os.path.expanduser("~/Downloads")
        zip_files = [
            "dict_artists.json.zip",
            "artists.csv.zip",
            "tracks.csv.zip"
        ]
        for zip_name in zip_files:
            full_zip_path = os.path.join(base_path, zip_name)
            extract_zip_file(full_zip_path, extract_output_dir, logger)

        fix_json_dict(extract_output_dir, logger)

        # Transformation phase
        spark = create_spark_session()
        artists_df, recommendations_df, tracks_df = load_and_clean(spark, extract_output_dir, transform_output_dir, logger)
        create_master_table(transform_output_dir, artists_df, recommendations_df, tracks_df, logger)
        create_query_tables(transform_output_dir, artists_df, recommendations_df, tracks_df, logger)
        spark.stop()

        end = time.time()
        logger.info("Transformation stage completed")
        logger.info(f"Total time taken {format_time(end - start)}")

    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
