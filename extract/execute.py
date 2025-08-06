import os
import sys
import time
from zipfile import ZipFile
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
    import json
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
                import json
                json.dump(record, f_out, ensure_ascii=False)
                f_out.write("\n")
        logger.info(f"Fixed and saved JSON to: {output_file}")
        os.remove(file_path)
    except Exception as e:
        logger.warning(f"Error fixing JSON dict: {e}")
        raise


if __name__ == "__main__":
    try:
        logger = setup_logging("extract.log")

        if len(sys.argv) != 2:
            print("Usage: python ETL/extract/execute.py <extract_output_dir>")
            sys.exit(1)

        extract_output_dir = sys.argv[1]
        os.makedirs(extract_output_dir, exist_ok=True)

        logger.info("Extraction stage started")
        start = time.time()

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

        end = time.time()
        logger.info("Extraction stage completed")
        logger.info(f"Total time taken {format_time(end - start)}")

    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
