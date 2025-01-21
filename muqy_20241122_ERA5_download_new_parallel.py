# -*- coding: utf-8 -*-
# @Author: Muqy
# @Date:   2024-11-22 10:06
# @Last Modified by:   Muqy
# @Last Modified time: 2025-01-21 15:40

import cdsapi
import os
import calendar
import netCDF4 as nc
import threading
from queue import Queue
import zipfile

os.environ["KMP_DUPLICATE_LIB_OK"] = "True"

################################################################################
# Define overall configuration for ERA5 download
CONFIG = {
    "dataset": "derived-era5-pressure-levels-daily-statistics",
    "variables": [
        "divergence",
        "geopotential",
        "potential_vorticity",
        "relative_humidity",
        "specific_humidity",
        "temperature",
        "vertical_velocity",
        "vorticity",
    ],
    "pressure_levels": [
        "1",
        "2",
        "3",
        "5",
        "7",
        "10",
        "20",
        "30",
        "50",
        "70",
        "100",
        "125",
        "150",
        "175",
        "200",
        "225",
        "250",
        "300",
        "350",
        "400",
        "450",
        "500",
    ],
    "daily_statistic": "daily_mean",
    "max_retries": 3,
    # ! Adjust the thread count to the number of cores on your machine
    "thread_count": 8,
}

################################################################################
# Main functions for downloading ERA5 data #####################################
################################################################################


def build_request(year: str, month: str, day: str) -> dict:
    """CDS API request dict"""
    return {
        "product_type": "reanalysis",
        "variable": CONFIG["variables"],
        "year": year,
        "month": [month],
        "day": [day],
        "pressure_level": CONFIG["pressure_levels"],
        # If you are downloading monthly or other time-scale data, you can delete this line
        "time_zone": "utc+00:00",
        # Same as above line
        "daily_statistic": CONFIG["daily_statistic"],
        # Same as above line
        "frequency": "1_hourly",
        # If your download order is large, EC center will automatically zip your vars together
        # Really enconvinient!
        # "data_format": "netcdf",
    }


def validate_zip(filepath: str) -> bool:
    """Verify the data file integrity"""
    try:
        with zipfile.ZipFile(filepath) as zf:
            return zf.testzip() is None
    except (OSError, zipfile.BadZipFile, ValueError):
        return False


def safe_download(request: dict, filepath: str) -> bool:
    """Download function with re-try mechanism"""
    for attempt in range(CONFIG["max_retries"]):
        try:
            client = cdsapi.Client()
            # This line can be modified based on the latest CDS API doc
            client.retrieve(CONFIG["dataset"], request).download(
                filepath
            )
            if validate_zip(filepath):
                return True
        except Exception as e:
            print(f"Download attempt {attempt+1} failed: {str(e)}")
            if os.path.exists(filepath):
                os.remove(filepath)
    return False


def process_download_task(year: str, month: str, day: str):
    """Handle download task for a single day"""
    filename = f"ERA5_{year}{month}{day}.zip"
    filepath = os.path.join(download_dir, filename)

    if os.path.exists(filepath):
        if validate_zip(filepath):
            print(f"File already exists: {filename}")
            return
        print(f"Corrupted file found: {filename}")
        os.remove(filepath)

    print(f"Downloading: {filename}")
    request = build_request(year, month, day)
    if safe_download(request, filepath):
        print(f"Downloaded: {filename}")
    else:
        print(f"Failed to download: {filename}")


class DownloadWorker(threading.Thread):
    def __init__(self, queue: Queue):
        # Force the thread to be a daemon thread, so it will exit when the main thread exits
        super().__init__(daemon=True)
        self.queue = queue

    def run(self):
        while True:
            try:
                year, month, day = self.queue.get()
                process_download_task(year, month, day)
            finally:
                self.queue.task_done()


def generate_tasks(start_year: int, end_year: int):
    """Generate per-day tasks"""
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            _, max_day = calendar.monthrange(year, month)
            for day in range(1, max_day + 1):
                yield (str(year), f"{month:02d}", f"{day:02d}")


def main():
    # Set download directory to global variable
    global download_dir
    # ! Change the dir based on your system
    download_dir = r"D:\ERA5_pressure_levels_multi_vars"
    os.makedirs(download_dir, exist_ok=True)

    # Create task queue
    task_queue = Queue()

    # Create worker threads
    for _ in range(CONFIG["thread_count"]):
        DownloadWorker(task_queue).start()

    # Add tasks to the queue
    for task in generate_tasks(2006, 2011):
        task_queue.put(task)

    task_queue.join()
    print("All tasks completed.")


################################################################################

if __name__ == "__main__":
    main()
