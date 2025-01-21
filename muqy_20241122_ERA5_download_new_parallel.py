# -*- coding: utf-8 -*-
# @Author: Muqy
# @Date:   2024-11-22 10:06
# @Last Modified by:   Muqy
# @Last Modified time: 2025-01-21 14:43

import cdsapi
import os
import calendar
import netCDF4 as nc
import threading
from queue import Queue
import zipfile

os.environ["KMP_DUPLICATE_LIB_OK"] = "True"


def download_era5_data(year, month, day, download_dir):
    dataset = "derived-era5-pressure-levels-daily-statistics"
    request = {
        "product_type": "reanalysis",
        "variable": [
            "divergence",
            "geopotential",
            "potential_vorticity",
            "relative_humidity",
            "specific_humidity",
            "temperature",
            "vertical_velocity",
            "vorticity",
        ],
        "year": year,
        "month": [month],
        "day": [day],
        "pressure_level": [
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
        "time_zone": "utc+00:00",
        "daily_statistic": "daily_mean",
        "frequency": "1_hourly",
        "data_format": "netcdf",
    }

    # Define file name format as ERA5_YYYYMMDD.nc
    filename = f"ERA5_{year}{month}{day}.zip"
    filepath = os.path.join(download_dir, filename)

    print(f"Checking if file {filename} exists and is complete...")

    # !!! This version is for ERA5 downloaded in zip format !!!
    # Check if zip file exists and is complete
    if os.path.exists(filepath):
        try:
            # check if the zip file is valid
            with zipfile.ZipFile(filepath, "r") as zip_ref:
                if zip_ref.testzip() is None:
                    print(f"File {filename} is complete and valid.")
                else:
                    raise zipfile.BadZipFile
        except (OSError, zipfile.BadZipFile) as e:
            print(f"File {filename} is corrupted. Redownloading...")
            os.remove(filepath)
            download_file_from_era5(request, filepath)
    else:
        print(f"File {filename} does not exist. Starting download...")
        download_file_from_era5(request, filepath)


def download_file_from_era5(request, filepath):
    print(f"Downloading data to {filepath}...")
    client = cdsapi.Client()
    client.retrieve(
        "derived-era5-pressure-levels-daily-statistics", request
    ).download(filepath)
    print(f"Download completed for {filepath}")


################################################################################
### Step 1: Define download directory ##########################################
# Define download directory
download_dir = r"D:\ERA5_pressure_levels_multi_vars"

# Check if directory exists, create if not
print(f"Checking if download directory {download_dir} exists...")
if not os.path.exists(download_dir):
    print(
        f"Directory {download_dir} does not exist. Creating directory..."
    )
    os.makedirs(download_dir)
else:
    print(f"Directory {download_dir} already exists.")

################################################################################
### Step 2: Define download task queue #########################################
# Define download task queue
queue = Queue()


# Create download worker thread class
class DownloadWorker(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            year, month, day = self.queue.get()
            print(
                f"Worker {threading.current_thread().name} processing download for {year}-{month}-{day}..."
            )
            try:
                download_era5_data(str(year), month, day, download_dir)
            except Exception as e:
                print(
                    f"Error downloading data for {year}-{month}-{day}: {e}"
                )
            finally:
                print(
                    f"Worker {threading.current_thread().name} finished processing download for {year}-{month}-{day}."
                )
                self.queue.task_done()


# Create four worker threads
print("Creating worker threads...")

# We can queen 32 theads all in once (It can save a lot of time queueing)!
for x in range(32):
    worker = DownloadWorker(queue)
    # Set worker thread as daemon so that it will exit when the main thread exits
    worker.daemon = True
    # Start worker thread
    worker.start()

# Add tasks for May to December 2006
print("Adding download tasks to the queue...")

for year in range(2006, 2012):  # 2006 to 2011
    for month in range(1, 13):  # 1 to 12
        # Get the number of days in the current month
        _, max_day = calendar.monthrange(year, month)
        # Loop through each days in the month
        for day in range(1, max_day + 1):
            # Format month and day as two-digit string
            month_str = f"{month:02d}"
            # Format day as two-digit string
            day_str = f"{day:02d}"
            print(
                f"Adding task for {year}-{month_str}-{day_str} to the queue..."
            )
            # Add task to the queue
            queue.put((str(year), month_str, day_str))

# Wait for all tasks to complete
queue.join()
print("All download tasks completed.")
