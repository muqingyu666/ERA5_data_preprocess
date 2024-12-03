# -*- coding: utf-8 -*-
# @Author: Your name
# @Date:   2024-12-03 15:06:54
# @Last Modified by:   Your name
# @Last Modified time: 2024-12-03 16:00:59

import os
import zipfile
import xarray as xr
from concurrent.futures import ThreadPoolExecutor

def process_zip_file(zip_file, data_folder, output_data_folder):
    if zip_file.endswith(".zip"):
        # Extract the date suffix from the zip file name (e.g., ERA5_20060325.zip -> 20060325)
        date_suffix = zip_file.split("_")[1].split(".")[0]

        # Create a path to the zip file
        zip_path = os.path.join(data_folder, zip_file)

        # Create a temp directory for extracted files
        extract_folder = os.path.join(
            data_folder, f"extracted_{date_suffix}"
        )
        os.makedirs(extract_folder, exist_ok=True)

        # Extract the zip file
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_folder)

        # List to store datasets for merging
        datasets = []

        # Open and read all nc files in the extracted folder
        for nc_file in os.listdir(extract_folder):
            if nc_file.endswith(".nc"):
                nc_path = os.path.join(extract_folder, nc_file)
                ds = xr.open_dataset(nc_path)  # Open the dataset
                datasets.append(ds)

        # Merge datasets if there are multiple
        if datasets:
            merged_ds = xr.merge(datasets)

            # Save the merged dataset as a new nc file with compression
            output_file = os.path.join(
                output_data_folder, f"merged_{date_suffix}.nc"
            )
            encoding = {
                var: {"zlib": True, "complevel": 4}
                for var in merged_ds.data_vars
            }
            merged_ds.to_netcdf(output_file, encoding=encoding, engine="h5netcdf")
            print(
                f"Processed {zip_file}, merged data saved to {output_file}."
            )

        # Clean up: Close all datasets
        for ds in datasets:
            ds.close()
            
        # Clean up: Delete temp dir


def main():
    # Set folder path
    data_folder = "D://ERA5_pressure_levels_multi_vars/"
    output_data_folder = "D://ERA5_pressure_levels_multi_vars_processed/"
    
    zip_files = [
        f for f in os.listdir(data_folder) if f.endswith(".zip")
    ]

    # Specify the number of threads (cores) to use
    num_threads = 4 

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(
            lambda zip_file: process_zip_file(
                zip_file, data_folder, output_data_folder
            ),
            zip_files,
        )


if __name__ == "__main__":
    main()
