# ERA5 Parallel Download and Processing System

This repository provides an optimized system for downloading ERA5 data with enhanced speed and efficiency, alongside powerful tools for file management and data processing. The main features include:

1. **Parallel Queuing Download**: A robust parallel downloading system for ERA5 data to significantly accelerate download speeds.
2. **Completed File Checking**: Ensures the integrity of downloaded files (specific to `.zip` files) by verifying completeness.
3. **Automated Data Processing**: Includes functionality to:
   - Decompress downloaded `.zip` files.
   - Combine their contents into a unified NetCDF file.

## Features

### Parallel Queuing Download
- Implements parallel downloading for ERA5 datasets.
- Uses an efficient queuing mechanism to maximize download performance.

### File Checking
- Verifies the completeness of downloaded `.zip` files to avoid corrupted or incomplete datasets.
- Ensures data reliability for downstream processing.

### Data Decompression and Processing
- Decompresses downloaded `.zip` files automatically.
- Combines the extracted data into a single NetCDF file.
- Utilizes the `h5netcdf` engine for optimal I/O performance.
- Applies zlib compression to reduce disk space usage while maintaining data quality.

## Requirements
- Python 3.8+
- Dependencies:
  - `h5netcdf`
  - `xarray`
  - `zipfile`
  - `concurrent.futures` (built-in)

To install additional dependencies, use:
```bash
pip install xarray h5netcdf
```

## How to Use

1. Parallel Download

Run the provided script for downloading ERA5 data:

python parallel_download.py

Modify the script’s configuration for your specific ERA5 data query.

2. File Checking

The system automatically checks downloaded .zip files for completeness during the download process.

3. Data Decompression and NetCDF Generation

Run the processing script to decompress .zip files and merge their contents into a NetCDF file.

The script:
•	Decompresses .zip files in the designated directory.
•	Reads and merges .nc files within the extracted folders.
•	Saves the merged dataset as a compressed NetCDF file.

Output
•	The output NetCDF files are stored in the specified directory, with zlib compression (level 4) applied to reduce file size.

Performance
•	The h5netcdf engine ensures high-performance I/O operations.
•	Zlib compression minimizes disk space usage without compromising data integrity.
•	The parallel queuing system optimizes download speed by leveraging multiple threads.

License

This repository is released under the MIT License.

Contributions

Contributions are welcome! Feel free to submit issues or pull requests to improve the system.

Contact

For questions or feedback, please contact muqy20@lzu.edu.cn.
