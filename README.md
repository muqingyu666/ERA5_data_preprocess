# ERA5 Parallel Download and Processing System

Welcome to the ERA5 Parallel Download and Processing System! This repository is designed to make your life easier when working with ERA5 data. If you’ve ever struggled with the slow download speeds or the hassle of dealing with `.zip` files from the updated ERA5 system, this tool is for you. Here’s what it does:

1. **Speeds up your downloads**: By using parallel queuing, we tackle the painfully long wait times for ERA5 data downloads. Queue up multiple tasks, and let the system handle the rest.
2. **Handles `.zip` files with ease**: The updated ERA5 system automatically compresses large datasets into `.zip` files, which can be a headache to manage. Our system decompresses and combines these files into a single, easy-to-use NetCDF file.

## Why This Exists

### Problem 1: The `.zip` File Hassle
The updated ERA5 system compresses large datasets into `.zip` files. While this saves space, it’s inconvenient for users who need to work with the data immediately. Our system automates the decompression and merging process, so you can focus on your analysis instead of file management.

### Problem 2: Slow Download Times
ERA5 downloads can take forever, especially if you’re downloading multiple datasets. Our parallel queuing system lets you download multiple files simultaneously, drastically reducing the total download time.

## Features

### Parallel Queuing Download
- **Faster downloads**: By running multiple download tasks in parallel, we cut down on the long wait times.
- **Efficient queuing**: The system manages the queue for you, ensuring optimal performance.

### File Integrity Checking
- **Ensures completeness**: We automatically check downloaded `.zip` files to make sure they’re not corrupted or incomplete.
- **Reliable data**: No more surprises when you start processing your files.

**Note**: Currently, the integrity check only works for `.zip` files. Support for `.nc` files is coming in a future update!

### Automated Data Processing
- **Decompress `.zip` files**: Extracts the contents of downloaded `.zip` files automatically.
- **Merge into NetCDF**: Combines the extracted `.nc` files into a single, unified NetCDF file.
- **Optimized performance**: Uses the `h5netcdf` engine for fast I/O operations.
- **Save disk space**: Applies zlib compression (level 4) to reduce file size without losing data quality.

## Requirements
- Python 3.8+
- Dependencies:
  - `h5netcdf`
  - `xarray`
  - `zipfile`
  - `concurrent.futures` (built-in)

To install the required dependencies, run:
```bash
pip install xarray h5netcdf
```

## How to Use

### 1. Parallel Download
Run the `parallel_download.py` script to start downloading ERA5 data:
```bash
python parallel_download.py
```
You can customize the script to fit your specific ERA5 data query.

### 2. File Checking
The system automatically checks the integrity of downloaded `.zip` files during the download process. If a file is incomplete or corrupted, it will be flagged for re-download.

### 3. Data Decompression and NetCDF Generation
Run the processing script to decompress `.zip` files and merge their contents into a single NetCDF file:
```bash
python process_data.py
```
This script will:
- Decompress all `.zip` files in the specified directory.
- Read and merge the `.nc` files from the extracted folders.
- Save the merged dataset as a compressed NetCDF file.

### Output
- The final NetCDF files are saved in your specified directory.
- Files are compressed using zlib (level 4) to save disk space.

## Performance
- **Fast I/O**: The `h5netcdf` engine ensures high-performance read/write operations.
- **Efficient storage**: Zlib compression reduces file size without sacrificing data quality.
- **Optimized downloads**: The parallel queuing system maximizes download speed by leveraging multiple threads.

## License
This project is released under the MIT License. Feel free to use, modify, and share it!

## Contributions
We welcome contributions! If you have ideas for improvements or find any issues, please open an issue or submit a pull request.

## Contact
For questions or feedback, feel free to reach out to [muqy20@lzu.edu.cn](mailto:muqy20@lzu.edu.cn). We’d love to hear from you!

---

We built this tool to solve real problems we faced while working with ERA5 data. We hope it makes your workflow smoother and faster. Happy downloading! 🚀
