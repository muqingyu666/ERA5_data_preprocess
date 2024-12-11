import subprocess

def run_script(script_name):
    try:
        print(f"Running {script_name}...")
        result = subprocess.run(['python', script_name], check=True, capture_output=True, text=True)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running {script_name}:")
        print(e.stderr)
        return False
    return True

if __name__ == "__main__":
    # Run the download script first
    if run_script('muqy_20241122_ERA5_download_new_parallel.py'):
        # Run the unzip and preprocess script after the download is complete
        run_script('muqy_20241203_unzip_and_preprocess_ERA5_daily.py')
    else:
        print("Download script failed. Aborting the unzip and preprocess step.")
