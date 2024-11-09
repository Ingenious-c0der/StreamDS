import os
import shutil

# Set the base directory path
dir_path = "/home/sra9/g28/HYDFS"

# Loop through all items in the given directory
for item in os.listdir(dir_path):
    full_path = os.path.join(dir_path, item)
    
    # Check if it's a directory and its name is not 'business'
    if os.path.isdir(full_path) and item != "business":
        print(f"Removing directory: {full_path}")
        shutil.rmtree(full_path)  # Remove the directory and its contents