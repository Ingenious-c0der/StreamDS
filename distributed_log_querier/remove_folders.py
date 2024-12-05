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

#also remove the state_files from the operator directory
dir_path = "/home/sra9/g28/distributed_log_querier/operators"

# dir_path = "/Users/ingenious/Documents/DSMP1_backup/CS-425-MP/MP1/g28/distributed_log_querier/operators"
for item in os.listdir(dir_path):
    #check if the item is a file, if it is, if it ends in .txt remove it
    if os.path.isfile(os.path.join(dir_path, item)) and item.endswith(".txt"):
        print(f"Removing file: {os.path.join(dir_path, item)}")
        os.remove(os.path.join(dir_path, item))  # Remove the file
#before shutting create an empty state file
with open(os.path.join(dir_path, "word_count_state.txt"), "w") as f:
    f.write("")
print("Done!")