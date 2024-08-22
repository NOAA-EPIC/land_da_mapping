import sys
import os
sys.path.append( '../modules' )
from data_map_generator import *
import pandas as pd
import argparse

'''
The development tool will translate the required Land DA's TAR-based cloud object's details into a data map.
The generated data map will be made against the current data structure featured within the Land DA TAR-based object 
called within the current Land DA's script, retrieved_data.sh.

Users must input the S3 bucket label & TAR-Based Object's key to translate the required Land DA's TAR-based cloud 
object's details into a data map.

Example:
python map_land_da_develop-20240626_data.py -b land-da -k develop-20240626/Landda_develop_data.tar.gz

'''

# User inputs
argParser = argparse.ArgumentParser()
argParser.add_argument("-b", "--bucket", help="Object's bucket label. Type: String. Options: 'land-da' ")
argParser.add_argument("-k", "--key", help="TAR-based object's key. Type: String. Ex: 'develop-20240626/Landda_develop_data.tar.gz' ")
args = argParser.parse_args()

# Read S3 cloud storage reserved for Land DA app's dataset
wrapper = DataMapGenerator(use_bucket=args.bucket)

# Generate list of keys from S3 cloud storage
key_list = wrapper.get_all_s3_keys()

# Read required Land DA's TAR-based object's details from cloud storage & save data details.
dir_list, sz_list = wrapper.read_s3_object_dirs(tar_object_fn=args.key)

# Generate & save data map for the Land DA's TAR-based dataset of interest. 
# Note: Data map for the Land DA's TAR-based dataset' details will be saved to a csv file, but
# map can be save in a different format should futher development be required.
df = wrapper.extract_object_details(dir_list, 
                                    sz_list,
                                    feats_dict = {3: "Dataset Type", 
                                                  1: 'Category', 
                                                  6: 'Sub-Category 1', 
                                                  8: 'Sub-Category 2',
                                                  9: 'Sub-Category 3',
                                                  10: 'YYYY',
                                                  7: 'File Extension'
                                                 }
                                   )

# = Additional Preprocessing Is Required for Generating Data Map =

# C resolution extracted w/ priority & secondary "resolution" feature columns set.
df = wrapper.extract_first_res(df, 'Data File', 'Sub-Category 2')

# Ocean resolution (mx) extracted w/ priority & secondary "ocean resolution" feature column set.
df = wrapper.extract_mx_res(df, 'Data File', 'Sub-Category 2')

# Filter out redundant column details.
df = df.drop([0], axis=1)
df = df.drop([2], axis=1)
df = df.drop([4], axis=1)
df = df.drop([5], axis=1)

# Re-arrange features.
df.insert(0, "Data File", df.pop("Data File"))
df.insert(1, "Dataset Type", df.pop("Dataset Type"))
df.insert(2, "Category", df.pop("Category"))
df.insert(3, "Resolution (C)", df.pop("Resolution (C)"))
df.insert(4, "Ocean Resolution (mx)", df.pop("Ocean Resolution (mx)"))
df.insert(5, "File Extension", df.pop("File Extension"))
df.insert(6, "File Size (Bytes)", df.pop("File Size (Bytes)"))
df.insert(7, "YYYY", df.pop("YYYY"))
df.insert(8, "Sub-Category 1", df.pop("Sub-Category 1"))
df.insert(9, "Sub-Category 2", df.pop("Sub-Category 2"))
df.insert(10, "Sub-Category 3", df.pop("Sub-Category 3"))

# Save data details.
# Create folder directory to save data maps & list of cloud keys.
if not os.path.exists(f'../results/develop-20240626'):
    os.makedirs(f'../results/develop-20240626')
sys.path.append( f'../results/develop-20240626' )
wrapper.save_data(df, 
                  f'../results/{args.key}_{args.bucket}_data_map.csv')

