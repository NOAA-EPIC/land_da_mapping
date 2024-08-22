import sys
sys.path.append( '../modules' )
from data_map_generator import *
import pandas as pd
import argparse

'''
The development tool will translate the required Land DA's TAR-based cloud object's details into a data map.
The generated data map will be made against the current data structure featured within the Land DA TAR-based object 
called within the current Land DA's script, retrieved_data.sh.

Users must input the S3 bucket & TAR-Based Object's key to translate the required Land DA's TAR-based cloud 
object's details into a data map.

Example:
python map_land_da_v1p0p0_test_comps_data.py -b land-da -k current_land_da_release_data/v1.0.0/landda-test-comps.tar.gz

'''

# User inputs
argParser = argparse.ArgumentParser()
argParser.add_argument("-b", "--bucket", help="Object's bucket label. Type: String. Options: 'land-da' ")
argParser.add_argument("-k", "--key", help="TAR-based object's key. Type: String. Ex: 'current_land_da_release_data/v1.0.0/landda-test-comps.tar.gz' ")
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
                                    feats_dict = {0: "Category", 
                                                  1: 'Sub-Category 1', 
                                                  2: 'Sub-Category 2',
                                                  3: 'Sub-Category 3',
                                                  5: 'YYYY'
                                                 }
                                   )

# = Additional Preprocessing Is Required for Generating Data Map =

# C resolution extracted w/ priority & secondary "resolution" feature columns set.
df = wrapper.extract_first_res(df, 'Data File', 'Sub-Category 1')

# Ocean resolution (mx) extracted w/ priority & secondary "ocean resolution" feature column set.
df = wrapper.extract_mx_res(df, 'Data File', 'Sub-Category 1')

# Filter out redundant column details.
df = df.drop([4], axis=1)

# Re-arrange features.
df.insert(0, "Data File", df.pop("Data File"))
df.insert(1, "Category", df.pop("Category"))
df.insert(2, "Sub-Category 1", df.pop("Sub-Category 1"))
df.insert(3, "Sub-Category 2", df.pop("Sub-Category 2"))
df.insert(4, "Sub-Category 3", df.pop("Sub-Category 3"))
df.insert(5, "Resolution (C)", df.pop("Resolution (C)"))
df.insert(6, "Ocean Resolution (mx)", df.pop("Ocean Resolution (mx)"))
df.insert(7, "File Extension", df.pop("File Extension"))
df.insert(8, "File Size (Bytes)", df.pop("File Size (Bytes)"))

# Save data details.
if not os.path.exists(f'../results/current_land_da_release_data/v1.0.0'):
    os.makedirs(f'../results/current_land_da_release_data/v1.0.0')
sys.path.append( f'../results/current_land_da_release_data/v1.0.0' )
wrapper.save_data(df, 
                  f'../results/{args.key}_{args.bucket}_data_map.csv')


