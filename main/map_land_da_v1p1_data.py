import sys
sys.path.append( '../modules' )
from data_map_generator import *
import pandas as pd
import argparse

'''
The development tool will translate the required Land DA's TAR-based cloud object's details into a data map.
The generated data map will be made against the current data structure featured within the Land DA TAR-based object 
called within the current Land DA's script, retrieved_data.sh. Note: At this time, the current Land DA application release
is v1.1.0 & taking placed within the Planning Interval (PI) 10.

Users must input the S3 bucket & TAR-Based Object's key to translate the required Land DA's TAR-based cloud 
object's details into a data map.

Example:
python map_land_da_v1p1_data.py -b land-da -k landda_inputs.tar.gz_v1.1

'''

# User inputs
argParser = argparse.ArgumentParser()
argParser.add_argument("-b", "--bucket", help="Object's bucket label. Type: String. Options: 'land-da' ")
argParser.add_argument("-k", "--key", help="TAR-based object's key. Type: String. Ex: 'landda_inputs.tar.gz_v1.1' ")
args = argParser.parse_args()

# Read S3 cloud storage reserved for Land DA app's dataset
wrapper = DataMapGenerator(use_bucket=args.bucket)

# Generate list of keys from S3 cloud storage
key_list = wrapper.get_all_s3_keys()

# Read required Land DA's TAR-based object's details from cloud storage & save data details.
dir_list, sz_list = wrapper.read_s3_object_dirs(tar_object_fn=args.key)

# Read details of Land DA's TAR saved on local disk .
#dir_list = wrapper.read_local_tar_dirs(tar_fn='landda_inputs.tar.gz_v1.1')

# Generate & save data map for the Land DA's TAR-based dataset of interest. 
# Note: Data map for the Land DA's TAR-based dataset' details will be saved to a csv file, but
# map can be save in a different format should futher development be required.
df = wrapper.extract_object_details(dir_list, 
                                    sz_list,
                                    feats_dict = {1: "Category", 
                                                  2: 'Dataset Type', 
                                                  3: 'Sub-Category', 
                                                  5: "Version (YYYY)"
                                                 }
                                   )

# = Additional Preprocessing Is Required for Generating Data Map Made Against Current Land DA TAR-Based Object's Data Structure Set For Land DA v1.1.0. =

# Currently, the "C" resolutions, data versions, & "mx" ocean resolutions are featured within multiple foldernames
# across the keys/directories of the Land DA app's TAR-based object residing 
# in cloud storage, noaa-ufs-land-da-pds. The reason is due to the current way
# the data has been strucutured for the Land DA app's framework.

# C resolution extracted w/ priority & secondary "resolution" feature columns set.
df = wrapper.extract_first_res(df, 'Data File', 4)

# Ocean resolution (mx) extracted w/ priority & secondary "ocean resolution" feature column set.
df = wrapper.extract_mx_res(df, 'Data File', 'Sub-Category')

# Filter out redundant column details.
df = df.drop([0], axis=1)
df = df.drop([4], axis=1)

# Re-arrange features.
df.insert(0, "Data File", df.pop("Data File"))
df.insert(1, "Category", df.pop("Category"))
df.insert(2, "Sub-Category", df.pop("Sub-Category"))
df.insert(3, "Resolution (C)", df.pop("Resolution (C)"))
df.insert(4, "Ocean Resolution (mx)", df.pop("Ocean Resolution (mx)"))
df.insert(5, "Data Format", df.pop("Data Format"))
df.insert(6, "File Size (Bytes)", df.pop("File Size (Bytes)"))
df.insert(7, "Version (YYYY)", df.pop("Version (YYYY)"))
df.insert(8, "Dataset Type", df.pop("Dataset Type"))

# Save data details.
wrapper.save_data(df, 
                  f'../results/{args.key}_{args.bucket}_data_map.csv')

