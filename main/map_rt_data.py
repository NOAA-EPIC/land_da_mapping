import sys
sys.path.append( '../modules' )
from data_map_generator import *
import pandas as pd
import argparse

'''
The development tool will translate the UFS-WM RT baseline & input datasets' details required for
the current Land DA app's test case into data maps. The generated data map will be made against the current data
structure featured within the UFS-WM RT baseline & input datasets called within the current Land DA's script, retrieved_data.sh.
Note: At this time, the current Land DA application release is v1.2.0 & taking placed within the Planning Interval (PI) 10.

Users must input the S3 bucket & TAR-Based Object's key to translate the UFS-WM RT baseline & input datasets' details
required for the current Land DA app's test case into data maps.

Example:
python map_rt_data.py -b land-da -k_input_data input-data-20221101 -k_bl_data develop-20231122

'''

# User inputs
argParser = argparse.ArgumentParser()
argParser.add_argument("-b", "--bucket", help="Object's bucket label. Type: String. Options: 'rt' ")
argParser.add_argument("-k_input_data", "--input_data_key", help="Input Data Object's key. Type: String. Ex: 'f'input-data-20221101' ")
argParser.add_argument("-k_bl_data", "--bl_data_key", help="Baseline Data Object's key. Type: String. Ex: 'f'develop-20231122' ")
args = argParser.parse_args()

# Read S3 cloud storage reserved for UFS-WM RT datasets
# Note: A subset of the UFS-WM RT's data is used for the current Land DA release's test case.
wrapper = DataMapGenerator(use_bucket=args.bucket)

# Generate list of keys from S3 cloud storage
key_list = wrapper.get_all_s3_keys()

# Generate & save data map for the UFS-WM RT input datasets of interest. 
# Note: Data map for the UFS-WM RT input datasets' details will be saved to a csv file, but
# map can be save in a different format should futher development be required.
df_input = wrapper.extract_object_details(key_list, 
                                          feats_dict={0: 'Dataset',
                                                      1: 'UFS Component',
                                                      2: 'Sub-Category',
                                                      4: 'Category'}, 
                                          filter2prefix=args.input_data_key
                                         )

# = Additional Preprocessing Is Required for Generating Data Map Made Against Current UFS-WM RT's Input Data Structure Set For Land DA v1.2.0. =

# C resolution extracted
# Currently, the "C" resolutions are featured within multiple foldernames
# across the keys/directories of the UFS-WM RT input datasets. The reason is
# the due to the current way the data has been structured for the UFS-WM RT framework.
df_input = wrapper.extract_cres(df_input, 'Sub-Category', 'UFS Component', 'Data File')

# Ocean resolution (o, mx, & (w/out symbol declared) extracted
df_input = wrapper.extract_o_res(df_input, 'Sub-Category')
df_input = wrapper.extract_mx_res(df_input, 'Sub-Category', 'Data File')
df_input = wrapper.extract_nosym_res(df_input, 'Sub-Category')

# Data version extracted
df_input = wrapper.extract_version(df_input, 5, 6)

# Filter out redundant column details
df_input = df_input.drop([3], axis=1)
df_input = df_input.drop([5], axis=1) # Features Some Version Dates
df_input = df_input.drop([6], axis=1) # Features Some Version Dates
df_input = df_input.drop([7], axis=1)

# Re-arrange data features
df_input.insert(0, "Data File", df_input.pop("Data File"))
df_input.insert(1, "UFS Component", df_input.pop("UFS Component"))
df_input.insert(2, "Resolution (C)", df_input.pop("Resolution (C)"))
df_input.insert(3, "Ocean Resolution (o)", df_input.pop("Ocean Resolution (o)"))
df_input.insert(4, "Ocean Resolution (mx)", df_input.pop("Ocean Resolution (mx)"))
df_input.insert(5, "Ocean Resolution (w/o symbol)", df_input.pop("Ocean Resolution (w/o symbol)"))
df_input.insert(6, "Data Format", df_input.pop("Data Format"))
df_input.insert(7, "File Size (Bytes)", df_input.pop("File Size (Bytes)"))
df_input.insert(8, "Category", df_input.pop("Category"))
df_input.insert(9, "Sub-Category", df_input.pop("Sub-Category"))
df_input.insert(10, "Dataset", df_input.pop("Dataset"))

# Generate & save data map for the UFS-WM RT baseline datasets of interest. 
# Note: Data map for the UFS-WM RT baseline datasets' details will be saved to a csv file, but
# map can be save in a different format should futher development be required.
df_bl = wrapper.extract_object_details(key_list,
                                       feats_dict={0: 'Dataset',
                                                   2: "Category"},
                                       filter2prefix=args.bl_data_key
                                      )
# = Additional Preprocessing Is Required for Generating Data Map Made Against Current UFS-WM RT's Baseline Data Structure Set For Land DA v1.2.0. =

# Associated regression test names extracted
df_bl = wrapper.extract_test_name(df_bl, 1)

# Associated compiler names extracted
df_bl = wrapper.extract_compiler(df_bl, 1)

# Filter out redundant column details
df_bl = df_bl.drop([1], axis=1)

# Re-arrange features
df_bl.insert(0, "Data File", df_bl.pop("Data File"))
df_bl.insert(1, "Test Name", df_bl.pop("Test Name"))
df_bl.insert(2, "Compiler", df_bl.pop("Compiler"))
df_bl.insert(len(df_bl.columns)-2, "File Size (Bytes)", df_bl.pop("File Size (Bytes)"))
df_bl.insert(len(df_bl.columns)-1, "Dataset", df_bl.pop("Dataset"))

# Save data details
wrapper.save_data(df_input, 
                  f'../results/{args.bucket}_{args.input_data_key}_data_map.csv')
wrapper.save_data(df_bl,
                  f'../results/{args.bucket}_{args.bl_data_key}_data_map.csv')
