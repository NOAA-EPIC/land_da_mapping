import sys
sys.path.append( '../modules' )
from data_map_generator import *
import argparse

'''
The development tool will consolidate the required Land DA's test data details into a single file.
The generated file will feature a data map made against the current data structure featured within the Land DA TAR-based object 
called within the current Land DA's script, retrieved_data.sh. Note: At this time, the current Land DA application release
is v1.2.0 & taking placed within the Planning Interval (PI) 10.

The mapping tool is based on the current data structure featured within the Land DA datasets required for testing. 
Due to the fact the Land DA application's forecasting data requirements can vary based on a unique case requested by a user, 
the mapping tool will map out the data files as they pertain to the current Land DA application's case required for regression testing. 
The Land DA application's case required for regression testing is specified by the code manager (CM) team. At this time, there are three 
datasets sourced for testing the Land DA application:

1) Baseline data used within the UFS-WM RT framework (__Source:__ https://noaa-ufs-regtests-pds.s3.amazonaws.com/index.html)
2) Input data used within the UFS-WM RT framework (__Source:__ https://noaa-ufs-regtests-pds.s3.amazonaws.com/index.html)
3) Data extracted from the Land DA TAR-based object (e.g. landda_inputs.tar.gz_v1.1, land_da_new.tar.gz_v1.2, Landdav1.2.0_input_data.tar.gz)
issued by the current CM responsible for the Land DA application (__Source:__ https://noaa-ufs-land-da-pds.s3.amazonaws.com/index.html)

Per _retrieve_data.py_, the current Land DA application's test case will require subsets of the following timestamped datasets:

- _develop-20231108_
- _input-data-20221101_
- _Landdav1.2.0_input_data.tar.gz_

Users must input the timestamp of the UFS-WM RT's baseline & input datasets as well as the name of the Land DA TAR-based object
required for their Land DA's test case to consolidate the translated required Land DA's data details into a data map.

Example:
python consolidate_maps.py -b land-da -bl_fn rt_baseline_{BL_DATE}_data_map.csv -input_fn rt_input_{INPUTDATA_DATE}_data_map.csv -tar_fn Landdav{VERSION}_input_data.tar.gz_land-da_data_map.csv -ver {VERSION}

python consolidate_maps.py -b land-da -bl_ts 20231122 -input_ts 20221101 -tar_fn Landdav1.2.0_input_data.tar.gz_land-da_data_map.csv -ver 1.2.0

Log:
# File to reference for the updated Land DA's v1.2.0
# ver = '1.2.0'
ver = '1.2.0'
fn = f'Landdav{ver}_input_data.tar.gz_land-da_data_map.csv'

# File to reference for the Land DA's v1.2.0
# ver = '1.2.0'
#fn = f'land_da_new.tar.gz_v{ver}_land-da_data_map.csv'

# File to reference for the Land DA's v1.1.0
# ver = '1.1.0
#fn = f'landda_inputs.tar.gz_v{ver}_land-da_data_map.csv'

'''

# User inputs
argParser = argparse.ArgumentParser()
argParser.add_argument("-b", "--bucket", help="Object's bucket label. Type: String. Options: 'land-da' ")
argParser.add_argument("-bl_ts", "--bl_data_ts", help="UFS-WM RT Baseline timestamp in UFS-WM RT Baseline data map's filename. Type: String. Ex: 'rt_baseline_{BL_DATE}_data_map.csv' ")
argParser.add_argument("-input_ts", "--input_data_ts", help="UFS-WM RT Input timestamp in UFS-WM RT Input data map's filename. Type: String. Ex: YYYYMMDD")
argParser.add_argument("-tar_fn", "--tar_map_fn", help="LAND DA TAR-based object's data map. Type: String. Ex: YYYYMMDD ")
argParser.add_argument("-ver", "--land_da_version", help="LAND DA version to save within filename of the consolidated mapped .xlsx file. Type: String. Ex: 'rt_input_{INPUTDATA_DATE}_data_map.csv' ")
args = argParser.parse_args()

# Read S3 cloud storage reserved for Land DA app's dataset
wrapper = DataMapGenerator(use_bucket=args.bucket)

# Consolidate data maps for the Land DA test case version of interest
wrapper.consolidate_maps(args.bl_data_ts, args.input_data_ts, args.tar_map_fn, args.land_da_version)
