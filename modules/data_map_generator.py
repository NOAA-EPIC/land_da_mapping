import io
import tarfile
import sys
import boto3
from boto3.s3.transfer import TransferConfig
from botocore import UNSIGNED
from botocore.client import Config
import pandas as pd
from pandas import ExcelWriter
import numpy as np
from pathlib import Path
import time
import csv
import re
import os
import warnings
warnings.filterwarnings("ignore")

class DataMapGenerator():
    """
    Map data from cloud service provider's data storage.
    
    """
    def __init__(self, use_bucket):
        """
        Args:                          
            use_bucket (str): If set to 'rt', data will be read from the cloud data
                              storage bucket designated for the UFS-WM RT datasets. If set 
                              to 'srw', data will be read from the cloud data
                              storage bucket designated for the UFS SRW datasets. If set to
                              'land-da', data will be read from the cloud data storage
                              bucket designated for the UFS Land DA datasets. 
                              Options: 'srw', 'land-da', 'rt'
                              
        """
        
        # Cloud service provider's data storage options.
        if use_bucket == 'land-da':
            self.bucket_name = 'noaa-ufs-land-da-pds'
            self.profile = 'land-da-app'
        elif use_bucket == 'srw':
            self.bucket_name = 'noaa-ufs-srw-pds'
            self.profile = 'srw-app'
        elif use_bucket == 'rt':
            self.bucket_name = 'noaa-ufs-regtests-pds'            
            self.profile = 'ufs-wm-rt-app'
        else:
            print(f"{use_bucket} Bucket Does Not Exist.")

        # Set client session.
        self.s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        
        # Create folder directory to save data maps & list of cloud keys.
        if not os.path.exists('../results'):
            os.makedirs('../results')
        sys.path.append( '../results' )
    
    def get_all_s3_keys(self):
        """
        Extract keys from cloud service provider's storage.
        
        Args:
            None
            
        Return (list): List of keys residing within the cloud
        storage of interest.

        """
        kwargs = {'Bucket': self.bucket_name}
        key_list = []
        while True:
            resp = self.s3.list_objects_v2(**kwargs)
            for content in resp.get('Contents', []):
                key_list.append(content['Key'])
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break
              
        return key_list
    
    def read_s3_object_dirs(self, tar_object_fn):
        """
        Extract directories from TAR-based object in cloud.
        
        Args:
            tar_object_fn (str): TAR-based object's key in cloud.
            
        Return (list, list): List of directories & their corresponding size in bytes
        featured within the TAR-based object in cloud.

        """
        # Extract all directories & file sizes featured within TAR-based cloud object.
        s3_object = self.s3.get_object(Bucket=self.bucket_name, Key=tar_object_fn)
        wholefile = s3_object['Body'].read()
        fileobj = io.BytesIO(wholefile)
        tarf = tarfile.open(fileobj=fileobj)
        dir_list = [tarinfo.name.replace('./', '', 1) for tarinfo in tarf]
        sz_list = [tarinfo.size for tarinfo in tarf]
        
        # Save list of directories to local ../results directory.
        with open(f'../results/{self.bucket_name}_all_keys.csv', 'w+', newline ='') as f_handle:
            for item in dir_list:
                f_handle.write(item + '\n')
        print(f"List of {self.bucket_name} keys saved to ../results.")
              
        return dir_list, sz_list
        
    def extract_object_details(self, dir_list, tar_file_sz_list=[], feats_dict=None, filter2prefix=''):
        """
        Extract key per object from s3 storage w/ filtering option.
        
        Args:
            dir_list (list): List of directories featured within the TAR-based 
                             object or list of objects' keys within cloud storage.
            
            tar_file_sz_list (list): If providing list of directories 
                                     featured within a TAR-based object, then set as
                                     list of file sizes featured within TAR-based object 
                                     (list can be obtained from read_s3_object_dirs()).
                                     If providing list of objects' keys within cloud storage, 
                                     then set as an empty list.
            
            feats_dict (dict): Dictionary of feature names to be set for a given dataframe's
                               column (each hierarchical folder/level presented within list 
                               of directories/keys).  If not applicable, set as None.
            
            filter2prefix (str): Prefix of object keys to extract
                                 from cloud storage. If not applicable, set as default value.
            
        Return (pd.DataFrame): Dataframe comprised of object names or filenames, 
        file format, & file size with the dataframe's columns set to the desired 
        feature names listed within feats_dict.

        """
        # Create a page iterator
        paginator = self.s3.get_paginator('list_objects')
        page_iterator = paginator.paginate(Bucket='noaa-ufs-regtests-pds')

        # Extract & parse each file/object's directory/key & their corresponding file format & file size
        key_list=[]
        sz_list=[]
        
        # For extracting detail of each object stored within cloud storage
        if filter2prefix != '':
            for page in page_iterator:
                for rc in page['Contents']:
                    if filter2prefix != '' and filter2prefix in rc.get('Key') and rc.get('Key').count(".") >= 1:
                        a_tokens =  rc.get('Key').split('/')
                        key_list.append(a_tokens)
                        sz_list.append(rc.get('Size'))

        # For extracting detail of each file from TAR stored within cloud storage
        else:
            # Factor only files & their respective file size.
            for idx, (path_dir, file_sz) in enumerate(zip(dir_list, tar_file_sz_list)):
                if path_dir.count(".") >= 1:
                    a_tokens = path_dir.split('/')
                    key_list.append(a_tokens)
                    sz_list.append(file_sz)

        # Drop first data file duplicate across column per row
        tokens_drop_list = []
        for element in key_list:
            tokens_drop_list.append(element[:-1])

        # Generate a dataframe comprised of the data details
        df = pd.DataFrame(tokens_drop_list)
        df['File Size (Bytes)'] = sz_list

        # Feature names to be set for a given dataframe's column 
        df = df.rename(columns=feats_dict)
        df.fillna("", inplace=True) 

        # Create a column comprised of the data filenames
        for idx, token in enumerate(key_list):
            df.loc[idx, 'Data File'] = token[-1]

        # Create a column comprised of the data file formats
        for idx, val in df['Data File'].items():
            df.loc[idx, 'Data Format'] = os.path.splitext(val)[-1]

        return df   
        
    def extract_cres(self, df, res_col_1, res_col_2, res_col_3):
        """
        Extract "C" resolution from each file's directory if applicable.

        Applicable to UFS-WM RT input datasets in cloud storage.
        Currently, the "C" resolutions are featured within multiple foldernames
        across the keys/directories of the UFS-WM RT input datasets residing in
        cloud storage, noaa-ufs-regtests-pds. The reason is the due to the current
        way the data has been strucutured for the UFS-WM RT framework.
        
        Args:
            df (pd.DataFrame): Dataframe to preprocess.
            
            res_col_1 (str): Priority column featuring details on
                             the "C" resolutions.
            
            res_col_2 (str): Secondary column featuring details on
                             the "C" resolutions.

            res_col_3 (str): Tertiary column featuring details on
                             the "C" resolutions.

        Return (pd.DataFrame): Dataframe with "C" resolutions extracted 
        & appended as a new feature column.

        """
        for idx, row in df[[res_col_1, res_col_2, res_col_3]].iterrows():
            res = [x for x in row if re.search(r'C\d{2,4}', x)]
            res2 = [x for x in row if re.search(r'data\d{2,4}', x)]
            res3 = [x for x in row if re.search(r'C\d{2,4}(.*?)\.', x)]
            if res:
                df.loc[idx, 'Resolution (C)']= re.findall(r'C\d{2,4}', res[0])[0].replace('C','')
            elif res2:
                df.loc[idx, 'Resolution (C)']= re.findall(r'data\d{2,4}', res2[0])[0].replace('data','')
            elif res3:
                df.loc[idx, 'Resolution (C)']= re.findall(r'C\d{2,4}', res3[0])[0].replace('C','')
            else:
                df.loc[idx, 'Resolution (C)']= np.nan
              
        return df
        
    def extract_first_res(self, df, res_col_1, res_col_2):
        """
        Extract first "C" resolution seen per row with priority & secondary feature column set.
        
        Applicable to Land DA app's TAR-based object in cloud storage. Currently, 
        the "C" resolutions are featured within multiple foldernames across the keys/directories
        of the Land DA app's TAR-based object residing in cloud storage, 
        noaa-ufs-land-da-pds.
        
        Args:
            df (pd.DataFrame): Dataframe to preprocess
            
            res_col_1 (str): Priority column featuring details on
                             the "C" resolutions.
            
            res_col_2 (str): Secondary column featuring details on
                             the "C" resolutions.

        Return (pd.DataFrame): Dataframe with "C" resolutions extracted 
        & appended as a new feature column.

        """
        for idx, row in df[[res_col_1, res_col_2]].iterrows():
            res = [x for x in row if re.search(r'C\d{2,3}', x)]
            if res:
                df.loc[idx, 'Resolution (C)']= re.findall(r'C\d{2,3}', res[0])[0].replace('C','')
            else:
                df.loc[idx, 'Resolution (C)']= np.nan
              
        return df
        
    def extract_mx_res(self, df, mx_res_col, mx_res_col2):
        """
        Extract "mx" ocean resolution seen per row with priority & secondary feature column set.
        
        Args:
            df (pd.DataFrame): Dataframe to preprocess

            mx_res_col (str): Priority column featuring details on
                              the "mx" ocean resolutions.

            mx_res_col2 (str): Secondary column featuring details on
                               the "mx" ocean resolutions.
            
        Return (pd.DataFrame): Dataframe with "mx" ocean resolutions extracted 
        & appended as a new feature column.

        """
        for idx, row in df.iterrows():
            mx_res = re.findall('mx\d{2,3}', row[mx_res_col])
            mx_res2 = re.findall('mx\d{2,3}', row[mx_res_col2])
            if mx_res:
                df.loc[idx, 'Ocean Resolution (mx)']= mx_res[0].replace('mx','')
            elif mx_res2:
                df.loc[idx, 'Ocean Resolution (mx)']= mx_res2[0].replace('mx','')
            else:
                df.loc[idx, 'Ocean Resolution (mx)']= np.nan
                
        return df

    def extract_o_res(self, df, o_res_col):
        """
        Extract "o" ocean resolution per row with feature column set.
        
        Args:
            df (pd.DataFrame): Dataframe to preprocess
            
            o_res_col (str): Column featuring details on
                             the "o" ocean resolutions.
            
        Return (pd.DataFrame): Dataframe with "o" ocean resolutions extracted 
        & appended as a new feature column.

        """
        for idx, row in df.iterrows():
            res = re.findall('o\d{2,3}', row[o_res_col])
            if res:
                df.loc[idx, 'Ocean Resolution (o)']= res[0].replace('o','')
            else:
                df.loc[idx, 'Ocean Resolution (o)']= np.nan
                
        return df

    def extract_dataset_type(self, df, dataset_type_col):
        """
        Extract dataset type per row with feature column set.
        
        Args:
            df (pd.DataFrame): Dataframe to preprocess
            
            dataset_type_col (str): Column featuring details on
                             the dataset type (e.g. baseline & input
                             UFS-WM RT data).
            
        Return (pd.DataFrame): Dataframe with dataset type extracted 
        & appended as a new feature column.

        """
        for idx, row in df.iterrows():
            data_type1 = re.findall('develop-\d{8}', row[dataset_type_col])
            data_type2 = re.findall('input-data-\d{8}', row[dataset_type_col])
            if data_type1:
                df.loc[idx, 'Dataset Type']= re.findall(r'develop-\d{8}', data_type1[0])[0]
            elif data_type2:
                df.loc[idx, 'Dataset Type']= re.findall(r'input-data-\d{8}', data_type2[0])[0]
            else:
                df.loc[idx, 'Dataset Type']= np.nan
                
        return df

    def extract_nosym_res(self, df, nosym_res_col):
        """
        Extract ocean resolution for which are featured without a symbol declared.
        
        Args:
            df (pd.DataFrame): Dataframe to preprocess.
            
            nosym_res_col (str): Column featuring details on the ocean resolution 
                                 for which does not have a symbol declared.
            
        Return (pd.DataFrame): Dataframe with ocean resolutions without 
        a symbol (e.g. mx, o) extracted & appended as a new feature column.

        """
        for idx, row in df.iterrows():
            if row[nosym_res_col].isnumeric() and len(row[nosym_res_col])<=3 and len(row[nosym_res_col])>=2:
                df.loc[idx, 'Ocean Resolution (w/o symbol)']= row[nosym_res_col]
            else:
                df.loc[idx, 'Ocean Resolution (w/o symbol)']= np.nan
                
        return df
        
    def extract_test_name(self, df, feat_col):
        """
        Extract test name from each file's directory if applicable.
        
        Args:
            df (pd.DataFrame): Dataframe to preprocess.
            
            feat_col (str): Column featuring details on the data's 
                            associated test names.
            
        Return (pd.DataFrame): Dataframe with test names extracted & 
        appended as a new feature column.

        """
        for idx, row in df.iterrows():
            if row[feat_col].count("_") >= 1:
                df.loc[idx, 'Test Name'] = row[feat_col].rpartition('_')[0]
            else:
                df.loc[idx, 'Test Name'] = np.nan
                
        return df
        
    def extract_compiler(self, df, feat_col):
        """
        Extract compiler from each file's directory if applicable.
        
        Args:
            df (pd.DataFrame): Dataframe to preprocess.
            
            feat_col (str): Column featuring details on the data's
                            associated compiler.
            
        Return (pd.DataFrame): Dataframe with compiler names extracted &
        appended as a new feature column.

        """
        for idx, row in df.iterrows():
            if row[feat_col].count("_") >= 1:
                df.loc[idx, 'Compiler'] = row[feat_col].split('_')[-1]
            else:
                df.loc[idx, 'Compiler'] = np.nan
        
        return df

    def extract_version(self, df, ver_col_1, ver_col_2):
        """
        Extract version seen per row with priority & secondary feature column set.
        
        Args:
            df (pd.DataFrame): Dataframe to preprocess.
            
            ver_col_1 (str): Priority column featuring details on
                             the data's version.
            
            ver_col_2 (str): Secondary column featuring details on
                             the data's version.
            
        Return (pd.DataFrame): Dataframe with version dates extracted &
        appended as a new feature column.

        """
        for idx, row in df[[ver_col_1, ver_col_2]].iterrows():
            ver = [x for x in row if re.search(r'[0-9]{4}-[0-9]{2}', x)]
            ver2 = [x for x in row if re.search(r'[0-9]{4}', x)]
            if ver:
                df.loc[idx, 'Version']= re.findall(r'[0-9]{4}-[0-9]{2}', ver[0])[0]
            elif ver2:
                df.loc[idx, 'Version']= re.findall(r'[0-9]{4}', ver2[0])[0]
            else:
                df.loc[idx, 'Version']= np.nan
        return df

    def read_local_tar_dirs(self, tar_fn):
        """
        [Optional] Extract directories featured within a TAR saved on local disk.
        
        Args:
            tar_fn (str): Name of TAR (include file extension).
            
        Return (list): List of directories featured within TAR saved on 
        local disk.

        """
        with tarfile.open(tar_fn) as tar:
            dir_list= [str(tarinfo) for tarinfo in tar.getmembers()]
              
        return dir_list

    def save_data(self, df, save_fn):
        """
        Save dataframe as csv file.

        Args:
            df (pd.DataFrame): Dataframe to save as csv file.
            
            save_fn (str): Filename to save as csv.

        Return: None

        """
        df.to_csv(save_fn,
                  index=False)

        print(f"Data map saved to {save_fn}.")

        return

    def consolidate_maps(self, rt_bl_date, rt_input_date, tar_fn, land_da_version):
        """
        Save dataframe as .xlsx file.

        Currently, applicable to the v1.1.0 & v1.2.0 Land DA's test cases.

        Args:
            bl_map_fn (str): Baseline timestamp/date featuring the UFS-WM RT baseline data map.
                             saved under ../results folder.
                             (e.g. rt_baseline_{BL_DATE}_data_map.csv)
            
            input_map_fn (str): Input timestamp/date featuring the UFS-WM RT input data map
                                saved under ../results folder.
                                (e.g. rt_input_{INPUTDATA_DATE}_data_map.csv)
            
            tar_fn (str): Filename featuring the Land DA TAR-based object's data map.
                          saved under ../results folder.
                          (e.g. Landdav{version}_input_data.tar.gz_land-da_data_map.csv')
            
            land_da_version (str): Version of the Land DA to save within filename of the consolidated mapped .xlsx file.

        Return: None

        """
        # Read files featuring the data maps of UFS-WM RT baseline datasets required for Land DA's v1.2.0
        ufs_bl_df = pd.read_csv(f'../results/rt_baseline_{rt_bl_date}_data_map.csv')
        
        # Read files featuring the data maps of UFS-WM RT input datasets required for Land DA's v1.2.0
        ufs_input_df = pd.read_csv(f'../results/rt_input_{rt_input_date}_data_map.csv')
        
        # Read files featuring the data maps of the Land DA's TAR-based dataset required for Land DA's v1.2.0
        
        # Read referenced files featuring data maps
        land_da_input_df = pd.read_csv(f'../results/{tar_fn}')

        # Filter to the "DATM" & "NOAHMP Initial Condition" data required from the UFS-WM RT S3
        ic_input_df = ufs_input_df[(ufs_input_df['Dataset']==f'input-data-{rt_input_date}') & (ufs_input_df['UFS Component'].isin(['DATM_GSWP3_input_data', 'NOAHMP_IC']))]
        
        # Filter to the "Non-Fixed FV3" data required from the UFS-WM RT S3
        ufs_input_filtered_df2 = ufs_input_df[(ufs_input_df['Dataset']==f'input-data-{rt_input_date}') & (ufs_input_df['UFS Component'].isin(['FV3_input_data'])) & (ufs_input_df['Data File'].isin(['grid_spec.nc'])) & (ufs_input_df['Sub-Category'].isin(['INPUT']))]
        ufs_input_filtered_df3 = ufs_input_df[(ufs_input_df['Dataset']==f'input-data-{rt_input_date}') & (ufs_input_df['UFS Component'].isin(['FV3_input_data'])) & (ufs_input_df['Data File'].str.startswith('C96_grid.tile')) & (ufs_input_df['Sub-Category'].isin(['INPUT']))]
        nonfixed_input_df = pd.concat([ufs_input_filtered_df2, ufs_input_filtered_df3])
        
        # Filter to the "Fixed FV3" data required from the UFS-WM RT S3
        fixed_input_df = ufs_input_df[(ufs_input_df['Dataset']==f'input-data-{rt_input_date}') & (ufs_input_df['UFS Component'].isin(['FV3_fix_tiled'])) & (ufs_input_df['Resolution (C)']==96)]
        
        # Filter to the "DATM CDEPS LAND GSWP3" data required from the UFS-WM RT S3
        bl_filtered_df = ufs_bl_df[(ufs_bl_df['Dataset']==f'develop-{rt_bl_date}') & (ufs_bl_df['Compiler'].isin(['intel'])) & (ufs_bl_df['Test Name'].isin(['datm_cdeps_lnd_gswp3']))]
        
        # Consolidate all generated data maps required for the specified version of the Land DA application's test case.
        list_dfs = [ic_input_df,
                    nonfixed_input_df, 
                    fixed_input_df, 
                    bl_filtered_df, 
                    land_da_input_df]
        names = ["DATM_NOAHMP_IC", 
                 "NonFixed_FV3", 
                 "Fixed_FV3", 
                 "Baseline", 
                 "Land_DA_TAR"]
        save_fn = f'land_da_test_case_{land_da_version}_data_maps.xlsx'
        with ExcelWriter(f'../results/{save_fn}') as writer:
            for i, df in enumerate(list_dfs):
                df.to_excel(writer,sheet_name = names[i], index=False)
                
        print(f"Data maps have been consolidated & saved under '../results/{save_fn}'.")

        return      