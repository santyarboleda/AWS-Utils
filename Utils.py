import sagemaker, boto3, os, io, zipfile
import pandas as pd

class Boto3Utils:
    def __init__(self):
        pass
    
    def read_s3_csv_file_resource(self, FILE, PATH, BUCKET_NAME):
        '''
        Function to read csv file from S3 file
        '''
        s3 = boto3.resource('s3')
        file_metadata = s3.Bucket(BUCKET_NAME).Object(PATH + FILE).get()
        flat_file = io.StringIO(file_metadata.get('Body').read().decode('latin-1'))
        return pd.read_csv(flat_file, sep = ';', error_bad_lines=True, encoding='latin-1')
    
    def read_s3_csv_ext_zip_file_resource(self, FILE, PATH, BUCKET_NAME):
        '''
        Function to extract zip file content from S3 file 
        '''
        s3 = boto3.resource('s3')
        file_metadata = s3.Bucket(BUCKET_NAME).Object(PATH + FILE).get()
        file_metadata = io.BytesIO(file_metadata["Body"].read())
        with zipfile.ZipFile(file_metadata, mode='r') as zipf:
            zipf.extractall()
        return pd.read_csv(zipf.namelist()[0], sep = ';', error_bad_lines=True, encoding='latin-1')
    
    def read_s3_csv_gz_file_resource(self, FILE, PATH, BUCKET_NAME):
        '''
        Function to read gz file from S3 file
        '''
        s3 = boto3.resource('s3')
        file_metadata = s3.Bucket(BUCKET_NAME).Object(PATH + FILE).get()
        file_metadata = io.BytesIO(file_metadata["Body"].read())
        return pd.read_csv(file_metadata, compression='gzip', header=0, sep=';', error_bad_lines = False, encoding='latin-1', low_memory=False)
    
    def read_s3_csv_zip_file_resource(self, FILE, PATH, BUCKET_NAME):
        '''
        Function to read zip file content from S3 file
        '''
        s3 = boto3.resource('s3')
        file_metadata = s3.Bucket(BUCKET_NAME).Object(PATH + FILE).get()
        file_metadata = io.BytesIO(file_metadata["Body"].read())
        return pd.read_csv(file_metadata, compression='zip', header=0, sep=';', error_bad_lines = False, encoding='latin-1')