import boto3
import pandas as pd
import io
import sys
import os
from smart_open import smart_open

BUCKET_NAME = 'raw-011-nutresadatalake-std'
PATH = 'laft/clientes/sapbw/ventas/'
FILE = 'GEO_CLIENTES_ZDNA_30012020_.csv'

class utils:
    def __init__(self):
        pass

    def read_s3_file_resource(self, FILE, PATH, BUCKET_NAME):
        s3 = boto3.resource('s3')
        file_metadata = s3.Bucket(BUCKET_NAME).Object(PATH + FILE).get()
        flat_file = io.StringIO(file_metadata.get('Body').read().decode('latin-1'))
        df = pd.read_csv(flat_file, sep=';')
        return df
        
    # def s3_to_pandas(self, client, bucket, key, header=None):

    #     # get key using boto3 client
    #     obj = client.get_object(Bucket=bucket, Key=key)
    #     gz = gzip.GzipFile(fileobj=obj['Body'])
        
    #     # load stream directly to DF
    #     return pd.read_csv(gz, header=header, dtype=str)
        
    # def s3_to_pandas_with_processing(self, client, bucket, key, header=None):
    
    #     # get key using boto3 client
    #     obj = client.get_object(Bucket=bucket, Key=key)
    #     gz = gzip.GzipFile(fileobj=obj['Body'])
    
    #     # replace some characters in incomming stream and load it to DF
    #     lines = "\n".join([line.replace('?', ' ') for line in gz.read().decode('utf-8').split('\n')])
    #     return pd.read_csv(io.StringIO(lines), header=None, dtype=str)

if __name__ == '__main__':
    utl = utils()
    df = utl.read_s3_file_resource(FILE, PATH, BUCKET_NAME)
    print(df.head())