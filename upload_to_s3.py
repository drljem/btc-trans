import os
import time
import boto3

# set the folder path to monitor
folder_path = 'raw_data'

# set the S3 bucket name and folder prefix to use for uploaded files
bucket_name = 'drljem-crypto-data'
folder_prefix = 'raw_data/'

# set the time interval (in seconds) between checks for new files
check_interval = 60

access_key_id = os.environ['AWS_ACCESS_KEY_ID']
secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

s3 = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)



# start an infinite loop to monitor the folder
while True:
    # get a list of all files in the folder
    file_list = os.listdir(folder_path)
    
    # filter out any files that have already been uploaded
    #uploaded_files = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)['Contents']
    #uploaded_file_names = [obj['Key'] for obj in uploaded_files]
    #new_file_list = [file for file in file_list if file not in uploaded_file_names]
    
     # upload the new files to S3

    if len(file_list)>1:
        for file in sorted(file_list)[:-1]:
            file_path = os.path.join(folder_path, file)
            folders_names = file_path.split('-')
            s3.upload_file(file_path, bucket_name, f"{folder_prefix}{folders_names[1]}/{folders_names[3]}/{folders_names[2]}/{folders_names[4]}")
            os.remove(file_path)
    
    # wait for the specified interval before checking for new files again
    time.sleep(check_interval)
