import boto3
import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread
from flask import current_app as app

class S3_Utils:

    _instance = None    
    s3 = None    
    MAX_THREADS = 4
    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance.s3 = boto3.client(
                's3',
                aws_access_key_id=app.config['S3_ACCESS_KEY_ID'],
                aws_secret_access_key=app.config['S3_SECRET_ACCESS_KEY'],
                endpoint_url=app.config['S3_ENDPOINT_URL']
            )
        return cls._instance

    def download_chunk(self, s3_client, bucket_name, object_key, start_byte, end_byte, file):
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=object_key,
            Range=f"bytes={start_byte}-{end_byte}"
        )
        print("download thread created")
        object_data = response['Body'].read()
        file.seek(start_byte)
        file.write(object_data)

    def download_object(self, bucket_name, object_key):        
        try:
            s3_client = self.s3
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            total_size = response['ContentLength']
            
            # Specify the local file path where the object will be saved
            local_file_path = os.path.join(app.config['CACHE_DIR'], object_key)
            working_file_path = local_file_path + '.working'
            
            if os.path.exists(local_file_path):
                with open(local_file_path, 'rb') as file:
                    file_content = file.read()
                return file_content, True
            
            if os.path.exists(working_file_path):
                return f"Downloading object '{object_key}' is already in progress. Please check again later.", False
            
            # Create a separate thread for downloading the object
            def download_thread():
                try:
                    with open(working_file_path, 'wb') as file:
                        chunk_size = 41943040
                        start_time = time.time()

                        with ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
                            futures = []
                            downloaded_bytes = 0

                            while downloaded_bytes < total_size:
                                sys.stdout.write(f"\rDownloading: {downloaded_bytes/1024/1024:.2f}/{total_size/1024/1024:.2f} MB")
                                sys.stdout.flush()

                                remaining_bytes = total_size - downloaded_bytes
                                bytes_to_download = min(chunk_size, remaining_bytes)

                                future = executor.submit(self.download_chunk, s3_client, bucket_name, object_key, downloaded_bytes, downloaded_bytes + bytes_to_download - 1, file)
                                futures.append(future)

                                downloaded_bytes += bytes_to_download

                                current_time = time.time()
                                elapsed_time = current_time - start_time
                                download_speed = (downloaded_bytes * 8 / 1024 / 1024) / elapsed_time
                                sys.stdout.write(f"  |  Download Speed: {download_speed:.2f} Mbps")

                                # Limit the maximum number of threads to 4
                                if len(futures) >= self.MAX_THREADS:
                                    completed_futures = as_completed(futures)
                                    for completed_future in completed_futures:
                                        completed_future.result()
                                        futures.remove(completed_future)

                            for future in futures:
                                future.result()

                            sys.stdout.write('\n')
                            print(f"Object downloaded successfully to: {working_file_path}")
                    
                    # Rename the working file to the final file path
                    os.rename(working_file_path, local_file_path)
                except Exception as e:
                    print(f"Error downloading object: {str(e)}")
                    # Remove the working file if an error occurred
                    os.remove(working_file_path)

            # Start the download thread in the background
            download_thread = Thread(target=download_thread)
            download_thread.start()

            # Return a message indicating that the download is in progress
            return f"Downloading object '{object_key}' in the background. Please check again later.", False
        except Exception as e:
            print(f"Error downloading object: {str(e)}")

    def list_objects(self, bucket_name):
        # Access Key Name - project_s3
        s3 = self.s3
        response = s3.list_objects_v2(Bucket=bucket_name)

        object_names = []

        if 'Contents' in response:
            for obj in response['Contents']:
                object_key = obj['Key']
                object_names.append(object_key)

        return object_names

    
    def get_bucket_names(self):
        response = self.s3.list_buckets()

        # Extract bucket names from the response
        bucket_names = [bucket['Name'] for bucket in response['Buckets']]        

        return bucket_names
    
    def get_object_details(self, bucket_name, object_key):
        s3 = self.s3
        response = s3.head_object(Bucket=bucket_name, Key=object_key)        
    
        return response
    
    def download_url(self, object_key, bucket):
        bucket_name = bucket
        expiration = 3600
        url = self.s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=expiration
        )
        return url


