import boto3
import botocore
import json
import os


class S3FileManager():

	def __init__(self, profile="", bucket="", source_location="", local_mode=False):
		if local_mode:
			self.file_location = ''
		else:
			self.file_location = '/home/ec2-user/processes/rt-data-argos/'
		self.raw_prefix = "raw/"
		self.profile = profile
		self.session = boto3.session.Session(profile_name=profile)
		self.s3 = self.session.resource('s3')
		self.bucket_name = bucket
		self.bucket = self.s3.Bucket(self.bucket_name)
		self.source_location = source_location
		self.downloaded_files = []


	def list_files(self):
		files = []
		for obj in self.bucket.objects.filter(Prefix=self.raw_prefix).all():
			files.append(obj.key)
		return files


	def download_all_files(self):
		files_to_download = self.list_files()
		if files_to_download :
			self.download_files(files_to_download)


	def download_files(self, keys=[]):
		for key in keys:
			local_filename = self.download_file(key)
			if local_filename:
				self.downloaded_files.append(local_filename)


	def download_file(self, key=None):
		try:
			local_filename = f"{self.file_location + self.source_location}{key.replace('/','_')}"
			self.bucket.download_file(key,local_filename)
			file_object =  {
				"key": key,
				"local_filename": local_filename,
				"json_events": self.get_json_records(local_filename)
			}
			return file_object
		except botocore.exceptions.ClientError as e:
			if e.response['Error']['Code'] == "404":
				print(f"{key} does not exist.")
				return False
			else:
				raise


	def get_json_records(self, file):
		raw_string = None
		with open(file, 'r') as f:
			raw_string = f.readline()

		adjusted_string = "[" + raw_string.replace('}{', '},{') + "]"
		return json.loads(adjusted_string)


	def remove_processed_files(self):
		processed_files = []
		for file in self.downloaded_files:
			new_filename = file['local_filename'] \
				.replace('raw_', 'processed_') \
				.replace('_', '/') \
				.replace('source/', '') \
				.replace(self.file_location, '')
			self.bucket.upload_file(file['local_filename'], new_filename)
			processed_files.append({
				"Key": file['key']
				})
		if processed_files:
			deletion_object = {"Objects": processed_files}
			self.bucket.delete_objects(Delete = deletion_object)


	def clear_local_files(self):
		for file in self.downloaded_files:
			cmd = f"rm {file['local_filename']}"
			os.system(cmd)


