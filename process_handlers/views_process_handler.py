from datetime import datetime
from utils.s3_file_manager import S3FileManager
from utils.document_db_manager import DocumentDbManager
from utils.logger import Logger


class ViewsProcessHandler():

	def __init__(self, local_mode = False):
		profile = 'roosterteeth'
		bucket = 'rt-data-stream'
		source = 'source/'
		self.start_time = datetime.now()
		self.logger = Logger()
		self.s3fm = S3FileManager(profile, bucket, source, local_mode)
		self.ddbm = DocumentDbManager(local_mode)


	def retrieve_data(self):
		self.logger.log("Downloading Files...")
		self.s3fm.download_all_files()


	def process_events(self):
		self.logger.log("Processing Events...")
		for file in self.s3fm.downloaded_files:
			for record in file['json_events']:
				record_type = record['event']
				if record_type == 'Video Heartbeat':
					self.handle_vod_event(record)
				elif record_type == 'Livestream Heartbeat':
					self.handle_live_event(record)


	def handle_vod_event(self, record):
		self.ddbm.handle_vod_event(record)


	def handle_live_event(self, record):
		self.ddbm.handle_live_event(record)


	def clean_up(self):
		self.logger.log("Cleaning Up...")
		self.remove_processed_files()
		self.remove_local_files()
		


	def remove_local_files(self):
		self.logger.log("Removing Local Files...")
		self.s3fm.clear_local_files()


	def remove_processed_files(self):
		self.logger.log("Removing Processed Files from S3...")
		self.s3fm.remove_processed_files()


	def run(self):
		self.logger.log("Processing Views...")
		self.retrieve_data()
		self.process_events()
		self.clean_up()
		self.logger.log("Process Complete!")
		self.logger.log(f"Process Run Time: {datetime.now() - self.start_time}")
