import urllib.parse
from base.manager import Manager
from pymongo import MongoClient


class DocumentDbManager(Manager):
	
	def __init__(self, local_mode = False):
		super().__init__(local_mode)

		self.VOD_HEARTBEAT_INTERVAL = 30
		self.LIVE_HEARTBEAT_INTERVAL = 30

		username = urllib.parse.quote_plus(self.creds['DOC_DB_USER'])
		password = urllib.parse.quote_plus(self.creds['DOC_DB_PASS'])
		host = self.creds['DOC_DB_HOST']
		ca_pem = f"{self.file_location}{self.creds['DOC_DB_CA']}"
		uri = f"mongodb://{username}:{password}@{host}"
		self.client = MongoClient(uri, tls=True, tlsCAFile=ca_pem, retryWrites=False)


	def platform_map(self):
		return {
			'82QUauRB2fcLwFdhxrBkPc': "XBox",
			'3S7cr7ZP4LPQwanZptak4X': "Fire TV",
			'kCuGxxttj19mqUtbiKLHyx': "AppleTV",
			'kVJbcWwpbnqj9z2iT22ew6': "Android TV",
			'vYnWYfQXdPu82NYN4xB3or': "Roku"
		}


	def parse_vod_event(self, event_json):
		if 'userId' not in event_json:
			if 'user_uuid' not in event_json['properties']:
				user_id = event_json['anonymousId']
				anonymous = True
			else:
				user_id = event_json['properties']['user_uuid']
				anonymous = False
		elif event_json['userId'] is None:
			user_id = event_json['anonymousId']
			anonymous = True
		else:
			user_id = event_json['userId']
			anonymous = False
		if 'session_id' not in event_json['properties']:
			return False
		return {
			"user_id": user_id,
			"anonymous": anonymous,
			"session_id": event_json['properties']['session_id'],
			"channel_id": event_json['properties']['channel_id'],
			"series_id": event_json['properties']['series_id'],
			"episode_id": event_json['properties']['content_id'],
			"platform": event_json['properties']['platform'],
			"tier": event_json['properties']['user_tier'],
			"heartbeat_at": event_json['receivedAt'],
			"heartbeat_id": event_json['messageId']
		}


	def parse_live_event(self, event_json):
		if 'userId' not in event_json:
			if 'user_uuid' not in event_json['properties']:
				user_id = event_json['anonymousId']
				anonymous = True
			else:
				user_id = event_json['properties']['user_uuid']
				anonymous = False
		elif event_json['userId'] is None:
			user_id = event_json['anonymousId']
			anonymous = True
		else:
			user_id = event_json['userId']
			anonymous = False
		if 'session_id' not in event_json['properties']:
			return False
		return {
			"user_id": user_id,
			"anonymous": anonymous,
			"session_id": event_json['properties']['session_id'],
			"stream_id": event_json['properties']['content_id'],
			"platform": self.platform_map()[event_json['projectId']],
			"tier": event_json['properties']['user_tier'],
			"heartbeat_at": event_json['receivedAt'],
			"heartbeat_id": event_json['messageId']
		}


	def handle_vod_event(self, record):
		db = self.client['viewership']
		viewers = db['vod_viewers']
		parsed_event = self.parse_vod_event(record)
		if not parsed_event:
			return False
		existing_doc = viewers.find_one({
			"user_id": parsed_event['user_id'],
			"session_id": parsed_event['session_id']
			})

		if (existing_doc): # The doc exists, so we just need to update it
			heartbeat_set = set(existing_doc['heartbeats'])
			heartbeat_set.add(parsed_event['heartbeat_id'])
			response = viewers.find_one_and_update(
				{
					"user_id": parsed_event['user_id'],
					"session_id": parsed_event['session_id']
				},{
					"$set": { 
						"total_watch_time": (len(heartbeat_set) * self.VOD_HEARTBEAT_INTERVAL),
						"tier": parsed_event['tier']
					},
					"$min": {
						"watch_start": parsed_event['heartbeat_at']
					},
					"$max": {
						"last_viewed": parsed_event['heartbeat_at']
					},
					"$addToSet": {
						"heartbeats": parsed_event['heartbeat_id']
					}
				}
			)
		else: # This is a new doc that needs to be created
			response = viewers.insert_one({
				"user_id": parsed_event['user_id'],
				"session_id": parsed_event['session_id'],
				"anonymous": parsed_event['anonymous'],
				"platform": parsed_event['platform'],
				"tier": parsed_event['tier'],
				"channel_id": parsed_event['channel_id'],
				"series_id": parsed_event['series_id'],
				"episode_id": parsed_event['episode_id'],
				"watch_start": parsed_event['heartbeat_at'],
				"last_viewed": parsed_event['heartbeat_at'],
				"heartbeats": [parsed_event['heartbeat_id']],
				"total_watch_time": self.VOD_HEARTBEAT_INTERVAL
			})


	def handle_live_event(self, record):
		db = self.client['viewership']
		viewers = db['live_viewers']
		parsed_event = self.parse_live_event(record)
		existing_doc = viewers.find_one({
			"user_id": parsed_event['user_id'],
			"session_id": parsed_event['session_id']
			})

		if (existing_doc): # The doc exists, so we just need to update it
			heartbeat_set = set(existing_doc['heartbeats'])
			heartbeat_set.add(parsed_event['heartbeat_id'])
			response = viewers.find_one_and_update(
				{
					"user_id": parsed_event['user_id'],
					"session_id": parsed_event['session_id']
				},{
					"$set": { 
						"total_watch_time": (len(heartbeat_set) * self.LIVE_HEARTBEAT_INTERVAL),
						"tier": parsed_event['tier']
					},
					"$min": {
						"watch_start": parsed_event['heartbeat_at']
					},
					"$max": {
						"last_viewed": parsed_event['heartbeat_at']
					},
					"$addToSet": {
						"heartbeats": parsed_event['heartbeat_id']
					}
				}
			)
		else: # This is a new doc that needs to be created
			response = viewers.insert_one({
				"user_id": parsed_event['user_id'],
				"session_id": parsed_event['session_id'],
				"anonymous": parsed_event['anonymous'],
				"platform": parsed_event['platform'],
				"tier": parsed_event['tier'],
				"stream_id": parsed_event['stream_id'],
				"watch_start": parsed_event['heartbeat_at'],
				"last_viewed": parsed_event['heartbeat_at'],
				"heartbeats": [parsed_event['heartbeat_id']],
				"total_watch_time": self.LIVE_HEARTBEAT_INTERVAL
			})