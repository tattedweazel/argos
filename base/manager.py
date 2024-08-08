from abc import ABC, abstractmethod
from utils.secret_squirrel import SecretSquirrel


class Manager(ABC):

	def __init__(self, local_mode = False):
		if local_mode:
			self.file_location = ''
		else:
			self.file_location = '/home/ec2-user/processes/rt-data-argos/'
		self.creds = SecretSquirrel(self.file_location).stash
		super().__init__()