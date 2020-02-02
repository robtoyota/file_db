from cmd import Cmd
from Util.Config import Config
import multiprocessing as MP
from FileDbDAL.Search import Search
import time
import sys
import traceback


class UserInterface(Cmd):
	# https://code-maven.com/interactive-shell-with-cmd-in-python

	prompt = 'file_db> '
	intro = "Type ? to list commands"

	def __init__(self, config_file):
		super().__init__()
		# Load the config
		self.config = Config.load_config(config_file)


	def search_cmd(self, args):
		try:
			criteria, path = args.split(' ', 1)
		except ValueError:
			return 'Error, missing arguments'

		if criteria == "name":
			self.search_name(path)
		elif criteria == "name_file":
			self.search_name_file(path)
		elif criteria == "name_dir":
			self.search_name_dir(path)
		elif criteria == "hash":
			self.search_hash(path, None)
		elif criteria == "duplicate_file":
			self.search_duplicate_file(path)
		elif criteria == "duplicate_dir":
			self.search_duplicate_dir(path)
		elif criteria == "file_size":
			self.search_file_size(path)
		elif criteria == "date":
			self.search_date(path)
		elif criteria == "timestamp":
			self.search_timestamp(path)
		elif criteria == "timestamp_range":
			self.search_timestamp_range(path)


	def hash_file_cmd(self, args):
		pass

	def hash_dir_cmd(self, args):
		pass

	def scrape_dir_cmd(self, args):
		pass

	def scrape_file_cmd(self, args):
		pass

	def reschedule_dir_cmd(self, args):
		pass


	def search_name(self, name):
		pass
		# Search.print_search(self.pg, 'search_name', {'name': name})

	def search_name_file(self, name):
		pass
		# Search.print_search(self.pg, 'search_name_file', {'name': name})

	def search_name_dir(self, name):
		pass
		# Search.print_search(self.pg, 'search_name_dir', {'name': name})

	def search_hash(self, hash, hash_algorithm=None):
		pass
		# Search.print_search(self.pg, 'search_hash', {'hash': hash, 'hash_algorithm': None})

	def search_duplicate_file(self, path):
		pass

	def search_duplicate_dir(self, path):
		pass

	def search_file_size(self, path):
		pass

	def search_timestamp(self, path):
		pass

	def search_date(self, path):
		pass

	def search_timestamp_range(self, path):
		pass


"""
search hash [path]
search duplicate_file [path]
search duplicate_dir [path]
search name [name]  # Searches both file and dir names
search name_file [name]
search name_dir [name]
search file_size
search date
search timestamp
search timestamp_range

hash_file [path]
hash_dir [path]

scrape_dir [path]
scrape_file [path]

reschedule_dir [path] [interval]

"""
