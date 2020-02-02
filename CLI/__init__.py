from cmd import Cmd
from Util.Config import Config
from CLI.Search import Search
import multiprocessing as MP
from FileDbDAL.Search import Search
import time
import sys
import traceback


class UserInterface(Cmd):
	# https://code-maven.com/interactive-shell-with-cmd-in-python

	# Set the "UI" elements
	prompt = "file_db> "
	intro = "Type ? to list commands"

	def __init__(self, config_file: str) -> None:
		super().__init__()
		# Load the config
		self.config = Config.load_config(config_file)



	# Default action if the input is not known
	def default(self, args: str) -> None:
		print(f"Command not recognized: {args}")

	# Exit the UI
	def help_exit(self) -> None:
		print("Exit this UI application. (Ctrl+D)")

	def do_exit(self) -> bool:
		return True

	# Handle ctrl+D close
	do_EOF = do_exit
	helP_EOF = help_exit

	def do_search(self, args):
		try:
			criteria, path = args.split(' ', 1)
		except ValueError:
			return 'Error, missing arguments'

		if criteria == "name":
			Search.search_name(path)
		elif criteria == "name_file":
			Search.search_name_file(path)
		elif criteria == "name_dir":
			Search.search_name_dir(path)
		elif criteria == "hash":
			Search.search_hash(path, None)
		elif criteria == "duplicate_file":
			Search.search_duplicate_file(path)
		elif criteria == "duplicate_dir":
			Search.search_duplicate_dir(path)
		elif criteria == "file_size":
			Search.search_file_size(path)
		elif criteria == "date":
			Search.search_date(path)
		elif criteria == "timestamp":
			Search.search_timestamp(path)
		elif criteria == "timestamp_range":
			Search.search_timestamp_range(path)

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
