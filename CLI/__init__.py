from Util.Config import Config
from Interface.Util import Util
from Interface.Hash import Hash
from Interface.Schedule import Schedule
from Interface.Scrape import Scrape
from Interface.Search import Search

import re
import csv

from prompt_toolkit import Application
from prompt_toolkit import PromptSession, HTML
from prompt_toolkit import print_formatted_text as print  # Replace the print() function!
from prompt_toolkit.completion import NestedCompleter


# https://python-prompt-toolkit.readthedocs.io
class UserInterface():
	def __init__(self, pg, config_file: str) -> None:
		# Load the config
		self.config = Config.load_config(config_file)
		# Accept the database connection object
		self.pg = pg

		# Config the UI
		ps = PromptSession()  # Start a session for input history
		line_prompt = "file_db> "

		# Get the list of available commands
		self.commands = self.available_commands(['do'])

		# Setup the auto completion
		completion_dict = {cmd: None for cmd in self.commands}  # Get the functions
		# Add the nesting for the completion
		completion_dict['search'] = {  # Search command
			'name': None,
			'name_file': None,
			'name_dir': None,
			'hash': None,
			'duplicate_file': None,
			'duplicate_dir': None,
			'file_size': None,
			'date': None,
			'timestamp': None,
			'timestamp_range': None,
		}
		completer = NestedCompleter.from_nested_dict(completion_dict)

		# Perform the main input loop
		continue_running = True
		print('Type ? to list commands')
		while continue_running:
			# Accept the commands
			inp = ps.prompt(line_prompt, completer=completer)
			# Execute the command (functions returning Truthy will exit the loop)
			continue_running = self.execute_input(inp)

	# Return the list of available commands in this class
	def available_commands(self, prefixes: list) -> list:
		functions = []
		# Loop through the list of functions in this class
		for fname in dir(self):
			# Split the function name into [prefix]_[command]
			try:
				prefix, cmd = fname.split("_", 1)
			except ValueError:  # If the function name cannot be parsed this way (no underscores)
				continue  # If there is no prefix, then don't bother evaluating it

			# Add this command to the list of available commands, if appropriate
			if prefix in prefixes:
				functions.append(cmd)
		return functions

	def execute_input(self, inp) -> bool:
		# Santize the input
		inp = inp.strip()
		inp = re.sub('\s+', ' ', inp)  # Remove duplicate spaces

		# Determine the command name, and its arguments
		try:
			cmd, args = inp.split(" ", 1)  # Split the arguments from the command
		except ValueError:  # If there are no spaces to split by
			cmd = inp
			args = ''
		cmd = cmd.lower()
		args = self.parse_args(args)

		# Execute the command, if it is valid
		# TODO: Dynamically execute cmds https://stackoverflow.com/a/42227682/4458445
		if cmd == "search":
			self.do_search(args)
		elif cmd == "scrape_dir":
			self.do_scrape_dir(args)
		elif cmd == "view_scrape_schedule":
			self.do_view_scrape_schedule(args)
		elif cmd == "exit":
			return False
		else:
			self.default(cmd, args)

		# Continue running the program?
		return True

	def parse_args(self, arg_string: str) -> list:
		# TODO: Properly parse these arguments
		args = csv.reader([arg_string], delimiter=' ', quotechar='"')
		return next(args)

	def parse_path(self, path: str) -> str:
		path = path.strip()
		if path[-1:] in ["/", "\\"]:  # Remove any trailing path separators
			path = path[:-1]
		return path

	# Default action if the input is not known
	def default(self, cmd: str, args: str) -> None:
		print(f"Command not recognized: {cmd}")
		if args:
			print(f"Arguments: {' '.join(args)}")

	def do_help(self, args: str) -> None:
		pass

	def do_exit(self) -> bool:
		return True

	# Exit the UI
	def help_exit(self) -> None:
		print("Exit this UI application. (Ctrl+D)")

	# Perform searches
	def do_search(self, args: list) -> None:
		criteria = args[0]
		path = args[1]
		print(f"Searching! {args}")

		if criteria == "name":
			Search.search_name(self.pg, path)
		elif criteria == "name_file":
			Search.search_name_file(self.pg, path)
		elif criteria == "name_dir":
			Search.search_name_dir(self.pg, path)
		elif criteria == "hash":
			Search.search_hash(self.pg, path, None)
		elif criteria == "duplicate_file":
			Search.search_duplicate_file(self.pg, path)
		elif criteria == "duplicate_dir":
			Search.search_duplicate_dir(self.pg, path)
		elif criteria == "file_size":
			Search.search_file_size(self.pg, path)
		elif criteria == "date":
			Search.search_date(self.pg, path)
		elif criteria == "timestamp":
			Search.search_timestamp(self.pg, path)
		elif criteria == "timestamp_range":
			Search.search_timestamp_range(self.pg, path)

	# Perform on-demand hashing
	def do_hash_file(self, path: str) -> None:
		Hash.hash_file(self.pg, path)

	def do_hash_dir(self, path: str) -> None:
		Hash.hash_dir(self.pg, path)

	# Perform on-demand scraping
	def do_scrape_dir(self, args: list) -> None:
		path = self.parse_path(args[0])
		Scrape.scrape_dir(self.pg, path)

	def do_scrape_file(self, path: str) -> None:
		Scrape.scrape_file(self.pg, path)

	# Perform on-demand rescheduling
	def do_reschedule_dir(self, args: list) -> None:
		path = self.parse_path(args[0])
		frequency = args[0]

		Schedule.reschedule_dir(self.pg, path, frequency)

	# Perform on-demand rescheduling
	def do_view_scrape_schedule(self, args: list) -> None:
		path = self.parse_path(args[0])
		recursive = Util.input_parse_bool(args[1]) if len(args) > 1 else False

		rows = Schedule.view_scrape_schedule(self.pg, path, recursive)

		# TODO: Make this output a table
		for directory_control in rows:
			print(
				str(directory_control.crawl_frequency)
				+ " | " + str(directory_control.next_crawl)
				+ " | " + str(directory_control.dir_path)
			)

	def do_set_context(self, ):
		"""cd C:"""
		"""All search results are performed only within this location"""


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
