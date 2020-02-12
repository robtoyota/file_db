##################################
# This is a back-burner skeleton that needs to be fleshed out for Cmd support as
# a backup on systems that do not support the more robust UIs
##################################
from cmd import Cmd
from Util.Config import Config
from API.Util import Util
from API.Hash import Hash
from API.Schedule import Schedule
from API.Scrape import Scrape
from API.Search import Search


class UserInterface(Cmd):
	# https://code-maven.com/interactive-shell-with-cmd-in-python

	# Set the "UI" elements
	prompt = "file_db> "
	intro = "Type ? to list commands"

	def __init__(self, pg, config_file: str) -> None:
		super().__init__()
		# Load the config
		self.config = Config.load_config(config_file)
		# Accept the database connection object
		self.pg = pg

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

	# Perform searches
	def do_search(self, args: str) -> None:
		try:
			criteria, path = Util.parse_args(args)
		except ValueError:
			print('***Error, missing arguments')
			return

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
	def do_scrape_dir(self, path: str) -> None:
		Scrape.scrape_dir(self.pg, path)

	def do_scrape_file(self, path: str) -> None:
		Scrape.scrape_file(self.pg, path)

	# Perform on-demand rescheduling
	def do_reschedule_dir(self, args: str) -> None:
		try:
			path, frequency = Util.parse_args(args)
		except ValueError:
			print('***Error, missing arguments')
			return

		Schedule.reschedule_dir(self.pg, path, frequency)

	# Perform on-demand rescheduling
	def do_view_scrape_schedule(self, args: str) -> None:
		try:
			# TODO: Parse this properly
			path, recursive = args.split(' ', 1)
		except ValueError:
			print('***Error, missing arguments')
			return

		recursive = Util.input_parse_bool(recursive)  # Parse the user-supplied string to bool
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
