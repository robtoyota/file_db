from FileDbDAL import Pg
from Util.Config import Config
from Process import Process
import sys
from CLI import UserInterface


def server(config_file):
	# Get the processes started
	p = Process(config_file=config_file)

	# Install the postgres tables, functions, etc
	p.install_sql()

	# Reset the crawling schedules in postgres
	p.reset_schedules()

	# Run the program
	p.start_crawling()


def ui(config_file):
	# Load the config
	config = Config.load_config(config_file)
	# Connect to the DB
	with Pg(config) as pg:
		# Start the UI
		UserInterface(pg=pg, config_file=config_file).cmdloop()


if __name__ == '__main__':
	# https://codeburst.io/building-beautiful-command-line-interfaces-with-python-26c7e1bb54df

	# Defaults
	in_config_file = None
	in_program_type = "ui"

	# Use the user-supplied config file
	if len(sys.argv) > 1:
		in_config_file = sys.argv[1]

	# Determine the way the program should be run
	if len(sys.argv) > 2:
		in_program_type = sys.argv[2]

	# Call the program
	if in_program_type.lower() == "db":
		server(in_config_file)
	elif in_program_type.lower() == "ui":
		ui(in_config_file)
	else:
		ui(in_config_file)  # Default



"""
Program flow:
* Manually do an initial insert into directory_control of the root/drives to crawl.
* Retrieves a list of dirs to crawl from directory_control
* The directories get scraped for a list of files and subdirs
* The files and subdirs get inserted into their "_staging" tables
* A stored proc then merges the data from the staging tables to the respective tables.
	The staging tables act as a snapshot of the dir.
"""