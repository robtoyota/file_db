from Util.Config import Config
from prompt_toolkit import prompt
from FileDbDAL.Pg import Pg
from psycopg2 import OperationalError
from Server import Process

import os


class Install:
	def __init__(self, config_file):
		# Load the config
		config = Config.load_config(config_file)

		# Get the connection string, if not already configured

		print(
			f"Configure the Postgres connection. You will be prompted for the postgres hostname"
			f" (commonly 'localhost'), port (commonly 5432), database name, user, and password."
			f"\nNote: You will need to create the database and user manually, before this install"
		)
		while True:  # Repeat until successfully connected
			config['POSTGRES']['host'] = prompt('file_db install: Postgres Hostname> ')
			config['POSTGRES']['port'] = prompt('file_db install: Postgres Port> ')
			config['POSTGRES']['dbname'] = prompt('file_db install: Postgres DB Name> ')
			config['POSTGRES']['user'] = prompt('file_db install: Postgres Username> ')
			config['POSTGRES']['password'] = prompt('file_db install: Postgres Password> ', is_password=True)

			# Test the connection
			try:
				with Pg(config) as pg:
					print("Connected successfully!")
					break  # Exit the loop
			except OperationalError:  # If there was an error connecting, then try again
				print("Error: Unable to connect to Postgres. Please enter the values again.")

		Config.write_config(config, config_file)
		with Process(config_file=config_file) as p:
			# Install the postgres tables, functions, etc
			p.install_sql()
		print("Sucessfully installed the database.")

		# Add dirs to crawl
		print(
			f"\n{'='*60}"
			f"\nEnter a directory to add to the crawl schedule (crawls recursively). This will loop to allow you to enter"
			f"more directories. Leave empty (press [enter]) when done entering dirs."
			f'\nFor example "C:\\" or "/home/"'
			f"\n{'='*60}"
		)
		while True:
			# Get the dir from the user's input
			new_dir = prompt("file_db install: dir path> ")
			new_dir = new_dir.strip()

			# Break out of entering dirs
			if not new_dir.strip():
				break

			# Check if the input is a valid directory
			if not os.path.isdir(new_dir):
				print(f'=***Error: this is not a valid directory to crawl: "{new_dir}"')
				continue

			# If all is good, then load into the DB
			with Pg(config) as pg:
				with pg.cursor() as cur:
					# Load the directory into the table
					cur.execute("insert into directory (dir_path) values (%s) on conflict do nothing", (new_dir,))
					# ...And schedule it for crawling
					cur.execute("select schedule_subdirs_in_directory_control(%s);", (new_dir,))
					print(f'Added for crawling: {new_dir}')
