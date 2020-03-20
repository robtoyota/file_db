import multiprocessing as MP
import FileDbDAL
from Util.Config import Config
import time
import sys
import traceback
import random


class Process:
	def __init__(self, config_file):
		# Load the config
		self.config = Config.load_config(config_file)

		# TODO: Load all the rest of this from config

		# Set how many processes should be spawned for each task
		# TODO: Have a single process spawn multiple threads instead of multiple processes
		self.process_count = {
			'manage_crawl_dirs': 1,
			'crawl_dir': 1,
			'insert_dir_contents': 1,
			'finalize_dir_contents': 1,
			'process_db_removal_file': 1,
			'process_db_removal_directory': 1,
			'manage_hash_queue': 1,
			'hash_files': 1,
			'load_hashes': 1
		}

		# Build the queues that will be available
		self.queues = {
			'crawl_dir_queue': MP.Queue(),
			'insert_dir_contents_queue': MP.Queue(),
			'hash_files_queue': MP.Queue(),
			'load_hashes_queue': MP.Queue(),
		}

		# Set the max size of each queue, relative to the number of processes to be spawned
		self.queue_maximums = {
			'crawl_dir_queue': self.process_count['crawl_dir'] * 10000,
			'insert_dir_contents_queue': self.process_count['insert_dir_contents'] * 10000,
			'hash_files_queue': self.process_count['hash_files'] * 5000,
			'load_hashes_queue': self.process_count['load_hashes'] * 50000,  # Max limit forces a dump to DB
		}

		# Set the max seconds before a queue needs to be cleared
		self.queue_timers = {
			'manage_crawl_dirs': 5,
			'insert_dir_contents_timer': 5,
			'finalize_dir_contents_timer': 5,
			'process_db_removal_file': 1,
			'process_db_removal_directory': 1,
			'manage_hash_queue': 5,
			'load_hashes_timer': 5,
		}

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		# TODO: Perform the cleanup
		return

	def start_crawling(self):
		processes = []  # List of processes that will run the program
		try:
			### Start up the process to get the list of directories to call
			self.crawl_drives(self.queues['crawl_dir_queue'])

			### Start up the process to get the list of directories to call
			# TODO: Make sure that all of these processes get created!
			# Maintain the queue of directories to crawl
			processes += [
				MP.Process(
					target=self.manage_crawl_dirs, args=(
						self.queue_maximums,
						self.queues['crawl_dir_queue'],
						self.queue_timers['manage_crawl_dirs']
					)
				)
				for i in range(self.process_count['manage_crawl_dirs'])
			]

			# Scrape each subdirectory in the queue
			processes += [
				MP.Process(
					target=self.crawl_dir, args=(
						self.queue_maximums,
						self.queues['crawl_dir_queue'],
						self.queues['insert_dir_contents_queue'],
					)
				)
				for i in range(self.process_count['crawl_dir'])
			]

			# Insert the contents of the directories (files and subdirs)
			processes += [
				MP.Process(
					target=self.insert_dir_contents, args=(
						self.queue_maximums,
						self.queues['insert_dir_contents_queue'],
						self.queue_timers['insert_dir_contents_timer'],
					)
				)
				for i in range(self.process_count['insert_dir_contents'])
			]

			# Finalize the directory crawl within the DB
			processes += [
				MP.Process(
					target=self.finalize_dir_contents, args=(
						self.queue_timers['finalize_dir_contents_timer'],
					)
				)
				for i in range(self.process_count['finalize_dir_contents'])
			]

			# Process the removal of any file rows in the DB that are ready to be deleted
			processes += [
				MP.Process(
					target=self.process_db_removal_file, args=(
						self.queue_timers['process_db_removal_file'],
						10000,  # row_limit
					)
				)
				for i in range(self.process_count['process_db_removal_file'])
			]

			# Process the removal of any directory rows in the DB that are ready to be deleted
			processes += [
				MP.Process(
					target=self.process_db_removal_directory, args=(
						self.queue_timers['process_db_removal_directory'],
						10000,  # row_limit
					)
				)
				for i in range(self.process_count['process_db_removal_directory'])
			]

			# Get the list of files to hash
			processes += [
				MP.Process(
					target=self.manage_hash_queue, args=(
						self.queue_maximums,
						self.queues['hash_files_queue'],
						self.queue_timers['manage_hash_queue'],
					)
				)
				for i in range(self.process_count['manage_hash_queue'])
			]

			# Hash each file
			processes += [
				MP.Process(
					target=self.hash_files, args=(
						self.queue_maximums,
						self.queues['hash_files_queue'],
						self.queues['load_hashes_queue'],
					)
				)
				for i in range(self.process_count['hash_files'])
			]

			# Insert the hashes to the staging table and process them
			processes += [
				MP.Process(
					target=self.load_hashes_into_db, args=(
						self.queue_maximums,
						self.queues['load_hashes_queue'],
						self.queue_timers['load_hashes_timer'],
					)
				)
				for i in range(self.process_count['load_hashes'])
			]

			# Output debugging data
			processes += [
				MP.Process(
					target=self.output_debug, args=(
						self.queues,
					)
				)
				for i in range(1)
			]

			# Get the processes started
			# TODO: Make sure that all of these processes actually get started!
			for p in processes:
				p.start()

			# TODO: Make sure that all of these processes are running, and restart them if required.
		except KeyboardInterrupt:
			self.kill()
		finally:
			# After the processes have died...
			for p in processes:
				p.join()

		print("Done crawling")

	def install_sql(self):
		with FileDbDAL.Pg(self.config) as pg:
			# Install everything
			print("Installing database DDLs")
			FileDbDAL.Install(pg, drop_tables=False)
			print("Installs are complete")

	def reset_schedules(self):
		with FileDbDAL.Pg(self.config) as pg:
			# Clean out data
			print("Resetting all tasks...")
			FileDbDAL.SQLUtil.util_reset_process_tasks(pg)
			print("Tasks are reset")

	# Populate the initial queue from the drives
	def crawl_drives(self, crawl_dir_queue):
		print("Initializing the crawl with the drives")
		with FileDbDAL.Pg(self.config) as pg:
			# Get the list of drives
			drives = FileDbDAL.DirectoryCrawl.get_drives_to_crawl(pg)

			# Put the drives into the queue to be crawled
			for drive in drives:
				crawl_dir_queue.put(drive)

	# Manage the directory crawling
	def manage_crawl_dirs(self, queue_maximums, crawl_dir_queue, empty_queue_sleep: float = 15):
		with FileDbDAL.Pg(self.config) as pg:
			# Populate the queues for the threads
			while True:
				# Output debug info
				try:
					# If the queue is not below the threshold to be refilled, then snooze
					if crawl_dir_queue.qsize() >= (queue_maximums['crawl_dir_queue'] * 0.50):
						continue

					# Get the list of dirs to crawl, and add them to a queue
					process_id = random.randint(1, 2 ** 16)
					num_dirs = (queue_maximums['crawl_dir_queue']) - crawl_dir_queue.qsize()
					# This function retrieves the dirs from the DB and puts them in the queue, then returns rowcount
					cursor_rowcount = FileDbDAL.DirectoryCrawl.get_dirs_to_crawl(
						pg,
						crawl_dir_queue,
						process_id,
						num_dirs
					)

					# Check if there are any dirs left to crawl after this batch
					if cursor_rowcount == 0:
						# Since there are no
						time.sleep(empty_queue_sleep)

				except:  # Ugh
					print("-" * 60)
					print("Exception occurred in manage_crawl_dirs")
					print(str(sys.exc_info()))
					traceback.print_exc(file=sys.stdout)
					print("-" * 60)
				finally:
					time.sleep(0.5)

	def crawl_dir(self, queue_maximums, crawl_dir_queue, insert_dir_contents_queue):
		while True:
			try:
				# Make sure the destination queue is not full
				if insert_dir_contents_queue.qsize() >= queue_maximums['insert_dir_contents_queue']:
					time.sleep(0.5)
					continue

				# Wait until the queue has data
				if crawl_dir_queue.empty() is True:
					time.sleep(0.2)
					continue

				# Get the current directory (DirectoryCrawl object) to work with
				dc = crawl_dir_queue.get(True)

				# Is the queue complete?
				if dc == "-1":
					print("Done crawl_dir")
					break

				# Scrape the directory's contents and build the objects of metadata
				dc.scrape_dir_contents(build_objects=True)

				# Now that the contents are collected, pass it to the queue to be inserted into the DB
				insert_dir_contents_queue.put(dc)

			except:  # Ugh
				print("-" * 60)
				print("Exception occurred in crawl_dir")
				print(str(sys.exc_info()))
				traceback.print_exc(file=sys.stdout)
				print("-" * 60)

	def insert_dir_contents(self, queue_maximums, insert_dir_contents_queue, db_dump_interval):
		# Start the timer
		last_flush = time.time()

		with FileDbDAL.Pg(self.config) as pg:
			while True:
				try:
					time.sleep(0.2)  # Give the queue time to fill up

					# Wait until the queue has data
					if insert_dir_contents_queue.empty() is True:
						continue

					# Check to see if it is time to flush the data to the DB
					if (
						time.time() - last_flush < db_dump_interval  # Has enough time passed?
						and insert_dir_contents_queue.qsize() < queue_maximums['insert_dir_contents_queue']  # Queue at limit?
					):
						continue  # If it is not time to flush the data to the DB

					# Reset the timer
					last_flush = time.time()
					FileDbDAL.DirectoryCrawl.stage_dir_contents(pg, insert_dir_contents_queue)
				except:  # Ugh
					print("-" * 60)
					print("Exception occurred in insert_dir_contents")
					print(str(sys.exc_info()))
					traceback.print_exc(file=sys.stdout)
					print("-" * 60)

	def finalize_dir_contents(self, db_dump_interval: float) -> None:
		# Start the timer
		last_flush = time.time()

		with FileDbDAL.Pg(self.config) as pg:
			while True:
				time.sleep(0.2)  # Give the staging tables time to fill up

				try:
					if time.time() - last_flush < db_dump_interval:  # Has enough time passed?
						continue

					# Reset the timer
					last_flush = time.time()
					# Server the dir's contents that were staged
					FileDbDAL.DirectoryCrawl.process_staged_dir_contents(pg)
				except Exception:  # Ugh
					print("-" * 60)
					print("Exception occurred in finalize_dir_contents")
					print(str(sys.exc_info()))
					traceback.print_exc(file=sys.stdout)
					print("-" * 60)
					raise

	def process_db_removal_file(self, db_dump_interval: float, row_limit: int = 10000) -> None:
		# Start the timer
		last_flush = time.time()

		with FileDbDAL.Pg(self.config) as pg:
			while True:
				time.sleep(0.2)  # Give the staging tables time to fill up

				try:
					if time.time() - last_flush < db_dump_interval:  # Has enough time passed?
						continue

					# Reset the timer
					last_flush = time.time()
					# Server the dir's contents that were staged
					FileDbDAL.DirectoryCrawl.process_db_removal_file(pg, row_limit)
				except Exception:  # Ugh
					print("-" * 60)
					print("Exception occurred in process_db_removal_file")
					print(str(sys.exc_info()))
					traceback.print_exc(file=sys.stdout)
					print("-" * 60)
					raise

	def process_db_removal_directory(self, db_dump_interval: float, row_limit: int = 10000) -> None:
		# Start the timer
		last_flush = time.time()

		with FileDbDAL.Pg(self.config) as pg:
			while True:
				time.sleep(0.2)  # Give the staging tables time to fill up

				try:
					if time.time() - last_flush < db_dump_interval:  # Has enough time passed?
						continue

					# Reset the timer
					last_flush = time.time()
					# Server the dir's contents that were staged
					FileDbDAL.DirectoryCrawl.process_db_removal_directory(pg, row_limit)
				except Exception:  # Ugh
					print("-" * 60)
					print("Exception occurred in process_db_removal_directory")
					print(str(sys.exc_info()))
					traceback.print_exc(file=sys.stdout)
					print("-" * 60)
					raise

	# Manage the file hashing queue
	def manage_hash_queue(self, queue_maximums, hash_files_queue, empty_queue_sleep):
		with FileDbDAL.Pg(self.config) as pg:
			# Populate the queues for the threads
			while True:
				# Output debug info
				try:
					# If the queue is not below the threshold to be refilled, then snooze
					if hash_files_queue.qsize() >= (queue_maximums['hash_files_queue'] * 0.50):
						continue

					# Get the list of files to hash, and add them to a queue
					process_id = random.randint(1, 2 ** 16)
					num_hashes = (queue_maximums['hash_files_queue']) - hash_files_queue.qsize()
					cursor_rowcount = FileDbDAL.DirectoryCrawl.get_files_to_hash(
						pg,
						hash_files_queue,
						process_id,
						num_hashes,
					)

					# Check if there are any dirs left to crawl after this batch
					if cursor_rowcount == 0:
						# Since there are no
						time.sleep(empty_queue_sleep)
				except:  # Ugh
					print("-" * 60)
					print("Exception occurred in manage_hash_queue")
					print(str(sys.exc_info()))
					traceback.print_exc(file=sys.stdout)
					print("-" * 60)
				finally:
					time.sleep(0.5)

			# dir_proc.join()
			pg.close()

	def hash_files(self, queue_maximums, hash_files_queue, load_hashes_queue):
		while True:
			try:
				# Make sure the destination queue is not full
				if load_hashes_queue.qsize() >= queue_maximums['load_hashes_queue']:
					time.sleep(0.5)
					continue

				# Wait until the queue has data
				if hash_files_queue.empty() is True:
					time.sleep(0.2)
					continue

				# Get the current directory (DirectoryCrawl object) to work with
				hash = hash_files_queue.get(True)

				# Is the queue complete?
				if hash == "-1":
					print("Done scraping")
					break

				# Scrape the directory's contents and build the objects of metadata
				hash.perform_hash()

				# Now that the hash are colelcted, pass it to the queue to be inserted into the DB
				if hash.md5_hash is not None or hash.sha1_hash is not None:  # Make sure it found a hash
					load_hashes_queue.put(hash)

			except:  # Ugh
				print("-" * 60)
				print("Exception occurred in hash_files")
				print(str(sys.exc_info()))
				traceback.print_exc(file=sys.stdout)
				print("-" * 60)

	def load_hashes_into_db (self, queue_maximums, load_hashes_queue, db_dump_interval):
		# Start the timer
		last_flush = time.time()

		with FileDbDAL.Pg(self.config) as pg:
			while True:
				try:
					time.sleep(0.2)  # Give the queue time to fill up

					# Wait until the queue has data
					if load_hashes_queue.empty() is True:
						continue

					# Check to see if it is time to flush the data to the DB
					if (
							time.time() - last_flush < db_dump_interval  # Has enough time passed?
							and load_hashes_queue.qsize() < queue_maximums['load_hashes_queue']  # Queue at limit?
						):
						continue  # If it is not time to flush the data to the DB

					# Reset the timer
					last_flush = time.time()

					# If all is ready to flush to the DB, then perform the dump
					FileDbDAL.DirectoryCrawl.stage_hashes(pg, load_hashes_queue)

					# Finally, process the staged hashes
					FileDbDAL.DirectoryCrawl.process_staged_hashes(pg)

				except:  # Ugh
					print("-" * 60)
					print("Exception occurred in load_hashes_into_db")
					print(str(sys.exc_info()))
					traceback.print_exc(file=sys.stdout)
					print("-" * 60)

	def output_debug(self, queues):
		while True:
			time.sleep(0.01)
			output = []
			for queue_name, queue in queues.items():
				output.append(f"[{queue_name.replace('_queue', '')}: {queue.qsize()}]")
			print(" | ".join(output), end='\r')

	# Manage the FileHandler
	def manage_copy_file_queue(self, queue_maximums, file_copy_list_queue):
		# Get the list of files (FileHandler.get_files_to_copy())
		# Feed into file_copy_list_queue
		pass

	def perform_copy_file(self, queue_maximums, file_copy_list_queue, file_copy_finalize_queue):
		# Call CopyFile.perform_copy()
		# Feed into file_copy_finalize_queue
		# Todo: error/warning handling (GUI)
		pass

	def finalize_copy_file(self, queue_maximums, file_copy_finalize_queue):
		# Use file_copy_finalize_queue to write back to the DB to delete the rows
		# Delete rows in the file table when a file got moved (and copy to file_deleted - turn into a trigger?)
		pass
