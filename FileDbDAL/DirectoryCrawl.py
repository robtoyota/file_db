from FileDbDAL.Directory import Directory
from FileDbDAL.File import File
from FileDbDAL.Hash import Hash
import os
import psycopg2.extras
import traceback
import sys
import time
from datetime import datetime
import multiprocessing as MP


class DirectoryCrawl:
	def __init__(self, db_row: dict = None):
		self.next_crawl_seconds = None  # next_crawl_seconds == None: default crawl_frequency

		# Default values for the vars from the DB
		self.id = ""
		self.dir_id = None
		self.dir_path = None
		self.file_count = None
		self.subdir_count = None
		self.next_crawl = None
		self.crawl_frequency = None
		self.process_assigned_on = None
		self.last_crawled = None
		self.last_active = None
		self.inserted_on = None
		self.updated_on = None

		# Populate from the DB row
		if db_row:
			self.populate_from_db_row(db_row)

		# Delete rows from the file/directory table if not found in the staging table during processing
		self.delete_missing = True

		# Data for the scraping queue
		self.crawled_on = None
		self.dir_not_found = False

		# Vars to hold scraping content
		self.subdir_names = []  # Name only
		self.file_names = []  # Name only
		self.files = {}  # Full File() instance
		self.subdirs = {}  # Full Directory instance

	def populate_from_db_row(self, db_row: dict) -> bool:
		self.id = db_row['id'] if 'id' in db_row else None
		self.dir_id = db_row['dir_id'] if 'dir_id' in db_row else None
		self.dir_path = db_row['dir_path'] if 'dir_path' in db_row else None
		self.file_count = db_row['file_count'] if 'file_count' in db_row else None
		self.subdir_count = db_row['subdir_count'] if 'subdir_count' in db_row else None
		self.next_crawl = db_row['next_crawl'] if 'next_crawl' in db_row else None
		self.crawl_frequency = db_row['crawl_frequency'] if 'crawl_frequency' in db_row else None
		self.process_assigned_on = db_row['process_assigned_on'] if 'process_assigned_on' in db_row else None
		self.last_crawled = db_row['last_crawled'] if 'last_crawled' in db_row else None
		self.last_active = db_row['last_active'] if 'last_active' in db_row else None
		self.inserted_on = db_row['inserted_on'] if 'inserted_on' in db_row else None
		self.updated_on = db_row['updated_on'] if 'updated_on' in db_row else None

	def scrape_dir_contents(self, build_objects: bool = False) -> None:
		try:
			# Get the list of file and subdir names
			root, self.subdir_names, self.file_names = next(os.walk(self.dir_path))

			# Build the File and Directory objects, if necessary:
			if build_objects:
				for file_name in self.file_names:
					self.files[file_name] = File(file_name, self.dir_path)
					self.files[file_name].scrape_metadata()
					self.files[file_name].dir_id = self.dir_id

				for subdir_name in self.subdir_names:
					self.subdirs[subdir_name] = Directory(os.path.join(self.dir_path, subdir_name))
					self.subdirs[subdir_name].scrape_metadata()
		except StopIteration:  # If os.walk fails (unreadable dir)
			self.dir_not_found = True

		# Get the counts
		self.subdir_count = len(self.subdir_names)  # Will default to 0
		self.file_count = len(self.file_names)  # Will default to 0

		# Mark when the crawling completed
		self.crawled_on = datetime.now()

	def iter_content_files(self):
		for name, file in self.files.items():
			# Make sure that the file name can be encoded to UTF-8 for DB insertion
			try:
				file.name.encode('utf8')
			except UnicodeEncodeError:
				# print(
				# 	f"Error: File name cannot be UTF-8 encoded for database insertion. "
				# 	f"File will not be recorded: {file.name}"
				# )
				continue

			# Yield the file's dictionary
			yield file.staging_table_dict()

	def iter_content_subdirs(self):
		for name, subdir in self.subdirs.items():
			# Make sure that the subdir name can be encoded to UTF-8 for DB insertion
			try:
				subdir.dir_path.encode('utf8')
			except UnicodeEncodeError:
				# print(
				# 	f"Error: Subdir name cannot be UTF-8 encoded for database insertion. The directory and its "
				# 	f"children will not be crawled: {subdir.dir_path}"
				# )
				continue

			# Yield the subdirectory's dictionary
			yield subdir.staging_table_dict()

	@staticmethod
	def iter_hashes_queue(load_hashes_queue):
		# Entries continue to get added to this queue, only process current items
		qsize = load_hashes_queue.qsize()
		i = 0

		# Iterate through the queue until the limit is reached, or there are none left
		while i < qsize and not load_hashes_queue.empty():
			i += 1
			hash = load_hashes_queue.get(True)
			yield hash.staging_table_dict()

	def insert_new_drive(self, pg, drive):
		# Populate the values
		self.dir_path = drive

		# Insert the values
		cur = pg.cursor()
		cur.execute("""
			insert into
				drive
				(drive)
			values
				(%s)
			on conflict
				do nothing;
			""",
			(self.dir_path,)
		)

		pg.commit()
		cur.close()

	@staticmethod
	def iter_multiple_subdirs(crawled_dirs):
		for dc in crawled_dirs:
			for subdir in dc.iter_content_subdirs():
				yield subdir

	@staticmethod
	def iter_insert_dir_contents_dir_finalize_queue(crawled_dirs):
		for dc in crawled_dirs:
			yield {'dir_path': dc.dir_path, 'delete_missing': dc.delete_missing}

	@staticmethod
	def iter_insert_dir_contents_files_queue(crawled_dirs):
		for dc in crawled_dirs:
			for file in dc.iter_content_files():
				yield file

	@staticmethod
	def iter_insert_dir_contents_files_finalize_queue(crawled_dirs):
		for dc in crawled_dirs:
			yield {'dir_id': dc.dir_id, 'delete_missing': dc.delete_missing}

	@staticmethod
	def iter_insert_dir_control_stage_queue (crawled_dirs):
		for dc in crawled_dirs:
			yield {
				'dir_id': dc.dir_id,
				'dir_path': dc.dir_path,
				'crawled_on': dc.crawled_on,
				'file_count': dc.file_count,
				'subdir_count': dc.subdir_count,
				'dir_not_found': dc.dir_not_found,
			}

	@staticmethod
	def stage_dir_contents(pg, insert_dir_contents_queue, page_size=1000):
		# Pull the objects out of the queue and into a list to get iterated over repeatedly
		crawled_dirs = []
		# Entries continue to get added to this queue, so only process the current items
		qsize = insert_dir_contents_queue.qsize()
		i = 0

		# Iterate through the queue until the limit is reached
		# while i < qsize and not insert_dir_contents_queue.empty():  # !! empty() lags behind qsize(), and gives false True
		while i < qsize:
			i += 1
			try:
				dc = insert_dir_contents_queue.get(True, 1)
				crawled_dirs.append(dc)
			except Empty:  # If the queue times out trying to pull more values, then just process what got pulled
				continue

		# Is there anything in the list of dirs?
		if not len(crawled_dirs):
			return

		# Dump the crawled data into the staging tables
		# Very good overview of inserting in bulk: https://hakibenita.com/fast-load-data-python-postgresql
		try:
			with pg.cursor() as cur:
				# Insert the subdirs into the dir staging table
				# TODO: Turn this into a stored proc
				psycopg2.extras.execute_values(
					cur,
					"""
					insert into directory_stage
						(dir_path, ctime, mtime, inserted_by_process_id) 
					values 
						%s
					on conflict on constraint directory_stage_pkey do nothing;
					""",
					(
						(
							subdir['dir_path'],
							subdir['ctime'],
							subdir['mtime'],
							1,  # Unneeded
						) for subdir in DirectoryCrawl.iter_multiple_subdirs(crawled_dirs)
					),
					page_size=page_size
				)

				# Queue the staged subdirs up to be processed
				# TODO: Turn this into a stored proc
				psycopg2.extras.execute_values(
					cur,
					"""
					insert into directory_stage_process
						(parent_dir_path, delete_missing) 
					values 
						%s
					on conflict on constraint directory_stage_process_pkey do nothing;
					""",
					(
						(
							stage['dir_path'],
							stage['delete_missing'],
						) for stage in DirectoryCrawl.iter_insert_dir_contents_dir_finalize_queue(crawled_dirs)
					),
					page_size=page_size
				)

				# Insert the files into the file staging table
				# Todo: Make this a stored proc
				psycopg2.extras.execute_values(
					cur,
					"""
						insert into file_stage
						(name, dir_id, size, ctime, mtime, atime, inserted_by_process_id) 
						values %s
						on conflict on constraint file_stage_pkey do nothing;
					""",
					(
						(
							file['name'],
							file['dir_id'],
							file['size'],
							file['ctime'],
							file['mtime'],
							file['atime'],
							1,  # Unneeded
						) for file in DirectoryCrawl.iter_insert_dir_contents_files_queue(crawled_dirs)
					),
					page_size=page_size
				)

				# Queue the staged files to be processed
				# Todo: Make this a stored proc
				psycopg2.extras.execute_values(
					cur,
					"""
						insert into file_stage_process
						(dir_id, delete_missing) 
						values %s
						on conflict on constraint file_stage_process_pkey do nothing;
					""",
					(
						(
							stage['dir_id'],
							stage['delete_missing'],
						) for stage in DirectoryCrawl.iter_insert_dir_contents_files_finalize_queue(crawled_dirs)
					),
					page_size=page_size
				)

				# Stage the crawled directory to be marked as crawled
				# Todo: Make this a stored proc
				psycopg2.extras.execute_values(
					cur,
					"""
						insert into directory_control_process
						(dir_id, dir_path, crawled_on, file_count, subdir_count, dir_not_found) 
						values %s
						on conflict on constraint directory_control_process_pkey do nothing;
					""",
					(
						(
							stage['dir_id'],
							stage['dir_path'],
							stage['crawled_on'],
							stage['file_count'],
							stage['subdir_count'],
							stage['dir_not_found'],
						) for stage in DirectoryCrawl.iter_insert_dir_control_stage_queue(crawled_dirs)
					),
					page_size=page_size
				)

		except:  # Ugh
			print(str(sys.exc_info()))
			traceback.print_exc(file=sys.stdout)
		# print(f"*- Staged ({round((time.time() - start_time), 3)})): {self.dir_path}")

	@staticmethod
	def process_staged_dir_contents(pg):
		with pg.cursor() as cur:
			try:
				# Upsert into directory and schedule the crawling of the subdirs
				cur.execute("select process_staged_dirs()")
				# Upsert into file and schedule the hashing of the files
				cur.execute("select process_staged_files()")
				# Mark the directories as crawled, so that they can be rescheduled and crawled again
				cur.execute("select mark_dirs_crawled()")
			except:  # Ugh
				print(str(sys.exc_info()))
				traceback.print_exc(file=sys.stdout)

	@staticmethod
	def stage_hashes(pg, load_hashes_queue, page_size=1000):
		# Very good overview of inserting in bulk: https://hakibenita.com/fast-load-data-python-postgresql

		# Insert the hashes from the queue
		try:
			with pg.cursor() as cur:
				psycopg2.extras.execute_values(
					cur,
					"""
					insert into hash_stage
					(file_id, md5_hash, md5_hash_time, sha1_hash, sha1_hash_time)
					values %s
					on conflict do nothing;
					""",
					(
						(
							hash['file_id'],
							hash['md5_hash'],
							hash['md5_hash_time'],
							hash['sha1_hash'],
							hash['sha1_hash_time']
						) for hash in DirectoryCrawl.iter_hashes_queue(load_hashes_queue)
					),
					page_size=page_size
				)
		except:  # Ugh
			print(str(sys.exc_info()))
			traceback.print_exc(file=sys.stdout)

	@staticmethod
	def process_staged_hashes(pg):
		with pg.cursor() as cur:
			cur.execute("select process_staged_hashes()")

	def insert_new_directory_to_crawl(self, pg, dir_path, crawl_frequency=(60*60*24*1)):
		# Populate the input values
		self.dir_path = dir_path
		self.crawl_frequency = crawl_frequency
		if self.file_count is None or self.subdir_count is None:
			self.scrape_dir_contents()

		# Insert the values
		cur = pg.cursor()
		cur.execute("""
			insert into
				directory_control
				(dir_path, dir_id, file_count, subdir_count, crawl_frequency)
			values
				(%s, %s, %s, %s, %s)
			on conflict on constraint directory_control_dir_path_pkey
				do nothing  -- If it's already scheduled, then don't meddle with its customized schedule.
			""",
			(
				self.dir_path,
				self.dir_id,
				self.file_count,
				self.subdir_count,
				self.crawl_frequency
			)
		)

		pg.commit()
		cur.close()

	@staticmethod
	def get_drives_to_crawl(pg, limit=1000):
		with pg.cursor() as cur:
			dirs = []
			# Get the list of directories to crawl, that are not already in the control table
			cur.execute("""
				select
					d.drive 
				from 
					drive d 
					left join directory_control dc
						on (d.drive=dc.dir_path)
				where
					dc is null
				order by 
					d.drive 
				limit 
					%s
				""",
				(limit,)
			)
			# Populate the dirs list with the paths:
			for row in cur:
				d = DirectoryCrawl(db_row=row)
				dirs.append(d)

		# Return the list of paths
		return dirs

	@staticmethod
	def get_dirs_to_crawl(pg, crawl_dir_queue, process_id: int, limit: int=10) -> int:
		# Get the dirs
		try:
			with pg.cursor() as cur:
				cur.execute("select dir_path, last_crawled, dir_id from get_dirs_to_crawl(%s, %s);", (process_id, limit))
				# Populate the dirs list with the paths:
				dirs = []
				d = None
				# print(f"Crawling {cur.rowcount} dirs")
				for row in cur:
					# Build the Directory object

					# Build the new directory object
					d = DirectoryCrawl(db_row=row)

					# Add to the directories to be returned
					crawl_dir_queue.put(d)

				return cur.rowcount
		except:  # Ugh:
			print(str(sys.exc_info()))
			traceback.print_exc(file=sys.stdout)
			return 0

	@staticmethod
	def get_files_to_hash(pg, hash_files_queue, process_id: int, limit: int = 10) -> int:
		# Get the dirs
		try:
			with pg.cursor() as cur:
				cur.execute("select file_id, file_path from get_files_to_hash(%s, %s);", (process_id, limit))
				# Populate the dirs list with the paths:
				hashes = []
				for row in cur:
					# Build the Directory object

					# Build the new directory object
					h = Hash(
						file_id=row['file_id'],
						file_path=row['file_path'],
					)

					# Add to the directories to be returned
					hash_files_queue.put(h)

				return cur.rowcount
		except:  # Ugh
			print(str(sys.exc_info()))
			traceback.print_exc(file=sys.stdout)
			return -1

	@staticmethod
	def install_tables(pg, drop_tables):
		cur = pg.cursor()

		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists drive cascade;")

		cur.execute("""
			create table if not exists drive
			(
				id 					serial unique,
				drive				text unique,			-- Eg: "C:/"
				assigned_process_id	int default 0 not null,	-- Python process thread number
				process_assigned_on	timestamp default null,	-- When was this assigned to be crawled?
				inserted_on 		timestamp default now(),
				primary key(id)
			);
		""")

		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists directory_control cascade;")

		cur.execute("""
			create table if not exists directory_control
			(
				dir_path			text not null,
				dir_id				int not null,
				file_count			int not null default 0,
				subdir_count		int not null default 0,
				next_crawl			timestamp not null default now(),
				crawl_frequency		int not null default 86400, -- 60*60*24
				assigned_process_id	int not null default 0,	-- Python process thread number
				process_assigned_on	timestamp default null,	-- When was this assigned to be crawled?
				last_crawled		timestamp default null,
				last_active			timestamp default null,
				dir_missing			boolean default false,  -- If dir cannot be found when trying to scrape it
				inserted_on 		timestamp not null default now(),
				primary key(dir_path)
			);
		""")

		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists hash_control cascade;")

		cur.execute("""
			create table if not exists hash_control
			(
				file_id				int not null,
				mtime				timestamp,
				file_size			numeric(18, 6), -- In MBs
				process_assigned_on	timestamp default null,	-- When was this assigned to be crawled?
				file_missing		boolean default false,  -- If file cannot be found when trying to hash
				inserted_on 		timestamp not null default now(),
				primary key(file_id)
			);
		""")

		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists directory_control_process cascade;")

		cur.execute("""
			create unlogged table if not exists directory_control_process
			(
				dir_id 			int not null,
				dir_path		text not null,
				crawled_on 		timestamp not null,
				file_count		int default 0,
				subdir_count	int default 0,	
				dir_not_found	boolean default false,
				inserted_on	timestamp not null default now(),
				primary key(dir_path)
			);
		""")

		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists file_db_removal_staging cascade;")

		cur.execute("""
			create unlogged table if not exists file_db_removal_staging
			(
				file_id 	int,
				inserted_on	timestamp not null default now(),
				primary key(file_id)
			);
		""")

		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists directory_db_removal_staging cascade;")

		cur.execute("""
			create unlogged table if not exists directory_db_removal_staging
			(
				dir_id 			int,
				delete_subdirs	boolean default false,
				inserted_on		timestamp not null default now(),
				primary key(dir_id)
			);
		""")

		pg.commit()
		cur.close()

	@staticmethod
	def install_indexes(pg):
		cur = pg.cursor()
		cur.execute("""
			create index if not exists drive_drive on drive (drive);
			create index if not exists drive_assigned_process_id on drive (assigned_process_id);
			
			create index if not exists directory_control_dir_id on directory_control (dir_id);
			create index if not exists directory_control_dir_path on directory_control (dir_path);
			create index if not exists directory_control_next_crawl on directory_control (next_crawl);
			create index if not exists directory_control_last_crawled on directory_control (last_crawled);
			create index if not exists directory_control_last_active on directory_control (last_active);
			create index if not exists directory_control_assigned_process_id on directory_control (assigned_process_id);
			create index if not exists directory_control_inserted_on on directory_control (inserted_on);
			
			create index if not exists hash_control_file_size on hash_control (file_size);
			create index if not exists hash_control_mtime on hash_control (mtime);
			create index if not exists hash_control_inserted_on on hash_control (inserted_on);
			
			create index if not exists file_db_removal_staging_inserted_on on file_db_removal_staging (inserted_on);
			create index if not exists directory_db_removal_staging_inserted_on on directory_db_removal_staging (inserted_on);
		""")
		pg.commit()
		cur.close()

	@staticmethod
	def install_pg_functions(pg):
		with pg.cursor() as cur:
			# get_dirs_to_crawl
			cur.execute(""" 
				create or replace function get_dirs_to_crawl
				(
					_process_id int,
					_row_limit int
				) 
				returns table 
				(
					dir_path text, 
					dir_id int, 
					last_crawled timestamp
				)
				as $$
				begin
					return query
					with dir_list as (  -- Identify the directories to crawl
						select d.dir_id
						from directory_control d
						where
							d.next_crawl < now()
							and d.process_assigned_on is null
						order by
							(
								extract(epoch from now() - d.next_crawl)/(60*60) -- Number of hours since it was due to crawl
								+ round(d.file_count/100)
								+ round(d.subdir_count/100)
							)
						limit _row_limit
					),
					dc_upd as (  -- Claim the directories for crawling
						update directory_control dc
						set
							process_assigned_on = now()
						from dir_list dl
						where dc.dir_id=dl.dir_id
						returning
							dc.dir_path, dc.dir_id, dc.last_crawled, dc.next_crawl
					)
					-- Return the list of directories to crawl
					select 
						dc.dir_path, dc.dir_id, dc.last_crawled
					from dc_upd dc
					order by
						dc.next_crawl asc;
				end;
				$$ LANGUAGE plpgsql;
			""")

			# get_files_to_hash
			cur.execute("""
				create or replace function get_files_to_hash
				(
					_process_id int,
					_row_limit int
				) 
				returns table 
				(
					file_path text, 
					file_id int,
					mtime timestamp
				)
				as $$
				begin
					return query
					with file_list as (  -- Get the list of files to hash
						select
							f.file_id
						from
							hash_control f
						where
							f.process_assigned_on is null
							-- and f.file_size > 0
							-- and f.file_size between 0.5 and 2500
						order by
							f.file_size asc  -- Get the "fastest to hash" (smallest) files
							-- f.file_size desc  -- Get the "slowest to hash" (largest) files
							-- ,f.mtime asc  -- Get the "most stable" (last changed longest ago) files
						limit _row_limit
					),
					upd as (  -- Claim the files to hash
						update
							hash_control hc
						set
							process_assigned_on = now()
						from
							file_list fl
						where
							hc.file_id = fl.file_id
						returning
							hc.file_id, hc.mtime
					)
					-- Return the list of directories to crawl
					select 
						f.full_path, upd.file_id, upd.mtime
					from
						upd 
						join vw_file_detail f  -- The list of files
							on (upd.file_id=f.id);
				end;
				$$ LANGUAGE plpgsql;
			""")

			# stage_hashes
			# cur.execute("""
			# 	create or replace function stage_hashes
			# 	(
			# 		_file_id int,
			# 		_md5_hash text,
			# 		_md5_hash_time timestamp,
			# 		_sha1_hash text,
			# 		_sha1_hash_time timestamp
			# 	)
			# 	returns boolean
			# 	as $$
			# 	begin
			# 		with del as (
			# 			delete from hash_control
			# 			where file_id=_file_id
			# 		)
			# 		insert into hash
			# 		(file_id, md5_hash, md5_hash_time, sha1_hash, sha1_hash_time)
			# 		values
			# 		(_file_id, _md5_hash, _md5_hash_time, _sha1_hash, _sha1_hash_time)
			# 		on conflict on constraint hash_file_id_key do nothing;
			#
			# 		return true;
			# 	end;
			# 	$$ LANGUAGE plpgsql;
			# """)

			# process_staged_hashes
			cur.execute("""
				create or replace function process_staged_hashes()
				returns boolean
				as $$
				begin
					with stage as (
						delete from hash_stage
						returning file_id, md5_hash, md5_hash_time, sha1_hash, sha1_hash_time
					),
					del as (
						delete from hash_control hc
						using stage s
						where hc.file_id=s.file_id
					)
					insert into hash
					(file_id, md5_hash, md5_hash_time, sha1_hash, sha1_hash_time)
					select file_id, md5_hash, md5_hash_time, sha1_hash, sha1_hash_time
					from stage
					on conflict on constraint hash_file_id_key do nothing;
	
					return true;
				end;
				$$ LANGUAGE plpgsql;
			""")

			# process_staged_files
			cur.execute("""
				create or replace function process_staged_files()
				returns boolean
				as $$
				begin
					-- Move (delete) files from staging and upsert into the file table
					-- !! IMPORTANT: Add all column names to all 5 sections below: DELETE, INSERT, SELECT, UPDATE, and WHERE
					with stg_process as (  -- Work with the rows in the staging process table
						delete from file_stage_process
						returning dir_id, delete_missing
					),
					stg as (  -- Move rows out of the staging table
						delete from file_stage fs
						using stg_process s
						where fs.dir_id = s.dir_id
						returning
							fs.name, fs.dir_id, fs.size, fs.ctime, fs.mtime, fs.atime
					),
					del as (  -- Delete files that are not in the staging table, meaning they were not found during the scrape.
						insert into file_db_removal_staging (file_id)  -- This staging table gets processed separately to perform the delete.
						select distinct f.id
						from
							file f
							join stg_process s
								on (f.dir_id = s.dir_id)
						where 
							s.delete_missing is true  -- Only delete missing files if required
							and not exists (  -- Is this file listed in the staging table?
								select from stg 
								where 
									f.dir_id=stg.dir_id 
									and f.name=stg.name
							)
						on conflict on constraint file_db_removal_staging_pkey
							do nothing
					),
					file_ins as (  -- Insert the rows into main table
						insert into file as f
							(name, dir_id, size, ctime, mtime, atime)
						select
							s.name, s.dir_id, s.size, s.ctime, s.mtime, s.atime
						from
							stg s
						on conflict on constraint file_pkey do
							update set 
								updated_on = now(),
								size = excluded.size,
								ctime = excluded.ctime,
								mtime = excluded.mtime,
								atime = excluded.atime
							where  -- Don't do empty updates
								f.size <> excluded.size
								or f.ctime <> excluded.ctime
								or f.mtime <> excluded.mtime
								or f.atime <> excluded.atime
						returning
							f.id, f.mtime, f.size
					)
					-- Schedule the new file for hashing (same as schedule_files_in_hash_control())
					insert into hash_control as t
						(file_id, mtime, file_size)
					select
						fi.id, fi.mtime, fi.size
					from
						file_ins fi
					where
						not exists (
							select from hash h
							where
								h.file_id=fi.id
						)
					on conflict on constraint hash_control_pkey do 
						update set
							mtime = excluded.mtime
						where
							t.mtime <> excluded.mtime;
					
					return true;
				end;
				$$ LANGUAGE plpgsql;
			""")

			# process_staged_dirs
			cur.execute("""
				create or replace function process_staged_dirs()
				returns boolean
				as $$
				begin
					-- Move (delete) subdirs from staging and upsert into the directory table
					-- !! IMPORTANT: Add all column names to all 5 sections below: DELETE, INSERT, SELECT, UPDATE, and WHERE
					with stg_process as (  -- Work with the rows in the staging process table
						delete from directory_stage_process
						returning parent_dir_path, delete_missing
					),
					stg as (  -- Move rows out of the staging table
						delete from directory_stage ds
						using stg_process s
						where basepath(ds.dir_path) = s.parent_dir_path
						returning
							dir_path, ctime, mtime
					),
					del as (  -- Delete dirs that are not in the staging table, meaning they were not found during the scrape. 
						delete from directory d
						using stg_process s
						where 
							basepath(d.dir_path) = s.parent_dir_path
							and s.delete_missing is true  -- Only delete missing files if required
							and not exists (  -- Is this dir listed in the staging table?
								select from stg 
								where d.dir_path=stg.dir_path 
							)
						returning
							d.id, d.dir_path
					),
					/*
					del_child as (  -- Delete all the subdirs "recursively" of any missing dirs
						delete from directory d
						using del
						where 
							d.dir_path like replace(del.dir_path, '\', '\\') || '%'  -- Grab all children
						returning
							d.id
					),
					del_files as (  -- Delete the files inside the deleted dirs and subdirs
						delete from file f
						using (  -- Get the list of dir_id values from the delete CTEs
								select id from del
								union
								select id from del_child
							) as d
						where f.dir_id=d.id
					),
					*/
					dir_ins as (  -- Insert the rows into main table
						insert into directory as t 
							(dir_path, ctime, mtime)
						select dir_path, ctime, mtime
						from stg
						on conflict on constraint directory_dir_path_key do
							update set 
								updated_on = now(),
								ctime = excluded.ctime,
								mtime = excluded.mtime
							where  -- Don't do empty updates 
								t.ctime <> excluded.ctime
								or t.mtime <> excluded.mtime
						returning
							id, dir_path
					)
					-- Schedule the new directory for crawling (same as schedule_subdirs_in_directory_control())
					insert into directory_control as t
						(dir_id, dir_path)
					select dir_ins.id, dir_ins.dir_path
					from dir_ins 
					on conflict on constraint directory_control_pkey do 
						update set
							dir_id = excluded.dir_id  -- In case the dir_id has changed for the dir_path
							-- Don't update/reschedule any existing directories.
						where  -- Don't do empty updates
							t.dir_id <> excluded.dir_id;
					
					return true;
				end;
				$$ LANGUAGE plpgsql;
			""")

			# mark_dirs_crawled
			cur.execute("""
				create or replace function mark_dirs_crawled()
				returns boolean
				as $$
				begin
					with stg as (  -- Clear out the staging table and get a list of the dirs to work with
						delete from directory_control_process dcs
						where
							-- Make sure there are no outstanding files staged
							not exists (
								select from file_stage fs
								where dcs.dir_id=fs.dir_id
							)
							-- Make sure there are no outstanding subdirs staged
							and not exists (
								select from directory_stage ds
								where dcs.dir_path=basepath(ds.dir_path)
							)
						returning
							dcs.dir_id, dcs.dir_path, dcs.crawled_on, dcs.file_count, dcs.subdir_count, dcs.dir_not_found
					),
					/*
					schedule_parent as (  -- Schedule the parent dir of any missing dirs. Missing dir indicates a change in the parent.
						update directory_control dc
						set next_crawl = '1900-01-01'  -- Scrape the parent immediately, to avoid wasting time scraping other deleted dirs.
						from stg
						where
							dc.dir_path = basename(stg.dir_path)
							and stg.dir_not_found = true
						returning
							dc.dir_id as parent_id, stg.dir_id
					),
					set_dir_missing as (  -- Update the dir_missing values of children
						update directory_control dc
						set
							dir_missing = case
								-- If the dir was not found, then mark it as not found. Unless this dir has no parent
								when (
									stg.dir_not_found  -- dir not found?
									and not (dc.dir_path=stg.dir_path and pnt.parent_id is not null)  -- And not the root parent
								) then 
									true
								else 
									false
								end
						from
							stg
							left join schedule_parent pnt
								on (stg.dir_id=pnt.dir_id)
						where
							dc.dir_path like replace(stg.dir_path, '\', '\\') || '%'  -- Scraped dir and its children
							and dc.dir_missing <> case  -- Don't perform empty updates. 
								when (
									stg.dir_not_found
									and not (dc.dir_path=stg.dir_path and pnt.parent_id is not null)
								) then 
									true
								else 
									false
								end
					),
					*/
					schd as (  -- Get the new crawling frequency for the dirs
						-- For dirs that exist: 
						select dir_id, new_frequency
						from crawl_frequency_last_ctime_calculate(
							30::float, -- _divide_seconds,
							round(60*60*0.25)::int, -- _min_frequency,
							round(60*60*24*7)::int, -- _max_frequency,
							array(select dir_id from stg where dir_not_found=false)::int[] -- _dir_id
						)
						-- For dirs that don't exist, try again later
						union all
						select dir_id, (60*60*24*1) as new_frequency
						from stg
						where dir_not_found = true
					)
					-- Update the crawl frequency of the dirs that just got crawled
					update directory_control dc
					set
						last_crawled = stg.crawled_on,
						crawl_frequency = coalesce(schd.new_frequency, dc.crawl_frequency),
						next_crawl = stg.crawled_on + (coalesce(schd.new_frequency, dc.crawl_frequency) || ' seconds')::interval,
						file_count = stg.file_count,
						subdir_count = stg.subdir_count,
						process_assigned_on	= null
					from 
						stg
						left join schd
							on (stg.dir_id=schd.dir_id)
					where
						dc.dir_id=stg.dir_id;
						
					return true;
				end;
				$$ LANGUAGE plpgsql;
			""")

			# schedule_subdirectories_to_scrape
			cur.execute("""
				create or replace function schedule_subdirectories_to_scrape
				(
					_dir_path text,
					_crawl_frequency int=60*60*24*14,
					_next_crawl timestamp=now()
				) 
				returns boolean
				as $$
				begin
					insert into directory_control as t
						(dir_id, dir_path, crawl_frequency, next_crawl)
					select
						id, dir_path, _crawl_frequency, _next_crawl
					from
						directory
					where
						basepath(dir_path) = _dir_path  -- Only select directories whose PARENT is the _dir_path 
					on conflict on constraint directory_control_pkey do 
						update set
							dir_id = excluded.dir_id  -- In case the dir_id has changed for the dir_path
							-- Don't update/reschedule any existing directories.
						where  -- Don't do empty updates
							t.dir_id <> excluded.dir_id;
	
					return true;
				end;
				$$ LANGUAGE plpgsql;
			""")

			# schedule_files_to_hash
			cur.execute("""
				create or replace function schedule_files_to_hash
				(
					_dir_id int
				) 
				returns boolean
				as $$
				begin
					insert into hash_control as t
						(file_id, mtime, file_size)
					select
						id, mtime, size
					from
						file
					where
						dir_id = _dir_id
						and not exists (
							select from hash h
							where h.file_id=f.id
						)
					on conflict on constraint hash_control_pkey do 
						update set
							mtime = excluded.mtime
						where
							t.mtime <> excluded.mtime;
	
					return true;
				end;
				$$ LANGUAGE plpgsql;
			""")

			# delete_file(int[])
			cur.execute("""
				create or replace function delete_file
				(
					_file_ids int[]
				) 
				returns table (id int)
				as $$
				begin
					return query
					with f as (  -- Get the list of files to delete
						select unnest(_file_ids) as file_id
					),
					del_hash as (  -- Delete the hash row
						delete from hash t
						using f
						where t.file_id=f.file_id
					),
					del_hash_schd as (  -- Delete the hash control row
						delete from hash_control t
						using f
						where t.file_id=f.file_id
					)
					-- Perform the actual file delete
					delete from file t
					using f
					where t.id=f.file_id
					returning t.id;
				end;
				$$ LANGUAGE plpgsql;
			""")

			# delete_file(text[])
			cur.execute("""
				create or replace function delete_file
				(
					_file_paths text[]
				) 
				returns table (id int)
				as $$
				begin
					return query
					select t.id 
					from delete_file(
						array(select s.id from search_file(_file_paths) s)::int[] -- Get the file_id for the paths					
					) t;
				end;
				$$ LANGUAGE plpgsql;
			""")

			# delete_file(int)
			cur.execute("""
				create or replace function delete_file
				(
					_file_id int
				) 
				returns table (id int)
				as $$
				begin
					return query
					select t.id from delete_file(array[_file_id]::int[]) t;
				end;
				$$ LANGUAGE plpgsql;
			""")

			# delete_file(text)
			cur.execute("""
				create or replace function delete_file
				(
					_file_path text
				) 
				returns table (id int)
				as $$
				begin
					return query
					select t.id from delete_file(array[_file_path]::text[]) t;
				end;
				$$ LANGUAGE plpgsql;
			""")

			# process_staged_hashes
			cur.execute("""
				create or replace function file_db_removal
				(
					_row_limit int = 10000
				)
				returns table (id int)
				as $$
				begin
					return query
					with f_id as (  -- Determine which files to delete
						select file_id
						from file_db_removal_staging
						order by inserted_on
						limit _row_limit
					),
					stage as (  -- Delete (and return) the file_ids that need to be run
						delete from file_db_removal_staging
						using f_id
						where file_id=f_id.file_id
						returning file_id
					)
					-- Execute the function to delete the rows
					select id
					from delete_file (array(  -- Pass the list of file_ids to the delete_file() function
						select s.file_id
						from stage
					)::int[]);
				end;
				$$ LANGUAGE plpgsql;
			""")

			pg.commit()
			cur.close()

	@staticmethod
	def install_foreign_keys(pg):
		pass