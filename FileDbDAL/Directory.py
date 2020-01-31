import os
import platform
import datetime
import FileDbDAL
import time


class Directory:
	def __init__(self, dir_path, last_crawled=datetime.datetime.now()):
		self.id = 0
		self.dir_path = dir_path
		self.ctime = None
		self.mtime = None
		self.inserted_on = ""
		self.deleted_on = ""
		self.name = ""

	# Get the metadata for the directory
	def scrape_metadata(self):
		# Only Windows has the ctime stored for a directory
		try:
			if platform.system() == "Windows":
				self.ctime = time.ctime(os.path.getctime(self.dir_path))
			self.mtime = time.ctime(os.path.getmtime(self.dir_path))
		except:
			print(f"Unable to collect metadata from {self.dir_path}")

	def staging_table_dict(self):
		return {
			'dir_path': self.dir_path,
			'ctime': self.ctime,
			'mtime': self.mtime,
		}

	# Insert the directory into the database
	def insert_new_directory(self, pg):
		# TODO: This is obsolete. Use the staging table instead.

		# Insert the values
		cur = pg.cursor()
		try:
			cur.execute("""
				select inserted_id from insert_directory(%s, %s, %s);
				""",
				(
					self.dir_path,
					datetime.datetime.fromtimestamp(self.ctime) if self.ctime is not None else None,
					datetime.datetime.fromtimestamp(self.mtime) if self.mtime is not None else None
				)
			)
		except:
			print(f"Error inserting new directory: {self.dir_path}")

		# Get the ID of the row that was just inserted
		self.id = next(cur)['inserted_id']

		pg.commit()
		cur.close()

		# Insert the directory into the control table to be crawled
		dc = FileDbDAL.DirectoryCrawl()
		dc.dir_id = self.id
		dc.insert_new_directory_to_crawl(pg, self.dir_path)

	@staticmethod
	def install_tables(pg, drop_tables):
		cur = pg.cursor()

		# Install the main directory table
		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists directory cascade;")

		cur.execute("""
			create table if not exists directory
			(
				id 				serial unique not null,
				dir_path		text not null unique,		-- Eg: "C:/windows/system32"
				ctime			timestamp null,
				mtime			timestamp null,
				inserted_on 	timestamp not null default now(),
				updated_on	 	timestamp not null default now(),
				primary key (id)
			);
		""")

		# Install the table for deleted directories
		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists directory_delete cascade;")

		cur.execute("""
			create table if not exists directory_delete
			(
				id 				int,
				dir_path		text not null,		-- Eg: "C:/windows/system32"
				ctime			timestamp null,
				mtime			timestamp null,
				original_inserted_on 	timestamp not null default now(),
				original_updated_on	 	timestamp not null default now(),
				deleted_on		timestamp null,		-- Literal deleted timestamp, if known from the file system
				inserted_on		timestamp not null default now(),
				primary key (id)
			);
		""")

		# Install the staging table (note: this is an unlogged table. Speed is needed more than data recovery on restart)
		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists directory_stage cascade;")

		cur.execute("""
			create unlogged table if not exists directory_stage
			(
				dir_path		text not null,		-- Eg: "C:/windows/system32"
				ctime			timestamp,
				mtime			timestamp,
				inserted_by_process_id int not null,
				primary key (dir_path)
			);
		""")

		# Install the staging process table (note: this is an unlogged table)
		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists directory_stage_process cascade;")

		cur.execute("""
					create unlogged table if not exists directory_stage_process
					(
						parent_dir_path	text not null,	-- Eg: "C:/windows/system32"
						delete_missing	boolean default false,	-- 1 = delete rows in directory if missing from directory_stage
						primary key (parent_dir_path)
					);
				""")

		pg.commit()
		cur.close()

	@staticmethod
	def install_indexes(pg):
		cur = pg.cursor()
		cur.execute("""
					create index if not exists directory_path_dir_path on directory (basepath(dir_path));
					create index if not exists directory_ctime on directory (ctime);
					create index if not exists directory_mtime on directory (mtime);
					create index if not exists directory_inserted_on on directory (inserted_on);
					
					create index if not exists directory_delete_path_dir_path on directory_delete (basepath(dir_path));
					create index if not exists directory_delete_ctime on directory_delete (ctime);
					create index if not exists directory_delete_mtime on directory_delete (mtime);
					create index if not exists directory_delete_inserted_on on directory_delete (inserted_on);
					create index if not exists directory_delete_original_inserted_on on directory_delete (original_inserted_on);
					
					create index if not exists directory_stage_basepath_dir_path on directory_stage (basepath(dir_path));
					create index if not exists directory_stage_inserted_by_process_id on directory_stage (inserted_by_process_id);
		""")
		pg.commit()
		cur.close()

	@staticmethod
	def install_pg_functions(pg):
		pass