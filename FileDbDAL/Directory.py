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
			cur.execute("drop table if exists directory_archive cascade;")

		cur.execute("""
			create table if not exists directory_archive
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
					
					create index if not exists directory_archive_path_dir_path on directory_archive (basepath(dir_path));
					create index if not exists directory_archive_ctime on directory_archive (ctime);
					create index if not exists directory_archive_mtime on directory_archive (mtime);
					create index if not exists directory_archive_inserted_on on directory_archive (inserted_on);
					create index if not exists directory_archive_original_inserted_on on directory_archive (original_inserted_on);
					
					create index if not exists directory_stage_basepath_dir_path on directory_stage (basepath(dir_path));
					create index if not exists directory_stage_inserted_by_process_id on directory_stage (inserted_by_process_id);
		""")
		pg.commit()
		cur.close()

	@staticmethod
	def install_pg_functions(pg):
		with pg.cursor() as cur:
			# dir_path_exists
			cur.execute("""
				create or replace function dir_path_exists (_path text) 
				returns bool
				as $$
				begin
					if exists (select 1 from directory where dir_path=_path) then
						return true;
					else 
						return false;
					end if;
				end;
				$$ LANGUAGE plpgsql;
			""")

			# delete_directory, and its overloads
			cur.execute("""
				-- Base function. Accepts an array of dir ID ints
				create or replace function delete_directory
				(
					_dir_ids int[],
					_delete_subdirs bool = false,
					_delete_children_immediately bool = true
				) 
				returns table (id int, "type" text)
				as $$
				begin
					return query
					-- User input
					with dirs as (  -- Get the list of dirs to delete
						-- Extract the list of IDs from the input
						select distinct unnest(_dir_ids) as dir_id
						-- And union in all the subdirs, if required
					),

					-- Delete subdirs
					subdirs as (  -- Get the list of subdirs to delete (if subdirs are meant to be deleted)
						select subdir.id as dir_id
						from
							directory parent
							join dirs inp
								on (parent.id=inp.dir_id)
							join directory subdir
								on (subdir.dir_path like sql_path_parse_wildcard_search(parent.dir_path) || '_%')
						where _delete_subdirs = true
					),
					del_subdirs_now as (  -- Delete the subdirs immediately
						select t.id, t."type"
						from delete_directory(  -- Pass the delete function the list of IDs
							array(
								select sd.dir_id
								from subdirs sd
								where _delete_children_immediately = true
							)::int[],
							false,  -- No need to delete subdirs, since this selects the subdirs already. Also prevents additional recursion
							true  -- Yes, continue to delete immediately
						) t
						where _delete_subdirs = true  -- Important to reduce unnecessary recursion.
					),
					del_subdirs_stg as (  -- ...Or stage subdirs to be deleted (this is more efficient, but relies on the program's server)
						-- This staging table gets processed separately to perform the delete.
						insert into db_removal_directory_staging (dir_id, delete_subdirs)
						select sd.dir_id, false -- Don't enable delete_subdirs, since this already inserts the CURRENT list of subdirs
						from subdirs sd
						where _delete_children_immediately = true
						returning dir_id as id
					),

					-- Delete files
					file_ids as (  -- Get the list of file IDs to be deleted
						select f.id
						from 
							file f
							join (  -- Get the list of files in subdirs ONLY if this has to be deleted immediately
								select dir_id from dirs
								union
								select dir_id from subdirs where _delete_children_immediately = true
							) d
								on (f.dir_id=d.dir_id)
					),
					del_files_now as (  -- Delete the files immediately
						select t.id
						from delete_file(array(  -- Pass the delete function the list of IDs
							select f.id
							from file_ids f
							where _delete_children_immediately = true
						)::int[]) t
					),
					del_files_stg as (  -- ...Or stage files to be deleted (this is more efficient, but relies on the program's server)
						-- This staging table gets processed separately to perform the delete.
						insert into db_removal_file_staging (file_id)
						select t.id
						from file_ids t
						where _delete_children_immediately = false
						returning file_id as id
					),

					-- Delete the requested dir itself
					del_schd as (  -- Delete the directory control row
						delete from directory_control t
						using dirs d
						where d.dir_id=t.dir_id
					),
					del_dir as (  -- Perform the actual dir delete
						delete from directory t
						using dirs d
						where t.id=d.dir_id
						returning t.id, t.dir_path, t.ctime, t.mtime, t.inserted_on, t.updated_on
					),
					archive as (  -- Copy the deleted row to the archive table
						insert into directory_archive
							(id, dir_path, ctime, mtime, original_inserted_on, original_updated_on)
						select t.id, t.dir_path, t.ctime, t.mtime, t.inserted_on, t.updated_on
						from del_dir t
					)

					-- Output the IDs that got deleted
					select t.id, 'dir'::text as "type" from del_dir t
					union all
					select t.id, t."type" from del_subdirs_now t  -- This is important in order to get delete_directory to execute!
					union all
					select t.id, 'dir' from del_subdirs_stg t
					union all
					select t.id, 'file' from del_files_now t  -- This is important in order to get delete_file to execute!
					union all
					select t.id, 'file' from del_files_stg t;
				end;
				$$ LANGUAGE plpgsql;

				-- Accepts a list of dir paths, and looks up the IDs and passes it to the main function
				create or replace function delete_directory
				(
					_dir_paths text[],
					_delete_subdirs bool = false,
					_delete_children_immediately bool = true
				) 
				returns table (id int, "type" text)
				as $$
				begin
					return query
					select t.id, t."type"
					from delete_directory(
						array(select s.id from search_dir(_dir_paths) s)::int[], -- Get the dir_ids for the paths
						_delete_subdirs					
					) t;
				end;
				$$ LANGUAGE plpgsql;

				-- Accepts a single dir ID int, and converts it to an array, and passes it to the main function
				create or replace function delete_directory
				(
					_dir_id int,
					_delete_subdirs bool = false,
					_delete_children_immediately bool = true
				) 
				returns table (id int, "type" text)
				as $$
				begin
					return query
					select t.id, t."type" from delete_directory(array[_dir_id]::int[], _delete_subdirs) t;
				end;
				$$ LANGUAGE plpgsql;

				-- Accepts a single dir path, and converts it to an array, and passes it to the function to lookup the IDs
				create or replace function delete_directory
				(
					_dir_path text,
					_delete_subdirs bool = false,
					_delete_children_immediately bool = true
				) 
				returns table (id int, "type" text)
				as $$
				begin
					return query
					select t.id, t."type" from delete_directory(array[_dir_path]::text[], _delete_subdirs) t;
				end;
				$$ LANGUAGE plpgsql;
			""")


	@staticmethod
	def install_pg_triggers(pg):
		pass
