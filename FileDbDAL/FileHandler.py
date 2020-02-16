from FileHandler import CopyFile
from datetime import datetime

class FileHandler:
	@staticmethod
	def get_files_to_copy(pg, copy_file_list_queue, row_limit):
		try:
			with pg.cursor() as cur:
				cur.execute("""
					select id, file_path, new_path, move_file, overwrite, file_hash, hash_type, file_size, perform_hash_check 
					from get_files_to_copy(%s);""",
					(row_limit,)
				)
				# Populate the dirs list with the paths:
				for row in cur:
					# Build the new object
					f = CopyFile(
						id=row['id'],
						file_path=row['file_path'],
						new_path=row['new_path'],
						move_file=row['move_file'],
						overwrite=row['overwrite'],
						file_hash=row['file_hash'],
						hash_type=row['hash_type'],
						file_size=row['file_size'],
						perform_hash_check=row['perform_hash_check']
					)

					# Add to the queue for further processing
					copy_file_list_queue.put(f)
		except:  # Ugh:
			pass

	@staticmethod
	def install_tables(pg, drop_tables):
		with pg.cursor() as cur:
			# Install the file copy table
			if drop_tables:
				# TODO: Check if this table contains data before dropping
				cur.execute("drop table if exists copy_file cascade;")

			cur.execute("""
				create table if not exists copy_file
				(
					id 				serial unique not null,
					file_id			int, 	-- ID from the file table to be copied. Optional if the file_path is populated
					file_path		text,	-- Full path of the file to be copied. Optional if file_id is populated.
					new_dir_path	text not null,
					new_file_name	text null,		-- If left blank/null, will use the original file name
					move_file		boolean default false,
					overwrite		char(1),		-- Y=Force overwrite | N=Force Skip | W=Warn
					file_hash		text null,		-- Optional: The source file's hash to verify the file has not been modified
					hash_type		text null,		-- Optional: The hash type (md5, sha1) for the file_hash
					file_size		numeric(18, 6),		-- Optional: The source's size to verify the file has not been modified. In MBs. 
					perform_hash_check boolean default false,
					assigned_on		timestamp null,
					inserted_on		timestamp default now(),
					primary key (id)
				);
			""")

			# Install the directory copy table
			if drop_tables:
				# TODO: Check if this table contains data before dropping
				cur.execute("drop table if exists copy_directory cascade;")

			cur.execute("""
				create table if not exists copy_directory
				(
					id 				serial unique not null,
					dir_id			int null, 	-- ID from the directory table to be copied. Optional if dir_path is populated
					dir_path		text null,	-- Full path of the directory to be copied. Optional if dir_id is populated
					new_parent_dir	text not null,
					new_dir_name	text null,		-- If left blank/null, will use the original directory name
					move_dir		boolean default false,
					overwrite		char(1),		-- Y=Force overwrite | N=Force Skip | W=Warn
					perform_hash_check boolean default false,
					assigned_on		timestamp null,
					inserted_on		timestamp default now(),
					primary key (id)
				);
			""")

	@staticmethod
	def install_indexes(pg):
		with pg.cursor() as cur:
			cur.execute("""
				create index if not exists copy_file_file_id on copy_file (file_id);
				create index if not exists copy_file_file_path on copy_file (file_path);
				create index if not exists copy_file_new_dir_path on copy_file (new_dir_path);
				create index if not exists copy_file_assigned_on on copy_file (assigned_on);
				create index if not exists copy_file_inserted_on on copy_file (inserted_on);
			""")

			cur.execute("""
				create index if not exists copy_directory_dir_id on copy_directory (dir_id);
				create index if not exists copy_directory_dir_path on copy_directory (dir_path);
				create index if not exists copy_directory_new_dir_path on copy_directory (new_parent_dir);
				create index if not exists copy_directory_assigned_on on copy_directory (assigned_on);
				create index if not exists copy_directory_inserted_on on copy_directory (inserted_on);
			""")

	@staticmethod
	def install_pg_functions(pg):
		with pg.cursor() as cur:
			# Install the function to get the list of files to copy
			cur.execute("""
				create or replace function get_files_to_copy(_row_limit int) 
				returns table 
				(
					id int,
					file_path text,
					new_path text,
					move_file boolean,
					overwrite text,
					file_hash text,
					hash_type text,
					file_size numeric(18, 6),
					perform_hash_check boolean
				)
				as $$
				begin
					-- Claim the files to copy, and return the values
					return query
					-- Select the rows to claim
					with id_list as (
						select
							c.id
						from copy_file c
						where assigned_on is null
						order by inserted_on
						limit _row_limit
					),
					-- Claim the rows
					upd as (
						update copy_file c
						set c.assigned_on = now()
						from id_list id
						where id.id=c.id
						returning 
							c.id, c.file_id, c.file_path, c.new_dir_path, c.new_file_name, 
							c.move_file, c.overwrite, c.file_hash, c.hash_type, c.file_size, c.perform_hash_check
					)
					-- Return the files to be copied
					select
						c.id,
						coalesce(f.full_path, c.file_path) as file_path,
						path_join(			-- Build the new path
							new_dir_path, 	-- Get the base path 
							coalesce(		-- If the new_file_name is entered, use it, otherwise use the original name
								ifnull(c.new_file_name, ''),
								basename(
									coalesce(f.full_path, c.file_path)
								)
							)
						) as new_path,
						c.move_file, c.overwrite, c.file_hash, c.hash_type, c.file_size, c.perform_hash_check
					from 
						upd c
						left join vw_file_detail f
							on (c.file_id=f.id and c.file_id > 0)
					order by 2;
				end;
				$$ LANGUAGE plpgsql;
			""")

	@staticmethod
	def install_foreign_keys(pg):
		pass