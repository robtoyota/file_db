import os
import platform
import time

class File:
	def __init__(self, file_name, dir_path='', dir_id=None):
		self.id 			= 0
		self.name 			= file_name
		self.dir_id			= dir_id
		self.size 			= None
		self.ctime			= None
		self.mtime			= None
		self.atime 			= None
		self.inserted_on 	= ""
		self.updated_on		= ""

		# Additional data about the file
		self.dir_path = dir_path
		self.parent_dir_last_crawled = ""

	def full_path(self):
		return os.path.join(self.dir_path, self.name)

	# Get the metadata for the directory
	def scrape_metadata(self):
		try:
			stat = os.stat(self.full_path())
			# Only Windows has the ctime stored for a directory
			if platform.system() == "Windows":
				self.ctime = time.ctime(stat.st_ctime)

			self.mtime = time.ctime(stat.st_mtime)
			self.atime = time.ctime(stat.st_atime)
			self.size = stat.st_size
			self.size = round(self.size / (1000 * 1000), 6)  # Convert from bytes to megabytes (windows != 1024)
		except PermissionError:
			print("PermissionError:", self.full_path())
		except FileNotFoundError:
			print("FileNotFoundError:", self.full_path())
		except:  # Ugh. Catchall
			print("Error - cannot scrape ", self.full_path())

	def staging_table_dict(self):
		return {
			'name': self.name,
			'dir_id': self.dir_id,
			'size': self.size,
			'ctime': self.ctime,
			'mtime': self.mtime,
			'atime': self.atime,
		}

	# Insert this object into the database
	def insert_new_file(self, pg):
		# TODO: This is obsolete. Use the staging table instead.

		# Insert the values
		cur = pg.cursor()
		cur.execute("""
			insert into
				file
				(name, dir_id, size, ctime, mtime, atime)
			values
				(%s, %s, %s, %s, %s, %s)
			on conflict on constraint file_pkey
				do update
				set
					size=excluded.size,
					ctime=excluded.ctime,
					mtime=excluded.mtime,
					atime=excluded.atime,
					updated_on=now();
			""",
			(
				self.name,
				self.dir_id,
				self.size,
				self.ctime,
				self.mtime,
				self.atime
			)
		)

		pg.commit()
		cur.close()

	@staticmethod
	def install_datatypes(pg):
		with pg.cursor() as cur:
			# file_detail datatype
			cur.execute("""
				do $$ begin
				create type file_detail as
				(
					full_path text,
					id int,
					file_name text,
					dir_id int,
					file_size float,
					ctime timestamp,
					mtime timestamp,
					atime timestamp,
					md5_hash text,
					sha1_hash text
				);
				exception when duplicate_object then null;
				end $$;
			""")

	@staticmethod
	def install_tables(pg, drop_tables):
		cur = pg.cursor()

		# Install the main file table
		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists file cascade;")

		cur.execute("""
			create table if not exists file
			(
				id 				serial unique not null,
				name			text not null, 		-- eg "calc.exe"
				dir_id			int not null,		-- ID for the directory table (will contain "C:/windows/system32")
				size 			numeric(18, 6),		-- In MBs
				ctime			timestamp,
				mtime			timestamp,
				atime			timestamp,
				inserted_on 	timestamp not null default now(),
				updated_on		timestamp not null default now(),
				primary key (name, dir_id)
			);
		""")

		# Install the table for deleted files
		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists file_archive cascade;")

		cur.execute("""
			create table if not exists file_archive
			(
				id 				int,
				name			text not null, 		-- eg "calc.exe"
				dir_id			int not null,		-- ID for the directory table (will contain "C:/windows/system32")
				size 			numeric(18, 6),		-- In MBs
				ctime			timestamp,
				mtime			timestamp,
				atime			timestamp,
				original_inserted_on 	timestamp not null,
				original_updated_on		timestamp not null,
				deleted_on		timestamp null,		-- Literal deleted timestamp, if known from the file system
				inserted_on		timestamp not null default now(),
				primary key (id)
			);
		""")

		# Install the staging table (note: this is an unlogged table. Speed is needed more than data recovery on restart.)
		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists file_stage cascade;")

		cur.execute("""
			create unlogged table if not exists file_stage
			(
				name			text not null, 		-- eg "calc.exe"
				dir_id			int not null,		-- ID for the directory table
				size 			numeric(18, 6),		-- In MBs
				ctime			timestamp,
				mtime			timestamp,
				atime			timestamp,
				inserted_by_process_id int not null,
				primary key (name, dir_id)
			);
		""")

		# Install the staging process table (note: this is an unlogged table)
		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists file_stage_process cascade;")

		cur.execute("""
					create unlogged table if not exists file_stage_process
					(
						dir_id			int not null,	-- ID for the directory containing the files
						delete_missing	boolean default false,	-- 1 = delete rows in file if missing from file_stage
						primary key (dir_id)
					);
				""")

		# Install the file category table
		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists file_category cascade;")

		cur.execute("""
			create table if not exists file_category
			(
				extension	text not null,
				category	text not null,
				primary key (extension, category)
			);
		""")

		pg.commit()
		cur.close()

	@staticmethod
	def install_indexes(pg):
		cur = pg.cursor()
		cur.execute("""
			create index if not exists file_id on file (id);
			create index if not exists file_dir_id on file (dir_id);
			create index if not exists file_size on file (size);
			create index if not exists file_ctime on file (ctime);
			create index if not exists file_mtime on file (mtime);
			create index if not exists file_atime on file (atime);
			create index if not exists file_inserted_on on file (inserted_on);
			create index if not exists file_updated_on on file (updated_on);
			create index if not exists file_reverse_name on file (reverse(name));
			
			create index if not exists file_archive_name on file_archive (name);
			create index if not exists file_archive_dir_id on file_archive (dir_id);
			create index if not exists file_archive_size on file_archive (size);
			create index if not exists file_archive_ctime on file_archive (ctime);
			create index if not exists file_archive_mtime on file_archive (mtime);
			create index if not exists file_archive_atime on file_archive (atime);
			create index if not exists file_archive_inserted_on on file_archive (inserted_on);
			create index if not exists file_archive_reverse_name on file_archive (reverse(name));
			create index if not exists file_archive_original_inserted_on on file_archive (original_inserted_on);
			
			create index if not exists file_stage_dir_id on file_stage (dir_id);
			create index if not exists file_stage_inserted_by_process_id on file_stage (inserted_by_process_id);
		""")
		pg.commit()
		cur.close()

	@staticmethod
	def install_pg_functions(pg):
		"""
		/*

			add_file
				upsert
				if the file is new or has changed, insert into control_hash
				
			delete_file
				delete from control_hash
				delete from hash
				delete from file
		*/
		"""
		with pg.cursor() as cur:
			cur.execute("""
				create or replace function file_path_exists (_path text) 
				returns bool
				as $$
				begin
					if exists (select 1 from vw_ll where dir_path=basepath(_path) and name=basename(_path) and type='file') then
						return true;
					else 
						return false;
					end if;
				end;
				$$ LANGUAGE plpgsql;
			""")

			# delete_file, and its overloads
			cur.execute("""
				-- Base function. Accepts an array of file ID ints
				create or replace function delete_file
				(
					_file_ids int[]
				) 
				returns table (id int)
				as $$
				begin
					return query
					with f as (  -- Get the list of files to delete
						select distinct unnest(_file_ids) as file_id
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
					),
					del as (  -- Perform the actual file delete
						delete from file t
						using f
						where t.id=f.file_id
						returning t.id, t.name, t.dir_id, t.size, t.ctime, t.mtime, t.atime, t.inserted_on, t.updated_on
					),
					archive as (  -- Insert the archive
						insert into file_archive
							(id, name, dir_id, size, ctime, mtime, atime, original_inserted_on, original_updated_on)
						select t.id, t.name, t.dir_id, t.size, t.ctime, t.mtime, t.atime, t.inserted_on, t.updated_on
						from del t
					)
					select t.id from del t;
				end;
				$$ LANGUAGE plpgsql;

				-- Accepts a list of file paths, and looks up the IDs and passes it to the main function
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

				-- Accepts a single file ID int, and converts it to an array, and passes it to the main function
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

				-- Accepts a single file path, and converts it to an array, and passes it to the function to lookup the IDs
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

	@staticmethod
	def install_pg_triggers(pg):
		pass

	@staticmethod
	def install_foreign_keys(pg):
		pass
