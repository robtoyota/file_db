class SQLUtil:
	@staticmethod
	def util_reset_process_tasks(pg):
		# Reset DB
		with pg.cursor() as cur:
			cur.execute("select * from util_reset_process_tasks();")

	# Install the base functions that any other function, view, FK, index, could use
	@staticmethod
	def install_base_functions(pg):
		cur = pg.cursor()

		# Path and file name parsing functions
		cur.execute("""
			-- Straightforward regex to remove the basename from the path
			-- https://stackoverflow.com/a/28879164
			-- Eg: "C:\Windows\calc.exe" -> returns "C:\Windows"
			create or replace function basepath(text) returns text
			as $path$
			declare
			FILE_PATH alias for $1;
			ret         text;
			begin
			ret := regexp_replace(FILE_PATH,'(?<=.)[\\/\\\\][^\\/\\\\]+$', '');
			ret := ret || case when (right(ret, 1) = ':') then '\\' else '' end; 
			return ret;
			end;
			$path$ LANGUAGE plpgsql
			immutable;

			-- Straightforward regex to remove the path from the basename
			-- https://stackoverflow.com/a/28879164
			-- Eg: "C:\Windows\calc.exe" -> returns "calc.exe"
			create or replace function basename(text) returns text
				as $basename$
				declare
				FILE_PATH alias for $1;
				ret         text;
			begin
				ret := regexp_replace(FILE_PATH,'^.+[\\/\\\\]', '');
				return ret;
			end;
			$basename$ LANGUAGE plpgsql
			immutable;

			-- Straightforward regex to remove the path and file name to return the extension
			-- Also returns lower case
			-- https://stackoverflow.com/a/28879164
			-- Eg: "C:\Windows\calc.eXE" -> returns ".exe"
			create or replace function extension(text) returns text
				as $extension$
			declare
				FILE_PATH alias for $1;
				ret         text;
			begin
				ret := lower(regexp_replace(FILE_PATH,'^.+\\.', ''));
				return ret;
			end;
			$extension$ LANGUAGE plpgsql
			immutable;
		""")

		# Join file paths together
		cur.execute("""
			create or replace function path_join(_dir_path text, _file_path text) 
			returns text
			as $$
			begin
				-- TODO: Determine if the file path should use / or \ as the separator. 
				return _dir_path || '\\' || _file_path;
			end;
			$$ LANGUAGE plpgsql
			immutable;
		""")

		pg.commit()
		cur.close()

	# These are the base views that other queries use
	@staticmethod
	def install_base_views(pg):
		cur = pg.cursor()
		# vw_ll: List the files and directories (*nix "ll")
		# TODO: Check if this exists first
		cur.execute("""
			create or replace view vw_ll as -- Reference to *nix "ll" command to list files and directories
			select
				'file' as type,
				dir.dir_path || '\\' || f.name as full_path,
				f.id as file_id, f.name, f.dir_id, f.size, f.ctime, f.mtime, f.atime,
				h.md5_hash, h.sha1_hash,
				dir.dir_path
			from
				directory dir
				join file f
					on (dir.id=f.dir_id)
				left join hash h
					on (f.id=h.file_id)
			union all
			select
				'dir' as type,
				dir.dir_path as full_path,
				0 as file_id, basename(dir.dir_path) as name, parent.id as dir_id, 0 as size, dir.ctime, dir.mtime, null as atime,
				null as md5_hash, null as sha1_hash,
				parent.dir_path as dir_path
			from
				directory dir
				join directory parent
					on (parent.dir_path = basepath(dir.dir_path));
		""")

		# vw_f: List the file details (full path, file meta data, hashes)
		# TODO: Check if this exists first
		cur.execute("""
			create or replace view vw_f as
			select
				path_join(dir.dir_path, f.name) as full_path,
				f.id, f.name, f.dir_id, f.size, f.ctime, f.mtime, f.atime,
				h.md5_hash, h.sha1_hash,
				dir.dir_path
			from
				directory dir
				join file f
					on (dir.id=f.dir_id)
				left join hash h
					on (f.id=h.file_id);
		""")


		cur.execute("""
			create or replace view vw_directory_activity_last_ctime as
			select dir_id, max(ctime) as last_ctime
			from vw_ll
			group by dir_id;
		""")


		cur.execute("""
			create or replace view vw_directory_activity_last_mtime as
			select dir_id, max(mtime) as last_mtime
			from vw_ll
			group by dir_id;
		""")
		pg.commit()
		cur.close()

	# Main method to install utility or common functions
	@staticmethod
	def install_pg_functions(pg):
		cur = pg.cursor()

		# util_reset_process_tasks - Reset DB functions
		cur.execute("""
			-- This function gets run at the beginning of every program startup, to clean data after a crash.
			create or replace function util_reset_process_tasks()
			returns boolean
			as $$
			begin
				-- Release directories first
				-- Delete all of the processes' data from the staging tables
				delete from directory_stage;
				
				delete from file_stage;
				
				-- Release the directory_control rows that were assigned to the process
				update directory_control
				set
					assigned_process_id	= 0,
					process_assigned_on	= null
				where process_assigned_on is not null;
				
				
				-- Now release the hashes
				
				-- Release the hash_control rows that were assigned to the process
				update hash_control
				set
					process_assigned_on	= null
				where process_assigned_on is not null;
				
				return true;
			end
			$$ language plpgsql;
		""")

		# crawl_frequency_last_ctime_calculate
		cur.execute("""
			create or replace function crawl_frequency_last_ctime_calculate
			(
				_divide_seconds float, -- Number to divide the number of seconds since the last_ctime by 
				_min_frequency int default null, -- Highest number of seconds allowed to be returned. null = no limit
				_max_frequency int default null, -- Lowest number of seconds allowed to be returned. null = no limit  
				_dir_id int[] default null -- Which dir to lookup. null = return every dir's newly calculated crawl_frequency 
				-- todo: make _dir_id accept a set of multiple IDs to return, if needed 
			)
			returns table
			(
				dir_id int,
				new_frequency int
			)
			as $$
			/*
			* This function divides the number of seconds since the latest ctime of a directory's immediate contents by
			* the _divide_seconds input value. 
			* (it does not evaluate the ctimes of the contents of subdirectories). 
			*/

			begin
				return query
				-- Calculate the new frequency
				with d as ( -- Get the list of dirs
					select dc.dir_id, dc.inserted_on  -- if _dir_id is not null: get the requested dirs
					from
						directory_control dc
						join (
							select unnest(_dir_id) as dir_id
						) d_id 
							on (dc.dir_id=d_id.dir_id)
					union  -- Do not check for a null input with an OR to compare the input to a column.
					select dc.dir_id, dc.inserted_on  -- if _dir_id is null: get all dirs
					from
						directory_control dc
					where _dir_id is null
				), 
				nf as ( -- New frequency
					select
						d.dir_id,
						(
							extract(epoch from (now() - coalesce(la.last_ctime, d.inserted_on)))  -- Number of seconds since the last crawl
							/ _divide_seconds  -- Divided by the user-supplied number
						) as new_frequency
					from 
						d
						join vw_directory_activity_last_ctime as la -- last activity
							on (d.dir_id=la.dir_id)
				)
				-- Apply a check to apply the minimum or maximum value
				select 
					nf.dir_id,
					(case
						when (nf.new_frequency < _min_frequency) then  -- Apply the minimum value
							_min_frequency
						when (nf.new_frequency > _max_frequency) then -- Apply the maximum value
							_max_frequency
						else
							nf.new_frequency  -- Apply the calculated value
					end)::int as new_frequency
				from 
					nf;
			end
			$$ language plpgsql;
		""")

		# crawl_frequency_last_ctime_calculate - Overload function to accept a single dir_id int
		cur.execute("""
			create or replace function crawl_frequency_last_ctime_calculate
				(
					_divide_seconds float, -- Number to divide the number of seconds since the last_ctime by 
					_min_frequency int default null, -- Highest number of seconds allowed to be returned. null = no limit
					_max_frequency int default null, -- Lowest number of seconds allowed to be returned. null = no limit  
					_dir_id int default null -- Which dir to lookup. null = return every dir's newly calculated crawl_frequency 
					-- todo: make _dir_id accept a set of multiple IDs to return, if needed 
				)
				returns table
				(
					dir_id int,
					new_frequency int
				)
				as $$
				begin
					return query
					select dir_id, new_frequency
					from crawl_frequency_last_ctime_calculate(
						_divide_seconds,
						_min_frequency,
						_max_frequency,
						array[_dir_id]::int[]  -- Convert the single value to an array
					);
				end
				$$ language plpgsql;
		""")

		# crawl_frequency_last_ctime_set
		cur.execute("""
			create or replace function crawl_frequency_last_ctime_set
			(
				_divide_seconds float, -- Number to divide the number of seconds since the last_ctime by 
				_min_frequency int default null, -- Highest number of seconds allowed to be returned. null = no limit
				_max_frequency int default null, -- Lowest number of seconds allowed to be returned. null = no limit  
				_dir_id int default null -- Which dir to lookup. null = return every dir's newly calculated crawl_frequency 
				-- todo: make _dir_id accept a set of multiple IDs to return, if needed 
			)
			returns boolean
			as $$
			begin
				update directory_control dc
				set
					crawl_frequency = new.new_frequency,
					next_crawl = last_crawled + (new.new_frequency * interval '1 second')
				from
					crawl_frequency_last_ctime_calculate(_divide_seconds, _min_frequency, _max_frequency, _dir_id) new
				where
					dc.dir_id = new.dir_id;
				
				return true;
			end
			$$ language plpgsql;
		""")

		pg.commit()
		cur.close()

	# Main method to install all views
	@staticmethod
	def install_views(pg):
		pass
