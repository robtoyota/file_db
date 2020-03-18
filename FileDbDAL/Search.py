class Search:
	def __init__(self):
		pass

	@staticmethod
	def iter_search(pg, function_name, args):
		try:
			with pg.cursor() as cur:
				# Execute the specified function. The cursor results will be processed further below
				if function_name in ["search_name", "search_name_file", "search_name_dir", "search_hash"]:
					sql = [
						"select",
						"*",
						"from",
						function_name,
					]

					if function_name in ["search_name", "search_name_file", "search_name_dir"]:
						sql.append("(%s)")
						values = (args['name'],)
					elif function_name in ['search_hash']:
						sql.append("(%s, %s)")
						values = (args['hash'], args['hash_algorithm'],)

					cur.execute(" ".join(sql), values)

				for row in cur:
					yield row

		except:  # Ugh:
			pass

	@staticmethod
	def print_search(pg, function_name, args, col_separator='\t'):
		# Todo: Prettify this

		# Perform the search, and iterate through the results
		result_count = 0
		for row in Search.iter_search(pg, function_name, args):
			# Output the results, in their required formats

			# Output mixed file and dir results
			if function_name in ['search_name', 'search_name_dir']:
				print(col_separator.join([
					row['name'],
					row['size'],
					row['full_path'],
				]))

			# Output file-only results
			if function_name in ['search_file', 'search_hash']:
				print(col_separator.join([
					row['name'],
					row['size'],
					row['full_path'],
				]))


	@staticmethod
	def install_tables(pg, drop_tables):
		pass

	@staticmethod
	def install_indexes(pg):
		pass

	@staticmethod
	def install_pg_functions(pg):
		with pg.cursor() as cur:
			# search_name
			cur.execute("""
				create or replace function search_name
				(
					_name text
				)
				returns setof vw_ll
				as $$
				begin
					-- todo: If a matching directory is found, don't return all the files inside of it
					return query
					select *
					from vw_ll
					where 
						name like _name  -- Match the file name
						or basename(dir_path) like _name;  -- Match the dir name
				end;
				$$ language plpgsql;
			""")

			# search_name_file
			cur.execute("""
				create or replace function search_name_file
				(
					_name text
				)
				returns setof vw_ll
				as $$
				begin
					return query
					select *
					from vw_ll
					where name like _name;
				end;
				$$ language plpgsql;
			""")

			# search_name_dir
			cur.execute("""
				create or replace function search_name_dir
				(
					_name text
				)
				returns setof directory
				as $$
				begin
					return query
					select *
					from directory
					where basename(dir_path) like _name;
				end;
				$$ language plpgsql;
			""")

			# search_full_path(text[])
			cur.execute("""
				create or replace function search_full_path
				(
					_full_path text[]
				)
				returns setof vw_ll
				as $$
				begin
					return query
					select ll.* 
					from 
						vw_ll ll
						join (select distinct unnest(_full_path) as full_path) fp
							on (ll.dir_path=basepath(fp.full_path) and ll.name=basename(fp.full_path));
				end;
				$$ language plpgsql;
			
				-- Accept a single path, and convert it to an array
				create or replace function search_full_path
				(
					_full_path text
				)
				returns setof vw_ll
				as $$
				begin
					return query
					select * from search_full_path(array[_full_path]::text[]);
				end;
				$$ language plpgsql;
			""")

			# search_file(text[])
			cur.execute("""
				create or replace function search_file
				(
					_full_path text[]
				)
				returns setof vw_file_detail
				as $$
				begin
					return query
					select f.* 
					from 
						vw_file_detail f
						join (select distinct unnest(_full_path) as full_path) fp
							on (f.dir_path=basepath(fp.full_path) and f.name=basename(fp.full_path));
				end;
				$$ language plpgsql;
				
				-- Accept a single path, and convert it to an array
				create or replace function search_file
				(
					_full_path text
				)
				returns setof vw_file_detail
				as $$
				begin
					return query
					select * from search_file(array[_full_path]::text[]);
				end;
				$$ language plpgsql;
			""")

			# search_dir(text[])
			cur.execute("""
				create or replace function search_dir
				(
					_full_path text[]
				)
				returns setof directory
				as $$
				begin
					return query
					select d.*
					from 
						directory d
						join (select distinct unnest(_full_path) as full_path) fp
							on (d.dir_path=fp.full_path);
				end;
				$$ language plpgsql;
			
				-- Accept a single path, and convert it to an array
				create or replace function search_dir
				(
					_full_path text
				)
				returns setof directory
				as $$
				begin
					return query
					select * from search_dir(array[_full_path]::text[]);
				end;
				$$ language plpgsql;
			""")

			# search_hash
			cur.execute("""
				create or replace function search_hash
				(
					_hash text,
					_hash_algorithm text default null
				)
				returns setof vw_ll
				as $$
				begin
					_hash_algorithm = upper(_hash_algorithm);

					return query
					select *
					from vw_ll
					where 
						(  -- Check MD5 hash
							(_hash_algorithm is null or _hash_algorithm = 'MD5')
							and md5_hash = _hash
						)
						or (  -- Check SHA-1 hash
							(_hash_algorithm is null or _hash_algorithm in ('SHA1', 'SHA-1'))
							and sha1_hash = _hash
						);
				end;
				$$ language plpgsql;
			""")

			# search_duplicate_file
			cur.execute("""
				create or replace function search_duplicate_file
				(
					_full_path text,
					_hash_match boolean=true,
					_name_match boolean=false
					-- todo: list of files to match
					-- todo: subdir full of files to match
					-- todo: limiting paths to search in
				)
				returns setof vw_ll
				as $$
				begin
					return query
					with needle as (
						select *
						from vw_ll
						where dir_path = basepath(_full_path) and name = basename(_full_path) -- Search by base path
					)
					select f.*
					from vw_ll f
						join needle n
							on (
								f.id=n.id -- Make sure to at least return the file at the given path
								or ( -- Find the matching hashes
									_hash_match = true
									and ( -- Compare the hashes
										( -- Check if there is a sha1 hash first (sha1 is preferred)
											(f.sha1_hash is not null and n.sha1_hash is not null) -- Is sha1_hash populated for both tables?
											and f.sha1_hash = n.sha1_hash
										)
										or ( -- If no sha1 hash, then try the md5 hash (higher chance of false positives)
											(f.sha1_hash is null or n.sha1_hash is null) -- If sha1_hash is not populated in one or both tables...
											and (f.md5_hash is not null and n.md5_hash is not null) -- Is md5_hash populated for both tables?
											and f.md5_hash = n.md5_hash
										)
									)
									and f.size = n.size -- Make sure the file sizes are the same, to reduce the chance of a false positive match
								)
								or ( -- Find matching names
									_name_match = true
									and f.name = n.name
								)
							);
				end;
				$$ language plpgsql;
			""")

			# search_duplicate_dir
			cur.execute("""
				create or replace function search_duplicate_dir
				(
					_dir_path text,
					_hash_match_files boolean=true,
					_name_match_files boolean=false
					-- todo: _num_of_files_match boolean=true, -- The num_of_... flag can be used with _hash_match_files to find dirs with identical contents
					-- todo: _num_of_subdirs_match boolean=true,
					-- todo: list of dirs to match
					-- todo: limiting paths to search in
				)
				returns setof vw_ll
				as $$
				begin
					return query
					with needle as (
						select *
						from vw_ll
						where dir_path = _dir_path
					)
					select f.*
					from vw_ll f
						join needle n
							on (
								f.id=n.id -- Make sure to at least return the files at the given path
								or ( -- Find the matching hashes
									_hash_match_files = true
									and ( -- Compare the hashes
										( -- Check if there is a sha1 hash first (sha1 is preferred)
											(f.sha1_hash is not null and n.sha1_hash is not null) -- Is sha1_hash populated for both tables?
											and f.sha1_hash = n.sha1_hash
										)
										or ( -- If no sha1 hash, then try the md5 hash (higher chance of false positives)
											(f.sha1_hash is null or n.sha1_hash is null) -- If sha1_hash is not populated in one or both tables...
											and (f.md5_hash is not null and n.md5_hash is not null) -- Is md5_hash populated for both tables?
											and f.md5_hash = n.md5_hash
										)
									)
									and f.size = n.size -- Make sure the file sizes are the same, to reduce the chance of a false positive match
								)
								or ( -- Find matching names
									_name_match_files = true
									and f.name = n.name
								)
							);
				end;
				$$ language plpgsql;
			""")

	@staticmethod
	def install_foreign_keys(pg):
		pass
