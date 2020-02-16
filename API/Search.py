import pandas as pd


class Search:
	@staticmethod
	def search_name(pg, name: str) -> bool:
		pass
		# Search.print_search(pg.pg, 'search_name', {'name': name})

	@staticmethod
	def search_name_file(pg, name: str) -> bool:
		pass
		# Search.print_search(pg.pg, 'search_name_file', {'name': name})

	@staticmethod
	def search_name_dir(pg, name: str) -> bool:
		pass
		# Search.print_search(pg.pg, 'search_name_dir', {'name': name})

	@staticmethod
	def search_hash(pg, hash, hash_algorithm=None) -> bool:
		pass
		# Search.print_search(pg.pg, 'search_hash', {'hash': hash, 'hash_algorithm': None})

	@staticmethod
	def search_duplicate_file(pg, path: str) -> pd.DataFrame:
		sql = """
			select haystack.name, haystack.full_path, haystack.ctime, haystack.mtime
			from 
				vw_file_detail as needle
				join vw_file_detail as haystack
					on (needle.sha1_hash=haystack.sha1_hash and needle.size=haystack.size)
			where
				needle.dir_path=basepath(%(_path)s) and needle.name=basename(%(_path)s)
		"""
		return pd.read_sql_query(sql, pg, params={'_path': path})  # Execute the search

	@staticmethod
	def search_duplicate_dir(pg, path: str) -> bool:
		pass

	@staticmethod
	def search_file_size(pg, path: str) -> bool:
		pass

	@staticmethod
	def search_timestamp(pg, path: str) -> bool:
		pass

	@staticmethod
	def search_date(pg, path: str) -> bool:
		pass

	@staticmethod
	def search_timestamp_range(pg, path: str) -> bool:
		pass
