from API.Util import Util
from FileDbDAL.DirectoryCrawl import DirectoryCrawl


class Schedule:
	@staticmethod
	def reschedule_dir(pg, path: str, frequency: int = None) -> bool:
		pass

	@staticmethod
	def view_scrape_schedule(pg, path: str, recursive: bool = False, order_by: str = '', row_limit: int = 100) -> list:
		with pg.cursor() as cur:
			# Make the input path searchable with wildcards
			path = Util.sql_path_parse_wildcard_search(path)

			# Add wildcard to directory path to make it recursive, unless user has already added it
			if recursive and not path[-1:] == '%':
				path += "%"

			# Sanitize the order by
			valid_order_cols = [
				'dir_path', 'file_count', 'subdir_count', 'next_crawl',
				'crawl_frequency', 'last_crawled', 'last_active', 'inserted_on'
			]
			if order_by := Util.sanitize_order_by(order_by, valid_order_cols) == "":
				order_by = "dir_path asc"

			# Sanitize the row_limit
			if row_limit < 1:
				row_limit = 1

			# Execute the query to get the list
			# TODO: Convert this to a paginated function, and allow for parameterised order_by
			cur.execute(f"""
				select
					dir_path, dir_id, file_count, subdir_count, next_crawl, crawl_frequency,
					process_assigned_on, last_crawled, last_active, inserted_on
				from directory_control
				where dir_path ilike %s
				order by {order_by}
				limit %s
				""",
				(path, row_limit)
			)

			# Build the list to be returned
			rows = []
			for row in cur:
				# Build the object for the DB row
				rows.append(DirectoryCrawl(db_row=row))

			return rows
