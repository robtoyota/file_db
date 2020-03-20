from API.Util import Util


class Scrape:
	@staticmethod
	def scrape_dir(pg, path: str) -> bool:
		# TODO: Actually scrape the dir, instead of just reschedule it
		Scrape.schedule_scrape_dir(pg, path, '1900-01-01 00:00:00')  # Schedule the dir to get crawled ASAP
		return True

	@staticmethod
	def scrape_file(pg, path: str) -> bool:
		pass

	@staticmethod
	def schedule_scrape_dir(pg, path: str, next_crawl: str) -> bool:
		path = Util.sql_path_parse_wildcard_search(path)
		# Expects next_crawl to be a string in a datetime format (eg: 1900-01-01 00:00:00)
		with pg.cursor() as cur:
			cur.execute(
				"update directory_control set next_crawl=%s where dir_path ilike %s",
				(next_crawl, path)
			)
		return True

	@staticmethod
	def delete_file_from_db(pg, path: str) -> bool:
		path = Util.sql_path_parse_exact_search(path)

		with pg.cursor() as cur:
			# Call the function to delete the file row
			cur.execute(
				"select id from delete_file(%s::text);",
				(path, )
			)

			# If no ID was returned, it means no row was deleted
			if not cur.rowcount:
				return False

			return True

	@staticmethod
	def delete_dir_from_db(pg, path: str, delete_subdirs: bool = False) -> bool:
		path = Util.sql_path_parse_exact_search(path)

		with pg.cursor() as cur:
			# Call the function to delete the directory row
			cur.execute(
				"select id from delete_directory(%s::text, %s::bool);",
				(path, delete_subdirs)
			)

			# If no ID was returned, it means no row was deleted
			if not cur.rowcount:
				return False

			return True
