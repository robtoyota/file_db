import psycopg2
import psycopg2.extras


class Pg:
	# connect to postgres
	@staticmethod
	def pg_connect(config: dict):
		# TODO: Change this to pooling: http://initd.org/psycopg/docs/pool.html
		connection = psycopg2.connect(
			host=config['POSTGRES']['host'],
			port=config['POSTGRES']['port'],
			dbname=config['POSTGRES']['dbname'],
			user=config['POSTGRES']['user'],
			password=config['POSTGRES']['password'],
			cursor_factory=psycopg2.extras.DictCursor,  # Return dicts instead of tuples
		)

		# Enable autocommit by default.
		# Some thoughts: Both the database design and the Python code is crafted to not require rollbacks.  Whenever
		# 	multiple statements in a transaction is require, they should be occurring within a database function.  This
		# 	also helps to enforce the practice of keeping the number of DB calls to a minimum by placing multiple
		# 	queries only within DB functions.  Any garbage data should be cleaned up by a cleanup function in the DB.
		connection.autocommit = True

		return connection
