class Util:
	# Split the input arguments into individual arguments
	@staticmethod
	def parse_args(args) -> list:
		# TODO: Parse this properly
		# TODO: strip() all elements
		arg_list = args.split(' ', 1)
		return arg_list

	# Convert a user-supplied value to a boolean True/False
	@staticmethod
	def input_parse_bool(val) -> bool:
		# Convert and sanitize the value
		try:
			val = str(val).strip()
		except:  # Ugh
			return False

		# Values that define True/False (entries should be lower case)
		true_vals = ['true', 't', 'yes', 'y', '1']
		false_vals = ['false', 'f', 'no', 'n', '0']

		if val in true_vals:
			return True
		elif val in false_vals:
			return False
		else:
			return False  # Default to False

	# Replace "*" wildcards in a user-supplied file/dir path, to the SQL wildcard "%"
	@staticmethod
	def path_parse_wildcard_search(path: str) -> str:
		# Escape back slashes
		path = path.replace("\\", "\\\\")
		# Escape the "%" wildcard, because it is a valid character in file/dir names
		path = path.replace("%", "\\%")
		# Convert system wildcards ("*") with the SQL wildcard
		path = path.replace("*", "%")
		return path

	# sanitize_order_by
	@staticmethod
	def sanitize_order_by(sql: str, valid_columns: list) -> str:
		# Convert and sanitize the input
		try:
			sql = str(sql).strip()
		except:  # Ugh
			return ''

		sql_list = sql.split(',')  # Get a list of the columns
		return_sql = []  # Build the output SQL
		for col in sql_list:  # Loop through each column
			if not col:  # Blank value?
				continue

			# Get the column name and optionally the ASC|DESC value
			col = col.split()

			if len(col) > 2:  # too many arguments
				continue

			if len(col) == 2:
				if not col[1].lower() in ['asc', 'desc']:  # Can only order by asc/desc
					continue

			if not col[0] in valid_columns:  # Illegal column (not allowed to be ordered by)
				continue

			# Column passed sanitization
			return_sql.append(f"{col[0]} {col[1]}".strip())

		# Return the sanitized list of order by columns
		return ",".join(return_sql).strip()
