from FileDbDAL.Pg import Pg
import re

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

	# Replace operating system wildcards (* and ?) in a user-supplied file/dir path, to the SQL wildcard (% and _).
	# Escape existing wildcards and backslashes
	@staticmethod
	def sql_path_parse_wildcard_search(path: str) -> str:
		# !! Important: Update the DB function as well (SQLUtil.py)

		path = path.strip()
		# Escape back slashes, because this is a pattern, not a literal string
		path = path.replace("\\", "\\\\")  # A backslash escapes quotes, so raw strings won't support: r'\'
		# Escape the existing wildcards to avoid accidental use, because they are valid characters in file/dir names
		path = path.replace("%", r"\%")
		path = path.replace("_", r"\_")
		# Swap the operating system's wildcards (* and ?) with the SQL wildcards (%/?)
		path = path.replace("*", "%")
		path = path.replace("?", "_")
		return path

	@staticmethod
	def sql_path_parse_exact_search(path: str) -> str:
		# !! Important: Update the DB function as well (SQLUtil.py)

		# Remove trailing slashes (/home/ -> /home). However, allow for drives (stored in the DB as C:\)
		path = Util.strip_trailing_slashes(path)
		return path

	# sanitize_order_by
	@staticmethod
	def sql_sanitize_order_by(sql: str, valid_columns: list) -> str:
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

	@staticmethod
	def path_os(path: str) -> str:
		# Looks at a path (eg "/home", or "C:\file.txt") to see if it is Windows (win) or *nix (nix)
		# If the input is not a valid path, make a best guess at the OS and append "?" to the returned value

		# Simply check if the path starts with a drive letter or a slash
		path = path.strip()
		if path[0] == "/" and not "\\" in path:
			return "nix"
		elif path[0].isalpha() and path[1] == ":" and not "/" in path:
			return "win"

		# If the path is missing the drive/parent prefix, guess at the OS by checking for forward or backslash
		elif "/" in path and not "\\" in path:
			return "nix?"
		elif "\\" in path and not "/" in path:
			return "win?"

		# If nothing can be determined from the path, then return nothing
		else:
			return ""


	@staticmethod
	def is_nix_path(path: str, fuzzy_check: bool) -> bool:
		os = Util.path_os(path)
		if os == "nix" or (os == "nix?" and fuzzy_check):
			return True
		else:
			return False

	@staticmethod
	def is_win_path(path: str, fuzzy_check: bool) -> bool:
		os = Util.path_os(path)
		if os == "win" or (os == "win?" and fuzzy_check):
			return True
		else:
			return False

	@staticmethod
	def path_separator(path: str):
		# Determine the path separators ("/" or "\")
		if Util.is_win_path(path, fuzzy_check=False):  # A Windows path is the most definitive (drive "C:")
			separator = "\\"
		elif Util.is_nix_path(path, fuzzy_check=False):  # A *nix path just starts with a "/"
			separator = "/"
		else:
			# If this is an incomplete path, do a fuzzy OS check (checks for "\" or "/" in the string)
			if Util.is_win_path(path, fuzzy_check=True):  # Check if the fuzzy guess is for a Windows separator
				separator = "\\"
			else:  # If the OS cannot be determined, default to *nix. Windows paths are forgiving with "/" as well as "\"
				separator = "/"
		return separator

	@staticmethod
	# Join a path together, similar to os.path.join(*args)
	def path_join(*path_slices: str) -> str:
		# This is a custom function because os.path.join() uses the user's OS's directory delimiter ("\" for Windows while
		# others user "/"), but the DB data might be from a different OS than the user's machine. So use the path slices
		# that the user supplies to figure out whether to use "/" or "\" to join.

		# Check if you can return early
		if len(path_slices) == 0:  # At least one slice of the path needs to be supplied
			return ""
		elif len(path_slices) == 1:  # If there's nothing to join, then take the rest of the day off. You deserve it.
			return path_slices[0]

		# Join the path slices together
		separator = Util.path_separator("".join(path_slices))  # This might be an incomplete path, so check all slices
		return separator.join(path_slices)

	# Strip any trailing slashes. Eg convert "/home/test/" to "/home/test"
	@staticmethod
	def strip_trailing_slashes(path: str) -> str:
		#  !! Important: Update the DB function as well (SQLUtil.py)
		path = path.strip()
		return re.sub(r'([^:])?[\\|/]+$', r'\1', path)


	@staticmethod
	def dir_in_db(pg, path: str) -> bool:
		path = Util.sql_path_parse_exact_search(path)  # Bring the path in line with how the data is stored in the DB

		with pg.cursor() as cur:
			cur.execute("select * from dir_path_exists(%s)", (path,))  # Query the DB
			exists = cur.fetchone()[0]
			return exists
