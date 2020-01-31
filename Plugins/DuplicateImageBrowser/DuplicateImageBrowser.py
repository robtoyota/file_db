from FileDbDAL.Pg import Pg
from PIL import Image
from tkinter import *

import sys
import traceback


class DuplicateImageBrowser(Frame):
	def __init__(self, pg, master_window = None):
		# Gist: https://gist.github.com/nakagami/3764702
		# Initialize the TKinter window
		Frame.__init__(self, master_window)
		self.master.title('Duplicate Image Browser')

		window_frame = Frame(self)
		# ###########################

		# Grab the postgres connection object
		self.pg = pg
		# Load the list of duplicates to work with
		self.duplicates = DuplicateFinder.get_files(pg, 1000, 'E%')


class DuplicateFinder:
	@staticmethod
	def get_files(pg, limit: int = 1000, dir_path: str = '%'):
		# Get the list of duplicate hashes and their files
		duplicate_hashes = {}
		try:
			with pg.cursor() as cur:
				#  Select the duplicate images
				cur.execute("""
						with dupe_hashes as (  -- Get the list of duplicated hashes
							select 
								sha1_hash, size, count(*) as duplicate_count
							from vw_f
							where 
								dir_path like %s
								and size > 0.010  -- 10 KBs
								and extension(name) in ('jpg', 'jpeg', 'png', 'bmp', 'gif', 'tiff', 'webp')
							group by sha1_hash, size
						)
						-- Get the corresponding files for those hashes
						select
							f.sha1_hash, f.size, d.duplicate_count,
							f.full_path, f.name, f.ctime, f.mtime
						from
							vw_f f
							join dupe_hashes d
								on (f.sha1_hash=d.sha1_hash)
						order by f.sha1_hash
							limit %s;
					""",
					(dir_path, limit,)
				)

				# Populate the list of hashes with what was found
				last_hash = ''
				for row in cur:  # SQL returns ordered by sha1_hash
					# Instantiate a new Hash to contain all the duplicated files
					if last_hash == row['sha1_hash']:
						last_hash = row['sha1_hash']
						duplicate_hashes[row['sha1_hash']] = Hash(
							sha1_hash = row['sha1_hash'],
							file_size = row['size'],
							duplicate_count = row['duplicate_count'],
						)

					# Add the duplicate file to the hash's list of files
					duplicate_hashes[row['sha1_hash']].append_file(
						full_path = row['full_path'],
						name = row['name'],
						ctime = row['ctime'],
						mtime = row['mtime'],
					)
		except:  # Ugh:
			print(str(sys.exc_info()))
			traceback.print_exc(file=sys.stdout)

		return duplicate_hashes

class Hash:
	def __init__(self, sha1_hash, file_size, duplicate_count):
		self.sha1_hash = sha1_hash
		self.file_size = file_size
		self.duplicate_count = duplicate_count

		self.files = []

	def append_file(self, full_path, name, ctime, mtime):
		# Append a File instance to the list of files
		self.files.append(
			File(full_path, name, ctime, mtime)
		)


class File:
	def __init__(self, full_path, name, ctime, mtime):
		self.full_path = full_path
		self.name = name
		self.ctime = ctime
		self.mtime = mtime


if __name__ == '__main__':
	with Pg.pg_connect() as pg:
		DuplicateImageBrowser(pg)