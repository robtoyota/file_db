from FilesDbDAL.Pg import Pg
from FilesDbDAL.DirectoryCrawl import DirectoryCrawl
from FilesDbDAL.File import File
from FilesDbDAL.Directory import Directory
from FilesDbDAL.Hash import Hash
from FilesDbDAL.SQLUtil import SQLUtil
from FilesDbDAL.Search import Search
from FilesDbDAL.FileHandler import FileHandler


class Install:
	def __init__(self, pg, drop_tables=False, drop_indexes=False):
		# !!! IMPORTANT !!!
		# Make sure that the order of installs follows the order of dependencies:
		# 	"directory" should not depend on other tables or their functions.
		# 	"file" should only depend on "directory".
		# 	"hash" should only depend on "file".
		# 	"control" and "staging" tables should only depend on their parent tables.
		# 	"SQLUtil" can rely on any table, so should always be installed last (except for the "base" DDLs)
		# 	"Search" can rely on any table or utility function/view so should always be installed last

		# Create the tables (do this first, and afterwards create the dependencies like functions, views, and FKs):
		print("Installing tables...")
		Directory.install_tables(pg, drop_tables)
		File.install_tables(pg, drop_tables)
		Hash.install_tables(pg, drop_tables)
		DirectoryCrawl.install_tables(pg, drop_tables)
		Search.install_tables(pg, drop_tables)
		FileHandler.install_tables(pg, drop_tables)

		# Create the base functions (these are dependencies used throughout other functions, views, and indexes):
		print("Installing base functions...")
		SQLUtil.install_base_functions(pg)

		# Create the indexes
		# Some index thoughts:
		# 	tl;dr: Use lots of indexes on file, directory, and hash, and few indexes on the staging and control tables.
		#
		# 	Indexes add a lot of expense to inserts/updates/deletes, but are essential for selecting.  This program
		# 	was designed to have an async thread feed staging tables mass data inserts (files and subdirs), and
		# 	have few indexes. Then another async thread calls a DB function to merge the staged data into the target
		# 	tables, which will have a lot of indexes for reading, and thus slower inserts.  The intention is to not
		# 	let the data changes in the tables with lots of indexes bottleneck the population of the staging tables.
		print("Installing indexes...")
		Directory.install_indexes(pg)
		File.install_indexes(pg)
		Hash.install_indexes(pg)
		DirectoryCrawl.install_indexes(pg)
		Search.install_indexes(pg)
		FileHandler.install_indexes(pg)

		# Create the base views (the objects' functions may depend on these views)
		print("Installing base views...")
		SQLUtil.install_base_views(pg)

		# Create the functions related to the objects (these functions might be used in views and FKs)
		print("Installing functions...")
		Directory.install_pg_functions(pg)
		File.install_pg_functions(pg)
		DirectoryCrawl.install_pg_functions(pg)
		SQLUtil.install_pg_functions(pg)
		Search.install_pg_functions(pg)
		FileHandler.install_pg_functions(pg)

		# Create the views
		print("Installing views...")
		SQLUtil.install_views(pg)

		# Create the foreign key relationships (nothing should rely on FKs during this install process)
		# TODO: Create the FKs
		print("Installing the foreign keys...")
		File.install_foreign_keys(pg)
		Hash.install_foreign_keys(pg)
		DirectoryCrawl.install_foreign_keys(pg)
		Search.install_foreign_keys(pg)
		FileHandler.install_foreign_keys(pg)

		# Insert the paths to crawl
		print("Inserting the paths to crawl...")
		if drop_tables:  # Only repopulate the tables if the tables were dropped
			with pg.cursor() as cur:
				for dp in [
					'C:\\',
					'D:\\',
					'E:\\',
					'F:\\',
					'G:\\',
					'H:\\',
					'I:\\',
					'K:\\',
					'M:\\',
					'S:\\',
					'T:\\',
					'X:\\',
				]:
					# Load all of these base directories into the directory table
					cur.execute("insert into directory (dir_path) values (%s)", (dp,))
					# Now schedule the dirs for crawling.
					cur.execute("select schedule_subdirs_in_directory_control(%s);", (dp,))



