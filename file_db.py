import FilesDbDAL
from Process import Process
import sys


def main(config_file):
	# Get the processes started
	p = Process(config_file)
	p.start_crawling()


if __name__ == '__main__':
	# Use the default config file location
	in_config_file = None

	# Use the user-supplied config file
	if len(sys.argv) > 1:
		in_config_file = sys.argv[1]

	# Call the program
	main(in_config_file)



"""
Program flow:
* Manually do an initial insert into directory_control of the root/drives to crawl.
* Retrieves a list of dirs to crawl from directory_control
* The directories get scraped for a list of files and subdirs
* The files and subdirs get inserted into their "_staging" tables
* A stored proc then merges the data from the staging tables to the respective tables.
	The staging tables act as a snapshot of the dir.
"""