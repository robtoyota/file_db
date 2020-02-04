#Query your file system
The __file_db__ tool loads the metadata of your files and directories into a PostgreSQL database, to allow for easy analysis and control of your file system. There is a bundled interactive shell for ease of use, or power users can hook directly into the database with customer applications and plugins, or even query the database directly with SQL.

#Features
* Load file system metadata into PostgresSQL (file/directory names, sizes, checksums, etc).
* Query the data to observe trends, identify duplicate files, track file system changes, and more. 
* Perform mass file copy/move/delete operations by inserting rows into a table.
* Crawls directories for new/changed files on an intelligent schedule.

#Examples:
###Find Duplicate Files
__file_db__ collects a file's size and checksum hash (both MD5 and SHA-1), which can be used to compare files for duplicates. You can search for duplicates of a single file, or of a list of files (such as the contents of a directory), using the included user interface. Software developers can also leverage this feature through database functions, or direct SQL.   

__Search for the duplicates for a single file:__

* __Via the interactive shell:__  
  * Input: `search duplicate_file "C:\my_file.txt"`  
  * Output: 
  
* __Via SQL functions:__
  * Input: `SELECT * FROM search_duplicate_file('C:\my_file.txt')`
  * Output: 
  
* __Direct SQL:__
  * Input: 
    ```sql
    SELECT * 
    FROM
        -- Pull the list of files to find the meta data of the comparison file
        vw_f src
        -- Join all the files to the source file by SHA-1 hash and the file size
        INNER JOIN vw_f dup
            on (src.sha1_hash=dup.sha1_hash and src.file_size=dup.file_size)
    WHERE
        src.full_path='C:\my_file.txt'
    ```
  * Output: