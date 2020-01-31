from FileHandler.HashFile import HashFile
import os
import time
import shutil


class CopyFile:
	def __init__(
			self, id: int = 0, file_path: str = '', new_path: str = '', move_file: bool = False, overwrite: str = 'N',
			file_hash: str = None, hash_type: str = None, file_size: float = 0, perform_hash_check: bool = False
	):
		self.id = id
		self.file_path = file_path.strip()
		self.new_path = new_path.strip()
		self.move_file = move_file
		self.overwrite = overwrite.strip().upper()
		self.file_hash = file_hash.strip()
		self.hash_type = hash_type.strip().upper()
		self.file_size = file_size
		self.perform_hash_check = perform_hash_check

		self.error_code = None

		self.pre_copy_size = 0
		self.pre_copy_hash = None
		self.pre_copy_modified_date = None

		self.destination_size = 0
		self.destination_hash = None

	# Do the pre-copy file validation (exists), information collection (file size), and hash
	def pre_copy_check(self):
		# Check if the file exists
		if not os.path.exists(self.file_path):
			self.error_code = "SourceFileNotFound"
			return False

		self.pre_copy_size = os.path.getsize(self.file_path)  # Get the file size for validating the destination file
		self.pre_copy_modified_date = time.ctime(os.path.getmtime(self.file_path))  # Get when the file was last modified

		# Check the file size to see if the file has changed
		if self.file_size > 0 and self.file_size != self.pre_copy_size:
			self.error_code = "SourceFileSizeChanged"
			return False

		# TODO: Add a check for the file modified date - add a column to the DB table

		# Hash the source file, if required
		if self.perform_hash_check or self.hash_type:
			hash = HashFile.hash_file(self.file_path, ['SHA1', self.hash_type])
			self.pre_copy_hash = hash['SHA1']

			# Check if the hash of the source file matches the file that is intended to be copied
			try:
				if self.file_hash and hash[self.hash_type] != self.file_hash:
					self.error_code = "SourceFileHashChanged"
					return False
			except KeyError:  # File did not get hashed in the given hash_type
				pass

		# Check if the file exists in the destination location
		if os.path.exists(self.new_path) and self.overwrite != 'Y':
			self.error_code = "DestinationFileExists"
			return False

		# If everything passes, then return true
		return True

	# Copy the file and delete the source (if file is being moved)
	def perform_copy(self):
		# Do the pre-copy file checking, and make sure it passes
		if not self.pre_copy_check():
			return False

		# Perform the copy
		# TODO: Make this file-exists check and the file copy an atomic operation to avoid race conditions
		# TODO: Make this use the move() function instead of copy
		# TODO: Make sure the destination is valid (drive exists, and folders are created)
		if not os.path.exists(self.new_path) or self.overwrite == 'Y':  # Check if destination exists
			# https://stackoverflow.com/a/30359308/4458445
			shutil.copy2(self.file_path, self.new_path)  # Will overwrite destination by default
			# TODO: Insert the copied file into the DB

		# Make sure it copied correctly
		if not self.validate_destination():
			return False

		# Delete the source file if required
		# TODO: Remove the source file from the DB
		if self.move_file:
			# os.remove(self.file_path)
			pass

	# Make sure the copied file is valid (exists), and was copied correctly (file size, hash)
	def validate_destination(self):
		# Check the new file size to confirm it moved
		self.destination_size = os.path.getsize(self.new_path)  # Get the file size
		if self.destination_size != self.pre_copy_size:
			self.error_code = "CopyDestinationFileSizeDifference"
			return False

		# Hash the new file to confirm it moved, if required
		if self.perform_hash_check:
			hash = HashFile.hash_file(self.new_path, ['SHA1'])
			self.destination_hash = hash['SHA1']
			if self.destination_hash != self.pre_copy_hash:
				self.error_code = "CopyDestinationHashDifference"
				return False

		# If all is good, continue on...
		return True

	def handle_error(self):
		if self.error_code is not None:
			pass
