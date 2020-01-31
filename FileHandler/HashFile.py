import hashlib


class HashFile:
	@staticmethod
	def hash_file(file_path: str, hash_types: list) -> dict:
		# https://stackoverflow.com/a/22058673/4458445

		buffer_size = 128 * 64  # https://stackoverflow.com/a/1131238/4458445
		hash_output = {}

		# Build the hash objects
		if not isinstance(hash_types, list):  # Make sure hash_types is an iterable list
			hash_types = [hash_types]

		# Loop through each of the hash types and build the hashlib objects
		hashes = {}
		for hash_type in hash_types:
			if not isinstance(hash_type, str):  # Make sure the hash type is a string
				continue

			hash_type = hash_type.upper()

			if hash_type == 'MD5':
				hashes['MD5'] = hashlib.md5()
			elif hash_type.upper() == 'SHA1':
				hashes['SHA1'] = hashlib.sha1()

		try:
			# Load the file in to get hashed
			with open(file_path, 'rb') as f:
				while True:
					data = f.read(buffer_size)
					if not data:
						break

					# Loop through each of the different hash types and update the hash with the new chunk
					for hash_type, hash in hashes.items():
						hash.update(data)
		except (PermissionError, OSError):
			# TODO: What to do when the hashing fails because the file is inaccessible?
			pass
		except FileNotFoundError:
			# TODO: Delete the file from the DB if it is not found
			pass

		# Get the hash to be returned
		for hash_type, hash in hashes.items():
			hash_output[hash_type] = hash.hexdigest()

		return hash_output
