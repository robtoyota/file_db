import FileDbDAL as DALHash


class Hash:
	@staticmethod
	def hash_file(pg, file_path: str, insert_db: bool = False) -> dict:
		# Hash the file
		h = DALHash.Hash(file_path=file_path)
		h.perform_hash()

		# Insert into the DB
		if insert_db:
			h.db_insert(pg)

		return {
			'md5_hash': h.md5_hash,
			'md5_hash_time': h.md5_hash_time,
			'sha1_hash': h.sha1_hash,
			'sha1_hash_time': h.sha1_hash_time
		}



	@staticmethod
	def hash_dir(pg, dir_path: str) -> bool:
		pass
