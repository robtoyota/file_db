from FileHandler.HashFile import HashFile
from datetime import datetime

class Hash:
	def __init__(
		self,
		id=None, file_id=None, md5_hash=None, md5_hash_time=None, sha1_hash=None, sha1_hash_time=None,
		file_path=None
	):
		self.id = id
		self.file_id = file_id
		self.md5_hash = md5_hash
		self.md5_hash_time = md5_hash_time
		self.sha1_hash = sha1_hash
		self.sha1_hash_time = sha1_hash_time

		self.file_path = file_path

	def perform_hash(self):
		# Attempt to perform the hash
		if hashes := HashFile.hash_file(self.file_path, ['MD5', 'SHA1']):
			# Populate the hashes within the object
			self.md5_hash = hashes['MD5']
			self.sha1_hash = hashes['SHA1']
			self.md5_hash_time = datetime.now()
			self.sha1_hash_time = datetime.now()

	def staging_table_dict(self):
		return {
			'file_id': self.file_id,
			'md5_hash': self.md5_hash,
			'md5_hash_time': self.md5_hash_time,
			'sha1_hash': self.sha1_hash,
			'sha1_hash_time': self.sha1_hash_time
		}

	@staticmethod
	def install_tables(pg, drop_tables):
		cur = pg.cursor()

		# Install the main hash table
		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists hash cascade;")

		cur.execute("""
			create table if not exists hash
			(
				id 				serial unique not null,
				file_id			int unique not null, 			-- ID from the file table
				md5_hash		text,
				md5_hash_time 	timestamp default null,
				sha1_hash		text,
				sha1_hash_time 	timestamp default null,
				primary key (id)
			);
		""")

		# Install the staging table (note: this is an unlogged table. Speed is needed more than data recovery on restart.)
		if drop_tables:
			# TODO: Check if this table contains data before dropping
			cur.execute("drop table if exists hash_stage cascade;")

		cur.execute("""
			create unlogged table if not exists hash_stage
			(
				file_id			int unique not null, 			-- ID from the file table
				md5_hash		text,
				md5_hash_time 	timestamp default null,
				sha1_hash		text,
				sha1_hash_time 	timestamp default null,
				primary key (file_id)
			);
		""")

		pg.commit()
		cur.close()

	@staticmethod
	def install_indexes(pg):
		with pg.cursor() as cur:
			cur.execute("""
				create index if not exists hash_file_id on hash (file_id);
				create index if not exists hash_md5_hash on hash (md5_hash);
				create index if not exists hash_md5_hash_time on hash (md5_hash_time);
				create index if not exists hash_sha1_hash on hash (sha1_hash);
				create index if not exists hash_sha1_hash_time on hash (sha1_hash_time);
			""")

	@staticmethod
	def install_foreign_keys(pg):
		pass
