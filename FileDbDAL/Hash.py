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

	def db_insert(self, pg) -> bool:
		# Insert this hash into the database, IF this file exists in the file table.
		with pg.cursor() as cur:
			cur.execute(
				"select * from hash_insert_if_file_exists(%s, %s, %s, %s, %s)", (
					self.file_path,
					self.md5_hash,
					self.md5_hash_time,
					self.sha1_hash,
					self.sha1_hash_time
				)
			)
			inserted = cur.fetchone()[0]
			return inserted

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

	@staticmethod
	def install_pg_functions(pg):
		with pg.cursor() as cur:
			cur.execute("""
				create or replace function upsert_hash 
				(
					_file_id int,
					_md5_hash text = null,
					_md5_hash_time timestamp = null,
					_sha1_hash text = null,
					_sha1_hash_time timestamp = null
				) 
				returns void
				as $$
				begin
					with ins as (
						insert into hash as t (file_id, md5_hash, md5_hash_time, sha1_hash, sha1_hash_time)
						values (_file_id, _md5_hash, _md5_hash_time, _sha1_hash, _sha1_hash_time)
						on conflict on constraint hash_file_id_key do -- Check if the file_id already exists 
							update set
								md5_hash = excluded.md5_hash,
								md5_hash_time = excluded.md5_hash_time,
								sha1_hash = excluded.sha1_hash,
								sha1_hash_time = excluded.sha1_hash_time
							where  -- Don't do empty updates
								t.md5_hash <> excluded.md5_hash
								or t.md5_hash_time <> excluded.md5_hash_time
								or t.sha1_hash <> excluded.sha1_hash
								or t.sha1_hash_time <> excluded.sha1_hash_time
					)
					-- Update the hash scheduler to avoid the file from getting hashed a second time, unless it changes
					update hash_control
					set mtime=now()
					where file_id=_file_id;
				end;
				$$ LANGUAGE plpgsql;
			""")

			cur.execute("""
				create or replace function hash_insert_if_file_exists 
				(
					_file_path text,
					_md5_hash text = null,
					_md5_hash_time timestamp = null,
					_sha1_hash text = null,
					_sha1_hash_time timestamp = null
				) 
				returns bool
				as $$
				declare
					_file_id int := null;
				begin
					-- Get the file_id for this file
					_file_id := (
						select file_id
						from vw_ll 
						where 
							dir_path=basepath(_file_path) and name=basename(_file_path) 
							and type='file'
					);
					
					-- Insert the new hash
					if _file_id > 0 then -- Check if the file exists
						perform upsert_hash(_file_id, _md5_hash, _md5_hash_time, _sha1_hash, _sha1_hash_time);
						return true;
					else
						return false;
					end if;
				end;
				$$ LANGUAGE plpgsql;
			""")
