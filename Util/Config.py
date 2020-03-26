import json
import os


class Config:
	# Call these static methods to get a dict returned
	
	def __init__(self, config_file_name: str) -> None:
		self.values = Config.load_config(config_file_name)

	@staticmethod
	def load_config(file_name: str = None) -> dict:
		file_name = Config.parse_config_path(file_name)
		# Get the config
		return Config.read_json(file_name)

	@staticmethod
	def write_config(config_vals: dict, file_name: str = None) -> None:
		file_name = Config.parse_config_path(file_name)
		Config.write_json(file_name, config_vals)

	@staticmethod
	def read_json(file_path: str, create_file: bool = False) -> dict:
		# Default to an empty dict
		data = {}
		# Load the data from the json file
		try:
			with open(file_path) as json_file:
				data = json.load(json_file)
		except FileNotFoundError:  # If file does not exist, then create it
			if create_file:
				open(file_path, 'a').close()
		except json.decoder.JSONDecodeError:  # If the file is not a valid JSON file
			pass
		# Return the json values
		return data

	@staticmethod
	def write_json(file_path: str, config_vals: dict) -> None:
		with open(file_path, 'w') as f:
			json.dump(config_vals, f, indent=4)

	@staticmethod
	def parse_config_path(file_name: str = None) -> str:
		# Check if the specified file exists. If not, then revert to the default file
		if file_name is None or not os.path.isfile(file_name):
			file_name = 'config.json'  # Default file
		return file_name
