import json
import os


class Config:
	def __init__(self, config_file_name: str) -> None:
		self.values = Config.load_config(config_file_name)

	@staticmethod
	def load_config(file_name: str = None) -> dict:
		# Check if the specified file exists. If not, then revert to the default file
		if file_name is None or not os.path.isfile(file_name):
			file_name = 'config.json'  # Default file
		# Get the config
		return Config.read_json(file_name)

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
	def write_json_file(json_file: str, dom_values: dict) -> None:
		json_output = []
		for data in dom_values.values():
			json_output.append(data.__dict__)

		with open(json_file, 'w') as f:
			json.dump(json_output, f)
