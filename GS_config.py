import yaml
import os


with  open(os.path.join(os.path.dirname(__file__), "config.yml"), "r")  as file:
    cfg = yaml.safe_load(file)

DIR_ENV = dict()
DIR_ENV["root"] = cfg["root"]
DIR_ENV["data"] = cfg["root"] + cfg["data"]["path"]
DIR_ENV["py_data"] = cfg["root"] + cfg["data"]["py_data_path"]

DB_CONFIG = dict()
DB_CONFIG["name"] = cfg["DB"]["name"]
DB_CONFIG["user"] = cfg["DB"]["user"]
DB_CONFIG["password"] = cfg["DB"]["password"]
DB_CONFIG["host"] = cfg["DB"]["host"]
DB_CONFIG["port"] = cfg["DB"]["port"]

API_KEYS = dict()
API_KEYS["alphavantage"] = cfg["keys"]["alphavantage"]
