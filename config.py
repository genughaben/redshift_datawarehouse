import configparser
import os
from dotenv import load_dotenv

CWD_PATH = os.path.dirname(__file__)
load_dotenv(os.path.join(CWD_PATH, ".aws"))

config = configparser.ConfigParser()
config.read(os.path.join(CWD_PATH, "dwh.cfg"))

config.add_section("AWS")
config.set("AWS", "KEY", os.environ.get("KEY"))
config.set("AWS", "SECRET", os.environ.get("SECRET"))