__version__ = '0.1.0'

import os
import json
from datetime import datetime
# import typer
#
# import ray
# from ray import serve
# from fastapi import FastAPI
#
from cineplex.config import Settings

settings = Settings()

os.makedirs(settings.tmp_dir, exist_ok=True)
os.makedirs(settings.data_dir, exist_ok=True)


#
# API
#

# api = FastAPI()
