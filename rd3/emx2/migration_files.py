"""
Migrate file metadata from EMX1 to EMX2
"""

from molgenis_emx2_pyclient import Client
from os import environ
from dotenv import load_dotenv
import molgenis.client
# from rd3tools.utils import flatten_data
import pandas as pd
load_dotenv()
