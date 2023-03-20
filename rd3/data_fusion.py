
from dotenv import load_dotenv
from os import environ
import requests
load_dotenv()

fusion = requests.Session()

data = fusion.get(
  f"{environ['FUSION_URL']}/search?queryString=*",
  headers = { 'Authorization': environ['FUSION_API'] }
)