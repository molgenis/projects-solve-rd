
from rd3.api.molgenis import Molgenis
from datatable import dt, f

from dotenv import load_dotenv
from os import environ
load_dotenv()

rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

files = dt.Frame(
  rd3.get(
    'rd3_freeze1_file',
    batch_size=10000,
    attributes='EGA,experimentID'
  )
)

del files['_href']