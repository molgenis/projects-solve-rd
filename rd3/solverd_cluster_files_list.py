#//////////////////////////////////////////////////////////////////////////////
# FILE: cluster_listfiles.py
# AUTHOR: David Ruvolo
# CREATED: 2021-10-11
# MODIFIED: 2023-03-24
# PURPOSE: compile file metadata on the cluster and import into RD3
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: This script is designed to run in the cluster.
#//////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from rd3.utils.utils import toKeyPairs, recodeValue
from os import environ, listdir, path, stat
from dotenv import load_dotenv
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import hashlib
import sys
import csv
import re

sys.setrecursionlimit(100000)
# basePath = '/groups/solve-rd/prm10/'
basePath = '/groups/solve-rd/tmp10/releases/'
entryFolder = 'freeze3' # fill this in
outputDir='data/'

def getFileType(value):
  search = re.search(r'(.json|.vcf.|.ped)', value)
  if search:
    return search.group().replace('.','')
  else:
    print(f'Unknown file format in {value}')

# def md5(filepath):
#   with open(filepath, 'rb') as stream:
#     md5 = hashlib.md5(stream.read()).hexdigest()
#     stream.close()
#   return md5

def ls(dir):
  """List files recursively
  @param dir directory to check
  """
  files = []
  dir_contents = listdir(dir)
  for item in tqdm(dir_contents):
    item_path = dir + '/' + item
    if path.isdir(item_path):
      print('Processing files subdirectory',item_path)
      subdir_contents = ls(item_path)
      files = files + subdir_contents
    if path.isfile(item_path):
      file = {
        'filepath': item_path,
        'filename': path.basename(item),
        # 'filetype': path.splitext(item)[1].replace('.',''),
      }
      files.append(file)
  return files

# connect to molgenis
load_dotenv()
rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
# rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

# get file formats
fileTypes = [row['id'] for row in rd3.get('solverd_lookups_typeFile')]

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Build file metadata
# Look for files at the specified path and compile relevant metadata.

# set full path
dir = basePath + entryFolder
files = ls(dir)

# to pandas 
filedata = pd.DataFrame(files)


# recode filetypes
filedata['filetype'] = filedata['filename'].apply(
  lambda value: getFileType(value=value)
)

filedata[['filename','filetype']]

# format date created into molgenis datetime format
filedata['created'] = filedata['created'].replace(
  to_replace=r'\s',
  value = 'T',
  regex=True
)

filedata['created'] = filedata['created'].apply(lambda value: f"{value}Z" )

filedata.to_csv('../rd3_cluster_files_freeze.csv',index=False)