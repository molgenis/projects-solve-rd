#//////////////////////////////////////////////////////////////////////////////
# FILE: cluster_listfiles.py
# AUTHOR: David Ruvolo
# CREATED: 2021-10-11
# MODIFIED: 2023-03-24
# PURPOSE: compile file metadata on the cluster
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: This script is designed to run in the cluster.
#//////////////////////////////////////////////////////////////////////////////

from os import listdir, path
from tqdm import tqdm
import pandas as pd
import sys, re

sys.setrecursionlimit(100000)
basePath = '/groups/solve-rd/tmp10/releases/'
entryFolder = 'freeze3' # fill this in

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
      file = {'filepath': item_path, 'filename': path.basename(item)}
      files.append(file)
  return files

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Build file metadata
# Look for files at the specified path and compile relevant metadata.

# set full path
dir = basePath + entryFolder
files = ls(dir)

# to pandas 
filedata = pd.DataFrame(files)

# save files and sync locally
# Download file to localy machine and continue
filedata.to_csv('../rd3_cluster_files_freeze.csv',index=False)
# rsync -av corridor+fender:rd3_cluster_files_freeze.csv .
