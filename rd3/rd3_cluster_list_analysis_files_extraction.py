#///////////////////////////////////////////////////////////////////////////////
# FILE: rd3_cluster_list_analysis_files_extraction.py
# AUTHOR: David Ruvolo
# CREATED: 2022-09-15
# MODIFIED: 2022-09-19
# PURPOSE: List available results files by working group
# STATUS: stable
# PACKAGES: NA
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Script to run on the cluster.
# This code block should be copied and run on the cluster. Remember to change
# the entryFolder

from os import path, listdir, stat
import sys
import csv

sys.setrecursionlimit(100000)
basePath = '/groups/solve-rd-/prm10/'
entryFolder = '' # fill this in

def ls(dir):
  """List files recursively
  @param dir directory to check
  """
  files = []
  dir_contents = listdir(dir)
  for item in dir_contents:
    item_path = dir + '/' + item
    if path.isdir(item_path):
      print('Processing files subdirectory',item_path)
      subdir_contents = ls(item_path)
      files = files + subdir_contents
    else:
      file = {
        'inode': stat(item_path).st_ino,
        'group': entryFolder,
        'ern': None,
        'name': path.basename(item),
        'path': item_path,
        'extension': path.splitext(item)[1].replace('.',''),
        'size': path.getsize(item_path)
      }
      files.append(file)
  return files


# set starting directory
dir = path.abspath('rd3')
files = ls(dir)

# write results to csv
outputFile = 'rd3_' + entryFolder + '_contents.csv'
with open(outputFile, 'w') as file:
  columns = ['inode','group','ern','name','path','extension','size']
  fileCsv = csv.DictWriter(file, fieldnames=columns, quoting=csv.QUOTE_ALL)
  fileCsv.writeheader()
  fileCsv.writerows(files)
