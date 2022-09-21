#///////////////////////////////////////////////////////////////////////////////
# FILE: rd3_ped_00_check.py
# AUTHOR: David Ruvolo
# CREATED: 2022-09-21
# MODIFIED: 2022-09-21
# PURPOSE: Check files to make sure they aren't corrupt
# STATUS: stable
# PACKAGES: os, re
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from os import listdir, path
import re

# set current release and list files at given path
currentRelease = ''
path = f"/groups/solve-rd-tmp10/releases/{currentRelease}/ped"
files = listdir(path)

availableFiles = [
  {'name': file, 'path': f"{path}/{file}"}
  for file in files
  if re.search(r'(.ped)$', file)
]

print('Found', len(availableFiles), 'PED files')

# test files and save a list of corrupt files (if any)
unreadableFiles = []
for file in availableFiles:
  print('Testing file', file['name'])
  try:
    with open(file['path'], 'r', encoding='utf-8') as file:
      data = file.readlines()
    file.close()
  except:
    unreadableFiles.append(file)

if unreadableFiles:
  print('Found', len(unreadableFiles), 'corrupt files. Saving a list of file names....')
  with open(f"{currentRelease}_corrupt_ped_files.txt", 'w') as f:
    for row in unreadableFiles:
      f.write(f"{row}\n")
    f.close()
else:
  print('No corrupt files found')