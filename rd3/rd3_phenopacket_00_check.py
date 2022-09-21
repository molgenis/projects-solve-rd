#///////////////////////////////////////////////////////////////////////////////
# FILE: rd3_phenopacket_00_check.py
# AUTHOR: David Ruvolo
# CREATED: 2022-09-21
# MODIFIED: 2022-09-21
# PURPOSE: Check Phenopacket files
# STATUS: stable
# PACKAGES: os, json, re
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from os import listdir, path
import json
import re

# set path based on target release
currentRelease = ''
path = f'/groups/solve-rd/tmp10/releases/{currentRelease}/phenopackets'
files = listdir(path)

availableFiles = [
  {'name': file, 'path': f"{path}/{file}"}
  for file in files
  if re.search(r'(.json)$', file)  
]

print('Found', len(availableFiles), 'phenopacket files')

# test files
unreadableFiles = []
for file in availableFiles:
  try:
    with open(file['name'], 'r') as f:
      data = json.load(f)
    f.close()
  except:
    unreadableFiles.append(file)
    
if unreadableFiles:
  print('Found', len(unreadableFiles), 'unreadable files. Saving a list of files....')
  with open(f"{currentRelease}_corrupt_phenopacket.txt", 'w') as f:
    for row in unreadableFiles:
      f.write(f"{row['name']}\n")
    f.close()
else:
  print('No unreadable files found')