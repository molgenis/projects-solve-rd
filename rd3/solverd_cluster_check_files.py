#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_cluster_check_files.py
# AUTHOR: David Ruvolo
# CREATED: 2023-01-23
# MODIFIED: 2023-01-23
# PURPOSE: Check files a specified paths to see if they are corrupted/valid
# STATUS: in.progess
# PACKAGES: os, re
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from os import listdir, path
from datatable import dt, f
from tqdm import tqdm
import re

def isCorrupt(file):
  try:
    with open(file, 'r', encoding='utf-8') as stream:
      data = stream.readlines()
    stream.close()
    return False
  except:
    return True

# specify paths
paths = [
  '/Users/davidcruvolo/Desktop/RD3/SolveRD_DF1-3_PEDs',
  '/Users/davidcruvolo/Desktop/RD3/SolveRD_NovelOmics_PEDs',
  '/Users/davidcruvolo/Desktop/RD3/SolveRD_DF1-3_Phenopackets',
  '/Users/davidcruvolo/Desktop/RD3/SolveRD_NovelOmics_Phenopackets'
]


# list all available files at each path
files = dt.Frame()
for p in tqdm(paths):
  dircontents = listdir(path.abspath(p))
  for file in dircontents:
    if re.search('(ped|json)$', file):
      files = dt.rbind(
        files,
        dt.Frame([{ 'name': file, 'path': f"{p}/{file}" }])
      )

del p, dircontents, file

# identify unreadable files
files['isCorrupt'] = dt.Frame([
  isCorrupt(file)
  for file in tqdm(files['path'].to_list()[0])
])

# save logs
corruptFiles = files[f.isCorrupt, :]
if corruptFiles.nrows > 0:
  print('Found', len(corruptFiles.nrows), 'corrupt files. Saving info....')
  corruptFiles.to_csv('corrupt_files.csv')
else:
  print('No corrupt files found! :-)')