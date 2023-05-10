#///////////////////////////////////////////////////////////////////////////////
# FILE: utils.py
# AUTHOR: David Ruvolo
# CREATED: 2023-05-10
# MODIFIED: 2023-05-10
# PURPOSE: misc functions
# STATUS: stable
# PACKAGES: NA
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from csv import QUOTE_ALL

def toTextCsv(data):
  return data.to_pandas().to_csv(index=False,encoding='UTF-8',quoting=QUOTE_ALL)