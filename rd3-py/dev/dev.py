#///////////////////////////////////////////////////////////////////////////////
# FILE: dev.py
# AUTHOR: David Ruvolo
# CREATED: 2024-01-17
# MODIFIED: 2024-01-17
# PURPOSE: debug functions in the lib
# STATUS: in.progress
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

import random
from datatable import dt

from rd3tools.src.rd3tools.utils import (
  as_key_pairs,
  flatten_data,
  print2,
  recode_value
)

from rd3tools.src.rd3tools.molgenis import Molgenis

# print message
print2('Test', 'tests')


# define data
data = [
  {'group': 'A', 'label': 'Group A', 'result': {'id': 'A-Q123'}},
  {'group': 'B', 'label': 'Group B', 'result': {'id': 'B-Q123'}},
  {'group': 'C', 'label': 'Group C', 'result': {'id': 'C-Q123'}},
  {'group': 'D', 'label': 'Group D', 'result': {'id': 'D-Q123'}},
]

# as pairs
pairs = as_key_pairs(data, 'group','label')

# recode values
recode_value(mappings=pairs, value='A', label='Group')
recode_value(mappings=pairs, value='B', label='Group')
recode_value(mappings=pairs, value='C', label='Group')
recode_value(mappings=pairs, value='D', label='Group')

# flatten data
data_flat=flatten_data(data=data, nested_keys='id')


# connect to database
db = Molgenis('')
db.login('','')

d = dt.Frame([
  {
    'id': str(random.random()).split('.')[1],
    'label': 'Test',
    'value': random.random()
  }
])

db.import_dt('', d)