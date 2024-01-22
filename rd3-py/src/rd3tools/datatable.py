
from datatable import dt
import pandas as pd

def unique_values_by_id(
    data: dt.Types.dt=None,
    group_by: str=None,
    column: str=None,
    drop_duplicates: bool=True,
    key_group_by: bool=True
):
  """Collapse all unique values in a column by group into a comma separated string.

  :param data: dataset containing the data to collapse
  :type data: datatable
  
  :param group_by: name of the column that will serve as the grouping variable
  :type group_by: str
  
  :param column: name of the column that contains the values to collapse
  :type column: str
  
  :param dropDuplicates: If True, all duplicate rows will be removed
  :type dropDuplicates: bool
  
  :param keyGroupBy: If True, returned object will be keyed using the value named in groupby
  :type keyGroupBy: bool
  
  
  :returns: dataset with a new column where values are collapsed by a group
  :rtype: datatable
  """
  data_pd = data.to_pandas()
  data_pd[column] = data_pd.dropna(subset=[column]) \
    .groupby(group_by)[column] \
    .transform(lambda val: ','.join(set(val)))
  
  if drop_duplicates:
    data_pd = data_pd[[group_by, column]].drop_duplicates()
    
  output = dt.Frame(data_pd)
  
  if key_group_by:
    output.key = group_by

  return output