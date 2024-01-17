import re
from datetime import datetime
import pytz

def as_key_pairs(data: list=None, key_attr: str=None, value_attr:str=None):
  """Convert a recordset into a dictionary of key-value pairs
  
  :param data: dataset containing the values to convert
  :type data: recordset
  
  :param key_attr: name of the key that contains the values to use as keys
  :param key_attr: str
  
  :param value_attr: name of the key that contains the values that correspond to the keys
  :param value_attr: str
  
  :returns: a dictionary of key-values
  :rtype: dictionary
  """
  output = {}
  for row in data:
    output[row[key_attr]] = row.get(value_attr)
  return output

def flatten_data(data, nested_keys=None):
  """Flatten dataset by column
  
  :param data: recordset containing nested data (objects and arrays)
  :type data: recordset (i.e.,list of dictionaries)
  
  :param nested_keys: names of the nested keys that contain the data to extract
    that are formatted as a re search pattern (key1|key2|keyN)
  
  :returns: recordset without nested data
  :rtype: recordset
  """
  new_data = data
  for row in new_data:
    
    if '_href' in row:
      del row['_href']

    for column in row.keys():

      if isinstance(row[column], dict):
        if bool(row[column]):
          col_match = re.search(nested_keys, ','.join(row[column].keys()))
          
          if bool(col_match):
            row[column] = row[column][col_match.group()]
          else:
            print(f'Variable {column} is type "dict", but no target column found')

        else:
          row[column] = None
          
      if isinstance(row[column], list):
        if bool(row[column]):
          values = []
                   
          for nested_row in row[column]:
            col_match = re.search(nested_keys, ','.join(nested_keys.keys()))

            if bool(col_match):
              values.append(nested_row[col_match.group()])
            else:
              print(f'Variable {column} is type "list", but no target column found')

          if bool(values):
            row[column] = ','.join(values)
        else:
          row[column] = None
  return new_data


def print2(*args):
  """Print message with timestamp
  :param *args: one or more strings containing a message to print
  :type *args: str
  
  :returns: a message with a timestamp
  :rtype: str
  """
  msg=' '.join(map(str, args))
  time=timestamp(fmt='%H:%M:%S.%f')[:-2]
  print(f"[{time}] {msg}")


def recode_value(mappings: None={}, value: str=None, label: str=None, warn: bool=True):
  """Recode value based on key-value pairs

  :param mappings: an object containing where each key corresponds to a specific value
  :type mappings: dict
  
  :param value: the value to recode
  :type value: str

  :param label: string that indicates the mapping type for error messages
  :type label: str

  :param warn: If True (default), a message will be displayed when a value cannot be mapped
  :type warn: bool

  :example:
  mappings = {'First Name': 'firstname', 'Last Name': 'surname'}
  recode_value(mappings=mappings, value='Last Name', label='Name)

  :return: a new value
  :rtype: str or NoneType
  """
  try:
    return mappings[value]
  except:
    if bool(value) and warn:
      error = f'Error in {label if label else ""} recoding: \"{value}\" not found'
      print(error)
    return None

  
def timestamp(tz='Europe/Amsterdam', fmt='%Y-%m-%d'):
  """Get the time of the user's timezone and desired format

  :param tz: the timezone to format time to
  :param tz: str
  
  :param fmt: time format pattern

  :returns: current datetime based on timezone
  :rtype: str
  """
  return datetime.now(tz=pytz.timezone(tz)).strftime(fmt)
