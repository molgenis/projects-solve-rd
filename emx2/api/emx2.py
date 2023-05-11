#'////////////////////////////////////////////////////////////////////////////
#' FILE: molgenis_emx2_client.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-02-09
#' MODIFIED: 2023-05-11
#' PURPOSE: EMX2 API py client
#' STATUS: stable
#' PACKAGES: requests
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from urllib.parse import urlparse, urlunparse
from emx2.api.graphql import graphql
from emx2.api.cli import cli
import requests
import re

print2 = cli()

def cleanUrl(string:str=None):
  """Clean Urls
  @param url (str) : string containing a URL
  """
  value = urlparse(string)
  value = value._replace(path = re.sub(r'([\/]{2,})|([\/]{1}$)','', value.path))
  return urlunparse(value)

class Molgenis:
  def __init__(self, url:str=None):
    self.host = cleanUrl(url)
    self.session = requests.Session()
    
  def _post(self,**kwargs):
    """POST Wrapper
    Send a POST request to an EMX2 instance

    @param *kwargs other parameters to pass down to session.post    
    @return response object
    """
    try:
      response = self.session.post(**kwargs)
      response.raise_for_status()
    except requests.exceptions.HTTPError as error:
      errors = '\n'.join([err['message'] for err in response.json()['errors']])
      print2.alert_error(print2.text_error('ERROR:\n', errors))
    return response


  def signin(self, username:str=None, password:str=None):
    """Signin
    Sign in into an EMX2 instance with your username and password
    
    @param username your username
    @param password your password

    @return a status message
    """
    self.username = username
    response = self._post(
      url=f'{self.host}/apps/central/graphql',
      json={
        'query': graphql.signin(),
        'variables': {'email': username, 'password': password}
      }
    )
    
    data = response.json().get('data', {}).get('signin', {})
    msg = f'{print2.text_value(self.host)} as {print2.text_value(username)}'
    
    if data.get('status') == 'SUCCESS':
      print2.alert_success('Signed into', msg)
      self.token = data.get('token')
    return response


  def add(self, database:str=None, table:str=None, data:list=[]):
    """Add Data
    Import one or more records into a table. Row identifiers are required.
    
    @param database name of the database
    @param table name of the table to import data into
    @param data list of one or more dictionaries
    
    @return response
    """
    response = self._post(
      url=f"{self.host}/{database}/api/graphql",
      json={
        'query': graphql.insert(table=table),
        'variables': {'records': data}
      }
    )

    if response.status_code==200:
      print2.alert_success(
        'Added', print2.text_value(len(data)),
        f"record{'s'[:len(data)^1]} into",
        print2.text_value(f"{database}::{table}")
      )

    return response


  def delete(self, database:str=None, table:str=None, data:list=[]):
    """Delete data
    Delete one or more records from a table. Row identifiers are required
    
    @param database name of the database
    @param table name of the table to remove records from
    @param data a list of dictionaries containing the row identifiers
    
    @return response
    """
    response = self._post(
      url=f"{self.host}/{database}/api/graphql",
      json={
        'query': graphql.delete(table=table),
        'variables': {'records': data}
      }
    )

    if response.status_code==200:
      print2.alert_success(
        'Deleted', print2.text_value(len(data)),
        f"record{'s'[:len(data)^1]} from",
        print2.text_value(f"{database}::{table}")
      )

    return response


  def update(self, database:str=None, table:str=None, data:list=[]):
    """Update data
    Update one or more records from a table. Row identifiers are required
    
    @param database name of the database
    @param table name of the table
    @param data a list of dictionaries containing the row identifiers
    
    @return response
    """
    response = self._post(
      url=f"{self.host}/{database}/api/graphql",
      json={
        'query': graphql.update(table=table),
        'variables': {'records': data}
      }
    )

    if response.status_code==200:
      print2.alert_success(
        'Updated', print2.text_value(len(data)),
        f"record{'s'[:len(data)^1]} in",
        print2.text_value(f"{database}::{table}")
      )

    return response
  
  
  def query(self, database:str=None, query:str=None, variables: dict={}):
    """Query
    Run a graphql query
    
    @param database name of the database to query
    @param query graphql query to run
    
    @return json or error message
    """
    response = self._post(
      url=f"{self.host}/{database}/api/graphql",
      json={'query': query, 'variables': variables}
    )

    if response.status_code == 200:
      print2.alert_success('Successfully executed query')

    return response
  
  
  def getSchema(self, database: str=None, table: str=None):
    """Get Schema
    Retrieve the schema of a database
    
    @param database the name of a database
    @param table if defined, response will be filtered for a specific table
    
    @return json
    """
    response = self._post(
      url=f"{self.host}/{database}/api/graphql",
      json={'query': graphql.schema()}
    )
    
    data = response.json()['data']

    if (table is not None) and bool(data):
      return [schematable for schematable in data['_schema']['tables'] if schematable['name'] == table][0]
    else:
      return data
      

  def importCsvFile(self, database:str=None, table:str=None, file:str=None):
    """Import CSV File
    Import a csv file into a table
    
    @param database name of the database you wish to import data into
    @param table name of the table
    @param file path to the location to a csv file
        
    @examples
    ````
    from emx2 import Molgenis
    
    # connect to EMX2 database
    db = Molgenis(url='https://my-emx2-server.com', database='test')
    db.login(username='myusername', password='mypassword')
    
    # import file
    db.importCsvFile(database='myDatabase', table='myTable', file='data.csv')
    ````
    
    @return status message
    """
    with open(file, 'rb') as stream:
      databinary = stream.read()
      stream.close()

    response = self._post(
      url=f"{self.host}/{database}/api/csv/{table}",
      data=databinary
    )
    
    if response.status_code == 200:
      print2.alert_success(
        'Import data into',
        print2.text_value(f"{database}::{table}")
      )
    
    return response

    
  def importData(self, database:str=None, table:str=None, data:str=None):
    """Import Data
    Import a data object into a schema table

    @param database name of the database you wish to import data into
    @param table name of the table
    @param data stringified version of your data (a text/csv object)
        
    @example
    ````
    from emx2 import Molgenis
    import pandas as pd
    
    # connect to EMX2 database
    db = Molgenis(url='https://my-emx2-server.com')
    db.login(username='myusername', password='mypassword')
    
    # load data and stringify
    data = [...]
    data_str = data.to_csv(index=False)
    
    # import data
    db.importData(database='myDatabase', table='myTable', data=data_str)
    ````
    
    @return status message
    """
    response=self._post(
      url=f"{self.host}/{database}/api/csv/{table}",
      headers={'Content-Type': 'text/csv'},
      data=data
    )
    
    if response.status_code == 200:
      print2.alert_success(
        'Import data into',
        print2.text_value(f"{database}::{table}")
      )

    return response
