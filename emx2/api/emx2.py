#'////////////////////////////////////////////////////////////////////////////
#' FILE: molgenis_emx2_client.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-02-09
#' MODIFIED: 2022-02-23
#' PURPOSE: EMX2 API py client
#' STATUS: in.progress
#' PACKAGES: requests
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from urllib.parse import urlparse, urlunparse
from rd3.api.emx2.cli import cli
import rd3.api.emx2.graphql as graphql
import requests
import re

print2 = cli()

def cleanUrl(string):
  """Clean Urls
  @param url (str) : string containing a URL
  """
  value = urlparse(string)
  value = value._replace(path = re.sub(r'([\/]{2,})|([\/]{1}$)','', value.path))
  return urlunparse(value)

class Molgenis:
  def __init__(self, url):
    self.host = cleanUrl(url)
    self.session = requests.Session()
    
  def _post(self,**kwargs):
    """POST Wrapper
    Send a POST request to an EMX2 instance

    @param *args other parameters to pass down to session.post    
    @return response object
    """
    response = self.session.post(**kwargs)
    response.raise_for_status()
    return response
  
  def __validateDataResponse__(self,response):
    json = response.json()
    if response.status_code == 200:
      return json.get('data')
    else:
      print2.alert_error(
        'Failed to retrieve data\n', print2.text_error('ERROR:\n'),
        print2.text_error(json.get('errors')[0].get('message'))
      )
   
  def __validateImportResponse__(self,response,database,table):
    """Validate Import Response
    Make sure all import posts are valid
    
    @return response object
    """
    location=print2.text_value(database, '::', table)
    if response.status_code == 200:
      print2.alert_success('Imported data into',location)
    else:
      error = response.json().get('errors')[0].get('message')
      print2.alert_error(
        'Failed to import data into', location,
        print2.text_error('\nError:\n  ', error)
      ) 

  def signin(self, username, password):
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
        'query': graphql.signin,
        'variables': {'email': username, 'password': password}
      }
    )
    
    data = response.json().get('data', {}).get('signin', {})
    msg = f'{print2.text_value(self.host)} as {print2.text_value(username)}'
    
    if data.get('status') == 'SUCCESS':
      print2.alert_success('Signed into', msg)
      self.token = data.get('token')
    else:
      print2.alert_error(
        'Unable to sign into', msg, '\n',
        print2.text_error('ERROR:'), '\n  ',
        print2.text_error(data.get('message'))
      )
    return response

  
  def query(self, database, query):
    """Query
    Run a graphql query
    
    @param database name of the database to query
    @param query graphql query to run
    
    @return json or error message
    """
    response = self._post(url=f"{self.host}/{database}/api/graphql", json={'query': query})
    return self.__validateDataResponse__(response)
  
  def getSchema(self, database, table=None):
    """Get Schema
    Retrieve the schema of a database
    
    @param database the name of a database
    @param table if defined, response will be filtered for a specific table
    
    @return json
    """
    response = self._post(
      url=f"{self.host}/{database}/api/graphql",
      json={'query': graphql.schema}
    )
    data = self.__validateDataResponse__(response)
    if (table is not None) and bool(data):
      return [schematable for schematable in data['_schema']['tables'] if schematable['name'] == table][0]
    else:
      return data
      
  def importCsvFile(self, database, table, file):
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

    url=f"{self.host}/{database}/api/csv/{table}"
    response = self._post(url=url,data=databinary)
    self.__validateImportResponse__(response=response,database=database,table=table)
    return response
          
  def importData(self, database, table, data):
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
    self.__validateImportResponse__(response=response,database=database,table=table)
    return response
