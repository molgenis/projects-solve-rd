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
from rd3.utils.cli import cli
import requests
import re

def cleanUrl(string):
  """Clean Urls
  @param url (str) : string containing a URL
  """
  value = urlparse(string)
  value = value._replace(path = re.sub(r'([\/]{2,})|([\/]{1}$)','', value.path))
  return urlunparse(value)

class Molgenis:
  def __init__(self, url,):
    self.username = None
    self.user_info = None
    self.database = None
    self.token = None
    self.signedIn = False
    self.url = cleanUrl(url)
    self.url_login = f'{self.url}/apps/central/graphql'
    self.url_db = f'{self.url}/{self.database}'
    self.url_db_api = f'{self.url_db}/api'
    self.url_db_graphql = f'{self.url_db}/graphql'
    self.session = requests.Session()
    self.cli = cli()
      
  def login(self, username, password):
    """Login
    Log in into an EMX2 instance with your username and password
    
    @param username: your username
    @param password: your password

    @return a status message
    """
    self.username = username
    variables = {'email': username, 'password': password}
    query = """
      mutation($email:String, $password: String) {
        signin(email: $email, password: $password) {
          status
          message
          token
        }
      }
    """
    response = self.session.post(
      url=self.url_login,
      json={'query': query, 'variables': variables}
    )
    response.raise_for_status()
    
    data = response.json().get('data', {}).get('signin', {})
    msg = f'{self.cli.text_value(self.url)} as {self.cli.text_value(username)}'
    
    if data.get('status') == 'SUCCESS':
      self.cli.alert_success('Signed into', msg)
      self.token = data.get('token')
    else:
      self.cli.alert_error(
        'Unable to sign into', msg, '\n',
        self.cli.text_error('Error:'), '\n  ',
        self.cli.text_error(data.get("message"))
      )
  
  def POST(self, url, **kwargs):
    response=self.session.post(url=url, **kwargs)
    response.raise_for_status()
    return response
      
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
    db.importCsvFile(database='myDatabase', table='myTable', filename='data.csv')
    ````
    
    @return status message
    """
    url=f'{self._url_db_api}/csv/{table}'
    with open(file, 'rb') as data:
      dataBinary = data.read()
      data.close()

    response=self.POST(url=url, data=dataBinary)

    location=self.cli.text_value(database, '::', table)
    if response.status_code == 200:
      self.cli.alert_success('Imported data into',location)
    else:
      self.cli.alert_error('Failed to import data into',location)
              
          
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
    location=self.cli.text_value(database, '::', table)
    response=self.session.post(
      url=f"{self.url}/{database}/api/csv/{table}",
      headers={'Content-Type': 'text/csv'},
      data=data
    )
    if response.status_code == 200:
      self.cli.alert_success('Imported data into',location)
    else:
      error = response.json().get('errors')[0].get('message')
      self.cli.alert_error(
        'Failed to import data into', location,
        self.cli.text_error('\nError:\n  ', error)
      )
    return response
