#///////////////////////////////////////////////////////////////////////////////
# FILE: graphql.py
# AUTHOR: David Ruvolo
# CREATED: 2023-05-10
# MODIFIED: 2023-05-12
# PURPOSE: commonly used graphql queries
# STATUS: stable
# PACKAGES: NA
# COMMENTS: Import file as 'import path.to.graphql.file as graphql'
#///////////////////////////////////////////////////////////////////////////////

from enum import Enum

class Operations(Enum):
  """EMX2 Graphql operations"""
  insert = "insert"
  update = "update"
  save = "save"
  delete = "delete"

class graphql:
  
  @staticmethod
  def signin():
    """signin
    Mutation to sign into an EMX2 molgenis instance. Function must include
    username and password in the posted data.
    
    @example
    ```py
    import requests
    requests.post(...,
      json={
        'query': graphql.query,
        'variables': {'username': ..., 'password': ...}
      }
    )
    
    ```
    """
    return """
      mutation($email:String, $password: String) {
        signin(email: $email, password: $password) {
          status
          message
        }
      }
    """
  
  @staticmethod
  def signout():
    """Signout"""
    return """
      mutation {
        signout {
          status
          message
        }
      }
    """

  @staticmethod
  def schema():
    """schema
    Default query for schema
    """
    return """ {
      _schema {
        name
        tables {
          name
          id
          externalSchema
          inherit
          tableType
          columns {
            name
            id
            position
            columnType
            key
            required
            refTable
            refLink
            refBack
            refLabel
            validation
            visible
            semantics
          }
        }
      }
    }
    """

  def _operation(operation:str=None, table:str=None):
    """Operation
    Wrapper around operations (update, delete, save, insert)
    
    @param operation choose insert, update, save, or delete
    @param table name of the table (passed from parent method)
    
    @return string; graphql string
    """
    if not hasattr(Operations, operation):
      raise ValueError('Invalid operation used in query')

    return (
      "mutation " + operation + "($records:["+ table + "Input]) {\n"
      "  " + operation + "(" + table + ":$records) {\n"
      "    status\n"
      "    message\n"
      "  }\n"
      "}"
    )

  def delete(table:str=None):
    """Delete    
    Query to remove record(s) from a table.
    
    @param table name of the table
    
    @examples
    When sending the query, define the post like so. (data should be a dict)
        ```
    emx2.delete(
      database='mydatabase',
      query=graphql.delete(table='mytable'),
      variables={'records': [records-to-import]}
    )
    ```
    """
    return graphql._operation(operation='delete', table=table)
  
  def insert(table:str=None):
    """insert
    Query to add record(s) into the database.
    
    @param table name of the table to import data into
   
    @examples
    When sending the query, define the post like so. (data should be a dict)
   
    ```
    emx2.insert(
      database='mydatabase',
      query=graphql.insert(table='mytable'),
      variables={'records': [record-to-import]}
    )
    ```
    """
    return graphql._operation(operation='insert', table=table)

  def update(table:str=None):
    """update
    Query to update record(s) into the database.
    
    @param table name of the table to import data into
    
    @examples
    When sending the query, define the post like so. (data should be a dict)
    
    ```
    emx2.update(
      database='mydatabase',
      query=graphql.update(table='mytable'),
      variables={'records': [record-to-import]}
    )
    ```
    """
    return graphql._operation(operation='update', table=table)
