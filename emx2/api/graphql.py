#///////////////////////////////////////////////////////////////////////////////
# FILE: graphql.py
# AUTHOR: David Ruvolo
# CREATED: 2023-05-10
# MODIFIED: 2023-05-11
# PURPOSE: commonly used graphql queries
# STATUS: stable
# PACKAGES: NA
# COMMENTS: Import file as 'import path.to.graphql.file as graphql'
#///////////////////////////////////////////////////////////////////////////////


class graphql:
  
  @staticmethod
  def signin():
    # @name Signin
    # @description Mutation to sign into an EMX2 molgenis instance. Function must include
    # username and password in the posted data.
    #
    # @example
    # ```py
    # import requests
    # requests.post(...,
    #   json={
    #     'query': graphql.query,
    #     'variables': {'username': ..., 'password': ...}
    #   }
    # )
    # 
    # ```
    #
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
    # @name signout
    # @description Signout of an EMX2 instance
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
    # @name schema
    # @description get database schema
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

  # @name delete
  # @description query to remove record(s) from a table.
  # 
  # @param table name of the table
  #
  # @examples
  # When sending the query, define the post like so. (data should be a dict)
  #
  # ```
  # emx2.delete(
  #   database='mydatabase',
  #   query=graphql.delete(table='mytable'),
  #   variables={'records': [records-to-import]}
  # )
  # ```
  def delete(table:str=None):
    return (
      "mutation delete($records:["+ table + "Input]) {\n"
      "  delete(" + table + ":$records) {\n"
      "    status\n"
      "    message\n"
      "  }\n"
      "}"
    )
  
  # @name insert
  # @description query to add record(s) into the database.
  # 
  # @param table name of the table to import data into
  #
  # @examples
  # When sending the query, define the post like so. (data should be a dict)
  #
  # ```
  # emx2.insert(
  #   database='mydatabase',
  #   query=graphql.insert(table='mytable'),
  #   variables={'records': [record-to-import]}
  # )
  # ```
  def insert(table:str=None):
    return (
      "mutation insert($records:["+ table + "Input]) {\n"
      "  insert(" + table + ":$records) {\n"
      "    status\n"
      "    message\n"
      "  }\n"
      "}"
    )

  # @name update
  # @description query to update record(s) into the database.
  # 
  # @param table name of the table to import data into
  #
  # @examples
  # When sending the query, define the post like so. (data should be a dict)
  #
  # ```
  # emx2.update(
  #   database='mydatabase',
  #   query=graphql.update(table='mytable'),
  #   variables={'records': [record-to-import]}
  # )
  # ```
  def update(table:str=None):
    return (
      "mutation update($records:["+ table + "Input]) {\n"
      "  update(" + table + ":$records) {\n"
      "    status\n"
      "    message\n"
      "  }\n"
      "}"
    )