#///////////////////////////////////////////////////////////////////////////////
# FILE: graphql.py
# AUTHOR: David Ruvolo
# CREATED: 2023-05-10
# MODIFIED: 2023-05-10
# PURPOSE: commonly used graphql queries
# STATUS: in.progress
# PACKAGES: NA
# COMMENTS: Import file as 'import path.to.graphql.file as graphql'
#///////////////////////////////////////////////////////////////////////////////


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
signin="""
  mutation($email:String, $password: String) {
    signin(email: $email, password: $password) {
      status
      message
      token
    }
  }
"""

# @name signout
# @description Signout of an EMX2 instance
signout="""
  mutation {
    signout {
      status
      message
    }
  }
"""

# @name schema
# @description get database schema
schema=""" {
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