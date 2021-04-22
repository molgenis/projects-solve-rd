#'////////////////////////////////////////////////////////////////////////////
#' FILE: utils.R
#' AUTHOR: David Ruvolo
#' CREATED: 2021-03-10
#' MODIFIED: 2021-03-10
#' PURPOSE: various functions for this project
#' STATUS: ongoing
#' PACKAGES: NA
#' COMMENTS: an experimental Molgenis R client
#'////////////////////////////////////////////////////////////////////////////

#' molgenis api client
#'
#' A homemade class for interacting with a molgenis database. This is an updated
#' version of the standard `molgenis.R` class that is available on each server
#' @ https://<molgenis.host>/molgenis.R
#'
#' @examples
#'
#' m <- molgenis$new(host = "https://mymolgenishost.com")
#' m$login(username = "some.username", password = "their.password")
#'
#' @noRd
molgenis <- R6::R6Class(
    classname = "molgenis",
    public = list(
        #' @field host database hostname
        host = NA,

        #' @title config
        #' @description create a new Molgenis API instance
        #' @param host database hostname
        #' @examples
        #' m <- molgenis$new(host = "https://mydatabase.com")
        #' @return an R6 class
        initialize = function(host) {
            if (!grepl("([/])$", host)) host <- paste0(host, "/")
            self$host <- host
        },

        #' @title login
        #' @description Sign in to a molgenis database
        #' @param username username; default is "admin"
        #' @param password user password; leave blank for command line input
        #' @examples
        #' m <- molgenis$new(host = "https://mydatabase.com")
        #' m$login()
        #' @returns if successful, returns connected = TRUE and API token
        login = function(username = NULL, password = NULL) {
            if (is.null(username)) {
                cli::cli_alert_info("Enter username")
                password <- readline(prompt = ": ")
            }

            if (is.null(password)) {
                cli::cli_alert_info("Enter Password")
                password <- getPass::getPass(msg = ": ")
            }

            resp <- httr::POST(
                url = paste0(self$host, "api/v1/login"),
                httr::add_headers(
                    `Content-Type` = "application/json"
                ),
                body = rjson::toJSON(
                    list(
                        username = username,
                        password = password
                    )
                ),
                httr::content_type_json()
            )


            if (resp$status_code == 200) {
                cli::cli_alert_success("You are now signed in")
                content <- httr::content(
                    resp,
                    type = "text",
                    encoding = "utf8"
                )
                result <- rjson::fromJSON(content)
                private$user <- username
                private$logged <- TRUE
                private$token <- result$token
            } else {
                cli::cli_alert_danger("Failed to sign in")
            }
        },

        #' @title logout
        #' @desciption disconnect from your molgenis database
        #' @returns returns FALSE if successfully logged out
        logout = function() {
            resp <- httr::POST(
                url = paste0(self$host, "api/v1/logout"),
                httr::add_headers(
                    `x-molgenis-token` = private$token
                )
            )

            if (resp$status_code == 200) {
                cli::cli_alert_success("You are signed out")
                private$user <- NA
                private$logged  <- FALSE
                private$token <- NA
            } else {
                cli::cli_alert_danger("Failed to sign out")
            }
        },

        #' @title get data from table
        #' @description pull data from a Molgenis entity
        #' @param table molgenis table ID (pkg_entity)
        #' @param attrs an array of column names
        #' @param q additional query to append (i.e., filters)
        #' @param start number indicating the row to start from (default is 0)
        #' @param num number of rows to return (API limit: 10,000 rows)
        get = function(
            table,
            attrs = NULL,
            q = NULL,
            start = 0,
            num = 1000
        ) {
            if (!private$logged) {
                cli::cli_alert_danger("You are not signed in")
            }

            if (private$logged) {
                url <- paste0(
                    self$host,
                    "api/v1/csv/",
                    table,
                    "?start=", start,
                    "&num=", num
                )

                if (!is.null(q)) {
                    url <- paste0(url, "&q=", URLencode(q))
                }

                if (!is.null(attrs)) {
                    url <- paste0(
                        url,
                        "&attributes=",
                        URLencode(paste0(attrs, collapse = ","))
                    )
                }

                resp <- httr::GET(
                    url = url,
                    httr::add_headers(
                        `x-molgenis-token` = private$token
                    )
                )

                if (resp$status_code == 200) {
                    cli::cli_alert_success("Data Retrieved!")
                    raw <- httr::content(resp, as = "text", encoding = "utf-8")
                    readr::read_delim(raw, delim = ",")
                } else {
                    cli::cli_alert_danger(
                        "Failed to retrieve data {.val {resp$status_code}}"
                    )
                    e <- rjson::fromJSON(httr::content(resp, "text"))
                    cli::cli_alert_danger(
                        "{.text {e[[1]][[1]]['message']}}"
                    )
                }
            }
        },

        #' @title prep entity data
        #' @param x a dataframe
        prep = function(x) {
            jsonlite::toJSON(list(entities = x), dataframe = "row")
        },

        #' @title Push Entity Data
        #' @param table molgenis table ID (pkg_entity)
        #' @param x json object to import
        push = function(table, x) {
            resp <- httr::POST(
                url = paste0(self$host, "api/v2/", table),
                body = x,
                httr::add_headers(
                    `x-molgenis-token` = private$token,
                    `ContentType` = "application/json"
                ),
                httr::content_type_json()
            )
            if (resp$status_code == 201) {
                cli::cli_alert_success("Data Imported!")
            } else {
                cli::cli_alert_danger(
                    "Import failed ({.val {resp$status_code}})"
                )
                e <- rjson::fromJSON(httr::content(resp, "text"))
                cli::cli_alert_danger(
                    "{.text {e[[1]][[1]]['message']}}"
                )
            }
        },

        #' @title delete entity data
        #' @description remove all data from a table
        #' @param table molgenis table ID (pkg_entity)
        delete = function(table) {
            resp <- httr::DELETE(
                url = paste0(self$host, "api/v1/", table),
                httr::add_headers(
                    `x-molgenis-token` = private$token
                )
            )
            if (resp$status_code == 204) {
                cli::cli_alert_success("Data deleted!")
            } else {
                cli::cli_alert_danger("Failed to remove data")
                cli::cli_alert_danger(
                    "{.text {e[[1]][[1]]['message']}}"
                )
            }
        }
    ),
    private = list(
        user = NA,
        logged = FALSE,
        token = NA
    )
)