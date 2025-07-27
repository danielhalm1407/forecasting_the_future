# Set out functions to use for working with an external database

#--Preamble--#

# Packages
library(DBI)
library(RPostgres)


# Define a function to connect to cloud engine in supabase
get_supabase_connection <- function(user, password, host, port, database) {
  con <- dbConnect(
    Postgres(),
    user = user,
    password = password,
    host = host,
    port = port,
    dbname = database
  )
  return(con)
}

a = 5