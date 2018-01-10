# Sentenai
#
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Build and Reload Package:  'Cmd + Shift + B'
#   Check Package:             'Cmd + Shift + E'
#   Test Package:              'Cmd + Shift + T'

host <- 'https://api.sentenai.com'
auth_key <- Sys.getenv('sentenai_auth_token')

get_api_headers <- function() {
  add_headers('Content-Type' = 'application/json', 'Auth-Key' = auth_key)
}

# TODO: Stream instance? is that a thing?name
get <- function(stream, event_id=NULL) {
  parts = c(host, 'streams', stream)
  if (!is.null(event_id)) {
    parts <- c(parts, 'events', event_id)
  }
  url <- paste(parts, collapse = '/')
  res <- GET(url, get_api_headers())
  content(res)
}

# TODO: start, end
get_field_stats <- function(stream, field) {
  url <- paste(c(host, 'streams', stream, 'fields', field, 'stats'), collapse = '/')
  res <- GET(url, get_api_headers())
  content(res)
}

# TODO: filter by meta
get_streams <- function(name=NULL) {
  url <- paste(c(host, 'streams'), collapse = '/')
  res <- GET(url, get_api_headers())
  streams <- content(res)
  if (is.null(name)) {
    streams
  } else {
    # Filter streams where stream$name contains `name`
    streams[lapply(streams, function(s){ grepl(name, s$name)} ) == T]
  }
}

get_fields <- function(stream) {
  url <- paste(c(host, 'streams', stream, 'fields'), collapse = '/')
  res <- GET(url, get_api_headers())
  content(res)
}

get_values <- function(stream) {
  url <- paste(c(host, 'streams', stream, 'values'), collapse = '/')
  res <- GET(url, get_api_headers())
  content(res)
}

get_newest <- function(stream) {
  url <- paste(c(host, 'streams', stream, 'newest'), collapse = '/')
  res <- GET(url, get_api_headers())
  content(res)
}

get_oldest <- function(stream) {
  url <- paste(c(host, 'streams', stream, 'oldest'), collapse = '/')
  res <- GET(url, get_api_headers())
  content(res)
}
