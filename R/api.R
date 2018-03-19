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

Sentenai <- setRefClass("Sentenai",
  fields = list(auth_key = "character", host = "character"),
  methods = list(
    get = function(stream, event_id=NULL) {
      parts = c(host, 'streams', stream$name)
      if (!is.null(event_id)) {
        parts <- c(parts, 'events', event_id)
      }
      url <- paste(parts, collapse = '/')
      res <- GET(url, get_api_headers())
      content(res)
    },
    # TODO: start, end
    field_stats = function(stream, field) {
      url <- paste(c(host, 'streams', stream$name, 'fields', field, 'stats'), collapse = '/')
      res <- GET(url, get_api_headers())
      content(res)
    },
    # TODO: filter by meta
    streams = function(name=NULL) {
      url <- paste(c(host, 'streams'), collapse = '/')
      res <- GET(url, get_api_headers())
      streams <- content(res)
      if (is.null(name)) {
        streams
      } else {
        # Filter streams where stream$name contains `name`
        streams[lapply(streams, function(s){ grepl(name, s$name)} ) == T]
      }
    },
    # `statements` is a JSON AST query string for now
    query = function(statements) {
      Cursor$new(client=.self, query=statements)$get()
    },
    fields = function(stream) {
      url <- paste(c(host, 'streams', stream$name, 'fields'), collapse = '/')
      res <- GET(url, get_api_headers())
      content(res)
    },
    values = function(stream) {
      url <- paste(c(host, 'streams', stream$name, 'values'), collapse = '/')
      res <- GET(url, get_api_headers())
      content(res)
    },
    newest = function(stream) {
      url <- paste(c(host, 'streams', stream$name, 'newest'), collapse = '/')
      res <- GET(url, get_api_headers())
      content(res)
    },
    oldest = function(stream) {
      url <- paste(c(host, 'streams', stream$name, 'oldest'), collapse = '/')
      res <- GET(url, get_api_headers())
      content(res)
    },
    get_api_headers = function() {
      add_headers('Content-Type' = 'application/json', 'Auth-Key' = auth_key)
    }
  )
)

Stream <- setRefClass("Stream",
  fields = list(name = "character")
)

Cursor <- setRefClass("Cursor",
  fields = list(client = "Sentenai", query = "character", limit = "numeric", query_id = "character"),
  methods = list(
    # TODO: can this happen in a constructor? look into $initialize
    get = function () {
      url <- sprintf("%s/query", client$host)
      r <- POST(url, client$get_api_headers(), body = query, encode = "json")
      if (status_code(r) == 201) {
        query_id <<- headers(r)$location
        .self
      } else {
        print("TODO: handle errors")
      }
    },
    spans = function () {
      cid <- query_id
      spans <- c()

      while (!is.null(cid)) {
        url <- sprintf("%s/query/%s/spans", client$host, cid)
        r <- content(GET(url, client$get_api_headers()))

        # TODO: parse start/end into POSIXlt?
        # TODO: hold onto `spans` reference
        spans <- c(spans, r$spans)
        cid <- r$cursor
      }

      sps <- lapply(spans, function(x) { x["cursor"] <- NULL; x })
      sps
    }
  )
)
