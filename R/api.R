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

#' Create Sentenai API client.
#'
#' @param auth_key Valid API authentication key.
#' @param host (optional) Defaults to https://api.sentenai.com
#' @export
sentenaiWithKey <- function(auth_key, host = 'https://api.sentenai.com') {
  Sentenai$new(auth_key = auth_key, host = host)
}

Sentenai <- setRefClass("Sentenai",
  fields = list(auth_key = "character", host = "character"),
  methods = list(
    get = function(stream, event_id=NULL) {
      parts = c(host, 'streams', stream$name)
      if (!is.null(event_id)) {
        parts <- c(parts, 'events', event_id)
      }
      url <- paste(parts, collapse = '/')
      res <- httr::GET(url, get_api_headers())
      httr::content(res)
    },
    field_stats = function(stream, field, start = NULL, end = NULL) {
      args = list()
      if (!is.null(start)) {
        args <- c(args, list(start = to_iso_8601(start)))
      }
      if (!is.null(end)) {
        args <- c(args, list(end = to_iso_8601(end)))
      }
      url <- paste(c(host, 'streams', stream$name, 'fields', field, 'stats'), collapse = '/')
      res <- httr::GET(url, get_api_headers(), query = args)
      httr::content(res)
    },
    # TODO: filter by meta
    streams = function(name=NULL) {
      url <- paste(c(host, 'streams'), collapse = '/')
      res <- httr::GET(url, get_api_headers())
      streams <- httr::content(res)
      if (is.null(name)) {
        streams
      } else {
        # Filter streams where stream$name contains `name`
        streams[lapply(streams, function(s){ grepl(name, s$name)} ) == T]
      }
    },
    range = function(stream, start, end) {
      url <- paste(
        c(host, 'streams', stream$name, 'start', to_iso_8601(start), 'end', to_iso_8601(end)),
        collapse = '/'
      )
      res <- httr::GET(url, get_api_headers())
      lines <- strsplit(httr::content(res, as = "text", encoding = "UTF-8"), "\n")[[1]]
      lapply(lines, function(line) { rjson::fromJSON(line) })
    },
    query = function(statements, limit = Inf) {
      Cursor$new(
        client = .self,
        query = rjson::toJSON(statements$to_ast()),
        limit = limit
      )$get()
    },
    fields = function(stream) {
      url <- paste(c(host, 'streams', stream$name, 'fields'), collapse = '/')
      res <- httr::GET(url, get_api_headers())
      httr::content(res)
    },
    values = function(stream, timestamp = NULL) {
      headers = list()
      args = list()
      if (!is.null(timestamp)) {
        headers = httr::add_headers(timestamp = to_iso_8601(timestamp))
        args = list(at = to_iso_8601(timestamp))
      }
      url <- paste(c(host, 'streams', stream$name, 'values'), collapse = '/')
      res <- httr::GET(url, c(get_api_headers(), headers), body = args)
      httr::content(res)
    },
    newest = function(stream) {
      url <- paste(c(host, 'streams', stream$name, 'newest'), collapse = '/')
      res <- httr::GET(url, get_api_headers())
      httr::content(res)
    },
    oldest = function(stream) {
      url <- paste(c(host, 'streams', stream$name, 'oldest'), collapse = '/')
      res <- httr::GET(url, get_api_headers())
      httr::content(res)
    },
    get_api_headers = function() {
      httr::add_headers('Content-Type' = 'application/json', 'Auth-Key' = auth_key)
    }
  )
)

parse_iso_8601 <- function(str) {
  as.POSIXlt(str, "UTC", "%Y-%m-%dT%H:%M:%S")
}

to_iso_8601 <- function(timestamp) {
  strftime(timestamp, "%FT%H:%M:%OS3Z")
}

Cursor <- setRefClass("Cursor",
  fields = list(client = "Sentenai", query = "character", limit = "numeric", query_id = "character"),
  methods = list(
    # TODO: can this happen in a constructor? look into $initialize
    get = function () {
      url <- sprintf("%s/query", client$host)
      r <- httr::POST(url, client$get_api_headers(), body = query, encode = "json")
      if (httr::status_code(r) == 201) {
        query_id <<- httr::headers(r)$location
        .self
      } else {
        print("TODO: handle errors")
        print(httr::status_code(r))
      }
    },
    spans = function () {
      cid <- query_id
      spans <- list()

      while (!is.null(cid) & length(spans) < limit) {
        if (is.finite(limit)) {
          url <- sprintf("%s/query/%s/spans?limit=%d", client$host, cid, limit - length(spans))
        } else {
          url <- sprintf("%s/query/%s/spans", client$host, cid)
        }

        r <- httr::content(httr::GET(url, client$get_api_headers()))

        # TODO: hold onto `spans` reference
        spans <- c(spans, r$spans)
        cid <- r$cursor
      }

      sps <- lapply(spans, function(s) {
        s["cursor"] <- NULL
        s$start <- if(is.null(s$start)) NULL else parse_iso_8601(s$start)
        s$end <- if(is.null(s$end)) NULL else parse_iso_8601(s$end)
        s
      })
      sps
    },
    .slice = function (cursor_id, start, end, max_retries = 3) {
      retries <- 0
      cursor <- sprintf(
        "%s+%s+%s",
        strsplit(cursor_id, "\\+")[[1]][[1]],
        to_iso_8601(start),
        to_iso_8601(end)
      )
      events <- list()

      while(!is.null(cursor)) {
        url <- sprintf("%s/query/%s/events", client$host, cursor)
        r <- httr::GET(url, client$get_api_headers())
        code <- httr::status_code(r)

        if (code == 400) {
          stop(sprintf("Client error in request for cursor: %s", cursor))
        } else if (code != 200 & retries >= max_retries) {
          stop('Failed to get cursor')
        } else if (code != 200) {
          retries <- retries + 1
        } else {
          # TODO: process this all, not sure how to mimic python lib
          cont <- httr::content(r)
          # has info about which streams were queries
          # print(cont[[1]])
          # has actual events
          # print(length(cont[[2]]))

          cursor <- httr::headers(r)$cursor
          events <- c(events, cont[[2]])
        }
      }

      events
    },
    dataframe = function (sp = spans()) {
      c1 <- parallel::makeCluster(parallel::detectCores() - 1)

      # this feels fragile
      parallel::clusterExport(c1, "to_iso_8601")
      parallel::clusterEvalQ(c1, library(httr))

      df <- parallel::parLapply(c1, sp, function(span) {
        .slice(query_id, span$start, span$end)
      })

      parallel::stopCluster(c1)
      df
    },
    stats = function (sp = spans()) {
      deltas <- unlist(lapply(sp, function(span) {
        difftime(span$end, span$start, units = "secs")
      }))
      if (length(deltas) == 0) { return(list()) }

      list(
        min = min(deltas),
        max = max(deltas),
        mean = sum(deltas) / length(deltas),
        median = sort(deltas)[[round(length(deltas) / 2)]],
        count = length(deltas)
      )
    }
  )
)
