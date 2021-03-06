#' Create a Stream instance
#'
#' @param name Name of the Sentenai stream.
#' @param filter Conditions applied to the raw stream.
#' @return An instance of Stream.
#' @export
stream <- function(name, filter = NULL) {
  expr <- substitute(filter)
  if (is.null(expr)) {
    Stream$new(name = name, filter = NULL)
  } else {
    Stream$new(name = name, filter = to_stream_filters(expr))
  }
}

Stream <- setRefClass("Stream",
  fields = c("name", "filter"),
  methods = list(
    to_ast = function() {
      if (is.null(filter)) {
        list(name = name)
      } else {
        list(name = name, filter = filter$to_ast())
      }
    }
  )
)

#' Initiate a select statement
#'
#' @param start A POSIXlt date.
#' @param end A POSIXlt date.
#' @return An instance of Select bound to start/end.
#' @export
select <- function(start = NULL, end = NULL) {
  Select$new(query = NULL, start = start, end = end, projection = NULL)
}

Select <- setRefClass('Select',
  fields = c('query', 'start', 'end', 'projection'),
  methods = list(
    span = function(x = NULL, min = NULL, max = NULL, exactly = NULL, returning = NULL) {
      q <- to_flare(substitute(x))
      raw_proj <- to_flare(substitute(returning), proj_env)
      projection <<- if (is.null(raw_proj) || is.list(raw_proj)) raw_proj else list(raw_proj)

      query <<- list(Span$new(q,
        min = min,
        max = max,
        exactly = exactly
      ))
      .self
    },
    then = function(x, ...) {
      if (is.null(query)) { stop('Use $span method to start select') }
      q <- to_flare(substitute(x))
      query <<- c(query, list(Span$new(q, ...)))
      .self
    },
    to_ast = function() {
      ast <- list()
      if (!is.null(start) && !is.null(end)) {
        ast$between = list(
          to_iso_8601(start),
          to_iso_8601(end)
        )
      } else if (!is.null(start)) {
        ast$after = to_iso_8601(start)
      } else if (!is.null(end)) {
        ast$before = to_iso_8601(end)
      }

      if (is.null(query)) {
        ast$select = list(expr = TRUE)
      } else if (length(query) == 1) {
        ast$select = query[[1]]$to_ast()
      } else {
        ast$select = Serial$new(query = query)$to_ast()
      }

      if (!is.null(projection)) {
        ast$projections <- list(
          explicit = lapply(projection, function(p) p$to_ast()),
          ... = FALSE
        )
      }

      ast
    }
  )
)

#' Create a Span instance to restrict conditions
#' based on within/after/min/max/exactly.
#' @param min The minimum valid span duration `delta()`.
#' @param max The maximum valid span duration `delta()`.
#' @param exactly The exact valid span duration `delta()`.
#' @param within The maximum distance in time between the end of the previous
#'               span and the start of this span.
#' @param after The minimum distance in time between the end of the previous
#'              span and the start of this span.
#' @return An instance of Delta.
#' @export
span <- function(
  query,
  within = NULL,
  after = NULL,
  min = NULL,
  max = NULL,
  exactly = NULL
) {
  expr <- to_flare(substitute(query))
  Span$new(
    expr,
    within = within,
    after = after,
    min = min,
    max = max,
    exactly = exactly
  )
}

Span <- setRefClass('Span',
  fields = c('query', 'within', 'after', 'min', 'max', 'width'),
  methods = list(
    initialize = function(
      query,
      within = NULL,
      after = NULL,
      min = NULL,
      max = NULL,
      exactly = NULL
    ) {
      callSuper(
        query = query,
        within = within,
        after = after,
        min = min,
        max = max,
        width = exactly
      )
    },
    then = function(x, ...) {
      q <- to_flare(substitute(x))
      Serial$new(query = list(.self, Span$new(q, ...)))
    },
    to_ast = function() {
      ast <- list(`for` = list())

      if(!is.null(within)) {
        ast$within <- within$to_ast()
      }

      if(!is.null(after)) {
        ast$after <- after$to_ast()
      }

      if(!is.null(min)) {
        ast$`for`$`at-least` <- min$to_ast()
      }

      if(!is.null(max)) {
        ast$`for`$`at-most` <- max$to_ast()
      }

      if(!is.null(width)) {
        ast$`for` <- width$to_ast()
      }

      if (length(ast$`for`) == 0) {
        ast$`for` = NULL
      }

      c(
        if (is.null(query)) list(expr = TRUE) else query$to_ast(),
        ast
      )
    }
  )
)

Serial <- setRefClass('Serial',
  fields = c('query'),
  methods = list(
    to_ast = function() {
      list(type = 'serial', conds = lapply(query, function(q) q$to_ast()))
    }
  )
)

Cond <- setRefClass("Cond",
  fields = c("path", "op", "val"),
  methods = list(
    to_ast = function() {
      # map types
      t <- switch(
        typeof(val),
        double = "double",
        logical = "bool",
        character = "string"
      )
      c(
        list(op = op, arg = list(type = t, val = val)),
        if (class(path) == 'StreamPath') list(type = 'span') else NULL,
        path$to_ast()
      )
    }
  )
)

Or <- setRefClass("Or",
  fields = c("left", "right"),
  methods = list(
    to_ast = function() {
      list(expr = "||", args = list(left$to_ast(), right$to_ast()))
    }
  )
)

And <- setRefClass("And",
  fields = c("left", "right"),
  methods = list(
    to_ast = function() {
      list(expr = "&&", args = list(left$to_ast(), right$to_ast()))
    }
  )
)

StreamPath <- setRefClass("StreamPath",
  fields = c("stream", "path"),
  methods = list(
    to_ast = function() {
      list(path = c("event", path), stream = stream$to_ast())
    }
  )
)

EventPath <- setRefClass("EventPath",
  fields = c("path"),
  methods = list(
    to_ast = function() {
      list(path = c("event", path))
    }
  )
)

Par <- setRefClass("Par",
  fields = c("type", "query"),
  methods = list(
    to_ast = function() {
      list(
        type = type,
        conds = lapply(query, function(q) { q$to_ast() })
      )
    }
  )
)

#' Create a Delta instance representing a time delta.
#'
#' @return An instance of Delta.
#' @export
delta <- function(
  seconds = 0,
  minutes = 0,
  hours = 0,
  days = 0,
  weeks = 0,
  months = 0,
  years = 0
) {
  Delta$new(
    seconds = seconds,
    minutes = minutes,
    hours = hours,
    days = days,
    weeks = weeks,
    months = months,
    years = years
  )
}

Delta <- setRefClass('Delta',
  fields = list(
    seconds = 'numeric',
    minutes = 'numeric',
    hours = 'numeric',
    days = 'numeric',
    weeks = 'numeric',
    months = 'numeric',
    years = 'numeric'
  ),
  methods = list(
    initialize = function(
      seconds = 0,
      minutes = 0,
      hours = 0,
      days = 0,
      weeks = 0,
      months = 0,
      years = 0
    ) {
      callSuper(
        seconds = seconds,
        minutes = minutes,
        hours = hours,
        days = days,
        weeks = weeks,
        months = months,
        years = years
      )
    },
    to_ast = function() {
      non_zero <- c(
        if (seconds > 0) list(seconds = seconds) else NULL,
        if (minutes > 0) list(minutes = minutes) else NULL,
        if (hours > 0) list(hours = hours) else NULL,
        if (days > 0) list(days = days) else NULL,
        if (weeks > 0) list(weeks = weeks) else NULL,
        if (months > 0) list(months = months) else NULL,
        if (years > 0) list(years = years) else NULL
      )

      if (length(non_zero) > 0) non_zero else list(seconds = 0)
    }
  )
)

Switch <- setRefClass('Switch',
  fields = c('stream', 'left', 'right'),
  methods = list(
    to_ast = function() {
      if (is.null(stream)) { stop('Switch must be bound to a Stream') }
      list(
        type = 'switch',
        stream = stream$to_ast(),
        conds = list(
          if (isTRUE(left)) list(expr = TRUE) else left$to_ast(),
          if (isTRUE(right)) list(expr = TRUE) else right$to_ast()
        )
      )
    }
  )
)

make_cond <- function (op) {
  force(op)
  function (path, val) {
    force(path)
    force(val)
    Cond$new(path = path, op = op, val = val)
  }
}

f_env <- new.env(parent = emptyenv())
f_env$">" <- make_cond(">")
f_env$"<" <- make_cond("<")
f_env$">=" <- make_cond(">=")
f_env$"<=" <- make_cond("<=")
f_env$"!=" <- make_cond("!=")
# TODO: handle "in" for lists
f_env$"==" <- make_cond("==")

f_env$"&&" <- function (left, right) {
  force(left)
  force(right)
  And$new(left = left, right = right)
}
f_env$"||" <- function (left, right) {
  force(left)
  force(right)
  Or$new(left = left, right = right)
}

# implementing `->` which gets parsed as `<-`
# so left/right are flipped
f_env$"<-" <- function (right, left) {
  force(right)
  force(left)
  Switch$new(stream = NULL, left = left, right = right)
}

f_env$"any_of" <- function(...) {
  Par$new(type = "any", query = list(...))
}
f_env$"all_of" <- function(...) {
  Par$new(type = "all", query = list(...))
}
f_env$"during" <- function(...) {
  Par$new(type = "during", query = list(...))
}

f_env$':' <- function(stream, switch) {
  if (class(stream) == 'Stream' && class(switch) == 'Switch') {
    switch$stream <- stream
    switch
  } else {
    stop('Expected `:` to connect a Stream and a Switch')
  }
}

ProjMath <- setRefClass('ProjMath',
  fields = c('op', 'left', 'right'),
  methods = list(
    to_ast = function() {
      convert <- function(p) {
        switch(
          class(p),
            numeric = list(lit = list(val = p, type = 'double')),
            EventPath = list(var = p$to_ast()$path),
            ProjMath = p$to_ast(),
            stop('Projection math with non-numeric types is unsupported.')
        )
      }

      list(op = op, lhs = convert(left), rhs = convert(right))
    }
  )
)

StreamProjection <- setRefClass('StreamProjection',
  fields = c('stream', 'proj'),
  methods = list(
    to_ast = function() {
      if (is.logical(proj)) {
        p <- if (isTRUE(proj)) 'default' else FALSE
        list(stream = stream$to_ast(), projection = p)
      } else {
        proj_ast <- function (proj) {
          Reduce(
            function(out, name) {
              ast <- switch(
                class(proj[[name]]),
                  ProjMath = list(proj[[name]]$to_ast()),
                  EventPath = list(list(var = proj[[name]]$to_ast()$path)),
                  list = proj_ast(proj[[name]]),
                  numeric = list(list(lit = list(val = proj[[name]], type = 'double'))),
                  logical = list(list(lit = list(val = proj[[name]], type = 'bool'))),
                  character = list(list(lit = list(val = proj[[name]], type = 'string'))),
                  stop(sprintf('%s is unsupported in projections', class(proj[[name]])))
              )
              out[[name]] <- ast
              out
            },
            names(proj),
            list()
          )
        }

        list(stream = stream$to_ast(), projection = proj_ast(proj))
      }
    }
  )
)

proj_env <- new.env(parent = emptyenv())
proj_env$'+' <- function(left, right) {
  ProjMath$new(op = '+', left = left, right = right)
}
proj_env$'-' <- function(left, right) {
  ProjMath$new(op = '-', left = left, right = right)
}
proj_env$'*' <- function(left, right) {
  ProjMath$new(op = '*', left = left, right = right)
}
proj_env$'/' <- function(left, right) {
  ProjMath$new(op = '/', left = left, right = right)
}
proj_env$'/' <- function(left, right) {
  ProjMath$new(op = '/', left = left, right = right)
}
proj_env$'from' <- function(stream, proj) {
  StreamProjection$new(stream = stream, proj = proj)
}

to_flare <- function(expr, base_env = f_env) {
  eval(expr, flare_env(expr, base_env))
}

# given an expression, produce an env with every
# name referencing a StreamPath.
flare_env <- function(expr, base_env) {
  symbol_list <- as.list(all_names(expr))
  stream_paths <- lapply(symbol_list, function(sym) {
    path <- strsplit(sym, "\\.")[[1]]
    if (path[[1]] == 'V') {
      EventPath$new(path = path[-1])
    } else {
      val <- eval(as.name(path[[1]]), find_frame(path[[1]]))
      if (class(val) == 'Stream' && length(path[-1]) > 0) {
        StreamPath$new(stream = val, path = path[-1])
      } else {
        val
      }
    }
  })
  stream_env <- list2env(setNames(stream_paths, symbol_list))

  clone_env(base_env, parent = stream_env)
}

to_stream_filters <- function(expr) {
  eval(expr, stream_filter_env(expr))
}

stream_filter_env <- function(expr) {
  symbol_list <- as.list(all_names(expr))
  event_paths <- lapply(symbol_list, function(sym) {
    path <- strsplit(sym, "\\.")[[1]]
    if (path[[1]] == 'V') {
      EventPath$new(path = path[-1])
    } else {
      eval(as.name(sym), find_frame(sym))
    }
  })
  events_env <- list2env(setNames(event_paths, symbol_list))

  clone_env(f_env, parent = events_env)
}

# climb stack frames searching for where `name` is defined
find_frame <- function(name) {
  n <- 1
  env <- parent.frame(n = n)
  global <- globalenv()
  while (!identical(env, global)) {
    if (exists(name, envir = env, inherits = FALSE)) {
      break
    }

    env <- parent.frame(n = n)
    n <- n + 1
  }

  env
}

# walk expression and gather all names
all_names <- function(x) {
  if (is.atomic(x)) {
    character()
  } else if (is.name(x)) {
    as.character(x)
  } else if (is.call(x) || is.pairlist(x)) {
    children <- lapply(x[-1], all_names)
    unique(unlist(children))
  } else {
    stop("Don't know how to handle type ", typeof(x), call. = FALSE)
  }
}

clone_env <- function(env, parent = parent.env(env)) {
  list2env(as.list(env), parent = parent)
}
