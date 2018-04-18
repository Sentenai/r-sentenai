Stream <- setRefClass("Stream",
  fields = list(name = "character"),
  methods = list(
    to_ast = function() {
      list(name = name)
    }
  )
)

select <- function() {
  Select$new()
}

Select <- setRefClass("Select",
  fields = c("query"),
  methods = list(
    initialize = function(query = NULL) {
      callSuper(query = query)
    },
    span = function(x) {
      query <<- to_flare(substitute(x))
      .self
    },
    to_ast = function() {
      if (is.null(query)) {
        list(select = list(expr = TRUE))
      } else {
        list(select = query$to_ast())
      }
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
        list(op = op, arg = list(type = t, val = val), type = "span"),
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

f_env$"any_of" <- function(...) {
  Par$new(type = "any", query = list(...))
}
f_env$"all_of" <- function(...) {
  Par$new(type = "all", query = list(...))
}
f_env$"during" <- function(...) {
  Par$new(type = "during", query = list(...))
}

to_flare <- function(expr) {
  eval(expr, flare_env(expr))
}

# given an expression, produce an env with every
# name referencing a StreamPath.
flare_env <- function(expr) {
  symbol_list <- as.list(all_names(expr))
  stream_paths <- lapply(symbol_list, function(sym) {
    splits <- strsplit(sym, "\\.")[[1]]
    # TODO: this probably throws a terrible error msg if the stream is undefined
    stream <- eval(as.name(splits[[1]]), find_frame(splits[[1]]))
    path <- splits[2:length(splits)]
    StreamPath$new(stream = stream, path = path)
  })
  stream_env <- list2env(setNames(stream_paths, symbol_list))

  clone_env(f_env, parent = stream_env)
}

# climb stack frames searching for where `name` is defined
find_frame <- function(name) {
  n <- 1
  env <- parent.frame(n = n)
  while (!identical(env, emptyenv())) {
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
