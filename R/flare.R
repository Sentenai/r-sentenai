Stream <- setRefClass("Stream",
  fields = list(name = "character")
)

Cond <- setRefClass("Cond",
  fields = c("path", "op", "val"),
  methods = list(
    to_ast = function() {
      # map types
      t <- switch(
        typeof(val),
        double = "double",
        logical = "boolean",
        character = "string"
      )
      # map vals
      wrapped = if (typeof(val) == "character") paste0('"', val, '"') else val
      v <- switch(
        as.character(wrapped),
        "TRUE" = "true",
        "FALSE" = "false",
        wrapped # default
      )
      sprintf('{ "op": "%s", "arg": { "type": "%s", "val": %s }, "type": "span" }', op, t, v)
    }
  )
)

Or <- setRefClass("Or",
  fields = c("left", "right"),
  methods = list(
    to_ast = function() {
      sprintf('{ "expr": "||", "args": [ %s, %s ] }', left$to_ast(), right$to_ast())
    }
  )
)

And <- setRefClass("Or",
  fields = c("left", "right"),
  methods = list(
    to_ast = function() {
      sprintf('{ "expr": "&&", "args": [ %s, %s ] }', left$to_ast(), right$to_ast())
    }
  )
)

StreamPath <- setRefClass("StreamPath",
  fields = c("stream", "path")
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

to_flare <- function(x) {
  expr <- substitute(x)
  eval(expr, flare_env(expr))
}

# given an expression, produce an env with every
# name referencing a StreamPath.
flare_env <- function(expr) {
  symbol_list <- as.list(all_names(expr))
  stream_paths <- lapply(symbol_list, function(sym) {
    splits <- strsplit(sym, "\\.")[[1]]
    stream <- eval(as.name(splits[[1]]))
    path <- splits[2:length(splits)]
    StreamPath$new(stream = stream, path = path)
  })
  stream_env <- list2env(setNames(stream_paths, symbol_list))

  clone_env(f_env, parent = stream_env)
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
