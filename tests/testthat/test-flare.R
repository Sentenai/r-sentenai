json_ast <- function(q) {
  rjson::toJSON(q$to_ast())
}

test_that('Basic select', {
  s <- Stream$new(name = 'S')
  real <- json_ast(select()$span(s.key == 'something'))
  expected <- '{"select":{"op":"==","arg":{"type":"string","val":"something"},"type":"span","path":["event","key"],"stream":{"name":"S"}}}'
  expect_equal(real, expected)
})

test_that('Or', {
  s <- Stream$new(name = 'S')
  t <- Stream$new(name = 'T')
  real <- json_ast(select()$span(s.x == TRUE || t.x == TRUE))
  expected <- '{"select":{"expr":"||","args":[{"op":"==","arg":{"type":"bool","val":true},"type":"span","path":["event","x"],"stream":{"name":"S"}},{"op":"==","arg":{"type":"bool","val":true},"type":"span","path":["event","x"],"stream":{"name":"T"}}]}}'
  expect_equal(real, expected)
})

test_that('any_of', {
  s <- Stream$new(name = 'moose')
  real <- json_ast(
    select()$span(any_of(
      s.x < 0,
      s.x >= 3.141592653589793,
      s.b != FALSE
    ))
  )
  expected <- '{"select":{"type":"any","conds":[{"op":"<","arg":{"type":"double","val":0},"type":"span","path":["event","x"],"stream":{"name":"moose"}},{"op":">=","arg":{"type":"double","val":3.14159265358979},"type":"span","path":["event","x"],"stream":{"name":"moose"}},{"op":"!=","arg":{"type":"bool","val":false},"type":"span","path":["event","b"],"stream":{"name":"moose"}}]}}'
  expect_equal(real, expected)
})
