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
  expected <- '{"select":{"expr":"||","args":[{"op":"==","arg":{"type":"boolean","val":true},"type":"span","path":["event","x"],"stream":{"name":"S"}},{"op":"==","arg":{"type":"boolean","val":true},"type":"span","path":["event","x"],"stream":{"name":"T"}}]}}'
  expect_equal(real, expected)
})
