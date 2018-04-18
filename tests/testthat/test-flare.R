json_ast <- function(q) {
  rjson::toJSON(q$to_ast())
}

test_that('Basic select', {
  s <- Stream$new(name = 'S')
  expected <- '{"select":{"op":"==","arg":{"type":"string","val":"something"},"type":"span","path":["event","key"],"stream":{"name":"S"}}}'
  real <- json_ast(select()$span(s.key == 'something'))
  expect_equal(real, expected)
})
