test_that('Cond can produce AST', {
  s <- Stream$new(name = 'S')
  expected <- '{ "op": "==", "arg": { "type": "string", "val": "something" }, "type": "span" }'
  real <- to_flare(s.key == 'something')$to_ast()
  expect_equal(real, expected)
})
