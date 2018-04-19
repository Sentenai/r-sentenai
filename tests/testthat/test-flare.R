context('test-flare.R')

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

test_that('during', {
  s <- Stream$new(name = 'S')
  real <- json_ast(
    select()$span(during(
      s.foo == 'bar',
      s.baz > 1.5
    ))
  )
  expected <- '{"select":{"type":"during","conds":[{"op":"==","arg":{"type":"string","val":"bar"},"type":"span","path":["event","foo"],"stream":{"name":"S"}},{"op":">","arg":{"type":"double","val":1.5},"type":"span","path":["event","baz"],"stream":{"name":"S"}}]}}'
  expect_equal(real, expected)
})

test_that('stream filters', {
  s = Stream$new('S', V.season == 'summer')
  temp <- 77
  real <- select()$span(s.temperature >= temp && s.sunny == TRUE)$to_ast()
  expected <- list(
    select = list(
      expr = '&&',
      args = list(
        list(
          op = '>=',
          arg = list(type = 'double', val = 77),
          type = 'span',
          path = c('event', 'temperature'),
          stream = list(
            name = 'S',
            filter = list(
              op = '==',
              arg = list(type = 'string', val = 'summer'),
              # TODO: remove these
              type = 'span',
              path = c('event', 'season')
            )
          )
        ),
        list(
          op = '==',
          arg = list(type = 'bool', val = TRUE),
          type = 'span',
          path = c('event', 'sunny'),
          stream = list(
            name = 'S',
            filter = list(
              op = '==',
              arg = list(type = 'string', val = 'summer'),
              # TODO: remove these
              type = 'span',
              path = c('event', 'season')
            )
          )
        )
      )
    )
  )

  expect_equal(real, expected)
})
