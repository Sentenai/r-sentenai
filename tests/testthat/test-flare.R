context('test-flare.R')

test_that('Basic select', {
  s <- Stream$new(name = 'S')
  real <- select()$span(s.key == 'something')$to_ast()
  expected <- list(
    select = list(
      op = '==',
      arg = list(type = 'string', val = 'something'),
      type = 'span',
      path = c('event', 'key'),
      stream = list(name = 'S')
    )
  )
  expect_equal(real, expected)
})

test_that('Or', {
  s <- Stream$new(name = 'S')
  t <- Stream$new(name = 'T')
  real <- select()$span(s.x == TRUE || t.x == TRUE)$to_ast()
  expected <- list(
    select = list(
      expr = '||',
      args = list(
        list(
          op = '==',
          arg = list(type = 'bool', val = TRUE),
          type = 'span',
          path = c('event', 'x'),
          stream = list(name = 'S')
        ),
        list(
          op = '==',
          arg = list(type = 'bool', val = TRUE),
          type = 'span',
          path = c('event', 'x'),
          stream = list(name = 'T')
        )
      )
    )
  )
  expect_equal(real, expected)
})

test_that('any_of', {
  s <- Stream$new(name = 'moose')
  real <- select()$span(any_of(
    s.x < 0,
    s.x >= 3.141592653589793,
    s.b != FALSE
  ))$to_ast()
  expected <- list(
    select = list(
      type = 'any',
      conds = list(
        list(
          op = '<',
          arg = list(type = 'double', val = 0),
          type = 'span',
          path = c('event', 'x'),
          stream = list(name = 'moose')
        ),
        list(
          op = '>=',
          arg = list(type = 'double', val = 3.14159265358979),
          type = 'span',
          path = c('event', 'x'),
          stream = list(name = 'moose')
        ),
        list(
          op = '!=',
          arg = list(type = 'bool', val = FALSE),
          type = 'span',
          path = c('event', 'b'),
          stream = list(name = 'moose')
        )
      )
    )
  )
  expect_equal(real, expected)
})

test_that('during', {
  s <- Stream$new(name = 'S')
  real <- select()$span(during(
    s.foo == 'bar',
    s.baz > 1.5
  ))$to_ast()
  expected <- list(
    select = list(
      type = 'during',
      conds = list(
        list(
          op = '==',
          arg = list(type = 'string', val = 'bar'),
          type = 'span',
          path = c('event', 'foo'),
          stream = list(name = 'S')
        ),
        list(
          op = '>',
          arg = list(type = 'double', val = 1.5),
          type = 'span',
          path = c('event', 'baz'),
          stream = list(name = 'S')
        )
      )
    )
  )
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
