context('test-flare.R')

test_that('Stream takes a name', {
  name <- 'boston-weather'
  s <- Stream$new(name)
  expect_equal(s$name, name)
  expect_equal(s$filter, NULL)
})

test_that('Stream can take filters', {
  name <- 'boston-weather'
  s <- Stream$new(name, V.foo == 'bar')
  expect_equal(s$name, name)
  expect_equal(
    s$filter$to_ast(),
    list(
      op = '==',
      arg = list(type = 'string', val = 'bar'),
      path = c('event', 'foo')
    )
  )
})

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
              path = c('event', 'season')
            )
          )
        )
      )
    )
  )

  expect_equal(real, expected)
})

test_that('or stream filters', {
  s <- Stream$new('S', V.season == 'summer' || V.season == 'winter')
  real <- select()$span(s.sunny == TRUE)$to_ast()
  expected <- list(
    select = list(
      op = '==',
      arg = list(type = 'bool', val = TRUE),
      type = 'span',
      path = c('event', 'sunny'),
      stream = list(
        name = 'S',
        filter = list(
          expr = '||',
          args = list(
            list(
              op = '==',
              arg = list(type = 'string', val = 'summer'),
              path = c('event', 'season')
            ),
            list(
              op = '==',
              arg = list(type = 'string', val = 'winter'),
              path = c('event', 'season')
            )
          )
        )
      )
    )
  )
  expect_equal(real, expected)
})

test_that('serial', {
  s = Stream$new('S')
  real <- select()$span(s.even == TRUE)$then(s.event == TRUE)$then(s.event.event == TRUE)$to_ast()
  expected <- list(
    select = list(
      type = 'serial',
      conds = list(
        list(
          op = '==',
          arg = list(type = 'bool', val = TRUE),
          type = 'span',
          path = c('event', 'even'),
          stream = list(name = 'S')
        ),
        list(
          # TODO: should have `within = list(seconds = 0)`
          op = '==',
          arg = list(type = 'bool', val = TRUE),
          type = 'span',
          path = c('event', 'event'),
          stream = list(name = 'S')
        ),
        list(
          op = '==',
          arg = list(type = 'bool', val = TRUE),
          type = 'span',
          path = c('event', 'event', 'event'),
          stream = list(name = 'S')
        )
      )
    )
  )
  expect_equal(real, expected)
})

test_that('all any serial', {
  foo <- Stream$new('foo')
  bar <- Stream$new('bar')
  baz <- Stream$new('baz')
  qux <- Stream$new('qux')
  quux <- Stream$new('quux')

  real <- select()$span(any_of(foo.x == TRUE, bar.y == TRUE))$then(baz.z == TRUE)$then(all_of(qux.α == TRUE, quux.β == TRUE))$to_ast()
  expected = list(
    select = list(
      type = 'serial',
      conds = list(
        list(
          type = 'any',
          conds = list(
            list(
              op = '==',
              arg = list(type = 'bool', val = TRUE),
              type = 'span',
              path = c('event', 'x'),
              stream = list(name = 'foo')
            ),
            list(
              op = '==',
              arg = list(type = 'bool', val = TRUE),
              type = 'span',
              path = c('event', 'y'),
              stream = list(name = 'bar')
            )
          )
        ),
        list(
          # TODO: within 0 seconds
          op = '==',
          arg = list(type = 'bool', val = TRUE),
          type = 'span',
          path = c('event', 'z'),
          stream = list(name = 'baz')
        ),
        list(
          type = 'all',
          conds = list(
            list(
              op = '==',
              arg = list(type = 'bool', val = TRUE),
              type = 'span',
              path = c('event', 'α'),
              stream = list(name = 'qux')
            ),
            list(
              op = '==',
              arg = list(type = 'bool', val = TRUE),
              type = 'span',
              path = c('event', 'β'),
              stream = list(name = 'quux')
            )
          )
        )
      )
    )
  )
  expect_equal(real, expected)
})

test_that('relative span', {
  s <- Stream$new('s')
  t <- Stream$new('t')

  real <- select()$span(
    span(s.x == TRUE, min=delta(years=1, months=1)) || span(t.x == TRUE, after=delta(minutes=11), within=delta(seconds=13))
    , max=delta(weeks=1)
  )$to_ast()

  expected <- list(
    select = list(
      expr = '||',
      args = list(
        list(
          op = '==',
          arg = list(type = 'bool', val = TRUE),
          type = 'span',
          path = c('event', 'x'),
          stream = list(name = 's'),
          `for` = list(`at-least` = list(months = 1, years = 1))
        ),
        list(
          op = '==',
          arg = list(type = 'bool', val = TRUE),
          type = 'span',
          path = c('event', 'x'),
          stream = list(name = 't'),
          within = list(seconds = 13),
          after = list(minutes = 11)
        )
      ),
      `for` = list(`at-most` = list(weeks = 1))
    )
  )
  expect_equal(real, expected)
})

test_that('nested relative spans', {
  s <- Stream$new('S')
  real <- select()$span(s.x < 0)$then(
    span(s.x == 0)$then(s.x > 0, within=delta(seconds = 1)),
    within = delta(seconds = 2)
  )$to_ast()

  expected <- list(
    select = list(
      type = 'serial',
      conds = list(
        list(
          op = '<',
          arg = list(type = 'double', val = 0),
          type = 'span',
          path = c('event', 'x'),
          stream = list(name = 'S')
        ),
        list(
          type = 'serial',
          conds = list(
            list(
              op = '==',
              arg = list(type = 'double', val = 0),
              type = 'span',
              path = c('event', 'x'),
              stream = list(name = 'S')
            ),
            list(
              op = '>',
              arg = list(type = 'double', val = 0),
              type = 'span',
              path = c('event', 'x'),
              stream = list(name = 'S'),
              within = list(seconds = 1)
            )
          ),
          within = list(seconds = 2)
        )
      )
    )
  )

  expect_equal(real, expected)
})
