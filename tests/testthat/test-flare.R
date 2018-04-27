context('test-flare.R')

test_that('Stream takes a name', {
  name <- 'boston-weather'
  s <- stream(name)
  expect_equal(s$name, name)
  expect_equal(s$filter, NULL)
})

test_that('Stream can take filters', {
  name <- 'boston-weather'
  s <- stream(name, V.foo == 'bar')
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
  s <- stream(name = 'S')
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
  s <- stream(name = 'S')
  t <- stream(name = 'T')
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
  s <- stream(name = 'moose')
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
  s <- stream(name = 'S')
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
  s = stream('S', V.season == 'summer')
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
  s <- stream('S', V.season == 'summer' || V.season == 'winter')
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

test_that('returning', {
  s <- stream('weather')
  real <- select()$span(returning = from(s, list(value = V.maxTemp, other = list(constant = 3))))$to_ast()
  expected <- list(
    select = list(expr = TRUE),
    projections = list(
      explicit = list(
        list(
          stream = list(name = 'weather'),
          projection = list(
            value = list(
              list(var = c('event', 'maxTemp'))
            ),
            other = list(
              constant = list(
                list(lit = list(val = 3, type = 'double'))
              )
            )
          )
        )
      ),
      ... = TRUE
    )
  )
  expect_equal(real, expected)
})

test_that('realistic returning', {
  weather <- stream('weather')
  real <- select()$span(
    returning = from(weather, list(
      air = V.air_temp,
      air_unit = 'F',
      hum = V.humidity,
      hum_unit = '%',
      co2 = V.co2,
      co2_unit = 'ppm'
    ))
  )$to_ast()

  expected <- list(
    select = list(expr = TRUE),
    projections = list(
      explicit = list(
        list(
          stream = list(name = 'weather'),
          projection = list(
            air = list(
              list(var = c('event', 'air_temp'))
            ),
            air_unit = list(
              list(lit = list(val = 'F', type = 'string'))
            ),
            hum = list(
              list(var = c('event', 'humidity'))
            ),
            hum_unit = list(
              list(lit = list(val = '%', type = 'string'))
            ),
            co2 = list(
              list(var = c('event', 'co2'))
            ),
            co2_unit = list(
              list(lit = list(val = 'ppm', type = 'string'))
            )
          )
        )
      ),
      ... = TRUE
    )
  )
  expect_equal(real, expected)
})

test_that('returning op', {
  weather <- stream('weather')
  real <- select()$span(
    returning = from(weather, list(
      air = 5 / 9 * (V.air_temp - 32),
      air_unit = 'C'
    ))
  )$to_ast()

  expected <- list(
    select = list(expr = TRUE),
    projections = list(
      explicit = list(
        list(
          stream = list(name = 'weather'),
          projection = list(
            air = list(
              list(
                op = '*',
                lhs = list(
                  op = '/',
                  lhs = list(
                    lit = list(val = 5, type = 'double')
                  ),
                  rhs = list(
                    lit = list(val = 9, type = 'double')
                  )
                ),
                rhs = list(
                  op = '-',
                  lhs = list(
                    var = c('event', 'air_temp')
                  ),
                  rhs = list(
                    lit = list(val = 32, type = 'double')
                  )
                )
              )
            ),
            air_unit = list(
              list(lit = list(val = 'C', type = 'string'))
            )
          )
        )
      ),
      ... = TRUE
    )
  )
  expect_equal(real, expected)
})

test_that('switches', {
  s <- stream('S')
  real <- select()$span(s:(V.x < 0 -> V.x > 0))$to_ast()
  expected <- list(
    select = list(
      type = 'switch',
      stream = list(name = 'S'),
      conds = list(
        list(
          op = '<',
          arg = list(type = 'double', val = 0),
          path = c('event', 'x')
        ),
        list(
          op = '>',
          arg = list(type = 'double', val = 0),
          path = c('event', 'x')
        )
      )
    )
  )
  expect_equal(real, expected)
})

test_that('unary switch', {
  s <- stream('S')
  real <- select()$span(s:(TRUE -> V.x < 0))$to_ast()
  expected <- list(
    select = list(
      type = 'switch',
      stream = list(name = 'S'),
      conds = list(
        list(expr = TRUE),
        list(
          op = '<',
          arg = list(type = 'double', val = 0),
          path = c('event', 'x')
        )
      )
    )
  )
  expect_equal(real, expected)
})

test_that('unary switch to anything', {
  s <- stream('S')
  real <- select()$span(s:(V.x < 0 -> TRUE))$to_ast()
  expected <- list(
    select = list(
      type = 'switch',
      stream = list(name = 'S'),
      conds = list(
        list(
          op = '<',
          arg = list(type = 'double', val = 0),
          path = c('event', 'x')
        ),
        list(expr = TRUE)
      )
    )
  )
  expect_equal(real, expected)
})

test_that('serial', {
  s = stream('S')
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
  foo <- stream('foo')
  bar <- stream('bar')
  baz <- stream('baz')
  qux <- stream('qux')
  quux <- stream('quux')

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
  s <- stream('s')
  t <- stream('t')

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
  s <- stream('S')
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
