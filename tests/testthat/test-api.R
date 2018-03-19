test_that('Sentenai takes your auth_key and host', {
  key <- 'abc123'
  host <- 'https://api.sentenai.com'

  s <- Sentenai$new(auth_key=key, host=host)
  expect_equal(s$auth_key, key)
  expect_equal(s$host, host)
})

test_that('Stream takes a name', {
  name <- 'boston-weather'
  s <- Stream$new(name=name)
  expect_equal(s$name, name)
})
