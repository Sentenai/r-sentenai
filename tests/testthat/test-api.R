context('test-api.R')

test_that('Sentenai takes your auth_key and host', {
  key <- 'abc123'
  host <- 'https://other.sentenai.com'

  s <- sentenaiWithKey(key, host=host)
  expect_equal(s$auth_key, key)
  expect_equal(s$host, host)
})

test_that('Sentenai has default host', {
  key <- 'abc123'
  host <- 'https://api.sentenai.com'

  s <- sentenaiWithKey(key)
  expect_equal(s$auth_key, key)
  expect_equal(s$host, host)
})
