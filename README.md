# r-sentenai

[![Build Status](https://travis-ci.org/Sentenai/r-sentenai.svg?branch=master)](https://travis-ci.org/Sentenai/r-sentenai)

> R client library for [Sentenai](http://sentenai.com/)

## Installation

```R
> devtools::install_github('sentenai/r-sentenai')
* installing *source* package ‘sentenai’ ...
** R
** tests
** preparing package for lazy loading
** help
*** installing help indices
** building package indices
** testing if installed package can be loaded
* DONE (sentenai)
```

## Usage

```R
> library(sentenai)
> sentenai <- sentenaiWithKey("<auth_key>")
> s <- stream("my-stream-name")

# Get stats about stream
> sentenai$get(s)

# Get all streams for account
> sentenai$streams()

# All fields in a stream
> sentenai$fields(s)

# Current values of all fields in a stream
> sentenai$values(s)

# Create filtered stream
> f <- stream("other-stream", V.foo == 'bar' && V.baz > 7)

# Query for spans
> sentenai$query(select()$span(s.temp > 32 && f.qux == FALSE))$spans()
```
