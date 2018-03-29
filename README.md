# r-sentenai

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
> library(httr)
> sentenai <- Sentenai$new("<auth_key>")
> s <- Stream$new("my-stream-name")

# Get stats about stream
> sentenai$get(s)

# Get all streams for account
> sentenai$streams()

# All fields in a stream
> sentenai$fields(s)

# Current values of all fields in a stream
> sentenai$values(s)
```
