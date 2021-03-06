% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/flare.R
\name{span}
\alias{span}
\title{Create a Span instance to restrict conditions
based on within/after/min/max/exactly.}
\usage{
span(query, within = NULL, after = NULL, min = NULL, max = NULL,
  exactly = NULL)
}
\arguments{
\item{within}{The maximum distance in time between the end of the previous
span and the start of this span.}

\item{after}{The minimum distance in time between the end of the previous
span and the start of this span.}

\item{min}{The minimum valid span duration `delta()`.}

\item{max}{The maximum valid span duration `delta()`.}

\item{exactly}{The exact valid span duration `delta()`.}
}
\value{
An instance of Delta.
}
\description{
Create a Span instance to restrict conditions
based on within/after/min/max/exactly.
}
