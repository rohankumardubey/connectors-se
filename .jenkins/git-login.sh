#!/usr/bin/env bash

set -xe

# Creates a ~/.netrc file with GitHub credentials set to connect to GitHub and its REST API
# $1: The git login
# $2: The git password
main() (
  gitLogin="${1?Missing git login}"
  gitPassword="${2?Missing git password}"

  {
    printf "machine github.com\n"
    printf "login %s\n" "${gitLogin}"
    printf "password %s\n" "${gitPassword}"

    printf "machine api.github.com\n"
    printf "login %s\n" "${gitLogin}"
    printf "password %s\n" "${gitPassword}"
  } > ~/.netrc

  chmod 600 ~/.netrc
)

main "$@"
