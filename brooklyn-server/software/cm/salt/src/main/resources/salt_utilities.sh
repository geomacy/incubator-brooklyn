#!/usr/bin/env bash

function state_exists () {
  salt-call --local state.sls test=True $1 2>/dev/null | grep Succeeded >/dev/null 2>&1
}

function verify_states () {
    for state in "$@" ; do
      state_exists $state || { >&2 echo $state not found ; exit 1 ; }
    done
}

function find_states () {
    for state in "$@" ; do
      if state_exists $state ; then
        echo $state
      fi
    done
}
