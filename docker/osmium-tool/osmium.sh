#!/bin/bash

osmium derive-changes --keep-details $1 $2 -f osc -o $3
