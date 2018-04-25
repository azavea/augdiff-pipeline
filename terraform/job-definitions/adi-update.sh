#!/bin/bash

curl -sH 'Accept-encoding: gzip' $1 | gunzip -c - > /tmp/changeset.xml
echo ALL GOOD
echo $(cat /tmp/changeset.xml)


