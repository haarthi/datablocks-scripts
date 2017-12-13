#!/usr/bin/env bash

snowsql -c snowadm -q "alter share looker_share_datablocks add accounts=$1;"
