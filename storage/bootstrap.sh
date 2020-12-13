#!/bin/bash

function print_usage()
{
    echo "Usage: $0 <connstring to your postgresql>"
}

CONNSTRING=$1
[[ $CONNSTRING ]] || { print_usage; exit 1; }

psql -f "$(dirname $0)/schema.sql" "$CONNSTRING"