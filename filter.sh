#!/bin/bash

NFTRACK_DATA_DIR=$HOME/code/nftrack/data
NFTRACK_FILTER_DIR=$NFTRACK_DATA_DIR/filtered


filter_successful() {
    local infile=$1
    cat $infile | sed 's/,$//' | jq '{total_price, created_date, id}'
}

filter_created() {
    local infile=$1
    cat $infile | sed 's/,$//' | jq '{ending_price, created_date, id}'
}

