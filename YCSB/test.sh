#!/bin/bash

source machines.sh

function join_by { local IFS="$1"; shift; caches="$*"; }

numcaches="4"

cache_ips=("${CACHE_IPS[@]:0:$numcaches}")
echo "Cache IPs: ${cache_ips[@]}"

caches=( "${cache_ips[@]/%/:11211}" )
join_by , ${caches[@]}
echo "Caches: $caches"
