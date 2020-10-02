#!/bin/bash

# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

config_file_path=$3
x=$(expr 1 + $(echo $config_file_path | cut -d '.' -f 1 | grep -o -i / | wc -l))
postfix=$(echo $config_file_path | cut -d '.' -f 1 | cut -d '/' -f $x)
echo $postfix

CONTAINER="bridg-tranformer-3"
echo "Starting Container $CONTAINER"

docker rm -vf $CONTAINER
docker build --no-cache -t $CONTAINER .
docker run -t --name $CONTAINER $CONTAINER "$@"

