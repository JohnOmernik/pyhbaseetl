#!/bin/bash

MAPR_LIB="/opt/mapr/lib"
MAPR_INC="/opt/mapr/include"
IMG="pyhbaseetl"


cp -R $MAPR_LIB ./
cp -R $MAPR_INC ./

sudo docker build -t $IMG .

rm -rf ./lib
rm -rf ./include
