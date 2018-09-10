#!/bin/bash

BIN_NAME="audit_output"
CONFIG_NAME="config.ini"
TEMPLATE_NAME="template"

#install curl
apt-get install curl -y

#read config.ini
CONFIG_PATH=`cat $CONFIG_NAME | grep "config_path" | awk '{print $3}'`
BIN_PATH=`cat $CONFIG_NAME | grep "bin_path" | awk '{print $3}'`

echo $CONFIG_PATH
echo $BIN_PATH

#check dirs
if [ -d $CONFIG_PATH ];then
echo "exits"
else
echo "not exist"
mkdir $CONFIG_PATH
fi

#copy files
cp $CONFIG_NAME "$CONFIG_PATH/$CONFIG_NAME"
cp -r $TEMPLATE_NAME "$CONFIG_PATH/$TEMPALTE_NAME"
cp $BIN_NAME "/bin/$BIN_NAME"

