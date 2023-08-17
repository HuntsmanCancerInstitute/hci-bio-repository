#!/bin/bash

# Wrapper for uploading Analysis projects

ID=$1

trap "echo $ID failed; exit" SIGTERM SIGINT

MANAGER=$HOME/bin/manage_repository.pl
PROCESSER=$HOME/bin/process_analysis_project.pl
DB=$HOME/analysis2018.db
SBAPP=$HOME/bin/sb

echo "==> processing $ID"
echo

# create project, this will fail to upload
echo "==> running: $PROCESSER --cat $DB --upload --sbup /nowhere/up $ID"
$PROCESSER --cat $DB --upload $ID

# change directory
# echo "==> running: $MANAGER --cat $DB --path $ID"
DIR=`$MANAGER --cat $DB --path $ID`
echo "==> changing to $DIR"
cd $DIR

# set additional values
ID2=${ID,,}
DIV=`$MANAGER --cat $DB --status $ID | tail -n 1 | cut -f8`
DATE=$(date +%Y%m%d)

# start uploader
# example: sb upload start --destination scott-hale/a6789 --profile scott-hale *
echo "==> running:"
echo "==> $SBAPP upload start --name $ID --destination $DIV/$ID2 --profile $DIV"
echo
$SBAPP upload start --name $ID --destination $DIV/$ID2 --profile $DIV * && \
cd $HOME && $MANAGER --cat $DB --update_up $DATE $ID 

echo
echo "==> finished upload $ID on $DATE"


