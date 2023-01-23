#!/bin/bash

ID=$1
NAME=$2

trap "echo $ID failed; exit" SIGTERM SIGINT

MANAGER=$HOME/bin/manage_repository.pl
PROCESSER=$HOME/bin/process_request_project.pl
DB=$HOME/request2018.db
SBAPP=$HOME/bin/sb

echo
echo "==> processing $ID"

# check name, new upload or continuation upload
if [ -n "$NAME" ]
then 
  # additional upload to existing project
  echo "==> uploading to existing project"
else
  # create project
  # this should exit cleanly after making the project
  echo "==> running: $PROCESSER --cat $DB --upload $ID"
  $PROCESSER --cat $DB --upload $ID
  NAME=$ID
fi

# change directory
# echo "==> running: $MANAGER --cat $DB --path $ID"
DIR=`$MANAGER --cat $DB --path $ID`
echo "==> changing to $DIR"
cd $DIR

# set additional values
ID2=${ID,,}
DIV=`$MANAGER --cat $DB --status $ID | tail -n 1 | cut -f8`


# start uploader
# example: sb upload start --destination big-shot-pi/123456r --profile big-shot-pi --manifest-file 123456R_MANIFEST.csv
echo "==> running:"
echo "==> $SBAPP upload start --name $NAME --destination $DIV/$ID2 --profile $DIV --manifest-file ${ID}_MANIFEST.csv"
echo
# --chunk-size 16777216 \
$SBAPP upload start \
--name $NAME \
--destination $DIV/$ID2 \
--profile $DIV \
--manifest-file \
${ID}_MANIFEST.csv && \
cd $HOME && \
$MANAGER --cat $DB --update_up $(date +%Y%m%d) $ID

echo
echo "==> finished with $ID"
echo


