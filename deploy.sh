#!/bin/bash
ROOT_PATH=$(cd $(dirname $0) && pwd);
PROJECT_PATH='/root/stat_analyzer'

CREDS='root@iggy'
echo $CREDS
ssh $CREDS "mkdir -p $PROJECT_PATH/;cd $PROJECT_PATH/;"
sleep 1
rsync -azv --delete --exclude='.idea' --exclude='.git' --exclude='uploads' --exclude='venv' --exclude='.env' --exclude='vendor' --exclude='cache/*' --exclude='parameters.yml' --exclude='*.db' --exclude='*.txt' --exclude='*test*' --exclude='__pycache__' --exclude='testdata' --exclude='test_data' --exclude='main' --exclude='uniarbi.tar' $ROOT_PATH/* $CREDS:$PROJECT_PATH
sleep 1
ssh $CREDS "cd $PROJECT_PATH;go build -o stat_analyzer .;supervisorctl restart stat_analyzer"
echo `date` $CREDS