#!/bin/bash
ROOT_PATH=$(cd $(dirname $0) && pwd);
PROJECT_PATH='/root/stat_analyzer'

VERSION=$(git describe --tags --always)
echo "Deploying version: $VERSION"

CREDS='root@iggy'
echo $CREDS

# Create and verify directory
ssh $CREDS "mkdir -p $PROJECT_PATH/;cd $PROJECT_PATH/;"
sleep 1

# Sync files
rsync -azv --delete --exclude='.idea' --exclude='.git' --exclude='uploads' --exclude='venv' --exclude='.env' --exclude='vendor' --exclude='cache/*' --exclude='parameters.yml' --exclude='*.db' --exclude='*.txt' --exclude='*test*' --exclude='__pycache__' --exclude='testdata' --exclude='test_data' --exclude='main' --exclude='uniarbi.tar' $ROOT_PATH/* $CREDS:$PROJECT_PATH

# Build and set permissions
ssh $CREDS "cd $PROJECT_PATH && \
    go build -ldflags=\"-X main.Version=$VERSION\" -o stat_analyzer . && \
    chmod +x stat_analyzer && \
    ls -l stat_analyzer && \
    ldd stat_analyzer && \
    ./stat_analyzer -v"

# Restart service
ssh $CREDS "supervisorctl restart stat_analyzer && supervisorctl status stat_analyzer"
echo `date` $CREDS