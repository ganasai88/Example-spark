#!/bin/bash

# Update the system
sudo apt-get update -y

# Install Python 3 and pip
sudo apt-get install -y python3 python3-pip

# Install necessary Python packages
sudo pip3 install boto3 botocore s3fs

# Verify installation
python3 -c "import boto3, botocore, s3fs; print('All dependencies installed successfully')"
