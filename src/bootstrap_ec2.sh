#!/bin/bash

# Update and install critical packages
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y zip unzip curl bzip2 python-dev build-essential git libssl1.0.0 libssl-dev \
    software-properties-common debconf-utils python-software-properties

#
# Install Java and setup ENV
#
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update
echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections
sudo apt-get install -y oracle-java8-installer oracle-java8-set-default

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
echo "export JAVA_HOME=/usr/lib/jvm/java-8-oracle" | sudo tee -a /home/ubuntu/.bash_profile

#
# Install Miniconda
#
curl -Lko /tmp/Miniconda3-latest-Linux-x86_64.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x /tmp/Miniconda3-latest-Linux-x86_64.sh
/tmp/Miniconda3-latest-Linux-x86_64.sh -b -p /home/ubuntu/anaconda

export PATH=/home/ubuntu/anaconda/bin:$PATH
echo 'export PATH=/home/ubuntu/anaconda/bin:$PATH' | sudo tee -a /home/ubuntu/.bash_profile

#
# Install Clone repo, install Python dependencies
#
cd /home/ubuntu
git clone https://github.com/AAbercrombie0492/gdelt_distributed_architecture.git
cd /home/ubuntu/gdelt_distributed_architecture
export PROJECT_HOME=/home/ubuntu/gdelt_distributed_architecture
echo "export PROJECT_HOME=/home/ubuntu/Agile_Data_Code_2" | sudo tee -a /home/ubuntu/.bash_profile
pip install --upgrade pip
pip install -r requirements.txt
sudo chown -R ubuntu /home/ubuntu/gdelt_distributed_architecture
sudo chgrp -R ubuntu /home/ubuntu/gdelt_distributed_architecture
cd /home/ubuntu

#
# Cleanup
#
sudo apt-get clean
sudo rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
