#!/bin/bash -ex

for host in <worker node1 IP> <worker node2 IP> ...
do
    ssh $host 'sudo apt update'
    ssh $host 'sudo apt upgrade -y'
    ssh $host 'sudo apt install openjdk-8-jre-headless scala -y'
    ssh $host 'wget https://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz'
    ssh $host 'tar xvf spark-2.4.3-bin-hadoop2.7.tgz'
    ssh $host 'sudo mv spark-2.4.3-bin-hadoop2.7/ /usr/local/spark'
    ssh $host "echo 'export PATH=/usr/local/spark/bin:\$PATH' >> ~/.profile"

    ssh $host 'sudo apt install python3-pip -y'
    ssh $host "pip3 install boto3"
done
