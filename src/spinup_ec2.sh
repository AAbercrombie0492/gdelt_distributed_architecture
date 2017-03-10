#!/usr/bin/env bash

# Launches a r3.xlarge ECS instance, which ec2_bootstrap.sh will initialize and setup.
# User needs to provide a key (key-name) and set configure aws with $aws config
aws ec2 run-instances \
    --image-id ami-4ae1fb5d \
    --key-name keypair \
    --user-data file://aws/ec2_bootstrap.sh \
    --instance-type r3.xlarge \
    --ebs-optimized \
    --placement "AvailabilityZone=us-west-2a" \
    --block-device-mappings '{"DeviceName":"/dev/sda1","Ebs":{"DeleteOnTermination":false,"VolumeSize":1024}}' \
    --count 1
