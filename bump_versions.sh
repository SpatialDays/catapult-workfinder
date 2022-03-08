#!/bin/bash

OLD_VERSION=$1
NEW_VERSION=$2

sed -i s/${OLD_VERSION}/${NEW_VERSION}/g k8s/*.yaml
sed -i s/${OLD_VERSION}/${NEW_VERSION}/g setup.py
