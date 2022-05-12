#!/bin/bash

OLD_VERSION=$1
NEW_VERSION=$2

sed -i s/${OLD_VERSION}/${NEW_VERSION}/g k8s/*.yaml
sed -i s/${OLD_VERSION}/${NEW_VERSION}/g charts/workfinder/values.yaml
sed -i s/${OLD_VERSION}/${NEW_VERSION}/g setup.py

git checkout -b "release-${NEW_VERSION}"
git commit -am "bump versions"
git push -u origin "release-${NEW_VERSION}"
