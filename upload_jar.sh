#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: PROG <VERSION_NUMBER>"
    exit 1
fi

VERSION=$1
VERSION_DIR=v$VERSION

cd target
rm -rf $VERSION_DIR
mkdir $VERSION_DIR
cp ocient-jdbc4-$VERSION-jar-with-dependencies.jar $VERSION_DIR
scp -r $VERSION_DIR user@ocient-archive:/home/user/www/ocientrepo/java/jdbc/