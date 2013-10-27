#!/usr/bin/env bash
cd ../third_party
find . -type f -print0 | xargs -0 sed -i 's/github.com\/syndtr\/goleveldb\/leveldb/github.com\/borgenk\/qdo\/third_party\/github.com\/syndtr\/goleveldb\/leveldb/g'
find . -type f -print0 | xargs -0 sed -i 's/code.google.com\/p\/snappy-go\/snappy/github.com\/borgenk\/qdo\/third_party\/code.google.com\/p\/snappy-go\/snappy/g'
