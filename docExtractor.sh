#!/bin/sh

directory=$1

mvn exec:java -Dexec.mainClass="ep.db.extractor.DocumentParserService" -Dexec.args="${directory}"