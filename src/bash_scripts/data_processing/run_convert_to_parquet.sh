#!/bin/bash -ex

pushd ../../python3/data_processing/convert_to_parquet/

rm utils.zip || true
pushd ../../utils
zip utils.zip *
mv utils.zip ../data_processing/convert_to_parquet
popd

/usr/local/spark/bin/spark-submit --executor-memory 6g --master spark://ec2-3-219-180-255.compute-1.amazonaws.com:7077 --packages  com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3 --driver-class-path  /usr/local/spark/jars/postgresql-42.2.13.jar --jars /usr/local/spark/jars/postgresql-42.2.13.jar --py-files utils.zip convert_json_to_parquet.py --year 2019 --month 11

popd
