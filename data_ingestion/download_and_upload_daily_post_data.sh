#!/bin/bash -ex


######################################################################
## download_and_upload_daily_post_data.sh
##
## Download Reddit post data for a given month
## and year, unzip and upload the data
## to the specified AWS S3 bucket
##
## @param year          the calendar year (e.g. 2019)
## @param month         the month representedd as a two digit number
##                      (e.g. 01 for January and 12 for December)
## @param s3_bucket     the AWS S3 bucket url that will store
##                      store the data
######################################################################


if [ $# -ne 3 ];then
    set +x
    echo "Usage: ./download_and_upload_daily_post_data.sh YEAR MONTH AWS_BUCKET"
    echo "example: ./download_and_upload_daily_post_data.sh 2019 09 \"s3a:my-redit-bucket"
fi

#calendar year
YEAR=$1
#month represented as a two digit number
MONTH=$2
#url for the AWS S3 bucket
s3_bucket_name=$3

dir_name="$YEAR-$MONTH"
mkdir $dir_name
pushd $dir_name

#number of days in the given month
number_days_in_month=$(cal $MONTH $YEAR | awk 'NF {DAYS = $NF}; END {print DAYS}')
#link to pushshift data
url_base="https://files.pushshift.io/reddit/daily"
#name of file to download without file extension
file_name_base="RS_$YEAR-$MONTH"
#download post data for each day
for day in $(seq -f "%02g" 1 $number_days_in_month)
do
    file_name="${file_name_base}-$day.gz"
    wget "${url_base}/${file_name}"
    gunzip ${file_name}
done

popd

#upload data to AWS S3
aws s3 cp $dir_name ${s3_bucket_name}/posts/$dir_name --recursive
