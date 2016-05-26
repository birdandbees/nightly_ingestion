#!/bin/bash

file_path=`echo -e "${WORKFLOW.HDFS_INPUT_PATH}" | perl -pe 's/(.*)\/(.*?)_incr/\2/'`

if [[ -z ${file_path} ]]
then
    exit 1
fi
source_schema='postgres_avant_basic_us_'
source_suffix='_1'
script_arg="${source_schema}${file_path}${source_suffix}"
script_path='/home/bedrock/scripts/nightly_ingestion/'
script_name='partition_by_year_month.py'
source_db='postgres'
target_db='postgres_refined'
STATUS='success'
scl enable python27 "${script_path}${script_name} --update --sourceTable ${script_arg}  --targetTable ${file_path} --sourceDB ${source_db} --targetDB ${target_db}"
if [[ $? != 0 ]]
then
        STATUS='fail'
fi


