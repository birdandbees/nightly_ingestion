#!/bin/bash

hadoop fs -mv ${WORKFLOW_HDFS_INPUT_DIR}/*.csv ${WORKFLOW_HDFS_INPUT_DIR}/archives/
if [[ $? != 0 ]]
then
				sh output_handler.sh username=admin password=admin STATUS=fail
else
				sh ooutput_handler.sh username=admin password=admin STATUS=success
fi


