#!/bin/bash

hadoop fs -mv ${WORKFLOW_HDFS_INPUT_DIR}/*.csv ${WORKFLOW_HDFS_INPUT_DIR}/archives/
if [[ $? != 0 ]]
then
				sh /opt/zaloni/bedrock/support/scripts/output_handlers/bedrock_output_handler.sh username=admin password=admin STATUS=fail
else
				sh /opt/zaloni/bedrock/support/scripts/output_handlers/bedrock_output_handler.sh username=admin password=admin STATUS=success
fi


