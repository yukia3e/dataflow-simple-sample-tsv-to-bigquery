#! /bin/bash -eu
LOG_OUT=/var/dataflow/stdout.log
LOG_ERR=/var/dataflow/stderr.log
PROJECTID=
STORAGE_BUCKET=
WORK_BUCKET=
DATASET=
SLACK_API=https://hooks.slack.com/services/XXX

export GOOGLE_APPLICATION_CREDENTIALS="/root/dataflow-controller-serviceaccount.json"

echo "$(date) [start] import-bulk.sh [start]"
echo "$(date) [start] import-bulk.sh [start]" >> $LOG_OUT
sleep 3m

# Copy as many rows as you need and set the tdate
sh /var/dataflow/exec.sh $PROJECTID $STORAGE_BUCKET $WORK_BUCKET $DATASET 20200831 $SLACK_API >>$LOG_OUT 2>>$LOG_ERR
sh /var/dataflow/exec.sh $PROJECTID $STORAGE_BUCKET $WORK_BUCKET $DATASET 20200901 $SLACK_API >>$LOG_OUT 2>>$LOG_ERR
sh /var/dataflow/exec.sh $PROJECTID $STORAGE_BUCKET $WORK_BUCKET $DATASET 20200902 $SLACK_API >>$LOG_OUT 2>>$LOG_ERR

sleep 5m
echo "$(date) [end] import-bulk.sh [end]"
echo "$(date) [end] import-bulk.sh [end]" >> $LOG_OUT
sudo shutdown -h now
exit 0
