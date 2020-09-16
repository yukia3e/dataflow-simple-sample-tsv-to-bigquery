#! /bin/bash -eu
LOG_OUT=/var/dataflow/stdout.log
LOG_ERR=/var/dataflow/stderr.log
PROJECTID=
STORAGE_BUCKET=
WORK_BUCKET=
DATASET=
SLACK_API=https://hooks.slack.com/services/XXX

export GOOGLE_APPLICATION_CREDENTIALS="/root/dataflow-controller-serviceaccount.json"

echo "$(date) [start] import-daily.sh [start]"
echo "$(date) [start] import-daily.sh [start]" >> $LOG_OUT
sleep 3m

TARGET=$(date "+%Y%m%d" --date '2 day ago')
sh /var/dataflow/exec.sh $PROJECTID $STORAGE_BUCKET $WORK_BUCKET $DATASET $TARGET $SLACK_API >>$LOG_OUT 2>>$LOG_ERR

sleep 5m
echo "$(date) [end] import-daily.sh [end]"
echo "$(date) [end] import-daily.sh [end]" >> $LOG_OUT
sudo shutdown -h now
exit 0
