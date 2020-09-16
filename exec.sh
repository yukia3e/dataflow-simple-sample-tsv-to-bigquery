#!/bin/bash -eu
echo "$(date) [start] python3 importer.py --project $1 --storagebucket $2 --workbucket $3 --dataset $4 --tdate $5 [start]"
curl -X POST --data-urlencode "payload={\"channel\": \"#XXX\", \"username\": \"dataflow\", \"text\": \"${date} Dataflow import daily job start: ${5}\", \"icon_emoji\": \":ghost:\"}" $6
python3 /var/dataflow/importer.py --project $1 --storagebucket $2 --workbucket $3 --dataset $4 --tdate $5

ret=$?
if [ $ret = 0 ]; then
    curl -X POST --data-urlencode "payload={\"channel\": \"#XXX\", \"username\": \"dataflow\", \"text\": \"${date} Dataflow import daily job end: ${5}\", \"icon_emoji\": \":ghost:\"}" $6
    echo "$(date) [end] python3 importer.py --project $1 --storagebucket $2 --workbucket $3 --dataset $4 --tdate $5 [end]"
else
    echo "$(date) [error] python3 importer.py --project $1 --storagebucket $2 --workbucket $3 --dataset $4 --tdate $5 [error]"
fi
