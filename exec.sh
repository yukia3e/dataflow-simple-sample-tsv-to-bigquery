#!/bin/bash -eu
echo "$(date) [start] python3 importer.py --project $1 --storagebucket $2 --workbucket $3 --dataset $4 --tdate $5 [start]"
python3 /var/dataflow/importer.py --project $1 --storagebucket $2 --workbucket $3 --dataset $4 --tdate $5
echo "$(date) [end] python3 importer.py --project $1 --storagebucket $2 --workbucket $3 --dataset $4 --tdate $5 [end]"
