#!/bin/bash
for f in /data-perusahaan/log_alert_harian_PKS-*.json; do
  echo "Importing $f..."
  mongoimport --db sawit_alerts --collection log_alert_harian \
    --file "$f" --jsonArray --upsert --upsertFields _id
done
echo "=== SELESAI ==="
