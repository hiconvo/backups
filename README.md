# Convo Backups

These scripts take backups of the Cloud Datastore database, save them to a Cloud Bucket, and export them to BigQuery.

For `cloud_datastore_admin.py`, which takes the backups, I just followed the instructions [here](https://cloud.google.com/datastore/docs/schedule-export).

For `bigquery_import.py`, I made some adjustments to the original `cloud_datastore_admin.py` to get it to import the backups to BigQuery.

`bigquery_import.py` is dependent on `cloud_datastore_admin.py` having run on the same calendar day and to have generated the backups that it looks for to export to BigQuery. These backups are expected to be named as `parts-YYYY-MM-DD`.

The [`cron.yaml`](https://github.com/hiconvo/api/blob/master/cron.yaml) file is in the [API repo](https://github.com/hiconvo/api).
