import datetime
import httplib
import json
import logging
import webapp2

from google.appengine.api import app_identity
from google.appengine.api import urlfetch


# Exposes an endpoint that loads data from Datastore backups in Cloud storage
# into BigQuery. It requires a few GET parameters.
#
#     GET /bigquery_import?input_url_prefix=gs://convo-backups&dataset_id=backups&kind=User&kind=Event&kind=Thread&kind=Message
#
# This assumes that backups are in the referenced bucket in a folder named
# "parts-YYYY-MM-DD"
class Import(webapp2.RequestHandler):
    def get(self):
        access_token, _ = app_identity.get_access_token(
            "https://www.googleapis.com/auth/bigquery"
        )
        app_id = app_identity.get_application_id()
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
        kinds = self.request.get_all("kind")
        dataset_id = self.request.get("dataset_id")
        assert dataset_id

        input_url_prefix = self.request.get("input_url_prefix")
        assert input_url_prefix and input_url_prefix.startswith("gs://")

        for kind in kinds:
            request = {
                "configuration": {
                    "load": {
                        "sourceUris": [
                            "{input_url_prefix}/parts-{timestamp}/all_namespaces/kind_{kind}/all_namespaces_kind_{kind}.export_metadata".format(
                                input_url_prefix=input_url_prefix,
                                timestamp=timestamp,
                                kind=kind,
                            )
                        ],
                        "destinationTable": {
                            "projectId": app_id,
                            "datasetId": dataset_id,
                            "tableId": "{kind}-{timestamp}".format(
                                kind=kind, timestamp=timestamp
                            ),
                        },
                    }
                }
            }
            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer " + access_token,
            }
            url = (
                "https://bigquery.googleapis.com/upload/bigquery/v2/projects/%s/jobs"
                % app_id
            )
            try:
                result = urlfetch.fetch(
                    url=url,
                    payload=json.dumps(request),
                    method=urlfetch.POST,
                    deadline=120,
                    headers=headers,
                )
                if result.status_code == httplib.OK:
                    logging.info(result.content)
                elif result.status_code >= 500:
                    logging.error(result.content)
                else:
                    logging.warning(result.content)
            except urlfetch.Error:
                logging.exception("Failed to initiate import.")
                self.response.status_int = httplib.INTERNAL_SERVER_ERROR
                return

        self.response.status_int = result.status_code


app = webapp2.WSGIApplication([("/bigquery_import", Import)], debug=True)
