import os
import requests
import google.auth
import google.auth.transport.requests
from flask import Flask, jsonify
from google.cloud import storage, bigquery

app = Flask(__name__)

# Core Environment Variables
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "aic-gcp-enablement")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "aic-gcp-enablement-sales-raw")
LOCATION = "asia-south2"
REPO_ID = "analytics-pipeline"
WORKFLOW_CONFIG_ID = "incremental-sales-sync"


@app.route("/", methods=["POST"])
def trigger_pipeline():
    try:
        # 1. Initialize Clients
        storage_client = storage.Client(project=PROJECT_ID)
        bq_client = bigquery.Client(project=PROJECT_ID)

        bucket = storage_client.bucket(BUCKET_NAME)
        # Target the raw sales file directly from your raw/ folder
        blob = bucket.blob("raw/sales.csv")

        if not blob.exists():
            return jsonify({
                "status": "skipped",
                "message": "raw/sales.csv not found"
            }), 404

        print("Found raw/sales.csv. Loading data to BigQuery bronze_layer.sales_raw...")

        # 2. Configure BigQuery Load Job (Overwrites staging with current file contents)
        dataset_ref = bq_client.dataset("bronze_layer")
        table_ref = dataset_ref.table("sales_raw")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=False
        )

        # Stream the file straight from GCS to BigQuery
        gcs_uri = f"gs://{BUCKET_NAME}/raw/sales.csv"

        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )

        load_job.result()

        print("Data successfully loaded into sales_raw. Fetching OAuth tokens...")

        # 3. Generate Auth Tokens to Hit Dataform API
        credentials, _ = google.auth.default()
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)

        # 4. Trigger the Dataform Workflow Config
        url = (
            f"https://dataform.googleapis.com/v1beta1/projects/"
            f"{PROJECT_ID}/locations/{LOCATION}/repositories/"
            f"{REPO_ID}/workflowInvocations"
        )

        headers = {
            "Authorization": f"Bearer {credentials.token}",
            "Content-Type": "application/json"
        }

        body = {
            "workflowConfig": (
                f"projects/{PROJECT_ID}/locations/{LOCATION}/repositories/"
                f"{REPO_ID}/workflowConfigs/{WORKFLOW_CONFIG_ID}"
            )
        }

        response = requests.post(url, headers=headers, json=body)

        if response.status_code in [200, 201]:
            print("Dataform incremental workflow successfully triggered.")
            return jsonify({
                "status": "success",
                "dataform_run": response.json()
            }), 200

        print(f"Dataform API failed: {response.text}")
        return jsonify({
            "status": "failed",
            "error": response.text
        }), 500

    except Exception as e:
        print(f"Unhandled exception: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8080))
    )