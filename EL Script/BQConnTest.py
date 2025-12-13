from google.cloud import bigquery

# Replace with your project ID
PROJECT_ID = "my2ndproject-479707"

client = bigquery.Client(project=PROJECT_ID)

print("Connected as:", client.project)

datasets = list(client.list_datasets())
print("Datasets in project:")

if datasets:
    for d in datasets:
        print(" -", d.dataset_id)
else:
    print("No datasets found")
