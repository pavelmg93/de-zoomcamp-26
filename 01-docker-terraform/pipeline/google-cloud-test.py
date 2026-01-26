from google.cloud import storage
# It automatically finds the credentials you just saved!
for bucket in storage.Client().list_buckets():
    print(f"✅ Success! Found bucket: {bucket.name}")