

# 1. Add the Google Cloud repo and key for Ubuntu Noble
sudo apt-get update && sudo apt-get install -y apt-transport-https ca-certificates gnupg curl
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# 2. Install the CLI and authenticate
sudo apt-get update && sudo apt-get install -y google-cloud-cli
gcloud auth application-default login

# Force the update despite the Yarn error and install the CLI
sudo apt-get update --allow-insecure-repositories
sudo apt-get install -y google-cloud-cli

# 3. Assign the Service Account Token Creator role to your user
gcloud iam service-accounts add-iam-policy-binding \
    de-zoomcamp-runner@project-c94cb82b-89ba-4659-98a.iam.gserviceaccount.com \
    --member="user:pavelmg93@gmail.com" \
    --role="roles/iam.serviceAccountTokenCreator"