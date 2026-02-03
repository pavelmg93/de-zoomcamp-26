git remote add upstream https://github.com/DataTalksClub/data-engineering-zoomcamp.git
git sparse-checkout init --cone
git sparse-checkout set 02-workflow-orchestration
git fetch upstream
git pull upstream main --allow-unrelated-histories