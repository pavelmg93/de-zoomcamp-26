git merge --abort

git remote add upstream https://github.com/DataTalksClub/data-engineering-zoomcamp.git
git sparse-checkout init --cone

# step 1
git sparse-checkout set 03-data-warehouse
# step 2
git checkout upstream/main -- 03-data-warehouse

git fetch upstream
git pull upstream main --allow-unrelated-histories


# -----------
# (Only if not already added)
git remote add upstream https://github.com/DataTalksClub/data-engineering-zoomcamp.git 

git fetch upstream

git checkout upstream/main -- 03-data-warehouse

git add 03-data-warehouse

git commit -m "Add module 03 files"

git push

git sparse-checkout disable
