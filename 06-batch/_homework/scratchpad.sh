# Yello Taxi 2025 data source to fix the download script
# https://d37ci6vzurychx.cloudfront.net/trip-data
# Month 12 does not exist yet

# find and remove 0 size broken data files
find data/ -type f -exec ls -lh {} +
find data/ -type f -size -100c | xargs rm
# or
find data/ -type f -seize -100c -delete

# Fix Jupiter lock error
pip install --upgrade ipykernel traitlets pyzmq