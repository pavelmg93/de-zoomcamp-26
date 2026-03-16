uv run python 10_spark_cluster_sql.py \
    --input_green=data/pq/green/2020 \
    --input_yello=data/pq/yellow/2020 \
    --output=data/report-2020

URL="spark://codespaces-472bba:7077" \

spark-submit \
    --master="${URL}" \
    10_spark_cluster_sql.py \
        --input_green=data/pq/green/2021 \
        --input_yello=data/pq/yellow/2021 \
        --output=data/report-2021
