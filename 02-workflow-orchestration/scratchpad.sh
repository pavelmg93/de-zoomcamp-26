# cleanup BQ temp tables:

> for table in $(bq ls --max_results=1000 taxi_rides_dataset |
grep -E "_(2019|2020|2021)_[0-9]{2}(_ext)?\b" | 
awk '{print $1}'); 
do   bq rm -f -t taxi_rides_dataset.$table; 
done