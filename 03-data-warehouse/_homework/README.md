## PAVEL GARANIN

# DATA ENGINEERING ZOOMCAMP by DataTalksClub
### | Module 03: Data Warehouse + BigQuery |

---
### HOMEWORK

#### Q1:
***A1: 20,332,093***
![](images/h3-1.png)


#### Q2:
***A2: 0 MB for the External Table and 155.12 MB for the Materialized Table***
![](images/h3-2a.png)
![](images/h3-2b.png)


#### Q3:
***A3: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.***
![](images/h3-3a.png)
![](images/h3-3b.png)


#### Q4:
***A4: 8,333***
![](images/h3-4.png)


#### Q5:
***A5: Partition by tpep_dropoff_datetime and Cluster on VendorID***
![](images/h3-5.png)


#### Q6:
***A6: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table***
![](images/h3-6a.png)
![](images/h3-6b.png)


### Q7:
***A7: GCP Bucket***


### Q8:
***A8: False***


### Q9:
***A9: 0B, because BigQuery saves ths in table details on creation.***
![](images/h3-9.png)
