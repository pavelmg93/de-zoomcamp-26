## PAVEL GARANIN

# DATA ENGINEERING ZOOMCAMP by DataTalksClub
### | Module 01: Docker + SQL + Terraform + GCP |

---
### HOMEWORK

#### Q1:
***A1: 25.3***


  (pipeline) > docker run -it --entrypoint=bash python:3.13
  root@f4ec0255d345:/# pip --version
  pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
  root@f4ec0255d345:/# 

#### Q2:
***A2: postgres:5432  OR  db:5432***


  pgAdmin can connect via container name (postgres) or service name (db).

#### Q3:
***A3: 8007***


  SELECT COUNT(1)
  FROM public.green_taxi_trips_2025_11
  WHERE trip_distance <= 1
  8009

#### Q4:
***A4: 2025-11-14***


  SELECT lpep_pickup_datetime
  FROM public.green_taxi_trips_2025_11
  WHERE trip_distance < 100
  ORDER BY trip_distance DESC LIMIT 1
  2025-11-14 15:36:27

#### Q5:
***A5: East Harlem North***


  SELECT z."Zone", SUM(t.total_amount) 
  FROM public.green_taxi_trips_2025_11 t
  JOIN zones z ON t."PULocationID" = z."LocationID"
  WHERE CAST(t.lpep_pickup_datetime AS DATE) = '2025-11-18'
  GROUP BY 1 
  ORDER BY 2 DESC 
  LIMIT 1;
  "East Harlem North"	9281.919999999991

#### Q6:
***A6: Yorkville West***


SELECT zdo."Zone", MAX(t.tip_amount) as max_tip
FROM public.green_taxi_trips_2025_11 t
JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE zpu."Zone" = 'East Harlem North'
GROUP BY 1
ORDER BY max_tip DESC
LIMIT 1;
"Yorkville West"	81.89

#### Q7:
***A7: terraform init, terraform apply -auto-approve, terraform destroy***
