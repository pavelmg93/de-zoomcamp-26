## PAVEL GARANIN

# DATA ENGINEERING ZOOMCAMP by DataTalksClub
### | Module 02: Workflow Orchesration w/ Kestra + GCP + Gemini |

---
### HOMEWORK

#### Q1:
***A1: 128.3MB***


![02-homework-pavelgaranin-dezc-q1](images/02-homework-pavelgaranin-dezc-q1.png)

#### Q2:
***A2: green_tripdata_2020-04.csv***

```YAML
variables:
  file: "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"
```
becomes "green_tripdata_2020-04.csv"

#### Q3:
***A3: 24,648,499***


![02-homework-pavelgaranin-dezc-q3a](images/02-homework-pavelgaranin-dezc-q3a.png)
![02-homework-pavelgaranin-dezc-q3b](images/02-homework-pavelgaranin-dezc-q3b.png)
#### Q4:
***A4: 1,734,051***


![02-homework-pavelgaranin-dezc-q4a](images/02-homework-pavelgaranin-dezc-q4a.png)
![02-homework-pavelgaranin-dezc-q4b](images/02-homework-pavelgaranin-dezc-q4b.png)

#### Q5:
***A5: 1,925,152***


![02-homework-pavelgaranin-dezc-q5](images/02-homework-pavelgaranin-dezc-q5.png)screenshots

#### Q6:
***A6: Add a timezone property set to America/New_York in the Schedule trigger configuration***


```YAML
triggers:
  - id: green_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    timezone: America/New_York
    cron: "0 9 1 * *"
    inputs:
      taxi: green
```
