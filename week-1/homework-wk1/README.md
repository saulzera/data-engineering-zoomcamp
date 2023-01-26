---------------------------------
# Question 1

When running _docker build_ which tag has the following text? - _Write the image ID to the file_

running:
```bash
docker build --help
``` 

output line with the answer:
--iidfile  string

---------------------

# Question 2

After running the docker python:3.9 image in an interactive mode and the entrypoint of bash how many python packages/modules are installed?

running:
```bash
docker run -it --entrypoint=bash python:3.9
```
then:
```bash
pip list
```

returns a list with 3 python packages (pip, setuptools and wheel) 

---------------------------------------------------------------


# Question 3

###### Using data from NYC-TLC dataset (green_tripdata_2019-01 and taxi+_zones_lookup)

How many taxi trips were totally made on January 15?

```sql
SELECT COUNT(1) FROM gt_2019
	WHERE DATE(lpep_pickup_datetime) = '20190115'
	AND DATE(lpep_dropoff_datetime) = '20190115';
```

> 20530
------
# Question 4

Which was the day with the largest trip distance? Use the pick up time for your calculations.

```sql
SELECT trip_distance, lpep_pickup_datetime FROM gt_2019
WHERE trip_distance = (SELECT MAX(trip_distance) FROM gt_2019)
```

> 2019-01-15
---------
# Question 5

In 2019-01-01 how many trips had 2 and 3 passengers?

```sql
SELECT passenger_count, COUNT(*)  FROM gt_2019
	WHERE DATE(lpep_pickup_datetime) = '2019-01-01'
	OR DATE(lpep_dropoff_datetime) = '2019-01-01'
GROUP BY passenger_count
```

> 2: 1282; 3: 254
-------------
# Question 6

For the passengers picked up in the Astoria Zone, which drop off zone had the largest tip? 
We want the name of the zone, not the id. (tip, not trip)

```sql

```





