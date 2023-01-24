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

--------
# Question 3

Using data from NYC-TLC dataset (green_tripdata_2019-01 and taxi+_zones_lookup) 


------
# Question 4






