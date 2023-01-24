

# Week 1 - Setup and First Steps

<br/><br/>
## Docker + Postgres
----------------------------

### Docker

Docker provides consistent and easily reproducible environments, isolating your pipeline.

![](docker.png)

- Docker image: a blueprint of a container.
	- Docker file: a text document with all commands to create a Docker image.
- Docker container: is the isolated environment created.


###### Running a script with python and pandas in a container.

First, we create a simple python script:
```python
import sys
import pandas as pd

print(sys.argv)
date = sys.argv[1]

print(f'job finished successfully for {date}')
```

Next, we create a Docker file which runs the .py file we've just made:
```dockerfile
# Docker image with python 
FROM python:3.9

# create an image with pandas installed 
RUN pip install pandas

# working dir inside the container
WORKDIR /app

# COPY source file to destination (which will be in our workdir)
COPY pipeline.py pipeline.py

# executables that will run when container is initiated
ENTRYPOINT ["python", "pipeline.py"]
```

Now we create the container:
```bash
docker build -t test:pandas . # the dot will search the directory for a docker file
```

And then, we run it:
```bash
docker run -it test:pandas 2023-01-15 # date passed as an argument
```

<br/><br/>

### Postgres



```bash
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v C:\Users\sauld\OneDrive\Documents\Repos\de-zoomcamp-notes\week-1\content\ny_taxi_postgres_data \
    -p 5432:5432 \
postgres:13
```

Then in a new window (or tab):
```bash
pgcli -h localhost -p 5432 -u root -d ny_taxi
```


