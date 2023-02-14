###### Top

# Data Lake

A repository that stores all kinds of raw data, making it available to a wide range of use cases.

![](data_lake.jpg)

With poor organization it might become a data swamp, turning it inaccessible to the users and providing little value.


# Workflow Orchestration

Governing a dataflow in a way that respects the **orchestration rules** and **business logic**, turning code into a workflow that we can schedule, run and observe.

##### Core Features
- Remote Execution
- Scheduling
- Retries
- Caching
- Integration with external systeams (API, database)
- Ad-hoc runs
- Parametrization
- Alerts

# Prefect

## Intro to Prefect Concepts


First we create an environment (I named it zoomcamp) and install all libraries from [requirements.txt]() through terminal:
```bash
conda install -r requirements.txt
```

#### Prefect Flow
Flows are like functions and any function can become a flow by using the @flow decorator, giving the following advantages:
- State transitions are reported to the API, allowing observation of flow execution.
- Input arguments types can be validated.
- Retries can be performed on failure.
- Timeouts can be enforced to prevent unintentional, long-running workflows.

We can transform the [ingest_data.py](https://github.com/saulzera/data-engineering-zoomcamp/blob/master/week-1/content/ingest_data.py) script into a Prefect Flow adding the @flow decorator before calling the main function.

#### Prefect Task
In a Prefect workflow, tasks are functions that receive metadata about upstream dependencies before they run, which could be used to have a task wait on the completion of another task before executing.

We can create a task to transform the data before ingesting it, for example, removing the trips with 0 passengers.



#### Blocks and Collections


#### Orion UI




# ETL with GPC & Prefect


#### Prefect Blocks: GCP Bucket

```bash
prefect block register -m prefect_gcp
```

Terminal output:
![](img/block_bucket_output.png)

> 


## Parametrizing Flow & Deployments with ETL into GCS flow


Using our _etl_web_to_gcs.py_ file as a base, we can parameterize a flow run.


```bash
prefect deployment build . ../parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"
```

Expected result:
```bash
Found flow 'etl-parent-flow'
Default '.prefectignore' file written to C:\Users\sauld\OneDrive\Documents\Repos\de-zoomcamp-notes\week-2\flows\gcp\.prefectignore
Deployment YAML created at 'C:\Users\sauld\OneDrive\Documents\Repos\de-zoomcamp-notes\week-2\flows\gcp\etl_parent_flow-deployment.yaml'.
Deployment storage None does not have upload capabilities; no files uploaded.  Pass --skip-upload to suppress this warning.
```

This creates an YAML file with the metadata from the flow, and we can set its parameters from there, such as 'color', 'months' and 'year'.

![](img/parameterized_etl.png)

Now we can apply them using command line:
```bash
prefect deployment apply etl_parent_flow-deployment.yaml
```





## Schedules & Docker Storage with Infrastructure

> Storage


```dockerfile
FROM prefecthq/prefect:2.7.7-python3.9  

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows
COPY flows/gcp/data /opt/prefect/data
```

Then create and run the image:
```bash
docker image build -t username/prefect:zoom .

docker image push username/prefect:zoom
```

Then create a docker-container block in prefect, and import it to a python file and run it:
```bash
python flows/docker_deploy.py
```

Before
```bash
prefect profile ls

prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

prefect agent start -q default
```



__[Back to Top](#top)__