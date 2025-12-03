## Using Apache Beam to write an ETL Pipeline

-- building a batch pipeline in Apache Beam which takes raw from GCS to BigQuery.<br>
-- Using dataflow to run the Apache Beam pipeline.<br>
-- Parameterizing the execution of the pipeline<br>

### Setting up all the imports
```python
import argparse
import time
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner
```

### Creating the pipeline

Pipeline - the top level Beam object
A pipeline holds a DAG of data transforms. Conceptually the nodes of the DAG are transforms (PTransform objects) and the edges are values (mostly PCollection objects). The transforms take as inputs one or more PValues and output one or more PValue s.
The pipeline offers functionality to traverse the graph. The actual operation to be executed for each node visited is specified through a runner object.

a Pipeline object is created using a PipelineOptions object and the final line of the method runs the pipeline:

```python
options = PipelineOptions()
# Set options
p = beam.Pipeline(options=options)
# Do stuff
p.run()
    
```
## Runner in Apache Beam
DEFAULT_RUNNER= 'DirectRunner'<br>
The Direct Runner executes pipelines on your machine and is designed to validate that pipelines adhere to the Apache Beam model as closely as possible. Instead of focusing on efficient pipeline execution, the Direct Runner performs additional checks to ensure that users do not rely on semantics that are not guaranteed by the model. Some of these checks include:

enforcing immutability of elements<br>
enforcing encodability of elements<br>
elements are processed in an arbitrary order at all points<br>
serialization of user functions (DoFn, CombineFn, etc.)<br>

KNOWN_RUNNER_NAMES= ['DataflowRunner', 'BundleBasedDirectRunner', 'DirectRunner', 'SwitchingDirectRunner', 'InteractiveRunner', 'FlinkRunner', 'PortableRunner', 'SparkRunner', 'TestDirectRunner', 'TestDataflowRunner']

### Defining the run function
Inside the run function we are setting up the Beampipeline Options 
(here we are returning the view as Google Cloud Options)<br>
Other options are ['StandardOptions','S30Options','TestOptions','SetupOptions','ProfilingOptions','DebugOptions','WorkerOptions','HadoopFileSystemOptions','DirectOptions']
```python
# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('my-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner
```

```python
    # Static input and output
    input = 'gs://{0}/events.json'.format(opts.project)
    output = '{0}:logs.logs'.format(opts.project)
```
```python
    # Table schema for BigQuery
    table_schema = {
        "fields": [
            {
                "name": "ip",
                "type": "STRING"
            },
            {
                "name": "user_id",
                "type": "STRING"
            },
            {
                "name": "lat",
                "type": "FLOAT"
            },
            {
                "name": "lng",
                "type": "FLOAT"
            },
            {
                "name": "timestamp",
                "type": "STRING"
            },
            {
                "name": "http_request",
                "type": "STRING"
            },
            {
                "name": "http_response",
                "type": "INTEGER"
            },
            {
                "name": "num_bytes",
                "type": "INTEGER"
            },
            {
                "name": "user_agent",
                "type": "STRING"
            }
        ]
    }
```
```python
    # Create the pipeline

    p = beam.Pipeline(options=options)

    '''

    Steps:
    1) Read something
    2) Transform something
    3) Write something

    '''

    (p
        | 'ReadFromGCS' >> beam.io.ReadFromText(input)
        | 'ParseJson' >> beam.Map(lambda line: json.loads(line))
        | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            output,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()

if __name__ == '__main__':
  run()
```