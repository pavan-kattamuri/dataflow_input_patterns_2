import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io import WriteToText


project_id = 'PROJECT_ID'
topic1 = 'topic1_name'
topic2 = 'topic2_name'
bucket1 = 'bucket1_name'
bucket2 = 'bucket1_name'
bucket3 = 'bucket1_name'


def add_attributes(elem):
    output_elem = {
        "data": {},
        "attributes": {}
    }
    if elem.data:
        output_elem['data'] = json.loads(elem.data)
    if elem.attributes:
        output_elem['attributes'] = elem.attributes
    return output_elem


def partition_fn(elem, n):
    print(elem)
    if elem['data']['bucket'] == bucket1:
        return 0
    else:
        return 1


def run(argv=None):
    # argument parser
    parser = argparse.ArgumentParser()

    # pipeline options, google_cloud_options
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.save_main_session = True

    p = beam.Pipeline(options=pipeline_options)

    # Create two PCollections by reading from two different pubsub topics
    p1 = p | 'read from topic 1' >> beam.io.ReadFromPubSub(topic='projects/{}/topics/{}'.format(project_id, topic1), with_attributes=True)
    p2 = p | 'read from topic 2' >> beam.io.ReadFromPubSub(topic='projects/{}/topics/{}'.format(project_id, topic2), with_attributes=True)

    # Merge the two PCollections and add attributes to the actual pubsub message
    merged = (p1, p2) | 'merge sources' >> beam.Flatten() | 'add attributes' >> beam.Map(add_attributes)

    topic1_rec, topic2_rec = merged | "split msgs" >> beam.Partition(partition_fn, 2)

    topic1_rec | "process bucket1 files" >> beam.Map(lambda x: x) | "write to bucket2" >> WriteToText("gs://{}/output/file".format(bucket2), file_name_suffix = '.csv' )
    topic2_rec | "process bucket2 files" >> beam.Map(lambda x: x) | "write to bucket3" >> WriteToText("gs://{}/output/file".format(bucket3), file_name_suffix = '.csv' )

    result = p.run()


if __name__ == "__main__":
    run()
