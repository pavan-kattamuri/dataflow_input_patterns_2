import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io import WriteToText


def run(argv=None):
    # argument parser
    parser = argparse.ArgumentParser()

    # pipeline options, google_cloud_options
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.save_main_session = True

    project_id = 'PROJECT_ID'
    topics = ['topic1', 'topic2']

    p = beam.Pipeline(options=pipeline_options)

    for i, topic in enumerate(topics):
        (p | 'read topic_{}'.format(i+1) >> beam.io.ReadFromPubSub(topic='projects/{}/topics/{}'.format(project_id, topic)) \
           | 'process topic_{}'.format(i+1) >> beam.Map(lambda x: json.loads(x))
           | "write topic_{}".format(i+1) >> WriteToText("gs://bucket_name/output_{}/file".format(i+1), file_name_suffix = '.csv' )
        )

    result = p.run()


if __name__ == "__main__":
    run()
