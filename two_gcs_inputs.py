import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io import ReadFromText, WriteToText


def run(argv=None):
    # argument parser
    parser = argparse.ArgumentParser()

    # pipeline options, google_cloud_options
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.save_main_session = True

    sources = ['gs://bucket_name/file1.csv', 'gs://bucket_name/file2.csv']

    p = beam.Pipeline(options=pipeline_options)

    for i, source in enumerate(sources):
        (p | "read source_{}".format(i+1) >> ReadFromText(source) \
           | "process source_{}".format(i+1) >> beam.Map(lambda x: x) \
           | "write source_{}".format(i+1) >> WriteToText("gs://bucket_name/output_{}/file".format(i+1), file_name_suffix = '.csv' )
        )

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
