import argparse, logging
from operator import add
import functools

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem


class GCSFileReader:
  """Helper class to read gcs files"""
  def __init__(self, gcs):
      self.gcs = gcs


class AddFilenamesFn(beam.DoFn):
    """ParDo to output a dict with filename and row"""
    def process(self, element, file_path):
        file_name = file_path.split("/")[-1]
        # yield (file_name, element) # use this to return a tuple instead
        yield {'filename':file_name, 'row':element}


# just logging output to visualize results
def write_res(element):
  logging.info(element)
  return element

def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  p = beam.Pipeline(options=PipelineOptions(pipeline_args))
  gcs = GCSFileSystem(PipelineOptions(pipeline_args))
  gcs_reader = GCSFileReader(gcs)

  # in my case I am looking for files that start with 'countries'
  BUCKET='BUCKET_NAME'
  result = [m.metadata_list for m in gcs.match(['gs://{}/sample*'.format(BUCKET)])]
  result = functools.reduce(add, result)

  # create each input PCollection name and unique step labels
  variables = ['p{}'.format(i) for i in range(len(result))]
  read_labels = ['Read file {}'.format(i) for i in range(len(result))]
  add_filename_labels = ['Add filename {}'.format(i) for i in range(len(result))]

  # load each input file into a separate PCollection and add filename to each row
  for i in range(len(result)):
    globals()[variables[i]] = p | read_labels[i] >> ReadFromText(result[i].path) | add_filename_labels[i] >> beam.ParDo(AddFilenamesFn(), result[i].path)

  # flatten all PCollections into a single one
  merged = [globals()[variables[i]] for i in range(len(result))] | 'Flatten PCollections' >> beam.Flatten() | 'Write results' >> beam.Map(write_res)

  p.run()

if __name__ == '__main__':
  run()