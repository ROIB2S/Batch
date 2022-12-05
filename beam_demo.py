#!/usr/bin/env python3

import apache_beam as beam

def processline(line):
    yield line

def run():
    argv = [
    ]

    pipeline = beam.Pipeline(argv=argv)
    input = 'samples\*.csv'
    output = 'output\output'
    
    (pipeline
     | 'Read Files' >> beam.io.ReadFromText(input)
     | 'Process Lines' >> beam.FlatMap(lambda line: processline(line))
     | 'Write Output' >> beam.io.WriteToText(output)
     )
    pipeline.run()


if __name__ == '__main__':
    run()
