from argparse import ArgumentParser
import pandas as pd
import shutil
import yaml
import os

def get_parser():
    parser = ArgumentParser(description = 'ChroniclePy Tests')
    parser.add_argument('--directory', action='store')
    return parser

def main():
    opts = get_parser().parse_args()
    expected_file = os.path.join(
        opts.directory, 
        'resources/constants/constants.yaml'
    )
    encountered_file =  os.path.join(
        opts.directory, 
        'resources/output/summary_hourly.csv'
    )
    
    with open(expected_file, 'r') as outfile:
        expected_output = yaml.safe_load(outfile)

    encountered_output = pd.read_csv(encountered_file).to_dict()
    if not encountered_output == expected_output:
        raise ValueError("This isn't right !")
    else:
        print("Test successful...")
        shutil.rmtree(os.path.join(opts.directory, "resources/preprocessed"))
        shutil.rmtree(os.path.join(opts.directory, "resources/output"))
    
if __name__ == '__main__':
    main()

