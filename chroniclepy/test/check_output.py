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
    
    # preprocessed
    files = {
        "preprocessing": {
            "expected": os.path.join(
                opts.directory,
                'resources/constants/constants_preprocessed.yaml'
            ), 
            "encountered": os.path.join(
                opts.directory, 
                'resources/preprocessed/ChronicleData_preprocessed-TestParticipant.csv'
            ),
            "folder": os.path.join(opts.directory, "resources/preprocessed")
        },
        "summarising": {
            "expected": os.path.join(
                opts.directory,
                'resources/constants/constants_summary.yaml'
            ), 
            "encountered": os.path.join(
                opts.directory, 
                'resources/output/summary_daily.csv'
            ),
            "folder": os.path.join(opts.directory, "resources/output")
        }
    }
        
    for keyword in files.keys():
        
        print("Testing pipeline for %s"%keyword)

        with open(files[keyword]['expected'], 'r') as outfile:
            expected_output = yaml.safe_load(outfile)

        encountered_output = pd.read_csv(files[keyword]['encountered']).to_dict()
        if not encountered_output == expected_output:
            print("    There are files with different values !!")
            for key, value in expected_output.items():
                if value != encountered_output[key]:
                    print("        Difference for %s: %s vs. %s"%(key, str(value[0]), str(encountered_output[key][0])))

            raise ValueError("    This isn't right !")
        else:
            print("    Test successful...")
            shutil.rmtree(files[keyword]['folder'])
    
if __name__ == '__main__':
    main()

