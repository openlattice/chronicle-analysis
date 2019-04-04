#!/usr/bin/python

from chroniclepy import preprocessing, summarising
from argparse import ArgumentParser
import os

def get_parser():
    parser = ArgumentParser(description = 'ChroniclePy: Preprocessing and Summarizing Chronicle data')
    parser.add_argument('stage', choices=['preprocessing','summary','all'],
        help = 'processing stage to be run: preprocessing or summary')
    parser.add_argument('input_dir', action='store',
        help = 'the folder with data files (csv\'s).')
    parser.add_argument('preproc_dir', action='store',
        help = 'the folder to write preprocessed files.')
    parser.add_argument('output_dir', action='store',
        help = 'the folder to write output files.')


    prepargs = parser.add_argument_group('Options for preprocessing the data.')
    prepargs.add_argument('--precision',action='store',type=int, default = 900,
        help = 'the precision in seconds for the output file. This defines the time \
            unit of the data.  Eg. if the data should be split up by the hour, use 3600.')
    prepargs.add_argument('--sessioninterval', action='append', default=['60'],
        help = 'the interval (in seconds) that define the start of a new session, i.e. \
            how long should the break be between 2 sessions of phone usages to be considered \
            a new session.')

    summaryargs = parser.add_argument_group('Options for summarising the data.')
    summaryargs.add_argument('--recodefile',action='store', default=None,
        help = 'a csv file with one column named "fullname" \
        with transformations of the apps (eg. category codes, other names,...)')
    summaryargs.add_argument('--subsetfile',action='store', default=None,
        help = 'a csv file with one column named "fullname" \
        with a column with 1/0 coding which apps to include in the summary statistics.')
    summaryargs.add_argument('--removefile',action='store', default=None,
        help = 'a csv file with one column named "fullname" \
        to indicate which apps to remove in the summary statistics.')
    summaryargs.add_argument('--fullapplistfile',action='store', default=None,
        help = 'a csv file that will be written with all applications \
        (to prepare/complete the recodefile).')
    summaryargs.add_argument('--includestartend', action='store_true', default=False,
        help = 'flag to include the first and last day in the summary table.')
    summaryargs.add_argument('--quarterly', action='store_true', default=False,
        help = 'flag to export quarterly summary statistics.')
    summaryargs.add_argument('--splitweek', action='store_true', default=False,
        help = 'flag to export summary statistics separately for week and weekend days.')
    summaryargs.add_argument('--weekdefinition', action='store', default='weekdayMF',
        help = 'One of "weekdayMF", "weekdayMTh", "weekdaySTh" to distinguish week and weekend\
        (only when using --splitweek flag)')
    return parser

def main():
    opts = get_parser().parse_args()

    if opts.stage=='preprocessing' or opts.stage=='all':
        preprocessing.preprocess(
            infolder = opts.input_dir,
            outfolder = opts.preproc_dir,
            precision = opts.precision,
            sessioninterval = [int(x) for x in opts.sessioninterval]
            )

    if opts.stage=='summary' or opts.stage=='all':
        if opts.precision > 15*60:
            raise ValueError("The precision is above a quarter and the minimum precision for summary is by quarter.")
        if isinstance(opts.weekdefinition,str):
            if opts.weekdefinition not in ['weekdayMTh', 'weekdaySTh', 'weekdayMF']:
                raise ValueError("Unknown weekday definition !")
        if opts.splitweek and not isinstance(opts.weekdefinition,str):
            raise ValueError("Please specify the weekdefinition if you want !")

        summarising.summary(
            infolder = opts.preproc_dir,
            outfolder = opts.output_dir,
            includestartend = opts.includestartend,
            recodefile = opts.recodefile,
            removefile = opts.removefile,
            subsetfile = opts.subsetfile,
            fullapplistfile = opts.fullapplistfile,
            quarterly = opts.quarterly,
            splitweek = opts.splitweek,
            weekdefinition = opts.weekdefinition
        )

if __name__ == '__main__':
    main()
