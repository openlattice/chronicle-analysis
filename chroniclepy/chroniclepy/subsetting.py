from chroniclepy import utils, summarise_person
from chroniclepy.constants import columns
from datetime import datetime, timedelta
from collections import Counter
from pytz import timezone
import pandas as pd
import numpy as np
import os
import re

def subset(infolder, outfolder, removefile=None, subsetfile = None):

    if not (isinstance(subsetfile,str) or isinstance(removefile,str)):
        return 0
        
    if not os.path.exists(outfolder):
        os.mkdir(outfolder)

    files = [x for x in os.listdir(infolder) if x.startswith("Chronicle")]
    for idx,filenm in enumerate(files):
        utils.logger("LOG: Subsetting file %s..."%filenm,level=1)
        preprocessed = pd.read_csv(os.path.join(infolder,filenm),
            parse_dates = [columns.datetime_start,columns.datetime_end],
            date_parser = lambda x: pd.to_datetime(x.rpartition('-')[0]),
            ).dropna(subset=[columns.full_name])
            
        if isinstance(subsetfile,str):
            subset = pd.read_csv(subsetfile,index_col='full_name').astype(str)
            if len(subset.columns)>1:
                utils.logger("WARNING: only 1 subset can be defined at a time.  Only the first column of the subset file will be used.")
            subsetname = list(subset.columns)[0]
            apps = list(subset.index[subset[subsetname]=='1'])
            preprocessed = preprocessed[preprocessed.app_fullname.isin(apps)].reset_index(drop=True)

        if isinstance(removefile,str):
            remove = pd.read_csv(removefile)['full_name']
            apps = list(remove)
            preprocessed = preprocessed[~preprocessed.app_fullname.isin(apps)].reset_index(drop=True)

        outfilename = filenm.replace('ChronicleData_preprocessed','ChronicleData_subsetted')
        preprocessed.to_csv(os.path.join(outfolder, outfilename),index=False)