from chroniclepy import utils, summarise_person
from datetime import datetime, timedelta
from collections import Counter
from pytz import timezone
import pandas as pd
import numpy as np
import os
import re

def summary(infolder, outfolder, includestartend=False, recodefile=None, subsetfile=None, fullapplistfile=None, quarterly = False, splitweek = True, weekdefinition = 'weekdayMF'):

    if not os.path.exists(outfolder):
        os.mkdir(outfolder)

    files = [x for x in os.listdir(infolder) if x.startswith("Chronicle")]

    allapps = set()

    full = {}
    for idx,filenm in enumerate(files):
        utils.logger("LOG: Summarising file %s..."%filenm,level=1)
        preprocessed = pd.read_csv(os.path.join(infolder,filenm),
            parse_dates = ['start_timestamp','end_timestamp'],
            date_parser = lambda x: pd.to_datetime(x.rpartition('-')[0]),
            ).dropna(subset=['app_fullname'])
        allapps = allapps.union(set(preprocessed['app_fullname']))
        personID = "-".join(str(filenm).split(".")[-2].split("-")[1:]).replace("_preprocessed","")
        person = summarise_person.summarise_person(
            preprocessed,
            personID = personID,
            quarterly = quarterly,
            splitweek = splitweek,
            weekdefinition = weekdefinition,
            subsetfile = subsetfile,
            recodefile = recodefile,
            includestartend = includestartend
            )
        for k,v in person.items():
            if not k in full.keys():
                full[k] = v
            else:
                full[k] = pd.concat([full[k],person[k]],sort=True)

    aggfuncs = {
        "daily":            ['mean','std'],
        "week":             ['mean','std'],
        "weekend":          ['mean','std'],
        "appcoding_daily":  ['mean','std'],
        'quarterly':        ['mean','std'],
        "hourly":           ['mean'],
        'appcoding_hourly': ['mean'],
        'appcoding_week':   ['mean'],
        'appcoding_weekend':['mean']
    }

    # get prefix for subset for naming
    prefix = ""
    if isinstance(subsetfile,str):
        subset = pd.read_csv(subsetfile,index_col='full_name').astype(str)
        prefix = "%s_"%list(subset.columns)[0]

    # run over all datasets, summarise and save
    for k,v in full.items():
        summary = v.fillna(0).groupby('participant_id').agg(aggfuncs[k])
        summary.columns = ['_'.join(col).strip() for col in summary.columns.values]
        if k == 'daily':
            summary['num_days'] = v[['dur','participant_id']].groupby('participant_id').agg(['count'])
        summary.to_csv(os.path.join(outfolder,"%ssummary_%s.csv"%(prefix,k)))

    if isinstance(fullapplistfile,str):
        fullapplist = pd.DataFrame({"full_name": list(allapps)})
        if isinstance(recodefile,str):
            recode = pd.read_csv(recodefile,index_col='full_name').astype(str)
            fullapplist = pd.merge(fullapplist,recode,left_on='full_name',right_index=True,how='outer')
        fullapplist.to_csv(fullapplistfile,index=False)
