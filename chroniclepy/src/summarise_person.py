from chroniclepy import utils, summarise_modalities
from collections import Counter
import pandas as pd
import numpy as np
import os
import re

def summarise_person(preprocessed,personID = None, quarterly=False, splitweek = True, weekdefinition = 'weekdayMF', recodefile=None, subsetfile=None, includestartend = False):

    # this needs to happen **before** subsetting
    preprocessed = utils.add_session_durations(preprocessed)

    if not includestartend:
        preprocessed = utils.cut_first_last(preprocessed).reset_index(drop=True)

    if isinstance(subsetfile,str):
        subset = pd.read_csv(subsetfile,index_col='full_name').astype(str)
        if len(subset.columns)>1:
            utils.logger("WARNING: only 1 subset can be defined at a time.  Only the first column of the subset file will be used.")
        subsetname = list(subset.columns)[0]
        apps = list(subset.index[subset[subsetname]=='1'])
        preprocessed = preprocessed[preprocessed.app_fullname.isin(apps)].reset_index(drop=True)

    if isinstance(recodefile,str):
        recode = pd.read_csv(recodefile,index_col='full_name').astype(str)
        newcols = preprocessed.apply(lambda x: utils.recode(x,recode),axis=1)
        preprocessed[recode.columns] = newcols

    if splitweek:
        if np.sum(preprocessed[weekdefinition]==1)==0:
            utils.logger("WARNING: No weekday data for %s..."%personID,level=1)
        if np.sum(preprocessed[weekdefinition]==0)==0:
            utils.logger("WARNING: No weekend data for %s..."%personID,level=1)

    # split columns and get recode columns
    stdcols = ['participant_id', 'app_fullname', 'date', 'start_timestamp',
           'end_timestamp', 'day', 'hour', 'quarter',
           'duration_seconds', 'weekdayMTh', 'weekdaySTh', 'weekdayMF', 'switch_app',
           'endtime', 'starttime', 'duration_minutes', 'index']
    engagecols = [x for x in preprocessed.columns if x.startswith('engage') and not x.endswith('dur')]
    engageall = [x for x in preprocessed.columns if x.startswith('engage')]
    noncustom = set(stdcols).union(set(engageall))
    custom = set(preprocessed.columns)-noncustom

    for col in engagecols:
        preprocessed[col] = preprocessed[col].astype(int)

    data = {}
    data['daily'] = summarise_modalities.summarise_daily(preprocessed,engagecols)
    data['hourly'] = summarise_modalities.summarise_hourly(preprocessed,engagecols)

    if len(noncustom) > 0:
        appcoding = summarise_modalities.summarise_recodes(preprocessed,custom,quarterly=quarterly,hourly=True)
        data.update(appcoding)

    if quarterly:
        data['quarterly'] = summarise_modalities.summarise_quarterly(preprocessed,engagecols)

    if splitweek:
        if np.sum(preprocessed[weekdefinition]==1) > 0:
            data['week'] = summarise_modalities.summarise_daily(preprocessed[preprocessed[weekdefinition]==1].reset_index(drop=True),engagecols)
            data['week'].columns = ['%s'%x for x in data['week'].columns]
            if len(custom) > 0:
                weekapp = summarise_modalities.summarise_recodes(preprocessed[preprocessed[weekdefinition]==1],custom,quarterly=False,hourly=False)
                data['appcoding_week'] = weekapp['appcoding_daily']
        if np.sum(preprocessed[weekdefinition]==0) > 0:
            data['weekend'] = summarise_modalities.summarise_daily(preprocessed[preprocessed[weekdefinition]==0].reset_index(drop=True),engagecols)
            data['weekend'].columns = ['%s'%x for x in data['weekend'].columns]
            if len(custom) > 0:
                weekndapp = summarise_modalities.summarise_recodes(preprocessed[preprocessed[weekdefinition]==0],custom,quarterly=False,hourly=False)
                data['appcoding_weekend'] = weekndapp['appcoding_daily']

    for key,values in data.items():
        # get appsperminute
        appcnts = [x for x in values.columns if 'appcnt' in x]
        for col in appcnts:
            data[key][col.replace("appcnt","switchpermin")] = data[key][col]/data[key][col.replace("appcnt","dur")]
        data[key] = data[key].drop(appcnts,axis=1)
        data[key]['participant_id'] = personID

    return data
    #
# for key,values in data.items():
#     print(values.columns)
