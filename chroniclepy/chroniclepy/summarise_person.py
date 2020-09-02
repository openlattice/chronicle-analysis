from . import utils, summarise_modalities
from .constants import columns, interactions
from collections import Counter
from datetime import datetime
import pandas as pd
import numpy as np
import os
import re

def summarise_person(preprocessed,personID = None, quarterly=False, splitweek = True, 
    weekdefinition = 'weekdayMF', recodefile=None, includestartend = False,
    splitday = False, daytime = "10:00", nighttime = "22:00", maxdays = None
    ):

    # for now not using non-duration timepoints
    preprocessed = preprocessed[~preprocessed[columns.prep_datetime_end].isnull()]
    if len(preprocessed) == 0:
        utils.logger("WARNING: No data for %s..."%personID,level=1)
        return pd.DataFrame()

    preprocessed, datelist = utils.cut_first_last(preprocessed, includestartend, maxdays, first = preprocessed.firstdate.iloc[0], last = preprocessed.lastdate.iloc[0])

    if len(preprocessed) == 0:
        utils.logger("WARNING: No data for %s..."%personID,level=1)
        return pd.DataFrame()
    
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
    stdcols = ['participant_id', columns.full_name, columns.title, columns.raw_record_type, columns.prep_datetime_start,
               columns.prep_datetime_end, 'day', 'hour', 'quarter', 'date', columns.prep_record_type, columns.flags,
               columns.prep_duration_seconds, 'weekdayMTh', 'weekdaySTh', 'weekdayMF', columns.switch_app,
           'endtime', 'starttime', 'duration_minutes', 'index', 'firstdate', 'lastdate', 'study_id', 'Unnamed: 0']
    engagecols = [x for x in preprocessed.columns if 'engage' in x and not x.endswith('dur')]
    engageall = [x for x in preprocessed.columns if 'engage' in x]
    noncustom = set(stdcols).union(set(engageall))
    custom = set(preprocessed.columns)-noncustom
    engagecols = []
    
    for col in engagecols:
        preprocessed[col] = preprocessed[col].astype(int)

    data = {}
    data['daily'] = summarise_modalities.summarise_daily(preprocessed,engagecols, datelist)
    data['hourly'] = summarise_modalities.summarise_hourly(preprocessed,engagecols)

    if len(noncustom) > 0:
        appcoding = summarise_modalities.summarise_recodes(preprocessed,custom,quarterly=quarterly,hourly=True)
        data.update(appcoding)

    if quarterly:
        data['quarterly'] = summarise_modalities.summarise_quarterly(preprocessed,engagecols)

    if splitweek:
        if np.sum(preprocessed[weekdefinition]==1) > 0:
            data['week'] = summarise_modalities.summarise_daily(preprocessed[preprocessed[weekdefinition]==1].reset_index(drop=True),engagecols, datelist)
            data['week'].columns = ['%s'%x for x in data['week'].columns]
            if len(custom) > 0:
                weekapp = summarise_modalities.summarise_recodes(preprocessed[preprocessed[weekdefinition]==1],custom,quarterly=False,hourly=False)
                data['appcoding_week'] = weekapp['appcoding_daily']
        if np.sum(preprocessed[weekdefinition]==0) > 0:
            data['weekend'] = summarise_modalities.summarise_daily(preprocessed[preprocessed[weekdefinition]==0].reset_index(drop=True),engagecols, datelist)
            data['weekend'].columns = ['%s'%x for x in data['weekend'].columns]
            if len(custom) > 0:
                weekndapp = summarise_modalities.summarise_recodes(preprocessed[preprocessed[weekdefinition]==0],custom,quarterly=False,hourly=False)
                data['appcoding_weekend'] = weekndapp['appcoding_daily']

    if splitday:
        daytime_dt = datetime.strptime(daytime, "%H:%M") 
        nighttime_dt = datetime.strptime(nighttime, "%H:%M")
    
        # daytime
        daytime_df = preprocessed[(preprocessed.start_timestamp.dt.time > daytime_dt.time()) & (preprocessed.start_timestamp.dt.time < nighttime_dt.time())]
        
        if len(daytime_df) > 0:
            data['daytime'] = summarise_modalities.summarise_daily(daytime_df.reset_index(drop=True),engagecols, datelist)
            data['daytime'].columns = ['%s'%x for x in data['daytime'].columns]
            if len(custom) > 0:
                dayapp = summarise_modalities.summarise_recodes(daytime_df,custom,quarterly=False,hourly=False)
                data['appcoding_daytime'] = dayapp['appcoding_daily']
        else:
            utils.logger("WARNING: No daytime data for %s..."%personID,level=1)

        # nighttime
        nighttime_df = preprocessed[(preprocessed.start_timestamp < nighttime_dt) | (preprocessed.start_timestamp > nighttime_dt)]

        if len(nighttime) > 0:
            data['nighttime'] = summarise_modalities.summarise_daily(nighttime_df.reset_index(drop=True),engagecols, datelist)
            data['nighttime'].columns = ['%s'%x for x in data['nighttime'].columns]
            if len(custom) > 0:
                nightapp = summarise_modalities.summarise_recodes(nighttime_df,custom,quarterly=False,hourly=False)
                data['appcoding_nighttime'] = nightapp['appcoding_daily']
        else:
            utils.logger("WARNING: No nighttime data for %s..."%personID,level=1)

    for key,values in data.items():
        # get appsperminute
        appcnts = [x for x in values.columns if 'appcnt' in x]
        for col in appcnts:
            data[key][col.replace("appcnt","switchpermin")] = data[key][col]/data[key][col.replace("appcnt","dur")]
        data[key] = data[key].drop(appcnts,axis=1)
        data[key]['participant_id'] = personID

    return data
