from datetime import datetime, timedelta, timezone
from collections import Counter
import pandas as pd
import numpy as np
import pytz
import os
import re

def get_dt(row):
    '''
    This function transforms the reported (string) datetime to a timestamp.
    A few notes:
    - Time is rounded to 10 milliseconds, to make sure the apps are in the right order.
      A potential downside of this is that when a person closes and re-opens an app
      within 10 milliseconds, it will be regarded as closed.
    '''

    zulutime = row['ol.datelogged'].split("Z")[0]
    try:
        zulutime = datetime.strptime(zulutime,"%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        zulutime = datetime.strptime(zulutime,"%Y-%m-%dT%H:%M:%S")
    localtime = zulutime.replace(tzinfo=timezone.utc).astimezone(tz=pytz.timezone(row['ol.timezone']))
    return localtime

def get_action(row):
    '''
    This function creates a column with a value 0 for foreground action, 1 for background
    action.  This can be used for sorting (when times are equal: foreground before background)
    '''
    if row['ol.recordtype']=='Move to Foreground':
        return 0
    if row['ol.recordtype']=='Move to Background':
        return 1

def recode(row,recode):
    newcols = {x:None for x in recode.columns}
    if row['app_fullname'] in recode.index:
        for col in recode.columns:
            newcols[col] = recode[col][row['app_fullname']]

    return pd.Series(newcols)

def logger(message,level=1):
    time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    prefix = "༼ つ ◕_◕ ༽つ" if level==0 else "-- "
    print("%s %s: %s"%(prefix,time,message))

def fill_dates(dataset,datelist):
    '''
    This function checks for empty days and fills them with 0's.
    '''
    for date in datelist:
        if not date in dataset.index:
            newrow = pd.Series({k:0 for k in dataset.columns}, name=date)
            dataset = dataset.append(newrow)
    return dataset


def fill_hours(dataset,datelist):
    '''
    This function checks for empty days/hours and fills them with 0's.
    '''
    for date in datelist:
        datestr = date.strftime("%Y-%m-%d")
        for hour in range(24):
            multind = (datestr,hour)
            if not multind in dataset.index:
                newrow = pd.Series({k:0 for k in dataset.columns}, name=multind)
                dataset = dataset.append(newrow)
    return dataset

def fill_quarters(dataset,datelist):
    '''
    This function checks for empty days/hours/quarters and fills them with 0's.
    '''
    for date in datelist:
        datestr = date.strftime("%Y-%m-%d")
        for hour in range(24):
            for quarter in range(1,5):
                multind = (datestr,hour,quarter)
                if not multind in dataset.index:
                    newrow = pd.Series({k:0 for k in dataset.columns}, name=multind)
                    dataset = dataset.append(newrow)
    return dataset

def fill_appcat_hourly(dataset,datelist,catlist):
    '''
    This function checks for empty days/hours and fills them with 0's for all categories.
    '''
    for date in datelist:
        datestr = date.strftime("%Y-%m-%d")
        for hour in range(24):
            for cat in catlist:
                multind = (datestr,str(cat),hour)
                if not multind in dataset.index:
                    newrow = pd.Series({k:0 for k in dataset.columns}, name=multind)
                    dataset = dataset.append(newrow)
    return dataset

def fill_appcat_quarterly(dataset,datelist,catlist):
    '''
    This function checks for empty days/hours/quarters and fills them with 0's for all categories.
    '''
    for date in datelist:
        datestr = date.strftime("%Y-%m-%d")
        for hour in range(24):
            for quarter in range(1,5):
                for cat in catlist:
                    multind = (datestr,str(cat),hour,quarter)
                    if not multind in dataset.index:
                        newrow = pd.Series({k:0 for k in dataset.columns}, name=multind)
                        dataset = dataset.append(newrow)
    return dataset

def cut_first_last(dataset):
    dataset = dataset[
        (dataset['date'] != min(dataset['date'])) & \
        (dataset['date'] != max(dataset['date']))]
    return dataset.reset_index(drop=True)

def add_session_durations(dataset):
    engagecols = [x for x in dataset.columns if x.startswith('engage')]
    for sescol in engagecols:
        newcol = '%s_dur'%sescol
        sesids = np.where(dataset[sescol]==1)[0][1:]
        starttimes = np.array(dataset.start_timestamp.loc[np.append([0],sesids)][:-1])
        endtimes = np.array(dataset.end_timestamp.loc[sesids-1])
        durs = (endtimes-starttimes)/ np.timedelta64(1, 'm')
        dataset[newcol] = 0
        for idx,sesid in enumerate(np.append([0],sesids)):
            if idx == len(sesids):
                continue
            lower = sesid
            upper = len(dataset) if idx == len(sesids)-1 else sesids[idx+1]
            dataset.loc[np.arange(lower,upper),newcol] = durs[idx]
    return dataset
