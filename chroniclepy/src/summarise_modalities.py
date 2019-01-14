from collections import Counter
from chroniclepy import utils
import pandas as pd
import numpy as np
import os
import re

def summarise_daily(dataset,engagecols):

    # simple daily aggregate functions
    dailyfunctions = {
        "duration_minutes": ['sum'],
        "switch_app": ['sum']
    }
    engagedurfunctions = {"%s_dur"%k: [lambda x: np.mean(np.unique(x))] for k in engagecols}
    dailyfunctions.update(engagedurfunctions)
    engagecntfunctions = {k: ['sum'] for k in engagecols}
    dailyfunctions.update(engagecntfunctions)

    # group by date
    daily = dataset.groupby('date').agg(dailyfunctions)
    cols = ['dur','appcnt'] + ['%s_dur'%k for k in engagecols] + ['%s_cnt'%k for k in engagecols]
    daily.columns = cols
    daily.index = pd.to_datetime(daily.index)

    # fill days of no usage
    datelist = pd.date_range(start = np.min(dataset.start_timestamp).date(), end = np.max(dataset.end_timestamp).date(), freq='D')
    daily = utils.fill_dates(daily, datelist)

    return daily

def summarise_hourly(dataset,engagecols):

    # hourly daily aggregate functions
    hourlyfunctions = {
        "duration_minutes": ['sum'],
        'switch_app': ['sum']
    }
    hourlyengagefunctions = {k: ['sum'] for k in engagecols}
    hourlyfunctions.update(hourlyengagefunctions)

    # group by date/ hour
    hourly = dataset.groupby(['date','hour']).agg(hourlyfunctions)
    cols = ['dur','appcnt'] + ['%s_cnt'%k for k in engagecols]
    hourly.columns = cols

    # fill days/hours of no usage
    datelist = pd.date_range(start = np.min(dataset.start_timestamp).date(), end = np.max(dataset.end_timestamp).date(), freq='D')
    hourly = utils.fill_hours(hourly,datelist)

    # unstack hour index (long to wide)
    hourly = hourly.unstack('hour')
    hourly.columns = ["%s_h%i"%(x[0],int(x[1])) for x in hourly.columns.values]
    hourly.index = pd.to_datetime(hourly.index)

    return hourly

def summarise_quarterly(dataset,engagecols):

    # quarterly daily aggregate functions
    quarterlyfunctions = {
        "duration_minutes": {"dur": 'sum'},
        'switch_app': {"appcnt": "sum"}
    }
    quarterlyengagefunctions = {k: {"%s_num"%k: 'sum'} for k in engagecols}
    quarterlyfunctions.update(quarterlyengagefunctions)

    # group by date / hour / quarter
    quarterly = dataset.groupby(['date','hour','quarter']).agg(quarterlyfunctions)
    cols = ['dur','appcnt'] + ['%s_cnt'%k for k in engagecols]
    quarterly.columns = cols

    # fill day/hours/quarters of no usage
    datelist = pd.date_range(start = np.min(dataset.start_timestamp).date(), end = np.max(dataset.end_timestamp).date(), freq='D')
    quarterly = utils.fill_quarters(quarterly,datelist)

    # unstack hour and quarter index (long to wide)
    quarterly = quarterly.unstack('hour').unstack('quarter')
    quarterly.columns = ["%s_h%i_q%i"%(x[0],int(x[1]),int(x[2])) for x in quarterly.columns.values]
    quarterly.index = pd.to_datetime(quarterly.index)

    return quarterly

def summarise_appcoding_daily(dataset,addedcols):
    custom = None
    for addedcol in addedcols:
        dataset = dataset.fillna(value = {addedcol:"NA"})
        customgrouped = dataset[['date',addedcol,'duration_minutes']].groupby(['date',addedcol]).agg(sum).unstack(addedcol)
        customgrouped.columns = ["%s_%s_dur"%(addedcol,x) for x in customgrouped.columns.droplevel(0)]
        customgrouped.index = pd.to_datetime(customgrouped.index)
        if not isinstance(custom,pd.DataFrame):
            custom = customgrouped
        else:
            custom = pd.merge(custom,customgrouped, on='date')
    return custom

def summarise_appcoding_hourly(dataset,addedcols):
    custom = None
    datelist = pd.date_range(start = np.min(dataset.start_timestamp).date(), end = np.max(dataset.end_timestamp).date(), freq='D')
    for addedcol in addedcols:
        catlist = list(Counter(dataset[addedcol]).keys())
        dataset = dataset.fillna(value = {addedcol:"NA"})
        customgrouped = dataset[['date',addedcol,'duration_minutes','hour']].groupby(['date',addedcol,'hour']).agg(sum)
        customgrouped = utils.fill_appcat_hourly(customgrouped,datelist,catlist).unstack([addedcol,'hour'])
        customgrouped.columns = ["%s_%s_dur_h%i"%(addedcol,x[1],int(x[2])) for x in customgrouped.columns]
        customgrouped.index = pd.to_datetime(customgrouped.index)
        if not isinstance(custom,pd.DataFrame):
            custom = customgrouped
        else:
            custom = pd.merge(custom,customgrouped, on='date')
    return custom

def summarise_appcoding_quarterly(dataset,addedcols):
    custom = None
    datelist = pd.date_range(start = np.min(dataset.start_timestamp).date(), end = np.max(dataset.end_timestamp).date(), freq='D')
    for addedcol in addedcols:
        catlist = list(Counter(dataset[addedcol]).keys())
        dataset = dataset.fillna(value = {addedcol:"NA"})
        customgrouped = dataset[['date',addedcol,'duration_minutes','hour','quarter']].groupby(['date',addedcol,'hour','quarter']).agg(sum)
        customgrouped = utils.fill_appcat_quarterly(customgrouped,datelist,catlist).unstack([addedcol,'hour','quarter'])
        customgrouped.columns = ["%s_%s_dur_h%i_q%i"%(addedcol,x[1],int(x[2]),int(x[3])) for x in customgrouped.columns]
        customgrouped.index = pd.to_datetime(customgrouped.index)
        if not isinstance(custom,pd.DataFrame):
            custom = customgrouped
        else:
            custom = pd.merge(custom,customgrouped, on='date')
    return custom


def summarise_recodes(dataset,addedcols,quarterly=False,hourly=True):
    appcoding = {}

    for addedcol in addedcols:
        appcoding['appcoding_daily'] = summarise_appcoding_daily(dataset,addedcols)
        if hourly:
            appcoding['appcoding_hourly'] = summarise_appcoding_hourly(dataset,addedcols)
        if quarterly:
            appcoding['appcoding_quarterly'] = summarise_appcoding_quarterly(dataset,addedcols)

    return appcoding
