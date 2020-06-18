from . import utils, summarise_person, summarise_app_categories, preprocessing
from .constants import columns, interactions
from datetime import datetime, timedelta
from collections import Counter
from pytz import timezone
import dateutil.parser
import pandas as pd
import numpy as np
import os
import re

def summary(infolder, outfolder, includestartend=False, recodefile=None, 
    fullapplistfile=None, quarterly = False, 
    splitweek = True, weekdefinition = 'weekdayMF',
    splitday = False, daytime = "10:00", nighttime = "22:00",
    maxdays = None
    ):
        
    if not os.path.exists(outfolder):
        os.mkdir(outfolder)

    files = [x for x in os.listdir(infolder) if x.startswith("Chronicle")]

    allapps = set()

    full = {}
    appcat = pd.DataFrame()
    
    for idx,filenm in enumerate(files):
        utils.logger("LOG: Summarising file %s..."%filenm,level=1)
        preprocessed = pd.read_csv(os.path.join(infolder,filenm))
        personID = str(filenm).replace("ChronicleData_preprocessed_","").replace(".csv", "")
        if not 'participant_id' in preprocessed.columns:
            preprocessed['participant_id'] = personID

        preprocessed = utils.backwards_compatibility(preprocessed)
        preprocessed = preprocessed.dropna(subset=[columns.full_name])
        preprocessed = preprocessing.add_preprocessed_columns(preprocessed)

        allapps = allapps.union(set(preprocessed[columns.full_name]))
        person = summarise_person.summarise_person(
            preprocessed,
            personID = personID,
            quarterly = quarterly,
            splitweek = splitweek,
            weekdefinition = weekdefinition,
            recodefile = recodefile,
            includestartend = includestartend,
            splitday = splitday,
            daytime = daytime,
            nighttime = nighttime,
            maxdays = maxdays
            )
        for k,v in person.items():
            if not k in full.keys():
                full[k] = v
            else:
                full[k] = pd.concat([full[k],person[k]],sort=True)

        if recodefile:
            app_percentages = summarise_app_categories.percentages(
                preprocessed,
                personID = personID,
                recodefile = recodefile         
            )
            appcat = appcat.append(app_percentages, ignore_index=True)        

    aggfuncs = {
        "daily":                ['mean','std'],
        "week":                 ['mean','std'],
        "weekend":              ['mean','std'],
        "appcoding_daily":      ['mean','std'],
        'quarterly':            ['mean','std'],
        "hourly":               ['mean'],
        'appcoding_hourly':     ['mean'],
        'appcoding_week':       ['mean'],
        'appcoding_weekend':    ['mean'],
        'daytime':              ['mean', 'std'],
        'nighttime':            ['mean', 'std'],
        'appcoding_daytime':    ['mean'],
        'appcoding_nighttime':  ['mean']
    }

    # run over all datasets, summarise and save
    for k,v in full.items():
        summary = v.fillna(0).groupby('participant_id').agg(aggfuncs[k])
        summary.columns = ['_'.join(col).strip() for col in summary.columns.values]
        if k == 'daily':
            summary['num_days'] = v[['dur','participant_id']].groupby('participant_id').agg(['count'])
        summary.to_csv(os.path.join(outfolder,"summary_%s.csv"%(k)))

    if isinstance(fullapplistfile,str):
        fullapplist = pd.DataFrame({"full_name": list(allapps)})
        if isinstance(recodefile,str):
            recode = pd.read_csv(recodefile,index_col='full_name').astype(str)
            fullapplist = pd.merge(fullapplist,recode,left_on='full_name',right_index=True,how='outer')
        fullapplist.to_csv(fullapplistfile,index=False)
    
    if recodefile:
        addedcol = list(set(appcat)-set(['count', 'percentage', 'personID']))[0]
        appcat = appcat.pivot(index="personID", columns = addedcol, values = 'percentage')
        appcat.to_csv(os.path.join(outfolder, "summary_appcoding_percentages.csv"))

