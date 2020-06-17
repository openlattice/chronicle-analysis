from collections import Counter
from chroniclepy import utils
import pandas as pd
import numpy as np
import os
import re

def percentages(preprocessed, personID = None, recodefile=None):
    
    recode = pd.read_csv(recodefile,index_col='full_name').astype(str)
    addedcol = list(set(recode.columns)-set('full_name'))[0]
    unique_apps = preprocessed \
        .drop_duplicates("app_fullname")[['participant_id', columns.full_name]] \
        .merge(recode, how="left", right_on="full_name", left_on="app_fullname") \
        .groupby(addedcol) \
        .agg({"app_fullname": 'count'}) \
        .reset_index()

    unique_apps.columns = [addedcol, 'count']
    unique_apps['percentage'] = unique_apps['count']/sum(unique_apps['count'])
    unique_apps['personID'] = personID

    return unique_apps
