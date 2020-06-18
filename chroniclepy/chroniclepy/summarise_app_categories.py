from .constants import columns, interactions
from collections import Counter
from . import utils
import pandas as pd
import numpy as np
import os
import re

def percentages(preprocessed, personID = None, recodefile=None):
    
    recode = pd.read_csv(recodefile,index_col='full_name').astype(str)
    addedcol = list(set(recode.columns)-set('full_name'))[0]
    unique_apps = preprocessed \
        .drop_duplicates(columns.full_name)[['participant_id', columns.full_name]] \
        .merge(recode, how="left", right_on="full_name", left_on=columns.full_name) \
        .groupby(addedcol) \
        .agg({columns.full_name: 'count'}) \
        .reset_index()

    unique_apps.columns = [addedcol, 'count']
    unique_apps['percentage'] = unique_apps['count']/sum(unique_apps['count'])
    unique_apps['personID'] = personID

    return unique_apps
