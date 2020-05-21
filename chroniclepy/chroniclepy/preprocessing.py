from datetime import timedelta
from pytz import timezone
import pandas as pd
import numpy as np
import os

from chroniclepy.constants import interactions
from chroniclepy import utils

def read_data(filenm):
    personid = "-".join(str(filenm).split(".")[-2].split("ChronicleData-")[1:])
    thisdata = pd.read_csv(filenm)
    thisdata['person'] = personid
    return thisdata
    

def clean_data(thisdata):
    '''
    This function transforms a csv file into a clean dataset:
    - only move-to-foreground and move-to-background actions
    - extracts person ID
    - extracts datetime information and rounds to 10ms
    - sorts events from the same 10ms by (1) foreground, (2) background
    '''

    thisdata = thisdata.dropna(subset=['ol.recordtype','ol.datelogged'])
    if len(thisdata)==0:
        return(thisdata)
    thisdata = thisdata[thisdata['ol.recordtype'] != 'Usage Stat']
    if not 'ol.timezone' in thisdata.keys() or any(thisdata['ol.timezone']==None):
        utils.logger("WARNING: Record has no timezone information.  Registering reported time.")
        thisdata['ol.timezone'] = "UTC"
    thisdata = thisdata[['general.fullname','ol.recordtype','ol.datelogged','person','ol.timezone']]
    # fill timezone by preceding timezone and then backwards
    thisdata = thisdata.sort_values(by=["ol.datelogged"]).reset_index(drop=True).fillna(method="ffill").fillna(method="bfill")
    thisdata['dt_logged'] = thisdata.apply(utils.get_dt,axis=1)
    thisdata['action'] = thisdata.apply(utils.get_action,axis=1)
    thisdata = thisdata.sort_values(by=['dt_logged', 'action']).reset_index(drop=True)
    return thisdata.drop(['action'],axis=1)

def get_timestamps(curtime, prevtime=False, row=None, precision=60):
    '''
    Function transforms an app usage statistic into bins (according to the desired precision).
    Returns a dataframe with the number of rows the number of time units (= precision).
    Precision in seconds.
    '''
    if not prevtime:
        starttime = curtime
        outtime = [{
            "start_timestamp": starttime,
            "end_timestamp": np.NaN,
            "date": starttime.strftime("%Y-%m-%d"),
            "starttime": starttime.strftime("%H:%M:%S.%f"),
            "endtime": np.NaN,
            "day": (starttime.weekday()+1)%7+1,
            "weekdayMF": 1 if starttime.weekday() < 5 else 0,
            "weekdayMTh": 1 if starttime.weekday() < 4 else 0,
            "weekdaySTh": 1 if (starttime.weekday() < 4 or starttime.weekday()==6) else 0,
            "hour": starttime.hour,
            "quarter": int(np.floor(starttime.minute/15.))+1,
            "duration_seconds": np.NaN,
            "log_type": np.NaN,
            "participant_id": row['person'],
            "app_fullname": row['general.fullname']
        }]
    
        return pd.DataFrame(outtime)

    #round down to precision

    prevtimehour = prevtime.replace(microsecond=0,second=0,minute=0)
    seconds_since_prevtimehour = np.floor((prevtime-prevtimehour).seconds/precision)*precision
    prevtimerounded = prevtimehour+timedelta(seconds=seconds_since_prevtimehour)
   
    # number of timepoints on precision scale (= new rows )
    timedif = (curtime-prevtimerounded)
    timepoints_n = int(np.floor(timedif.seconds/precision)+int(timedif.days*24*60*60/precision))
   
    # run over timepoints and append datetimestamps
    delta = timedelta(seconds=0)
    outtime = []
    
    for timepoint in range(timepoints_n+1):
        starttime = prevtime if timepoint == 0 else prevtimerounded+delta
        endtime = curtime if timepoint == timepoints_n else prevtimerounded+delta+timedelta(seconds=precision)
        
        outmetrics = {
            "start_timestamp": starttime,
            "end_timestamp": endtime,
            "date": starttime.strftime("%Y-%m-%d"),
            "starttime": starttime.strftime("%H:%M:%S.%f"),
            "endtime": endtime.strftime("%H:%M:%S.%f"),
            "day": (starttime.weekday()+1)%7+1,
            "weekdayMF": 1 if starttime.weekday() < 5 else 0,
            "weekdayMTh": 1 if starttime.weekday() < 4 else 0,
            "weekdaySTh": 1 if (starttime.weekday() < 4 or starttime.weekday()==6) else 0,
            "hour": starttime.hour,
            "quarter": int(np.floor(starttime.minute/15.))+1,
            "duration_seconds": (endtime-starttime).seconds
        }

        outmetrics['participant_id'] = row['person']
        outmetrics['app_fullname'] = row['general.fullname']
       
        delta = delta+timedelta(seconds=precision)
        outtime.append(outmetrics)

    return pd.DataFrame(outtime)

def extract_usage(dataframe,precision=3600):
    '''
    function to extract usage from a filename.  Precision in seconds.
    '''

    cols = ['participant_id',
            'app_fullname',
            'date',
            'start_timestamp',
            'end_timestamp',
            'starttime',
            'endtime',
            'day', # note: starts on Sunday !
            'weekdayMF',
            'weekdayMTh',
            'weekdaySTh',
            'hour',
            'quarter',
            'duration_seconds',
            'log_type']

    alldata = pd.DataFrame()
    rawdata = clean_data(dataframe)
    openapps = {}
    latest_unbackgrounded = False

    for idx, row in rawdata.iterrows():

        interaction = row['ol.recordtype']
        app = row['general.fullname']

        # decode timestamp and correct for timezone
        curtime = row.dt_logged
        curtime_zulustring = curtime.astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")

        if interaction == interactions.foreground:
            openapps[app] = {"open" : True,
                             "time": curtime}

            for olderapp, appdata in openapps.items():
                
                if app == olderapp:
                    continue
                                        
                if appdata['open'] == True and appdata['time'] < curtime:
                    
                    latest_unbackgrounded = {'unbgd_app':olderapp, 'fg_time':curtime, 'unbgd_time':appdata['time']}

                    utils.logger(f"WARNING: App {app} is moved to foreground on {curtime_zulustring}\
                                    but {olderapp} was still open.  Discarding {olderapp} now...")

                    openapps[olderapp]['open'] = False

        if interaction == interactions.background:

            if latest_unbackgrounded and app == latest_unbackgrounded['unbgd_app']:
                timediff = curtime - latest_unbackgrounded['fg_time']
                
                if timediff < timedelta(seconds=1):

                    timepoints = get_timestamps(curtime, latest_unbackgrounded['unbgd_time'], precision=precision, row=row)

                    timepoints['log_type'] = 'App Usage'

                    alldata = pd.concat([alldata,timepoints], sort=False)

                    openapps[app]['open'] = False

                    latest_unbackgrounded = False

            if app in openapps.keys() and openapps[app]['open']==True:

                # get time of opening
                prevtime = openapps[app]['time']

                if curtime-prevtime<timedelta(0):
                    raise ValueError("ALARM ALARM: timepoints out of order !!")

                # split up timepoints by precision
                timepoints = get_timestamps(curtime,prevtime,precision=precision,row=row)

                timepoints['log_type'] = 'App Usage'

                alldata = pd.concat([alldata,timepoints], sort=False)
                
                openapps[app]['open'] = False

        if interaction == interactions.power_off:
	        
            for app in openapps.keys():
            
                if openapps[app]['open'] == True:
                    
                    # get time of opening
                    prevtime = openapps[app]['time']
                    
                    if curtime-prevtime<timedelta(0):
                        raise ValueError("ALARM ALARM: timepoints out of order !!")
                    
                    # split up timepoints by precision
                    timepoints = get_timestamps(curtime,prevtime,precision=precision,row=row)
                    
                    timepoints['log_type'] = 'Power Off'

                    alldata = pd.concat([alldata,timepoints], sort=False)
                    
                    openapps[app] = {'open': False}
            
        if interaction == interactions.notification_seen:
            timepoints = get_timestamps(curtime, precision=precision, row=row)
            timepoints['log_type'] = 'Notification Seen'
            
            alldata = pd.concat([alldata,timepoints], sort=False)
            
        if interaction == interactions.notification_interruption:
            timepoints = get_timestamps(curtime, precision=precision, row=row)
            timepoints['log_type'] = 'Notification Interruption'
            
            alldata = pd.concat([alldata,timepoints], sort=False)
            
        if interaction == interactions.screen_non_interactive:
            timepoints = get_timestamps(curtime, precision=precision, row=row)
            timepoints['log_type'] = 'Screen Non-interactive'
            
            alldata = pd.concat([alldata,timepoints], sort=False)
        
        if interaction == interactions.screen_interactive:
            timepoints = get_timestamps(curtime, precision=precision, row=row)
            timepoints['log_type'] = 'Screen Interactive'
            
            alldata = pd.concat([alldata,timepoints], sort=False)

    if len(alldata)>0:
        alldata = alldata.sort_values(by=['start_timestamp','end_timestamp']).reset_index(drop=True)
        return alldata[cols].reset_index(drop=True)


def check_overlap_add_sessions(data, session_def = [5*60]):
    '''
    Function to loop over dataset, spot overlaps (and remove them), and add columns
    to indicate whether a new session has been started or not.
    '''
    data = data[data.duration_seconds > 0].reset_index(drop=True)

    # initiate session column(s)
    for sess in session_def:
        data['engage_%is'%int(sess)] = 0

    data['switch_app'] = 0
    # loop over dataset:
    # - prevent overlap (with warning)
    # - check if a new session is started
    for idx,row in data.iterrows():
        if idx == 0:
            for sess in session_def:
                data.at[idx, 'engage_%is'%int(sess)] = 1

        # check time between previous and this app usage
        nousetime = row['start_timestamp'].astimezone(timezone("CET"))-data['end_timestamp'].iloc[idx-1].astimezone(timezone("CET"))

        # check overlap
        if nousetime < timedelta(microseconds=0) and row['start_timestamp'].date == row['end_timestamp'].date:
            utils.logger("WARNING: Overlapping usage for participant %s: %s was open since %s when %s was openened on %s. \
            Manually closing %s..."%(
                row['participant_id'],
                data.iloc[idx-1]['app_fullname'],
                data.iloc[idx-1]['start_timestamp'].strftime("%Y-%m-%d %H:%M:%S"),
                row['app_fullname'],
                row['start_timestamp'].strftime("%Y-%m-%d %H:%M:%S"),
                data.iloc[idx-1]['app_fullname']
            ))
            data.at[idx-1,'end_timestamp'] = row['start_timestamp']
            data.at[idx-1,'duration_seconds'] = (data.at[idx-1,'end_timestamp']-data.at[idx-1,'start_timestamp']).seconds

        # check sessions
        else:
            for sess in session_def:
                if nousetime > timedelta(seconds = sess):
                    data.at[idx, 'engage_%is'%int(sess)] = 1

        # check appswitch
        data.at[idx,'switch_app'] = 1-(row['app_fullname']==data['app_fullname'].iloc[idx-1])*1
        data['firstdate'] = min(data['start_timestamp']).date()
        data['lastdate'] = max(data['end_timestamp']).date()
    return data.reset_index(drop=True)

def log_exceed_durations_minutes(row, threshold, outfile):
    timestamp = row['start_timestamp'].astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    with open(outfile, "a+") as fl:
        fl.write("Person {participant} used {app} more than {threshold} minutes on {timestamp}\n".format(
            participant = row['participant_id'],
            app = row['app_fullname'],
            threshold = threshold,
            timestamp = timestamp
        ))

def preprocess_dataframe(dataframe, precision=3600,sessioninterval = [5*60], logdir=None, logopts=None):
    tmp = extract_usage(dataframe,precision=precision)
    if not isinstance(tmp,pd.DataFrame) or np.sum(tmp['duration_seconds']) == 0:
        return None
        utils.logger("WARNING: File %s does not seem to contain relevant data.  Skipping..."%filename)
    data = check_overlap_add_sessions(tmp,session_def=sessioninterval)
    data['duration_minutes'] = data['duration_seconds']/60.
    data = utils.add_session_durations(data)
    
    if 'log_exceed_durations_minutes' in logopts.keys():
        if not os.path.exists(logdir):
            os.mkdir(logdir)
        for threshold in logopts['log_exceed_durations_minutes']:
            subset = data[data.duration_minutes > float(threshold)]
            outfile = os.path.join(logdir, "log_exceed_durations_minutes_%s.txt"%threshold)
            if len(subset) > 0:
                for idx, row in data[data.duration_minutes > threshold].iterrows():
                    log_exceed_durations_minutes(row, threshold, outfile)
    return data
    
    
def preprocess_folder(infolder,outfolder,precision=3600,sessioninterval = [5*60], logdir=None, logopts=None):

    if not os.path.exists(outfolder):
        os.mkdir(outfolder)

    for filename in [x for x in os.listdir(infolder) if x.startswith("Chronicle")]:
        utils.logger("LOG: Preprocessing file %s..."%filename,level=1)
        dataframe = read_data(os.path.join(infolder,filename))
        data = preprocess_dataframe(dataframe, precision=precision,sessioninterval = sessioninterval, logdir=logdir, logopts=logopts)
        if data is not None:
            outfilename = filename.replace('ChronicleData','ChronicleData_preprocessed')
            data.to_csv(os.path.join(outfolder,outfilename),index=False)
