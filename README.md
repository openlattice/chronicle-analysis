# chroniclepy

This repository contains code to preprocess data from the Chronicle app.

## Installing the software

The program is written using docker.  This makes is straightforward to use and only requires installing *one* program.  Visit [docker's homepage](https://www.docker.com/get-started) to install.

After installing, get the chronicle docker container.  Go to a terminal:

    docker pull openlattice/chroniclepy:v0.2

This will pull our container from https://hub.docker.com/r/openlattice/chroniclepy/.  If that worked, you're ready to preprocess your data !

## Tutorial for preprocessing and summary

### Get the example data

Click the button `clone or download` on github and click `Download ZIP`. Extract.
For what's next, we're assuming the data is in in `/Users/openlattice/chroniclepy/examples/`.   For your local application, replace `/Users/openlattice/chroniclepy/examples/` with the directory you put the data in.

### Run preprocessing, subsetting and summary

To run the data processing, run in the terminal:

    docker run \
      -v /Users/openlattice/chroniclepy/examples/:/Users/openlattice/chroniclepy/examples/ \
      openlattice/chroniclepy:v0.2 \
      all \
      /Users/openlattice/chroniclepy/examples/rawdata \
      /Users/openlattice/chroniclepy/examples/preprocessed \
      /Users/openlattice/chroniclepy/examples/subsetted \
      /Users/openlattice/chroniclepy/examples/output

If you'd want to set a folder as an environment variable for easier readability, you could run:

    FOLDER=/Users/openlattice/chroniclepy/examples/
    FOLDER=/Users/jokedurnez/Documents/accounts/CAFE/CAFE_code/chronicle/examples/

    docker run \
      -v $FOLDER:$FOLDER \
      openlattice/chroniclepy:v0.2 \
      all \
      $FOLDER/rawdata \
      $FOLDER/preprocessed \
      $FOLDER/subsetted \
      $FOLDER/output

##### A little bit of explanation of the parts  of this statement:
- `docker run`: Docker is a service of containers.  This statement allows to run our container.
- `-v $FOLDER:$FOLDER`: Docker can't access your local files.  This is very handy to re-run your analysis on a different computer since it only depends on itself.  However, that means that you have to tell Docker which folders on your computer it should be allowed to access.  This statement makes sure docker can read/write your files.
- `openlattice/chroniclepy`: This is the name of the container.
- `all`: To run the everything.  You can also separately run `preprocessing` and `summary`
- `$FOLDER/rawdata`: This is the folder where the raw data sits (the data you can download on the chronicle website).
- `$FOLDER/preprocessed`.  This is the folder where the preprocessed data will go to.
- `$FOLDER/subsetted`.  This is the folder where the subsetted data will go to.  **You need to define this folder even if you're not subsetting**.
- `$FOLDER/output`.  This is the folder where the preprocessed data will go to.

##### There are a few additional parameters passed to the program.
- Preprocessing arguments:
    - `--precision`: This is the precision in seconds.  This default is 3600 seconds (1 hour).  This means that when an app was used when the hour was passed (eg. 21.45-22.15), the data will be split up in two lines: *21.45-22.00* and *22.00-22.15*.  This allows to analyze the data by any time unit (eg. seconds for biophysical data, quarters for diary data,...).
    - `--sessioninterval`: This is the minimal interval (in seconds) of non-activity for an engagement to be considered a *new* engagement.  There can be multiple session intervals defined (i.e. `--sessioninterval=60 --sessioninterval=300`).  The default is 60 seconds (1 minute).
    - `--log_dir`: The directory where custom logs are put.
    - `--log_options`: Options for custom logs.  Example: `'{"log_exceed_durations_minutes": [5, 15]}'` will export a file with all app-usages over 5 and over 15 minutes. (watch out, the apostrophies need to match this format)
- Subsetting arguments:
    - `--subsetfile`: This is a file to select a specific subset of apps.  The format is the same as the recodefile, but is at this point restricted to one column.
    - `--removefile`: This is a file to remove a specific subset of apps.  The format is the same as the recodefile: a column with header `full_name` with names of apps.
- Summary arguments:
    - `--recodefile`: This is a file that has some more information on apps, for example categorisation.  This information will be added to the preprocessed data.  Refer to the example data for the exact format: the file needs one column `full_name` that has the apps, and the other columns are recode columns.  Summary statistics will be separately computed for all unique values in the recode columns.  Note that if a column is present with many different values, the program could get stuck on calculating statistics for all of these values.
    - `--fullapplistfile`: This is a file that will be written with all applications (to prepare/complete the recodefile).
    - `--includestartend`: Flag to include the first and last day.  These are cut off by default to keep the summary unbiased (due to missing data in the beginning of the start date or the end of the end date).
    - `--splitweek`: Flag to include the analyse separately week and weekend.  Requires argument `weekdefinition`.
    - `--weekdefinition`: One of `weekdayMF`, `weekdayMTh`, `weekdaySTh` to distinguish week and weekend (only when using `--splitweek` flag)

An example statement for the example data with all custom arguments:

    docker run -it \
      -v $FOLDER:$FOLDER \
      # for local development
      # -v /Users/jokedurnez/Documents/accounts/CAFE/CAFE_code/chronicle/src/:/opt/conda/lib/python3.7/site-packages/chroniclepy-1.2-py3.7/ \
      openlattice/chroniclepy:v0.2 \
      all \
      $FOLDER/rawdata \
      $FOLDER/preprocessed \
      $FOLDER/subsetted \
      $FOLDER/output \
      --precision=630 \
      --sessioninterval=300 \
      --splitweek --weekdefinition='weekdayMF' \
      --log_dir=$FOLDER/logs \
      --log_options='{"log_exceed_durations_minutes": [5, 15]}'
      

## Description of output

#### Preprocessed data

When running the preprocessing, the data is transformed into a table for each participant with the following variables:
- *participant_id:* Cut out from the csv name.
- *app_fullname:* The name of the app as it appears on the raw data.
- *date:* The date.
- *start_timestamp:* The start of the app usage
- *end_timestamp:* The end of the app usage
- *starttime:* The starttime of the app usage
- *endttime:* The endtime of the app usage
- *day:* The day of the week.  The week starts on Sunday, i.e. 1 = Sunday, 2 = Monday,...
- *weekdayMF:* Whether or not this is a weekday: 1 = Mon-Fri, 0 = Sat+Sun
- *weekdayMTh:* Whether or not this is a weekday: 1 = Mon-Thu, 0 = Fri-Sun
- *weekdaySTh:* Whether or not this is a weekday: 1 = Sun-Thu, 0 = Fri+Sat
- *hour:* Hour of day
- *quarter:* Quarter of hour: 1 = first (.00-.14), 2 = second,...
- *duration_seconds:* Duration (in seconds)
- *duration_minutes:* Duration (in minutes)
- *new_engage_(duration):* Whether a new session was initiated with this app usage based on the definition(s) of sessioninterval.

#### Subsetted data

The subsetted data looks exactly like the preprocessed data, but only for a subset of the apps.

#### Summary data

The summary analysis created the following tables:
- **summary_daily.csv:** Averages per day.
    - *participant_id*
    - *dur\_(mean/std):* mean/std daily usage (minutes)
    - *engage_(duration)\_cnt\_(mean/std):* mean/std number of sessions per day (wrt newsession definition)
    - *engage_(duration)\_dur\_(mean/std):* mean/std duration of sessions per day (wrt newsession definition)
    - *switchpermin_(mean/std):* mean/std on number of times apps are switched per minute
    - *num\_days:* Number of days of data collection
- **summary_hourly:** Averages per day for each hour of day.
  - *participant_id*
  - *dur\_(h0-h23)_mean:* mean daily usage (minutes)
  - *engage\_(duration)\_(h0-h23)\_cnt\_mean:* mean number of sessions per day (wrt newsession definition)
  - *engage\_(duration)\_(h0-h23)\_dur\_mean:* mean duration of sessions per day (wrt newsession definition)
  - *switchpermin\_(h0-h23)_mean:* mean on number of times apps are switched per minute
- **summary_quarterly:** (with `quarterly`-flag)
  - Equal output as the hourly summary, but with `(h0-h23)_(q1-q4)` to indicate each quarter per hour.
- **summary\_week.csv and summary_weekend.csv:** (with `splitweek`-flag)
  - Equal output as the daily summary, but separately for week and weekend days.
- **summary\_appcoding_(daily/hourly/quarterly/week/weekend):** (with `recodefile`-argument)
  - Equal output as daily and hourly summaries, but split out by codes/variables in the `recodefile`-argument

If a subsetfile is provided, the output summary files will have a prefix equal to the name of the column used in the subsetfile.

### Notes:

- Weekday starts on Sunday ! Weekday: 1-7 = Sun-Sat
- For any column in recodes: there will also be stats for NA, for the apps that are not described in the recode file.
- In the examples, we separate variables from strings with `\`.  This works on MacOSX.  Replace with `/` on Windows.
- If you know your way around python and you'd like to develop, feel free to submit pull requests.  Docker is only a level of abstraction to make usage easier, but there's many ways to directly interact with the python code:
    - after installing the python library, you can directly call functions (see `run.py` for arguments/example)
    - after installing the python library, you can for example run:

          python -i run.py all \
            $FOLDER/rawdata \
            $FOLDER/preprocessed \
            $FOLDER/subsetted \
            $FOLDER/output \
            --recodefile=$FOLDER\categorisation.csv \
            --fullapplistfile=$FOLDER\categorisation.csv \
            --removefile=$FOLDER\remove.csv \
            --precision=630 \
            --sessioninterval=300 \
            --includestartend --splitweek --weekdefinition='weekdayMF'

## Build container

      docker build -t openlattice/chroniclepy . --no-cache
      docker push openlattice/chroniclepy
