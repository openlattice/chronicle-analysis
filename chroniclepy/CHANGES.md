0.0.1 - ??/??/2018
* initial release

1.1-rc2 - 04/02/2019
* add option for removing apps

1.2 - 05/26/2019
* when two applications are overlapping, discard the first when the second starts instead of closing
* subsetting was extracted into a different step, this allows for subsetting per person before summarising
* better logs

1.2-rc1 - 06/27/2019
* remove rounding of time
* change way of sorting apps

1.2-rc2 - 07/09/2019
* only shut off app when another one is moved to foreground (not background)

1.2-rc3 - 07/31/2019
* fix bug based on new backend

1.3 - 08/08/2019
* add possibility to split by daytime / nighttime
* add option for maximum number of days

1.3-rc1 - 08/29/2019
* better error logs
* add feature to get percentages of apps

1.4 - 09/27/2019
*  fix bugs on first and last days
*  add end-to-end test

1.5 - 01/29/2020
* adding device shutdowns

1.6 - 06/17/2020
* update to work with preprocessed data exported from OpenLattice
* backwards compatible
* better structure in code
* added UnitTests

1.7 - 09/01/2020
* minor bug fixes on column names
* fix missing record_type, summary, title
* add flags
* split up column names betweeen preprocessed and raw, progressbar
* bugfix when less records than steps
* remove progressbar, messes up rundeck
