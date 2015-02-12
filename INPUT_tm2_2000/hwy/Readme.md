
The columns in bridge_tolls and value_tolls.csv are as follows.

**TOLL_XXX** values are in 2000 cents.

 Column Number | Column Name | Description
 --------------|-------------|-------------
  1 | **TOLLBOOTH** | Corresponds to the TOLLBOOTH field in the network. 0 is the default for all bridges.
  2 | **tollbooth description** | Comment string so the location makes sense
  3 | **time period** | One of EA, AM, MD, PM, EV - the time period to which the toll applies
  4 | **TOLL_TIME** | The tollbooth & time code. This is **TOLLBOOTH** + (1000 for EA, 2000 for AM, 3000 for MD, 4000 for PM or 5000 for EV). Used by [SetTolls.JOB](https://github.com/MetropolitanTransportationCommission/travel-model-two/blob/master/model-files/scripts/preprocess/SetTolls.JOB) to lookup the appropriate row.
  5 | **TOLL_DA** | The drive alone toll for this tollbooth and time period
  6 | **TOLL_S2** | The shared ride 2 toll for this tollbooth and time period
  7 | **TOLL_S3** | The shared ride 3+ toll for this tollbooth and time period
  8 | **TOLL_VSM** | The very small commercial trucks toll for this tollbooth and time period
  9 | **TOLL_SML** | The small commercial trucks toll for this tollbooth and time period
 10 | **TOLL_MED** | The medium commercial trucks toll for this tollbooth and time period
 11 | **TOLL_LRG** | The large commercial trucks toll for this tollbooth and time period
 12 | **feet multiplier** | 1 if the toll should be multiplied by **FEET**, or 0 if flat toll.  This is for
     `value_tolls.csv` only, since `bridge_tolls.csv` are all assumed to be flat.
