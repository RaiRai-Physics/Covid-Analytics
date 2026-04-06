# Pipeline Flow

## Bronze to Silver
The Silver Glue job reads raw CSV data from the Bronze layer and standardizes the data.

### Cases dataset
Source:
- `bronze/nytimes/us_states.csv`

Standardization:
- convert `date` to `full_date`
- map state name to state code using `states_abv.csv`
- cast `cases` to `cases_cum`
- cast `deaths` to `deaths_cum`

### Testing dataset
Source:
- `bronze/covid_tracking/states_daily.csv`

Standardization:
- convert integer date `yyyyMMdd` to `full_date`
- standardize `state_code`
- cast:
  - `totalTestResults` → `tests_total_cum`
  - `positive` → `tests_pos_cum`
  - `negative` → `tests_neg_cum`

## Silver to Gold
The Gold Glue job reads Silver Parquet files and creates the warehouse model.

### `dim_date`
Created from the union of distinct dates from cases and testing.

Attributes:
- `date_id`
- `full_date`
- `year`
- `month`
- `day`
- `dow`
- `is_weekend`

### `dim_state`
Created from distinct state code/name combinations.

### `fact_cases_state_daily`
Created from Silver cases data using window functions.

Measures:
- `cases_cum`
- `deaths_cum`
- `new_cases`
- `new_deaths`

### `fact_testing_state_daily`
Created from Silver testing data using window functions.

Measures:
- `tests_total_cum`
- `tests_pos_cum`
- `tests_neg_cum`
- `new_tests`
- `positivity_rate`

## Snowflake Load
Gold Parquet files are loaded into Snowflake curated tables using `COPY INTO`.
