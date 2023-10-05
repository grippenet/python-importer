# Export Command

Export command is dedicated to populate data from surveys response table (pollster_results_*) into export table containing data for all the seasons (one export table by survey).

For each season, a yaml file is expected to configure the column to export to the table and eventual mapping.

Value in [] denote variable, value must be replaced by a real value

For each survey the following mapping structure is expected (one for each survey in the yaml file)
```yaml
[name]:
 source: [source_table]
 mapping:
  [target_col]: [source_expr]
  ...
```

[name] = Name of the survey, to be used to identify the target export table ('intake', 'weekly', 'vaccination')
[source_table]: Name of the table
[target_col]: Name of the target column in the export column (do not quote, it will be quoted)
[source_expr]: Source expression to be populated in the target_column. 

Source expressions :
 - ~ : (Yaml null value) = source_column with same name as target_column
 - "'FR'" = String value constant, note the double quote, to have a yaml value with quote
 

Example:

```yaml
intake:
 source: public.pollster_results_intake_2022
 mapping:
  country: "'FR'"
  global_id: ~
  timestamp: 
  Q0: 'CASE WHEN "Q0"=3 OR "Q0"=1 THEN 1 ELSE "Q0" END AS "Q0"'
  Q1: ~
  Q2: ~
  ...
```
