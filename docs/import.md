# Import Command

Import command is dedicated to populate data from csv data exported from the influenzanet platfrom (using the management API) into
postgreSQL table (pollster_results_*)

For each season, a yaml file is expected to configure the column to import to the table and eventual mapping.

Value in $$ denote variable, value must be replaced by a real value

For each survey the following mapping structure is expected (one for each survey in the yaml file)
```yaml
_config:
  key_separator: '|' # Separator used between itemKey and responseKey
  migrations: 
    file: '../migration/migrations.json' # Location of the migrations.json file
# For each survey 
$name$:
 table: $table$
 csv_types:
    $colname: $colType
    ...
 prepare: # Preparation step
  - ...
  - ...
 mapping: # Mapping step, map a final column name to a SQL type
  $target_col$: $type_exp$
  ...
```

$name$ = Name of the survey, to be used to identify the export directory ('intake', 'weekly', 'vaccination')
$table$: Name of the target table
$target_col$: Name of the target column in the export column (do not quote, it will be quoted)
$source_expr$: Source expression to be populated in the target_column. 

Example:
```yaml
_config:
  key_separator: '|'
  migrations: 
    file: '../migration/migrations.json'
intake:
  table: intake_2023
  csv_types:
    Q22: int8
    intake.main.Q3: str
    intake.main.Q4b_0: str
  prepare:
    - timeelapsed: true
    - rename:
        "intake\\.main\\.": ""
        "intake\\.": ""
        "version": "survey_version"
        "submitted": "timestamp"
        "participantID": "global_id"
    - unjson:
        columns: 
          - 'Q6|mcg'
          - glob: 'Q6|mat.*'
        parser: items_keys
    - rename:
        "Q6\\|mcg": "Q6_5"
        "Q4h\\|5": "Q4h_5_open"
        "Q6\\|mat\\.row(\\d+)\\.col1": "Q6_\\1_open"
        "Q4b_0": Q4b_0_open
    - mcg: ['Q11','Q14','Q17', 'Q5']
    - indicator:
        "Q6_(\\d+)_open": "Q6_\\1"
    - boolean:
        columns: ['Q6_5']
        na_false: true
    - # Migration must be done after the rename of column global_id
      migration: {}
  mapping:
    ID: ~
    global_id: str
    session: ~
    engineVersion: ~
    language: ~
    QEnd: ~
    Q2: 'month-year'
    opened: ~
```

## csv_types

Define type of column to use for loading the csv file. This can be used to force the type of the column
to use ()

## Preparation Steps

Preparations steps will transform data at each step in order to fit the format to be imported into the table.
It can be as many as needed preparation step and it's possible to repeat the same kind of operation several times (for example
rename step can be repeated to simplify names at each step). 

Preparations steps are executed in order, a step gets the result of the previous step as input and pass the result to the next.

The preparations steps are described by a list (each entry is a list entry)
Each Preparation step is described by a dictionnary named by the name of the operation, the value is the configuration (can be simple value or a dictionnary for complex config).


```yaml
prepare:
  - timeelapsed: true
  - rename:
      "intake\\.main\\.": ""
      "intake\\.": ""
      "version": "survey_version"
      "submitted": "timestamp"
      "participantID": "global_id"
  - rename: # Possible to do the same type of step several times reusing result of the previous
      "Q6\\|mcg": "Q6_5"
      "Q4h\\|5": "Q4h_5_open"
      "Q6\\|mat\\.row(\\d+)\\.col1": "Q6_\\1_open"
      "Q4b_0": Q4b_0_open

```

Type of preparation steps:

- timeelapsed
- rename
- unjson
- indicator
- boolean
- migration
- mcg
- skip_if_null

### Columns selector

A column selector is an expression describing a way to select a list of columns (in the list of columns of the source data).

A selector can be:
  - a string (column name), used as a fixed
  - a list of string (list of column names)
  - a dictionary with either:
      'glob' key with pattern(s) (single string or list of pattern)
      're' key with pattern(s) (single string or list of pattern)

### timeelapsed

Create the timeelapsed column, and compute the time between the opening and submission of the survey

### rename

Rename column using regex pattern.
Config is a dictionary, each entry is a a rule to rename columns. 
To be read key => target
Where key is the column pattern to rename, value the target column name (possibily with replace pattern variable, e.g. \\1, \\2...)

```yaml
- rename: 
    "Q6\\|mcg": "Q6_5"
    "Q4h\\|5": "Q4h_5_open"
    "Q6\\|mat\\.row(\\d+)\\.col1": "Q6_\\1_open"
    "Q4b_0": Q4b_0_open

```


### unjson
Transform json provided columns into a flat structure (add new column)
Expected config:

- config: a list of column selectors to look for JSON value
- parser: the name of the parser for the JSON


```yaml
columns: 
    - 'Q6|mcg'
    - glob: 'Q6|mat.*'
parser: items_keys
```

columns: list of column selectors (fixed name of)
parser: how to parse the json

### mcg

Handle multiple choice question. Just indicate list of prefix of the question (Q6 for Q6.mcg|1)

### boolean
Force column to be boolean

### migration
Map new participant id to old participant Id. This step MUST appear once.

### skip_if_null

### indicator
Create a boolean variable (indicator) if another has non empty data
Config is a dictionary with a set of rules (like rename) with 
key is column selector where data are and the value the pattern to create indicator columns

Example:
```yaml
    - indicator:
        "Q6_(\\d+)_open": "Q6_\\1"
```
Crete a indicator named Q6_[x] where x is the value of the number in the middle of Q6_[x]_open (eg. Q6_5_open => Q6_5), this new column
takes value True if the source column has a value, False if not


## Mapping

Mapping can force to ignore (using null `~` operator) or force type of column. 
Only column to be modifed has to be in mapping section (columns with already good type are not needed to be mentionned)