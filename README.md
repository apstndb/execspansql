# execspansql

Yet another `gcloud spanner databases execute-sql` replacement for better composability.

## Usage: 

```
Usage of execspansql:
  -database string
        (required) ID of the database.
  -file string
        File name contains SQL query; exclusive with --sql
  -format string
        Output format; possible values(case-insensitive): json, json-compact, yaml; default=json
  -instance string
        (required) ID of the instance.
  -param value
        [name]=[Cloud Spanner type or literal]
  -project string
        (required) ID of the project.
  -query-mode string
        Query mode; possible values(case-insensitive): NORMAL, PLAN, PROFILE; default=PLAN
  -redact-rows
        Redact result rows from output
  -sql string
        SQL query text; exclusive with --file.
```

## Notable features

There are examples omitting some required options.

### Parameter support

Many Cloud Spanner clients don't support parameter.
Without modifications, query which have parameters are impossible to execute and query whose parameters' types are `STRUCT` or `ARRAY` are impossible to show query plans.

execspansql supports query with query parameters.

#### PLAN with complex typed parameters

```
$ execspansql --sql='SELECT * FROM UNNEST(@arr) WITH OFFSET' --query-mode=PLAN --param='arr=ARRAY<STRUCT<STRING>>'
```
```
$ execspansql --sql='SELECT @str.*' --query-mode=PROFILE --param='str=STRUCT<FirstName STRING, LastName STRING>'
```

#### PROFILE with complex typed parameterized values 

```
$ execspansql --sql='SELECT * FROM UNNEST(@arr) WITH OFFSET' --param='arr=[STRUCT<pk INT64, col STRING>(1, "foo"), (42, "foobar")]' --query-mode=PROFILE
```
```
$ execspansql --sql='SELECT * FROM Singers WHERE STRUCT<FirstName STRING, LastName STRING>(FirstName, LastName) IN UNNEST(@names)' \
              --param='names=[STRUCT<FirstName STRING, LastName STRING>("John", "Doe"), ("Mary", "Sue")]' \
              --query-mode=PROFILE
```
