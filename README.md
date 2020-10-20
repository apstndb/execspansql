# execspansql

Yet another `gcloud spanner databases execute-sql` replacement for better composability.

* Support query parameters
* Embedded jq
* Emit gRPC message logs

This tool is still pre-release quality and none of guarantees.

## Usage: 

```
Usage:
  execspansql [OPTIONS] [database]

Application Options:
      --sql=                             SQL query text; exclusive with --sql-file.
      --sql-file=                        File name contains SQL query; exclusive with --sql
      --project=                         (required) ID of the project. [$CLOUDSDK_CORE_PROJECT]
      --instance=                        (required) ID of the instance. [$CLOUDSDK_SPANNER_INSTANCE]
      --query-mode=[NORMAL|PLAN|PROFILE] Query mode. (default: NORMAL)
      --format=[json|yaml]               Output format. (default: json)
      --redact-rows                      Redact result rows from output
      --jq-filter=                       jq filter
      --compact-output                   Compact JSON output(--compact-output of jq)
      --jq-raw-output                    (--raw-output of jq)
      --jq-from-file=                    (--from-file of jq)
      --param=                           [name]:[Cloud Spanner type(PLAN only) or literal]
      --log-grpc                         Show gRPC logs

Help Options:
  -h, --help                             Show this help message

Arguments:
  database:                              (required) ID of the database.
```

## Notable features

There are examples omitting some required options.

### Parameter support

Many Cloud Spanner clients don't support parameter.
Without modifications, query which have parameters are impossible to execute and query whose parameters' types are `STRUCT` or `ARRAY` are impossible to show query plans.

execspansql supports query parameters.

#### PLAN with complex typed parameters

You can use type syntax to plan a query.

```
$ execspansql --sql='SELECT * FROM UNNEST(@arr) WITH OFFSET' --query-mode=PLAN --param='arr=ARRAY<STRUCT<STRING>>'
```
```
$ execspansql --sql='SELECT @str.*' --query-mode=PROFILE --param='str=STRUCT<FirstName STRING, LastName STRING>'
```

#### PROFILE with complex typed parameterized values 

You can use subset of literal syntax to execute a query.

Note: It only emulates literals and doesn't emulate coercion.

```
$ execspansql --sql='SELECT * FROM UNNEST(@arr) WITH OFFSET' --param='arr=[STRUCT<pk INT64, col STRING>(1, "foo"), (42, "foobar")]' --query-mode=PROFILE
```
```
$ execspansql --sql='SELECT * FROM Singers WHERE STRUCT<FirstName STRING, LastName STRING>(FirstName, LastName) IN UNNEST(@names)' \
              --param='names=[STRUCT<FirstName STRING, LastName STRING>("John", "Doe"), ("Mary", "Sue")]' \
              --query-mode=PROFILE
```

## Limitations

* DML and Partitioned DML are not yet supported
* Supports only json and yaml format
