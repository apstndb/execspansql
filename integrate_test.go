package main

import (
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"context"
	_ "embed"
	"fmt"
	"github.com/apstndb/gsqlsep"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/gcloud"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
	"slices"
	"spheric.cloud/xiter"
	"testing"
)

//go:embed testdata/ddl.sql
var ddl string

//go:embed testdata/dml.sql
var dml string

const (
	projectID  = "fake"
	instanceID = "fake"
	databaseID = "fake"
)

var (
	projectStr  = fmt.Sprintf("projects/%v", projectID)
	instanceStr = fmt.Sprintf("%v/instances/%v", projectStr, instanceID)
	databaseStr = fmt.Sprintf("%v/databases/%v", instanceStr, databaseID)
)

func setupDB(t *testing.T, ctx context.Context, opts ...option.ClientOption) {
	t.Helper()

	t.Log("setup instance")
	instanceClient, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer instanceClient.Close()
	createInstance, err := instanceClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     projectStr,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Name:            instanceStr,
			Config:          "regional-asia-northeast1",
			DisplayName:     "fake",
			ProcessingUnits: 100,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	i, err := createInstance.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dumper := spew.ConfigState{}
	t.Log("instance: ", dumper.Sdump(i))

	t.Log("setup database")
	{
		dbCli, err := database.NewDatabaseAdminClient(ctx, opts...)
		if err != nil {
			t.Fatal(err)
		}
		createDatabaseOp, err := dbCli.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
			Parent:          instanceStr,
			CreateStatement: fmt.Sprintf("CREATE DATABASE `%v`", databaseID),
			ExtraStatements: gsqlsep.SeparateInputString(ddl),
		})
		if err != nil {
			t.Fatal(err)
		}
		createDatabaseResp, err := createDatabaseOp.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("database: ", dumper.Sdump(createDatabaseResp))
	}
	t.Log("setup data")
	{
		cli, err := spanner.NewClient(ctx, databaseStr, opts...)
		if err != nil {
			t.Fatal(err)
		}
		defer cli.Close()

		dmlStmts := slices.Collect(xiter.Map(slices.Values(gsqlsep.SeparateInputString(dml)), spanner.NewStatement))
		_, err = cli.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			_, err := tx.BatchUpdate(ctx, dmlStmts)
			return err
		})
		if err != nil {
			return
		}
	}
}

func TestWithCloudSpannerEmulator(t *testing.T) {
	ctx := context.Background()
	t.Log("start emulator")

	spannerContainer, err := gcloud.RunSpanner(ctx, "gcr.io/cloud-spanner-emulator/emulator:1.5.23", testcontainers.WithLogger(testcontainers.TestLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spannerContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}()

	t.Log("emulator started")

	opts := []option.ClientOption{
		option.WithEndpoint(spannerContainer.URI),
		option.WithoutAuthentication(),
		internaloption.SkipDialSettingsValidation(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))}

	setupDB(t, ctx, opts...)

	cli, err := spanner.NewClient(ctx, databaseStr, opts...)
	if err != nil {
		t.Fatal(err)
	}

	defer cli.Close()

	t.Run("PLAN with generateParams", func(t *testing.T) {
		params, err := generateParams(map[string]string{
			"i64":   "INT64",
			"f32":   "FLOAT32",
			"f64":   "FLOAT64",
			"s":     "STRING",
			"bs":    "BYTES",
			"bl":    "BOOL",
			"dt":    `DATE`,
			"ts":    `TIMESTAMP`,
			"n":     `NUMERIC`,
			"a_str": `ARRAY<STRUCT<int64_value INT64>>`,
			"a_s":   `ARRAY<STRING>`,
			"j":     `JSON`,
		}, true)
		if err != nil {
			t.Fatal(err)
		}

		rowIter := cli.Single().QueryWithOptions(ctx,
			spanner.Statement{SQL: "SELECT @i64, @f32, @f64, @s, @bs, @bl, @dt, @ts, @n, @a_str, @a_s, @j", Params: params},
			spanner.QueryOptions{Mode: sppb.ExecuteSqlRequest_PLAN.Enum()})
		defer rowIter.Stop()

		err = skipRowIter(rowIter)
		if err != nil {
			t.Fatal(err)
		}

		want := &sppb.StructType{
			Fields: []*sppb.StructType_Field{
				{Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
				{Type: &sppb.Type{Code: sppb.TypeCode_FLOAT32}},
				{Type: &sppb.Type{Code: sppb.TypeCode_FLOAT64}},
				{Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
				{Type: &sppb.Type{Code: sppb.TypeCode_BYTES}},
				{Type: &sppb.Type{Code: sppb.TypeCode_BOOL}},
				{Type: &sppb.Type{Code: sppb.TypeCode_DATE}},
				{Type: &sppb.Type{Code: sppb.TypeCode_TIMESTAMP}},
				{Type: &sppb.Type{Code: sppb.TypeCode_NUMERIC}},
				{Type: &sppb.Type{Code: sppb.TypeCode_ARRAY,
					ArrayElementType: &sppb.Type{Code: sppb.TypeCode_STRUCT, StructType: &sppb.StructType{Fields: []*sppb.StructType_Field{
						{Name: "int64_value", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
					}}}}},
				{Type: &sppb.Type{Code: sppb.TypeCode_ARRAY, ArrayElementType: &sppb.Type{Code: sppb.TypeCode_STRING}}},
				{Type: &sppb.Type{Code: sppb.TypeCode_JSON}},
			},
		}
		got := rowIter.Metadata.GetRowType()
		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Error(diff)
			return
		}
	})

	t.Run("NORMAL with generateParams", func(t *testing.T) {
		for _, tcase := range []struct {
			desc  string
			input string
			want  spanner.GenericColumnValue
		}{
			{
				"INT64",
				"1",
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_INT64},
					Value: structpb.NewStringValue("1"),
				},
			},
			{
				"FLOAT64",
				"1.0",
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_FLOAT64},
					Value: structpb.NewNumberValue(1.0),
				},
			},
			{
				"STRING",
				`'foo'`,
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_STRING},
					Value: structpb.NewStringValue("foo"),
				},
			},
			{
				"BYTES",
				`b'foo'`,
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_BYTES},
					Value: structpb.NewStringValue("Zm9v"),
				},
			},
			{
				"DATE",
				`DATE '1970-01-01'`,
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_DATE},
					Value: structpb.NewStringValue("1970-01-01"),
				},
			},
			{
				"TIMESTAMP",
				`TIMESTAMP '1970-01-01T00:00:00Z'`,
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_TIMESTAMP},
					Value: structpb.NewStringValue("1970-01-01T00:00:00Z"),
				},
			},
			{
				"NUMERIC",
				`NUMERIC '1.0'`,
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_NUMERIC},
					Value: structpb.NewStringValue("1"),
				},
			},
			{
				"ARRAY<STRUCT<int64_value INT64>>",
				`ARRAY<STRUCT<int64_value INT64>>[STRUCT(1)]`,
				spanner.GenericColumnValue{
					Type: &sppb.Type{
						Code: sppb.TypeCode_ARRAY,
						ArrayElementType: &sppb.Type{
							Code: sppb.TypeCode_STRUCT,
							StructType: &sppb.StructType{
								Fields: []*sppb.StructType_Field{
									{Name: "int64_value", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
								},
							},
						},
					},
					Value: structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewListValue(&structpb.ListValue{
								Values: []*structpb.Value{
									structpb.NewStringValue("1")}})}}),
				},
			},
			{
				"ARRAY<STRUCT<int64_value INT64>> verbose",
				`ARRAY<STRUCT<int64_value INT64>>[STRUCT<int64_value INT64>(1)]`,
				spanner.GenericColumnValue{
					Type: &sppb.Type{Code: sppb.TypeCode_ARRAY, ArrayElementType: &sppb.Type{Code: sppb.TypeCode_STRUCT, StructType: &sppb.StructType{
						Fields: []*sppb.StructType_Field{{Name: "int64_value", Type: &sppb.Type{Code: sppb.TypeCode_INT64}}}}},
					},
					Value: structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewListValue(&structpb.ListValue{
								Values: []*structpb.Value{
									structpb.NewStringValue("1")}})}}),
				},
			},
			{
				"ARRAY<STRING>",
				`['foo']`,
				spanner.GenericColumnValue{
					Type: &sppb.Type{
						Code:             sppb.TypeCode_ARRAY,
						ArrayElementType: &sppb.Type{Code: sppb.TypeCode_STRING},
					},
					Value: structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewStringValue("foo")}}),
				},
			},
			{
				"JSON",
				`JSON '{"foo": "bar"}'`,
				spanner.GenericColumnValue{
					Type:  &sppb.Type{Code: sppb.TypeCode_JSON},
					Value: structpb.NewStringValue(`{"foo":"bar"}`),
				},
			},
		} {
			t.Run(tcase.desc, func(t *testing.T) {
				params, err := generateParams(map[string]string{"v": tcase.input}, false)
				if err != nil {
					t.Fatal(err)
				}
				rowIter := cli.Single().QueryWithOptions(ctx,
					spanner.Statement{SQL: "SELECT @v AS v", Params: params},
					spanner.QueryOptions{Mode: sppb.ExecuteSqlRequest_NORMAL.Enum()})
				defer rowIter.Stop()

				rows, err := xiter.TryCollect(rowIterSeq(rowIter))
				if err != nil {
					t.Error(err)
					return
				}

				if lenRows := len(rows); lenRows != 1 {
					t.Errorf("len(rows) must be 1, but: %v", lenRows)
					return
				}
				row := rows[0]
				if row.Size() != 1 {
					t.Errorf("row.Size() must be 1, but: %v", row.Size())
					return
				}

				var got spanner.GenericColumnValue
				err = row.ColumnByName("v", &got)
				if err != nil {
					t.Error(err)
					return
				}
				if diff := cmp.Diff(tcase.want, got, protocmp.Transform()); diff != "" {
					t.Error(diff)
					return
				}
			})
		}
	})
	require.NoError(t, err)
}
