package main

import (
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"context"
	_ "embed"
	"github.com/apstndb/gsqlsep"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/prototext"
	"slices"
	"spheric.cloud/xiter"
	"testing"
)

//go:embed testdata/ddl.sql
var ddl string

//go:embed testdata/dml.sql
var dml string

func setupDB(t *testing.T, ctx context.Context, opts ...option.ClientOption) {
	t.Helper()

	t.Log("setup instance")
	instanceClient, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer instanceClient.Close()
	createInstance, err := instanceClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/fake",
		InstanceId: "fake",
		Instance: &instancepb.Instance{
			Name:            "projects/fake/instances/fake",
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
			Parent:          "projects/fake/instances/fake",
			CreateStatement: "CREATE DATABASE fake",
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
		cli, err := spanner.NewClient(ctx, "projects/fake/instances/fake/databases/fake", opts...)
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
	req := testcontainers.ContainerRequest{
		Image:        "gcr.io/cloud-spanner-emulator/emulator:1.5.23",
		ExposedPorts: []string{"9020/tcp", "9010/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}
	spannerEmu, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	defer spannerEmu.Terminate(ctx)

	t.Log("emulator started")

	grpcPort, err := spannerEmu.PortEndpoint(ctx, "9010/tcp", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("grpcPort:", grpcPort)
	opts := []option.ClientOption{option.WithEndpoint(grpcPort), option.WithoutAuthentication(), option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))}

	setupDB(t, ctx, opts...)

	cli, err := spanner.NewClient(ctx, "projects/fake/instances/fake/databases/fake", opts...)
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
			spanner.Statement{SQL: "SELECT @i64, @f32, @f64, @s, @bs, @b, @dt, @ts, @n, @a_str, @a_s, @j", Params: params},
			spanner.QueryOptions{Mode: spannerpb.ExecuteSqlRequest_PLAN.Enum()})
		defer rowIter.Stop()

		err = skipRowIter(rowIter)
		if err != nil {
			t.Fatal(err)
		}

		t.Log(prototext.Format(rowIter.Metadata.GetRowType()))
	})

	require.NoError(t, err)
}
