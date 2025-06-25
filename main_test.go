package main

import (
	"bytes"
	"context"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestProcessResults(t *testing.T) {
	rs := &sppb.ResultSet{
		Metadata: &sppb.ResultSetMetadata{
			RowType: &sppb.StructType{
				Fields: []*sppb.StructType_Field{
					{Name: "ID", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
					{Name: "Name", Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
				},
			},
		},
		Rows: []*structpb.ListValue{
			{
				Values: []*structpb.Value{
					structpb.NewStringValue("1"),
					structpb.NewStringValue("Alice"),
				},
			},
			{
				Values: []*structpb.Value{
					structpb.NewStringValue("2"),
					structpb.NewStringValue("Bob"),
				},
			},
		},
	}

	tests := []struct {
		name       string
		opts       opts
		expected   string
	}{
		{
			name: "json output pretty",
			opts: opts{Format: "json"},
			expected: `{
  "metadata": {
    "row_type": {
      "fields": [
        {
          "name": "ID",
          "type": {
            "code": "INT64"
          }
        },
        {
          "name": "Name",
          "type": {
            "code": "STRING"
          }
        }
      ]
    }
  },
  "rows": [
    [
      "1",
      "Alice"
    ],
    [
      "2",
      "Bob"
    ]
  ]
}
`,
		},
		{
			name: "json output compact",
			opts: opts{Format: "json", CompactOutput: true},
			expected: `{"metadata":{"row_type":{"fields":[{"name":"ID","type":{"code":"INT64"}},{"name":"Name","type":{"code":"STRING"}}]}},"rows":[["1","Alice"],["2","Bob"]]}
`,
		},
		{
			name: "yaml output",
			opts: opts{Format: "yaml"},
			expected: `metadata:
  row_type:
    fields:
    - name: ID
      type:
        code: INT64
    - name: Name
      type:
        code: STRING
rows:
- - "1"
  - Alice
- - "2"
  - Bob
`,
		},
		{
			name: "jq filter",
			opts: opts{Format: "json", JqFilter: ".rows[0][1]"},
			expected: "\"Alice\"\n",
		},
		{
			name: "raw output",
			opts: opts{Format: "json", JqFilter: ".rows[0][1]", JqRawOutput: true},
			expected: "Alice\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := processResults(context.Background(), rs, tt.opts, &buf)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, buf.String())
		})
	}
}
