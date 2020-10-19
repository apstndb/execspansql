module github.com/apstndb/execspansql

go 1.15

require (
	cloud.google.com/go/spanner v1.10.0
	github.com/MakeNowJust/memefish v0.0.0-20200430105843-c8e9c6d29dd6
	google.golang.org/genproto v0.0.0-20200904004341-0bd0a958aa1d
	google.golang.org/protobuf v1.25.0
	gopkg.in/yaml.v2 v2.2.8
)

// replace github.com/MakeNowJust/memefish v0.0.0-20200430105843-c8e9c6d29dd6 => ../../memefish
// replace github.com/MakeNowJust/memefish v0.0.0-20200430105843-c8e9c6d29dd6 => ./vendor/github.com/MakeNowJust/memefish

replace github.com/MakeNowJust/memefish v0.0.0-20200430105843-c8e9c6d29dd6 => github.com/apstndb/memefish v0.0.0-20201019043231-a0aff415c2d9
