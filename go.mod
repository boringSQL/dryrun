module github.com/boringsql/dryrun

go 1.26.4

require (
	github.com/BurntSushi/toml v1.6.0
	github.com/boringsql/fixturize v0.7.0
	github.com/boringsql/qshape v0.2.0
	github.com/boringsql/queries v1.6.1
	github.com/jackc/pgx/v5 v5.10.0
	github.com/klauspost/compress v1.18.6
	github.com/mark3labs/mcp-go v0.54.1
	github.com/mattn/go-isatty v0.0.22
	github.com/opencontainers/go-digest v1.0.0
	github.com/opencontainers/image-spec v1.1.1
	github.com/pganalyze/pg_query_go/v6 v6.2.2
	github.com/spf13/cobra v1.10.2
	golang.org/x/oauth2 v0.36.0
	modernc.org/sqlite v1.52.0
	oras.land/oras-go/v2 v2.6.1
)

require (
	cloud.google.com/go/compute/metadata v0.3.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/jsonschema-go v0.4.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	golang.org/x/sync v0.21.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	modernc.org/libc v1.73.0 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace github.com/boringsql/qshape => ../qshape
