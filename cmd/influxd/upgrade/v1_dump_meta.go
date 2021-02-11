package upgrade

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/influxdata/influxdb/v2/fluxinit"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/influxdata/influxql"
	"github.com/spf13/cobra"
)

var v1DumpMetaOptions = struct {
	optionsV1
	json bool
}{}

var v1DumpMetaCommand = &cobra.Command{
	Use:    "v1-dump-meta",
	Short:  "Dump InfluxDB 1.x meta.db",
	Args:   cobra.NoArgs,
	Hidden: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		fluxinit.FluxInit()
		svc, err := newInfluxDBv1(&v1DumpMetaOptions.optionsV1)
		if err != nil {
			return fmt.Errorf("error opening 1.x meta.db: %w", err)
		}
		meta := svc.meta
		if !v1DumpMetaOptions.json {
			return v1DumpMetaText(meta, os.Stdout)
		}
		return v1DumpMetaJSON(meta, os.Stdout)
	},
}

func v1DumpMetaText(meta *meta.Client, out io.Writer) error {
	tw := tabwriter.NewWriter(out, 15, 4, 1, ' ', 0)

	showBool := func(b bool) string {
		if b {
			return "✓"
		}
		return ""
	}

	fmt.Fprintln(out, "Databases")
	fmt.Fprintln(out, "---------")
	fmt.Fprintf(tw, "%s\t%s\t%s\n", "Name", "Default RP", "Shards")
	for _, row := range meta.Databases() {
		fmt.Fprintf(tw, "%s\t%s\t", row.Name, row.DefaultRetentionPolicy)
		for i, si := range row.ShardInfos() {
			if i > 0 {
				fmt.Fprint(tw, ",")
			}
			fmt.Fprintf(tw, "%d", si.ID)
		}
		fmt.Fprintln(tw)
	}
	_ = tw.Flush()
	fmt.Fprintln(out)

	fmt.Fprintln(out, "Retention policies")
	fmt.Fprintln(out, "---------")
	fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", "Database", "Name", "Duration", "Shard Group duration")
	for _, db := range meta.Databases() {
		for _, rp := range db.RetentionPolicies {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", db.Name, rp.Name, rp.Duration.String(), rp.ShardGroupDuration.String())
		}
	}
	_ = tw.Flush()
	fmt.Fprintln(out)

	fmt.Fprintln(out, "Shard groups")
	fmt.Fprintln(out, "---------")
	fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", "Database/RP", "Start Time", "End Time", "Shards")
	for _, db := range meta.Databases() {
		for _, rp := range db.RetentionPolicies {
			for _, sg := range rp.ShardGroups {
				fmt.Fprintf(tw, "%s/%s\t%s\t%s\t", db.Name, rp.Name, sg.StartTime.String(), sg.EndTime.String())
				for i, si := range sg.Shards {
					if i > 0 {
						fmt.Fprint(tw, ",")
					}
					fmt.Fprintf(tw, "%d", si.ID)
				}
				fmt.Fprintln(tw)
			}
		}
	}
	_ = tw.Flush()
	fmt.Fprintln(out)

	fmt.Fprintln(out, "Users")
	fmt.Fprintln(out, "-----")
	fmt.Fprintf(tw, "%s\t%s\n", "Name", "Admin")
	for _, row := range meta.Users() {
		fmt.Fprintf(tw, "%s\t%s\n", row.Name, showBool(row.Admin))
	}
	_ = tw.Flush()
	fmt.Fprintln(out)
	return nil
}

func v1DumpMetaJSON(meta *meta.Client, out io.Writer) error {
	dbrps := make([]interface{}, 0, 10)
	for _, db := range meta.Databases() {
		for _, rp := range db.RetentionPolicies {
			dbrps = append(dbrps, map[string]interface{}{
				"db":              db.Name,
				"rp":              rp.Name,
				"durationSeconds": int64(rp.Duration.Seconds()),
				"default":         rp.Name == db.DefaultRetentionPolicy,
			})
		}
	}
	users := make([]interface{}, 0, 10)
	for _, user := range meta.Users() {
		readDBs := make([]string, 0, 10)
		writeDBs := make([]string, 0, 10)
		for key, val := range user.Privileges {
			switch val {
			case influxql.ReadPrivilege:
				readDBs = append(readDBs, key)
			case influxql.WritePrivilege:
				writeDBs = append(writeDBs, key)
			case influxql.AllPrivileges:
				readDBs = append(readDBs, key)
				writeDBs = append(writeDBs, key)
			}
		}
		users = append(users, map[string]interface{}{
			"name":     user.Name,
			"isAdmin":  user.Admin,
			"hash":     user.Hash,
			"readDBs":  readDBs,
			"writeDBs": writeDBs,
		})
	}

	jsonData, err := json.MarshalIndent(map[string]interface{}{
		"dbrps": dbrps,
		"users": users,
	}, "", "  ")
	if err == nil {
		fmt.Fprintln(out, string(jsonData))
	}
	return err
}

func init() {
	flags := v1DumpMetaCommand.Flags()

	v1dir, err := influxDirV1()
	if err != nil {
		panic("error fetching default InfluxDB 1.x dir: " + err.Error())
	}

	flags.StringVar(&v1DumpMetaOptions.metaDir, "v1-meta-dir", filepath.Join(v1dir, "meta"), "Path to meta.db directory")
	flags.BoolVar(&v1DumpMetaOptions.json, "json", false, "json output")
}
