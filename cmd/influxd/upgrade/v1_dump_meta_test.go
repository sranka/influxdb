package upgrade

import (
	"bytes"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestV1DumpMetaJSON(t *testing.T) {
	// read test data
	tmpdir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)
	err = testutil.Unzip("testdata/v1db.zip", tmpdir)
	require.NoError(t, err)
	v1opts := &optionsV1{dbDir: tmpdir + "/v1db"}
	v1opts.populateDirs()
	expected, err := ioutil.ReadFile("testdata/v1meta.json")
	require.NoError(t, err)

	v1DumpMetaOptions.optionsV1 = *v1opts
	svc, err := newInfluxDBv1(v1opts)
	require.NoError(t, err)
	buffer := &bytes.Buffer{}
	v1DumpMetaJSON(svc.meta, buffer)
	json := string(buffer.Bytes())
	require.Equal(t, string(expected), strings.TrimSpace(json))
}
