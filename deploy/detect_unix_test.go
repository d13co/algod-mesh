//go:build unix

package deploy

import (
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// writeFiles writes each file under dir, making directories as needed.
func writeFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// fakeBin puts scripts earlier in PATH, under dir/name.
func fakeBin(t *testing.T, dir, name string, scripts map[string]string) string {
	t.Helper()
	bin := filepath.Join(dir, name)
	for file, body := range scripts {
		writeFiles(t, bin, map[string]string{file: "#!/bin/sh\n" + body})
		if err := os.Chmod(filepath.Join(bin, file), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return "PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH")
}

// Detection finds stock algod and dtalgorand nodes: running ones by -d or the
// variable each binary reads, the shell's variables, and the packages' usual
// dirs. With several and no terminal it lists them, dtalgorand ones labelled.
func TestAutoconfigDetectsDtalgorand(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, "root")
	tt := `{"EnableTimeTravel": true}`
	writeFiles(t, dir, map[string]string{
		// Stock paths, under the fake root.
		"root/var/lib/algorand/genesis.json":           "{}",
		"root/var/lib/algorand/algod.net":              "127.0.0.1:8080",
		"root/var/lib/dtalgorand_testnet/genesis.json": "{}",
		"root/var/lib/dtalgorand_testnet/config.json":  tt,
		"root/var/lib/dtalgorand_testnet/algod.net":    "127.0.0.1:8081",
		// `dtalgod -d DIR`.
		"byflag/genesis.json": "{}",
		"byflag/config.json":  "{\n  \"enableTimeTravel\" : true\n}",
		// dtalgod with $DTALGORAND_DATA, algod with $ALGORAND_DATA; each
		// ignores the other variable.
		"dtenv/genesis.json":    "{}",
		"dtenv/config.json":     tt,
		"algodenv/genesis.json": "{}",
		"ignored1/genesis.json": "{}",
		"ignored2/genesis.json": "{}",
		// The shell's $DTALGORAND_DATA.
		"shell/genesis.json": "{}",
		"shell/config.json":  `{"EnableTimeTravel": false}`,
		// The fake /proc.
		"root/proc/42/cmdline": "/usr/bin/dtalgod\x00-d\x00" + dir + "/byflag\x00",
		"root/proc/43/cmdline": "/usr/bin/dtalgod\x00",
		"root/proc/43/environ": "ALGORAND_DATA=" + dir + "/ignored1\x00DTALGORAND_DATA=" + dir + "/dtenv\x00",
		"root/proc/44/cmdline": "/usr/bin/algod\x00",
		"root/proc/44/environ": "DTALGORAND_DATA=" + dir + "/ignored2\x00ALGORAND_DATA=" + dir + "/algodenv\x00",
		"client.token":         "t",
	})
	path := fakeBin(t, dir, "bin", map[string]string{
		"pgrep": "case \"$2\" in algod) echo 44 ;; dtalgod) printf '42\\n43\\n' ;; esac\n",
	})
	env := []string{path, "AUTOCONFIG_ROOT=" + root,
		// The same dir again, spelled differently: listed once.
		"ALGORAND_DATA=" + root + "/var/lib/algorand/",
		"DTALGORAND_DATA=" + dir + "/shell"}
	cmd := autoconfigCmd(t, dir, "", env, "--no-nodely", "--id", "k44", "--address", "10.112.0.44", "--app-id", "7")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // no terminal to ask on
	out, err := cmd.CombinedOutput()
	want := "several algod data dirs: " + strings.Join([]string{
		dir + "/algodenv (not running)",
		dir + "/byflag (not running, dtalgorand)",
		dir + "/dtenv (not running, dtalgorand)",
		root + "/var/lib/algorand (127.0.0.1:8080)",
		dir + "/shell (not running)",
		root + "/var/lib/dtalgorand_testnet (127.0.0.1:8081, dtalgorand)",
	}, ", ") + "; pass --data-dir"
	if err == nil || !strings.Contains(string(out), want) {
		t.Errorf("err %v\n got %s\nwant %s", err, out, want)
	}

	// The package's own dir, alone, is picked without asking.
	root2 := filepath.Join(dir, "root2")
	writeFiles(t, root2, map[string]string{"var/lib/dtalgorand/genesis.json": "{}", "var/lib/dtalgorand/algod.net": "127.0.0.1:8080"})
	c := autoconfigExec(t, dir, "", []string{path, "AUTOCONFIG_ROOT=" + root2, "ALGORAND_DATA=", "DTALGORAND_DATA="},
		"--no-nodely", "--id", "k44", "--address", "10.112.0.44", "--app-id", "7", "--sync-key-file", writeSyncKey(t, dir))
	if c.Local.DataDir != root2+"/var/lib/dtalgorand" {
		t.Errorf("data dir %q", c.Local.DataDir)
	}
}

// setup.sh runs the service as the data dir's owner, else algorand, else root.
func TestSetupDefaultUser(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("no bash")
	}
	defaultUser := func(dataDir string, env ...string) string {
		t.Helper()
		cmd := exec.Command("bash", "-c", `source ./setup.sh && default_user "$1"`, "_", dataDir)
		cmd.Env = append(os.Environ(), env...)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("default_user %q: %v", dataDir, err)
		}
		return strings.TrimSpace(string(out))
	}

	// A real dir: its owner.
	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	if got := defaultUser(t.TempDir()); got != me.Username {
		t.Errorf("own temp dir: %q, want %q", got, me.Username)
	}

	// Faked owners and users: `stat` prints $OWNER (fails when empty), and
	// `id -u NAME` knows only the names in $USERS.
	path := fakeBin(t, t.TempDir(), "bin", map[string]string{
		"stat": "[ -n \"$OWNER\" ] || exit 1\necho \"$OWNER\"\n",
		"id":   "case \" $USERS \" in *\" $2 \"*) echo 1000 ;; *) exit 1 ;; esac\n",
	})
	for _, tc := range []struct{ dir, owner, users, want string }{
		{"/var/lib/dtalgorand", "dtalgorand", "algorand dtalgorand", "dtalgorand"},
		{"/var/lib/algorand", "algorand", "algorand dtalgorand", "algorand"},
		{"/var/lib/x", "UNKNOWN", "algorand", "algorand"}, // a uid with no name
		{"/var/lib/x", "gone", "algorand", "algorand"},
		{"/var/lib/x", "", "", "root"},
		{"", "dtalgorand", "algorand dtalgorand", "algorand"}, // a balancer: no data dir
		{"", "", "", "root"},
	} {
		if got := defaultUser(tc.dir, path, "OWNER="+tc.owner, "USERS="+tc.users); got != tc.want {
			t.Errorf("dir %q owner %q users %q: %q, want %q", tc.dir, tc.owner, tc.users, got, tc.want)
		}
	}
}
