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

	"github.com/d13co/algod-mesh/internal/config"
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
		"root/var/lib/algorand/genesis.json": "{}",
		"root/var/lib/algorand/algod.net":    "127.0.0.1:8080",
		// The package's genesis files: no data dir.
		"root/var/lib/algorand/genesis/mainnet/genesis.json": "{}",
		"root/var/lib/algorand_testnet/genesis.json":         "{}",
		"root/var/lib/dtalgorand_testnet/genesis.json":       "{}",
		"root/var/lib/dtalgorand_testnet/config.json":        tt,
		"root/var/lib/dtalgorand_testnet/algod.net":          "127.0.0.1:8081",
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
		root + "/var/lib/algorand_testnet (not running)",
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

// A node that cannot be read (it exited, or belongs to another user) is left
// out, not every candidate after it. A dtalgod started by goal runs as algod:
// the path of its binary says it reads $DTALGORAND_DATA. Without a terminal,
// the one dir a running node uses wins over the packages' idle dirs.
func TestAutoconfigDetectsRunningNode(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, "root")
	writeFiles(t, dir, map[string]string{
		"root/var/lib/algorand/genesis.json":   "{}",
		"root/var/lib/dtalgorand/genesis.json": "{}",
		"root/var/lib/dtalgorand/config.json":  `{"EnableTimeTravel": true}`,
		// 45 has no environ to read.
		"root/proc/45/cmdline": "/usr/bin/algod\x00",
		"client.token":         "t",
	})
	path := fakeBin(t, dir, "bin", map[string]string{
		"pgrep": "[ \"$2\" = algod ] && echo \"$PIDS\"\n",
	})
	args := []string{"--no-nodely", "--id", "k44", "--address", "10.112.0.44", "--app-id", "7"}
	env := []string{path, "AUTOCONFIG_ROOT=" + root, "ALGORAND_DATA=", "DTALGORAND_DATA="}

	// Nothing running to read: both dirs, and no pick.
	cmd := autoconfigCmd(t, dir, "", append(env, "PIDS=45"), args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	out, err := cmd.CombinedOutput()
	want := "several algod data dirs: " + root + "/var/lib/algorand (not running), " +
		root + "/var/lib/dtalgorand (not running, dtalgorand); pass --data-dir"
	if err == nil || !strings.Contains(string(out), want) {
		t.Errorf("err %v\n got %s\nwant %s", err, out, want)
	}

	// 46 is /usr/lib/dtalgorand/bin/algod: its $DTALGORAND_DATA, not its
	// $ALGORAND_DATA, and the only dir a running node uses.
	writeFiles(t, dir, map[string]string{
		"root/proc/46/cmdline": "/usr/lib/dtalgorand/bin/algod\x00",
		"root/proc/46/environ": "ALGORAND_DATA=" + root + "/var/lib/algorand\x00DTALGORAND_DATA=" + root + "/var/lib/dtalgorand\x00",
	})
	if err := os.Symlink("/usr/lib/dtalgorand/bin/algod", filepath.Join(root, "proc/46/exe")); err != nil {
		t.Fatal(err)
	}
	cmd = autoconfigCmd(t, dir, "", append(env, "PIDS=45\n46"),
		append(args, "-o", filepath.Join(dir, "config.yaml"), "--sync-key-file", writeSyncKey(t, dir))...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("autoconfig.sh: %v\n%s", err, out)
	}
	c, err := config.Load(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Local.DataDir != root+"/var/lib/dtalgorand" {
		t.Errorf("data dir %q", c.Local.DataDir)
	}
}

// setupFunc runs a function of setup.sh with the arguments given.
func setupFunc(t *testing.T, env []string, fn string, args ...string) string {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("no bash")
	}
	cmd := exec.Command("bash", append([]string{"-c", `source ./setup.sh && "$@"`, "_", fn}, args...)...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("%s %q: %v", fn, args, err)
	}
	return strings.TrimSpace(string(out))
}

// fakeOwners fakes `stat -c %U PATH`, printing PATH's owner from $OWNERS
// (lines of "PATH OWNER"; it fails for a path not there), and `id -u NAME`,
// which knows only the names in $USERS.
func fakeOwners(t *testing.T) string {
	return fakeBin(t, t.TempDir(), "bin", map[string]string{
		"stat": "for p; do :; done\no=$(printf '%s\\n' \"$OWNERS\" | awk -v p=\"$p\" '$1 == p { print $2 }')\n[ -n \"$o\" ] || exit 1\necho \"$o\"\n",
		"id":   "case \" $USERS \" in *\" $2 \"*) echo 1000 ;; *) exit 1 ;; esac\n",
	})
}

// setup.sh runs the service as the data dir's owner when that is algorand or
// dtalgorand, else algorand, else root: never as some other owner.
func TestSetupDefaultUser(t *testing.T) {
	path := fakeOwners(t)
	for _, tc := range []struct{ dir, owner, users, want string }{
		{"/var/lib/dtalgorand", "dtalgorand", "algorand dtalgorand", "dtalgorand"},
		{"/var/lib/algorand", "algorand", "algorand dtalgorand", "algorand"},
		{"/var/lib/algorand", "algorand", "", "root"}, // stat and id disagree
		// update.sh's root-owned node, a container's uid, a user's own algod.
		{"/opt/algorand/node/data", "root", "root algorand", "algorand"},
		{"/opt/algorand/node/data", "root", "root", "root"},
		{"/srv/algod", "bob", "algorand bob", "algorand"},
		{"/tmp/evil", "eve", "eve", "root"},
		{"/var/lib/x", "UNKNOWN", "algorand", "algorand"}, // a uid with no name
		{"/var/lib/x", "", "", "root"},
		{"", "", "algorand dtalgorand", "algorand"}, // a balancer: no data dir
		{"", "", "", "root"},
	} {
		owners := ""
		if tc.owner != "" {
			owners = tc.dir + " " + tc.owner
		}
		if got := setupFunc(t, []string{path, "OWNERS=" + owners, "USERS=" + tc.users}, "default_user", tc.dir); got != tc.want {
			t.Errorf("dir %q owner %q users %q: %q, want %q", tc.dir, tc.owner, tc.users, got, tc.want)
		}
	}

	// A real dir: its owner is not algorand or dtalgorand.
	if u, err := user.Current(); err == nil && u.Username != "root" && u.Username != "algorand" && u.Username != "dtalgorand" {
		if got := setupFunc(t, nil, "default_user", t.TempDir()); got == u.Username {
			t.Errorf("own temp dir: %q", got)
		}
	}
}

// With other agents on the host, or a config to keep, the service runs as the
// user that owns their configs, even without units that say so: a dtalgorand
// node added beside a stock one does not take the dirs from the first agent.
func TestSetupExistingUser(t *testing.T) {
	path := fakeOwners(t)
	cfg := t.TempDir()
	dt := "/var/lib/dtalgorand"
	pick := func(target, others, owners, users string) string {
		t.Helper()
		return setupFunc(t, []string{path, "CONFIG_DIR=" + cfg, "OWNERS=" + owners, "USERS=" + users},
			"pick_user", filepath.Join(cfg, target), others, dt)
	}
	users := "algorand dtalgorand"

	// No agents yet: the data dir's owner.
	if got := pick("config.yaml", "0", cfg+" algorand\n"+dt+" dtalgorand", users); got != "dtalgorand" {
		t.Errorf("first agent: %q", got)
	}
	// A second instance beside config.yaml, which runs as algorand.
	writeFiles(t, cfg, map[string]string{"config.yaml": "x"})
	all := cfg + " algorand\n" + cfg + "/config.yaml algorand\n" + dt + " dtalgorand"
	if got := pick("testnet.yaml", "1", all, users); got != "algorand" {
		t.Errorf("second instance: %q", got)
	}
	// A re-run that keeps config.yaml.
	if got := pick("config.yaml", "0", all, users); got != "algorand" {
		t.Errorf("re-run: %q", got)
	}
	// Only the config dir says.
	if got := pick("testnet.yaml", "1", cfg+" algorand\n"+dt+" dtalgorand", users); got != "algorand" {
		t.Errorf("config dir: %q", got)
	}
	// Root's configs say nothing: the data dir decides.
	if got := pick("testnet.yaml", "1", cfg+" root\n"+cfg+"/config.yaml root\n"+dt+" dtalgorand", users); got != "dtalgorand" {
		t.Errorf("root's configs: %q", got)
	}
}
