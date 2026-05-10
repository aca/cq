// cq - Command Queue
//
// A job queue that runs commands sequentially using zmx (terminal multiplexer).
//
// Architecture:
//   - Jobs are stored in SQLite (single `jobs` table, namespaced via the `ns`
//     column).
//   - A single worker process per namespace pulls pending jobs and runs each
//     in its own zmx session. The worker is spawned on demand by `cq <cmd>`
//     (detached via Setsid) and exits after a short idle timeout. Mutual
//     exclusion is enforced with an flock on the namespace lock file.
//   - The worker polls `zmx list` to detect session completion; no in-session
//     callback is required.
//   - Each job runs inside its own zmx session, enabling:
//     1. Detached execution: jobs continue running even without a terminal
//     2. Later attachment: `cq attach <id>` connects your terminal to the job
//     3. Scrollback history: `cq log <id>` retrieves output via `zmx history`
//
// Shell quoting through zmx:
//   `zmx run <session> <argv...>` does not exec argv directly — it joins the
//   trailing argv with spaces and feeds the whole thing to a shell. Different
//   zmx builds disagree on whether to %q-quote each argv element first (the
//   nix flake build does; some local builds do not). To dodge that, we write
//   the wrapped command to a script file in the state dir and invoke it as
//   `sh <path>` — both tokens are quote-safe so they survive any join/quote
//   variation untouched. The worker removes the script when the job is done.
//
// Remote execution:
//   `cq --host <ssh-target> ...` execs ssh and runs cq on the remote host,
//   forwarding the namespace and remaining args. `attach` allocates a TTY.
//
// zmx fallback:
//   If `zmx` is not on PATH, cq falls back to
//   `nix --extra-experimental-features 'nix-command flakes' run github:neurosnap/zmx -- <args>`,
//   so a host with nix but no zmx still works.

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	flag "github.com/spf13/pflag"
	"mvdan.cc/sh/v3/syntax"
)

var namespace string
var host string
var workerMode bool
var dbFile string
var lockFile string

func init() {
	defaultNS := os.Getenv("CQ_NS")
	if defaultNS == "" {
		defaultNS = "default"
	}
	flag.StringVarP(&namespace, "namespace", "n", defaultNS, "job queue namespace")
	flag.StringVar(&host, "host", "", "run cq on remote host via ssh")
	flag.BoolVar(&workerMode, "worker", false, "internal: run worker loop")
}

func getStateDir() string {
	stateHome := os.Getenv("XDG_STATE_HOME")
	if stateHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			home = os.Getenv("HOME")
		}
		stateHome = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(stateHome, "cq")
}

func initNamespace() {
	stateDir := getStateDir()
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create state directory: %v\n", err)
		os.Exit(1)
	}
	dbFile = filepath.Join(stateDir, "cq.db")
	lockFile = filepath.Join(stateDir, fmt.Sprintf("%s.lock", namespace))
}

func main() {
	flag.CommandLine.SetInterspersed(false)
	flag.Parse()
	initNamespace()

	if workerMode {
		cmdWorker()
		return
	}

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	if host != "" {
		runRemote(host, args)
		return
	}

	// `cq -- <cmd>` forces queuing, bypassing subcommand matching
	if flag.CommandLine.ArgsLenAtDash() == 0 {
		cmdQueue(args)
		return
	}

	switch args[0] {
	case "attach", "a":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "usage: cq attach <job-id>\n")
			os.Exit(1)
		}
		cmdAttach(args[1])
	case "kill", "k":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "usage: cq kill <job-id>\n")
			os.Exit(1)
		}
		cmdKill(args[1])
	case "list", "ls", "l":
		cmdList()
	case "log":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "usage: cq log <job-id>\n")
			os.Exit(1)
		}
		cmdLog(args[1])
	case "retry", "r":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "usage: cq retry <job-id>\n")
			os.Exit(1)
		}
		cmdRetry(args[1])
	case "reset":
		cmdReset()
	case "rm":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "usage: cq rm <job-id>\n")
			os.Exit(1)
		}
		cmdRm(args[1])
	case "clean", "clear":
		cmdClean()
	case "resume":
		spawnWorker()
	case "cat":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "usage: cq cat <job-id>\n")
			os.Exit(1)
		}
		cmdCat(args[1])
	default:
		cmdQueue(args)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: cq [-n namespace] <command> [args...]

commands:
  <cmd> [args]        queue and run command
  attach <job-id>     attach to running job
  kill <job-id>       kill running job
  list                list jobs
  log <job-id>        show job output
  retry <job-id>      re-queue job with original env/workdir
  rm <job-id>         remove job from queue (pending/done/killed only)
  clean               remove done/killed jobs
  reset               clear all jobs in namespace
  resume              start processing pending jobs
  cat <job-id>        show job command with workdir and env

flags:
  -n, --namespace     job queue namespace (default: "default", or CQ_NS env)
      --host          run cq on remote host via ssh
`)
	os.Exit(1)
}

// zmxArgs returns the command and args to invoke zmx.
// Falls back to `nix run github:neurosnap/zmx --` if zmx is not in PATH.
func zmxArgs(args ...string) (string, []string) {
	if _, err := exec.LookPath("zmx"); err == nil {
		return "zmx", args
	}
	nixArgs := []string{"--extra-experimental-features", "nix-command flakes", "run", "github:neurosnap/zmx", "--"}
	return "nix", append(nixArgs, args...)
}

func zmxExec(args ...string) *exec.Cmd {
	bin, fullArgs := zmxArgs(args...)
	return exec.Command(bin, fullArgs...)
}

func jobSession(id int64) string {
	return fmt.Sprintf("cq-%s-%d", namespace, id)
}

func jobScriptPath(id int64) string {
	return filepath.Join(getStateDir(), fmt.Sprintf("%s-%d.sh", namespace, id))
}

func openDB() *sql.DB {
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open database: %v\n", err)
		os.Exit(1)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			ns TEXT NOT NULL,
			command TEXT NOT NULL,
			args TEXT NOT NULL,
			workdir TEXT NOT NULL,
			env TEXT NOT NULL,
			status TEXT DEFAULT 'pending',
			pid INTEGER,
			created_at DATETIME,
			started_at DATETIME,
			finished_at DATETIME,
			exit_code INTEGER,
			log TEXT
		)
	`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create table: %v\n", err)
		os.Exit(1)
	}

	migrateOldTables(db)

	return db
}

// migrateOldTables copies rows from per-namespace `jobs_<ns>` tables (the
// pre-refactor schema) into the unified `jobs` table, then drops them.
func migrateOldTables(db *sql.DB) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'jobs\\_%' ESCAPE '\\'")
	if err != nil {
		return
	}
	var oldTables []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		oldTables = append(oldTables, name)
	}
	rows.Close()

	for _, t := range oldTables {
		ns := strings.TrimPrefix(t, "jobs_")
		// ns came from the user's CQ_NS / -n flag and was previously a SQL
		// identifier suffix, so it's already constrained to safe chars.
		db.Exec(fmt.Sprintf(`INSERT INTO jobs (ns, command, args, workdir, env, status, pid, created_at, started_at, finished_at, exit_code, log)
			SELECT '%s', command, args, workdir, env, status, pid, created_at, started_at, finished_at, exit_code, log FROM %s`, ns, t))
		db.Exec(fmt.Sprintf("DROP TABLE %s", t))
	}
}

func cmdQueue(args []string) {
	db := openDB()
	defer db.Close()

	cmdName := args[0]
	cmdArgsJSON, _ := json.Marshal(args[1:])
	workdir, _ := os.Getwd()
	envJSON, _ := json.Marshal(os.Environ())

	result, err := db.Exec("INSERT INTO jobs (ns, command, args, workdir, env, status, created_at) VALUES (?, ?, ?, ?, ?, 'pending', ?)",
		namespace, cmdName, string(cmdArgsJSON), workdir, string(envJSON), time.Now())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to queue job: %v\n", err)
		os.Exit(1)
	}

	jobID, _ := result.LastInsertId()
	fmt.Fprintf(os.Stderr, "queued: [%d] %s\n", jobID, args)

	spawnWorker()
}

// spawnWorker launches a detached `cq --worker` process. If a worker is
// already running for this namespace it will fail to acquire the flock and
// exit silently, so calling this is always safe.
func spawnWorker() {
	self, err := os.Executable()
	if err != nil {
		return
	}
	cmd := exec.Command(self, "-n", namespace, "--worker")
	devNull, _ := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if devNull != nil {
		cmd.Stdin = devNull
		cmd.Stdout = devNull
		cmd.Stderr = devNull
		defer devNull.Close()
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return
	}
	cmd.Process.Release()
}

// cmdWorker is the long-running worker loop. Only one worker per namespace
// can run at a time, enforced by an flock on the namespace's lock file.
// Polls SQLite for pending jobs, runs each in its own zmx session, and
// polls `zmx list` to detect completion. Exits after a short idle window
// so a stale queue doesn't leak processes.
func cmdWorker() {
	lock, err := os.OpenFile(lockFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return // another worker holds the lock
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)

	db := openDB()
	defer db.Close()

	const idleExitRounds = 30 // exit after ~30s with no pending work
	idle := 0
	for idle < idleExitRounds {
		job, _ := getNextJob(db)
		if job == nil {
			time.Sleep(time.Second)
			idle++
			continue
		}
		idle = 0
		runJob(db, job)
	}
}

// runJob executes a single job: marks it running, spawns its zmx session,
// polls until completion, then records the result and saves scrollback.
func runJob(db *sql.DB, job *Job) {
	db.Exec("UPDATE jobs SET status = 'running', started_at = ? WHERE id = ?", time.Now(), job.ID)

	sessionName := jobSession(job.ID)

	userCmd := shellQuote(job.Command)
	for _, a := range job.Args {
		userCmd += " " + shellQuote(a)
	}
	display := job.Command
	if len(job.Args) > 0 {
		display += " " + strings.Join(job.Args, " ")
	}
	// Echo the command first so `cq attach` / `cq log` shows what's running.
	// The subshell wraps the user command so a builtin `exit` doesn't kill
	// zmx's shell before we can capture the exit code.
	shellCmd := "echo " + shellQuote("$ "+display) + "\n(" + userCmd + ")\n"

	scriptPath := jobScriptPath(job.ID)
	os.WriteFile(scriptPath, []byte(shellCmd), 0644)

	cmd := zmxExec("run", sessionName, "sh", scriptPath)
	cmd.Dir = job.Workdir
	cmd.Env = job.Env
	devNull, _ := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if devNull != nil {
		cmd.Stdin = devNull
		cmd.Stdout = devNull
		cmd.Stderr = devNull
		defer devNull.Close()
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		db.Exec("UPDATE jobs SET status = 'done', exit_code = -1, finished_at = ? WHERE id = ?", time.Now(), job.ID)
		os.Remove(scriptPath)
		return
	}
	cmd.Process.Release()

	// Wait for the session to register and capture its pid.
	for i := 0; i < 40; i++ {
		if s := getSession(sessionName); s != nil && s.pid > 0 {
			db.Exec("UPDATE jobs SET pid = ? WHERE id = ?", s.pid, job.ID)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Poll until zmx reports the task as ended (or the session vanishes).
	exitCode := -1
	for {
		s := getSession(sessionName)
		if s == nil {
			break
		}
		if s.ended {
			exitCode = s.exitCode
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Save scrollback while the session still exists.
	if logOutput, err := zmxExec("history", sessionName).Output(); err == nil {
		db.Exec("UPDATE jobs SET log = ? WHERE id = ?", string(logOutput), job.ID)
	}

	// If the user already marked the job killed, preserve that status.
	var status string
	db.QueryRow("SELECT status FROM jobs WHERE id = ?", job.ID).Scan(&status)
	if status != "killed" {
		db.Exec("UPDATE jobs SET status = 'done', finished_at = ?, exit_code = ? WHERE id = ?",
			time.Now(), exitCode, job.ID)
	} else {
		db.Exec("UPDATE jobs SET finished_at = ?, exit_code = ? WHERE id = ?",
			time.Now(), exitCode, job.ID)
	}

	os.Remove(scriptPath)
	zmxExec("kill", sessionName).Run() // best-effort cleanup; harmless if already gone
	notifyDone(job.ID, exitCode)
}

// runRemote execs ssh to run cq on a remote host, forwarding namespace
// and the remaining args.
func runRemote(host string, args []string) {
	sshBin, err := exec.LookPath("ssh")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ssh not found: %v\n", err)
		os.Exit(1)
	}

	remote := "cq"
	if namespace != "" && namespace != "default" {
		remote += " -n " + shellQuote(namespace)
	}
	for _, a := range args {
		remote += " " + shellQuote(a)
	}

	sshArgs := []string{"ssh"}
	// allocate a tty for interactive subcommand
	if args[0] == "attach" || args[0] == "a" {
		sshArgs = append(sshArgs, "-t")
	}
	sshArgs = append(sshArgs, host, remote)
	syscall.Exec(sshBin, sshArgs, os.Environ())
}

func notifyDone(id int64, exitCode int) {
	if runtime.GOOS != "linux" {
		return
	}
	if _, err := exec.LookPath("notify-send"); err != nil {
		return
	}

	db := openDB()
	defer db.Close()

	var command, argsJSON string
	if err := db.QueryRow("SELECT command, args FROM jobs WHERE id = ? AND ns = ?", id, namespace).Scan(&command, &argsJSON); err != nil {
		return
	}
	var args []string
	json.Unmarshal([]byte(argsJSON), &args)
	cmdStr := command
	if len(args) > 0 {
		cmdStr += " " + strings.Join(args, " ")
	}

	urgency := "normal"
	status := "done"
	if exitCode != 0 {
		urgency = "critical"
		status = fmt.Sprintf("failed (%d)", exitCode)
	}
	title := fmt.Sprintf("cq [%d] %s", id, status)
	exec.Command("notify-send", "-u", urgency, "-a", "cq", title, cmdStr).Run()
}

func cmdAttach(idStr string) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid job id: %s\n", idStr)
		os.Exit(1)
	}

	sessionName := jobSession(int64(id))
	bin, fullArgs := zmxArgs("attach", sessionName)
	binPath, err := exec.LookPath(bin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s not found: %v\n", bin, err)
		os.Exit(1)
	}
	syscall.Exec(binPath, append([]string{bin}, fullArgs...), os.Environ())
}

func cmdLog(idStr string) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid job id: %s\n", idStr)
		os.Exit(1)
	}

	sessionName := jobSession(int64(id))

	// Try live session first, fall back to saved log
	if getSession(sessionName) != nil {
		cmd := zmxExec( "history", sessionName)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Run()
		return
	}

	db := openDB()
	defer db.Close()

	var log sql.NullString
	err = db.QueryRow("SELECT log FROM jobs WHERE id = ? AND ns = ?", id, namespace).Scan(&log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "job not found: %d\n", id)
		os.Exit(1)
	}
	if log.Valid {
		fmt.Print(log.String)
	}
}

func cmdKill(idStr string) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid job id: %s\n", idStr)
		os.Exit(1)
	}

	sessionName := jobSession(int64(id))
	cmd := zmxExec( "kill", sessionName)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to kill job %d\n", id)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "killed: [%d]\n", id)

	db := openDB()
	defer db.Close()
	db.Exec("UPDATE jobs SET status = 'killed' WHERE id = ? AND ns = ?", id, namespace)

	// Wake the worker so it can pick up the next pending job.
	spawnWorker()
}

func cmdRetry(idStr string) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid job id: %s\n", idStr)
		os.Exit(1)
	}

	db := openDB()
	defer db.Close()

	var command, argsJSON, workdir, envJSON string
	err = db.QueryRow("SELECT command, args, workdir, env FROM jobs WHERE id = ? AND ns = ?", id, namespace).
		Scan(&command, &argsJSON, &workdir, &envJSON)
	if err == sql.ErrNoRows {
		fmt.Fprintf(os.Stderr, "job not found: %d\n", id)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get job: %v\n", err)
		os.Exit(1)
	}

	result, err := db.Exec("INSERT INTO jobs (ns, command, args, workdir, env, status, created_at) VALUES (?, ?, ?, ?, ?, 'pending', ?)",
		namespace, command, argsJSON, workdir, envJSON, time.Now())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to queue job: %v\n", err)
		os.Exit(1)
	}

	jobID, _ := result.LastInsertId()
	var args []string
	json.Unmarshal([]byte(argsJSON), &args)
	fmt.Fprintf(os.Stderr, "queued: [%d] %s %v\n", jobID, command, args)

	spawnWorker()
}

func cmdList() {
	db := openDB()
	defer db.Close()

	rows, err := db.Query(
		"SELECT id, command, args, workdir, status, pid, exit_code FROM jobs WHERE ns = ? ORDER BY id DESC LIMIT 20",
		namespace,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to query jobs: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	fmt.Printf("%-4s %-12s %-8s %-20s %s\n", "ID", "STATUS", "PID", "DIR", "COMMAND")
	for rows.Next() {
		var id int64
		var command, argsJSON, workdir, status string
		var pid, exitCode sql.NullInt64

		rows.Scan(&id, &command, &argsJSON, &workdir, &status, &pid, &exitCode)

		var args []string
		json.Unmarshal([]byte(argsJSON), &args)

		pidStr := "-"
		if pid.Valid && pid.Int64 > 0 {
			pidStr = fmt.Sprintf("%d", pid.Int64)
		}

		statusStr := status
		if status == "done" && exitCode.Valid && exitCode.Int64 != 0 {
			statusStr = fmt.Sprintf("fail(%d)", exitCode.Int64)
		} else if status == "killed" && exitCode.Valid {
			statusStr = fmt.Sprintf("killed(%d)", exitCode.Int64)
		}

		cmdStr := command
		if len(args) > 0 {
			cmdStr += " " + strings.Join(args, " ")
		}

		fmt.Printf("%-4d %-12s %-8s %-20s %s\n", id, statusStr, pidStr, shortenPath(workdir), cmdStr)
	}
}

func cmdReset() {
	db := openDB()
	defer db.Close()

	_, err := db.Exec("DELETE FROM jobs WHERE ns = ?", namespace)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to reset: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "reset: cleared all jobs in namespace %q\n", namespace)
}

func cmdRm(idStr string) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid job id: %s\n", idStr)
		os.Exit(1)
	}

	db := openDB()
	defer db.Close()

	var status string
	err = db.QueryRow("SELECT status FROM jobs WHERE id = ? AND ns = ?", id, namespace).Scan(&status)
	if err == sql.ErrNoRows {
		fmt.Fprintf(os.Stderr, "job not found: %d\n", id)
		os.Exit(1)
	}
	if status == "running" {
		fmt.Fprintf(os.Stderr, "cannot remove running job %d, kill it first\n", id)
		os.Exit(1)
	}

	db.Exec("DELETE FROM jobs WHERE id = ? AND ns = ?", id, namespace)
	fmt.Fprintf(os.Stderr, "removed: [%d]\n", id)
}

func cmdClean() {
	db := openDB()
	defer db.Close()

	result, err := db.Exec("DELETE FROM jobs WHERE ns = ? AND status IN ('done', 'killed')", namespace)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to clean: %v\n", err)
		os.Exit(1)
	}
	n, _ := result.RowsAffected()
	fmt.Fprintf(os.Stderr, "clean: removed %d jobs\n", n)
}

func cmdCat(idStr string) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid job id: %s\n", idStr)
		os.Exit(1)
	}

	db := openDB()
	defer db.Close()

	var command, argsJSON, workdir, envJSON string
	err = db.QueryRow("SELECT command, args, workdir, env FROM jobs WHERE id = ? AND ns = ?", id, namespace).
		Scan(&command, &argsJSON, &workdir, &envJSON)
	if err == sql.ErrNoRows {
		fmt.Fprintf(os.Stderr, "job not found: %d\n", id)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get job: %v\n", err)
		os.Exit(1)
	}

	var args []string
	json.Unmarshal([]byte(argsJSON), &args)
	var env []string
	json.Unmarshal([]byte(envJSON), &env)

	for _, e := range env {
		fmt.Printf("export %s\n", shellQuote(e))
	}
	fmt.Printf("cd %s\n", shellQuote(workdir))
	cmdStr := shellQuote(command)
	for _, a := range args {
		cmdStr += " " + shellQuote(a)
	}
	fmt.Printf("%s\n", cmdStr)
}

// shortenPath abbreviates a path: replaces $HOME with ~, shortens
// intermediate directories to first char. e.g. ~/src/github.com/aca/cq -> ~/s/g/a/cq
func shortenPath(p string) string {
	home, _ := os.UserHomeDir()
	if home != "" && strings.HasPrefix(p, home) {
		p = "~" + p[len(home):]
	}
	parts := strings.Split(p, "/")
	// Shorten all but the last component
	for i := 1; i < len(parts)-1; i++ {
		if len(parts[i]) > 0 {
			parts[i] = parts[i][:1]
		}
	}
	return strings.Join(parts, "/")
}

func shellQuote(s string) string {
	q, err := syntax.Quote(s, syntax.LangPOSIX)
	if err != nil {
		// syntax.Quote only fails for strings containing characters that
		// cannot be expressed in POSIX (e.g. NUL). Fall back to a literal.
		return s
	}
	return q
}

type Job struct {
	ID      int64
	Command string
	Args    []string
	Workdir string
	Env     []string
}

func getNextJob(db *sql.DB) (*Job, error) {
	row := db.QueryRow(
		"SELECT id, command, args, workdir, env FROM jobs WHERE ns = ? AND status = 'pending' ORDER BY id ASC LIMIT 1",
		namespace,
	)

	var job Job
	var argsJSON, envJSON string
	err := row.Scan(&job.ID, &job.Command, &argsJSON, &job.Workdir, &envJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	json.Unmarshal([]byte(argsJSON), &job.Args)
	json.Unmarshal([]byte(envJSON), &job.Env)
	return &job, nil
}

type sessionInfo struct {
	pid      int
	ended    bool
	exitCode int
}

// getSession queries zmx for a session's state.
// Returns nil if session doesn't exist.
func getSession(sessionName string) *sessionInfo {
	output, err := zmxExec( "list").Output()
	if err != nil {
		return nil
	}
	for _, line := range strings.Split(string(output), "\n") {
		if !strings.Contains(line, "session_name="+sessionName+"\t") {
			continue
		}
		info := &sessionInfo{}
		for _, part := range strings.Split(line, "\t") {
			if strings.HasPrefix(part, "pid=") {
				info.pid, _ = strconv.Atoi(strings.TrimPrefix(part, "pid="))
			} else if strings.HasPrefix(part, "task_ended_at=") {
				info.ended = true
			} else if strings.HasPrefix(part, "task_exit_code=") {
				info.exitCode, _ = strconv.Atoi(strings.TrimPrefix(part, "task_exit_code="))
			}
		}
		return info
	}
	return nil
}
