// cq - Command Queue
//
// A job queue that runs commands sequentially using zmx (terminal multiplexer).
//
// Architecture:
//   - Jobs are stored in SQLite with command, args, workdir, and full environment
//   - A worker daemon processes jobs sequentially (one at a time per namespace)
//   - Each job runs inside a zmx session, enabling:
//     1. Detached execution: jobs continue running even without a terminal
//     2. Later attachment: `cq attach <id>` connects your terminal to a running job
//     3. Scrollback history: `cq log <id>` retrieves output via `zmx history`
//
// How zmx enables `cq vim` to work:
//   - When you run `cq vim file.txt`, the job is queued and vim starts in a zmx session
//   - vim runs headless initially (no terminal attached), but zmx provides a virtual PTY
//   - Running `cq attach <id>` executes `zmx attach <session>`, connecting your terminal
//   - Your terminal's stdin/stdout/stderr are now wired to vim through zmx
//   - You can detach (zmx hotkey) and reattach later, or from a different terminal
//
// Why Setsid is used:
//   - Setsid creates a new session and process group for the spawned process
//   - This detaches the process from the current terminal's controlling TTY
//   - Without Setsid, the worker/job would receive SIGHUP when the queuing terminal closes
//   - It also prevents the background process from being affected by terminal job control
//
// Worker lifecycle:
//   - Worker runs inside its own zmx session (cq-worker-<namespace>)
//   - Uses flock() for distributed locking - only one worker per namespace
//   - Polls for pending jobs, runs them sequentially, exits when queue is empty
//   - Automatically respawned when new jobs are queued

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	flag "github.com/spf13/pflag"
)

var namespace string
var dbFile string
var lockFile string
var workerSession string

func init() {
	// Default from env
	defaultNS := os.Getenv("CQ_NS")
	if defaultNS == "" {
		defaultNS = "default"
	}
	flag.StringVarP(&namespace, "namespace", "n", defaultNS, "job queue namespace")
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
	workerSession = fmt.Sprintf("cq-worker-%s", namespace)
}

func main() {
	// Check for internal --worker command and filter it out before flag parsing
	isWorker := false
	var filteredArgs []string
	for _, arg := range os.Args {
		if arg == "--worker" {
			isWorker = true
		} else {
			filteredArgs = append(filteredArgs, arg)
		}
	}
	os.Args = filteredArgs

	flag.Parse()
	initNamespace()

	if isWorker {
		cmdWorkerDaemon()
		return
	}

	args := flag.Args()
	if len(args) < 1 {
		usage()
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
	case "clean":
		cmdClean()
	case "resume":
		spawnWorkerDaemon()
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
  clean               remove done/killed jobs
  reset               clear all jobs in namespace
  resume              respawn worker to process pending jobs
  cat <job-id>        show job command with workdir and env

flags:
  -n, --namespace     job queue namespace (default: "default", or CQ_NS env)
`)
	os.Exit(1)
}

func tableName() string {
	return fmt.Sprintf("jobs_%s", namespace)
}

func jobSession(id int64) string {
	return fmt.Sprintf("cq-%s-%d", namespace, id)
}

func openDB() *sql.DB {
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open database: %v\n", err)
		os.Exit(1)
	}

	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			command TEXT NOT NULL,
			args TEXT NOT NULL,
			workdir TEXT NOT NULL,
			env TEXT NOT NULL,
			status TEXT DEFAULT 'pending',
			pid INTEGER,
			created_at DATETIME,
			started_at DATETIME,
			finished_at DATETIME,
			exit_code INTEGER
		)
	`, tableName()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create table: %v\n", err)
		os.Exit(1)
	}

	return db
}

func cmdQueue(args []string) {
	db := openDB()
	defer db.Close()

	cmdName := args[0]
	cmdArgsJSON, _ := json.Marshal(args[1:])
	workdir, _ := os.Getwd()
	envJSON, _ := json.Marshal(os.Environ())

	result, err := db.Exec("INSERT INTO "+tableName()+" (command, args, workdir, env, status, created_at) VALUES (?, ?, ?, ?, 'pending', ?)",
		cmdName, string(cmdArgsJSON), workdir, string(envJSON), time.Now())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to queue job: %v\n", err)
		os.Exit(1)
	}

	jobID, _ := result.LastInsertId()
	fmt.Fprintf(os.Stderr, "queued: [%d] %s\n", jobID, args)

	// Spawn worker daemon via zmx (if not already running)
	spawnWorkerDaemon()
}

// spawnWorkerDaemon starts the background worker if not already running.
// The worker is spawned inside a zmx session so it persists after this process exits.
func spawnWorkerDaemon() {
	// Check if worker is still running (session exists and task hasn't ended)
	if s := getSession(workerSession); s != nil && !s.ended {
		return
	}
	// Kill stale session if task ended but session lingers
	exec.Command("zmx", "kill", workerSession).Run()

	self, _ := os.Executable()
	cwd, _ := os.Getwd()

	// zmx run returns immediately after sending the command, so we just need
	// to wait for it to complete. No Setsid needed since zmx manages the session.
	cmd := exec.Command("zmx", "run", workerSession, self, "-n", namespace, "--worker")
	cmd.Dir = cwd
	cmd.Run()
}

func cmdWorkerDaemon() {
	db := openDB()
	defer db.Close()
	runWorker(db)
}

// cmdAttach connects the current terminal to a running job's zmx session.
// This is how interactive programs like vim become usable:
// - zmx provides a PTY (pseudo-terminal) that the job writes to
// - `zmx attach` connects our real terminal to that PTY
// - stdin/stdout/stderr flow through zmx to the job
// - User can detach with zmx hotkey and reattach later
func cmdAttach(idStr string) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid job id: %s\n", idStr)
		os.Exit(1)
	}

	sessionName := jobSession(int64(id))
	zmxPath, err := exec.LookPath("zmx")
	if err != nil {
		fmt.Fprintf(os.Stderr, "zmx not found: %v\n", err)
		os.Exit(1)
	}
	syscall.Exec(zmxPath, []string{"zmx", "attach", sessionName}, os.Environ())
}

func cmdLog(idStr string) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid job id: %s\n", idStr)
		os.Exit(1)
	}

	sessionName := jobSession(int64(id))
	cmd := exec.Command("zmx", "history", sessionName)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Run()
}

func cmdKill(idStr string) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid job id: %s\n", idStr)
		os.Exit(1)
	}

	sessionName := jobSession(int64(id))
	cmd := exec.Command("zmx", "kill", sessionName)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to kill job %d\n", id)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "killed: [%d]\n", id)

	// Update status in db
	db := openDB()
	defer db.Close()
	db.Exec("UPDATE "+tableName()+" SET status = 'killed' WHERE id = ?", id)
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
	err = db.QueryRow("SELECT command, args, workdir, env FROM "+tableName()+" WHERE id = ?", id).
		Scan(&command, &argsJSON, &workdir, &envJSON)
	if err == sql.ErrNoRows {
		fmt.Fprintf(os.Stderr, "job not found: %d\n", id)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get job: %v\n", err)
		os.Exit(1)
	}

	result, err := db.Exec("INSERT INTO "+tableName()+" (command, args, workdir, env, status, created_at) VALUES (?, ?, ?, ?, 'pending', ?)",
		command, argsJSON, workdir, envJSON, time.Now())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to queue job: %v\n", err)
		os.Exit(1)
	}

	jobID, _ := result.LastInsertId()
	var args []string
	json.Unmarshal([]byte(argsJSON), &args)
	fmt.Fprintf(os.Stderr, "queued: [%d] %s %v\n", jobID, command, args)

	spawnWorkerDaemon()
}

func cmdList() {
	db := openDB()
	defer db.Close()

	rows, err := db.Query(
		"SELECT id, command, args, status, pid, exit_code FROM " + tableName() + " ORDER BY id DESC LIMIT 20",
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to query jobs: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	fmt.Printf("%-4s %-10s %-8s %s\n", "ID", "STATUS", "PID", "COMMAND")
	for rows.Next() {
		var id int64
		var command, argsJSON, status string
		var pid, exitCode sql.NullInt64

		rows.Scan(&id, &command, &argsJSON, &status, &pid, &exitCode)

		var args []string
		json.Unmarshal([]byte(argsJSON), &args)

		pidStr := "-"
		if pid.Valid && pid.Int64 > 0 {
			pidStr = fmt.Sprintf("%d", pid.Int64)
		}

		cmdStr := command
		if len(args) > 0 {
			cmdStr += " " + strings.Join(args, " ")
		}

		fmt.Printf("%-4d %-10s %-8s %s\n", id, status, pidStr, cmdStr)
	}
}

func cmdReset() {
	db := openDB()
	defer db.Close()

	_, err := db.Exec("DELETE FROM " + tableName())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to reset: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "reset: cleared all jobs in namespace %q\n", namespace)
}

func cmdClean() {
	db := openDB()
	defer db.Close()

	result, err := db.Exec("DELETE FROM "+tableName()+" WHERE status IN ('done', 'killed')")
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
	err = db.QueryRow("SELECT command, args, workdir, env FROM "+tableName()+" WHERE id = ?", id).
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

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	safe := true
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '/' || c == '.' || c == ',' || c == ':' || c == '=' || c == '+') {
			safe = false
			break
		}
	}
	if safe {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// runWorker is the main loop that processes queued jobs sequentially.
// Only one worker runs per namespace, enforced by flock().
func runWorker(db *sql.DB) {
	// flock (file lock) ensures only one worker per namespace.
	// LOCK_EX = exclusive lock - blocks if another process holds the lock.
	// Lock is automatically released when process exits (even if crashed).
	lock, err := os.OpenFile(lockFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open lock file: %v\n", err)
		return
	}
	defer lock.Close()

	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		fmt.Fprintf(os.Stderr, "failed to acquire lock: %v\n", err)
		return
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)

	// First, wait for any "running" jobs (orphaned from killed workers)
	waitForRunningJobs(db)

	for {
		job, err := getNextJob(db)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error getting job: %v\n", err)
			return
		}
		if job == nil {
			return
		}

		runJob(db, job)
	}
}

func waitForRunningJobs(db *sql.DB) {
	rows, err := db.Query("SELECT id FROM "+tableName()+" WHERE status = 'running'")
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		rows.Scan(&id)
		sessionName := jobSession(int64(id))

		// Wait for this session to complete
		for {
			s := getSession(sessionName)
			if s == nil || s.ended {
				exec.Command("zmx", "kill", sessionName).Run()
				db.Exec("UPDATE "+tableName()+" SET status = 'done', finished_at = ? WHERE id = ?", time.Now(), id)
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
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
		"SELECT id, command, args, workdir, env FROM " + tableName() + " WHERE status = 'pending' ORDER BY id ASC LIMIT 1",
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

// runJob executes a job inside a zmx session.
// The job runs with its original workdir and environment (captured at queue time).
func runJob(db *sql.DB, job *Job) {
	db.Exec("UPDATE "+tableName()+" SET status = 'running', started_at = ? WHERE id = ?", time.Now(), job.ID)

	sessionName := jobSession(job.ID)
	fmt.Fprintf(os.Stderr, "running: [%d] %s %v\n", job.ID, job.Command, job.Args)

	zmxArgs := []string{"run", sessionName, job.Command}
	zmxArgs = append(zmxArgs, job.Args...)

	cmd := exec.Command("zmx", zmxArgs...)
	cmd.Dir = job.Workdir
	cmd.Env = job.Env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Run()

	// Poll zmx to track job lifecycle
	var exitCode int
	for {
		s := getSession(sessionName)
		if s == nil {
			break
		}
		if s.pid > 0 {
			db.Exec("UPDATE "+tableName()+" SET pid = ? WHERE id = ? AND pid IS NULL", s.pid, job.ID)
		}
		if s.ended {
			exitCode = s.exitCode
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Kill the zmx session (it lingers with a shell after task ends)
	exec.Command("zmx", "kill", sessionName).Run()

	db.Exec("UPDATE "+tableName()+" SET status = 'done', finished_at = ?, exit_code = ? WHERE id = ?", time.Now(), exitCode, job.ID)
}

type sessionInfo struct {
	pid      int
	ended    bool
	exitCode int
}

// getSession queries zmx for a session's state.
// Returns nil if session doesn't exist.
func getSession(sessionName string) *sessionInfo {
	output, err := exec.Command("zmx", "list").Output()
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
