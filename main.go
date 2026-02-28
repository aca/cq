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
	// Check if worker zmx session exists by looking for its PID in `zmx list`
	if getSessionPID(workerSession) != 0 {
		return
	}

	self, _ := os.Executable()
	cwd, _ := os.Getwd()

	// `zmx attach <session> <cmd>` creates a new zmx session and runs cmd inside it.
	// If session already exists, it attaches to it (but we checked above).
	cmd := exec.Command("zmx", "attach", workerSession, self, "-n", namespace, "--worker")
	cmd.Dir = cwd

	// Setsid: create new session, detach from controlling terminal.
	// This ensures zmx (and the worker) survives after we exit:
	// - New process group: won't receive signals meant for our terminal's foreground group
	// - No controlling TTY: won't get SIGHUP when terminal closes
	// - Independent session: shell job control (Ctrl+Z, bg, fg) won't affect it
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	// Redirect to /dev/null since worker runs detached (output goes to zmx session)
	devNull, _ := os.Open("/dev/null")
	defer devNull.Close()
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = devNull
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
	cmd := exec.Command("zmx", "attach", sessionName)
	// Pass through our terminal's stdin/stdout/stderr directly
	// This makes the job interactive - keystrokes go to job, output comes back
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Run()
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

	fmt.Printf("%-4s %-10s %-8s %-30s\n", "ID", "STATUS", "PID", "COMMAND")
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
			cmdStr += " " + fmt.Sprintf("%v", args)
		}
		if len(cmdStr) > 30 {
			cmdStr = cmdStr[:27] + "..."
		}

		fmt.Printf("%-4d %-10s %-8s %-30s\n", id, status, pidStr, cmdStr)
	}
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
			pid := getSessionPID(sessionName)
			if pid == 0 {
				// Session done
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

	// `zmx attach <session> <cmd> <args...>` creates a new session running the command.
	// zmx provides a PTY even though we're not attached - this is key for interactive apps.
	// The PTY buffers output (scrollback) and provides terminal emulation (ANSI codes, etc).
	zmxArgs := []string{"attach", sessionName, job.Command}
	zmxArgs = append(zmxArgs, job.Args...)

	cmd := exec.Command("zmx", zmxArgs...)
	cmd.Dir = job.Workdir // Run in original directory where job was queued
	cmd.Env = job.Env     // Use original environment (PATH, EDITOR, etc)

	// Worker runs detached, so redirect to /dev/null.
	// The job's actual I/O goes through zmx's PTY, not these file descriptors.
	devNull, _ := os.Open("/dev/null")
	defer devNull.Close()
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = devNull

	// Setsid: detach from terminal so job survives if worker's zmx session closes.
	// Also prevents job from inheriting worker's process group signals.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	// zmx attach returns immediately after spawning (returns 1 when no terminal attached)
	cmd.Run()

	// Poll zmx to track job lifecycle - zmx list shows running sessions with PIDs
	pid := getSessionPID(sessionName)
	if pid == 0 {
		// Session already finished (fast command like `echo hello`)
		db.Exec("UPDATE "+tableName()+" SET status = 'done', finished_at = ? WHERE id = ?", time.Now(), job.ID)
		return
	}
	db.Exec("UPDATE "+tableName()+" SET status = 'running', pid = ? WHERE id = ?", pid, job.ID)

	// Wait for zmx session to end (job finished or killed)
	for {
		time.Sleep(500 * time.Millisecond)
		if getSessionPID(sessionName) == 0 {
			break
		}
	}

	db.Exec("UPDATE "+tableName()+" SET status = 'done', finished_at = ? WHERE id = ?", time.Now(), job.ID)
}

// getSessionPID queries zmx for a session's PID. Returns 0 if session doesn't exist.
// Used to check if worker/job is running and to track job lifecycle.
func getSessionPID(sessionName string) int {
	output, err := exec.Command("zmx", "list").Output()
	if err != nil {
		return 0
	}
	// zmx list output format: session_name=cq-default-1\tpid=12345\tclients=0
	for _, line := range strings.Split(string(output), "\n") {
		if strings.Contains(line, "session_name="+sessionName+"\t") {
			for _, part := range strings.Split(line, "\t") {
				if strings.HasPrefix(part, "pid=") {
					pid, _ := strconv.Atoi(strings.TrimPrefix(part, "pid="))
					return pid
				}
			}
		}
	}
	return 0
}
