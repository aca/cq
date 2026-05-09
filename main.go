// cq - Command Queue
//
// A job queue that runs commands sequentially using zmx (terminal multiplexer).
//
// Architecture:
//   - Jobs are stored in SQLite with command, args, workdir, and full environment
//   - Each job runs inside a zmx session with a wrapper that chains to the next job
//   - No daemon or polling: when a job finishes, the wrapper calls `cq --done`
//     which saves the result and starts the next pending job
//   - Each job runs inside a zmx session, enabling:
//     1. Detached execution: jobs continue running even without a terminal
//     2. Later attachment: `cq attach <id>` connects your terminal to a running job
//     3. Scrollback history: `cq log <id>` retrieves output via `zmx history`
//
// Shell quoting through zmx:
//   `zmx run <session> <argv...>` does not exec argv directly — it joins the
//   trailing argv with spaces, appends `; echo ZMX_TASK_COMPLETED:$?`, and
//   feeds the whole thing to a shell. Worse, different zmx builds disagree on
//   whether to %q-quote each argv element before joining (the nix flake build
//   does; some local builds do not). Either way, any shell metacharacter we
//   try to pass through argv ends up mangled by one side or the other.
//
//   To dodge the entire problem, we write the wrapped command to a script
//   file in the state dir and invoke it as `sh <path>`. Both `sh` and the
//   path contain only quote-safe characters (alnum, `/`, `-`, `.`, `_`),
//   so they survive any join/quote variation untouched. cmdDone removes the
//   script after recording the result.
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
)

var namespace string
var host string
var dbFile string
var lockFile string

func init() {
	defaultNS := os.Getenv("CQ_NS")
	if defaultNS == "" {
		defaultNS = "default"
	}
	flag.StringVarP(&namespace, "namespace", "n", defaultNS, "job queue namespace")
	flag.StringVar(&host, "host", "", "run cq on remote host via ssh")
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
	// Check for internal --done <id> <exit_code> before flag parsing.
	// Save the done args, then strip them so pflag doesn't see them.
	var doneArgs []string
	var filteredArgs []string
	for i, arg := range os.Args {
		if arg == "--done" && i+2 < len(os.Args) {
			doneArgs = os.Args[i+1 : i+3]
			filteredArgs = append(filteredArgs, os.Args[:i]...)
			// skip --done <id> <exit_code>
			break
		}
	}
	if doneArgs == nil {
		filteredArgs = os.Args
	}

	os.Args = filteredArgs
	flag.CommandLine.SetInterspersed(false)
	flag.Parse()
	initNamespace()

	if doneArgs != nil {
		cmdDone(doneArgs)
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
		startNextJob()
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

func tableName() string {
	return fmt.Sprintf("jobs_%s", namespace)
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
			exit_code INTEGER,
			log TEXT
		)
	`, tableName()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create table: %v\n", err)
		os.Exit(1)
	}

	// migrate: add log column if missing
	db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN log TEXT", tableName()))

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

	startNextJob()
}

// startNextJob starts the next pending job if nothing is currently running.
func startNextJob() {
	db := openDB()
	defer db.Close()

	// Use flock to prevent races between concurrent cq calls
	lock, err := os.OpenFile(lockFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		// Another process holds the lock, it will handle chaining
		return
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)

	// Don't start if something is already running
	var count int
	db.QueryRow("SELECT COUNT(*) FROM " + tableName() + " WHERE status = 'running'").Scan(&count)
	if count > 0 {
		return
	}

	job, err := getNextJob(db)
	if err != nil || job == nil {
		return
	}

	db.Exec("UPDATE "+tableName()+" SET status = 'running', started_at = ? WHERE id = ?", time.Now(), job.ID)

	sessionName := jobSession(job.ID)

	// Build the wrapped command:
	//   (user_command); cq -n <ns> --done <id> $?
	// The subshell (...) prevents builtins like `exit` from killing zmx's shell.
	// After the command finishes, --done saves the result and starts the next job.
	self, _ := os.Executable()
	shellCmd := "(" + shellQuote(job.Command)
	for _, a := range job.Args {
		shellCmd += " " + shellQuote(a)
	}
	shellCmd += "); " + shellQuote(self) + " -n " + shellQuote(namespace) + " --done " + strconv.FormatInt(job.ID, 10) + " $?"

	// Write the wrapped command to a script file rather than passing it
	// through zmx's argv. See the "Shell quoting through zmx" note at the
	// top of the file for why argv-based passing is unreliable.
	scriptPath := jobScriptPath(job.ID)
	os.WriteFile(scriptPath, []byte(shellCmd+"\n"), 0644)

	cmd := zmxExec("run", sessionName, "sh", scriptPath)
	cmd.Dir = job.Workdir
	cmd.Env = job.Env
	cmd.Run()

	// Record PID
	if s := getSession(sessionName); s != nil && s.pid > 0 {
		db.Exec("UPDATE "+tableName()+" SET pid = ? WHERE id = ?", s.pid, job.ID)
	}
}

// cmdDone is called by the wrapper after a job finishes.
// It saves the result and starts the next pending job.
func cmdDone(args []string) {
	if len(args) < 2 {
		return
	}
	id, _ := strconv.Atoi(args[0])
	exitCode, _ := strconv.Atoi(args[1])

	db := openDB()
	defer db.Close()

	sessionName := jobSession(int64(id))

	// Save scrollback history before killing the session
	if logOutput, err := zmxExec( "history", sessionName).Output(); err == nil {
		db.Exec("UPDATE "+tableName()+" SET log = ? WHERE id = ?", string(logOutput), id)
	}

	db.Exec("UPDATE "+tableName()+" SET status = 'done', finished_at = ?, exit_code = ? WHERE id = ?",
		time.Now(), exitCode, id)

	os.Remove(jobScriptPath(int64(id)))

	notifyDone(int64(id), exitCode)

	// Start the next pending job
	startNextJob()
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
	if err := db.QueryRow("SELECT command, args FROM "+tableName()+" WHERE id = ?", id).Scan(&command, &argsJSON); err != nil {
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
	err = db.QueryRow("SELECT log FROM "+tableName()+" WHERE id = ?", id).Scan(&log)
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
	db.Exec("UPDATE "+tableName()+" SET status = 'killed' WHERE id = ?", id)

	// Start next pending job since the running one was killed
	startNextJob()
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

	startNextJob()
}

func cmdList() {
	db := openDB()
	defer db.Close()

	rows, err := db.Query(
		"SELECT id, command, args, workdir, status, pid, exit_code FROM " + tableName() + " ORDER BY id DESC LIMIT 20",
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

	_, err := db.Exec("DELETE FROM " + tableName())
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
	err = db.QueryRow("SELECT status FROM "+tableName()+" WHERE id = ?", id).Scan(&status)
	if err == sql.ErrNoRows {
		fmt.Fprintf(os.Stderr, "job not found: %d\n", id)
		os.Exit(1)
	}
	if status == "running" {
		fmt.Fprintf(os.Stderr, "cannot remove running job %d, kill it first\n", id)
		os.Exit(1)
	}

	db.Exec("DELETE FROM "+tableName()+" WHERE id = ?", id)
	fmt.Fprintf(os.Stderr, "removed: [%d]\n", id)
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
