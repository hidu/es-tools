package internal

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strings"
	"time"
)

// SubProcess 独立子进程，用于调用外部程序去处理数据
type SubProcess struct {
	cmdStr string
	id     string
	cmd    *exec.Cmd
	reader *bufio.Reader
	writer io.WriteCloser
}

// NewSubProcess 创建一个新的子进程
func NewSubProcess(cmdStr string, id string) (*SubProcess, error) {
	cmdStr = strings.TrimSpace(cmdStr)
	if cmdStr == "" {
		return nil, fmt.Errorf("call NewSubProcess with empty command line,id=%s", id)
	}
	task := &SubProcess{
		cmdStr: cmdStr,
		id:     id,
	}
	var err error
	for {
		err = task.start()
		if err == nil {
			break
		}
		time.Sleep(3 * time.Second)
	}
	return task, err
}

func (task *SubProcess) log(msg ...interface{}) {
	s := fmt.Sprintf("SubProcess id=%s cmd=[%s]", task.id, task.cmdStr)
	sl := fmt.Sprint(msg...)
	log.Println(s, sl)
}

func (task *SubProcess) start() (err error) {
	task.log("starting")

	if task.cmd != nil && task.cmd.Process != nil {
		task.cmd.Process.Kill()
	}

	defer func() {
		if err == nil {
			task.log("started success")
		} else {
			task.log("started failed")
		}
	}()
	cmd := exec.Command("sh", "-c", task.cmdStr)

	var stdin io.WriteCloser
	stdin, err = cmd.StdinPipe()
	if err != nil {
		return err
	}
	var stdout io.ReadCloser
	stdout, err = cmd.StdoutPipe()
	if err != nil {
		return err
	}

	var errorReader io.ReadCloser

	errorReader, err = cmd.StderrPipe()
	if err != nil {
		return err
	}

	go func() {
		defer errorReader.Close()
		reader := bufio.NewReader(errorReader)
		for {
			l, e := reader.ReadString('\n')
			task.log("cmd_stderr:", strings.TrimSpace(l), e)
			if cmd.ProcessState == nil || cmd.ProcessState.Exited() {
				task.log("subprocess is Exited")
				break
			}
		}
	}()

	err = cmd.Start()
	task.cmd = cmd
	task.reader = bufio.NewReader(stdout)
	task.writer = stdin
	return err
}

func (task *SubProcess) processExists() bool {
	return task.cmd.ProcessState != nil && !task.cmd.ProcessState.Exited()
}

// Deal 处理数据
func (task *SubProcess) Deal(str string) (ret string, err error) {
	str = strings.Trim(str, "\n")
write:
	_, err = io.WriteString(task.writer, str+"\n")
	if err != nil {
		task.log("write error:", err)
		if !task.processExists() {
			task.start()
			goto write
		}
		return "", err
	}
	resp, err := task.reader.ReadString('\n')
	if err != nil {
		task.log("read error:", err)
		if !task.processExists() {
			task.start()
			goto write
		}
		return "", err
	}
	return strings.TrimSpace(resp), nil
}

// Close 子进程关闭
func (task *SubProcess) Close() error {
	return task.writer.Close()
}
