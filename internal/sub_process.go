package internal

import (
	"io"
	"log"
	"os/exec"
	"bufio"
	"strings"
	"fmt"
)

type SubProcess struct{
	cmdStr string
	id    string
	cmd *exec.Cmd
	reader *bufio.Reader
	writer io.WriteCloser
}

func NewSubProcess(cmdStr string,id string) (*SubProcess,error){
	cmdStr = strings.TrimSpace(cmdStr)
	if cmdStr == ""{
		return nil,fmt.Errorf("call NewSubProcess with empty command line,id=",id)
	}
	task:=&SubProcess{
		cmdStr:cmdStr,
		id  :id,
	}
	err:=task.start()
	return task,err
}

func (task *SubProcess)log(msg ... interface{} ){
	s:=fmt.Sprintf("SubProcess id=%s cmd=[%s]", task.id, task.cmdStr)
	sl:=fmt.Sprint(msg...)
	log.Println(s,sl)
}

func (task *SubProcess)start()(err error){
	task.log("starting")
	defer func(){
		if err==nil{
			task.log("started success")
		}else{
			task.log("started failed")
		}
	}()
	cmd := exec.Command("sh","-c",task.cmdStr)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	
	stdout,err:=cmd.StdoutPipe()
	if err != nil {
		return err
	}
	
	erout,err:=cmd.StderrPipe()
	if err != nil {
		return err
	}
	
	go func(){
		defer erout.Close()
		berr:=bufio.NewReader(erout)
		for{
			l,e:=berr.ReadString('\n')
			task.log("cmd_stderr:",strings.TrimSpace(l),e)
			if cmd.ProcessState==nil ||  cmd.ProcessState.Exited(){
				task.log("subprocess is Exited")
				break
			}
		}
	}()
	
	err=cmd.Start()
	task.cmd = cmd
	task.reader = bufio.NewReader(stdout)
	task.writer = stdin
	return err
}

func (task *SubProcess)processExixts()bool{
	return task.cmd.ProcessState!=nil && !task.cmd.ProcessState.Exited()
}



func (task *SubProcess)Deal(str string)(ret string,err error){
	str = strings.Trim(str, "\n")
	write:
		_,err=io.WriteString(task.writer, str+"\n")
		if err!=nil{
			task.log("write error:",err)
			if !task.processExixts(){
				task.start()
				goto write
			}
			return "",err
		}
		resp,err:=task.reader.ReadString('\n')
		if err!=nil{
			task.log("read error:",err)
			return "",err
		}
	return strings.TrimSpace(resp),nil
}

func (task *SubProcess)Close()error{
	return task.writer.Close()
}
