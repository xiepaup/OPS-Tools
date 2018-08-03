package db

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"time"
)

type RedisContext struct {
	Addr     string
	Db       int
	Password string
	timeout  time.Duration
	Conn     net.Conn
}

func NewRedis(addr, pass string) *RedisContext {
	rc := &RedisContext{
		Addr:     addr,
		Password: pass,
		Db:       0,
	}

	return rc
}

type RedisError string

func (err RedisError) Error() string { return "Redis Error: " + string(err) }

var doesNotExist = RedisError("Key does not exist ")

// reads a bulk reply (i.e $5\r\nhello)
func readBulk(reader *bufio.Reader, head string) ([]byte, error) {
	var err error
	var data []byte

	if head == "" {
		head, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
	}
	switch head[0] {
	case ':':
		data = []byte(strings.TrimSpace(head[1:]))

	case '$':
		size, err := strconv.Atoi(strings.TrimSpace(head[1:]))
		if err != nil {
			return nil, err
		}
		if size == -1 {
			return nil, doesNotExist
		}
		lr := io.LimitReader(reader, int64(size))
		data, err = ioutil.ReadAll(lr)
		if err == nil {
			// read end of line
			_, err = reader.ReadString('\n')
		}
	default:
		return nil, RedisError("Expecting Prefix '$' or ':'")
	}

	return data, err
}

func writeRequest(writer io.Writer, cmd string, args ...string) error {
	b := commandBytes(cmd, args...)
	_, err := writer.Write(b)
	return err
}

func commandBytes(cmd string, args ...string) []byte {
	var cmdbuf bytes.Buffer
	fmt.Fprintf(&cmdbuf, "*%d\r\n$%d\r\n%s\r\n", len(args)+1, len(cmd), cmd)
	for _, s := range args {
		fmt.Fprintf(&cmdbuf, "$%d\r\n%s\r\n", len(s), s)
	}
	return cmdbuf.Bytes()
}

func readResponse(reader *bufio.Reader) (interface{}, error) {

	var line string
	var err error

	//read until the first non-whitespace line
	for {
		line, err = reader.ReadString('\n')
		if len(line) == 0 || err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)
		if len(line) > 0 {
			break
		}
	}

	if line[0] == '+' {
		return strings.TrimSpace(line[1:]), nil
	}

	if strings.HasPrefix(line, "-ERR ") {
		errmesg := strings.TrimSpace(line[5:])
		return nil, RedisError(errmesg)
	}

	if line[0] == ':' {
		n, err := strconv.ParseInt(strings.TrimSpace(line[1:]), 10, 64)
		if err != nil {
			return nil, RedisError("Int reply is not a number")
		}
		return n, nil
	}

	if line[0] == '*' {
		size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return nil, RedisError("MultiBulk reply expected a number")
		}
		if size <= 0 {
			return make([][]byte, 0), nil
		}
		res := make([][]byte, size)
		for i := 0; i < size; i++ {
			res[i], err = readBulk(reader, "")
			if err == doesNotExist {
				continue
			}
			if err != nil {
				return nil, err
			}
			// dont read end of line as might not have been bulk
		}
		return res, nil
	}
	return readBulk(reader, line)
}

func (this *RedisContext) rawSend(c net.Conn, cmd []byte) (interface{}, error) {
	_, err := c.Write(cmd)
	//defer c.Close()
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(c)

	data, err := readResponse(reader)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (this *RedisContext) openConnection() (c net.Conn, err error) {

	c, err = net.DialTimeout("tcp", this.Addr, this.timeout)
	if err != nil {
		return nil, err
	}

	if this.Password != "" {
		cmd := fmt.Sprintf("AUTH %s\r\n", this.Password)
		_, err = this.rawSend(c, []byte(cmd))
		if err != nil {
			return nil, err
		}
	}

	if this.Db != 0 {
		cmd := fmt.Sprintf("SELECT %d\r\n", this.Db)
		_, err = this.rawSend(c, []byte(cmd))
		if err != nil {
			return nil, err
		}
	}
	this.Conn = c
	return c, nil
}

func (this *RedisContext) sendCommand(cmd string, args ...string) (data interface{}, err error) {
	// grab a connection from the pool
	var b []byte

	c, err := this.openConnection()
	if err != nil {
		//println(err.Error())
		goto End
	}

	b = commandBytes(cmd, args...)
	data, err = this.rawSend(c, b)
	if err == io.EOF {
		c, err = this.openConnection()
		if err != nil {
			println(err.Error())
			goto End
		}
		data, err = this.rawSend(c, b)
	}

End:

	return data, err
}

func (this *RedisContext) sendCmdMonitor() (net.Conn, error) {
	c, err := this.openConnection()

	if err != nil {
		return nil, err
	}

	err = writeRequest(c, "MONITOR")
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (this *RedisContext) sendCommands(cmdArgs <-chan []string, data chan<- interface{}) (err error) {
	// grab a connection from the pool
	c, err := this.openConnection()
	var reader *bufio.Reader
	var pong interface{}
	var errs chan error
	var errsClosed = false

	if err != nil {
		goto End
	}

	reader = bufio.NewReader(c)

	// Ping first to verify connection is open
	err = writeRequest(c, "PING")

	// On first attempt permit a reconnection attempt
	if err == io.EOF {
		// Looks like we have to open a new connection
		c, err = this.openConnection()
		if err != nil {
			goto End
		}
		reader = bufio.NewReader(c)
		reader.ReadLine()
	} else {
		// Read Ping response
		pong, err = readResponse(reader)
		if pong != "PONG" {
			return RedisError("Unexpected response to PING.")
		}
		if err != nil {
			goto End
		}
	}

	errs = make(chan error)

	go func() {
		for cmdArg := range cmdArgs {
			err = writeRequest(c, cmdArg[0], cmdArg[1:]...)
			if err != nil {
				if !errsClosed {
					errs <- err
				}
				break
			}
		}
		if !errsClosed {
			errsClosed = true
			close(errs)
		}
	}()

	go func() {
		for {
			response, err := readResponse(reader)
			if err != nil {
				if !errsClosed {
					errs <- err
				}
				break
			}
			data <- response
		}
		if !errsClosed {
			errsClosed = true
			close(errs)
		}
	}()

	// Block until errs channel closes
	for e := range errs {
		err = e
	}

End:

	// Close client and synchronization issues are a nightmare to solve.
	c.Close()

	return err
}

func (this *RedisContext) Auth(password string) error {
	_, err := this.sendCommand("AUTH", password)
	if err != nil {
		return err
	}

	return nil
}

func (this *RedisContext) Type(key string) (string, error) {
	res, err := this.sendCommand("TYPE", key)

	if err != nil {
		return "", err
	}

	return res.(string), nil
}

func (this *RedisContext) Ttl(key string) (int64, error) {
	res, err := this.sendCommand("TTL", key)
	if err != nil {
		return -1, err
	}

	return res.(int64), nil
}

func (this *RedisContext) Set(key string, val []byte) error {
	_, err := this.sendCommand("SET", key, string(val))

	if err != nil {
		return err
	}

	return nil
}

func (this *RedisContext) Get(key string) ([]byte, error) {
	res, _ := this.sendCommand("GET", key)
	if res == nil {
		return nil, RedisError("Key `" + key + "` does not exist")
	}

	data := res.([]byte)
	return data, nil
}

func (this *RedisContext) GetKeyByDbN(db int, key string) ([]byte, error) {
	this.Db = db
	return this.Get(key)
}

func (this *RedisContext) Info(args ...string) ([]byte, error) {
	res, _ := this.sendCommand("INFO", args...)
	if res == nil {
		return nil, RedisError("info failed !")
	}

	data := res.([]byte)
	return data, nil
}

func (this *RedisContext) CmdStatShow(args ...string) ([]byte, error) {
	res, _ := this.sendCommand("CMDSTAT", args...)
	if res == nil {
		return nil, RedisError("cmdstat failed !")
	}

	data := res.([]byte)
	return data, nil
}

func (this *RedisContext) Monitor(dchan chan interface{}, stopChan chan struct{}) error {
	c, err := this.sendCmdMonitor()
	if err != nil {
		return err
	}

	reader := bufio.NewReader(c)

	go func() {
		defer c.Close()
		for {
			select {
			case <-stopChan:
				break
			default:
				response, err := readResponse(reader)
				if err != nil {
					stopChan <- struct{}{}
					break
				}
				dchan <- response
			}
		}
	}()
	return nil
}
