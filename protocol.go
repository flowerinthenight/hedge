package hedge

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
)

func doConfirmLeader(ctx context.Context, op *Op, conn net.Conn, _ string) {
	reply := op.buildAckReply(nil)
	if hl, _ := op.HasLock(); !hl {
		reply = "\n"
	}

	b := []byte(reply)
	conn.Write(b)
}

func doWrite(ctx context.Context, op *Op, conn net.Conn, msg string) {
	reply := op.buildAckReply(nil)
	if hl, _ := op.HasLock(); hl {
		ss := strings.Split(msg, " ")
		payload := ss[1]
		var noappend bool
		if len(ss) >= 3 {
			if ss[2] == FlagNoAppend {
				noappend = true
			}
		}

		decoded, _ := base64.StdEncoding.DecodeString(payload)
		var kv KeyValue
		err := json.Unmarshal(decoded, &kv)
		if err != nil {
			reply = op.buildAckReply(err)
		} else {
			reply = op.buildAckReply(op.Put(ctx, kv, PutOptions{
				DirectWrite: true,
				NoAppend:    noappend,
			}))
		}
	} else {
		reply = "\n" // not leader, possible even if previously confirmed
	}

	b := []byte(reply)
	conn.Write(b)
}

func doSend(ctx context.Context, op *Op, conn net.Conn, msg string) {
	reply := base64.StdEncoding.EncodeToString([]byte(ErrNoLeader.Error())) + "\n"
	if hl, _ := op.HasLock(); hl {
		reply = base64.StdEncoding.EncodeToString([]byte(ErrNoHandler.Error())) + "\n"
		if op.fnLeader != nil {
			payload := strings.Split(msg, " ")[1]
			decoded, _ := base64.StdEncoding.DecodeString(payload)
			data := op.fnLdrData
			if data == nil {
				data = op
			}

			r, e := op.fnLeader(data, decoded) // call leader handler
			if e != nil {
				reply = base64.StdEncoding.EncodeToString([]byte(e.Error())) + "\n"
			} else {
				br := base64.StdEncoding.EncodeToString([]byte(""))
				if r != nil {
					br = base64.StdEncoding.EncodeToString(r)
				}

				// The final correct reply format.
				reply = fmt.Sprintf("%v %v\n", CmdAck, br)
			}
		}
	}

	b := []byte(reply)
	conn.Write(b)
}

func doBroadcast(ctx context.Context, op *Op, conn net.Conn, msg string) {
	reply := base64.StdEncoding.EncodeToString([]byte(ErrNoHandler.Error())) + "\n"
	if op.fnBroadcast != nil {
		payload := strings.Split(msg, " ")[1]
		decoded, _ := base64.StdEncoding.DecodeString(payload)
		data := op.fnBcData
		if data == nil {
			data = op
		}

		r, e := op.fnBroadcast(data, decoded) // call broadcast handler
		if e != nil {
			reply = base64.StdEncoding.EncodeToString([]byte(e.Error())) + "\n"
		} else {
			br := base64.StdEncoding.EncodeToString([]byte(""))
			if r != nil {
				br = base64.StdEncoding.EncodeToString(r)
			}

			// The final correct reply format.
			reply = fmt.Sprintf("%v %v\n", CmdAck, br)
		}
	}

	b := []byte(reply)
	conn.Write(b)
}

func doHeartbeat(ctx context.Context, op *Op, conn net.Conn, msg string) {
	op.addMember(strings.Split(msg, " ")[1])
	reply := op.encodeMembers() + "\n"
	conn.Write([]byte(reply))
}

func doMembers(ctx context.Context, op *Op, conn net.Conn, msg string) {
	payload := strings.Split(msg, " ")[1]
	decoded, _ := base64.StdEncoding.DecodeString(payload)
	var m map[string]struct{}
	json.Unmarshal(decoded, &m)
	m[op.hostPort] = struct{}{} // just to be sure
	op.setMembers(m)            // then replace my records
	members := op.getMembers()
	mlist := []string{}
	for k := range members {
		mlist = append(mlist, k)
	}

	op.logger.Printf("members=%v, list=%v",
		len(op.getMembers()), strings.Join(mlist, ","))

	reply := op.buildAckReply(nil)
	conn.Write([]byte(reply))
}

func doCreateSemaphore(ctx context.Context, op *Op, conn net.Conn, msg string) {
	reply := op.buildAckReply(nil)
	func() {
		op.mtxSem.Lock()
		defer op.mtxSem.Unlock()
		ss := strings.Split(msg, " ")
		name, slimit, caller := ss[1], ss[2], ss[3]
		limit, err := strconv.Atoi(slimit)
		if err != nil {
			reply = op.buildAckReply(err)
			return
		}

		// See if this semaphore already exists.
		s, err := readSemaphoreEntry(ctx, op, name)
		if err != nil {
			err = createSemaphoreEntry(ctx, op, name, caller, limit)
			if err != nil {
				reply = op.buildAckReply(err)
				return
			}

			// Read again after create.
			s, err = readSemaphoreEntry(ctx, op, name)
			if err != nil {
				reply = op.buildAckReply(err)
				return
			}
		}

		slmt, _ := strconv.Atoi(strings.Split(s.Id, "=")[1])
		if slmt != limit {
			err = fmt.Errorf("semaphore already exists with a different limit")
			reply = op.buildAckReply(err)
			return
		}
	}()

	b := []byte(reply)
	conn.Write(b)
}

func doAcquireSemaphore(ctx context.Context, op *Op, conn net.Conn, msg string) {
	reply := op.buildAckReply(nil)
	func() {
		op.mtxSem.Lock()
		defer op.mtxSem.Unlock()
		ss := strings.Split(msg, " ")
		name, caller := ss[1], ss[2]
		go ensureLiveness(ctx, op)
		op.ensureCh <- name
		s, err := readSemaphoreEntry(ctx, op, name) // to get the current limit
		if err != nil {
			err = fmt.Errorf("0:%v", err) // final
			reply = op.buildAckReply(err)
			return
		}

		limit, _ := strconv.Atoi(strings.Split(s.Id, "=")[1])
		retry, err := createAcquireSemaphoreEntry(ctx, op, name, caller, limit)
		if err != nil {
			switch {
			case retry:
				err = fmt.Errorf("1:%v", err) // can retry
			default:
				err = fmt.Errorf("0:%v", err) // final
			}

			reply = op.buildAckReply(err)
			return
		}
	}()

	b := []byte(reply)
	conn.Write(b)
}

func doReleaseSemaphore(ctx context.Context, op *Op, conn net.Conn, msg string) {
	reply := op.buildAckReply(nil)
	func() {
		op.mtxSem.Lock()
		defer op.mtxSem.Unlock()
		ss := strings.Split(msg, " ")
		name, caller := ss[1], ss[2]
		s, err := readSemaphoreEntry(ctx, op, name) // to get the current limit
		if err != nil {
			reply = op.buildAckReply(err)
			return
		}

		limit, _ := strconv.Atoi(strings.Split(s.Id, "=")[1])
		err = releaseSemaphore(ctx, op, name, caller, s.Value, limit)
		if err != nil {
			reply = op.buildAckReply(err)
			return
		}
	}()

	b := []byte(reply)
	conn.Write(b)
}

func handleMsg(ctx context.Context, op *Op, conn net.Conn) {
	defer conn.Close()
	fns := map[string]func(ctx context.Context, op *Op, conn net.Conn, msg string){
		CmdLeader:           doConfirmLeader,    // confirm leader only
		CmdWrite + " ":      doWrite,            // actual write
		CmdSend + " ":       doSend,             // Send() API
		CmdBroadcast + " ":  doBroadcast,        // Broadcast() API
		CmdPing + " ":       doHeartbeat,        // heartbeat
		CmdMembers + " ":    doMembers,          // broadcast online members
		CmdSemaphore + " ":  doCreateSemaphore,  // create semaphore (we are leader)
		CmdSemAcquire + " ": doAcquireSemaphore, // acquire semaphore (we are leader)
		CmdSemRelease + " ": doReleaseSemaphore, // release semaphore (we are leader)
	}

	for {
		var prefix string
		msg, err := op.recv(conn)
		if err != nil || ctx.Err() != nil {
			return
		}

		switch {
		case msg == CmdPing: // leader asking if we are online (msg has no prefix)
			reply := op.buildAckReply(nil)
			conn.Write([]byte(reply))
			return
		case strings.HasPrefix(msg, CmdLeader):
			prefix = CmdLeader
		case strings.HasPrefix(msg, CmdWrite+" "):
			prefix = CmdLeader + " "
		case strings.HasPrefix(msg, CmdSend+" "):
			prefix = CmdSend + " "
		case strings.HasPrefix(msg, CmdBroadcast+" "):
			prefix = CmdBroadcast + " "
		case strings.HasPrefix(msg, CmdPing+" "):
			prefix = CmdPing + " "
		case strings.HasPrefix(msg, CmdMembers+" "):
			prefix = CmdMembers + " "
		case strings.HasPrefix(msg, CmdSemaphore+" "):
			prefix = CmdSemaphore + " "
		case strings.HasPrefix(msg, CmdSemAcquire+" "):
			prefix = CmdSemAcquire + " "
		case strings.HasPrefix(msg, CmdSemRelease+" "):
			prefix = CmdSemRelease + " "
		default:
			return // do nothing
		}

		fns[prefix](ctx, op, conn, msg)
	}
}
