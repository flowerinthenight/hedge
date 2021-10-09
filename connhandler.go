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

func handleConn(ctx context.Context, op *Op, conn net.Conn) {
	defer conn.Close()
	for {
		msg, err := op.recv(conn)
		if err != nil {
			op.logger.Printf("recv failed: %v", err)
			return
		}

		switch {
		case strings.HasPrefix(msg, CmdLeader): // confirm leader only
			reply := op.buildAckReply(nil)
			if hl, _ := op.HasLock(); !hl {
				reply = "\n"
			}

			conn.Write([]byte(reply))
			return
		case strings.HasPrefix(msg, CmdWrite+" "): // actual write
			reply := op.buildAckReply(nil)
			if hl, _ := op.HasLock(); hl {
				payload := strings.Split(msg, " ")[1]
				decoded, _ := base64.StdEncoding.DecodeString(payload)
				var kv KeyValue
				err = json.Unmarshal(decoded, &kv)
				if err != nil {
					reply = op.buildAckReply(err)
				} else {
					err = op.Put(ctx, kv, true)
					reply = op.buildAckReply(err)
				}
			} else {
				reply = "\n" // not leader, possible even if previously confirmed
			}

			conn.Write([]byte(reply))
			return
		case strings.HasPrefix(msg, CmdSend+" "): // Send(...) API
			reply := base64.StdEncoding.EncodeToString([]byte(ErrNoLeader.Error())) + "\n"
			if hl, _ := op.HasLock(); hl {
				reply = base64.StdEncoding.EncodeToString([]byte(ErrNoHandler.Error())) + "\n"
				if op.fnLeader != nil {
					payload := strings.Split(msg, " ")[1]
					decoded, _ := base64.StdEncoding.DecodeString(payload)
					r, e := op.fnLeader(op.fnLdrData, decoded) // call leader handler
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

			conn.Write([]byte(reply))
			return
		case strings.HasPrefix(msg, CmdBroadcast+" "): // Broadcast(...) API
			reply := base64.StdEncoding.EncodeToString([]byte(ErrNoHandler.Error())) + "\n"
			if op.fnBroadcast != nil {
				payload := strings.Split(msg, " ")[1]
				decoded, _ := base64.StdEncoding.DecodeString(payload)
				r, e := op.fnBroadcast(op.fnBcData, decoded) // call broadcast handler
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

			conn.Write([]byte(reply))
			return
		case msg == CmdPing: // leader asking if we are online
			reply := op.buildAckReply(nil)
			conn.Write([]byte(reply))
			return
		case strings.HasPrefix(msg, CmdPing+" "): // heartbeat
			op.addMember(strings.Split(msg, " ")[1])
			reply := op.encodeMembers() + "\n"
			conn.Write([]byte(reply))
			return
		case strings.HasPrefix(msg, CmdMembers+" "): // broadcast online members
			payload := strings.Split(msg, " ")[1]
			decoded, _ := base64.StdEncoding.DecodeString(payload)
			var m map[string]struct{}
			json.Unmarshal(decoded, &m)
			m[op.hostPort] = struct{}{} // just to be sure
			op.setMembers(m)            // then replace my records
			op.logger.Printf("members=%v", len(op.getMembers()))
			reply := op.buildAckReply(nil)
			conn.Write([]byte(reply))
			return
		case strings.HasPrefix(msg, CmdSemaphore+" "): // create semaphore; we are leader
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
					op.logger.Printf("readSemaphoreEntry failed: %v, attempt create", err)
					err = createSemaphoreEntry(ctx, op, name, caller, limit)
					if err != nil {
						op.logger.Printf("createSemaphoreEntry failed: %v", err)
						reply = op.buildAckReply(err)
						return
					}

					op.logger.Printf("semaphore created: name=%v, limit=%v, caller=%v",
						name, limit, caller)
					return
				}

				// See if limit matches.
				slmt, _ := strconv.Atoi(strings.Split(s.Id, "=")[1])
				if slmt != limit {
					err = fmt.Errorf("semaphore already exists with a different limit")
					reply = op.buildAckReply(err)
					return
				}

				op.logger.Printf("return existing semaphore: %v", s)
				return
			}()

			conn.Write([]byte(reply))
			return
		case strings.HasPrefix(msg, CmdSemAcquire+" "): // acquire semaphore; we are leader
			reply := op.buildAckReply(nil)
			func() {
				op.mtxSem.Lock()
				defer op.mtxSem.Unlock()
				ss := strings.Split(msg, " ")
				name, caller := ss[1], ss[2]
				s, err := readSemaphoreEntry(ctx, op, name) // to get the current limit
				if err != nil {
					op.logger.Printf("readSemaphoreEntry failed: %v, attempt create", err)
					err = fmt.Errorf("0:%v", err) // final
					reply = op.buildAckReply(err)
					return
				}

				limit, _ := strconv.Atoi(strings.Split(s.Id, "=")[1])
				retry, err := createAcquireSemaphoreEntry(ctx, op, name, caller, limit)
				if err != nil {
					op.logger.Printf("createAcquireSemaphoreEntry failed: %v, %v", retry, err)
					switch {
					case retry:
						err = fmt.Errorf("1:%v", err) // can retry
					default:
						err = fmt.Errorf("0:%v", err) // final
					}

					reply = op.buildAckReply(err)
					return
				}

				op.logger.Printf("semaphore acquired")
				return
			}()

			conn.Write([]byte(reply))
			return
		default:
			return // close conn
		}
	}
}
