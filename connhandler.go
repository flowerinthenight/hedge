package hedge

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strings"
)

func handleConn(ctx context.Context, o *Op, conn net.Conn) {
	defer conn.Close()
	for {
		msg, err := o.recv(conn)
		if err != nil {
			o.logger.Printf("recv failed: %v", err)
			return
		}

		switch {
		case strings.HasPrefix(msg, CmdLeader): // confirm leader only
			reply := o.buildAckReply(nil)
			if hl, _ := o.HasLock(); !hl {
				reply = "\n"
			}

			conn.Write([]byte(reply))
			return
		case strings.HasPrefix(msg, CmdWrite+" "): // actual write
			reply := o.buildAckReply(nil)
			if hl, _ := o.HasLock(); hl {
				payload := strings.Split(msg, " ")[1]
				decoded, _ := base64.StdEncoding.DecodeString(payload)
				var kv KeyValue
				err = json.Unmarshal(decoded, &kv)
				if err != nil {
					reply = o.buildAckReply(err)
				} else {
					err = o.Put(ctx, kv, true)
					reply = o.buildAckReply(err)
				}
			} else {
				reply = "\n" // not leader, possible even if previously confirmed
			}

			conn.Write([]byte(reply))
			return
		case strings.HasPrefix(msg, CmdSend+" "): // Send(...) API
			reply := base64.StdEncoding.EncodeToString([]byte(ErrNoLeader.Error())) + "\n"
			if hl, _ := o.HasLock(); hl {
				reply = base64.StdEncoding.EncodeToString([]byte(ErrNoHandler.Error())) + "\n"
				if o.fnLeader != nil {
					payload := strings.Split(msg, " ")[1]
					decoded, _ := base64.StdEncoding.DecodeString(payload)
					r, e := o.fnLeader(o.fnLdrData, decoded) // call leader handler
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
			if o.fnBroadcast != nil {
				payload := strings.Split(msg, " ")[1]
				decoded, _ := base64.StdEncoding.DecodeString(payload)
				r, e := o.fnBroadcast(o.fnBcData, decoded) // call broadcast handler
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
			reply := o.buildAckReply(nil)
			conn.Write([]byte(reply))
			return
		case strings.HasPrefix(msg, CmdPing+" "): // heartbeat
			o.addMember(strings.Split(msg, " ")[1])
			reply := o.encodeMembers() + "\n"
			conn.Write([]byte(reply))
			return
		case strings.HasPrefix(msg, CmdMembers+" "): // broadcast online members
			payload := strings.Split(msg, " ")[1]
			decoded, _ := base64.StdEncoding.DecodeString(payload)
			var m map[string]struct{}
			json.Unmarshal(decoded, &m)
			m[o.hostPort] = struct{}{} // just to be sure
			o.setMembers(m)            // then replace my records
			o.logger.Printf("members=%v", len(o.getMembers()))
			reply := o.buildAckReply(nil)
			conn.Write([]byte(reply))
			return
		default:
			return // close conn
		}
	}
}
