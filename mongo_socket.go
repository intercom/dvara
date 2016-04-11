// mgo - MongoDB driver for Go
//
// Copyright (c) 2010-2012 - Gustavo Niemeyer <gustavo@niemeyer.net>
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package dvara

import (
	"fmt"
	"net"

	"gopkg.in/mgo.v2-unstable/bson"
)

type replyFunc func(err error, reply *replyOp, docNum int, docData []byte)

type mongoSocket struct {
	conn net.Conn
	addr string // For debugging only.
}

type queryOp struct {
	collection string
	query      interface{}
	skip       int32
	limit      int32
	selector   interface{}
	flags      uint32
	replyFunc  replyFunc
}

type replyOp struct {
	flags     uint32
	cursorId  int64
	firstDoc  int32
	replyDocs int32
}

func (socket *mongoSocket) kill(err error, abend bool) {
	fmt.Printf("WARN: Killing socket: %s, with error: %s, and abend:%s \n", socket, err, abend)
	socket.conn.Close()
}

func (socket *mongoSocket) Query(op *queryOp) (err error) {

	buf := make([]byte, 0, 256)
	start := 0
	buf = addHeader(buf, 2004)
	buf = addInt32(buf, int32(op.flags))
	buf = addCString(buf, op.collection)
	buf = addInt32(buf, op.skip)
	buf = addInt32(buf, op.limit)
	buf, err = addBSON(buf, op.query)
	if err != nil {
		return err
	}
	replyFunc := op.replyFunc
	setInt32(buf, start, int32(len(buf)-start))

	socket.conn.Write(buf)

	p := make([]byte, 36) // 16 from header + 20 from OP_REPLY fixed fields
	fill(socket.conn, p)

	fmt.Printf("%d", getInt32(p, 32))
	reply := replyOp{
		flags:     uint32(getInt32(p, 16)),
		cursorId:  getInt64(p, 20),
		firstDoc:  getInt32(p, 28),
		replyDocs: getInt32(p, 32),
	}

	if replyFunc != nil && reply.replyDocs == 0 {
		replyFunc(nil, &reply, -1, nil)
	} else {
		s := make([]byte, 4)
		for i := 0; i != int(reply.replyDocs); i++ {
			err = fill(socket.conn, s)
			if err != nil {
				if replyFunc != nil {
					replyFunc(err, nil, -1, nil)
				}
				socket.kill(err, true)
				return
			}

			b := make([]byte, int(getInt32(s, 0)))

			// copy(b, s) in an efficient way.
			b[0] = s[0]
			b[1] = s[1]
			b[2] = s[2]
			b[3] = s[3]

			err = fill(socket.conn, b[4:])
			if err != nil {
				if replyFunc != nil {
					replyFunc(err, nil, -1, nil)
				}
				socket.kill(err, true)
				return
			}

			m := bson.M{}
			if err := bson.Unmarshal(b, m); err == nil {
				fmt.Printf("Socket %p to %s: received document: %#v\n", socket, socket.addr, m)
			}

			if replyFunc != nil {
				replyFunc(nil, &reply, i, b)
			}
			// XXX Do bound checking against totalLen.
		}
	}
	return err
}

func fill(r net.Conn, b []byte) error {
	l := len(b)
	n, err := r.Read(b)
	for n != l && err == nil {
		var ni int
		ni, err = r.Read(b[n:])
		n += ni
	}
	return err
}
