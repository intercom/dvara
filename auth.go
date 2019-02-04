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
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"

	"gopkg.in/mgo.v2-unstable/bson"
)

// Credential holds details to authenticate with a MongoDB server.
type Credential struct {
	// Username and Password hold the basic details for authentication.
	// Password is optional with some authentication mechanisms.
	Username string
	Password string

	// Source is the database used to establish credentials and privileges
	// with a MongoDB server. Defaults to the default database provided
	// during dial, or "admin" if that was unset.
	Source string
}

type authCmd struct {
	Authenticate int

	Nonce string
	User  string
	Key   string
}

type authResult struct {
	ErrMsg string
	Ok     bool
}

type getNonceCmd struct {
	GetNonce int
}

type getNonceResult struct {
	Nonce string
	Err   string "$err"
	Code  int
}

func (socket *mongoSocket) getNonce() (nonce string, err error) {
	fmt.Printf("Socket %p to %s: requesting a new nonce\n", socket, socket.addr)
	op := &queryOp{}
	op.query = &getNonceCmd{GetNonce: 1}
	op.collection = "admin.$cmd"
	op.limit = -1
	op.replyFunc = func(err error, reply *replyOp, docNum int, docData []byte) {
		if err != nil {
			socket.kill(errors.New("getNonce: "+err.Error()), true)
			return
		}
		result := &getNonceResult{}
		err = bson.Unmarshal(docData, &result)
		if err != nil {
			socket.kill(errors.New("Failed to unmarshal nonce: "+err.Error()), true)
			return
		}
		fmt.Printf("Socket %p to %s: nonce unmarshalled: %#v\n", socket, socket.addr, result)
		if result.Code == 13390 {
			// mongos doesn't yet support auth (see http://j.mp/mongos-auth)
			result.Nonce = "mongos"
		} else if result.Nonce == "" {
			var msg string
			if result.Err != "" {
				msg = fmt.Sprintf("Got an empty nonce: %s (%d)", result.Err, result.Code)
			} else {
				msg = "Got an empty nonce"
			}
			fmt.Println(msg)
			err = errors.New(msg)
			socket.kill(errors.New(msg), true)
			return
		}
		nonce = result.Nonce
	}

	err = socket.Query(op)
	if err != nil {
		socket.kill(errors.New("resetNonce: "+err.Error()), true)
	}
	return
}

func (socket *mongoSocket) Login(cred Credential) error {
	nonce, err := socket.getNonce()
	if err != nil {
		return err
	}

	psum := md5.New()
	psum.Write([]byte(cred.Username + ":mongo:" + cred.Password))

	ksum := md5.New()
	ksum.Write([]byte(nonce + cred.Username))
	ksum.Write([]byte(hex.EncodeToString(psum.Sum(nil))))

	key := hex.EncodeToString(ksum.Sum(nil))

	cmd := authCmd{Authenticate: 1, User: cred.Username, Nonce: nonce, Key: key}
	fmt.Printf("Trying to login with nonce:%s \n", nonce)
	res := authResult{}
	op := queryOp{}
	op.query = &cmd
	op.collection = cred.Source + ".$cmd"
	op.limit = -1
	op.replyFunc = func(err error, reply *replyOp, docNum int, docData []byte) {
		err = bson.Unmarshal(docData, &res)
		if err != nil {
			return
		}
	}
	err = socket.Query(&op)
	if err != nil {
		return err
	}
	if !res.Ok {
		return errors.New(res.ErrMsg)
	}
	return err
}
