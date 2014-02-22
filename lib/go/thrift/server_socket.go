/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package thrift

import (
	"net"
	"time"
)

type TServerSocket struct {
	listener      net.Listener
	clientTimeout time.Duration
	interrupted   bool
}

func NewTServerSocket(listenAddr string) (*TServerSocket, error) {
	return NewTServerSocketTimeout(listenAddr, 0)
}

func NewTServerSocketTimeout(listenAddr string, clientTimeout time.Duration) (*TServerSocket, error) {
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	return NewTServerSocketListener(l, clientTimeout)
}

func NewTServerSocketListener(l net.Listener, clientTimeout time.Duration) (*TServerSocket, error) {
	return &TServerSocket{listener: l, clientTimeout: clientTimeout}, nil
}

func (p *TServerSocket) Listen() error {
	if p.IsListening() {
		return nil
	}

	return nil
}

func (p *TServerSocket) Accept() (TTransport, error) {
	if p.interrupted {
		return nil, errTransportInterrupted
	}

	if p.listener == nil {
		return nil, NewTTransportException(NOT_OPEN, "No underlying server socket")
	}

	conn, err := p.listener.Accept()
	if err != nil {
		return nil, NewTTransportExceptionFromError(err)
	}

	return NewTSocketFromConnTimeout(conn, p.clientTimeout), nil
}

// Checks whether the socket is listening.
func (p *TServerSocket) IsListening() bool {
	return p.listener != nil
}

// Connects the socket, creating a new socket object if necessary.
func (p *TServerSocket) Open() error {
	if p.IsListening() {
		return NewTTransportException(ALREADY_OPEN, "Server socket already open")
	}

	// The socket should already be listening here

	return nil
}

func (p *TServerSocket) Addr() net.Addr {
	return p.listener.Addr()
}

func (p *TServerSocket) Close() error {
	defer func() {
		p.listener = nil
	}()
	if p.IsListening() {
		return p.listener.Close()
	}
	return nil
}

func (p *TServerSocket) Interrupt() error {
	p.interrupted = true
	return nil
}
