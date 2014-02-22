package main

import (
	"flag"
	"log"
	"net"
	"os"

	"git.apache.org/thrift.git/lib/go/thrift"
)

var (
	addr          string
	unixSocket    string
	transportType string
	protocolType  string
)

func init() {
	flag.StringVar(&addr, "addr", ":9090", "The address to listen on")
	flag.StringVar(&unixSocket, "unix-socket", "", "The unix socket to listen on")
	flag.StringVar(&transportType, "transport", "buffered", "The transport to use")
	flag.StringVar(&protocolType, "protocol", "binary", "The protocol to use")
}

func main() {
	flag.Parse()

	var (
		protocolFactory  thrift.TProtocolFactory
		transportFactory thrift.TTransportFactory
		transport        thrift.TServerTransport
		processor        thrift.TProcessor
		listener         net.Listener
	)

	if unixSocket != "" {
		f, err := os.Open(unixSocket)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		listener, err = net.FileListener(f)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		var err error
		listener, err = net.Listen("tcp", addr)
		if err != nil {
			log.Fatal(err)
		}
	}

	transport, err := thrift.NewTServerSocketListener(listener, 0)
	if err != nil {
		log.Fatal(err)
	}

	switch transportType {
	case "buffered":
		transportFactory = thrift.NewTBufferedTransportFactory(8196)
	case "framed":
		transportFactory = thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	case "http":
		log.Fatal("HTTPServerTransport not supported")
	default:
		log.Fatalf("Not supported transport %q\n", transportType)
	}

	switch protocolType {
	case "binary":
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
	case "compact":
		protocolFactory = thrift.NewTCompactProtocolFactory()
	case "json":
		protocolFactory = thrift.NewTJSONProtocolFactory()
	default:
		log.Fatalf("Not supported protocol %q\n", protocolType)
	}

	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)
	log.Println("Listening...")
	server.Serve()
}
