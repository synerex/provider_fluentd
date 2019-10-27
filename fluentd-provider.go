package main

//go:generate protoc -I ../../proto/fluentd/ --go_out=paths=source_relative:../../proto/fluentd/ fluentd.proto

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"

	"github.com/golang/protobuf/proto"
	fluentd "github.com/synerex/proto_fluentd"
	pb "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	idlist          []uint64
	spMap           map[uint64]*sxutil.SupplyOpts
	mu              sync.Mutex
	sxServerAddress string
)

type Channel struct {
	Channel string `json:"channel"`
}

type MyFleet struct {
	VehicleId  int                    `json:"vehicle_id"`
	Status     int                    `json:"status"`
	Coord      map[string]interface{} `json:"coord"`
	Angle      float32                `json:"angle"`
	Speed      int                    `json:"speed"`
	MyServices map[string]interface{} `json"services"`
	Demands    []int                  `json:"demands"`
}

type MyVehicle struct {
	vehicles []*MyFleet `json:"vehicles"`
}

type MyJson map[string]interface{}

func init() {
	idlist = make([]uint64, 0)
	spMap = make(map[uint64]*sxutil.SupplyOpts)
}

// callback for each Supply
func supplyCallback(clt *sxutil.SXServiceClient, sp *pb.Supply) {
	// check if demand is match with my supply.
	log.Println("Got Fluentd Supply callback")

	record := &fluentd.FluentdRecord{}
	err := proto.Unmarshal(sp.Cdata.Entity, record)

	if err == nil {
		log.Println("Got record:", record.Tag, record.Time, record.Record)
	}

}

func subscribeSupply(client *sxutil.SXServiceClient) {
	// goroutine!
	ctx := context.Background() //
	client.SubscribeSupply(ctx, supplyCallback)
	// comes here if channel closed
	log.Printf("Server closed... on fluentd provider")
}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{pbase.FLUENTD_SERVICE}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNode(*nodesrv, "Fluentd-Fleet-Provider", channelTypes, nil)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	wg := sync.WaitGroup{} // for syncing other goroutines
	sxServerAddress = srv
	client := sxutil.GrpcConnectServer(srv)
	argJson := fmt.Sprintf("{Client:Fluentd}")
	sclient := sxutil.NewSXServiceClient(client, pbase.FLUENTD_SERVICE, argJson)

	wg.Add(1)
	go subscribeSupply(sclient)

	wg.Wait()
	sxutil.CallDeferFunctions() // cleanup!

}
