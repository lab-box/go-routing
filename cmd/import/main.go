package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

type CSVRecord struct {
	EdgeID          int64
	SourceNode      int64
	TargetNode      int64
	EdgeCost        float64
	EdgeReverseCost float64
	EdgeMode        string
	EdgeDistanceKM  float64
	ParseError      error
}

func main() {
	var (
		CSVfile = flag.String("CSV", "./files/edge_antwerpen_subgraph_car.csv", "The CSV input file")
	)
	f, err := os.Open(*CSVfile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	var csvData [] CSVRecord

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		var csvRecord CSVRecord

		for idx, value := range record {
			switch idx {
			case 0:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					log.Printf("Unexpected type in column %d\n", idx)
					csvRecord.ParseError = errors.New("incorrect field")
					break
				}
				csvRecord.EdgeID = intValue
				continue
			case 1:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					log.Printf("Unexpected type in column %d\n", idx)
					csvRecord.ParseError = errors.New("incorrect field")
					break
				}
				csvRecord.SourceNode = intValue
				continue
			case 2:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					log.Printf("Unexpected type in column %d\n", idx)
					csvRecord.ParseError = errors.New("incorrect field")
					break
				}
				csvRecord.TargetNode = intValue
				continue
			case 3:
				intValue, err := strconv.ParseFloat(value, 64)
				if err != nil {
					log.Printf("Unexpected type in column %d\n", idx)
					csvRecord.ParseError = errors.New("incorrect field")
					break
				}
				csvRecord.EdgeCost = intValue
				continue
			case 4:
				intValue, err := strconv.ParseFloat(value, 64)
				if err != nil {
					log.Printf("Unexpected type in column %d\n", idx)
					csvRecord.ParseError = errors.New("incorrect field")
					break
				}
				csvRecord.EdgeReverseCost = intValue
				continue
			case 5:
				if value == "" {
					log.Printf("Unexpected type in column %d\n", idx)
					csvRecord.ParseError = fmt.Errorf("empty edge mode value")
					break
				}
				csvRecord.EdgeMode = value
				continue
			case 6:
				intValue, err := strconv.ParseFloat(value, 64)
				if err != nil {
					log.Printf("Unexpected type in column %d\n", idx)
					csvRecord.ParseError = errors.New("incorrect field")
					break
				}
				csvRecord.EdgeDistanceKM = intValue
				continue
			}
		}

		if csvRecord.ParseError == nil {
			csvData = append(csvData, csvRecord)
			if csvRecord.EdgeDistanceKM <= 0 {
				log.Println("No distance")
			}
			if csvRecord.EdgeCost <= 0 {
				log.Println("No cost")
			}
			if csvRecord.EdgeReverseCost <= 0 {
				log.Println("No reverse Edge")
			}
		}
	}
	log.Printf("Number of nodes %d", len(csvData))

	conn, err := grpc.Dial("127.0.0.1:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal("While trying to dial gRPC")
	}
	defer conn.Close()

	dc := api.NewDgraphClient(conn)
	dg := dgo.NewDgraphClient(dc)

	op := &api.Operation{}
	op.Schema = `
	node_id: int @index(int).
	edge_id: int @index(int).
	mode: string @index(exact).
	cost: float @index(float).
	distance: float @index(float).
	reverse: bool .
`

	ctx := context.Background()
	err = dg.Alter(ctx, op)
	if err != nil {
		log.Fatal(err)
	}

	mu := &api.Mutation{
		CommitNow: true,
	}
	var nodes []Node
	for _, csvRecord := range csvData {
		//For each record we create edge for one direction and onte for the reverse
		sourceNode := Node{
			ID: fmt.Sprintf("_:%d", csvRecord.SourceNode),
			NodeID:csvRecord.SourceNode,
			ConnectTo: Edge{
				TargetID: fmt.Sprintf("_:%d", csvRecord.TargetNode),
				NodeID:csvRecord.TargetNode,
				EdgeID:       csvRecord.EdgeID,
				Reverse:      false,
				Cost:         csvRecord.EdgeCost,
				Distance:     csvRecord.EdgeDistanceKM,
				Mode:         csvRecord.EdgeMode,
			},
		}
		nodes = append(nodes, sourceNode)

		if csvRecord.EdgeReverseCost > 0 {
			SourceNodeReverse := Node{
				ID: fmt.Sprintf("_:%d", csvRecord.TargetNode),
				NodeID:csvRecord.TargetNode,
				ConnectTo: Edge{
					TargetID: fmt.Sprintf("_:%d", csvRecord.SourceNode),
					NodeID:csvRecord.SourceNode,
					EdgeID:       csvRecord.EdgeID,
					Reverse:      true,
					Cost:         csvRecord.EdgeReverseCost,
					Distance:     csvRecord.EdgeDistanceKM,
					Mode:         csvRecord.EdgeMode,
				},
			}
			nodes = append(nodes, SourceNodeReverse)
		}
	}
	nodesJson, err := json.Marshal(nodes)
	if err != nil {
		log.Fatal(err)
	}

	mu.SetJson = nodesJson
	//_, err = dg.NewTxn().Mutate(ctx, mu)
	//if err != nil {
	//	log.Fatal(err)
	//}

	err = ioutil.WriteFile("output.json", nodesJson, 0644)
	if err != nil {
		log.Fatal(err)
	}
}

type Node struct {
	ID        string `json:"uid,omitempty"` //"_:SourceID"
	NodeID    int64  `json:"node_id,omitempty"`
	ConnectTo Edge   `json:"connect_to,omitempty"`
}

type Edge struct {
	TargetID string  `json:"uid,omitempty"` //"_:TargetID"
	NodeID   int64   `json:"node_id,omitempty"`
	EdgeID   int64   `json:"connect_to|edge_id,omitempty"`
	Reverse  bool    `json:"connect_to|reverse"`
	Cost     float64 `json:"connect_to|cost,omitempty"`
	Distance float64 `json:"connect_to|distance,omitempty"`
	Mode     string  `json:"connect_to|mode,omitempty"`
}
