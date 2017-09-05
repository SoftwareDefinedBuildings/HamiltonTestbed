package main

import (
	"fmt"
	"time"
	"bytes"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/docker/docker/client"
	"github.com/docker/docker/api/types"

	"golang.org/x/net/context"
)

type TestbedEntry struct {
	Nodemonthcat string `json:"nodemonthcat"`
	Timestamp string `json:"timestamp"`
	Dockerlogs TestbedData `json:"dockerlogs"`
}

type TestbedData struct {
	ContainerID string `json:"containerID"`
	Data string `json:"data"`
}
func main() {
	// initialize connection to container
	// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// defer cancel()
	ctx := context.Background()
  	cli, err := client.NewEnvClient()
  	if err != nil {
  		panic(err)
  	}

  	// get container id
	containers, err := cli.ContainerList(ctx, types.ContainerListOptions{})
	if err != nil {
		panic(err)
	}

	// fmt.Println(containers[0].ID)
	containerID := containers[0].ID

	// connect to container and create container log reader
	reader, err := cli.ContainerLogs(ctx, containerID, types.ContainerLogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Timestamps: true,
			Details: true,
			Since: "30s",
		})
	defer reader.Close()

	if err != nil {
	    panic(err)
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(reader)
	dataBuf := new(bytes.Buffer)

	lines := strings.Split(buf.String(), "\n")
	for _, m := range lines[:len(lines)-1] {
		temp := []byte(m)
    	dataBuf.Write(temp[8:])
    	dataBuf.WriteString("\n")
	}	

	dataString := dataBuf.String()
	fmt.Println(dataString)
	
  	// connect to dynamo db
	svc := dynamodb.New(session.New(&aws.Config{
			Region: aws.String("us-west-1"),
		}))

	// create keys for testbed database
	nodeNum := 1 // hardcoded node number for testing

	month := time.Now().Unix() / (60*60*24*30)

	partitionKey := fmt.Sprintf("%d.%d.dockerlogs", nodeNum, month)

	sortKey := fmt.Sprintf("%d", time.Now().UnixNano())
	fmt.Println(partitionKey, sortKey)
	
	// create database entry
	tb := TestbedData {
		ContainerID: containerID,
		Data: dataString,
	}

	te := TestbedEntry {
		Nodemonthcat: partitionKey,
		Timestamp: sortKey,
		Dockerlogs: tb,
	}

	// put data into testbed db
	av, err := dynamodbattribute.MarshalMap(te)
	fmt.Println("err", err)
	fmt.Println(av)
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = svc.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String("testbed"),
		Item: av,
	})

	if err != nil {
		fmt.Println(err)
		return
	}

	// // retrieve records to confirm they were added
	// var records []TestbedEntry
	// err = svc.ScanPages(&dynamodb.ScanInput{
	//     TableName: aws.String("testbed"),
	// }, func(page *dynamodb.ScanOutput, last bool) bool {
	//     recs := []TestbedEntry{}

	//     err := dynamodbattribute.UnmarshalListOfMaps(page.Items, &recs)
	//     if err != nil {
	//          panic(fmt.Sprintf("failed to unmarshal Dynamodb Scan Items, %v", err))
	//     }

	//     records = append(records, recs...)

	//     return true // keep paging
	// })
	// fmt.Println(records);
}
