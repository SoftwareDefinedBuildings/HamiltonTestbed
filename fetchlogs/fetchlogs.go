package main

import (
    "fmt"
    "os"
    "time"

    "github.com/urfave/cli"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/aws/awserr"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

)

type TestbedData struct {
    ContainerID string `json:"containerID"`
    Data string `json:"data"`
}

func main() {
    app := cli.NewApp()
    app.Name = "fetchlogs"
    app.Usage = "fetches Hamilton testbed logs"
    app.ArgsUsage = "nodeID"

    // set date flags
    var startDate, endDate string
    form := "01/02/2006"
    curDate := time.Now().Format(form)
    app.Flags = []cli.Flag {
    cli.StringFlag{
          Name: "startDate, s",
          Value: "01/01/1970",
          Usage: "start date to view logs in mm/dd/yyyy format",
          Destination: &startDate,
        },
    cli.StringFlag{
          Name: "endDate, e",
          Value: curDate,
          Usage: "end date to view logs in mm/dd/yyyy format, inclusive of end date",
          Destination: &endDate,
        },
    }

    app.Action = func(c *cli.Context) error {
        nodeID := c.Args().Get(0)
        if nodeID == "" {
            fmt.Println("Error: must specify node ID")
            return nil
        }

        // connect to db
        svc := dynamodb.New(session.New(&aws.Config{
          Region: aws.String("us-west-1"),
        }))

        // set time filters
        startDay, e := time.Parse(form, startDate)
        if e != nil {
            fmt.Println(e.Error())
            return nil
        }
        endDay, e := time.Parse(form, endDate)
        if e != nil {
            fmt.Println(e.Error())
            return nil
        }

        // add one day to print all logs on the specified end date
        endDay = endDay.AddDate(0, 0, 1)
        fmt.Println(fmt.Sprintf("LOGS FOR NODEID %s BETWEEN %s AND %s", 
            nodeID, startDay.String(), endDay.String()))

        startTime := fmt.Sprintf("%d", startDay.UnixNano())
        endTime := fmt.Sprintf("%d", endDay.UnixNano())

        // query db
        tStr := "timestamp"
        month := time.Now().Unix() / (60*60*24*30)
        partitionKey := fmt.Sprintf("%s.%d.dockerlogs", nodeID, month)
        input := &dynamodb.QueryInput{
            ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
                ":v1": {
                    S: aws.String(partitionKey),
                },
                ":v2": {
                    S: aws.String(startTime),
                },
                ":v3": {
                    S: aws.String(endTime),
                },
            },
            ExpressionAttributeNames: map[string]*string{
                "#t": &tStr,
            },
            KeyConditionExpression: aws.String("nodemonthcat = :v1 AND #t between :v2 and :v3"),
            ProjectionExpression:   aws.String("dockerlogs"),
            TableName:              aws.String("testbed"),
        }

        result, err := svc.Query(input)
        if err != nil {
            if aerr, ok := err.(awserr.Error); ok {
                switch aerr.Code() {
                case dynamodb.ErrCodeProvisionedThroughputExceededException:
                    fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
                case dynamodb.ErrCodeResourceNotFoundException:
                    fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
                case dynamodb.ErrCodeInternalServerError:
                    fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
                default:
                    fmt.Println(aerr.Error())
                }
            } else {
                // Print the error, cast err to awserr.Error to get the Code and
                // Message from an error.
                fmt.Println(err.Error())
            }
            return nil
        }

        // print out results
        if len(result.Items) == 0 {
            fmt.Println("NO LOGS TO DISPLAY")
        } else {
            fmt.Println("BEGIN LOGS\n")
            tb := TestbedData{}
            for _, element := range result.Items {
                dynamodbattribute.Unmarshal(element["dockerlogs"], &tb)
                fmt.Println(tb.Data);
            }
            fmt.Println("END LOGS")
        }
        return nil
  }

  app.Run(os.Args)
}