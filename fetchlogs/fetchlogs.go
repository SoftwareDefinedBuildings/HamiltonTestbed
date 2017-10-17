package main

import (
    "fmt"
    "os"
    "time"
    "strings"

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
    var date string
    form := "01/02/2006"
    curDate := time.Now().Format(form)
    app.Flags = []cli.Flag {
    cli.StringFlag {
        Name: "date, d",
        Value: curDate,
        Usage: `date or date range to view logs, mm/dd/yyyy format for a single date,
            mm/dd/yyyy-mm/dd/yyyy format for date range, inclusive of end date`,
        Destination: &date,
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
        dates := strings.Split(date, "-")
        if len(dates) < 1 || len(dates) > 2{
            fmt.Println("Invalid date range")
            return nil
        }
        startDate, endDate := dates[0], dates[0]
        if len(dates) == 2 {
            endDate = dates[1]
        }

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
        startMonth := startDay.Unix() / (60*60*24*30)
        endMonth := endDay.Unix() / (60*60*24*30)

        // loop through partition keys in the date range
        for i := startMonth; i <= endMonth; i++ {

            partitionKey := fmt.Sprintf("%s.%d.dockerlogs", nodeID, i)

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
            if len(result.Items) != 0 {
                tb := TestbedData{}
                for _, element := range result.Items {
                    dynamodbattribute.Unmarshal(element["dockerlogs"], &tb)
                    fmt.Println(tb.Data);
                }
            }
        }

    return nil
  }

  app.Run(os.Args)
}