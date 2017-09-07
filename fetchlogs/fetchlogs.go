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
)

func main() {
    app := cli.NewApp()
    app.Name = "fetchlogs"
    app.Usage = "fetches Hamilton testbed logs"
    app.ArgsUsage = "nodeID"
    app.Action = func(c *cli.Context) error {
        nodeID := c.Args().Get(0)
        if nodeID == "" {
            fmt.Println("must specify node ID")
          return nil
        }

        // connect to db
        svc := dynamodb.New(session.New(&aws.Config{
          Region: aws.String("us-west-1"),
        }))

        // query db
        month := time.Now().Unix() / (60*60*24*30)
        partitionKey := fmt.Sprintf("%s.%s.dockerlogs", nodeID, month)
        input := &dynamodb.QueryInput{
            ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
                ":v1": {
                    S: aws.String(partitionKey),
                },
            },
            KeyConditionExpression: aws.String("Nodemonthcat = :v1"),
            ProjectionExpression:   aws.String("Timestamp"),
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
        fmt.Println(result);
        return nil
  }

  app.Run(os.Args)
}