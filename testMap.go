package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	_ "github.com/lib/pq"
)

const (
	host = "38.242.143.122"
	port = 14065
)

// type User struct {
// 	name  string
// 	photo string
// }

// type Vacation struct {
// 	status string
// }

// type SickLeave struct {
// 	isViewed bool
// }

// type UserWithVacation struct {
// 	user     User
// 	vacation Vacation
// }

// type Row struct {
// 	name     string
// 	photo    string
// 	status   string
// 	isViewed bool
// }

type Row struct {
	Email  string `json:"email"`
	Name   string `json:"key"`
	Photo  string `json:"photo"`
	Status string `json:"status"`
	Viewed bool   `json:"viewed"`
}

func getQuery(db *sql.DB, offset int) (*sql.Rows, error) {
	return db.Query(
		"SELECT users.email, users.name, users.photo, vacations.status, sick_leaves.is_viewed FROM users INNER JOIN vacations ON users.id = vacations.user_id INNER JOIN sick_leaves ON users.id = sick_leaves.user_id LIMIT 10 OFFSET $1;", offset)
}

type DynamoDBWriter struct {
	dynamoDBClient *dynamodb.DynamoDB
}

func NewDynamoDBWriter() (*DynamoDBWriter, error) {
	//sess, err := session.NewSession(&aws.Config{
	//	Region: aws.String(os.Getenv("AWS_REGION")),
	//})
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-west-2"),
	})
	if err != nil {
		return nil, err
	}

	dynamoDBClient := dynamodb.New(sess)
	return &DynamoDBWriter{dynamoDBClient: dynamoDBClient}, nil
}

func (d *DynamoDBWriter) SaveMetrics(ctx context.Context, metrics []Row) error {

	fmt.Println(metrics)
	for _, metric := range metrics {

		fmt.Println(metric)
		item, err := dynamodbattribute.MarshalMap(metric)
		fmt.Println(item)
		if err != nil {
			return err
		}

		input := &dynamodb.PutItemInput{
			Item:      item,
			TableName: aws.String("therealyo--image-uploader-dynamodb"),
		}

		_, err = d.dynamoDBClient.PutItem(input)
		if err != nil {
			return err
		}
	}
	return nil
}

// func includes(arr []interface{}, el interface{}) bool {

// }

func main() {
	//connString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
	//	host, port, user, password, dbname,
	//)

	db, err := sql.Open("postgres", os.Getenv("POSTGRES_URL"))
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()

	dynamo, err := NewDynamoDBWriter()
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("here")
	i := 0
	for {
		res, err := getQuery(db, i*10)

		if !res.Next() {
			break
		}

		var rows []Row
		for res.Next() {
			var row Row

			err = res.Scan(&row.Email, &row.Name, &row.Photo, &row.Status, &row.Viewed)
			if err != nil {
				fmt.Println(err)
			}
			rows = append(rows, row)

		}

		fmt.Println(len(rows))
		dynamo.SaveMetrics(context.Background(), rows)

		i++
	}

	// res, err := getQuery(db, 10000)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// var rows []Row

	// fmt.Println(res.Next())
	// for res.Next() {
	// 	var row Row
	// 	err = res.Scan(&row.name, &row.photo, &row.status, &row.isViewed)

	// 	rows = append(rows, row)
	// }
	// // fmt.Println(len(rows))
	// for _, row := range rows {
	// 	fmt.Println(row)
	// }

}
