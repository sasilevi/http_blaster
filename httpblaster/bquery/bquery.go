package bquery

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// Bquery : bquery return query result channel
type Bquery struct {
}

//Query : execute the query string on the project id
func (b *Bquery) Query(ctx context.Context, projectID, queryStr, certs string) chan interface{} {
	chItems := make(chan interface{}, 2000)
	go func() {
		defer close(chItems)
		var query *bigquery.Query
		client, err := bigquery.NewClient(ctx, projectID)

		if err != nil {
			panic(err)
		}
		query = client.Query(queryStr)

		it, err := query.Read(ctx)
		log.Println("Query is done")
		if err != nil {
			panic(err.Error())
		}

		log.Println(fmt.Sprintf("Total raws returned from query:%v", it.TotalRows))

		for {
			var values map[string]bigquery.Value
			err := it.Next(&values)
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Println(err.Error())
			}
			if values["user_agent"] != nil {
				chItems <- values["user_agent"].(string)
			} else {
				log.Println("recieved nil value from bigquery: ", values)
			}
		}
	}()
	return chItems
}
