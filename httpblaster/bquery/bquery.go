package bquery

import (
	"context"
	"log"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type Bquery struct {
}

//Query : execute the query string on the project id
func (b *Bquery) Query(ctx context.Context, projectID, queryStr, platform, certs string) chan interface{} {
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
				// v := values["user_agent"].(string)
				// message := &dto.UserAgentMessage{UserAgent: v, Platform: platform}
				chItems <- values["user_agent"].(string)
			} else {
				log.Println("recieved nil value from bigquery: ", values)
			}
		}
	}()
	return chItems
}
