package memqueue

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"

	"github.com/streadway/amqp"
	"github.com/v3io/http_blaster/httpblaster/dto"
)

//RabbitMQ rabbit mq obj
type RabbitMQ struct {
	queueName string
	server    string
	port      string
	user      string
}

//New : new rabbit mq obj
func New(queueName, server, port, user string) *RabbitMQ {
	return &RabbitMQ{
		queueName: queueName,
		server:    server,
		port:      port,
		user:      user,
	}
}

//NewClient : new client connection to rmq
func (rmq *RabbitMQ) NewClient() chan *dto.UserAgentMessage {
	chUsrAgent := make(chan *dto.UserAgentMessage)

	go func() {
		defer close(chUsrAgent)
		conn, ch, q := rmq.getQueue(rmq.queueName, rmq.server, rmq.port, rmq.user)
		defer conn.Close()
		defer ch.Close()

		msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)

		rmq.failOnError(err, "Failed to register consumer")

		for msg := range msgs {
			r := bytes.NewReader(msg.Body)
			dec := gob.NewDecoder(r)
			ua := dto.UserAgentMessage{}

			err = dec.Decode(&ua)
			if err != nil {
				panic(err.Error())
			}

			// log.Printf("Recieved message with message %s", msg.Body)
			chUsrAgent <- &ua
		}
		log.Println("No more messages")
	}()
	return chUsrAgent
}

func (rmq *RabbitMQ) getQueue(queueName, server, port, user string) (*amqp.Connection, *amqp.Channel, *amqp.Queue) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s@%s:%s", user, server, port))
	rmq.failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	rmq.failOnError(err, "Failed to open a Channel")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	rmq.failOnError(err, "fail to declear queue")
	return conn, ch, &q
}

func (rmq *RabbitMQ) failOnError(err error, msg string) {
	if err != nil {
		log.Fatal(fmt.Sprintf("%s:%s", msg, err))
		panic(fmt.Sprintf("%s:%s", msg, err))
	}
}
