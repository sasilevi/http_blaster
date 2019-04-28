package memqueue

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	queueName string
	server    string
	port      string
	user      string
}

func New(queueName, server, port, user string) *RabbitMQ {
	return &RabbitMQ{
		queueName: queueName,
		server:    server,
		port:      port,
		user:      user,
	}
}

func (self *RabbitMQ) NewClient() chan string {
	chUsrAgent := make(chan string)

	go func() {
		defer close(chUsrAgent)
		conn, ch, q := self.getQueue(self.queueName, self.server, self.port, self.user)
		defer conn.Close()
		defer ch.Close()

		msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)

		self.failOnError(err, "Failed to register consumer")

		for msg := range msgs {
			// log.Printf("Recieved message with message %s", msg.Body)
			chUsrAgent <- string(msg.Body)
		}
		log.Println("No more messages")
	}()
	return chUsrAgent
}

func (self *RabbitMQ) getQueue(queueName, server, port, user string) (*amqp.Connection, *amqp.Channel, *amqp.Queue) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s@%s:%s", user, server, port))
	self.failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	self.failOnError(err, "Failed to open a Channel")
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	self.failOnError(err, "fail to declear queue")
	return conn, ch, &q
}

func (self *RabbitMQ) failOnError(err error, msg string) {
	if err != nil {
		log.Fatal(fmt.Sprintf("%s:%s", msg, err))
		panic(fmt.Sprintf("%s:%s", msg, err))
	}
}
