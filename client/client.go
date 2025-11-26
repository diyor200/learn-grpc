package client

import (
	"context"
	hellov1 "github.com/diyor200/learn-grpc/proto"
	"log"
)

type Client struct {
	conn hellov1.UnimplementedHelloServer
}

func (c *Client) Say(ctx context.Context, name string) {
	resp, err := c.conn.SayHello(ctx, &hellov1.HelloRequest{Name: name})
	if err != nil {
		log.Fatal(err)
	}

	log.Println(resp.HelloText)
}
