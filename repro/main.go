// Copyright 2021-2024 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"io"

	connect "connectrpc.com/connect"
	pingv1 "connectrpc.com/connect/internal/gen/connect/ping/v1"
	"connectrpc.com/connect/internal/gen/connect/ping/v1/pingv1connect"
	"github.com/gin-gonic/gin"
)

// ExamplePingServer implements some trivial business logic. The Protobuf
// definition for this API is in proto/connect/ping/v1/ping.proto.
type ExamplePingServer struct {
	pingv1connect.UnimplementedPingServiceHandler
}

// Ping implements pingv1connect.PingServiceHandler.
func (*ExamplePingServer) Ping(
	_ context.Context,
	request *connect.Request[pingv1.PingRequest],
) (*connect.Response[pingv1.PingResponse], error) {
	return connect.NewResponse(
		&pingv1.PingResponse{
			Number: request.Msg.GetNumber(),
			Text:   request.Msg.GetText(),
		},
	), nil
}

// Sum implements pingv1connect.PingServiceHandler.
func (p *ExamplePingServer) Sum(ctx context.Context, stream *connect.ClientStream[pingv1.SumRequest]) (*connect.Response[pingv1.SumResponse], error) {
	var sum int64
	for stream.Receive() {
		sum += stream.Msg().GetNumber()
	}
	if stream.Err() != nil {
		return nil, stream.Err()
	}
	return connect.NewResponse(&pingv1.SumResponse{Sum: sum}), nil
}

// CountUp implements pingv1connect.PingServiceHandler.
func (p *ExamplePingServer) CountUp(ctx context.Context, request *connect.Request[pingv1.CountUpRequest], stream *connect.ServerStream[pingv1.CountUpResponse]) error {
	for number := int64(1); number <= request.Msg.GetNumber(); number++ {
		if err := stream.Send(&pingv1.CountUpResponse{Number: number}); err != nil {
			return err
		}
	}
	return nil
}

// CumSum implements pingv1connect.PingServiceHandler.
func (p *ExamplePingServer) CumSum(ctx context.Context, stream *connect.BidiStream[pingv1.CumSumRequest, pingv1.CumSumResponse]) error {
	var sum int64
	for {
		msg, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		sum += msg.GetNumber()
		if err := stream.Send(&pingv1.CumSumResponse{Sum: sum}); err != nil {
			return err
		}
	}
}

func main() {
	app := gin.New()
	app.UseH2C = true
	path, handler := pingv1connect.NewPingServiceHandler(
		&ExamplePingServer{}, // our business logic
	)
	app.POST(path+"/*method", gin.WrapH(handler))

	app.Run(":8080")
}
