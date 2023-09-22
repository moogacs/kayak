package service

import (
	"context"
	"log/slog"

	"connectrpc.com/connect"
	"github.com/binarymatt/kayak/internal/log"
	"go.opentelemetry.io/otel/trace"
)

func NewLogInterceptor() connect.UnaryInterceptorFunc {
	interceptor := func(next connect.UnaryFunc) connect.UnaryFunc {
		return connect.UnaryFunc(func(
			ctx context.Context,
			req connect.AnyRequest,
		) (connect.AnyResponse, error) {
			httpMethod := req.HTTPMethod()
			method := req.Spec().Procedure
			scontext := trace.SpanContextFromContext(ctx)
			logger := slog.Default()
			if scontext.HasTraceID() {
				logger = logger.With("trace_id", scontext.TraceID())
			}
			log.WithContext(ctx, logger)
			logger.InfoContext(ctx, "grpc endpoint called", slog.Group("grpc", slog.String("http_method", httpMethod), slog.String("method", method)))
			//slog.SetDefault(slog.Default().With(slog.Group("grpc", slog.String("http_method", httpMethod), slog.String("method", method))))
			return next(ctx, req)
		})
	}
	return connect.UnaryInterceptorFunc(interceptor)
}
