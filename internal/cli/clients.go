package cli

import (
	"net/http"

	"github.com/kayak/gen/proto/admin/v1/adminv1connect"
	"github.com/kayak/gen/proto/kayak/v1/kayakv1connect"
)

func buildAdminClient(host string) adminv1connect.AdminServiceClient {
	return adminv1connect.NewAdminServiceClient(http.DefaultClient, host)
}
func buildClient(host string) kayakv1connect.KayakServiceClient {
	return kayakv1connect.NewKayakServiceClient(http.DefaultClient, host)
}
