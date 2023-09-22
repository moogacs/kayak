package cli

import (
	"net/http"

	"github.com/binarymatt/kayak/gen/admin/v1/adminv1connect"
	"github.com/binarymatt/kayak/gen/kayak/v1/kayakv1connect"
)

func buildAdminClient(host string) adminv1connect.AdminServiceClient {
	return adminv1connect.NewAdminServiceClient(http.DefaultClient, host)
}
func buildClient(host string) kayakv1connect.KayakServiceClient {
	return kayakv1connect.NewKayakServiceClient(http.DefaultClient, host)
}
