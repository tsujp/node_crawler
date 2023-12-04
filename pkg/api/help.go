package api

import (
	"net/http"

	"github.com/ethereum/node-crawler/public"
)

func (a *API) handleHelp(w http.ResponseWriter, r *http.Request) {
	helpPage := public.HelpPage(a.enode)

	index := public.Index(public.URLFromReq(r), helpPage, 1, -1)
	_ = index.Render(r.Context(), w)
}
