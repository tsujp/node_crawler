package api

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/public"
)

func mapPosition(x, y int) string {
	return fmt.Sprintf(
		`style="position: absolute; top: %d%%; left: %d%%; transform: translate(-50%%, -50%%)"`,
		y,
		x,
	)
}

func (a *API) nodesHandler(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")

	// /nodes/{id}
	if len(pathParts) != 3 {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	nodeID := pathParts[2]

	// /nodes/
	if nodeID == "" {
		a.nodesListHandler(w, r)

		return
	}

	nodes, err := a.db.GetNodeTable(r.Context(), nodeID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNotFound)

			return
		}
		log.Error("get node page failed", "err", err, "id", nodeID)
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	sb := new(strings.Builder)

	nodeTable := public.NodeTable(*nodes)
	index := public.Index(public.URLFromReq(r), nodeTable, 1, -1)
	_ = index.Render(r.Context(), sb)

	// This is the worst, but templating the style attribute is
	// not allowed for security concerns.
	out := strings.Replace(
		sb.String(),
		`style="REPLACE_THIS_ETH_LOGO"`,
		mapPosition(nodes.XOffsetPercent(), nodes.YOffsetPercent()),
		1,
	)

	w.Write([]byte(out))
}
