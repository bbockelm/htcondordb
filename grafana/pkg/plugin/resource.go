package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

// attrSampleLimit bounds how many ads the /attributes endpoint samples to infer a
// table's attribute names (ClassAd tables are schemaless, so we union the
// attributes seen across a sample).
const attrSampleLimit = 50

// CallResource serves the builder-support endpoints the QueryEditor calls to
// populate dropdowns without the user memorizing schema:
//
//	GET  /tables                -> ["ads","history","jobs","machines", ...]
//	GET  /attributes?table=jobs -> ["ClusterId","JobStatus","Owner", ...]
//	POST /values {sql}          -> distinct first-column values (template variables)
func (d *Datasource) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	path := strings.Trim(pathOnly(req.Path), "/")
	switch {
	case req.Method == http.MethodGet && path == "tables":
		return d.resTables(ctx, sender)
	case req.Method == http.MethodGet && path == "attributes":
		return d.resAttributes(ctx, req, sender)
	case req.Method == http.MethodPost && path == "values":
		return d.resValues(ctx, req, sender)
	default:
		return sendJSON(sender, http.StatusNotFound, errBody(fmt.Errorf("unknown resource %s %q", req.Method, path)))
	}
}

// resValues runs a SELECT and returns the distinct values of its first column, in
// first-seen order -- the backing for dashboard template variables (e.g.
// `SELECT DISTINCT Owner FROM jobs` -> the list of owners for a $owner variable).
func (d *Datasource) resValues(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	var body struct {
		SQL string `json:"sql"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return sendJSON(sender, http.StatusBadRequest, errBody(fmt.Errorf("bad request body: %w", err)))
	}
	sql := strings.TrimSpace(body.SQL)
	if sql == "" {
		return sendJSON(sender, http.StatusBadRequest, errBody(fmt.Errorf("empty variable query")))
	}
	exec, err := d.conns.executor(ctx)
	if err != nil {
		d.conns.reset()
		return sendJSON(sender, http.StatusBadGateway, errBody(err))
	}
	res, err := exec.ExecString(sql)
	if err != nil {
		if isConnError(err) {
			d.conns.reset()
		}
		return sendJSON(sender, http.StatusBadGateway, errBody(err))
	}
	seen := make(map[string]struct{}, len(res.Rows))
	values := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		if len(row) == 0 {
			continue
		}
		if _, ok := seen[row[0]]; ok {
			continue
		}
		seen[row[0]] = struct{}{}
		values = append(values, row[0])
	}
	return sendJSON(sender, http.StatusOK, values)
}

func (d *Datasource) resTables(ctx context.Context, sender backend.CallResourceResponseSender) error {
	exec, err := d.conns.executor(ctx)
	if err != nil {
		d.conns.reset()
		return sendJSON(sender, http.StatusBadGateway, errBody(err))
	}
	tables, err := exec.Tables()
	if err != nil {
		if isConnError(err) {
			d.conns.reset()
		}
		return sendJSON(sender, http.StatusBadGateway, errBody(err))
	}
	sort.Strings(tables)
	return sendJSON(sender, http.StatusOK, tables)
}

func (d *Datasource) resAttributes(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	table := strings.TrimSpace(queryParam(req.URL, "table"))
	if table == "" {
		return sendJSON(sender, http.StatusBadRequest, errBody(fmt.Errorf("missing ?table=")))
	}
	exec, err := d.conns.executor(ctx)
	if err != nil {
		d.conns.reset()
		return sendJSON(sender, http.StatusBadGateway, errBody(err))
	}
	// SELECT * populates Result.Ads; union their attribute names.
	res, err := exec.ExecString(fmt.Sprintf("SELECT * FROM %s LIMIT %d", table, attrSampleLimit))
	if err != nil {
		if isConnError(err) {
			d.conns.reset()
		}
		return sendJSON(sender, http.StatusBadGateway, errBody(err))
	}
	seen := map[string]struct{}{}
	for _, ad := range res.Ads {
		if ad == nil {
			continue
		}
		for _, a := range ad.GetAttributes() {
			seen[a] = struct{}{}
		}
	}
	attrs := make([]string, 0, len(seen))
	for a := range seen {
		attrs = append(attrs, a)
	}
	sort.Strings(attrs)
	return sendJSON(sender, http.StatusOK, attrs)
}

// pathOnly strips any query string from a resource path.
func pathOnly(p string) string {
	if i := strings.IndexByte(p, '?'); i >= 0 {
		return p[:i]
	}
	return p
}

// queryParam extracts one query parameter from a forwarded resource URL.
func queryParam(rawURL, key string) string {
	if u, err := url.Parse(rawURL); err == nil {
		return u.Query().Get(key)
	}
	return ""
}

func sendJSON(sender backend.CallResourceResponseSender, status int, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return sender.Send(&backend.CallResourceResponse{
		Status:  status,
		Headers: map[string][]string{"Content-Type": {"application/json"}},
		Body:    body,
	})
}

func errBody(err error) map[string]string { return map[string]string{"error": err.Error()} }
