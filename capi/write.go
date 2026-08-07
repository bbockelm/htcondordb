// Batch ClassAd writes: hcdb_write_ads upserts a chunk of whole ads in one round trip,
// which is what makes a bulk load practical -- the SQL path costs a statement parse and a
// round trip per row.
//
// One chunk per call, rather than a streaming cursor like the read side. Chunking belongs
// to the caller: it is the one holding the iterator, it knows how much to buffer, and a
// per-chunk call keeps the C surface to a single function with no handle to leak.
package main

/*
#include <stdlib.h>
#include <stdint.h>
*/
import "C"

import (
	"encoding/json"

	"github.com/bbockelm/htcondordb/repl"
)

// adWriteRequest is the JSON a caller passes: the table and the ads, each with its key and
// its new-ClassAd (bracketed) text.
type adWriteRequest struct {
	Table string `json:"table"`
	Ads   []struct {
		Key string `json:"key"`
		Ad  string `json:"ad"`
	} `json:"ads"`
}

// adWriteResponse is the JSON returned. Rejects carry the caller's own index so a loader
// can report which record of its input failed.
type adWriteResponse struct {
	Written int `json:"written"`
	Rejects []struct {
		Index  int    `json:"index"`
		Reason string `json:"reason"`
	} `json:"rejects,omitempty"`
	Conflicts []string `json:"conflicts,omitempty"`
}

// Upserts a chunk of ClassAds. req is a JSON adWriteRequest; on success writes a JSON
// adWriteResponse to *out and returns hcdbOK. On failure writes a plain-text message and
// returns hcdbDenied for an authorization refusal or hcdbErr otherwise.
//
// Ads that individually cannot be written are NOT failures: they come back in the
// response's rejects, by index, and the rest of the chunk applies.
//
//export hcdb_write_ads
func hcdb_write_ads(h C.uintptr_t, req *C.char, out **C.char) C.int {
	c := handleConn(h)
	if c == nil {
		*out = C.CString("invalid connection handle")
		return hcdbErr
	}

	var request adWriteRequest
	if err := json.Unmarshal([]byte(C.GoString(req)), &request); err != nil {
		*out = C.CString("decoding the write request: " + err.Error())
		return hcdbBadSQL
	}

	items := make([]repl.AdItem, len(request.Ads))
	for i, a := range request.Ads {
		items[i] = repl.AdItem{Key: a.Key, AdText: a.Ad}
	}

	// Same lock a statement takes: the executor is per-connection state, and a write that
	// stages into an open transaction touches it.
	c.mu.Lock()
	res, err := c.ex.WriteAds(request.Table, items)
	c.mu.Unlock()

	if err != nil {
		if hint := repl.HintFor(err); hint != "" {
			*out = C.CString(err.Error() + ": " + hint)
			return hcdbDenied
		}
		*out = C.CString(err.Error())
		return hcdbErr
	}

	resp := adWriteResponse{Written: res.Written, Conflicts: res.Conflicts}
	for _, r := range res.Rejects {
		resp.Rejects = append(resp.Rejects, struct {
			Index  int    `json:"index"`
			Reason string `json:"reason"`
		}{r.Index, r.Reason})
	}
	doc, err := json.Marshal(resp)
	if err != nil {
		*out = C.CString("encoding the write result: " + err.Error())
		return hcdbErr
	}
	*out = C.CString(string(doc))
	return hcdbOK
}
