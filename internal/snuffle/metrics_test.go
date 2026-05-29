package snuffle

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

func TestSelfScrapeWriteRequestAddsTargetLabels(t *testing.T) {
	server := newServer(Config{
		SelfScrapeJob:      "snuffle",
		SelfScrapeInstance: "test-instance:9091",
	})

	req, samples, err := server.selfScrapeWriteRequest(time.Unix(10, 0))
	if err != nil {
		t.Fatalf("selfScrapeWriteRequest returned error: %v", err)
	}
	if samples == 0 || len(req.GetTimeseries()) == 0 {
		t.Fatalf("self scrape produced samples=%d series=%d, want non-zero", samples, len(req.GetTimeseries()))
	}

	for _, ts := range req.GetTimeseries() {
		labels := labelMap(ts.GetLabels())
		if labels["__name__"] == "snuffle_self_scrape_last_timestamp_seconds" {
			if labels["job"] != "snuffle" || labels["instance"] != "test-instance:9091" {
				t.Fatalf("self scrape labels = job %q instance %q", labels["job"], labels["instance"])
			}
			return
		}
	}
	t.Fatalf("self scrape did not include snuffle_self_scrape_last_timestamp_seconds")
}

func labelMap(labels []prompb.Label) map[string]string {
	out := make(map[string]string, len(labels))
	for _, label := range labels {
		out[label.Name] = label.Value
	}
	return out
}
