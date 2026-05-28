package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
)

const remoteWriteTimeSeriesField = 0x0a

type replayResult struct {
	DurationMillis int64 `json:"DurationMillis"`
	Totals         struct {
		MetricRate float64 `json:"metricRate"`
		RowRate    float64 `json:"rowRate"`
	} `json:"Totals"`
}

type replayBatch struct {
	payload []byte
	rows    uint64
}

func main() {
	filePath := flag.String("file", "", "TSBS prometheus data file")
	targetURL := flag.String("url", "http://localhost:9091/api/v1/write", "remote-write URL")
	resultsFile := flag.String("results-file", "", "write summary JSON to this file")
	batchSize := flag.Int("batch-size", 250000, "TimeSeries messages per remote-write request")
	workers := flag.Int("workers", 8, "parallel HTTP workers")
	reportingPeriod := flag.Duration("reporting-period", 5*time.Second, "progress reporting period")
	timeout := flag.Duration("timeout", 5*time.Minute, "HTTP client timeout")
	flag.Parse()

	if *filePath == "" {
		fatalf("--file is required")
	}
	if *batchSize <= 0 {
		fatalf("--batch-size must be positive")
	}
	if *workers <= 0 {
		fatalf("--workers must be positive")
	}

	started := time.Now()
	var rows atomic.Uint64
	if err := replay(*filePath, *targetURL, *batchSize, *workers, *timeout, *reportingPeriod, &rows); err != nil {
		fatalf("%v", err)
	}
	elapsed := time.Since(started)
	totalRows := rows.Load()
	rate := float64(totalRows) / elapsed.Seconds()
	fmt.Printf("\nSummary:\n")
	fmt.Printf("loaded %d metrics in %.3fsec with %d workers (mean rate %.2f metrics/sec)\n", totalRows, elapsed.Seconds(), *workers, rate)
	fmt.Printf("loaded %d rows in %.3fsec with %d workers (mean rate %.2f rows/sec)\n", totalRows, elapsed.Seconds(), *workers, rate)

	if *resultsFile != "" {
		result := replayResult{DurationMillis: elapsed.Milliseconds()}
		result.Totals.MetricRate = rate
		result.Totals.RowRate = rate
		data, err := json.MarshalIndent(result, "", " ")
		if err != nil {
			fatalf("marshal results: %v", err)
		}
		data = append(data, '\n')
		if err := os.WriteFile(*resultsFile, data, 0o644); err != nil {
			fatalf("write results: %v", err)
		}
		fmt.Printf("Saving results json file to %s\n", *resultsFile)
	}
}

func replay(filePath, targetURL string, batchSize, workers int, timeout time.Duration, reportingPeriod time.Duration, rows *atomic.Uint64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	batches := make(chan replayBatch, workers*2)
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	client := replayHTTPClient(timeout)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batches {
				if err := postRemoteWrite(client, targetURL, batch.payload); err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
				rows.Add(batch.rows)
			}
		}()
	}

	done := make(chan struct{})
	go reportProgress(reportingPeriod, rows, done)

	readErr := readBatches(file, batchSize, batches, errs)
	close(batches)
	wg.Wait()
	close(done)

	select {
	case err := <-errs:
		return err
	default:
	}
	return readErr
}

func readBatches(reader io.Reader, batchSize int, batches chan<- replayBatch, errs <-chan error) error {
	buffered := bufio.NewReaderSize(reader, 16<<20)
	if _, err := binary.ReadUvarint(buffered); err != nil {
		return fmt.Errorf("read TSBS file version: %w", err)
	}

	payload := make([]byte, 0, batchSize*256)
	var rows uint64
	for {
		select {
		case err := <-errs:
			return err
		default:
		}
		if _, err := buffered.Peek(1); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		messageSize, err := binary.ReadUvarint(buffered)
		if err != nil {
			return fmt.Errorf("read TSBS message size: %w", err)
		}
		payload = append(payload, remoteWriteTimeSeriesField)
		payload = binary.AppendUvarint(payload, messageSize)
		start := len(payload)
		payload = append(payload, make([]byte, int(messageSize))...)
		if _, err := io.ReadFull(buffered, payload[start:]); err != nil {
			return fmt.Errorf("read TSBS message: %w", err)
		}
		rows++
		if int(rows) >= batchSize {
			if err := sendBatch(payload, rows, batches, errs); err != nil {
				return err
			}
			payload = make([]byte, 0, cap(payload))
			rows = 0
		}
	}
	if rows > 0 {
		return sendBatch(payload, rows, batches, errs)
	}
	return nil
}

func sendBatch(payload []byte, rows uint64, batches chan<- replayBatch, errs <-chan error) error {
	compressed := snappy.Encode(nil, payload)
	select {
	case err := <-errs:
		return err
	case batches <- replayBatch{payload: compressed, rows: rows}:
		return nil
	}
}

func replayHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			MaxIdleConns:          20000,
			MaxIdleConnsPerHost:   1000,
			DisableKeepAlives:     false,
			DisableCompression:    true,
			IdleConnTimeout:       5 * time.Minute,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
}

func postRemoteWrite(client *http.Client, targetURL string, payload []byte) error {
	req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("remote write returned status %s: %s", resp.Status, string(body))
	}
	return nil
}

func reportProgress(period time.Duration, rows *atomic.Uint64, done <-chan struct{}) {
	if period <= 0 {
		return
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	started := time.Now()
	var lastRows uint64
	var lastTime = started
	fmt.Println("time,per. metric/s,metric total,overall metric/s,per. row/s,row total,overall row/s")
	for {
		select {
		case now := <-ticker.C:
			current := rows.Load()
			periodRate := float64(current-lastRows) / now.Sub(lastTime).Seconds()
			overallRate := float64(current) / now.Sub(started).Seconds()
			fmt.Printf("%d,%.2f,%E,%.2f,%.2f,%E,%.2f\n", now.Unix(), periodRate, float64(current), overallRate, periodRate, float64(current), overallRate)
			lastRows = current
			lastTime = now
		case <-done:
			return
		}
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
