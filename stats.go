package main

import (
	"fmt"
	"math"
	"sort"
	"time"
)

func percentile(values []time.Duration, fraction float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(math.Ceil(fraction*float64(len(sorted)))) - 1
	return sorted[max(0, min(index, len(sorted)-1))]
}

func printReport(report workflowReport, potentialBytesPerOperation int) {
	seconds := report.duration.Seconds()
	qps := float64(report.successes) / seconds
	throughputMiB := float64(report.bytes) / (1024 * 1024) / seconds
	firstP50 := "-"
	if len(report.firstResponses) > 0 {
		firstP50 = percentile(report.firstResponses, 0.50).String()
	}
	fmt.Printf("%-10s %8d %8d %10.2f %12.2f %12s %12s %12s %14s\n",
		report.mode,
		report.successes,
		report.errors,
		qps,
		throughputMiB,
		percentile(report.latencies, 0.50),
		percentile(report.latencies, 0.95),
		percentile(report.latencies, 0.99),
		firstP50,
	)
	if report.successes == 0 {
		return
	}
	potentialBytes := int64(potentialBytesPerOperation) * int64(report.successes)
	savedPercent := 100 * (1 - float64(report.bytes)/float64(potentialBytes))
	fmt.Printf("transfer mode=%s potential_bytes_per_operation=%d received_bytes_per_operation=%d protobuf_bytes_per_operation=%d response_messages_per_operation=%.2f received_units_per_operation=%.2f emitted_units_per_operation=%.2f unused_units_per_operation=%.2f saved_percent=%.2f\n",
		report.mode,
		potentialBytesPerOperation,
		report.bytes/int64(report.successes),
		report.protobufBytes/int64(report.successes),
		float64(report.responseMessages)/float64(report.successes),
		float64(report.receivedUnits)/float64(report.successes),
		float64(report.emittedUnits)/float64(report.successes),
		float64(report.receivedUnits-report.emittedUnits)/float64(report.successes),
		savedPercent,
	)
}

type workflowReport struct {
	mode             transferMode
	duration         time.Duration
	successes        int
	errors           int
	bytes            int64
	protobufBytes    int64
	responseMessages int64
	receivedUnits    int64
	emittedUnits     int64
	latencies        []time.Duration
	firstResponses   []time.Duration
}
