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

func printReport(report workflowReport) {
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
}
