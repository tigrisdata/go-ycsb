// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package measurement

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type histogram struct {
	boundCounts util.ConcurrentMap
	startTime   time.Time
	hist        *hdrhistogram.Histogram
}

// Metric name.
const (
	ELAPSED   = "ELAPSED"
	COUNT     = "COUNT"
	QPS       = "QPS"
	AVG       = "AVG"
	MIN       = "MIN"
	MAX       = "MAX"
	PER50TH   = "PER50TH"
	PER95TH   = "PER95TH"
	PER99TH   = "PER99TH"
	PER999TH  = "PER999TH"
	PER9999TH = "PER9999TH"
)

func (h *histogram) Info() ycsb.MeasurementInfo {
	res := h.getInfo()
	delete(res, ELAPSED)
	return newHistogramInfo(res)
}

func newHistogram(p *properties.Properties) *histogram {
	h := new(histogram)
	h.startTime = time.Now()
	h.hist = hdrhistogram.New(1, 24*60*60*1000*1000, 3)
	return h
}

func (h *histogram) Measure(latency time.Duration) {
	h.hist.RecordValue(latency.Microseconds())
}

func (h *histogram) Summary() string {
	res := h.getInfo()

	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("Takes(s): %.1f, ", res[ELAPSED]))
	buf.WriteString(fmt.Sprintf("Count: %d, ", res[COUNT]))
	buf.WriteString(fmt.Sprintf("OPS: %.1f, ", res[QPS]))
	buf.WriteString(fmt.Sprintf("Avg(us): %d, ", res[AVG]))
	buf.WriteString(fmt.Sprintf("Min(us): %d, ", res[MIN]))
	buf.WriteString(fmt.Sprintf("Max(us): %d, ", res[MAX]))
	buf.WriteString(fmt.Sprintf("50th(us): %d, ", res[PER50TH]))
	buf.WriteString(fmt.Sprintf("95th(us): %d, ", res[PER95TH]))
	buf.WriteString(fmt.Sprintf("99th(us): %d", res[PER99TH]))

	return buf.String()
}

func (h *histogram) getInfo() map[string]interface{} {
	min := h.hist.Min()
	max := h.hist.Max()
	avg := int64(h.hist.Mean())
	count := h.hist.TotalCount()

	bounds := h.boundCounts.Keys()
	sort.Ints(bounds)

	per50 := h.hist.ValueAtPercentile(50)
	per95 := h.hist.ValueAtPercentile(95)
	per99 := h.hist.ValueAtPercentile(99)
	per999 := h.hist.ValueAtPercentile(99.9)
	per9999 := h.hist.ValueAtPercentile(99.99)

	elapsed := time.Now().Sub(h.startTime).Seconds()
	qps := float64(count) / elapsed
	res := make(map[string]interface{})
	res[ELAPSED] = elapsed
	res[COUNT] = count
	res[QPS] = qps
	res[AVG] = avg
	res[MIN] = min
	res[MAX] = max
	res[PER50TH] = per50
	res[PER95TH] = per95
	res[PER99TH] = per99
	res[PER999TH] = per999
	res[PER9999TH] = per9999
	return res
}

type histogramInfo struct {
	info map[string]interface{}
}

func newHistogramInfo(info map[string]interface{}) *histogramInfo {
	return &histogramInfo{info: info}
}

func (hi *histogramInfo) Get(metricName string) interface{} {
	if value, ok := hi.info[metricName]; ok {
		return value
	}
	return nil
}
