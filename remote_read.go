package main

import (
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
)

// @Summary Prometheus Remote Read endpoint
// @Description Allows querying historical purchase metrics from the database using the Prometheus remote read protocol.
// @Tags Monitoring
// @Accept application/x-protobuf
// @Produce application/x-protobuf
// @Success 200 {string} string "Protobuf encoded response"
// @Failure 400 {string} string "Bad request"
// @Router /api/v1/read [post]
func remoteReadHandler(w http.ResponseWriter, r *http.Request) {
	req, err := remote.DecodeReadRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := &prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(req.Queries)),
	}

	for i, query := range req.Queries {
		resp.Results[i] = executeRemoteReadQuery(query)
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	if err := remote.EncodeReadResponse(resp, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Helper function to check if a time series matches label matchers
func matchesLabels(ts *prompb.TimeSeries, matchers []*prompb.LabelMatcher) bool {
	labels := make(map[string]string)
	for _, label := range ts.Labels {
		labels[label.Name] = label.Value
	}

	for _, matcher := range matchers {
		// Skip internal Prometheus labels not present on our time series
		if strings.HasPrefix(matcher.Name, "prometheus") ||
			matcher.Name == "job" || matcher.Name == "instance" ||
			matcher.Name == "endpoint" || matcher.Name == "namespace" ||
			matcher.Name == "pod" || matcher.Name == "service" ||
			(strings.HasPrefix(matcher.Name, "__") && matcher.Name != "__name__") {
			continue
		}

		labelValue, exists := labels[matcher.Name]

		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			if !exists || labelValue != matcher.Value {
				return false
			}
		case prompb.LabelMatcher_NEQ:
			if exists && labelValue == matcher.Value {
				return false
			}
		case prompb.LabelMatcher_RE:
			if !exists {
				return false
			}
			matched, err := regexp.MatchString(matcher.Value, labelValue)
			if err != nil || !matched {
				return false
			}
		case prompb.LabelMatcher_NRE:
			if exists {
				matched, err := regexp.MatchString(matcher.Value, labelValue)
				if err != nil || matched {
					return false
				}
			}
		}
	}
	return true
}

// Execute a single remote read query against the database
func executeRemoteReadQuery(query *prompb.Query) *prompb.QueryResult {
	result := &prompb.QueryResult{
		Timeseries: []*prompb.TimeSeries{},
	}

	// Extract time range (convert milliseconds to seconds for SQL)
	startMs := query.StartTimestampMs
	endMs := query.EndTimestampMs

	// Parse matchers to extract label filters
	productFilter := ""
	machineFilter := ""
	methodFilter := ""
	matchesMetricName := false

	for _, matcher := range query.Matchers {
		if matcher.Name == "__name__" {
			// Check if this query is for our historical metric
			if matcher.Type == prompb.LabelMatcher_EQ && matcher.Value == "purchases_historical_total" {
				matchesMetricName = true
			} else if matcher.Type == prompb.LabelMatcher_RE && strings.Contains(matcher.Value, "purchases_historical_total") {
				matchesMetricName = true
			}
		} else if matcher.Name == "product" && matcher.Type == prompb.LabelMatcher_EQ {
			productFilter = matcher.Value
		} else if matcher.Name == "machine_id" && matcher.Type == prompb.LabelMatcher_EQ {
			machineFilter = matcher.Value
		} else if matcher.Name == "method" && matcher.Type == prompb.LabelMatcher_EQ {
			methodFilter = matcher.Value
		}
	}

	// Debug: Log all matchers received from Prometheus
	log.Printf("Remote read query - Matchers: %d total", len(query.Matchers))
	for i, matcher := range query.Matchers {
		matcherType := "UNKNOWN"
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			matcherType = "=="
		case prompb.LabelMatcher_NEQ:
			matcherType = "!="
		case prompb.LabelMatcher_RE:
			matcherType = "=~"
		case prompb.LabelMatcher_NRE:
			matcherType = "!~"
		}
		log.Printf("  Matcher %d: %s %s %q", i, matcher.Name, matcherType, matcher.Value)
	}

	// Only process if this query is for our metric
	if !matchesMetricName {
		log.Printf("Query does not match metric name, returning empty result")
		return result
	}

	// Build SQL query with optional filters
	// This query returns cumulative counts over time for each product/machine/method combination
	// We need to get ALL transactions up to endMs to calculate proper cumulative counts,
	// but we'll filter to the query range after getting the baseline
	sqlQuery := `
		SELECT
			COALESCE(product, '') as product,
			COALESCE(machine_id, '') as machine_id,
			CASE
				WHEN is_cash = true THEN 'cash'
				ELSE COALESCE(payment_method, 'unknown')
			END as method,
			created_at,
			COUNT(*) OVER (
				PARTITION BY product, machine_id,
				(CASE WHEN is_cash = true THEN 'cash' ELSE COALESCE(payment_method, 'unknown') END)
				ORDER BY created_at
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			) as cumulative_count
		FROM transactions
		WHERE status = 'confirmed'
		AND amount < 0
		AND EXTRACT(EPOCH FROM created_at) * 1000 <= ?
	`

	args := []interface{}{endMs}

	if productFilter != "" {
		sqlQuery += " AND product = ?"
		args = append(args, productFilter)
	}
	if machineFilter != "" {
		sqlQuery += " AND machine_id = ?"
		args = append(args, machineFilter)
	}
	if methodFilter != "" {
		if methodFilter == "cash" {
			sqlQuery += " AND is_cash = true"
		} else {
			sqlQuery += " AND is_cash = false AND payment_method = ?"
			args = append(args, methodFilter)
		}
	}

	sqlQuery += " ORDER BY product, machine_id, (CASE WHEN is_cash = true THEN 'cash' ELSE COALESCE(payment_method, 'unknown') END), created_at"

	type RemoteReadRow struct {
		Product         string
		MachineID       string
		Method          string
		CreatedAt       time.Time
		CumulativeCount int64
	}

	var rows []RemoteReadRow
	err := db.Raw(sqlQuery, args...).Scan(&rows).Error
	if err != nil {
		log.Printf("Error querying remote read data: %v", err)
		return result
	}

	// Group samples by time series (product, machine_id, method)
	timeSeriesMap := make(map[string]*prompb.TimeSeries)

	for _, row := range rows {
		// Create time series key
		tsKey := fmt.Sprintf("%s|%s|%s", row.Product, row.MachineID, row.Method)

		// Get or create time series
		ts, exists := timeSeriesMap[tsKey]
		if !exists {
			ts = &prompb.TimeSeries{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "purchases_historical_total"},
					{Name: "product", Value: row.Product},
					{Name: "machine_id", Value: row.MachineID},
					{Name: "method", Value: row.Method},
				},
				Samples: []prompb.Sample{},
			}
			timeSeriesMap[tsKey] = ts
		}

		// Add sample (cumulative count at this timestamp)
		ts.Samples = append(ts.Samples, prompb.Sample{
			Timestamp: row.CreatedAt.UnixMilli(),
			Value:     float64(row.CumulativeCount),
		})
	}

	// Process samples and add interpolated points for proper counter visualization
	for _, ts := range timeSeriesMap {
		if len(ts.Samples) == 0 {
			continue
		}

		// Find the baseline value (count at startMs) and samples within range
		var baselineValue float64 = 0
		var filteredSamples []prompb.Sample

		for _, sample := range ts.Samples {
			if sample.Timestamp < startMs {
				// Track the counter value just before our query range
				baselineValue = sample.Value
			} else {
				// This sample is within our query range
				filteredSamples = append(filteredSamples, sample)
			}
		}

		// Skip time series with no transactions in the query range
		// This hides products that had no purchases during the selected time period
		if len(filteredSamples) == 0 {
			continue
		}

		// Calculate step interval for interpolation
		// Match Prometheus scrape interval (typically 1 minute) for short ranges,
		// but use larger intervals for longer ranges to avoid too many points
		rangeMs := endMs - startMs
		var stepMs int64

		if rangeMs > 30*24*60*60*1000 { // > 30 days
			stepMs = 60 * 60 * 1000 // 1 hour
		} else if rangeMs > 7*24*60*60*1000 { // > 7 days
			stepMs = 15 * 60 * 1000 // 15 minutes
		} else if rangeMs > 24*60*60*1000 { // > 1 day
			stepMs = 5 * 60 * 1000 // 5 minutes
		} else if rangeMs > 6*60*60*1000 { // > 6 hours
			stepMs = 2 * 60 * 1000 // 2 minutes
		} else {
			stepMs = 60 * 1000 // 1 minute for < 6 hours
		}

		// Build a merged list of interpolated points AND actual transaction times
		var allSampleTimes []int64
		timeMap := make(map[int64]bool)

		// Add regular interval times
		for t := startMs; t <= endMs; t += stepMs {
			allSampleTimes = append(allSampleTimes, t)
			timeMap[t] = true
		}

		// Add actual transaction times if not already present
		for _, sample := range filteredSamples {
			if !timeMap[sample.Timestamp] {
				allSampleTimes = append(allSampleTimes, sample.Timestamp)
				timeMap[sample.Timestamp] = true
			}
		}

		// Always include start and end
		if !timeMap[startMs] {
			allSampleTimes = append(allSampleTimes, startMs)
		}
		if !timeMap[endMs] {
			allSampleTimes = append(allSampleTimes, endMs)
		}

		// Sort all times
		sortInt64Slice(allSampleTimes)

		// Generate samples at all these times
		var interpolatedSamples []prompb.Sample
		currentValue := baselineValue
		sampleIdx := 0

		for _, timestamp := range allSampleTimes {
			// Update value based on any transactions up to this point
			for sampleIdx < len(filteredSamples) && filteredSamples[sampleIdx].Timestamp <= timestamp {
				currentValue = filteredSamples[sampleIdx].Value
				sampleIdx++
			}

			interpolatedSamples = append(interpolatedSamples, prompb.Sample{
				Timestamp: timestamp,
				Value:     currentValue,
			})
		}

		if len(interpolatedSamples) > 0 {
			ts.Samples = interpolatedSamples

			// Apply label matchers to filter time series
			if matchesLabels(ts, query.Matchers) {
				result.Timeseries = append(result.Timeseries, ts)
			} else {
				// Debug: Log why this time series was filtered out
				var productLabel string
				for _, label := range ts.Labels {
					if label.Name == "product" {
						productLabel = label.Value
						break
					}
				}
				log.Printf("  Filtered out time series: product=%s (didn't match matchers)", productLabel)
			}
		}
	}

	log.Printf("Returning %d time series after filtering", len(result.Timeseries))
	return result
}

// Helper function to sort int64 slices
func sortInt64Slice(slice []int64) {
	for i := 0; i < len(slice); i++ {
		for j := i + 1; j < len(slice); j++ {
			if slice[i] > slice[j] {
				slice[i], slice[j] = slice[j], slice[i]
			}
		}
	}
}
