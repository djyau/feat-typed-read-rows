// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !emulator
// +build !emulator

package tests

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
)

// TestTypedReadRows_MultipleRows_Success verifies multiple rows sent across three batches with
// chained cumulative CRC32C checksums.
func TestTypedReadRows_MultipleRows_Success(t *testing.T) {
	row1 := makeTypedRow([]byte("row1"),
		makeTypedFamily("fam1",
			makeTypedColumn([]byte("col1"), makeTypedCell([]byte("val1"))),
		),
	)
	row2 := makeTypedRow([]byte("row2"),
		makeTypedFamily("fam1",
			makeTypedColumn([]byte("col2"), makeTypedCell([]byte("val2"))),
		),
	)
	row3 := makeTypedRow([]byte("row3"),
		makeTypedFamily("fam2",
			makeTypedColumn([]byte("col3"), makeTypedCell([]byte("val3"))),
		),
	)
	row4 := makeDefaultTypedRow("row4", "val4")

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil,
		typedFlushAction("token1", []*btpb.TypedRow{row1, row2}),
		typedFlushAction("token2", []*btpb.TypedRow{row3}, []*btpb.TypedRow{row1, row2}),
		typedFlushAction("token3", []*btpb.TypedRow{row4}, []*btpb.TypedRow{row1, row2}, row3),
	)

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "test-table"), nil)

	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2, row3, row4}, res.Rows)
}

// TestTypedReadRows_TargetRouting verifies table_name, authorized_view_name, and
// materialized_view_name routing in both the request payload and x-goog-request-params header.
func TestTypedReadRows_TargetRouting(t *testing.T) {
	row := makeDefaultTypedRow("row-target", "v")
	instanceResource := fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID)

	tests := []struct {
		name          string
		targetReq     *btpb.TypedReadRowsRequest
		wantHeaderKey string
		wantHeaderVal string
		verifyReq     func(t *testing.T, req *btpb.TypedReadRowsRequest)
	}{
		{
			name:          "TableName",
			targetReq:     makeTypedReadRowsRequest(buildTableName("target-table")),
			wantHeaderKey: "table_name",
			wantHeaderVal: buildTableName("target-table"),
			verifyReq: func(t *testing.T, req *btpb.TypedReadRowsRequest) {
				assert.Equal(t, buildTableName("target-table"), req.GetTableName())
				assert.Empty(t, req.GetAuthorizedViewName())
				assert.Empty(t, req.GetMaterializedViewName())
			},
		},
		{
			name: "AuthorizedViewName",
			targetReq: &btpb.TypedReadRowsRequest{
				Target: &btpb.TypedReadRowsRequest_AuthorizedViewName{
					AuthorizedViewName: buildAuthorizedViewName("target-table", "target-view"),
				},
			},
			wantHeaderKey: "table_name",
			wantHeaderVal: buildTableName("target-table"),
			verifyReq: func(t *testing.T, req *btpb.TypedReadRowsRequest) {
				assert.Equal(t, buildAuthorizedViewName("target-table", "target-view"), req.GetAuthorizedViewName())
				assert.Empty(t, req.GetTableName())
				assert.Empty(t, req.GetMaterializedViewName())
			},
		},
		{
			name: "MaterializedViewName",
			targetReq: &btpb.TypedReadRowsRequest{
				Target: &btpb.TypedReadRowsRequest_MaterializedViewName{
					MaterializedViewName: buildMaterializedViewName("target-mv"),
				},
			},
			wantHeaderKey: "name",
			wantHeaderVal: instanceResource,
			verifyReq: func(t *testing.T, req *btpb.TypedReadRowsRequest) {
				assert.Equal(t, buildMaterializedViewName("target-mv"), req.GetMaterializedViewName())
				assert.Empty(t, req.GetTableName())
				assert.Empty(t, req.GetAuthorizedViewName())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := make(chan *typedReadRowsReqRecord, 10)
			mdRecords := make(chan metadata.MD, 10)

			server := initMockServer(t)
			server.TypedReadRowsFn = mockTypedReadRowsFnWithMetadata(recorder, mdRecords, typedDefaultFlushAction("row-target", "v", "token"))

			req := &testproxypb.TypedReadRowsRequest{
				ClientId: t.Name(),
				Request:  tc.targetReq,
			}

			res := doTypedReadRowsOp(t, server, req, nil)

			checkResultOkStatus(t, res)
			assertTypedRowsEqual(t, []*btpb.TypedRow{row}, res.Rows)
			capturedReq := assertResumeTokens(t, recorder, "")[0]
			tc.verifyReq(t, capturedReq)

			md := <-mdRecords
			params := md["x-goog-request-params"]
			if assert.NotEmpty(t, params, "x-goog-request-params header must be present") {
				parsed, err := url.ParseQuery(params[0])
				assert.NoError(t, err, "failed to parse x-goog-request-params %q", params[0])
				assert.Equal(t, tc.wantHeaderVal, parsed.Get(tc.wantHeaderKey),
					"unexpected %s in x-goog-request-params %q", tc.wantHeaderKey, params[0])
			}
		})
	}
}

// TestTypedReadRows_Generic_Headers tests that TypedReadRows request sends client and resource info,
// app_profile_id in both the request payload and headers, and the required Bigtable feature flags.
func TestTypedReadRows_Generic_Headers(t *testing.T) {
	const profileID = "test_profile"
	tableName := buildTableName("table-headers")
	row := makeDefaultTypedRow("row-headers", "v")

	recorder := make(chan *typedReadRowsReqRecord, 10)
	mdRecords := make(chan metadata.MD, 10)

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFnWithMetadata(recorder, mdRecords, typedDefaultFlushAction("row-headers", "v", "token"))

	req := makeProxyTypedReadRowsRequest(t, "table-headers")
	res := doTypedReadRowsOp(t, server, req, &clientOpts{profile: profileID})

	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row}, res.Rows)
	capturedReq := assertResumeTokens(t, recorder, "")[0]
	assert.Equal(t, profileID, capturedReq.GetAppProfileId())

	md := <-mdRecords
	if len(md["user-agent"]) == 0 && len(md["x-goog-api-client"]) == 0 {
		assert.Fail(t, "Client info is missing in the request header")
	}

	params := md["x-goog-request-params"]
	assert.NotEmpty(t, params, "x-goog-request-params header must be present")
	if len(params) == 0 {
		return
	}
	resource := params[0]
	if !strings.Contains(resource, tableName) && !strings.Contains(resource, url.QueryEscape(tableName)) {
		assert.Fail(t, "Resource info is missing in the request header")
	}
	assert.Contains(t, resource, profileID)

	ff, err := getClientFeatureFlags(md)
	assert.Nil(t, err, "failed to decode client feature flags")
	if assert.NotNil(t, ff, "client feature flags must be present") {
		assert.True(t, ff.ReverseScans, "client must enable ReverseScans feature flag")
		assert.True(t, ff.RoutingCookie, "client must enable RoutingCookie feature flag")
		assert.True(t, ff.RetryInfo, "client must enable RetryInfo feature flag")
	}
}

// TestTypedReadRows_RequestFields_Fidelity verifies that request fields -- Rows (including
// raw_value and structured array_value keys across all 8 supported data types, row_prefixes, and
// bounded/half-open/unbounded row_ranges), Filter, RowsLimit, Reversed, and RowKeyFormat
// (use_structured_key) -- are propagated faithfully.
func TestTypedReadRows_RequestFields_Fidelity(t *testing.T) {
	structuredKey := arrayVal(
		strVal("tenant-1"),
		bytesVal([]byte{0x01, 0x02}),
		intVal(42),
		floatVal(3.141592653589793),
		floatVal(1.5),
		boolVal(true),
		timestampVal(1700000000, 123456000),
		dateVal(2026, 3, 25),
		nullVal(),
	)
	wantRowSet := &btpb.TypedRowSet{
		RowKeys: []*btpb.Value{
			rawVal([]byte("key1")),
			rawVal([]byte("key2")),
			structuredKey,
		},
		RowPrefixes: []*btpb.Value{
			arrayVal(strVal("pref-"), intVal(10)),
		},
		RowRanges: []*btpb.TypedValueRange{
			{
				StartValue: &btpb.TypedValueRange_StartValueClosed{StartValueClosed: rawVal([]byte("start-val"))},
				EndValue:   &btpb.TypedValueRange_EndValueOpen{EndValueOpen: rawVal([]byte("end-val"))},
			},
			{
				StartValue: &btpb.TypedValueRange_StartValueOpen{StartValueOpen: rawVal([]byte("start-open"))},
				EndValue:   &btpb.TypedValueRange_EndValueClosed{EndValueClosed: rawVal([]byte("end-closed"))},
			},
			{
				// Unbounded end (start_value_closed only) with structured ArrayValue key
				StartValue: &btpb.TypedValueRange_StartValueClosed{StartValueClosed: structuredKey},
			},
			{
				// Unbounded start (end_value_open only)
				EndValue: &btpb.TypedValueRange_EndValueOpen{EndValueOpen: rawVal([]byte("upper-bound"))},
			},
		},
	}
	wantFilter := &btpb.RowFilter{
		Filter: &btpb.RowFilter_CellsPerColumnLimitFilter{
			CellsPerColumnLimitFilter: 7,
		},
	}

	for _, structured := range []bool{false, true} {
		t.Run(fmt.Sprintf("structured_%v", structured), func(t *testing.T) {
			recorder := make(chan *typedReadRowsReqRecord, 10)

			server := initMockServer(t)
			server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, typedDefaultFlushAction("row1", "v", "token"))

			req := makeProxyTypedReadRowsRequest(t, "fidelity-table")
			req.Request.Rows = wantRowSet
			req.Request.Filter = wantFilter
			req.Request.RowsLimit = 42
			req.Request.Reversed = true
			req.Request.RowKeyFormat = &btpb.TypedReadRowsRequest_UseStructuredKey{UseStructuredKey: structured}

			res := doTypedReadRowsOp(t, server, req, nil)

			checkResultOkStatus(t, res)
			capturedReq := assertResumeTokens(t, recorder, "")[0]
			if diff := cmp.Diff(wantRowSet, capturedReq.GetRows(), protocmp.Transform()); diff != "" {
				t.Errorf("TypedRowSet mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(wantFilter, capturedReq.GetFilter(), protocmp.Transform()); diff != "" {
				t.Errorf("RowFilter mismatch (-want +got):\n%s", diff)
			}
			assert.Equal(t, int64(42), capturedReq.GetRowsLimit())
			assert.True(t, capturedReq.GetReversed())
			// GetUseStructuredKey() returns false both when the oneof is set to false and when it is
			// not set at all, so assert presence separately -- otherwise the structured=false pass
			// would still succeed against a client that dropped the field entirely.
			assert.NotNil(t, capturedReq.GetRowKeyFormat(),
				"the row_key_format oneof must be propagated even when use_structured_key is false")
			assert.Equal(t, structured, capturedReq.GetUseStructuredKey())
		})
	}
}

// TestTypedReadRows_EmptyTable_Success verifies that an empty table scan returns 0 rows and OK
// status both when the server closes the stream immediately with 0 messages and when the server
// sends an initial response carrying only TableSchema (Response: nil) before closing.
func TestTypedReadRows_EmptyTable_Success(t *testing.T) {
	testCases := []struct {
		name    string
		actions []*typedReadRowsAction
	}{
		{"zero messages", nil},
		{"schema-only response", []*typedReadRowsAction{
			{response: &btpb.TypedReadRowsResponse{TableSchema: &btpb.TableSchema{}}},
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := make(chan *typedReadRowsReqRecord, 10)
			server := initMockServer(t)
			server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, tc.actions...)

			res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "empty-table"), nil)

			checkResultOkStatus(t, res)
			assert.Empty(t, res.Rows)
			assertResumeTokens(t, recorder, "")
		})
	}
}

// TestTypedReadRows_Cell_EmptyByteQualifierEmptyValueAndLabels covers the column/cell payload edge
// cases that a server can actually produce: a column whose raw_value byte qualifier is present but
// zero-length ([]byte{}), a cell whose raw_value is present but zero-length ([]byte{}), and a cell
// carrying an explicit timestamp plus labels.
//
// Note on scope: on the server side a TypedCell's value is always raw_value, and a TypedColumn's
// qualifier is always raw_value -- no other Value kind is reachable, and a column/cell always
// carries a raw_value rather than leaving the Value field unset. So there is deliberately no
// coverage here of string_value/int_value/etc. qualifiers or cells, or of a nil Value; such a
// stream cannot occur, and asserting on it would invite client authors to implement handling for
// cases that never arrive. Structured array_value only ever appears in row keys (see
// MidStream_SchemaEvolution and StructuredRowKeys_Resumption) and in TypedRowSet.row_prefixes on
// the request side.
func TestTypedReadRows_Cell_EmptyByteQualifierEmptyValueAndLabels(t *testing.T) {
	// 0. Common variables
	row := makeTypedRow([]byte("row-cell-edge-cases"),
		makeTypedFamily("cf",
			// makeTypedColumn([]byte{}, ...) constructs a present raw_value qualifier with empty
			// bytes (Value{Kind: &Value_RawValue{RawValue: []byte{}}}), NOT a nil/unset Value.
			// Empty byte column qualifiers are a standard Bigtable pattern and must not be
			// misclassified as null.
			makeTypedColumn([]byte{}, makeTypedCell([]byte("empty-byte-qualifier-val"))),
			// An empty-but-present cell raw_value is a genuine Bigtable state and must survive
			// intact rather than being normalised away to a nil Value.
			makeTypedColumn([]byte("empty_value_col"), makeTypedCell([]byte{})),
			makeTypedColumn([]byte("labelled_col"),
				makeTypedCellWithTimestampAndLabels([]byte("payload-with-labels"), 1609459200000000, "label1", "label2"),
			),
		),
	)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, typedFlushAction("token-cell-edge-cases", []*btpb.TypedRow{row}))

	// 2. Build the request to test proxy
	req := makeProxyTypedReadRowsRequest(t, "cell-edge-cases-table")

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row}, res.Rows)
}

// TestTypedReadRows_MultiFamily_MultiColumn_MultiCell_Ordering verifies that the intra-row
// ordering guarantees from data.proto survive the round trip: TypedFamily.columns sorted by
// increasing qualifier, TypedColumn.cells sorted by decreasing timestamp, and family order
// (which the proto leaves unspecified) passed through untouched.
//
// Note that cross-row key ordering is deliberately not asserted anywhere in this suite. v1
// ReadRows rejects out-of-order keys because its chunk merger relies on key monotonicity to find
// row boundaries, and readrows_test.go has tests for both scan directions. TypedReadRows has no
// such state machine -- batches arrive as a fully serialized TypedRows proto and are parsed
// wholesale -- and typed resumption keys off resume_token alone, never off a last-seen row key.
// The reference client performs no ordering validation as a result.
func TestTypedReadRows_MultiFamily_MultiColumn_MultiCell_Ordering(t *testing.T) {
	// 0. Common variables
	// Place fam_b before fam_a so that a client which re-sorts families alphabetically would fail.
	row := makeTypedRow([]byte("row-ordering"),
		makeTypedFamily("fam_b",
			makeTypedColumn([]byte("col_x"),
				makeTypedCellWithTimestamp([]byte("vx_cell"), 1000),
			),
		),
		makeTypedFamily("fam_a",
			makeTypedColumn([]byte("col_1"),
				makeTypedCellWithTimestamp([]byte("v1_newest"), 3000),
				makeTypedCellWithTimestamp([]byte("v1_middle"), 2000),
				makeTypedCellWithTimestamp([]byte("v1_oldest"), 1000),
			),
			makeTypedColumn([]byte("col_2"),
				makeTypedCellWithTimestamp([]byte("v2_cell"), 1000),
			),
		),
	)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, typedFlushAction("token-ordering", []*btpb.TypedRow{row}))

	// 2. Build the request to test proxy
	req := makeProxyTypedReadRowsRequest(t, "ordering-table")

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row}, res.Rows)
}

// TestTypedReadRows_ServerErrorPropagated verifies that non-retryable server errors fail
// immediately on the first attempt (without retrying) and surface their original status code.
// Retryable error exhaustion is covered separately by TestTypedReadRows_Resumption_RetryExhaustion.
func TestTypedReadRows_ServerErrorPropagated(t *testing.T) {
	for _, code := range []codes.Code{codes.PermissionDenied, codes.InvalidArgument, codes.NotFound} {
		t.Run(code.String(), func(t *testing.T) {
			recorder := make(chan *typedReadRowsReqRecord, 10)
			server := initMockServer(t)
			server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, &typedReadRowsAction{rpcError: code})

			res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "test-table"), nil)

			assert.NotNil(t, res)
			assert.Equal(t, int32(code), res.GetStatus().GetCode())
			assert.Empty(t, res.GetRows())
			assertResumeTokens(t, recorder, "")
		})
	}
}

// TestTypedReadRows_Generic_MultiStreams tests that client can have multiple concurrent
// TypedReadRows streams, each driven by its own action sequence (selected by the "opX-" table id).
func TestTypedReadRows_Generic_MultiStreams(t *testing.T) {
	// 0. Common variables
	const concurrency = 4
	const requestRecorderCapacity = 10

	expectedRows := make([][]*btpb.TypedRow, concurrency)

	// 1. Instantiate the mock server
	recorder := make(chan *typedReadRowsReqRecord, requestRecorderCapacity)
	actionSequences := make([][]*typedReadRowsAction, concurrency)
	for i := 0; i < concurrency; i++ {
		rowKey := fmt.Sprintf("op%d-row", i)
		value := fmt.Sprintf("value%d", i)
		expectedRows[i] = []*btpb.TypedRow{makeDefaultTypedRow(rowKey, value)}

		// Every stream sleeps before responding. Served serially the requests would arrive
		// roughly 2s apart, which is what turns step 4b into a real concurrency check rather
		// than a formality. Mirrors readrows_test.go's Generic_MultiStreams.
		action := typedDefaultFlushAction(rowKey, value, fmt.Sprintf("token-op%d", i))
		action.delayStr = "2s"
		actionSequences[i] = []*typedReadRowsAction{action}
	}
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFnMultiOp(recorder, actionSequences...)

	// 2. Build the requests to test proxy
	reqs := make([]*testproxypb.TypedReadRowsRequest, concurrency)
	for i := 0; i < concurrency; i++ {
		reqs[i] = makeProxyTypedReadRowsRequest(t, fmt.Sprintf("op%d-table", i))
	}

	// 3. Perform the operations via test proxy
	results := doTypedReadRowsOps(t, server, reqs, nil)

	// 4a. Check that all the requests succeeded
	assert.Equal(t, concurrency, len(results))
	checkResultOkStatus(t, results...)

	// 4b. Check that the timestamps of requests should be very close
	assert.Equal(t, concurrency, len(recorder))
	checkRequestsAreWithin(t, 1000, recorder)

	// 4c. Check the rows in the results
	for i := 0; i < concurrency; i++ {
		assertTypedRowsEqual(t, expectedRows[i], results[i].Rows)
	}
}

// TestTypedReadRows_Generic_DeadlineExceeded verifies that client-side call deadline is respected.
func TestTypedReadRows_Generic_DeadlineExceeded(t *testing.T) {
	// 0. Common variables
	action := typedDefaultFlushAction("row1", "v", "tok")
	action.delayStr = "10s"

	recorder := make(chan *typedReadRowsReqRecord, 10)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, action)

	// 2. Build the request to test proxy
	req := makeProxyTypedReadRowsRequest(t, "deadline-table")

	opts := clientOpts{
		timeout: durationpb.New(2 * time.Second),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, &opts)

	// 4. Check the response
	assert.NotNil(t, res)
	curTs := time.Now()
	if !assert.Equal(t, 1, len(recorder)) {
		t.FailNow()
	}
	loggedReq := <-recorder
	assert.Empty(t, loggedReq.req.GetResumeToken())
	runTime := curTs.Sub(loggedReq.ts)

	assert.GreaterOrEqual(t, runTime, 1500*time.Millisecond)
	assert.Less(t, runTime, 8*time.Second)

	assert.NotEqual(t, int32(codes.OK), res.GetStatus().GetCode())
	if res.GetStatus().GetCode() != int32(codes.DeadlineExceeded) {
		msg := res.GetStatus().GetMessage()
		assert.Contains(t, strings.ToLower(strings.ReplaceAll(msg, " ", "")), "deadlineexceeded")
	}
	assert.Empty(t, res.GetRows())
}

// TestTypedReadRows_Generic_CloseClient tests that in-flight requests started before client
// closing either complete or are cancelled (depending on the client language's channel-close
// semantics), while new requests issued after client closing are rejected without reaching the
// server.
func TestTypedReadRows_Generic_CloseClient(t *testing.T) {
	// 0. Common variables
	const halfBatchSize = 3
	const requestRecorderCapacity = 10
	clientID := t.Name()

	// 1. Instantiate the mock server
	// Each operation is routed to its own action sequence via the "opX-" table id prefix.
	recorder := make(chan *typedReadRowsReqRecord, requestRecorderCapacity)
	actionSequences := make([][]*typedReadRowsAction, 2*halfBatchSize)
	expectedRows := make([]*btpb.TypedRow, 2*halfBatchSize)
	for i := 0; i < 2*halfBatchSize; i++ {
		rowKey := fmt.Sprintf("op%d-row", i)
		value := fmt.Sprintf("value%d", i)
		expectedRows[i] = makeDefaultTypedRow(rowKey, value)
		action := typedDefaultFlushAction(rowKey, value, fmt.Sprintf("token-op%d", i))
		action.delayStr = "2s"
		actionSequences[i] = []*typedReadRowsAction{action}
	}
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFnMultiOp(recorder, actionSequences...)

	// 2. Build the requests to test proxy
	reqsBatchOne := make([]*testproxypb.TypedReadRowsRequest, halfBatchSize) // Will be finished
	reqsBatchTwo := make([]*testproxypb.TypedReadRowsRequest, halfBatchSize) // Will be rejected by client
	for i := 0; i < halfBatchSize; i++ {
		reqsBatchOne[i] = makeProxyTypedReadRowsRequest(t, fmt.Sprintf("op%d-table", i))
		reqsBatchTwo[i] = makeProxyTypedReadRowsRequest(t, fmt.Sprintf("op%d-table", i+halfBatchSize))
	}

	// 3. Perform the operations via test proxy
	setUp(t, server, clientID, nil)
	defer tearDown(t, server, clientID)

	closeClientAfter := time.Second
	resultsBatchOne := doTypedReadRowsOpsCore(t, clientID, reqsBatchOne, &closeClientAfter)
	resultsBatchTwo := doTypedReadRowsOpsCore(t, clientID, reqsBatchTwo, nil)

	// 4a. Check that server only receives batch-one requests
	assert.Equal(t, halfBatchSize, len(recorder))

	// 4b. Check that all the batch-one requests succeeded or were cancelled
	checkResultOkOrCancelledStatus(t, resultsBatchOne...)
	for i := 0; i < halfBatchSize; i++ {
		assert.NotNil(t, resultsBatchOne[i])
		if resultsBatchOne[i] == nil {
			continue
		}
		if resultsBatchOne[i].GetStatus().GetCode() == int32(codes.Canceled) {
			continue
		}
		assertTypedRowsEqual(t, []*btpb.TypedRow{expectedRows[i]}, resultsBatchOne[i].Rows)
	}

	// 4c. Check that all the batch-two requests failed at the proxy level:
	// the proxy tries to use close client. Client and server have nothing to blame.
	for i := 0; i < halfBatchSize; i++ {
		if resultsBatchTwo[i] == nil {
			continue
		}
		assertTypedReadRowsFailure(t, resultsBatchTwo[i], "expected post-close request to fail")
	}
}
