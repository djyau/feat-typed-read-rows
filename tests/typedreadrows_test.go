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
	"google.golang.org/grpc/status"
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
	row4 := dummyTypedRow("row4", "val4")

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

// TestTypedReadRows_ArbitraryChunkFragmentation verifies stream reassembly across various arbitrary
// chunk sizes, including the degenerate case of one byte per response.
func TestTypedReadRows_ArbitraryChunkFragmentation(t *testing.T) {
	row1 := makeTypedRow([]byte("row1-long-key-for-chunking-test"),
		makeTypedFamily("cf-data",
			makeTypedColumn([]byte("col-a"), makeTypedCell([]byte("payload-value-1234567890abcdefghijklmnopqrstuvwxyz"))),
			makeTypedColumn([]byte("col-b"), makeTypedCell([]byte("another-value-ABCDEFGHIJKLMNOPQRSTUVWXYZ"))),
		),
	)
	row2 := makeTypedRow([]byte("row2-long-key-for-chunking-test"),
		makeTypedFamily("cf-data",
			makeTypedColumn([]byte("col-c"), makeTypedCell([]byte("third-value-!@#$%^&*()_+"))),
		),
	)

	data := serializeTypedRows(row1, row2)

	for _, chunkSize := range []int{1, 2, 3, 7, 15, 23} {
		t.Run(fmt.Sprintf("ChunkSize_%d", chunkSize), func(t *testing.T) {
			responses := chunkedTypedResponses(data, chunkSize, []byte("token-frag"))

			server := initMockServer(t)
			server.TypedReadRowsFn = mockTypedReadRowsFn(nil, responsesToActions(responses...)...)

			res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "test-table"), nil)

			checkResultOkStatus(t, res)
			assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
		})
	}
}

// TestTypedReadRows_Checksum_Corrupt_Fails verifies that corrupted checksum triggers an error.
func TestTypedReadRows_Checksum_Corrupt_Fails(t *testing.T) {
	resp := makeCorruptFlushResponse("token-corrupt-crc", []*btpb.TypedRow{dummyTypedRow("corrupt-crc-row", "some-data")})

	server := initMockServer(t)
	// A closure rather than mockTypedReadRowsFn: the client retries on checksum mismatch, and
	// this must serve the same corrupt response on every attempt so the failure is the client
	// exhausting retries. A one-shot action queue would hand the retry an empty stream, which
	// the client reports as a successful read of zero rows.
	server.TypedReadRowsFn = func(req *btpb.TypedReadRowsRequest, srv btpb.Bigtable_TypedReadRowsServer) error {
		return srv.Send(resp)
	}

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "test-table"), nil)

	assertTypedReadRowsFailure(t, res, "expected failure on corrupt checksum")
}

// TestTypedReadRows_Reset_WithDataInSameResponse verifies the ordering the protocol mandates when
// reset arrives alongside other fields: "any data buffered since the last non-empty `resume_token`
// must be discarded before the other parts of this message, if any, are handled." Here the reset,
// a fresh batch, and the flush that commits it all travel in a single response. A client that
// appended the batch first and applied the reset afterwards would discard the new data instead of
// the stale data, yielding row1 alone.
func TestTypedReadRows_Reset_WithDataInSameResponse(t *testing.T) {
	row1 := dummyTypedRow("row1", "committed-1")
	row2 := dummyTypedRow("row2", "committed-2")

	resetWithData := dummyFlushResponse("row2", "committed-2", "token-row2", row1)
	resetWithData.Response.Reset_ = true

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil,
		dummyTypedAction("row1", "committed-1", "token-row1"),
		dummyUncommittedAction("row2", "uncommitted-2"),
		&typedReadRowsAction{response: resetWithData},
	)

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "test-table"), nil)

	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_TargetRouting verifies table_name, authorized_view_name, and
// materialized_view_name routing in both the request payload and x-goog-request-params header.
func TestTypedReadRows_TargetRouting(t *testing.T) {
	row := dummyTypedRow("row-target", "v")
	instanceResource := fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID)

	tests := []struct {
		name           string
		targetReq      *btpb.TypedReadRowsRequest
		wantHeaderPart string
		verifyReq      func(t *testing.T, req *btpb.TypedReadRowsRequest)
	}{
		{
			name:           "TableName",
			targetReq:      makeTypedReadRowsRequest(buildTableName("target-table")),
			wantHeaderPart: buildTableName("target-table"),
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
			wantHeaderPart: buildTableName("target-table"),
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
			wantHeaderPart: instanceResource,
			verifyReq: func(t *testing.T, req *btpb.TypedReadRowsRequest) {
				assert.Equal(t, buildMaterializedViewName("target-mv"), req.GetMaterializedViewName())
				assert.Empty(t, req.GetTableName())
				assert.Empty(t, req.GetAuthorizedViewName())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := make(chan *typedReadRowsReqRecord, 1)
			mdRecords := make(chan metadata.MD, 1)

			server := initMockServer(t)
			server.TypedReadRowsFn = mockTypedReadRowsFnWithMetadata(recorder, mdRecords, dummyTypedAction("row-target", "v", "token"))

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
				assert.True(t,
					strings.Contains(params[0], tc.wantHeaderPart) || strings.Contains(params[0], url.QueryEscape(tc.wantHeaderPart)),
					"expected x-goog-request-params %q to contain %q", params[0], tc.wantHeaderPart)
			}
		})
	}
}

// TestTypedReadRows_ServerErrorPropagated verifies that non-retryable server errors fail
// immediately on the first attempt (without retrying) and surface their original status code.
// Retryable error exhaustion is covered separately by TestTypedReadRows_Resumption_RetryExhaustion.
func TestTypedReadRows_ServerErrorPropagated(t *testing.T) {
	for _, code := range []codes.Code{codes.PermissionDenied, codes.InvalidArgument, codes.NotFound} {
		t.Run(code.String(), func(t *testing.T) {
			recorder := make(chan *typedReadRowsReqRecord, 2)
			server := initMockServer(t)
			server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, &typedReadRowsAction{rpcError: code})

			res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "test-table"), nil)

			assert.NotNil(t, res)
			assert.Equal(t, int32(code), res.GetStatus().GetCode())
			assertResumeTokens(t, recorder, "")
		})
	}
}

// TestTypedReadRows_MissingTarget_InvalidArgument verifies that a request with no target returns
// INVALID_ARGUMENT without sending an RPC to the server.
func TestTypedReadRows_MissingTarget_InvalidArgument(t *testing.T) {
	recorder := make(chan *typedReadRowsReqRecord, 1)
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder)

	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  &btpb.TypedReadRowsRequest{}, // No target set
	}

	res := doTypedReadRowsOp(t, server, req, nil)

	assert.NotNil(t, res)
	assert.Equal(t, int32(codes.InvalidArgument), res.GetStatus().GetCode())
	assert.Empty(t, recorder, "server must not be called when target is missing")
}

// TestTypedReadRows_EmptyTable_Success verifies that an empty stream returns 0 rows and OK status.
func TestTypedReadRows_EmptyTable_Success(t *testing.T) {
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil)

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "empty-table"), nil)

	checkResultOkStatus(t, res)
	assert.Empty(t, res.Rows)
}

// TestTypedReadRows_UnflushedDataAtStreamEnd_Fails verifies that batch data which is never
// committed by a Flush is not yielded to the caller, even though the stream ends cleanly, while
// rows committed by an earlier Flush before the incomplete tail are preserved.
func TestTypedReadRows_UnflushedDataAtStreamEnd_Fails(t *testing.T) {
	committedRow := dummyTypedRow("row-committed", "val-committed")
	uncommittedRow := dummyTypedRow("row-no-flush", "val-no-flush")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		typedFlushAction("token-1", []*btpb.TypedRow{committedRow}),
		typedUncommittedAction(uncommittedRow),
	)

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "test-table"), nil)

	assert.NotNil(t, res)
	assert.NotEqual(t, int32(codes.OK), res.GetStatus().GetCode(), "expected failure when the stream ends with unflushed data")
	assertTypedRowsEqual(t, []*btpb.TypedRow{committedRow}, res.Rows)
	assertResumeTokens(t, recorder, "")
}

// TestTypedReadRows_Reset_BeforeAnyFlush verifies that reset=true discards a partially received
// unparseable fragment even when no resume_token has been seen yet, so there is no earlier
// checkpoint to fall back to.
//
// If the client eagerly parsed chunks before flush or forgot to clear its raw byte buffer on reset,
// the surviving partial bytes would be concatenated with the following batch and fail to parse.
func TestTypedReadRows_Reset_BeforeAnyFlush(t *testing.T) {
	garbageRow := dummyTypedRow("row-discarded", "this-value-is-long-enough-to-fragment")
	validRow := dummyTypedRow("row-after-reset", "v")

	garbageData := serializeTypedRows(garbageRow)
	partial := garbageData[:len(garbageData)/3]

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil,
		&typedReadRowsAction{
			response: &btpb.TypedReadRowsResponse{
				Response: &btpb.PartialRowResponse{
					PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
						TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: partial},
					},
				},
			},
		},
		typedResetAction(),
		dummyTypedAction("row-after-reset", "v", "token-after-reset"),
	)

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "test-table"), nil)

	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{validRow}, res.Rows)
}

// TestTypedReadRows_InvalidProtobuf_Fails verifies that a batch whose bytes are not a valid
// TypedRows message is rejected rather than surfaced as rows.
//
// The flush carries a valid CRC32C checksum over the malformed bytes so that the checksum passes
// and the failure is isolated to protobuf parsing (rather than failing earlier on checksum
// validation). Unlike Checksum_Corrupt_Fails, a one-shot action queue is sufficient because a
// protobuf parse failure is not retryable.
func TestTypedReadRows_InvalidProtobuf_Fails(t *testing.T) {
	badBytes := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	crc := trrMetaChecksum(badBytes)
	resp := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{
					BatchData: badBytes,
				},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crc,
				ResumeToken: []byte("token"),
			},
		},
	}

	recorder := make(chan *typedReadRowsReqRecord, 2)
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, &typedReadRowsAction{response: resp})

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "test-table"), nil)

	assertTypedReadRowsFailure(t, res, "expected failure on unparseable batch data")
	assertResumeTokens(t, recorder, "")
}

// TestTypedReadRows_Generic_Headers tests that TypedReadRows request sends client and resource info,
// app_profile_id in both the request payload and headers, and the ReverseScans feature flag.
func TestTypedReadRows_Generic_Headers(t *testing.T) {
	const profileID = "test_profile"
	tableName := buildTableName("table-headers")
	row := dummyTypedRow("row-headers", "v")

	recorder := make(chan *typedReadRowsReqRecord, 1)
	mdRecords := make(chan metadata.MD, 1)

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFnWithMetadata(recorder, mdRecords, dummyTypedAction("row-headers", "v", "token"))

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
	assert.True(t, ff != nil && ff.ReverseScans, "client must enable ReverseScans feature flag")
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
		expectedRows[i] = []*btpb.TypedRow{dummyTypedRow(rowKey, value)}

		// Every stream sleeps before responding. Served serially the requests would arrive
		// roughly 2s apart, which is what turns step 4b into a real concurrency check rather
		// than a formality. Mirrors readrows_test.go's Generic_MultiStreams.
		action := dummyTypedAction(rowKey, value, fmt.Sprintf("token-op%d", i))
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
			recorder := make(chan *typedReadRowsReqRecord, 1)

			server := initMockServer(t)
			server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, dummyTypedAction("row1", "v", "token"))

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

// TestTypedReadRows_Resumption_DiscardsUncommittedOnDisconnect verifies that uncommitted rows
// received after the last resume_token are discarded when the stream disconnects and reconnects.
func TestTypedReadRows_Resumption_DiscardsUncommittedOnDisconnect(t *testing.T) {
	// 0. Common variables
	committedRow1 := dummyTypedRow("committed-1", "v1")
	committedRow2 := dummyTypedRow("committed-2", "v2")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("committed-1", "v1", "token1"),
		dummyUncommittedAction("uncommitted-ghost", "ghost"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		dummyTypedAction("committed-2", "v2", "token2", committedRow1),
	)

	// 2. Build the request to test proxy
	req := makeProxyTypedReadRowsRequest(t, "resumption-uncommitted-table")

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertResumeTokens(t, recorder, "", "token1")
	assertTypedRowsEqual(t, []*btpb.TypedRow{committedRow1, committedRow2}, res.Rows)
}

// TestTypedReadRows_Resumption_DisconnectMidChunk verifies that if a stream drops in the middle
// of receiving multi-chunk fragmented batch data (before flush/token), the client purges its
// partial uncommitted chunk buffer upon reconnection rather than concatenating stale fragments.
func TestTypedReadRows_Resumption_DisconnectMidChunk(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row-committed", "v1")
	row2 := dummyTypedRow("row-chunked-resumed", "a-longer-value-for-chunking-1234567890")

	data1 := serializeTypedRows(row1)
	data2 := serializeTypedRows(row2)
	chunks := chunkedTypedResponses(data2, len(data2)/3, []byte("token2"), data1)

	actions := []*typedReadRowsAction{dummyTypedAction("row-committed", "v1", "token1")}
	actions = append(actions, responsesToActions(chunks[:2]...)...)
	actions = append(actions, &typedReadRowsAction{rpcError: codes.Unavailable})
	actions = append(actions, responsesToActions(chunks...)...)

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, actions...)

	// 2. Build the request to test proxy
	req := makeProxyTypedReadRowsRequest(t, "midchunk-disconnect-table")

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertResumeTokens(t, recorder, "", "token1")
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_Resumption_MultipleSequentialDisconnects verifies that multiple cascading
// stream disconnections (covering both UNAVAILABLE and ABORTED retryable status codes, including
// an intermediate retry attempt that fails immediately before emitting any data or token) preserve
// the latest resume_token and running checksum across attempts and accumulate the full set of rows
// without loss or duplication.
func TestTypedReadRows_Resumption_MultipleSequentialDisconnects(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row-seq-1", "v1")
	row2 := dummyTypedRow("row-seq-2", "v2")
	row3 := dummyTypedRow("row-seq-3", "v3")

	recorder := make(chan *typedReadRowsReqRecord, 4)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("row-seq-1", "v1", "token-seq-1"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		dummyTypedAction("row-seq-2", "v2", "token-seq-2", row1),
		&typedReadRowsAction{rpcError: codes.Aborted},
		&typedReadRowsAction{rpcError: codes.Unavailable}, // Immediate failure before any response on attempt 3
		dummyTypedAction("row-seq-3", "v3", "token-seq-3", row1, row2),
	)

	// 2. Build the request to test proxy
	req := makeProxyTypedReadRowsRequest(t, "seq-disconnect-table")

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertResumeTokens(t, recorder, "", "token-seq-1", "token-seq-2", "token-seq-2")
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2, row3}, res.Rows)
}

// TestTypedReadRows_Resumption_RetryExhaustion verifies that when retryable errors exceed
// the maximum retry limit (e.g. 3 retries), the client terminates and returns the UNAVAILABLE status.
func TestTypedReadRows_Resumption_RetryExhaustion(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row-before-outage", "v1")

	attempts := 0

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = func(req *btpb.TypedReadRowsRequest, srv btpb.Bigtable_TypedReadRowsServer) error {
		attempts++
		if attempts == 1 {
			assert.Empty(t, req.GetResumeToken())
			// Commit row1
			if err := srv.Send(makeFlushResponse("token-before-outage", []*btpb.TypedRow{row1})); err != nil {
				return err
			}
			return status.Error(codes.Unavailable, "permanent backend failure")
		}
		// Continuous failure on retries
		assert.Equal(t, []byte("token-before-outage"), req.GetResumeToken(), "retry attempt %d must resume from token-before-outage", attempts)
		return status.Error(codes.Unavailable, "still failing")
	}

	// 2. Build the request to test proxy
	req := makeProxyTypedReadRowsRequest(t, "exhaustion-table")

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	// Ensure the client actually retried rather than giving up on the first error: at least
	// 1 initial attempt + 3 retries.
	//
	// This encodes a retry budget rather than a protocol rule. A conformant client configured
	// with a smaller budget would fail here through no fault of its own, so revisit this bound
	// if the suite is run against a client other than Java.
	assert.GreaterOrEqual(t, attempts, 4)
	// Unlike the other failure tests, rows are expected here rather than asserted empty: row1
	// was committed by a flush with a resume_token before the outage began, so it is durable and
	// must survive the terminal error. Only *uncommitted* data must be withheld.
	assert.NotNil(t, res)
	assert.Equal(t, int32(codes.Unavailable), res.GetStatus().GetCode())
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1}, res.Rows)
}

// TestTypedReadRows_Resumption_WithResetInNewStream verifies that in-stream resets function
// correctly even after the client has resumed from a prior disconnect.
func TestTypedReadRows_Resumption_WithResetInNewStream(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row1", "committed-1")
	row2 := dummyTypedRow("row2", "committed-2")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("row1", "committed-1", "token-r1"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		dummyUncommittedAction("row2", "uncommitted-2"),
		typedResetAction(),
		dummyTypedAction("row2", "committed-2", "token-r2", row1),
	)

	// 2. Build the request to test proxy
	req := makeProxyTypedReadRowsRequest(t, "resumed-reset-table")

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertResumeTokens(t, recorder, "", "token-r1")
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_Resumption_CancelAfterRows verifies that the user-visible stream can be
// cancelled after cancel_after_rows rows have been surfaced across a stream resumption, and that
// only those rows are returned.
func TestTypedReadRows_Resumption_CancelAfterRows(t *testing.T) {
	row1 := dummyTypedRow("row-cancel-1", "v1")
	row2 := dummyTypedRow("row-cancel-2", "v2")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("row-cancel-1", "v1", "token-c1"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		dummyTypedAction("row-cancel-2", "v2", "token-c2", row1),
		dummyTypedAction("row-cancel-3", "v3", "token-c3", row1, row2),
	)

	req := makeProxyTypedReadRowsRequest(t, "cancel-resumption-table")
	req.CancelAfterRows = 2

	res := doTypedReadRowsOp(t, server, req, nil)

	checkResultOkOrCancelledStatus(t, res)
	assertResumeTokens(t, recorder, "", "token-c1")
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_Resumption_NonRetryableErrorAfterDrop verifies that if the stream drops
// with a retryable error, but the reconnected attempt encounters a non-retryable error
// (e.g. PermissionDenied), the client preserves committed rows and surfaces the error status.
func TestTypedReadRows_Resumption_NonRetryableErrorAfterDrop(t *testing.T) {
	row1 := dummyTypedRow("row-committed-before-perm-denied", "v1")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("row-committed-before-perm-denied", "v1", "token-perm-1"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		&typedReadRowsAction{rpcError: codes.PermissionDenied},
	)

	req := makeProxyTypedReadRowsRequest(t, "permdenied-resumption-table")
	res := doTypedReadRowsOp(t, server, req, nil)

	assert.NotNil(t, res)
	assert.Equal(t, int32(codes.PermissionDenied), res.GetStatus().GetCode())
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1}, res.Rows)
	assertResumeTokens(t, recorder, "", "token-perm-1")
}

// TestTypedReadRows_Resumption_InitialTransientFailure verifies that if the stream fails with
// an UNAVAILABLE error immediately on the initial call before any data or tokens have been sent,
// the client safely retries with the original request (empty resume token) and succeeds.
func TestTypedReadRows_Resumption_InitialTransientFailure(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row-init-fail", "v1")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		&typedReadRowsAction{rpcError: codes.Unavailable},
		dummyTypedAction("row-init-fail", "v1", "token-init-success"),
	)

	// 2. Build the request to test proxy
	req := makeProxyTypedReadRowsRequest(t, "initial-transient-table")

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertResumeTokens(t, recorder, "", "")
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1}, res.Rows)
}

// TestTypedReadRows_Resumption_FlushInSeparateMessage verifies that when the server separates
// a multi-chunk data batch and the flush indicator into distinct messages, the client buffers
// the chunks, commits on the subsequent standalone flush message, and resumes properly if dropped
// thereafter.
func TestTypedReadRows_Resumption_FlushInSeparateMessage(t *testing.T) {
	row1 := dummyTypedRow("row-sep-1", "value-long-enough-to-split-across-two-chunks")
	row2 := dummyTypedRow("row-sep-2", "v2")

	data1 := serializeTypedRows(row1)
	mid := len(data1) / 2

	recorder := make(chan *typedReadRowsReqRecord, 2)

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		&typedReadRowsAction{
			response: &btpb.TypedReadRowsResponse{
				Response: &btpb.PartialRowResponse{
					PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
						TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data1[:mid]},
					},
				},
			},
		},
		&typedReadRowsAction{
			response: &btpb.TypedReadRowsResponse{
				Response: &btpb.PartialRowResponse{
					PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
						TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data1[mid:]},
					},
				},
			},
		},
		&typedReadRowsAction{
			response: &btpb.TypedReadRowsResponse{
				Response: &btpb.PartialRowResponse{
					Flush: makeFlushResponse("token-sep-msg-1", []*btpb.TypedRow{row1}).GetResponse().GetFlush(),
				},
			},
		},
		&typedReadRowsAction{rpcError: codes.Unavailable},
		typedUncommittedAction(row2),
		&typedReadRowsAction{
			response: &btpb.TypedReadRowsResponse{
				Response: &btpb.PartialRowResponse{
					Flush: makeFlushResponse("token-sep-msg-2", []*btpb.TypedRow{row2}, row1).GetResponse().GetFlush(),
				},
			},
		},
	)

	req := makeProxyTypedReadRowsRequest(t, "sep-msg-table")
	res := doTypedReadRowsOp(t, server, req, nil)

	checkResultOkStatus(t, res)
	assertResumeTokens(t, recorder, "", "token-sep-msg-1")
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_Resumption_EmptyTableWithToken verifies that an empty table scan emitting
// heartbeat resume_tokens both before a stream drop and on the resumed stream reconnects with
// the first token, filters out the heartbeat marker rows, and completes with 0 rows.
func TestTypedReadRows_Resumption_EmptyTableWithToken(t *testing.T) {
	recorder := make(chan *typedReadRowsReqRecord, 2)

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		typedHeartbeatAction("token-empty-table-1"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		typedHeartbeatAction("token-empty-table-2"),
	)

	req := makeProxyTypedReadRowsRequest(t, "empty-table-resumption")
	res := doTypedReadRowsOp(t, server, req, nil)

	checkResultOkStatus(t, res)
	assertResumeTokens(t, recorder, "", "token-empty-table-1")
	assert.Empty(t, res.Rows)
}

// TestTypedReadRows_MissingInitialTableSchema_Fails verifies that when the server omits TableSchema
// on the very first message carrying data, the client fails fast rather than processing data.
func TestTypedReadRows_MissingInitialTableSchema_Fails(t *testing.T) {
	recorder := make(chan *typedReadRowsReqRecord, 2)
	server := initMockServer(t)
	server.DisableTypedReadRowsAutoSchema = true
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, dummyTypedAction("row-no-schema", "v", "token-no-schema"))

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "missing-schema-table"), nil)

	assertTypedReadRowsFailure(t, res, "expected failure when the initial TableSchema is missing")
	assertResumeTokens(t, recorder, "")
}

// TestTypedReadRows_Resumption_MissingTableSchemaOnRetry_Fails verifies that every reconnected
// stream attempt must also provide TableSchema on its first response, and that if the resumed
// stream omits TableSchema, the read fails while preserving rows committed on the earlier attempt.
func TestTypedReadRows_Resumption_MissingTableSchemaOnRetry_Fails(t *testing.T) {
	row1 := dummyTypedRow("row1", "v1")
	resp1 := makeFlushResponse("token-1", []*btpb.TypedRow{row1})
	resp1.TableSchema = &btpb.TableSchema{}
	resp2NoSchema := dummyFlushResponse("row2", "v2", "token-2", row1)

	recorder := make(chan *typedReadRowsReqRecord, 2)

	server := initMockServer(t)
	server.DisableTypedReadRowsAutoSchema = true
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		&typedReadRowsAction{response: resp1},
		&typedReadRowsAction{rpcError: codes.Unavailable},
		&typedReadRowsAction{response: resp2NoSchema},
	)

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "missing-schema-on-retry-table"), nil)

	assert.NotNil(t, res)
	assert.NotEqual(t, int32(codes.OK), res.GetStatus().GetCode(), "expected failure when the resumed stream omits TableSchema on its first response")
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1}, res.Rows)
	assertResumeTokens(t, recorder, "", "token-1")
}

// TestTypedReadRows_SchemaRowKeyKindMismatch_Fails verifies that the client enforces agreement
// between TableSchema and the kind of Value carrying each row key, in both directions:
//
//   - row_key_schema present => row keys must arrive as array_value
//   - row_key_schema absent  => row keys must arrive as raw_value
//
// That kind check is the only part of TableSchema the reference client acts on. Field names,
// types and arity are not validated -- a one-field schema accepts a four-element key -- so a
// mismatch in the row key's Value kind is the sole schema violation a client can detect, and
// both directions of it are worth pinning down.
func TestTypedReadRows_SchemaRowKeyKindMismatch_Fails(t *testing.T) {
	structuredSchema := makeStructRowKeySchema(strType())
	rawKeyRow := dummyTypedRow("row-raw-key", "v")
	arrayKeyRow := makeStructuredTypedRow(
		[]*btpb.Value{strVal("part1-val")},
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v")))),
	)

	testCases := []struct {
		name   string
		schema *btpb.TableSchema
		row    *btpb.TypedRow
	}{
		{"structured schema with raw_value key", structuredSchema, rawKeyRow},
		{"unstructured schema with array_value key", &btpb.TableSchema{}, arrayKeyRow},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp := makeFlushResponse("token-mismatch", []*btpb.TypedRow{tc.row})
			resp.TableSchema = tc.schema

			recorder := make(chan *typedReadRowsReqRecord, 2)
			server := initMockServer(t)
			server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, &typedReadRowsAction{response: resp})

			req := makeProxyTypedReadRowsRequest(t, "schema-mismatch-table")
			res := doTypedReadRowsOp(t, server, req, nil)

			assertTypedReadRowsFailure(t, res, "expected failure for "+tc.name)
			assertResumeTokens(t, recorder, "")
		})
	}
}

// TestTypedReadRows_InvalidRowKeySchemaType_Fails verifies that when TableSchema.row_key_schema
// contains a field with an unset/invalid Type (KIND_NOT_SET), the client rejects the schema and
// fails the read without retrying.
func TestTypedReadRows_InvalidRowKeySchemaType_Fails(t *testing.T) {
	invalidSchema := makeStructRowKeySchema(&btpb.Type{})
	row := makeStructuredTypedRow(
		[]*btpb.Value{strVal("part1-val")},
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v")))),
	)
	resp := makeFlushResponse("token-invalid-type", []*btpb.TypedRow{row})
	resp.TableSchema = invalidSchema

	recorder := make(chan *typedReadRowsReqRecord, 2)
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, &typedReadRowsAction{response: resp})

	req := makeProxyTypedReadRowsRequest(t, "invalid-schema-type-table")
	res := doTypedReadRowsOp(t, server, req, nil)

	assertTypedReadRowsFailure(t, res, "expected failure when row_key_schema has an invalid/unset field type")
	assertResumeTokens(t, recorder, "")
}

// TestTypedReadRows_Cell_EmptyValueAndLabels covers the two cell-payload edge cases that a server
// can actually produce: a cell whose raw_value is present but zero-length, and a cell carrying an
// explicit timestamp plus labels.
//
// Note on scope: on the server side a TypedCell's value is always raw_value, and a TypedColumn's
// qualifier is always raw_value -- no other Value kind is reachable, and a cell always carries a
// value rather than leaving the field unset. So there is deliberately no coverage here of
// string_value/int_value/etc. cells or of a nil cell value; such a stream cannot occur, and
// asserting on it would invite client authors to implement handling for cases that never arrive.
// Structured array_value only ever appears in row keys (see MidStream_SchemaEvolution and
// StructuredRowKeys_Resumption) and in TypedRowSet.row_prefixes on the request side.
func TestTypedReadRows_Cell_EmptyValueAndLabels(t *testing.T) {
	// 0. Common variables
	row := makeTypedRow([]byte("row-cell-edge-cases"),
		makeTypedFamily("cf",
			// An empty-but-present cell value is a genuine Bigtable state and must survive intact
			// rather than being normalised away to a nil Value.
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
	row := makeTypedRow([]byte("row-ordering"),
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
		makeTypedFamily("fam_b",
			makeTypedColumn([]byte("col_x"),
				makeTypedCellWithTimestamp([]byte("vx_cell"), 1000),
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

// TestTypedReadRows_ConsecutiveResets verifies that reset=true discards uncommitted data buffered
// since the last resume_token while preserving earlier committed rows and their running checksum,
// and that a second back-to-back reset arriving with an already-empty buffer is a harmless no-op.
func TestTypedReadRows_ConsecutiveResets(t *testing.T) {
	row1 := dummyTypedRow("row1", "committed-1")
	row2 := dummyTypedRow("row2", "committed-2")

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil,
		dummyTypedAction("row1", "committed-1", "token-row1"),
		dummyUncommittedAction("row2", "uncommitted-2"),
		typedResetAction(),
		typedResetAction(),
		dummyTypedAction("row2", "committed-2", "token-row2", row1),
	)

	req := makeProxyTypedReadRowsRequest(t, "consecutive-resets-table")
	res := doTypedReadRowsOp(t, server, req, nil)

	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_Resumption_ReverseScan verifies resumption behavior when reading in reverse order.
func TestTypedReadRows_Resumption_ReverseScan(t *testing.T) {
	// 0. Common variables
	row3 := dummyTypedRow("row-03", "v3")
	row2 := dummyTypedRow("row-02", "v2")
	row1 := dummyTypedRow("row-01", "v1")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("row-03", "v3", "token-r3"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		typedFlushAction("token-r1", []*btpb.TypedRow{row2, row1}, row3),
	)

	// 2. Build the request to test proxy
	req := makeProxyTypedReadRowsRequest(t, "reverse-resumption-table")
	req.Request.Reversed = true

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	reqs := assertResumeTokens(t, recorder, "", "token-r3")
	assert.True(t, reqs[0].GetReversed())
	assert.True(t, reqs[1].GetReversed())
	assertTypedRowsEqual(t, []*btpb.TypedRow{row3, row2, row1}, res.Rows)
}

// TestTypedReadRows_Resumption_RowsLimitUnmodified verifies that on retryable disconnect,
// the client replays the original rows_limit unchanged (because the server encodes rows_read
// inside the resume_token and nets the original rows_limit against it on resumption).
func TestTypedReadRows_Resumption_RowsLimitUnmodified(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row1", "v1")
	row2 := dummyTypedRow("row2", "v2")
	row3 := dummyTypedRow("row3", "v3")
	row4 := dummyTypedRow("row4", "v4")
	row5 := dummyTypedRow("row5", "v5")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		typedFlushAction("token-r2", []*btpb.TypedRow{row1, row2}),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		typedFlushAction("token-r5", []*btpb.TypedRow{row3, row4, row5}, []*btpb.TypedRow{row1, row2}),
	)

	// 2. Build the request to test proxy
	req := makeProxyTypedReadRowsRequest(t, "rows-limit-table")
	req.Request.RowsLimit = 5

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	reqs := assertResumeTokens(t, recorder, "", "token-r2")
	assert.Equal(t, int64(5), reqs[0].GetRowsLimit())
	assert.Equal(t, int64(5), reqs[1].GetRowsLimit())
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2, row3, row4, row5}, res.Rows)
}

// TestTypedReadRows_Resumption_RowsLimitFulfilledBeforeDisconnect verifies that if rows_limit
// rows are flushed before a stream disconnect occurs without OK, the client resumes with the
// original rows_limit and latest resume_token so the server can complete the stream cleanly.
func TestTypedReadRows_Resumption_RowsLimitFulfilledBeforeDisconnect(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row1", "v1")
	row2 := dummyTypedRow("row2", "v2")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		typedFlushAction("token-r2", []*btpb.TypedRow{row1, row2}),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		typedHeartbeatAction("token-r2-done"),
	)

	// 2. Build the request to test proxy
	req := makeProxyTypedReadRowsRequest(t, "rows-limit-fulfilled-table")
	req.Request.RowsLimit = 2

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	reqs := assertResumeTokens(t, recorder, "", "token-r2")
	assert.Equal(t, int64(2), reqs[0].GetRowsLimit())
	assert.Equal(t, int64(2), reqs[1].GetRowsLimit())
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_Resumption_ErrorAfterFinalData verifies that when an unbounded scan flushes all
// rows and then disconnects with a retryable error before the server closes the stream with OK, the
// client resumes from the latest resume_token and completes cleanly without duplicating rows when
// the resumed stream finishes with a trailing heartbeat.
func TestTypedReadRows_Resumption_ErrorAfterFinalData(t *testing.T) {
	row1 := dummyTypedRow("row1", "v1")
	row2 := dummyTypedRow("row2", "v2")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		typedFlushAction("token-final", []*btpb.TypedRow{row1, row2}),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		typedHeartbeatAction("token-final-done"),
	)

	req := makeProxyTypedReadRowsRequest(t, "error-after-final-data-table")
	res := doTypedReadRowsOp(t, server, req, nil)

	checkResultOkStatus(t, res)
	assertResumeTokens(t, recorder, "", "token-final")
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_Resumption_CorruptChecksumRetriedFromLastGoodToken verifies that a corrupt
// flush checksum triggers a retry from the last good resume_token rather than the rejected one.
func TestTypedReadRows_Resumption_CorruptChecksumRetriedFromLastGoodToken(t *testing.T) {
	row1 := dummyTypedRow("row1", "v1")
	row2 := dummyTypedRow("row2", "v2")

	recorder := make(chan *typedReadRowsReqRecord, 2)
	attempt := 0

	server := initMockServer(t)
	server.TypedReadRowsFn = func(req *btpb.TypedReadRowsRequest, srv btpb.Bigtable_TypedReadRowsServer) error {
		saveReqRecord(recorder, &typedReadRowsReqRecord{req: req, ts: time.Now()})
		attempt++
		if attempt == 1 {
			if err := srv.Send(makeFlushResponse("token-1", []*btpb.TypedRow{row1})); err != nil {
				return err
			}
			return srv.Send(makeCorruptFlushResponse("token-2", []*btpb.TypedRow{row2}, row1))
		}
		return srv.Send(makeFlushResponse("token-2", []*btpb.TypedRow{row2}, row1))
	}

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "corrupt-checksum-retry-table"), nil)

	checkResultOkStatus(t, res)
	assertResumeTokens(t, recorder, "", "token-1")
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_Resumption_CorruptFirstBatchRetriedFromStart verifies that when the very first
// batch on a stream has a corrupt checksum (before any valid flush has occurred), the client
// rejects the corrupt flush's resume_token and retries from the start of the scan with an empty
// resume_token.
func TestTypedReadRows_Resumption_CorruptFirstBatchRetriedFromStart(t *testing.T) {
	row1 := dummyTypedRow("row1", "v1")

	recorder := make(chan *typedReadRowsReqRecord, 2)
	attempt := 0

	server := initMockServer(t)
	server.TypedReadRowsFn = func(req *btpb.TypedReadRowsRequest, srv btpb.Bigtable_TypedReadRowsServer) error {
		saveReqRecord(recorder, &typedReadRowsReqRecord{req: req, ts: time.Now()})
		attempt++
		if attempt == 1 {
			return srv.Send(makeCorruptFlushResponse("token-1", []*btpb.TypedRow{row1}))
		}
		return srv.Send(makeFlushResponse("token-1", []*btpb.TypedRow{row1}))
	}

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "corrupt-first-batch-retry-table"), nil)

	checkResultOkStatus(t, res)
	assertResumeTokens(t, recorder, "", "")
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1}, res.Rows)
}

// TestTypedReadRows_MissingResumeToken_Fails verifies that a Flush with an empty resume_token is
// rejected both when committing a non-empty batch and when arriving as an empty heartbeat Flush.
//
// Caveat for other client implementers: this rule comes from the reference (Java) client, not
// from data.proto. PartialRowResponse.Flush declares resume_token as a plain proto3 `bytes`
// field -- which has no presence, so empty and absent are indistinguishable -- and nowhere states
// that it must be non-empty. The sibling ExecuteQuery protocol explicitly permits the equivalent
// state: PartialResultSet's normative pseudocode treats `batch_checksum` and `resume_token` as
// two independent conditionals, so "batch verified but not yet yieldable" is legal there.
// TypedReadRows merged those two fields into a single Flush sub-message, which implies they
// always co-occur, but that intent was never written down. Pending a spec clarification, the
// suite follows the reference client.
func TestTypedReadRows_MissingResumeToken_Fails(t *testing.T) {
	row := dummyTypedRow("row1", "v")
	data := serializeTypedRows(row)
	crc := trrMetaChecksum(data)

	tests := []struct {
		name string
		resp *btpb.TypedReadRowsResponse
	}{
		{
			name: "non_empty_batch",
			resp: &btpb.TypedReadRowsResponse{
				Response: &btpb.PartialRowResponse{
					PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
						TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data},
					},
					Flush: &btpb.PartialRowResponse_Flush{
						Checksum:    &crc,
						ResumeToken: nil,
					},
				},
			},
		},
		{
			name: "empty_heartbeat_flush",
			resp: &btpb.TypedReadRowsResponse{
				Response: &btpb.PartialRowResponse{
					Flush: &btpb.PartialRowResponse_Flush{},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := make(chan *typedReadRowsReqRecord, 2)
			server := initMockServer(t)
			server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, &typedReadRowsAction{response: tc.resp})

			res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "missing-token-table"), nil)

			assertTypedReadRowsFailure(t, res, "expected failure when flush has no resume token ("+tc.name+")")
			assertResumeTokens(t, recorder, "")
		})
	}
}

// TestTypedReadRows_NonEmptyBatchWithoutChecksum_Fails verifies that a non-empty batch flushed without a checksum fails.
func TestTypedReadRows_NonEmptyBatchWithoutChecksum_Fails(t *testing.T) {
	row := dummyTypedRow("row1", "v")
	data := serializeTypedRows(row)

	recorder := make(chan *typedReadRowsReqRecord, 2)
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, &typedReadRowsAction{
		response: &btpb.TypedReadRowsResponse{
			Response: &btpb.PartialRowResponse{
				PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
					TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data},
				},
				Flush: &btpb.PartialRowResponse_Flush{
					Checksum:    nil, // Omitted checksum on non-empty batch
					ResumeToken: []byte("token-1"),
				},
			},
		},
	})

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "no-checksum-table"), nil)

	assertTypedReadRowsFailure(t, res, "expected failure when non-empty batch lacks checksum")
	assertResumeTokens(t, recorder, "")
}

// TestTypedReadRows_MidStream_SchemaEvolution verifies that the client re-reads TableSchema on
// every response rather than latching the first one it sees.
//
// The only schema property the client acts on is whether row_key_schema is present: when it is,
// row keys must arrive as array_value; when it is absent, they must arrive as raw_value. Field
// names, types and arity are never inspected -- a one-field schema accepts a four-element key --
// so varying the field list mid-stream would be entirely invisible to the client and would prove
// nothing. This test therefore flips the one bit that is observable: response 1 declares a
// row_key_schema and carries a structured key, response 2 drops the schema and carries a raw
// key. A client that cached the first schema rejects row 2 with "Unstructured row keys must be
// provided as a raw_value Value."
func TestTypedReadRows_MidStream_SchemaEvolution(t *testing.T) {
	// Response 1 declares a structured row key...
	schema1 := makeStructRowKeySchema(strType())
	// ...and response 2 withdraws it, so row keys revert to unstructured raw_value.
	schema2 := &btpb.TableSchema{}

	row1 := makeStructuredTypedRow(
		[]*btpb.Value{strVal("val1")},
		makeTypedFamily("cf",
			makeTypedColumn([]byte("col1"), makeTypedCell([]byte("val1"))),
			makeTypedColumn([]byte("col2"), makeTypedCell([]byte("12345"))),
		),
	)
	row2 := dummyTypedRow("row-unstructured", "val2")

	resp1 := makeFlushResponse("token-1", []*btpb.TypedRow{row1})
	resp1.TableSchema = schema1
	resp2 := dummyFlushResponse("row-unstructured", "val2", "token-2", row1)
	resp2.TableSchema = schema2

	server := initMockServer(t)
	// Both responses set TableSchema explicitly, so auto-schema would not fire anyway; disabling
	// it makes the intent unambiguous.
	server.DisableTypedReadRowsAutoSchema = true
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, responsesToActions(resp1, resp2)...)

	req := makeProxyTypedReadRowsRequest(t, "schema-evolution-table")
	res := doTypedReadRowsOp(t, server, req, nil)

	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_StructuredRowKeys_Resumption verifies reading and resuming across a stream
// disconnect with multi-field structured row keys covering all 8 supported structured row key data
// types (String, Bytes, Int64, Float64, Float32, Bool, Timestamp, Date), null (KIND_NOT_SET)
// elements across every type, and subsequent responses in the same stream omitting TableSchema
// (since the server only populates table_schema on the first response of each stream).
func TestTypedReadRows_StructuredRowKeys_Resumption(t *testing.T) {
	schema := makeStructRowKeySchema(
		strType(),
		bytesType(),
		int64Type(),
		float64Type(),
		float32Type(),
		boolType(),
		timestampType(),
		dateType(),
		strType(),
	)

	row1 := makeStructuredTypedRow(
		[]*btpb.Value{
			strVal("tenant-a"),
			bytesVal([]byte{0x01, 0x02}),
			intVal(100),
			floatVal(3.141592653589793),
			floatVal(1.5),
			boolVal(true),
			timestampVal(1700000000, 123456000),
			dateVal(2026, 3, 25),
			nullVal(), // Null element (KIND_NOT_SET)
		},
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v1")))),
	)
	row2 := makeStructuredTypedRow(
		[]*btpb.Value{
			strVal("tenant-b"),
			bytesVal([]byte{0x03, 0x04}),
			intVal(200),
			floatVal(-2.718281828459045),
			floatVal(-0.25),
			boolVal(false),
			timestampVal(1700086400, 0),
			dateVal(2026, 3, 26),
			strVal("non-null-tag"),
		},
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v2")))),
	)
	// row3 exercises NULL (KIND_NOT_SET) across all 8 supported structured row key scalar types.
	row3 := makeStructuredTypedRow(
		[]*btpb.Value{
			nullVal(),
			nullVal(),
			nullVal(),
			nullVal(),
			nullVal(),
			nullVal(),
			nullVal(),
			nullVal(),
			nullVal(),
		},
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v3")))),
	)

	resp1 := makeFlushResponse("token-srk-1", []*btpb.TypedRow{row1})
	resp1.TableSchema = schema
	resp2 := makeFlushResponse("token-srk-2", []*btpb.TypedRow{row2}, row1)
	resp2.TableSchema = schema
	// Subsequent response on the same stream omits TableSchema to verify the client retains
	// the stream's initial TableSchema across batches within a stream.
	resp3 := makeFlushResponse("token-srk-3", []*btpb.TypedRow{row3}, row1, row2)

	recorder := make(chan *typedReadRowsReqRecord, 2)

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		&typedReadRowsAction{response: resp1},
		&typedReadRowsAction{rpcError: codes.Unavailable},
		&typedReadRowsAction{response: resp2},
		&typedReadRowsAction{response: resp3},
	)

	req := makeProxyTypedReadRowsRequest(t, "structured-row-keys-resumption-table")
	req.Request.RowKeyFormat = &btpb.TypedReadRowsRequest_UseStructuredKey{UseStructuredKey: true}

	res := doTypedReadRowsOp(t, server, req, nil)

	checkResultOkStatus(t, res)
	assertResumeTokens(t, recorder, "", "token-srk-1")
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2, row3}, res.Rows)
}

// TestTypedReadRows_Resumption_MultipleSequentialHeartbeats verifies that intermediate sparse-query
// heartbeats (flushes with resume_token but no batch data) after a committed batch preserve the
// non-zero running checksum, advance the resume token across consecutive heartbeats, and allow the
// client to resume from the latest heartbeat token upon disconnect.
func TestTypedReadRows_Resumption_MultipleSequentialHeartbeats(t *testing.T) {
	row1 := dummyTypedRow("hb-crc-row-1", "payload-1")
	row2 := dummyTypedRow("hb-crc-row-2", "payload-2")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("hb-crc-row-1", "payload-1", "token-row1"),
		typedHeartbeatAction("token-hb-1"),
		typedHeartbeatAction("token-hb-2"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		dummyTypedAction("hb-crc-row-2", "payload-2", "token-row2", row1),
	)

	req := makeProxyTypedReadRowsRequest(t, "heartbeats-resumption-table")
	res := doTypedReadRowsOp(t, server, req, nil)

	checkResultOkStatus(t, res)
	assertResumeTokens(t, recorder, "", "token-hb-2")
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_Generic_DeadlineExceeded verifies that client-side call deadline is respected.
func TestTypedReadRows_Generic_DeadlineExceeded(t *testing.T) {
	// 0. Common variables
	action := dummyTypedAction("row1", "v", "tok")
	action.delayStr = "10s"

	recorder := make(chan *typedReadRowsReqRecord, 1)

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
	loggedReq := <-recorder
	runTimeSecs := int(curTs.Unix() - loggedReq.ts.Unix())

	assert.GreaterOrEqual(t, runTimeSecs, 2)
	assert.Less(t, runTimeSecs, 8)

	if res.GetStatus().GetCode() != int32(codes.DeadlineExceeded) {
		msg := res.GetStatus().GetMessage()
		assert.Contains(t, strings.ToLower(strings.ReplaceAll(msg, " ", "")), "deadlineexceeded")
	}
}

// TestTypedReadRows_Resumption_RowSetUnmodified verifies that the client does NOT mutate the
// request's TypedRowSet when resuming after a retryable disconnect.
//
// This is the key behavioral difference from the v1 ReadRows protocol. There, the client had to
// truncate the RowSet on retry so already-returned rows were not re-read (see
// TestReadRows_NoRetry_MultipleRowRanges and friends). In TypedReadRows all progress is carried by
// the opaque resume_token, so the RowSet must be replayed verbatim. A client that carries over the
// v1 truncation logic would still pass every other resumption test in this file.
func TestTypedReadRows_Resumption_RowSetUnmodified(t *testing.T) {
	row1 := dummyTypedRow("row1", "v1")
	row2 := dummyTypedRow("row2", "v2")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("row1", "v1", "token-r1"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		dummyTypedAction("row2", "v2", "token-r2", row1),
	)

	req := makeProxyTypedReadRowsRequest(t, "rowset-unmodified-table")
	// Unstructured scan, so row keys and range bounds are raw_value -- the same kind the client
	// requires on the response side.
	req.Request.Rows = &btpb.TypedRowSet{
		RowKeys: []*btpb.Value{
			rawVal([]byte("row1")),
			rawVal([]byte("row2")),
		},
		RowRanges: []*btpb.TypedValueRange{
			{
				StartValue: &btpb.TypedValueRange_StartValueClosed{StartValueClosed: rawVal([]byte("row0"))},
				EndValue:   &btpb.TypedValueRange_EndValueOpen{EndValueOpen: rawVal([]byte("row9"))},
			},
		},
	}

	res := doTypedReadRowsOp(t, server, req, nil)

	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)

	reqs := assertResumeTokens(t, recorder, "", "token-r1")

	// The RowSet on the retry must be byte-for-byte what the caller supplied.
	if diff := cmp.Diff(req.Request.GetRows(), reqs[1].GetRows(), protocmp.Transform()); diff != "" {
		t.Errorf("RowSet was modified on resume (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(reqs[0].GetRows(), reqs[1].GetRows(), protocmp.Transform()); diff != "" {
		t.Errorf("RowSet differs between first attempt and retry (-first +retry):\n%s", diff)
	}
}

// TestTypedReadRows_Generic_CloseClient tests that the client doesn't kill inflight requests after
// client closing, but will reject new requests.
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
		expectedRows[i] = dummyTypedRow(rowKey, value)
		action := dummyTypedAction(rowKey, value, fmt.Sprintf("token-op%d", i))
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
	// We are a little permissive here by just checking if failures occur.
	for i := 0; i < halfBatchSize; i++ {
		if resultsBatchTwo[i] == nil {
			continue
		}
		assert.NotEmpty(t, resultsBatchTwo[i].GetStatus().GetCode())
	}
}

// TestTypedReadRows_Retry_WithRoutingCookie_MultipleErrorResponses verifies that the routing cookie
// is carried forward across consecutive failures, retained when an error omits it, and replaced
// when the server supplies a new one.
func TestTypedReadRows_Retry_WithRoutingCookie_MultipleErrorResponses(t *testing.T) {
	cookie := "test-cookie-trr"
	newCookie := "new-test-cookie-trr"
	row1 := dummyTypedRow("row-01", "v1")
	row5 := dummyTypedRow("row-05", "v5")

	mdRecords := make(chan metadata.MD, 4)
	recorder := make(chan *typedReadRowsReqRecord, 4)

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFnWithMetadata(recorder, mdRecords,
		dummyTypedAction("row-01", "v1", "token-r1"),
		&typedReadRowsAction{rpcError: codes.Unavailable, routingCookie: cookie},    // Error with a routing cookie
		&typedReadRowsAction{rpcError: codes.Unavailable},                           // Error with no routing cookie
		&typedReadRowsAction{rpcError: codes.Unavailable, routingCookie: newCookie}, // Error with new routing cookie
		dummyTypedAction("row-05", "v5", "token-r5", row1),
	)

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "routing-cookie-multi-table"), nil)

	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row5}, res.Rows)
	assert.Equal(t, 4, len(mdRecords))
	for i, wantCookie := range []string{"", cookie, cookie, newCookie} {
		md := <-mdRecords
		val := md["x-goog-cbt-cookie-test"]
		if wantCookie == "" {
			assert.Empty(t, val, "request #%d must not include routing cookie", i+1)
		} else {
			assert.NotEmpty(t, val, "request #%d must include routing cookie", i+1)
			if len(val) > 0 {
				assert.Equal(t, wantCookie, val[0], "request #%d routing cookie mismatch", i+1)
			}
		}
	}
	assertResumeTokens(t, recorder, "", "token-r1", "token-r1", "token-r1")
}

// TestTypedReadRows_Retry_WithRetryInfo_MultipleErrorResponses verifies that a server-provided
// RetryInfo delay governs the next attempt, and that the client falls back to its default backoff
// once the server stops supplying one.
func TestTypedReadRows_Retry_WithRetryInfo_MultipleErrorResponses(t *testing.T) {
	row1 := dummyTypedRow("row-01", "v1")
	row5 := dummyTypedRow("row-05", "v5")

	recorder := make(chan *typedReadRowsReqRecord, 3)

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("row-01", "v1", "token-r1"),
		&typedReadRowsAction{rpcError: codes.Unavailable, retryInfo: "2s"}, // Error with retry info
		&typedReadRowsAction{rpcError: codes.Unavailable},                  // Second error without retry info
		dummyTypedAction("row-05", "v5", "token-r5", row1),
	)

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "retry-info-multi-table"), nil)

	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row5}, res.Rows)
	assert.Equal(t, 3, len(recorder))
	firstReq := <-recorder
	retryReq1 := <-recorder
	retryReq2 := <-recorder

	// Both retries resume from the committed token.
	assert.Equal(t, []byte("token-r1"), retryReq1.req.GetResumeToken())
	assert.Equal(t, []byte("token-r1"), retryReq2.req.GetResumeToken())

	// The server-specified delay must be honored...
	assert.True(t, retryReq1.ts.Sub(firstReq.ts) >= 2*time.Second,
		"expected the RetryInfo delay of 2s to be respected")
	// ...and the following attempt falls back to the default backoff (initial delay is 10ms).
	assert.True(t, retryReq2.ts.Sub(retryReq1.ts) > 10*time.Millisecond,
		"expected default backoff after an error without RetryInfo")
}

// TestTypedReadRows_Retry_WithRetryInfo_OverallDeadline verifies that RetryInfo delays cannot push
// an operation past the overall call deadline.
func TestTypedReadRows_Retry_WithRetryInfo_OverallDeadline(t *testing.T) {
	row1 := dummyTypedRow("row-01", "v1")

	// There should only be 2 attempts due to the effect of client side timeout.
	recorder := make(chan *typedReadRowsReqRecord, 2)

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("row-01", "v1", "token-r1"),
		&typedReadRowsAction{rpcError: codes.Unavailable, retryInfo: "2s"},
		&typedReadRowsAction{rpcError: codes.Unavailable, retryInfo: "6s"},
		dummyTypedAction("row-05", "v5", "token-r5", row1),
	)

	req := makeProxyTypedReadRowsRequest(t, "retry-info-deadline-table")
	opts := clientOpts{
		timeout: &durationpb.Duration{Seconds: 3},
	}

	res := doTypedReadRowsOp(t, server, req, &opts)

	assert.NotNil(t, res)
	assert.NotEqual(t, int32(codes.OK), res.GetStatus().GetCode(), "expected call to exceed deadline")

	// 3s deadline is far below the combined 2s + 6s of server-requested delay, so the operation
	// must give up rather than sleeping through it.
	assert.Equal(t, 2, len(recorder))
	firstReq := <-recorder
	retryReq := <-recorder
	assert.Equal(t, []byte("token-r1"), retryReq.req.GetResumeToken())

	curTs := time.Now()
	runTimeSecs := int(curTs.Unix() - firstReq.ts.Unix())
	assert.Less(t, runTimeSecs, 4)
}
