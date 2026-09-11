// Copyright 2024 Google LLC
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"

	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
)

// TestTypedReadRows_SingleRow_Success verifies that a single row with schema and flush is received successfully.
func TestTypedReadRows_SingleRow_Success(t *testing.T) {
	// 0. Common variables
	row := makeTypedRow([]byte("row1"),
		makeTypedFamily("fam1",
			makeTypedColumn([]byte("col1"),
				makeTypedCell([]byte("val1")),
			),
		),
	)

	data := serializeTypedRows(row)

	responses := chunkedTypedResponses(data, len(data), []byte("token1"), true)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, responsesToActions(responses...)...)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("test-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row}, res.Rows)
}

// TestTypedReadRows_MultipleRows_Success verifies multiple rows sent across multiple batches.
func TestTypedReadRows_MultipleRows_Success(t *testing.T) {
	// 0. Common variables
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

	batch1Data := serializeTypedRows(row1, row2)
	batch2Data := serializeTypedRows(row3)

	responses1 := chunkedTypedResponses(batch1Data, len(batch1Data), []byte("token1"), true)
	responses2 := chunkedTypedResponses(batch2Data, len(batch2Data), []byte("token2"), true, batch1Data)
	allResponses := append(responses1, responses2...)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, responsesToActions(allResponses...)...)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("test-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2, row3}, res.Rows)
}

// TestTypedReadRows_ArbitraryChunkFragmentation verifies stream reassembly across various arbitrary
// chunk sizes, including the degenerate case of one byte per response.
func TestTypedReadRows_ArbitraryChunkFragmentation(t *testing.T) {
	// 0. Common variables
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

	chunkSizes := []int{1, 2, 3, 7, 15, 23}
	for _, chunkSize := range chunkSizes {
		t.Run(fmt.Sprintf("ChunkSize_%d", chunkSize), func(t *testing.T) {
			responses := chunkedTypedResponses(data, chunkSize, []byte("token-frag"), true)

			// 1. Instantiate the mock server
			server := initMockServer(t)
			server.TypedReadRowsFn = mockTypedReadRowsFn(nil, responsesToActions(responses...)...)

			// 2. Build the request to test proxy
			req := &testproxypb.TypedReadRowsRequest{
				ClientId: fmt.Sprintf("%s-%d", t.Name(), chunkSize),
				Request:  makeTypedReadRowsRequest(buildTableName("test-table")),
			}

			// 3. Perform the operation via test proxy
			res := doTypedReadRowsOp(t, server, req, nil)

			// 4. Check the response
			checkResultOkStatus(t, res)
			assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
		})
	}
}

// TestTypedReadRows_Checksum_Corrupt_Fails verifies that corrupted checksum triggers an error.
func TestTypedReadRows_Checksum_Corrupt_Fails(t *testing.T) {
	// 0. Common variables
	row := makeTypedRow([]byte("corrupt-crc-row"),
		makeTypedFamily("cf",
			makeTypedColumn([]byte("col"), makeTypedCell([]byte("some-data"))),
		),
	)

	data := serializeTypedRows(row)

	corruptCrc := trrMetaChecksum(data) ^ 0xFFFFFFFF
	resp := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{
					BatchData: data,
				},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &corruptCrc,
				ResumeToken: []byte("token-corrupt-crc"),
			},
		},
	}

	// 1. Instantiate the mock server
	server := initMockServer(t)
	// A closure rather than mockTypedReadRowsFn: the client retries on checksum mismatch, and
	// this must serve the same corrupt response on every attempt so the failure is the client
	// exhausting retries. A one-shot action queue would hand the retry an empty stream, which
	// the client reports as a successful read of zero rows.
	server.TypedReadRowsFn = func(req *btpb.TypedReadRowsRequest, srv btpb.Bigtable_TypedReadRowsServer) error {
		return srv.Send(resp)
	}

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("test-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	assert.NotEqual(t, int32(codes.OK), res.GetStatus().GetCode(), "expected failure on corrupt checksum")
	assert.Empty(t, res.GetRows(), "unverified rows must not be yielded")
	t.Logf("The full error message is: %s", res.GetStatus().GetMessage())
}

// TestTypedReadRows_CancelAfterRows verifies that the stream is cancelled by the test proxy
// once cancel_after_rows rows have been surfaced, and that only those rows are returned.
func TestTypedReadRows_CancelAfterRows(t *testing.T) {
	// 0. Common variables
	var rows []*btpb.TypedRow
	for i := 1; i <= 5; i++ {
		rows = append(rows, makeTypedRow(
			[]byte(fmt.Sprintf("row-%02d", i)),
			makeTypedFamily("cf",
				makeTypedColumn([]byte("c"), makeTypedCell([]byte(fmt.Sprintf("val-%d", i)))),
			),
		))
	}

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = func(req *btpb.TypedReadRowsRequest, srv btpb.Bigtable_TypedReadRowsServer) error {
		var prevBatches [][]byte
		for i, r := range rows {
			data := serializeTypedRows(r)
			prevBatches = append(prevBatches, data)
			crc := trrMetaChecksumMulti(prevBatches...)
			resp := &btpb.TypedReadRowsResponse{
				Response: &btpb.PartialRowResponse{
					PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
						TypedRowsBatch: &btpb.TypedRowsBatch{
							BatchData: data,
						},
					},
					Flush: &btpb.PartialRowResponse_Flush{
						Checksum:    &crc,
						ResumeToken: []byte(fmt.Sprintf("token-%d", i+1)),
					},
				},
			}
			if err := srv.Send(resp); err != nil {
				return err
			}
		}
		return nil
	}

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId:        t.Name(),
		CancelAfterRows: 2,
		Request:         makeTypedReadRowsRequest(buildTableName("test-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkOrCancelledStatus(t, res)
	assertTypedRowsEqual(t, rows[:2], res.Rows)
}

// TestTypedReadRows_Reset_DiscardsUncommitted verifies that reset=true discards data buffered
// since the last resume_token, while data already committed by an earlier flush survives.
func TestTypedReadRows_Reset_DiscardsUncommitted(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row1", "committed-1")
	row3 := dummyTypedRow("row3", "committed-3")

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil,
		dummyTypedAction("row1", "committed-1", "token-row1"),
		// Uncommitted row2: batch data without flush
		dummyUncommittedAction("row2", "uncommitted-2"),
		// Reset signal: must discard uncommitted data
		typedResetAction(),
		// Committed row3 with cumulative checksum across row1 and row3
		dummyTypedAction("row3", "committed-3", "token-row3", row1),
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("test-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	// Only row1 and row3 should be in the result; row2 was discarded by reset
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row3}, res.Rows)
}

// TestTypedReadRows_Reset_WithDataInSameResponse verifies the ordering the protocol mandates when
// reset arrives alongside other fields: "any data buffered since the last non-empty `resume_token`
// must be discarded before the other parts of this message, if any, are handled." Here the reset,
// a fresh batch, and the flush that commits it all travel in a single response. A client that
// appended the batch first and applied the reset afterwards would discard the new data instead of
// the stale data, yielding row1 alone.
func TestTypedReadRows_Reset_WithDataInSameResponse(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row1", "committed-1")
	row3 := dummyTypedRow("row3", "committed-3")

	// One response carrying reset, a fresh batch, and its flush. The cumulative checksum spans
	// row1 and row3 only, since the reset drops the uncommitted row2 batch.
	resetWithData := dummyFlushResponse("row3", "committed-3", "token-row3", row1)
	resetWithData.Response.Reset_ = true

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil,
		dummyTypedAction("row1", "committed-1", "token-row1"),
		// Uncommitted row2: batch data without flush
		dummyUncommittedAction("row2", "uncommitted-2"),
		&typedReadRowsAction{response: resetWithData},
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("test-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	// row3 travelled with the reset and must survive it; row2 must not.
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row3}, res.Rows)
}

// TestTypedReadRows_TargetRouting verifies table_name, authorized_view_name, and materialized_view_name routing.
func TestTypedReadRows_TargetRouting(t *testing.T) {
	// 0. Common variables
	row := makeTypedRow([]byte("row-target"),
		makeTypedFamily("cf",
			makeTypedColumn([]byte("c"), makeTypedCell([]byte("v"))),
		),
	)
	data := serializeTypedRows(row)
	responses := chunkedTypedResponses(data, len(data), []byte("token"), true)

	tests := []struct {
		name      string
		targetReq *btpb.TypedReadRowsRequest
		verifyReq func(t *testing.T, req *btpb.TypedReadRowsRequest)
	}{
		{
			name: "TableName",
			targetReq: &btpb.TypedReadRowsRequest{
				Target: &btpb.TypedReadRowsRequest_TableName{
					TableName: buildTableName("target-table"),
				},
			},
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

			// 1. Instantiate the mock server
			server := initMockServer(t)
			server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, responsesToActions(responses...)...)

			// 2. Build the request to test proxy
			req := &testproxypb.TypedReadRowsRequest{
				ClientId: fmt.Sprintf("%s-%s", t.Name(), tc.name),
				Request:  tc.targetReq,
			}

			// 3. Perform the operation via test proxy
			res := doTypedReadRowsOp(t, server, req, nil)

			// 4. Check the response
			checkResultOkStatus(t, res)
			assertTypedRowsEqual(t, []*btpb.TypedRow{row}, res.Rows)
			rec := <-recorder
			tc.verifyReq(t, rec.req)
		})
	}
}

// TestTypedReadRows_ServerErrorPropagated verifies that a server error reaches the caller with its
// original status code.
//
// The two cases exercise different paths. PermissionDenied is terminal, so the client surfaces it
// immediately. Unavailable is retryable, so the client retries until its budget is exhausted
// (a few seconds, vs. milliseconds for the terminal case) and only then surfaces the code; what
// that case really proves is that retry exhaustion preserves the original status rather than
// hanging or masking it behind a generic error. The
// mock uses a closure rather than an action queue so that every attempt sees the same error.
func TestTypedReadRows_ServerErrorPropagated(t *testing.T) {
	// 0. Common variables
	tests := []struct {
		name     string
		code     codes.Code
		expected int32
	}{
		{
			name:     "Unavailable",
			code:     codes.Unavailable,
			expected: int32(codes.Unavailable),
		},
		{
			name:     "PermissionDenied",
			code:     codes.PermissionDenied,
			expected: int32(codes.PermissionDenied),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 1. Instantiate the mock server
			server := initMockServer(t)
			server.TypedReadRowsFn = func(req *btpb.TypedReadRowsRequest, srv btpb.Bigtable_TypedReadRowsServer) error {
				return status.Error(tc.code, fmt.Sprintf("server error: %s", tc.name))
			}

			// 2. Build the request to test proxy
			req := &testproxypb.TypedReadRowsRequest{
				ClientId: fmt.Sprintf("%s-%s", t.Name(), tc.name),
				Request:  makeTypedReadRowsRequest(buildTableName("test-table")),
			}

			// 3. Perform the operation via test proxy
			res := doTypedReadRowsOp(t, server, req, nil)

			// 4. Check the response
			assert.NotEmpty(t, res)
			assert.Equal(t, tc.expected, res.GetStatus().GetCode())
		})
	}
}

// TestTypedReadRows_MissingTarget_InvalidArgument verifies that a request with no target returns INVALID_ARGUMENT.
func TestTypedReadRows_MissingTarget_InvalidArgument(t *testing.T) {
	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = func(req *btpb.TypedReadRowsRequest, srv btpb.Bigtable_TypedReadRowsServer) error {
		return nil
	}

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  &btpb.TypedReadRowsRequest{}, // No target set
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	assert.NotEmpty(t, res)
	assert.Equal(t, int32(codes.InvalidArgument), res.GetStatus().GetCode())
}

// TestTypedReadRows_EmptyTable_Success verifies that an empty stream returns 0 rows and OK status.
func TestTypedReadRows_EmptyTable_Success(t *testing.T) {
	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("empty-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assert.Empty(t, res.Rows)
}

// TestTypedReadRows_CumulativeRunningChecksum verifies that the running CRC32c accumulator across multiple batches succeeds.
func TestTypedReadRows_CumulativeRunningChecksum(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row-crc-1", "val1")
	row2 := dummyTypedRow("row-crc-2", "val2")
	row3 := dummyTypedRow("row-crc-3", "val3")

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil,
		dummyTypedAction("row-crc-1", "val1", "token1"),
		dummyTypedAction("row-crc-2", "val2", "token2", row1),
		dummyTypedAction("row-crc-3", "val3", "token3", row1, row2),
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("test-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2, row3}, res.Rows)
}

// TestTypedReadRows_UnflushedDataAtStreamEnd_Fails verifies that batch data which is never
// committed by a Flush is not yielded to the caller, even though the stream ends cleanly.
//
// Per the protocol, a resume_token is required before buffered values may be surfaced. A server
// holding a partial batch when the stream finishes is obliged to commit it and send a final
// response carrying a flush; a server whose buffer is already empty simply closes with OK and
// sends nothing further. A stream that ends with uncommitted bytes therefore means the server
// skipped that obligation, and the client must not invent the commit on its behalf. The mock
// models the same rule, so its auto-flush has to be disabled here for the scenario to be
// reachable at all.
func TestTypedReadRows_UnflushedDataAtStreamEnd_Fails(t *testing.T) {
	// 0. Common variables
	row := dummyTypedRow("row-no-flush", "val-no-flush")

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.DisableTypedReadRowsAutoFlush = true
	// Send batch data only, NO flush message
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, typedUncommittedAction(row))

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("test-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	// The uncommitted row must never reach the caller. Clients are expected to surface this as an
	// error rather than silently truncating, so assert on both.
	assert.NotEqual(t, int32(codes.OK), res.GetStatus().GetCode(),
		"expected failure when the stream ends with unflushed data")
	assert.Empty(t, res.GetRows(), "uncommitted rows must not be yielded")
	t.Logf("The full error message is: %s", res.GetStatus().GetMessage())
}

// TestTypedReadRows_Reset_BeforeAnyFlush verifies that reset=true discards buffered data even when
// no resume_token has been seen yet, so there is no earlier checkpoint to fall back to.
//
// The discarded bytes are deliberately unparseable: the client must drop them without ever
// attempting to decode them, then read the following row normally. A client that parsed eagerly
// instead of at flush time would fail here.
func TestTypedReadRows_Reset_BeforeAnyFlush(t *testing.T) {
	// 0. Common variables
	validRow := dummyTypedRow("row-after-reset", "v")

	respGarbage := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{
					BatchData: []byte{0xDE, 0xAD, 0xBE, 0xEF},
				},
			},
		},
	}

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil,
		&typedReadRowsAction{response: respGarbage},
		typedResetAction(),
		dummyTypedAction("row-after-reset", "v", "token-after-reset"),
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("test-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{validRow}, res.Rows)
}

// TestTypedReadRows_InvalidProtobuf_Fails verifies that a batch whose bytes are not a valid
// TypedRows message is rejected rather than surfaced as rows.
//
// The flush deliberately carries no checksum so that the mock's auto-checksum supplies a correct
// one. That is what isolates the parse failure: with an absent or wrong checksum the client would
// reject the batch before ever trying to decode it (see NonEmptyBatchWithoutChecksum_Fails) and
// this test would pass for the wrong reason. Do not disable auto-checksum here.
//
// Unlike Checksum_Corrupt_Fails, a one-shot action queue is sufficient: a parse failure is not
// retryable, so the client makes exactly one attempt.
func TestTypedReadRows_InvalidProtobuf_Fails(t *testing.T) {
	// 0. Common variables
	invalidBytes := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	resp := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{
					BatchData: invalidBytes,
				},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				ResumeToken: []byte("token"),
			},
		},
	}

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, &typedReadRowsAction{response: resp})

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("test-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	assert.NotEqual(t, int32(codes.OK), res.GetStatus().GetCode(),
		"expected failure on unparseable batch data")
	assert.Empty(t, res.GetRows(), "unparseable rows must not be yielded")
	t.Logf("The full error message is: %s", res.GetStatus().GetMessage())
}

// TestTypedReadRows_AppProfileIdRouting verifies that app_profile_id is correctly propagated.
func TestTypedReadRows_AppProfileIdRouting(t *testing.T) {
	// 0. Common variables
	const customProfile = "custom-profile-123"
	row := dummyTypedRow("row-app-profile", "v")

	recorder := make(chan *typedReadRowsReqRecord, 1)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, dummyTypedAction("row-app-profile", "v", "token"))

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("test-table")),
	}
	opts := &clientOpts{
		profile: customProfile,
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, opts)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row}, res.Rows)
	rec := <-recorder
	assert.Equal(t, customProfile, rec.req.GetAppProfileId())
}

// TestTypedReadRows_Generic_Headers tests that TypedReadRows request sends client and resource info,
// as well as app_profile_id in the headers.
func TestTypedReadRows_Generic_Headers(t *testing.T) {
	// 0. Common variables
	const profileID string = "test_profile"
	tableName := buildTableName("table-headers")

	mdRecords := make(chan metadata.MD, 1)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = func(req *btpb.TypedReadRowsRequest, srv btpb.Bigtable_TypedReadRowsServer) error {
		md, _ := metadata.FromIncomingContext(srv.Context())
		mdRecords <- md
		return nil
	}

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(tableName),
	}

	opts := &clientOpts{
		profile: profileID,
	}

	// 3. Perform the operation via test proxy
	doTypedReadRowsOp(t, server, req, opts)

	// 4. Check the response
	md := <-mdRecords
	if len(md["user-agent"]) == 0 && len(md["x-goog-api-client"]) == 0 {
		assert.Fail(t, "Client info is missing in the request header")
	}

	resource := md["x-goog-request-params"][0]
	if !strings.Contains(resource, tableName) && !strings.Contains(resource, url.QueryEscape(tableName)) {
		assert.Fail(t, "Resource info is missing in the request header")
	}
	assert.Contains(t, resource, profileID)
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
		reqs[i] = &testproxypb.TypedReadRowsRequest{
			ClientId: t.Name(),
			Request:  makeTypedReadRowsRequest(buildTableName(fmt.Sprintf("op%d-table", i))),
		}
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

// TestTypedReadRows_RequestFields_Fidelity verifies that fields such as RowsLimit and Reversed are propagated.
func TestTypedReadRows_RequestFields_Fidelity(t *testing.T) {
	// 0. Common variables
	recorder := make(chan *typedReadRowsReqRecord, 1)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, dummyTypedAction("row1", "v", "token"))

	rawReq := makeTypedReadRowsRequest(buildTableName("fidelity-table"))
	rawReq.RowsLimit = 42
	rawReq.Reversed = true

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  rawReq,
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	rec := <-recorder
	assert.Equal(t, int64(42), rec.req.GetRowsLimit())
	assert.True(t, rec.req.GetReversed())
}

// TestTypedReadRows_Resumption_Unavailable verifies that when the gRPC stream terminates
// with a retryable UNAVAILABLE error after receiving partial data with a resume_token,
// the client automatically reconnects, sends the resume_token, and successfully consumes
// the remainder of the rows without duplicating previously committed rows.
func TestTypedReadRows_Resumption_Unavailable(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row1", "v1")
	row2 := dummyTypedRow("row2", "v2")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("row1", "v1", "token1"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		dummyTypedAction("row2", "v2", "token2", row1),
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("resumption-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	rec1 := <-recorder
	rec2 := <-recorder
	assert.Empty(t, rec1.req.GetResumeToken())
	assert.Equal(t, []byte("token1"), rec2.req.GetResumeToken())
	assert.Len(t, res.Rows, 2)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
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
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("resumption-uncommitted-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	rec1 := <-recorder
	rec2 := <-recorder
	assert.Empty(t, rec1.req.GetResumeToken())
	assert.Equal(t, []byte("token1"), rec2.req.GetResumeToken())
	assert.Len(t, res.Rows, 2)
	assertTypedRowsEqual(t, []*btpb.TypedRow{committedRow1, committedRow2}, res.Rows)
}

// TestTypedReadRows_Resumption_DisconnectMidChunk verifies that if a stream drops in the middle
// of receiving multi-chunk fragmented batch data (before flush/token), the client purges its
// partial uncommitted chunk buffer upon reconnection rather than concatenating stale fragments.
func TestTypedReadRows_Resumption_DisconnectMidChunk(t *testing.T) {
	// 0. Common variables
	row1 := makeTypedRow([]byte("row-committed"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v1")))),
	)
	row2 := makeTypedRow([]byte("row-chunked-resumed"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("a-longer-value-for-chunking-1234567890")))),
	)

	data1 := serializeTypedRows(row1)
	crc1 := trrMetaChecksum(data1)

	data2 := serializeTypedRows(row2)
	crc2 := trrMetaChecksumMulti(data1, data2)

	// Split row2 into 3 chunks
	chunkSize := len(data2) / 3
	chunk1 := data2[:chunkSize]
	chunk2 := data2[chunkSize : chunkSize*2]
	chunk3 := data2[chunkSize*2:]

	respRow1 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data1},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crc1,
				ResumeToken: []byte("token1"),
			},
		},
	}
	respChunk1 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: chunk1},
			},
		},
	}
	respChunk2 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: chunk2},
			},
		},
	}
	respChunk3Flush := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: chunk3},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crc2,
				ResumeToken: []byte("token2"),
			},
		},
	}

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		&typedReadRowsAction{response: respRow1},
		&typedReadRowsAction{response: respChunk1},
		&typedReadRowsAction{response: respChunk2},
		&typedReadRowsAction{rpcError: codes.Unavailable},
		&typedReadRowsAction{response: respChunk1},
		&typedReadRowsAction{response: respChunk2},
		&typedReadRowsAction{response: respChunk3Flush},
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("midchunk-disconnect-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	rec1 := <-recorder
	rec2 := <-recorder
	assert.Empty(t, rec1.req.GetResumeToken())
	assert.Equal(t, []byte("token1"), rec2.req.GetResumeToken())
	assert.Len(t, res.Rows, 2)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// DISABLED: this test encodes a Java-client bug rather than the protocol.
//
// On the third stream it expects a cumulative checksum over {d1, d3}, skipping d2 -- the batch
// the second stream committed. That is the only value the current client accepts, but no correct
// server could produce it: which batch landed in which attempt depends on where the client's
// connection happened to drop, and a server resuming from token-seq-2 has no way to know.
//
// Root cause: TypedReadRowsResumptionStrategy.getResumeRequest() re-seeds committedHashes from
// the attempt-1 call context on every retry, because gax always passes initialRequest. Hashes
// committed during attempts 2+ are discarded. The resume token is unaffected because it lives on
// the strategy, not the call context -- hence "keeps the row, forgets the row's checksum".
//
// Re-enable once the client is fixed, changing the last action's prevBatches to (row1, row2) so
// the checksum spans {d1, d2, d3}. At that point this becomes the regression test for the fix.
// Commented out rather than deleted so the fix has something to restore.
// // TestTypedReadRows_Resumption_MultipleSequentialDisconnects verifies that multiple cascading
// // stream disconnections at sequential checkpoints each advance the resume_token properly and
// // accumulate the full set of rows without loss or duplication.
// func TestTypedReadRows_Resumption_MultipleSequentialDisconnects(t *testing.T) {
// 	// 0. Common variables
// 	row1 := dummyTypedRow("row-seq-1", "v1")
// 	row2 := dummyTypedRow("row-seq-2", "v2")
// 	row3 := dummyTypedRow("row-seq-3", "v3")
//
// 	recorder := make(chan *typedReadRowsReqRecord, 3)
//
// 	// 1. Instantiate the mock server
// 	server := initMockServer(t)
// 	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
// 		dummyTypedAction("row-seq-1", "v1", "token-seq-1"),
// 		&typedReadRowsAction{rpcError: codes.Unavailable},
// 		dummyTypedAction("row-seq-2", "v2", "token-seq-2", row1),
// 		&typedReadRowsAction{rpcError: codes.Unavailable},
// 		dummyTypedAction("row-seq-3", "v3", "token-seq-3", row1),
// 	)
//
// 	// 2. Build the request to test proxy
// 	req := &testproxypb.TypedReadRowsRequest{
// 		ClientId: t.Name(),
// 		Request:  makeTypedReadRowsRequest(buildTableName("seq-disconnect-table")),
// 	}
//
// 	// 3. Perform the operation via test proxy
// 	res := doTypedReadRowsOp(t, server, req, nil)
//
// 	// 4. Check the response
// 	checkResultOkStatus(t, res)
// 	rec1 := <-recorder
// 	rec2 := <-recorder
// 	rec3 := <-recorder
// 	assert.Empty(t, rec1.req.GetResumeToken())
// 	assert.Equal(t, []byte("token-seq-1"), rec2.req.GetResumeToken())
// 	assert.Equal(t, []byte("token-seq-2"), rec3.req.GetResumeToken())
// 	assert.Len(t, res.Rows, 3)
// 	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2, row3}, res.Rows)
// }

// TestTypedReadRows_Resumption_HeartbeatTokenWithoutData verifies that the server can send a
// progress/heartbeat flush with a resume_token but no row data, and upon reconnection the client
// resumes from that heartbeat token.
func TestTypedReadRows_Resumption_HeartbeatTokenWithoutData(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row-after-heartbeat", "v1")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		typedHeartbeatAction("heartbeat-checkpoint-token"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		dummyTypedAction("row-after-heartbeat", "v1", "final-token"),
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("heartbeat-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	rec1 := <-recorder
	rec2 := <-recorder
	assert.Empty(t, rec1.req.GetResumeToken())
	assert.Equal(t, []byte("heartbeat-checkpoint-token"), rec2.req.GetResumeToken())
	assert.Len(t, res.Rows, 1)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1}, res.Rows)
}

// TestTypedReadRows_Resumption_RetryExhaustion verifies that when retryable errors exceed
// the maximum retry limit (e.g. 3 retries), the client terminates and returns the UNAVAILABLE status.
func TestTypedReadRows_Resumption_RetryExhaustion(t *testing.T) {
	// 0. Common variables
	row1 := makeTypedRow([]byte("row-before-outage"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v1")))),
	)
	data1 := serializeTypedRows(row1)
	crc1 := trrMetaChecksum(data1)

	attempts := 0

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = func(req *btpb.TypedReadRowsRequest, srv btpb.Bigtable_TypedReadRowsServer) error {
		attempts++
		if attempts == 1 {
			// Commit row1
			if err := srv.Send(&btpb.TypedReadRowsResponse{
				Response: &btpb.PartialRowResponse{
					PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
						TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data1},
					},
					Flush: &btpb.PartialRowResponse_Flush{
						Checksum:    &crc1,
						ResumeToken: []byte("token-before-outage"),
					},
				},
			}); err != nil {
				return err
			}
			return status.Error(codes.Unavailable, "permanent backend failure")
		}
		// Continuous failure on retries
		return status.Error(codes.Unavailable, "still failing")
	}

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("exhaustion-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	assert.NotNil(t, res)
	assert.Equal(t, int32(codes.Unavailable), res.GetStatus().GetCode())
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
	assert.Len(t, res.Rows, 1)
}

// TestTypedReadRows_Resumption_WithResetInNewStream verifies that in-stream resets function
// correctly even after the client has resumed from a prior disconnect.
func TestTypedReadRows_Resumption_WithResetInNewStream(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row-before-disconnect", "v1")
	row2 := dummyTypedRow("row-valid-after-reset", "v2")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("row-before-disconnect", "v1", "token-r1"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		dummyUncommittedAction("row-dirty-after-reconnect", "dirty"),
		typedResetAction(),
		dummyTypedAction("row-valid-after-reset", "v2", "token-r2", row1),
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("resumed-reset-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	rec1 := <-recorder
	rec2 := <-recorder
	assert.Empty(t, rec1.req.GetResumeToken())
	assert.Equal(t, []byte("token-r1"), rec2.req.GetResumeToken())
	assert.Len(t, res.Rows, 2)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_Resumption_CancelAfterRows verifies that the test proxy's cancel_after_rows
// counter does not reset to zero upon stream resumption across a retry, but properly tracks
// total collected rows across reconnections and cancels the stream when the limit is reached.
// Note that cancel_after_rows is a harness-side control, not a client library feature: the proxy
// counts the rows it has surfaced and then cancels the user-visible stream.
func TestTypedReadRows_Resumption_CancelAfterRows(t *testing.T) {
	// 0. Common variables
	row1 := makeTypedRow([]byte("row-cancel-1"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v1")))),
	)
	row2 := makeTypedRow([]byte("row-cancel-2"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v2")))),
	)
	row3 := makeTypedRow([]byte("row-cancel-3"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v3")))),
	)

	data1 := serializeTypedRows(row1)
	crc1 := trrMetaChecksum(data1)
	data2 := serializeTypedRows(row2)
	crc2 := trrMetaChecksumMulti(data1, data2)
	data3 := serializeTypedRows(row3)
	crc3 := trrMetaChecksumMulti(data1, data2, data3)

	attempts := 0

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = func(req *btpb.TypedReadRowsRequest, srv btpb.Bigtable_TypedReadRowsServer) error {
		attempts++
		if attempts == 1 {
			// Deliver row1, then drop
			if err := srv.Send(&btpb.TypedReadRowsResponse{
				Response: &btpb.PartialRowResponse{
					PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
						TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data1},
					},
					Flush: &btpb.PartialRowResponse_Flush{
						Checksum:    &crc1,
						ResumeToken: []byte("token-c1"),
					},
				},
			}); err != nil {
				return err
			}
			return status.Error(codes.Unavailable, "drop before limit")
		}

		// Reconnected stream delivers row2 and row3
		if err := srv.Send(&btpb.TypedReadRowsResponse{
			Response: &btpb.PartialRowResponse{
				PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
					TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data2},
				},
				Flush: &btpb.PartialRowResponse_Flush{
					Checksum:    &crc2,
					ResumeToken: []byte("token-c2"),
				},
			},
		}); err != nil {
			return err
		}

		// Row 3 should be ignored/cancelled by client because cancel_after_rows is 2
		if err := srv.Send(&btpb.TypedReadRowsResponse{
			Response: &btpb.PartialRowResponse{
				PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
					TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data3},
				},
				Flush: &btpb.PartialRowResponse_Flush{
					Checksum:    &crc3,
					ResumeToken: []byte("token-c3"),
				},
			},
		}); err != nil {
			// Client may cancel stream
			return nil
		}
		return nil
	}

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId:        t.Name(),
		Request:         makeTypedReadRowsRequest(buildTableName("cancel-resumption-table")),
		CancelAfterRows: 2, // Cancel after exactly 2 rows
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assert.Equal(t, 2, attempts)
	assert.Len(t, res.Rows, 2, "Proxy must return exactly 2 rows despite reconnection")
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_Resumption_SingleByteChunks verifies that 1-byte defragmentation continues
// to function reliably when interrupted by a stream disconnection.
func TestTypedReadRows_Resumption_SingleByteChunks(t *testing.T) {
	// 0. Common variables
	row1 := makeTypedRow([]byte("row-byte-1"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("byte1")))),
	)
	row2 := makeTypedRow([]byte("row-byte-2"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("byte2")))),
	)

	data1 := serializeTypedRows(row1)
	crc1 := trrMetaChecksum(data1)
	data2 := serializeTypedRows(row2)
	crc2 := trrMetaChecksumMulti(data1, data2)

	attempts := 0

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = func(req *btpb.TypedReadRowsRequest, srv btpb.Bigtable_TypedReadRowsServer) error {
		attempts++
		if attempts == 1 {
			// Stream row1 in 1-byte fragments
			for i := 0; i < len(data1); i++ {
				var flush *btpb.PartialRowResponse_Flush
				if i == len(data1)-1 {
					flush = &btpb.PartialRowResponse_Flush{
						Checksum:    &crc1,
						ResumeToken: []byte("token-byte-1"),
					}
				}
				if err := srv.Send(&btpb.TypedReadRowsResponse{
					Response: &btpb.PartialRowResponse{
						PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
							TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data1[i : i+1]},
						},
						Flush: flush,
					},
				}); err != nil {
					return err
				}
			}

			// Stream first 3 bytes of row2 without flush
			for i := 0; i < 3 && i < len(data2); i++ {
				if err := srv.Send(&btpb.TypedReadRowsResponse{
					Response: &btpb.PartialRowResponse{
						PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
							TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data2[i : i+1]},
						},
					},
				}); err != nil {
					return err
				}
			}
			return status.Error(codes.Unavailable, "drop in single-byte stream")
		}

		// Attempt 2: Re-stream all of row2 in 1-byte chunks
		for i := 0; i < len(data2); i++ {
			var flush *btpb.PartialRowResponse_Flush
			if i == len(data2)-1 {
				flush = &btpb.PartialRowResponse_Flush{
					Checksum:    &crc2,
					ResumeToken: []byte("token-byte-2"),
				}
			}
			if err := srv.Send(&btpb.TypedReadRowsResponse{
				Response: &btpb.PartialRowResponse{
					PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
						TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data2[i : i+1]},
					},
					Flush: flush,
				},
			}); err != nil {
				return err
			}
		}
		return nil
	}

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("singlebyte-resumption-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assert.Equal(t, 2, attempts)
	assert.Len(t, res.Rows, 2)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_Resumption_MultiRowBatches verifies that batches containing multiple rows
// in a single message correctly reassemble and commit across stream resumptions.
func TestTypedReadRows_Resumption_MultiRowBatches(t *testing.T) {
	// 0. Common variables
	rowA := makeTypedRow([]byte("row-batch1-A"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("vA")))),
	)
	rowB := makeTypedRow([]byte("row-batch1-B"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("vB")))),
	)
	rowC := makeTypedRow([]byte("row-batch1-C"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("vC")))),
	)
	rowD := makeTypedRow([]byte("row-batch2-D"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("vD")))),
	)
	rowE := makeTypedRow([]byte("row-batch2-E"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("vE")))),
	)

	dataBatch1 := serializeTypedRows(rowA, rowB, rowC)
	crcBatch1 := trrMetaChecksum(dataBatch1)
	dataBatch2 := serializeTypedRows(rowD, rowE)
	crcBatch2 := trrMetaChecksumMulti(dataBatch1, dataBatch2)

	half1 := len(dataBatch1) / 2
	resp1Chunk1 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: dataBatch1[:half1]},
			},
		},
	}
	resp1Chunk2 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: dataBatch1[half1:]},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crcBatch1,
				ResumeToken: []byte("token-multirow-1"),
			},
		},
	}

	half2 := len(dataBatch2) / 2
	resp2Chunk1 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: dataBatch2[:half2]},
			},
		},
	}
	resp2Chunk2 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: dataBatch2[half2:]},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crcBatch2,
				ResumeToken: []byte("token-multirow-2"),
			},
		},
	}

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		&typedReadRowsAction{response: resp1Chunk1},
		&typedReadRowsAction{response: resp1Chunk2},
		&typedReadRowsAction{rpcError: codes.Unavailable},
		&typedReadRowsAction{response: resp2Chunk1},
		&typedReadRowsAction{response: resp2Chunk2},
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("multirow-resumption-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	rec1 := <-recorder
	rec2 := <-recorder
	assert.Empty(t, rec1.req.GetResumeToken())
	assert.Equal(t, []byte("token-multirow-1"), rec2.req.GetResumeToken())
	assert.Len(t, res.Rows, 5)
	assertTypedRowsEqual(t, []*btpb.TypedRow{rowA, rowB, rowC, rowD, rowE}, res.Rows)
}

// TestTypedReadRows_Resumption_NonRetryableErrorAfterDrop verifies that if the stream drops
// with a retryable error, but the reconnected attempt encounters a non-retryable error
// (e.g. PermissionDenied), the client preserves committed rows and surfaces the error status.
func TestTypedReadRows_Resumption_NonRetryableErrorAfterDrop(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row-committed-before-perm-denied", "v1")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("row-committed-before-perm-denied", "v1", "token-perm-1"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		&typedReadRowsAction{rpcError: codes.PermissionDenied},
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("permdenied-resumption-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	assert.NotNil(t, res)
	assert.Equal(t, int32(codes.PermissionDenied), res.GetStatus().GetCode())
	assert.Len(t, res.Rows, 1, "Committed row before non-retryable error must be preserved")
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1}, res.Rows)
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
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("initial-transient-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	rec1 := <-recorder
	rec2 := <-recorder
	assert.Empty(t, rec1.req.GetResumeToken(), "First attempt should have empty resume token")
	assert.Empty(t, rec2.req.GetResumeToken(), "Second attempt should also have empty resume token since no token was received yet")
	assert.Len(t, res.Rows, 1)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1}, res.Rows)
}

// TestTypedReadRows_Resumption_FlushInSeparateMessage verifies that when the server separates
// the data batch and the flush indicator into distinct messages, the client buffers the batch data,
// commits on the subsequent flush message, and resumes properly if dropped thereafter.
func TestTypedReadRows_Resumption_FlushInSeparateMessage(t *testing.T) {
	// 0. Common variables
	row1 := makeTypedRow([]byte("row-sep-1"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v1")))),
	)
	row2 := makeTypedRow([]byte("row-sep-2"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v2")))),
	)

	data1 := serializeTypedRows(row1)
	crc1 := trrMetaChecksum(data1)
	data2 := serializeTypedRows(row2)
	crc2 := trrMetaChecksumMulti(data1, data2)

	respData1 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data1},
			},
		},
	}
	respFlush1 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crc1,
				ResumeToken: []byte("token-sep-msg-1"),
			},
		},
	}
	respData2 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data2},
			},
		},
	}
	respFlush2 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crc2,
				ResumeToken: []byte("token-sep-msg-2"),
			},
		},
	}

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		&typedReadRowsAction{response: respData1},
		&typedReadRowsAction{response: respFlush1},
		&typedReadRowsAction{rpcError: codes.Unavailable},
		&typedReadRowsAction{response: respData2},
		&typedReadRowsAction{response: respFlush2},
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("sep-msg-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	rec1 := <-recorder
	rec2 := <-recorder
	assert.Empty(t, rec1.req.GetResumeToken())
	assert.Equal(t, []byte("token-sep-msg-1"), rec2.req.GetResumeToken())
	assert.Len(t, res.Rows, 2)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_Resumption_EmptyBatchData verifies that empty batch data messages
// (zero bytes) are handled gracefully by the chunk defragmenter and do not corrupt resumption.
func TestTypedReadRows_Resumption_EmptyBatchData(t *testing.T) {
	// 0. Common variables
	row1 := makeTypedRow([]byte("row-empty-batch-1"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v1")))),
	)
	row2 := makeTypedRow([]byte("row-empty-batch-2"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v2")))),
	)

	data1 := serializeTypedRows(row1)
	crc1 := trrMetaChecksum(data1)
	data2 := serializeTypedRows(row2)
	crc2 := trrMetaChecksumMulti(data1, data2)

	respEmpty := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: []byte{}},
			},
		},
	}
	resp1 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data1},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crc1,
				ResumeToken: []byte("token-empty-batch-1"),
			},
		},
	}
	resp2 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data2},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crc2,
				ResumeToken: []byte("token-empty-batch-2"),
			},
		},
	}

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		&typedReadRowsAction{response: respEmpty},
		&typedReadRowsAction{response: resp1},
		&typedReadRowsAction{rpcError: codes.Unavailable},
		&typedReadRowsAction{response: resp2},
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("empty-chunk-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	rec1 := <-recorder
	rec2 := <-recorder
	assert.Empty(t, rec1.req.GetResumeToken())
	assert.Equal(t, []byte("token-empty-batch-1"), rec2.req.GetResumeToken())
	assert.Len(t, res.Rows, 2)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_Resumption_EmptyTableWithToken verifies that an empty table scan emitting
// a resume_token followed by a stream drop correctly reconnects with that token and completes.
func TestTypedReadRows_Resumption_EmptyTableWithToken(t *testing.T) {
	// 0. Common variables
	respFlush := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			Flush: &btpb.PartialRowResponse_Flush{
				ResumeToken: []byte("token-empty-table"),
			},
		},
	}

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		&typedReadRowsAction{response: respFlush},
		&typedReadRowsAction{rpcError: codes.Unavailable},
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("empty-table-resumption")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	rec1 := <-recorder
	rec2 := <-recorder
	assert.Empty(t, rec1.req.GetResumeToken())
	assert.Equal(t, []byte("token-empty-table"), rec2.req.GetResumeToken())
	assert.Empty(t, res.Rows)
}

// TestTypedReadRows_MissingInitialTableSchema_Fails verifies that when the server omits TableSchema
// on the very first message carrying data, the client fails fast rather than processing data.
func TestTypedReadRows_MissingInitialTableSchema_Fails(t *testing.T) {
	// 0. Common variables
	row := makeTypedRow([]byte("row-no-schema"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v")))),
	)
	data := serializeTypedRows(row)

	crc := trrMetaChecksum(data)
	resp := &btpb.TypedReadRowsResponse{
		// Note: TableSchema is nil!
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crc,
				ResumeToken: []byte("token-no-schema"),
			},
		},
	}

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.DisableTypedReadRowsAutoSchema = true
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, &typedReadRowsAction{response: resp})

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("missing-schema-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	assert.NotEqual(t, int32(codes.OK), res.GetStatus().GetCode(),
		"expected failure when the initial TableSchema is missing")
	assert.Empty(t, res.GetRows(), "rows must not be yielded without a schema")
	t.Logf("The full error message is: %s", res.GetStatus().GetMessage())
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
	// 0. Common variables
	structuredSchema := &btpb.TableSchema{
		RowKeySchema: &btpb.Type_Struct{
			Fields: []*btpb.Type_Struct_Field{
				{FieldName: "part1", Type: &btpb.Type{Kind: &btpb.Type_StringType{StringType: &btpb.Type_String{}}}},
			},
		},
	}
	rawKeyRow := dummyTypedRow("row-raw-key", "v")
	arrayKeyRow := &btpb.TypedRow{
		RowKey: &btpb.Value{Kind: &btpb.Value_ArrayValue{
			ArrayValue: &btpb.ArrayValue{
				Values: []*btpb.Value{{Kind: &btpb.Value_StringValue{StringValue: "part1-val"}}},
			},
		}},
		Families: []*btpb.TypedFamily{
			makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v")))),
		},
	}

	testCases := []struct {
		name   string
		schema *btpb.TableSchema
		row    *btpb.TypedRow
	}{
		{"structured schema with raw_value key", structuredSchema, rawKeyRow},
		{"unstructured schema with array_value key", &btpb.TableSchema{}, arrayKeyRow},
	}

	for _, tc := range testCases {
		data := serializeTypedRows(tc.row)
		crc := trrMetaChecksum(data)

		// 1. Instantiate the mock server
		server := initMockServer(t)
		server.TypedReadRowsFn = mockTypedReadRowsFn(nil, &typedReadRowsAction{
			response: &btpb.TypedReadRowsResponse{
				TableSchema: tc.schema,
				Response: &btpb.PartialRowResponse{
					PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
						TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data},
					},
					Flush: &btpb.PartialRowResponse_Flush{
						Checksum:    &crc,
						ResumeToken: []byte("token-mismatch"),
					},
				},
			},
		})

		// 2. Build the request to test proxy
		req := &testproxypb.TypedReadRowsRequest{
			ClientId: fmt.Sprintf("%s-%s", t.Name(), tc.name),
			Request:  makeTypedReadRowsRequest(buildTableName("schema-mismatch-table")),
		}

		// 3. Perform the operation via test proxy
		res := doTypedReadRowsOp(t, server, req, nil)

		// 4. Check the response
		assert.NotEqual(t, int32(codes.OK), res.GetStatus().GetCode(),
			"expected failure for %s", tc.name)
		assert.Empty(t, res.GetRows(), "rows contradicting the schema must not be yielded (%s)", tc.name)
		t.Logf("The full error message for %s is: %s", tc.name, res.GetStatus().GetMessage())
	}
}

// TestTypedReadRows_DuplicateTableSchema_MidStream verifies that a TableSchema repeated on a
// later response is tolerated: the client keeps merging normally rather than treating the
// redundant schema as a protocol violation. Both responses set TableSchema explicitly, so the
// mock's auto-schema (which only populates the first message of a stream) is not involved.
func TestTypedReadRows_DuplicateTableSchema_MidStream(t *testing.T) {
	// 0. Common variables
	row1 := makeTypedRow([]byte("row-schema-1"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v1")))),
	)
	row2 := makeTypedRow([]byte("row-schema-2"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v2")))),
	)

	data1 := serializeTypedRows(row1)
	data2 := serializeTypedRows(row2)
	crc1 := trrMetaChecksum(data1)
	crc2 := trrMetaChecksumMulti(data1, data2)

	resp1 := &btpb.TypedReadRowsResponse{
		TableSchema: &btpb.TableSchema{},
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data1},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crc1,
				ResumeToken: []byte("token-s1"),
			},
		},
	}

	resp2 := &btpb.TypedReadRowsResponse{
		TableSchema: &btpb.TableSchema{}, // Resent TableSchema mid-stream
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data2},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crc2,
				ResumeToken: []byte("token-s2"),
			},
		},
	}

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, responsesToActions(resp1, resp2)...)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("dup-schema-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
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
// Structured array_value only ever appears in row keys (see MidStream_SchemaEvolution) and in
// TypedRowSet.row_prefixes on the request side.
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

	data := serializeTypedRows(row)
	responses := chunkedTypedResponses(data, len(data), []byte("token-cell-edge-cases"), true)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, responsesToActions(responses...)...)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("cell-edge-cases-table")),
	}

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
	row := &btpb.TypedRow{
		RowKey: &btpb.Value{
			Kind: &btpb.Value_RawValue{RawValue: []byte("row-ordering")},
		},
		Families: []*btpb.TypedFamily{
			{
				FamilyName: "fam_a",
				Columns: []*btpb.TypedColumn{
					{
						Qualifier: &btpb.Value{Kind: &btpb.Value_RawValue{RawValue: []byte("col_1")}},
						Cells: []*btpb.TypedCell{
							makeTypedCellWithTimestamp([]byte("v1_newest"), 3000),
							makeTypedCellWithTimestamp([]byte("v1_middle"), 2000),
							makeTypedCellWithTimestamp([]byte("v1_oldest"), 1000),
						},
					},
					{
						Qualifier: &btpb.Value{Kind: &btpb.Value_RawValue{RawValue: []byte("col_2")}},
						Cells: []*btpb.TypedCell{
							makeTypedCellWithTimestamp([]byte("v2_cell"), 1000),
						},
					},
				},
			},
			{
				FamilyName: "fam_b",
				Columns: []*btpb.TypedColumn{
					{
						Qualifier: &btpb.Value{Kind: &btpb.Value_RawValue{RawValue: []byte("col_x")}},
						Cells: []*btpb.TypedCell{
							makeTypedCellWithTimestamp([]byte("vx_cell"), 1000),
						},
					},
				},
			},
		},
	}

	data := serializeTypedRows(row)

	responses := chunkedTypedResponses(data, len(data), []byte("token-ordering"), true)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, responsesToActions(responses...)...)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("ordering-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row}, res.Rows)
}

// TestTypedReadRows_ConsecutiveResets verifies that back-to-back reset signals are handled
// correctly: the first discards genuinely buffered data, and the second -- arriving with nothing
// left to discard -- must be a harmless no-op rather than underflowing the client's state buffers.
func TestTypedReadRows_ConsecutiveResets(t *testing.T) {
	// 0. Common variables
	row := dummyTypedRow("row-consecutive-resets", "clean")

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil,
		// Buffer a row first so that reset #1 has something real to throw away. Without this the
		// test would only ever exercise resetting an already-empty buffer.
		dummyUncommittedAction("row-discarded", "gone"),
		typedResetAction(),
		typedResetAction(),
		dummyTypedAction("row-consecutive-resets", "clean", "token-resets"),
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("consecutive-resets-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	// Only the post-reset row survives; "row-discarded" was dropped by the first reset.
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row}, res.Rows)
}

// TestTypedReadRows_ReverseScans_FeatureFlag_Enabled verifies that the client advertises the
// ReverseScans capability in its feature-flag header when issuing a reversed TypedReadRows.
// Mirrors TestReadRows_ReverseScans_FeatureFlag_Enabled in readrows_test.go.
func TestTypedReadRows_ReverseScans_FeatureFlag_Enabled(t *testing.T) {
	// 1. Instantiate the mock server
	// Don't call mockTypedReadRowsFn() as the behavior is to record metadata of the request
	mdRecords := make(chan metadata.MD, 1)
	server := initMockServer(t)
	server.TypedReadRowsFn = func(req *btpb.TypedReadRowsRequest, srv btpb.Bigtable_TypedReadRowsServer) error {
		md, _ := metadata.FromIncomingContext(srv.Context())
		mdRecords <- md
		return nil
	}

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request: &btpb.TypedReadRowsRequest{
			Target: &btpb.TypedReadRowsRequest_TableName{
				TableName: buildTableName("reverse-feature-flag-table"),
			},
			Reversed: true,
		},
	}

	// 3. Perform the operation via test proxy
	doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the request headers in the metadata
	md := <-mdRecords

	ff, err := getClientFeatureFlags(md)
	assert.Nil(t, err, "failed to decode client feature flags")
	assert.True(t, ff != nil && ff.ReverseScans, "client must enable ReverseScans feature flag")
}

// TestTypedReadRows_ReverseScan_Success verifies reading rows in descending order with reversed=true.
func TestTypedReadRows_ReverseScan_Success(t *testing.T) {
	// 0. Common variables
	row3 := dummyTypedRow("row-03", "v3")
	row2 := dummyTypedRow("row-02", "v2")
	row1 := dummyTypedRow("row-01", "v1")

	recorder := make(chan *typedReadRowsReqRecord, 1)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, typedFlushAction("token-reverse", []*btpb.TypedRow{row3, row2, row1}))

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request: &btpb.TypedReadRowsRequest{
			Target: &btpb.TypedReadRowsRequest_TableName{
				TableName: buildTableName("reverse-table"),
			},
			Reversed: true,
		},
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	rec := <-recorder
	assert.True(t, rec.req.GetReversed())
	assertTypedRowsEqual(t, []*btpb.TypedRow{row3, row2, row1}, res.Rows)
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
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request: &btpb.TypedReadRowsRequest{
			Target: &btpb.TypedReadRowsRequest_TableName{
				TableName: buildTableName("reverse-resumption-table"),
			},
			Reversed: true,
		},
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	rec1 := <-recorder
	rec2 := <-recorder
	assert.Empty(t, rec1.req.GetResumeToken())
	assert.True(t, rec1.req.GetReversed())
	assert.Equal(t, []byte("token-r3"), rec2.req.GetResumeToken())
	assert.True(t, rec2.req.GetReversed())
	assertTypedRowsEqual(t, []*btpb.TypedRow{row3, row2, row1}, res.Rows)
}

// TestTypedReadRows_Resumption_RowsLimitDecremented verifies that on retryable disconnect,
// the client automatically decrements rows_limit by the number of delivered rows.
func TestTypedReadRows_Resumption_RowsLimitDecremented(t *testing.T) {
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

	rawReq := makeTypedReadRowsRequest(buildTableName("rows-limit-table"))
	rawReq.RowsLimit = 5

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  rawReq,
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	rec1 := <-recorder
	rec2 := <-recorder
	assert.Equal(t, int64(5), rec1.req.GetRowsLimit())
	assert.Equal(t, int64(3), rec2.req.GetRowsLimit())
	assert.Equal(t, []byte("token-r2"), rec2.req.GetResumeToken())
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2, row3, row4, row5}, res.Rows)
}

// TestTypedReadRows_Resumption_RowsLimitFulfilledBeforeDisconnect verifies that if rows_limit
// has already been completely fulfilled before a stream disconnect occurs, the client does not retry.
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
	)

	rawReq := makeTypedReadRowsRequest(buildTableName("rows-limit-fulfilled-table"))
	rawReq.RowsLimit = 2

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  rawReq,
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assert.Equal(t, 1, len(recorder))
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_MissingResumeToken_Fails verifies that a Flush carrying a checksum but an
// empty resume_token is rejected.
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
	// 0. Common variables
	row := makeTypedRow([]byte("row1"), makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v")))))
	data := serializeTypedRows(row)
	crc := trrMetaChecksum(data)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, &typedReadRowsAction{
		response: &btpb.TypedReadRowsResponse{
			Response: &btpb.PartialRowResponse{
				PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
					TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data},
				},
				Flush: &btpb.PartialRowResponse_Flush{
					Checksum:    &crc,
					ResumeToken: nil, // Omitted resume token
				},
			},
		},
	})

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("missing-token-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	assert.NotEqual(t, int32(codes.OK), res.GetStatus().GetCode(), "expected failure when flush has no resume token")
	assert.Empty(t, res.GetRows(), "rows must not be yielded without a resume token")
	t.Logf("The full error message is: %s", res.GetStatus().GetMessage())
}

// TestTypedReadRows_NonEmptyBatchWithoutChecksum_Fails verifies that a non-empty batch flushed without a checksum fails.
func TestTypedReadRows_NonEmptyBatchWithoutChecksum_Fails(t *testing.T) {
	// 0. Common variables
	row := makeTypedRow([]byte("row1"), makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v")))))
	data := serializeTypedRows(row)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.DisableTypedReadRowsAutoChecksum = true
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, &typedReadRowsAction{
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

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("no-checksum-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	assert.NotEqual(t, int32(codes.OK), res.GetStatus().GetCode(), "expected failure when non-empty batch lacks checksum")
	assert.Empty(t, res.GetRows(), "unverified rows must not be yielded")
	t.Logf("The full error message is: %s", res.GetStatus().GetMessage())
}

// TestTypedReadRows_RequestFields_TypedRowSet verifies that row_keys, row_prefixes, and row_ranges in TypedRowSet are propagated.
func TestTypedReadRows_RequestFields_TypedRowSet(t *testing.T) {
	// 0. Common variables
	recorder := make(chan *typedReadRowsReqRecord, 1)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, dummyTypedAction("key1", "v", "tok"))

	rawReq := makeTypedReadRowsRequest(buildTableName("rowset-table"))
	// This is an unstructured scan (the request carries no row key schema), so row keys and range
	// bounds are raw_value -- the same kind the client requires on the response side. row_prefixes
	// is deliberately different: data.proto specifies each prefix as an array_value giving a
	// leading subset of the row-key schema fields.
	rawReq.Rows = &btpb.TypedRowSet{
		RowKeys: []*btpb.Value{
			{Kind: &btpb.Value_RawValue{RawValue: []byte("key1")}},
			{Kind: &btpb.Value_RawValue{RawValue: []byte("key2")}},
		},
		RowPrefixes: []*btpb.Value{
			{Kind: &btpb.Value_ArrayValue{ArrayValue: &btpb.ArrayValue{
				Values: []*btpb.Value{{Kind: &btpb.Value_StringValue{StringValue: "pref-"}}},
			}}},
		},
		RowRanges: []*btpb.TypedValueRange{
			{
				StartValue: &btpb.TypedValueRange_StartValueClosed{
					StartValueClosed: &btpb.Value{Kind: &btpb.Value_RawValue{RawValue: []byte("start-val")}},
				},
				EndValue: &btpb.TypedValueRange_EndValueOpen{
					EndValueOpen: &btpb.Value{Kind: &btpb.Value_RawValue{RawValue: []byte("end-val")}},
				},
			},
		},
	}

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  rawReq,
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assert.Equal(t, 1, len(recorder))
	rec := <-recorder
	capturedReq := rec.req
	assert.NotNil(t, capturedReq)
	assert.NotNil(t, capturedReq.GetRows())
	assert.Equal(t, 2, len(capturedReq.GetRows().GetRowKeys()))
	assert.Equal(t, []byte("key1"), capturedReq.GetRows().GetRowKeys()[0].GetRawValue())
	assert.Equal(t, []byte("key2"), capturedReq.GetRows().GetRowKeys()[1].GetRawValue())
	assert.Equal(t, 1, len(capturedReq.GetRows().GetRowPrefixes()))
	prefixParts := capturedReq.GetRows().GetRowPrefixes()[0].GetArrayValue().GetValues()
	assert.Equal(t, 1, len(prefixParts))
	assert.Equal(t, "pref-", prefixParts[0].GetStringValue())
	assert.Equal(t, 1, len(capturedReq.GetRows().GetRowRanges()))
	assert.Equal(t, []byte("start-val"), capturedReq.GetRows().GetRowRanges()[0].GetStartValueClosed().GetRawValue())
	assert.Equal(t, []byte("end-val"), capturedReq.GetRows().GetRowRanges()[0].GetEndValueOpen().GetRawValue())
}

// TestTypedReadRows_RequestFields_RowFilter verifies that RowFilter is propagated faithfully.
func TestTypedReadRows_RequestFields_RowFilter(t *testing.T) {
	// 0. Common variables
	recorder := make(chan *typedReadRowsReqRecord, 1)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, dummyTypedAction("row1", "v", "tok"))

	rawReq := makeTypedReadRowsRequest(buildTableName("filter-table"))
	rawReq.Filter = &btpb.RowFilter{
		Filter: &btpb.RowFilter_CellsPerColumnLimitFilter{
			CellsPerColumnLimitFilter: 7,
		},
	}

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  rawReq,
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assert.Equal(t, 1, len(recorder))
	rec := <-recorder
	capturedReq := rec.req
	assert.NotNil(t, capturedReq)
	assert.NotNil(t, capturedReq.GetFilter())
	assert.Equal(t, int32(7), capturedReq.GetFilter().GetCellsPerColumnLimitFilter())
}

// TestTypedReadRows_RequestFields_UseStructuredKey verifies propagation of use_structured_key.
func TestTypedReadRows_RequestFields_UseStructuredKey(t *testing.T) {
	// 0. Common variables
	for _, structured := range []bool{false, true} {
		recorder := make(chan *typedReadRowsReqRecord, 1)

		// 1. Instantiate the mock server
		server := initMockServer(t)
		server.TypedReadRowsFn = mockTypedReadRowsFn(recorder, dummyTypedAction("row1", "v", "tok"))

		rawReq := makeTypedReadRowsRequest(buildTableName("use-structured-key-table"))
		rawReq.RowKeyFormat = &btpb.TypedReadRowsRequest_UseStructuredKey{UseStructuredKey: structured}

		// 2. Build the request to test proxy
		req := &testproxypb.TypedReadRowsRequest{
			ClientId: fmt.Sprintf("%s-%v", t.Name(), structured),
			Request:  rawReq,
		}

		// 3. Perform the operation via test proxy
		res := doTypedReadRowsOp(t, server, req, nil)

		// 4. Check the response
		checkResultOkStatus(t, res)
		assert.Equal(t, 1, len(recorder))
		rec := <-recorder
		capturedReq := rec.req
		assert.NotNil(t, capturedReq)
		// GetUseStructuredKey() returns false both when the oneof is set to false and when it is
		// not set at all, so assert presence separately -- otherwise the structured=false pass
		// would still succeed against a client that dropped the field entirely.
		assert.NotNil(t, capturedReq.GetRowKeyFormat(),
			"the row_key_format oneof must be propagated even when use_structured_key is false")
		assert.Equal(t, structured, capturedReq.GetUseStructuredKey())
	}
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
	// 0. Common variables
	// Response 1 declares a structured row key...
	schema1 := &btpb.TableSchema{
		RowKeySchema: &btpb.Type_Struct{
			Fields: []*btpb.Type_Struct_Field{
				{FieldName: "part1", Type: &btpb.Type{Kind: &btpb.Type_StringType{StringType: &btpb.Type_String{}}}},
			},
		},
	}
	// ...and response 2 withdraws it, so row keys revert to unstructured raw_value.
	schema2 := &btpb.TableSchema{}

	row1 := &btpb.TypedRow{
		RowKey: &btpb.Value{Kind: &btpb.Value_ArrayValue{
			ArrayValue: &btpb.ArrayValue{
				Values: []*btpb.Value{{Kind: &btpb.Value_StringValue{StringValue: "val1"}}},
			},
		}},
		Families: []*btpb.TypedFamily{
			makeTypedFamily("cf",
				makeTypedColumn([]byte("col1"), makeTypedCell([]byte("val1"))),
				makeTypedColumn([]byte("col2"), makeTypedCell([]byte("12345"))),
			),
		},
	}
	row2 := dummyTypedRow("row-unstructured", "val2")

	data1 := serializeTypedRows(row1)
	crc1 := trrMetaChecksum(data1)

	data2 := serializeTypedRows(row2)
	crc2 := trrMetaChecksumMulti(data1, data2)

	resp1 := &btpb.TypedReadRowsResponse{
		TableSchema: schema1,
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data1},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crc1,
				ResumeToken: []byte("token-1"),
			},
		},
	}
	resp2 := &btpb.TypedReadRowsResponse{
		TableSchema: schema2,
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data2},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crc2,
				ResumeToken: []byte("token-2"),
			},
		},
	}

	// 1. Instantiate the mock server
	server := initMockServer(t)
	// Both responses set TableSchema explicitly, so auto-schema would not fire anyway; disabling
	// it makes the intent unambiguous.
	server.DisableTypedReadRowsAutoSchema = true
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, responsesToActions(resp1, resp2)...)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("schema-evolution-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_EmptyTypedRowsBatch_ZeroRows verifies that a batch containing a serialized TypedRows
// with zero rows is processed cleanly without emitting spurious rows.
func TestTypedReadRows_EmptyTypedRowsBatch_ZeroRows(t *testing.T) {
	// 0. Common variables
	emptyBatchData, err := proto.Marshal(&btpb.TypedRows{Rows: []*btpb.TypedRow{}})
	assert.NoError(t, err)

	row := makeTypedRow([]byte("row1"), makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("v")))))
	dataRow := serializeTypedRows(row)
	crcRow := trrMetaChecksum(dataRow)

	// Marshalling a TypedRows with no rows yields zero bytes, so this batch is empty on the wire.
	// The flush therefore carries no checksum, which is legal: the client's "a non-empty batch
	// must be accompanied by a checksum" rule does not fire, and the mock's accumulator skips
	// empty batches so the running CRC is left untouched. That is why resp2 below can use a
	// single-batch trrMetaChecksum rather than a cumulative one.
	resp1 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: emptyBatchData},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				ResumeToken: []byte("token-empty"),
			},
		},
	}
	resp2 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: dataRow},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    &crcRow,
				ResumeToken: []byte("token-row"),
			},
		},
	}

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, responsesToActions(resp1, resp2)...)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("empty-batch-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row}, res.Rows)
}

// TestTypedReadRows_Resumption_MultipleSequentialHeartbeats verifies that sequential heartbeat flushes
// advance the resume token correctly and the client resumes using the latest heartbeat token.
func TestTypedReadRows_Resumption_MultipleSequentialHeartbeats(t *testing.T) {
	// 0. Common variables
	row := dummyTypedRow("row1", "v")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		typedHeartbeatAction("token-hb-1"),
		typedHeartbeatAction("token-hb-2"),
		typedHeartbeatAction("token-hb-3"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		dummyTypedAction("row1", "v", "token-row1"),
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("heartbeats-resumption-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assert.Equal(t, 2, len(recorder))
	rec1 := <-recorder
	rec2 := <-recorder
	assert.Empty(t, rec1.req.GetResumeToken())
	assert.Equal(t, []byte("token-hb-3"), rec2.req.GetResumeToken())
	assertTypedRowsEqual(t, []*btpb.TypedRow{row}, res.Rows)
}

// TestTypedReadRows_Retry_WithRoutingCookie verifies that a routing cookie returned in an error
// trailer is echoed on the retry attempt, and that the retry still resumes from the last
// committed token rather than restarting.
//
// Mirrors TestReadRows_Retry_WithRoutingCookie, which likewise commits a row before the error so
// that the retry request itself is worth inspecting (its step 4c).
func TestTypedReadRows_Retry_WithRoutingCookie(t *testing.T) {
	// 0. Common variables
	cookie := "test-cookie-trr"
	row1 := dummyTypedRow("row1", "v1")
	row2 := dummyTypedRow("row2", "v2")

	mdRecords := make(chan metadata.MD, 2)
	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFnWithMetadata(recorder, mdRecords,
		dummyTypedAction("row1", "v1", "token-r1"),
		&typedReadRowsAction{rpcError: codes.Unavailable, routingCookie: cookie},
		dummyTypedAction("row2", "v2", "token-r2", row1),
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("routing-cookie-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4a. Verify that the read succeeds
	checkResultOkStatus(t, res)
	assert.Equal(t, 2, len(recorder))
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)

	// 4b. Verify routing cookie is seen
	// First attempt does not have the cookie
	<-mdRecords
	// Second attempt must have the cookie
	md2 := <-mdRecords
	val := md2["x-goog-cbt-cookie-test"]
	assert.NotEmpty(t, val)
	if len(val) == 0 {
		return
	}
	assert.Equal(t, cookie, val[0])

	// 4c. Verify the retry request resumed from the token committed before the error
	<-recorder
	retryReq := <-recorder
	assert.Equal(t, []byte("token-r1"), retryReq.req.GetResumeToken())
}

// TestTypedReadRows_Retry_WithRetryInfo verifies that RetryInfo backoff delay is respected on transient errors.
func TestTypedReadRows_Retry_WithRetryInfo(t *testing.T) {
	// 0. Common variables
	row := dummyTypedRow("row1", "v")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		&typedReadRowsAction{rpcError: codes.Unavailable, retryInfo: "2s"},
		dummyTypedAction("row1", "v", "token-r1"),
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("retry-info-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assert.Equal(t, 2, len(recorder))
	assertTypedRowsEqual(t, []*btpb.TypedRow{row}, res.Rows)

	firstReq := <-recorder
	retryReq := <-recorder
	assert.True(t, retryReq.ts.Sub(firstReq.ts) >= 2*time.Second, "expected retry delay to be at least 2 seconds")
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
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("deadline-table")),
	}

	opts := clientOpts{
		timeout: durationpb.New(2 * time.Second),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, &opts)

	// 4. Check the response
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

// TestTypedReadRows_UnrecognizedType_IgnoredDuringStream verifies that when a cell contains
// an unrecognized Value field (such as a future data type), the client does not abort the stream
// and successfully merges the row, allowing known columns to be read.
func TestTypedReadRows_UnrecognizedType_IgnoredDuringStream(t *testing.T) {
	// 0. Common variables
	// Construct an unrecognized Value using unknown proto field tag 99 (wire type 2)
	unknownVal := &btpb.Value{}
	// Tag 99, wire type 2: (99 << 3) | 2 = 794 (varint 0x9a, 0x06), length 4 ("data")
	err := proto.Unmarshal([]byte{0x9a, 0x06, 0x04, 'd', 'a', 't', 'a'}, unknownVal)
	assert.NoError(t, err)

	row := &btpb.TypedRow{
		RowKey: &btpb.Value{Kind: &btpb.Value_RawValue{RawValue: []byte("row-unrecognized-type")}},
		Families: []*btpb.TypedFamily{
			makeTypedFamily("cf",
				makeTypedColumn([]byte("col_known"), makeTypedCell([]byte("known_value"))),
				makeTypedColumn([]byte("col_future"), &btpb.TypedCell{
					Timestamp: timestamppb.New(time.UnixMicro(0)),
					Value:     unknownVal,
				}),
			),
		},
	}

	data := serializeTypedRows(row)
	crc := trrMetaChecksum(data)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, &typedReadRowsAction{
		response: &btpb.TypedReadRowsResponse{
			Response: &btpb.PartialRowResponse{
				PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
					TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data},
				},
				Flush: &btpb.PartialRowResponse_Flush{
					Checksum:    &crc,
					ResumeToken: []byte("tok-unrecognized"),
				},
			},
		},
	})

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("unrecognized-type-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assert.Equal(t, 1, len(res.Rows))
	assertTypedRowsEqual(t, []*btpb.TypedRow{row}, res.Rows)
}

// TestTypedReadRows_AutoChecksum_TerminalAutoFlush_AfterExplicitFlush verifies that
// after an explicit flush without checksum, a subsequent un-flushed batch is automatically
// committed at stream completion by autoFlush with the correct running cumulative checksum.
func TestTypedReadRows_AutoChecksum_TerminalAutoFlush_AfterExplicitFlush(t *testing.T) {
	// 0. Common variables
	row1 := makeTypedRow([]byte("term-crc-row-1"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("data1")))),
	)
	row2 := makeTypedRow([]byte("term-crc-row-2"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("data2")))),
	)

	data1 := serializeTypedRows(row1)
	data2 := serializeTypedRows(row2)

	resp1 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data1},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				ResumeToken: []byte("tok-term-1"),
			},
		},
	}
	resp2 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data2},
			},
		},
	}

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, responsesToActions(resp1, resp2)...)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("autocrc-term-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assert.Equal(t, 2, len(res.Rows))
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
}

// TestTypedReadRows_AutoChecksum_ResetPreservesCommittedAndDiscardsUncommitted verifies that a
// reset removes the discarded batch from the *running checksum*, not merely from the client's
// row buffer, while previously committed batch hashes are preserved.
//
// The wire shape here is the same as TestTypedReadRows_Reset_DiscardsUncommitted, but the two
// tests pin down different halves of the contract. There the {d1, d3} checksum is supplied by the
// fixture, so the mock's buffer state is never consulted and only the client is under test. Here
// the checksum comes from the mock's own accumulator, so a mock that failed to clear currentBatch
// on reset would emit a checksum over {d1, d2+d3}, the client would reject it, and this test
// would fail. It is the only test that reaches that branch of mockserver.go, which is why it is
// kept alongside the explicit-checksum version rather than folded into it.
func TestTypedReadRows_AutoChecksum_ResetPreservesCommittedAndDiscardsUncommitted(t *testing.T) {
	// 0. Common variables
	row1 := makeTypedRow([]byte("reset-crc-row-1"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("val1")))),
	)
	row2 := makeTypedRow([]byte("reset-crc-row-2"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("val2-discarded")))),
	)
	row3 := makeTypedRow([]byte("reset-crc-row-3"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("val3")))),
	)

	data1 := serializeTypedRows(row1)
	data2 := serializeTypedRows(row2)
	data3 := serializeTypedRows(row3)

	resp1 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data1},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				ResumeToken: []byte("tok-r1"),
			},
		},
	}
	resp2 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data2},
			},
		},
	}
	respReset := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			Reset_: true,
		},
	}
	resp3 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data3},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				ResumeToken: []byte("tok-r3"),
			},
		},
	}

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, responsesToActions(resp1, resp2, respReset, resp3)...)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("autocrc-reset-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assert.Equal(t, 2, len(res.Rows))
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row3}, res.Rows)
}

// TestTypedReadRows_AutoChecksum_HeartbeatDoesNotCorruptRunningChecksum verifies that
// an intermediate sparse query heartbeat (flush with resume token but no batch data) does not
// pollute the running checksum history, allowing subsequent data flushes with auto-checksum to succeed.
func TestTypedReadRows_AutoChecksum_HeartbeatDoesNotCorruptRunningChecksum(t *testing.T) {
	// 0. Common variables
	row1 := makeTypedRow([]byte("hb-crc-row-1"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("payload-1")))),
	)
	row2 := makeTypedRow([]byte("hb-crc-row-2"),
		makeTypedFamily("cf", makeTypedColumn([]byte("c"), makeTypedCell([]byte("payload-2")))),
	)

	data1 := serializeTypedRows(row1)
	data2 := serializeTypedRows(row2)

	resp1 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data1},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				ResumeToken: []byte("tok-hb-1"),
			},
		},
	}
	respHb := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			Flush: &btpb.PartialRowResponse_Flush{
				ResumeToken: []byte("tok-heartbeat-midstream"),
			},
		},
	}
	resp2 := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: data2},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				ResumeToken: []byte("tok-hb-2"),
			},
		},
	}

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil, responsesToActions(resp1, respHb, resp2)...)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("autocrc-hb-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assert.Equal(t, 2, len(res.Rows))
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)
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
	// 0. Common variables
	row1 := dummyTypedRow("row1", "v1")
	row2 := dummyTypedRow("row2", "v2")

	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("row1", "v1", "token-r1"),
		&typedReadRowsAction{rpcError: codes.Unavailable},
		dummyTypedAction("row2", "v2", "token-r2", row1),
	)

	rawReq := makeTypedReadRowsRequest(buildTableName("rowset-unmodified-table"))
	// Unstructured scan, so row keys and range bounds are raw_value -- the same kind the client
	// requires on the response side.
	rawReq.Rows = &btpb.TypedRowSet{
		RowKeys: []*btpb.Value{
			{Kind: &btpb.Value_RawValue{RawValue: []byte("row1")}},
			{Kind: &btpb.Value_RawValue{RawValue: []byte("row2")}},
		},
		RowRanges: []*btpb.TypedValueRange{
			{
				StartValue: &btpb.TypedValueRange_StartValueClosed{
					StartValueClosed: &btpb.Value{Kind: &btpb.Value_RawValue{RawValue: []byte("row0")}},
				},
				EndValue: &btpb.TypedValueRange_EndValueOpen{
					EndValueOpen: &btpb.Value{Kind: &btpb.Value_RawValue{RawValue: []byte("row9")}},
				},
			},
		},
	}

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  rawReq,
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2}, res.Rows)

	firstReq := <-recorder
	retryReq := <-recorder

	// Sanity check: this really is a resumed attempt.
	assert.Empty(t, firstReq.req.GetResumeToken())
	assert.Equal(t, []byte("token-r1"), retryReq.req.GetResumeToken())

	// The RowSet on the retry must be byte-for-byte what the caller supplied.
	if diff := cmp.Diff(rawReq.GetRows(), retryReq.req.GetRows(), protocmp.Transform()); diff != "" {
		t.Errorf("RowSet was modified on resume (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(firstReq.req.GetRows(), retryReq.req.GetRows(), protocmp.Transform()); diff != "" {
		t.Errorf("RowSet differs between first attempt and retry (-first +retry):\n%s", diff)
	}
}

// TestTypedReadRows_Reset_MidFragment verifies that reset=true clears a partially received batch,
// not just fully-formed uncommitted batches.
//
// If the client only drops parsed rows and forgets to clear the raw byte buffer, the surviving
// partial bytes get concatenated with the next batch and deserialization fails later, far from the
// actual cause.
func TestTypedReadRows_Reset_MidFragment(t *testing.T) {
	// 0. Common variables
	garbageRow := dummyTypedRow("row-discarded", "this-value-is-long-enough-to-fragment")
	validRow := dummyTypedRow("row-after-reset", "v")

	garbageData := serializeTypedRows(garbageRow)
	// Send only the leading third, so the buffer ends mid-field.
	partial := garbageData[:len(garbageData)/3]
	assert.NotEmpty(t, partial)

	partialResp := &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{BatchData: partial},
			},
		},
	}

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil,
		&typedReadRowsAction{response: partialResp},
		typedResetAction(),
		dummyTypedAction("row-after-reset", "v", "token-after-reset"),
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("reset-midfragment-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{validRow}, res.Rows)
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
		reqsBatchOne[i] = &testproxypb.TypedReadRowsRequest{
			ClientId: clientID,
			Request:  makeTypedReadRowsRequest(buildTableName(fmt.Sprintf("op%d-table", i))),
		}
		reqsBatchTwo[i] = &testproxypb.TypedReadRowsRequest{
			ClientId: clientID,
			Request:  makeTypedReadRowsRequest(buildTableName(fmt.Sprintf("op%d-table", i+halfBatchSize))),
		}
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
	// 0. Common variables
	cookie := "test-cookie-trr"
	newCookie := "new-test-cookie-trr"
	row1 := dummyTypedRow("row-01", "v1")
	row5 := dummyTypedRow("row-05", "v5")

	mdRecords := make(chan metadata.MD, 4)
	recorder := make(chan *typedReadRowsReqRecord, 4)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFnWithMetadata(recorder, mdRecords,
		dummyTypedAction("row-01", "v1", "token-r1"),
		&typedReadRowsAction{rpcError: codes.Unavailable, routingCookie: cookie},    // Error with a routing cookie
		&typedReadRowsAction{rpcError: codes.Unavailable},                           // Error with no routing cookie
		&typedReadRowsAction{rpcError: codes.Unavailable, routingCookie: newCookie}, // Error with new routing cookie
		dummyTypedAction("row-05", "v5", "token-r5", row1),
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("routing-cookie-multi-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row5}, res.Rows)

	// First attempt has no cookie yet.
	<-mdRecords
	// Second attempt carries the cookie from the first error.
	md1 := <-mdRecords
	val1 := md1["x-goog-cbt-cookie-test"]
	assert.NotEmpty(t, val1)
	if len(val1) == 0 {
		return
	}
	assert.Equal(t, cookie, val1[0])
	// Third attempt: the preceding error had no cookie, so the previous one must be retained.
	md2 := <-mdRecords
	val2 := md2["x-goog-cbt-cookie-test"]
	assert.NotEmpty(t, val2)
	if len(val2) == 0 {
		return
	}
	assert.Equal(t, cookie, val2[0])
	// Fourth attempt picks up the replacement cookie.
	md3 := <-mdRecords
	val3 := md3["x-goog-cbt-cookie-test"]
	assert.NotEmpty(t, val3)
	if len(val3) == 0 {
		return
	}
	assert.Equal(t, newCookie, val3[0])

	// Every retry must resume from the only token committed so far.
	firstReq := <-recorder
	assert.Empty(t, firstReq.req.GetResumeToken())
	for i := 0; i < 3; i++ {
		retryReq := <-recorder
		assert.Equal(t, []byte("token-r1"), retryReq.req.GetResumeToken(), "retry %d", i+1)
	}
}

// TestTypedReadRows_Retry_WithRetryInfo_MultipleErrorResponses verifies that a server-provided
// RetryInfo delay governs the next attempt, and that the client falls back to its default backoff
// once the server stops supplying one.
func TestTypedReadRows_Retry_WithRetryInfo_MultipleErrorResponses(t *testing.T) {
	// 0. Common variables
	row1 := dummyTypedRow("row-01", "v1")
	row5 := dummyTypedRow("row-05", "v5")

	recorder := make(chan *typedReadRowsReqRecord, 3)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("row-01", "v1", "token-r1"),
		&typedReadRowsAction{rpcError: codes.Unavailable, retryInfo: "2s"}, // Error with retry info
		&typedReadRowsAction{rpcError: codes.Unavailable},                  // Second error without retry info
		dummyTypedAction("row-05", "v5", "token-r5", row1),
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("retry-info-multi-table")),
	}

	// 3. Perform the operation via test proxy
	res := doTypedReadRowsOp(t, server, req, nil)

	// 4. Check the response
	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row5}, res.Rows)

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
	// 0. Common variables
	row1 := dummyTypedRow("row-01", "v1")

	// There should only be 2 attempts due to the effect of client side timeout.
	recorder := make(chan *typedReadRowsReqRecord, 2)

	// 1. Instantiate the mock server
	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(recorder,
		dummyTypedAction("row-01", "v1", "token-r1"),
		&typedReadRowsAction{rpcError: codes.Unavailable, retryInfo: "2s"},
		&typedReadRowsAction{rpcError: codes.Unavailable, retryInfo: "6s"},
		dummyTypedAction("row-05", "v5", "token-r5", row1),
	)

	// 2. Build the request to test proxy
	req := &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName("retry-info-deadline-table")),
	}

	opts := clientOpts{
		timeout: &durationpb.Duration{Seconds: 3},
	}

	// 3. Perform the operation via test proxy
	doTypedReadRowsOp(t, server, req, &opts)

	// 4. Check the response
	// 3s deadline is far below the combined 2s + 6s of server-requested delay, so the operation
	// must give up rather than sleeping through it.
	curTs := time.Now()
	loggedReq := <-recorder
	runTimeSecs := int(curTs.Unix() - loggedReq.ts.Unix())
	assert.Less(t, runTimeSecs, 4)
}
