# Stage 2 QueryNode Failure Qualification Report

## Run

- Evidence: `runs/20261008T043348Z-89735`
- Milvus commit: `75e7392ebe2c61f6d3043431984a44582078a161`
- Test repository commit used by the run: `216d2f6815c2d53b2d74cee2b2c28aff2b6cf4c4`
- Topology: one QueryNode, 63 sealed segments, 1,000,000 rows, no selected StreamingNode
- Workload: Search iterator and Plain Query, each requesting 8,192 results

The run executed every planned Search and Query case and both final recovery requests. It exited during final validation because the validator required one accepted request for `Search/EOF-AFTER`. Commit `527c5d71d70ac44b7851011933797bb581084c0b` corrected that classification, and the preserved run then passed validation.

## Results

| Failure point | Search | Query |
| --- | --- | --- |
| Baseline | 8,192 results | 8,192 results |
| Error before first Chunk | Retried the same request ID and returned the baseline result | Retried the same request ID and returned the baseline result |
| Error after first Chunk | Returned an error; no partial success | Returned an error; no partial success |
| Clean EOF after first Chunk | Returned the baseline result after the iterator issued a second page request with a different request ID | Returned a successful partial result containing 6 rows |
| Deadline before or after first Chunk | Returned `DEADLINE_EXCEEDED`; no partial success | Returned `DEADLINE_EXCEEDED`; no partial success |
| `SIGKILL` or `SIGTERM` before or after first Chunk | Returned an error; no partial success | Returned an error; no partial success |
| TCP reset before first Chunk | Retried the same request ID and returned the baseline result | Retried the same request ID and returned the baseline result |
| TCP reset after first Chunk | Returned an error; no partial success | Returned an error; no partial success |
| Final recovery | Result count and hash matched the baseline | Result count and hash matched the baseline |

## Conclusions

1. Failures before the first final Chunk are retryable when a usable QueryNode remains reachable. The explicit error and TCP reset cases retried with the same request ID.
2. Failures after the first final Chunk are not retried and do not return partial SDK success.
3. A clean EOF is not a transport failure. Search iterator pagination recovered by issuing another Search request, but Plain Query accepted the truncated six-row result as successful. The Query behavior is a confirmed correctness gap.
4. Killing the only QueryNode before the first Chunk cannot complete through retry because the test intentionally provides no replacement node; those requests reached their deadline.
5. A fresh fault-free QueryNode served both recovery requests successfully after the complete failure matrix.

StreamingNode failure and multi-child pending-`Recv()` cleanup were outside this N1 sealed-only experiment.
