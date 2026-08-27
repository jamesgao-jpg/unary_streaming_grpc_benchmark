# Benchmark Test Artifacts

- For every proposed benchmark test, create a dedicated directory under
  `result/` named `test_<MMDD>_<NN>` using the local date and a zero-padded
  sequence number for that date. For example: `result/test_0820_01/`.
- Do not reuse a directory for a different test proposal. Increment `<NN>` when
  another test is proposed on the same date.
- Keep the complete test definition and evidence together:

  ```text
  result/test_<MMDD>_<NN>/
    TEST_PLAN.md
    run.sh
    logs/
  ```

- `TEST_PLAN.md` must record the objective, topology, parameter matrix,
  execution procedure, metrics, and acceptance criteria before execution.
- `run.sh` must implement the recorded plan and write all benchmark output to
  the test directory's `logs/` directory.
- Preserve generated configurations and raw logs needed to reproduce or audit
  the result. Do not overwrite prior logs; use distinct timestamped filenames
  for repeated runs.
- Clearly mark a test as proposed, running, completed, or failed in
  `TEST_PLAN.md`. Do not report proposed or partial results as completed.
