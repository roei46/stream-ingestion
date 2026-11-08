# Stream Ingestion Engine — Python study project

## 1. The assignment (summary)

Home assignment "Advanced Python — High-Throughput Stream Ingestion Engine" (Eyal Segal).
Original PDF: `/Users/roeibaruch/Downloads/eyal_advanced_python_stream_ingestion.pdf` (7 pages,
outside the repo). Read it with `pdftotext -layout` and check each section word for word before
building it; this file is only a summary.

**The mission:** read a JSON Lines file of shipment events one line at a time, validate each line,
turn good lines into small frozen `Event` objects, group them into batches, and save each batch to
a **fake** transactional database (commit / rollback). Bad lines are rejected and processing
continues. No web server, no real database, no iOS: a Python package + tests.

**Scope (decided with my mentor): sections 1–4 only.**

| # | Topic | What to build |
|---|---|---|
| 1 | Dataclasses | `Event` (`frozen=True, slots=True, kw_only=True`): event_id, shipment_id, occurred_at (UTC), status, tags (tuple). `fingerprint` (SHA-256, computed in `__post_init__`). Equality/hash contract. Mutable `BatchStats` with `default_factory=list`. Ordered `EventCursor` (`order=True`). Short `__repr__`. Small language experiments on `__hash__` / `eq` / `unsafe_hash`. |
| 2 | Pydantic V2 | `EventPayload` at the input boundary: reject unknown fields, size limits, timezone required → UTC, status in created / in_transit / delivered, schema_version must be the integer 1 (reject `"1"` and `true`), "delivered needs ≥ 1 tag". `to_domain(payload) -> Event`. Typed rejection record (line number, category, short message), streamed out, never an unbounded list. |
| 3 | Context managers | `ManagedBatchSink`: outer context = connection, per-batch context = begin / commit / rollback. Recoverable error → rollback, suppress only that, continue. Fatal → rollback, propagate. Cleanup failures must not hide the original error. `timed_lock(lock, *, clock)` with `@contextmanager`. |
| 4 | Generators | Lazy pipeline: line reader (max line size) → Events → batches → sink. Batch generator that accepts `send(k)` to change the next batch size, `throw()` for cancellation, `close()` for cleanup. Prove laziness with a counting source. |

**Skipped (mentor's decision):** section 5 (descriptors) and section 6 (benchmark).

**Acceptance scenarios (from the PDF):**
- Baseline: 12 records, 2 invalid, B=4 → batches [4, 4, 2]; read=12, accepted=10, rejected=2, committed=10.
- Recoverable: 2nd batch fails before commit → committed=6, failed_records=4, failed_batches=1.
- Fatal: 2nd batch fails fatally → first 4 stay committed, no later batch, resources released, error propagates.
- Early stop: cancel/close after the first batch → no more input read, everything closed.
- Boundaries: empty input, all invalid, final partial batch, overlong line, invalid control size, unknown fields, cleanup errors.

Package layout from the PDF: `models.py` (Pydantic contracts, Event, EventCursor, BatchStats),
`engine.py` (lazy stages, batching, orchestration), extra modules if clearer, `tests/`, `README.md`.

## 2. Stack
- Python 3.12, Poetry (`package-mode = false`, `.venv` inside the project).
- Pydantic V2, pytest, mypy `--strict`, ruff. Pin exact versions (the PDF asks for it).
- Commands: `poetry run pytest`, `poetry run mypy --strict .`, `poetry run ruff check .`

## 3. How to work with me (this is a STUDY project)

I'm an experienced iOS developer; Python is new to me. The goal is that **I understand** every
line, not just that it works.

- **Order:** propose the next small step in 2–3 lines → wait for my OK → write a **small** piece
  (a few lines, never a whole file) → **show the code** → explain it line by line → run the checks.
  Never explain code I haven't seen yet.
- **Compare to Swift** for every new Python thing (`with` ≈ `defer` / `withLock`, generator ≈
  `Sequence`/`IteratorProtocol` with `yield`, `@dataclass` ≈ struct with memberwise init, etc.).
  When there's no Swift equivalent, say so plainly.
- **Explain why, not just what:** what problem does this feature solve, and what breaks without it.
- **Short answers to simple questions.** A few lines. No extra sections, no unrequested offers.
- **Precise words.** A type is not a function; a closure type is not a closure. Don't simplify
  into something wrong.
- **Check yourself before showing me code:** best practice for Python 3.12 / Pydantic V2, the
  PDF wording, and mypy / ruff / pytest all passing. Say briefly what was checked.
- **Keep it minimal.** Only what the assignment asks for. If something extra seems needed, ask first.
- **Simple must never mean unsafe** (e.g. don't drop a lock or cleanup to save lines).
- Sometimes I'll write the code myself: then review it, run it, and tell me what you found.
- When I ask "what now", give the single next step.
- **When I ask a question: answer it directly, first, and do nothing else** (no edits, no commands).
- **Always show the exact code from the file.** Never shorten or "simplify" it in an explanation.
- **Explain short and plain, but cover every line** — no skipped lines, names or annotations.

## 4. Secrets
None expected. If any appear: only in `.env` (git-ignored), never in code or chat.
