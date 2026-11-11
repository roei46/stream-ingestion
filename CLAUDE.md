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

**Scope: all 6 sections** (mentor updated this; 5 and 6 were skipped before).

| # | Topic | What to build |
|---|---|---|
| 1 | Dataclasses | `Event` (`frozen=True, slots=True, kw_only=True`): event_id, shipment_id, occurred_at (UTC), status, tags (tuple). `fingerprint` (SHA-256, computed in `__post_init__`). Equality/hash contract. Mutable `BatchStats` with `default_factory=list`. Ordered `EventCursor` (`order=True`). Short `__repr__`. Small language experiments on `__hash__` / `eq` / `unsafe_hash`. |
| 2 | Pydantic V2 | `EventPayload` at the input boundary: reject unknown fields, size limits, timezone required → UTC, status in created / in_transit / delivered, schema_version must be the integer 1 (reject `"1"` and `true`), "delivered needs ≥ 1 tag". `to_domain(payload) -> Event`. Typed rejection record (line number, category, short message), streamed out, never an unbounded list. |
| 3 | Context managers | `ManagedBatchSink`: outer context = connection, per-batch context = begin / commit / rollback. Recoverable error → rollback, suppress only that, continue. Fatal → rollback, propagate. Cleanup failures must not hide the original error. `timed_lock(lock, *, clock)` with `@contextmanager`. |
| 4 | Generators | Lazy pipeline: line reader (max line size) → Events → batches → sink. Batch generator that accepts `send(k)` to change the next batch size, `throw()` for cancellation, `close()` for cleanup. Prove laziness with a counting source. |
| 5 | Descriptors | `AwareTimestamp` data descriptor on a mutable `RunAudit` (separate from `Event`). `__set_name__` picks the backing attribute; `__set__` accepts only timezone-aware datetimes and stores UTC on the instance; `__get__` returns the descriptor for class access and raises `AttributeError` if unset. No per-instance dict inside the descriptor; two `RunAudit`s must not overwrite each other. Data-descriptor precedence over the instance `__dict__`. |
| 6 | Benchmark | `benchmark.py`: Pydantic vs dataclass vs slotted dataclass, 100,000 inputs. End-to-end (parse + validate + convert) measured separately from construction. Memory with `tracemalloc`, a fresh process per variant. Timing with `timeit`/`perf_counter`: warm-up, median + spread. Streaming peak memory for 10k / 100k / 1M records at fixed B and R. Results table. |

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

### The 6 rules that matter most
1. **Only what I asked.** No commands, edits, explanations or tasks I didn't ask for in this
   message. Need information (e.g. an error text)? Ask me to paste it. "check" = run mypy / ruff
   (and the code) on what I wrote.
2. **Answer my question first, directly.** Yes/no question → one word.
3. **My words unclear → ask what I meant.** Never say "not quite" before asking.
4. **Before a task: what goes in, what comes out, and what it creates**, with real data, in 2–3
   lines (e.g. "`make_lines(3)` gets only a number and creates 3 JSON lines: e0, e1, e2").
5. **At most 1–2 new Python things per step**, each explained before I need it: one line, with
   types (the example must pass `mypy --strict`) and a Swift comparison.
6. **Short.** If it doesn't fit in a few lines, the step is too big: split it.

### Coach mode (default): I write the code, you coach
1. **Task in words:** file → class → method, what goes in / out (rule 4). No skeleton up front.
2. **New things:** max 1–2 (rule 5). A tiny example only if the idea is new.
3. **I write it.** Stuck → hints in levels: a hint in words → the skeleton (signature +
   `# TODO`) → the function/syntax name → one line → full code only if I ask.
4. **"check"** → run mypy / ruff (and the code), show the real output, point to the exact line.
   I fix it myself.
5. **Tests at the end** of a section, not first.
6. Write code yourself only when I say "you write it": show it first, explain every line, wait
   for my OK.

### Other rules
- **Touch only what we're working on.** For every limit, value or name: from the PDF or your choice?
- **Every example follows the project rules** (types, `snake_case`, passes `mypy --strict`).
- **If you give me a choice, don't decide it for me afterwards.**
- **Don't start the next section until I say the current one is finished.**
- **Signature vs call:** say which one you're showing.
- **Always show the exact code from the file**, and cover every line, name and annotation.
- **Every explanation has a real example** with real data, stating every value it depends on.
- **Precise words.** A type is not a function; a function type is not a function.
- **"What now" → the single next step.** "Step by step" → one step per message, wait for "next".
- **Discussion questions:** I answer first, then you correct or add.
- **Diagrams start from `run` and show who calls whom,** grouped by file and class.
- **Keep it minimal and safe:** only what the assignment asks; never drop a lock or cleanup.
- **Long session → suggest a fresh one** before quality drops.
- **Project map:** https://claude.ai/artifact/KU9GD6g7VR1gqqCur3Dyr6. Update it when a piece is
  added or changes.

## 4. Secrets
None expected. If any appear: only in `.env` (git-ignored), never in code or chat.
