---
name: cancel-safe-async
description: Use when writing, reviewing, or designing async Rust code that involves futures, tokio::select!, timeouts, try_join, task aborts, Tokio mutexes, MPSC channels, or any operation where a future could be dropped mid-execution. Captures the practical guidance from Oxide RFD 400 on cancel safety and cancel correctness.
---

# RFD 400: Cancel safety in async Rust

## Core mental model

- A future is **cancelled** when it is dropped. Cancellation propagates from parent to child futures. Any owned values are dropped (their `Drop` impls run).
- Cancellation can **only happen at await points** (cooperative multitasking). Code between awaits runs to completion.
- Cancellation **can only call synchronous code** — there is no `AsyncDrop` (as of Rust 1.89). Anything that needs to await during cleanup cannot run during cancellation.
- **Cancel safety is a local property** of one future: "can it be dropped before completion without implications for the rest of the system?"
- **Cancel correctness is a global property**: violated only if (1) a cancel-unsafe future exists in the system AND (2) it actually gets cancelled AND (3) cancelling it violates some system property.
- Think in terms of a future's **"cancellation blast radius"** — what other parts of the system are affected when it's dropped. Self-contained futures (e.g. `tokio::time::sleep`) have radius zero; futures touching shared mutable state have the largest radius.

## What "violating a system property" means

- **Data loss**: in-flight data on a stream gets dropped.
- **Broken invariants**: shared state left in an invalid mid-mutation state. Key rule: **in cancel-safe futures, shared state must not be invalid across await points** (the async analogue of "invariants restored before releasing `&mut`").
- **Missed cleanup**: cleanup futures get cancelled before they run.
- **Lost fairness**: dropping makes you lose your place in line (e.g. `Mutex::lock`). Futures that are cancel-safe except for fairness are "**mostly cancel-safe**."
- Cancellations that affect state **outside the process** (zones, files, network peers) are the most fraught — see sled-agent zone deletion case study.

## Sources of cancellation (where the bugs live)

- **`tokio::select!`** — polls futures concurrently; whichever doesn't complete first gets dropped. The single biggest source of cancel-safety bugs.
- **`tokio::time::timeout`** — equivalent to `select!` against a sleep. Same hazards.
- **`try_join!` / `try_join_all` / `TryStreamExt::try_*`** — all perform **early cancellation**: as soon as one future errors, the rest are cancelled. Wrong for operations with side effects (e.g. concurrent flushes, concurrent zone deletions).
- **Task aborts** (`JoinHandle::abort()`) — cancels the task at **any await point**. Arbitrary async code is rarely robust to this. **Treat task aborts like panics: not part of normal control flow.**
- **Runtime shutdown** — cancels all tasks. Panics can trigger shutdown unexpectedly (see Dropshot #709 for nondeterministic-shutdown-order bugs). Don't assume a spawned task whose `JoinHandle` you dropped will run forever — a panic-induced shutdown will kill it.

## Anti-patterns / things that are NOT cancel-safe

- **`tokio::sync::mpsc::Sender::send(value)`** — owns `value`; if dropped mid-send, the value is dropped and lost. Use `sender.reserve().await` to get a `Permit`, then `permit.send(value)` (synchronous).
- **`futures::SinkExt::send`** — same hazard as `mpsc::Sender::send`, inherent to the operation regardless of the underlying `Sink`. Use the `reserve` API from the `cancel-safe-futures` crate.
- **`tokio::sync::Mutex`** — `lock().await` is fairness-cancel-unsafe, AND there is no poisoning, so a cancellation mid-critical-section silently leaves shared state invalid. **Recommendation: avoid `tokio::sync::Mutex` entirely.**
- **Methods named `next()` / `recv()` that do more than read** — e.g. read a message then `.await` on processing it. The read can succeed but the processed result gets dropped.
- **Convenience APIs that combine a cancel-safe step with a cancel-unsafe step** (like `Sender::send` combining `reserve` + actual send). Easy to misuse in `select!`.

## How to make code cancel-safe (patterns ranked by usefulness)

- **Split complex operations into reserve + commit**. The reserve step is cancel-safe (or mostly so); the commit step is synchronous or guarded by a permit holding a `&mut` to the parent. This is the canonical fix. Examples: `mpsc::Sender::reserve` → `Permit::send`; `cancel-safe-futures` `SinkExt::reserve` → `Permit::send`.
- **Resume from partial progress**. Track progress either internally (a field on `self`, e.g. `self.migration_message: Option<T>`) or externally via a `&mut B where B: bytes::Buf` parameter (see `AsyncWriteExt::write_all_buf`). On re-entry, pick up where you left off.
- **Spawn a background task** to do the cancel-unsafe work. Store its `JoinHandle` on `self` and await it; if your future is cancelled, the task keeps running and the next call picks it up. Caveats: `tokio::task::spawn` requires `'static`, and the task itself becomes uncancellable unless you wire up a cancel channel.
- **Use cooperative (explicit) cancellation channels** instead of task aborts. Use the `coop_cancel` module from the [`cancel-safe-futures` crate](https://github.com/oxidecomputer/cancel-safe-futures) (fan-in: many cancelers, one receiver) when you need to cancel long-running tasks safely.
- **Perform cleanup separately** rather than relying on the operation's success path. E.g. a `ConnectionPool` must clean up connections **when they're returned to the pool OR before the next checkout**, not only at the end of `execute_transaction`, because the latter can be cancelled.
- **Use alternative channel modes** to remove async-ness entirely:
  - `try_send` on bounded channels (synchronous, errors on full — externalize backpressure as HTTP 429, etc.).
  - **`tokio::sync::watch`** channels when only the latest value matters (the wicketd fix).
  - Unbounded channels — only as a last resort; you lose backpressure.

## How to consume cancel-unsafe code safely

- **Read names, signatures, and docs.** Methods called `execute()`, `http_post_data()`, methods that take `self` by value, etc. are signaling "don't use me in a `select!` loop." Each method should ideally have a "Cancel safety" doc section.
- **Resume, don't recreate, futures inside `select!` loops.** Create the future **once outside** the loop, then `select!` on `&mut fut`. Pin it with `std::pin::pin!(...)` (not the older `tokio::pin!`). If you need to replace the future inside the loop, use `Box::pin(...)`.
- **Use `then_try` adapters instead of `try_` adapters** for operations with side effects. From `cancel-safe-futures`: `join_then_try!`, `for_each_concurrent_then_try`, etc. — they run everything to completion, then surface the first error.
- **Avoid task aborts.** Use a cooperative cancel channel instead.

## Naming guidelines for cancel-unsafe APIs you write

- **Don't** name cancel-unsafe methods `next()`, `recv()`, `reserve()`, or `acquire()` — these read as "safe to drop in a `select!`."
- **Do** use names that suggest an irrevocable action: `http_post_data()`, `execute()`, `commit()`.
- **Do** use the type system: `fn execute(self)` (consuming, non-`Clone`) physically prevents `select!`-loop misuse.
- **Do** write a "Cancel safety" doc section, at least for commonly used methods.

## Case studies — canonical patterns to follow

- **Oxide serial console (`proxy_instance_serial_ws`, propolis#435, #438)** — canonical example of the **split-into-two-futures** pattern. `InstanceSerialConsoleHelper::recv` returns a future that, when awaited, returns *another* future that performs the cancel-unsafe migration work. The outer future holds `&mut self`, so the type system prevents misuse. **Follow this pattern when you have "read message, then process it with more awaits."**
- **wicketd installinator progress (omicron#3950)** — canonical example of **eliminating async-ness by switching channel type**. Replaced a bounded MPSC channel + Tokio mutex with a **watch channel** + `std::sync::Mutex`. Cascading wins: report method became sync, mutex became sync, downstream code became sync. **When only the latest value matters, reach for `tokio::sync::watch`.**
- **sled-agent zone deletion (omicron#3758)** — canonical example of **`try_*` adapter misuse on side-effectful operations**. Switched `try_for_each_concurrent` → `for_each_concurrent_then_try` so all zones finish their deletion before errors are surfaced.
- **Dropshot client disconnects (dropshot#701, #702)** — canonical example of **using background tasks to neutralize an entire class of cancellations**. Each HTTP request now runs on its own task; client disconnects no longer abort the task. Default behavior. This eliminated client-triggered cancel-safety bugs across all of Omicron in one stroke.
- **`AsyncWriteExt::write_all_buf` + `bytes::Buf`** — canonical example of the **external partial-progress** pattern. The `Buf` trait (`remaining`, `chunk`, `advance`) lets the writer report progress to the caller's buffer; safe to call inside `select!` loops.

## Project-specific resources

- **`cancel-safe-futures` crate** (`github.com/oxidecomputer/cancel-safe-futures`) — Oxide-maintained. Provides:
  - `SinkExt::reserve` → `Permit` (the `Sink` analogue of `mpsc::Sender::reserve`).
  - `then_try` family: `join_then_try!`, `for_each_concurrent_then_try`, etc.
  - `coop_cancel` module: cooperative cancellation channels.
  - **The crate is incomplete by design** — adapters are added as needed. If you need one that's missing, add it and send a PR.

## Glossary

- **Cancel-safe**: a future that can be dropped without implications for the rest of the system. Local property.
- **Mostly cancel-safe**: cancel-safe except for fairness (e.g. `Sender::reserve` — drop loses your place in line, but nothing else breaks).
- **Cancel-unsafe**: dropping the future can violate some system property (data loss, broken invariant, missed cleanup).
- **Cancel correctness**: the global property of a system continuing to behave correctly under cancellation. Distinct from cancel safety.
- **Cancellation blast radius**: the set of other components affected when a given future is cancelled.
- **Cooperative cancellation**: cancellation that flows through an explicit channel that the future checks at known points (contrast with future-drop, which can happen at any await).
- **Reserve pattern**: split an operation into a cancel-safe "reserve a slot" async step followed by a synchronous (or cancel-unsafe but guarded) commit step.
- **Resume vs. recreate**: in a `select!` loop, awaiting `&mut fut` to continue an existing future vs. constructing a new future each iteration. Resume preserves progress; recreate loses it.

## Quick decision rules

- Reaching for `tokio::sync::Mutex`? → Don't. Use message passing (actor model with mpsc + oneshot reply), or `std::sync::Mutex` not held across awaits, or a watch channel.
- Reaching for `tokio::select!` with a sender/sink? → Use `reserve` + `Permit`, not `send`.
- Reaching for `try_join!` / `try_for_each_concurrent`? → If the futures have side effects, use the `then_try` equivalent from `cancel-safe-futures`.
- Reaching for `JoinHandle::abort()`? → Use a `coop_cancel` channel instead.
- Reaching for an unbounded MPSC just to make `send` synchronous? → First consider `watch` or `try_send`.
- Writing a `next()` or `recv()` that does post-read processing? → Split into two futures, return the inner future from the outer.
- Inside a `select!` loop with a cancel-unsafe future? → Create the future **once** outside the loop and `select!` on `&mut pin!(fut)`. Never recreate per iteration.
