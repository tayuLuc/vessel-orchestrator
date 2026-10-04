// The durable state moved from the session log to a plugin-owned store in 0.3.0, so the store is now
// the one thing here that can lose work silently: a wrong shape means vessel_collect reports "no forks"
// or the outbox audit comes back empty, with nothing anywhere to complain. These four tests cover the
// paths that actually lose data — the migration off a pre-0.3.0 log, the outbox trim, and the
// store-wins-over-log precedence.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const sandbox = mkdtempSync(join(tmpdir(), "vessel-store-test-"));
process.env.VESSEL_ORCHESTRATOR_STORE_FILE = join(sandbox, "store.json");
const { readStore, recordFork, recordOutbox, loadForks, loadOutbox } = await import("../index.js");

const SID = "session-x";
const legacy = (data) => ({ type: "vessel/state", data });

test("a fork recorded in the store survives a fresh read", () => {
  recordFork(SID, { forkId: "f1", question: "q", branches: [{ childId: "c1", title: "a" }] });
  const forks = loadForks(SID, []);
  assert.equal(forks.size, 1);
  assert.equal(forks.get("f1").branches[0].childId, "c1");
});

test("a pre-0.3.0 log row is read once and migrated into the store", () => {
  const rows = [legacy({ fork: { forkId: "legacy-1", question: "q", branches: [] } })];
  const forks = loadForks("session-legacy", rows);
  assert.equal(forks.get("legacy-1").forkId, "legacy-1", "the log still answers the first time");
  const migrated = loadForks("session-legacy", []);            // second call with no log at all
  assert.equal(migrated.get("legacy-1").forkId, "legacy-1", "and the store answers afterwards");
  assert.ok(readStore()["session-legacy"].forks["legacy-1"], "the migration is written, not just remembered");
});

test("the store wins over the log when both know a fork", () => {
  recordFork(SID, { forkId: "store-1", question: "q", branches: [] });
  const forks = loadForks(SID, [legacy({ fork: { forkId: "log-1", question: "q", branches: [] } })]);
  assert.ok(forks.has("store-1"), "the store is the current truth");
  assert.equal(forks.has("log-1"), false, "a stale log row cannot resurrect an old fork");
});

test("the outbox is trimmed and the audit sees the newest 10", () => {
  for (let i = 0; i < 60; i++) recordOutbox("session-out", { direction: "out", to: "t", dedupId: "d" + i, at: i });
  const outbox = loadOutbox("session-out", []);
  assert.equal(outbox.length, 10, "the audit slice is bounded");
  assert.equal(outbox[9].dedupId, "d59", "and it is the newest end of the list");
  const stored = JSON.parse(readFileSync(process.env.VESSEL_ORCHESTRATOR_STORE_FILE, "utf8"));
  assert.equal(stored["session-out"].outbox.length, 50, "the store itself is bounded, not just the view");
});

test("an empty store and an empty log answer empty, not throw", () => {
  assert.equal(loadForks("session-none", []).size, 0);
  assert.deepEqual(loadOutbox("session-none", []), []);
});
