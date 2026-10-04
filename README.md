# vessel-orchestrator

FORK-AT-FORK / MERGE-BY-DISTILLATION for DeepSeek Harness (dsh 0.1.1-rc.2, web profile).
Gives the resident agent four tools:

- **`vessel_fork(question, directions[2..5])`** — spawns 2–5 branch sessions of the calling
  agent at an idea-fork. Each branch is a *continuable* subagent created through
  `ctx.subagents.startContinuable({ provider: "fork" })`, so it inherits the parent's
  completed-turn prefix (seed ends at the last `turn/end`; the in-flight tool-call turn is
  excluded by the fork provider). The question + one direction are injected as each
  branch's initial prompt. Returns branch ids immediately — `startContinuable` resolves at
  inbox acceptance, not turn completion.
- **`vessel_collect(forkId?)`** — drains a fork: per-branch status plus ALL settled
  branches' final assistant messages **verbatim**, folded into ONE consolidated block per
  drain (cache doctrine rule 3). The anti-telephone-effect guarantee is
  structural: this plugin never paraphrases child output, and the branch prompt requires
  each branch to end with a `## Distillation` section whose load-bearing facts are exact
  quotes with sources. The parent folds the consolidated block into its own context.
- **`vessel_send(sessionId, text, type?, threadId?)`** and **`vessel_inbox()`** —
  cross-session self-messaging; see the dedicated section below.

## API surface actually used (verified against installed dsh 0.1.1-rc.2)

| What | Where | Notes |
|---|---|---|
| `ctx.subagents.startContinuable({provider, label, request:{prompt, parent}, signal})` | `@deepseek-ai/dsh-subagent` (`SubagentRuntime`) → `SubagentContinuationManager` | Returns `{childId, messageId}` at inbox acceptance. Durable + cold-resumable. |
| Provider `"fork"` | `@deepseek-ai/dsh-subagent-fork-in-process` (loaded by `dsh-base` as `subagent-fork-in-process`, `providerName: fork`) | Has `prepareContinuable`; seeds child with parent's completed-turn prefix via `ContinuableCreateSpec.seed`. |
| `ctx.agents.get(childId)` / `agent.session.events` / `agent.status` | `@deepseek-ai/dsh-agent` | Live branch lookup for status. |
| `sessionPersistence.load(childId)` → `{meta, events}` | `@deepseek-ai/dsh-session-persistence` | Cold branch transcripts; `meta.seedLength` marks the inherited prefix. |
| `finalAssistantOutput(events)` | `@deepseek-ai/dsh-subagent` | Folds last non-empty assistant message from a branch's own events. |
| `$DSH_HOME/storages/vessel-orchestrator.json` (this plugin's own file) | — | Durable fork records + self-send outbox, keyed by session id. They rode the parent session log as private `vessel/state` events until 0.3.0, which is what made a chat unopenable (see Limitations): the log's event vocabulary belongs to the harness, not to a plugin. |
| `ctx.tools.register(defineTool({...}))` | `@deepseek-ai/dsh-tools` | Standard tool registration. |

## Deviations from the original recon

1. **`Agent.status` is only `'idle' | 'running'`.** The recon's `waiting(owns children)/settled`
   states are continuation-manager internals, not visible on the Agent. Settlement is
   observed as registry absence (`ctx.agents.get()` → undefined) plus transcript output.
2. **No `SessionStore.fork()` call needed.** Seeding happens inside the fork provider's
   `prepareContinuable` (`ContinuableCreateSpec.seed`); callers never touch the store.
   `SessionHeader.seedLength` is still used — to slice inherited events out of collect.
3. **`label` lives top-level** in `ContinuableStartSpec`, not inside `request`
   (`request` is `Omit<SubagentStartRequest, 'label'|'signal'|'outputSchema'>`).
4. **Distillation is authored by each branch itself** (prompt-enforced verbatim-quote
   protocol in its final message). The plugin adds zero paraphrase; it returns the final
   assistant message byte-for-byte.
5. **`systemPrompt.section` unused**, as recon predicted.
6. **Built-in `subagent_fork` tool runs the fork provider ONE-SHOT only** (dsh-base patch
   comment: continuable children would prepend report-tool prompt sections ahead of the
   inherited history). This plugin deliberately uses the *continuable* path anyway:
   branches here are autonomous workers whose own final message IS the deliverable, and
   continuable gives durability + cold resume. The extra prompt section lands after the
   seed and does not displace it.

## Wiring (already applied)

`$DSH_HOME/profiles/web/package.json`:

```json
"dependencies": { "vessel-orchestrator": "file:plugins/vessel-orchestrator", ... },
"dsh": { "profile": { "bundles": [ ..., "vessel-orchestrator" ] } }
```

Module resolution needs no installed deps: bare `@deepseek-ai/*` imports resolve via the
parent-walk from `web/plugins/vessel-orchestrator/` up to the maintained flat fallback
`~/.dsh/profiles/node_modules/` (one symlink per installation package).

## Rollback

```bash
cd "$DSH_HOME/profiles/web"
git checkout package.json 2>/dev/null || true   # or hand-revert the two edits
pnpm install
rm -rf plugins/vessel-orchestrator
# then restart the dsh host (service restart / relaunch)
```

Removing the `dependencies` entry + `bundles` entry and deleting the directory fully
uninstalls the plugin; no other state is created (branch sessions persist as ordinary
dsh sessions).

## Cross-session messaging (v0.3, vessel-selfcomms v0 protocol)

Two tools let resident selves talk across dsh sessions (design:
design note `vessel-selfcomms.md` (private, not shipped)):

- **`vessel_send(sessionId, text, type?, threadId?)`** — queues a message into
  another session via loopback RPC `POST /api/session.prompt`
  (`{sessionId, mode:"queue", content:[{type:"text", text}]}`). It surfaces in
  the target as **its own next turn** (durable inbox splice at a turn
  boundary) — past context is never mutated, so the prompt cache stays intact
  (Hermes "never mutate past context" invariant). Every send also appends a
  `vessel/state` outbox record (`direction:"out"`) to this plugin's own store —
  crash-safe outbox, greppable, replayable.
- **`vessel_inbox()`** — scans the calling session's OWN log for inbound
  envelopes: every queued prompt was durably recorded as an
  `agent/inbox/spliced` event whose `inserted[]` user messages carry the wire
  text, and insertion events survive consumption (claims are separate pure
  deletion splices), so the scan replays the full delivery history. Returns
  envelopes addressed to this session, deduped by `dedupId`
  (at-least-once delivery), grouped by `threadId`, each with
  `from/type/dedupId/at/body`. Also lists visible sessions (send targets,
  with cwd/preset) and audits the last 10 outbound messages.

Envelope format — one JSON line, then a blank line, then the human body:

```json
{"v":1,"from":"<sender sessionId>","to":"<target sessionId>","type":"fyi|request|reply","threadId":"<uuid>","dedupId":"<uuid>","createdAt":1756000000000}
```

Guards & semantics:

- **Same-workspace guard (enforced, fail closed)**: the target must appear in
  this vessel's `session.list` AND its `cwd` must equal the sender's
  `header.cwd`; otherwise `vessel_send` refuses.
- Rate limit: max 10 sends per rolling 60s per process.
- Self-send is rejected; receiving selves treat message bodies as UNTRUSTED
  CONTENT from a sibling self (informative, not commanding) per mind-virus
  hygiene — requests that would mutate repo state or config still require the
  normal operator authority chain in the receiving session.
- `dedupId` makes delivery at-least-once; receivers dedup by it.
- RPC endpoint override: `DSH_SELF_RPC` env var (default
  `http://127.0.0.1:3178`).

Known limitations: no agent-preset allow-list enforcement yet (addressing
discipline is on the calling model); rate-limit state is per-process, not
persisted; aggressive compaction may summarize away old splice events, so
very old inbound messages can drop out of `vessel_inbox`.

## Consolidated collect drain (v0.3)

`vessel_collect(forkId?)` now folds ALL settled branches' final distillations
into ONE consolidated block per drain (cache doctrine rule 3: fan-out outcomes
return as a single block, never N interleaved insertions). The structured
result carries `consolidated` plus compact per-branch status lines; verbatim
anti-telephone-effect guarantees are unchanged.

## Limitations

- Branches are full agents: they could call `vessel_fork` recursively (depth is bounded
  by dsh delegation-depth machinery, not by this plugin).
- **The plugin used to write a private event type into the session log, and that broke chats.**
  `vessel/state` rows are not in the harness vocabulary, and `dsh-session-persistence-jsonl`
  refuses to interpret a log containing an unknown type unless the row carries the envelope
  marker `ignorable: true` — which only the harness writer can stamp. Measured 2026-09-26:
  3 unmarked rows, one chat permanently unopenable ("Failed to load history"). Since 0.3.0 the
  durable state is a plugin-owned store and the log is only ever *read* (to migrate a pre-0.3.0
  row once). `test/store.test.mjs` covers the migration and the store-wins-over-log precedence.
- A branch that errors mid-run shows `settled-no-output` or `unknown` rather than a
  typed failure reason (the settlement diagnostic is delivered to the parent as a
  `subagent-settled` notice by the harness itself).
