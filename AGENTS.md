<!-- webtyp:base — copied from devskills skill agents-md; do not edit here -->
## Ecosystem rules — read before any change

`docs/PLAN.md` is the spec: no design judgment; anything unclear is a question in the PR, not a guess.

- Public API: typed (no `any`/`interface{}`), explicit, minimal; never export a symbol only a test uses.
- One intent, one path: the old path is deleted in the same change. No silent fallback, `TODO`, commented code or stub.
- Fail at compile time, else a loud diagnostic — never silently. Zero values deny.
- A defect in a dependency is reported, never wrapped or patched here.
- No string literals in logic: named constants. The same logic never lives in two repos.
- `cmd/*/main.go`: parse flags, inject, print/exit only. stdout = data, stderr = logs, exit codes are the contract.
- WASM code: `webtyp/fmt`, `webtyp/time`, `webtyp/json` instead of stdlib; no `map`; `dom.Element` embedded by value.
- `css.go`, `svg.go`, `js.go`, `html.go` carry `//go:build !wasm`; no `ssr.go`, no `front.go`.
- Code and keys in English; UI text is `lang.Text`, translations live in `lang.json`.
- Tests cover behaviour and domain rules, never the look (CSS, colours, markup order, class strings). In `tests/` (external package), real stack with fakes only at the edge; run `gotest`.
- Docs (`README.md`, `docs/*`) updated with the API; permanent docs never link `docs/PLAN.md`.
- Before the PR: `git diff --stat main` lists every file of the plan's stages, and nothing the plan did not order is deleted.
<!-- /webtyp:base -->

## This repo

- **The one pub/sub contract of the ecosystem** (`Event`, `Handler`, `Publisher`, `Subscriber`, `Subscription`, `Broker`, `Fanout`). There is no second pub/sub package: `webtyp/bus` is retired. A feature a consumer needs is added here, never in a parallel package.
- The contract is **delivery-agnostic**: `mock.Broker` is synchronous and in-order and is the reference; a broker that is asynchronous or crosses a wire (`webtyp/sse`) must still pass `conformance.Run`.
- The root package, `mock` and `conformance` compile to both WASM and native: no `map` (slices), no `reflect`, no stdlib `errors`/`strings`/`strconv`/`fmt`.
- A `Handler` that panics is **not** recovered: a broken subscriber must fail loudly.
- Payloads are `model.Encodable`, never `any`. A topic is a typed constant exported by the module that publishes it.
- `conformance` is the executable behaviour contract: a rule about delivery changes there first, then in the brokers.
- Implementers of `Subscriber` outside this repo (`webtyp/sse`) are adapted by their own plan after a tag here; do not edit them from this repo.
