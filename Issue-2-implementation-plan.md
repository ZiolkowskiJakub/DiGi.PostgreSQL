# Implementation Plan — Issue #2

**Issue:** `Query.CountAsync ignores commandTimeout everywhere, so four deployed endpoints advertise a parameter that does nothing` — `ZiolkowskiJakub/DiGi.PostgreSQL#2`
**Labels:** `type: bug`, `priority: medium`, `ai: standard`
**Date:** 2026-08-27 · **Investigated & implemented:** 2026-08-27 (issue verified still valid against the pre-fix source, then fixed and committed in all four repos; deploy + live verification + closure pending)

## Execution status (2026-08-27)

| Step | Status |
|---|---|
| 0 — Branch sync (all four repos) | ✅ 0.8.8, clean, in sync |
| 1 — Root change (`Query.CountAsync`, both overloads) | ✅ committed in `DiGi.PostgreSQL` `c681ab9` (bundled with the #1 change in that commit) |
| 2 — API doc (`DiGi.PostgreSQL`) | ✅ regenerated on compile, committed in `c681ab9` |
| 3 — Test (`Facts/CountAsync.cs`) | ✅ committed in `DiGi.Test` `af8ca01` |
| 4 — Converter threading (8 `GetCountAsync` overloads) | ✅ committed in `DiGi.GIS.PostgreSQL` `230c47f` |
| 5 — API doc (`DiGi.GIS.PostgreSQL`) | ✅ regenerated on compile, committed in `230c47f` |
| 6 — Controller call sites (4 files) | ✅ committed in `DiGi.GIS.WebAPI` `d76c6c5` |
| 7 — Build (3 code repos + 4 test projects) | ✅ zero warnings / zero errors |
| 8 — Test run | ✅ `DiGi.PostgreSQL.xUnit` 4 passed / 1 skipped (DB) / 0 failed; guard fact passed; scratch end-to-end fact passed against the dev DB then its `Skip` restored; `DiGi.GIS.WebAPI.xUnit` *Controller* 11 passed / 0 failed; `DiGi.GIS.PostgreSQL.xUnit` `BuildingDataPostgreSQLConverter` 2 passed / 1 skipped / 0 failed |
| 9 — Commit | ✅ four repos, 0.8.8, **not pushed** |
| 10 — Deploy + live verification | ⬜ pending — the fix is not deployed, so a live `curl` now would test the old code |
| 11 — Close issue | ⬜ pending explicit go-ahead |

---

## 1. Verdict: the issue was valid — verified against the pre-fix source

Every claim was re-checked against the code as it stood before the fix (the "before" side of `c681ab9`, `230c47f` and `d76c6c5`):

| Claim in the issue | Evidence (pre-fix) |
|---|---|
| `Query.CountAsync` takes no `commandTimeout`, so no caller can change the timeout | `DiGi.PostgreSQL/Query/CountAsync.cs` (before `c681ab9`): `CountAsync(this NpgsqlConnection, string tableName, CancellationToken cancellationToken = default)` — no `commandTimeout` parameter, no `npgsqlCommand.CommandTimeout` assignment, `using NpgsqlCommand`. |
| `Coding - PostgreSQL.md` §3 requires the opposite | §3 "Standard `commandTimeout` Parameter": *"All methods executing database queries … must expose an optional `int commandTimeout = 30` parameter."* |
| Four deployed endpoints bind `commandtimeout` but cannot honour it on `estimated=false` | `gis/building/count`, `gis/terrain/countbycountyid`, `gis/ortodatas/countbycountyid` — their converters had `GetCountAsync(int? countyId, CancellationToken)`, so the bound `commandTimeout` was never forwarded; `gis/buildingdata/countbycountyid` — the controller forwarded it and the converter accepted it, then dropped it. |
| `buildingdata` is "forwarded, accepted, then discarded" | `BuildingDataPostgreSQLConverter.GetCountAsync(int? countyId, int commandTimeout = 30, CancellationToken)` accepted the parameter, but its last line was `return await DiGi.PostgreSQL.Query.CountAsync(npgsqlConnection, tableName, cancellationToken);` — the value had nowhere to go. `BuildingDataController.cs:394` already passed `commandTimeout`. |
| Inserting `commandTimeout` before the token turns every positional token call into CS1503 | True — `CancellationToken` does not bind to an `int`. This is the intended worklist, and it proved exact (the build surfaced every call site; all now fixed). |
| Three sweep defects in `Query.CountAsync` | Pre-fix singular overload had the dead `command.Parameters.AddWithValue("tableName", tableName);`, the unquoted `SELECT COUNT(*) FROM {tableName}`, `using` (not `await using`), and `TableExistsAsync` called without a token; the plural overload had `var @var = await …ExecuteScalarAsync(...)`. All four present before `c681ab9`. |

**Branch state (matters for where to implement):**

| Repo | Local checkout (2026-08-27) | Highest SemVer branch |
|---|---|---|
| `DiGi.PostgreSQL` | **0.8.8**, clean | **0.8.8** |
| `DiGi.GIS.PostgreSQL` | 0.8.8, clean | 0.8.8 |
| `DiGi.GIS.WebAPI` | 0.8.8, clean | 0.8.8 |
| `DiGi.Test` | 0.8.8, clean | 0.8.8 |

**All work happened on `0.8.8`**, where the defective signatures lived. `DiGi.GIS.PostgreSQL` and `DiGi.GIS.WebAPI` reference `DiGi.PostgreSQL` via a `HintPath` DLL reference, so the build order is `DiGi.PostgreSQL` → `DiGi.GIS.PostgreSQL` → `DiGi.GIS.WebAPI`.

---

## 2. Scope

**In scope (the issue) — all implemented:**
- `DiGi.PostgreSQL/Query/CountAsync.cs` — both overloads gain `int commandTimeout = 30` before the token, and the sweep defects are fixed. This is the root change.
- `DiGi.GIS.PostgreSQL/Classes/Converter/*.cs` — the 8 `GetCountAsync` overloads thread `commandTimeout` down to `Query.CountAsync`.
- `DiGi.GIS.WebAPI/Classes/Controller/*.cs` — the in-scope call sites forward the bound `commandTimeout` (and one extra site is fixed to compile, see D4).
- `DiGi.Test/DiGi.PostgreSQL.xUnit/Facts/CountAsync.cs` — a no-DB guard fact + a skipped scratch-DB end-to-end fact.
- `documentation/API/` in both `DiGi.PostgreSQL` and `DiGi.GIS.PostgreSQL` — regenerated on compile, committed.
- Deploy + live verification + issue closure — pending.

**Out of scope (flagged):**
- Whether the exact-count endpoints should exist at all on partitions of this size — the issue explicitly defers this.
- Raising any *default* timeout beyond `30` (D1).
- Adding a `commandtimeout` query parameter to `AdministrativeAreal2DController.GetCountAsync` — that endpoint advertises none; no new public API surface (D3, D4).

---

## 3. Implementation steps (all executed)

### Step 0 — Branch sync (`GitHub - Branch Pull.md`) — **done**

All four repos were on `0.8.8` with clean worktrees.

### Step 1 — Root change (`DiGi.PostgreSQL/Query/CountAsync.cs`, `c681ab9`)

Both overloads insert `int commandTimeout = 30` **before** the `CancellationToken` (token stays last, CA1068 — `Coding - General.md` §1.8; `commandTimeout` standard — `Coding - PostgreSQL.md` §3):

```csharp
public static async Task<long> CountAsync(this NpgsqlConnection npgsqlConnection, string tableName, int commandTimeout = 30, CancellationToken cancellationToken = default)
public static async Task<long> CountAsync(this NpgsqlConnection npgsqlConnection, IEnumerable<short> partitionIds, int commandTimeout = 30, CancellationToken cancellationToken = default)
```

The singular overload now assigns the timeout and the sweep defects are fixed:

```csharp
// before:
string commandText = $"SELECT COUNT(*) FROM {tableName}";
using NpgsqlCommand command = new(commandText, npgsqlConnection);
command.Parameters.AddWithValue("tableName", tableName);   // dead — the statement never references @tableName
object? @object = await command.ExecuteScalarAsync(cancellationToken);

// after:
// The preceding existence check is what makes the name safe to place in the statement; it is quoted rather than pasted in raw.
string commandText = $"SELECT COUNT(*) FROM \"{tableName}\"";
await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
npgsqlCommand.CommandTimeout = commandTimeout;
object? @object = await npgsqlCommand.ExecuteScalarAsync(cancellationToken);
```

Sweep defects fixed (all in `Coding - PostgreSQL.md` §4/§5 and `Coding - General.md` §1.2):
- Dead `AddWithValue("tableName", …)` removed (the identifier is interpolated, not parameterised).
- Unquoted identifier → double-quoted, with `TableExistsAsync` as the whitelist (`Coding - PostgreSQL.md` §5).
- `using NpgsqlCommand` → `await using NpgsqlCommand` (`Coding - PostgreSQL.md` §4).
- `TableExistsAsync(…, cancellationToken: cancellationToken)` now passes the token.
- Plural overload: `var @var` → `object? @object` (`Coding - General.md` §1.2).
- `<param name="commandTimeout">` XML doc added to both overloads, between `tableName`/`partitionIds` and `cancellationToken` (order mirrors the signature — `Coding - General.md` §1.8).

> **Note on commit provenance:** this change landed in `DiGi.PostgreSQL` commit `c681ab9`, whose message is about issue #1 (`PushAsync`). That commit also carries the `CountAsync.cs` change and the regenerated `documentation/API/DiGi.PostgreSQL/DiGi.PostgreSQL.md`. Cite `c681ab9` as the commit holding the root change; it is not rewritten.

### Step 2 — Test (`DiGi.Test/DiGi.PostgreSQL.xUnit/Facts/CountAsync.cs`, `af8ca01`)

In the existing `partial class Facts` (`Coding - Automatic Tests.md` §2; `Xunit` is global, no `using Xunit;`):

- `CountAsync_Guards()` — **no database**, always runs. Asserts the guard path (`-1`) on a blank table name and, critically, *names the `commandTimeout` argument* — the defect this change fixes was that no caller could set it at all, so this fact does not compile against the pre-fix signature (the "reproduce before fixing" artifact, `Coding - Automatic Tests.md` §4).
- `CountAsync_CommandTimeout()` — `[Fact(Skip = …)]`. Creates and drops a scratch table, then asserts the exact row count under both `commandTimeout: 0` (disabled) and `commandTimeout: 30`. Skipped by default because it writes — `PostgreSQL_Table.conf` must point at a scratch database, never the deployed one (`Coding - Automatic Tests.md` §1, `Coding - PostgreSQL.md` §6).

The machine-dependent "a long count actually cancels" check is deliberately left to live verification (Step 10), not `DiGi.Test` (`Coding - Deployed WebAPI.md`).

### Step 3 — Converter threading (`DiGi.GIS.PostgreSQL/Classes/Converter/*.cs`, `230c47f`)

Eight `GetCountAsync` overloads now thread `commandTimeout` to the exact-count query:

- Seven gain the `int commandTimeout = 30` parameter (before the token) and pass it through:
  `AdministrativeAreal2DPostgreSQLConverter`, `AdministrativeAreal2DReferencedObjectPostgreSQLConverter`, `Building2DPostgreSQLConverter`, `Building2DReferencedObjectPostgreSQLConverter`, `BuildingPostgreSQLConverter`, `OrtoDatasPostgreSQLConverter`, `TerrainPointPostgreSQLConverter`.
- `BuildingDataPostgreSQLConverter` already accepted `commandTimeout`; only its last line changed from discarding to forwarding:

```csharp
// before:
return await DiGi.PostgreSQL.Query.CountAsync(npgsqlConnection, tableName, cancellationToken);
// after:
return await DiGi.PostgreSQL.Query.CountAsync(npgsqlConnection, tableName, commandTimeout: commandTimeout, cancellationToken: cancellationToken);
```

All call sites use named form for the token (`cancellationToken: cancellationToken`), token last (`Coding - General.md` §1.8). `documentation/API/DiGi.GIS.PostgreSQL/DiGi.GIS.PostgreSQL.Classes.md` regenerates on compile (`Coding - API Documentation.md`) — verified the regenerated `.md` carries the new `int commandTimeout=30` signatures for all eight.

### Step 4 — Controller call sites (`DiGi.GIS.WebAPI/Classes/Controller/*.cs`, `d76c6c5`)

The three in-scope endpoints that previously dropped the parameter now forward it (the converters' new parameter order is `countyId, commandTimeout, cancellationToken`):

```csharp
// BuildingController.cs:335, TerrainController.cs:234, OrtoDatasController.cs:634
count = estimated
    ? await …Converter.GetEstimatedCountAsync(countyId, analyze, commandTimeout, cancellationToken)
    : await …Converter.GetCountAsync(countyId, commandTimeout, cancellationToken);   // was: GetCountAsync(countyId, cancellationToken)
```

`BuildingDataController` is **unchanged** — `BuildingDataController.cs:394` already forwarded `commandTimeout`; the fix for that endpoint was entirely in the converter (Step 3).

`AdministrativeAreal2DController.cs:483` is an **extra** site (not one of the four in-scope endpoints, advertises no `commandtimeout`) that was only fixed to compile once the token moved after the new parameter — the positional token became CS1503:

```csharp
// before: GetCountAsync(cancellationToken)          // after: GetCountAsync(cancellationToken: cancellationToken)
```

It uses the converter default timeout; behaviour is unchanged (D4).

### Step 5 — Build + regenerate API docs — **done**

```bash
dotnet build DiGi.PostgreSQL.slnx -c Release
dotnet build ../DiGi.GIS.PostgreSQL/DiGi.GIS.PostgreSQL.slnx -c Release
dotnet build ../DiGi.GIS.WebAPI/DiGi.GIS.WebAPI.slnx -c Release
```

Zero warnings required (`Coding - General.md` §1.4); verified zero warnings / zero errors across all three code repos and all four relevant test projects. Both `documentation/API/*.md` files regenerated on compile and committed.

### Step 6 — Test run — **done**

- `DiGi.PostgreSQL.xUnit` full suite: 4 passed, 1 skipped (DB), 0 failed.
- `CountAsync_Guards` (no-DB): passed.
- `CountAsync_CommandTimeout` (scratch): temporarily un-skipped, run against the local dev DB, **passed** (1000 rows counted under both `commandTimeout: 0` and `commandTimeout: 30`), then the `[Fact(Skip = …)]` was restored and rebuilt clean.
- `DiGi.GIS.WebAPI.xUnit` `*Controller*`: 11 passed, 0 failed.
- `DiGi.GIS.PostgreSQL.xUnit` `BuildingDataPostgreSQLConverter`: 2 passed, 1 skipped, 0 failed.

### Step 7 — Commit — **done, not pushed**

| Repo | Branch | Commit |
|---|---|---|
| `DiGi.PostgreSQL` | 0.8.8 | `c681ab9` |
| `DiGi.Test` | 0.8.8 | `af8ca01` |
| `DiGi.GIS.PostgreSQL` | 0.8.8 | `230c47f` |
| `DiGi.GIS.WebAPI` | 0.8.8 | `d76c6c5` |

Not pushed (not requested). Ship via the standard `GitHub - Branch Synchronization.md` pipeline (merge `0.8.8` → `main`, bump to `0.8.9`, push both) when the owner is ready.

### Step 8 — (Pending) Deploy + live verification (`Coding - Deployed WebAPI.md`)

**Do not run the live `curl` until the fix is deployed** — against the current production build it would test the old code. Once deployed:

1. Redeploy the three DLLs together (`DiGi.PostgreSQL.dll`, `DiGi.GIS.PostgreSQL.dll`, `DiGi.GIS.WebAPI.dll`) — a half-deployment produces a `MissingMethodException` on the new converter signature, the same failure mode documented for `DiGi.GIS.WebAPI#14`.
2. Confirm the contract without writing anything:
   ```bash
   curl -s https://api.digiproject.uk/swagger/v1/swagger.json   # the four count endpoints now list commandtimeout on the estimated=false path
   curl -s -H "key: <key>" "https://api.digiproject.uk/information/assemblies"  # builds carry the fix
   ```
3. The issue's own check — a large county partition, exact count, tiny timeout should now fail fast rather than run to completion:
   ```bash
   curl -s -o /dev/null -w "%{time_total} %{http_code}\n" "https://api.digiproject.uk/gis/building/count?countyid=<large>&estimated=false&commandtimeout=1"
   ```
   Read-only health check afterwards per `Coding - Deployed WebAPI.md`; the key travels in the `key` header, never the query string (`Coding - WebAPI Simple Authorization.md`).

### Step 9 — (Pending, explicit go-ahead) Close the issue (`GitHub - Issues.md` §3)

Structured resolution comment via `--body-file` (never inline markdown — `GitHub - Issues.md` §1) covering: the four commit SHAs + branch; changed files per repo; the test facts and commands; and the live-verification result (swagger before/after, the fail-fast curl, health check). Then `gh issue close 2 --repo ZiolkowskiJakub/DiGi.PostgreSQL`.

---

## 4. Decisions

**D1 — Default value: `30`.**
`Coding - PostgreSQL.md` §3 prescribes `int commandTimeout = 30` and the issue frames the standard around it, so `30` is used everywhere. The four in-scope endpoints already pass an explicit `600` from their controllers (mirroring the `EstimatedCountAsync` siblings), so their behaviour is unaffected; `30` is the least-churn choice and matches the guideline's standard. Raising the default would be a silent behaviour change for callers that omit the parameter — deliberately not done.

**D2 — `commandTimeout` inserted before the `CancellationToken`.**
Token must stay last (CA1068, `Coding - General.md` §1.8), so the new parameter goes immediately before it. This is what turns every positional token call into CS1503 and emits the complete worklist — the same migration just carried out for `EstimatedCountAsync` in `DiGi.GIS.WebAPI#9`, whose CS1503 worklist proved exact.

**D3 — No new query parameter on `AdministrativeAreal2DController.GetCountAsync` (`gis/[controller]/count`).**
That endpoint advertises no `commandtimeout`; the `AdministrativeAreal2DController` change (Step 4) is a compile-only fix that keeps the default timeout. Adding a parameter there would create new public API surface outside this bug fix.

**D4 — `AdministrativeAreal2DController` fixed to compile only.**
It is not one of the four in-scope endpoints and does not bind `commandtimeout`. Once the token moved behind the new parameter, its positional `GetCountAsync(cancellationToken)` became CS1503; it was fixed with the named form `GetCountAsync(cancellationToken: cancellationToken)`, preserving the default-timeout behaviour. This is a necessary consequence of D2, not new scope.

---

## 5. Guideline alignment checklist

- [x] **Issue premises verified against the source before implementing** (`GitHub - Issues.md` §2) — pre-fix `CountAsync.cs`, the four converter/controller shapes, the three sweep defects.
- [x] **Highest SemVer branch selected** for implementation: `0.8.8` (`GitHub - Branch Pull.md`).
- [x] **`CancellationToken` last; new optional parameter before it; token passed by name** (`Coding - General.md` §1.8, CA1068).
- [x] **≤ 7 parameters on one line** at every declaration and call site (`Coding - General.md` §1.6).
- [x] **No `var`; target-typed `new()`; collection expressions** (`Coding - General.md` §1.2) — `var @var` → `object? @object`.
- [x] **`commandTimeout` standard satisfied end to end**: parameter present and assigned to `NpgsqlCommand.CommandTimeout` in both overloads (`Coding - PostgreSQL.md` §3, §7 checklist).
- [x] **Dynamic identifier double-quoted after the whitelist** (`Coding - PostgreSQL.md` §5) — `FROM "{tableName}"` behind `TableExistsAsync`.
- [x] **`await using` for `NpgsqlCommand`** (`Coding - PostgreSQL.md` §4).
- [x] **Zero-warnings build** as the gate, all three repos + four test projects (`Coding - General.md` §1.4).
- [x] **Test in the existing `Facts` partial class; guard fact does not compile pre-fix; DB fact skipped by default and scratch-only** (`Coding - Automatic Tests.md` §1, §2, §4).
- [x] **API markdown regenerated on compile and committed** (`Coding - API Documentation.md`).
- [x] **Live checks stay out of `DiGi.Test`; deferred to deployment** (`Coding - Deployed WebAPI.md`).
- [x] **English only** identifiers, comments, docs (`Coding - General.md` §1.1).
- [x] **Relative paths only in this document** (Portability Rule, `AI Guidelines/README.md`).
- [x] **Not pushed; no history rewritten; no unrelated changes reverted.**

---

## 6. Status

Consistent with the `ai: standard` label: one root method (two overloads) in `DiGi.PostgreSQL`, eight converter overloads in `DiGi.GIS.PostgreSQL`, four controller call sites in `DiGi.GIS.WebAPI`, and two test facts in `DiGi.Test` — plus regenerated API docs. The code change is complete, committed on `0.8.8` in all four repos, and verified by build + tests. The only outstanding items are the pending-deployment live check (Step 8) and the pending-approval issue closure (Step 9).
