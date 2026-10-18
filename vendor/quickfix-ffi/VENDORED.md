# Vendored quickfix-ffi with a LaminarDB bridge

Status: adopted 2026-10-14 for the optional `fix` source feature. Remove the bridge file once an
upstream binding exposes length-aware field access, dictionary introspection, forced stop, and
externally owned stores, and drop the `build.rs` change once upstream compiles with `/EHsc` on
MSVC.

## Why

`quickfix-ffi` 0.2.2 builds the QuickFIX C++ engine and a C binding. Its published Rust wrapper,
`quickfix` 0.2.2, is not used, and the binding alone is not sufficient:

- every field getter, `FixMessage_readString`, `FixMessage_fromString`, and the log callbacks
  pass NUL-terminated strings, truncating FIX `data` fields at an embedded NUL byte;
- there is no field-map access that covers header, trailer, and nested groups uniformly, and no
  dictionary introspection for validating column mappings before activation;
- `FixInitiator_stop`/`FixAcceptor_stop` cannot request a forced stop.

The `quickfix` crate also maps a Rust panic in a callback to success (`callback_to_code`), its
`MsgFromAppError` cannot express the `RejectLogon` result that disconnects without advancing the
inbound sequence number, and its `UnsupportedMessageType` code is ignored by the binding's
`fromApp` switch. The LaminarDB source therefore implements its own callbacks over this binding.

## Provenance

| Part | Source |
|---|---|
| Crate (`build.rs`, `CMakeLists.txt`, `quickfix-bind/`, `src/lib.rs`, `README.md`) | crates.io `quickfix-ffi` 0.2.2, crate SHA-256 `28f8a81fff42b6ea4c3619e6400e1f301fdcfabde066753ccc8402c6b0070f47`, git `arthurlm/quickfix-rs` tag `v0.2.2` (`cc2b7a5cda080d22162e50f08b97bac7c5273af9`) |
| `libquickfix/` | `quickfix/quickfix` commit `386ce46e917ae494ab6e90b1be90fd421cdbe3f9` (2026-05-20), 28 commits after the `v1.16.0` release tag, as bundled by the crate |

The engine revision after `v1.16.0` adds fixes for GapFill queue memory, peer-initiated
disconnect detection, SSL connection failure handling, and duplicate `onDisconnect`. The
upstream repository published no security advisories as of 2026-10-14.

## Local changes

- `quickfix-bind/include/laminar_bridge.h` and `quickfix-bind/src/laminar_bridge.cpp` (new):
  length-aware field read/write and serialization, uniform field-map and group access,
  dictionary introspection, session state (store creation time and sequence numbers), sender
  sequence setter for test counterparties, `stop(force)` for initiators and acceptors, dictionary
  version, parsing with separate session and application dictionaries, and a
  `MessageStoreFactory` whose stores persist every sequence-number change and reset through
  caller callbacks before caching it (outbound messages are never stored). Every function
  catches all C++ exceptions, including non-`std::exception` ones.
- `libquickfix/src/C++/ThreadedSocket{Connection,Initiator,Acceptor}.cpp` and `Utility.{h,cpp}`:
  each connection socket is closed exactly once. Upstream closed it in
  `ThreadedSocketConnection::disconnect()` and again in `onStop()` (or in `disconnect()` after
  `onStop()` had closed it), so a descriptor number reused by another thread in between was closed
  under it; Rust's debug-build `OwnedFd` check then aborted the process with "owned file
  descriptor already closed". `disconnect()` and `onStop()` now only shut a connection down; the
  socket is closed by whoever removes it from the thread map (`removeThread`, or `onStop()` after
  joining the thread). Listening sockets are still closed before the join, since shutdown does
  not wake `accept` on every platform.
- `quickfix-bind/CMakeLists.txt` compiles the bridge into `quickfixbind`.
- `build.rs` compiles both libraries with `/EHsc` on MSVC. `cmake-rs` replaces CMake's default
  MSVC flags, so the published crate builds without unwind semantics (warning C4530): an engine
  exception then skips destructors, and an `IOException` from a store left the session-state
  mutex locked, deadlocking the reconnect thread.
- `src/lib.rs` declares the bridge functions.
- `Cargo.toml` is renamed to `laminardb-quickfix-ffi` 0.33.0, declares `links = "quickfixbind"`
  so Cargo rejects a second copy of the native library in one build, and packages this file;
  `.cargo-ok`, `.cargo_vcs_info.json`, `Cargo.toml.orig`, and `Cargo.lock` from the crate archive
  are omitted.

## Publication

The release workflow publishes this crate with the other maintained forks, before
`laminar-connectors`, on the release version; CI checks that it packages.

No engine source under `libquickfix/` is modified.

## Engine behaviour this integration depends on

Verified against `libquickfix/src/C++/Session.cpp` and the qualification tests in
`crates/laminar-connectors/src/fix/engine/tests.rs`:

- `Session::next(const Message&)` validates against the dictionary, then `verify()` checks
  CompIDs, SendingTime, and sequence; a too-high message is queued and a too-low one without
  PossDupFlag disconnects, both before any application callback. `fromApp` runs only for the
  expected sequence number, and `incrNextTargetMsgSeqNum` follows its successful return.
- A `RejectLogon` thrown from `fromApp`/`fromAdmin` is handled by `generateLogout` and
  `disconnect` without advancing the target sequence number.
- `generateReject` and `generateBusinessReject` advance the target sequence number before the
  outbound reject reaches `toAdmin`; dictionary validation runs before sequencing, so a message
  that arrives ahead of the expected number is rejected without consuming it.
- `FileStore` uses `fflush` without `fsync`; the LaminarDB source replaces it with its own store.
- Every store call runs under the session-state mutex. An `IOException` from a store makes
  `Session::next` disconnect and `Session::sendRaw` return without sending; with
  `PersistMessages=N`, `nextResendRequest` answers with a gap fill without reading the store.
- Under FIXT.1.1 the engine parses inbound messages with the application dictionary of its own
  DefaultApplVerID but validates each one against the dictionary of the message's ApplVerID
  (1128) or the counterparty's DefaultApplVerID (1137); an ApplVerID without a configured
  dictionary gets an empty dictionary, which validates nothing. `AppDataDictionary` is
  registered under the configured DefaultApplVerID, not under the dictionary's own version.
- A dictionary's version is `type.major.minor` of its root element; the service pack attribute
  is ignored.
- The engine resets its store (`SessionState::reset`) for an accepted Logon with
  ResetSeqNumFlag=Y, `ResetOnLogon`/`ResetOnLogout`/`ResetOnDisconnect`, and when the store's
  creation time falls outside the current schedule period.
- The SSL initiator sets SNI but performs no hostname verification and skips peer verification
  when no CA is configured; the LaminarDB source does not use it.
- The single-threaded `SocketInitiator` never reconnects after a session-initiated disconnect
  (`SocketMonitor::drop` removes the socket before `ConnectorWrapper::onError` calls it again),
  so the source uses `ThreadedSocketInitiator`.
- `~Initiator` and `~Acceptor` delete their sessions without stopping engine threads; the Rust
  owners call `stop(force)` before deleting.
- The session registry is process-global: `Session::Session` ignores a failed `addSession` for a
  duplicate ID, and `~Session` erases the registry entry by ID, so two live sessions with one ID
  corrupt each other. The Rust sockets claim each session ID process-wide.

## Licences

The binding is `MIT OR Apache-1.1`. The engine is under the QuickFIX Software License 1.0
(`libquickfix/LICENSE`): binary redistributions must reproduce its notice, and end-user
documentation must include "This product includes software developed by quickfixengine.org
(http://www.quickfixengine.org/)." The engine bundles pugixml (MIT) and double-conversion
(BSD-3-Clause). The `fix` feature is off by default; distributions that enable it must carry
these notices.
