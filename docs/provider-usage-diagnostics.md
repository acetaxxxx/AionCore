# Provider usage and context attribution diagnostics

AionCore has two token usage paths:

- The existing session runtime usage snapshot and `message.stream` usage frame
  power the UI context usage indicator.
- The per-turn `provider_usage` diagnostic envelope records raw provider usage
  measurements in the turn attribution sidecar. It is additive telemetry and
  is independent of the UI snapshot.

The per-turn diagnostic and context attribution paths are disabled by default.
Enable both explicitly for an AionCore process with:

```text
AIONUI_ENABLE_PROVIDER_USAGE_DIAGNOSTICS=1
```

`1`, `true`, `yes`, and `on` (case-insensitive) enable both paths. Any other
value, including an unset variable, leaves them disabled. The toggle controls
only persisted diagnostics; it does not disable the existing UI usage
indicator.

## Context attribution

PR16 source attribution records use the same toggle. This controls attribution
records emitted by the conversation orchestrator, Team adapter, and streaming
tool-event relay. It does not disable lifecycle pre-turn, mid-turn, or
terminal records.
