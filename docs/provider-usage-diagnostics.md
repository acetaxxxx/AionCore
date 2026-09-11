# Provider usage diagnostics

AionCore has two token usage paths:

- The existing session runtime usage snapshot and `message.stream` usage frame
  power the UI context usage indicator.
- The per-turn `provider_usage` diagnostic envelope records raw provider usage
  measurements in the turn attribution sidecar. It is additive telemetry and
  is independent of the UI snapshot.

The per-turn diagnostic path is disabled by default. Enable it explicitly for
an AionCore process with:

```text
AIONUI_ENABLE_PROVIDER_USAGE_DIAGNOSTICS=1
```

`1`, `true`, `yes`, and `on` (case-insensitive) enable the path. Any other
value, including an unset variable, leaves it disabled. The toggle controls
only persisted per-turn provider usage diagnostics; it does not disable the
existing UI usage indicator.
