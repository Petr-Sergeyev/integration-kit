# integration-kit

The reliability layer for gluing external APIs into one workflow. **Stdlib only.**

```bash
python3 -m unittest test_integration -v   # 18 offline tests
```

Companion sample: [`scraper-kit`](https://github.com/Petr-Sergeyev/scraper-kit) (scraping).

## What it provides

| Capability | Where | Test |
|---|---|---|
| OAuth2 client-credentials, token caching, refresh-before-expiry | `TokenVault` | `TestTokenVault` |
| Secrets from env only — never hardcoded | `TokenVault.get` | `test_missing_credentials_raises` |
| Retries with jittered backoff, honours `Retry-After` | `Client.request` | `test_retries_503_then_ok` |
| Non-retryable errors fail fast (protects quota) | `Client.request` | `test_404_fails_fast_no_retry` |
| **Circuit breaker** — a dead upstream stops burning requests | `CircuitBreaker` | `TestCircuitBreaker`, `test_circuit_open_blocks_request` |
| **Dead-letter queue** — failed jobs persisted to JSONL, never lost | `DeadLetterQueue` | `TestDLQ` |
| Workflow keeps running past a single failed step | `run_workflow` | `test_failed_step_goes_to_dlq_others_continue` |
| Replay of failed jobs once upstream recovers | `replay` | `TestWorkflow` |
| Structured JSON logging of every step | `Logger` | asserted in workflow tests |

## Why a client cares

Most "integrations" fall over on day two: token expires mid-run, one flaky 503 kills the whole
batch, a dead partner API hammers itself into a rate-limit ban, and nobody can tell you what
actually ran. This kit makes each of those a non-event and leaves an auditable log plus a
replayable queue behind.

## Contract scope

Included: auth handling (OAuth2/API keys), retries/backoff, circuit breaking, DLQ + replay,
scheduling or webhook trigger, structured logging, tests against mocked responses, live
end-to-end demo, README with setup.

Not included: storing secrets in the repo (they come from env / your secret manager),
and anything that would violate a partner API's terms.

## Tests

```
python3 -m unittest test_integration -v
# Ran 18 tests ... OK
```

All offline: the HTTP layer and the token fetcher are injected, and time is an injectable
clock, so refresh-before-expiry and breaker timing are asserted deterministically.

## Licence

MIT.
