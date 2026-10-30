#!/usr/bin/env python3
"""
integration-kit — the reliability layer for gluing external APIs together.

Stdlib only. Offline-testable. Run tests:  python3 -m unittest test_integration -v

What a client actually pays for (and what this provides):
  * OAuth2 client-credentials with token caching + refresh-before-expiry
  * retries with jittered backoff, honouring Retry-After
  * a circuit breaker so a dead upstream stops burning your quota
  * a dead-letter queue (on-disk JSONL) so failed jobs are never lost, and can be replayed
  * structured JSON logging of every step so you can see exactly what ran and why it failed
  * secrets never hardcoded: loaded from env only
"""
from __future__ import annotations

import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

RETRYABLE = {408, 425, 429, 500, 502, 503, 504}


class UpstreamError(RuntimeError):
    def __init__(self, status: int, url: str, retry_after: float | None = None):
        super().__init__(f"HTTP {status} on {url}")
        self.status, self.url, self.retry_after = status, url, retry_after


# ----------------------------------------------------------------------------
class Logger:
    """Minimal structured JSON-lines logger."""

    def __init__(self, path: Path | None = None):
        self.path = path
        self.events: list[dict] = []

    def log(self, event: str, **fields: Any) -> None:
        rec = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "event": event, **fields}
        self.events.append(rec)
        if self.path:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(rec) + "\n")


# ----------------------------------------------------------------------------
class TokenVault:
    """OAuth2 client-credentials with caching and refresh-before-expiry.

    Secrets come from the environment, never from code or config files.
    `fetch` is injectable so tests never hit the network.
    """

    def __init__(self, token_url: str, client_id_env: str, client_secret_env: str,
                 fetch: Callable[[str, str, str], dict] | None = None,
                 skew_seconds: int = 60, clock: Callable[[], float] = time.time):
        self.token_url = token_url
        self.client_id_env = client_id_env
        self.client_secret_env = client_secret_env
        self._fetch = fetch or self._default_fetch
        self.skew = skew_seconds
        self.clock = clock
        self._token: str | None = None
        self._expires_at: float = 0.0
        self.refresh_count = 0

    def _default_fetch(self, url: str, cid: str, secret: str) -> dict:
        data = urllib.parse.urlencode({"grant_type": "client_credentials",
                                       "client_id": cid, "client_secret": secret}).encode()
        req = urllib.request.Request(url, data=data,
                                     headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())

    def get(self) -> str:
        now = self.clock()
        if self._token and now < self._expires_at - self.skew:
            return self._token
        cid = os.environ.get(self.client_id_env, "")
        secret = os.environ.get(self.client_secret_env, "")
        if not cid or not secret:
            raise RuntimeError(f"missing credentials in env: {self.client_id_env}/"
                               f"{self.client_secret_env}")
        payload = self._fetch(self.token_url, cid, secret)
        self._token = payload["access_token"]
        self._expires_at = now + float(payload.get("expires_in", 3600))
        self.refresh_count += 1
        return self._token

    def invalidate(self) -> None:
        self._token, self._expires_at = None, 0.0


# ----------------------------------------------------------------------------
class CircuitBreaker:
    """OPEN after `threshold` consecutive failures; half-open after `reset_s`."""
    CLOSED, OPEN, HALF = "closed", "open", "half-open"

    def __init__(self, threshold: int = 4, reset_s: float = 20.0,
                 clock: Callable[[], float] = time.time):
        self.threshold, self.reset_s, self.clock = threshold, reset_s, clock
        self.state = self.CLOSED
        self.failures = 0
        self._opened_at = 0.0

    def allow(self) -> bool:
        if self.state == self.OPEN:
            if self.clock() - self._opened_at >= self.reset_s:
                self.state = self.HALF
                return True
            return False
        return True

    def record_success(self) -> None:
        self.failures = 0
        self.state = self.CLOSED

    def record_failure(self) -> None:
        self.failures += 1
        if self.failures >= self.threshold:
            self.state = self.OPEN
            self._opened_at = self.clock()


class CircuitOpenError(RuntimeError):
    pass


# ----------------------------------------------------------------------------
@dataclass
class Client:
    """Resilient HTTP caller: retries + backoff + Retry-After + circuit breaker."""
    max_retries: int = 4
    base_delay: float = 0.5
    timeout: float = 30.0
    breaker: CircuitBreaker = field(default_factory=CircuitBreaker)
    log: Logger | None = None
    opener: Callable[[str, dict], tuple[int, bytes, dict]] | None = None
    retry_count: int = 0

    def _open(self, url: str, headers: dict) -> tuple[int, bytes, dict]:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return r.status, r.read(), {k.lower(): v for k, v in r.headers.items()}

    def request(self, url: str, headers: dict | None = None) -> tuple[int, Any]:
        if not self.breaker.allow():
            raise CircuitOpenError(f"circuit open; refusing {url}")
        hdrs = dict(headers or {})
        delay = self.base_delay
        for attempt in range(self.max_retries + 1):
            try:
                status, body, resp_hdrs = (self.opener or self._open)(url, hdrs)
                if status >= 400:
                    raise UpstreamError(status, url,
                                        _retry_after(resp_hdrs))
                self.breaker.record_success()
                try:
                    return status, json.loads(body)
                except (ValueError, json.JSONDecodeError):
                    return status, body
            except UpstreamError as e:
                if e.status not in RETRYABLE or attempt == self.max_retries:
                    self.breaker.record_failure()
                    if self.log:
                        self.log("upstream_error", url=url, status=e.status,
                                 attempts=attempt + 1)
                    raise
                self.retry_count += 1
                nap = e.retry_after if e.retry_after else delay * (2 ** attempt)
                if self.log:
                    self.log("retry", url=url, status=e.status, attempt=attempt + 1,
                             sleep=round(min(nap, 30), 2))
                time.sleep(min(nap + random.uniform(0, 0.2), 30))
            except (urllib.error.URLError, TimeoutError, OSError):
                if attempt == self.max_retries:
                    self.breaker.record_failure()
                    raise UpstreamError(0, url) from None
                self.retry_count += 1
                time.sleep(delay * (2 ** attempt) + random.uniform(0, 0.2))
        raise UpstreamError(0, url)


def _retry_after(hdrs: dict) -> float | None:
    ra = (hdrs or {}).get("retry-after")
    if not ra:
        return None
    try:
        return float(ra)
    except ValueError:
        return None


# ----------------------------------------------------------------------------
class DeadLetterQueue:
    """Failed jobs are appended to a JSONL file; nothing is silently dropped."""

    def __init__(self, path: Path):
        self.path = path

    def push(self, job: dict, error: str) -> None:
        rec = {"failed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "error": error, "job": job}
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")

    def drain(self) -> list[dict]:
        if not self.path.exists():
            return []
        recs = [json.loads(l) for l in self.path.read_text().splitlines() if l.strip()]
        self.path.write_text("")
        return recs

    def pending(self) -> int:
        if not self.path.exists():
            return 0
        return sum(1 for l in self.path.read_text().splitlines() if l.strip())


# ----------------------------------------------------------------------------
@dataclass
class StepResult:
    name: str
    ok: bool
    detail: str = ""


def run_workflow(steps: list[tuple[str, Callable[[], Any]]], dlq: DeadLetterQueue,
                 log: Logger) -> list[StepResult]:
    """Run each step; on failure push the job to the DLQ and continue the rest."""
    results = []
    for name, fn in steps:
        try:
            out = fn()
            log.log("step_ok", step=name)
            results.append(StepResult(name, True, str(out)[:120]))
        except Exception as e:  # noqa: BLE001 - a workflow must not die on one step
            log.log("step_failed", step=name, error=f"{type(e).__name__}: {e}")
            dlq.push({"step": name}, f"{type(e).__name__}: {e}")
            results.append(StepResult(name, False, f"{type(e).__name__}: {e}"))
    return results


def replay(dlq: DeadLetterQueue, handlers: dict[str, Callable[[], Any]],
           log: Logger) -> list[StepResult]:
    """Re-run jobs that landed in the DLQ, using the registered handler per step."""
    jobs = dlq.drain()
    out = []
    for rec in jobs:
        name = rec["job"].get("step", "?")
        fn = handlers.get(name)
        if not fn:
            log.log("replay_no_handler", step=name)
            out.append(StepResult(name, False, "no handler"))
            continue
        try:
            fn()
            log.log("replay_ok", step=name)
            out.append(StepResult(name, True))
        except Exception as e:  # noqa: BLE001
            dlq.push(rec["job"], f"{type(e).__name__}: {e}")
            log.log("replay_failed", step=name, error=str(e))
            out.append(StepResult(name, False, str(e)))
    return out
