#!/usr/bin/env python3
"""Offline tests for integration-kit. No network. python3 -m unittest test_integration -v"""
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path

import integration as I
from integration import (TokenVault, CircuitBreaker, CircuitOpenError, Client,
                         DeadLetterQueue, Logger, UpstreamError, run_workflow, replay)


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += s


class TestTokenVault(unittest.TestCase):
    def setUp(self):
        import os
        os.environ["TEST_CID"] = "id"
        os.environ["TEST_SEC"] = "sec"

    def _vault(self, expires_in=3600, clock=None):
        calls = []

        def fetch(url, cid, secret):
            calls.append((cid, secret))
            return {"access_token": f"tok{len(calls)}", "expires_in": expires_in}

        v = TokenVault("https://auth/x", "TEST_CID", "TEST_SEC", fetch=fetch, clock=clock or FakeClock())
        v._calls = calls
        return v

    def test_first_get_fetches(self):
        v = self._vault()
        self.assertEqual(v.get(), "tok1")
        self.assertEqual(v.refresh_count, 1)

    def test_cached_token_reused(self):
        v = self._vault()
        v.get(); v.get(); v.get()
        self.assertEqual(v.refresh_count, 1, "should reuse cached token")

    def test_refresh_before_expiry(self):
        clk = FakeClock(1000)
        v = self._vault(expires_in=100, clock=clk)
        v.get()
        clk.advance(50)   # 50s left, skew=60 -> must refresh
        self.assertEqual(v.get(), "tok2")
        self.assertEqual(v.refresh_count, 2)

    def test_no_refresh_well_before_expiry(self):
        clk = FakeClock(1000)
        v = self._vault(expires_in=1000, clock=clk)
        v.get()
        clk.advance(500)  # 500s left > skew 60 -> reuse
        v.get()
        self.assertEqual(v.refresh_count, 1)

    def test_missing_credentials_raises(self):
        v = TokenVault("https://auth/x", "NOPE_CID", "NOPE_SEC",
                       fetch=lambda *a: {"access_token": "x"}, clock=FakeClock())
        with self.assertRaises(RuntimeError):
            v.get()

    def test_invalidate_forces_refresh(self):
        v = self._vault()
        v.get(); v.invalidate()
        self.assertEqual(v.get(), "tok2")


class TestCircuitBreaker(unittest.TestCase):
    def test_opens_after_threshold(self):
        clk = FakeClock()
        b = CircuitBreaker(threshold=3, reset_s=10, clock=clk)
        for _ in range(3):
            b.record_failure()
        self.assertEqual(b.state, CircuitBreaker.OPEN)
        self.assertFalse(b.allow())

    def test_half_open_after_reset(self):
        clk = FakeClock()
        b = CircuitBreaker(threshold=2, reset_s=10, clock=clk)
        b.record_failure(); b.record_failure()
        self.assertFalse(b.allow())
        clk.advance(11)
        self.assertTrue(b.allow())
        self.assertEqual(b.state, CircuitBreaker.HALF)

    def test_success_closes(self):
        b = CircuitBreaker(threshold=2)
        b.record_failure(); b.record_failure()
        b.record_success()
        self.assertEqual(b.state, CircuitBreaker.CLOSED)
        self.assertTrue(b.allow())


class TestClientRetry(unittest.TestCase):
    def _client(self, script, **kw):
        state = {"i": 0}

        def opener(url, headers):
            item = script[min(state["i"], len(script) - 1)]
            state["i"] += 1
            if isinstance(item, Exception):
                raise item
            return item

        c = Client(base_delay=0.01, **kw)
        c.opener = opener
        return c

    def test_retries_503_then_ok(self):
        def op(url, headers):
            op.n = getattr(op, "n", 0) + 1
            if op.n == 1:
                raise UpstreamError(503, url)
            return 200, b'{"ok": true}', {}
        c = Client(base_delay=0.01); c.opener = op
        st, body = c.request("https://x")
        self.assertEqual(st, 200); self.assertEqual(body, {"ok": True})
        self.assertEqual(c.retry_count, 1)

    def test_404_fails_fast_no_retry(self):
        def op(url, headers):
            op.n = getattr(op, "n", 0) + 1
            raise UpstreamError(404, url)
        c = Client(base_delay=0.01); c.opener = op
        with self.assertRaises(UpstreamError) as ctx:
            c.request("https://x")
        self.assertEqual(ctx.exception.status, 404)
        self.assertEqual(op.n, 1)
        self.assertEqual(c.breaker.failures, 1)

    def test_circuit_open_blocks_request(self):
        b = CircuitBreaker(threshold=1)
        b.record_failure()
        c = Client(breaker=b)
        with self.assertRaises(CircuitOpenError):
            c.request("https://x")

    def test_breaker_opens_on_repeated_failure(self):
        def op(url, headers):
            raise UpstreamError(500, url)
        b = CircuitBreaker(threshold=2)
        c = Client(base_delay=0.01, max_retries=0, breaker=b); c.opener = op
        for _ in range(2):
            with self.assertRaises(UpstreamError):
                c.request("https://x")
        self.assertEqual(b.state, CircuitBreaker.OPEN)


class TestDLQ(unittest.TestCase):
    def test_push_then_drain_empties(self):
        with tempfile.TemporaryDirectory() as d:
            q = DeadLetterQueue(Path(d) / "dlq.jsonl")
            q.push({"step": "a"}, "boom")
            q.push({"step": "b"}, "bang")
            self.assertEqual(q.pending(), 2)
            jobs = q.drain()
            self.assertEqual([j["job"]["step"] for j in jobs], ["a", "b"])
            self.assertEqual(q.pending(), 0)

    def test_drain_empty(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(DeadLetterQueue(Path(d) / "none.jsonl").drain(), [])


class TestWorkflow(unittest.TestCase):
    def test_failed_step_goes_to_dlq_others_continue(self):
        with tempfile.TemporaryDirectory() as d:
            dlq = DeadLetterQueue(Path(d) / "dlq.jsonl")
            log = Logger()
            done = []

            def ok1(): done.append(1); return "a"
            def bad(): raise ValueError("nope")
            def ok2(): done.append(2); return "b"

            res = run_workflow([("s1", ok1), ("s2", bad), ("s3", ok2)], dlq, log)
            self.assertEqual([r.ok for r in res], [True, False, True])
            self.assertEqual(done, [1, 2], "workflow must continue past a failure")
            self.assertEqual(dlq.pending(), 1)
            self.assertTrue(any(e["event"] == "step_failed" for e in log.events))

    def test_replay_reruns_failed_job(self):
        with tempfile.TemporaryDirectory() as d:
            dlq = DeadLetterQueue(Path(d) / "dlq.jsonl")
            log = Logger()
            dlq.push({"step": "sync"}, "timeout")
            called = []
            out = replay(dlq, {"sync": lambda: called.append(1)}, log)
            self.assertTrue(out[0].ok)
            self.assertEqual(called, [1])
            self.assertEqual(dlq.pending(), 0)

    def test_replay_failure_requeues(self):
        with tempfile.TemporaryDirectory() as d:
            dlq = DeadLetterQueue(Path(d) / "dlq.jsonl")
            log = Logger()
            dlq.push({"step": "sync"}, "timeout")

            def boom(): raise RuntimeError("still down")
            replay(dlq, {"sync": boom}, log)
            self.assertEqual(dlq.pending(), 1, "failed replay must be re-queued")


if __name__ == "__main__":
    unittest.main(verbosity=2)
