# qdone Code Review Guide

## Purpose

qdone is a public Node.js package and command-line job queue for Amazon SQS. Lead with concrete findings. Comment only
when changed code creates a reachable correctness, security, data integrity, backward compatibility, production
reliability, or meaningful performance problem.

For every finding, identify the triggering input or state, trace the relevant execution path, and explain the
resulting effect. Do not infer a defect from the changed line alone when callers, defaults, or downstream consumers
determine the behavior.

## Review priorities

- Preserve the documented public JavaScript API, CLI flags and aliases, queue naming behavior, and Node.js 16
  compatibility unless a breaking change is explicit.
- Trace SQS message receipt, visibility extension, acknowledgement, failure-queue and dead-letter-queue routing,
  retry behavior, wildcard queue discovery, and AWS throttling or partial-failure handling.
- For worker and scheduler changes, verify process lifecycle behavior: inline versus child-process execution,
  SIGTERM/SIGKILL escalation, PID ownership, timers, completion races, and per-job state cleanup.
- For Redis or in-memory coordination, check atomicity, expiry, deduplication, reconnect behavior, and consistency
  across concurrent workers.
- Treat a missing test as a finding only when a specific changed behavior has an untested regression path. Name the
  scenario the test should cover.
- Source is ESM and CommonJS output is generated. Review source and build configuration rather than generated output.
- JavaScript Standard Style and the build/test commands enforce formatting and compilation. Do not duplicate those
  checks as review comments.

## Evidence and trust boundaries

Do not label dependency injection, option objects, queue contents, or configuration as attacker-controlled without
tracing how untrusted data reaches the operation. Distinguish public API misuse from an exploitable boundary.

Do not claim a package version is nonexistent, deprecated, incompatible, or vulnerable without evidence in the
repository. If a conclusion depends on current npm, AWS, Redis, or Node.js documentation that is not available, state
the uncertainty instead of asserting a defect.

## Do not post

Do not comment solely about wording, spelling, naming preferences, prose polish, redundant comments, broad refactors,
minor readability changes, formatting, magic-number extraction, test-helper deduplication, or micro-optimizations.
Do not request defensive null checks or catch blocks without showing a reachable failure path and the incorrect
resulting behavior.

Avoid findings about pre-existing behavior outside the changed execution path unless the pull request makes that
behavior newly reachable or materially worse.

## Severity

- **Critical:** exploitable security issue, lost or duplicated jobs at scale, data corruption, or broad outage.
- **High:** concrete production regression, public API break, or material queue-processing failure.
- **Medium:** bounded but reachable correctness, reliability, compatibility, or meaningful performance defect.
- **Low:** style, cleanup, optional hardening, or preference; do not post these comments.
