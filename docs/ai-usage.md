# How I used AI coding agents

This project was built with two AI coding agents as pair programmers: Claude Code and Google
Antigravity. I set the direction and made the decisions. The agents drafted code, tests, and
docs inside guardrails written in [`AGENTS.md`](../AGENTS.md): privacy rules, cost limits, and
"never run `terraform apply`". Both agents read the same file (Claude Code through
`CLAUDE.md`), so the rules did not depend on which agent was working.

## Process

- **Instructions were reviewed before any code was written.** `AGENTS.md` (then `CLAUDE.md`) went through three
  review rounds. In each round, independent agents simulated tasks using only the file,
  fact-checked its technical claims against official docs, and scored the likely result
  against the project goals. I made the scope calls between rounds.
- **Privacy rules are enforced by tests, not by trust.** For example, the ingest tests plant
  canary strings in ZDR records and assert that they never reach any output, DLP request, or
  log line. I also confirmed that the test fails when the ZDR strip is disabled.

## Where I overrode or corrected the AI

Each line is a proposal that I rejected or rewrote.

- Claude recommended dropping the live Composer deployment to fit the timeline. I kept
  Composer, since running Airflow on GCP is core to the project, and cut Dataplex, the MCP agent,
  and extra dashboards instead.
- Claude recommended a $100/month budget. I kept $50 and destroy Composer after each demo
  window.
- Claude proposed human-approved commits so the history would show review. I kept automatic
  small commits and document review here instead.
- Claude planned a `RUN_MODE=local` path that runs the DAG under `airflow standalone` on my Mac.
  I dropped it: the DAG runs only on Composer, and locally it gets import and wiring tests.
