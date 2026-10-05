# pi-slurm

Pi extension providing session-persistent Slurm submission, monitoring, inspection, and cancellation tools.

## Tools

- `slurm_submit`
- `slurm_jobs`
- `slurm_cancel`

Jobs are persisted in the Pi session and state changes generate context notifications. Notifications steer an active agent run rather than starting another prompt during `agent_settled`. The extension runs Slurm commands on the local host and stores logs under `slurm-logs/` in the agent's run directory by default; set `PI_RESEARCH_SLURM_LOG_DIR` to override the log directory.

`slurm_submit` sets the Slurm job comment to the basename of the agent's working directory by default: a submission from `/path/to/lyapunov` gets `--comment lyapunov`. Pass `comment` to override it (including an empty string to leave the comment blank). The comment is independent of the job name.

## Optional integrations

The extension emits `slurm:finished` on Pi's event bus when a tracked job reaches
a terminal state. Its payload is `{ id, name, logPath, status, detail }`.
`pi-sleep` uses this event to interrupt a sleep watching that Slurm job.

`slurm_submit` accepts an optional `qos`. When it is omitted, the extension checks the current account's Slurm associations and selects a QoS matching the requested partition (for example, `cpu` for the `cpu` partition). Use `PI_RESEARCH_SLURM_QOS` to set a site-specific default when accounting queries are unavailable.
