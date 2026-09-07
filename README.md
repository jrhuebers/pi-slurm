# pi-slurm

Pi extension providing session-persistent Slurm submission, monitoring, inspection, and cancellation tools.

## Tools

- `slurm_submit`
- `slurm_jobs`
- `slurm_cancel`

Jobs are persisted in the Pi session and state changes generate context notifications. The extension runs Slurm commands on the local host and stores logs under `.pi-research-engineer/slurm` by default; set `PI_RESEARCH_SLURM_LOG_DIR` to override the log directory.
