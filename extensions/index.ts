/**
 * Lightweight, session-persistent Slurm submission and monitoring for Pi.
 * It deliberately has no knowledge of experiments, run directories, W&B, or
 * other agents. Jobs submitted through this tool are kept in the Pi session
 * and their state changes are delivered as normal context messages.
 */

import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import { Type } from "typebox";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";

const STATE_ENTRY = "pi-research-engineer-slurm-state";
const MESSAGE_TYPE = "pi-research-engineer-slurm";
const POLL_INTERVAL_MS = 5_000;
function logRoot(): string {
	// A project-local default remains visible from a compute node on clusters
	// where /tmp is node-local. Override when a different shared location is
	// appropriate for the site.
	return process.env.PI_RESEARCH_SLURM_LOG_DIR
		?? path.join(process.cwd(), ".pi-research-engineer", "slurm");
}

type JobState = "PENDING" | "RUNNING" | "COMPLETED" | "FAILED" | "CANCELLED" | "TIMEOUT" | "UNKNOWN";

type TrackedJob = {
	id: string;
	name: string;
	command: string;
	workingDirectory: string;
	partition?: string;
	logPath: string;
	submittedAt: string;
	startedAt?: string;
	lastState: JobState;
	notifyAfterMinutes?: number;
	reminderSent: boolean;
};

type SlurmObservation = {
	state: JobState;
	detail: string;
	terminal: boolean;
};

let jobs = new Map<string, TrackedJob>();
let monitoring = false;

function run(command: string, args: string[], timeout = 10_000): string {
	return execFileSync(command, args, {
		encoding: "utf8",
		timeout,
		maxBuffer: 1024 * 1024,
	}).trim();
}

function hasSlurm(): boolean {
	try {
		run("sbatch", ["--version"]);
		return true;
	} catch {
		return false;
	}
}

function shellQuote(value: string): string {
	return "'" + value.replaceAll("'", "'\"'\"'") + "'";
}

function normaliseState(value: string): JobState {
	const state = value.trim().toUpperCase().split(/[ +]/, 1)[0] ?? "UNKNOWN";
	if (state === "PENDING" || state === "CONFIGURING") return "PENDING";
	if (state === "RUNNING" || state === "COMPLETING") return "RUNNING";
	if (state === "COMPLETED") return "COMPLETED";
	if (state === "CANCELLED") return "CANCELLED";
	if (state === "TIMEOUT") return "TIMEOUT";
	if (state === "FAILED" || state === "OUT_OF_MEMORY" || state === "NODE_FAIL" || state === "PREEMPTED" || state === "BOOT_FAIL") return "FAILED";
	return "UNKNOWN";
}

function isTerminal(state: JobState): boolean {
	return state === "COMPLETED" || state === "FAILED" || state === "CANCELLED" || state === "TIMEOUT";
}

function partitionMaxTime(partition: string): string {
	const output = run("scontrol", ["show", "partition", partition, "--oneliner"]);
	const match = output.match(/\bMaxTime=(\S+)/);
	if (!match || match[1] === "INFINITE") {
		throw new Error(`could not resolve a finite MaxTime for Slurm partition ${JSON.stringify(partition)}`);
	}
	return match[1];
}

function defaultPartition(): string {
	const output = run("sinfo", ["--noheader", "--format=%P"]);
	const candidates = output
		.split("\n")
		.map((line) => line.trim().replace(/\*$/, ""))
		.filter(Boolean);
	const defaultCandidate = output
		.split("\n")
		.map((line) => line.trim())
		.find((line) => line.endsWith("*"));
	if (defaultCandidate) return defaultCandidate.slice(0, -1);
	if (candidates.length === 1) return candidates[0];
	throw new Error("no unique default Slurm partition; pass partition explicitly");
}

function observe(jobId: string): SlurmObservation {
	try {
		const queue = run("squeue", ["--noheader", "--jobs", jobId, "--format=%T|%M|%R"]);
		if (queue) {
			const [rawState, elapsed = "-", reason = "-"] = queue.split("|", 3);
			const state = normaliseState(rawState);
			return { state, detail: `elapsed=${elapsed}; ${reason}`, terminal: false };
		}
	} catch {
		// A job that left squeue is queried through accounting below.
	}
	try {
		const accounting = run("sacct", ["--noheader", "--allocations", "--jobs", jobId, "--format=State,ExitCode,Elapsed", "--parsable2"]);
		const line = accounting.split("\n").find(Boolean);
		if (line) {
			const [rawState, exitCode = "-", elapsed = "-"] = line.split("|", 3);
			const state = normaliseState(rawState);
			return { state, detail: `exit=${exitCode}; elapsed=${elapsed}`, terminal: isTerminal(state) };
		}
	} catch {
		// Scheduler/accounting may be temporarily unavailable.
	}
	return { state: "UNKNOWN", detail: "not currently visible in squeue or sacct", terminal: false };
}

function persist(pi: ExtensionAPI): void {
	pi.appendEntry(STATE_ENTRY, { jobs: [...jobs.values()] });
}

function restore(ctx: ExtensionContext): void {
	const entries = ctx.sessionManager.getEntries() as Array<{
		type?: string;
		customType?: string;
		data?: unknown;
	}>;
	const latest = [...entries]
		.reverse()
		.find((entry) => entry.type === "custom" && entry.customType === STATE_ENTRY)?.data;
	if (!latest || !Array.isArray((latest as { jobs?: unknown }).jobs)) return;
	jobs = new Map(
		(latest as { jobs: unknown[] }).jobs
			.filter((value): value is TrackedJob => typeof value === "object" && value !== null && typeof (value as TrackedJob).id === "string")
			.map((job) => [job.id, job]),
	);
}

function notify(pi: ExtensionAPI, content: string, details: Record<string, string> = {}): void {
	pi.sendMessage(
		{ customType: MESSAGE_TYPE, content, display: true, details },
		{ triggerTurn: true, deliverAs: "followUp" },
	);
}

function startMonitor(pi: ExtensionAPI): void {
	if (monitoring) return;
	monitoring = true;
	setInterval(() => {
		let changed = false;
		for (const job of jobs.values()) {
			if (isTerminal(job.lastState)) continue;
			const observation = observe(job.id);
			if (observation.state !== job.lastState && observation.state !== "UNKNOWN") {
				const previous = job.lastState;
				job.lastState = observation.state;
				changed = true;
				if (observation.state === "RUNNING" && previous === "PENDING") {
					job.startedAt = new Date().toISOString();
					notify(pi, `[SLURM] ${job.name} (${job.id}) started. Log: ${job.logPath}`, { jobId: job.id, status: "RUNNING" });
				} else if (observation.terminal) {
					notify(pi, `[SLURM] ${job.name} (${job.id}) ${observation.state.toLowerCase()}: ${observation.detail}. Log: ${job.logPath}`, { jobId: job.id, status: observation.state });
				}
			}

			if (!job.reminderSent && job.notifyAfterMinutes && job.lastState === "RUNNING") {
				const startedAt = Date.parse(job.startedAt ?? job.submittedAt);
				if (Number.isFinite(startedAt) && Date.now() - startedAt >= job.notifyAfterMinutes * 60_000) {
					job.reminderSent = true;
					changed = true;
					notify(pi, `[SLURM] ${job.name} (${job.id}) has been running; inspect its log if useful: ${job.logPath}`, { jobId: job.id, status: "RUNNING" });
				}
			}
		}
		if (changed) persist(pi);
	}, POLL_INTERVAL_MS);
}

export default function slurm(pi: ExtensionAPI): void {
	pi.on("session_start", async (_event, ctx) => {
		restore(ctx);
		startMonitor(pi);
	});

	pi.registerTool({
		name: "slurm_submit",
		label: "Submit Slurm Job",
		description: "Submit and track a Slurm job. Uses the selected partition's maximum walltime and sends start/completion notifications.",
		promptSnippet: "slurm_submit(command, partition?, gpus?, cpus?, mem?, name?, notify_after_minutes?) - submit tracked Slurm work",
		promptGuidelines: [
			"Use slurm_submit for new Slurm work so completion is reported without a polling loop.",
			"slurm_submit automatically requests the selected partition's maximum walltime; do not use shell sbatch or srun for tracked work.",
		],
		parameters: Type.Object({
			command: Type.String({ description: "Shell command to run on the allocated node." }),
			partition: Type.Optional(Type.String({ description: "Slurm partition. Required if no unique default exists." })),
			gpus: Type.Optional(Type.String({ description: "GRES request, for example gpu:1." })),
			cpus: Type.Optional(Type.String({ description: "CPUs per task, for example 8." })),
			mem: Type.Optional(Type.String({ description: "Memory request, for example 64G." })),
			name: Type.Optional(Type.String({ description: "Human-readable job name." })),
			notify_after_minutes: Type.Optional(Type.Number({ minimum: 1, description: "Optional one-off reminder after this many minutes." })),
		}),
		async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
			if (!hasSlurm()) throw new Error("Slurm is unavailable: sbatch was not found or did not respond.");
			const partition = params.partition ?? defaultPartition();
			const maxTime = partitionMaxTime(partition);
			const logs = logRoot();
			fs.mkdirSync(logs, { recursive: true });
			const name = params.name?.trim() || "pi-research";
			const logPath = path.join(logs, `${name}-%j.out`);
			const args = [
				"--parsable",
				"--job-name", name,
				"--partition", partition,
				"--time", maxTime,
				"--output", logPath,
				"--chdir", process.cwd(),
				...(params.gpus ? ["--gres", params.gpus] : []),
				...(params.cpus ? ["--cpus-per-task", params.cpus] : []),
				...(params.mem ? ["--mem", params.mem] : []),
				"--wrap", `exec bash -lc ${shellQuote(params.command)}`,
			];
			const submitted = run("sbatch", args, 60_000);
			const jobId = submitted.split(";", 1)[0]?.trim();
			if (!jobId || !/^\d+(?:_\d+)?$/.test(jobId)) throw new Error(`unexpected sbatch response: ${submitted}`);
			const job: TrackedJob = {
				id: jobId,
				name,
				command: params.command,
				workingDirectory: process.cwd(),
				partition,
				logPath: logPath.replace("%j", jobId),
				submittedAt: new Date().toISOString(),
				lastState: "PENDING",
				notifyAfterMinutes: params.notify_after_minutes,
				reminderSent: false,
			};
			jobs.set(job.id, job);
			persist(pi);
			startMonitor(pi);
			return {
				content: [{ type: "text", text: `Submitted ${name} as Slurm job ${job.id}. Partition: ${partition}; walltime: ${maxTime}; log: ${job.logPath}` }],
				details: {},
			};
		},
	});

	pi.registerTool({
		name: "slurm_jobs",
		label: "Inspect Slurm Jobs",
		description: "Inspect tracked Slurm jobs, or all jobs belonging to the current Unix account.",
		promptSnippet: "slurm_jobs(show_all?, include_completed?) - inspect tracked Slurm work",
		promptGuidelines: [
			"Use slurm_jobs to diagnose a notification or answer a concrete status question, not as a routine wait loop.",
		],
		parameters: Type.Object({
			show_all: Type.Optional(Type.Boolean({ description: "Show all jobs for this Unix account, rather than this Pi session's tracked jobs." })),
			include_completed: Type.Optional(Type.Boolean({ description: "Include recent sacct history." })),
		}),
		async execute(_toolCallId, params) {
			if (!hasSlurm()) throw new Error("Slurm is unavailable: squeue/sbatch was not found or did not respond.");
			const ids = params.show_all ? [] : [...jobs.keys()];
			const queueArgs = ["--me", "--noheader", "--format=%.18i|%.12T|%.16P|%.32j|%.12M|%.12l|%.8D|%.24R"];
			const queue = run("squeue", queueArgs);
			const current = queue
				.split("\n")
				.filter(Boolean)
				.filter((line) => params.show_all || ids.includes(line.split("|", 1)[0]?.split(".", 1)[0] ?? ""));
			const sections = [
				"Current jobs (job_id|state|partition|name|elapsed|time_limit|nodes|reason):",
				current.join("\n") || "(none)",
			];
			if (params.include_completed) {
				const history = run("sacct", ["--noheader", "--allocations", "--starttime", "now-1day", "--format=JobIDRaw,State,ExitCode,Elapsed,JobName", "--parsable2"])
					.split("\n")
					.filter(Boolean)
					.filter((line) => params.show_all || ids.includes(line.split("|", 1)[0] ?? ""));
				sections.push("Recent completed jobs (job_id|state|exit_code|elapsed|name):", history.join("\n") || "(none)");
			}
			return { content: [{ type: "text", text: sections.join("\n") }], details: {} };
		},
	});

	pi.registerTool({
		name: "slurm_cancel",
		label: "Cancel Slurm Job",
		description: "Cancel one Slurm job after confirming it is obsolete, invalid, or wedged.",
		promptSnippet: "slurm_cancel(job_id, reason) - cancel obsolete Slurm work",
		promptGuidelines: [
			"Use slurm_cancel only for obsolete, invalid, or clearly wedged work; do not cancel healthy work merely because it is long-running.",
		],
		parameters: Type.Object({
			job_id: Type.String({ description: "Slurm job ID to cancel." }),
			reason: Type.String({ description: "Why cancelling is appropriate." }),
		}),
		async execute(_toolCallId, params) {
			run("scancel", [params.job_id]);
			const job = jobs.get(params.job_id);
			if (job) {
				persist(pi);
			}
			return {
				content: [{ type: "text", text: `Cancellation requested for Slurm job ${params.job_id}: ${params.reason}` }],
				details: {},
			};
		},
	});
}
