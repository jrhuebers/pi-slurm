import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync, chmodSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import slurm from "../extensions/index.ts";

function trackedJob(id, state = "PENDING") {
	return {
		id,
		name: `test-${id}`,
		command: "true",
		workingDirectory: "/tmp",
		logPath: `/tmp/test-${id}.out`,
		submittedAt: new Date().toISOString(),
		lastState: state,
		reminderSent: false,
	};
}

async function withMonitor(job, squeueOutput, sacctOutput, verify) {
	const dir = mkdtempSync(join(tmpdir(), "pi-slurm-test-"));
	const oldPath = process.env.PATH;
	const oldSetInterval = globalThis.setInterval;
	const oldClearInterval = globalThis.clearInterval;
	let poll;
	for (const [name, output] of [["squeue", squeueOutput], ["sacct", sacctOutput]]) {
		const file = join(dir, name);
		writeFileSync(file, `#!/bin/sh\nprintf '%s\\n' '${output}'\n`);
		chmodSync(file, 0o700);
	}
	process.env.PATH = `${dir}:${oldPath}`;
	globalThis.setInterval = (callback) => { poll = callback; return 1; };
	globalThis.clearInterval = () => {};
	const handlers = new Map();
	const notifications = [];
	const finished = [];
	let settling = false;
	const pi = {
		on: (event, handler) => { handlers.set(event, handler); },
		events: { emit: (event, payload) => { if (event === "slurm:finished") finished.push(payload); } },
		registerTool: () => {},
		appendEntry: () => {},
		sendMessage: (message, options) => {
			// During agent_settled Pi's outer session is idle, but its low-level
			// agent still owns the run. Starting a prompt here reproduces the error.
			if (settling && options.triggerTurn) throw new Error("Agent is already processing a prompt");
			assert.deepEqual(options, { triggerTurn: true, deliverAs: "steer" });
			notifications.push(message);
		},
	};
	try {
		slurm(pi);
		await handlers.get("session_start")({}, {
			sessionManager: {
				getEntries: () => [{ type: "custom", customType: "pi-research-engineer-slurm-state", data: { jobs: [job] } }],
			},
		});
		await verify({ poll: () => poll(), handlers, notifications, finished, setSettling: (value) => { settling = value; } });
	} finally {
		handlers.get("session_shutdown")?.();
		globalThis.setInterval = oldSetInterval;
		globalThis.clearInterval = oldClearInterval;
		process.env.PATH = oldPath;
		rmSync(dir, { recursive: true, force: true });
	}
}

test("a running-job notification is steered during an active run, not flushed from agent_settled", async () => {
	await withMonitor(trackedJob("12345"), "RUNNING|00:01|node", "", async ({ poll, handlers, notifications, setSettling }) => {
		await handlers.get("agent_start")?.();
		poll();
		setSettling(true);
		await handlers.get("agent_settled")?.();
		assert.equal(notifications.length, 1);
		assert.match(notifications[0].content, /12345.*started/);
	});
});

test("completion wakes sleep listeners and steers exactly one notification", async () => {
	await withMonitor({ ...trackedJob("12346", "RUNNING"), startedNotified: true }, "", "COMPLETED|0:0|00:00:05", async ({ poll, handlers, notifications, finished }) => {
		await handlers.get("agent_start")?.();
		poll();
		poll();
		assert.equal(finished.length, 1);
		assert.equal(finished[0].id, "12346");
		assert.equal(notifications.length, 1);
		assert.match(notifications[0].content, /12346.*completed/);
	});
});
