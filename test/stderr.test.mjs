import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

// Run in a child process: intercepting process.stderr.write alone would miss
// execFileSync's native writes to the terminal file descriptor.
test("failed scheduler commands do not leak stderr into Pi's terminal", () => {
	const dir = mkdtempSync(join(tmpdir(), "pi-slurm-stderr-test-"));
	try {
		for (const name of ["squeue", "sacct", "sbatch"]) {
			const file = join(dir, name);
			writeFileSync(file, `#!/bin/sh\nprintf '${name}: MOCK scheduler error\\n' >&2\nexit 1\n`);
			chmodSync(file, 0o700);
		}
		const moduleUrl = new URL("../extensions/index.ts", import.meta.url).href;
		const script = `
			import assert from "node:assert/strict";
			import slurm from ${JSON.stringify(moduleUrl)};
			const handlers = new Map();
			const tools = new Map();
			let poll;
			globalThis.setInterval = callback => { poll = callback; return 1; };
			globalThis.clearInterval = () => {};
			slurm({
				on: (name, handler) => handlers.set(name, handler),
				registerTool: tool => tools.set(tool.name, tool),
				appendEntry: () => {},
				sendMessage: () => assert.fail("failed observations must not notify"),
				events: { emit: () => assert.fail("failed observations must not emit completion") },
			});
			await handlers.get("session_start")({}, {
				sessionManager: { getEntries: () => [{
					type: "custom", customType: "pi-research-engineer-slurm-state",
					data: { jobs: [{ id: "12345", lastState: "RUNNING" }] },
				}] },
			});
			poll();
			poll();
			// Tool failures should still retain the captured scheduler diagnostic.
			await assert.rejects(
				tools.get("slurm_cancel").execute("test", { job_id: "12345", reason: "test" }),
				/scancel: MOCK scheduler error/,
			);
			await assert.rejects(
				tools.get("slurm_submit").execute("test", {}, undefined, undefined, {}),
				/Slurm is unavailable/,
			);
			handlers.get("session_shutdown")();
		`;
		const cancel = join(dir, "scancel");
		writeFileSync(cancel, "#!/bin/sh\nprintf 'scancel: MOCK scheduler error\\n' >&2\nexit 1\n");
		chmodSync(cancel, 0o700);
		const result = spawnSync(process.execPath, [
			"--experimental-strip-types", "--disable-warning=ExperimentalWarning",
			"--input-type=module", "--eval", script,
		], {
			encoding: "utf8",
			env: { ...process.env, PATH: `${dir}:${process.env.PATH}` },
			timeout: 10_000,
		});
		assert.ifError(result.error);
		assert.equal(result.status, 0, result.stderr);
		assert.equal(result.stderr, "", "scheduler stderr must be captured, not written to the terminal");
		assert.equal(result.stdout, "");
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
});
