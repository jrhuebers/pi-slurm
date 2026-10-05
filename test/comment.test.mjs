import assert from "node:assert/strict";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import slurm from "../extensions/index.ts";

test("submission defaults the comment to ctx.cwd's basename and allows overrides", async () => {
	const dir = mkdtempSync(join(tmpdir(), "pi-slurm-comment-"));
	const bin = join(dir, "bin");
	const argsFile = join(bin, "args");
	const oldPath = process.env.PATH;
	const handlers = new Map();
	const tools = new Map();
	let persisted;
	mkdirSync(bin);
	for (const [name, script] of [
		["sbatch", 'if [ "$1" = "--version" ]; then echo "slurm mock"; else printf "%s\\n" "$@" > "$(dirname "$0")/args"; echo 12345; fi'],
		["scontrol", 'echo "PartitionName=cpu MaxTime=1-00:00:00"'],
	]) {
		const file = join(bin, name);
		writeFileSync(file, `#!/bin/sh\n${script}\n`);
		chmodSync(file, 0o700);
	}
	process.env.PATH = `${bin}:${oldPath}`;
	try {
		slurm({
			on: (event, handler) => handlers.set(event, handler),
			registerTool: (tool) => tools.set(tool.name, tool),
			appendEntry: (_type, data) => { persisted = data; },
		});
		const submit = tools.get("slurm_submit");
		for (const [directory, override, expected] of [
			["lyapunov", undefined, "lyapunov"],
			["project with spaces/", undefined, "project with spaces"],
			["another-project", "custom comment", "custom comment"],
			["another-project", "", ""],
		]) {
			const cwd = `${dir}/${directory}`;
			await submit.execute("test", {
				command: "true", partition: "cpu", qos: "cpu", comment: override,
			}, undefined, undefined, { cwd });
			const args = readFileSync(argsFile, "utf8").split("\n");
			assert.equal(args.filter((arg) => arg === "--comment").length, 1);
			assert.equal(args[args.indexOf("--comment") + 1], expected);
			assert.equal(args[args.indexOf("--chdir") + 1], cwd);
			assert.equal(persisted.jobs.find((job) => job.id === "12345").comment, expected);
		}
	} finally {
		handlers.get("session_shutdown")?.();
		process.env.PATH = oldPath;
		rmSync(dir, { recursive: true, force: true });
	}
});
