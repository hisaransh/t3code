import { GitCommandError, type VcsRemoveWorktreeInput } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import { ProviderService } from "../provider/Services/ProviderService.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { TerminalManager } from "../terminal/Manager.ts";
import { GitWorkflowService } from "./GitWorkflowService.ts";
import { tryWorktreeCleanup } from "./worktreeLifecycleGate.ts";

export function findWorktreeUseReason(input: {
  readonly activeThreadPaths: ReadonlyArray<string>;
  readonly providerSessionPaths: ReadonlyArray<string | undefined>;
  readonly terminalPaths: ReadonlyArray<string>;
  readonly isWithinTarget: (path: string) => boolean;
}): string | null {
  if (input.activeThreadPaths.some(input.isWithinTarget)) {
    return "Cleanup skipped: an active thread uses this worktree. Archive it first.";
  }
  if (input.providerSessionPaths.some((path) => path === undefined || input.isWithinTarget(path))) {
    return "Cleanup skipped: a provider session still uses this worktree.";
  }
  if (input.terminalPaths.some(input.isWithinTarget)) {
    return "Cleanup skipped: a terminal still uses this worktree.";
  }
  return null;
}

/** Shared by policy-driven removal and the safe manual action. */
export const cleanupWorktree = Effect.fn("cleanupWorktree")(function* (
  input: VcsRemoveWorktreeInput,
) {
  const reject = (detail: string) =>
    new GitCommandError({
      operation: "worktreeCleanup",
      command: "git worktree remove",
      cwd: input.cwd,
      detail,
    });
  yield* Effect.logDebug("worktree cleanup attempted", { path: input.path });
  const result = yield* tryWorktreeCleanup(
    input.path,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const canonicalize = (value: string) =>
        fs.realPath(value).pipe(Effect.orElseSucceed(() => path.resolve(value)));
      const target = yield* canonicalize(input.path);
      const withinTarget = (value: string) => {
        const relative = path.relative(target, value);
        return (
          relative === "" ||
          (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative))
        );
      };
      const query = yield* ProjectionSnapshotQuery;
      const snapshot = yield* query
        .getCommandReadModel()
        .pipe(
          Effect.mapError(() => reject("Cleanup skipped: thread state could not be verified.")),
        );
      const activeThreadPaths: string[] = [];
      for (const thread of snapshot.threads) {
        if (thread.deletedAt !== null || thread.archivedAt !== null) continue;
        const project = snapshot.projects.find((project) => project.id === thread.projectId);
        const cwd = thread.worktreePath ?? project?.workspaceRoot;
        if (cwd) activeThreadPaths.push(yield* canonicalize(cwd));
      }
      const providers = yield* ProviderService;
      const providerSessionPaths: Array<string | undefined> = [];
      for (const session of yield* providers.listSessions()) {
        providerSessionPaths.push(session.cwd ? yield* canonicalize(session.cwd) : undefined);
      }
      const terminals = yield* TerminalManager;
      const terminalPaths: string[] = [];
      const unsubscribe = yield* terminals.subscribeMetadata((event) =>
        Effect.gen(function* () {
          if (event.type !== "snapshot") return;
          for (const terminal of event.terminals) {
            if (terminal.pid !== null) terminalPaths.push(yield* canonicalize(terminal.cwd));
          }
        }),
      );
      unsubscribe();
      const useReason = findWorktreeUseReason({
        activeThreadPaths,
        providerSessionPaths,
        terminalPaths,
        isWithinTarget: withinTarget,
      });
      if (useReason) {
        yield* Effect.logDebug("worktree cleanup skipped", { path: input.path, reason: useReason });
        return yield* reject(useReason);
      }
      const git = yield* GitWorkflowService;
      yield* git
        .removeWorktree({
          ...input,
          force: false,
          cleanup: input.cleanup ?? { mergedOnly: false, deleteBranch: false },
        })
        .pipe(
          Effect.tapError((error) =>
            Effect.logDebug("worktree cleanup failed", { path: input.path, reason: error.message }),
          ),
        );
      yield* Effect.logDebug("worktree cleanup completed", { path: input.path });
    }),
  );
  if (Option.isNone(result)) {
    yield* Effect.logDebug("worktree cleanup skipped", {
      path: input.path,
      reason: "runtime activity or another cleanup is in progress",
    });
    return yield* reject(
      "Cleanup skipped: runtime admission, delivery, shutdown, or another cleanup is in progress.",
    );
  }
  return;
});
