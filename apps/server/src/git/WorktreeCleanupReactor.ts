import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderService } from "../provider/Services/ProviderService.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import { forkParked } from "../serverActivation.ts";
import { TerminalManager } from "../terminal/Manager.ts";
import { cleanupWorktree } from "./worktreeCleanup.ts";

// Re-evaluate inactive worktrees after lifecycle and pull-request events. The
// startup sweep covers merges that happened while the server was offline.
export const WorktreeCleanupReactorLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const settings = yield* ServerSettingsService;
    const query = yield* ProjectionSnapshotQuery;
    const engine = yield* OrchestrationEngineService;
    const providers = yield* ProviderService;
    const terminals = yield* TerminalManager;
    const events = yield* engine.subscribeDomainEvents;
    const sweep = (threadId?: string) =>
      Effect.gen(function* () {
        const policy = (yield* settings.getSettings).worktreeCleanup;
        if (policy.mode === "never") return;
        const snapshot = yield* query.getCommandReadModel();
        const visited = new Set<string>();
        for (const thread of snapshot.threads) {
          if (threadId !== undefined && thread.id !== threadId) continue;
          if (!thread.worktreePath || (thread.deletedAt === null && thread.archivedAt === null))
            continue;
          if (visited.has(thread.worktreePath)) continue;
          visited.add(thread.worktreePath);
          const project = snapshot.projects.find((project) => project.id === thread.projectId);
          if (!project) continue;
          yield* cleanupWorktree({
            cwd: project.workspaceRoot,
            path: thread.worktreePath,
            cleanup: { mergedOnly: policy.mode === "merged", deleteBranch: policy.deleteBranch },
          }).pipe(
            Effect.catch((error) =>
              Effect.logDebug("worktree cleanup skipped or failed", {
                threadId: thread.id,
                reason: error.message,
              }),
            ),
          );
        }
      }).pipe(
        Effect.catch(() =>
          Effect.logDebug("worktree cleanup skipped: policy or thread state unavailable"),
        ),
      );
    yield* forkParked(sweep());
    yield* forkParked(
      Stream.runForEach(events, (event) => {
        switch (event.type) {
          case "thread.archived":
          case "thread.deleted":
          case "thread.pull-request-linked":
          case "thread.pull-request-synced":
            return sweep(event.payload.threadId);
          case "thread.session-set":
            return event.payload.session.status === "stopped"
              ? sweep(event.payload.threadId)
              : Effect.void;
          default:
            return Effect.void;
        }
      }),
    );
    yield* forkParked(
      Stream.runForEach(providers.streamEvents, (event) =>
        event.type === "session.exited" ? sweep(event.threadId) : Effect.void,
      ),
    );
    const terminalCleanupThreadIds = yield* Queue.unbounded<string>();
    const unsubscribeTerminals = yield* terminals.subscribeMetadata((event) => {
      if (event.type === "remove")
        return Queue.offer(terminalCleanupThreadIds, event.threadId).pipe(Effect.asVoid);
      if (event.type === "upsert" && event.terminal.pid === null) {
        return Queue.offer(terminalCleanupThreadIds, event.terminal.threadId).pipe(Effect.asVoid);
      }
      return Effect.void;
    });
    yield* Effect.addFinalizer(() =>
      Effect.sync(unsubscribeTerminals).pipe(
        Effect.andThen(Queue.shutdown(terminalCleanupThreadIds)),
      ),
    );
    yield* forkParked(
      Stream.fromQueue(terminalCleanupThreadIds).pipe(
        Stream.runForEach((threadId) => sweep(threadId)),
      ),
    );
    yield* forkParked(
      settings.streamChanges.pipe(
        Stream.map((next) => next.worktreeCleanup),
        Stream.changesWith(
          (previous, next) =>
            previous.mode === next.mode && previous.deleteBranch === next.deleteBranch,
        ),
        Stream.runForEach(() => sweep()),
      ),
    );
  }),
);
