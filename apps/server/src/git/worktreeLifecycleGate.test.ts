import { expect } from "vite-plus/test";
import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";

import { tryWorktreeCleanup, withWorktreeActivity } from "./worktreeLifecycleGate.ts";

it.effect("rejects cleanup while matching worktree activity is running", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const activity = yield* withWorktreeActivity(
      "/repo/worktree",
      Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))),
    ).pipe(Effect.forkChild);
    yield* Deferred.await(started);

    const cleanup = yield* tryWorktreeCleanup("/repo/worktree", Effect.succeed("removed"));

    expect(Option.isNone(cleanup)).toBe(true);
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(activity);
  }),
);

it.effect("allows unrelated worktrees to make progress independently", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const activity = yield* withWorktreeActivity(
      "/repo/first",
      Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))),
    ).pipe(Effect.forkChild);
    yield* Deferred.await(started);

    const cleanup = yield* tryWorktreeCleanup("/repo/second", Effect.succeed("removed"));

    expect(Option.getOrUndefined(cleanup)).toBe("removed");
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(activity);
  }),
);

it.effect("holds new matching activity until admitted cleanup completes", () =>
  Effect.gen(function* () {
    const cleanupStarted = yield* Deferred.make<void>();
    const releaseCleanup = yield* Deferred.make<void>();
    const activityStarted = yield* Deferred.make<void>();
    const cleanup = yield* tryWorktreeCleanup(
      "/repo/worktree",
      Deferred.succeed(cleanupStarted, undefined).pipe(
        Effect.andThen(Deferred.await(releaseCleanup)),
        Effect.as("removed"),
      ),
    ).pipe(Effect.forkChild);
    yield* Deferred.await(cleanupStarted);
    const activity = yield* withWorktreeActivity(
      "/repo/worktree/subdir",
      Deferred.succeed(activityStarted, undefined),
    ).pipe(Effect.forkChild);
    yield* Effect.yieldNow;

    expect(yield* Deferred.isDone(activityStarted)).toBe(false);
    yield* Deferred.succeed(releaseCleanup, undefined);
    expect(Option.getOrUndefined(yield* Fiber.join(cleanup))).toBe("removed");
    yield* Fiber.join(activity);
    expect(yield* Deferred.isDone(activityStarted)).toBe(true);
  }),
);
