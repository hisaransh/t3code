// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";

const UNKNOWN_PATH = "*";
const mutex = Semaphore.makeUnsafe(1);
const activePaths = new Map<string, number>();
const cleanupPaths = new Map<string, Deferred.Deferred<void>>();

const normalize = (path: string | undefined) => (path ? NodePath.resolve(path) : UNKNOWN_PATH);
const overlaps = (left: string, right: string) => {
  if (left === UNKNOWN_PATH || right === UNKNOWN_PATH) return true;
  const relative = NodePath.relative(left, right);
  const withinLeft =
    relative === "" ||
    (!relative.startsWith(`..${NodePath.sep}`) &&
      relative !== ".." &&
      !NodePath.isAbsolute(relative));
  if (withinLeft) return true;
  const inverse = NodePath.relative(right, left);
  return (
    inverse === "" ||
    (!inverse.startsWith(`..${NodePath.sep}`) && inverse !== ".." && !NodePath.isAbsolute(inverse))
  );
};

const acquireActivity = (path: string): Effect.Effect<void> =>
  mutex
    .withPermit(
      Effect.sync(() => {
        const cleanup = [...cleanupPaths].find(([cleanupPath]) => overlaps(cleanupPath, path));
        if (cleanup) return cleanup[1];
        activePaths.set(path, (activePaths.get(path) ?? 0) + 1);
        return null;
      }),
    )
    .pipe(
      Effect.flatMap((cleanup) =>
        cleanup ? Deferred.await(cleanup).pipe(Effect.andThen(acquireActivity(path))) : Effect.void,
      ),
    );

const releaseActivity = (path: string) =>
  mutex.withPermit(
    Effect.sync(() => {
      const next = (activePaths.get(path) ?? 1) - 1;
      if (next === 0) activePaths.delete(path);
      else activePaths.set(path, next);
    }),
  );

export const withWorktreeActivity = <A, E, R>(
  path: string | undefined,
  effect: Effect.Effect<A, E, R>,
) => {
  const normalized = normalize(path);
  return Effect.acquireUseRelease(
    acquireActivity(normalized),
    () => effect,
    () => releaseActivity(normalized),
  );
};

export const tryWorktreeCleanup = <A, E, R>(
  path: string,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<Option.Option<A>, E, R> =>
  Effect.gen(function* () {
    const normalized = normalize(path);
    const completed = yield* Deferred.make<void>();
    const admitted = yield* mutex.withPermit(
      Effect.sync(() => {
        if ([...activePaths.keys()].some((activePath) => overlaps(activePath, normalized))) {
          return false;
        }
        if ([...cleanupPaths.keys()].some((cleanupPath) => overlaps(cleanupPath, normalized))) {
          return false;
        }
        cleanupPaths.set(normalized, completed);
        return true;
      }),
    );
    if (!admitted) return Option.none<A>();
    return yield* effect.pipe(
      Effect.map(Option.some),
      Effect.ensuring(
        mutex
          .withPermit(
            Effect.sync(() => {
              cleanupPaths.delete(normalized);
            }),
          )
          .pipe(Effect.andThen(Deferred.succeed(completed, undefined))),
      ),
    );
  });
