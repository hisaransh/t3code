import {
  CommandId,
  MessageId,
  TurnId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  ProjectId,
  ThreadId,
  type OrchestrationCommand,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";

import { decideOrchestrationCommand } from "./decider.ts";
import { createEmptyReadModel, projectEvent } from "./projector.ts";

const asCommandId = (value: string): CommandId => CommandId.make(value);
const asEventId = (value: string): EventId => EventId.make(value);
const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asThreadId = (value: string): ThreadId => ThreadId.make(value);

const seedReadModel = Effect.gen(function* () {
  const now = "2026-01-01T00:00:00.000Z";
  const initial = createEmptyReadModel(now);
  const withProject = yield* projectEvent(initial, {
    sequence: 1,
    eventId: asEventId("evt-project-create"),
    aggregateKind: "project",
    aggregateId: asProjectId("project-delete"),
    type: "project.created",
    occurredAt: now,
    commandId: asCommandId("cmd-project-create"),
    causationEventId: null,
    correlationId: asCommandId("cmd-project-create"),
    metadata: {},
    payload: {
      projectId: asProjectId("project-delete"),
      title: "Project Delete",
      workspaceRoot: "/tmp/project-delete",
      defaultModelSelection: null,
      scripts: [],
      createdAt: now,
      updatedAt: now,
    },
  });

  const withFirstThread = yield* projectEvent(withProject, {
    sequence: 2,
    eventId: asEventId("evt-thread-create-1"),
    aggregateKind: "thread",
    aggregateId: asThreadId("thread-delete-1"),
    type: "thread.created",
    occurredAt: now,
    commandId: asCommandId("cmd-thread-create-1"),
    causationEventId: null,
    correlationId: asCommandId("cmd-thread-create-1"),
    metadata: {},
    payload: {
      threadId: asThreadId("thread-delete-1"),
      projectId: asProjectId("project-delete"),
      title: "Thread Delete 1",
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      runtimeMode: "approval-required",
      branch: null,
      worktreePath: null,
      createdAt: now,
      updatedAt: now,
    },
  });

  return yield* projectEvent(withFirstThread, {
    sequence: 3,
    eventId: asEventId("evt-thread-create-2"),
    aggregateKind: "thread",
    aggregateId: asThreadId("thread-delete-2"),
    type: "thread.created",
    occurredAt: now,
    commandId: asCommandId("cmd-thread-create-2"),
    causationEventId: null,
    correlationId: asCommandId("cmd-thread-create-2"),
    metadata: {},
    payload: {
      threadId: asThreadId("thread-delete-2"),
      projectId: asProjectId("project-delete"),
      title: "Thread Delete 2",
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      runtimeMode: "approval-required",
      branch: null,
      worktreePath: null,
      createdAt: now,
      updatedAt: now,
    },
  });
});

it.layer(NodeServices.layer)("steering admission", (it) => {
  for (const status of ["ready", "running"] as const) {
    it.effect(`preserves ${status} send semantics`, () =>
      Effect.gen(function* () {
        const base = yield* seedReadModel;
        const threadId = asThreadId("thread-delete-1");
        const activeTurnId = TurnId.make("active-turn");
        const readModel = {
          ...base,
          threads: base.threads.map((thread) =>
            thread.id !== threadId
              ? thread
              : {
                  ...thread,
                  session: {
                    threadId,
                    status,
                    providerName: "codex",
                    runtimeMode: thread.runtimeMode,
                    activeTurnId: status === "running" ? activeTurnId : null,
                    lastError: null,
                    updatedAt: thread.updatedAt,
                  },
                },
          ),
        };
        const command: OrchestrationCommand = {
          type: "thread.turn.start",
          commandId: asCommandId("steer"),
          threadId,
          message: {
            messageId: MessageId.make("instruction"),
            role: "user",
            text: "Keep the API",
            attachments: [],
          },
          runtimeMode: "approval-required",
          interactionMode: "default",
          createdAt: "2026-01-01T00:00:01.000Z",
        };
        const result = yield* decideOrchestrationCommand({ readModel, command });
        const events = Array.isArray(result) ? result : [result];
        const request = events.find((event) => event.type === "thread.turn-start-requested");
        const message = events.find((event) => event.type === "thread.message-sent");
        expect(request?.payload.expectedTurnId).toBe(
          status === "running" ? activeTurnId : undefined,
        );
        expect(message?.payload.turnId).toBe(status === "running" ? activeTurnId : null);
        expect(events.some((event) => event.type === "thread.turn-interrupt-requested")).toBe(
          false,
        );
        if (status === "ready") {
          const failure = yield* decideOrchestrationCommand({
            readModel,
            command: { ...command, expectedTurnId: activeTurnId },
          }).pipe(Effect.flip);
          expect(String(failure)).toContain("completed or changed");
        } else {
          const failure = yield* decideOrchestrationCommand({
            readModel,
            command: {
              ...command,
              threadId: asThreadId("thread-delete-2"),
              expectedTurnId: activeTurnId,
            },
          }).pipe(Effect.flip);
          expect(String(failure)).toContain("completed or changed");
        }
      }),
    );
  }
});
