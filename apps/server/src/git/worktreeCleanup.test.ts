import { describe, expect, it } from "vite-plus/test";
import { findWorktreeUseReason } from "./worktreeCleanup.ts";

const inTarget = (path: string) => path === "/repo/worktree" || path.startsWith("/repo/worktree/");

describe("findWorktreeUseReason", () => {
  it("protects active threads, provider sessions, and terminals", () => {
    expect(
      findWorktreeUseReason({
        activeThreadPaths: ["/repo/worktree"],
        providerSessionPaths: [],
        terminalPaths: [],
        isWithinTarget: inTarget,
      }),
    ).toContain("active thread");
    expect(
      findWorktreeUseReason({
        activeThreadPaths: [],
        providerSessionPaths: ["/repo/worktree/subdir"],
        terminalPaths: [],
        isWithinTarget: inTarget,
      }),
    ).toContain("provider session");
    expect(
      findWorktreeUseReason({
        activeThreadPaths: [],
        providerSessionPaths: [],
        terminalPaths: ["/repo/worktree"],
        isWithinTarget: inTarget,
      }),
    ).toContain("terminal");
  });

  it("permits cleanup when every runtime belongs elsewhere", () => {
    expect(
      findWorktreeUseReason({
        activeThreadPaths: ["/repo/other"],
        providerSessionPaths: ["/repo/other"],
        terminalPaths: ["/repo/other"],
        isWithinTarget: inTarget,
      }),
    ).toBeNull();
  });

  it("fails closed when a provider session has no known cwd", () => {
    expect(
      findWorktreeUseReason({
        activeThreadPaths: [],
        providerSessionPaths: [undefined],
        terminalPaths: [],
        isWithinTarget: inTarget,
      }),
    ).toContain("provider session");
  });
});
