import type { ExecutionBindingIssue, MissionDefinitionIssue } from "./types.js"

/** Reports every safety or graph issue found while compiling a mission. */
export class MissionDefinitionError extends Error {
  override readonly name = "MissionDefinitionError"

  constructor(
    readonly missionId: string,
    readonly issues: readonly MissionDefinitionIssue[],
  ) {
    super(
      `Mission ${missionId} has ${issues.length} definition issue${issues.length === 1 ? "" : "s"}`,
    )
  }
}

/** Reports bindings that cannot safely satisfy a compiled mission. */
export class ExecutionBindingError extends Error {
  override readonly name = "ExecutionBindingError"

  constructor(readonly issues: readonly ExecutionBindingIssue[]) {
    super(`Execution binding has ${issues.length} issue${issues.length === 1 ? "" : "s"}`)
  }
}
