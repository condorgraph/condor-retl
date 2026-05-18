---
name: retl-organize-project
description: Organize an existing RETL project into a maintainable project layout using assisted judgment.
---

# Organize A RETL Project

Use this skill when an existing RETL script or small project has outgrown a
single file and should move toward an organized, inspectable project layout.

## Workflow

1. Inspect the existing project and identify public entrypoints, tests, config,
   Source declarations, State declarations, Event declarations, destinations,
   Syncs, and run scripts.
2. Preserve behavior first. Add characterization tests or import checks before
   moving code.
3. Separate declaration modules from run entrypoints.
4. Keep local destination code under the user's project package.
5. Keep generated examples, TOML files, and tests secret-free.
6. Prefer small moves that keep existing command names working.

## Boundaries

- Project organization requires judgment and should be handled as assisted
  editing.
- Do not move repository contributor skills from `.agents/skills/` into the
  user project.
- Use the `retl-start-project` skill's organized layout guidance as a reference,
  not as a mandatory migration target.

## Validation

Run import tests and the existing project checks after every meaningful move.
End with a dry-run command or test that proves the organized Sync still loads.
