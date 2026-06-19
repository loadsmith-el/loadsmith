# loadsmith — agent instructions (Claude)

Before anything else, read and follow [`.agents/AGENTS.md`](.agents/AGENTS.md) —
the source of truth for this repository. This file is only a pointer; the full,
agent-agnostic knowledge base lives in `.agents/` (also read directly by Codex,
Gemini, and other agents).

Claude slash commands under `.claude/commands/` are thin stubs that defer to
their real logic in [`.agents/skills/`](.agents/skills/).

**Do not add instructions or skills to this file.** A new directive → put it in
`.agents/AGENTS.md`. A new skill → write `.agents/skills/<name>.md` and add a thin
stub in `.claude/commands/<name>.md`. See the "Authoring rule" in
[`.agents/AGENTS.md`](.agents/AGENTS.md).
