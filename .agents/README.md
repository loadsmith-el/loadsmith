# `.agents/` — source of truth for AI agents

This folder defines how AI agents operate in this repository. It is the shared,
**agent-agnostic** knowledge base read directly by the multi-agent ecosystem
(Codex, Gemini, …). Claude reaches the same content through the root `CLAUDE.md`,
which is a thin pointer to [AGENTS.md](AGENTS.md).

## Layout

```
.agents/
├── AGENTS.md     ← operating instructions for this repo (start here)
└── skills/       ← source logic for this repo's commands/skills
```

## Skills convention

The real, agent-agnostic logic for each command/skill lives in
`.agents/skills/<name>.md`. The Claude-specific slash-command entry points in
`.claude/commands/<name>.md` are thin **stubs**: they keep their frontmatter
(so Claude registers the slash command) and redirect to the matching file here,
so the knowledge base stays centralized in `.agents/`.

Current skills:

- [skills/create-source-plugin.md](skills/create-source-plugin.md)
- [skills/create-destination-plugin.md](skills/create-destination-plugin.md)
- [skills/create-config-provider-plugin.md](skills/create-config-provider-plugin.md)
