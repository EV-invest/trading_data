# The `trading-data` skill

An agent skill for building against this framework from another repo. `SKILL.md` is the entry point;
`references/` is loaded on demand.

## One payload, two agents

Claude Code and Codex CLI read the same thing: a directory whose `SKILL.md` opens with `---`-delimited
YAML frontmatter carrying `name` and `description`, plus whatever files that file links to. There is
no shared "standard" document, but the formats have converged, and neither reads anything the other
rejects — so this needs no per-agent `src/`, no generation step and no packaging layer. Only the
install path differs.

| agent | user scope | project scope |
|---|---|---|
| Claude Code | `~/.claude/skills/<name>/` | `<project>/.claude/skills/<name>/` |
| Codex CLI | `${CODEX_HOME:-~/.codex}/skills/<name>/` | `<project>/.agents/skills/<name>/` |

The directory name must match the frontmatter `name`, hence `trading-data` rather than `skill`.

```sh
docs/skill/install.sh              # both user scopes
docs/skill/install.sh ../some-repo # both project scopes
```

Codex additionally accepts a `SKILL.json` whose `interface.short_description` supersedes the
frontmatter one. Adding it would fork the payload for one field, so this does not.

## Links are absolute on purpose

Installed, this directory sits outside the repo, so a relative `../ARCHITECTURE.md` resolves to
nothing. `SKILL.md` therefore states a base — a local checkout found through `cargo metadata`, else
`github.com/EV-invest/trading_data` — and every repo path is written against it. Links *within*
`docs/skill/` stay relative, since the tree moves as a unit.
