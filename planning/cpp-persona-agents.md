# paglets/cpp: persona agents (round 3)

Status: concept with work packages, planned after round 2. Companion to
[cpp-edition-plan.md](cpp-edition-plan.md),
[cpp-security-and-communication.md](cpp-security-and-communication.md) and
[cpp-round2-paglets.md](cpp-round2-paglets.md). Generalizes round 2 idea R2-17
(Mission Agent).

## 1. Concept

A **persona agent** is a roaming paglet with an identity, a mission, a memory
and a personality, that you can chat with. It does "intelligent" work on many
hosts, and every time it needs to think it travels to a host offering AI (its
"brain"), thinks there, and moves on with the result.

Guiding idea: **stateful traveller, stateless brain.** The persona carries its
whole mind (charter, mission, memory, plans, conversation) in its memory
image. AI hosts only provide thinking power and keep nothing.

The clever part: the persona does not call its brain for every step. On each
brain visit the LLM prepares an **executable plan with decision rules**. The
persona's body then works autonomously on other hosts and only returns to a
brain when the plan reaches a point that needs real judgment.

## 2. Anatomy

| Part | Content |
|---|---|
| **Charter** | Name, role, voice and personality, mission, values and limits ("never delete", "ask before writing"), preferred brains, budgets. Signed by the owner, carried in the passport, scopes the persona's grants. Only the owner can change it. |
| **Working memory** | Current mission state, open questions, active plan and position in it |
| **Journal** | Where it went, what it did, what it found; also the audit trail |
| **Knowledge** | Condensed facts learned ("project X reports live on lab-1 and lab-3") |
| **Skills** | Plans that worked before, parameterized for reuse |
| **Conversation** | Chat history with the owner and other personas |
| **Body** | Plan interpreter plus tools through system paglets (`files`, `server-info`, `web`, `storage`, `locator`, ...) |

All of it lives in the paglet's memory image, so it moves, checkpoints and
sleeps with the persona without extra serialization.

## 3. Thinking trips

Two ways to reach a brain:

- **Whole persona travels** (default, the pure mobile-agent way): the persona
  moves to the best host offering `ai.generate`, thinks with its full context,
  and moves on. Page dedupe makes repeated trips cheap: only memory pages that
  changed since the last visit travel. Required when the persona carries
  restricted content; residency marks decide which brains it may visit at all.
- **Thought courier**: the persona stays where it is (for example while pinned
  or watching something) and sends a small child paglet carrying only the
  context needed for one thought. The courier returns with the answer and is
  disposed.

Brain choice per thought, through service offers and the charter's
preferences: a small fast model (for example Apple's on-device model on a
macOS 27 host) for classification and short answers, a bigger Ollama model for
planning and long texts.

## 4. Plans as data: the brain prepares, the body executes

On a brain visit the LLM produces a plan in a small, safe **plan language**:
structured data (MessagePack/JSON AST), never code. Building blocks:

- **Tool steps**: typed calls to system paglet operations (`files.find`,
  `server-info.load`, `web.fetch`, ...).
- **Movement steps**: go to a host, go to the best offer for a service, visit
  every host matching a selector.
- **Local logic** that needs no AI: loops over results, conditions, filters,
  sorting, regular expressions, counting, aggregation, text templates.
- **Consult-brain points**: which data to take along and which question to
  ask; the plan continues with the answer.
- **Report points**: tell the owner, ask the owner before continuing.
- **Budgets and stop conditions** per plan.

Before execution the persona **validates** the plan against its grants,
manifest, charter limits and budgets. Steps it may not take are rejected up
front and either go back to the brain for replanning or become an `ask`
request for a human.

One brain visit can thus produce a whole expedition: "visit all lab hosts,
find reports changed this week, skip files under 10 KB, collect the titles; if
more than 20, come back and ask me which to read."

Benefits: few brain trips, auditable plans ("show me your plan"), replayable
execution, reusable skills, and no generated code to trust: the LLM only
produces data that a fixed interpreter executes within fixed capabilities.

## 5. Chatting with a roaming persona

- `paglets chat <persona>` (terminal), later a chat window and the MCP
  gateway (R2-18). Messages reach the persona wherever it is through the
  locator.
- **Quick answers from memory**, without a brain trip: "where are you?",
  "what have you found?", "what is your plan?".
- **Thinking answers**: "let me think" or "on my way to a brain", then the
  answer after the trip.
- Owner messages have priority; the mission can be changed mid-way.
- **Proactive messages** through user-info: findings, questions, briefings.
- Between missions the persona sleeps as a stored image and wakes on a
  schedule, a trigger or the next chat message, with its full memory.

Example:

```text
you:       What changed in the calibration docs this week?
librarian: I last checked on Monday. Give me a moment, I'll look around.
           [plan: find on hosts lab-*, filter modified >= Monday → 7 files]
           [thinking trip to mac-studio (ai.summarize), back]
librarian: Seven documents changed. The important one: the tolerance for
           detector B went from 5 to 3 ppm (lab-2, calibration/SOP-12.md).
           Two files are drafts. Want a digest file on your machine?
you:       Yes, and remind me Friday if SOP-12 is still a draft.
librarian: Done. I'll check Friday at 9:00 and message you.
```

## 6. Safety

- **The brain only suggests.** Every plan and step goes through the validator
  and the normal capability checks; the brain has no authority of its own.
- **Prompt injection**: content read from files or web pages enters the
  context marked as data, never as instructions. Side-effecting steps need
  matching grants, sensitive ones a human `ask` approval. A web page can make
  the persona do nothing it was not already allowed to do.
- **Budgets** in the charter: maximum steps, brain calls, tokens, time, hosts
  visited, clones and children per mission.
- **Transparency and control**: owners and admins can inspect plan and
  journal, pause or stop the persona, and revoke its grants mesh-wide.
- **Stateless brains**: the `ai` system paglet runs in a no-prompt-logging
  mode for personas; the persona's memory exists only in its own image and in
  what residency rules allow.
- **The persona cannot extend itself**: it cannot change its charter, grant
  itself access, or load code; it can only request grants like any paglet.

## 7. Example personas

| Persona | Mission | Typical behaviour |
|---|---|---|
| **Librarian** | Knows the documents of the mesh | Roams, reads, summarizes, remembers where things are; answers from memory next time |
| **Lab Assistant** | Follows instrument runs | Watches new raw files and QC values; explains anomalies after a brain trip; suggests reruns but asks before starting anything |
| **Night Watch** | Patrols overnight | Rounds on load, disks and logs; investigates anomalies on the spot; morning briefing in chat |
| **Researcher** | Answers questions from the web | Searches and fetches on a web host, thinks on an AI host, iterates, reports with sources |
| **Data Steward** | Keeps data sets documented | Finds data without metadata, drafts descriptions on a brain, asks the data owners to confirm |

Personas can work as a **team**: they talk through endpoints and pubsub rooms
the owner can watch, and delegate sub-missions to helper paglets with narrowed
grants.

## 8. Prerequisites from rounds 1 and 2

| Prerequisite | From |
|---|---|
| Memory-image mobility with page dedupe | WP7, WP12 |
| Capabilities, grants, `ask` approvals, audit | WP6, WP9 |
| Service offers, `ai` and `web` system paglets, data residency | WP17 |
| Locator for chat delivery to roaming paglets | WP13 |
| Tool descriptions generated from service contracts | Round 2 platform features (R2-17, R2-18) |
| MCP gateway for external chat clients (optional) | R2-18 |

## 9. Work packages

IDs are prefixed `P` to keep them apart from the main plan.

### Milestone R3-M1: single persona, single host, mock brain

**P1: Plan language specification**

- Typed AST for tool, movement, logic, consult-brain and report steps;
  variables and result references; budgets and stop conditions; versioning.
- JSON schema of the plan for constrained LLM output.
- Exit: specification with examples; a corpus of hand-written plans that
  covers every construct.

**P2: Plan interpreter and validator (guest SDK)**

- Deterministic interpreter, step journaling, resumption after a move or
  restart at any step boundary, replay from the journal.
- Static validation against grants, manifest, charter limits and budgets;
  runtime checks for dynamic values.
- Exit: the plan corpus runs identically on macOS, Linux and Windows hosts;
  invalid plans are rejected with precise reasons.

**P3: Persona core**

- Charter format as a passport extension, signed by the owner.
- Memory model: working memory, journal, knowledge, skills, conversation;
  size limits and memory condensation (summarizing journal and knowledge on
  brain visits).
- Mission lifecycle: brief, clarify, plan, approve, execute, report, debrief,
  sleep.
- Exit: a persona completes a mission on one host with a scripted mock brain
  and sleeps and wakes with its memory intact.

**P4: Mock brain and test harness**

- Deterministic fake LLM that returns scripted plans and answers; scenario
  tests in CI without AI hosts.
- Exit: every later work package can be tested without real models.

### Milestone R3-M2: thinking trips and chat

**P5: Brain protocol**

- Context assembly from charter, memory and mission, with strict separation
  of instructions and data.
- Brain selection via offers, charter preferences and residency marks.
- Structured output: plans and answers validated against their schemas, with
  retries and fallback to another brain.
- Both trip modes: whole persona travels, thought courier.
- Extensions of the `ai` system paglet: schema-constrained output where the
  backend supports it, no-prompt-logging mode.
- Exit: a persona on a Linux host thinks on the macOS 27 host and an Ollama
  host, choosing by task; a persona carrying restricted content only visits
  allowed brains.

**P6: Tool catalogue**

- Tool descriptions for the brain generated from reflected service contracts,
  filtered to what the persona's grants allow on the relevant hosts.
- Exit: plans produced by a real model only use tools that exist and are
  granted.

**P7: Chat channel**

- `paglets chat`: delivery through the locator, quick answers from memory,
  thinking answers after trips, priority handling, mission changes,
  proactive messages via user-info, conversation history.
- Exit: chatting with a persona while it moves between three hosts works
  without message loss.

### Milestone R3-M3: safety and skills

**P8: Safety layer**

- Charter budgets, approval points for side effects, prompt-injection
  defences, pause/stop controls, plan and journal inspection for owners and
  admins.
- Red-team scenarios: injected instructions in files and web pages, runaway
  planning, budget exhaustion, attempts to exceed grants.
- Exit: all red-team scenarios end in a refused step, an approval request or a
  stopped persona, and are visible in the audit log.

**P9: Skills**

- Saving successful plans as parameterized skills, reuse by the brain,
  owner-approved sharing of skills between personas.
- Exit: a repeated mission needs measurably fewer brain trips the second time.

### Milestone R3-M4: teams and example personas

**P10: Persona teams**

- Persona-to-persona conversations, delegation of sub-missions to helper
  paglets with narrowed grants, observable pubsub rooms.
- Exit: the Librarian and the Researcher answer a question together that
  needs both mesh documents and web sources.

**P11: Example personas and evaluation**

- Librarian, Lab Assistant, Night Watch, Researcher, Data Steward, each with
  a charter, default skills and a `paglets persona <name>` command.
- Evaluation: task success, brain trips per mission, image sizes, latency,
  owner interventions; with mock brains in CI and real brains in periodic
  runs.

**P12: Documentation**

- Persona authoring guide (charters, skills, budgets), plan language
  reference, safety guide for owners and admins.

## 10. Risks

| Risk | Mitigation |
|---|---|
| Small local models produce poor or invalid plans | Schema-constrained output, validator with precise feedback for replanning, skills as proven templates, brain choice per task |
| Prompt injection through files and web content | Data/instruction separation, capability checks, approvals for side effects, red-team tests (P8) |
| Memory images grow with journal and knowledge | Condensation on brain visits, size limits, old journal parts exported as artifacts |
| Many brain trips make personas slow | Plans with local logic and consult points, thought couriers, page dedupe |
| Non-deterministic LLM output makes tests flaky | Mock brains in CI (P4); real-model runs tracked as evaluations, not pass/fail tests |
| Owners over-trust the persona | Visible plans and journal, approval points, clear budgets in the charter |

## 11. Open questions

1. Plan language: own minimal typed AST (proposed), or a subset of an existing
   workflow format?
2. Chat front end beyond the terminal: a local web page, the MCP gateway, or
   both?
3. May personas share knowledge with each other by default, or only through
   explicit owner-approved exchanges (proposed)?
4. Should brains ever be allowed to keep a per-persona cache (for speed), or
   stay strictly stateless (proposed)?
