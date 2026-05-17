# Real-Time Dynamic Pricing Pipeline — End-to-End Project Plan

> **For fresh Claude instances:** Read this entire file before doing anything. It contains all the context, decisions, and rationale that the user and previous Claude session built up over multiple sessions. Do not re-litigate the locked-in decisions in the "Key Decisions" section.

---

## TL;DR

Build a **real-time dynamic pricing pipeline** for e-commerce price variation on high-demand days (Black Friday, Christmas), driven by demand signals, competitor prices, expiry dates, and seasonal calendar events. Augmented with **AI components** (embedding-based competitor matching + LLM anomaly explainer) and **progressive CI/CD** integrated phase-by-phase. Built entirely on free tier / local Docker. Portfolio project for senior Data Engineer interview prep.

**9 phases. ~22-29 sessions of 30 min each. Polished portfolio in 2-3 weeks.**

---

## Context for Claude

**Who the user is:**
- Data engineer preparing for senior DE interviews
- Comfortable with Python; learning Java/Kafka Streams/Spring Boot patterns
- Wants to focus on real-time streaming pipelines

**How to collaborate with this user (mandatory):**
1. **Learn while building** — Teach DE concepts as you implement. Interleave: introduce concept → write code → explain what just happened. Don't info-dump first then code. Don't dump code without context. Skip explanations for boilerplate (imports, trivial Docker definitions) — focus teaching on load-bearing concepts.
2. **Interview format for explanations** — Problem → Solution (brute + optimal) → Code trace → Real-world DE pattern. This is how the user wants concepts framed.
3. **Stay in DE lane** — Ingest / lakehouse / transform / orchestrate. Do NOT expand into ML serving, API gateways, auth platforms, RL pricing, ML forecasting, or chatbot interfaces. (Embeddings and LLM-as-component ARE in scope — that's the "DE for AI" half.)
4. **Direct action over questionnaires** — Propose a concrete plan and start. Avoid stacked AskUserQuestion prompts on broad requests.
5. **After each phase**, do a 60-second recap of what concepts the user now owns and how they'd surface in an interview.

---

## Use Case

E-commerce platforms repricing 10,000+ SKUs in real time on high-demand days. Each price decision factors in:
- **Demand signal** — sliding-window click + add-to-cart velocity
- **Competitor pricing** — latest snapshot from rival retailers
- **Expiry pressure** — perishable / seasonal stock urgency
- **Seasonal calendar** — Black Friday, Cyber Monday, Christmas multipliers
- **AI competitor matching** — embeddings match "iPhone 15 Pro Max 256GB" across rival catalogs
- **AI anomaly explanation** — LLM generates plain-English reason for each significant change

---

## Architecture

```
                          ┌──────────────────────────────┐
                          │  Python Generator Services    │
                          │  - demand events              │
                          │  - competitor prices          │
                          │  - inventory events           │
                          │  - product catalog            │
                          └───────────────┬──────────────┘
                                          │
                                          ▼
              ┌──────────────────────────────────────────────────┐
              │            Confluent Kafka (Local Docker)         │
              │  + Schema Registry (Avro, BACKWARD compat)        │
              │  11 topics — see "Kafka Topics" section           │
              └────────────┬────────────────────┬─────────────────┘
                           │                    │
                           ▼                    ▼
        ┌───────────────────────────────┐  ┌────────────────────────┐
        │  Kafka Streams (Spring Boot)  │  │  AI Services (Python)  │
        │  Java 17, Maven, EOS v2       │  │  - Embedding matcher   │
        │  - DemandScorer               │  │    (sentence-          │
        │  - CompetitorPriceTable       │  │     transformers +     │
        │  - InventoryUrgency           │  │     pgvector)          │
        │  - CalendarMultiplier         │  │  - LLM explainer       │
        │  - SimulationCommandHandler   │  │    (Claude API or      │
        │  - PricingEngine              │  │     local Ollama)      │
        │  - DlqRouter                  │  └──────────┬─────────────┘
        └────────────┬──────────────────┘             │
                     │                                │
                     ▼                                ▼
        ┌─────────────────────────┐    ┌──────────────────────────┐
        │  Snowflake (free tier)  │    │  Streamlit Dashboard     │
        │  via Kafka Connect      │    │  - Live price feed       │
        │  Idempotent via         │    │  - Simulation controls   │
        │  event_id               │    │    (publishes to         │
        └─────────────────────────┘    │     simulation.commands) │
                                       │  - LLM explanations      │
                                       │  - Cost telemetry        │
                                       └──────────────────────────┘
```

### Kafka Topics (11 total)

| Topic | Partitions | Cleanup | Retention | Key |
|-------|-----------|---------|-----------|-----|
| `demand.events` | 6 | delete | 7d | sku |
| `competitor.prices` | 6 | compact | infinite | sku\|competitor |
| `inventory.events` | 6 | compact | infinite | sku |
| `product.catalog` | 3 | compact | infinite | sku |
| `calendar.events` | 1 | delete | 30d | event_date |
| `simulation.commands` | 3 | delete | 7d | command_id |
| `pricing.decisions` | 6 | delete | 7d | sku |
| `pricing.audit` | 6 | delete | 365d | sku |
| `pricing.dlq` | 3 | delete | 30d | original_topic |
| `product.matches` (AI) | 3 | compact | infinite | external_sku |
| `pricing.explanations` (AI) | 3 | delete | 90d | sku\|timestamp |

### Pricing Formula

```
recommended_price = base_price
                    × demand_multiplier      // 1 + (score - baseline) × 0.3
                    × competitor_factor      // competitor_min × 0.98
                    × expiry_factor          // 0.4 – 1.0
                    × seasonal_multiplier    // BF: 0.7, Cyber Mon: 0.75, Christmas: 1.1
```

---

## Stack

| Layer | Tool | Notes |
|-------|------|-------|
| Producers | Python 3.11 | Poisson arrivals, weighted SKU popularity |
| Schemas | Avro + Confluent Schema Registry | BACKWARD compatibility |
| Transport | Confluent Kafka (Community Edition, local Docker) | KRaft mode |
| Stream processing | Kafka Streams + Spring Boot 3.x + Java 17 + Maven | EOS v2 |
| AI: Embeddings | sentence-transformers + pgvector | all-MiniLM-L6-v2 model |
| AI: LLM | Claude API (or local Ollama for free) | Cost-tracked |
| Sink | Kafka Connect (Snowflake Sink, new connector path) | Idempotent via event_id |
| Warehouse | Snowflake (free trial / free tier) | Analytics views |
| Dashboard | Streamlit | DE-idiomatic, publishes to `simulation.commands` |
| Orchestration | Docker Compose | All services local |
| CI/CD | GitHub Actions + pre-commit hooks | Progressive per-phase |
| Repo hosting | GitHub (public, for portfolio visibility) | |

---

## 9-Phase Plan

### Phase 0: Repo + CI/CD Foundation (~45-60 min)

**Goal:** Working public GitHub repo with branch protection, base CI, AND a working devcontainer before any pipeline code exists.

**Deliverables:**
- `realtime-pricing-pipeline` repo (public)
- Branch protection on `main` (require PR, require CI green)
- `.pre-commit-config.yaml` — ruff, black, spotless
- `.github/workflows/lint.yml` — runs on every PR
- `.devcontainer/devcontainer.json` + `Dockerfile` — Java 17 + Python 3.11 + Docker-in-Docker (**critical: this is the bridge to Codespaces for Phase 7-8**)
- README skeleton with build badge placeholder + "Open in Codespaces" badge

**Concepts to teach:**
- Pre-commit vs CI vs CD — the cost asymmetry of catching bugs at each stage
- Why portfolio repos should be public
- Branch protection as a "production discipline" signal

**Interview talking point:** *"I have a green-badge public repo with branch protection; every PR is gated by CI checks."*

---

### Phase 1: Kafka Infrastructure (~60-75 min)

**Goal:** Working Kafka cluster + Schema Registry + Kafka Connect + Kafka UI on local Docker, with 9 core topics declaratively defined.

**Deliverables:**
- `docker-compose.yml` — Kafka (KRaft), Schema Registry, Kafka Connect, Kafka UI
- `topics.yaml` — declarative topic config for 9 core topics (AI topics added in Phases 7+8)
- `scripts/create-topics.sh` — idempotent topic creator reading from `topics.yaml`
- Smoke test: produce + consume round-trip via console tools

**CI additions:**
- `.github/workflows/topics-as-code.yml` — validates `topics.yaml` syntax, runs `create-topics.sh --dry-run`

**Concepts to teach:**
- Partitions, brokers, KRaft vs ZooKeeper
- Cleanup policies (`compact` vs `delete`) — when to use which
- Schema Registry's role (decoupling producers from consumers)
- Idempotent topic creation
- "GitOps for topics" — config in repo, applied by CI

**Interview talking point:** *"My topics are declarative — `topics.yaml` is the source of truth, applied by CI. No manual UI changes."*

---

### Phase 2: Python Event Producers + Avro Schemas (~75-90 min)

**Goal:** Four Python producer services publishing realistic event streams in Avro format with schemas registered to Schema Registry.

**Deliverables:**
- `schemas/*.avsc` — 8 Avro schema files (demand event, competitor price, inventory event, product catalog, calendar event, pricing decision, pricing audit, simulation command)
- `generators/demand_producer.py` — Poisson-arrival demand events with weighted SKU popularity
- `generators/competitor_producer.py` — competitor price snapshots
- `generators/inventory_producer.py` — stock + expiry events
- `generators/catalog_loader.py` — one-shot SKU catalog seeder
- Shared base producer with confluent-kafka-python + AvroSerializer

**CI additions:**
- `.github/workflows/schema-compat.yml` — diffs `.avsc` files against registered schemas, blocks PR on BACKWARD-incompatible changes
- `.github/workflows/python-tests.yml` — pytest + mypy
- Round-trip test fixture: serialize → deserialize → assert equal

**Concepts to teach:**
- Avro schema evolution (BACKWARD vs FORWARD vs FULL compat)
- Why schemas + a registry instead of JSON
- Producer acks, idempotence, batching
- Generating "realistic" stream data (Poisson arrivals, popularity distributions)

**Interview talking point:** *"Schema compatibility is gated at PR merge — a BACKWARD-breaking change to `product.catalog` can't reach main."*

---

### Phase 3: Kafka Streams Pricing Engine (~3-4 sessions, the largest phase)

**Goal:** Spring Boot application with full Kafka Streams topology that consumes all input topics and produces pricing decisions.

**Deliverables:**
- `pricing-streams/` Spring Boot app, Java 17, Maven
- Topologies:
  - `DemandScorerTopology` — 5-min tumbling window with 1-min grace, scores per SKU
  - `CompetitorPriceTable` — KTable on `competitor.prices` (compacted)
  - `InventoryUrgencyTopology` — expiry-based urgency score
  - `CalendarMultiplierTopology` — GlobalKTable on `calendar.events`
  - `SimulationCommandHandler` — consumes `simulation.commands`, injects scenarios
  - `PricingEngineTopology` — joins all signals, produces `pricing.decisions` + `pricing.audit`
  - `DlqRouter` — bad-event handler → `pricing.dlq`
- EOS v2 enabled
- Interactive Queries REST endpoint for state-store reads

**CI additions:**
- `pricing-streams/src/test/java/...` — `TopologyTestDriver` tests for each topology
- `.github/workflows/java-build.yml` — Maven build + tests + JAR artifact

**Concepts to teach:**
- KStream vs KTable vs GlobalKTable
- Stream-table joins, stream-stream joins, windowed joins
- Tumbling vs hopping vs session windows; grace period vs allowed lateness
- Exactly-once semantics v2 (what it actually guarantees, what it doesn't)
- Interactive Queries pattern
- DLQ pattern + replay/recovery via `kafka-consumer-groups --reset-offsets`
- Stateful processing + state store backing topics

**Interview talking point:** *"EOS v2 guarantees the topology stage; we never claim end-to-end exactly-once. The warehouse sink is separately idempotent via event_id."*

---

### Phase 4: Streamlit Dashboard (~60-75 min)

**Goal:** Live dashboard showing price feed + simulation controls that publish to Kafka.

**Deliverables:**
- `dashboard/app.py` — Streamlit app
- Live price feed (consumes `pricing.decisions`)
- Buttons that publish to `simulation.commands` (Black Friday surge, competitor undercut, expiry crisis)
- Charts: top movers, demand heatmap, DLQ rate, consumer lag

**CI additions:**
- `.github/workflows/streamlit-smoke.yml` — boots Streamlit, asserts it renders without errors

**Concepts to teach:**
- Why dashboard simulation controls publish events instead of mutating state (event-driven discipline)
- Kafka as a control plane, not just a data plane
- Why Streamlit instead of Flask/NestJS for DE dashboards

**Interview talking point:** *"The dashboard doesn't touch state directly — it publishes intent to `simulation.commands` and lets the topology react. Same loop as production traffic."*

---

### Phase 5: Snowflake Sink + Analytics Views (~75 min)

**Goal:** `pricing.decisions` + `pricing.audit` land in Snowflake; analytics views built on top.

**Deliverables:**
- `snowflake/ddl/` — table definitions
- `snowflake/migrations/` — versioned migration files
- `snowflake/connectors/snowflake-sink.json` — Kafka Connect config
- Materialized view: BF day vs normal day pricing comparison
- Idempotent ingestion using `event_id` as merge key

**CI additions:**
- `.github/workflows/sql-lint.yml` — sqlfluff on all SQL files
- `.github/workflows/snowflake-migrate.yml` — applies migrations on merge to `main`

**Concepts to teach:**
- Idempotent warehouse ingestion (MERGE on event_id)
- Versioned migrations (Flyway-style discipline)
- Why dashboards don't query the streaming pipeline directly
- Materialized views for BI-friendly aggregates

**Interview talking point:** *"Warehouse ingestion is idempotent via MERGE on event_id — replay a topic, get the same warehouse state."*

---

### Phase 6: Documentation + Interview Polish (~60-90 min)

**Goal:** Repo looks like a senior DE shipped it.

**Deliverables:**
- README with architecture diagram, quickstart, design decisions
- Demo script (`scripts/demo.sh`) — spins up everything, runs a 60-second simulation
- `docs/schema-evolution.md` — walk-through of v1 → v2 schema change with compat gate
- `docs/replay-drill.md` — how to replay a topic from a given offset
- `docs/late-events.md` — what happens to events beyond grace period
- `docs/interview-talking-points.md` — your script for interview Q&A
- Demo GIF or short Loom

**CI additions:**
- README badges: build, schema compat, last commit
- `.github/workflows/demo-script.yml` — full pipeline boots in CI as smoke test

**Concepts to teach:**
- Documentation as the *primary* deliverable of a portfolio project
- The interview narrative: problem → architecture → trade-off you'd make differently

---

### Phase 7: AI — Embedding-Based Competitor Matching (~3-4 sessions)

**Goal:** Match SKUs across competitor catalogs using semantic similarity instead of fragile string matching.

**Deliverables:**
- `embedding-matcher/` Python service
- sentence-transformers `all-MiniLM-L6-v2` (free, runs on CPU, ~80MB)
- pgvector (Postgres extension via Docker)
- Consumes `product.catalog` + competitor product feeds
- Generates 384-dim embeddings, stores in pgvector
- Top-k nearest-neighbor lookup per incoming competitor product
- Publishes match → `product.matches` (compacted topic, keyed by external_sku)
- Kafka Streams topology updated to join `competitor.prices` with `product.matches`

**CI additions:**
- pytest fixtures with known vectors → assert expected nearest neighbors
- Embedding model version pinned in CI (so test results are deterministic)

**Concepts to teach:**
- What embeddings are (vectors in semantic space)
- Cosine similarity vs Euclidean distance
- pgvector vs FAISS vs Pinecone — when each makes sense
- Embeddings as a *data product* in a streaming pipeline
- Model version pinning (embedding drift is a real production issue)

**Interview talking point:** *"Embeddings are just another column in the pipeline — same idempotency, lineage, and observability concerns as any other field. pgvector gives me transactional consistency for free."*

---

### Phase 8: AI — LLM Anomaly Explainer (~2-3 sessions)

**Goal:** For every significant price change, generate a plain-English explanation.

**Deliverables:**
- `anomaly-explainer/` Python service
- Consumes `pricing.audit`
- Filters: only changes > 15% absolute or > 20% from baseline
- Enriches with `product.catalog` snapshot
- Calls Claude API (or local Ollama for free tier)
- Structured prompt with audit fields + product context
- Publishes natural-language explanation → `pricing.explanations` (keyed by `sku|timestamp`)
- Cost telemetry: tokens used, $/event, p95 latency
- DLQ for failed LLM calls
- Streamlit dashboard updated to display explanation next to each change

**CI additions:**
- Snapshot tests for prompt output (against canned LLM responses)
- LLM cost-guard: `assert tokens_per_call < 800`
- Circuit-breaker test: API failure → DLQ, not crash

**Concepts to teach:**
- LLM-as-a-stream-processor (it's just another component with latency + cost budgets)
- Prompt templates with structured output
- Cost telemetry as a first-class metric
- Testing non-deterministic systems (snapshot tests, output schemas)
- Circuit breaker pattern for external API dependencies

**Interview talking point:** *"The LLM is treated like any external service — bounded latency budget, circuit breaker, DLQ on failure, cost telemetry per call. The pipeline degrades gracefully if the LLM is down."*

---

## Repo Structure (Final State)

```
realtime-pricing-pipeline/
├── README.md
├── docker-compose.yml
├── topics.yaml
├── .pre-commit-config.yaml
├── .github/workflows/
│   ├── lint.yml
│   ├── topics-as-code.yml
│   ├── schema-compat.yml
│   ├── python-tests.yml
│   ├── java-build.yml
│   ├── streamlit-smoke.yml
│   ├── sql-lint.yml
│   ├── snowflake-migrate.yml
│   └── demo-script.yml
├── .devcontainer/                # for Codespaces
├── scripts/
│   ├── create-topics.sh
│   ├── inject-bad-events.sh
│   ├── replay-drill.sh
│   └── demo.sh
├── schemas/                      # Avro
│   ├── demand_event.avsc
│   ├── competitor_price.avsc
│   ├── inventory_event.avsc
│   ├── product_catalog.avsc
│   ├── calendar_event.avsc
│   ├── pricing_decision.avsc
│   ├── pricing_audit.avsc
│   └── simulation_command.avsc
├── generators/                   # Python producers
│   ├── demand_producer.py
│   ├── competitor_producer.py
│   ├── inventory_producer.py
│   └── catalog_loader.py
├── pricing-streams/              # Spring Boot + Kafka Streams
│   ├── pom.xml
│   └── src/main/java/com/pricing/
│       ├── PricingStreamsApplication.java
│       └── topology/
│           ├── DemandScorerTopology.java
│           ├── CompetitorPriceTable.java
│           ├── InventoryUrgencyTopology.java
│           ├── CalendarMultiplierTopology.java
│           ├── SimulationCommandHandler.java
│           ├── PricingEngineTopology.java
│           └── DlqRouter.java
├── embedding-matcher/            # AI Phase 7
│   ├── matcher.py
│   ├── pgvector_store.py
│   └── tests/
├── anomaly-explainer/            # AI Phase 8
│   ├── explainer.py
│   ├── prompts/
│   ├── cost_tracker.py
│   └── tests/
├── dashboard/                    # Streamlit
│   └── app.py
├── snowflake/
│   ├── ddl/
│   ├── migrations/
│   └── connectors/
│       └── snowflake-sink.json
└── docs/
    ├── architecture.md
    ├── schema-evolution.md
    ├── replay-drill.md
    ├── late-events.md
    └── interview-talking-points.md
```

---

## Key Decisions (Locked In — Do Not Re-Litigate)

| Decision | Rationale |
|----------|-----------|
| Streamlit for dashboard | Spring Boot already owns backend; second web framework duplicates work. Streamlit is DE-idiomatic. |
| Kafka topics as control plane | Simulation buttons publish to `simulation.commands` instead of mutating state. Demonstrates event-driven discipline. |
| EOS v2 for Streams topology only | Never claim end-to-end exactly-once. Warehouse sink is separately idempotent via event_id. |
| `product.catalog` as compacted topic | SKU reference enrichment via KTable join. |
| Snowflake deferred to Phase 5 | Pipeline is demonstrable on local Kafka alone first; reduces dev friction. |
| Schema Registry BACKWARD compat | Phase 6 includes a v1 → v2 schema evolution example. |
| Augment with AI, not pivot to AI-native | Adding AI to an existing pipeline is more authentic to real DE work than building RAG from scratch. |
| Embeddings + LLM, not ML forecasting/RL | DE-for-AI skillset (vector DBs + LLM-as-component). ML/RL is MLE territory. |
| Progressive CI/CD, not big-bang at end | CI only catches things if it exists when the change happens. End-loaded CI is shallow rubber-stamp work. |
| GitOps over UIs | Topics, schemas, connectors, migrations all in repo. No manual UI changes. |
| Hybrid runtime: Local Docker for Phase 0-6, Codespaces for Phase 7-8 | User has ~5-6 GB free RAM on a 16 GB machine. Local fits through Phase 6 (~5 GB peak); Phase 7+8 (Postgres + embedder + LLM service) push past headroom. Switch to Codespaces free tier (8 GB / 60 hr/month) for AI phases. Devcontainer in Phase 0 is the bridge. |

**Rejected (do not reintroduce without explicit new request):**
- Flask or NestJS for dashboard — redundant with Spring Boot
- ML forecasting model — MLE territory
- Reinforcement learning for pricing — research project
- Chatbot interface — gimmicky

---

## Free Tier Resources Required

| Service | Free Tier Limit | Used For |
|---------|-----------------|----------|
| GitHub | Unlimited public repos + unlimited Actions for public | Repo + CI/CD |
| Docker Desktop | Free for personal use | Local Kafka, Postgres, etc. |
| Snowflake | 30-day free trial ($400 credit) | Warehouse (Phase 5+) |
| Claude API | Pay-as-you-go (very cheap for this scale) | LLM explainer (Phase 8) — or use local Ollama for $0 |
| GitHub Codespaces (optional) | 60 hr/month free | Zero-install path if Docker Desktop isn't available |

---

## Pre-Flight Checklist Before Phase 0

- [ ] GitHub account (you have this)
- [ ] Git installed + configured (`git config --global user.email`)
- [ ] Docker Desktop installed + running on Windows
- [ ] VS Code (or editor of choice)
- [ ] Python 3.11+ installed
- [ ] Java 17+ installed (`java -version`)
- [ ] Maven installed (`mvn -version`)

If any are missing, install them as the first 5 min of the session.

---

## How to Use This Plan in a Fresh Claude Terminal

1. **Create the repo directory:**
   ```powershell
   New-Item -ItemType Directory -Path C:\Users\iamka\realtime-pricing-pipeline
   ```
2. **Copy this file in:**
   ```powershell
   Copy-Item C:\Users\iamka\PROJECT_PLAN.md C:\Users\iamka\realtime-pricing-pipeline\PROJECT_PLAN.md
   ```
3. **Open Claude Code in the new directory:**
   ```powershell
   cd C:\Users\iamka\realtime-pricing-pipeline
   claude
   ```
4. **First prompt to the fresh Claude:**

   > Read PROJECT_PLAN.md in full. This is a senior DE portfolio project I've been planning. The plan, decisions, and collaboration style are all in that file — do not re-litigate the locked-in decisions. Acknowledge briefly when you've read it, then let's start Phase 0 (repo + CI/CD foundation).

5. The fresh Claude will pick up exactly where this session left off.

---

## Current Status (as of 2026-05-17)

- Plan locked in: 9 phases, AI additions, progressive CI/CD
- All 9 phases tracked as tasks in this Claude session (will not carry to fresh session — that's fine, plan is in this doc)
- **Next action:** Phase 0 — create the GitHub repo and set up base CI
- **Estimated time to walking skeleton:** 1-2 long days
- **Estimated time to polished portfolio:** 2-3 weeks
