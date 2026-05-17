# Real-Time Dynamic Pricing Pipeline

[![lint](https://github.com/OWNER/realtime-pricing-pipeline/actions/workflows/lint.yml/badge.svg)](https://github.com/OWNER/realtime-pricing-pipeline/actions/workflows/lint.yml)
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/OWNER/realtime-pricing-pipeline)

End-to-end real-time pricing engine for e-commerce SKUs on high-demand days (Black Friday, Cyber Monday, Christmas). Demand signals, competitor snapshots, expiry pressure, and seasonal calendar drive per-SKU repricing in real time. Augmented with embedding-based competitor matching and an LLM anomaly explainer.

> **Status:** Phase 0 — repo + CI/CD foundation. Full 9-phase plan in [`PROJECT_PLAN.md`](./PROJECT_PLAN.md).

---

## Architecture (target state)

```
generators (Python)
    │
    ▼
Confluent Kafka (KRaft) + Schema Registry (Avro, BACKWARD)
    │                       │
    ▼                       ▼
Kafka Streams           AI services
(Spring Boot, EOS v2)   (embeddings + LLM)
    │
    ▼
Kafka Connect ──► Snowflake
                  Streamlit dashboard
```

11 topics, declarative in `topics.yaml`. Pricing formula:

```
recommended_price = base_price
                  × demand_multiplier
                  × competitor_factor
                  × expiry_factor
                  × seasonal_multiplier
```

## Stack

| Layer | Tool |
|---|---|
| Producers | Python 3.11 |
| Schemas | Avro + Confluent Schema Registry |
| Transport | Confluent Kafka (KRaft) |
| Stream processing | Kafka Streams + Spring Boot 3 + Java 17 |
| AI | sentence-transformers + pgvector; Claude API / Ollama |
| Sink | Kafka Connect → Snowflake |
| Dashboard | Streamlit |
| Orchestration | Docker Compose |
| CI/CD | GitHub Actions, pre-commit |

## Quickstart

> Container-based dev recommended — open in VS Code "Reopen in Container" or GitHub Codespaces.

```bash
# Phase 0 only — installs hooks
pip install pre-commit
pre-commit install
```

Pipeline boot will land in Phase 1.

## Repo map

```
.devcontainer/      reproducible dev image (Java 17 + Python 3.11 + docker-in-docker)
.github/workflows/  CI: lint, schema-compat (Phase 2), java-build (Phase 3) ...
schemas/            Avro schemas              (Phase 2)
generators/         Python event producers    (Phase 2)
pricing-streams/    Spring Boot + Kafka Streams (Phase 3)
dashboard/          Streamlit                  (Phase 4)
snowflake/          DDL + migrations + connector (Phase 5)
embedding-matcher/  sentence-transformers + pgvector (Phase 7)
anomaly-explainer/  LLM-as-stream-processor    (Phase 8)
docs/               schema evolution, replay drill, late events, interview script
```

## License

MIT.
