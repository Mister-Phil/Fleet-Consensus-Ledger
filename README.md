# Fleet Consensus Ledger (FCL)

**Coordination multi-agents sans conflit, à l'échelle, sur CockroachDB.**

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![CockroachDB](https://img.shields.io/badge/CockroachDB-Cloud-6933FF)](https://www.cockroachlabs.com/)
[![AWS SAM](https://img.shields.io/badge/AWS-SAM%20Serverless-FF9900)](https://aws.amazon.com/serverless/sam/)

> Trois agents. Une même flotte. Zéro conflit d'écriture, zéro tâche perdue si un agent meurt en cours de route.

---

## Le problème

Une flotte d'agents IA qui travaille en parallèle sur un état partagé se heurte à trois pannes silencieuses :

1. **Deux agents écrivent sur la même donnée** → l'un écrase le travail de l'autre, personne ne le voit
2. **Un agent crashe en pleine tâche** → la tâche reste bloquée indéfiniment
3. **Aucune trace de qui a écrit quoi** → un résultat corrompu est impossible à tracer

La plupart des architectures multi-agents ignorent ces trois problèmes jusqu'à ce qu'ils arrivent en production.

## La solution

FCL est un moteur de coordination — pas un framework d'orchestration de plus. Il traite CockroachDB comme la machine d'état transactionnelle de la flotte, et laisse le moteur distribué faire ce qu'il fait de mieux : garantir la cohérence sous écriture concurrente.

```
  Agent A (Scraper) ─┐
  Agent B (Synth.)   ─┼──> CockroachDB (SERIALIZABLE) ──> état cohérent, toujours
  Agent C (Validator) ┘
```

Trois mécaniques, aucune magie :

| Mécanique | Ce qu'elle règle |
|---|---|
| **Verrou optimiste** (`version` + retry sur `40001`) | Deux agents en conflit → un gagne, l'autre rejoue proprement |
| **Failover par bail** (`lease_expires_at`) | Un agent meurt → sa tâche redevient revendicable automatiquement |
| **Provenance signée** | Chaque écriture porte l'identité + le niveau d'accès de l'agent qui l'a faite |

Zéro composant de type "gate" ou "guardrail". C'est de l'**orchestration**, pas de la surveillance.

---

## Pourquoi CockroachDB (pas un "toy" key-value)

FCL exploite trois capacités distribuées natives, pas seulement du stockage :

- **Isolation SERIALIZABLE par défaut** — le niveau le plus strict d'ANSI SQL. Pas de lock blocking : le moteur avorte la transaction perdante avec `SQLSTATE 40001`, et la boucle applicative la rejoue avec backoff exponentiel + jitter.
- **`FOR UPDATE` + garde optimiste** sur la revendication de tâche — la mécanique de consensus tient dans une seule requête atomique.
- **Index vectoriel distribué (C-SPANN)** sur `task_embeddings` — les agents partagent un contexte sémantique sans dupliquer la donnée dans un store séparé.

## Architecture

```
                  [ Fleet: Scraper · Synthesizer · Validator ]
                         (AWS Lambda, arm64, serverless)
                                     │
                        (écritures concurrentes)
                                     │
                                     ▼
                    [ CockroachDB Cloud — SERIALIZABLE ]
                                     │
                ┌────────────────────┼────────────────────┐
                ▼                    ▼                    ▼
        state_ledger         provenance_ledger      task_embeddings
      (consensus + baux)    (écritures signées)    (index C-SPANN)
```

Déploiement serverless complet, sans état côté compute — voir [`/deploy`](./deploy).

---

## Démo rapide

```bash
# 1. Schéma
cockroach sql --url $DATABASE_URL -f fcl_schema.sql

# 2. Local (3 agents en threads concurrents)
export DATABASE_URL="postgresql://..."
python fcl_agents.py --seed
python fcl_agents.py --chaos   # tue un agent en cours -> failover visible
```

Sortie attendue en mode `--chaos` :

```
[14:02:11] CHAOS            kill scraper-01 — la flotte doit reprendre sa tache seule
[14:02:41] validator-01       FAILOVER: reprise de la tache abandonnee par scraper-01
```

Aucune intervention humaine. Le bail expire, la flotte se répare.

Déploiement AWS complet : voir [`deploy/README.md`](./deploy/README.md).

---

## Structure du dépôt

```
.
├── fcl_schema.sql          # DDL CockroachDB — consensus, provenance, vecteurs
├── fcl_agents.py            # Runner local — 3 agents concurrents, retry, chaos mode
├── layer/
│   └── fcl_core.py          # Noyau partagé : retry 40001, claim/complete, provenance
├── src/
│   ├── agent_handler.py     # Handler Lambda unique pour tous les agents
│   └── seed_handler.py      # Création de mission de démo
├── deploy/
│   ├── template.yaml        # AWS SAM — Lambda + EventBridge + Secrets Manager
│   └── README.md            # Guide de déploiement + démo de failover
└── mcp/
    └── config.json          # Intégration MCP (Claude Code / Cursor)
```

---

## Ce que ce dépôt ne contient pas

Ce dépôt public montre le **moteur de coordination** — la preuve technique que le consensus tient. Il ne contient pas :

- l'algorithme d'arbitrage de priorité entre agents à valeur inégale
- les connecteurs enterprise (Slack, Notion, CRM)
- le dashboard d'observabilité temps réel

Ces composants font partie de l'offre **Medusa Black Labs Omega** — le moteur FCL packagé, opéré et supporté pour la production.

---

## Stack

CockroachDB Cloud · AWS Lambda (arm64/Graviton) · AWS SAM · Secrets Manager · EventBridge · CloudWatch · Python 3.12 · psycopg3

## Licence

Apache 2.0 — voir [`LICENSE`](./LICENSE).

---

*Construit par [Medusa Black Labs](https://github.com/FOW-Digital-Labs) pour le hackathon CockroachDB × AWS.*
