# `/deploy` — Fleet Consensus Ledger sur AWS

Déploiement serverless de la flotte. Trois agents, aucun état local : tout l'état
vit dans CockroachDB Cloud. Une Lambda qui meurt ne bloque rien — son bail expire
et un autre agent reprend la tâche.

## Architecture

```
  EventBridge (rate: 1 min)
          │
          ├──> Lambda: scraper-01      (arm64)
          ├──> Lambda: synthesizer-01  (arm64)
          └──> Lambda: validator-01    (arm64)
                      │
                      │  connexion via AWS Secrets Manager
                      ▼
             CockroachDB Cloud
               ├── state_ledger        (consensus + baux)
               ├── provenance_ledger   (écritures signées)
               └── task_embeddings     (index vectoriel C-SPANN)
```

Aucun credential dans le template. Aucun état dans les Lambdas. C'est le point.

## Prérequis

- AWS SAM CLI
- Un cluster CockroachDB Cloud (le tier gratuit suffit pour la démo)
- Schéma appliqué : `cockroach sql --url $DATABASE_URL -f ../fcl_schema.sql`

## 1. Créer le secret

La chaîne de connexion ne doit jamais être commitée ni passée en variable
d'environnement en clair.

```bash
aws secretsmanager create-secret \
  --name fcl/cockroachdb \
  --secret-string '{"database_url":"postgresql://USER:PASS@HOST:26257/fcl?sslmode=verify-full"}'
```

## 2. Déployer

```bash
sam build
sam deploy --guided
```

Paramètres proposés au `--guided` :

| Paramètre | Défaut | Note |
|---|---|---|
| `CockroachSecretName` | `fcl/cockroachdb` | doit matcher l'étape 1 |
| `LeaseSeconds` | `30` | garder > durée de travail typique d'un agent |
| `FleetTickRate` | `rate(1 minute)` | densité de la flotte |

## 3. Lancer une mission de démo

```bash
aws lambda invoke --function-name fcl-mission-seeder out.json
```

Les agents la prennent en charge au prochain tick.

## 4. Démontrer le failover

Couper un agent en pleine tâche — le ledger doit se réparer seul :

```bash
# suspendre le scraper
aws lambda put-function-concurrency \
  --function-name fcl-agent-scraper --reserved-concurrent-executions 0

# après LeaseSeconds, sa tâche redevient revendicable :
cockroach sql --url $DATABASE_URL --execute \
  "SELECT agent_id, action, ts FROM provenance_ledger
   WHERE action='reclaim' ORDER BY ts DESC LIMIT 5;"
```

Une ligne `reclaim` = un autre agent a repris la tâche sans intervention humaine.

Rétablir :

```bash
aws lambda delete-function-concurrency --function-name fcl-agent-scraper
```

## Notes de coût

- **arm64 (Graviton)** : environ 20 % moins cher que x86 à performance égale.
- **Tick à 1 minute** : ajustable. Plus dense = plus réactif, plus de conflits 40001.
  L'alarme CloudWatch `fcl-serialization-conflicts-high` signale la saturation.

## Ce qui n'est pas ici

Ce dossier déploie le **core public**. L'arbitrage de priorité entre agents,
les connecteurs enterprise et le dashboard temps réel ne font pas partie de ce
dépôt.
