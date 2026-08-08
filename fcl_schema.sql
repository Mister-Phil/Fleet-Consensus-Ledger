-- ============================================================================
-- Fleet Consensus Ledger (FCL) — schéma CockroachDB
-- Medusa Black Labs · Apache 2.0 · Hackathon CockroachDB x AWS
--
-- Appliquer :
--   cockroach sql --url "$DATABASE_URL" -f fcl_schema.sql
--
-- Trois tables, un seul principe : l'état de la flotte vit dans la base,
-- protégé par l'isolation SERIALIZABLE + un contrôle de concurrence optimiste
-- sur la colonne `version`. Aucun agent ne détient d'état local.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- missions — une unité de travail de haut niveau
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS missions (
    mission_id  UUID        NOT NULL DEFAULT gen_random_uuid(),
    title       STRING      NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_missions PRIMARY KEY (mission_id)
);

-- ----------------------------------------------------------------------------
-- state_ledger — le registre d'état : une ligne = une tâche revendiquable
--
--   status              pending -> claimed -> done | failed
--   version             garde optimiste ; incrémentée à CHAQUE écriture
--   claimed_by          agent détenteur du bail courant
--   agent_access_level  niveau d'accès du détenteur (alimente la provenance)
--   lease_expires_at    échéance du bail ; si dépassée, la tâche est reprenable
--                       par un autre agent (FAILOVER)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS state_ledger (
    task_id             UUID        NOT NULL DEFAULT gen_random_uuid(),
    mission_id          UUID        NOT NULL,
    task_type           STRING      NOT NULL,          -- scrape | synthesize | validate
    payload             JSONB       NOT NULL DEFAULT '{}',
    status              STRING      NOT NULL DEFAULT 'pending',
    version             INT8        NOT NULL DEFAULT 0,
    claimed_by          STRING          NULL,
    agent_access_level  STRING          NULL,
    lease_expires_at    TIMESTAMPTZ     NULL,
    result              JSONB           NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_state_ledger PRIMARY KEY (task_id),
    CONSTRAINT fk_state_ledger_mission
        FOREIGN KEY (mission_id) REFERENCES missions (mission_id) ON DELETE CASCADE,
    CONSTRAINT chk_status
        CHECK (status IN ('pending', 'claimed', 'in_progress', 'done', 'failed'))
);

-- Index de revendication : un agent cherche les tâches de son type qui sont
-- soit neuves (pending), soit à bail expiré. Cet index rend le SELECT ... FOR
-- UPDATE ciblé et bon marché sous forte concurrence.
CREATE INDEX IF NOT EXISTS idx_ledger_claimable
    ON state_ledger (task_type, status, lease_expires_at);

CREATE INDEX IF NOT EXISTS idx_ledger_mission
    ON state_ledger (mission_id);

-- ----------------------------------------------------------------------------
-- provenance_ledger — piste d'audit signée, append-only
--
-- Chaque transition d'état écrit une ligne ici : QUI, QUEL niveau d'accès,
-- QUELLE action, de QUELLE version vers QUELLE version, + signature
-- déterministe. C'est la couche « provenance signée ».
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS provenance_ledger (
    id                  UUID        NOT NULL DEFAULT gen_random_uuid(),
    task_id             UUID        NOT NULL,
    agent_id            STRING      NOT NULL,
    agent_access_level  STRING      NOT NULL,
    action              STRING      NOT NULL,          -- claim | reclaim | complete | fail
    prev_version        INT8        NOT NULL,
    new_version         INT8        NOT NULL,
    signature           STRING      NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_provenance PRIMARY KEY (id),
    CONSTRAINT fk_provenance_task
        FOREIGN KEY (task_id) REFERENCES state_ledger (task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_provenance_task
    ON provenance_ledger (task_id, created_at);

-- ----------------------------------------------------------------------------
-- task_embeddings — représentation vectorielle des tâches (recherche sémantique)
--
-- NOTE D'ARCHITECTURE : aucun script Python actuel n'interroge cette table.
-- Elle fait néanmoins partie de l'architecture prévue (récupération sémantique
-- des tâches / provenance) et doit rester dans le DDL public.
--
-- VECTOR(1536) = dimension d'un embedding OpenAI text-embedding-3-small.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS task_embeddings (
    task_id     UUID         NOT NULL,
    embedding   VECTOR(1536) NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_task_embeddings PRIMARY KEY (task_id),
    CONSTRAINT fk_task_embeddings_task
        FOREIGN KEY (task_id) REFERENCES state_ledger (task_id) ON DELETE CASCADE
);

CREATE VECTOR INDEX idx_task_embedding ON task_embeddings (embedding);
