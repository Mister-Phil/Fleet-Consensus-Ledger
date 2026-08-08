"""
fcl_core — noyau partage de la Fleet Consensus Ledger.
Medusa Black Labs · Apache 2.0

Deploye en AWS Lambda Layer. Contient tout ce qui doit rester identique
entre agents : connexion, retry 40001, provenance signee, claim/complete.

Un handler d'agent n'a AUCUNE logique de consensus. Il appelle ce module.
C'est ce qui rend l'ajout d'un 4e agent gratuit.
"""

from __future__ import annotations

import functools
import hashlib
import json
import logging
import os
import random
import time

import boto3
import psycopg
from psycopg import errors as pg_errors
from psycopg.rows import dict_row

logger = logging.getLogger()
logger.setLevel(logging.INFO)

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
BASE_BACKOFF = 0.05
MAX_BACKOFF = 2.0
LEASE_SECONDS = int(os.environ.get("LEASE_SECONDS", "30"))
SECRET_NAME = os.environ.get("COCKROACH_SECRET_NAME", "fcl/cockroachdb")

CW_NAMESPACE = "FleetConsensusLedger"


# ----------------------------------------------------------------------
# CONNEXION — secret mis en cache hors handler (survit aux invocations chaudes)
# ----------------------------------------------------------------------

@functools.lru_cache(maxsize=1)
def _database_url() -> str:
    """Lit la chaine de connexion depuis Secrets Manager. Jamais en clair."""
    client = boto3.client("secretsmanager")
    raw = client.get_secret_value(SecretId=SECRET_NAME)["SecretString"]
    try:
        return json.loads(raw)["database_url"]
    except (json.JSONDecodeError, KeyError):
        return raw  # secret stocke en chaine brute


_conn = None


def get_connection():
    """
    Reutilise la connexion entre invocations chaudes. Si le container a ete gele
    trop longtemps, la connexion est morte : on la recree silencieusement.
    """
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg.connect(_database_url(), autocommit=True)
    else:
        try:
            with _conn.cursor() as cur:
                cur.execute("SELECT 1")
        except psycopg.Error:
            _conn = psycopg.connect(_database_url(), autocommit=True)
    return _conn


# ----------------------------------------------------------------------
# METRIQUE — alimente l'alarme CloudWatch fcl-serialization-conflicts-high
# ----------------------------------------------------------------------

def _emit_conflict_metric(count: int) -> None:
    if count <= 0:
        return
    try:
        boto3.client("cloudwatch").put_metric_data(
            Namespace=CW_NAMESPACE,
            MetricData=[{
                "MetricName": "SerializationConflicts",
                "Value": count,
                "Unit": "Count",
            }],
        )
    except Exception:
        logger.warning("emission metrique CloudWatch echouee", exc_info=False)


# ----------------------------------------------------------------------
# RETRY LOOP SERIALIZABLE — le contrat CockroachDB
# ----------------------------------------------------------------------
# Sous SERIALIZABLE, CockroachDB n'attend pas : il avorte la transaction
# perdante avec SQLSTATE 40001. Rejouer cote applicatif est OBLIGATOIRE.
# Le jitter evite que toute la flotte rejoue au meme instant.
# ----------------------------------------------------------------------

class RetryExhausted(Exception):
    """Conflit non resolu apres MAX_RETRIES tentatives."""


def run_in_transaction(conn, fn):
    backoff = BASE_BACKOFF
    conflicts = 0

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with conn.transaction():
                with conn.cursor(row_factory=dict_row) as cur:
                    result = fn(cur)
            _emit_conflict_metric(conflicts)
            return result

        except pg_errors.SerializationFailure:
            conflicts += 1
            if attempt == MAX_RETRIES:
                _emit_conflict_metric(conflicts)
                raise RetryExhausted(f"40001 non resolu apres {MAX_RETRIES} tentatives")

            sleep_for = min(backoff, MAX_BACKOFF) * (1 + random.random())
            logger.info("conflit 40001 (essai %s) -> retry dans %.0fms",
                        attempt, sleep_for * 1000)
            time.sleep(sleep_for)
            backoff *= 2

    raise RetryExhausted("unreachable")


# ----------------------------------------------------------------------
# PROVENANCE — signature de chaque ecriture d'etat
# ----------------------------------------------------------------------

def sign(agent_id: str, task_id: str, new_version: int, action: str) -> str:
    return hashlib.sha256(
        f"{agent_id}|{task_id}|{new_version}|{action}".encode()
    ).hexdigest()


def record_provenance(cur, *, task_id, agent_id, access_level,
                      action, prev_version, new_version) -> None:
    cur.execute(
        """
        INSERT INTO provenance_ledger
            (task_id, agent_id, agent_access_level, action,
             prev_version, new_version, signature)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (task_id, agent_id, access_level, action, prev_version, new_version,
         sign(agent_id, str(task_id), new_version, action)),
    )


# ----------------------------------------------------------------------
# CLAIM — revendication atomique + reprise des baux expires (failover)
# ----------------------------------------------------------------------

def claim_task(cur, *, agent_id: str, access_level: str, task_type: str):
    cur.execute(
        """
        SELECT task_id, version, status, claimed_by
        FROM state_ledger
        WHERE task_type = %s
          AND (
                status = 'pending'
                OR (status IN ('claimed', 'in_progress') AND lease_expires_at < now())
              )
        ORDER BY created_at
        LIMIT 1
        FOR UPDATE
        """,
        (task_type,),
    )
    row = cur.fetchone()
    if not row:
        return None

    is_failover = row["status"] != "pending"
    prev_version = row["version"]

    cur.execute(
        """
        UPDATE state_ledger
        SET status             = 'claimed',
            claimed_by         = %s,
            agent_access_level = %s,
            lease_expires_at   = now() + (%s || ' seconds')::INTERVAL,
            version            = version + 1,
            updated_at         = now()
        WHERE task_id = %s AND version = %s
        RETURNING task_id, version
        """,
        (agent_id, access_level, LEASE_SECONDS, row["task_id"], prev_version),
    )
    updated = cur.fetchone()
    if not updated:
        return None  # course perdue

    record_provenance(
        cur, task_id=updated["task_id"], agent_id=agent_id,
        access_level=access_level,
        action="reclaim" if is_failover else "claim",
        prev_version=prev_version, new_version=updated["version"],
    )

    if is_failover:
        logger.info("FAILOVER: reprise de la tache abandonnee par %s", row["claimed_by"])

    return {
        "task_id": updated["task_id"],
        "version": updated["version"],
        "failover": is_failover,
    }


def complete_task(cur, *, task_id, version, agent_id, access_level, result) -> bool:
    cur.execute(
        """
        UPDATE state_ledger
        SET status = 'done', result = %s, lease_expires_at = NULL,
            version = version + 1, updated_at = now()
        WHERE task_id = %s AND version = %s
        RETURNING version
        """,
        (psycopg.types.json.Jsonb(result), task_id, version),
    )
    updated = cur.fetchone()
    if not updated:
        return False  # bail vole entre-temps : on laisse la tache a la flotte

    record_provenance(
        cur, task_id=task_id, agent_id=agent_id, access_level=access_level,
        action="complete", prev_version=version, new_version=updated["version"],
    )
    return True


def fail_task(cur, *, task_id, version, agent_id, access_level, error: str) -> bool:
    cur.execute(
        """
        UPDATE state_ledger
        SET status = 'failed', result = %s, lease_expires_at = NULL,
            version = version + 1, updated_at = now()
        WHERE task_id = %s AND version = %s
        RETURNING version
        """,
        (psycopg.types.json.Jsonb({"error": error}), task_id, version),
    )
    updated = cur.fetchone()
    if not updated:
        return False

    record_provenance(
        cur, task_id=task_id, agent_id=agent_id, access_level=access_level,
        action="fail", prev_version=version, new_version=updated["version"],
    )
    return True
