"""
mcp/server.py — Fleet Consensus Ledger expose comme serveur MCP.
Medusa Black Labs · Apache 2.0

Permet a Claude Code / Cursor d'interroger et de piloter la flotte directement
depuis l'IDE : voir l'etat des taches, revendiquer, cloturer, consulter la
provenance — sans quitter le chat de l'agent.

Lancement local :
    export DATABASE_URL="postgresql://..."
    python mcp/server.py

Ce serveur reutilise fcl_core (meme module que les Lambdas) : la logique de
consensus n'existe qu'a UN seul endroit dans tout le depot.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "layer"))

from mcp.server.fastmcp import FastMCP  # pip install mcp
from psycopg.rows import dict_row

from fcl_core import (
    claim_task,
    complete_task,
    get_connection,
    run_in_transaction,
)

mcp = FastMCP("fleet-consensus-ledger")


@mcp.tool()
def ledger_status() -> dict:
    """Vue d'ensemble du ledger : nombre de taches par statut."""
    conn = get_connection()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT status, count(*) AS n FROM state_ledger GROUP BY status"
        )
        return {row["status"]: row["n"] for row in cur.fetchall()}


@mcp.tool()
def list_pending_tasks(task_type: str | None = None, limit: int = 10) -> list[dict]:
    """Liste les taches en attente ou dont le bail a expire (revendicables)."""
    conn = get_connection()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT task_id, task_type, status, claimed_by, lease_expires_at
            FROM state_ledger
            WHERE (status = 'pending'
                   OR (status IN ('claimed','in_progress') AND lease_expires_at < now()))
              AND (%s IS NULL OR task_type = %s)
            ORDER BY created_at
            LIMIT %s
            """,
            (task_type, task_type, limit),
        )
        return [dict(r, task_id=str(r["task_id"])) for r in cur.fetchall()]


@mcp.tool()
def claim(agent_id: str, access_level: str, task_type: str) -> dict:
    """Revendique une tache pour un agent donne. Gere le failover automatiquement."""
    conn = get_connection()
    result = run_in_transaction(
        conn,
        lambda cur: claim_task(
            cur, agent_id=agent_id, access_level=access_level, task_type=task_type
        ),
    )
    if not result:
        return {"claimed": False, "reason": "aucune tache disponible"}
    return {"claimed": True, "task_id": str(result["task_id"]),
            "version": result["version"], "failover": result["failover"]}


@mcp.tool()
def complete(task_id: str, version: int, agent_id: str, access_level: str,
             result_json: str) -> dict:
    """Cloture une tache revendiquee. `result_json` = resultat serialise en JSON."""
    import json
    conn = get_connection()
    ok = run_in_transaction(
        conn,
        lambda cur: complete_task(
            cur, task_id=task_id, version=version, agent_id=agent_id,
            access_level=access_level, result=json.loads(result_json),
        ),
    )
    return {"completed": ok}


@mcp.tool()
def provenance(task_id: str) -> list[dict]:
    """Historique signe des ecritures sur une tache."""
    conn = get_connection()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT agent_id, agent_access_level, action, prev_version,
                   new_version, ts
            FROM provenance_ledger
            WHERE task_id = %s
            ORDER BY ts
            """,
            (task_id,),
        )
        return cur.fetchall()


if __name__ == "__main__":
    if not os.environ.get("DATABASE_URL"):
        print("ERREUR: DATABASE_URL manquante.", file=sys.stderr)
        sys.exit(1)
    mcp.run()
