"""
seed_handler — cree une mission de demonstration et ses taches.
Medusa Black Labs · Apache 2.0

Invocation manuelle :
    aws lambda invoke --function-name fcl-mission-seeder out.json

Payload optionnel :
    {"title": "Rapport Q3", "chunks_per_type": 3}
"""

from __future__ import annotations

import logging

import psycopg
from psycopg.rows import dict_row

from fcl_core import get_connection

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TASK_TYPES = ("scrape", "synthesize", "validate")


def lambda_handler(event, context):
    title = (event or {}).get("title", "Rapport marche semi-conducteurs Q3")
    chunks = int((event or {}).get("chunks_per_type", 3))

    conn = get_connection()

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "INSERT INTO missions (title) VALUES (%s) RETURNING mission_id",
            (title,),
        )
        mission_id = cur.fetchone()["mission_id"]

        for task_type in TASK_TYPES:
            for i in range(chunks):
                cur.execute(
                    """
                    INSERT INTO state_ledger (mission_id, task_type, payload)
                    VALUES (%s, %s, %s)
                    """,
                    (mission_id, task_type, psycopg.types.json.Jsonb({"chunk": i})),
                )

    total = chunks * len(TASK_TYPES)
    logger.info("mission %s creee — %s taches", mission_id, total)
    return {"mission_id": str(mission_id), "title": title, "tasks_created": total}
