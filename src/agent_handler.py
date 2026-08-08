"""
agent_handler — handler Lambda d'un agent de la flotte.
Medusa Black Labs · Apache 2.0

UN SEUL fichier pour TOUS les agents. Ce qui differencie un agent d'un autre
tient entierement dans ses variables d'environnement (AGENT_ID, TASK_TYPE,
ACCESS_LEVEL) definies dans template.yaml.

Ajouter un 4e agent a la flotte = ajouter un bloc YAML. Zero code.

Contrat d'execution :
  - draine les taches de son TASK_TYPE tant qu'il reste du budget de temps
  - s'arrete proprement avant le timeout Lambda (garde de securite)
  - si la Lambda meurt malgre tout : le bail expire, un autre agent reprend
"""

from __future__ import annotations

import logging
import os
import random
import time

from fcl_core import (
    RetryExhausted,
    claim_task,
    complete_task,
    fail_task,
    get_connection,
    run_in_transaction,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

AGENT_ID = os.environ["AGENT_ID"]
TASK_TYPE = os.environ["TASK_TYPE"]
ACCESS_LEVEL = os.environ["ACCESS_LEVEL"]

# Marge avant le timeout Lambda. En dessous, on ne revendique plus de nouvelle
# tache : mieux vaut finir proprement que se faire couper en pleine ecriture.
SAFETY_MARGIN_MS = 10_000


def do_work(task_id, payload: dict) -> dict:
    """
    Substitut du vrai travail (appel LLM, scraping, validation...).
    C'est le seul endroit a remplacer pour brancher une charge reelle.
    """
    time.sleep(random.uniform(0.2, 0.8))
    return {"by": AGENT_ID, "task": str(task_id), "payload": payload, "ok": True}


def _remaining_ms(context) -> int:
    try:
        return context.get_remaining_time_in_millis()
    except AttributeError:
        return 60_000  # execution locale / test


def lambda_handler(event, context):
    conn = get_connection()

    processed = 0
    failovers = 0
    failures = 0

    while _remaining_ms(context) > SAFETY_MARGIN_MS:

        # --- 1. Revendiquer -------------------------------------------------
        try:
            claimed = run_in_transaction(
                conn,
                lambda cur: claim_task(
                    cur,
                    agent_id=AGENT_ID,
                    access_level=ACCESS_LEVEL,
                    task_type=TASK_TYPE,
                ),
            )
        except RetryExhausted as exc:
            logger.warning("revendication abandonnee: %s", exc)
            break

        if not claimed:
            logger.info("aucune tache %s disponible", TASK_TYPE)
            break

        task_id = claimed["task_id"]
        version = claimed["version"]
        if claimed["failover"]:
            failovers += 1

        logger.info("CLAIM %s (v%s)", str(task_id)[:8], version)

        # --- 2. Travailler --------------------------------------------------
        try:
            result = do_work(task_id, event.get("payload", {}))
            terminal = "complete"
        except Exception as exc:
            logger.exception("travail echoue sur %s", str(task_id)[:8])
            result = str(exc)
            terminal = "fail"

        # --- 3. Cloturer ----------------------------------------------------
        # Si l'ecriture echoue ou si le bail a ete vole, on ne force RIEN.
        # La tache reste dans le ledger et sera reprise. Pas de perte silencieuse.
        try:
            if terminal == "complete":
                ok = run_in_transaction(
                    conn,
                    lambda cur: complete_task(
                        cur, task_id=task_id, version=version,
                        agent_id=AGENT_ID, access_level=ACCESS_LEVEL,
                        result=result,
                    ),
                )
                if ok:
                    processed += 1
                else:
                    logger.info("bail perdu sur %s — laissee a la flotte", str(task_id)[:8])
            else:
                run_in_transaction(
                    conn,
                    lambda cur: fail_task(
                        cur, task_id=task_id, version=version,
                        agent_id=AGENT_ID, access_level=ACCESS_LEVEL,
                        error=result,
                    ),
                )
                failures += 1

        except RetryExhausted as exc:
            logger.warning("cloture abandonnee sur %s: %s", str(task_id)[:8], exc)
            break

    summary = {
        "agent_id": AGENT_ID,
        "task_type": TASK_TYPE,
        "processed": processed,
        "failovers_recovered": failovers,
        "failures": failures,
    }
    logger.info("fin d'invocation: %s", summary)
    return summary
