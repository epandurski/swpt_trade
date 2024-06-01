import logging
import time
import click
from datetime import datetime, timezone
from flask import current_app
from flask.cli import with_appcontext
from swpt_trade import procedures
from .common import swpt_trade


@swpt_trade.command("roll_worker_turns")
@with_appcontext
@click.option(
    "-w",
    "--wait",
    type=float,
    help=(
        "Poll the solver's database for new or progressed turns every"
        " FLOAT seconds. If not specified, the value of the"
        " APP_ROLL_WORKER_TURNS_WAIT environment variable will be used,"
        " defaulting to 60 seconds if empty."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def roll_worker_turns(wait, quit_early):
    """Run a process which synchronizes worker server's turn states
    with solver server's turn states.
    """
    from swpt_trade.run_turn_subphases import (
        run_phase1_subphase0,
        run_phase2_subphase0,
        run_phase2_subphase5,
        run_phase3_subphase0,
        run_phase3_subphase5,
    )

    wait_seconds = (
        wait
        if wait is not None
        else current_app.config["APP_ROLL_WORKER_TURNS_WAIT"]
    )
    logger = logging.getLogger(__name__)
    logger.info("Started rolling worker turns.")

    while True:
        unfinished_turn_ids = set()

        for unfinished_turn in procedures.get_unfinished_turns():
            procedures.update_or_create_worker_turn(unfinished_turn)
            unfinished_turn_ids.add(unfinished_turn.turn_id)

        overlooked_turn_ids = [
            # These are the IDs of worker turns which are not finished
            # yet, but their corresponding solver turns have been
            # finished already.
            #
            # NOTE: This can happen in the rare case when, for the
            # given turn, the worker has not been assigned to
            # participate in any transfers, and the solver finished
            # the turn without the worker even noticing.
            turn_id
            for turn_id in procedures.get_unfinished_worker_turn_ids()
            if turn_id not in unfinished_turn_ids
        ]
        for finished_turn in procedures.get_turns_by_ids(overlooked_turn_ids):
            assert finished_turn.phase == 4
            procedures.update_or_create_worker_turn(finished_turn)

        for worker_turn in procedures.get_pending_worker_turns():
            current_ts = datetime.now(tz=timezone.utc)
            turn_id = worker_turn.turn_id
            phase = worker_turn.phase
            subphase = worker_turn.worker_turn_subphase
            assert 1 <= phase <= 3
            assert 0 <= subphase < 10

            if phase == 1 and subphase == 0:
                run_phase1_subphase0(turn_id)
            elif phase == 2 and subphase == 0:
                run_phase2_subphase0(turn_id)
            elif phase == 2 and subphase == 5:
                phase_deadline = worker_turn.phase_deadline
                cushion_interval = min(
                    # We ensure that the cushion interval does not
                    # exceed 10% of the time between the start of the
                    # turn, and the phase deadline.
                    current_app.config["APP_TURN_PHASE_CUSHION_PERIOD"],
                    0.1 * (phase_deadline - worker_turn.started_at),
                )
                if current_ts > phase_deadline - cushion_interval:
                    run_phase2_subphase5(turn_id)
            elif phase == 3 and subphase == 0:
                run_phase3_subphase0(turn_id)
            elif phase == 3 and subphase == 5:
                run_phase3_subphase5(turn_id)
            else:  # pragma: no cover
                raise RuntimeError(
                    f"Invalid subphase for worker turn {worker_turn.turn_id}."
                )

        if quit_early:
            break
        if wait_seconds > 0.0:  # pragma: no cover
            time.sleep(wait_seconds)
