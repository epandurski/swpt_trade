import logging
import time
import click
from datetime import datetime, timezone, timedelta
from flask import current_app
from flask.cli import with_appcontext
from swpt_trade.extensions import db
from swpt_trade import procedures
from .common import swpt_trade


@swpt_trade.command("roll_turns")
@with_appcontext
@click.option(
    "-p",
    "--period",
    type=str,
    help=(
        "Start a new turn every TEXT seconds."
        " If not specified, the value of the TURN_PERIOD environment"
        " variable will be used, defaulting to 1 day if empty. A unit"
        " can also be included in the value. For example, 10m would be"
        " equivalent to 600 seconds."
    ),
)
@click.option(
    "-o",
    "--period-offset",
    type=str,
    help=(
        "Start each turn TEXT seconds after the start of each period."
        " If not specified, the value of the TURN_PERIOD_OFFSET environment"
        " variable will be used, defaulting to 0 if empty. A unit can"
        " also be included in the value. For example, if the turn period"
        " is 1d, and the turn period offset is 1h, new turns will be"
        " started every day at 1:00am UTC time."
    ),
)
@click.option(
    "-c",
    "--check-interval",
    type=str,
    help=(
        "The process will wake up every TEXT seconds to check whether"
        " a new turn has to be started, or an already started turn"
        " has to be advanced. If not specified, the smaller among"
        " 1 minute, and 1/10th of TURN_PHASE1_DURATION will be used."
        " A unit can also be included in the value. For example, 2m"
        " would be equivalent to 120 seconds."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def roll_turns(period, period_offset, check_interval, quit_early):
    """Run a process which starts new turns, and advances started
    turns.

    Every turn consists of several phases. When one phase is
    completed, the turn advances to the next phase. The durations of
    phases 1 and 2 are controlled by the environment variables
    TURN_PHASE1_DURATION and TURN_PHASE2_DURATION. (Note that time
    units can also be included in values of these variables.)

    Another important environment variables which control the way
    trading turns work are: BASE_DEBTOR_INFO_LOCATOR, BASE_DEBTOR_ID,
    MAX_DISTANCE_TO_BASE, MIN_TRADE_AMOUNT.
    """
    from swpt_trade.utils import parse_timedelta
    from swpt_trade.solve_turn import try_to_advance_turn_to_phase3

    c = current_app.config
    period = parse_timedelta(period or c["TURN_PERIOD"])
    period_offset = parse_timedelta(period_offset or c["TURN_PERIOD_OFFSET"])
    phase1_duration = parse_timedelta(c["TURN_PHASE1_DURATION"])
    phase2_duration = parse_timedelta(c["TURN_PHASE2_DURATION"])
    max_commit_period = c["APP_TURN_MAX_COMMIT_PERIOD"]
    default_check_interval = timedelta(seconds=c["APP_ROLL_TURNS_WAIT"])
    check_interval = (
        parse_timedelta(check_interval)
        if check_interval
        else min(default_check_interval, phase1_duration / 10)
    )
    logger = logging.getLogger(__name__)
    logger.info("Started rolling turns.")

    while True:
        logger.debug("Trying to start a new turn or advance started turns.")
        check_began_at = datetime.now(tz=timezone.utc)
        started_turns = procedures.start_new_turn_if_possible(
            turn_period=period,
            turn_period_offset=period_offset,
            phase1_duration=phase1_duration,
            base_debtor_info_locator=c["BASE_DEBTOR_INFO_LOCATOR"],
            base_debtor_id=c["BASE_DEBTOR_ID"],
            max_distance_to_base=c["MAX_DISTANCE_TO_BASE"],
            min_trade_amount=c["MIN_TRADE_AMOUNT"],
        )
        for turn in started_turns:
            turn_id = turn.turn_id
            phase = turn.phase
            next_phase = phase + 1

            def log_attempt():
                logger.info(
                    "Turn %i, phase %i:"
                    " trying to advance to phase %i.",
                    turn_id, phase, next_phase
                )

            def log_result(is_done: bool):
                if is_done:
                    logger.info(
                        "Turn %i, phase %i:"
                        " successfully advanced to phase %i.",
                        turn_id, phase, next_phase
                    )
                else:  # pragma: no cover
                    logger.info(
                        "Turn %i, phase %i:"
                        " can not advance to phase %i.",
                        turn_id, phase, next_phase
                    )

            if phase == 1 and turn.phase_deadline < check_began_at:
                log_attempt()
                is_done = procedures.try_to_advance_turn_to_phase2(
                    turn_id=turn_id,
                    phase2_duration=phase2_duration,
                    max_commit_period=max_commit_period,
                    work_mem=current_app.config["SOLVER_INCREASED_WORK_MEM"],
                )
                log_result(is_done)

            elif phase == 2 and turn.phase_deadline < check_began_at:
                log_attempt()
                is_done = try_to_advance_turn_to_phase3(turn)
                log_result(is_done)

            elif phase == 3:
                log_attempt()
                is_done = procedures.try_to_advance_turn_to_phase4(turn_id)
                log_result(is_done)

        db.session.close()
        elapsed_time = datetime.now(tz=timezone.utc) - check_began_at
        wait_seconds = (check_interval - elapsed_time).total_seconds()

        if quit_early:
            break
        if wait_seconds > 0.0:  # pragma: no cover
            time.sleep(wait_seconds)
