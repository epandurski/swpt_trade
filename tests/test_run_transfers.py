from datetime import timedelta
from swpt_trade.process_transfers import process_rescheduled_transfers
from swpt_trade.models import (
    TransferAttempt,
    TriggerTransferSignal,
    DispatchingStatus,
    WorkerCollecting,
    WorkerSending,
    WorkerReceiving,
    WorkerDispatching,
    StartSendingSignal,
    StartDispatchingSignal,
)
from swpt_trade.process_transfers import (
    signal_dispatching_statuses_ready_to_send,
    update_dispatching_statuses_with_everything_sent,
    signal_dispatching_statuses_ready_to_dispatch,
    delete_dispatching_statuses_with_everything_dispatched,
)


def test_process_rescheduled_transfers(app, db_session, current_ts):
    ta1 = TransferAttempt(
            collector_id=666,
            turn_id=1,
            debtor_id=222,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1000.5,
            collection_started_at=current_ts,
            recipient="account123",
            recipient_version=1,
            rescheduled_for=current_ts - timedelta(minutes=10),
    )
    ta2 = TransferAttempt(
            collector_id=666,
            turn_id=1,
            debtor_id=333,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1000.5,
            collection_started_at=current_ts,
            recipient="account123",
            recipient_version=1,
            rescheduled_for=None,
    )
    ta3 = TransferAttempt(
            collector_id=666,
            turn_id=1,
            debtor_id=444,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1000.5,
            collection_started_at=current_ts,
            recipient="account123",
            recipient_version=1,
            rescheduled_for=current_ts + timedelta(days=10),
    )
    db_session.add(ta1)
    db_session.add(ta2)
    db_session.add(ta3)
    db_session.commit()

    process_rescheduled_transfers()
    attempts = TransferAttempt.query.all()
    attempts.sort(key=lambda x: x.debtor_id)
    assert len(attempts) == 3
    assert attempts[0].debtor_id == 222
    assert attempts[0].rescheduled_for is None
    assert attempts[1].debtor_id == 333
    assert attempts[1].rescheduled_for is None
    assert attempts[2].debtor_id == 444
    assert attempts[2].rescheduled_for is not None

    tts = TriggerTransferSignal.query.all()
    assert len(tts) == 1
    assert tts[0].collector_id == 666
    assert tts[0].turn_id == 1
    assert tts[0].debtor_id == 222
    assert tts[0].creditor_id == 123
    assert tts[0].is_dispatching is True


def test_signal_dispatching_statuses_ready_to_send(
        mocker,
        app,
        db_session,
        current_ts,
):
    mocker.patch("swpt_trade.process_transfers.INSERT_BATCH_SIZE", new=1)
    mocker.patch("swpt_trade.process_transfers.SELECT_BATCH_SIZE", new=1)

    db_session.add(
        DispatchingStatus(
            collector_id=888,
            turn_id=1,
            debtor_id=666,
            amount_to_collect=2000,
            amount_to_send=0,
            amount_to_receive=0,
            number_to_receive=0,
            amount_to_dispatch=2000,
            awaiting_signal_flag=False,
        )
    )
    db_session.add(
        DispatchingStatus(
            collector_id=999,
            turn_id=1,
            debtor_id=666,
            amount_to_collect=1000,
            amount_to_send=0,
            amount_to_receive=0,
            number_to_receive=0,
            amount_to_dispatch=1000,
            awaiting_signal_flag=False,
        )
    )
    db_session.add(
        WorkerCollecting(
            collector_id=888,
            turn_id=1,
            debtor_id=666,
            creditor_id=123,
            amount=2000,
            collected=False,
            purge_after=current_ts,
        )
    )
    db_session.commit()

    signal_dispatching_statuses_ready_to_send()

    dss = DispatchingStatus.query.all()
    dss.sort(key=lambda x: x.collector_id)
    assert len(dss) == 2
    assert dss[0].collector_id == 888
    assert dss[0].awaiting_signal_flag is False
    assert dss[1].collector_id == 999
    assert dss[1].awaiting_signal_flag is True

    ssss = StartSendingSignal.query.all()
    assert len(ssss) == 1
    assert ssss[0].collector_id == 999
    assert ssss[0].turn_id == 1
    assert ssss[0].debtor_id == 666
    assert ssss[0].inserted_at >= current_ts
    assert len(StartDispatchingSignal.query.all()) == 0

    signal_dispatching_statuses_ready_to_send()
    assert len(StartSendingSignal.query.all()) == 1
    assert len(StartDispatchingSignal.query.all()) == 0


def test_update_dispatching_statuses_with_everything_sent(
        mocker,
        app,
        db_session,
        current_ts,
):
    mocker.patch("swpt_trade.process_transfers.INSERT_BATCH_SIZE", new=1)
    mocker.patch("swpt_trade.process_transfers.SELECT_BATCH_SIZE", new=1)

    db_session.add(
        DispatchingStatus(
            collector_id=888,
            turn_id=1,
            debtor_id=666,
            amount_to_collect=2000,
            total_collected_amount=2000,
            amount_to_send=0,
            amount_to_receive=0,
            number_to_receive=0,
            amount_to_dispatch=2000,
            awaiting_signal_flag=False,
            started_sending=True,
            all_sent=False,
        )
    )
    db_session.add(
        DispatchingStatus(
            collector_id=999,
            turn_id=1,
            debtor_id=666,
            amount_to_collect=1000,
            total_collected_amount=1000,
            amount_to_send=0,
            amount_to_receive=0,
            number_to_receive=0,
            amount_to_dispatch=1000,
            awaiting_signal_flag=False,
            started_sending=True,
            all_sent=False,
        )
    )
    db_session.add(
        WorkerSending(
            from_collector_id=888,
            turn_id=1,
            debtor_id=666,
            to_collector_id=555,
            amount=2000,
            purge_after=current_ts,
        )
    )
    db_session.commit()

    update_dispatching_statuses_with_everything_sent()

    dss = DispatchingStatus.query.all()
    dss.sort(key=lambda x: x.collector_id)
    assert len(dss) == 2
    assert dss[0].collector_id == 888
    assert dss[0].all_sent is False
    assert dss[1].collector_id == 999
    assert dss[1].all_sent is True
    assert len(StartSendingSignal.query.all()) == 0
    assert len(StartDispatchingSignal.query.all()) == 0

    update_dispatching_statuses_with_everything_sent()
    assert len(StartSendingSignal.query.all()) == 0
    assert len(StartDispatchingSignal.query.all()) == 0


def test_signal_dispatching_statuses_ready_to_dispatch(
        mocker,
        app,
        db_session,
        current_ts,
):
    mocker.patch("swpt_trade.process_transfers.INSERT_BATCH_SIZE", new=1)
    mocker.patch("swpt_trade.process_transfers.SELECT_BATCH_SIZE", new=1)

    db_session.add(
        DispatchingStatus(
            collector_id=888,
            turn_id=1,
            debtor_id=666,
            amount_to_collect=2000,
            total_collected_amount=2000,
            amount_to_send=0,
            amount_to_receive=0,
            number_to_receive=0,
            amount_to_dispatch=2000,
            awaiting_signal_flag=False,
            started_sending=True,
            all_sent=True,
        )
    )
    db_session.add(
        DispatchingStatus(
            collector_id=999,
            turn_id=1,
            debtor_id=666,
            amount_to_collect=1000,
            total_collected_amount=1000,
            amount_to_send=0,
            amount_to_receive=0,
            number_to_receive=0,
            amount_to_dispatch=1000,
            awaiting_signal_flag=False,
            started_sending=True,
            all_sent=True,
        )
    )
    db_session.add(
        WorkerReceiving(
            to_collector_id=888,
            turn_id=1,
            debtor_id=666,
            from_collector_id=555,
            expected_amount=2000,
            received_amount=0,
            purge_after=current_ts,
        )
    )
    db_session.commit()

    signal_dispatching_statuses_ready_to_dispatch()

    dss = DispatchingStatus.query.all()
    dss.sort(key=lambda x: x.collector_id)
    assert len(dss) == 2
    assert dss[0].collector_id == 888
    assert dss[0].awaiting_signal_flag is False
    assert dss[1].collector_id == 999
    assert dss[1].awaiting_signal_flag is True

    assert len(StartSendingSignal.query.all()) == 0
    sdss = StartDispatchingSignal.query.all()
    assert len(sdss) == 1
    assert sdss[0].collector_id == 999
    assert sdss[0].turn_id == 1
    assert sdss[0].debtor_id == 666
    assert sdss[0].inserted_at >= current_ts

    signal_dispatching_statuses_ready_to_dispatch()
    assert len(StartSendingSignal.query.all()) == 0
    assert len(StartDispatchingSignal.query.all()) == 1


def test_delete_dispatching_statuses_with_everything_dispatched(
        mocker,
        app,
        db_session,
        current_ts,
):
    mocker.patch("swpt_trade.process_transfers.INSERT_BATCH_SIZE", new=1)
    mocker.patch("swpt_trade.process_transfers.SELECT_BATCH_SIZE", new=1)

    db_session.add(
        DispatchingStatus(
            collector_id=888,
            turn_id=1,
            debtor_id=666,
            amount_to_collect=2000,
            total_collected_amount=2000,
            amount_to_send=0,
            amount_to_receive=0,
            number_to_receive=0,
            total_received_amount=0,
            all_received=True,
            amount_to_dispatch=2000,
            awaiting_signal_flag=False,
            started_sending=True,
            all_sent=True,
            started_dispatching=True,
        )
    )
    db_session.add(
        DispatchingStatus(
            collector_id=999,
            turn_id=1,
            debtor_id=666,
            amount_to_collect=1000,
            total_collected_amount=1000,
            amount_to_send=0,
            amount_to_receive=0,
            number_to_receive=0,
            total_received_amount=0,
            all_received=True,
            amount_to_dispatch=1000,
            awaiting_signal_flag=False,
            started_sending=True,
            all_sent=True,
            started_dispatching=True,
        )
    )
    db_session.add(
        WorkerDispatching(
            collector_id=888,
            turn_id=1,
            debtor_id=666,
            creditor_id=123,
            amount=2000,
            purge_after=current_ts,
        )
    )
    db_session.commit()

    delete_dispatching_statuses_with_everything_dispatched()

    dss = DispatchingStatus.query.all()
    dss.sort(key=lambda x: x.collector_id)
    assert len(dss) == 1
    assert dss[0].collector_id == 888
    assert dss[0].awaiting_signal_flag is False
    assert len(StartSendingSignal.query.all()) == 0
    assert len(StartDispatchingSignal.query.all()) == 0

    delete_dispatching_statuses_with_everything_dispatched()
    assert len(DispatchingStatus.query.all()) == 1
    assert len(StartSendingSignal.query.all()) == 0
    assert len(StartDispatchingSignal.query.all()) == 0
