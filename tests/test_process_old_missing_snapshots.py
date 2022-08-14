from unittest.mock import patch

from open_bus_siri_etl import parallel_process_old_missing_snapshots


@patch('open_bus_siri_etl.parallel_process_old_missing_snapshots.iterate_pending_snapshots')
def test_iterate_snapshot_id_batches(mock_iterate_pending_snapshots):
    # no pending snapshots
    mock_iterate_pending_snapshots.return_value = iter([])
    assert list(parallel_process_old_missing_snapshots.iterate_snapshot_id_batches(60)) == []
    # one pending snapshot
    mock_iterate_pending_snapshots.return_value = iter(['2019/01/01/00/00'])
    assert list(parallel_process_old_missing_snapshots.iterate_snapshot_id_batches(60)) == [
        ('2019/01/01/00/00', '2019/01/01/00/00')
    ]
    # two consecutive pending snapshots
    mock_iterate_pending_snapshots.return_value = iter(['2019/01/01/00/00', '2019/01/01/00/01'])
    assert list(parallel_process_old_missing_snapshots.iterate_snapshot_id_batches(60)) == [
        ('2019/01/01/00/00', '2019/01/01/00/01')
    ]
    # two non-consecutive pending snapshots
    mock_iterate_pending_snapshots.return_value = iter(['2019/01/01/00/00', '2019/01/01/00/02'])
    assert list(parallel_process_old_missing_snapshots.iterate_snapshot_id_batches(60)) == [
        ('2019/01/01/00/00', '2019/01/01/00/00'),
        ('2019/01/01/00/02', '2019/01/01/00/02')
    ]
    # consecutive and non-consecutive snapshots
    mock_iterate_pending_snapshots.return_value = iter([
        '2019/01/01/00/00',
        '2019/01/01/00/02', '2019/01/01/00/03', '2019/01/01/00/04',
        '2020/02/01/00/05', '2020/02/01/00/06',
        '2023/02/01/00/05'
    ])
    assert list(parallel_process_old_missing_snapshots.iterate_snapshot_id_batches(60)) == [
        ('2019/01/01/00/00', '2019/01/01/00/00'),
        ('2019/01/01/00/02', '2019/01/01/00/04'),
        ('2020/02/01/00/05', '2020/02/01/00/06'),
        ('2023/02/01/00/05', '2023/02/01/00/05'),
    ]
    # batch with more then 60 snapshots
    mock_iterate_pending_snapshots.return_value = iter([
        '2019/01/01/00/00',
        '2019/01/01/00/02', '2019/01/01/00/03', '2019/01/01/00/04',
        '2020/02/01/00/05', '2020/02/01/00/06',
        *['2021/11/25/21/{:02d}'.format(i) for i in range(0, 60)],
        *['2021/11/25/22/{:02d}'.format(i) for i in range(0, 10)],
        '2023/02/01/00/05'
    ])
    assert list(parallel_process_old_missing_snapshots.iterate_snapshot_id_batches(60)) == [
        ('2019/01/01/00/00', '2019/01/01/00/00'),
        ('2019/01/01/00/02', '2019/01/01/00/04'),
        ('2020/02/01/00/05', '2020/02/01/00/06'),
        ('2021/11/25/21/00', '2021/11/25/21/59'),
        ('2021/11/25/22/00', '2021/11/25/22/09'),
        ('2023/02/01/00/05', '2023/02/01/00/05'),
    ]