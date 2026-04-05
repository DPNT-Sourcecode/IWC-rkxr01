# from queue_solution_legacy import Queue
# from task_types import TaskSubmission

# queue = Queue()

# task1 = TaskSubmission(user_id=1, provider="bank_statements", timestamp='2025-10-20 12:00:00')
# task2 = TaskSubmission(user_id=1, provider="bank_statements", timestamp='2025-10-20 11:00:00')
# task3 = TaskSubmission(user_id=1, provider="id_verification", timestamp='2025-10-20 11:00:00')

# print(queue.enqueue(task1))
# print(queue.enqueue(task2))
# print(queue.enqueue(task3))

from datetime import datetime

from .task_types import TaskSubmission, TaskDispatch
from .queue_solution_legacy import Queue

import pytest


@pytest.fixture
def queue():
    return Queue()


def make_task(
    provider: str, user_id: int, timestamp: str, metadata: dict | None = None
):
    return TaskSubmission(
        provider=provider,
        user_id=user_id,
        timestamp=datetime.fromisoformat(timestamp),
        metadata={} if metadata is None else metadata,
    )


#########################################
# TESTS EXAMPLES IN ORIGINAL SPEC
#########################################


def test_dependency_resolution_credit_check_enqueues_companies_house_first(queue):
    size = queue.enqueue(make_task("credit_check", 1, "2025-10-20 12:00:00"))

    assert size == 2
    assert queue.size == 2

    first = queue.dequeue()
    second = queue.dequeue()

    assert first == TaskDispatch(provider="companies_house", user_id=1)
    assert second == TaskDispatch(provider="credit_check", user_id=1)


def test_timestamp_ordering_older_timestamp_processed_first(queue):
    queue.enqueue(make_task("bank_statements", 1, "2025-10-20 12:05:00"))
    queue.enqueue(make_task("bank_statements", 2, "2025-10-20 12:00:00"))

    first = queue.dequeue()
    second = queue.dequeue()

    assert first == TaskDispatch(provider="bank_statements", user_id=2)
    assert second == TaskDispatch(provider="bank_statements", user_id=1)


def test_rule_of_three_moves_all_tasks_for_that_user_to_front(queue):
    assert queue.enqueue(make_task("companies_house", 1, "2025-10-20 12:00:00")) == 1
    assert queue.enqueue(make_task("bank_statements", 2, "2025-10-20 12:00:00")) == 2
    assert queue.enqueue(make_task("id_verification", 1, "2025-10-20 12:00:00")) == 3
    assert queue.enqueue(make_task("bank_statements", 1, "2025-10-20 12:00:00")) == 4

    assert queue.dequeue() == TaskDispatch(provider="companies_house", user_id=1)
    assert queue.dequeue() == TaskDispatch(provider="id_verification", user_id=1)
    assert queue.dequeue() == TaskDispatch(provider="bank_statements", user_id=1)
    assert queue.dequeue() == TaskDispatch(provider="bank_statements", user_id=2)


#########################################
# TESTS HELPER FUNCTIONS
#########################################


def test_size_reflects_current_pending_task_count(queue):
    assert queue.size == 0

    queue.enqueue(make_task("companies_house", 1, "2025-10-20 12:00:00"))
    assert queue.size == 1

    # credit_check pulls in companies_house dependency, so total grows by 2
    queue.enqueue(make_task("credit_check", 2, "2025-10-20 12:00:00"))
    assert queue.size == 3

    queue.dequeue()
    assert queue.size == 2

    queue.dequeue()
    queue.dequeue()
    assert queue.size == 0


def test_dequeue_returns_none_when_queue_is_empty(queue):
    assert queue.dequeue() is None


def test_purge_clears_queue_and_returns_true(queue):
    queue.enqueue(make_task("companies_house", 1, "2025-10-20 12:00:00"))
    queue.enqueue(make_task("bank_statements", 2, "2025-10-20 12:00:00"))

    assert queue.size == 2
    assert queue.purge() is True
    assert queue.size == 0
    assert queue.dequeue() is None


#########################################
# TESTS DEDUPLICATION LOGIC
#########################################


def test_enqueue_deduplicates_same_provider_and_user_when_newer_timestamp_arrives(
    queue,
):
    assert queue.enqueue(make_task("bank_statements", 7, "2025-10-20 12:00:00")) == 1

    # Same (provider, user_id), but newer timestamp: should not replace existing item.
    assert queue.enqueue(make_task("bank_statements", 7, "2025-10-20 12:05:00")) == 1
    assert queue.size == 1

    dispatched = queue.dequeue()
    assert dispatched == TaskDispatch(provider="bank_statements", user_id=7)
    assert queue.dequeue() is None


def test_enqueue_replaces_same_provider_and_user_when_earlier_timestamp_arrives(queue):
    assert queue.enqueue(make_task("companies_house", 7, "2025-10-20 12:05:00")) == 1

    # Same (provider, user_id), but earlier timestamp: should replace existing task.
    assert queue.enqueue(make_task("companies_house", 7, "2025-10-20 12:00:00")) == 1
    assert queue.size == 1

    # Add another task so we can verify the earlier replacement now sorts correctly.
    assert queue.enqueue(make_task("id_verification", 8, "2025-10-20 12:01:00")) == 2

    first = queue.dequeue()
    second = queue.dequeue()

    assert first == TaskDispatch(provider="companies_house", user_id=7)
    assert second == TaskDispatch(provider="id_verification", user_id=8)


#########################################
# TESTS PROVIDER PRIORITY LOGIC
#########################################


def test_bank_statements_is_deprioritized_for_non_rule_of_three_users(queue):
    queue.enqueue(make_task("bank_statements", 1, "2025-10-20 12:00:00"))
    queue.enqueue(make_task("id_verification", 1, "2025-10-20 12:01:00"))
    queue.enqueue(make_task("companies_house", 2, "2025-10-20 12:02:00"))

    assert queue.dequeue() == TaskDispatch(provider="id_verification", user_id=1)
    assert queue.dequeue() == TaskDispatch(provider="companies_house", user_id=2)
    assert queue.dequeue() == TaskDispatch(provider="bank_statements", user_id=1)


def test_bank_statements_is_last_within_prioritized_user_group(queue):
    queue.enqueue(make_task("bank_statements", 1, "2025-10-20 12:00:00"))
    queue.enqueue(make_task("id_verification", 1, "2025-10-20 12:01:00"))
    queue.enqueue(make_task("companies_house", 1, "2025-10-20 12:02:00"))

    assert queue.dequeue() == TaskDispatch(provider="id_verification", user_id=1)
    assert queue.dequeue() == TaskDispatch(provider="companies_house", user_id=1)
    assert queue.dequeue() == TaskDispatch(provider="bank_statements", user_id=1)


def test_non_bank_tasks_still_use_timestamp_ordering(queue):
    queue.enqueue(make_task("id_verification", 1, "2025-10-20 12:05:00"))
    queue.enqueue(make_task("companies_house", 2, "2025-10-20 12:00:00"))

    assert queue.dequeue() == TaskDispatch(provider="companies_house", user_id=2)
    assert queue.dequeue() == TaskDispatch(provider="id_verification", user_id=1)


#########################################
# TESTS AGE PROPERTY
#########################################


def test_age_returns_zero_when_queue_is_empty(queue):
    assert queue.age == 0


def test_age_returns_gap_between_oldest_and_newest_task(queue):
    queue.enqueue(make_task("id_verification", 1, "2025-10-20 12:00:00"))
    queue.enqueue(make_task("id_verification", 2, "2025-10-20 12:05:00"))

    assert queue.age == 300


def test_age_returns_zero_when_only_one_task_exists(queue):
    queue.enqueue(make_task("id_verification", 1, "2025-10-20 12:00:00"))

    assert queue.age == 0


def test_age_updates_after_earlier_duplicate_replacement(queue):
    queue.enqueue(make_task("id_verification", 1, "2025-10-20 12:05:00"))
    queue.enqueue(make_task("companies_house", 2, "2025-10-20 12:10:00"))

    assert queue.age == 300

    queue.enqueue(make_task("id_verification", 1, "2025-10-20 12:00:00"))

    assert queue.age == 600


#########################################
# TESTS BANK STATEMENT PRIORITISATION
#########################################


def test_aged_bank_statement_can_move_ahead_of_later_tasks(queue):
    queue.enqueue(make_task("id_verification", 1, "2025-10-20 12:00:00"))
    queue.enqueue(make_task("bank_statements", 2, "2025-10-20 12:01:00"))
    queue.enqueue(make_task("companies_house", 3, "2025-10-20 12:07:00"))

    assert queue.dequeue() == TaskDispatch(provider="id_verification", user_id=1)
    assert queue.dequeue() == TaskDispatch(provider="bank_statements", user_id=2)
    assert queue.dequeue() == TaskDispatch(provider="companies_house", user_id=3)


def test_aged_bank_statement_does_not_skip_older_timestamp(queue):
    queue.enqueue(make_task("companies_house", 1, "2025-10-20 12:00:00"))
    queue.enqueue(make_task("bank_statements", 2, "2025-10-20 12:01:00"))
    queue.enqueue(make_task("id_verification", 3, "2025-10-20 12:07:00"))

    assert queue.dequeue() == TaskDispatch(provider="companies_house", user_id=1)
    assert queue.dequeue() == TaskDispatch(provider="bank_statements", user_id=2)
    assert queue.dequeue() == TaskDispatch(provider="id_verification", user_id=3)


def test_aged_bank_statement_uses_fifo_on_tie(queue):
    queue.enqueue(make_task("id_verification", 1, "2025-10-20 12:00:00"))
    queue.enqueue(make_task("bank_statements", 2, "2025-10-20 12:02:00"))
    queue.enqueue(make_task("bank_statements", 1, "2025-10-20 12:02:00"))
    queue.enqueue(make_task("companies_house", 1, "2025-10-20 12:03:00"))
    queue.enqueue(make_task("companies_house", 3, "2025-10-20 12:10:00"))

    assert queue.dequeue() == TaskDispatch(provider="id_verification", user_id=1)
    assert queue.dequeue() == TaskDispatch(provider="bank_statements", user_id=2)
    assert queue.dequeue() == TaskDispatch(provider="bank_statements", user_id=1)
    assert queue.dequeue() == TaskDispatch(provider="companies_house", user_id=1)
    assert queue.dequeue() == TaskDispatch(provider="companies_house", user_id=3)


def test_non_aged_bank_statement_still_deprioritized(queue):
    queue.enqueue(make_task("bank_statements", 1, "2025-10-20 12:00:00"))
    queue.enqueue(make_task("id_verification", 2, "2025-10-20 12:01:00"))
    queue.enqueue(make_task("companies_house", 3, "2025-10-20 12:04:00"))

    assert queue.dequeue() == TaskDispatch(provider="id_verification", user_id=2)
    assert queue.dequeue() == TaskDispatch(provider="companies_house", user_id=3)
    assert queue.dequeue() == TaskDispatch(provider="bank_statements", user_id=1)
