from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import IntEnum

# LEGACY CODE ASSET
# RESOLVED on deploy
from solutions.IWC.task_types import TaskSubmission, TaskDispatch

# solutions.IWC


class Priority(IntEnum):
    """Represents the queue ordering tiers observed in the legacy system."""

    HIGH = 1
    NORMAL = 2


@dataclass
class Provider:
    name: str
    base_url: str
    depends_on: list[str]


MAX_TIMESTAMP = datetime.max.replace(tzinfo=None)

COMPANIES_HOUSE_PROVIDER = Provider(
    name="companies_house", base_url="https://fake.companieshouse.co.uk", depends_on=[]
)


CREDIT_CHECK_PROVIDER = Provider(
    name="credit_check",
    base_url="https://fake.creditcheck.co.uk",
    depends_on=["companies_house"],
)


BANK_STATEMENTS_PROVIDER = Provider(
    name="bank_statements", base_url="https://fake.bankstatements.co.uk", depends_on=[]
)

ID_VERIFICATION_PROVIDER = Provider(
    name="id_verification", base_url="https://fake.idv.co.uk", depends_on=[]
)


REGISTERED_PROVIDERS: list[Provider] = [
    BANK_STATEMENTS_PROVIDER,
    COMPANIES_HOUSE_PROVIDER,
    CREDIT_CHECK_PROVIDER,
    ID_VERIFICATION_PROVIDER,
]


class Queue:
    def __init__(self):
        self._queue = []

    def _collect_dependencies(self, task: TaskSubmission) -> list[TaskSubmission]:
        """
        Recursively collect all dependency tasks for a given task.

        Dependencies are resolved using the REGISTERED_PROVIDERS registry.
        For each dependency, a new TaskSubmission is created with the same
        user_id and timestamp.

        Parameters
        ----------
        task : TaskSubmission
            The task for which dependencies should be resolved.

        Returns
        -------
        list[TaskSubmission]
            A list of dependency tasks in dependency order (parents before children).
        """
        provider = next(
            (p for p in REGISTERED_PROVIDERS if p.name == task.provider), None
        )
        if provider is None:
            return []

        tasks: list[TaskSubmission] = []
        for dependency in provider.depends_on:
            dependency_task = TaskSubmission(
                provider=dependency,
                user_id=task.user_id,
                timestamp=task.timestamp,
            )
            tasks.extend(self._collect_dependencies(dependency_task))
            tasks.append(dependency_task)
        return tasks

    @staticmethod
    def _priority_for_task(task: TaskSubmission) -> Priority:
        """
        Determine the priority level for a task.

        Parameters
        ----------
        task : TaskSubmission
            The task whose priority is to be evaluated.

        Returns
        -------
        Priority
            The resolved priority level (HIGH or NORMAL).
        """
        metadata = task.metadata
        raw_priority = metadata.get("priority", Priority.NORMAL)
        try:
            return Priority(raw_priority)
        except (TypeError, ValueError):
            return Priority.NORMAL

    @staticmethod
    def _earliest_group_timestamp_for_task(task: TaskSubmission) -> datetime:
        """
        Retrieve the group-level earliest timestamp for a task.

        Parameters
        ----------
        task : TaskSubmission
            The task whose group timestamp is to be retrieved.

        Returns
        -------
        datetime
            The earliest timestamp associated with the task's user group,
            or MAX_TIMESTAMP if not set.
        """
        metadata = task.metadata
        return metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)

    @staticmethod
    def _timestamp_for_task(task: TaskSubmission) -> datetime:
        """
        Normalise the timestamp of a task to a timezone-naive datetime.

        Parameters
        ----------
        task : TaskSubmission
            The task whose timestamp is to be normalised.

        Returns
        -------
        datetime
            A timezone-naive datetime object representing the task timestamp.
        """
        timestamp = task.timestamp
        if isinstance(timestamp, datetime):
            return timestamp.replace(tzinfo=None)
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp).replace(tzinfo=None)
        return timestamp

    @staticmethod
    def _provider_speed_priority(task: TaskSubmission) -> int:
        """
        Assign a relative ordering rank based on provider type.

        Parameters
        ----------
        task : TaskSubmission
            The task whose provider rank is to be determined.

        Returns
        -------
        int
            0 for normal providers, 1 for "bank_statements".
        """
        return 1 if task.provider == "bank_statements" else 0

    @staticmethod
    def _is_bank_statments_provider(task: TaskSubmission) -> bool:
        return True if task.provider == "bank_statements" else False
    
    def _is_time_sensitive_bank_task(self, task: TaskSubmission, newest_timestamp: datetime) -> bool:
        if not self._is_bank_statements_provider:
            return False
        
        task_timestamp = self._timestamp_for_task(task)
        return (newest_timestamp - task_timestamp) >= 

    def enqueue(self, item: TaskSubmission) -> int:
        """
        Add a task and its dependencies to the queue.

        The method expands the input task into its dependency closure and
        inserts all tasks into the queue with deduplication and replacement
        semantics based on timestamp.

        Parameters
        ----------
        item : TaskSubmission
            The task to enqueue.

        Returns
        -------
        int
            The size of the queue after insertion.
        """

        index_map = {
            (task.user_id, task.provider): i for i, task in enumerate(self._queue)
        }

        tasks = [*self._collect_dependencies(item), item]

        for new_task in tasks:
            key = (new_task.user_id, new_task.provider)

            if key in index_map:
                existing_idx = index_map[key]
                existing_task = self._queue[existing_idx]

                if new_task.timestamp < existing_task.timestamp:
                    metadata = new_task.metadata
                    metadata.setdefault("priority", Priority.NORMAL)
                    metadata.setdefault("group_earliest_timestamp", MAX_TIMESTAMP)
                    self._queue[existing_idx] = new_task
            else:
                metadata = new_task.metadata
                metadata.setdefault("priority", Priority.NORMAL)
                metadata.setdefault("group_earliest_timestamp", MAX_TIMESTAMP)
                index_map[key] = len(self._queue)
                self._queue.append(new_task)

        return self.size

    def dequeue(self) -> TaskDispatch | None:
        """
        Remove and return the next task to be processed.

        The queue is reordered prior to removal based on:
        - Rule of 3 (priority escalation for users with >= 3 tasks)
        - Provider deprioritisation ("bank_statements")
        - Timestamp ordering

        Returns
        -------
        TaskDispatch or None
            The next task to process, or None if the queue is empty.
        """
        if self.size == 0:
            return None

        user_ids = {task.user_id for task in self._queue}
        task_count = {}
        priority_timestamps = {}
        for user_id in user_ids:
            user_tasks = [t for t in self._queue if t.user_id == user_id]
            earliest_timestamp = sorted(user_tasks, key=lambda t: t.timestamp)[
                0
            ].timestamp
            priority_timestamps[user_id] = earliest_timestamp
            task_count[user_id] = len(user_tasks)

        for task in self._queue:
            metadata = task.metadata
            current_earliest = metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)
            raw_priority = metadata.get("priority")
            try:
                priority_level = Priority(raw_priority)
            except (TypeError, ValueError):
                priority_level = None

            if priority_level is None or priority_level == Priority.NORMAL:
                metadata["group_earliest_timestamp"] = MAX_TIMESTAMP
                if task_count[task.user_id] >= 3:
                    metadata["group_earliest_timestamp"] = priority_timestamps[
                        task.user_id
                    ]
                    metadata["priority"] = Priority.HIGH
                else:
                    metadata["priority"] = Priority.NORMAL
            else:
                metadata["group_earliest_timestamp"] = current_earliest
                metadata["priority"] = priority_level

        self._queue.sort(
            key=lambda i: (
                self._priority_for_task(i),
                self._earliest_group_timestamp_for_task(i),
                self._provider_speed_priority(i),
                self._timestamp_for_task(i),
            )
        )

        task = self._queue.pop(0)
        return TaskDispatch(
            provider=task.provider,
            user_id=task.user_id,
        )

    @property
    def size(self) -> int:
        """
        Return the number of tasks currently in the queue.

        Returns
        -------
        int
            The number of pending tasks.
        """
        return len(self._queue)

    @property
    def age(self) -> int:
        """
        Compute the time span between the oldest and newest tasks in the queue.

        Returns
        -------
        int
            The difference in seconds between the earliest and latest task
            timestamps, or 0 if the queue is empty.
        """
        if self.size == 0:
            return 0
        timestamps = [self._timestamp_for_task(task) for task in self._queue]
        oldest = min(timestamps)
        newest = max(timestamps)
        return int((newest - oldest).total_seconds())

    def purge(self) -> bool:
        """
        Clear all tasks from the queue.

        Returns
        -------
        bool
            True if the queue was successfully cleared.
        """
        self._queue.clear()
        return True


"""
===================================================================================================

The following code is only to visualise the final usecase.
No changes are needed past this point.

To test the correct behaviour of the queue system, import the `Queue` class directly in your tests.

===================================================================================================

```python
import asyncio
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(queue_worker())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Queue worker cancelled on shutdown.")


app = FastAPI(lifespan=lifespan)
queue = Queue()


@app.get("/")
def read_root():
    return {
        "registered_providers": [
            {"name": p.name, "base_url": p.base_url} for p in registered_providers
        ]
    }


class DataRequest(BaseModel):
    user_id: int
    providers: list[str]


@app.post("/fetch_customer_data")
def fetch_customer_data(data: DataRequest):
    provider_names = [p.name for p in registered_providers]

    for provider in data.providers:
        if provider not in provider_names:
            logger.warning(f"Provider {provider} doesn't exists. Skipping")
            continue

        queue.enqueue(
            TaskSubmission(
                provider=provider,
                user_id=data.user_id,
                timestamp=datetime.now(),
            )
        )

    return {"status": f"{len(data.providers)} Task(s) added to queue"}


async def queue_worker():
    while True:
        if queue.size == 0:
            await asyncio.sleep(1)
            continue

        task = queue.dequeue()
        if not task:
            continue

        logger.info(f"Processing task: {task}")
        await asyncio.sleep(2)
        logger.info(f"Finished task: {task}")
```
"""



