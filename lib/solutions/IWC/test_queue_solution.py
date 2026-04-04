from lib.solutions.IWC.queue_solution_legacy import Queue
from lib.solutions.IWC.task_types import TaskSubmission

queue = Queue()

task = TaskSubmission(user_id=1, provider="bank_statements", timestamp='2025-10-20 12:00:00')

print(queue.enqueue(task))
print(queue.enque(task))