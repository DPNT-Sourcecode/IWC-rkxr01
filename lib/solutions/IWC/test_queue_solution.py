from queue_solution_legacy import Queue
from task_types import TaskSubmission

queue = Queue()

task1 = TaskSubmission(user_id=1, provider="bank_statements", timestamp='2025-10-20 12:00:00')
task2 = TaskSubmission(user_id=1, provider="bank_statements", timestamp='2025-10-20 11:00:00')
task3 = TaskSubmission(user_id=1, provider="id_verification", timestamp='2025-10-20 11:00:00')

print(queue.enqueue(task1))
print(queue.enqueue(task2))
print(queue.enqueue(task3))

