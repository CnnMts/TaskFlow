# server.py
import argparse
import queue
import threading
import time
import uuid
from concurrent import futures
import datetime

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import taskflow_pb2
import taskflow_pb2_grpc

_STOP = object()  # sentinelle : sert à débloquer q.get() (voir TODO 8)


def _now() -> Timestamp:
    ts = Timestamp()
    ts.GetCurrentTime()
    return ts


def _status_name(value: int) -> str:
    return taskflow_pb2.TaskStatus.Name(value)   # 2 -> "DONE"


class TaskFlowService(taskflow_pb2_grpc.TaskFlowServicer):
    def __init__(self, slow: bool = False):
        self._tasks = {}
        self._lock = threading.RLock()
        self._subscribers = []          # liste de (username, event_types, Queue)
        self._subs_lock = threading.Lock()
        self._slow = slow               # bonus B1

    # ---------- TODO(1) ----------
    def CreateTask(self, request, context):
        if not request.title:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "title is required")
        if not request.created_by:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "created_by is required")

        task = {
            "id": str(uuid.uuid4()),
            "title": request.title,
            "description": request.description,
            "status": taskflow_pb2.TaskStatus.TODO,
            "assigned_to": request.assigned_to,
            "created_by": request.created_by,
            "created_at": _now(),
            "comments": [],
        }
        with self._lock:
            self._tasks[task["id"]] = task

        self._publish(
            "CREATED", task,
            author=task["created_by"],
            message=f"{task['created_by']} a créé '{task['title']}'",
        )
        return taskflow_pb2.CreateTaskResponse(task=self._to_pb(task))

    # ---------- TODO(2) ----------
    def GetTask(self, request, context):
        task = self._get_or_abort(request.id, context)
        return self._to_pb(task)

    # ---------- TODO(3) ----------
    def ListTasks(self, request, context):
        with self._lock:
            tasks_snapshot = list(self._tasks.values())

        status_filter = request.status_filter if request.HasField("status_filter") else None
        assigned_filter = request.assigned_filter if request.HasField("assigned_filter") else None

        for task in tasks_snapshot:
            if status_filter is not None and task["status"] != status_filter:
                continue
            if assigned_filter is not None and task["assigned_to"] != assigned_filter:
                continue
            yield self._to_pb(task)

    # ---------- TODO(4) ----------
    def UpdateStatus(self, request, context):
        task = self._get_or_abort(request.id, context)

        with self._lock:
            if request.new_status == task["status"]:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"Task already in status {taskflow_pb2.TaskStatus.Name(task['status'])}",
                )

            if task["status"] == taskflow_pb2.TaskStatus.DONE:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Cannot reopen a DONE task",
                )

            task["status"] = request.new_status

        self._publish(
            "STATUS_CHANGED",
            task,
            author=request.requested_by,
            message=f"{request.requested_by} a passé '{task['title']}' à "
                    f"{taskflow_pb2.TaskStatus.Name(task['status'])}",
        )
        return self._to_pb(task)

    # ---------- TODO(5) ----------
    def AssignTask(self, request, context):
        if not request.new_assignee:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "new_assignee is required")

        task = self._get_or_abort(request.id, context)

        with self._lock:
            if task["assigned_to"] == request.new_assignee:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"task already assigned to {request.new_assignee}",
                )
            previous_assignee = task["assigned_to"]
            task["assigned_to"] = request.new_assignee

        self._publish(
            "ASSIGNED",
            task,
            author=request.requested_by,
            message=f"{request.requested_by} a assigné '{task['title']}' à {request.new_assignee}"
                    f" (précédemment {previous_assignee or 'personne'})",
        )
        return self._to_pb(task)

    # ---------- TODO(6) ----------
    def AddComment(self, request, context):
        if not request.text:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "text is required")

        task = self._get_or_abort(request.id, context)

        comment = {
            "author": request.author,
            "text": request.text,
            "created_at": _now(),
        }
        with self._lock:
            task["comments"].append(comment)

        self._publish(
            "COMMENTED",
            task,
            author=request.author,
            message=f"{request.author} a commenté '{task['title']}'",
        )
        return self._to_pb(task)

    # ---------- TODO(7) ----------
    def DeleteTask(self, request, context):
        with self._lock:
            task = self._tasks.get(request.id)
            if task is None:
                context.abort(grpc.StatusCode.NOT_FOUND, f"task {request.id} not found")

            if request.requested_by != task["created_by"]:
                context.abort(grpc.StatusCode.PERMISSION_DENIED,
                               "only the creator can delete this task")

            del self._tasks[request.id]

        self._publish(
            "DELETED",
            task,
            author=request.requested_by,
            message=f"{request.requested_by} a supprimé '{task['title']}'",
        )

        return taskflow_pb2.Empty()

    # ---------- TODO(8) ----------
    def Subscribe(self, request, context):
        q = queue.Queue()
        entry = (request.username, set(request.event_types), q)

        with self._subs_lock:
            self._subscribers.append(entry)

        context.add_callback(lambda: q.put(_STOP))

        return self._event_stream(entry)

    def _event_stream(self, entry):
        username, event_types, q = entry
        try:
            while True:
                event = q.get()
                if event is _STOP:
                    return
                if event_types and event.event_type not in event_types:
                    continue
                yield event
        finally:
            with self._subs_lock:
                if entry in self._subscribers:
                    self._subscribers.remove(entry)

    # ---------- TODO(9) ----------
    def SearchKeywords(self, request_iterator, context):
        raise NotImplementedError()

    # ----- Utilitaires fournis -----
    def _get_or_abort(self, task_id: str, context) -> dict:
        with self._lock:
            t = self._tasks.get(task_id)
        if t is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"task {task_id} not found")
        return t

    def _to_pb(self, t: dict) -> taskflow_pb2.Task:
        comments = [taskflow_pb2.Comment(author=c["author"], text=c["text"],
                                         created_at=c["created_at"])
                    for c in t.get("comments", [])]
        return taskflow_pb2.Task(
            id=t["id"], title=t["title"], description=t["description"],
            status=t["status"], assigned_to=t["assigned_to"],
            created_by=t["created_by"], created_at=t["created_at"],
            comments=comments)

    def _publish(self, event_type: str, task: dict, author: str = "", message: str = ""):
        """Envoie un événement à tous les abonnés."""
        event = taskflow_pb2.TaskEvent(event_type=event_type, task_id=task["id"],
                                       author=author, message=message)
        with self._subs_lock:
            for _, _, q in self._subscribers:
                q.put(event)


def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--slow", action="store_true", help="bonus B1")
    args = parser.parse_args()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    taskflow_pb2_grpc.add_TaskFlowServicer_to_server(TaskFlowService(args.slow), server)
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    print(f"TaskFlow server listening on :{args.port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1)


if __name__ == "__main__":
    serve()