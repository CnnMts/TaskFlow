# test_flow.py — python test_flow.py
import queue
import subprocess
import sys
import tempfile
import threading
import time

import grpc

import taskflow_pb2 as pb
import taskflow_pb2_grpc

PORT = 50061  # port dédié aux tests : ne gêne pas un serveur de démo déjà lancé


def expect_error(fn, code):
    """Appelle fn() et vérifie qu'elle échoue avec le code gRPC attendu."""
    try:
        fn()
    except grpc.RpcError as e:
        assert e.code() == code, f"attendu {code}, reçu {e.code()}"
        return
    raise AssertionError(f"attendu une erreur {code}, aucun échec")


def run_tests(stub, log_path):
    default_timeout = 3

    # Créer une tâche "Rapport" assignée à "alice"
    request_create = pb.CreateTaskRequest(title="Rapport", assigned_to="alice", created_by="alice")
    response_create = stub.CreateTask(request_create, timeout=default_timeout)
    task_id = response_create.task.id

    # Récupérer la tâche par son id et vérifier son titre et son statut
    task = stub.GetTask(pb.GetTaskRequest(id=task_id), timeout=default_timeout)
    assert task.title == "Rapport", f"titre inattendu : {task.title}"
    assert task.status == pb.TODO, f"statut inattendu : {task.status}"

    # Mettre à jour le statut de la tâche à DONE
    stub.UpdateStatus(pb.UpdateStatusRequest(id=task_id, new_status=pb.DONE), timeout=default_timeout)

    # Vérifier que le statut est bien à DONE
    task = stub.GetTask(pb.GetTaskRequest(id=task_id), timeout=default_timeout)
    assert task.status == pb.DONE, f"statut inattendu après mise à jour : {task.status}"

    # Vérifier que la liste des tâches contient au moins une tâche
    tasks_list = list(stub.ListTasks(pb.ListTasksRequest(), timeout=default_timeout))
    print(f"Liste des tâches : {len(tasks_list)} tâches")
    assert len(tasks_list) >= 1, "la liste des tâches est vide"

    # Vérifier qu'on a exactement une tâche avec le statut DONE
    tasks_list_filtered = list(stub.ListTasks(pb.ListTasksRequest(status_filter=pb.DONE), timeout=default_timeout))
    assert len(tasks_list_filtered) == 1, "la liste des tâches filtrées est vide"

    # Tâche inconnue : GetTask doit renvoyer NOT_FOUND
    try: 
        stub.GetTask(pb.GetTaskRequest(id="inconnu"), timeout=default_timeout)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND, f"attendu NOT_FOUND, reçu {e.code()}"

    # Titre vide : CreateTask doit renvoyer INVALID_ARGUMENT
    try:
        request_title_empty = pb.CreateTaskRequest(title="", assigned_to="alice", created_by="alice")
        stub.CreateTask(request_title_empty, timeout=default_timeout)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT, f"attendu INVALID_ARGUMENT, reçu {e.code()}"

    # UpdateStatus au même statut : doit renvoyer INVALID_ARGUMENT
    try:
        request_same_status = pb.UpdateStatusRequest(id=task_id, new_status=pb.DONE)
        stub.UpdateStatus(request_same_status, timeout=default_timeout)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT, f"attendu INVALID_ARGUMENT, reçu {e.code()}"

    # DONE -> IN_PROGRESS -> INVALID_ARGUMENT
    try:
        request_done_to_in_progress = pb.UpdateStatusRequest(id=task_id, new_status=pb.IN_PROGRESS)
        stub.UpdateStatus(request_done_to_in_progress, timeout=default_timeout)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT, f"attendu INVALID_ARGUMENT, reçu {e.code()}"

    # DeleteTask par "bob" -> PERMISSION_DENIED
    try:
        request_delete = pb.DeleteTaskRequest(id=task_id, requested_by="bob")
        stub.DeleteTask(request_delete, timeout=default_timeout)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.PERMISSION_DENIED, f"attendu PERMISSION_DENIED, reçu {e.code()}"

    # DeleteTask par "alice" puis GetTask -> NOT_FOUND
    try:
        request_delete_not_found = pb.DeleteTaskRequest(id=task_id, requested_by="alice")
        stub.DeleteTask(request_delete_not_found, timeout=default_timeout)
        stub.GetTask(pb.GetTaskRequest(id=task_id), timeout=default_timeout)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND, f"attendu NOT_FOUND, reçu {e.code()}"

    # Créer 3 tâches de titres "alpha", "beta", "alpha beta".
    stub.CreateTask(pb.CreateTaskRequest(
        title="alpha", assigned_to="", created_by="alice"), timeout=default_timeout)
    stub.CreateTask(pb.CreateTaskRequest(
        title="beta", assigned_to="", created_by="alice"), timeout=default_timeout)
    stub.CreateTask(pb.CreateTaskRequest(
        title="alpha beta", assigned_to="", created_by="alice"), timeout=default_timeout)

    # Envoyer les mots
    def gen_keywords(keywords):
        for kw in keywords:
            yield pb.SearchEntry(keyword=kw)

    # Check que le résumé contient bien 2 requêtes et 2 matchs pour "alpha" et "BETA"
    summary = stub.SearchKeywords(gen_keywords(["alpha", "BETA"]), timeout=3)
    assert summary.total_requests == 2, f"attendu 2, reçu {summary.total_requests}"

    hits = {r.keyword: r.match_count for r in summary.results}
    assert hits.get("alpha") == 2, f"matchs alpha inattendus : {hits.get('alpha')}"
    assert hits.get("BETA") == 2, f"matchs BETA inattendus : {hits.get('BETA')}"

    # Un keyword vide dans le flux -> INVALID_ARGUMENT
    expect_error(
        lambda: stub.SearchKeywords(gen_keywords(["alpha", ""]), timeout=3),
        grpc.StatusCode.INVALID_ARGUMENT)

    # Création du channel et du stub pour l'abonnement
    sub_channel = grpc.insecure_channel(f"localhost:{PORT}")
    sub_stub = taskflow_pb2_grpc.TaskFlowStub(sub_channel)

    q = queue.Queue()

    # Fonction écouter
    def listen():
        try:
            for event in sub_stub.Subscribe(
                    pb.SubscribeRequest(username="testeur", event_types=["DELETED"])):
                q.put(event)
        except grpc.RpcError:
            pass  # channel fermé -> stream coupé, normal

    t = threading.Thread(target=listen, daemon=True)
    t.start()

    # laisser l'abonnement s'établir côté serveur
    time.sleep(0.3)

    # On crée une tâche "ToDelete" et on la supprime : l'événement DELETED doit être reçu
    resp = stub.CreateTask(pb.CreateTaskRequest(
        title="ToDelete", description="d", assigned_to="",
        created_by="alice"), timeout=3)
    task_id = resp.task.id

    stub.DeleteTask(pb.DeleteTaskRequest(
        id=task_id, requested_by="alice"), timeout=3)

    # On check que l'événement DELETED est bien reçu dans la queue
    event = q.get(timeout=2)
    assert event.event_type == "DELETED", f"attendu DELETED, reçu {event.event_type}"
    assert event.task_id == task_id, "task_id de l'événement incorrect"

    # On ferme la connection
    sub_channel.close()

    # --- TODO(25) : Étape 5 — metadata x-user ---
    # Le stdout du serveur est écrit dans log_path. Vérifiez qu'il contient
    # "user=testeur" (metadata ajouté par HeaderInterceptor) et une ligne
    # avec code=NOT_FOUND (produite par LoggingInterceptor).
    pass


def main():
    log = tempfile.NamedTemporaryFile("w+", suffix=".log", delete=False)
    proc = subprocess.Popen([sys.executable, "server.py", "--port", str(PORT)],
                            stdout=log, stderr=subprocess.STDOUT)
    channel = grpc.insecure_channel(f"localhost:{PORT}")
    try:
        grpc.channel_ready_future(channel).result(timeout=10)  # attend le serveur
        # Étape 5 : channel = grpc.intercept_channel(channel, HeaderInterceptor("testeur"))
        run_tests(taskflow_pb2_grpc.TaskFlowStub(channel), log.name)
        print("✅ Tous les tests passent.")
    finally:
        channel.close()
        proc.terminate()   # toujours exécuté, même si un assert échoue
        proc.wait()


if __name__ == "__main__":
    main()
