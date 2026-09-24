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
    # --- TODO(19) : scénario nominal ---
    # 1. Créer "Rapport" assignée à alice (created_by="alice") -> récupérer l'id
    # 2. GetTask(id) -> titre == "Rapport", statut == TODO
    # 3. UpdateStatus(id, DONE) -> statut == DONE
    # 4. ListTasks() -> contient au moins 1 tâche ;
    #    ListTasks(status_filter=DONE) -> exactement cette tâche

    # --- TODO(20) : erreurs attendues ---
    # expect_error(lambda: stub.GetTask(pb.GetTaskRequest(
    #     id="inconnu"), timeout=3), grpc.StatusCode.NOT_FOUND)
    # Ajouter : titre vide -> INVALID_ARGUMENT ;
    # UpdateStatus au même statut -> INVALID_ARGUMENT ;
    # DONE -> IN_PROGRESS -> INVALID_ARGUMENT ;
    # DeleteTask par "bob" -> PERMISSION_DENIED ;
    # DeleteTask par "alice" puis GetTask -> NOT_FOUND.

    # --- TODO(21) : client streaming ---
    # Créer 3 tâches de titres "alpha", "beta", "alpha beta".
    # Envoyer ["alpha", "BETA"] en client streaming ->
    # total_requests == 2, matchs alpha == 2, BETA == 2 (casse ignorée).
    # Un keyword vide dans le flux -> INVALID_ARGUMENT.

    # --- TODO(24) : Subscribe ---
    # Sur un channel séparé, s'abonner avec event_types=["DELETED"] ;
    # lire le flux dans un threading.Thread qui pousse dans une queue.Queue.
    # time.sleep(0.3) pour laisser l'abonnement s'établir, puis créer et
    # supprimer une tâche : q.get(timeout=2) doit renvoyer un DELETED
    # (le CREATED a été filtré). Fermez ensuite ce channel.

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
