# client.py
import argparse
import threading

import grpc

import taskflow_pb2
import taskflow_pb2_grpc

STATUS_NAMES = {0: "TODO", 1: "IN_PROGRESS", 2: "DONE"}
received_events = []  # pour l'option 9 (débug)


def print_event(event):
    print(f"\n🔔 [{event.event_type}] {event.author}: {event.message}\n> ",
          end="", flush=True)


# ---------- TODO(10) ----------
# Thread d'écoute : s'abonner via
#   stub.Subscribe(SubscribeRequest(username=..., event_types=[...]))
# puis for event in stream: mémoriser dans received_events + print_event(event)
# Entourez le tout d'un try/except grpc.RpcError : si le serveur tombe,
# afficher UNE ligne propre (code + details), pas une traceback.
# (CANCELLED = c'est nous qui quittons : ne rien afficher.)
def listen_events(stub, username, event_types = []):
    try:
        stream = stub.Subscribe(taskflow_pb2.SubscribeRequest(username=username, event_types=event_types))
        for event in stream:
            received_events.append(event)
            print_event(event)

    except grpc.RpcError as e:
        # (CANCELLED = c'est nous qui quittons : ne rien afficher.)
        if e.code() == grpc.StatusCode.CANCELLED:
            return
        print(f"[listen_events] Connexion au serveur perdue : {e.code()} - {e.details()}")


def print_task(task):
    print(f"  [{STATUS_NAMES[task.status]:12}] {task.id[:8]}… "
          f"« {task.title} » → {task.assigned_to or 'non assignée'} "
          f"({len(task.comments)} commentaire(s))")


def main():
    parser = argparse.ArgumentParser(description="Client TaskFlow")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--user", required=True)
    parser.add_argument("--events", default="",
                        help="filtre, ex: CREATED,DELETED (vide = tout)")
    parser.add_argument("--no-listen", action="store_true")
    parser.add_argument("--timeout", type=float, default=3, help="bonus B1")
    args = parser.parse_args()

    channel = grpc.insecure_channel(f"{args.host}:{args.port}")
    # Étape 5 : channel = grpc.intercept_channel(channel, HeaderInterceptor(args.user))
    stub = taskflow_pb2_grpc.TaskFlowStub(channel)
    T = args.timeout  # à passer en timeout=T sur TOUS les appels (sauf Subscribe)

    event_types = [e.strip().upper() for e in args.events.split(",") if e.strip()]

    if not args.no_listen:
        threading.Thread(target=listen_events,
                         args=(stub, args.user, event_types),
                         daemon=True).start()

    while True:
        print("""
=== TaskFlow ===  (utilisateur: {u})
 1. Créer une tâche        6. Commenter une tâche
 2. Lister les tâches      7. Supprimer une tâche
 3. Voir une tâche         8. Recherche multi-mots-clés (streaming)
 4. Changer le statut      9. Événements reçus (aide au débug)
 5. Réassigner             0. Quitter""".format(u=args.user))

        try:
            choice = input("choix > ").strip()
            if choice == "1":
                print("--------- Création d'une tâche ---------")
                title = input("Titre : ").strip()
                description = input("Description : ").strip()
                assigned_to = input("Assigné à : ").strip()

                request = taskflow_pb2.CreateTaskRequest(
                    title=title,
                    description=description,
                    assigned_to=assigned_to,
                    created_by=args.user
                )
                response = stub.CreateTask(request, timeout=T)
                print(f"✅ Tâche créée avec l'id : {response}")
                pass
            elif choice == "2":
                # ---------- TODO(12) ----------
                # Proposer un filtre statut (vide = tous) et un filtre
                # assigné (vide = tous), appeler ListTasks en streaming.
                # Filtre vide -> NE PAS affecter le champ optional.
                pass
            elif choice == "3":
                # ---------- TODO(13) ----------
                # GetTask : afficher la tâche ET ses commentaires
                # (auteur, date ISO via c.created_at.ToDatetime(), texte).
                pass
            elif choice == "4":
                # ---------- TODO(14) ----------
                # Menu TODO/IN_PROGRESS/DONE -> UpdateStatus
                # (requested_by=args.user).
                pass
            elif choice == "5":
                # ---------- TODO(15) ----------
                # AssignTask (requested_by=args.user).
                pass
            elif choice == "6":
                # ---------- TODO(16) ----------
                # AddComment (texte multi-mots, author=args.user).
                pass
            elif choice == "7":
                # ---------- TODO(17) ----------
                # DeleteTask (requested_by=args.user).
                pass
            elif choice == "8":
                # ---------- TODO(18) ----------
                # Client streaming : demander des mots-clés un par un
                # (ligne vide = fin), construire un GÉNÉRATEUR Python qui
                # yield les SearchEntry, appeler SearchKeywords(generator)
                # et afficher le SearchSummary (total + résultats).
                pass
            elif choice == "9":
                print(f"{len(received_events)} événement(s) reçu(s)")
            elif choice == "0":
                print("Au revoir !")
                break
        except grpc.RpcError as e:
            print(f"❌ Erreur gRPC [{e.code().name}] : {e.details()}")
        except (KeyboardInterrupt, EOFError):
            print()
            break
    channel.close()


if __name__ == "__main__":
    main()
