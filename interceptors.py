# interceptors.py
import collections
import time
from datetime import datetime

import grpc


import time

class LoggingInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None

        method = handler_call_details.method
        user = dict(handler_call_details.invocation_metadata or {}).get("x-user", "-")

        def log(start, context, default_code):
            duration_ms = (time.perf_counter() - start) * 1000
            code = context.code()
            code_name = code.name if code is not None else default_code
            print(f"[{time.strftime('%H:%M:%S')}] {method}  "
                  f"duration={duration_ms:.1f}ms  code={code_name}  user={user}",
                  flush=True)

        if handler.unary_unary:
            original = handler.unary_unary
            def wrapper(request, context):
                start = time.perf_counter()
                try:
                    response = original(request, context)
                except Exception:
                    log(start, context, "UNKNOWN")
                    raise
                log(start, context, "OK")
                return response
            return grpc.unary_unary_rpc_method_handler(
                wrapper, request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer)

        if handler.unary_stream:
            original = handler.unary_stream
            def wrapper(request, context):
                start = time.perf_counter()
                try:
                    for response in original(request, context):
                        yield response
                    log(start, context, "OK")
                except Exception:
                    log(start, context, "UNKNOWN")
                    raise
            return grpc.unary_stream_rpc_method_handler(
                wrapper, request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer)

        if handler.stream_unary:
            original = handler.stream_unary
            def wrapper(request_iterator, context):
                start = time.perf_counter()
                try:
                    response = original(request_iterator, context)
                except Exception:
                    log(start, context, "UNKNOWN")
                    raise
                log(start, context, "OK")
                return response
            return grpc.stream_unary_rpc_method_handler(
                wrapper, request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer)

        if handler.stream_stream:
            original = handler.stream_stream
            def wrapper(request_iterator, context):
                start = time.perf_counter()
                try:
                    for response in original(request_iterator, context):
                        yield response
                    log(start, context, "OK")
                except Exception:
                    log(start, context, "UNKNOWN")
                    raise
            return grpc.stream_stream_rpc_method_handler(
                wrapper, request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer)

        return handler

# ---------- TODO(23) ----------
# HeaderInterceptor (client) : ajoute le metadata ("x-user", <pseudo>) à
# CHAQUE appel — y compris ListTasks, Subscribe et SearchKeywords, d'où
# l'héritage des 4 interfaces.
# grpc.ClientCallDetails est abstraite : utilisez la classe _ClientCallDetails
# ci-dessous pour construire des détails modifiés, puis
#   return continuation(nouveaux_details, request)
class _ClientCallDetails(
        collections.namedtuple(
            "_ClientCallDetails",
            ("method", "timeout", "metadata", "credentials",
             "wait_for_ready", "compression")),
        grpc.ClientCallDetails):
    pass


class HeaderInterceptor(grpc.UnaryUnaryClientInterceptor,
                        grpc.UnaryStreamClientInterceptor,
                        grpc.StreamUnaryClientInterceptor,
                        grpc.StreamStreamClientInterceptor):
    def __init__(self, user: str):
        self._user = user

    def intercept_unary_unary(self, continuation, details, request):
        raise NotImplementedError()

    def intercept_unary_stream(self, continuation, details, request):
        raise NotImplementedError()

    def intercept_stream_unary(self, continuation, details, request_iterator):
        raise NotImplementedError()

    def intercept_stream_stream(self, continuation, details, request_iterator):
        raise NotImplementedError()
