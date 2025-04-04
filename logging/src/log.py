import logging
import grpc
from grpc_generated import logging_pb2, logging_pb2_grpc
from google.protobuf.json_format import (
    MessageToDict,
)

from hazelcaster import Hazelcaster


class Logger(logging_pb2_grpc.LoggingServiceServicer):
    def __init__(self, client: Hazelcaster, logger: logging.Logger):
        self.logs = client.map
        self.logger = logger

    async def AddLog(self, request, context):
        request = MessageToDict(
            request,
            preserving_proto_field_name=True,
            use_integers_for_enums=False,
        )
        self.logger.info(f"Got a request to add log: ({request})")

        if self.logs.contains_key(request["uuid"]):
            await context.abort(grpc.StatusCode.ALREADY_EXISTS, "Log already exists")

        self.logs.put(request["uuid"], request)

        return logging_pb2.AddLogResponse(success=True)

    async def GetLogs(self, request, context):
        request = MessageToDict(
            request,
            preserving_proto_field_name=True,
            use_integers_for_enums=False,
        )
        self.logger.info("Got a request to get logs")

        try:
            logs_string = "\n".join([log["message"] for log in self.logs.values()])
            self.logger.info("Success")
            return logging_pb2.LogsString(logs_string=logs_string)
        except Exception as e:
            self.logger.error(f"Failed to access logs for a request: {request}", exc_info=e)
            await context.abort(grpc.StatusCode.INTERNAL, "Failed to get logs")
