from .handler_db import HandlerDb
from .sender import Sender


_handler_instance = None


def get_handler() -> HandlerDb:
    global _handler_instance

    if _handler_instance is None:
        _handler_instance = HandlerDb()

    return _handler_instance


def get_sender(email) -> Sender:
    return Sender(email, get_handler())
