import json
import logging
from typing import Iterable, Optional, Tuple

from config import Config

from .typing import Action

from src import services
from src.db import get_db_pool

logger = logging.getLogger(__name__)


class HandlerDb:
    def __init__(self, app_config: Config = None) -> None:
        self.app_config = app_config if app_config else services["app_config"]

    def get_action_for_sender(self, sender: str) -> Optional[Tuple[Action, str]]:
        """
        Return any action for the given sender
        """
        with get_db_pool(self.app_config["db"], "db").connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        action, ref
                        FROM senders
                        WHERE sender=%(sender)s AND type='E'
                    """,
                    {"sender": sender}
                )
                result = cursor.fetchone()

                if result:
                    refs = self._extract_refs(result[1]) if result[1] else None
                    return (result[0], refs)

        return ("unknown", None)

    def _extract_refs(self, ref_entry) -> Optional[list[str]]:
        refs = None

        try:
            refs = json.loads(ref_entry)
        except json.JSONDecodeError:
            if ref_entry:
                # This must be a bare string, so make it a list.
                refs = [ref_entry]

        return refs

    def get_patterns(self) -> Iterable[Tuple[str, str, str]]:
        """
        Returns any pattern-type actions
        """
        with get_db_pool(self.app_config["db"], "db").connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        sender, action, ref
                        FROM senders
                        WHERE type='P'
                    """
                )

                for row in cursor:
                    yield row

    def set_action_for_sender(self, sender: str, action: Action, ref: str) -> bool:
        """
        Sets the action for the sender
        """
        with get_db_pool(self.app_config["db"], "db").connection() as connection:
            with connection.cursor() as cursor:
                parsed_ref = ref[0] if ref else None

                try:
                    cursor.execute(
                        """
                        INSERT INTO senders
                            (sender, action, ref, type, source)
                            VALUES
                                (%(sender)s, %(action)s, %(ref)s, 'E', 'postconfirm')
                            ON CONFLICT (sender)
                                DO UPDATE SET action=%(action)s, updated=now()
                        """,
                        {"sender": sender, "action": action, "ref": parsed_ref}
                    )
                    connection.commit()
                    return True

                except Exception as e:
                    print(f"ERROR setting sender: {e}", flush=True)
                    return False

    def stash_message_for_sender(
        self, sender: str, msg: str, recipients: list[str]
    ) -> bool:
        """
        Stores the message for the sender
        """
        with get_db_pool(self.app_config["db"], "db").connection() as connection:
            with connection.cursor() as cursor:
                try:
                    cursor.execute(
                        """
                        INSERT INTO stash
                            (sender, recipients, message)
                            VALUES
                                (%(sender)s, %(recipients)s, %(message)s)
                        """,
                        {"sender": sender, "recipients": json.dumps(recipients), "message": msg}
                    )
                    connection.commit()
                    return True

                except Exception as e:
                    print(f"ERROR stashing mail: {e}")
                    return False

    def unstash_messages_for_sender(
        self, sender: str
    ) -> Iterable[Tuple[str, list[str]]]:
        """
        Yields the messages for the sender
        """
        with get_db_pool(self.app_config["db"], "db").connection() as connection:
            with connection.cursor() as cursor:
                try:
                    cursor.execute(
                        """
                        SELECT
                            id, recipients, message
                            FROM stash
                            WHERE sender=%(sender)s
                        """,
                        {"sender": sender}
                    )

                    for (row_id, recipients, message) in cursor:
                        yield (json.loads(recipients), message)

                        connection.cursor().execute(
                            """
                            DELETE FROM stash
                                WHERE id=%(row_id)s
                            """,
                            {"row_id": row_id}
                        )
                        connection.commit()

                except Exception as e:
                    print(f"ERROR unstashing mails: {e}", flush=True)
                    return
