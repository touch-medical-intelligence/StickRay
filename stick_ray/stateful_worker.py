import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

__all__ = [
    'StatefulWorker',
    'SessionStateNotFound'
]
logger = logging.getLogger(__name__)


class SessionStateNotFound(Exception):
    """
    Exception raised when a session id is not found.
    """
    pass


class AbstractStatefulWorker(ABC):
    """
    Base class for stateful worker.
    """

    @abstractmethod
    async def _create_session(self, session_id: str):
        """
        Called when a session is created. This is where you would initialise the session state, and store it.

        Args:
            session_id: session id to create
        """
        ...

    @abstractmethod
    async def _close_session(self, session_id: str):
        """
        Called when a session is closed. This is where you would clean up the session state, and remove it.

        Args:
            session_id: session id to close
        """
        ...

    @abstractmethod
    async def _start(self):
        """
        Called when the worker is started. This can be used to create any resources needed for the worker, e.g.
        database connections.
        """
        ...

    @abstractmethod
    async def _shutdown(self):
        """
        Called when the worker is shut down. This can be used to clean up any resources needed for the worker, e.g.
        database connections.
        """
        ...


class StatefulWorker(AbstractStatefulWorker):
    """
    Base class for stateful workers. This class is responsible for managing sessions, and ferrying methods to the
    correct session. It also provides a control loop to manage the worker, and a health check method.
    """

    async def _initialise(self):
        """
        Initialises the worker from proxy
        """
        self._session_states: Dict[str, Any] = dict()

    async def get_session_state(self, session_id: str) -> Any:
        """
        Get the session state for a session id.

        Args:
            session_id: session id to get state for

        Returns:
            the session state

        Raises:
            SessionStateNotFound if it's not found.
        """
        if session_id not in self._session_states:
            raise SessionStateNotFound(session_id)
        return self._session_states[session_id].state

    async def set_session_state(self, session_id: str, session_state: Any):
        """
        Set the session state for the session id.

        Args:
            session_id: session id
            session_state: a state object.
        """
        self._session_states[session_id] = session_state

    async def _create_session(self, session_id: str):
        pass

    async def _close_session(self, session_id: str):
        pass

    async def _start(self):
        pass

    async def _shutdown(self):
        pass
