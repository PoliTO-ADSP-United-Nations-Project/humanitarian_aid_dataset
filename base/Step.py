from abc import ABC, abstractmethod
from typing import Any


class Step(ABC):

    def __init__(self, identifier: str, params: Any = None, enable: bool = True) -> None:
        self._id = identifier
        self._params = params
        self._enable = enable

    @property
    def id(self) -> str:
        return self._id

    @property
    def params(self) -> Any:
        return self._params

    @property
    def enable(self) -> bool:
        return self._enable

    @enable.setter
    def enable(self, value: bool):
        self._enable = value

    @abstractmethod
    def execute(self, path: str = None) -> Any:
        pass

    @abstractmethod
    def can_execute(self, path: str = None) -> bool:
        return True
