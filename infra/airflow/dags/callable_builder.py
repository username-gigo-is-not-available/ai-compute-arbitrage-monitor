from importlib import import_module
from typing import Callable


class CallableBuilder:

    def __init__(self, path: str):
        self.path = path

    def build(self) -> Callable:
        return import_module(self.path).run