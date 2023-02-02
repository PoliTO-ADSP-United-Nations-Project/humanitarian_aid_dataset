"""This module defines some utility functions related to managing File."""

import fnmatch
import os
from typing import List


class FileUtils:

    @staticmethod
    def has_file(pattern: str, path: str) -> bool:
        for root, dirs, files in os.walk(path):
            for name in files:
                if fnmatch.fnmatch(name, pattern):
                    return True
        return False

    @staticmethod
    def find_files(pattern: str, path: str) -> List[str]:
        result = []
        for root, dirs, files in os.walk(path):
            for name in files:
                if fnmatch.fnmatch(name, pattern):
                    result.append(os.path.join(root, name))
        return result

