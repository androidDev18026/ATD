import os
import subprocess
from collections import namedtuple
from types import NoneType
from typing import List, NamedTuple


def check_file_exists(filename: str) -> bool:
    if os.path.exists(filename) and os.path.isfile(filename):
        return True
    return False


def execute_cmd(filename: str, *keywords) -> List[NamedTuple] | NoneType:
    assert check_file_exists(filename), "File not found"

    keywords_ = "|".join(i for i in keywords)

    try:
        output = subprocess.run(
            ["/usr/bin/egrep", "-inE", f"{keywords_}", os.path.abspath(filename)],
            check=True,
            capture_output=True,
            shell=False,
            encoding="utf-8",
            text=True,
        )

        Line: NamedTuple = namedtuple("Line", "lineno value", rename=False)

        lines = [l.split(":") for l in output.stdout.rstrip().split("\n")]

        return [Line(int(lineno), val) for lineno, val in lines]

    except subprocess.CalledProcessError as e:
        return None
