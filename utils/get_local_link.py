import csv
import os
import sys
from typing import List


def get_paths(basedir: str) -> List[str]:
    assert os.path.isdir(basedir), f"No such directory {basedir}"
    assert any(os.scandir(basedir)), f"No files in {basedir}"

    filenames = map(lambda f: os.path.abspath(os.path.join(basedir, f)), os.listdir(basedir))
    
    return sorted(filenames, key=lambda x: int("".join(filter(str.isdigit, x))))


if __name__ == "__main__":

    assert len(sys.argv) == 3, 'Not enough arguments!'

    paths = get_paths(sys.argv[1])

    with open(sys.argv[2], mode='w', encoding='utf-8') as out:
        writer = csv.writer(out)
        writer.writerow(("id", "path"))
        writer.writerows([(i, path) for i, path in enumerate(paths)])
