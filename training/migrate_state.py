"""One-time SQLite backup into the independent training volume; never overwrite it."""
import os
from pathlib import Path
import sqlite3
from contextlib import closing


def migrate(source=Path('/old/training.sqlite3'), target=Path('/state/training.sqlite3')):
    if target.exists() or not source.exists():
        print('Training state already exists or no legacy state; no migration needed.')
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix('.migrating')
    with closing(sqlite3.connect(source.as_uri()+'?mode=ro', uri=True)) as old:
        with closing(sqlite3.connect(temporary)) as new:
            old.backup(new)
            new.commit()
    os.replace(temporary, target)
    print('Preserved legacy training history in the independent volume.')


if __name__ == '__main__':
    migrate()
