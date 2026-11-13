"""Create a validated working copy; never rewrite the user's raw C-MAPSS files."""
import hashlib
import json
from pathlib import Path
import re
import sys


def prepare(root):
    source = root / 'data/raw/cmapss'
    target = root / '.local/cmapss'
    target.mkdir(parents=True, exist_ok=True)
    changes = []
    for path in sorted(source.glob('*.txt')):
        original = path.read_bytes()
        content = original
        if path.name.startswith(('train_', 'test_')):
            # Formatting backticks are removable only when the whole token is numeric.
            def clean(match):
                value = match.group().replace(b'`', b'')
                float(value)
                changes.append({'file': path.name, 'offset': match.start(), 'original': match.group().decode(), 'numeric': value.decode()})
                return value
            content = re.sub(rb'\S*`\S*', clean, original)
            for line, row in enumerate(content.splitlines(), 1):
                cells = row.split()
                if len(cells) != 26:
                    raise ValueError(f'{path.name}:{line}: expected 26 columns')
                for value in cells:
                    float(value)
        (target / path.name).write_bytes(content)
    for subset in ('FD001', 'FD002', 'FD003', 'FD004'):
        if not (target / f'train_{subset}.txt').is_file():
            raise ValueError(f'Missing train_{subset}.txt')
    manifest = {'source_unchanged': True, 'formatting_fixes': changes,
                'sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(target.glob('*.txt'))}}
    (target / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(json.dumps({'working_copy': str(target), 'formatting_fixes': len(changes)}))


if __name__ == '__main__':
    prepare(Path(sys.argv[1]).resolve())
