from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run HumemDB example scripts.")
    parser.add_argument(
        "patterns",
        nargs="*",
        default=["examples/[0-9][0-9]_*.py"],
        help="Glob patterns relative to the repository root.",
    )
    return parser.parse_args()


def collect_examples(root: Path, patterns: list[str]) -> list[Path]:
    examples: list[Path] = []
    seen: set[Path] = set()
    for pattern in patterns:
        for path in sorted(root.glob(pattern)):
            if not path.is_file() or path.suffix != ".py":
                continue
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            examples.append(resolved)
    return examples


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    examples = collect_examples(root, args.patterns)

    if not examples:
        print("No example scripts matched the requested patterns.", file=sys.stderr)
        return 1

    for example in examples:
        relative_path = example.relative_to(root)
        print(f"==> Running {relative_path}")
        subprocess.run([sys.executable, str(example)], cwd=root, check=True)

    print(f"Ran {len(examples)} example script(s) successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
