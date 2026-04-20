"""Pre-commit content guard — blocks forbidden terms in staged files."""

import re
import subprocess
import sys

# Patterns that must never appear in committed content.
# Maintained as compiled regexes for performance and obfuscation.
_BLOCKED = [
    re.compile(r"[Ll][Ee][Cc][Hh][Ll][Ee][Rr]", re.IGNORECASE),
    re.compile(r"[Ss][Pp][Rr][Aa][Yy]", re.IGNORECASE),
    re.compile(r"[Nn][Oo][Zz]{1,2}[Ll][Ee]", re.IGNORECASE),
]

_SKIP_EXT = {".woff2", ".png", ".jpg", ".jpeg", ".gif", ".ico", ".zip", ".whl", ".tar.gz"}

_IGNORE_MARKER = "content-guard:ignore"


def main() -> int:
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
        capture_output=True, text=True,
    )
    files = [f for f in result.stdout.strip().splitlines() if f]
    violations = []
    for path in files:
        if any(path.endswith(ext) for ext in _SKIP_EXT):
            continue
        try:
            with open(path, "r", errors="ignore") as fh:
                for lineno, line in enumerate(fh, 1):
                    if _IGNORE_MARKER in line:
                        continue
                    for pat in _BLOCKED:
                        if pat.search(line):
                            violations.append((path, lineno, line.rstrip()))
        except (OSError, UnicodeDecodeError):
            continue
    if violations:
        print("\n[CONTENT GUARD] Blocked terms found in staged files:\n")
        for path, lineno, line in violations:
            print(f"  {path}:{lineno}: {line[:120]}")
        print(f"\n  {len(violations)} violation(s) — commit rejected.\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
