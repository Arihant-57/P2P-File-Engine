#!/usr/bin/env python3
import os
import re

repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

patterns = [
    (re.compile(r"ghp_[A-Za-z0-9_]{36,}"), "GitHub Classic PAT"),
    (re.compile(r"gho_[A-Za-z0-9_]{36,}"), "GitHub OAuth Token"),
    (re.compile(r"github_pat_[A-Za-z0-9_]{50,}"), "GitHub Fine-Grained PAT"),
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"), "Private Key Header"),
    (re.compile(r"(?i)(api_key|secret_key|private_key)\s*[:=]\s*['\"][A-Za-z0-9_\-]{16,}['\"]"), "Hardcoded Secret String"),
    (re.compile(r"(?i)aws_access_key_id|aws_secret_access_key"), "AWS Credentials"),
]

skip_dirs = {".git", "build", "dist", ".vs", "out", "package_staging"}
skip_exts = {".exe", ".dll", ".lib", ".pdb", ".zip", ".chunk", ".db", ".db-wal", ".db-shm", ".png", ".jpg", ".pdf"}

findings = []
scanned_count = 0

for root, dirs, files in os.walk(repo_root):
    dirs[:] = [d for d in dirs if d not in skip_dirs]
    for file in files:
        _, ext = os.path.splitext(file)
        if ext.lower() in skip_exts:
            continue
        filepath = os.path.join(root, file)
        scanned_count += 1
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                for line_num, line in enumerate(f, 1):
                    for pat, desc in patterns:
                        if pat.search(line):
                            findings.append((filepath, line_num, desc))
        except Exception:
            pass

print(f"Scanned {scanned_count} files.")
print(f"Total secret findings: {len(findings)}")
for path, line, desc in findings:
    rel = os.path.relpath(path, repo_root)
    print(f"  [ALERT] {rel}:{line} - {desc}")
