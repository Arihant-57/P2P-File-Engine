#!/usr/bin/env python3
r"""
prepare_clean_github_repo.py
Assembles a clean, pristine publishing copy of the project at
C:\Users\ariha\Documents\P2P-File-Engine-GitHub-Publish
excluding build artifacts, test chunks, runtime databases, and temp files.
"""

import os
import shutil

repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
target_dir = os.path.abspath(r"C:\Users\ariha\Documents\P2P-File-Engine-GitHub-Publish")

print(f"Source: {repo_root}")
print(f"Target: {target_dir}")

if os.path.exists(target_dir):
    print(f"Removing existing target directory: {target_dir}")
    shutil.rmtree(target_dir, ignore_errors=True)

os.makedirs(target_dir, exist_ok=True)

# Directories to copy
dirs_to_copy = [
    "cli",
    "configs",
    "controller",
    "docs",
    "engine",
    "models",
    "peer",
    "scripts",
    "third_party",
    "web"
]

for d in dirs_to_copy:
    src = os.path.join(repo_root, d)
    dst = os.path.join(target_dir, d)
    if os.path.exists(src):
        print(f"Copying directory: {d}...")
        shutil.copytree(src, dst)

# Root files to copy
root_files = [
    "CMakeLists.txt",
    "config.json",
    "README.md",
    "ARCHITECTURE.md",
    "USER_GUIDE.md",
    "CLI_GUIDE.md",
    "WEB_DASHBOARD_GUIDE.md",
    "PORTABLE_RELEASE_GUIDE.md",
    "LAN_DEPLOYMENT_GUIDE.md",
    "FOUR_LAPTOP_DEMO_GUIDE.md",
    "FINAL_ENGINEERING_AUDIT.md",
    "AUTONOMOUS_ENGINEERING_PROGRESS.md",
    "SECURITY_AND_LIMITATIONS.md",
    "RELEASE_NOTES.md",
    "THIRD_PARTY_NOTICES.txt"
]

for f in root_files:
    src = os.path.join(repo_root, f)
    dst = os.path.join(target_dir, f)
    if os.path.exists(src):
        print(f"Copying file: {f}...")
        shutil.copy2(src, dst)

# Create a professional .gitignore
gitignore_content = """# ==============================================================================
# Git Ignore Configuration - P2P Decentralized File Transfer & Recovery Engine
# ==============================================================================

# Build artifacts & CMake directories
build/
bin/
out/
CMakeFiles/
CMakeCache.txt
cmake_install.cmake
Makefile
*.vcxproj*
*.sln
*.dir/

# Compiled object & binary files
*.obj
*.o
*.lib
*.a
*.pdb
*.ilk
*.exp
*.idb
*.tlog

# Runtime executable binaries (distributed via GitHub Releases)
*.exe
!build/Release/*.exe

# SQLite runtime databases & WAL journals
*.db
*.db-wal
*.db-shm
*.sqlite
*.sqlite3

# Local peer storage & chunk stores
storage/
output/
staging/
*.chunk
*.tmp

# Temporary files & logs
*.log
*.tmp
*.bak
test_data/
package_staging/
e2e_audit_workspace/

# Visual Studio / VS Code local configuration
.vs/
.vscode/
*.user
*.suo

# Operating System specific
Thumbs.db
Desktop.ini
.DS_Store

# Release output directory (packaged archives distributed via GitHub Releases)
dist/
"""

with open(os.path.join(target_dir, ".gitignore"), "w", encoding="utf-8") as f:
    f.write(gitignore_content)
print("Created comprehensive .gitignore")

# Clean any accidentally copied temp files, chunks, or obj files
removed_count = 0
for root, dirs, files in os.walk(target_dir):
    for f in files:
        if f.endswith((".obj", ".pdb", ".ilk", ".chunk", ".tmp", ".db", ".db-wal", ".db-shm")):
            os.remove(os.path.join(root, f))
            removed_count += 1

print(f"Cleaned {removed_count} temporary/binary files from target.")
print(f"=== Clean Publishing Repository Ready at {target_dir} ===")
