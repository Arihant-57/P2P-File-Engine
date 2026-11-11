# Decentralized P2P File Transfer & Recovery Engine
## GitHub Collaboration & Team Setup Guide

This guide explains how to invite your college teammates, collaborate on the codebase, distribute the application to friends and classmates, and share direct links over WhatsApp or email.

---

## 1. Repository Quick Links

| Resource | Link |
| :--- | :--- |
| **GitHub Repository** | [https://github.com/Arihant-57/P2P-File-Engine](https://github.com/Arihant-57/P2P-File-Engine) |
| **Latest Release (v1.0.0)** | [https://github.com/Arihant-57/P2P-File-Engine/releases/tag/v1.0.0](https://github.com/Arihant-57/P2P-File-Engine/releases/tag/v1.0.0) |
| **Windows x64 Portable ZIP** | [Direct Download (4.03 MB)](https://github.com/Arihant-57/P2P-File-Engine/releases/download/v1.0.0/P2P_File_Engine_Windows_x64.zip) |
| **Complete Project Guide (PDF)**| [Direct Download (730 KB)](https://github.com/Arihant-57/P2P-File-Engine/releases/download/v1.0.0/P2P_File_Engine_Complete_Project_Guide.pdf) |
| **Source Code ZIP** | [Direct Download (5.60 MB)](https://github.com/Arihant-57/P2P-File-Engine/releases/download/v1.0.0/P2P_File_Engine_Source.zip) |

---

## 2. Ready-to-Send WhatsApp Message Templates

You can copy and paste either of these messages directly into WhatsApp to share with your friends, teammates, or college professors.

### Template A: For Classmates & Friends (To Try Out the App)

```
Hey everyone! 👋 Check out our college capstone project — Decentralized Peer-to-Peer File Transfer and Recovery Engine (built in C++17):

🚀 What it does:
- Genuine P2P file sharing (zero central servers, zero single point of failure)
- Reed-Solomon RS(3,2) erasure coding: your file reconstructs even if any 2 peer laptops shut down simultaneously!
- Built-in live Web Dashboard with real-time network topology visualizer
- Self-healing autonomous recovery across peer nodes

📥 Download & Run (No compiler or installation needed):
1. Download Windows Portable ZIP:
   https://github.com/Arihant-57/P2P-File-Engine/releases/download/v1.0.0/P2P_File_Engine_Windows_x64.zip
2. Extract the ZIP folder.
3. Right-click and run `launch_local_cluster.ps1` with PowerShell.
4. Open your browser at http://localhost:8081 to view the Web Dashboard!

📖 Complete Technical Guide & Theory (PDF):
https://github.com/Arihant-57/P2P-File-Engine/releases/download/v1.0.0/P2P_File_Engine_Complete_Project_Guide.pdf

⭐ GitHub Repo: https://github.com/Arihant-57/P2P-File-Engine
```

---

### Template B: For Project Evaluators / Faculty

```
Respected Professor,

Here are the project submission materials and repository links for our project: "Decentralized Peer-to-Peer File Transfer and Recovery Engine" (C++17):

1. GitHub Repository:
   https://github.com/Arihant-57/P2P-File-Engine

2. Release v1.0.0 & Binary Downloads:
   https://github.com/Arihant-57/P2P-File-Engine/releases/tag/v1.0.0

3. Comprehensive 15-Chapter Project Report & Learning Guide (PDF):
   https://github.com/Arihant-57/P2P-File-Engine/releases/download/v1.0.0/P2P_File_Engine_Complete_Project_Guide.pdf

4. Automated Verification Results:
   - 16/16 Unit & Regression Tests Passing (controller.exe test)
   - 8/8 Real-World Multi-Node End-to-End Scenarios Passing

Thank you,
Arihant Narayankar & Project Team
```

---

## 3. How to Add Teammates as GitHub Collaborators

Since the repository is owned by your account (`Arihant-57`), you can give your project teammates direct write/push access so they can push commits, create branches, and review pull requests:

1. Open your browser and navigate to:
   [https://github.com/Arihant-57/P2P-File-Engine/settings/access](https://github.com/Arihant-57/P2P-File-Engine/settings/access)
2. Click the green button: **"Add people"**.
3. In the search box, enter your teammate's **GitHub username** or **email address**.
4. Select their account and click **"Add [username] to this repository"**.
5. Your teammate will receive an email invitation and a notification on GitHub. Once they click **Accept Invitation**, they have full collaborative access to the repository.

---

## 4. Teammate Workflow: Cloning & Building

### Step 1: Clone the Repository
```bash
git clone https://github.com/Arihant-57/P2P-File-Engine.git
cd P2P-File-Engine
```

### Step 2: Build the Codebase (Windows MSVC 2019 / 2022)
Open **Developer Command Prompt for VS 2022** (or PowerShell with CMake and MSVC in PATH):
```powershell
# Create build directory
mkdir build
cd build

# Configure CMake in Release mode
cmake .. -G "Visual Studio 17 2022" -A x64

# Compile all binaries
cmake --build . --config Release
```
The compiled binaries will be located in `build/Release/`:
- `peer.exe`
- `rfe.exe`
- `controller.exe`

### Step 3: Run the Verification Suite
```powershell
cd build\Release
.\controller.exe test
```
All 16 test suites must return `PASS=16 FAIL=0`.

---

## 5. Team Git Development Best Practices

To keep the `main` branch stable, team members should follow this workflow:

1. **Pull Latest Changes:**
   ```bash
   git checkout main
   git pull origin main
   ```
2. **Create a Feature Branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Write Code & Verify Locally:**
   Always run `.\controller.exe test` before committing.
4. **Commit & Push to Feature Branch:**
   ```bash
   git add .
   git commit -m "feat: describe the enhancement"
   git push -u origin feature/your-feature-name
   ```
5. **Open a Pull Request (PR):**
   Go to GitHub and click **"Compare & pull request"**. After teammate review, merge into `main`.
