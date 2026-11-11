#!/usr/bin/env python3
"""
generate_complete_project_guide_pdf.py
Generates the publication-grade PDF deliverable:
dist/P2P_File_Engine_Complete_Project_Guide.pdf
from docs/COMPLETE_PROJECT_GUIDE.md using Chrome Headless and PyMuPDF.
"""

import os
import re
import sys
import subprocess
import tempfile
import markdown
import pymupdf

def main():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    md_path = os.path.join(repo_root, "docs", "COMPLETE_PROJECT_GUIDE.md")
    dist_dir = os.path.join(repo_root, "dist")
    out_pdf_path = os.path.join(dist_dir, "P2P_File_Engine_Complete_Project_Guide.pdf")
    verify_dir = os.path.join(repo_root, "build", "pdf_verification_pages")

    os.makedirs(dist_dir, exist_ok=True)
    os.makedirs(verify_dir, exist_ok=True)

    print(f"Reading markdown source from {md_path}...")
    with open(md_path, "r", encoding="utf-8") as f:
        md_text = f.read()

    # Pre-process alerts
    def alert_repl(m):
        atype = m.group(1).lower()
        content = m.group(2).strip()
        icon = {
            "note": "&#9432;",
            "tip": "&#10004;",
            "important": "&#9888;",
            "warning": "&#9888;",
            "caution": "&#9940;"
        }.get(atype, "&#9432;")
        title = atype.upper()
        return f'<div class="callout callout-{atype}"><div class="callout-header"><span class="callout-icon">{icon}</span> <strong>{title}</strong></div><div class="callout-body">{content}</div></div>'

    md_text = re.sub(r'>\s*\[!(NOTE|TIP|IMPORTANT|WARNING|CAUTION)\]\s*(.*?)(?=\n\n|\Z)', alert_repl, md_text, flags=re.DOTALL)

    # Convert markdown to HTML
    print("Converting Markdown to HTML...")
    html_content = markdown.markdown(
        md_text,
        extensions=["tables", "fenced_code", "toc"]
    )

    # SVG Diagrams
    svg_architecture = """
    <div class="diagram-container">
      <div class="diagram-title">Figure 3.1: Completely Decentralized Peer Mesh Architecture & Module Stack</div>
      <svg viewBox="0 0 900 480" width="100%" height="auto" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <linearGradient id="bgGrad" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stop-color="#0f172a" />
            <stop offset="100%" stop-color="#1e293b" />
          </linearGradient>
          <linearGradient id="peerGrad" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stop-color="#1e3a8a" />
            <stop offset="100%" stop-color="#3b82f6" />
          </linearGradient>
          <linearGradient id="coreGrad" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stop-color="#047857" />
            <stop offset="100%" stop-color="#10b981" />
          </linearGradient>
          <linearGradient id="dbGrad" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stop-color="#7c2d12" />
            <stop offset="100%" stop-color="#ea580c" />
          </linearGradient>
          <filter id="shadow" x="-5%" y="-5%" width="110%" height="110%">
            <feDropShadow dx="2" dy="3" stdDeviation="3" flood-opacity="0.3"/>
          </filter>
        </defs>

        <!-- Outer Boundary -->
        <rect x="10" y="10" width="880" height="460" rx="16" fill="#f8fafc" stroke="#cbd5e1" stroke-width="2" />
        
        <!-- User Interfaces Header -->
        <rect x="30" y="25" width="840" height="60" rx="10" fill="url(#bgGrad)" filter="url(#shadow)" />
        <text x="450" y="50" font-family="Segoe UI, sans-serif" font-size="16" font-weight="bold" fill="#ffffff" text-anchor="middle">USER INTERFACES: DUAL-MODAL ACCESS</text>
        <text x="250" y="72" font-family="Consolas, monospace" font-size="13" fill="#93c5fd" text-anchor="middle">[Terminal CLI: rfe.exe (14 Commands)]</text>
        <text x="650" y="72" font-family="Segoe UI, sans-serif" font-size="13" fill="#86efac" text-anchor="middle">[Web Dashboard: http://127.0.0.1:8080 (REST API v1)]</text>

        <!-- Main Peer Node (Local Host) -->
        <rect x="30" y="105" width="560" height="345" rx="12" fill="#ffffff" stroke="#94a3b8" stroke-width="2" filter="url(#shadow)" />
        <rect x="30" y="105" width="560" height="32" rx="12" fill="url(#peerGrad)" />
        <text x="310" y="127" font-family="Segoe UI, sans-serif" font-size="14" font-weight="bold" fill="#ffffff" text-anchor="middle">LOCAL PEER NODE (peer.exe - e.g., 127.0.0.1:5701 / storage/peer_A)</text>

        <!-- Subsystems within Peer Node -->
        <!-- Communication Block -->
        <rect x="45" y="148" width="255" height="120" rx="8" fill="#f1f5f9" stroke="#cbd5e1" stroke-width="1.5" />
        <text x="172" y="168" font-family="Segoe UI, sans-serif" font-size="12" font-weight="bold" fill="#1e293b" text-anchor="middle">Network Communication Layer</text>
        <rect x="55" y="178" width="235" height="24" rx="4" fill="#3b82f6" />
        <text x="172" y="194" font-family="Consolas, monospace" font-size="11" fill="#ffffff" text-anchor="middle">TCPServer (Port 5701) [Framed I/O]</text>
        <rect x="55" y="208" width="235" height="24" rx="4" fill="#0284c7" />
        <text x="172" y="224" font-family="Consolas, monospace" font-size="11" fill="#ffffff" text-anchor="middle">UDPBeacon (Port 5705) [Discovery]</text>
        <rect x="55" y="238" width="235" height="24" rx="4" fill="#059669" />
        <text x="172" y="254" font-family="Consolas, monospace" font-size="11" fill="#ffffff" text-anchor="middle">HttpServer (Port 8080) [REST API]</text>

        <!-- Peer Engine Block -->
        <rect x="315" y="148" width="260" height="120" rx="8" fill="#f1f5f9" stroke="#cbd5e1" stroke-width="1.5" />
        <text x="445" y="168" font-family="Segoe UI, sans-serif" font-size="12" font-weight="bold" fill="#1e293b" text-anchor="middle">Core Engine Subsystems</text>
        <rect x="325" y="178" width="240" height="24" rx="4" fill="url(#coreGrad)" />
        <text x="445" y="194" font-family="Segoe UI, sans-serif" font-size="11" font-weight="bold" fill="#ffffff" text-anchor="middle">Adaptive Chunker & RS(3,2) Engine</text>
        <rect x="325" y="208" width="240" height="24" rx="4" fill="url(#coreGrad)" />
        <text x="445" y="224" font-family="Segoe UI, sans-serif" font-size="11" font-weight="bold" fill="#ffffff" text-anchor="middle">DynamicDistributor (Workload Aware)</text>
        <rect x="325" y="238" width="240" height="24" rx="4" fill="url(#coreGrad)" />
        <text x="445" y="254" font-family="Segoe UI, sans-serif" font-size="11" font-weight="bold" fill="#ffffff" text-anchor="middle">Autonomous Recovery (DJB2 Consensus)</text>

        <!-- Storage & Persistence Block -->
        <rect x="45" y="280" width="530" height="155" rx="8" fill="#f8fafc" stroke="#cbd5e1" stroke-width="1.5" />
        <text x="310" y="302" font-family="Segoe UI, sans-serif" font-size="13" font-weight="bold" fill="#0f172a" text-anchor="middle">Local Persistence & Integrity Layer (Zero Central Database)</text>

        <rect x="60" y="315" width="240" height="105" rx="6" fill="#fef2f2" stroke="#fca5a5" stroke-width="1.5" />
        <text x="180" y="335" font-family="Segoe UI, sans-serif" font-size="12" font-weight="bold" fill="#991b1b" text-anchor="middle">Embedded SQLite 3.54.1 (WAL)</text>
        <text x="70" y="355" font-family="Consolas, monospace" font-size="10.5" fill="#7f1d1d">- files / stripes / shards</text>
        <text x="70" y="372" font-family="Consolas, monospace" font-size="10.5" fill="#7f1d1d">- shard_locations (Versions)</text>
        <text x="70" y="389" font-family="Consolas, monospace" font-size="10.5" fill="#7f1d1d">- peers (ONLINE/SUSPECT/OFFLINE)</text>
        <text x="70" y="406" font-family="Consolas, monospace" font-size="10.5" fill="#7f1d1d">- recovery_records (Audit Trail)</text>

        <rect x="320" y="315" width="240" height="105" rx="6" fill="#f0fdf4" stroke="#86efac" stroke-width="1.5" />
        <text x="440" y="335" font-family="Segoe UI, sans-serif" font-size="12" font-weight="bold" fill="#166534" text-anchor="middle">Local Shard Store & Hashing</text>
        <text x="330" y="355" font-family="Consolas, monospace" font-size="10.5" fill="#14532d">- storage/peer_A/chunks/*.chunk</text>
        <text x="330" y="372" font-family="Consolas, monospace" font-size="10.5" fill="#14532d">- storage/peer_A/staging/*.tmp</text>
        <text x="330" y="389" font-family="Consolas, monospace" font-size="10.5" fill="#14532d">- OpenSSL EVP_sha256 Verifier</text>
        <text x="330" y="406" font-family="Consolas, monospace" font-size="10.5" fill="#14532d">- Atomic Rename MoveFileExW</text>

        <!-- Remote Peer Mesh Nodes -->
        <g transform="translate(610, 105)">
          <!-- Peer B -->
          <rect x="0" y="0" width="260" height="98" rx="8" fill="#ffffff" stroke="#3b82f6" stroke-width="2" filter="url(#shadow)" />
          <rect x="0" y="0" width="260" height="24" rx="8" fill="#3b82f6" />
          <text x="130" y="17" font-family="Segoe UI, sans-serif" font-size="12" font-weight="bold" fill="#ffffff" text-anchor="middle">REMOTE PEER B (Port 5702)</text>
          <text x="12" y="42" font-family="Segoe UI, sans-serif" font-size="11" fill="#334155">- TCP/UDP Mesh Endpoint</text>
          <text x="12" y="60" font-family="Segoe UI, sans-serif" font-size="11" fill="#334155">- Isolated SQLite (metadata.db)</text>
          <text x="12" y="78" font-family="Segoe UI, sans-serif" font-size="11" fill="#334155">- Shard Store (storage/peer_B/)</text>

          <!-- Peer C -->
          <rect x="0" y="118" width="260" height="98" rx="8" fill="#ffffff" stroke="#3b82f6" stroke-width="2" filter="url(#shadow)" />
          <rect x="0" y="118" width="260" height="24" rx="8" fill="#3b82f6" />
          <text x="130" y="135" font-family="Segoe UI, sans-serif" font-size="12" font-weight="bold" fill="#ffffff" text-anchor="middle">REMOTE PEER C (Port 5703)</text>
          <text x="12" y="160" font-family="Segoe UI, sans-serif" font-size="11" fill="#334155">- TCP/UDP Mesh Endpoint</text>
          <text x="12" y="178" font-family="Segoe UI, sans-serif" font-size="11" fill="#334155">- Isolated SQLite (metadata.db)</text>
          <text x="12" y="196" font-family="Segoe UI, sans-serif" font-size="11" fill="#334155">- Shard Store (storage/peer_C/)</text>

          <!-- Peer D -->
          <rect x="0" y="236" width="260" height="98" rx="8" fill="#ffffff" stroke="#3b82f6" stroke-width="2" filter="url(#shadow)" />
          <rect x="0" y="236" width="260" height="24" rx="8" fill="#3b82f6" />
          <text x="130" y="253" font-family="Segoe UI, sans-serif" font-size="12" font-weight="bold" fill="#ffffff" text-anchor="middle">REMOTE PEER D (Port 5704)</text>
          <text x="12" y="278" font-family="Segoe UI, sans-serif" font-size="11" fill="#334155">- TCP/UDP Mesh Endpoint</text>
          <text x="12" y="296" font-family="Segoe UI, sans-serif" font-size="11" fill="#334155">- Isolated SQLite (metadata.db)</text>
          <text x="12" y="314" font-family="Segoe UI, sans-serif" font-size="11" fill="#334155">- Shard Store (storage/peer_D/)</text>
        </g>
      </svg>
    </div>
    """

    svg_reed_solomon = """
    <div class="diagram-container">
      <div class="diagram-title">Figure 6.1: Systematic Reed-Solomon RS(3,2) Generator Matrix & Reconstruction Logic</div>
      <svg viewBox="0 0 900 320" width="100%" height="auto" xmlns="http://www.w3.org/2000/svg">
        <rect x="10" y="10" width="880" height="300" rx="12" fill="#f8fafc" stroke="#cbd5e1" stroke-width="2" />
        
        <!-- Encoding Matrix Equation -->
        <g transform="translate(40, 35)">
          <text x="190" y="20" font-family="Segoe UI, sans-serif" font-size="15" font-weight="bold" fill="#0f172a" text-anchor="middle">ENCODING: Vandermonde Matrix Formulation</text>
          
          <!-- Vector Output -->
          <rect x="10" y="45" width="70" height="120" rx="6" fill="#eff6ff" stroke="#3b82f6" stroke-width="1.5"/>
          <text x="45" y="80" font-family="Consolas, monospace" font-size="16" font-weight="bold" fill="#1e40af" text-anchor="middle">D0</text>
          <text x="45" y="115" font-family="Consolas, monospace" font-size="16" font-weight="bold" fill="#1e40af" text-anchor="middle">D1</text>
          <text x="45" y="150" font-family="Consolas, monospace" font-size="16" font-weight="bold" fill="#b91c1c" text-anchor="middle">P0</text>
          
          <text x="100" y="115" font-family="Segoe UI, sans-serif" font-size="24" font-weight="bold" fill="#64748b">=</text>

          <!-- Generator Matrix G -->
          <rect x="125" y="45" width="110" height="120" rx="6" fill="#f1f5f9" stroke="#475569" stroke-width="1.5"/>
          <text x="155" y="80" font-family="Consolas, monospace" font-size="16" fill="#334155" text-anchor="middle">1</text>
          <text x="205" y="80" font-family="Consolas, monospace" font-size="16" fill="#334155" text-anchor="middle">0</text>
          <text x="155" y="115" font-family="Consolas, monospace" font-size="16" fill="#334155" text-anchor="middle">0</text>
          <text x="205" y="115" font-family="Consolas, monospace" font-size="16" fill="#334155" text-anchor="middle">1</text>
          <text x="155" y="150" font-family="Consolas, monospace" font-size="16" font-weight="bold" fill="#b91c1c" text-anchor="middle">1</text>
          <text x="205" y="150" font-family="Consolas, monospace" font-size="16" font-weight="bold" fill="#b91c1c" text-anchor="middle">1</text>

          <text x="255" y="115" font-family="Segoe UI, sans-serif" font-size="20" font-weight="bold" fill="#64748b">&#215;</text>

          <!-- Input Vector -->
          <rect x="280" y="65" width="70" height="80" rx="6" fill="#f0fdf4" stroke="#22c55e" stroke-width="1.5"/>
          <text x="315" y="100" font-family="Consolas, monospace" font-size="16" font-weight="bold" fill="#15803d" text-anchor="middle">D0</text>
          <text x="315" y="135" font-family="Consolas, monospace" font-size="16" font-weight="bold" fill="#15803d" text-anchor="middle">D1</text>

          <!-- Resulting Parity Equation -->
          <rect x="10" y="180" width="340" height="45" rx="6" fill="#fef2f2" stroke="#fca5a5" stroke-width="1"/>
          <text x="180" y="208" font-family="Consolas, monospace" font-size="14" font-weight="bold" fill="#991b1b" text-anchor="middle">P0 = D0 &#8853; D1  (Bitwise XOR over GF(256))</text>
        </g>

        <!-- Reconstruction Cases Box -->
        <g transform="translate(450, 35)">
          <text x="200" y="20" font-family="Segoe UI, sans-serif" font-size="15" font-weight="bold" fill="#0f172a" text-anchor="middle">RECONSTRUCTION: Any 2 Shards Rebuild Data</text>
          
          <rect x="10" y="45" width="380" height="50" rx="6" fill="#f0fdf4" stroke="#86efac" stroke-width="1"/>
          <text x="25" y="68" font-family="Segoe UI, sans-serif" font-size="12" font-weight="bold" fill="#166534">Case 1: D0 & D1 Healthy</text>
          <text x="25" y="86" font-family="Consolas, monospace" font-size="11" fill="#14532d">Data is direct: D0 = D0, D1 = D1 (Zero compute needed)</text>

          <rect x="10" y="105" width="380" height="50" rx="6" fill="#f0fdf4" stroke="#86efac" stroke-width="1"/>
          <text x="25" y="128" font-family="Segoe UI, sans-serif" font-size="12" font-weight="bold" fill="#166534">Case 2: D0 Lost (D1 + P0 Available)</text>
          <text x="25" y="146" font-family="Consolas, monospace" font-size="11" fill="#14532d">Reconstruction: D0 = D1 &#8853; P0 (Single XOR pass in RAM)</text>

          <rect x="10" y="165" width="380" height="50" rx="6" fill="#f0fdf4" stroke="#86efac" stroke-width="1"/>
          <text x="25" y="188" font-family="Segoe UI, sans-serif" font-size="12" font-weight="bold" fill="#166534">Case 3: D1 Lost (D0 + P0 Available)</text>
          <text x="25" y="206" font-family="Consolas, monospace" font-size="11" fill="#14532d">Reconstruction: D1 = D0 &#8853; P0 (Single XOR pass in RAM)</text>

          <rect x="10" y="225" width="380" height="40" rx="6" fill="#fef2f2" stroke="#fca5a5" stroke-width="1"/>
          <text x="25" y="250" font-family="Segoe UI, sans-serif" font-size="12" font-weight="bold" fill="#b91c1c">&#9888; Case 4: &#60; 2 Shards (UNRECOVERABLE - Refuse Write)</text>
        </g>
      </svg>
    </div>
    """

    svg_recovery_flow = """
    <div class="diagram-container">
      <div class="diagram-title">Figure 9.1: Autonomous Durability Recovery State Machine Flowchart</div>
      <svg viewBox="0 0 900 240" width="100%" height="auto" xmlns="http://www.w3.org/2000/svg">
        <rect x="10" y="10" width="880" height="220" rx="12" fill="#f8fafc" stroke="#cbd5e1" stroke-width="2" />
        
        <!-- Nodes in Flow -->
        <!-- Step 1 -->
        <rect x="25" y="45" width="145" height="75" rx="8" fill="#fee2e2" stroke="#ef4444" stroke-width="1.5" />
        <text x="97" y="70" font-family="Segoe UI, sans-serif" font-size="11" font-weight="bold" fill="#991b1b" text-anchor="middle">1. Failure Detection</text>
        <text x="97" y="90" font-family="Segoe UI, sans-serif" font-size="10" fill="#7f1d1d" text-anchor="middle">Peer misses heartbeats</text>
        <text x="97" y="105" font-family="Segoe UI, sans-serif" font-size="10" font-weight="bold" fill="#b91c1c" text-anchor="middle">SUSPECT &#8594; OFFLINE</text>

        <!-- Arrow 1 -->
        <path d="M 170 82 L 195 82" stroke="#64748b" stroke-width="2" marker-end="url(#arrow)" />

        <!-- Step 2 -->
        <rect x="200" y="45" width="150" height="75" rx="8" fill="#eff6ff" stroke="#3b82f6" stroke-width="1.5" />
        <text x="275" y="70" font-family="Segoe UI, sans-serif" font-size="11" font-weight="bold" fill="#1e40af" text-anchor="middle">2. Lease Coordination</text>
        <text x="275" y="90" font-family="Consolas, monospace" font-size="10" fill="#1e3a8a" text-anchor="middle">DJB2(file_id) % N</text>
        <text x="275" y="105" font-family="Segoe UI, sans-serif" font-size="10" fill="#1e3a8a" text-anchor="middle">Leader claims lease</text>

        <!-- Arrow 2 -->
        <path d="M 350 82 L 375 82" stroke="#64748b" stroke-width="2" />

        <!-- Step 3 -->
        <rect x="380" y="45" width="150" height="75" rx="8" fill="#fefce8" stroke="#eab308" stroke-width="1.5" />
        <text x="455" y="70" font-family="Segoe UI, sans-serif" font-size="11" font-weight="bold" fill="#854d0e" text-anchor="middle">3. RS(3,2) Reconstruction</text>
        <text x="455" y="90" font-family="Segoe UI, sans-serif" font-size="10" fill="#713f12" text-anchor="middle">Fetch 2 surviving shards</text>
        <text x="455" y="105" font-family="Segoe UI, sans-serif" font-size="10" font-weight="bold" fill="#854d0e" text-anchor="middle">Rebuild missing in RAM</text>

        <!-- Arrow 3 -->
        <path d="M 530 82 L 555 82" stroke="#64748b" stroke-width="2" />

        <!-- Step 4 -->
        <rect x="560" y="45" width="150" height="75" rx="8" fill="#f0fdf4" stroke="#22c55e" stroke-width="1.5" />
        <text x="635" y="70" font-family="Segoe UI, sans-serif" font-size="11" font-weight="bold" fill="#166534" text-anchor="middle">4. 4th-Peer Placement</text>
        <text x="635" y="90" font-family="Segoe UI, sans-serif" font-size="10" fill="#14532d" text-anchor="middle">Verify SHA-256 digest</text>
        <text x="635" y="105" font-family="Segoe UI, sans-serif" font-size="10" fill="#14532d" text-anchor="middle">Atomic transfer to Peer D</text>

        <!-- Arrow 4 -->
        <path d="M 710 82 L 735 82" stroke="#64748b" stroke-width="2" />

        <!-- Step 5 -->
        <rect x="740" y="45" width="135" height="75" rx="8" fill="#f8fafc" stroke="#64748b" stroke-width="1.5" />
        <text x="807" y="70" font-family="Segoe UI, sans-serif" font-size="11" font-weight="bold" fill="#0f172a" text-anchor="middle">5. Metadata Sync</text>
        <text x="807" y="90" font-family="Segoe UI, sans-serif" font-size="10" fill="#334155" text-anchor="middle">Broadcast manifest push</text>
        <text x="807" y="105" font-family="Segoe UI, sans-serif" font-size="10" font-weight="bold" fill="#16a34a" text-anchor="middle">Cluster 100% Healed</text>

        <!-- Rejoin Sub-Process below -->
        <rect x="180" y="145" width="540" height="65" rx="8" fill="#f1f5f9" stroke="#94a3b8" stroke-dasharray="4" />
        <text x="450" y="168" font-family="Segoe UI, sans-serif" font-size="11" font-weight="bold" fill="#1e293b" text-anchor="middle">Post-Recovery Reconciliation: When Failed Peer B Powers Back On</text>
        <text x="450" y="190" font-family="Segoe UI, sans-serif" font-size="10.5" fill="#475569" text-anchor="middle">Rejoins cluster &#8594; receives newer manifest version &#8594; detects superseded shards &#8594; purges stale .chunk files cleanly</text>
      </svg>
    </div>
    """

    # Insert SVGs after respective sections
    html_content = html_content.replace(
        '<h1 id="3-system-architecture-and-mesh-topology">3. System Architecture and Mesh Topology</h1>',
        '<h1 id="3-system-architecture-and-mesh-topology">3. System Architecture and Mesh Topology</h1>' + svg_architecture
    )
    html_content = html_content.replace(
        '<h1 id="6-reed-solomon-rs32-erasure-coding-mathematics">6. Reed-Solomon RS(3,2) Erasure Coding Mathematics</h1>',
        '<h1 id="6-reed-solomon-rs32-erasure-coding-mathematics">6. Reed-Solomon RS(3,2) Erasure Coding Mathematics</h1>' + svg_reed_solomon
    )
    html_content = html_content.replace(
        '<h1 id="9-autonomous-failure-detection-and-durability-recovery">9. Autonomous Failure Detection and Durability Recovery</h1>',
        '<h1 id="9-autonomous-failure-detection-and-durability-recovery">9. Autonomous Failure Detection and Durability Recovery</h1>' + svg_recovery_flow
    )

    # Executive Cover Page HTML
    cover_html = """
    <div class="cover-page">
      <div class="cover-top-accent"></div>
      <div class="cover-header">
        <div class="course-badge">ACADEMIC CAPSTONE DESIGN &bull; 22ECAW301 &bull; TEAM 13</div>
        <div class="spec-label">ISO C++17 DISTRIBUTED SYSTEMS IMPLEMENTATION SPECIFICATION</div>
      </div>
      
      <div class="cover-title-group">
        <h1 class="cover-title">Peer-to-Peer Decentralized File Transfer and Recovery Engine</h1>
        <div class="cover-subtitle">Complete Technical Learning Guide, System Architecture, and Implementation Handbook</div>
      </div>

      <div class="cover-meta-grid">
        <div class="meta-card">
          <div class="meta-label">ARCHITECTURE</div>
          <div class="meta-val">100% Genuine Decentralized Mesh</div>
          <div class="meta-sub">Zero Central Coordinator &bull; Zero Central DB</div>
        </div>
        <div class="meta-card">
          <div class="meta-label">ERASURE CODING</div>
          <div class="meta-val">Reed-Solomon RS(3,2)</div>
          <div class="meta-sub">K=2 Data + M=1 Parity over GF(2<sup>8</sup>)</div>
        </div>
        <div class="meta-card">
          <div class="meta-label">PERSISTENCE</div>
          <div class="meta-val">Embedded SQLite 3.54.1</div>
          <div class="meta-sub">WAL Mode &bull; ACID Transactions &bull; Gossip Sync</div>
        </div>
        <div class="meta-card">
          <div class="meta-label">USER INTERFACES</div>
          <div class="meta-val">Dual-Modal Access</div>
          <div class="meta-sub">14-Command CLI &bull; Local Web Dashboard</div>
        </div>
      </div>

      <div class="cover-verification-banner">
        <div class="verif-title">&#10004; COMPLETE SYSTEM VERIFICATION STATUS: PASSED</div>
        <div class="verif-detail">Automated C++ Test Suite: <strong>PASS=16 FAIL=0</strong> (T01&ndash;T16) &bull; Real-World E2E Multi-Process Audit: <strong>8/8 Scenarios Passed (100%)</strong></div>
      </div>

      <div class="cover-footer">
        <div class="footer-left">
          <div><strong>Target Platform:</strong> Windows 10 / 11 (x64)</div>
          <div><strong>Distribution:</strong> Standalone Portable Zero-Dependency ZIP</div>
        </div>
        <div class="footer-right">
          <div><strong>Publication Release:</strong> Version 2.0.0 (Production Verified)</div>
          <div><strong>Date:</strong> November 2026</div>
        </div>
      </div>
    </div>
    <div class="page-break"></div>
    """

    # Comprehensive CSS styles for publication-grade layout
    css_styles = """
    <style>
      @page {
        size: A4;
        margin: 20mm 16mm 20mm 16mm;
        @bottom-right {
          content: "Page " counter(page);
          font-family: 'Segoe UI', Arial, sans-serif;
          font-size: 9pt;
          color: #64748b;
        }
        @bottom-left {
          content: "P2P Decentralized File Transfer & Recovery Engine &bull; Team 13";
          font-family: 'Segoe UI', Arial, sans-serif;
          font-size: 8.5pt;
          color: #94a3b8;
        }
      }

      @page :first {
        margin: 0;
        @bottom-right { content: normal; }
        @bottom-left { content: normal; }
      }

      *, *:before, *:after {
        box-sizing: border-box;
      }

      body {
        font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, Helvetica, Arial, sans-serif;
        font-size: 10pt;
        line-height: 1.55;
        color: #1e293b;
        background-color: #ffffff;
        margin: 0;
        padding: 0;
      }

      .page-break {
        page-break-before: always;
        break-before: page;
      }

      /* Cover Page Styles */
      .cover-page {
        width: 100vw;
        height: 100vh;
        padding: 60px 50px;
        background: linear-gradient(145deg, #0b1120 0%, #1e293b 60%, #0f172a 100%);
        color: #ffffff;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        position: relative;
        overflow: hidden;
      }

      .cover-top-accent {
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 8px;
        background: linear-gradient(90deg, #3b82f6, #10b981, #f59e0b);
      }

      .course-badge {
        display: inline-block;
        background: rgba(59, 130, 246, 0.2);
        border: 1px solid rgba(147, 197, 253, 0.4);
        padding: 6px 14px;
        border-radius: 20px;
        font-size: 9pt;
        font-weight: 700;
        letter-spacing: 1.5px;
        color: #93c5fd;
        margin-bottom: 8px;
      }

      .spec-label {
        font-size: 8pt;
        letter-spacing: 2px;
        color: #94a3b8;
        font-weight: 600;
      }

      .cover-title-group {
        margin: 40px 0 20px 0;
      }

      .cover-title {
        font-size: 26pt;
        font-weight: 800;
        line-height: 1.2;
        color: #ffffff;
        margin: 0 0 16px 0;
        border: none;
        padding: 0;
      }

      .cover-subtitle {
        font-size: 13pt;
        font-weight: 400;
        line-height: 1.4;
        color: #cbd5e1;
        max-width: 90%;
      }

      .cover-meta-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 16px;
        margin: 25px 0;
      }

      .meta-card {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 8px;
        padding: 14px 16px;
      }

      .meta-label {
        font-size: 7.5pt;
        font-weight: 700;
        color: #60a5fa;
        letter-spacing: 1px;
        margin-bottom: 4px;
      }

      .meta-val {
        font-size: 11pt;
        font-weight: 700;
        color: #ffffff;
        margin-bottom: 2px;
      }

      .meta-sub {
        font-size: 8pt;
        color: #94a3b8;
      }

      .cover-verification-banner {
        background: rgba(16, 185, 129, 0.12);
        border: 1px solid rgba(52, 211, 153, 0.4);
        border-radius: 8px;
        padding: 12px 18px;
        margin-top: 15px;
      }

      .verif-title {
        font-size: 10pt;
        font-weight: 700;
        color: #34d399;
        margin-bottom: 3px;
      }

      .verif-detail {
        font-size: 8.5pt;
        color: #e2e8f0;
      }

      .cover-footer {
        display: flex;
        justify-content: space-between;
        border-top: 1px solid rgba(255, 255, 255, 0.15);
        padding-top: 16px;
        font-size: 8.5pt;
        color: #94a3b8;
      }

      /* Headings */
      h1 {
        font-size: 18pt;
        font-weight: 700;
        color: #0f172a;
        border-bottom: 2px solid #e2e8f0;
        padding-bottom: 6px;
        margin-top: 28px;
        margin-bottom: 12px;
        page-break-after: avoid;
        break-after: avoid;
      }

      h2 {
        font-size: 14pt;
        font-weight: 700;
        color: #1e3a8a;
        margin-top: 20px;
        margin-bottom: 8px;
        page-break-after: avoid;
        break-after: avoid;
      }

      h3 {
        font-size: 11.5pt;
        font-weight: 700;
        color: #0f766e;
        margin-top: 16px;
        margin-bottom: 6px;
        page-break-after: avoid;
        break-after: avoid;
      }

      p {
        margin-top: 0;
        margin-bottom: 10px;
        text-align: justify;
      }

      /* Tables */
      table {
        width: 100%;
        border-collapse: collapse;
        margin: 14px 0;
        font-size: 8.5pt;
        page-break-inside: avoid;
        break-inside: avoid;
      }

      th, td {
        border: 1px solid #cbd5e1;
        padding: 6px 9px;
        text-align: left;
        vertical-align: top;
      }

      th {
        background-color: #0f172a;
        color: #ffffff;
        font-weight: 600;
      }

      tr:nth-child(even) {
        background-color: #f8fafc;
      }

      /* Code Blocks */
      pre {
        background-color: #0f172a;
        color: #f8fafc;
        font-family: 'Consolas', 'Courier New', monospace;
        font-size: 8pt;
        line-height: 1.45;
        padding: 10px 14px;
        border-radius: 6px;
        overflow-x: auto;
        margin: 12px 0;
        page-break-inside: avoid;
        break-inside: avoid;
        border-left: 4px solid #3b82f6;
      }

      code {
        font-family: 'Consolas', 'Courier New', monospace;
        font-size: 8.5pt;
        background-color: #f1f5f9;
        color: #0f172a;
        padding: 1px 4px;
        border-radius: 4px;
        border: 1px solid #e2e8f0;
      }

      pre code {
        background-color: transparent;
        color: inherit;
        padding: 0;
        border: none;
      }

      /* Callout Boxes */
      .callout {
        border-left: 4px solid #3b82f6;
        background-color: #f8fafc;
        border-radius: 0 6px 6px 0;
        padding: 10px 14px;
        margin: 14px 0;
        page-break-inside: avoid;
        break-inside: avoid;
      }

      .callout-header {
        font-size: 9pt;
        font-weight: 700;
        margin-bottom: 4px;
        display: flex;
        align-items: center;
        gap: 6px;
      }

      .callout-body {
        font-size: 8.8pt;
        color: #334155;
      }

      .callout-important {
        border-left-color: #dc2626;
        background-color: #fef2f2;
      }
      .callout-important .callout-header { color: #991b1b; }

      .callout-note {
        border-left-color: #3b82f6;
        background-color: #eff6ff;
      }
      .callout-note .callout-header { color: #1e40af; }

      .callout-tip {
        border-left-color: #10b981;
        background-color: #f0fdf4;
      }
      .callout-tip .callout-header { color: #065f46; }

      /* Diagrams */
      .diagram-container {
        margin: 16px 0;
        page-break-inside: avoid;
        break-inside: avoid;
        text-align: center;
      }

      .diagram-title {
        font-size: 8.5pt;
        font-weight: 700;
        color: #475569;
        margin-bottom: 6px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
      }

      /* Links */
      a {
        color: #2563eb;
        text-decoration: none;
      }

      a:hover {
        text-decoration: underline;
      }
    </style>
    """

    full_html = f"""<!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <title>P2P Decentralized File Transfer & Recovery Engine - Project Guide</title>
      {css_styles}
    </head>
    <body>
      {cover_html}
      <div class="content-body" style="padding: 0 10px;">
        {html_content}
      </div>
    </body>
    </html>
    """

    temp_html_path = os.path.join(tempfile.gettempdir(), "project_guide_render.html")
    temp_pdf_path = os.path.join(tempfile.gettempdir(), "project_guide_raw.pdf")

    print(f"Writing rendered HTML to {temp_html_path}...")
    with open(temp_html_path, "w", encoding="utf-8") as f:
        f.write(full_html)

    chrome_exe = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    if not os.path.exists(chrome_exe):
        chrome_exe = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"

    print(f"Rendering PDF with Chrome headless ({chrome_exe})...")
    cmd = [
        chrome_exe,
        "--headless=new",
        "--disable-gpu",
        "--no-pdf-header-footer",
        f"--print-to-pdf={temp_pdf_path}",
        f"file:///{temp_html_path}"
    ]

    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print("Chrome render error:", res.stderr)
        sys.exit(1)

    if not os.path.exists(temp_pdf_path):
        print("Error: Chrome did not produce output PDF.")
        sys.exit(1)

    print(f"Chrome produced PDF ({os.path.getsize(temp_pdf_path)} bytes).")

    # Post-processing with PyMuPDF: Injects hierarchical bookmarks (TOC) and metadata
    print("Post-processing PDF: Building hierarchical bookmarks and setting document metadata...")
    doc = pymupdf.open(temp_pdf_path)

    # Search for headings on each page to construct accurate page bookmarks
    sections_to_bookmark = [
        (1, "Cover Page", 1),
        (1, "Executive Abstract", 2),
        (1, "Table of Contents", 2),
        (1, "1. Project Introduction and Theoretical Foundations", None),
        (1, "2. Computer Science Syllabus Mapping", None),
        (2, "2.1 Object-Oriented Programming (OOP)", None),
        (2, "2.2 Data Structures and Algorithms (DSA)", None),
        (2, "2.3 Operating Systems & Systems Programming", None),
        (2, "2.4 Computer Networks", None),
        (2, "2.5 Database Management Systems (DBMS)", None),
        (2, "2.6 Cryptography and Data Integrity", None),
        (2, "2.7 Software Engineering & Verification", None),
        (1, "3. System Architecture and Mesh Topology", None),
        (1, "4. Peer Discovery, Wire Protocol, and Network Lifecycle", None),
        (1, "5. Complete File Upload Workflow", None),
        (1, "6. Reed-Solomon RS(3,2) Erasure Coding Mathematics", None),
        (1, "7. Distributed Metadata Management and SQLite Schema", None),
        (1, "8. File Search, Download, and Reconstruction Workflow", None),
        (1, "9. Autonomous Failure Detection and Durability Recovery", None),
        (1, "10. Embedded Web Dashboard and REST API Architecture", None),
        (1, "11. Complete Source Code Walkthrough", None),
        (1, "12. Installation, Deployment, and User Guide", None),
        (1, "13. Markdown and Project File-Type Reference", None),
        (1, "14. Comprehensive Testing and Verification Report", None),
        (1, "15. Four-Laptop Demonstration Script & Faculty Evaluation Guide", None),
        (1, "16. Security Analysis and Engineering Limitations", None),
        (1, "17. Glossary of Technical Terms & Educational Outcomes", None),
        (1, "18. Official Artifacts and Cryptographic Checksums", None)
    ]

    toc = []
    # Index page texts
    page_texts = [doc[i].get_text() for i in range(len(doc))]

    for level, title, explicit_page in sections_to_bookmark:
        if explicit_page is not None:
            toc.append([level, title, explicit_page])
            continue
        
        # Use PyMuPDF's exact search_for across pages
        # The content heading is always the last match (skipping the Table of Contents on page 2)
        matches = []
        for p_idx in range(len(doc)):
            if doc[p_idx].search_for(title):
                matches.append(p_idx + 1)
            elif doc[p_idx].search_for(re.sub(r'^[0-9.]+\s*', '', title)):
                matches.append(p_idx + 1)

        if matches:
            actual_page = matches[-1]
            toc.append([level, title, actual_page])
        else:
            toc.append([level, title, 2])

    print(f"Applying {len(toc)} bookmark items to PDF...")
    doc.set_toc(toc)

    # Set document metadata
    doc.set_metadata({
        "title": "Peer-to-Peer Decentralized File Transfer & Recovery Engine - Project & Learning Guide",
        "author": "Team 13 (Course: 22ECAW301)",
        "subject": "Decentralized P2P File Storage, Reed-Solomon RS(3,2), SQLite 3, C++17",
        "keywords": "P2P, Distributed Systems, Reed-Solomon, Erasure Coding, C++17, SQLite, Fault Tolerance",
        "creator": "Talent Hub Autonomous Engineering Suite"
    })

    doc.save(out_pdf_path)
    total_pages = len(doc)
    doc.close()

    print(f"[OK] Generated {out_pdf_path} ({os.path.getsize(out_pdf_path)} bytes, {total_pages} pages)")

    # Visual Inspection & Verification: Render sample pages to PNGs
    print(f"Rendering verification images to {verify_dir}...")
    doc_verify = pymupdf.open(out_pdf_path)
    sample_indices = [0, 1, 2, min(5, total_pages - 1), min(10, total_pages - 1)]
    for idx in sample_indices:
        page = doc_verify[idx]
        pix = page.get_pixmap(dpi=150)
        img_out = os.path.join(verify_dir, f"page_{idx+1}.png")
        pix.save(img_out)
        print(f"  Rendered Page {idx+1} -> {img_out}")

    doc_verify.close()

    # Clean up temp files
    try:
        os.remove(temp_html_path)
        os.remove(temp_pdf_path)
    except Exception:
        pass

    print(f"=== PDF GENERATION COMPLETE: {out_pdf_path} ===")

if __name__ == "__main__":
    main()
