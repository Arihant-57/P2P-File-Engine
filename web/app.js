// ==========================================================================
// P2P Decentralized File Transfer & Recovery Engine - Dashboard Logic
// ==========================================================================
(function () {
  "use strict";

  // Extract optional LAN authentication token from URL query (?token=...)
  const urlParams = new URLSearchParams(window.location.search);
  if (urlParams.has("token")) {
    sessionStorage.setItem("rfe_ui_token", urlParams.get("token"));
  }
  const uiToken = sessionStorage.getItem("rfe_ui_token") || "";

  let state = {
    node: null,
    peers: [],
    files: [],
    transfers: [],
    recovery: null,
    storage: null,
    events: [],
    selectedFiles: [],
    currentModalFileId: null
  };

  function apiHeaders(extra) {
    const h = Object.assign({ "X-RFE-Request": "1" }, extra || {});
    if (uiToken) h["X-RFE-Token"] = uiToken;
    return h;
  }

  async function apiGet(path) {
    const res = await fetch(path, { method: "GET", headers: apiHeaders() });
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.reason || ("HTTP " + res.status));
    }
    return data;
  }

  async function apiPostJson(path, payload) {
    const res = await fetch(path, {
      method: "POST",
      headers: apiHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(payload || {})
    });
    const data = await res.json();
    if (!res.ok || data.status === "FAILED") {
      throw new Error(data.reason || ("HTTP " + res.status));
    }
    return data;
  }

  function escapeHtml(s) {
    if (s === null || s === undefined) return "";
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function formatBytes(bytes) {
    const b = Number(bytes) || 0;
    if (b >= 1024 * 1024 * 1024) return (b / (1024 * 1024 * 1024)).toFixed(2) + " GB";
    if (b >= 1024 * 1024) return (b / (1024 * 1024)).toFixed(2) + " MB";
    if (b >= 1024) return (b / 1024).toFixed(2) + " KB";
    return b + " B";
  }

  function formatSpeed(bps) {
    const s = Number(bps) || 0;
    if (s <= 0) return "--";
    return formatBytes(s) + "/s";
  }

  function formatTime(epochMs) {
    if (!epochMs || Number(epochMs) <= 0) return "--";
    const d = new Date(Number(epochMs));
    return d.toLocaleTimeString();
  }

  function statusBadge(st) {
    const u = String(st || "").toUpperCase();
    if (u === "ONLINE" || u === "AVAILABLE" || u === "COMPLETED" || u === "HEALTHY" || u === "STORED" || u === "OK") {
      return '<span class="badge badge-success">' + escapeHtml(u) + '</span>';
    }
    if (u === "SUSPECT" || u === "DEGRADED" || u === "STARTED" || u === "IN_PROGRESS" || u === "UPLOADING" || u === "STALE") {
      return '<span class="badge badge-warning">' + escapeHtml(u) + '</span>';
    }
    if (u === "OFFLINE" || u === "UNRECOVERABLE" || u === "FAILED" || u === "CORRUPTED") {
      return '<span class="badge badge-danger">' + escapeHtml(u) + '</span>';
    }
    return '<span class="badge badge-neutral">' + escapeHtml(u || "UNKNOWN") + '</span>';
  }

  function showToast(msg, isError) {
    const box = document.getElementById("toast-container");
    if (!box) return;
    const t = document.createElement("div");
    t.className = "toast";
    if (isError) t.style.borderLeftColor = "var(--danger)";
    t.textContent = msg;
    box.appendChild(t);
    setTimeout(() => {
      if (t.parentNode) t.parentNode.removeChild(t);
    }, 4200);
  }

  // Tab Navigation
  window.switchTab = function (tabId) {
    document.querySelectorAll(".nav-btn").forEach((btn) => {
      btn.classList.toggle("active", btn.getAttribute("data-tab") === tabId);
    });
    document.querySelectorAll(".tab-section").forEach((sec) => {
      sec.classList.toggle("active", sec.id === "tab-" + tabId);
    });
  };

  document.querySelectorAll(".nav-btn").forEach((btn) => {
    btn.addEventListener("click", () => switchTab(btn.getAttribute("data-tab")));
  });

  // Render Section A: Overview & Header
  function renderOverview() {
    const n = state.node;
    if (!n) return;

    document.getElementById("hdr-peer-id").textContent = n.peer_id;
    document.getElementById("hdr-tcp-ep").textContent = n.address + ":" + n.port;
    document.getElementById("hdr-health").textContent = n.cluster_health;
    document.getElementById("hdr-peers-count").textContent =
      n.online_peers_count + "/" + n.total_peers_count + " ONLINE";

    const dot = document.getElementById("hdr-dot");
    dot.className = "status-dot " + (n.cluster_health === "HEALTHY" ? "dot-online" : "dot-warning");

    document.getElementById("nav-badge-health").textContent = n.cluster_health === "HEALTHY" ? "OK" : "DEG";
    document.getElementById("nav-badge-peers").textContent = n.online_peers_count;
    document.getElementById("nav-badge-files").textContent = n.files_count;
    document.getElementById("nav-badge-transfers").textContent = n.active_transfers;
    document.getElementById("nav-badge-recovery").textContent = n.completed_recovery_count || 0;

    document.getElementById("ov-peer-id").textContent = "Peer " + n.peer_id;
    document.getElementById("ov-endpoint").textContent =
      "TCP " + n.address + ":" + n.port + " • UDP " + n.discovery_port + " (ACTIVE)";
    document.getElementById("ov-peers-online").textContent = n.online_peers_count + " ONLINE";
    document.getElementById("ov-peers-breakdown").textContent =
      n.suspect_peers_count + " Suspect • " + n.offline_peers_count + " Offline";
    document.getElementById("ov-storage-avail").textContent = formatBytes(n.available_storage_bytes);
    document.getElementById("ov-storage-used").textContent =
      "Used: " + formatBytes(n.used_storage_bytes) + " (" + n.local_shard_count + " local shards)";
    document.getElementById("ov-files-count").textContent = n.files_count + " Files";
    document.getElementById("ov-network-health").textContent = "Network Health: " + n.cluster_health;
    document.getElementById("ov-active-transfers").textContent = String(n.active_transfers);
    document.getElementById("ov-transfers-sub").textContent =
      "Uploads: " + (n.active_uploads_count || 0) + " • Downloads: " + (n.active_downloads_count || 0);
    document.getElementById("ov-recovery-count").textContent = (n.completed_recovery_count || 0) + " Repaired";
    document.getElementById("ov-recovery-sub").textContent =
      "Active Leases: " + (n.active_recovery_count || 0) + " • Unrecoverable: " + (n.unrecoverable_count || 0);

    document.getElementById("up-dest-peer").textContent =
      n.peer_id + " (" + n.address + ":" + n.port + ")";

    // Events table
    const evTbody = document.getElementById("ov-events-tbody");
    if (!state.events || state.events.length === 0) {
      evTbody.innerHTML = '<tr><td colspan="4" style="color:var(--text-secondary)">No recent events recorded yet.</td></tr>';
    } else {
      evTbody.innerHTML = state.events.slice(0, 18).map((ev) => {
        return "<tr>" +
          '<td class="mono">' + escapeHtml(formatTime(ev.timestamp_ms)) + "</td>" +
          '<td><span class="badge badge-info">' + escapeHtml(ev.category) + "</span></td>" +
          "<td>" + statusBadge(ev.severity) + "</td>" +
          "<td>" + escapeHtml(ev.message) + "</td>" +
          "</tr>";
      }).join("");
    }
  }

  // Render Section B: Peer Network
  function renderPeers() {
    const tbody = document.getElementById("peers-tbody");
    const peers = state.peers || [];
    document.getElementById("peers-summary-badge").textContent = peers.length + " Known Peers";

    if (peers.length === 0) {
      tbody.innerHTML = '<tr><td colspan="8">No peers discovered yet.</td></tr>';
      return;
    }

    tbody.innerHTML = peers.map((p) => {
      const selfTag = p.is_self ? ' <span class="badge badge-info">SELF</span>' : "";
      const w = p.workload || {};
      const avail = formatBytes(w.available_storage_bytes || p.available_storage_bytes || 0);
      const activeTf = (w.active_transfers !== undefined ? w.active_transfers : (p.active_transfers || 0));
      const capTf = (w.transfer_capacity !== undefined ? w.transfer_capacity : (p.transfer_capacity || 10));
      const hbText = p.is_self ? "Local Node (0 ms)" :
        (p.last_seen_epoch_ms ? formatTime(p.last_seen_epoch_ms) : "Active");
      return "<tr>" +
        '<td class="mono"><strong>' + escapeHtml(p.id || p.peer_id) + "</strong>" + selfTag + "</td>" +
        '<td class="mono">' + escapeHtml(p.address) + "</td>" +
        '<td class="mono">' + escapeHtml(p.port) + "</td>" +
        "<td>" + statusBadge(p.health) + "</td>" +
        '<td class="mono">' + escapeHtml(avail) + "</td>" +
        '<td class="mono">' + escapeHtml(hbText) + "</td>" +
        '<td class="mono">' + escapeHtml(activeTf + " / " + capTf) + "</td>" +
        '<td><span class="badge badge-neutral">' + escapeHtml(p.discovery_source || "P2P_MESH") + "</span></td>" +
        "</tr>";
    }).join("");
  }

  // Render Section D & Section F Catalogue Tables
  function renderCatalogue() {
    const q = (document.getElementById("catalogue-search-input").value || "").trim().toLowerCase();
    const stFilter = document.getElementById("catalogue-status-filter").value;

    const allFiles = state.files || [];
    const filtered = allFiles.filter((f) => {
      if (stFilter !== "ALL" && f.live_status !== stFilter) return false;
      if (!q) return true;
      return (
        String(f.original_name || "").toLowerCase().includes(q) ||
        String(f.file_id || "").toLowerCase().includes(q) ||
        String(f.sha256 || "").toLowerCase().includes(q)
      );
    });

    const tbody = document.getElementById("catalogue-tbody");
    if (filtered.length === 0) {
      tbody.innerHTML = '<tr><td colspan="9" style="color:var(--text-secondary)">No matching files in distributed catalogue.</td></tr>';
    } else {
      tbody.innerHTML = filtered.map((f) => {
        const peersList = (f.hosting_peers || []).map((pid) => '<span class="badge badge-neutral">' + escapeHtml(pid) + '</span>').join(" ");
        const shaShort = f.sha256 ? (f.sha256.substring(0, 14) + "...") : "--";
        return "<tr>" +
          "<td><strong>" + escapeHtml(f.original_name) + "</strong></td>" +
          '<td class="mono">' + escapeHtml(f.file_id) + "</td>" +
          '<td class="mono">' + escapeHtml(formatBytes(f.file_size)) + "</td>" +
          "<td>" + statusBadge(f.status) + "</td>" +
          "<td>" + statusBadge(f.live_status) + "</td>" +
          '<td class="mono">' + escapeHtml(f.total_stripes + " str / " + f.online_shards_count + "/" + f.total_shards_count + " sh") + "</td>" +
          "<td>" + (peersList || "--") + "</td>" +
          '<td class="mono" title="' + escapeHtml(f.sha256) + '">' + escapeHtml(shaShort) + "</td>" +
          "<td>" +
            '<div class="toolbar">' +
              '<button class="btn btn-secondary btn-sm" onclick="openFileDetails(\'' + escapeHtml(f.file_id) + '\')">Details</button>' +
              '<button class="btn btn-primary btn-sm" onclick="triggerBrowserDownload(\'' + escapeHtml(f.file_id) + '\', \'' + escapeHtml(f.original_name) + '\')">DOWNLOAD</button>' +
            "</div>" +
          "</td>" +
          "</tr>";
      }).join("");
    }

    // Also update Section F Download table
    const dlTbody = document.getElementById("dl-catalogue-tbody");
    if (allFiles.length === 0) {
      dlTbody.innerHTML = '<tr><td colspan="7" style="color:var(--text-secondary)">No files in distributed catalogue yet. Upload a file first.</td></tr>';
    } else {
      dlTbody.innerHTML = allFiles.map((f) => {
        const peersList = (f.hosting_peers || []).join(", ");
        return "<tr>" +
          "<td><strong>" + escapeHtml(f.original_name) + "</strong></td>" +
          '<td class="mono">' + escapeHtml(f.file_id) + "</td>" +
          '<td class="mono">' + escapeHtml(formatBytes(f.file_size)) + "</td>" +
          "<td>" + statusBadge(f.live_status) + "</td>" +
          '<td class="mono">' + escapeHtml(f.online_shards_count + "/" + f.total_shards_count) + "</td>" +
          '<td class="mono">' + escapeHtml(peersList || "--") + "</td>" +
          '<td><button class="btn btn-primary btn-sm" onclick="triggerBrowserDownload(\'' + escapeHtml(f.file_id) + '\', \'' + escapeHtml(f.original_name) + '\')">DOWNLOAD</button></td>' +
          "</tr>";
      }).join("");
    }
  }

  // Render Section G: Transfers
  function renderTransfers() {
    const tbody = document.getElementById("transfers-tbody");
    const list = state.transfers || [];
    if (list.length === 0) {
      tbody.innerHTML = '<tr><td colspan="9" style="color:var(--text-secondary)">No transfers recorded on this peer yet.</td></tr>';
    } else {
      tbody.innerHTML = list.map((t) => {
        const pct = t.total_bytes > 0 ? Math.min(100, Math.round((t.bytes_transferred * 100) / t.total_bytes)) : 100;
        const dirBadge = t.direction === "UPLOAD"
          ? '<span class="badge badge-info">UPLOAD</span>'
          : '<span class="badge badge-neutral">DOWNLOAD</span>';
        return "<tr>" +
          '<td class="mono">' + escapeHtml(t.transfer_id) + "</td>" +
          "<td>" + dirBadge + "</td>" +
          "<td><strong>" + escapeHtml(t.filename) + '</strong> <span class="mono" style="color:var(--text-secondary)">(' + escapeHtml(t.file_id || "--") + ")</span></td>" +
          '<td class="mono">' + escapeHtml(formatBytes(t.bytes_transferred) + " / " + formatBytes(t.total_bytes) + " (" + pct + "%)") + "</td>" +
          '<td class="mono">' + escapeHtml(formatSpeed(t.speed_bps)) + "</td>" +
          '<td class="mono">' + escapeHtml(t.shards_count || 0) + "</td>" +
          '<td class="mono">' + escapeHtml(t.peer_id || "--") + "</td>" +
          "<td>" + statusBadge(t.status) + "</td>" +
          '<td style="font-size:0.79rem">' + escapeHtml(t.detail || formatTime(t.timestamp_ms)) + "</td>" +
          "</tr>";
      }).join("");
    }

    const wTbody = document.getElementById("workload-tbody");
    const peers = state.peers || [];
    wTbody.innerHTML = peers.map((p) => {
      const w = p.workload || {};
      const act = w.active_transfers || 0;
      const cap = w.transfer_capacity || 10;
      return "<tr>" +
        '<td class="mono"><strong>' + escapeHtml(p.id || p.peer_id) + "</strong></td>" +
        '<td class="mono">' + escapeHtml(p.address + ":" + p.port) + "</td>" +
        "<td>" + statusBadge(p.health) + "</td>" +
        '<td class="mono">' + escapeHtml(act + " / " + cap) + "</td>" +
        "<td>" + (w.is_busy ? '<span class="badge badge-warning">BUSY</span>' : '<span class="badge badge-success">IDLE / ACCEPTING</span>') + "</td>" +
        "</tr>";
    }).join("");
  }

  // Render Section H: Recovery Monitoring
  function renderRecovery() {
    const rec = state.recovery;
    if (!rec) return;

    const offlinePeers = rec.offline_peers || [];
    document.getElementById("rec-offline-peers").textContent = String(offlinePeers.length);
    document.getElementById("rec-offline-list").textContent =
      offlinePeers.length > 0 ? ("Offline: " + offlinePeers.join(", ")) : "All known peers online";
    document.getElementById("rec-completed-count").textContent = String(rec.completed_count || 0);
    document.getElementById("rec-active-count").textContent = String(rec.started_count || 0);
    document.getElementById("rec-unrec-count").textContent = String(rec.unrecoverable_count || 0);

    const tbody = document.getElementById("recovery-tbody");
    const records = rec.recovery_records || [];
    if (records.length === 0) {
      tbody.innerHTML = '<tr><td colspan="9" style="color:var(--text-secondary)">No autonomous recovery operations have been triggered yet (all peers healthy).</td></tr>';
      return;
    }

    tbody.innerHTML = records.map((r) => {
      return "<tr>" +
        '<td class="mono">' + escapeHtml(formatTime(r.timestamp_epoch_ms)) + "</td>" +
        '<td class="mono">' + escapeHtml(r.recovery_id) + "</td>" +
        '<td class="mono">' + escapeHtml(r.file_id) + "</td>" +
        '<td class="mono"><strong>' + escapeHtml(r.shard_id) + "</strong></td>" +
        '<td class="mono">' + escapeHtml(r.failed_peer_id) + "</td>" +
        '<td class="mono">' + escapeHtml(r.rebuilder_peer_id) + "</td>" +
        '<td class="mono"><strong>' + escapeHtml(r.target_peer_id || "--") + "</strong></td>" +
        "<td>" + statusBadge(r.status) + "</td>" +
        "<td>" + escapeHtml(r.reason) + "</td>" +
        "</tr>";
    }).join("");
  }

  // Render Section I: Settings & Local Storage
  function renderSettings() {
    const n = state.node;
    const st = state.storage;
    if (!n) return;

    const kv = [
      { k: "Local Peer ID", v: n.peer_id },
      { k: "P2P TCP Endpoint", v: n.address + ":" + n.port },
      { k: "UDP Discovery Port", v: String(n.discovery_port) },
      { k: "Web Dashboard Bind", v: n.ui_bind + ":" + n.ui_port + (n.ui_lan_enabled ? " (LAN Token Mode)" : " (Localhost Only)") },
      { k: "Network Architecture", v: n.network_mode },
      { k: "Local Chunks Directory", v: n.storage_dir },
      { k: "Local SQLite Database", v: n.db_path },
      { k: "Used / Free Storage", v: formatBytes(n.used_storage_bytes) + " / " + formatBytes(n.available_storage_bytes) }
    ];

    document.getElementById("settings-kv-grid").innerHTML = kv.map((item) => {
      return '<div class="kv-box"><div class="k">' + escapeHtml(item.k) + '</div><div class="v">' + escapeHtml(item.v) + "</div></div>";
    }).join("");

    const quotaInput = document.getElementById("set-quota-mb");
    if (document.activeElement !== quotaInput) {
      const qMb = n.max_storage_bytes > 0 ? Math.round(n.max_storage_bytes / (1024 * 1024)) : 0;
      quotaInput.value = qMb;
    }

    if (st) {
      document.getElementById("set-local-shard-count").textContent = String(st.local_shards_count || 0);
      const lTbody = document.getElementById("local-shards-tbody");
      const shards = st.local_shards || [];
      if (shards.length === 0) {
        lTbody.innerHTML = '<tr><td colspan="3" style="color:var(--text-secondary)">No .chunk files currently stored on this peer.</td></tr>';
      } else {
        lTbody.innerHTML = shards.map((sh) => {
          return "<tr>" +
            '<td class="mono">' + escapeHtml(sh.filename) + "</td>" +
            '<td class="mono"><strong>' + escapeHtml(sh.shard_id) + "</strong></td>" +
            '<td class="mono">' + escapeHtml(formatBytes(sh.size_bytes)) + "</td>" +
            "</tr>";
        }).join("");
      }
    }
  }

  // Section E: Open File Details Modal
  window.openFileDetails = async function (fileId) {
    try {
      const res = await apiGet("/api/v1/files/" + encodeURIComponent(fileId));
      const f = res.file;
      state.currentModalFileId = f.file_id;

      document.getElementById("md-title").textContent = "File Details: " + f.original_name;
      document.getElementById("md-subtitle").textContent =
        "File ID: " + f.file_id + " • Uploaded via Peer " + f.uploader_peer_id + " • Manifest v" + f.version;

      const kv = [
        { k: "File ID", v: f.file_id },
        { k: "Original Filename", v: f.original_name },
        { k: "Original Size", v: formatBytes(f.file_size) + " (" + f.file_size + " B)" },
        { k: "Live Availability", v: f.live_status + (f.reconstructable ? " (Reconstructable: YES)" : " (Reconstructable: NO)") },
        { k: "Reed-Solomon Policy", v: "RS(3,2) — k=" + f.data_shards_k + " Data + m=" + f.parity_shards_m + " Parity" },
        { k: "Stripes & Shards", v: f.total_stripes + " Stripes / " + f.online_shards_count + " of " + f.total_shards_count + " Shards Online" },
        { k: "Storage Peers", v: (f.hosting_peers || []).join(", ") || "None" },
        { k: "Full-File SHA-256", v: f.sha256 }
      ];

      document.getElementById("md-kv-grid").innerHTML = kv.map((item) => {
        return '<div class="kv-box"><div class="k">' + escapeHtml(item.k) + '</div><div class="v">' + escapeHtml(item.v) + "</div></div>";
      }).join("");

      const peerHealthMap = {};
      (state.peers || []).forEach((p) => { peerHealthMap[p.id || p.peer_id] = p.health; });

      const shTbody = document.getElementById("md-shards-tbody");
      const shards = f.shards || [];
      shTbody.innerHTML = shards.map((sh) => {
        const locs = (sh.locations || []).map((loc) => {
          const ph = peerHealthMap[loc.peer_id] || "UNKNOWN";
          return '<span class="mono">' + escapeHtml(loc.peer_id) + "</span> " +
            statusBadge(loc.location_status) + " " + statusBadge(ph);
        }).join("<br/>");
        return "<tr>" +
          '<td class="mono">S' + String(sh.stripe_index).padStart(3, "0") + "</td>" +
          '<td class="mono"><strong>' + escapeHtml(sh.shard_id) + "</strong></td>" +
          "<td>" + statusBadge(sh.shard_type) + "</td>" +
          '<td class="mono">' + escapeHtml(formatBytes(sh.original_bytes)) + "</td>" +
          '<td class="mono">' + escapeHtml(formatBytes(sh.shard_size)) + "</td>" +
          "<td>" + (locs || "--") + "</td>" +
          '<td class="mono" title="' + escapeHtml(sh.sha256) + '">' + escapeHtml((sh.sha256 || "").substring(0, 16) + "...") + "</td>" +
          "</tr>";
      }).join("");

      document.getElementById("file-details-modal").classList.add("open");
    } catch (err) {
      showToast("Failed to load file details: " + err.message, true);
    }
  };

  document.getElementById("md-btn-close").addEventListener("click", () => {
    document.getElementById("file-details-modal").classList.remove("open");
  });

  document.getElementById("md-btn-download").addEventListener("click", () => {
    if (state.currentModalFileId) {
      triggerBrowserDownload(state.currentModalFileId, "");
    }
  });

  // Section C: Upload Workflow (Drag & Drop + Browse + Progress + Retry)
  const dropzone = document.getElementById("upload-dropzone");
  const fileInput = document.getElementById("upload-file-input");

  function setSelectedUploadFiles(fileList) {
    state.selectedFiles = Array.from(fileList || []);
    const box = document.getElementById("upload-selected-box");
    const tbody = document.getElementById("upload-queue-tbody");
    if (state.selectedFiles.length === 0) {
      box.style.display = "none";
      return;
    }
    box.style.display = "block";
    const peerLabel = state.node ? (state.node.peer_id + " (" + state.node.address + ":" + state.node.port + ")") : "Local Peer";
    tbody.innerHTML = state.selectedFiles.map((f, idx) => {
      return '<tr id="up-row-' + idx + '">' +
        "<td><strong>" + escapeHtml(f.name) + "</strong></td>" +
        '<td class="mono">' + escapeHtml(formatBytes(f.size)) + " (" + f.size + " B)</td>" +
        '<td class="mono">' + escapeHtml(peerLabel) + "</td>" +
        '<td id="up-row-status-' + idx + '"><span class="badge badge-neutral">READY</span></td>' +
        "</tr>";
    }).join("");
  }

  document.getElementById("btn-browse-files").addEventListener("click", (e) => {
    e.stopPropagation();
    fileInput.click();
  });

  dropzone.addEventListener("click", () => fileInput.click());
  fileInput.addEventListener("change", () => setSelectedUploadFiles(fileInput.files));

  dropzone.addEventListener("dragover", (e) => {
    e.preventDefault();
    dropzone.classList.add("dragover");
  });
  dropzone.addEventListener("dragleave", () => dropzone.classList.remove("dragover"));
  dropzone.addEventListener("drop", (e) => {
    e.preventDefault();
    dropzone.classList.remove("dragover");
    if (e.dataTransfer && e.dataTransfer.files) {
      setSelectedUploadFiles(e.dataTransfer.files);
    }
  });

  document.getElementById("btn-clear-upload").addEventListener("click", () => {
    fileInput.value = "";
    setSelectedUploadFiles([]);
    document.getElementById("upload-progress-box").style.display = "none";
    document.getElementById("upload-result-box").style.display = "none";
  });

  function uploadSingleFile(file, rowIdx) {
    return new Promise((resolve, reject) => {
      const progBox = document.getElementById("upload-progress-box");
      const progBar = document.getElementById("up-progress-bar");
      const speedLbl = document.getElementById("up-speed-label");
      const titleLbl = document.getElementById("up-progress-title");
      const stageMsg = document.getElementById("up-stage-msg");
      const rowStatus = document.getElementById("up-row-status-" + rowIdx);

      progBox.style.display = "block";
      progBar.style.width = "0%";
      titleLbl.textContent = "Uploading " + file.name + " (" + formatBytes(file.size) + ")...";
      stageMsg.textContent = "Streaming file to local peer & encoding RS(3,2) stripes...";
      if (rowStatus) rowStatus.innerHTML = '<span class="badge badge-warning">UPLOADING</span>';

      const startTime = performance.now();
      const xhr = new XMLHttpRequest();
      xhr.open("POST", "/api/v1/uploads?filename=" + encodeURIComponent(file.name), true);
      xhr.setRequestHeader("X-RFE-Request", "1");
      xhr.setRequestHeader("X-File-Name", encodeURIComponent(file.name));
      xhr.setRequestHeader("Content-Type", "application/octet-stream");
      if (uiToken) xhr.setRequestHeader("X-RFE-Token", uiToken);

      xhr.upload.onprogress = function (ev) {
        if (ev.lengthComputable && ev.total > 0) {
          const pct = Math.min(95, Math.round((ev.loaded * 100) / ev.total));
          const elapsedSec = Math.max(0.001, (performance.now() - startTime) / 1000.0);
          const bps = ev.loaded / elapsedSec;
          progBar.style.width = pct + "%";
          speedLbl.textContent = formatSpeed(bps) + " (" + pct + "%)";
          if (pct >= 95) {
            stageMsg.textContent = "Distributing RS(3,2) shards across 3 distinct storage peers & verifying SHA-256...";
          }
        }
      };

      xhr.onload = function () {
        let resp = {};
        try { resp = JSON.parse(xhr.responseText || "{}"); } catch (_) {}
        if (xhr.status >= 200 && xhr.status < 300 && resp.status === "OK") {
          progBar.style.width = "100%";
          speedLbl.textContent = formatSpeed(resp.speed_bps || 0) + " (100%)";
          stageMsg.textContent = "Upload & RS(3,2) distribution complete! All shards verified via SHA-256.";
          if (rowStatus) rowStatus.innerHTML = '<span class="badge badge-success">COMPLETED</span>';
          resolve(resp);
        } else {
          if (rowStatus) rowStatus.innerHTML = '<span class="badge badge-danger">FAILED</span>';
          reject(new Error(resp.reason || ("Upload failed (HTTP " + xhr.status + ")")));
        }
      };

      xhr.onerror = function () {
        if (rowStatus) rowStatus.innerHTML = '<span class="badge badge-danger">FAILED</span>';
        reject(new Error("Network error while communicating with local peer HTTP server"));
      };

      xhr.send(file);
    });
  }

  async function executeUploadQueue() {
    if (!state.selectedFiles || state.selectedFiles.length === 0) {
      showToast("Please select or drag a file first.", true);
      return;
    }

    const uploadBtn = document.getElementById("btn-upload-file");
    const retryBtn = document.getElementById("btn-retry-upload");
    const resBox = document.getElementById("upload-result-box");
    const resKv = document.getElementById("up-result-kv");
    const resLog = document.getElementById("up-placement-log");
    const resTitle = document.getElementById("up-result-title");

    uploadBtn.disabled = true;
    retryBtn.style.display = "none";

    try {
      for (let i = 0; i < state.selectedFiles.length; i++) {
        const f = state.selectedFiles[i];
        const resp = await uploadSingleFile(f, i);
        const m = resp.manifest || {};

        resBox.style.display = "block";
        resTitle.innerHTML = 'Upload Completed — <span class="badge badge-success">AVAILABLE</span>';
        const kv = [
          { k: "File ID", v: m.file_id || resp.file_id },
          { k: "Filename", v: m.original_name || f.name },
          { k: "Original Size", v: formatBytes(m.file_size || f.size) },
          { k: "Transfer Speed", v: formatSpeed(resp.speed_bps) + " (" + (resp.elapsed_ms || 0) + " ms)" },
          { k: "Stripes & Shards", v: (m.total_stripes || 0) + " Stripes / " + (m.total_shards || 0) + " Shards" },
          { k: "SHA-256 Digest", v: m.sha256 || "--" }
        ];
        resKv.innerHTML = kv.map((item) => {
          return '<div class="kv-box"><div class="k">' + escapeHtml(item.k) + '</div><div class="v">' + escapeHtml(item.v) + "</div></div>";
        }).join("");
        resLog.textContent = resp.placement_log || "All shards distributed and verified.";
        showToast("Uploaded " + f.name + " (" + (m.file_id || resp.file_id) + ") successfully!");
      }
      await refreshAll();
    } catch (err) {
      resBox.style.display = "block";
      resTitle.innerHTML = 'Upload Failed — <span class="badge badge-danger">ERROR</span>';
      resKv.innerHTML = '<div class="kv-box"><div class="k">Error</div><div class="v">' + escapeHtml(err.message) + "</div></div>";
      resLog.textContent = err.message;
      retryBtn.style.display = "inline-flex";
      showToast("Upload failed: " + err.message, true);
    } finally {
      uploadBtn.disabled = false;
    }
  }

  document.getElementById("btn-upload-file").addEventListener("click", executeUploadQueue);
  document.getElementById("btn-retry-upload").addEventListener("click", executeUploadQueue);

  // Section F: Download & Reconstruct Workflow
  window.triggerBrowserDownload = function (fileIdOrName, suggestedName) {
    const target = (fileIdOrName || "").trim();
    if (!target) {
      showToast("Please specify a File ID or filename to download.", true);
      return;
    }

    const statusBox = document.getElementById("dl-status-box");
    const statusTitle = document.getElementById("dl-status-title");
    const speedLbl = document.getElementById("dl-speed-label");
    const progBar = document.getElementById("dl-progress-bar");
    const kvBox = document.getElementById("dl-telemetry-kv");

    statusBox.style.display = "block";
    statusTitle.textContent = "Retrieving & reconstructing '" + target + "' from P2P storage peers...";
    progBar.style.width = "15%";
    speedLbl.textContent = "Connecting to storage peers...";
    kvBox.innerHTML = "";

    const startTime = performance.now();
    const xhr = new XMLHttpRequest();
    xhr.open("GET", "/api/v1/downloads/" + encodeURIComponent(target), true);
    xhr.responseType = "blob";
    xhr.setRequestHeader("X-RFE-Request", "1");
    if (uiToken) xhr.setRequestHeader("X-RFE-Token", uiToken);

    xhr.onprogress = function (ev) {
      if (ev.lengthComputable && ev.total > 0) {
        const pct = Math.min(100, Math.round((ev.loaded * 100) / ev.total));
        const elapsedSec = Math.max(0.001, (performance.now() - startTime) / 1000.0);
        progBar.style.width = pct + "%";
        speedLbl.textContent = formatSpeed(ev.loaded / elapsedSec) + " (" + pct + "%)";
      }
    };

    xhr.onload = function () {
      if (xhr.status >= 200 && xhr.status < 300) {
        const blob = xhr.response;
        const elapsedMs = Math.max(1, Math.round(performance.now() - startTime));
        const bps = Math.round((blob.size * 1000) / elapsedMs);
        const sha256 = xhr.getResponseHeader("X-SHA256") || "Verified";
        const fileId = xhr.getResponseHeader("X-File-Id") || target;
        const shardsRet = xhr.getResponseHeader("X-Shards-Retrieved") || "--";
        const stripesRec = xhr.getResponseHeader("X-Stripes-Reconstructed") || "--";
        let dlFilename = suggestedName || xhr.getResponseHeader("X-File-Name") || target;

        progBar.style.width = "100%";
        statusTitle.innerHTML = 'Download Complete &amp; Verified — <span class="badge badge-success">SHA-256 MATCH</span>';
        speedLbl.textContent = formatSpeed(bps) + " (100%)";

        const kv = [
          { k: "File ID", v: fileId },
          { k: "Filename", v: dlFilename },
          { k: "Reconstructed Size", v: formatBytes(blob.size) + " (" + blob.size + " B)" },
          { k: "Shards / Stripes", v: shardsRet + " Shards / " + stripesRec + " Stripes" },
          { k: "Transfer Speed", v: formatSpeed(bps) + " (" + elapsedMs + " ms)" },
          { k: "Verified SHA-256", v: sha256 }
        ];
        kvBox.innerHTML = kv.map((item) => {
          return '<div class="kv-box"><div class="k">' + escapeHtml(item.k) + '</div><div class="v">' + escapeHtml(item.v) + "</div></div>";
        }).join("");

        // Trigger standard browser save dialog
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.style.display = "none";
        a.href = url;
        a.download = dlFilename;
        document.body.appendChild(a);
        a.click();
        setTimeout(() => {
          window.URL.revokeObjectURL(url);
          if (a.parentNode) a.parentNode.removeChild(a);
        }, 1500);

        showToast("Downloaded & verified " + dlFilename + " (SHA-256 verified)!");
        refreshAll();
      } else {
        const reader = new FileReader();
        reader.onload = function () {
          let errMsg = "Download failed (HTTP " + xhr.status + ")";
          try {
            const j = JSON.parse(reader.result);
            if (j.reason) errMsg = j.reason;
          } catch (_) {}
          progBar.style.width = "0%";
          statusTitle.innerHTML = 'Download Refused — <span class="badge badge-danger">FAILED</span>';
          speedLbl.textContent = "--";
          kvBox.innerHTML = '<div class="kv-box"><div class="k">Failure Reason</div><div class="v">' + escapeHtml(errMsg) + "</div></div>";
          showToast("Download failed: " + errMsg, true);
        };
        reader.readAsText(xhr.response);
      }
    };

    xhr.onerror = function () {
      showToast("Network error during download request.", true);
    };

    xhr.send();
  };

  document.getElementById("btn-start-download").addEventListener("click", () => {
    const val = document.getElementById("dl-target-input").value;
    triggerBrowserDownload(val, "");
  });

  // Catalogue search & sync handlers
  document.getElementById("btn-catalogue-search").addEventListener("click", renderCatalogue);
  document.getElementById("catalogue-search-input").addEventListener("input", renderCatalogue);
  document.getElementById("catalogue-status-filter").addEventListener("change", renderCatalogue);
  document.getElementById("btn-catalogue-sync").addEventListener("click", async () => {
    try {
      const q = document.getElementById("catalogue-search-input").value.trim();
      const res = await apiGet("/api/v1/search?q=" + encodeURIComponent(q));
      state.files = res.files || [];
      renderCatalogue();
      showToast("Synchronized catalogue across online peers (" + state.files.length + " files).");
    } catch (err) {
      showToast("Catalogue sync failed: " + err.message, true);
    }
  });

  // Trigger Recovery Scan handler
  document.getElementById("btn-trigger-recovery").addEventListener("click", async () => {
    try {
      const res = await apiPostJson("/api/v1/recovery", { force_owner: true });
      showToast("Recovery scan complete: " + (res.repaired_count || 0) + " shard(s) repaired.");
      await refreshAll();
    } catch (err) {
      showToast("Recovery scan failed: " + err.message, true);
    }
  });

  // Add seed peer handlers (Section B & Section I)
  async function handleAddSeed(inputId) {
    const ep = document.getElementById(inputId).value.trim();
    if (!ep) {
      showToast("Enter a seed peer endpoint (<ip:port>).", true);
      return;
    }
    try {
      await apiPostJson("/api/v1/settings", { add_seed_peer: ep });
      document.getElementById(inputId).value = "";
      showToast("Added & probed seed peer " + ep);
      setTimeout(refreshAll, 350);
    } catch (err) {
      showToast("Failed to add seed peer: " + err.message, true);
    }
  }

  document.getElementById("btn-add-seed").addEventListener("click", () => handleAddSeed("input-quick-seed"));
  document.getElementById("btn-settings-add-seed").addEventListener("click", () => handleAddSeed("set-seed-ep"));

  document.getElementById("btn-save-quota").addEventListener("click", async () => {
    const mb = Number(document.getElementById("set-quota-mb").value) || 0;
    try {
      await apiPostJson("/api/v1/settings", { max_storage_mb: mb });
      showToast("Updated local storage quota to " + (mb > 0 ? mb + " MB" : "Unlimited"));
      await refreshAll();
    } catch (err) {
      showToast("Failed to update storage quota: " + err.message, true);
    }
  });

  document.getElementById("btn-global-refresh").addEventListener("click", () => {
    refreshAll();
    showToast("Refreshed local peer telemetry.");
  });

  // Poll & Refresh All Dashboard Data
  window.refreshAll = async function () {
    try {
      const [nodeRes, peersRes, filesRes, tfRes, recRes, stRes, evRes] = await Promise.all([
        apiGet("/api/v1/node"),
        apiGet("/api/v1/peers"),
        apiGet("/api/v1/files"),
        apiGet("/api/v1/transfers"),
        apiGet("/api/v1/recovery"),
        apiGet("/api/v1/storage"),
        apiGet("/api/v1/events")
      ]);

      state.node = nodeRes;
      state.peers = peersRes.peers || [];
      state.files = filesRes.files || [];
      state.transfers = tfRes.transfers || [];
      state.recovery = recRes;
      state.storage = stRes;
      state.events = evRes.events || [];

      renderOverview();
      renderPeers();
      renderCatalogue();
      renderTransfers();
      renderRecovery();
      renderSettings();
    } catch (err) {
      const badge = document.getElementById("ov-poll-status");
      if (badge) {
        badge.className = "badge badge-danger";
        badge.textContent = "Peer Connection Lost";
      }
    }
  };

  // Initial load + periodic lightweight refresh every 1.5s
  refreshAll();
  setInterval(refreshAll, 1500);
})();
