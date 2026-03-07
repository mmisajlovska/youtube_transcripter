/**
 * script.js — YouTube Pipeline Orchestrator v20
 *
 * Changes over v19:
 *  - Config form now uses PG_* keys (was MYSQL_* — broken in v19)
 *  - Correct default port 5432 (was 3306)
 *  - Correct default user 'postgres' (was 'root')
 *  - All MySQL references removed
 */

// ── State ──────────────────────────────────────────────────────────────────
const S = {
  chunkerMode:     'channel',
  storageMode:     'local',
  allPickerVideos: [],
  channels:        [],
  activeChunkerTaskId: null,
};

// ── DOM helpers ────────────────────────────────────────────────────────────
const $    = id  => document.getElementById(id);
const show = id  => $(id).classList.remove('hidden');
const hide = id  => $(id).classList.add('hidden');

function setDot(active) {
  const d   = $('statusDot');
  d.className = 'status-dot' + (active ? ' active' : '');
  d.title     = active ? 'Running' : 'Idle';
}

// ── Error banner ───────────────────────────────────────────────────────────
let _errorTimer = null;

function showError(msg) {
  $('errorMsg').textContent = msg;
  const banner = $('errorBanner');
  banner.classList.remove('hidden', 'fade-out');
  if (_errorTimer) clearTimeout(_errorTimer);
  _errorTimer = setTimeout(() => closeError(), 5000);
}

function closeError() {
  const banner = $('errorBanner');
  banner.classList.add('fade-out');
  setTimeout(() => banner.classList.add('hidden'), 400);
}

// ── Log helpers ────────────────────────────────────────────────────────────
function appendLog(id, line) {
  const pre = $(id);
  const ts  = new Date().toLocaleTimeString('en-US', { hour12: false });
  pre.textContent += `[${ts}] ${line}\n`;
  pre.scrollTop    = pre.scrollHeight;
}

function toggleLog(wrapId, btnId) {
  const wrap = $(wrapId);
  const btn  = $(btnId);
  const collapsed = wrap.classList.toggle('log-collapsed');
  btn.textContent = collapsed ? '▶ Show Log' : '▼ Hide Log';
}

function copyLog(preId) {
  const text = $(preId).textContent;
  if (!text.trim()) return;
  navigator.clipboard.writeText(text).then(() => {
    showToast('Log copied to clipboard');
  }).catch(() => {
    const ta = document.createElement('textarea');
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
    showToast('Log copied to clipboard');
  });
}

function showToast(msg) {
  const t = $('toastMsg');
  t.textContent = msg;
  t.classList.remove('hidden', 'fade-out');
  setTimeout(() => {
    t.classList.add('fade-out');
    setTimeout(() => t.classList.add('hidden'), 400);
  }, 2000);
}

/** Subscribe to a server-sent-event task stream. */
function sseSubscribe(taskId, { onLog, onProgress, onCurrentVideo, onDone, onError, onStopped }) {
  const es = new EventSource(`/status/${taskId}`);
  es.addEventListener('log',           e => onLog          && onLog(JSON.parse(e.data).line));
  es.addEventListener('progress',      e => onProgress     && onProgress(JSON.parse(e.data)));
  es.addEventListener('current_video', e => onCurrentVideo && onCurrentVideo(JSON.parse(e.data)));
  es.addEventListener('done',          e => { es.close(); onDone    && onDone(JSON.parse(e.data)); });
  es.addEventListener('stopped',       e => { es.close(); onStopped && onStopped(JSON.parse(e.data)); });
  es.addEventListener('error',         e => {
    let data = {};
    try { data = JSON.parse(e.data); } catch (_) {}
    es.close();
    onError && onError(data.message || 'Unknown error', data);
  });
  return es;
}

/** Convert seconds → "Xh Ym" string. */
function secsToHM(totalSecs) {
  if (!totalSecs || totalSecs <= 0) return '0h 0m';
  const h = Math.floor(totalSecs / 3600);
  const m = Math.floor((totalSecs % 3600) / 60);
  return `${h}h ${m}m`;
}


// ── localStorage helpers ───────────────────────────────────────────────────
function lsGet(key, def = '') {
  try { return localStorage.getItem(key) ?? def; } catch { return def; }
}
function lsSet(key, val) {
  try { localStorage.setItem(key, val); } catch {}
}


// ── Password toggle ────────────────────────────────────────────────────────
function togglePassword(inputId, btnId) {
  const inp = $(inputId);
  const btn = $(btnId);
  if (inp.type === 'password') {
    inp.type      = 'text';
    btn.innerHTML = eyeOffIcon();
    btn.title     = 'Hide password';
  } else {
    inp.type      = 'password';
    btn.innerHTML = eyeIcon();
    btn.title     = 'Show password';
  }
}

function eyeIcon() {
  return `<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>`;
}
function eyeOffIcon() {
  return `<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/></svg>`;
}


// ── Page switching ─────────────────────────────────────────────────────────
function switchPage(page) {
  hide('pageConfig'); hide('pageCollector'); hide('pageChunker');
  ['tab0', 'tab1', 'tab2'].forEach(t => $(t).classList.remove('active'));

  if (page === 'config') {
    show('pageConfig');
    $('tab0').classList.add('active');
    loadConfigForm();
  } else if (page === 'collector') {
    show('pageCollector');
    $('tab1').classList.add('active');
  } else {
    show('pageChunker');
    $('tab2').classList.add('active');
    loadChunkerPage();
  }
}


// ── Storage mode toggle ────────────────────────────────────────────────────
function setStorageMode(mode) {
  S.storageMode = mode;
  $('radioCardLocal').classList.toggle('active', mode === 'local');
  $('radioCardMinio').classList.toggle('active', mode === 'minio');
  if (mode === 'local') {
    show('storageLocal'); hide('storageMinio');
  } else {
    hide('storageLocal'); show('storageMinio');
  }
}


// ── Chunker mode toggle ────────────────────────────────────────────────────
function setChunkerMode(mode) {
  S.chunkerMode = mode;
  if (mode === 'channel') {
    show('modeChannel'); hide('modeSelected');
    $('radioCardChannel').classList.add('active');
    $('radioCardSelected').classList.remove('active');
  } else {
    hide('modeChannel'); show('modeSelected');
    $('radioCardSelected').classList.add('active');
    $('radioCardChannel').classList.remove('active');
  }
}


// ── Chunker page loader ────────────────────────────────────────────────────
async function loadChunkerPage() {
  await loadGlobalStats();
  await loadChannelDropdowns();
}


// ── Global stats ───────────────────────────────────────────────────────────
async function loadGlobalStats() {
  try {
    const res  = await fetch('/api/global_stats');
    const data = await res.json();
    if (data.error) return;
    $('gStatTotal').textContent   = (data.total_videos   || 0).toLocaleString();
    $('gStatChunked').textContent = (data.chunked_count  || 0).toLocaleString();
    $('gStatPending').textContent = (data.pending_count  || 0).toLocaleString();
    $('gStatTime').textContent    = secsToHM(data.total_chunked_seconds || 0);
  } catch (_) {}
}


// ── Channel dropdowns ──────────────────────────────────────────────────────
async function loadChannelDropdowns() {
  try {
    const res  = await fetch('/api/channels');
    const data = await res.json();
    if (data.error) { showError(data.error); return; }
    S.channels = data;
    populateDropdown('channelDropdown',  data);
    populateDropdown('channelDropdownB', data);
    onChannelDropdownChange();
    if (S.chunkerMode === 'selected' && $('channelDropdownB').value) {
      loadVideoTable();
    }
  } catch (e) {
    showError('Could not load channels: ' + e.message);
  }
}

function populateDropdown(dropId, channels) {
  const sel = $(dropId);
  const cur = sel.value;
  sel.innerHTML = '<option value="">— select a channel —</option>' +
    channels.map(c =>
      `<option value="${c.channel_id}">${c.channel_name || c.channel_id} (${c.video_count} videos)</option>`
    ).join('');
  if (cur && [...sel.options].some(o => o.value === cur)) sel.value = cur;
}

function onChannelDropdownChange() {
  const ch = S.channels.find(c => c.channel_id === $('channelDropdown').value);
  if (ch) {
    show('channelStats');
    $('statTotal').textContent       = ch.video_count;
    $('statChunked').textContent     = ch.chunked_count || 0;
    $('statPending').textContent     = ch.video_count - (ch.chunked_count || 0);
    $('statChunkedTime').textContent = secsToHM(ch.total_chunked_seconds || 0);
    $('chunkChannelBtn').disabled    = false;
  } else {
    hide('channelStats');
    $('chunkChannelBtn').disabled = true;
  }
}


// ── Video picker (mode B) ──────────────────────────────────────────────────
async function loadVideoTable() {
  const channelId = $('channelDropdownB').value;
  if (!channelId) { hide('videoPicker'); return; }

  $('pickerTableBody').innerHTML = '<tr><td colspan="6" class="loading-cell">Loading…</td></tr>';
  show('videoPicker');

  try {
    const res  = await fetch(`/api/channels/${encodeURIComponent(channelId)}/videos`);
    const data = await res.json();
    if (data.error) { showError(data.error); return; }
    S.allPickerVideos  = data;
    $('videoFilter').value = '';
    renderPickerTable(data);
    updateSelCount();
  } catch (e) {
    showError('Could not load videos: ' + e.message);
  }
}

function reloadVideoTable() {
  loadChunkerPage();
  if (S.chunkerMode === 'selected') loadVideoTable();
}

function renderPickerTable(videos) {
  const tbody = $('pickerTableBody');
  if (!videos.length) {
    tbody.innerHTML = '';
    show('pickerEmpty');
    return;
  }
  hide('pickerEmpty');
  tbody.innerHTML = videos.map(v => {
    const badge   = v.is_chunked
      ? '<span class="badge badge-green">✓ Chunked</span>'
      : '<span class="badge badge-red">✗ Pending</span>';
    const checked = !v.is_chunked ? 'checked' : '';
    return `<tr>
      <td><input type="checkbox" class="vid-check" data-id="${v.video_id}" ${checked} onchange="updateSelCount()"/></td>
      <td class="vid-id"><a href="https://www.youtube.com/watch?v=${v.video_id}" target="_blank" rel="noopener">${v.video_id}</a></td>
      <td class="vid-title">${v.video_title || '<span class="no-title">No title</span>'}</td>
      <td class="vid-date">${v.published_at || '—'}</td>
      <td class="vid-dur">${v.duration || '—'}</td>
      <td>${badge}</td>
    </tr>`;
  }).join('');
  updateSelCount();
}

function filterPickerVideos() {
  const q = $('videoFilter').value.toLowerCase();
  renderPickerTable(
    q ? S.allPickerVideos.filter(v =>
      (v.video_title || '').toLowerCase().includes(q) || v.video_id.toLowerCase().includes(q)
    ) : S.allPickerVideos
  );
}

function getCheckedIds() {
  return [...document.querySelectorAll('.vid-check:checked')].map(cb => cb.dataset.id);
}

function updateSelCount() {
  const all = document.querySelectorAll('.vid-check');
  const n   = getCheckedIds().length;
  $('selCount').textContent       = `${n} selected`;
  $('chunkSelectedBtn').disabled  = n === 0;
  $('masterCheck').checked        = n > 0 && n === all.length;
  $('masterCheck').indeterminate  = n > 0 && n < all.length;
}

function masterToggle(cb) {
  document.querySelectorAll('.vid-check').forEach(c => { c.checked = cb.checked; });
  updateSelCount();
}

function selectAll()       { document.querySelectorAll('.vid-check').forEach(c => c.checked = true);  updateSelCount(); }
function selectNone()      { document.querySelectorAll('.vid-check').forEach(c => c.checked = false); updateSelCount(); }
function selectUnchecked() {
  document.querySelectorAll('.vid-check').forEach(c => {
    const vid = S.allPickerVideos.find(v => v.video_id === c.dataset.id);
    c.checked = vid ? !vid.is_chunked : false;
  });
  updateSelCount();
}


// ── Collector (Tab 1) ──────────────────────────────────────────────────────
async function runCollector() {
  const apiKey     = $('api_key').value.trim();
  const channelUrl = $('channel_id').value.trim();
  if (!apiKey)     { showError('YouTube API Key is required.'); return; }
  if (!channelUrl) { showError('Channel URL is required.');     return; }

  lsSet('yt_api_key', apiKey);

  const btn    = $('collectorBtn');
  btn.disabled = true;
  btn.innerHTML = spinnerBtn('Resolving…');
  setDot(true);

  let channelId;
  try {
    const rres  = await fetch('/api/resolve_channel', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ api_key: apiKey, channel_url: channelUrl }),
    });
    const rdata = await rres.json();
    if (rdata.error) {
      showError('Could not resolve channel: ' + rdata.error);
      resetCollectorBtn(); setDot(false); return;
    }
    channelId = rdata.channel_id;
  } catch (e) {
    showError('Channel resolution failed: ' + e.message);
    resetCollectorBtn(); setDot(false); return;
  }

  btn.innerHTML = spinnerBtn('Running…');

  show('collectorOutputCard');
  hide('collectorResult');
  $('collectorLog').textContent   = '';
  $('collectorBadge').textContent = 'RUNNING';
  $('collectorBadge').className   = 'running-badge';
  $('collectorOutputCard').scrollIntoView({ behavior: 'smooth', block: 'nearest' });

  try {
    const res = await fetch('/run_collector', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: apiKey, channel_id: channelId }),
    });
    const d = await res.json();
    if (d.error) { showError(d.error); resetCollectorBtn(); setDot(false); return; }

    sseSubscribe(d.task_id, {
      onLog: line => appendLog('collectorLog', line),
      onDone: data => {
        $('collectorBadge').textContent = 'DONE';
        $('collectorBadge').classList.add('done');
        $('resChannelName').textContent = data.channel_name || '—';
        $('resInserted').textContent    = (data.inserted  ?? 0).toLocaleString();
        $('resApiItems').textContent    = (data.api_items ?? 0).toLocaleString();
        show('collectorResult');
        const ids = data.new_ids || [];
        if (ids.length) {
          $('idsList').innerHTML = ids.map(id => `<span class="id-chip">${id}</span>`).join('');
          show('idsWrap');
        } else {
          hide('idsWrap');
        }
        setDot(false); resetCollectorBtn();
      },
      onError: (msg) => {
        appendLog('collectorLog', '[ERROR] ' + msg);
        showError('Collector failed: ' + msg);
        $('collectorBadge').textContent = 'ERROR';
        $('collectorBadge').classList.add('error');
        setDot(false); resetCollectorBtn();
      },
    });
  } catch (e) {
    showError(e.message); resetCollectorBtn(); setDot(false);
  }
}

function resetCollectorBtn() {
  const btn    = $('collectorBtn');
  btn.disabled = false;
  btn.innerHTML = `<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polygon points="10,8 16,12 10,16"/></svg> Run Collector`;
}

function resetCollector() {
  hide('collectorOutputCard');
  $('collectorLog').textContent   = '';
  $('collectorBadge').textContent = 'RUNNING';
  $('collectorBadge').className   = 'running-badge';
  resetCollectorBtn(); setDot(false);
}


// ── Chunker (Tab 2) ────────────────────────────────────────────────────────
async function runChunkChannel() {
  const channelId = $('channelDropdown').value;
  if (!channelId) { showError('Please select a channel.'); return; }
  const ch = S.channels.find(c => c.channel_id === channelId);
  const payload = buildChunkerPayload({ channel_id: channelId, mode: 'channel', total_videos: ch?.video_count || 0 });
  if (!payload) return;
  startChunker(payload);
}

async function runChunkSelected() {
  const channelId = $('channelDropdownB').value;
  const videoIds  = getCheckedIds();
  if (!channelId)       { showError('Please select a channel.');   return; }
  if (!videoIds.length) { showError('Select at least one video.'); return; }
  const payload = buildChunkerPayload({ channel_id: channelId, mode: 'selected', video_ids: videoIds, total_videos: videoIds.length });
  if (!payload) return;
  startChunker(payload);
}

function buildChunkerPayload(base) {
  if (S.storageMode === 'minio') {
    const endpoint   = $('minio_endpoint').value.trim();
    const accessKey  = $('minio_access_key').value.trim();
    const secretKey  = $('minio_secret_key').value.trim();
    const bucket     = $('minio_bucket').value.trim();
    if (!endpoint)   { showError('MinIO endpoint is required.');   return null; }
    if (!accessKey)  { showError('MinIO access key is required.'); return null; }
    if (!secretKey)  { showError('MinIO secret key is required.'); return null; }
    if (!bucket)     { showError('MinIO bucket is required.');     return null; }
    return { ...base, storage: 'minio', minio_endpoint: endpoint, minio_access_key: accessKey, minio_secret_key: secretKey, minio_bucket: bucket };
  }
  const outputDir = $('chunker_output').value.trim();
  if (!outputDir) { showError('Output folder is required.'); return null; }
  return { ...base, storage: 'local', output_path: outputDir };
}

async function startChunker(payload) {
  $('chunkChannelBtn').disabled  = true;
  $('chunkSelectedBtn').disabled = true;
  setDot(true);

  show('chunkerOutputCard');
  $('chunkerLog').textContent      = '';
  $('chunkerBadge').textContent    = 'RUNNING';
  $('chunkerBadge').className      = 'running-badge';
  hide('chunkerDone');
  hide('chunkerStopped');
  hide('chunkerIpBan');
  $('progressBar').style.width     = '0%';
  $('progressPct').textContent     = '0%';
  $('progressLabel').textContent   = 'Initializing…';
  $('progressVideos').textContent  = `0 / ${payload.total_videos}`;
  $('currentVideoWrap').textContent = '';
  hide('currentVideoWrap');
  show('stopChunkerBtn');
  hide('resetChunkerBtn');
  $('chunkerOutputCard').scrollIntoView({ behavior: 'smooth', block: 'nearest' });

  try {
    const endpoint = payload.storage === 'minio' ? '/run_chunker_minio' : '/run_chunker';
    const res = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const d = await res.json();
    if (d.error) { showError(d.error); resetChunkerBtns(); setDot(false); return; }

    S.activeChunkerTaskId = d.task_id;

    sseSubscribe(d.task_id, {
      onLog: line => appendLog('chunkerLog', line),
      onProgress: data => {
        $('progressBar').style.width    = data.pct + '%';
        $('progressPct').textContent    = data.pct + '%';
        $('progressLabel').textContent  = `Processing ${data.processed} of ${data.total} videos`;
        $('progressVideos').textContent = `${data.processed} / ${data.total}`;
      },
      onCurrentVideo: data => {
        if (data.title) {
          $('currentVideoWrap').textContent = `▶ ${data.title}`;
          show('currentVideoWrap');
        }
      },
      onDone: data => {
        $('progressBar').style.width    = '100%';
        $('progressPct').textContent    = '100%';
        $('progressLabel').textContent  = 'Complete!';
        $('currentVideoWrap').textContent = '';
        hide('currentVideoWrap');
        $('chunkerBadge').textContent   = 'DONE';
        $('chunkerBadge').classList.add('done');
        $('doneOutputPath').textContent = data.output_path || '—';
        $('doneSummary').textContent    = `${data.successful || 0} of ${data.total} videos chunked successfully`;
        if ((data.deleted_count || 0) > 0) {
          $('doneDeleted').textContent       = data.deleted_count;
          $('doneDeletedWrap').style.display = '';
        } else {
          $('doneDeletedWrap').style.display = 'none';
        }
        show('chunkerDone');
        hide('stopChunkerBtn');
        show('resetChunkerBtn');
        setDot(false); resetChunkerBtns();
        S.activeChunkerTaskId = null;
        loadChunkerPage();
        if (payload.mode === 'selected') loadVideoTable();
      },
      onStopped: data => {
        $('currentVideoWrap').textContent = '';
        hide('currentVideoWrap');
        $('chunkerBadge').textContent = 'STOPPED';
        $('chunkerBadge').className   = 'running-badge stopped';
        $('stoppedSummary').textContent =
          data.successful != null
            ? `${data.successful} video${data.successful !== 1 ? 's' : ''} chunked before stopping.`
            : 'All progress up to this point has been saved.';
        show('chunkerStopped');
        hide('chunkerDone');
        hide('stopChunkerBtn');
        show('resetChunkerBtn');
        setDot(false); resetChunkerBtns();
        S.activeChunkerTaskId = null;
        loadChunkerPage();
        if (payload.mode === 'selected') loadVideoTable();
      },
      onError: (msg, data) => {
        appendLog('chunkerLog', '[ERROR] ' + msg);
        showError(msg);
        const isIpBan = data && data.ip_ban;
        $('chunkerBadge').textContent = isIpBan ? 'IP BANNED' : 'ERROR';
        $('chunkerBadge').className   = 'running-badge error';
        $('currentVideoWrap').textContent = '';
        hide('currentVideoWrap');
        if (isIpBan) {
          show('chunkerIpBan');
          hide('chunkerDone');
          hide('chunkerStopped');
        }
        hide('stopChunkerBtn');
        show('resetChunkerBtn');
        setDot(false); resetChunkerBtns();
        S.activeChunkerTaskId = null;
      },
    });
  } catch (e) {
    showError(e.message); resetChunkerBtns(); setDot(false);
    hide('stopChunkerBtn'); show('resetChunkerBtn');
  }
}

async function stopChunker() {
  if (!S.activeChunkerTaskId) return;
  const btn = $('stopChunkerBtn');
  btn.disabled  = true;
  btn.innerHTML = spinnerBtn('Stopping…');
  try {
    await fetch(`/api/tasks/${S.activeChunkerTaskId}/stop`, { method: 'POST' });
  } catch (e) {
    showError('Could not stop chunker: ' + e.message);
    btn.disabled  = false;
    btn.innerHTML = stopBtnContent();
  }
}

function stopBtnContent() {
  return `<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><rect x="4" y="4" width="16" height="16" rx="2"/></svg> Stop`;
}

function spinnerBtn(label) {
  return `<svg class="spin" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83"/></svg> ${label}`;
}

function resetChunkerBtns() {
  $('chunkChannelBtn').disabled  = !$('channelDropdown').value;
  $('chunkSelectedBtn').disabled = getCheckedIds().length === 0;
}

function resetChunker() {
  hide('chunkerOutputCard');
  $('chunkerLog').textContent    = '';
  $('progressBar').style.width   = '0%';
  $('progressPct').textContent   = '0%';
  $('chunkerBadge').textContent  = 'RUNNING';
  $('chunkerBadge').className    = 'running-badge';
  $('currentVideoWrap').textContent = '';
  hide('currentVideoWrap');
  hide('chunkerStopped');
  hide('chunkerDone');
  hide('chunkerIpBan');
  hide('stopChunkerBtn');
  show('resetChunkerBtn');
  resetChunkerBtns(); setDot(false);
}


// ── Config Tab ─────────────────────────────────────────────────────────────
async function loadConfigForm() {
  try {
    const res  = await fetch('/api/config');
    const data = await res.json();
    // v20: uses PG_* keys (fixed from v19 which was incorrectly reading MYSQL_*)
    $('cfg_host').value              = data.PG_HOST           ?? 'localhost';
    $('cfg_port').value              = data.PG_PORT           ?? 5432;
    $('cfg_user').value              = data.PG_USER           ?? 'postgres';
    $('cfg_password').value          = data.PG_PASSWORD       ?? '';
    $('cfg_db').value                = data.PG_DB             ?? 'youtube_db';
    $('cfg_chunk_size').value        = data.CHUNK_SIZE        ?? 30;
    $('cfg_langs').value             = data.LANGS             ?? 'mk';
    $('cfg_max_workers').value       = data.MAX_WORKERS       ?? 2;
    $('cfg_max_video_workers').value = data.MAX_VIDEO_WORKERS ?? 3;
  } catch (e) {
    setCfgStatus('Failed to load config', 'error');
  }
}

function setCfgStatus(msg, type) {
  const el    = $('cfgStatus');
  el.textContent = msg;
  el.className   = 'cfg-status cfg-status--' + (type || 'ok');
  if (type !== 'error') setTimeout(() => { el.textContent = ''; el.className = 'cfg-status'; }, 3500);
}

async function saveConfig() {
  // v20: sends PG_* keys to backend
  const payload = {
    PG_HOST:           $('cfg_host').value.trim(),
    PG_PORT:           parseInt($('cfg_port').value)              || 5432,
    PG_USER:           $('cfg_user').value.trim(),
    PG_PASSWORD:       $('cfg_password').value,
    PG_DB:             $('cfg_db').value.trim(),
    CHUNK_SIZE:        parseInt($('cfg_chunk_size').value)        || 30,
    LANGS:             $('cfg_langs').value,
    MAX_WORKERS:       parseInt($('cfg_max_workers').value)       || 2,
    MAX_VIDEO_WORKERS: parseInt($('cfg_max_video_workers').value) || 3,
  };
  try {
    const res  = await fetch('/api/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (data.ok) {
      setCfgStatus('✓ Config saved — DB connection updated', 'ok');
      await loadGlobalStats();
      await loadChannelDropdowns();
    } else {
      setCfgStatus('Error: ' + (data.error || 'unknown'), 'error');
    }
  } catch (e) {
    setCfgStatus('Save failed: ' + e.message, 'error');
  }
}

async function testDbConnection() {
  // v20: sends PG_* keys
  const payload = {
    PG_HOST:     $('cfg_host').value.trim(),
    PG_PORT:     parseInt($('cfg_port').value) || 5432,
    PG_USER:     $('cfg_user').value.trim(),
    PG_PASSWORD: $('cfg_password').value,
    PG_DB:       $('cfg_db').value.trim(),
  };
  setCfgStatus('Testing connection…', 'ok');
  try {
    const res  = await fetch('/api/config/test_db', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    setCfgStatus((data.ok ? '✓ ' : '✗ ') + data.message, data.ok ? 'ok' : 'error');
  } catch (e) {
    setCfgStatus('Test failed: ' + e.message, 'error');
  }
}

async function resetConfig() {
  if (!confirm('Reset all config values to defaults?')) return;
  try {
    await fetch('/api/config/reset', { method: 'POST' });
    await loadConfigForm();
    setCfgStatus('✓ Reset to defaults', 'ok');
  } catch (e) {
    setCfgStatus('Reset failed: ' + e.message, 'error');
  }
}


// ── Init ───────────────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  const savedKey = lsGet('yt_api_key');
  if (savedKey) $('api_key').value = savedKey;
  switchPage('config');
});
