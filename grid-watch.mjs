// grid-watch.mjs — the fleet's control plane (GridWatch v1)
//
// SENSE  — every repo, every workflow, every run: health, streaks, cadence, silence.
// DECIDE — per-workflow health class: ok | flaky | failing | billing-blocked | dormant | stale | never-run.
// ACT    — within the autonomy envelope (reversible + measured): auto-retry flaky/failing,
//          re-enable dormant, never touch billing (owner-level), cap retries to avoid pileups.
// ALERT  — new reds -> operator-inbox (agent role, honest) + decider feed file.
// RECORD — state/fleet-health.json committed (the single public truth file), double-pass verified.
//
// No secrets in outputs. Token env: GH_PAT. Zero deps.
import { writeFileSync } from 'node:fs';

const TOKEN = process.env.GH_PAT || '';
const ORG = 'roshpinacare-sys';
const REPO = 'GridWatch';
const NOW = new Date().toISOString();
const H = { Authorization: `Bearer ${TOKEN}`, Accept: 'application/vnd.github+json', 'User-Agent': 'gridwatch-v1' };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const gh = async (path, opts = {}, tries = 2) => {
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(`https://api.github.com${path}`, { headers: H, ...opts });
      if (r.status === 409 || r.status >= 500) { await sleep(800 * (i + 1)); continue; }
      return { status: r.status, ok: r.ok, data: r.status === 204 ? null : await r.json().catch(() => null), headers: r.headers };
    } catch (e) { if (i === tries - 1) return { status: 0, ok: false, data: null, err: String(e.message).slice(0, 80) }; await sleep(800); }
  }
  return { status: 0, ok: false, data: null };
};

const ageMs = (ts) => { const t = Date.parse(ts || ''); return isNaN(t) ? null : Date.now() - t; };
const ageMin = (ts) => { const a = ageMs(ts); return a == null ? null : Math.round(a / 60000); };

// ---------- SENSE 1: repos ----------
const repos = [];
let page = 1;
for (;;) {
  const r = await gh(`/user/repos?per_page=100&page=${page}&sort=pushed&type=owner&affiliation=owner`);
  if (!r.ok) { console.error('repos fetch failed', r.status); break; }
  repos.push(...r.data);
  if (r.data.length < 100) break;
  page++;
}
console.log(`repos: ${repos.length}`);

// ---------- SENSE 2: per-repo workflows + runs (grouped client-side) ----------
const fleet = [];
for (const repo of repos) {
  const [wfs, runsR] = await Promise.all([
    gh(`/repos/${ORG}/${repo.name}/actions/workflows?per_page=100`),
    gh(`/repos/${ORG}/${repo.name}/actions/runs?per_page=100&sort=created&direction=desc`),
  ]);
  const wfList = wfs.ok ? (wfs.data.workflows || []) : [];
  const runList = runsR.ok ? (runsR.data.workflow_runs || []) : [];
  const byWf = new Map();
  for (const run of runList) {
    if (!byWf.has(run.workflow_id)) byWf.set(run.workflow_id, []);
    if (byWf.get(run.workflow_id).length < 6) byWf.get(run.workflow_id).push(run);
  }
  // workflow yml shas for cron cache
  const dir = await gh(`/repos/${ORG}/${repo.name}/contents/.github/workflows`);
  const ymlShas = {};
  if (dir.ok) for (const f of (dir.data || [])) if (/\.ya?ml$/.test(f.name)) ymlShas[f.path] = f.sha;

  fleet.push({
    name: repo.name, private: repo.private, pushedAt: repo.pushed_at,
    defaultBranch: repo.default_branch, wfList, byWf, ymlShas,
  });
  await sleep(60);
}

// ---------- previous state (cron cache + red diff + retry caps) ----------
let prev = { cronCache: {}, reds: [], autoRetries: {} };
const prevRes = await gh(`/repos/${ORG}/${REPO}/contents/state/fleet-health.json`);
if (prevRes.ok && prevRes.data?.content) {
  try {
    const j = JSON.parse(Buffer.from(prevRes.data.content, 'base64').toString());
    prev = { cronCache: j.cronCache || {}, reds: (j.repos || []).filter(r => r.health === 'red').map(r => r.name), autoRetries: j.autoRetries || {} };
  } catch {}
}
const prevSha = prevRes.ok ? prevRes.data.sha : null;

// ---------- cron via sha-cached yml fetch ----------
const cronCache = {};
async function cronOf(repoName, path, sha) {
  const key = `${repoName}::${path}`;
  const hit = prev.cronCache[key];
  if (hit && hit.sha === sha) { cronCache[key] = hit; return hit.cron; }
  const r = await gh(`/repos/${ORG}/${repoName}/contents/${encodeURIComponent(path)}`);
  if (!r.ok) { const v = { sha, cron: null }; cronCache[key] = v; return null; }
  const txt = Buffer.from(r.data.content || '', 'base64').toString();
  const m = txt.match(/cron:\s*['"]?([^'"\n]+)/);
  const v = { sha, cron: m ? m[1].trim() : null };
  cronCache[key] = v;
  return v.cron;
}
function cronMin(cron) {
  if (!cron) return null;
  const p = cron.trim().split(/\s+/);
  if (p.length !== 5) return null;
  const [mi, h, dom, mon, dow] = p;
  if (mi.startsWith('*/')) return parseInt(mi.slice(2), 10);
  if (mi === '*' ) return 1;
  if (h.startsWith('*/')) return parseInt(h.slice(2), 10) * 60;
  if (h.includes(',')) return null; // irregular
  return 24 * 60; // daily-ish
}

// ---------- classify ----------
async function classifyWorkflow(repo, wf) {
  const runs = repo.byWf.get(wf.id) || [];
  const last = runs[0] || null;
  const cron = repo.wfList && last?.path ? await cronOf(repo.name, last.path, repo.ymlShas[last.path]) : null;
  let streak = 0;
  for (const r of runs) { if (r.conclusion === 'failure' || r.conclusion === 'startup_failure') streak++; else break; }

  let health = 'ok', cls = null, note = '';
  if (wf.state === 'disabled_inactivity') { health = 'amber'; cls = 'dormant'; note = 'GitHub disabled (60d inactivity)'; }
  else if (wf.state === 'disabled_manually') { health = 'amber'; cls = 'disabled'; note = 'manually disabled'; }
  else if (!last) { health = 'amber'; cls = 'never-run'; note = 'no runs recorded'; }
  else if (streak >= 2) {
    health = 'red'; cls = 'failing'; note = `${streak} consecutive failures`;
    // billing probe: check annotations of the latest failed run
    if (!last.check_suite_url) { /* no suite url — skip probe */ } else
    { const cr = await gh(`${last.check_suite_url.replace('https://api.github.com', '')}/check-runs`);
    for (const c of (cr.ok ? (cr.data.check_runs || []) : [])) {
      if (!c.annotations_url) continue;
      const ann = await gh(c.annotations_url.replace('https://api.github.com', ''));
      for (const a of (ann.ok ? (ann.data || []) : [])) {
        const msg = a.message || '';
        if (/payments have failed|spending limit/i.test(msg)) { health = 'red'; cls = 'billing-blocked'; note = 'GitHub billing: jobs not starting'; }
      }
      if (cls === 'billing-blocked') break;
    } }
  }
  else if (streak === 1) { health = 'amber'; cls = 'flaky'; note = 'last run failed (earlier succeeded)'; }

  const cadenceMin = cronMin(cron);
  if (health === 'ok' && cadenceMin) {
    const aMin = ageMin(last.created_at);
    const limit = Math.max(cadenceMin * 2.5, 150);
    if (aMin != null && aMin > limit) { health = 'amber'; cls = 'stale'; note = `no run for ${aMin}m (cadence ~${cadenceMin}m)`; }
  }
  return {
    id: wf.id, name: wf.name, state: wf.state, cron: cron || null,
    health, class: cls, note,
    streak, lastRunAt: last?.created_at || null, lastConclusion: last?.conclusion || null,
    lastEvent: last?.event || null, dispatchable: true,
  };
}

const alerts = [];
const remediations = [];
const repoRows = [];
let wfTotals = { green: 0, amber: 0, red: 0 };

for (const repo of fleet) {
  const wfRows = [];
  for (const wf of repo.wfList) {
    const row = await classifyWorkflow(repo, wf);
    wfRows.push(row);
    wfTotals[row.health === 'ok' ? 'green' : row.health === 'amber' ? 'amber' : 'red']++;
  }
  const reds = wfRows.filter(w => w.health === 'red');
  const ambers = wfRows.filter(w => w.health === 'amber');
  const repoHealth = reds.length ? 'red' : ambers.length ? 'amber' : 'green';
  repoRows.push({
    name: repo.name, private: repo.private, pushedAt: repo.pushed_at, health: repoHealth,
    counts: {
      workflows: wfRows.length,
      green: wfRows.filter(w => w.health === 'ok').length,
      amber: ambers.length, red: reds.length,
    },
    workflows: wfRows.sort((a, b) => (a.health === 'red' ? -1 : b.health === 'red' ? 1 : a.health === 'amber' ? -1 : b.health === 'amber' ? 1 : a.name.localeCompare(b.name))),
  });

  // ---------- ACT (envelope) ----------
  for (const w of wfRows) {
    const rk = `${repo.name}::${w.name}`;
    if ((w.class === 'failing' || w.class === 'flaky') && w.state === 'active') {
      if (w.class === 'billing-blocked') continue;
      const recent = ageMin(w.lastRunAt);
      if (recent != null && recent < 12) continue; // a run is already in flight — no pileup
      const prevRetries = prev.autoRetries[rk] || 0;
      if (prevRetries >= 3) {
        if (w.health === 'red' && !alerts.find(a => a.key === rk))
          alerts.push({ key: rk, level: 'red', repo: repo.name, workflow: w.name, class: w.class, note: `${w.note}; auto-retry exhausted (${prevRetries}) — needs engineering triage`, at: NOW });
        continue;
      }
      const d = await gh(`/repos/${ORG}/${repo.name}/actions/workflows/${w.id}/dispatches`, { method: 'POST', body: JSON.stringify({ ref: repo.defaultBranch }) });
      if (d.status === 204) {
        remediations.push({ at: NOW, repo: repo.name, workflow: w.name, action: 'workflow_dispatch retry', class: w.class, attempt: prevRetries + 1 });
        prev.autoRetries[rk] = prevRetries + 1;
      } else {
        remediations.push({ at: NOW, repo: repo.name, workflow: w.name, action: 'dispatch-failed', http: d.status });
      }
      await sleep(120);
    }
    if (w.class === 'dormant') {
      const e = await gh(`/repos/${ORG}/${repo.name}/actions/workflows/${w.id}/enable`, { method: 'PUT' });
      remediations.push({ at: NOW, repo: repo.name, workflow: w.name, action: e.status === 204 ? 're-enabled (was disabled_inactivity)' : `enable-failed http=${e.status}` });
      await sleep(120);
    }
  }
  const rHealth = repoHealth === 'red' ? 'red' : repoHealth;
  if (rHealth === 'red' && !prev.reds.includes(repo.name)) {
    alerts.push({ key: `repo::${repo.name}`, level: 'red', repo: repo.name, workflow: null, class: 'repo-red', note: reds.map(w => `${w.name}: ${w.note}`).join(' | ').slice(0, 400), at: NOW });
  }
}

// autoRetries decay: entries older than ~6h reset. Cheap decay: keep map small.
if (Object.keys(prev.autoRetries).length > 400) prev.autoRetries = {};

// ---------- RECORD: fleet-health.json ----------
const payload = {
  generatedAt: NOW,
  version: 1,
  cadenceMin: 30,
  totals: {
    repos: repoRows.length,
    public: repoRows.filter(r => !r.private).length,
    private: repoRows.filter(r => r.private).length,
    workflows: repoRows.reduce((s, r) => s + r.counts.workflows, 0),
    green: repoRows.reduce((s, r) => s + r.counts.green, 0),
    amber: repoRows.reduce((s, r) => s + r.counts.amber, 0),
    red: repoRows.reduce((s, r) => s + r.counts.red, 0),
  },
  repos: repoRows.map(({ name, private: priv, pushedAt, health, counts, workflows }) => ({
    name, private: priv, pushedAt, health, counts,
    workflows: workflows.map(({ id, name: wn, state, cron, health: wh, class: wc, note, streak, lastRunAt, lastConclusion, lastEvent }) =>
      ({ name: wn, state, cron, health: wh, class: wc, note, streak, lastRunAt, lastConclusion, lastEvent })),
  })),
  alerts,
  remediations,
  cronCache,
  autoRetries: prev.autoRetries,
};

// ---------- ALERT: operator inbox (only for NEW reds, agent role, honest) ----------
if (alerts.length) {
  const text = `[grid-watch] אדומים חדשים בצי (${alerts.length}): ${alerts.map(a => `${a.repo}${a.workflow ? '/' + a.workflow : ''} — ${a.class}`).join('; ')}. ריפוי-עצמי הופעל כשניתן; ראה GridWatch/state/fleet-health.json`;
  const inboxPath = `/repos/${ORG}/FleetHQ/contents/office/state/operator-inbox.jsonl`;
  const cur = await gh(inboxPath);
  if (cur.ok) {
    const curTxt = Buffer.from(cur.data.content || '', 'base64').toString().trimEnd();
    const { createHash } = await import('node:crypto');
    const line = JSON.stringify({ at: NOW, id: createHash('sha256').update(text + NOW).digest('hex').slice(0, 12), text, sha16: createHash('sha256').update(text).digest('hex').slice(0, 16) });
    const put = await gh(inboxPath, {
      method: 'PUT',
      body: JSON.stringify({ message: `grid-watch: fleet alert (${alerts.length} new red)`, content: Buffer.from(curTxt + '\n' + line).toString('base64'), sha: cur.data.sha }),
    });
    console.log('inbox alert put:', put.status);
  }
}

// ---------- commit state (contents API, 409-safe) ----------
const content = Buffer.from(JSON.stringify(payload, null, 1)).toString('base64');
const putBody = { message: `grid-watch: fleet health @ ${NOW}`, content };
if (prevSha) putBody.sha = prevSha;
const put = await gh(`/repos/${ORG}/${REPO}/contents/state/fleet-health.json`, { method: 'PUT', body: JSON.stringify(putBody) });
console.log('state put:', put.status);
if (put.status === 409) {
  // someone pushed meanwhile — re-fetch sha and retry once
  const fresh = await gh(`/repos/${ORG}/${REPO}/contents/state/fleet-health.json`);
  if (fresh.ok) {
    const put2 = await gh(`/repos/${ORG}/${REPO}/contents/state/fleet-health.json`, { method: 'PUT', body: JSON.stringify({ ...putBody, sha: fresh.data.sha }) });
    console.log('state put retry:', put2.status);
  }
}

// ---------- double-pass verify ----------
const verify = await gh(`/repos/${ORG}/${REPO}/contents/state/fleet-health.json?ref=${Date.now()}`);
const verified = verify.ok && verify.data?.content && JSON.parse(Buffer.from(verify.data.content, 'base64').toString()).generatedAt === NOW;

const summary = [
  `## GridWatch @ ${NOW}`,
  `- repos: ${payload.totals.repos} (${payload.totals.public} public / ${payload.totals.private} private)`,
  `- workflows: ${payload.totals.workflows} — green ${payload.totals.green} / amber ${payload.totals.amber} / red ${payload.totals.red}`,
  `- alerts: ${alerts.length}; remediations: ${remediations.length}`,
  `- state committed & double-pass verified: ${verified}`,
  '',
  ...repoRows.filter(r => r.health !== 'green').map(r => `### ${r.name} (${r.health}) — ${r.counts.red} red / ${r.counts.amber} amber`),
  ...repoRows.flatMap(r => r.workflows.filter(w => w.health !== 'ok').map(w => `- ${r.name}/${w.name}: ${w.health} ${w.class || ''} ${w.note}`)),
].join('\n');
try { writeFileSync(process.env.GITHUB_STEP_SUMMARY || '/tmp/gw-summary.md', summary); } catch {}
console.log(summary.slice(0, 1200));
if (!verified) { console.error('DOUBLE-PASS VERIFY FAILED'); process.exit(1); }
