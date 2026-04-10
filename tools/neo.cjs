#!/usr/bin/env node
// neo — CLI for Neo, the web app API discovery & execution tool
//
// Usage:
//   neo status                              Overview of captured data
//   neo capture start [--tab pattern]       Start CDP Network capture daemon
//   neo capture stop                        Stop CDP Network capture daemon
//   neo capture list [domain] [--limit N]   List captured API calls
//   neo capture count                       Total capture count
//   neo capture domains                     List domains with counts
//   neo capture detail <id>                 Show full capture details
//   neo capture stats <domain>              Domain statistics
//   neo capture clear [domain]              Clear captures
//   neo capture export [domain] [--format har] [--include-auth] Export captures as JSON or HAR 1.2
//   neo capture import <file>               Import captures from JSON file
//   neo schema generate <domain> [--all]     Generate API schema from captures
//   neo schema show <domain>                Show cached schema
//   neo exec <url> [options]                Execute fetch in browser tab context
//   neo eval <js> --tab <pattern>           Evaluate JS in page context
//   neo open <url>                          Open URL in Chrome
//   neo replay <id> [--tab pattern] [--auto-headers] Replay a captured API call
//   neo read <tab-pattern>                  Extract readable text from page
//   neo setup                               Setup Neo local config + extension assets
//   neo start                               Launch Chrome with configured profile
//   neo profile list                        List system Chrome profiles
//   neo profile use <name>                  Set default system Chrome profile
//   neo launch <app> [--port N]             Launch Electron app with CDP enabled
//   neo attach                               Attach to user's running Chrome via DevToolsActivePort
//   neo connect [port]                      Connect to Chrome/Electron CDP and save session
//   neo connect --electron <app-name>       Auto-discover Electron CDP port and connect
//   neo discover                            Discover reachable CDP targets on localhost ports
//   neo sessions                            List saved active sessions
//   neo tab                                 List CDP targets in the active session
//   neo tab <index> | neo tab --url <pat>  Switch active tab target
//   neo inject [--persist] [--tab pattern]  Inject Neo capture script into page target
//   neo cookies list [domain]               List cookies for the active CDP session
//   neo cookies export [domain] [file]      Export cookies as JSON
//   neo cookies import <file>               Import cookies from JSON file
//   neo cookies clear [domain]              Clear cookies
//   neo snapshot [-i] [-C] [--json] [--diff] Snapshot a11y tree with compact ref mapping
//   neo click <ref> [--new-tab]             Click element by ref
//   neo fill <ref> "text"                    Clear then fill element by ref
//   neo type <ref> "text"                    Type text without clearing
//   neo press <key>                          Press keyboard key (supports Ctrl+a)
//   neo hover <ref>                          Hover over element by ref
//   neo scroll <dir> [px] [--selector css]  Scroll by direction and distance
//   neo select <ref> "value"                 Select option value by ref
//   neo screenshot [path] [--full] [--annotate] Capture screenshot to file
//   neo get text <ref> | neo get url | neo get title Extract page/element info
//   neo wait <ref> | neo wait --load networkidle | neo wait <ms> Wait for UI/load/time
//   neo bridge [port] [--json] [--quiet]    Start WebSocket bridge for real-time capture streaming
//   neo label <domain> [--dry-run]          Semantic endpoint labeling (heuristics + optional LLM JSON)
//   neo workflow discover <domain>           Discover multi-step workflows from dependencies
//   neo workflow show <name>                 Show a saved workflow
//   neo workflow run <name> [--params k=v]  Execute workflow step-by-step

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { spawn, execSync } = require('child_process');
let WebSocket = null;

const CDP_URL = process.env.NEO_CDP_URL || 'http://localhost:9222';
const DB_NAME = 'neo-capture-v01';
const STORE_NAME = 'capturedRequests';
const NEO_EXTENSION_ID = process.env.NEO_EXTENSION_ID || null;
const SCHEMA_DIR = process.env.NEO_SCHEMA_DIR || path.join(process.env.HOME, '.neo/schemas');
const WORKFLOW_FILE_EXT = '.workflows.json';
const SESSION_FILE = '/tmp/neo-sessions.json';
const EXTENSION_ID_CACHE_FILE = '/tmp/neo-ext-id';
const NEO_BASE_DIR = path.join(process.env.HOME, '.neo');
const NEO_ROOT_CONFIG_FILE = path.join(NEO_BASE_DIR, 'config.json');
const LOCAL_CAPTURE_FILE = path.join(NEO_BASE_DIR, 'captures.json');
const CDP_CAPTURE_STATE_FILE = '/tmp/neo-cdp-capture.json';
const NEO_PROFILE = process.env.NEO_PROFILE || null;
function getNeoHomeDir(profile) {
  const p = profile || NEO_PROFILE;
  return p ? path.join(NEO_BASE_DIR, 'profiles', p) : NEO_BASE_DIR;
}
const NEO_HOME_DIR = getNeoHomeDir();
const NEO_CONFIG_FILE = path.join(NEO_HOME_DIR, 'config.json');
const NEO_EXTENSION_DIR = path.join(NEO_HOME_DIR, 'extension');
const DEFAULT_SESSION_NAME = '__default__';
const EXTENSION_NOT_FOUND_MESSAGE = 'Neo extension service worker not found. Is it installed and active?\n  - Check chrome://extensions for the Neo extension\n  - Make sure Chrome was launched with --remote-debugging-port=9222';
const STATIC_RESOURCE_EXTENSIONS = /\.(?:js|css|png|jpe?g|gif|webp|ico|svg|woff2?|eot|ttf|otf|map|mp4|mpe?g|avi|mov|webm|pdf|zip|gz|tar|glb|gltf|obj|fbx|stl|dae|3ds|wasm|bin)(?:[?#].*)?$/i;
const CDP_CAPTURE_SKIP_PROTOCOLS = new Set(['chrome-extension:', 'devtools:', 'data:', 'blob:', 'about:']);
const CHROME_PROFILE_ROOTS = Object.freeze([
  { browserName: 'Google Chrome', userDataDir: path.join(process.env.HOME, '.config/google-chrome') },
  { browserName: 'Chromium', userDataDir: path.join(process.env.HOME, '.config/chromium') },
]);
const ELECTRON_APPS = Object.freeze({
  slack: Object.freeze(['/usr/bin/slack', 'slack']),
  code: Object.freeze(['/usr/bin/code', 'code']),
  vscode: Object.freeze(['/usr/bin/code', 'code']),
  discord: Object.freeze(['/usr/bin/discord', 'discord']),
  notion: Object.freeze(['/usr/bin/notion', 'notion']),
  figma: Object.freeze([]),
  feishu: Object.freeze(['/opt/bytedance/feishu/feishu', 'feishu']),
});
const INTERACTIVE_ROLES = new Set([
  'button', 'textbox', 'link', 'combobox', 'checkbox', 'menuitem', 'tab',
  'radio', 'slider', 'switch', 'searchbox', 'spinbutton', 'option',
  'treeitem', 'menuitemcheckbox', 'menuitemradio', 'listbox'
]);

function loadSessions() {
  if (!fs.existsSync(SESSION_FILE)) return {};
  try {
    const raw = fs.readFileSync(SESSION_FILE, 'utf8').trim();
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};
    return parsed;
  } catch {
    return {};
  }
}

function saveSessions(sessions) {
  const safeSessions = (!sessions || typeof sessions !== 'object' || Array.isArray(sessions))
    ? {}
    : sessions;
  fs.writeFileSync(SESSION_FILE, `${JSON.stringify(safeSessions, null, 2)}\n`, 'utf8');
}

function getSession(name = DEFAULT_SESSION_NAME) {
  const sessions = loadSessions();
  return sessions[name] || null;
}

function setSession(name = DEFAULT_SESSION_NAME, data = {}) {
  const sessions = loadSessions();
  sessions[name] = (!data || typeof data !== 'object' || Array.isArray(data)) ? {} : data;
  saveSessions(sessions);
  return sessions[name];
}

function ensureParentDirectory(filePath) {
  const target = String(filePath || '').trim();
  if (!target) return;
  fs.mkdirSync(path.dirname(target), { recursive: true });
}

function readJsonFileSafe(filePath, fallback = {}) {
  const target = String(filePath || '').trim();
  if (!target || !fs.existsSync(target)) return structuredClone(fallback);
  try {
    const raw = String(fs.readFileSync(target, 'utf8') || '').trim();
    if (!raw) return structuredClone(fallback);
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return structuredClone(fallback);
    }
    return parsed;
  } catch {
    return structuredClone(fallback);
  }
}

function writeJsonFile(filePath, value) {
  ensureParentDirectory(filePath);
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function getConfigFile(profileName = null) {
  if (!profileName) return NEO_ROOT_CONFIG_FILE;
  return path.join(getNeoHomeDir(profileName), 'config.json');
}

function loadNeoConfig(profileName = null) {
  const rootConfig = readJsonFileSafe(NEO_ROOT_CONFIG_FILE, {});
  if (!profileName) return rootConfig;
  const profileConfig = readJsonFileSafe(getConfigFile(profileName), {});
  return { ...rootConfig, ...profileConfig };
}

function saveNeoConfig(config, profileName = null) {
  const targetFile = getConfigFile(profileName);
  writeJsonFile(targetFile, (!config || typeof config !== 'object' || Array.isArray(config)) ? {} : config);
}

function getConfiguredCdpPort(profileName = null) {
  const config = loadNeoConfig(profileName);
  const parsed = parseInt(String(config && config.cdpPort !== undefined ? config.cdpPort : 9222), 10);
  return Number.isInteger(parsed) && parsed > 0 && parsed <= 65535 ? parsed : 9222;
}

function getConfiguredCdpUrl(profileName = null) {
  return `http://localhost:${getConfiguredCdpPort(profileName)}`;
}

function hasActiveSession(sessionName = DEFAULT_SESSION_NAME, deps = {}) {
  const getSessionFn = typeof deps.getSession === 'function' ? deps.getSession : getSession;
  const normalizedSessionName = sessionName || DEFAULT_SESSION_NAME;
  const session = getSessionFn(normalizedSessionName);
  return !!(session && typeof session.pageWsUrl === 'string' && session.pageWsUrl.trim());
}

// For Lightpanda-style single-WS backends, each CLI invocation needs to
// re-establish the page context (Target.createTarget + attachToTarget) since
// the WS connection (and thus the browsing context) doesn't survive across
// separate process runs.
async function ensureLightpandaPageContext(sessionName = DEFAULT_SESSION_NAME) {
  const session = getSession(sessionName);
  if (!session || !session.pageWsUrl) return session;
  // Only needed for bare WS URLs (Lightpanda), not per-tab Chrome WS URLs
  if (!/^wss?:\/\/[^/]+\/?$/.test(session.pageWsUrl)) return session;

  const pageUrl = session.currentUrl || 'about:blank';
  const wsUrl = session.pageWsUrl;

  // Create target with the saved URL and attach in one persistent connection
  const created = await cdpSendPersistent(wsUrl, 'Target.createTarget', { url: pageUrl });
  const targetId = created && created.targetId;
  if (targetId) {
    try {
      await cdpSendPersistent(wsUrl, 'Target.attachToTarget', { targetId, flatten: true });
    } catch {}
  }

  // Wait for the page to finish loading
  if (pageUrl !== 'about:blank') {
    try {
      await cdpSendPersistent(wsUrl, 'Page.enable', {});
      // Poll until DOM is ready (up to 5s)
      for (let i = 0; i < 10; i++) {
        await new Promise(r => setTimeout(r, 500));
        const evalResult = await cdpSendPersistent(wsUrl, 'Runtime.evaluate', { expression: 'document.readyState' });
        if (evalResult && evalResult.result && (evalResult.result.value === 'complete' || evalResult.result.value === 'interactive')) break;
      }
    } catch {}
  }

  return session;
}

function shellEscape(value) {
  return `'${String(value || '').replace(/'/g, `'\\''`)}'`;
}

function commandExists(commandName) {
  const name = String(commandName || '').trim();
  if (!name || name.includes('/')) return false;
  try {
    execSync(`command -v ${shellEscape(name)}`, { stdio: 'ignore', shell: '/bin/sh' });
    return true;
  } catch {
    return false;
  }
}

function resolveCommandPath(commandName, deps = {}) {
  const execSyncFn = typeof deps.execSync === 'function' ? deps.execSync : execSync;
  const name = String(commandName || '').trim();
  if (!name || name.includes('/')) return null;
  try {
    const output = String(execSyncFn(`command -v ${shellEscape(name)}`, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      shell: '/bin/sh',
    }) || '');
    const resolved = output.trim().split('\n')[0];
    return resolved || null;
  } catch {
    return null;
  }
}

function detectLightpandaBinaryPath(deps = {}) {
  const resolveCommandPathFn = typeof deps.resolveCommandPath === 'function' ? deps.resolveCommandPath : resolveCommandPath;
  return resolveCommandPathFn('lightpanda');
}

function detectChromeBinaryPath(deps = {}) {
  const resolveCommandPathFn = typeof deps.resolveCommandPath === 'function' ? deps.resolveCommandPath : resolveCommandPath;
  const existsSyncFn = typeof deps.existsSync === 'function' ? deps.existsSync : fs.existsSync;
  const platform = deps.platform || process.platform;
  const candidates = ['google-chrome-stable', 'google-chrome', 'chromium-browser', 'chromium'];

  for (const candidate of candidates) {
    const resolved = resolveCommandPathFn(candidate);
    if (resolved) return resolved;
  }

  if (platform === 'darwin') {
    const macChromePath = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
    if (existsSyncFn(macChromePath)) return macChromePath;
  }

  return null;
}

function getWebSocketCtor() {
  if (WebSocket) return WebSocket;
  try {
    WebSocket = require('ws');
    return WebSocket;
  } catch {
    throw new Error('Missing dependency: ws. Install project dependencies before using CDP or bridge features.');
  }
}

function extractChromeProfileEmail(preferences) {
  const prefs = preferences && typeof preferences === 'object' ? preferences : {};
  const accountInfo = Array.isArray(prefs.account_info) ? prefs.account_info : [];
  const first = accountInfo.find(item => item && typeof item.email === 'string' && item.email.trim());
  if (first) return first.email.trim();
  const googleServices = prefs.google && prefs.google.services && typeof prefs.google.services === 'object'
    ? prefs.google.services
    : {};
  if (typeof googleServices.last_username === 'string' && googleServices.last_username.trim()) {
    return googleServices.last_username.trim();
  }
  return '';
}

function scanChromeProfiles(deps = {}) {
  const readdirSyncFn = typeof deps.readdirSync === 'function' ? deps.readdirSync : fs.readdirSync;
  const existsSyncFn = typeof deps.existsSync === 'function' ? deps.existsSync : fs.existsSync;
  const readFileSyncFn = typeof deps.readFileSync === 'function' ? deps.readFileSync : fs.readFileSync;
  const roots = Array.isArray(deps.roots) ? deps.roots : CHROME_PROFILE_ROOTS;
  const profiles = [];

  for (const root of roots) {
    if (!root || !root.userDataDir) continue;
    if (!existsSyncFn(root.userDataDir)) continue;
    let entries = [];
    try {
      entries = readdirSyncFn(root.userDataDir, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const entry of entries) {
      const name = typeof entry === 'string' ? entry : entry && entry.name;
      const isDirectory = typeof entry === 'string' ? true : !!(entry && typeof entry.isDirectory === 'function' && entry.isDirectory());
      if (!name || !isDirectory) continue;
      const preferencesFile = path.join(root.userDataDir, name, 'Preferences');
      if (!existsSyncFn(preferencesFile)) continue;
      let prefs = {};
      try {
        prefs = JSON.parse(readFileSyncFn(preferencesFile, 'utf8'));
      } catch {}
      const email = extractChromeProfileEmail(prefs);
      profiles.push({
        browserName: root.browserName || 'Chrome',
        name: String(name),
        email,
        userDataDir: root.userDataDir,
        profileDir: path.join(root.userDataDir, name),
        preferencesFile,
      });
    }
  }

  return profiles.sort((a, b) => {
    if (a.browserName !== b.browserName) return a.browserName.localeCompare(b.browserName);
    if (a.name === 'Default' && b.name !== 'Default') return -1;
    if (b.name === 'Default' && a.name !== 'Default') return 1;
    return a.name.localeCompare(b.name);
  });
}

function resolveChromeProfileSelection(name, profiles = []) {
  const target = String(name || '').trim();
  if (!target) return { error: 'missing-name', matches: [] };
  const source = Array.isArray(profiles) ? profiles : [];
  const normalized = target.toLowerCase();
  const matches = source.filter((profile) => {
    const profileName = String(profile && profile.name || '');
    const browserName = String(profile && profile.browserName || '');
    const display = `${profileName} (${browserName})`;
    const alias = `${browserName}:${profileName}`;
    return profileName.toLowerCase() === normalized
      || display.toLowerCase() === normalized
      || alias.toLowerCase() === normalized;
  });
  if (!matches.length) return { error: 'not-found', matches: [] };
  if (matches.length > 1) return { error: 'ambiguous', matches };
  return { error: null, profile: matches[0], matches };
}

function parseChromeCommandFlags(commandLine = '') {
  const text = String(commandLine || '');
  const extract = (flag) => {
    const quoted = text.match(new RegExp(`--${flag}=(\"[^\"]+\"|'[^']+'|\\S+)`));
    if (!quoted) return '';
    return quoted[1].replace(/^['"]|['"]$/g, '');
  };
  return {
    userDataDir: extract('user-data-dir'),
    profileDirectory: extract('profile-directory'),
    remoteDebuggingPort: extract('remote-debugging-port'),
  };
}

function listChromeProcesses(deps = {}) {
  const execSyncFn = typeof deps.execSync === 'function' ? deps.execSync : execSync;
  const command = deps.command || 'ps -eo pid=,args=';
  let output = '';
  try {
    output = String(execSyncFn(command, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      shell: '/bin/sh',
      timeout: 3000,
    }) || '');
  } catch {
    return [];
  }

  const rows = [];
  for (const line of output.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const match = trimmed.match(/^(\d+)\s+(.+)$/);
    if (!match) continue;
    const commandLine = match[2];
    if (!/\b(chrome|chromium)\b/i.test(commandLine)) continue;
    if (/chrome_crashpad_handler|chromium-browser-sandbox/i.test(commandLine)) continue;
    const flags = parseChromeCommandFlags(commandLine);
    rows.push({
      pid: Number(match[1]),
      commandLine,
      browserName: /\bchromium\b/i.test(commandLine) ? 'Chromium' : 'Google Chrome',
      userDataDir: flags.userDataDir || '',
      profileDirectory: flags.profileDirectory || '',
      remoteDebuggingPort: flags.remoteDebuggingPort ? Number(flags.remoteDebuggingPort) : 0,
    });
  }
  return rows;
}

function describeConfiguredChromeProfile(config = {}) {
  const activeConfig = config && typeof config === 'object' ? config : {};
  const profileName = String(activeConfig.defaultProfile || '').trim();
  const userDataDir = String(activeConfig.userDataDir || '').trim();
  if (!profileName && !userDataDir) return 'Not configured';
  const segments = [];
  if (profileName) segments.push(profileName);
  if (userDataDir) segments.push(userDataDir);
  return segments.join(' @ ');
}

function isLightpandaBrowser(configOrPath) {
  if (!configOrPath) return false;
  const p = typeof configOrPath === 'string' ? configOrPath : (configOrPath.browserType || configOrPath.chromePath || '');
  return p === 'lightpanda' || /lightpanda/i.test(p);
}

function copyDirectoryRecursive(sourceDir, destinationDir, deps = {}) {
  const existsSyncFn = typeof deps.existsSync === 'function' ? deps.existsSync : fs.existsSync;
  const mkdirSyncFn = typeof deps.mkdirSync === 'function' ? deps.mkdirSync : fs.mkdirSync;
  const readdirSyncFn = typeof deps.readdirSync === 'function' ? deps.readdirSync : fs.readdirSync;
  const copyFileSyncFn = typeof deps.copyFileSync === 'function' ? deps.copyFileSync : fs.copyFileSync;
  if (!existsSyncFn(sourceDir)) {
    throw new Error(`Missing extension source directory: ${sourceDir}`);
  }

  mkdirSyncFn(destinationDir, { recursive: true });
  const entries = readdirSyncFn(sourceDir, { withFileTypes: true });
  for (const entry of entries) {
    const srcPath = path.join(sourceDir, entry.name);
    const destPath = path.join(destinationDir, entry.name);
    if (entry.isDirectory()) {
      copyDirectoryRecursive(srcPath, destPath, deps);
      continue;
    }
    if (entry.isFile()) {
      copyFileSyncFn(srcPath, destPath);
    }
  }
}

function normalizeElectronAppName(appName) {
  const normalized = String(appName || '').trim().toLowerCase();
  if (normalized === 'vscode') return 'code';
  return normalized;
}

function resolveElectronExecutable(appName, deps = {}) {
  const existsSyncFn = typeof deps.existsSync === 'function' ? deps.existsSync : fs.existsSync;
  const commandExistsFn = typeof deps.commandExists === 'function' ? deps.commandExists : commandExists;
  const normalizedName = normalizeElectronAppName(appName);
  if (!normalizedName || !Object.prototype.hasOwnProperty.call(ELECTRON_APPS, normalizedName)) {
    return {
      app: normalizedName,
      executable: null,
      error: 'unknown-app',
      candidates: [],
    };
  }
  const candidates = Array.isArray(ELECTRON_APPS[normalizedName]) ? ELECTRON_APPS[normalizedName] : [];
  if (!candidates.length) {
    return {
      app: normalizedName,
      executable: null,
      error: 'unsupported-on-linux',
      candidates,
    };
  }
  for (const candidate of candidates) {
    if (candidate.includes('/')) {
      if (existsSyncFn(candidate)) {
        return { app: normalizedName, executable: candidate, error: null, candidates };
      }
      continue;
    }
    if (commandExistsFn(candidate)) {
      return { app: normalizedName, executable: candidate, error: null, candidates };
    }
  }
  return {
    app: normalizedName,
    executable: null,
    error: 'executable-not-found',
    candidates,
  };
}

function extractRemoteDebugPort(text) {
  const input = String(text || '');
  const match = input.match(/--remote-debugging-port(?:=|\s+)(\d{1,5})/);
  if (!match) return null;
  const port = parseInt(match[1], 10);
  if (!Number.isInteger(port) || port <= 0 || port > 65535) return null;
  return port;
}

function findElectronDebugPort(appName, deps = {}) {
  const execSyncFn = typeof deps.execSync === 'function' ? deps.execSync : execSync;
  const normalized = String(appName || '').trim();
  if (!normalized) return null;
  const command = `ps aux | grep ${shellEscape(normalized)} | grep -- '--remote-debugging-port' | grep -v grep`;
  try {
    const output = String(execSyncFn(command, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      shell: '/bin/sh',
    }) || '');
    return extractRemoteDebugPort(output);
  } catch {
    return null;
  }
}

async function waitForCdpPort(port, timeoutMs = 10000, intervalMs = 250, deps = {}) {
  const fetchFn = typeof deps.fetch === 'function' ? deps.fetch : fetch;
  const sleepFn = typeof deps.sleep === 'function' ? deps.sleep : sleep;
  const nowFn = typeof deps.now === 'function' ? deps.now : Date.now;
  const target = `http://localhost:${port}/json/version`;
  const deadline = nowFn() + timeoutMs;

  while (nowFn() <= deadline) {
    try {
      const resp = await fetchFn(target);
      if (resp && resp.ok) {
        return true;
      }
    } catch {}
    if (nowFn() >= deadline) break;
    await sleepFn(intervalMs);
  }
  return false;
}

// ─── CDP Helpers ────────────────────────────────────────────────

function parseExtensionIdFromUrl(url) {
  const match = String(url || '').match(/^chrome-extension:\/\/([a-z]{32})\//i);
  if (!match) return null;
  return match[1].toLowerCase();
}

function findServiceWorkerByExtensionId(serviceWorkers, extensionId) {
  const expected = String(extensionId || '').trim().toLowerCase();
  if (!expected) return null;
  const workers = Array.isArray(serviceWorkers) ? serviceWorkers : [];
  for (const worker of workers) {
    const found = parseExtensionIdFromUrl(worker && worker.url);
    if (found === expected) return worker;
  }
  return null;
}

async function findExtensionServiceWorker(deps = {}) {
  const fetchFn = typeof deps.fetch === 'function' ? deps.fetch : fetch;
  const cdpEvalFn = typeof deps.cdpEval === 'function' ? deps.cdpEval : cdpEval;
  const existsSyncFn = typeof deps.existsSync === 'function' ? deps.existsSync : fs.existsSync;
  const readFileSyncFn = typeof deps.readFileSync === 'function' ? deps.readFileSync : fs.readFileSync;
  const writeFileSyncFn = typeof deps.writeFileSync === 'function' ? deps.writeFileSync : fs.writeFileSync;
  const cdpUrl = deps.cdpUrl || CDP_URL;
  const extensionId = deps.extensionId !== undefined ? deps.extensionId : NEO_EXTENSION_ID;
  const extensionIdCacheFile = deps.extensionIdCacheFile || EXTENSION_ID_CACHE_FILE;

  try {
    const resp = await fetchFn(`${cdpUrl}/json/list`);
    if (!resp || !resp.ok) return null;
    const tabs = await resp.json();
    const serviceWorkers = (Array.isArray(tabs) ? tabs : [])
      .filter(t => t && t.type === 'service_worker' && typeof t.webSocketDebuggerUrl === 'string' && t.webSocketDebuggerUrl.trim());

    if (extensionId) {
      return findServiceWorkerByExtensionId(serviceWorkers, extensionId);
    }

    let cachedExtensionId = '';
    try {
      if (existsSyncFn(extensionIdCacheFile)) {
        cachedExtensionId = String(readFileSyncFn(extensionIdCacheFile, 'utf8') || '').trim();
      }
    } catch {}

    if (cachedExtensionId) {
      const cachedWorker = findServiceWorkerByExtensionId(serviceWorkers, cachedExtensionId);
      if (cachedWorker) return cachedWorker;
    }

    for (const worker of serviceWorkers) {
      try {
        const manifestName = await cdpEvalFn(worker.webSocketDebuggerUrl, 'chrome.runtime.getManifest().name', 5000);
        if (manifestName !== 'Neo') continue;
        const discoveredId = parseExtensionIdFromUrl(worker.url);
        if (discoveredId) {
          try {
            writeFileSyncFn(extensionIdCacheFile, `${discoveredId}\n`, 'utf8');
          } catch {}
        }
        return worker;
      } catch {}
    }

    return null;
  } catch {
    return null;
  }
}

async function findExtensionWs(deps = {}) {
  const worker = await findExtensionServiceWorker(deps);
  if (!worker || !worker.webSocketDebuggerUrl) return null;
  return worker.webSocketDebuggerUrl;
}

async function requireExtensionWs() {
  const wsUrl = await findExtensionWs();
  if (!wsUrl) {
    throw new Error(EXTENSION_NOT_FOUND_MESSAGE);
  }
  return wsUrl;
}

async function isSessionMode(sessionName = DEFAULT_SESSION_NAME, deps = {}) {
  const getSessionFn = typeof deps.getSession === 'function' ? deps.getSession : getSession;
  const findExtensionWsFn = typeof deps.findExtensionWs === 'function' ? deps.findExtensionWs : findExtensionWs;
  const normalizedSessionName = sessionName || DEFAULT_SESSION_NAME;
  const session = getSessionFn(normalizedSessionName);
  if (!session || typeof session.pageWsUrl !== 'string' || !session.pageWsUrl.trim()) {
    return false;
  }

  if (deps.extensionWsUrl !== undefined) {
    return !deps.extensionWsUrl;
  }

  const cdpUrl = deps.cdpUrl || session.cdpUrl || CDP_URL;
  const extensionWsUrl = await findExtensionWsFn({ cdpUrl });
  return !extensionWsUrl;
}

function applySessionCaptureFilters(captures, filters = {}) {
  if (!Array.isArray(captures)) return [];
  const config = (filters && typeof filters === 'object' && !Array.isArray(filters)) ? filters : {};
  let rows = captures.slice();

  if (config.domain) {
    const expected = String(config.domain);
    rows = rows.filter(item => String(item && item.domain || '') === expected);
  }
  if (config.method) {
    const expectedMethod = String(config.method).toUpperCase();
    rows = rows.filter(item => String(item && item.method || '').toUpperCase() === expectedMethod);
  }
  if (config.status !== undefined && config.status !== null && config.status !== '') {
    const expectedStatus = Number(config.status);
    rows = rows.filter(item => Number(item && item.responseStatus) === expectedStatus);
  }
  if (config.query) {
    const needle = String(config.query);
    rows = rows.filter((item) => {
      const url = String(item && item.url || '');
      const domain = String(item && item.domain || '');
      return url.includes(needle) || domain.includes(needle);
    });
  }
  if (config.id) {
    const targetId = String(config.id);
    rows = rows.filter(item => String(item && item.id || '') === targetId);
  }
  if (config.idPrefix) {
    const targetPrefix = String(config.idPrefix);
    rows = rows.filter(item => String(item && item.id || '').startsWith(targetPrefix));
  }
  if (config.since) {
    const sinceTs = Number(config.since) || 0;
    rows = rows.filter(item => (Number(item && item.timestamp) || 0) >= sinceTs);
  }
  if (config.sort === 'timestamp-desc') {
    rows.sort((a, b) => (Number(b && b.timestamp) || 0) - (Number(a && a.timestamp) || 0));
  }

  const limit = Number(config.limit);
  if (Number.isFinite(limit) && limit > 0) {
    rows = rows.slice(0, limit);
  }

  return rows;
}

function stripCaptureLimitFilters(filters = {}) {
  const config = (filters && typeof filters === 'object' && !Array.isArray(filters)) ? { ...filters } : {};
  delete config.limit;
  delete config.sort;
  delete config.source;
  return config;
}

function normalizeCaptureSourceFlag(source) {
  const value = String(source || 'all').trim().toLowerCase();
  if (value === 'extension' || value === 'cdp' || value === 'all') return value;
  return 'all';
}

function truncateCaptureText(value, maxBytes = 102400) {
  const text = String(value || '');
  if (Buffer.byteLength(text, 'utf8') <= maxBytes) return text;
  let truncated = text;
  while (Buffer.byteLength(truncated, 'utf8') > maxBytes && truncated.length > 0) {
    truncated = truncated.slice(0, Math.max(1, Math.floor(truncated.length * 0.75)));
  }
  const skipped = Math.max(0, Buffer.byteLength(text, 'utf8') - Buffer.byteLength(truncated, 'utf8'));
  return `${truncated}\n[truncated ${skipped} bytes]`;
}

function parseCaptureTextBody(raw, contentType) {
  const text = String(raw || '');
  const lowerType = String(contentType || '').toLowerCase();
  const looksJson = lowerType.includes('application/json')
    || lowerType.includes('+json')
    || ((text.startsWith('{') && text.endsWith('}')) || (text.startsWith('[') && text.endsWith(']')));
  if (looksJson) {
    try { return JSON.parse(text); }
    catch {}
  }
  return truncateCaptureText(text);
}

function deriveCaptureDomain(url) {
  try { return new URL(url).hostname; }
  catch { return 'unknown'; }
}

function isApiLikeOtherRequest(url, method, headers = {}) {
  const upperMethod = String(method || 'GET').toUpperCase();
  if (upperMethod !== 'GET') return true;
  const normalizedHeaders = headers && typeof headers === 'object' ? headers : {};
  const acceptHeader = String(findHeaderKey(normalizedHeaders, 'accept') ? normalizedHeaders[findHeaderKey(normalizedHeaders, 'accept')] : '').toLowerCase();
  const contentTypeHeader = String(findHeaderKey(normalizedHeaders, 'content-type') ? normalizedHeaders[findHeaderKey(normalizedHeaders, 'content-type')] : '').toLowerCase();
  const requestedWith = String(findHeaderKey(normalizedHeaders, 'x-requested-with') ? normalizedHeaders[findHeaderKey(normalizedHeaders, 'x-requested-with')] : '').toLowerCase();
  if (acceptHeader.includes('json') || acceptHeader.includes('graphql') || acceptHeader.includes('text/event-stream')) return true;
  if (contentTypeHeader.includes('json') || contentTypeHeader.includes('graphql') || contentTypeHeader.includes('x-www-form-urlencoded')) return true;
  if (requestedWith === 'xmlhttprequest') return true;
  try {
    const parsed = new URL(url);
    const pathname = parsed.pathname.toLowerCase();
    if (STATIC_RESOURCE_EXTENSIONS.test(pathname)) return false;
    return /(\/api\/|\/graphql\b|\/rpc\b|\/ajax\b|\/rest\b|\/query\b|\/mutation\b|\.json$)/.test(pathname);
  } catch {
    return false;
  }
}

function shouldCaptureCdpRequest(url, resourceType, method = 'GET', headers = {}) {
  const normalizedResourceType = String(resourceType || '').trim();
  if (!normalizedResourceType) return false;
  if (!['XHR', 'Fetch', 'Other'].includes(normalizedResourceType)) return false;
  try {
    const parsed = new URL(String(url || ''));
    if (CDP_CAPTURE_SKIP_PROTOCOLS.has(parsed.protocol)) return false;
    if (STATIC_RESOURCE_EXTENSIONS.test(parsed.pathname.toLowerCase())) return false;
    if (normalizedResourceType === 'Other') {
      return isApiLikeOtherRequest(parsed.toString(), method, headers);
    }
    return true;
  } catch {
    return false;
  }
}

function normalizeCdpResponseBody(body, mimeType = '', base64Encoded = false) {
  if (body === undefined || body === null) return undefined;
  try {
    const decoded = base64Encoded ? Buffer.from(String(body), 'base64').toString('utf8') : String(body);
    return parseCaptureTextBody(decoded, mimeType);
  } catch {
    return '[unreadable response body]';
  }
}

function normalizeCdpRequestBody(postData, headers = {}) {
  if (postData === undefined || postData === null || postData === '') return undefined;
  const contentTypeKey = findHeaderKey(headers, 'content-type');
  const contentType = contentTypeKey ? headers[contentTypeKey] : '';
  return parseCaptureTextBody(String(postData), contentType);
}

function buildCdpCaptureRecord(entry, responseBody) {
  const request = entry && entry.request ? entry.request : {};
  const response = entry && entry.response ? entry.response : {};
  const responseMimeType = String(response.mimeType || (findHeaderKey(response.headers || {}, 'content-type') ? response.headers[findHeaderKey(response.headers || {}, 'content-type')] : '') || '');
  const startTimestamp = Number(entry && entry.timestamp) || Date.now();
  const endTimestamp = Number(entry && entry.finishedAt) || startTimestamp;
  const duration = Math.max(0, Math.round(endTimestamp - startTimestamp));
  return {
    id: entry && entry.id ? entry.id : crypto.randomUUID(),
    timestamp: startTimestamp,
    domain: deriveCaptureDomain(request.url),
    url: String(request.url || ''),
    method: String(request.method || 'GET').toUpperCase(),
    requestHeaders: request.headers && typeof request.headers === 'object' ? { ...request.headers } : {},
    requestBody: normalizeCdpRequestBody(request.postData, request.headers || {}),
    responseStatus: Number(response.status) || 0,
    responseHeaders: response.headers && typeof response.headers === 'object' ? { ...response.headers } : {},
    responseBody,
    duration,
    trigger: undefined,
    tabId: entry && entry.tabId !== undefined ? entry.tabId : -1,
    tabUrl: String(entry && entry.tabUrl || request.documentURL || ''),
    source: String(entry && entry.resourceType || '').toUpperCase() === 'XHR' ? 'xhr' : 'fetch',
    mimeType: responseMimeType || undefined,
  };
}

function readLocalCaptureJsonl(filePath = LOCAL_CAPTURE_FILE) {
  const target = String(filePath || '').trim();
  if (!target || !fs.existsSync(target)) return [];
  try {
    const raw = String(fs.readFileSync(target, 'utf8') || '');
    const rows = [];
    for (const line of raw.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        const parsed = JSON.parse(trimmed);
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) rows.push(parsed);
      } catch {}
    }
    return rows;
  } catch {
    return [];
  }
}

function writeLocalCaptureJsonl(rows, filePath = LOCAL_CAPTURE_FILE) {
  ensureParentDirectory(filePath);
  const items = Array.isArray(rows) ? rows.filter(item => item && typeof item === 'object' && !Array.isArray(item)) : [];
  const payload = items.map(item => JSON.stringify(item)).join('\n');
  fs.writeFileSync(filePath, payload ? `${payload}\n` : '', 'utf8');
}

function appendLocalCaptureRecord(record, filePath = LOCAL_CAPTURE_FILE) {
  ensureParentDirectory(filePath);
  fs.appendFileSync(filePath, `${JSON.stringify(record)}\n`, 'utf8');
}

async function getExtensionCaptures(filters = {}, deps = {}) {
  const cdpEvalFn = typeof deps.cdpEval === 'function' ? deps.cdpEval : cdpEval;
  const findExtensionWsFn = typeof deps.findExtensionWs === 'function' ? deps.findExtensionWs : findExtensionWs;
  const cdpUrl = deps.cdpUrl || CDP_URL;
  const wsUrl = deps.wsUrl !== undefined ? deps.wsUrl : await findExtensionWsFn({ cdpUrl });
  if (!wsUrl) return [];
  const queryFilters = stripCaptureLimitFilters(filters);
  const raw = await cdpEvalFn(wsUrl, dbEval(`
    var rows = [];
    var domain = ${queryFilters.domain ? JSON.stringify(String(queryFilters.domain)) : 'null'};
    var method = ${queryFilters.method ? JSON.stringify(String(queryFilters.method).toUpperCase()) : 'null'};
    var status = ${queryFilters.status !== undefined && queryFilters.status !== null && queryFilters.status !== '' ? Number(queryFilters.status) : 'null'};
    var query = ${queryFilters.query ? JSON.stringify(String(queryFilters.query)) : 'null'};
    var targetId = ${queryFilters.id ? JSON.stringify(String(queryFilters.id)) : 'null'};
    var targetPrefix = ${queryFilters.idPrefix ? JSON.stringify(String(queryFilters.idPrefix)) : 'null'};
    var since = ${queryFilters.since ? Number(queryFilters.since) : 0};
    store.openCursor().onsuccess = function(e) {
      var c = e.target.result;
      if (!c) { resolve(JSON.stringify(rows)); return; }
      var v = c.value || {};
      if (since && (Number(v.timestamp) || 0) < since) { c.continue(); return; }
      if (domain && String(v.domain || '') !== domain) { c.continue(); return; }
      if (method && String(v.method || '').toUpperCase() !== method) { c.continue(); return; }
      if (status !== null && Number(v.responseStatus) !== status) { c.continue(); return; }
      if (query) {
        var url = String(v.url || '');
        var captureDomain = String(v.domain || '');
        if (url.indexOf(query) < 0 && captureDomain.indexOf(query) < 0) { c.continue(); return; }
      }
      if (targetId && String(v.id || '') !== targetId) { c.continue(); return; }
      if (targetPrefix && !String(v.id || '').startsWith(targetPrefix)) { c.continue(); return; }
      rows.push(v);
      c.continue();
    };
  `), 60000);
  let rows = [];
  try {
    const parsed = JSON.parse(raw || '[]');
    rows = Array.isArray(parsed) ? parsed : [];
  } catch {}
  return applySessionCaptureFilters(rows, queryFilters);
}

async function clearExtensionCaptureRecords(domain = null, deps = {}) {
  const cdpEvalFn = typeof deps.cdpEval === 'function' ? deps.cdpEval : cdpEval;
  const findExtensionWsFn = typeof deps.findExtensionWs === 'function' ? deps.findExtensionWs : findExtensionWs;
  const cdpUrl = deps.cdpUrl || CDP_URL;
  const wsUrl = deps.wsUrl !== undefined ? deps.wsUrl : await findExtensionWsFn({ cdpUrl });
  if (!wsUrl) return { deleted: 0 };

  if (domain) {
    const result = await cdpEvalFn(wsUrl, `new Promise(function(resolve) {
      var req = indexedDB.open("${DB_NAME}");
      req.onsuccess = function() {
        var db = req.result;
        var tx = db.transaction("${STORE_NAME}", "readwrite");
        var store = tx.objectStore("${STORE_NAME}");
        var idx = store.index("domain");
        var deleted = 0;
        idx.openCursor(IDBKeyRange.only(${JSON.stringify(domain)})).onsuccess = function(e) {
          var c = e.target.result;
          if (!c) { resolve(String(deleted)); return; }
          c.delete();
          deleted++;
          c.continue();
        };
      };
      req.onerror = function() { resolve("0"); };
    })`, 30000);
    return { deleted: Number(result) || 0 };
  }

  await cdpEvalFn(wsUrl, `new Promise(function(resolve) {
    var req = indexedDB.open("${DB_NAME}");
    req.onsuccess = function() {
      var db = req.result;
      var tx = db.transaction("${STORE_NAME}", "readwrite");
      tx.objectStore("${STORE_NAME}").clear().onsuccess = function() { resolve("ok"); };
      tx.onerror = function() { resolve("ok"); };
    };
    req.onerror = function() { resolve("ok"); };
  })`, 15000);
  return { deleted: 0 };
}

function clearLocalCaptureRecords(domain = null, filePath = LOCAL_CAPTURE_FILE) {
  const existing = readLocalCaptureJsonl(filePath);
  if (!existing.length) {
    if (!domain && fs.existsSync(filePath)) writeLocalCaptureJsonl([], filePath);
    return { deleted: 0, total: 0 };
  }
  const next = [];
  let deleted = 0;
  for (const item of existing) {
    if (!domain || String(item && item.domain || '') === String(domain)) deleted++;
    else next.push(item);
  }
  writeLocalCaptureJsonl(next, filePath);
  return { deleted, total: next.length };
}

function readCaptureDaemonState(filePath = CDP_CAPTURE_STATE_FILE) {
  return readJsonFileSafe(filePath, null);
}

function isProcessAlive(pid) {
  const parsed = Number(pid);
  if (!Number.isInteger(parsed) || parsed <= 0) return false;
  try {
    process.kill(parsed, 0);
    return true;
  } catch {
    return false;
  }
}

async function loadCapturesFromSources(sessionName = DEFAULT_SESSION_NAME, filters = {}, deps = {}) {
  const queryFilters = (filters && typeof filters === 'object' && !Array.isArray(filters)) ? { ...filters } : {};
  const source = normalizeCaptureSourceFlag(queryFilters.source || 'all');
  const storeFilters = stripCaptureLimitFilters(queryFilters);
  const combined = [];
  const cdpUrl = deps.cdpUrl || getSessionCdpUrl(sessionName);
  const findExtensionWsFn = typeof deps.findExtensionWs === 'function' ? deps.findExtensionWs : findExtensionWs;
  let extensionWsUrl = deps.extensionWsUrl;
  if (extensionWsUrl === undefined && (source === 'all' || source === 'extension')) {
    extensionWsUrl = await findExtensionWsFn({ cdpUrl });
  }

  if ((source === 'all' || source === 'extension') && extensionWsUrl) {
    combined.push(...await getExtensionCaptures(storeFilters, {
      ...deps,
      wsUrl: extensionWsUrl,
      cdpUrl,
    }));
  }

  let cdpRows = [];
  if (source === 'all' || source === 'cdp') {
    cdpRows = applySessionCaptureFilters(readLocalCaptureJsonl(deps.captureFile || LOCAL_CAPTURE_FILE), storeFilters);
    combined.push(...cdpRows);
  }

  if ((source === 'all' || source === 'cdp') && cdpRows.length === 0 && hasActiveSession(sessionName, deps)) {
    try {
      combined.push(...await getSessionCaptures(sessionName, storeFilters, deps));
    } catch {}
  }

  return applySessionCaptureFilters(combined, queryFilters);
}

async function findCaptureById(sessionName = DEFAULT_SESSION_NAME, id, filters = {}, deps = {}) {
  const targetId = String(id || '');
  if (!targetId) return null;
  const rows = await loadCapturesFromSources(sessionName, {
    ...filters,
    source: filters && filters.source ? filters.source : 'all',
  }, deps);
  return rows.find(item => item && String(item.id || '') === targetId)
    || rows.find(item => item && String(item.id || '').startsWith(targetId))
    || null;
}

async function getSessionCaptures(sessionName = DEFAULT_SESSION_NAME, filters = {}, deps = {}) {
  let resolvedFilters = filters;
  let resolvedDeps = deps;
  if (
    resolvedFilters
    && typeof resolvedFilters === 'object'
    && !Array.isArray(resolvedFilters)
    && (typeof resolvedFilters.getSession === 'function' || typeof resolvedFilters.cdpSend === 'function')
  ) {
    resolvedDeps = resolvedFilters;
    resolvedFilters = {};
  }

  const getSessionFn = typeof resolvedDeps.getSession === 'function' ? resolvedDeps.getSession : getSession;
  const sendFn = typeof resolvedDeps.cdpSend === 'function' ? resolvedDeps.cdpSend : cdpSend;
  const normalizedSessionName = sessionName || DEFAULT_SESSION_NAME;
  const session = getSessionFn(normalizedSessionName);
  if (!session || typeof session.pageWsUrl !== 'string' || !session.pageWsUrl.trim()) {
    return [];
  }

  const evaluated = await sendFn(session.pageWsUrl, 'Runtime.evaluate', {
    expression: `(function() {
      try {
        var rows = Array.isArray(globalThis.__NEO_CAPTURES__) ? globalThis.__NEO_CAPTURES__ : [];
        if (rows.length > 500) rows = rows.slice(rows.length - 500);
        return rows;
      } catch (e) {
        return [];
      }
    })()`,
    returnByValue: true,
    awaitPromise: true,
  });

  const value = evaluated && evaluated.result ? evaluated.result.value : [];
  const rows = Array.isArray(value) ? value.filter(item => item && typeof item === 'object') : [];
  return applySessionCaptureFilters(rows, resolvedFilters);
}

async function clearSessionCaptures(sessionName = DEFAULT_SESSION_NAME, domain = null, deps = {}) {
  const getSessionFn = typeof deps.getSession === 'function' ? deps.getSession : getSession;
  const sendFn = typeof deps.cdpSend === 'function' ? deps.cdpSend : cdpSend;
  const normalizedSessionName = sessionName || DEFAULT_SESSION_NAME;
  const session = getSessionFn(normalizedSessionName);
  if (!session || typeof session.pageWsUrl !== 'string' || !session.pageWsUrl.trim()) {
    return { deleted: 0, total: 0 };
  }

  const evaluated = await sendFn(session.pageWsUrl, 'Runtime.evaluate', {
    expression: `(function() {
      try {
        var rows = Array.isArray(globalThis.__NEO_CAPTURES__) ? globalThis.__NEO_CAPTURES__ : [];
        var domain = ${domain ? JSON.stringify(domain) : 'null'};
        if (!domain) {
          var deletedAll = rows.length;
          globalThis.__NEO_CAPTURES__ = [];
          return { deleted: deletedAll, total: 0 };
        }
        var next = [];
        var deleted = 0;
        for (var i = 0; i < rows.length; i++) {
          var item = rows[i];
          if (item && item.domain === domain) deleted++;
          else next.push(item);
        }
        if (next.length > 500) next = next.slice(next.length - 500);
        globalThis.__NEO_CAPTURES__ = next;
        return { deleted: deleted, total: next.length };
      } catch (e) {
        return { deleted: 0, total: 0 };
      }
    })()`,
    returnByValue: true,
    awaitPromise: true,
  });

  const value = evaluated && evaluated.result ? evaluated.result.value : null;
  if (!value || typeof value !== 'object') {
    return { deleted: 0, total: 0 };
  }
  return {
    deleted: Number(value.deleted) || 0,
    total: Number(value.total) || 0,
  };
}

async function findTab(pattern, deps = {}) {
  const fetchFn = typeof deps.fetch === 'function' ? deps.fetch : fetch;
  const cdpUrl = deps.cdpUrl || CDP_URL;
  let tabs;
  try {
    tabs = await (await fetchFn(`${cdpUrl}/json/list`)).json();
  } catch {
    // Lightpanda: no /json/list — synthesize from session + re-bootstrap context
    const session = getSession(DEFAULT_SESSION_NAME);
    if (session && session.pageWsUrl && /^wss?:\/\/[^/]+\/?$/.test(session.pageWsUrl)) {
      await ensureLightpandaPageContext(DEFAULT_SESSION_NAME);
      return {
        id: session.tabId || 'lightpanda',
        type: 'page',
        title: '',
        url: session.currentUrl || 'about:blank',
        webSocketDebuggerUrl: session.pageWsUrl,
      };
    }
    throw new Error(`No tabs found (${cdpUrl}/json/list unavailable)`);
  }
  if (pattern) {
    const tab = tabs.find(t => t.type === 'page' && t.url.includes(pattern));
    if (!tab) throw new Error(`No tab matching "${pattern}"`);
    return tab;
  }
  const pages = tabs.filter(t => t.type === 'page');
  if (!pages.length) throw new Error('No browser tabs found');
  return pages[0];
}

// Persistent WebSocket pool for CDP backends that multiplex on a single connection
// (e.g. Lightpanda). Chrome uses per-tab WS URLs so each connection is independent.
const _persistentWsPool = {};
let _persistentWsIdCounter = 1;

function cdpSendPersistent(wsUrl, method, params = {}, timeout = 10000) {
  return new Promise((resolve, reject) => {
    const WebSocketCtor = getWebSocketCtor();
    if (!wsUrl) { reject(new Error('Missing WebSocket URL')); return; }
    if (!method) { reject(new Error('Missing CDP method')); return; }

    let entry = _persistentWsPool[wsUrl];
    if (entry && (entry.ws.readyState === WebSocketCtor.CLOSING || entry.ws.readyState === WebSocketCtor.CLOSED)) {
      delete _persistentWsPool[wsUrl];
      entry = null;
    }

    const sendMessage = (ws) => {
      const id = _persistentWsIdCounter++;
      let settled = false;
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        reject(new Error(`CDP timeout: ${method}`));
      }, timeout);
      const handler = (raw) => {
        let msg;
        try { msg = JSON.parse(raw.toString()); } catch { return; }
        if (msg.id !== id) return;
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        ws.removeListener('message', handler);
        if (msg.error) reject(new Error(`CDP ${method} failed: ${msg.error.message || 'Unknown error'}`));
        else resolve(msg.result);
      };
      ws.on('message', handler);
      const message = { id, method, params: params || {} };
      ws.send(JSON.stringify(message));
    };

    if (entry && entry.ws.readyState === WebSocketCtor.OPEN) {
      sendMessage(entry.ws);
    } else {
      // Create new persistent connection
      const ws = new WebSocketCtor(wsUrl);
      const connectTimer = setTimeout(() => {
        try { ws.close(); } catch {}
        reject(new Error(`CDP connect timeout: ${wsUrl}`));
      }, timeout);
      ws.on('open', () => {
        clearTimeout(connectTimer);
        _persistentWsPool[wsUrl] = { ws };
        sendMessage(ws);
      });
      ws.on('error', (err) => {
        clearTimeout(connectTimer);
        delete _persistentWsPool[wsUrl];
        reject(err);
      });
    }
  });
}

function cdpSend(pageWsUrl, method, params = {}, timeout = 10000) {
  // Use persistent connection for Lightpanda-style single-WS backends
  // Detected by: URL does not contain a target/page-specific path segment
  if (pageWsUrl && /^wss?:\/\/[^/]+\/?$/.test(pageWsUrl)) {
    return cdpSendPersistent(pageWsUrl, method, params, timeout);
  }
  // Chrome: per-tab WS URL, create a fresh connection each time
  return new Promise((resolve, reject) => {
    const WebSocketCtor = getWebSocketCtor();
    if (!pageWsUrl) {
      reject(new Error('Missing page WebSocket URL'));
      return;
    }
    if (!method) {
      reject(new Error('Missing CDP method'));
      return;
    }

    const ws = new WebSocketCtor(pageWsUrl);
    const id = 1;
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      try { ws.close(); } catch {}
      reject(new Error(`CDP timeout: ${method}`));
    }, timeout);

    function done(err, result) {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try { ws.close(); } catch {}
      if (err) reject(err);
      else resolve(result);
    }

    ws.on('open', () => {
      ws.send(JSON.stringify({ id, method, params: params || {} }));
    });

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); }
      catch { return; }
      if (msg.id !== id) return;
      if (msg.error) {
        done(new Error(`CDP ${method} failed: ${msg.error.message || 'Unknown error'}`));
        return;
      }
      done(null, msg.result);
    });

    ws.on('error', (err) => done(err));
    ws.on('close', () => {
      if (!settled) done(new Error(`CDP socket closed before response: ${method}`));
    });
  });
}

function cdpEval(wsUrl, expression, timeout = 30000) {
  return new Promise((resolve, reject) => {
    const WebSocketCtor = getWebSocketCtor();
    const ws = new WebSocketCtor(wsUrl);
    const timer = setTimeout(() => { ws.close(); reject(new Error('CDP timeout')); }, timeout);
    ws.on('open', () => {
      ws.send(JSON.stringify({ id: 1, method: 'Runtime.enable' }));
      setTimeout(() => {
        ws.send(JSON.stringify({
          id: 2, method: 'Runtime.evaluate',
          params: { expression, awaitPromise: true, returnByValue: true }
        }));
      }, 200);
    });
    ws.on('message', raw => {
      const msg = JSON.parse(raw.toString());
      if (msg.id === 2) {
        clearTimeout(timer);
        ws.close();
        if (msg.result?.exceptionDetails) {
          reject(new Error(JSON.stringify(msg.result.exceptionDetails)));
        } else {
          resolve(msg.result?.result?.value);
        }
      }
    });
    ws.on('error', err => { clearTimeout(timer); reject(err); });
  });
}

function axValueToString(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  if (typeof value === 'object' && value.value !== undefined && value.value !== null) {
    return String(value.value);
  }
  return '';
}

function normalizeRefKey(ref) {
  const raw = String(ref === undefined || ref === null ? '' : ref).trim();
  if (!raw) {
    throw new Error(`Invalid ref: ${ref}`);
  }

  let value = raw;
  const legacyMatch = value.match(/^@e(.+)$/i);
  if (legacyMatch) value = legacyMatch[1].trim();
  const bracketMatch = value.match(/^\[(.+)\]$/);
  if (bracketMatch) value = bracketMatch[1].trim();
  if (!/^\d+$/.test(value)) {
    throw new Error(`Invalid ref: ${ref}`);
  }

  return String(parseInt(value, 10));
}

function getRefEntry(session, ref) {
  const normalizedRef = normalizeRefKey(ref);
  const refs = session && typeof session === 'object' && session.refs && typeof session.refs === 'object'
    ? session.refs
    : {};
  const candidates = [normalizedRef, `@e${normalizedRef}`];
  for (const key of candidates) {
    const value = refs[key];
    if (value && typeof value === 'object') {
      return { normalizedRef, key, value };
    }
  }
  return { normalizedRef, key: normalizedRef, value: null };
}

function hasStoredRef(session, ref) {
  try {
    return !!getRefEntry(session, ref).value;
  } catch {
    return false;
  }
}

function formatDisplayRef(ref) {
  try {
    return normalizeRefKey(ref);
  } catch {
    const raw = String(ref === undefined || ref === null ? '' : ref).trim();
    return raw || '?';
  }
}

function assignRefs(axTreeNodes) {
  const sourceNodes = Array.isArray(axTreeNodes) ? axTreeNodes.filter(node => node && typeof node === 'object') : [];
  const nodeById = new Map();
  const childIds = new Set();

  for (const node of sourceNodes) {
    if (node.nodeId === undefined || node.nodeId === null) continue;
    nodeById.set(String(node.nodeId), node);
  }

  for (const node of sourceNodes) {
    for (const childId of Array.isArray(node.childIds) ? node.childIds : []) {
      childIds.add(String(childId));
    }
  }

  const roots = sourceNodes.filter((node) => {
    if (node.nodeId === undefined || node.nodeId === null) return true;
    return !childIds.has(String(node.nodeId));
  });

  const visited = new Set();
  const nodes = [];
  const refs = {};
  let nextRef = 0;

  function visit(node, depth) {
    if (!node || typeof node !== 'object') return;
    if (visited.has(node)) return;
    visited.add(node);

    const ref = String(nextRef++);
    const role = axValueToString(node.role).trim().toLowerCase() || 'unknown';
    const name = axValueToString(node.name).replace(/\s+/g, ' ').trim();
    const backendDOMNodeId = Number.isInteger(node.backendDOMNodeId) ? node.backendDOMNodeId : null;
    const bounds = node.bounds && typeof node.bounds === 'object' ? node.bounds : null;

    nodes.push({ ref, depth, role, name, backendDOMNodeId, bounds });
    refs[ref] = { backendDOMNodeId, role, name, bounds };

    for (const childId of Array.isArray(node.childIds) ? node.childIds : []) {
      const childNode = nodeById.get(String(childId));
      if (childNode) visit(childNode, depth + 1);
    }
  }

  for (const root of roots) visit(root, 0);
  for (const node of sourceNodes) {
    if (!visited.has(node)) visit(node, 0);
  }

  return { nodes, refs };
}

function formatSnapshot(nodes) {
  const rows = [];
  for (const node of Array.isArray(nodes) ? nodes : []) {
    if (!node || typeof node !== 'object') continue;
    const depth = Number.isInteger(node.depth) && node.depth > 0 ? node.depth : 0;
    const indent = '  '.repeat(depth);
    const ref = formatDisplayRef(node.ref);
    const role = String(node.role || 'unknown');
    const name = String(node.name || '').replace(/\s+/g, ' ').trim().replace(/"/g, '\\"');
    rows.push(`${indent}[${ref}] ${role} "${name}"`);
  }
  return rows.join('\n');
}

function getActivePageSession(sessionName = DEFAULT_SESSION_NAME) {
  const session = getSession(sessionName);
  if (!session || typeof session.pageWsUrl !== 'string' || !session.pageWsUrl.trim()) {
    throw new Error('Run neo connect [port] first');
  }
  return session;
}

function getSessionPageWsUrl(sessionName = DEFAULT_SESSION_NAME) {
  return getActivePageSession(sessionName).pageWsUrl;
}

function getBackendNodeIdFromRef(session, ref) {
  const entry = getRefEntry(session, ref);
  const backendDOMNodeId = entry.value && Number.isInteger(entry.value.backendDOMNodeId)
    ? entry.value.backendDOMNodeId
    : null;
  if (!backendDOMNodeId) {
    throw new Error(`Unknown ref: ${entry.normalizedRef}. Run neo snapshot first`);
  }
  return backendDOMNodeId;
}

function centerFromQuad(quad) {
  if (!Array.isArray(quad) || quad.length < 8) return null;
  const xs = [quad[0], quad[2], quad[4], quad[6]].map(Number).filter(Number.isFinite);
  const ys = [quad[1], quad[3], quad[5], quad[7]].map(Number).filter(Number.isFinite);
  if (xs.length !== 4 || ys.length !== 4) return null;
  const x = xs.reduce((sum, value) => sum + value, 0) / xs.length;
  const y = ys.reduce((sum, value) => sum + value, 0) / ys.length;
  return { x, y };
}

async function resolveRef(sessionName, ref, deps = {}) {
  const getSessionFn = typeof deps.getSession === 'function' ? deps.getSession : getSession;
  const sendFn = typeof deps.cdpSend === 'function' ? deps.cdpSend : cdpSend;
  const normalizedSessionName = sessionName || DEFAULT_SESSION_NAME;
  const session = getSessionFn(normalizedSessionName);
  if (!session || !session.pageWsUrl) {
    throw new Error('Run neo connect [port] first');
  }

  const backendDOMNodeId = getBackendNodeIdFromRef(session, ref);
  const resolved = await sendFn(session.pageWsUrl, 'DOM.resolveNode', { backendNodeId: backendDOMNodeId });
  const objectId = resolved && resolved.object && resolved.object.objectId;
  if (!objectId) {
    throw new Error(`Failed to resolve node for ${ref}`);
  }

  let boxModel;
  try {
    boxModel = await sendFn(session.pageWsUrl, 'DOM.getBoxModel', { objectId });
  } catch {
    boxModel = await sendFn(session.pageWsUrl, 'DOM.getBoxModel', { backendNodeId: backendDOMNodeId });
  }
  const quad = boxModel && boxModel.model && boxModel.model.content;
  const center = centerFromQuad(quad);
  if (!center) {
    throw new Error(`Failed to get box model for ${ref}`);
  }

  return {
    objectId,
    x: center.x,
    y: center.y,
    backendDOMNodeId,
  };
}

const PRESS_KEY_MAP = {
  enter: { key: 'Enter', code: 'Enter' },
  tab: { key: 'Tab', code: 'Tab' },
  escape: { key: 'Escape', code: 'Escape' },
  esc: { key: 'Escape', code: 'Escape' },
  backspace: { key: 'Backspace', code: 'Backspace' },
  arrowup: { key: 'ArrowUp', code: 'ArrowUp' },
  arrowdown: { key: 'ArrowDown', code: 'ArrowDown' },
  arrowleft: { key: 'ArrowLeft', code: 'ArrowLeft' },
  arrowright: { key: 'ArrowRight', code: 'ArrowRight' },
  space: { key: ' ', code: 'Space', text: ' ' },
  delete: { key: 'Delete', code: 'Delete' },
  home: { key: 'Home', code: 'Home' },
  end: { key: 'End', code: 'End' },
  pageup: { key: 'PageUp', code: 'PageUp' },
  pagedown: { key: 'PageDown', code: 'PageDown' },
};

function parsePressKey(rawKey) {
  const input = String(rawKey || '').trim();
  if (!input) return null;

  const parts = input.split('+').map(part => part.trim()).filter(Boolean);
  if (!parts.length) return null;

  const keyPart = parts.pop();
  const lowerKey = String(keyPart || '').toLowerCase();
  let mapped = PRESS_KEY_MAP[lowerKey] ? { ...PRESS_KEY_MAP[lowerKey] } : null;

  if (!mapped && /^[a-z]$/i.test(keyPart)) {
    const char = keyPart.toLowerCase();
    mapped = { key: char, code: `Key${char.toUpperCase()}` };
  }

  if (!mapped) return null;

  let modifiers = 0;
  for (const token of parts) {
    const lower = token.toLowerCase();
    if (lower === 'ctrl' || lower === 'control') {
      modifiers |= 2;
      continue;
    }
    if (lower === 'alt') {
      modifiers |= 1;
      continue;
    }
    if (lower === 'meta' || lower === 'cmd' || lower === 'command') {
      modifiers |= 4;
      continue;
    }
    if (lower === 'shift') {
      modifiers |= 8;
      continue;
    }
    return null;
  }

  if (modifiers) mapped.modifiers = modifiers;
  return mapped;
}

async function resolveScrollPoint(pageWsUrl, selector) {
  const expression = `(function() {
    try {
      var el = ${selector ? `document.querySelector(${JSON.stringify(selector)})` : 'null'};
      if (el) {
        var rect = el.getBoundingClientRect();
        return {
          x: Math.round(rect.left + rect.width / 2),
          y: Math.round(rect.top + rect.height / 2)
        };
      }
    } catch (e) {}
    return {
      x: Math.round(window.innerWidth / 2),
      y: Math.round(window.innerHeight / 2)
    };
  })()`;
  const result = await cdpSend(pageWsUrl, 'Runtime.evaluate', {
    expression,
    returnByValue: true,
  });
  const value = result && result.result && result.result.value;
  if (!value || !Number.isFinite(value.x) || !Number.isFinite(value.y)) {
    return { x: 0, y: 0 };
  }
  return {
    x: Number(value.x),
    y: Number(value.y),
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function selectorFromObject(pageWsUrl, objectId) {
  const result = await cdpSend(pageWsUrl, 'Runtime.callFunctionOn', {
    objectId,
    functionDeclaration: `function() {
      function fallbackEscape(value) {
        return String(value || '').replace(/[^a-zA-Z0-9_\\-]/g, '\\\\$&');
      }
      function esc(value) {
        if (typeof CSS !== 'undefined' && CSS.escape) return CSS.escape(value);
        return fallbackEscape(value);
      }
      if (!this || this.nodeType !== 1) return '';
      if (this.id) return '#' + esc(this.id);
      var parts = [];
      var node = this;
      while (node && node.nodeType === 1 && parts.length < 8) {
        var part = node.nodeName.toLowerCase();
        if (node.classList && node.classList.length > 0) {
          part += '.' + Array.from(node.classList).slice(0, 2).map(esc).join('.');
        }
        var parent = node.parentElement;
        if (parent) {
          var siblings = Array.from(parent.children).filter(function(child) {
            return child.nodeName === node.nodeName;
          });
          if (siblings.length > 1) {
            part += ':nth-of-type(' + (siblings.indexOf(node) + 1) + ')';
          }
        }
        parts.unshift(part);
        if (node.id) break;
        node = parent;
      }
      return parts.join(' > ');
    }`,
    returnByValue: true,
  });
  const selector = result && result.result ? result.result.value : '';
  return typeof selector === 'string' ? selector.trim() : '';
}

async function waitForSelector(pageWsUrl, selector, timeoutMs = 10000, intervalMs = 500) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    const doc = await cdpSend(pageWsUrl, 'DOM.getDocument', { depth: 1, pierce: true });
    const rootNodeId = doc && doc.root && doc.root.nodeId;
    if (Number.isInteger(rootNodeId)) {
      const found = await cdpSend(pageWsUrl, 'DOM.querySelector', {
        nodeId: rootNodeId,
        selector,
      });
      if (found && Number.isInteger(found.nodeId) && found.nodeId > 0) {
        return true;
      }
    }
    if (Date.now() + intervalMs > deadline) break;
    await sleep(intervalMs);
  }
  return false;
}

function normalizeCookieDomainFilter(domain) {
  const raw = String(domain || '').trim().toLowerCase();
  if (!raw) return '';
  try {
    const parsed = raw.includes('://') ? new URL(raw) : new URL(`https://${raw}`);
    return parsed.hostname.toLowerCase().replace(/^\.+/, '');
  } catch {
    return raw.replace(/^https?:\/\//, '').replace(/\/.*$/, '').replace(/^\.+/, '');
  }
}

function buildCookieQueryUrls(domain) {
  const host = normalizeCookieDomainFilter(domain);
  if (!host) return null;
  return [`https://${host}/`, `http://${host}/`];
}

function cookieMatchesDomain(cookie, domain) {
  const target = normalizeCookieDomainFilter(domain);
  if (!target) return true;
  const cookieDomain = normalizeCookieDomainFilter(cookie && cookie.domain);
  if (!cookieDomain) return false;
  return cookieDomain === target
    || cookieDomain.endsWith(`.${target}`)
    || target.endsWith(`.${cookieDomain}`);
}

function normalizeCookieForExport(cookie) {
  const source = cookie && typeof cookie === 'object' ? cookie : {};
  const normalized = {
    name: String(source.name || ''),
    value: String(source.value || ''),
    domain: String(source.domain || ''),
    path: String(source.path || '/'),
    expires: Number.isFinite(Number(source.expires)) ? Number(source.expires) : -1,
    httpOnly: !!source.httpOnly,
    secure: !!source.secure,
    sameSite: source.sameSite === undefined || source.sameSite === null ? undefined : String(source.sameSite),
  };
  if (normalized.sameSite === undefined) delete normalized.sameSite;
  return normalized;
}

function serializeCookiesForExport(cookies) {
  const rows = Array.isArray(cookies) ? cookies.map(normalizeCookieForExport) : [];
  return JSON.stringify(rows, null, 2);
}

function parseCookiesImportText(raw) {
  const parsed = JSON.parse(String(raw || ''));
  if (!Array.isArray(parsed)) {
    throw new Error('Cookie import file must contain a JSON array');
  }
  return parsed.map((cookie) => {
    if (!cookie || typeof cookie !== 'object' || Array.isArray(cookie)) {
      throw new Error('Cookie import entries must be objects');
    }
    const normalized = normalizeCookieForExport(cookie);
    if (!normalized.name) {
      throw new Error('Cookie import entry is missing name');
    }
    if (!normalized.domain) {
      throw new Error(`Cookie ${normalized.name} is missing domain`);
    }
    return normalized;
  });
}

function formatCookieValueForDisplay(value, maxLength = 40) {
  const text = String(value === undefined || value === null ? '' : value);
  if (text.length <= maxLength) return text;
  if (maxLength <= 3) return text.slice(0, maxLength);
  return `${text.slice(0, maxLength - 3)}...`;
}

function formatCookieExpires(expires) {
  const numeric = Number(expires);
  if (!Number.isFinite(numeric) || numeric <= 0) return 'session';
  return new Date(numeric * 1000).toISOString();
}

function formatCookieRow(cookie) {
  const normalized = normalizeCookieForExport(cookie);
  return {
    Domain: normalized.domain,
    Name: normalized.name,
    Value: formatCookieValueForDisplay(normalized.value),
    Expires: formatCookieExpires(normalized.expires),
    Secure: normalized.secure ? 'yes' : 'no',
    HttpOnly: normalized.httpOnly ? 'yes' : 'no',
  };
}

function renderCookieTable(cookies) {
  const rows = Array.isArray(cookies) ? cookies.map(formatCookieRow) : [];
  if (!rows.length) return '(no cookies)';

  const headers = ['Domain', 'Name', 'Value', 'Expires', 'Secure', 'HttpOnly'];
  const widths = headers.map((header) => Math.max(header.length, ...rows.map(row => String(row[header] || '').length)));
  const renderLine = (row) => headers
    .map((header, index) => String(row[header] || '').padEnd(widths[index]))
    .join('  ')
    .trimEnd();

  return [
    renderLine(Object.fromEntries(headers.map(header => [header, header]))),
    ...rows.map(renderLine),
  ].join('\n');
}

async function getCookiesForSession(sessionName = DEFAULT_SESSION_NAME, domain = null, deps = {}) {
  const getSessionFn = typeof deps.getSession === 'function' ? deps.getSession : getSession;
  const sendFn = typeof deps.cdpSend === 'function' ? deps.cdpSend : cdpSend;
  const normalizedSessionName = sessionName || DEFAULT_SESSION_NAME;
  const session = getSessionFn(normalizedSessionName);
  if (!session || typeof session.pageWsUrl !== 'string' || !session.pageWsUrl.trim()) {
    throw new Error('Run neo connect [port] first');
  }

  const params = {};
  const urls = buildCookieQueryUrls(domain);
  if (urls && urls.length) params.urls = urls;
  const result = await sendFn(session.pageWsUrl, 'Network.getCookies', params);
  const cookies = Array.isArray(result && result.cookies) ? result.cookies : [];
  return domain ? cookies.filter(cookie => cookieMatchesDomain(cookie, domain)) : cookies;
}

function buildSetCookieParams(cookie) {
  const normalized = normalizeCookieForExport(cookie);
  const params = {
    name: normalized.name,
    value: normalized.value,
    domain: normalized.domain,
    path: normalized.path || '/',
    secure: normalized.secure,
    httpOnly: normalized.httpOnly,
  };
  if (normalized.sameSite) params.sameSite = normalized.sameSite;
  if (Number.isFinite(normalized.expires) && normalized.expires > 0) {
    params.expires = normalized.expires;
  }
  return params;
}

function buildDeleteCookieParams(cookie) {
  const source = cookie && typeof cookie === 'object' ? cookie : {};
  const params = {
    name: String(source.name || ''),
    domain: String(source.domain || ''),
    path: String(source.path || '/'),
  };
  if (source.partitionKey !== undefined) params.partitionKey = source.partitionKey;
  return params;
}

function looksLikeCookieExportFile(value) {
  const raw = String(value || '').trim();
  if (!raw) return false;
  if (raw.includes(path.sep) || raw.startsWith('./') || raw.startsWith('../')) return true;
  return /\.json$/i.test(raw);
}

function dbEval(body) {
  return `new Promise(function(resolve) {
    var req = indexedDB.open("${DB_NAME}");
    req.onsuccess = function() {
      var db = req.result;
      var tx = db.transaction("${STORE_NAME}", "readonly");
      var store = tx.objectStore("${STORE_NAME}");
      ${body}
    };
    req.onerror = function() { resolve("Error: " + req.error); };
    setTimeout(function() { resolve("timeout"); }, 10000);
  })`;
}

async function fetchJsonOrThrow(url, timeoutMs = 3000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { signal: controller.signal });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status} for ${url}`);
    }
    return response.json();
  } finally {
    clearTimeout(timer);
  }
}

const AUTH_HEADER_PATTERNS = [
  'authorization', 'x-csrf-token', 'x-twitter-auth-type', 'x-twitter-active-user',
  'x-twitter-client-language', 'x-client-transaction-id', 'x-requested-with',
  'github-verified-fetch', 'x-fetch-nonce', 'x-github-client-version',
  'x-api-key', 'api-key',
];

const REDACTED_HEADER_VALUE = '[REDACTED]';
const AUTH_HEADER_REGEX = /token|auth|key|secret|session/i;
const FORBIDDEN_FETCH_HEADER_NAMES = ['host', 'content-length', 'cookie'];

function isAuthHeader(name) {
  const lk = String(name || '').toLowerCase();
  if (!lk) return false;
  if (lk === 'authorization' || lk === 'cookie' || lk === 'x-csrf-token') return true;
  return AUTH_HEADER_PATTERNS.includes(lk)
    || lk.startsWith('x-csrf')
    || lk.startsWith('x-api')
    || lk.startsWith('x-twitter')
    || AUTH_HEADER_REGEX.test(lk);
}

function isRedactedAuthValue(value) {
  return String(value || '').trim() === REDACTED_HEADER_VALUE;
}

function findHeaderKey(headers, name) {
  const target = String(name || '').toLowerCase();
  for (const key of Object.keys(headers || {})) {
    if (key.toLowerCase() === target) return key;
  }
  return null;
}

function setHeaderValue(headers, name, value, overwrite = false) {
  if (!headers || !name) return false;
  const existing = findHeaderKey(headers, name);
  if (existing) {
    if (!overwrite) return false;
    headers[existing] = String(value);
    return true;
  }
  headers[name] = String(value);
  return true;
}

function deleteHeaderCaseInsensitive(headers, name) {
  const existing = findHeaderKey(headers, name);
  if (!existing) return;
  delete headers[existing];
}

function redactAuthHeaders(headers) {
  const redacted = {};
  for (const [name, value] of Object.entries(headers || {})) {
    redacted[name] = isAuthHeader(name) ? REDACTED_HEADER_VALUE : String(value);
  }
  return redacted;
}

function extractAuthHeaders(headers, { includeRedacted = false } = {}) {
  const selected = {};
  for (const [name, value] of Object.entries(headers || {})) {
    if (!isAuthHeader(name)) continue;
    const stringValue = String(value);
    if (!includeRedacted && isRedactedAuthValue(stringValue)) continue;
    selected[name] = stringValue;
  }
  return selected;
}

function hasRedactedAuthHeaders(headers) {
  for (const [name, value] of Object.entries(headers || {})) {
    if (!isAuthHeader(name)) continue;
    if (isRedactedAuthValue(value)) return true;
  }
  return false;
}

function applyAuthHeaders(targetHeaders, sourceHeaders, overwrite = true) {
  let applied = 0;
  for (const [name, value] of Object.entries(sourceHeaders || {})) {
    if (!isAuthHeader(name)) continue;
    if (isRedactedAuthValue(value)) continue;
    if (setHeaderValue(targetHeaders, name, String(value), overwrite)) {
      applied++;
    }
  }
  return applied;
}

function stripForbiddenFetchHeaders(headers) {
  for (const name of FORBIDDEN_FETCH_HEADER_NAMES) {
    deleteHeaderCaseInsensitive(headers, name);
  }
}

function hostMatchesDomain(hostname, domain) {
  const host = String(hostname || '').toLowerCase();
  const expected = String(domain || '').toLowerCase();
  if (!host || !expected) return false;
  return host === expected || host.endsWith(`.${expected}`) || expected.endsWith(`.${host}`);
}

async function findStoredAuthHeaders(wsUrl, domain, limit = 80) {
  const raw = await cdpEval(wsUrl, dbEval(`
    var rows = [], domain = ${JSON.stringify(domain)}, limit = ${limit};
    store.openCursor(null, "prev").onsuccess = function(e) {
      var c = e.target.result;
      if (c && rows.length < limit) {
        var v = c.value;
        if (v.domain === domain && v.requestHeaders) rows.push(v.requestHeaders);
        c.continue();
      } else {
        resolve(JSON.stringify(rows));
      }
    };
  `), 20000);

  let rows = [];
  try { rows = JSON.parse(raw || '[]'); }
  catch { return {}; }

  for (const headers of rows) {
    const candidate = extractAuthHeaders(headers);
    if (Object.keys(candidate).length > 0) return candidate;
  }
  return {};
}

function captureLiveAuthHeadersFromTab(tab, domain, probeUrl, timeoutMs = 2500) {
  return new Promise((resolve) => {
    if (!tab?.webSocketDebuggerUrl || !domain) { resolve({}); return; }

    const WebSocketCtor = getWebSocketCtor();
    const ws = new WebSocketCtor(tab.webSocketDebuggerUrl);
    let nextId = 0;
    const pending = new Map();
    const matchingRequestIds = new Set();
    let finished = false;
    let timer = null;

    const finish = (headers = {}) => {
      if (finished) return;
      finished = true;
      if (timer) clearTimeout(timer);
      try { ws.close(); } catch {}
      resolve(headers);
    };

    const send = (method, params = {}) => new Promise((resolveSend, rejectSend) => {
      if (ws.readyState !== WebSocketCtor.OPEN) {
        rejectSend(new Error('CDP socket not open'));
        return;
      }
      const id = ++nextId;
      pending.set(id, { resolveSend, rejectSend });
      ws.send(JSON.stringify({ id, method, params }));
    });

    ws.on('open', async () => {
      try {
        await send('Network.enable');
        await send('Runtime.enable');
        await send('Runtime.evaluate', {
          expression: `(function() {
            try {
              var target = ${JSON.stringify(probeUrl || '')} || location.href;
              fetch(target, { method: "GET", credentials: "include", cache: "no-store", mode: "cors" }).catch(function() {});
            } catch (e) {}
            return true;
          })()`,
          awaitPromise: true,
          returnByValue: true
        }).catch(() => {});
      } catch {
        finish({});
      }
    });

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); }
      catch { return; }

      if (msg.id && pending.has(msg.id)) {
        const { resolveSend, rejectSend } = pending.get(msg.id);
        pending.delete(msg.id);
        if (msg.error) rejectSend(new Error(msg.error.message || String(msg.error)));
        else resolveSend(msg.result);
        return;
      }

      if (msg.method === 'Network.requestWillBeSent') {
        const params = msg.params || {};
        const req = params.request || {};
        let host = '';
        try { host = new URL(req.url || '').hostname; } catch {}
        if (!hostMatchesDomain(host, domain)) return;
        if (params.requestId) matchingRequestIds.add(params.requestId);
        const auth = extractAuthHeaders(req.headers || {});
        if (Object.keys(auth).length > 0) finish(auth);
        return;
      }

      if (msg.method === 'Network.requestWillBeSentExtraInfo') {
        const params = msg.params || {};
        if (params.requestId && !matchingRequestIds.has(params.requestId)) return;
        const auth = extractAuthHeaders(params.headers || {});
        if (Object.keys(auth).length > 0) finish(auth);
      }
    });

    ws.on('error', () => finish({}));
    ws.on('close', () => finish({}));

    timer = setTimeout(() => finish({}), timeoutMs);
  });
}

function mergeAuthHeadersPreferLive(liveHeaders, fallbackHeaders) {
  const merged = {};
  applyAuthHeaders(merged, liveHeaders, true);
  applyAuthHeaders(merged, fallbackHeaders, false);
  return merged;
}

async function collectExecutionAuthHeaders({ wsUrl, tab, domain, probeUrl }) {
  const [liveHeaders, fallbackHeaders] = await Promise.all([
    captureLiveAuthHeadersFromTab(tab, domain, probeUrl).catch(() => ({})),
    findStoredAuthHeaders(wsUrl, domain).catch(() => ({})),
  ]);
  return {
    liveHeaders,
    fallbackHeaders,
    mergedHeaders: mergeAuthHeadersPreferLive(liveHeaders, fallbackHeaders),
  };
}

function applyExportAuthPolicy(capture, liveHeadersByDomain = {}, includeAuth = false) {
  const headers = { ...(capture.requestHeaders || {}) };
  if (!includeAuth) {
    return {
      ...capture,
      requestHeaders: redactAuthHeaders(headers),
    };
  }

  const live = extractAuthHeaders(liveHeadersByDomain[capture.domain] || {});
  const merged = {};

  for (const [name, value] of Object.entries(headers)) {
    if (!isAuthHeader(name)) {
      merged[name] = String(value);
      continue;
    }

    const liveKey = findHeaderKey(live, name);
    merged[name] = liveKey ? live[liveKey] : REDACTED_HEADER_VALUE;
  }

  for (const [name, value] of Object.entries(live)) {
    if (!findHeaderKey(merged, name)) {
      merged[name] = value;
    }
  }

  return {
    ...capture,
    requestHeaders: merged,
  };
}

function pickTabForDomain(domain) {
  return findTab(domain).catch(async () => {
    const tabs = await (await fetch(`${CDP_URL}/json/list`)).json();
    const pages = tabs.filter(t => t.type === 'page');
    return pages[0] || null;
  });
}

async function collectLiveHeadersByDomainForExport(captures) {
  const liveHeadersByDomain = {};
  const probeByDomain = {};

  for (const cap of captures || []) {
    if (!cap || !cap.domain || probeByDomain[cap.domain]) continue;
    try { probeByDomain[cap.domain] = `${new URL(cap.url).origin}/`; }
    catch {}
  }

  for (const domain of Object.keys(probeByDomain)) {
    try {
      const tab = await pickTabForDomain(domain);
      if (!tab) { liveHeadersByDomain[domain] = {}; continue; }
      liveHeadersByDomain[domain] = await captureLiveAuthHeadersFromTab(tab, domain, probeByDomain[domain]);
    } catch {
      liveHeadersByDomain[domain] = {};
    }
  }

  return liveHeadersByDomain;
}

function dropRedactedAuthHeaders(headers) {
  for (const [name, value] of Object.entries(headers || {})) {
    if (isAuthHeader(name) && isRedactedAuthValue(value)) {
      delete headers[name];
    }
  }
}

// ─── Helpers ────────────────────────────────────────────────────

/** Parse duration strings like "1h", "30m", "2d" into milliseconds */
function parseDuration(str) {
  const m = String(str).match(/^(\d+)\s*(s|m|h|d)$/);
  if (!m) return parseInt(str) || 0;
  const n = parseInt(m[1]);
  const unit = { s: 1000, m: 60000, h: 3600000, d: 86400000 }[m[2]];
  return n * unit;
}

function capitalize(text) {
  const s = String(text || '');
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : s;
}

function normalizeMethod(method) {
  return String(method || '').toUpperCase();
}

function extractSchemaKeys(obj, maxDepth) {
  if (maxDepth <= 0 || obj === null || typeof obj !== 'object') {
    return typeof obj;
  }
  if (Array.isArray(obj)) {
    return obj.length > 0 ? [extractSchemaKeys(obj[0], maxDepth - 1)] : [];
  }
  const result = {};
  for (const [key, value] of Object.entries(obj)) {
    if (value === null) {
      result[key] = 'null';
    } else if (typeof value === 'object') {
      result[key] = extractSchemaKeys(value, maxDepth - 1);
    } else {
      result[key] = typeof value;
    }
  }
  return result;
}

function normalizeSchemaPath(pathname) {
  const source = String(pathname || '/');
  return source.split('/').map((segment) => {
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(segment)) return ':uuid';
    if (/^\d{4,}$/.test(segment)) return ':id';
    if (/^[a-zA-Z0-9_-]{15,30}$/.test(segment) && /[a-z]/.test(segment) && /[A-Z]/.test(segment)) {
      let transitions = 0;
      for (let i = 1; i < segment.length; i++) {
        if (/\d/.test(segment[i - 1]) !== /\d/.test(segment[i])) transitions++;
      }
      if (transitions >= 3) return ':hash';
      if (!/[a-z]{4,}/.test(segment)) return ':hash';
    }
    return segment;
  }).join('/');
}

function parseCaptureObjectBody(raw) {
  if (!raw) return null;
  if (typeof raw === 'string') {
    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null;
    } catch {
      return null;
    }
  }
  if (raw && typeof raw === 'object' && !Array.isArray(raw)) {
    return raw;
  }
  return null;
}

function firstHeaderValue(headers, name) {
  const target = String(name || '').toLowerCase();
  for (const [key, value] of Object.entries(headers || {})) {
    if (key.toLowerCase() === target) return String(value || '');
  }
  return '';
}

function inferSchemaCategory(method, pathText) {
  const m = normalizeMethod(method);
  const p = String(pathText || '').toLowerCase();
  if (m.startsWith('WS_') || m.startsWith('SSE_')) return 'realtime';
  if (p.includes('auth') || p.includes('login') || p.includes('token') || p.includes('oauth') || p.includes('session')) return 'auth';
  if (p.includes('search') || p.includes('query')) return 'search';
  if (p.includes('/log_') || p.includes('/log/') || p.endsWith('/log') || p.includes('track') || p.includes('/event') || p.includes('beacon') || p.includes('metric') || p.includes('telemetry')) return 'telemetry';
  if (m === 'GET' || m === 'HEAD' || m === 'OPTIONS') return 'read';
  if (m === 'POST' || m === 'PUT' || m === 'PATCH' || m === 'DELETE') return 'write';
  return undefined;
}

function buildSchemaFromCaptures(domain, captures) {
  const rows = Array.isArray(captures) ? captures : [];
  const endpoints = new Map();
  const authKeys = [
    'authorization', 'x-csrf-token', 'x-twitter-auth-type',
    'x-requested-with', 'x-github-client-version', 'x-client-transaction-id',
    'x-twitter-active-user', 'x-twitter-client-language', 'x-fetch-nonce',
    'github-verified-fetch', 'x-api-key', 'api-key',
  ];
  let total = 0;

  for (const capture of rows) {
    if (!capture || capture.domain !== domain) continue;
    total++;

    let url;
    try {
      url = new URL(capture.url);
    } catch {
      continue;
    }
    const method = normalizeMethod(capture.method || 'GET');
    const pathText = normalizeSchemaPath(url.pathname);
    const key = `${method} ${pathText}`;

    if (!endpoints.has(key)) {
      endpoints.set(key, {
        method,
        path: pathText,
        queryParams: new Set(),
        statusCodes: {},
        headers: new Set(),
        durations: [],
        count: 0,
        responseType: null,
        bodyKeys: null,
        bodySamples: [],
        responseKeys: null,
        triggers: new Map(),
        sources: {},
      });
    }

    const endpoint = endpoints.get(key);
    endpoint.count++;
    const source = capture.source || 'fetch';
    endpoint.sources[source] = (endpoint.sources[source] || 0) + 1;
    for (const queryKey of url.searchParams.keys()) {
      endpoint.queryParams.add(queryKey);
    }
    const statusCode = capture.responseStatus;
    endpoint.statusCodes[statusCode] = (endpoint.statusCodes[statusCode] || 0) + 1;

    const duration = Number(capture.duration);
    if (Number.isFinite(duration) && duration > 0) {
      endpoint.durations.push(duration);
    }

    if (capture.requestBody && endpoint.bodySamples.length < 5) {
      const bodyObj = parseCaptureObjectBody(capture.requestBody);
      if (bodyObj) {
        if (!endpoint.bodyKeys) endpoint.bodyKeys = extractSchemaKeys(bodyObj, 2);
        const sample = {};
        for (const [field, value] of Object.entries(bodyObj)) {
          if (value === null || value === undefined) sample[field] = null;
          else if (typeof value === 'object') {
            try { sample[field] = JSON.stringify(value); }
            catch { sample[field] = '[unserializable]'; }
          } else sample[field] = String(value);
        }
        endpoint.bodySamples.push(sample);
      }
    }

    if (!endpoint.responseKeys && statusCode >= 200 && statusCode < 300 && capture.responseBody) {
      const responseObj = parseCaptureObjectBody(capture.responseBody);
      if (responseObj) {
        endpoint.responseKeys = extractSchemaKeys(responseObj, 2);
      }
    }

    for (const headerName of Object.keys(capture.requestHeaders || {})) {
      const lower = headerName.toLowerCase();
      if (authKeys.includes(lower) || lower.startsWith('x-csrf') || lower.startsWith('x-api')) {
        endpoint.headers.add(headerName);
      }
    }

    const contentType = firstHeaderValue(capture.responseHeaders || {}, 'content-type').toLowerCase();
    if (contentType.includes('json')) endpoint.responseType = 'json';
    else if (contentType.includes('html')) endpoint.responseType = 'html';
    else if (contentType.includes('text')) endpoint.responseType = 'text';

    if (capture.trigger && capture.trigger.selector) {
      const eventName = capture.trigger.event;
      const selector = capture.trigger.selector;
      const triggerKey = `${eventName} ${selector}`;
      if (!endpoint.triggers.has(triggerKey)) {
        endpoint.triggers.set(triggerKey, {
          event: eventName,
          selector,
          text: capture.trigger.text,
          count: 0,
        });
      }
      endpoint.triggers.get(triggerKey).count++;
    }
  }

  const endpointList = [...endpoints.values()]
    .sort((a, b) => b.count - a.count)
    .map((endpoint) => {
      const avgDuration = endpoint.durations.length
        ? `${Math.round(endpoint.durations.reduce((sum, value) => sum + value, 0) / endpoint.durations.length)}ms`
        : null;

      let bodyFieldVariability;
      if (endpoint.bodySamples.length >= 2) {
        const allFields = new Set();
        for (const sample of endpoint.bodySamples) {
          for (const field of Object.keys(sample)) allFields.add(field);
        }
        const variability = {};
        for (const field of allFields) {
          const values = new Set();
          for (const sample of endpoint.bodySamples) {
            if (sample[field] !== undefined) values.add(sample[field]);
          }
          variability[field] = values.size <= 1 ? 'constant' : 'variable';
        }
        bodyFieldVariability = Object.keys(variability).length ? variability : undefined;
      }

      const sources = Object.keys(endpoint.sources);
      const sourceValue = sources.length === 1 ? sources[0] : endpoint.sources;

      return {
        method: endpoint.method,
        path: endpoint.path,
        callCount: endpoint.count,
        queryParams: [...endpoint.queryParams],
        statusCodes: endpoint.statusCodes,
        avgDuration,
        authHeaders: endpoint.headers.size ? [...endpoint.headers] : undefined,
        responseType: endpoint.responseType,
        source: sourceValue,
        requestBodyStructure: endpoint.bodyKeys || undefined,
        bodyFieldVariability,
        responseBodyStructure: endpoint.responseKeys || undefined,
        triggers: endpoint.triggers.size
          ? [...endpoint.triggers.values()].sort((a, b) => b.count - a.count).slice(0, 5)
          : undefined,
        category: inferSchemaCategory(endpoint.method, endpoint.path),
      };
    });

  return {
    domain,
    generatedAt: new Date().toISOString(),
    totalCaptures: total,
    uniqueEndpoints: endpointList.length,
    endpoints: endpointList,
  };
}

function endpointKey(method, path) {
  return `${normalizeMethod(method)} ${path || ''}`;
}

function splitEndpointKey(key) {
  const i = String(key).indexOf(' ');
  if (i <= 0) return { method: normalizeMethod(key), path: '' };
  return { method: normalizeMethod(key.slice(0, i)), path: key.slice(i + 1) };
}

function escapeForRegex(text) {
  return String(text).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function pathWords(raw) {
  return String(raw || '')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/[-_\s]+/g, ' ')
    .trim()
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean);
}

function cleanGraphqlOperation(rawSegment) {
  const seg = String(rawSegment || '').replace(/[^a-zA-Z0-9]+/g, ' ').trim();
  if (!seg) return '';
  return pathWords(seg).join(' ');
}

function inferVerbFromMethod(method) {
  const m = normalizeMethod(method);
  if (m.startsWith('WS_') || m.startsWith('SSE_')) return 'Stream';
  if (m === 'GET' || m === 'HEAD' || m === 'OPTIONS') return 'Get';
  if (m === 'POST') return 'Create';
  if (m === 'PUT' || m === 'PATCH') return 'Update';
  if (m === 'DELETE') return 'Delete';
  return 'Process';
}

function extractMeaningfulPathWord(path) {
  const segments = String(path || '').split('?')[0].split('/').filter(Boolean);
  const picked = [...segments].reverse().find(s => !s.startsWith(':') && s.length > 2) || segments[segments.length - 1] || 'endpoint';
  const cleaned = cleanGraphqlOperation(picked);
  return (cleaned || 'endpoint').replace(/^v\d+$/, '').trim() || 'endpoint';
}

function extractGraphQLOperation(path) {
  const normalized = String(path || '').split('?')[0];
  const segments = normalized.split('/').filter(Boolean);
  for (const seg of [...segments].reverse()) {
    const cleaned = cleanGraphqlOperation(seg);
    if (!cleaned) continue;
    const parts = cleaned.split(' ');
    if (parts.length > 1 || /[A-Z]/.test(seg) || seg.includes('_') || seg.includes('-')) {
      return cleaned;
    }
  }
  return '';
}

function normalizeLabelText(text) {
  return String(text || '').replace(/\s+/g, ' ').trim();
}

function inferEndpointLabel(endpoint) {
  if (!endpoint || !endpoint.path) return 'Unknown endpoint';
  const gql = extractGraphQLOperation(endpoint.path);
  if (gql) {
    const lower = gql.toLowerCase();
    if (lower.startsWith('delete ') || lower.startsWith('remove ')) {
      return normalizeLabelText(`Delete ${gql.split(' ').slice(1).join(' ')}`);
    }
    return normalizeLabelText(`${inferVerbFromMethod(endpoint.method)} ${gql}`);
  }

  const triggerText = Array.isArray(endpoint.triggers) && endpoint.triggers[0]?.text;
  if (triggerText) {
    const lower = triggerText.toLowerCase();
    if (lower.includes('post')) return `Post ${extractMeaningfulPathWord(endpoint.path)}`;
    if (lower.includes('upload')) return `Upload ${extractMeaningfulPathWord(endpoint.path)}`;
    if (lower.includes('save')) return `Save ${extractMeaningfulPathWord(endpoint.path)}`;
    if (lower.includes('delete') || lower.includes('remove')) return `Delete ${extractMeaningfulPathWord(endpoint.path)}`;
    if (lower.includes('search') || lower.includes('query')) return `Search ${extractMeaningfulPathWord(endpoint.path)}`;
    if (lower.includes('follow') || lower.includes('subscribe')) return `Follow ${extractMeaningfulPathWord(endpoint.path)}`;
    if (lower.includes('like') || lower.includes('favorite') || lower.includes('bookmark')) return `Like ${extractMeaningfulPathWord(endpoint.path)}`;
    if (lower.includes('message') || lower.includes('chat') || lower.includes('reply') || lower.includes('comment')) return `Create ${extractMeaningfulPathWord(endpoint.path)}`;
  }

  const segments = String(endpoint.path).split('?')[0].split('/').filter(Boolean);
  const last = segments[segments.length - 1] || '';
  const cleaned = cleanGraphqlOperation(last);
  if (cleaned) {
    const parts = cleaned.split(' ');
    const verbs = ['create', 'post', 'delete', 'remove', 'update', 'patch', 'send', 'reply', 'comment', 'search', 'query', 'upload', 'attach', 'save', 'follow', 'like', 'bookmark', 'get', 'read', 'fetch'];
    if (verbs.includes(parts[0])) {
      return `${capitalize(parts[0])} ${parts.slice(1).join(' ')}`.trim();
    }
    if (parts.includes('list') || parts.includes('history')) return `Get ${parts.join(' ')}`;
  }

  return `${inferVerbFromMethod(endpoint.method)} ${extractMeaningfulPathWord(endpoint.path)}`.trim();
}

function buildLabelPromptForSchema(schema) {
  const lines = [];
  lines.push(`Domain: ${schema.domain}`);
  lines.push('Goal: label each endpoint as a concise human-readable action phrase.');
  lines.push('Return JSON mapping from endpoint key to label, e.g. {"POST /path": "Create content"}');
  lines.push('Use only plain JSON, no markdown.');
  lines.push('');
  for (const ep of schema.endpoints || []) {
    lines.push(endpointKey(ep.method, ep.path));
    lines.push(`method: ${ep.method}`);
    lines.push(`path: ${ep.path}`);
    lines.push(`category: ${ep.category || 'unknown'}`);
    if (ep.queryParams?.length) lines.push(`query params: ${ep.queryParams.join(', ')}`);
    if (ep.requestBodyStructure) lines.push(`request body: ${JSON.stringify(ep.requestBodyStructure)}`);
    if (ep.responseBodyStructure) lines.push(`response body: ${JSON.stringify(ep.responseBodyStructure)}`);
    if (ep.triggers?.length) {
      const t = ep.triggers[0];
      lines.push(`trigger: ${t.event} ${t.text || t.selector || ''} (${t.count}x)`);
    }
    lines.push(`heuristic label: ${inferEndpointLabel(ep)}`);
    lines.push('');
  }
  return lines.join('\\n');
}

function collectHeuristicLabels(schema) {
  const output = {};
  for (const ep of schema.endpoints || []) {
    output[endpointKey(ep.method, ep.path)] = inferEndpointLabel(ep);
  }
  return output;
}

function coerceLabelInput(raw) {
  if (process.env.NEO_ENDPOINT_LABELS) return JSON.parse(process.env.NEO_ENDPOINT_LABELS);
  if (!raw) return null;
  const trimmed = String(raw).trim();
  if (!trimmed) return null;
  const parsed = JSON.parse(trimmed);
  if (Array.isArray(parsed)) {
    const out = {};
    for (const item of parsed) {
      const key = item.key || (item.method && item.path ? endpointKey(item.method, item.path) : null);
      if (key && typeof item.label === 'string') out[key] = item.label;
    }
    return out;
  }
  if (typeof parsed === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(parsed)) {
      if (typeof v === 'string') {
        out[k] = v;
      } else if (v && typeof v === 'object' && typeof v.label === 'string') {
        const key = v.key || k;
        out[key] = v.label;
      }
    }
    return out;
  }
  return null;
}

function readStdinText() {
  if (process.stdin.isTTY) return Promise.resolve('');
  return new Promise((resolve) => {
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (chunk) => { data += chunk; });
    process.stdin.on('end', () => { resolve(data); });
  });
}

function loadSchemaFile(domain) {
  const file = path.join(SCHEMA_DIR, `${domain}.json`);
  if (!fs.existsSync(file)) return null;
  try {
    return {
      file,
      schema: JSON.parse(fs.readFileSync(file, 'utf8')),
    };
  } catch {
    return null;
  }
}

function saveSchemaFile(domain, schema) {
  const file = path.join(SCHEMA_DIR, `${domain}.json`);
  fs.mkdirSync(SCHEMA_DIR, { recursive: true });
  fs.writeFileSync(file, JSON.stringify(schema, null, 2));
  return file;
}

function setRequestField(target, field, value) {
  if (!field || value === undefined) return;
  if (String(field).startsWith('query.')) {
    setByPath(target.query || (target.query = {}), field.slice(6), value);
    return;
  }
  setByPath(target.body || (target.body = {}), field, value);
}

function normalizeResponseValue(value) {
  if (value === undefined || value === null) return value;
  if (typeof value === 'object' && !Array.isArray(value)) {
    const keys = Object.keys(value);
    if (keys.length === 0) return value;
    if (keys.length === 1) return normalizeResponseValue(value[keys[0]]);
    return value;
  }
  if (Array.isArray(value)) return value.length ? value[0] : value;
  const trimmed = String(value).trim();
  return trimmed.length ? trimmed : value;
}

function mergeEndpointLabels(schema, labelsByKey, fallback = {}) {
  const map = new Map((schema.endpoints || []).map(ep => [endpointKey(ep.method, ep.path), ep]));
  let changed = 0;
  let heuristic = 0;
  for (const ep of (schema.endpoints || [])) {
    const key = endpointKey(ep.method, ep.path);
    const merged = labelsByKey[key] || fallback[key];
    if (!merged) continue;
    ep.label = merged.trim();
    changed++;
    if (!labelsByKey[key]) heuristic++;
  }
  return { changed, heuristic };
}

function normalizeForEndpointMatch(pathText) {
  return String(pathText || '')
    .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, ':uuid')
    .replace(/\/\d{4,}\b/g, '/:id')
    .replace(/\/[0-9a-f]{24,}/gi, '/:hash');
}

function normalizeEndpointForDeps(method, url) {
  try {
    const u = new URL(url);
    return endpointKey(method, normalizeForEndpointMatch(u.pathname));
  } catch {
    return endpointKey(method, String(url));
  }
}

function collectLeafValues(payload) {
  let value = payload;
  if (typeof value === 'string') {
    try {
      value = JSON.parse(value);
    } catch {
      return {};
    }
  }
  if (!value || typeof value !== 'object') return {};

  const out = {};
  function walk(node, prefix) {
    if (node === null || node === undefined) return;
    if (typeof node === 'string') {
      const v = node.trim();
      if (v.length >= 3 && v.length <= 260) out[prefix] = v;
      return;
    }
    if (typeof node === 'number') {
      if (!Number.isFinite(node) || node <= 0) return;
      out[prefix] = String(node);
      return;
    }
    if (Array.isArray(node)) {
      for (let i = 0; i < Math.min(node.length, 3); i++) {
        walk(node[i], prefix ? `${prefix}[${i}]` : String(i));
      }
      return;
    }
    if (typeof node !== 'object') return;
    for (const [k, v] of Object.entries(node)) {
      walk(v, prefix ? `${prefix}.${k}` : k);
    }
  }

  walk(value, '');
  return out;
}

function collectRequestValues(capture) {
  const values = {};
  const bodyVals = collectLeafValues(capture.requestBody);
  for (const [k, v] of Object.entries(bodyVals)) values[k] = v;
  try {
    const u = new URL(capture.url);
    u.searchParams.forEach((value, key) => {
      if (value.length >= 3 && value.length <= 260) values[`query.${key}`] = value;
    });
  } catch {}
  return values;
}

function collectResponseValues(capture) {
  return collectLeafValues(capture.responseBody);
}

function computeDependencyLinks(captures, windowMs = 10000, minConfidence = 2) {
  const rawLinks = [];
  for (let i = 0; i < captures.length; i++) {
    const producer = captures[i];
    if (!producer.respVals || !Object.keys(producer.respVals).length) continue;
    for (let j = i + 1; j < captures.length; j++) {
      const consumer = captures[j];
      if (consumer.timestamp - producer.timestamp > windowMs) break;
      if (!consumer.reqVals || !Object.keys(consumer.reqVals).length) continue;
      if (producer.endpointKey === consumer.endpointKey) continue;
      for (const [rKey, rVal] of Object.entries(producer.respVals)) {
        if (!rVal || rVal.length < 4 || ['true', 'false', 'null', 'undefined', '0', '1', '2'].includes(rVal)) continue;
        for (const [qKey, qVal] of Object.entries(consumer.reqVals)) {
          if (rVal !== qVal) continue;
          rawLinks.push({
            producerEndpoint: producer.endpointKey,
            consumerEndpoint: consumer.endpointKey,
            respField: rKey,
            reqField: qKey,
            count: 1,
          });
        }
      }
    }
  }
  const merged = new Map();
  for (const item of rawLinks) {
    const k = `${item.producerEndpoint}|${item.consumerEndpoint}|${item.respField}|${item.reqField}`;
    const existing = merged.get(k);
    if (existing) existing.count += item.count;
    else merged.set(k, item);
  }
  return [...merged.values()].filter(item => item.count >= minConfidence);
}

function loadWorkflowFiles() {
  if (!fs.existsSync(SCHEMA_DIR)) return [];
  return fs.readdirSync(SCHEMA_DIR)
    .filter(f => f.endsWith(WORKFLOW_FILE_EXT))
    .map(f => path.join(SCHEMA_DIR, f));
}

function loadWorkflowsFromDisk(domain) {
  const files = loadWorkflowFiles();
  if (!files.length) return [];
  const out = [];
  for (const file of files) {
    try {
      const raw = JSON.parse(fs.readFileSync(file, 'utf8'));
      for (const wf of raw.workflows || []) {
        if (!domain || raw.domain === domain) out.push({ file, domain: raw.domain, ...wf });
      }
    } catch {}
  }
  return out;
}

function pathToParts(pathText) {
  return String(pathText || '')
    .replace(/\[(\d+)\]/g, '.$1')
    .split('.')
    .filter(Boolean);
}

function getByPath(obj, pathText) {
  const parts = pathToParts(pathText);
  let current = obj;
  for (const part of parts) {
    if (!current || typeof current !== 'object') return undefined;
    current = current[part];
  }
  return current;
}

function setByPath(obj, pathText, value) {
  const parts = pathToParts(pathText);
  if (!parts.length) return;
  let current = obj;
  for (let i = 0; i < parts.length; i++) {
    const key = parts[i];
    if (i === parts.length - 1) {
      current[key] = value;
      return;
    }
    if (!current[key] || typeof current[key] !== 'object') current[key] = {};
    current = current[key];
  }
}

function buildTemplateFromStructure(structure) {
  if (!structure || typeof structure !== 'object') return {};
  if (Array.isArray(structure)) {
    return Array.isArray(structure[0]) ? [buildTemplateFromStructure(structure[0])] : [];
  }
  const out = {};
  for (const [k, v] of Object.entries(structure)) {
    if (typeof v === 'string') {
      if (v === 'number') out[k] = 0;
      else if (v === 'boolean') out[k] = true;
      else out[k] = '';
    } else if (Array.isArray(v)) {
      out[k] = v.length ? [buildTemplateFromStructure(v[0])] : [];
    } else if (v && typeof v === 'object') {
      out[k] = buildTemplateFromStructure(v);
    } else {
      out[k] = '';
    }
  }
  return out;
}

function parseMaybeJson(value) {
  if (typeof value !== 'string') return value;
  try { return JSON.parse(value); } catch { return value; }
}

function appendQueryParams(rawUrl, queryParams) {
  if (!queryParams || typeof queryParams !== 'object') return rawUrl;
  try {
    const url = new URL(rawUrl);
    for (const [k, v] of Object.entries(queryParams)) {
      if (v === undefined || v === null || v === '') continue;
      url.searchParams.set(k, String(v));
    }
    return url.toString();
  } catch {
    const entries = Object.entries(queryParams).map(([k, v]) => [k, String(v)]);
    if (!entries.length) return rawUrl;
    const suffix = new URLSearchParams(entries).toString();
    return rawUrl.includes('?') ? `${rawUrl}&${suffix}` : `${rawUrl}?${suffix}`;
  }
}

function parseWorkflowParams(args) {
  const map = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--params' && args[i + 1]) {
      const pair = args[i + 1];
      const idx = pair.indexOf('=');
      if (idx > 0) {
        map[pair.slice(0, idx)] = pair.slice(idx + 1);
      }
      i++;
    }
  }
  return map;
}

function pathPatternToRegex(pathText) {
  const escaped = String(pathText || '')
    .split('/')
    .map(seg => seg.startsWith(':') ? '[^/]+' : escapeForRegex(seg))
    .join('/');
  return new RegExp(`^${escaped}(?:/.*)?$`);
}

function discoverWorkflowChains(dependencyLinks, minSteps, maxSteps, minEvidence) {
  const eligibleLinks = dependencyLinks.filter(link => (link.count || 0) >= minEvidence);
  if (!eligibleLinks.length) return [];

  const byFrom = new Map();
  for (const link of eligibleLinks) {
    const arr = byFrom.get(link.producerEndpoint) || [];
    arr.push(link);
    byFrom.set(link.producerEndpoint, arr);
  }

  const consumerPaths = new Set(eligibleLinks.map(link => splitEndpointKey(link.consumerEndpoint).path));
  const starts = eligibleLinks.filter(link => !consumerPaths.has(splitEndpointKey(link.producerEndpoint).path));
  if (!starts.length) return [];

  const chains = [];

  function dfs(endpoint, path, visited) {
    if (path.length >= maxSteps - 1) {
      if (path.length >= minSteps - 1) chains.push([...path]);
      return;
    }

    const nextLinks = byFrom.get(endpoint) || [];
    const candidates = nextLinks.filter(link => !visited.has(link.consumerEndpoint));
    if (!candidates.length) {
      if (path.length >= minSteps - 1) chains.push([...path]);
      return;
    }

    for (const link of candidates) {
      path.push(link);
      visited.add(link.consumerEndpoint);
      dfs(link.consumerEndpoint, path, visited);
      visited.delete(link.consumerEndpoint);
      path.pop();
    }
  }

  for (const link of starts) {
    dfs(link.consumerEndpoint, [link], new Set([link.producerEndpoint, link.consumerEndpoint]));
  }

  const dedup = new Map();
  for (const chain of chains) {
    const key = chain.map(c => `${c.producerEndpoint}→${c.consumerEndpoint}#${c.respField}:${c.reqField}`).join('|');
    dedup.set(key, chain);
  }
  return [...dedup.values()];
}

function toWorkflowNameFromChain(domain, chain) {
  const endpoints = [chain[0].producerEndpoint];
  for (const link of chain) endpoints.push(link.consumerEndpoint);
  const nounParts = endpoints.map((ep) => {
    const split = splitEndpointKey(ep).path.split('/').filter(Boolean);
    return split[split.length - 1] || 'api';
  });
  const base = `${domain}-${nounParts.join('-to-')}`.toLowerCase()
    .replace(/[^a-z0-9]/g, '-')
    .replace(/-+/g, '-')
    .replace(/-$/, '')
    .slice(0, 60);
  const score = chain.reduce((sum, c) => sum + c.count, 0);
  return `${base}-${score}w`;
}

function buildWorkflowFromChains(domain, chains, schema) {
  const endpointMap = new Map((schema.endpoints || []).map(ep => [endpointKey(ep.method, ep.path), ep]));
  const workflows = [];
  for (const chain of chains) {
    const stepKeys = [chain[0].producerEndpoint];
    for (const link of chain) stepKeys.push(link.consumerEndpoint);
    const steps = [];
    for (let i = 0; i < stepKeys.length; i++) {
      const key = stepKeys[i];
      const ep = endpointMap.get(key);
      const parsed = splitEndpointKey(key);
      steps.push({
        endpointKey: key,
        method: parsed.method,
        path: parsed.path,
        label: (ep && ep.label) || inferEndpointLabel(ep || { method: parsed.method, path: parsed.path }),
      });
    }

    const transitions = [];
    for (let i = 0; i < chain.length; i++) {
      const link = chain[i];
      transitions.push({
        from: i,
        to: i + 1,
        fields: [{ sourceField: link.respField, targetField: link.reqField, count: link.count }],
      });
    }

    workflows.push({
      name: toWorkflowNameFromChain(domain, chain),
      domain,
      score: chain.reduce((sum, c) => sum + c.count, 0),
      endpointCount: steps.length,
      uniqueSignature: stepKeys.join(' -> '),
      steps,
      transitions,
    });
  }
  return workflows;
}

function dedupeWorkflows(workflows) {
  const map = new Map();
  for (const wf of workflows) {
    const existing = map.get(wf.uniqueSignature);
    if (!existing || existing.score < wf.score) map.set(wf.uniqueSignature, wf);
  }
  return [...map.values()].sort((a, b) => b.score - a.score);
}

function loadDependencyData(wsUrl, domain) {
  return cdpEval(wsUrl, `
    (async () => {
      const db = await new Promise((resolve, reject) => {
        const r = indexedDB.open("${DB_NAME}");
        r.onsuccess = () => resolve(r.result);
        r.onerror = () => reject(r.error);
      });
      return new Promise((resolve, reject) => {
        const tx = db.transaction("${STORE_NAME}", "readonly");
        const store = tx.objectStore("${STORE_NAME}");
        const idx = store.index("domain");
        const results = [];
        idx.openCursor(IDBKeyRange.only(${JSON.stringify(domain)})).onsuccess = function(e) {
          const c = e.target.result;
          if (!c) { resolve(results); return; }
          const v = c.value;
          results.push({
            method: v.method,
            url: v.url,
            timestamp: v.timestamp,
            requestBody: v.requestBody,
            responseBody: v.responseBody,
          });
          c.continue();
        };
        idx.onerror = () => reject(tx.error);
      });
    })()
  `, 60000).then(text => {
    const rows = JSON.parse(text || '[]');
    return rows.map(r => ({
      ...r,
      endpointKey: normalizeEndpointForDeps(r.method, r.url),
      reqVals: collectRequestValues(r),
      respVals: collectResponseValues(r),
    }));
  });
}

// ─── CLI Parsing ────────────────────────────────────────────────

function parseSnapshotArgs(argv) {
  const options = {
    interactiveOnly: false,
    includeCursorPointer: false,
    json: false,
    diff: false,
    selector: null,
  };
  const unknown = [];

  const args = Array.isArray(argv) ? argv : [];
  for (let i = 0; i < args.length; i++) {
    const current = args[i];
    if (current === '-i') {
      options.interactiveOnly = true;
      continue;
    }
    if (current === '-C') {
      options.includeCursorPointer = true;
      continue;
    }
    if (current === '--json') {
      options.json = true;
      continue;
    }
    if (current === '--diff') {
      options.diff = true;
      continue;
    }
    if (current === '--selector') {
      if (args[i + 1] && !args[i + 1].startsWith('-')) {
        options.selector = args[i + 1];
        i++;
      } else {
        unknown.push(current);
      }
      continue;
    }
    if (current.startsWith('--selector=')) {
      options.selector = current.slice('--selector='.length);
      continue;
    }
    unknown.push(current);
  }

  return { options, unknown };
}

function parseArgs(argv) {
  const positional = [];
  const flags = {};
  let sessionName = DEFAULT_SESSION_NAME;
  for (let i = 0; i < argv.length; i++) {
    const current = argv[i];
    if (current.startsWith('--')) {
      const eqIndex = current.indexOf('=');
      const hasInlineValue = eqIndex > 2;
      const key = hasInlineValue ? current.slice(2, eqIndex) : current.slice(2);
      const inlineValue = hasInlineValue ? current.slice(eqIndex + 1) : null;
      const next = argv[i + 1];
      if (key === 'session') {
        if (hasInlineValue) {
          sessionName = inlineValue || DEFAULT_SESSION_NAME;
        } else if (next && !next.startsWith('--')) {
          sessionName = next;
          i++;
        } else {
          sessionName = DEFAULT_SESSION_NAME;
        }
        continue;
      }
      if (hasInlineValue) {
        flags[key] = inlineValue;
      } else if (next && !next.startsWith('--')) {
        flags[key] = next;
        i++;
      } else {
        flags[key] = true;
      }
    } else {
      positional.push(current);
    }
  }
  return { positional, flags, sessionName };
}

function stripGlobalSessionFlag(argv) {
  const clean = [];
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--session') {
      if (argv[i + 1] && !argv[i + 1].startsWith('--')) i++;
      continue;
    }
    if (arg.startsWith('--session=')) continue;
    clean.push(arg);
  }
  return clean;
}

// ---------------------------------------------------------------------------
// DevToolsActivePort discovery (live attach to user's running Chrome)
// ---------------------------------------------------------------------------

function getDevToolsActivePort() {
  const candidates = [
    path.join(process.env.HOME, 'Library/Application Support/Google/Chrome/DevToolsActivePort'),
    path.join(process.env.HOME, '.config/google-chrome/DevToolsActivePort'),
    path.join(process.env.HOME, '.config/chromium/DevToolsActivePort'),
    path.join(process.env.HOME, '.config/google-chrome-beta/DevToolsActivePort'),
    path.join(process.env.HOME, '.config/google-chrome-unstable/DevToolsActivePort'),
  ];

  for (const filePath of candidates) {
    try {
      if (!fs.existsSync(filePath)) continue;
      const content = fs.readFileSync(filePath, 'utf8').trim();
      const lines = content.split('\n');
      if (lines.length < 2) continue;
      const port = parseInt(lines[0], 10);
      if (!Number.isInteger(port) || port <= 0 || port > 65535) continue;
      return { port, wsPath: lines[1], filePath };
    } catch {
      continue;
    }
  }
  return null;
}

async function connectToCdpPort(port, sessionName = DEFAULT_SESSION_NAME) {
  const cdpUrl = `http://localhost:${port}`;
  const versionInfo = await fetchJsonOrThrow(`${cdpUrl}/json/version`);

  // Try standard Chrome /json/list first; fall back to Lightpanda-style
  // browser-level WebSocket with Target.createTarget
  let tabId = '';
  let page = {};
  let pageWsUrl = '';

  try {
    const targets = await fetchJsonOrThrow(`${cdpUrl}/json/list`);
    const found = targets.find(target => target.type === 'page');
    if (found) {
      tabId = found.id || found.targetId || '';
      page = found;
      pageWsUrl = found.webSocketDebuggerUrl || '';
    }
  } catch {
    // /json/list not available (e.g. Lightpanda) — use browser WS endpoint
  }

  if (!pageWsUrl && versionInfo.webSocketDebuggerUrl) {
    // Lightpanda / minimal CDP: create a target via the browser-level WS
    const browserWsUrl = versionInfo.webSocketDebuggerUrl;
    const created = await cdpSend(browserWsUrl, 'Target.createTarget', { url: 'about:blank' });
    const targetId = created && created.targetId;
    if (targetId) {
      // Lightpanda needs attachToTarget to activate the session
      try {
        await cdpSend(browserWsUrl, 'Target.attachToTarget', { targetId, flatten: true });
      } catch {
        // Some CDP servers don't require explicit attach
      }
      tabId = targetId;
      pageWsUrl = browserWsUrl; // Lightpanda multiplexes on the same WS
      page = { id: targetId, title: '', url: 'about:blank', type: 'page', webSocketDebuggerUrl: browserWsUrl };
    } else {
      throw new Error(`Connected to ${cdpUrl} but failed to create a page target`);
    }
  }

  if (!pageWsUrl) {
    throw new Error(`Connected to ${cdpUrl} but no page target found`);
  }

  setSession(sessionName, {
    cdpUrl,
    pageWsUrl,
    tabId,
    refs: {},
  });

  return {
    cdpUrl,
    versionInfo,
    tabId,
    page,
  };
}

function parseTabTargets(targets) {
  const source = Array.isArray(targets) ? targets : [];
  return source.map((target, index) => {
    const item = target && typeof target === 'object' ? target : {};
    const id = item.id || item.targetId || '';
    return {
      index,
      type: String(item.type || 'unknown'),
      id: String(id || ''),
      title: String(item.title || ''),
      url: String(item.url || ''),
      webSocketDebuggerUrl: String(item.webSocketDebuggerUrl || ''),
    };
  });
}

function getSessionCdpUrl(sessionName = DEFAULT_SESSION_NAME) {
  const session = getSession(sessionName);
  if (session && session.cdpUrl) return session.cdpUrl;
  return CDP_URL;
}

function updateSessionTab(sessionName, cdpUrl, target) {
  const existing = getSession(sessionName) || {};
  setSession(sessionName, {
    ...existing,
    cdpUrl,
    pageWsUrl: target.webSocketDebuggerUrl,
    tabId: target.id,
    refs: {},
  });
}

async function listSessionTargets(sessionName = DEFAULT_SESSION_NAME) {
  const cdpUrl = getSessionCdpUrl(sessionName);
  let targets;
  try {
    targets = parseTabTargets(await fetchJsonOrThrow(`${cdpUrl}/json/list`));
  } catch {
    // Lightpanda: no /json/list — synthesize from current session
    const session = getSession(sessionName);
    targets = session && session.tabId
      ? [{ index: 0, type: 'page', id: session.tabId, title: '', url: '', webSocketDebuggerUrl: session.pageWsUrl || '' }]
      : [];
  }
  return { cdpUrl, targets };
}

function findTabTargetByUrlPattern(targets, pattern) {
  const list = Array.isArray(targets) ? targets : [];
  const input = String(pattern || '');
  if (!input) return null;
  return list.find(target => target.url.includes(input)) || null;
}

function activateSessionTarget(sessionName, cdpUrl, target) {
  if (!target || !target.webSocketDebuggerUrl) {
    throw new Error(`Target ${target && (target.id || target.index)} does not expose webSocketDebuggerUrl`);
  }
  updateSessionTab(sessionName, cdpUrl, target);
  return target;
}

async function switchSessionTabByUrl(sessionName, pattern) {
  const { cdpUrl, targets } = await listSessionTargets(sessionName);
  const selected = findTabTargetByUrlPattern(targets, pattern);
  if (!selected) {
    throw new Error(`No tab matching URL pattern: ${pattern}`);
  }
  return activateSessionTarget(sessionName, cdpUrl, selected);
}

function loadInjectScriptSource(deps = {}) {
  const existsSyncFn = typeof deps.existsSync === 'function' ? deps.existsSync : fs.existsSync;
  const readFileSyncFn = typeof deps.readFileSync === 'function' ? deps.readFileSync : fs.readFileSync;
  const rootDir = deps.rootDir || path.join(fs.realpathSync(__dirname), '..');
  const primary = path.join(rootDir, 'extension', 'inject.js');
  const fallback = path.join(rootDir, 'extension-dist', 'content.js');

  if (existsSyncFn(primary)) {
    return { sourcePath: primary, source: readFileSyncFn(primary, 'utf8') };
  }
  if (existsSyncFn(fallback)) {
    return { sourcePath: fallback, source: readFileSyncFn(fallback, 'utf8') };
  }
  throw new Error(`Inject script not found. Tried: ${primary} and ${fallback}`);
}

function buildInjectScript(sourceCode) {
  const raw = String(sourceCode || '');
  return `(function() {
  try {
    if (!Array.isArray(globalThis.__NEO_CAPTURES__)) {
      globalThis.__NEO_CAPTURES__ = [];
    }
    if (!globalThis.__NEO_CAPTURE_MESSAGE_LISTENER__) {
      globalThis.__NEO_CAPTURE_MESSAGE_LISTENER__ = function(event) {
        try {
          var data = event && event.data;
          if (!data || data.type !== '__neo_capture_request' || !data.payload) return;
          globalThis.__NEO_CAPTURES__.push(data.payload);
          if (globalThis.__NEO_CAPTURES__.length > 500) {
            globalThis.__NEO_CAPTURES__.shift();
          }
        } catch {}
      };
      window.addEventListener('message', globalThis.__NEO_CAPTURE_MESSAGE_LISTENER__);
    }
    if (typeof globalThis.__neoMiniReporter !== 'function') {
      globalThis.__neoMiniReporter = function(event, payload) {
        try {
          console.debug('[neo:inject]', event, payload || {});
        } catch {}
      };
    }
    globalThis.__neoMiniReporter('inject:start', { captures: globalThis.__NEO_CAPTURES__.length });
    (function() {
${raw}
    }).call(globalThis);
    globalThis.__neoMiniReporter('inject:ready', { captures: globalThis.__NEO_CAPTURES__.length });
    return { ok: true, captures: globalThis.__NEO_CAPTURES__.length };
  } catch (error) {
    return {
      ok: false,
      error: String(error && error.message ? error.message : error),
    };
  }
})();`;
}

async function waitForCaptureDaemonState(pid, stateFile = CDP_CAPTURE_STATE_FILE, timeoutMs = 4000, intervalMs = 100) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    const state = readCaptureDaemonState(stateFile);
    if (state && Number(state.pid) === Number(pid) && isProcessAlive(state.pid)) {
      return state;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  return null;
}

async function startCdpCaptureDaemon(target, options = {}) {
  const stateFile = options.stateFile || CDP_CAPTURE_STATE_FILE;
  const captureFile = options.captureFile || LOCAL_CAPTURE_FILE;
  const existing = readCaptureDaemonState(stateFile);
  if (existing && isProcessAlive(existing.pid)) {
    throw new Error(`CDP capture daemon already running (pid ${existing.pid})`);
  }
  if (existing && !isProcessAlive(existing.pid)) {
    try { fs.unlinkSync(stateFile); } catch {}
  }

  const child = spawn(process.execPath, [
    __filename,
    '_capture-daemon',
    '--ws-url', String(target && target.webSocketDebuggerUrl || ''),
    '--tab-id', String(target && (target.id || target.targetId || '') || ''),
    '--tab-url', String(target && target.url || ''),
    '--state-file', stateFile,
    '--capture-file', captureFile,
  ], {
    detached: true,
    stdio: 'ignore',
  });
  child.unref();

  const state = await waitForCaptureDaemonState(child.pid, stateFile);
  if (!state) {
    throw new Error('Failed to start CDP capture daemon');
  }
  return state;
}

async function stopCdpCaptureDaemon(stateFile = CDP_CAPTURE_STATE_FILE) {
  const state = readCaptureDaemonState(stateFile);
  if (!state) {
    return { stopped: false, message: 'CDP capture daemon not running' };
  }
  if (!isProcessAlive(state.pid)) {
    try { fs.unlinkSync(stateFile); } catch {}
    return { stopped: false, message: 'Removed stale CDP capture daemon state' };
  }
  process.kill(Number(state.pid), 'SIGTERM');
  const deadline = Date.now() + 4000;
  while (Date.now() <= deadline) {
    if (!isProcessAlive(state.pid)) {
      try { if (fs.existsSync(stateFile)) fs.unlinkSync(stateFile); } catch {}
      return { stopped: true, message: `Stopped CDP capture daemon (pid ${state.pid})` };
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  return { stopped: false, message: `Failed to stop CDP capture daemon (pid ${state.pid})` };
}

async function runCaptureDaemon(args) {
  const { flags } = parseArgs(args || []);
  const wsUrl = String(flags['ws-url'] || '').trim();
  const tabId = String(flags['tab-id'] || '').trim();
  const tabUrl = String(flags['tab-url'] || '').trim();
  const stateFile = String(flags['state-file'] || CDP_CAPTURE_STATE_FILE);
  const captureFile = String(flags['capture-file'] || LOCAL_CAPTURE_FILE);
  if (!wsUrl) {
    throw new Error('Missing --ws-url for _capture-daemon');
  }

  ensureParentDirectory(captureFile);
  const pending = new Map();
  const requests = new Map();
  let nextId = 1;
  let shuttingDown = false;
  let ready = false;
  let ws = null;
  const WebSocketCtor = getWebSocketCtor();

  function writeState(extra = {}) {
    writeJsonFile(stateFile, {
      pid: process.pid,
      wsUrl,
      tabId,
      tabUrl,
      startedAt: Date.now(),
      ...extra,
    });
  }

  function send(method, params = {}, timeout = 10000) {
    return new Promise((resolve, reject) => {
      if (!ws || ws.readyState !== WebSocketCtor.OPEN) {
        reject(new Error('CDP capture daemon socket is not open'));
        return;
      }
      const id = nextId++;
      const timer = setTimeout(() => {
        pending.delete(id);
        reject(new Error(`CDP timeout: ${method}`));
      }, timeout);
      pending.set(id, {
        resolve: (result) => {
          clearTimeout(timer);
          resolve(result);
        },
        reject: (error) => {
          clearTimeout(timer);
          reject(error);
        },
      });
      ws.send(JSON.stringify({ id, method, params }));
    });
  }

  async function finalizeRequest(requestId, extra = {}) {
    const entry = requests.get(requestId);
    if (!entry) return;
    requests.delete(requestId);
    entry.response = { ...(entry.response || {}), ...(extra.response || {}) };
    if (extra.finishedAt) entry.finishedAt = extra.finishedAt;
    let responseBody = extra.responseBody;
    if (responseBody === undefined && entry.response && entry.response.status !== 0) {
      try {
        const bodyResult = await send('Network.getResponseBody', { requestId }, 15000);
        responseBody = normalizeCdpResponseBody(bodyResult && bodyResult.body, entry.response.mimeType, !!(bodyResult && bodyResult.base64Encoded));
      } catch {
        responseBody = undefined;
      }
    }
    const record = buildCdpCaptureRecord(entry, responseBody);
    appendLocalCaptureRecord(record, captureFile);
  }

  async function shutdown() {
    if (shuttingDown) return;
    shuttingDown = true;
    try {
      if (ws && ws.readyState === WebSocketCtor.OPEN) {
        try { await send('Network.disable', {}, 3000); } catch {}
        try { ws.close(); } catch {}
      }
    } finally {
      try {
        const current = readCaptureDaemonState(stateFile);
        if (current && Number(current.pid) === process.pid) fs.unlinkSync(stateFile);
      } catch {}
      process.exit(0);
    }
  }

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  await new Promise((resolve, reject) => {
    ws = new WebSocketCtor(wsUrl);

    ws.on('open', async () => {
      try {
        await send('Network.enable', { maxPostDataSize: 1024 * 1024 }, 10000);
        writeState({ ready: true });
        ready = true;
        resolve();
      } catch (error) {
        reject(error);
      }
    });

    ws.on('message', (raw) => {
      let message;
      try {
        message = JSON.parse(raw.toString());
      } catch {
        return;
      }

      if (message.id) {
        const waiter = pending.get(message.id);
        if (!waiter) return;
        pending.delete(message.id);
        if (message.error) waiter.reject(new Error(message.error.message || 'Unknown CDP error'));
        else waiter.resolve(message.result);
        return;
      }

      if (!message.method) return;
      if (message.method === 'Network.requestWillBeSent') {
        const params = message.params || {};
        const request = params.request || {};
        if (!shouldCaptureCdpRequest(request.url, params.type, request.method, request.headers || {})) return;
        requests.set(String(params.requestId), {
          id: crypto.randomUUID(),
          requestId: String(params.requestId),
          resourceType: params.type || 'Fetch',
          timestamp: Number(params.wallTime) ? Math.round(Number(params.wallTime) * 1000) : Date.now(),
          monotonicStartedAt: Number(params.timestamp) || 0,
          tabId: tabId || -1,
          tabUrl: params.documentURL || tabUrl,
          request: {
            url: String(request.url || ''),
            method: String(request.method || 'GET').toUpperCase(),
            headers: request.headers && typeof request.headers === 'object' ? { ...request.headers } : {},
            postData: request.postData,
            documentURL: params.documentURL || tabUrl,
          },
          response: {
            status: 0,
            headers: {},
            mimeType: '',
          },
        });
        return;
      }

      if (message.method === 'Network.responseReceived') {
        const params = message.params || {};
        const entry = requests.get(String(params.requestId));
        if (!entry) return;
        const response = params.response || {};
        entry.response = {
          status: Number(response.status) || 0,
          headers: response.headers && typeof response.headers === 'object' ? { ...response.headers } : {},
          mimeType: String(response.mimeType || ''),
        };
        return;
      }

      if (message.method === 'Network.loadingFinished') {
        const params = message.params || {};
        const entry = requests.get(String(params.requestId));
        if (!entry) return;
        const finishedAt = entry.monotonicStartedAt && Number(params.timestamp)
          ? entry.timestamp + Math.max(0, Math.round((Number(params.timestamp) - entry.monotonicStartedAt) * 1000))
          : Date.now();
        finalizeRequest(String(params.requestId), { finishedAt }).catch(() => {});
        return;
      }

      if (message.method === 'Network.loadingFailed') {
        const params = message.params || {};
        const entry = requests.get(String(params.requestId));
        if (!entry) return;
        const finishedAt = entry.monotonicStartedAt && Number(params.timestamp)
          ? entry.timestamp + Math.max(0, Math.round((Number(params.timestamp) - entry.monotonicStartedAt) * 1000))
          : Date.now();
        finalizeRequest(String(params.requestId), {
          finishedAt,
          response: { status: Number(entry.response && entry.response.status) || 0 },
          responseBody: params.errorText ? truncateCaptureText(String(params.errorText)) : undefined,
        }).catch(() => {});
      }
    });

    ws.on('close', () => {
      if (shuttingDown) return;
      if (!ready) {
        reject(new Error('CDP capture daemon socket closed'));
        return;
      }
      try {
        const current = readCaptureDaemonState(stateFile);
        if (current && Number(current.pid) === process.pid) fs.unlinkSync(stateFile);
      } catch {}
      process.exit(1);
    });

    ws.on('error', (error) => {
      if (!ready) {
        reject(error);
        return;
      }
      try {
        const current = readCaptureDaemonState(stateFile);
        if (current && Number(current.pid) === process.pid) fs.unlinkSync(stateFile);
      } catch {}
      process.exit(1);
    });
  });

  await new Promise(() => {});
}

// ─── Commands ───────────────────────────────────────────────────

const commands = {};

// neo version
commands.version = function() {
  const pkg = JSON.parse(fs.readFileSync(path.join(fs.realpathSync(__dirname), '..', 'package.json'), 'utf8'));
  console.log(`neo v${pkg.version}`);
};

commands['_capture-daemon'] = async function(args) {
  await runCaptureDaemon(args || []);
};

// neo profile <action>
commands.profile = function(args) {
  const { positional } = parseArgs(args || []);
  const action = positional[0];

  switch (action) {
    case 'list': {
      const profiles = scanChromeProfiles();
      if (!profiles.length) {
        console.log('No system Chrome profiles found.');
        return;
      }
      const width = Math.max(...profiles.map(profile => profile.name.length), 7);
      for (const profile of profiles) {
        const email = profile.email || '(no account)';
        console.log(`  ${profile.name.padEnd(width)}  ${email} (${profile.browserName})`);
      }
      return;
    }

    case 'use': {
      const targetName = positional[1];
      if (!targetName) {
        console.error('Usage: neo profile use <name>');
        process.exit(1);
      }
      const profiles = scanChromeProfiles();
      const resolved = resolveChromeProfileSelection(targetName, profiles);
      if (resolved.error === 'not-found') {
        console.error(`Profile not found: ${targetName}`);
        process.exit(1);
      }
      if (resolved.error === 'ambiguous') {
        console.error(`Profile name is ambiguous: ${targetName}`);
        for (const match of resolved.matches) {
          console.error(`  - ${match.name} (${match.browserName})`);
        }
        process.exit(1);
      }

      const currentConfig = loadNeoConfig();
      const nextConfig = {
        ...currentConfig,
        defaultProfile: resolved.profile.name,
        userDataDir: resolved.profile.userDataDir,
      };
      saveNeoConfig(nextConfig);

      console.log(`Using Chrome profile: ${resolved.profile.name} (${resolved.profile.browserName})`);
      console.log(`  Email: ${resolved.profile.email || '(no account)'}`);
      console.log(`  user-data-dir: ${resolved.profile.userDataDir}`);

      const running = listChromeProcesses().find((proc) => proc && (proc.remoteDebuggingPort > 0 || proc.userDataDir || proc.profileDirectory));
      if (running) {
        const configuredProfile = resolved.profile.name;
        const sameUserDataDir = !running.userDataDir || running.userDataDir === resolved.profile.userDataDir;
        const sameProfileDir = !running.profileDirectory || running.profileDirectory === configuredProfile;
        if (!sameUserDataDir || !sameProfileDir) {
          console.warn('Warning: a running Chrome process appears to use a different profile.');
          console.warn(`  Running: ${running.profileDirectory || '(unknown profile)'} @ ${running.userDataDir || '(default user-data-dir)'}`);
        }
      }
      return;
    }

    default:
      console.log(`neo profile — System Chrome profile management

  neo profile list                       List system Chrome/Chromium profiles
  neo profile use <name>                 Set the default Chrome profile in ~/.neo/config.json`);
  }
};

// neo profiles — list configured profiles
commands.profiles = function() {
  const profilesDir = path.join(NEO_BASE_DIR, 'profiles');
  const profiles = [];
  if (fs.existsSync(profilesDir)) {
    for (const name of fs.readdirSync(profilesDir)) {
      const configPath = path.join(profilesDir, name, 'config.json');
      if (fs.existsSync(configPath)) {
        profiles.push(name);
      }
    }
  }
  const defaultExists = fs.existsSync(NEO_CONFIG_FILE);
  if (defaultExists) console.log(`  default  ${NEO_HOME_DIR}`);
  for (const name of profiles) {
    console.log(`  ${name.padEnd(9)} ${path.join(profilesDir, name)}`);
  }
  if (!defaultExists && profiles.length === 0) {
    console.log('No profiles configured. Run neo setup to create default profile.');
  }
};

// neo setup [--profile <name>]
commands.setup = async function(args) {
  const { positional, flags } = parseArgs(args || []);
  const profileName = typeof flags.profile === 'string' ? flags.profile : null;
  if (positional.length > 0 || (Object.keys(flags).length > 0 && !('profile' in flags))) {
    console.error('Usage: neo setup [--profile <name>]');
    process.exit(1);
  }

  // If profile specified, override home dir for this run
  const homeDir = profileName ? getNeoHomeDir(profileName) : NEO_HOME_DIR;
  const configFile = path.join(homeDir, 'config.json');
  const extensionDir = path.join(homeDir, 'extension');
  const setupSchemaDir = path.join(homeDir, 'schemas');

  const chromePath = detectChromeBinaryPath();
    // Detect Lightpanda as an alternative browser
  const lightpandaPath = detectLightpandaBinaryPath();
  if (!chromePath && !lightpandaPath) {
    throw new Error('No supported browser found. Tried: google-chrome-stable, google-chrome, chromium-browser, chromium, lightpanda');
  }

  const projectRoot = path.join(fs.realpathSync(__dirname), '..');
  const extensionSourceDir = path.join(projectRoot, 'extension-dist');

  fs.mkdirSync(homeDir, { recursive: true });
  copyDirectoryRecursive(extensionSourceDir, extensionDir);
  fs.mkdirSync(setupSchemaDir, { recursive: true });

  // Detect user-data-dir from running Chrome process
  let detectedUserDataDir = null;
  if (chromePath) {
    try {
      const { execSync: es } = require('child_process');
      const psOut = es('ps aux', { encoding: 'utf8', timeout: 3000 });
      for (const line of psOut.split('\n')) {
        if (/chrome.*--remote-debugging-port/.test(line)) {
          const m = line.match(/--user-data-dir=(\S+)/);
          if (m) { detectedUserDataDir = m[1]; break; }
        }
      }
    } catch {}
  }

  const existingConfig = loadNeoConfig(profileName);
  const config = {
    ...existingConfig,
    chromePath: chromePath || existingConfig.chromePath || lightpandaPath,
    browserType: chromePath ? 'chrome' : (existingConfig.browserType || 'lightpanda'),
    cdpPort: existingConfig.cdpPort || 9222,
  };
  if (detectedUserDataDir) config.userDataDir = detectedUserDataDir;
  if (profileName) config.profile = profileName;

  if (lightpandaPath) {
    console.log(`Lightpanda detected: ${lightpandaPath}`);
    if (chromePath) {
      console.log(`Using Chrome as default (set browserType: "lightpanda" in config to switch)`);
    } else {
      console.log(`No Chrome found — using Lightpanda as browser backend`);
    }
  }

  saveNeoConfig(config, profileName);

  // Pre-register Neo extension in Chrome Preferences
  // Use detected user-data-dir if available, otherwise fall back to neo's own profile
  const chromeProfileDir = detectedUserDataDir || path.join(homeDir, 'chrome-profile');
  const profileDefaultDir = path.join(chromeProfileDir, 'Default');
  const prefsFile = path.join(profileDefaultDir, 'Preferences');
  fs.mkdirSync(profileDefaultDir, { recursive: true });
  let prefs = {};
  try {
    if (fs.existsSync(prefsFile)) {
      prefs = JSON.parse(fs.readFileSync(prefsFile, 'utf8'));
    }
  } catch {}

  // Enable developer mode
  if (!prefs.extensions) prefs.extensions = {};
  if (!prefs.extensions.ui) prefs.extensions.ui = {};
  prefs.extensions.ui.developer_mode = true;

  // Generate deterministic extension ID from path (same algorithm Chrome uses for unpacked)
  const extRealPath = fs.realpathSync(extensionDir);
  const hashHex = crypto.createHash('sha256').update(extRealPath).digest('hex').slice(0, 32);
  const extensionId = [...hashHex].map(c => String.fromCharCode('a'.charCodeAt(0) + parseInt(c, 16))).join('');

  // Read extension manifest for permissions
  const manifest = JSON.parse(fs.readFileSync(path.join(extensionDir, 'manifest.json'), 'utf8'));
  const permissions = manifest.permissions || [];

  // Register as unpacked extension (location: 4)
  if (!prefs.extensions.settings) prefs.extensions.settings = {};
  prefs.extensions.settings[extensionId] = {
    account_extension_type: 0,
    active_permissions: {
      api: permissions,
      explicit_host: ['<all_urls>'],
      manifest_permissions: [],
      scriptable_host: ['<all_urls>'],
    },
    content_settings: [],
    creation_flags: 38,
    first_install_time: String(Date.now() * 10000 + 116444736000000000),
    from_webstore: false,
    granted_permissions: {
      api: permissions,
      explicit_host: ['<all_urls>'],
      manifest_permissions: [],
      scriptable_host: ['<all_urls>'],
    },
    incognito_content_settings: [],
    incognito_preferences: {},
    location: 4,
    newAllowFileAccess: true,
    path: extRealPath,
    preferences: {},
    regular_only_preferences: {},
    state: 1,
    was_installed_by_default: false,
    was_installed_by_oem: false,
    withholding_permissions: false,
  };

  fs.writeFileSync(prefsFile, JSON.stringify(prefs, null, 2) + '\n', 'utf8');

  console.log('Neo setup complete');
  if (profileName) console.log(`  Profile: ${profileName}`);
  console.log(`  Chrome binary: ${chromePath}`);
  if (detectedUserDataDir) console.log(`  Chrome user-data-dir: ${detectedUserDataDir} (detected from running Chrome)`);
  console.log(`  Extension dir: ${extensionDir}`);
  console.log(`  Extension ID: ${extensionId}`);
  console.log(`  Config file: ${configFile}`);
  console.log(`  Schema dir: ${setupSchemaDir}`);
  console.log('');
  if (detectedUserDataDir) {
    console.log('Extension registered in your Chrome profile. Restart Chrome to activate:');
    console.log('  neo start --force');
  } else {
    console.log('Launch Chrome with: neo start');
  }
};

// neo start [--profile <name>] [--force]
commands.start = async function(args) {
  const { positional, flags } = parseArgs(args || []);
  const profileName = typeof flags.profile === 'string' ? flags.profile : null;
  const force = 'force' in flags;

  const homeDir = profileName ? getNeoHomeDir(profileName) : NEO_HOME_DIR;
  const configFile = path.join(homeDir, 'config.json');
  const extensionDir = path.join(homeDir, 'extension');

  if (!fs.existsSync(configFile) && !(profileName && fs.existsSync(NEO_ROOT_CONFIG_FILE))) {
    throw new Error(`Missing config: ${configFile}. Run neo setup${profileName ? ' --profile ' + profileName : ''} first`);
  }

  const config = loadNeoConfig(profileName);

  const browserType = String(config && config.browserType || 'chrome').trim();
  const browserPath = String(config && config.chromePath || '').trim();
  const cdpPort = parseInt(String(config && config.cdpPort !== undefined ? config.cdpPort : 9222), 10);
  const configuredUserDataDir = String(config && config.userDataDir || '').trim();
  const configuredProfileName = String(config && config.defaultProfile || '').trim();
  const useLightpanda = browserType === 'lightpanda' || isLightpandaBrowser(browserPath);

  if (!browserPath) {
    throw new Error(`Missing chromePath in ${configFile}. Run neo setup again`);
  }
  if (!Number.isInteger(cdpPort) || cdpPort <= 0 || cdpPort > 65535) {
    throw new Error(`Invalid cdpPort in ${configFile}: ${config && config.cdpPort}`);
  }
  if (browserPath.includes('/') && !fs.existsSync(browserPath)) {
    throw new Error(`Browser binary does not exist: ${browserPath}`);
  }

  // Check if browser is already running with CDP on the target port
  if (!force) {
    try {
      const resp = await fetch(`http://localhost:${cdpPort}/json/version`);
      if (resp.ok) {
        const info = await resp.json();
        const label = useLightpanda ? 'Lightpanda' : 'Chrome';
        console.log(`${label} already running with CDP on port ${cdpPort} (${info.Browser || 'Lightpanda'})`);
        console.log(`CDP endpoint: http://localhost:${cdpPort}`);
        console.log('Use --force to launch a new instance anyway.');
        return;
      }
    } catch {
      // CDP not reachable, proceed to launch
    }
  }

  let child;
  if (useLightpanda) {
    // Lightpanda: headless browser — no user-data-dir, no extensions
    const lpArgs = ['serve', '--host', '127.0.0.1', '--port', String(cdpPort)];
    child = spawn(browserPath, lpArgs, { detached: true, stdio: 'ignore' });
  } else {
    // Chrome: standard launch with extension and user-data-dir
    const userDataDir = configuredUserDataDir || path.join(homeDir, 'chrome-profile');
    fs.mkdirSync(userDataDir, { recursive: true });
    const chromeArgs = [
      `--remote-debugging-port=${cdpPort}`,
      `--user-data-dir=${userDataDir}`,
      '--no-first-run',
      '--no-default-browser-check',
    ];
    if (configuredProfileName) {
      chromeArgs.push(`--profile-directory=${configuredProfileName}`);
    }
    child = spawn(browserPath, chromeArgs, { detached: true, stdio: 'ignore' });
  }
  child.unref();

  const ready = await waitForCdpPort(cdpPort, useLightpanda ? 3000 : 5000, 250);
  if (!ready) {
    const label = useLightpanda ? 'Lightpanda' : 'Chrome';
    throw new Error(`Failed to start ${label}: CDP endpoint http://localhost:${cdpPort} did not respond in time`);
  }

  const label = useLightpanda ? 'Lightpanda' : 'Chrome';
  console.log(`${label} started on port ${cdpPort}`);
  console.log(`CDP endpoint: http://localhost:${cdpPort}`);
  if (!useLightpanda && configuredProfileName) {
    console.log(`Profile: ${configuredProfileName}`);
  }
};

// neo launch <app> [--port N]
commands.launch = async function(args) {
  const { positional, flags } = parseArgs(args || []);
  const rawApp = positional[0];
  const rawPort = flags.port === undefined ? '9222' : String(flags.port);
  const port = parseInt(rawPort, 10);
  if (!rawApp || positional.length > 1 || flags.port === true || !Number.isInteger(port) || port <= 0 || port > 65535) {
    console.error('Usage: neo launch <app> [--port N]');
    process.exit(1);
  }

  const resolved = resolveElectronExecutable(rawApp);
  if (resolved.error === 'unknown-app') {
    console.error(`Unknown app: ${rawApp}`);
    console.error(`Supported apps: ${Object.keys(ELECTRON_APPS).join(', ')}`);
    process.exit(1);
  }
  if (resolved.error === 'unsupported-on-linux') {
    console.error(`${normalizeElectronAppName(rawApp)}: no native Linux client`);
    process.exit(1);
  }
  if (!resolved.executable) {
    console.error(`Cannot find executable for ${normalizeElectronAppName(rawApp)}.`);
    console.error(`Tried: ${resolved.candidates.join(', ')}`);
    process.exit(1);
  }

  const spawnArgs = [`--remote-debugging-port=${port}`];
  const child = spawn(resolved.executable, spawnArgs, {
    detached: true,
    stdio: 'ignore',
  });
  child.unref();

  const ready = await waitForCdpPort(port, 10000, 250);
  if (!ready) {
    throw new Error(`Launched ${normalizeElectronAppName(rawApp)} but CDP endpoint on port ${port} was not ready within 10s`);
  }

  console.log(`Launched ${normalizeElectronAppName(rawApp)} on port ${port}`);
};

// neo connect [port]
commands.connect = async function(args, context = {}) {
  const { positional, flags } = parseArgs(args || []);
  const electronApp = flags.electron ? String(flags.electron) : null;
  let rawPort = positional[0];
  if (electronApp) {
    if (flags.electron === true || positional.length > 0) {
      console.error('Usage: neo connect [port] | neo connect --electron <app-name>');
      process.exit(1);
    }
    const discoveredPort = findElectronDebugPort(electronApp);
    if (!discoveredPort) {
      throw new Error(`No running Electron app "${electronApp}" with --remote-debugging-port found. Run: neo launch ${normalizeElectronAppName(electronApp)}`);
    }
    rawPort = String(discoveredPort);
  }

  const port = rawPort ? parseInt(rawPort, 10) : 9222;
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    console.error(`Invalid port: ${rawPort}`);
    console.error('Usage: neo connect [port] | neo connect --electron <app-name>');
    process.exit(1);
  }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const connected = await connectToCdpPort(port, sessionName);

  console.log(`Connected: ${connected.versionInfo.Browser || 'CDP'} @ ${connected.cdpUrl}`);
  console.log(`Session: ${sessionName}`);
  console.log(`Tab: ${connected.tabId || '(no-id)'} ${connected.page.title ? `- ${connected.page.title}` : ''}`);
};

// neo attach — connect to user's running Chrome via DevToolsActivePort
commands.attach = async function(args, context = {}) {
  const sessionName = (context && context.sessionName) || DEFAULT_SESSION_NAME;

  // 1. Try DevToolsActivePort file
  const activePort = getDevToolsActivePort();
  let port = null;

  if (activePort) {
    // Verify the port is actually reachable
    try {
      await fetchJsonOrThrow(`http://localhost:${activePort.port}/json/version`, 2000);
      port = activePort.port;
      console.log(`Found Chrome via DevToolsActivePort (${activePort.filePath})`);
    } catch {
      console.log(`DevToolsActivePort file found but port ${activePort.port} not reachable, scanning...`);
    }
  }

  // 2. Fallback: scan common ports
  if (!port) {
    const scanPorts = [9222, 9223, 9224, 9225];
    for (const p of scanPorts) {
      try {
        await fetchJsonOrThrow(`http://localhost:${p}/json/version`, 1000);
        port = p;
        console.log(`Found Chrome CDP on port ${p}`);
        break;
      } catch {
        // not on this port
      }
    }
  }

  if (!port) {
    console.error('Could not find a running Chrome with debugging enabled.\n');
    console.error('To enable Chrome remote debugging:');
    console.error('  1. Open chrome://inspect/#remote-debugging in Chrome');
    console.error('  2. Toggle the switch to enable remote debugging');
    console.error('  3. Run neo attach again\n');
    console.error('Or launch Chrome with: --remote-debugging-port=9222');
    process.exit(1);
  }

  const connected = await connectToCdpPort(port, sessionName);
  console.log(`Connected: ${connected.versionInfo.Browser || 'CDP'} @ ${connected.cdpUrl}`);
  console.log(`Session: ${sessionName}`);

  // List all open tabs
  let targets;
  try {
    targets = parseTabTargets(await fetchJsonOrThrow(`http://localhost:${port}/json/list`));
  } catch {
    targets = connected.tabId
      ? [{ index: 0, type: 'page', id: connected.tabId, title: connected.page.title || '', url: connected.page.url || '', webSocketDebuggerUrl: '' }]
      : [];
  }
  const pages = targets.filter(t => t.type === 'page');
  if (pages.length > 0) {
    console.log(`\nOpen tabs (${pages.length}):`);
    for (const page of pages) {
      const id = (page.id || '').slice(0, 12);
      const title = (page.title || '(untitled)').substring(0, 50);
      console.log(`  ${id}  ${title}`);
      console.log(`         ${page.url}`);
    }
    console.log(`\nActive tab: ${connected.tabId || '(no-id)'}`);
    console.log('Use neo tab <index> or neo tab --url <pattern> to switch tabs');
  }
};

// neo discover
commands.discover = async function() {
  // Check DevToolsActivePort first
  const activePort = getDevToolsActivePort();
  if (activePort) {
    try {
      const cdpUrl = `http://localhost:${activePort.port}`;
      const versionInfo = await fetchJsonOrThrow(`${cdpUrl}/json/version`, 2000);
      const browser = versionInfo?.Browser || 'Unknown Browser';
      console.log(`[DevToolsActivePort] ${browser} (port ${activePort.port})`);
      console.log(`  Source: ${activePort.filePath}`);
      const targets = await fetchJsonOrThrow(`${cdpUrl}/json/list`, 2000);
      const pages = Array.isArray(targets) ? targets.filter(t => t.type === 'page') : [];
      if (!pages.length) {
        console.log('  (no page targets)');
      } else {
        for (const page of pages) {
          const tabId = page.id || page.targetId || '(no-id)';
          const title = page.title || '(untitled)';
          console.log(`  ${tabId} ${title}`);
          console.log(`    ${page.url || '(no-url)'}`);
        }
      }
      console.log('');
    } catch {
      console.log(`[DevToolsActivePort] Found file ${activePort.filePath} but port ${activePort.port} not reachable\n`);
    }
  }

  // Scan port range (skip port already found via DevToolsActivePort)
  const startPort = 9222;
  const endPort = 9299;
  const ports = [];
  const skipPort = activePort ? activePort.port : -1;
  for (let port = startPort; port <= endPort; port++) {
    if (port !== skipPort) ports.push(port);
  }

  const discovered = await Promise.all(ports.map(async (port) => {
    const cdpUrl = `http://localhost:${port}`;
    try {
      const versionInfo = await fetchJsonOrThrow(`${cdpUrl}/json/version`, 800);
      const targets = await fetchJsonOrThrow(`${cdpUrl}/json/list`, 800);
      const pages = Array.isArray(targets) ? targets.filter(target => target.type === 'page') : [];
      return { port, cdpUrl, versionInfo, pages };
    } catch {
      return null;
    }
  }));

  const available = discovered.filter(Boolean);
  if (!available.length) {
    console.log('No CDP endpoints found on localhost ports 9222-9299');
    return;
  }

  for (const item of available) {
    const browser = item.versionInfo?.Browser || 'Unknown Browser';
    console.log(`[${item.port}] ${browser}`);
    if (!item.pages.length) {
      console.log('  (no page targets)');
      continue;
    }
    for (const page of item.pages) {
      const tabId = page.id || page.targetId || '(no-id)';
      const title = page.title || '(untitled)';
      console.log(`  ${tabId} ${title}`);
      console.log(`    ${page.url || '(no-url)'}`);
    }
  }
};

// neo sessions
commands.sessions = function() {
  const sessions = loadSessions();
  const active = Object.entries(sessions)
    .filter(([, value]) => value && typeof value === 'object' && value.cdpUrl);

  if (!active.length) {
    console.log('No active sessions');
    return;
  }

  active.sort(([nameA], [nameB]) => {
    if (nameA === DEFAULT_SESSION_NAME && nameB !== DEFAULT_SESSION_NAME) return -1;
    if (nameB === DEFAULT_SESSION_NAME && nameA !== DEFAULT_SESSION_NAME) return 1;
    return nameA.localeCompare(nameB);
  });

  for (const [name, data] of active) {
    const tab = data.tabId || '(no-tab)';
    console.log(`${name} ${data.cdpUrl} ${tab}`);
  }
};

// neo tab / neo tab <index> / neo tab --url <pattern>
commands.tab = async function(args, context = {}) {
  const { positional, flags } = parseArgs(args || []);
  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const { cdpUrl, targets } = await listSessionTargets(sessionName);
  if (!targets.length) {
    console.log('No CDP targets found');
    return;
  }

  const urlPattern = flags.url === undefined ? null : flags.url;
  if (urlPattern === true || positional.length > 1 || (urlPattern !== null && positional.length > 0)) {
    console.error('Usage: neo tab | neo tab <index> | neo tab --url <pattern>');
    process.exit(1);
  }

  if (urlPattern === null && positional.length === 0) {
    for (const target of targets) {
      const title = target.title || '(untitled)';
      const id = target.id || '(no-id)';
      console.log(`[${target.index}] ${target.type} ${id} ${title}`);
      console.log(`    ${target.url || '(no-url)'}`);
    }
    return;
  }

  let selected = null;
  if (urlPattern !== null) {
    selected = findTabTargetByUrlPattern(targets, String(urlPattern));
    if (!selected) {
      throw new Error(`No tab matching URL pattern: ${String(urlPattern)}`);
    }
  } else {
    const index = parseInt(String(positional[0]), 10);
    if (!Number.isInteger(index) || index < 0 || index >= targets.length) {
      throw new Error(`Invalid tab index: ${positional[0]}`);
    }
    selected = targets[index];
  }

  activateSessionTarget(sessionName, cdpUrl, selected);
  console.log(`Switched to tab ${selected.index}: ${selected.id || '(no-id)'} ${selected.title || '(untitled)'}`);
};

// neo inject [--persist] [--tab pattern]
commands.inject = async function(args, context = {}) {
  const { positional, flags } = parseArgs(args || []);
  if (positional.length > 0 || flags.tab === true) {
    console.error('Usage: neo inject [--persist] [--tab pattern]');
    process.exit(1);
  }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  if (flags.tab !== undefined) {
    await switchSessionTabByUrl(sessionName, String(flags.tab));
  }

  const session = getSession(sessionName);
  if (!session || !session.pageWsUrl) {
    console.error('Run neo connect [port] first');
    process.exit(1);
  }

  const loaded = loadInjectScriptSource();
  const script = buildInjectScript(loaded.source);
  const evaluated = await cdpSend(session.pageWsUrl, 'Runtime.evaluate', {
    expression: script,
    returnByValue: true,
    awaitPromise: true,
  });
  if (evaluated && evaluated.exceptionDetails) {
    throw new Error('Inject evaluate failed');
  }

  const payload = evaluated && evaluated.result ? evaluated.result.value : null;
  if (!payload || payload.ok !== true) {
    throw new Error(payload && payload.error ? payload.error : 'Inject script failed');
  }

  if (flags.persist !== undefined) {
    await cdpSend(session.pageWsUrl, 'Page.addScriptToEvaluateOnNewDocument', {
      source: script,
    });
  }

  const persisted = flags.persist !== undefined ? ' (persist)' : '';
  console.log(`Injected Neo capture script${persisted}`);
};

// neo cookies <action>
commands.cookies = async function(args, context = {}) {
  const { positional } = parseArgs(args || []);
  const action = positional[0];
  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  let session;

  try {
    session = getActivePageSession(sessionName);
  } catch (err) {
    console.error(err.message);
    process.exit(1);
  }

  switch (action) {
    case 'list': {
      if (positional.length > 2) {
        console.error('Usage: neo cookies list [domain]');
        process.exit(1);
      }
      const domain = positional[1] || null;
      const cookies = await getCookiesForSession(sessionName, domain);
      console.log(renderCookieTable(cookies));
      return;
    }

    case 'export': {
      if (positional.length > 3) {
        console.error('Usage: neo cookies export [domain] [file]');
        process.exit(1);
      }

      let domain = null;
      let outputFile = null;
      if (positional.length === 2) {
        if (looksLikeCookieExportFile(positional[1])) outputFile = positional[1];
        else domain = positional[1];
      } else if (positional.length === 3) {
        domain = positional[1];
        outputFile = positional[2];
      }

      const cookies = await getCookiesForSession(sessionName, domain);
      const payload = serializeCookiesForExport(cookies);
      if (!outputFile) {
        console.log(payload);
        return;
      }

      const resolvedPath = path.resolve(outputFile);
      ensureParentDirectory(resolvedPath);
      fs.writeFileSync(resolvedPath, `${payload}\n`, 'utf8');
      console.log(resolvedPath);
      return;
    }

    case 'import': {
      const file = positional[1];
      if (!file || positional.length !== 2) {
        console.error('Usage: neo cookies import <file>');
        process.exit(1);
      }

      const resolvedPath = path.resolve(file);
      if (!fs.existsSync(resolvedPath)) {
        console.error(`File not found: ${resolvedPath}`);
        process.exit(1);
      }

      const cookies = parseCookiesImportText(fs.readFileSync(resolvedPath, 'utf8'));
      let imported = 0;
      const failed = [];
      for (const cookie of cookies) {
        const result = await cdpSend(session.pageWsUrl, 'Network.setCookie', buildSetCookieParams(cookie));
        if (result && result.success === false) {
          failed.push(cookie.name || '(unnamed)');
          continue;
        }
        imported++;
      }

      if (failed.length > 0) {
        throw new Error(`Imported ${imported}/${cookies.length} cookies. Failed: ${failed.join(', ')}`);
      }

      console.log(`Imported ${imported} cookies from ${resolvedPath}`);
      return;
    }

    case 'clear': {
      if (positional.length > 2) {
        console.error('Usage: neo cookies clear [domain]');
        process.exit(1);
      }

      const domain = positional[1] || null;
      if (!domain) {
        await cdpSend(session.pageWsUrl, 'Storage.clearCookies');
        console.log('Cleared all cookies');
        return;
      }

      const cookies = await getCookiesForSession(sessionName, domain);
      for (const cookie of cookies) {
        await cdpSend(session.pageWsUrl, 'Network.deleteCookies', buildDeleteCookieParams(cookie));
      }
      console.log(`Cleared ${cookies.length} cookies for ${normalizeCookieDomainFilter(domain) || String(domain)}`);
      return;
    }

    default:
      console.error('Usage: neo cookies list [domain]');
      console.error('       neo cookies export [domain] [file]');
      console.error('       neo cookies import <file>');
      console.error('       neo cookies clear [domain]');
      process.exit(1);
  }
};

// neo snapshot [-i] [-C] [--json] [--diff] [--selector css]
commands.snapshot = async function(args, context = {}) {
  const { options, unknown } = parseSnapshotArgs(args || []);
  if (unknown.length > 0) {
    console.error('Usage: neo snapshot [-i] [-C] [--json] [--diff] [--selector css]');
    process.exit(1);
  }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const session = getSession(sessionName);
  if (!session || !session.pageWsUrl) {
    console.error('Run neo connect [port] first');
    process.exit(1);
  }

  await ensureLightpandaPageContext(sessionName);
  await cdpSend(session.pageWsUrl, 'Accessibility.enable');
  const treeResult = await cdpSend(session.pageWsUrl, 'Accessibility.getFullAXTree');
  const assigned = assignRefs(treeResult && Array.isArray(treeResult.nodes) ? treeResult.nodes : []);

  const displayNodes = options.interactiveOnly
    ? assigned.nodes.filter(node => INTERACTIVE_ROLES.has(String(node.role || '').toLowerCase()))
    : assigned.nodes;
  const storedSnapshot = displayNodes.map(node => ({
    ref: node.ref,
    role: node.role,
    name: node.name,
    depth: node.depth,
  }));

  if (options.includeCursorPointer) {
    // TODO: Add Runtime.evaluate scan for cursor:pointer elements.
    console.error('TODO: snapshot -C (cursor:pointer) is not implemented yet');
  }
  if (options.selector) {
    // TODO: Add CSS selector scoped snapshot filtering.
    console.error('TODO: snapshot --selector is not implemented yet');
  }

  if (options.diff) {
    const prev = Array.isArray(session.prevSnapshot) ? session.prevSnapshot : [];
    const prevMap = new Map(prev.map(node => [`${node.role}:${node.name}`, node]));
    const currMap = new Map(displayNodes.map(node => [`${node.role}:${node.name}`, node]));

    const added = displayNodes.filter(node => !prevMap.has(`${node.role}:${node.name}`));
    const removed = prev.filter(node => !currMap.has(`${node.role}:${node.name}`));
    const changed = displayNodes.filter((node) => {
      const previousNode = prevMap.get(`${node.role}:${node.name}`);
      return previousNode && previousNode.depth !== node.depth;
    });

    const latestSession = getSession(sessionName) || session || {};
    setSession(sessionName, {
      ...latestSession,
      refs: assigned.refs,
      prevSnapshot: storedSnapshot,
    });

    if (options.json) {
      console.log(JSON.stringify({ added, removed, changed }, null, 2));
      return;
    }

    const lines = [];
    if (added.length) {
      lines.push(`+ Added (${added.length}):`);
      lines.push(formatSnapshot(added));
    }
    if (removed.length) {
      lines.push(`- Removed (${removed.length}):`);
      lines.push(formatSnapshot(removed));
    }
    if (changed.length) {
      lines.push(`~ Changed (${changed.length}):`);
      lines.push(formatSnapshot(changed));
    }
    if (lines.length === 0) {
      lines.push('(no changes)');
    }
    console.log(lines.join('\n'));
    return;
  }

  setSession(sessionName, {
    ...session,
    refs: assigned.refs,
    prevSnapshot: storedSnapshot,
  });

  if (options.json) {
    const refs = {};
    for (const node of displayNodes) {
      if (node && node.ref && assigned.refs[node.ref]) {
        refs[node.ref] = assigned.refs[node.ref];
      }
    }
    console.log(JSON.stringify({
      session: sessionName,
      count: displayNodes.length,
      nodes: displayNodes,
      refs,
    }, null, 2));
    return;
  }

  const output = formatSnapshot(displayNodes);
  console.log(output || '(empty snapshot)');
};

// neo click <ref> [--new-tab]
commands.click = async function(args, context = {}) {
  const { positional, flags } = parseArgs(args || []);
  const ref = positional[0];
  if (!ref || positional.length > 1) {
    console.error('Usage: neo click <ref> [--new-tab]');
    process.exit(1);
  }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const pageWsUrl = getSessionPageWsUrl(sessionName);
  const target = await resolveRef(sessionName, ref);
  const modifiers = flags['new-tab'] !== undefined ? 4 : 0;
  const base = {
    x: target.x,
    y: target.y,
    button: 'left',
    clickCount: 1,
  };
  if (modifiers) base.modifiers = modifiers;

  await cdpSend(pageWsUrl, 'Input.dispatchMouseEvent', {
    ...base,
    type: 'mousePressed',
  });
  await cdpSend(pageWsUrl, 'Input.dispatchMouseEvent', {
    ...base,
    type: 'mouseReleased',
  });
  console.log(`Clicked ${ref}`);
};

// neo fill <ref> "text"
commands.fill = async function(args, context = {}) {
  const { positional } = parseArgs(args || []);
  const ref = positional[0];
  const text = positional.length > 1 ? positional.slice(1).join(' ') : null;
  if (!ref || text === null) {
    console.error('Usage: neo fill <ref> "text"');
    process.exit(1);
  }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const pageWsUrl = getSessionPageWsUrl(sessionName);
  const target = await resolveRef(sessionName, ref);

  await cdpSend(pageWsUrl, 'DOM.focus', {
    backendNodeId: target.backendDOMNodeId,
  });
  await cdpSend(pageWsUrl, 'Input.dispatchKeyEvent', {
    type: 'keyDown',
    key: 'a',
    code: 'KeyA',
    modifiers: 2,
  });
  await cdpSend(pageWsUrl, 'Input.dispatchKeyEvent', {
    type: 'keyUp',
    key: 'a',
    code: 'KeyA',
    modifiers: 2,
  });
  await cdpSend(pageWsUrl, 'Input.dispatchKeyEvent', {
    type: 'keyDown',
    key: 'Backspace',
    code: 'Backspace',
  });
  await cdpSend(pageWsUrl, 'Input.dispatchKeyEvent', {
    type: 'keyUp',
    key: 'Backspace',
    code: 'Backspace',
  });
  await cdpSend(pageWsUrl, 'Input.insertText', { text });
  console.log(`Filled ${ref}`);
};

// neo type <ref> "text"
commands.type = async function(args, context = {}) {
  const { positional } = parseArgs(args || []);
  const ref = positional[0];
  const text = positional.length > 1 ? positional.slice(1).join(' ') : null;
  if (!ref || text === null) {
    console.error('Usage: neo type <ref> "text"');
    process.exit(1);
  }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const pageWsUrl = getSessionPageWsUrl(sessionName);
  const target = await resolveRef(sessionName, ref);
  await cdpSend(pageWsUrl, 'DOM.focus', {
    backendNodeId: target.backendDOMNodeId,
  });
  await cdpSend(pageWsUrl, 'Input.insertText', { text });
  console.log(`Typed into ${ref}`);
};

// neo press <key>
commands.press = async function(args, context = {}) {
  const { positional } = parseArgs(args || []);
  const rawKey = positional[0];
  if (!rawKey || positional.length > 1) {
    console.error('Usage: neo press <key>');
    process.exit(1);
  }

  const mapped = parsePressKey(rawKey);
  if (!mapped) {
    console.error(`Unsupported key: ${rawKey}`);
    console.error('Supported: Enter, Tab, Escape, Backspace, ArrowUp/Down/Left/Right, Space, Delete, Home, End, PageUp, PageDown, Ctrl+a');
    process.exit(1);
  }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const pageWsUrl = getSessionPageWsUrl(sessionName);
  const down = {
    type: 'keyDown',
    key: mapped.key,
    code: mapped.code,
  };
  if (mapped.modifiers) down.modifiers = mapped.modifiers;
  if (mapped.text !== undefined) down.text = mapped.text;

  const up = {
    type: 'keyUp',
    key: mapped.key,
    code: mapped.code,
  };
  if (mapped.modifiers) up.modifiers = mapped.modifiers;

  await cdpSend(pageWsUrl, 'Input.dispatchKeyEvent', down);
  await cdpSend(pageWsUrl, 'Input.dispatchKeyEvent', up);
  console.log(`Pressed ${rawKey}`);
};

// neo hover <ref>
commands.hover = async function(args, context = {}) {
  const { positional } = parseArgs(args || []);
  const ref = positional[0];
  if (!ref || positional.length > 1) {
    console.error('Usage: neo hover <ref>');
    process.exit(1);
  }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const pageWsUrl = getSessionPageWsUrl(sessionName);
  const target = await resolveRef(sessionName, ref);
  await cdpSend(pageWsUrl, 'Input.dispatchMouseEvent', {
    type: 'mouseMoved',
    x: target.x,
    y: target.y,
  });
  console.log(`Hovered ${ref}`);
};

// neo scroll <up|down|left|right> [px] [--selector css]
commands.scroll = async function(args, context = {}) {
  const { positional, flags } = parseArgs(args || []);
  const direction = String(positional[0] || '').toLowerCase();
  const rawDistance = positional[1];
  const distance = rawDistance ? parseInt(rawDistance, 10) : 300;
  const selector = flags.selector || null;
  if (flags.selector === true || !direction || !['up', 'down', 'left', 'right'].includes(direction) || (rawDistance && (!Number.isInteger(distance) || distance <= 0))) {
    console.error('Usage: neo scroll <up|down|left|right> [px] [--selector css]');
    process.exit(1);
  }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const pageWsUrl = getSessionPageWsUrl(sessionName);
  const point = await resolveScrollPoint(pageWsUrl, selector);
  const wheel = {
    type: 'mouseWheel',
    x: point.x,
    y: point.y,
    deltaX: 0,
    deltaY: 0,
  };
  if (direction === 'up') wheel.deltaY = -distance;
  if (direction === 'down') wheel.deltaY = distance;
  if (direction === 'left') wheel.deltaX = -distance;
  if (direction === 'right') wheel.deltaX = distance;

  await cdpSend(pageWsUrl, 'Input.dispatchMouseEvent', wheel);
  console.log(`Scrolled ${direction} ${distance}px`);
};

// neo select <ref> "value"
commands.select = async function(args, context = {}) {
  const { positional } = parseArgs(args || []);
  const ref = positional[0];
  const value = positional.length > 1 ? positional.slice(1).join(' ') : null;
  if (!ref || value === null) {
    console.error('Usage: neo select <ref> "value"');
    process.exit(1);
  }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const pageWsUrl = getSessionPageWsUrl(sessionName);
  const target = await resolveRef(sessionName, ref);
  const result = await cdpSend(pageWsUrl, 'Runtime.callFunctionOn', {
    objectId: target.objectId,
    functionDeclaration: `function(nextValue) {
      if (!this) return { ok: false, error: 'missing element' };
      if (!('value' in this)) return { ok: false, error: 'element has no value property' };
      if (typeof this.focus === 'function') this.focus();
      this.value = nextValue;
      this.dispatchEvent(new Event('input', { bubbles: true }));
      this.dispatchEvent(new Event('change', { bubbles: true }));
      return { ok: true, value: this.value };
    }`,
    arguments: [{ value }],
    returnByValue: true,
  });

  if (result && result.exceptionDetails) {
    throw new Error(`Failed to select value for ${ref}`);
  }
  const payload = result && result.result && result.result.value;
  if (!payload || payload.ok !== true) {
    throw new Error(payload && payload.error ? payload.error : `Failed to select value for ${ref}`);
  }

  console.log(`Selected ${ref} = "${payload.value}"`);
};

// neo screenshot [path] [--full] [--annotate]
commands.screenshot = async function(args, context = {}) {
  const { positional, flags } = parseArgs(args || []);
  if (positional.length > 1) {
    console.error('Usage: neo screenshot [path] [--full] [--annotate]');
    process.exit(1);
  }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const pageWsUrl = getSessionPageWsUrl(sessionName);
  const outputPath = positional[0]
    ? path.resolve(positional[0])
    : `/tmp/neo-screenshot-${Date.now()}.png`;
  const screenshotParams = {
    format: 'png',
  };

  if (flags.full !== undefined) {
    const layout = await cdpSend(pageWsUrl, 'Page.getLayoutMetrics');
    const contentSize = (layout && layout.contentSize) || (layout && layout.cssContentSize) || null;
    if (contentSize && Number.isFinite(contentSize.width) && Number.isFinite(contentSize.height)) {
      screenshotParams.captureBeyondViewport = true;
      screenshotParams.clip = {
        x: 0,
        y: 0,
        width: Math.max(1, Math.ceil(contentSize.width)),
        height: Math.max(1, Math.ceil(contentSize.height)),
        scale: 1,
      };
    } else {
      screenshotParams.captureBeyondViewport = true;
    }
  }

  const result = await cdpSend(pageWsUrl, 'Page.captureScreenshot', screenshotParams);
  if (!result || !result.data) {
    throw new Error('Failed to capture screenshot');
  }

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, Buffer.from(result.data, 'base64'));
  if (flags.annotate !== undefined) {
    console.error('TODO: screenshot --annotate is not implemented yet');
  }
  console.log(outputPath);
};

// neo get text <ref> | neo get url | neo get title
commands.get = async function(args, context = {}) {
  const { positional } = parseArgs(args || []);
  const subject = positional[0];
  if (!subject) {
    console.error('Usage: neo get text <ref> | neo get url | neo get title');
    process.exit(1);
  }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const pageWsUrl = getSessionPageWsUrl(sessionName);

  if (subject === 'text') {
    const ref = positional[1];
    if (!ref || positional.length > 2) {
      console.error('Usage: neo get text <ref>');
      process.exit(1);
    }
    const target = await resolveRef(sessionName, ref);
    const result = await cdpSend(pageWsUrl, 'Runtime.callFunctionOn', {
      objectId: target.objectId,
      functionDeclaration: `function() {
        if (!this) return '';
        if (typeof this.innerText === 'string') return this.innerText;
        if (typeof this.textContent === 'string') return this.textContent;
        return '';
      }`,
      returnByValue: true,
    });
    const text = result && result.result ? result.result.value : '';
    console.log(text === undefined || text === null ? '' : String(text));
    return;
  }

  if (subject === 'url' || subject === 'title') {
    if (positional.length > 1) {
      console.error(`Usage: neo get ${subject}`);
      process.exit(1);
    }
    const expression = subject === 'url' ? 'location.href' : 'document.title';
    const result = await cdpSend(pageWsUrl, 'Runtime.evaluate', {
      expression,
      returnByValue: true,
    });
    const value = result && result.result ? result.result.value : '';
    console.log(value === undefined || value === null ? '' : String(value));
    return;
  }

  console.error('Usage: neo get text <ref> | neo get url | neo get title');
  process.exit(1);
};

// neo wait <ref> | neo wait --load networkidle | neo wait <ms>
commands.wait = async function(args, context = {}) {
  const { positional, flags } = parseArgs(args || []);

  if (flags.load !== undefined) {
    if (String(flags.load || '').toLowerCase() !== 'networkidle') {
      console.error('Usage: neo wait --load networkidle');
      process.exit(1);
    }
    await sleep(2000);
    console.log('Waited for networkidle (2000ms)');
    return;
  }

  const target = positional[0];
  if (!target || positional.length > 1) {
    console.error('Usage: neo wait <ref> | neo wait --load networkidle | neo wait <ms>');
    process.exit(1);
  }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const session = getSession(sessionName);
  const rawTarget = String(target).trim();
  const shouldResolveRef = rawTarget.startsWith('@') || rawTarget.startsWith('[') || hasStoredRef(session, rawTarget);

  if (shouldResolveRef) {
    const pageWsUrl = getSessionPageWsUrl(sessionName);
    const resolved = await resolveRef(sessionName, target);
    const selector = await selectorFromObject(pageWsUrl, resolved.objectId);
    if (!selector) {
      throw new Error(`Failed to derive selector for ${target}`);
    }
    const ok = await waitForSelector(pageWsUrl, selector, 10000, 500);
    if (!ok) {
      console.error(`Timeout waiting for ${target}`);
      process.exit(1);
    }
    console.log(`Found ${target}`);
    return;
  }

  const ms = parseInt(target, 10);
  if (!Number.isInteger(ms) || ms < 0) {
    console.error('Usage: neo wait <ref> | neo wait --load networkidle | neo wait <ms>');
    process.exit(1);
  }
  await sleep(ms);
  console.log(`Waited ${ms}ms`);
};

// neo label <domain> [--dry-run]
commands.label = async function(args) {
  const { positional, flags } = parseArgs(args);
  const domain = positional[0];
  if (!domain) {
    console.error('Usage: neo label <domain> [--dry-run]');
    process.exit(1);
  }

  const loaded = loadSchemaFile(domain);
  if (!loaded) {
    console.error(`No schema for ${domain}. Run: neo schema generate ${domain}`);
    process.exit(1);
  }

  const schema = loaded.schema;
  const prompt = buildLabelPromptForSchema(schema);
  console.log(prompt);
  if (flags['dry-run']) {
    return;
  }

  const raw = await readStdinText();
  const userLabels = coerceLabelInput(raw);
  const heuristic = collectHeuristicLabels(schema);
  const fallback = userLabels ? { ...heuristic, ...userLabels } : heuristic;
  const { changed } = mergeEndpointLabels(schema, fallback);
  saveSchemaFile(domain, schema);
  console.log(`Updated ${changed} endpoint labels in ${loaded.file}`);
};

// neo status
commands.status = async function(args, context = {}) {
  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const captures = await loadCapturesFromSources(sessionName, { source: 'all' });
  const counts = {};
  for (const capture of captures) {
    const domain = String(capture && capture.domain || '');
    if (!domain) continue;
    counts[domain] = (counts[domain] || 0) + 1;
  }
  const domains = Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .map(([domain, count]) => `${domain}: ${count}`)
    .join('\n');
  console.log(`Total captures: ${captures.length}\n`);
  console.log(domains || '(no captures)');
};

// neo capture <action>
commands.capture = async function(args, context = {}) {
  const { positional, flags } = parseArgs(args);
  const action = positional[0];
  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const cdpUrl = getSessionCdpUrl(sessionName);
  const source = normalizeCaptureSourceFlag(flags.source || 'all');

  switch (action) {
    case 'start': {
      const tabPattern = typeof flags.tab === 'string' ? flags.tab : null;
      const target = await findTab(tabPattern, { cdpUrl });
      if (!target || !target.webSocketDebuggerUrl) {
        throw new Error(`No page target found for ${cdpUrl}`);
      }
      const state = await startCdpCaptureDaemon(target, {
        stateFile: CDP_CAPTURE_STATE_FILE,
        captureFile: LOCAL_CAPTURE_FILE,
      });
      console.log(`CDP capture started on tab ${target.id || '(no-id)'}`);
      console.log(`  PID: ${state.pid}`);
      console.log(`  WS: ${state.wsUrl}`);
      console.log(`  Store: ${LOCAL_CAPTURE_FILE}`);
      break;
    }

    case 'stop': {
      const result = await stopCdpCaptureDaemon(CDP_CAPTURE_STATE_FILE);
      console.log(result.message);
      break;
    }

    case 'count': {
      const captures = await loadCapturesFromSources(sessionName, { source });
      console.log(String(captures.length));
      break;
    }

    case 'domains': {
      const captures = await loadCapturesFromSources(sessionName, { source });
      const counts = {};
      for (const capture of captures) {
        const domain = String(capture && capture.domain || '');
        if (!domain) continue;
        counts[domain] = (counts[domain] || 0) + 1;
      }
      const lines = Object.entries(counts)
        .sort((a, b) => b[1] - a[1])
        .map(([domain, count]) => `${domain}: ${count}`);
      console.log(lines.join('\n') || '(no captures)');
      break;
    }

    case 'list': {
      const domain = positional[1];
      const limit = parseInt(flags.limit, 10) || 20;
      const since = flags.since ? Date.now() - parseDuration(flags.since) : 0;
      const captures = await loadCapturesFromSources(sessionName, {
        source,
        domain,
        since,
        sort: 'timestamp-desc',
        limit,
      });
      const rows = captures.map((capture) => {
        const src = capture.source === 'websocket' ? ' [ws]' : capture.source === 'eventsource' ? ' [sse]' : '';
        const shortId = String(capture.id || 'unknown').slice(0, 8);
        const method = String(capture.method || 'GET');
        const status = capture.responseStatus === undefined ? '?' : capture.responseStatus;
        const url = String(capture.url || '').slice(0, 90);
        const duration = Number(capture.duration) || 0;
        return `${shortId}  ${method} ${status} ${url} (${duration}ms)${src}`;
      });
      console.log(rows.join('\n') || '(no captures)');
      break;
    }

    case 'detail': {
      const id = positional[1];
      if (!id) { console.error('Usage: neo capture detail <id> [--curl] [--neo] [--source cdp|extension|all]'); process.exit(1); }
      const capture = await findCaptureById(sessionName, id, { source, sort: 'timestamp-desc' }, { cdpUrl });
      if (!capture) { console.log('Not found'); break; }
      const copy = JSON.parse(JSON.stringify(capture));
      if (copy.responseBody && typeof copy.responseBody === 'string' && copy.responseBody.length > 5000) {
        copy.responseBody = `${copy.responseBody.slice(0, 5000)}... [truncated]`;
      }
      const rendered = JSON.stringify(copy, null, 2);

      if (flags.curl || flags.neo) {
        if (flags.curl) {
          const parts = [`curl -X ${copy.method} '${copy.url}'`];
          for (const [k, v] of Object.entries(copy.requestHeaders || {})) {
            if (k.toLowerCase() !== 'host' && k.toLowerCase() !== 'content-length') {
              parts.push(`  -H '${k}: ${v}'`);
            }
          }
          if (copy.requestBody && copy.method !== 'GET') {
            const body = typeof copy.requestBody === 'string' ? copy.requestBody : JSON.stringify(copy.requestBody);
            parts.push(`  -d '${body.replace(/'/g, "'\\''")}'`);
          }
          console.log(parts.join(' \\\n'));
        } else {
          const parts = [`neo exec '${copy.url}'`];
          if (copy.method !== 'GET') parts.push(`--method ${copy.method}`);
          parts.push('--auto-headers');
          if (copy.requestBody && copy.method !== 'GET') {
            const body = typeof copy.requestBody === 'string' ? copy.requestBody : JSON.stringify(copy.requestBody);
            parts.push(`--body '${body.replace(/'/g, "'\\''")}'`);
          }
          console.log(parts.join(' '));
        }
      } else {
        console.log(rendered);
      }
      break;
    }

    case 'clear': {
      const domain = positional[1];
      if (!domain && !flags.all) {
        console.error('Usage: neo capture clear <domain>  or  neo capture clear --all [--source cdp|extension|all]');
        process.exit(1);
      }
      let deleted = 0;
      if (source === 'all' || source === 'extension') {
        const extensionWsUrl = await findExtensionWs({ cdpUrl });
        if (extensionWsUrl) {
          const result = await clearExtensionCaptureRecords(flags.all ? null : domain, { wsUrl: extensionWsUrl, cdpUrl });
          deleted += Number(result.deleted) || 0;
        }
      }
      if (source === 'all' || source === 'cdp') {
        const local = clearLocalCaptureRecords(flags.all ? null : domain, LOCAL_CAPTURE_FILE);
        deleted += local.deleted;
        if (hasActiveSession(sessionName)) {
          const sessionCleared = await clearSessionCaptures(sessionName, flags.all ? null : domain);
          deleted += sessionCleared.deleted;
        }
      }
      if (flags.all) {
        console.log(`Cleared captures (${source})`);
      } else {
        console.log(`Deleted ${deleted} captures for ${domain} (${source})`);
      }
      break;
    }

    case 'export': {
      const domain = positional[1];
      const since = flags.since ? Date.now() - parseDuration(flags.since) : 0;
      const format = flags.format || 'json';
      const includeAuth = flags['include-auth'] !== undefined;
      const captures = await loadCapturesFromSources(sessionName, {
        source,
        domain,
        since,
      });
      const rows = captures.map((capture) => {
        const copy = JSON.parse(JSON.stringify(capture));
        if (copy.responseBody && typeof copy.responseBody === 'string' && copy.responseBody.length > 2000) {
          copy.responseBody = `${copy.responseBody.slice(0, 2000)}...[truncated]`;
        }
        return copy;
      });
      const liveHeadersByDomain = includeAuth
        ? await collectLiveHeadersByDomainForExport(rows)
        : {};
      const exportRows = rows.map(cap => applyExportAuthPolicy(cap, liveHeadersByDomain, includeAuth));
      if (format === 'har') {
        const entries = exportRows.map(cap => {
          const reqHeaders = Object.entries(cap.requestHeaders || {}).map(([n, v]) => ({ name: n, value: String(v) }));
          const respHeaders = Object.entries(cap.responseHeaders || {}).map(([n, v]) => ({ name: n, value: String(v) }));
          const u = (() => { try { return new URL(cap.url); } catch { return null; } })();
          const queryString = u ? [...u.searchParams].map(([n, v]) => ({ name: n, value: v })) : [];
          const postData = cap.requestBody ? {
            mimeType: (cap.requestHeaders || {})['content-type'] || 'application/json',
            text: typeof cap.requestBody === 'string' ? cap.requestBody : JSON.stringify(cap.requestBody),
          } : undefined;
          return {
            startedDateTime: new Date(cap.timestamp).toISOString(),
            time: cap.duration || 0,
            request: {
              method: cap.method,
              url: cap.url,
              httpVersion: 'HTTP/1.1',
              cookies: [],
              headers: reqHeaders,
              queryString,
              ...(postData ? { postData, bodySize: (postData.text || '').length } : { bodySize: 0 }),
              headersSize: -1,
            },
            response: {
              status: cap.responseStatus || 0,
              statusText: '',
              httpVersion: 'HTTP/1.1',
              cookies: [],
              headers: respHeaders,
              content: {
                size: cap.responseBody ? (typeof cap.responseBody === 'string' ? cap.responseBody.length : JSON.stringify(cap.responseBody).length) : 0,
                mimeType: (cap.responseHeaders || {})['content-type'] || 'application/octet-stream',
                text: cap.responseBody ? (typeof cap.responseBody === 'string' ? cap.responseBody : JSON.stringify(cap.responseBody)) : '',
              },
              redirectURL: '',
              headersSize: -1,
              bodySize: -1,
            },
            cache: {},
            timings: { send: 0, wait: cap.duration || 0, receive: 0 },
          };
        });
        console.log(JSON.stringify({
          log: {
            version: '1.2',
            creator: { name: 'Neo', version: '0.6.0' },
            entries,
          },
        }, null, 2));
      } else {
        console.log(JSON.stringify(exportRows, null, 2));
      }
      break;
    }

    case 'stats': {
      const domain = positional[1];
      if (!domain) { console.error('Usage: neo capture stats <domain> [--source cdp|extension|all]'); process.exit(1); }
      const stats = { total: 0, methods: {}, statuses: {}, sources: {}, totalDuration: 0, errors: 0 };
      const captures = await loadCapturesFromSources(sessionName, { source, domain });
      for (const capture of captures) {
        if (!capture || capture.domain !== domain) continue;
        stats.total++;
        const method = String(capture.method || 'GET');
        const status = capture.responseStatus;
        const captureSource = capture.source || 'fetch';
        const duration = Number(capture.duration) || 0;
        stats.methods[method] = (stats.methods[method] || 0) + 1;
        stats.statuses[status] = (stats.statuses[status] || 0) + 1;
        stats.sources[captureSource] = (stats.sources[captureSource] || 0) + 1;
        stats.totalDuration += duration;
        if (status >= 400 || status === 0) stats.errors++;
      }
      if (!stats.total) { console.log(`No captures for ${domain}`); break; }
      console.log(`${domain} — ${stats.total} captures\n`);
      console.log(`Methods:  ${Object.entries(stats.methods).map(([k, v]) => `${k}: ${v}`).join(', ')}`);
      console.log(`Statuses: ${Object.entries(stats.statuses).sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k}: ${v}`).join(', ')}`);
      console.log(`Sources:  ${Object.entries(stats.sources).map(([k, v]) => `${k}: ${v}`).join(', ')}`);
      console.log(`Avg duration: ${Math.round(stats.totalDuration / stats.total)}ms`);
      console.log(`Error rate: ${(stats.errors / stats.total * 100).toFixed(1)}%`);
      break;
    }

    case 'search': {
      const query = positional.slice(1).join(' ');
      if (!query) { console.error('Usage: neo capture search <query> [--method GET] [--status 200] [--limit N] [--source cdp|extension|all]'); process.exit(1); }
      const limit = parseInt(flags.limit, 10) || 20;
      const methodFilter = flags.method ? String(flags.method).toUpperCase() : null;
      const statusFilter = flags.status ? parseInt(flags.status, 10) : null;
      const captures = await loadCapturesFromSources(sessionName, {
        source,
        query,
        method: methodFilter,
        status: statusFilter,
        sort: 'timestamp-desc',
        limit,
      });
      const rows = captures.map((capture) => `${capture.id || ''}  ${capture.method || 'GET'} ${capture.responseStatus} ${String(capture.url || '').slice(0, 100)} (${Number(capture.duration) || 0}ms)`);
      console.log(rows.join('\n') || '(no matches)');
      break;
    }

    case 'import': {
      const file = positional[1];
      if (!file) { console.error('Usage: neo capture import <file.json>'); process.exit(1); }
      if (!fs.existsSync(file)) { console.error(`File not found: ${file}`); process.exit(1); }
      const data = JSON.parse(fs.readFileSync(file, 'utf8'));
      const items = Array.isArray(data) ? data : [data];
      if (!items.length) { console.log('No captures in file'); break; }
      const wsUrl = await requireExtensionWs();
      const batchSize = 50;
      let imported = 0;
      for (let i = 0; i < items.length; i += batchSize) {
        const batch = items.slice(i, i + batchSize);
        const result = await cdpEval(wsUrl, `new Promise(function(resolve) {
          var req = indexedDB.open("${DB_NAME}");
          req.onsuccess = function() {
            var db = req.result;
            var tx = db.transaction("${STORE_NAME}", "readwrite");
            var store = tx.objectStore("${STORE_NAME}");
            var items = ${JSON.stringify(batch)};
            var count = 0;
            items.forEach(function(item) {
              store.put(item).onsuccess = function() { count++; };
            });
            tx.oncomplete = function() { resolve(String(count)); };
            tx.onerror = function() { resolve("Error: " + tx.error); };
          };
          req.onerror = function() { resolve("Error: " + req.error); };
          setTimeout(function() { resolve("timeout"); }, 30000);
        })`, 35000);
        const n = parseInt(result, 10);
        if (!isNaN(n)) imported += n;
        else console.error(`Batch error: ${result}`);
      }
      console.log(`Imported ${imported} captures from ${file}`);
      break;
    }

    case 'watch': {
      const domain = positional[1];
      console.error(`Watching captures${domain ? ' for ' + domain : ''}... (Ctrl+C to stop)`);
      let lastTimestamp = Date.now();
      const poll = async () => {
        try {
          const items = await loadCapturesFromSources(sessionName, {
            source,
            domain,
            since: lastTimestamp + 1,
            sort: 'timestamp-desc',
          });
          for (const item of items.reverse()) {
            const time = new Date(item.timestamp).toLocaleTimeString();
            const src = item.source === 'websocket' ? ' [ws]' : item.source === 'eventsource' ? ' [sse]' : '';
            console.log(`${time}  ${item.method} ${item.responseStatus} ${String(item.url || '').slice(0, 100)} (${item.duration || 0}ms)${src}`);
            if (item.timestamp > lastTimestamp) lastTimestamp = item.timestamp;
          }
        } catch {}
      };
      await poll();
      const interval = setInterval(poll, 2000);
      process.on('SIGINT', () => { clearInterval(interval); process.exit(0); });
      await new Promise(() => {});
      break;
    }

    case 'summary': {
      const captures = await loadCapturesFromSources(sessionName, { source });
      const summary = { total: 0, domains: {}, sources: {}, oldest: Infinity, newest: 0 };
      for (const capture of captures) {
        summary.total++;
        summary.domains[capture.domain] = (summary.domains[capture.domain] || 0) + 1;
        summary.sources[capture.source || 'fetch'] = (summary.sources[capture.source || 'fetch'] || 0) + 1;
        if (capture.timestamp < summary.oldest) summary.oldest = capture.timestamp;
        if (capture.timestamp > summary.newest) summary.newest = capture.timestamp;
      }
      if (!summary.total) { console.log('No captures.'); break; }
      const span = Math.round((summary.newest - summary.oldest) / 3600000);
      console.log(`${summary.total} captures across ${Object.keys(summary.domains).length} domains (${span}h span)\n`);
      console.log('Sources:', Object.entries(summary.sources).map(([k, v]) => `${k}: ${v}`).join(', '));
      console.log('\nDomains:');
      const sorted = Object.entries(summary.domains).sort((a, b) => b[1] - a[1]);
      for (const [domain, count] of sorted.slice(0, 15)) {
        console.log(`  ${domain}: ${count}`);
      }
      if (sorted.length > 15) console.log(`  ... and ${sorted.length - 15} more`);
      break;
    }

    case 'prune': {
      const older = flags['older-than'] || flags.older || '7d';
      const cutoff = Date.now() - parseDuration(older);
      let deleted = 0;
      if (source === 'all' || source === 'cdp') {
        const keep = readLocalCaptureJsonl(LOCAL_CAPTURE_FILE).filter((item) => {
          const keepItem = (Number(item && item.timestamp) || 0) >= cutoff;
          if (!keepItem) deleted++;
          return keepItem;
        });
        writeLocalCaptureJsonl(keep, LOCAL_CAPTURE_FILE);
      }
      if (source === 'all' || source === 'extension') {
        const extensionWsUrl = await findExtensionWs({ cdpUrl });
        if (extensionWsUrl) {
          const r = await cdpEval(extensionWsUrl, `new Promise(function(resolve) {
            var req = indexedDB.open("${DB_NAME}");
            req.onsuccess = function() {
              var db = req.result;
              var tx = db.transaction("${STORE_NAME}", "readwrite");
              var store = tx.objectStore("${STORE_NAME}");
              var deleted = 0;
              store.openCursor().onsuccess = function(e) {
                var c = e.target.result;
                if (c) {
                  if ((Number(c.value.timestamp) || 0) < ${cutoff}) { c.delete(); deleted++; }
                  c.continue();
                } else { resolve(String(deleted)); }
              };
              tx.onerror = function() { resolve("0"); };
            };
            req.onerror = function() { resolve("0"); };
          })`, 60000);
          deleted += Number(r) || 0;
        }
      }
      console.log(`Pruned ${deleted} captures older than ${older}`);
      break;
    }

    case 'gc': {
      const domain = positional[1];
      const dryRun = flags['dry-run'] || flags.dryrun;
      const captures = await loadCapturesFromSources(sessionName, { source, domain, sort: 'timestamp-desc' });
      const seen = new Map();
      let duplicateCount = 0;
      const keepRows = [];
      for (const capture of captures) {
        let pathname = '/';
        try { pathname = new URL(capture.url).pathname; } catch {}
        pathname = pathname.replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, ':uuid');
        pathname = pathname.replace(/\/\d{4,}/g, '/:id');
        const key = `${capture.method} ${capture.domain}${pathname} ${capture.responseStatus || '?'}`;
        if (seen.has(key)) {
          duplicateCount++;
          continue;
        }
        seen.set(key, true);
        keepRows.push(capture);
      }
      if (dryRun) {
        console.log(`[dry-run] Would keep ${keepRows.length} unique patterns, delete ${duplicateCount} duplicates`);
        break;
      }
      if (source === 'all' || source === 'cdp') {
        const current = readLocalCaptureJsonl(LOCAL_CAPTURE_FILE);
        const keepIds = new Set(keepRows.map((item) => String(item.id || '')));
        writeLocalCaptureJsonl(current.filter((item) => keepIds.has(String(item && item.id || ''))), LOCAL_CAPTURE_FILE);
      }
      console.log(`Kept ${keepRows.length} unique patterns, deleted ${duplicateCount} duplicates`);
      break;
    }

    default:
      console.log(`neo capture — Manage captured API traffic

  neo capture start [--tab pattern]         Start CDP Network capture daemon
  neo capture stop                          Stop CDP Network capture daemon
  neo capture list [domain] [--limit N]     List recent captures
  neo capture count                         Total capture count
  neo capture domains                       List domains with counts
  neo capture stats <domain>                Domain statistics (methods, errors, timing)
  neo capture detail <id>                   Show full capture details
  neo capture search <query>                Search captures by URL (--method, --status, --limit)
  neo capture clear [domain]                Clear captures (all or by domain)
  neo capture export [domain] [--since 1h] [--format har] [--include-auth]
                                            Export as JSON or HAR 1.2
  neo capture import <file>                 Import captures from JSON file
  neo capture watch [domain]                Live tail of new captures
  neo capture summary                       Quick overview for AI agents
  neo capture prune [--older-than 7d]       Delete old captures
  neo capture gc [domain] [--dry-run]       Smart dedup: keep one per unique pattern

Options:
  --source cdp|extension|all                Filter capture store source (default: all)`);
  }
};

// neo schema <action>
commands.schema = async function(args, context = {}) {
  const { positional, flags } = parseArgs(args);
  const action = positional[0];
  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const cdpUrl = getSessionCdpUrl(sessionName);

  switch (action) {
    case 'generate': {
      const domain = positional[1];
      const source = normalizeCaptureSourceFlag(flags.source || 'all');
      
      // --all: generate schemas for all domains with captures
      if (flags.all || domain === '--all') {
        let domainCounts = {};
        const captures = await loadCapturesFromSources(sessionName, { source }, { cdpUrl });
        for (const capture of captures) {
          const d = capture && capture.domain ? String(capture.domain) : '';
          if (!d) continue;
          domainCounts[d] = (domainCounts[d] || 0) + 1;
        }
        const domainList = Object.entries(domainCounts)
          .filter(([, count]) => count >= 2)  // skip domains with only 1 capture
          .sort((a, b) => b[1] - a[1]);
        
        if (!domainList.length) { console.error('No domains with enough captures'); process.exit(1); }
        
        console.error(`Generating schemas for ${domainList.length} domains...\n`);
        let generated = 0, skipped = 0;
        
        // Re-run the command for each domain by recursing into the switch
        for (const [d, count] of domainList) {
          process.stdout.write(`  ${d} (${count} captures)... `);
          try {
            // Inline the single-domain generation (call the same CLI)
            const { execSync } = require('child_process');
            const sessionArg = sessionName ? ` --session ${shellEscape(sessionName)}` : '';
            const sourceArg = source !== 'all' ? ` --source ${shellEscape(source)}` : '';
            execSync(`node ${__filename} schema generate ${shellEscape(d)} --json${sourceArg}${sessionArg}`, { stdio: ['pipe', 'pipe', 'pipe'], timeout: 40000 });
            console.log('✓');
            generated++;
          } catch (e) {
            console.log('✗');
            skipped++;
          }
        }
        console.error(`\nDone: ${generated} generated, ${skipped} skipped`);
        break;
      }
      
      if (!domain) { console.error('Usage: neo schema generate <domain> | neo schema generate --all'); process.exit(1); }
      
      const captures = await loadCapturesFromSources(sessionName, { source, domain }, { cdpUrl });
      const schema = buildSchemaFromCaptures(domain, captures);

      if (!schema.totalCaptures) { console.error(`No captures for ${domain}`); process.exit(1); }
      
      // Save to schema dir (with diff detection)
      fs.mkdirSync(SCHEMA_DIR, { recursive: true });
      const outFile = path.join(SCHEMA_DIR, `${domain}.json`);
      if (fs.existsSync(outFile)) {
        try {
          const prev = JSON.parse(fs.readFileSync(outFile, 'utf8'));
          const prevPaths = new Set((prev.endpoints || []).map(e => `${e.method} ${e.path || e.pathPattern}`));
          const newPaths = new Set((schema.endpoints || []).map(e => `${e.method} ${e.path || e.pathPattern}`));
          const added = [...newPaths].filter(p => !prevPaths.has(p));
          const removed = [...prevPaths].filter(p => !newPaths.has(p));
          if (added.length || removed.length) {
            console.error(`Schema diff: +${added.length} -${removed.length} endpoints`);
            added.forEach(p => console.error(`  + ${p}`));
            removed.forEach(p => console.error(`  - ${p}`));
          }
          // Archive previous version
          const histDir = path.join(SCHEMA_DIR, '.history');
          fs.mkdirSync(histDir, { recursive: true });
          const ts = prev.generatedAt ? prev.generatedAt.replace(/[:.]/g, '-') : Date.now();
          fs.writeFileSync(path.join(histDir, `${domain}.${ts}.json`), fs.readFileSync(outFile));
        } catch {}
      }
      schema.version = (schema.version || 0) + 1;
      fs.writeFileSync(outFile, JSON.stringify(schema, null, 2));
      console.error(`Schema saved to ${outFile} (${schema.totalCaptures} captures → ${schema.uniqueEndpoints} endpoints)`);
      console.log(JSON.stringify(schema, null, 2));
      break;
    }

    case 'show': {
      const domain = positional[1];
      if (!domain) { console.error('Usage: neo schema show <domain>'); process.exit(1); }
      const file = path.join(SCHEMA_DIR, `${domain}.json`);
      if (!fs.existsSync(file)) {
        console.error(`No schema for ${domain}. Run: neo schema generate ${domain}`);
        process.exit(1);
      }
      const schema = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (flags.json) {
        console.log(JSON.stringify(schema, null, 2));
      } else {
        // Human-readable summary
        console.log(`${schema.domain} — ${schema.uniqueEndpoints} endpoints from ${schema.totalCaptures} captures`);
        console.log(`Generated: ${schema.generatedAt}\n`);
        for (const ep of schema.endpoints) {
          const auth = ep.authHeaders?.length ? ` [auth: ${ep.authHeaders.join(', ')}]` : '';
          const params = ep.queryParams?.length ? ` ?${ep.queryParams.join('&')}` : '';
          const body = ep.requestBodyStructure ? ` body:{${Object.keys(ep.requestBodyStructure).join(', ')}}` : '';
          const label = ep.label ? ` [label: ${ep.label}]` : '';
          const variability = ep.bodyFieldVariability
            ? Object.entries(ep.bodyFieldVariability).filter(([,v]) => v === 'variable').map(([k]) => k)
            : [];
          const varNote = variability.length ? ` [varies: ${variability.join(', ')}]` : '';
          const cat = ep.category ? ` (${ep.category})` : '';
          console.log(`  ${ep.method} ${ep.path}${params}  (${ep.callCount}x, ${ep.avgDuration || '?'})${label}${auth}${body}${varNote}${cat}`);
          if (ep.triggers?.length) {
            for (const t of ep.triggers) {
              console.log(`    ← ${t.event} ${t.selector}${t.text ? ' "' + t.text + '"' : ''} (${t.count}x)`);
            }
          }
        }
      }
      break;
    }

    case 'list': {
      if (!fs.existsSync(SCHEMA_DIR)) { console.log('No schemas generated yet.'); break; }
      const files = fs.readdirSync(SCHEMA_DIR).filter(f => f.endsWith('.json'));
      if (!files.length) { console.log('No schemas generated yet.'); break; }
      for (const f of files) {
        try {
          const s = JSON.parse(fs.readFileSync(path.join(SCHEMA_DIR, f), 'utf8'));
          const age = Math.round((Date.now() - new Date(s.generatedAt).getTime()) / 3600000);
          const v = s.version ? `v${s.version}` : 'v1';
          console.log(`${s.domain}  ${s.uniqueEndpoints} endpoints  ${s.totalCaptures} captures  ${age}h ago  ${v}`);
        } catch { console.log(f); }
      }
      break;
    }

    case 'search': {
      const query = positional.slice(1).join(' ').toLowerCase();
      if (!query) { console.error('Usage: neo schema search <query>'); process.exit(1); }
      if (!fs.existsSync(SCHEMA_DIR)) { console.log('No schemas.'); break; }
      const files = fs.readdirSync(SCHEMA_DIR).filter(f => f.endsWith('.json'));
      let found = 0;
      for (const f of files) {
        try {
          const s = JSON.parse(fs.readFileSync(path.join(SCHEMA_DIR, f), 'utf8'));
          for (const ep of (s.endpoints || [])) {
            const searchable = `${ep.method} ${ep.path} ${(ep.queryParams || []).join(' ')} ${ep.category || ''}`.toLowerCase();
            if (searchable.includes(query)) {
              const cat = ep.category ? ` (${ep.category})` : '';
              console.log(`${s.domain}  ${ep.method} ${ep.path}  (${ep.callCount}x)${cat}`);
              found++;
            }
          }
        } catch {}
      }
      if (!found) console.log(`No endpoints matching "${query}" across ${files.length} schemas`);
      break;
    }

    case 'coverage': {
      // Show which domains have schemas vs just captures
      const domainCounts = {};
      const captures = await loadCapturesFromSources(sessionName, { source: 'all' }, { cdpUrl });
      for (const capture of captures) {
        const domain = capture && capture.domain ? String(capture.domain) : '';
        if (!domain) continue;
        domainCounts[domain] = (domainCounts[domain] || 0) + 1;
      }
      const schemaFiles = fs.existsSync(SCHEMA_DIR)
        ? new Set(fs.readdirSync(SCHEMA_DIR).filter(f => f.endsWith('.json')).map(f => f.replace('.json', '')))
        : new Set();
      
      const entries = Object.entries(domainCounts).sort((a, b) => b[1] - a[1]);
      console.log(`Schema coverage: ${schemaFiles.size} schemas / ${entries.length} domains with captures\n`);
      for (const [domain, count] of entries) {
        const hasSchema = schemaFiles.has(domain);
        const icon = hasSchema ? '✓' : '○';
        let detail = '';
        if (hasSchema) {
          try {
            const s = JSON.parse(fs.readFileSync(path.join(SCHEMA_DIR, `${domain}.json`), 'utf8'));
            detail = ` → ${s.uniqueEndpoints} endpoints`;
          } catch {}
        }
        console.log(`  ${icon} ${domain} (${count} captures)${detail}`);
      }
      break;
    }

    case 'openapi': {
      // Convert Neo schema to OpenAPI 3.0 spec
      const domain = positional[1];
      if (!domain) { console.error('Usage: neo schema openapi <domain>'); process.exit(1); }
      const file = path.join(SCHEMA_DIR, `${domain}.json`);
      if (!fs.existsSync(file)) {
        console.error(`No schema for ${domain}. Run: neo schema generate ${domain}`);
        process.exit(1);
      }
      const schema = JSON.parse(fs.readFileSync(file, 'utf8'));
      
      // Convert Neo type structure to JSON Schema
      function neoToJsonSchema(struct) {
        if (!struct || typeof struct !== 'object') return { type: 'object' };
        if (Array.isArray(struct)) {
          return { type: 'array', items: struct.length ? neoToJsonSchema(struct[0]) : {} };
        }
        const properties = {};
        for (const [k, v] of Object.entries(struct)) {
          if (typeof v === 'string') {
            properties[k] = v === 'null' ? {} : { type: v };
          } else if (typeof v === 'object') {
            properties[k] = neoToJsonSchema(v);
          }
        }
        return { type: 'object', properties };
      }
      
      // Build OpenAPI paths
      const paths = {};
      for (const ep of schema.endpoints) {
        const oaPath = ep.path.replace(/:uuid/g, '{uuid}').replace(/:id/g, '{id}').replace(/:hash/g, '{hash}');
        if (!paths[oaPath]) paths[oaPath] = {};
        
        const method = ep.method.toLowerCase();
        if (method.startsWith('ws_') || method.startsWith('sse_')) continue;
        
        const operation = {
          summary: `${ep.category || method} — ${ep.callCount}x observed`,
          tags: [ep.category || 'default'],
        };
        
        // Query parameters
        if (ep.queryParams?.length) {
          operation.parameters = ep.queryParams.map(p => ({
            name: p, in: 'query', schema: { type: 'string' },
          }));
        }
        
        // Path parameters
        const pathParams = oaPath.match(/\{(\w+)\}/g);
        if (pathParams) {
          operation.parameters = [
            ...(operation.parameters || []),
            ...pathParams.map(p => ({
              name: p.slice(1, -1), in: 'path', required: true, schema: { type: 'string' },
            })),
          ];
        }
        
        // Security
        if (ep.authHeaders?.length) {
          operation.security = [{ apiKey: [] }];
        }
        
        // Request body
        if (ep.requestBodyStructure && ['post', 'put', 'patch'].includes(method)) {
          operation.requestBody = {
            content: { 'application/json': { schema: neoToJsonSchema(ep.requestBodyStructure) } },
          };
        }
        
        // Responses
        const responses = {};
        for (const [code, count] of Object.entries(ep.statusCodes || {})) {
          responses[code] = {
            description: `${count}x observed`,
            ...(ep.responseBodyStructure && parseInt(code) >= 200 && parseInt(code) < 300
              ? { content: { 'application/json': { schema: neoToJsonSchema(ep.responseBodyStructure) } } }
              : {}),
          };
        }
        operation.responses = Object.keys(responses).length ? responses : { '200': { description: 'OK' } };
        
        paths[oaPath][method] = operation;
      }
      
      // Security schemes
      const allAuthHeaders = new Set();
      for (const ep of schema.endpoints) {
        for (const h of (ep.authHeaders || [])) allAuthHeaders.add(h);
      }
      const components = {};
      if (allAuthHeaders.size) {
        components.securitySchemes = {
          apiKey: {
            type: 'apiKey', in: 'header',
            name: [...allAuthHeaders][0],
            description: `Auth headers: ${[...allAuthHeaders].join(', ')}`,
          },
        };
      }
      
      const openapi = {
        openapi: '3.0.3',
        info: {
          title: `${domain} API (discovered by Neo)`,
          description: `Auto-generated from ${schema.totalCaptures} captured API calls across ${schema.uniqueEndpoints} endpoints.`,
          version: `${schema.version || 1}.0.0`,
        },
        servers: [{ url: `https://${domain}` }],
        paths,
        ...(Object.keys(components).length ? { components } : {}),
      };
      
      console.log(JSON.stringify(openapi, null, 2));
      break;
    }

    case 'diff': {
      const domain = positional[1];
      if (!domain) { console.error('Usage: neo schema diff <domain>'); process.exit(1); }
      
      const currentFile = path.join(SCHEMA_DIR, `${domain}.json`);
      if (!fs.existsSync(currentFile)) {
        console.error(`No schema for ${domain}. Run: neo schema generate ${domain}`);
        process.exit(1);
      }
      
      const histDir = path.join(SCHEMA_DIR, '.history');
      if (!fs.existsSync(histDir)) {
        console.log('No history available (first schema version)');
        break;
      }
      
      const prefix = domain + '.';
      const histFiles = fs.readdirSync(histDir)
        .filter(f => f.startsWith(prefix) && f.endsWith('.json'))
        .sort()
        .reverse();
      
      if (histFiles.length === 0) {
        console.log('No previous versions found');
        break;
      }
      
      const current = JSON.parse(fs.readFileSync(currentFile, 'utf8'));
      const previous = JSON.parse(fs.readFileSync(path.join(histDir, histFiles[0]), 'utf8'));
      
      const currentEndpoints = new Set((current.endpoints || []).map(e => `${e.method} ${e.path}`));
      const previousEndpoints = new Set((previous.endpoints || []).map(e => `${e.method} ${e.path}`));
      
      const added = [...currentEndpoints].filter(e => !previousEndpoints.has(e));
      const removed = [...previousEndpoints].filter(e => !currentEndpoints.has(e));
      const shared = [...currentEndpoints].filter(e => previousEndpoints.has(e));
      
      const currentMap = new Map((current.endpoints || []).map(e => [`${e.method} ${e.path}`, e]));
      const previousMap = new Map((previous.endpoints || []).map(e => [`${e.method} ${e.path}`, e]));
      
      const changed = [];
      for (const key of shared) {
        const c = currentMap.get(key);
        const p = previousMap.get(key);
        const diffs = [];
        if (c.callCount !== p.callCount) diffs.push(`calls: ${p.callCount} → ${c.callCount}`);
        const cStatuses = Object.keys(c.statusCodes || {}).sort().join(',');
        const pStatuses = Object.keys(p.statusCodes || {}).sort().join(',');
        if (cStatuses !== pStatuses) diffs.push(`status codes: ${pStatuses} → ${cStatuses}`);
        if (diffs.length) changed.push({ key, diffs });
      }
      
      const prevTs = histFiles[0].replace(prefix, '').replace('.json', '');
      const prevDate = prevTs.replace(/T/, ' ').replace(/-(\d{2})-(\d{2})-(\d{3})/, ':$1:$2.$3').slice(0, 19);
      console.log(`Schema diff for ${domain}`);
      console.log(`  Previous: ${prevDate} (${previousEndpoints.size} endpoints)`);
      console.log(`  Current:  ${current.generatedAt?.slice(0, 19).replace('T', ' ')} (${currentEndpoints.size} endpoints)`);
      console.log(`  History versions: ${histFiles.length}\n`);
      
      if (added.length === 0 && removed.length === 0 && changed.length === 0) {
        console.log('  No changes detected');
        break;
      }
      
      if (added.length > 0) {
        console.log(`  + ${added.length} new endpoint(s):`);
        for (const e of added) console.log(`    + ${e}`);
        console.log();
      }
      if (removed.length > 0) {
        console.log(`  - ${removed.length} removed endpoint(s):`);
        for (const e of removed) console.log(`    - ${e}`);
        console.log();
      }
      if (changed.length > 0) {
        console.log(`  ~ ${changed.length} changed endpoint(s):`);
        for (const c of changed) console.log(`    ~ ${c.key}  (${c.diffs.join(', ')})`);
        console.log();
      }
      break;
    }

    default:
      console.log(`neo schema — API schema management

  neo schema list                 List all cached schemas
  neo schema generate <domain>    Generate schema from captures (--all for all domains)
  neo schema show <domain>        Show cached schema (--json for raw)
  neo schema search <query>       Search all schemas for matching endpoints
  neo schema coverage             Show which domains have schemas vs just captures
  neo schema openapi <domain>     Export as OpenAPI 3.0 spec
  neo schema diff <domain>        Show changes from previous schema version`);
  }
};

// neo exec <url> [options]
commands.exec = async function(args, context = {}) {
  const { positional, flags } = parseArgs(args);
  const url = positional[0];
  if (!url) { console.error('Usage: neo exec <url> [--method POST] [--header "K: V"] [--body "{}"] [--tab pattern] [--auto-headers]'); process.exit(1); }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const cdpUrl = getSessionCdpUrl(sessionName);
  const method = (flags.method || 'GET').toUpperCase();
  const tabPattern = flags.tab || flags['tab-url'] || null;
  const body = flags.body || null;
  const headers = {};
  const extensionWsUrl = await findExtensionWs({ cdpUrl });
  const domain = new URL(url).hostname;

  // Parse --header flags (may appear multiple times in raw argv)
  const rawArgs = process.argv.slice(3); // skip node, script, 'exec'
  for (let i = 0; i < rawArgs.length; i++) {
    if (rawArgs[i] === '--header' && rawArgs[i + 1]) {
      const h = rawArgs[++i];
      const colon = h.indexOf(':');
      if (colon > 0) headers[h.slice(0, colon).trim()] = h.slice(colon + 1).trim();
    }
  }

  let tab;
  if (tabPattern) {
    tab = await findTab(tabPattern, { cdpUrl });
  } else {
    try { tab = await findTab(domain, { cdpUrl }); }
    catch { tab = await findTab(null, { cdpUrl }); }
  }
  if (!tab || !tab.webSocketDebuggerUrl) {
    throw new Error(`No page target found for ${cdpUrl}`);
  }

  // Auto-detect auth headers from live browser traffic, with legacy capture fallback.
  if (flags['auto-headers'] !== undefined || !Object.keys(headers).some(k => k.toLowerCase() === 'authorization')) {
    try {
      const probeUrl = `${new URL(url).origin}/`;
      const { liveHeaders, fallbackHeaders } = await collectExecutionAuthHeaders({
        wsUrl: extensionWsUrl,
        tab,
        domain,
        probeUrl,
      });

      const fromLive = applyAuthHeaders(headers, liveHeaders, false);
      const fromFallback = applyAuthHeaders(headers, fallbackHeaders, false);

      if (fromLive + fromFallback > 0) {
        const segments = [];
        if (fromLive > 0) segments.push(`${fromLive} live`);
        if (fromFallback > 0) segments.push(`${fromFallback} fallback`);
        console.error(`Auto-detected ${fromLive + fromFallback} auth headers (${segments.join(', ')})`);
      }
    } catch {}
  }

  stripForbiddenFetchHeaders(headers);
  const tabUrl = String(tab.url || '');
  console.error(`${method} ${url.slice(0, 80)}... → tab: ${tabUrl.slice(0, 50)}...`);

  const fetchOpts = { method, headers, credentials: 'include' };
  if (body && method !== 'GET') {
    fetchOpts.body = body;
    if (!headers['content-type'] && !headers['Content-Type']) {
      headers['Content-Type'] = 'application/json';
    }
  }

  const result = await cdpEval(tab.webSocketDebuggerUrl, `
    (async function() {
      try {
        var resp = await fetch(${JSON.stringify(url)}, ${JSON.stringify(fetchOpts)});
        var text = await resp.text();
        return JSON.stringify({
          status: resp.status, statusText: resp.statusText,
          headers: Object.fromEntries(resp.headers.entries()),
          body: text.length > 50000 ? text.slice(0, 50000) + '...[truncated]' : text
        });
      } catch (err) { return JSON.stringify({ error: err.message }); }
    })()
  `);

  const parsed = JSON.parse(result);
  if (parsed.error) { console.error('Error:', parsed.error); process.exit(1); }
  console.log(`HTTP ${parsed.status} ${parsed.statusText}`);
  console.log('---');
  try { console.log(JSON.stringify(JSON.parse(parsed.body), null, 2)); }
  catch { console.log(parsed.body); }
};

// neo eval <js> --tab <pattern>
commands.eval = async function(args) {
  const { positional, flags } = parseArgs(args);
  const js = positional.join(' ');
  if (!js) { console.error('Usage: neo eval "<js>" --tab <pattern>'); process.exit(1); }
  const tab = await findTab(flags.tab || flags['tab-url'] || null);
  console.error(`Evaluating in: ${tab.url.slice(0, 60)}...`);
  const result = await cdpEval(tab.webSocketDebuggerUrl, `
    (async function() {
      try { var r = await (${js}); return typeof r === 'object' ? JSON.stringify(r, null, 2) : String(r); }
      catch(e) { return 'Error: ' + e.message; }
    })()
  `);
  console.log(result);
};

// neo open <url>
commands.open = async function(args, context = {}) {
  const url = args[0];
  if (!url) { console.error('Usage: neo open <url>'); process.exit(1); }

  const sessionName = (context && context.sessionName) || DEFAULT_SESSION_NAME;
  const session = getSession(sessionName);
  const cdpUrl = (session && session.cdpUrl) || CDP_URL;

  // Detect if backend supports /json/list (Chrome) or not (Lightpanda)
  let usedChromeEndpoint = false;
  try {
    const listResp = await fetch(`${cdpUrl}/json/new?${url}`, { method: 'PUT' });
    const tab = await listResp.json();
    if (tab && tab.url) {
      console.log(`Opened: ${tab.url}`);
      usedChromeEndpoint = true;

      // Update session to point to the new tab
      if (tab.webSocketDebuggerUrl) {
        setSession(sessionName, {
          ...(session || {}),
          cdpUrl,
          pageWsUrl: tab.webSocketDebuggerUrl,
          tabId: tab.id || tab.targetId || '',
          currentUrl: tab.url || url,
          refs: {},
        });
      }
    }
  } catch {
    // /json/new not available
  }

  if (!usedChromeEndpoint) {
    // Lightpanda path: create a new target with the URL
    const versionInfo = await fetchJsonOrThrow(`${cdpUrl}/json/version`);
    const browserWsUrl = versionInfo.webSocketDebuggerUrl;
    if (!browserWsUrl) {
      throw new Error(`Cannot open URL: no browser WebSocket endpoint available`);
    }
    const created = await cdpSend(browserWsUrl, 'Target.createTarget', { url });
    const targetId = created && created.targetId;
    if (!targetId) {
      throw new Error(`Failed to create target for ${url}`);
    }
    // Attach to get a session
    try {
      await cdpSend(browserWsUrl, 'Target.attachToTarget', { targetId, flatten: true });
    } catch {}
    setSession(sessionName, {
      cdpUrl,
      pageWsUrl: browserWsUrl,
      tabId: targetId,
      currentUrl: url,
      refs: {},
    });
    console.log(`Opened: ${url}`);
  }
};

// neo replay <id> [--tab pattern] [--auto-headers]
commands.replay = async function(args, context = {}) {
  const { positional, flags } = parseArgs(args);
  const id = positional[0];
  if (!id) { console.error('Usage: neo replay <capture-id> [--tab pattern] [--auto-headers]'); process.exit(1); }

  const sessionName = context.sessionName || DEFAULT_SESSION_NAME;
  const cdpUrl = getSessionCdpUrl(sessionName);
  const extensionWsUrl = await findExtensionWs({ cdpUrl });
  const capture = await findCaptureById(sessionName, id, { sort: 'timestamp-desc', source: 'all' }, { cdpUrl, extensionWsUrl });
  if (!capture) { console.error(`Capture not found: ${id}`); process.exit(1); }

  if (capture.method.startsWith('WS_')) {
    console.error('Cannot replay WebSocket captures. Use neo exec for HTTP calls.');
    process.exit(1);
  }

  const tabPattern = flags.tab || capture.domain;
  const tab = await findTab(tabPattern, { cdpUrl });
  if (!tab || !tab.webSocketDebuggerUrl) {
    throw new Error(`No page target found for ${cdpUrl}`);
  }
  console.error(`Replaying: ${capture.method} ${capture.url.slice(0, 80)}...`);
  console.error(`Target tab: ${String(tab.url || '').slice(0, 60)}...`);

  const headers = { ...(capture.requestHeaders || {}) };

  if (flags['auto-headers'] !== undefined || hasRedactedAuthHeaders(headers)) {
    try {
      const probeUrl = `${new URL(capture.url).origin}/`;
      const { liveHeaders, fallbackHeaders } = await collectExecutionAuthHeaders({
        wsUrl: extensionWsUrl,
        tab,
        domain: capture.domain,
        probeUrl,
      });
      const fromLive = applyAuthHeaders(headers, liveHeaders, true);
      const fromFallback = applyAuthHeaders(headers, fallbackHeaders, false);
      if (fromLive + fromFallback > 0) {
        const segments = [];
        if (fromLive > 0) segments.push(`${fromLive} live`);
        if (fromFallback > 0) segments.push(`${fromFallback} fallback`);
        console.error(`Auto-detected ${fromLive + fromFallback} auth headers (${segments.join(', ')})`);
      }
    } catch {}
  }

  dropRedactedAuthHeaders(headers);
  stripForbiddenFetchHeaders(headers);

  const fetchOpts = { method: capture.method, headers, credentials: 'include' };
  if (capture.requestBody && capture.method !== 'GET') {
    fetchOpts.body = typeof capture.requestBody === 'string'
      ? capture.requestBody : JSON.stringify(capture.requestBody);
  }

  const result = await cdpEval(tab.webSocketDebuggerUrl, `
    (async function() {
      try {
        var resp = await fetch(${JSON.stringify(capture.url)}, ${JSON.stringify(fetchOpts)});
        var text = await resp.text();
        return JSON.stringify({
          status: resp.status, statusText: resp.statusText,
          headers: Object.fromEntries(resp.headers.entries()),
          body: text.length > 50000 ? text.slice(0, 50000) + '...[truncated]' : text
        });
      } catch (err) { return JSON.stringify({ error: err.message }); }
    })()
  `);

  const parsed = JSON.parse(result);
  if (parsed.error) { console.error('Error:', parsed.error); process.exit(1); }
  console.log(`HTTP ${parsed.status} ${parsed.statusText}`);
  console.log('---');
  try { console.log(JSON.stringify(JSON.parse(parsed.body), null, 2)); }
  catch { console.log(parsed.body); }
};

// neo read <tab-pattern>
commands.read = async function(args) {
  const pattern = args[0];
  if (!pattern) { console.error('Usage: neo read <tab-pattern>'); process.exit(1); }
  const tab = await findTab(pattern);
  const result = await cdpEval(tab.webSocketDebuggerUrl, `
    (function() {
      var main = document.querySelector('main, article, [role="main"], .content, #content');
      var el = main || document.body;
      var walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT, {
        acceptNode: function(node) {
          var p = node.parentElement;
          if (!p) return NodeFilter.FILTER_REJECT;
          var t = p.tagName.toLowerCase();
          if (['script','style','noscript','svg','path'].includes(t)) return NodeFilter.FILTER_REJECT;
          if (p.offsetHeight === 0 || p.hidden) return NodeFilter.FILTER_REJECT;
          return NodeFilter.FILTER_ACCEPT;
        }
      });
      var texts = [], n;
      while (n = walker.nextNode()) { var t = n.textContent.trim(); if (t.length > 1) texts.push(t); }
      return 'Title: ' + document.title + '\\nURL: ' + location.href + '\\n\\n' + texts.join('\\n');
    })()
  `);
  console.log(result);
};

// ─── Bridge Server ──────────────────────────────────────────────

commands.bridge = async function(args) {
  const { WebSocketServer } = require('ws');
  const http = require('http');
  const port = parseInt(args.find(a => /^\d+$/.test(a)) || '9234', 10);
  const json = args.includes('--json');
  const quiet = args.includes('--quiet');
  const wsOnly = args.includes('--ws-only');

  const wss = new WebSocketServer({ noServer: true });
  const clients = new Set();
  const pendingResponses = new Map(); // id → { resolve, timer }
  let cmdIdCounter = 0;

  function sendCommand(cmd, cmdArgs) {
    return new Promise((resolve, reject) => {
      if (clients.size === 0) { reject(new Error('no extension connected')); return; }
      const id = String(++cmdIdCounter);
      const timer = setTimeout(() => { pendingResponses.delete(id); reject(new Error('timeout')); }, 10000);
      pendingResponses.set(id, { resolve, timer });
      const msg = JSON.stringify({ id, cmd, args: cmdArgs });
      for (const c of clients) c.send(msg);
    });
  }

  if (!quiet) {
    process.stderr.write(`[Neo Bridge] listening on ws://127.0.0.1:${port}\n`);
    if (!wsOnly) process.stderr.write(`[Neo Bridge] REST API at http://127.0.0.1:${port}\n`);
    process.stderr.write(`[Neo Bridge] waiting for extension to connect...\n`);
  }

  // ─── HTTP REST handler ───────────────────────────────────
  function jsonResponse(res, status, data) {
    const body = JSON.stringify(data, null, 2);
    res.writeHead(status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(body);
  }

  function readBody(req) {
    return new Promise((resolve) => {
      let body = '';
      req.on('data', c => body += c);
      req.on('end', () => resolve(body));
    });
  }

  async function handleHttpRequest(req, res) {
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
      });
      res.end();
      return;
    }

    const url = new URL(req.url, 'http://localhost');
    const pathname = url.pathname;

    try {
      // GET /status
      if (pathname === '/status' && req.method === 'GET') {
        return jsonResponse(res, 200, { ok: true, data: { clients: clients.size, uptime: process.uptime() } });
      }

      // GET /captures?domain=...&limit=...
      if (pathname === '/captures' && req.method === 'GET') {
        const cmdArgs = {};
        if (url.searchParams.has('domain')) cmdArgs.domain = url.searchParams.get('domain');
        if (url.searchParams.has('limit')) cmdArgs.limit = parseInt(url.searchParams.get('limit'));
        const resp = await sendCommand('capture.list', cmdArgs);
        return jsonResponse(res, 200, { ok: true, data: resp.result });
      }

      // GET /captures/count
      if (pathname === '/captures/count' && req.method === 'GET') {
        const resp = await sendCommand('capture.count', {});
        return jsonResponse(res, 200, { ok: true, data: resp.result });
      }

      // GET /captures/domains
      if (pathname === '/captures/domains' && req.method === 'GET') {
        const resp = await sendCommand('capture.domains', {});
        return jsonResponse(res, 200, { ok: true, data: resp.result });
      }

      // GET /tabs
      if (pathname === '/tabs' && req.method === 'GET') {
        const tabs = await (await fetch(`${CDP_URL}/json/list`)).json();
        const pages = tabs.filter(t => t.type === 'page');
        return jsonResponse(res, 200, { ok: true, data: pages.map(t => ({ title: t.title, url: t.url, id: t.id })) });
      }

      // GET /schemas
      if (pathname === '/schemas' && req.method === 'GET') {
        const files = fs.existsSync(SCHEMA_DIR) ? fs.readdirSync(SCHEMA_DIR).filter(f => f.endsWith('.json')) : [];
        const schemas = files.map(f => f.replace('.json', ''));
        return jsonResponse(res, 200, { ok: true, data: schemas });
      }

      // GET /schemas/:domain
      if (pathname.startsWith('/schemas/') && req.method === 'GET') {
        const domain = pathname.slice('/schemas/'.length);
        const schemaPath = path.join(SCHEMA_DIR, `${domain}.json`);
        if (!fs.existsSync(schemaPath)) {
          return jsonResponse(res, 404, { ok: false, error: `No schema for ${domain}` });
        }
        const schema = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));
        return jsonResponse(res, 200, { ok: true, data: schema });
      }

      // GET /snapshot?interactive=true
      if (pathname === '/snapshot' && req.method === 'GET') {
        const sessionName = DEFAULT_SESSION_NAME;
        const session = getSession(sessionName);
        if (!session || !session.pageWsUrl) {
          return jsonResponse(res, 400, { ok: false, error: 'No active session. Run neo connect first.' });
        }
        await cdpSend(session.pageWsUrl, 'Accessibility.enable');
        const treeResult = await cdpSend(session.pageWsUrl, 'Accessibility.getFullAXTree');
        const assigned = assignRefs(treeResult && Array.isArray(treeResult.nodes) ? treeResult.nodes : []);
        const interactiveOnly = url.searchParams.get('interactive') === 'true';
        const nodes = interactiveOnly
          ? assigned.nodes.filter(node => INTERACTIVE_ROLES.has(String(node.role || '').toLowerCase()))
          : assigned.nodes;
        setSession(sessionName, { ...session, refs: assigned.refs });
        return jsonResponse(res, 200, { ok: true, data: { count: nodes.length, nodes } });
      }

      // POST /eval  { expression: "..." }
      if (pathname === '/eval' && req.method === 'POST') {
        const body = JSON.parse(await readBody(req));
        const sessionName = DEFAULT_SESSION_NAME;
        const session = getSession(sessionName);
        if (!session || !session.pageWsUrl) {
          return jsonResponse(res, 400, { ok: false, error: 'No active session' });
        }
        const result = await cdpSend(session.pageWsUrl, 'Runtime.evaluate', {
          expression: body.expression,
          returnByValue: true,
        });
        return jsonResponse(res, 200, { ok: true, data: result.result });
      }

      return jsonResponse(res, 404, { ok: false, error: 'Not found' });
    } catch (err) {
      return jsonResponse(res, 500, { ok: false, error: err.message });
    }
  }

  // ─── HTTP Server + WebSocket upgrade ─────────────────────
  const server = http.createServer(wsOnly ? (req, res) => {
    jsonResponse(res, 404, { ok: false, error: 'REST disabled (--ws-only)' });
  } : handleHttpRequest);

  server.on('upgrade', (req, socket, head) => {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit('connection', ws, req);
    });
  });

  server.listen(port);

  wss.on('connection', (ws) => {
    clients.add(ws);
    if (!quiet) process.stderr.write(`[Neo Bridge] extension connected (${clients.size} client${clients.size > 1 ? 's' : ''})\n`);

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch { return; }

      if (msg.type === 'capture') {
        const c = msg.data;
        if (json) {
          process.stdout.write(JSON.stringify(c) + '\n');
        } else {
          const method = (c.method || '???').padEnd(6);
          const status = c.status || '---';
          const src = c.source === 'websocket' ? ' [ws]' : c.source === 'eventsource' ? ' [sse]' : '';
          const dur = c.duration ? ` ${c.duration}ms` : '';
          const trigger = c.trigger ? ` \u2190 ${c.trigger.event}(${c.trigger.text || c.trigger.selector})` : '';
          const ts = new Date(c.timestamp).toLocaleTimeString();
          process.stdout.write(`${ts} ${method} ${status} ${c.url}${src}${dur}${trigger}\n`);
        }
      } else if (msg.type === 'response') {
        const d = msg.data;
        if (d && d.id && pendingResponses.has(d.id)) {
          const { resolve, timer } = pendingResponses.get(d.id);
          clearTimeout(timer);
          pendingResponses.delete(d.id);
          resolve(d);
        }
      }
    });

    ws.on('close', () => {
      clients.delete(ws);
      if (!quiet) process.stderr.write(`[Neo Bridge] extension disconnected (${clients.size} client${clients.size > 1 ? 's' : ''})\n`);
    });
  });

  // Interactive mode: read commands from stdin
  if (process.stdin.isTTY || args.includes('--interactive')) {
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin });
    if (!quiet) process.stderr.write('[Neo Bridge] interactive — commands: ping, status, capture.count, capture.list [domain] [limit]\n');
    rl.on('line', async (line) => {
      const parts = line.trim().split(/\s+/);
      if (!parts[0]) return;
      try {
        const cmdArgs = {};
        if (parts[1]) cmdArgs.domain = parts[1];
        if (parts[2]) cmdArgs.limit = parseInt(parts[2]);
        const resp = await sendCommand(parts[0], cmdArgs);
        console.log(JSON.stringify(resp.result, null, 2));
        if (resp.error) process.stderr.write(`Error: ${resp.error}\n`);
      } catch (e) {
        process.stderr.write(`Error: ${e.message}\n`);
      }
    });
  }

  // Keep process alive
  await new Promise(() => {});
};

// ─── Smart API Call ─────────────────────────────────────────────

commands.api = async function(args) {
  const { positional, flags } = parseArgs(args);
  const domain = positional[0];
  const query = positional.slice(1).join(' ');

  if (!domain || !query) {
    console.log(`Usage: neo api <domain> <search-term> [--body '{}'] [--method POST]

Search schema for a matching endpoint and execute it with auto-detected auth.

Examples:
  neo api x.com CreateTweet --body '{"variables":{"tweet_text":"hello"}}'
  neo api github.com notifications
  neo api x.com HomeTimeline`);
    return;
  }

  // Load schema
  const schemaFile = path.join(SCHEMA_DIR, `${domain}.json`);
  if (!fs.existsSync(schemaFile)) {
    console.error(`No schema for ${domain}. Run: neo schema generate ${domain}`);
    process.exit(1);
  }
  const schema = JSON.parse(fs.readFileSync(schemaFile, 'utf8'));

  // Search endpoints by name/path
  const queryLower = query.toLowerCase();
  const matches = schema.endpoints.filter(ep => {
    const path = (ep.pathPattern || ep.path || '').toLowerCase();
    return path.includes(queryLower);
  });

  if (matches.length === 0) {
    console.error(`No endpoint matching "${query}" in ${domain} schema`);
    console.error('Available endpoints:');
    for (const ep of schema.endpoints.slice(0, 15)) {
      console.error(`  ${ep.method} ${ep.pathPattern || ep.path}`);
    }
    process.exit(1);
  }

  // Use first match (or exact match if available)
  const exact = matches.find(ep => (ep.pathPattern || ep.path || '').toLowerCase().endsWith(queryLower.toLowerCase()));
  const endpoint = exact || matches[0];

  if (matches.length > 1) {
    console.error(`Found ${matches.length} matches, using: ${endpoint.method} ${endpoint.pathPattern || endpoint.path}`);
    if (matches.length <= 5) {
      for (const m of matches) {
        const mark = m === endpoint ? '  →' : '   ';
        console.error(`${mark} ${m.method} ${m.pathPattern || m.path}`);
      }
    }
  }

  // Build URL — need an actual capture to get the real URL with hash
  const wsUrl = await findExtensionWs();
  const pathPattern = endpoint.pathPattern || endpoint.path;

  // Find a recent capture matching this endpoint pattern
  const captureUrl = await cdpEval(wsUrl, `
    (async () => {
      const db = await new Promise((resolve, reject) => {
        const r = indexedDB.open("${DB_NAME}");
        r.onsuccess = () => resolve(r.result);
        r.onerror = () => reject(r.error);
      });
      return new Promise((resolve) => {
        const tx = db.transaction("${STORE_NAME}", "readonly");
        const store = tx.objectStore("${STORE_NAME}");
        const idx = store.index("domain");
        let found = null;
        idx.openCursor(IDBKeyRange.only(${JSON.stringify(domain)}), "prev").onsuccess = function(e) {
          const cursor = e.target.result;
          if (cursor) {
            const v = cursor.value;
            const url = new URL(v.url);
            const normalized = url.pathname
              .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, ':uuid')
              .replace(/\\/\\d{4,}\\b/g, '/:id')
              .replace(/\\/[0-9a-f]{24,}/gi, '/:hash');
            // Check hash parameterization for GraphQL
            const segments = url.pathname.split('/');
            let parameterized = normalized;
            for (const seg of segments) {
              if (seg.length >= 10) {
                let switches = 0;
                for (let i = 1; i < seg.length; i++) {
                  const prevIsDigit = /\\d/.test(seg[i-1]);
                  const currIsDigit = /\\d/.test(seg[i]);
                  if (prevIsDigit !== currIsDigit) switches++;
                }
                if (switches >= 3) parameterized = parameterized.replace(seg, ':hash');
              }
            }
            if (v.method === ${JSON.stringify(endpoint.method)} && parameterized.toLowerCase().includes(${JSON.stringify(queryLower)})) {
              found = v.url;
              resolve(found);
              return;
            }
            cursor.continue();
          } else {
            resolve(found);
          }
        };
      });
    })()
  `, 30000);

  if (!captureUrl) {
    console.error(`No capture found matching ${endpoint.method} ...${query}. Cannot reconstruct full URL.`);
    console.error(`Try: neo exec <full-url> --auto-headers`);
    process.exit(1);
  }

  console.error(`Matched: ${endpoint.method} ${captureUrl.slice(0, 100)}${captureUrl.length > 100 ? '...' : ''}`);

  // Delegate to exec with auto-headers, auto-detect tab from domain
  const execArgs = [captureUrl, '--method', flags.method || endpoint.method, '--auto-headers'];
  if (flags.body) execArgs.push('--body', flags.body);
  execArgs.push('--tab', flags.tab || domain);

  // Rewrite process.argv for exec's rawArgs parsing
  const savedArgv = process.argv;
  process.argv = ['node', 'neo', 'exec', ...execArgs];
  try {
    await commands.exec(execArgs);
  } finally {
    process.argv = savedArgv;
  }
};

// ─── Flow Analysis ──────────────────────────────────────────────

commands.flows = async function(args) {
  const domain = args[0];
  if (!domain) {
    console.log(`Usage: neo flows <domain> [--window <ms>] [--min-count <n>]
    
Analyze API call sequences to discover multi-step workflows.
Groups temporally adjacent calls (within window) into "flows",
then finds recurring patterns.

Options:
  --window <ms>     Max gap between calls in a flow (default: 2000)
  --min-count <n>   Min occurrences to show a pattern (default: 2)`);
    return;
  }

  const flags = {};
  for (let i = 1; i < args.length; i++) {
    if (args[i] === '--window' && args[i+1]) { flags.window = parseInt(args[i+1]); i++; }
    if (args[i] === '--min-count' && args[i+1]) { flags.minCount = parseInt(args[i+1]); i++; }
  }
  const windowMs = flags.window || 2000;
  const minCount = flags.minCount || 2;

  const wsUrl = await findExtensionWs();

  // Fetch all captures for domain, sorted by timestamp
  const captures = await cdpEval(wsUrl, `
    (async () => {
      const db = await new Promise((resolve, reject) => {
        const r = indexedDB.open("${DB_NAME}");
        r.onsuccess = () => resolve(r.result);
        r.onerror = () => reject(r.error);
      });
      return new Promise((resolve, reject) => {
        const tx = db.transaction("${STORE_NAME}", "readonly");
        const store = tx.objectStore("${STORE_NAME}");
        const idx = store.index("domain");
        const results = [];
        idx.openCursor(IDBKeyRange.only(${JSON.stringify(domain)})).onsuccess = function(e) {
          const cursor = e.target.result;
          if (cursor) {
            const v = cursor.value;
            results.push({ method: v.method, url: v.url, timestamp: v.timestamp, status: v.responseStatus, trigger: v.trigger });
            cursor.continue();
          } else {
            resolve(results);
          }
        };
        tx.onerror = () => reject(tx.error);
      });
    })()
  `, 60000);

  if (!captures || captures.length === 0) {
    console.error(`No captures for ${domain}`);
    process.exit(1);
  }

  // Normalize URLs (remove query params, parameterize IDs)
  function normalizeUrl(url) {
    try {
      const u = new URL(url);
      let p = u.pathname;
      // Parameterize: UUIDs, numeric IDs, hex hashes
      p = p.replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, ':uuid');
      p = p.replace(/\/\d{4,}\b/g, '/:id');
      p = p.replace(/\/[0-9a-f]{24,}/gi, '/:hash');
      return `${u.hostname}${p}`;
    } catch { return url; }
  }

  // Sort by timestamp
  captures.sort((a, b) => a.timestamp - b.timestamp);

  // Group into flows (sequences with gaps < windowMs)
  const flows = [];
  let currentFlow = [captures[0]];
  for (let i = 1; i < captures.length; i++) {
    if (captures[i].timestamp - captures[i-1].timestamp <= windowMs) {
      currentFlow.push(captures[i]);
    } else {
      if (currentFlow.length >= 2) flows.push(currentFlow);
      currentFlow = [captures[i]];
    }
  }
  if (currentFlow.length >= 2) flows.push(currentFlow);

  // Convert flows to normalized sequences
  const sequenceMap = new Map(); // normalized key → { count, examples }
  for (const flow of flows) {
    const steps = flow.map(c => `${c.method} ${normalizeUrl(c.url)}`);
    const key = steps.join(' → ');
    if (!sequenceMap.has(key)) {
      sequenceMap.set(key, { count: 0, steps, example: flow });
    }
    sequenceMap.get(key).count++;
  }

  // Filter by minCount and sort by frequency
  const patterns = [...sequenceMap.values()]
    .filter(p => p.count >= minCount)
    .sort((a, b) => b.count - a.count);

  if (patterns.length === 0) {
    console.log(`No recurring flow patterns found for ${domain} (window: ${windowMs}ms, min: ${minCount}x)`);
    console.log(`  Total captures: ${captures.length}, flows detected: ${flows.length}`);
    return;
  }

  console.log(`Flow patterns for ${domain} (${patterns.length} patterns from ${flows.length} flows)\n`);
  for (const p of patterns.slice(0, 20)) {
    console.log(`  ${p.count}x  [${p.steps.length} steps]`);
    for (const step of p.steps) {
      console.log(`       ${step}`);
    }
    // Show trigger info from example if available
    const triggers = p.example.filter(c => c.trigger);
    if (triggers.length > 0) {
      const t = triggers[0].trigger;
      console.log(`       ← triggered by ${t.event}(${t.text || t.selector || '?'})`);
    }
    console.log();
  }

  // Also show "pair frequency" — which endpoints commonly co-occur
  const pairCount = new Map();
  for (const flow of flows) {
    const endpoints = [...new Set(flow.map(c => `${c.method} ${normalizeUrl(c.url)}`))];
    for (let i = 0; i < endpoints.length; i++) {
      for (let j = i + 1; j < endpoints.length; j++) {
        const pair = [endpoints[i], endpoints[j]].sort().join(' + ');
        pairCount.set(pair, (pairCount.get(pair) || 0) + 1);
      }
    }
  }

  const topPairs = [...pairCount.entries()]
    .filter(([, c]) => c >= minCount)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);

  if (topPairs.length > 0) {
    console.log(`Common API pairs:`);
    for (const [pair, count] of topPairs) {
      console.log(`  ${count}x  ${pair}`);
    }
  }
};

// neo suggest — suggest actions based on captured schema
commands.suggest = async function(args) {
  const { positional } = parseArgs(args || []);
  const domain = positional[0];
  if (!domain) {
    console.log(`Usage: neo suggest <domain>

Analyze a domain's schema and suggest what an AI agent can do with it.
Groups endpoints by category and suggests high-level capabilities.`);
    return;
  }

  const schemaFile = path.join(SCHEMA_DIR, `${domain}.json`);
  if (!fs.existsSync(schemaFile)) {
    console.error(`No schema for ${domain}. Run: neo schema generate ${domain}`);
    process.exit(1);
  }

  const schema = JSON.parse(fs.readFileSync(schemaFile, 'utf8'));
  const endpoints = schema.endpoints || [];

  // Group by category
  const categories = {};
  for (const ep of endpoints) {
    const cat = ep.category || 'unknown';
    if (!categories[cat]) categories[cat] = [];
    categories[cat].push(ep);
  }

  // Detect capabilities
  const capabilities = [];
  const readEndpoints = endpoints.filter(e => e.method === 'GET' || e.category === 'read');
  const writeEndpoints = endpoints.filter(e => ['POST', 'PUT', 'PATCH', 'DELETE'].includes(e.method));
  const wsEndpoints = endpoints.filter(e => e.method?.startsWith('WS_') || e.source === 'websocket');
  const sseEndpoints = endpoints.filter(e => e.method?.startsWith('SSE_') || e.source === 'eventsource');
  const authedEndpoints = endpoints.filter(e => e.authHeaders?.length > 0);

  if (readEndpoints.length > 0) capabilities.push(`📖 Read data (${readEndpoints.length} endpoints)`);
  if (writeEndpoints.length > 0) capabilities.push(`✏️  Write/mutate (${writeEndpoints.length} endpoints)`);
  if (wsEndpoints.length > 0) capabilities.push(`🔌 Real-time WebSocket streams (${wsEndpoints.length})`);
  if (sseEndpoints.length > 0) capabilities.push(`📡 Server-Sent Events (${sseEndpoints.length})`);
  if (authedEndpoints.length > 0) capabilities.push(`🔐 Authenticated access (${authedEndpoints.length} endpoints need auth)`);

  // Suggest specific actions based on path patterns
  const suggestions = [];
  const pathLower = endpoints.map(e => (e.path || '').toLowerCase()).join(' ');
  
  if (pathLower.includes('search') || pathLower.includes('query')) suggestions.push('🔍 Search content');
  if (pathLower.includes('create') || pathLower.includes('post') || pathLower.includes('compose')) suggestions.push('📝 Create new content');
  if (pathLower.includes('delete') || pathLower.includes('remove')) suggestions.push('🗑️  Delete content');
  if (pathLower.includes('notif')) suggestions.push('🔔 Read/manage notifications');
  if (pathLower.includes('user') || pathLower.includes('profile') || pathLower.includes('account')) suggestions.push('👤 Access user/profile data');
  if (pathLower.includes('timeline') || pathLower.includes('feed') || pathLower.includes('home')) suggestions.push('📰 Read feed/timeline');
  if (pathLower.includes('message') || pathLower.includes('chat') || pathLower.includes('conversation')) suggestions.push('💬 Send/read messages');
  if (pathLower.includes('upload') || pathLower.includes('media') || pathLower.includes('image')) suggestions.push('🖼️  Upload/manage media');
  if (pathLower.includes('setting') || pathLower.includes('config') || pathLower.includes('preference')) suggestions.push('⚙️  Manage settings');
  if (pathLower.includes('like') || pathLower.includes('favorite') || pathLower.includes('bookmark') || pathLower.includes('vote')) suggestions.push('❤️  Like/favorite/bookmark');
  if (pathLower.includes('follow') || pathLower.includes('subscribe')) suggestions.push('👥 Follow/subscribe');
  if (pathLower.includes('comment') || pathLower.includes('reply')) suggestions.push('💬 Comment/reply');
  if (pathLower.includes('analytics') || pathLower.includes('stats') || pathLower.includes('metrics')) suggestions.push('📊 View analytics/stats');

  console.log(`Neo capabilities for ${domain}`);
  console.log(`  ${endpoints.length} endpoints discovered\n`);

  if (capabilities.length > 0) {
    console.log('Capabilities:');
    for (const c of capabilities) console.log(`  ${c}`);
    console.log();
  }

  if (suggestions.length > 0) {
    console.log('Suggested actions:');
    for (const s of suggestions) console.log(`  ${s}`);
    console.log();
  }

  console.log('Endpoint categories:');
  for (const [cat, eps] of Object.entries(categories).sort((a, b) => b[1].length - a[1].length)) {
    console.log(`  ${cat}: ${eps.length} endpoint(s)`);
    for (const ep of eps.slice(0, 5)) {
      console.log(`    ${ep.method.padEnd(6)} ${ep.path}`);
    }
    if (eps.length > 5) console.log(`    ... and ${eps.length - 5} more`);
  }
};

// neo deps — discover API dependency chains (response fields → request fields)
commands.workflow = async function(args) {
  const parsed = parseArgs(args || []);
  const action = parsed.positional[0];
  if (!action) {
    console.log(`Usage:
  neo workflow discover <domain> [--window ms] [--min-confidence n] [--min-steps n] [--max-steps n]
  neo workflow show <name> [--json]
  neo workflow run <name> [--params key=value]

Discover and execute reusable multi-step workflows built from dependency chains.`);
    return;
  }

  if (action === 'discover') {
    const domain = parsed.positional[1];
    if (!domain) {
      console.error('Usage: neo workflow discover <domain> [--window ms] [--min-confidence n]');
      process.exit(1);
    }

    const windowMs = parseInt(parsed.flags.window) || 10000;
    const minConfidence = parseInt(parsed.flags['min-confidence'] || parsed.flags.minConfidence) || 2;
    const minSteps = parseInt(parsed.flags['min-steps']) || 2;
    const maxSteps = parseInt(parsed.flags['max-steps']) || 4;
    const wsUrl = await findExtensionWs();
    const captures = await loadDependencyData(wsUrl, domain);
    if (!captures || captures.length < 2) {
      console.error(`Not enough captures for ${domain} to discover workflow chains`);
      return;
    }
    captures.sort((a, b) => a.timestamp - b.timestamp);
    const links = computeDependencyLinks(captures, windowMs, minConfidence);
    if (!links.length) {
      console.log(`No dependency links found for ${domain}`);
      return;
    }

    const schemaData = loadSchemaFile(domain);
    const schema = schemaData ? schemaData.schema : { domain, endpoints: [] };
    const chains = discoverWorkflowChains(links, minSteps, maxSteps, minConfidence);
    const workflows = dedupeWorkflows(buildWorkflowFromChains(domain, chains, schema));
    if (!workflows.length) {
      console.log(`No workflow chains found for ${domain} (min-steps ${minSteps}, window ${windowMs}ms, min-confidence ${minConfidence})`);
      return;
    }

    const payload = {
      domain,
      generatedAt: new Date().toISOString(),
      windowMs,
      minConfidence,
      minSteps,
      maxSteps,
      workflows,
    };
    const outFile = path.join(SCHEMA_DIR, `${domain}${WORKFLOW_FILE_EXT}`);
    fs.mkdirSync(SCHEMA_DIR, { recursive: true });
    fs.writeFileSync(outFile, JSON.stringify(payload, null, 2));
    console.log(`Discovered ${workflows.length} workflows for ${domain}`);
    for (const wf of workflows.slice(0, 20)) {
      console.log(`  - ${wf.name}`);
    }
    console.log(`Saved to ${outFile}`);
    return;
  }

  if (action === 'show') {
    const name = parsed.positional[1];
    if (!name) {
      console.error('Usage: neo workflow show <name> [--json]');
      process.exit(1);
    }
    const workflows = loadWorkflowsFromDisk(parsed.flags.domain);
    const matched = workflows.filter(w => w.name === name || w.name.toLowerCase().startsWith(name.toLowerCase()));
    if (!matched.length) {
      console.log(`No workflow matching "${name}"`);
      return;
    }
    const exact = matched.filter(w => w.name === name);
    const wf = exact.length === 1 ? exact[0] : matched[0];
    if (parsed.flags.json) {
      console.log(JSON.stringify(wf, null, 2));
      return;
    }
    if (exact.length > 1) {
      console.log(`Multiple workflows match "${name}":`);
      for (const item of exact) {
        console.log(`  - ${item.name}  (${item.domain}, ${item.endpointCount} steps)`);
      }
      console.log('Run with exact name or use a longer prefix.');
      return;
    }
    console.log(`${wf.name} (${wf.domain}) — score ${wf.score}, ${wf.endpointCount} steps`);
    console.log(`Created: ${wf.createdAt || wf.generatedAt || 'n/a'}\n`);
    for (let i = 0; i < wf.steps.length; i++) {
      const step = wf.steps[i];
      const label = step.label ? ` [${step.label}]` : '';
      console.log(`  ${i + 1}. ${step.endpointKey}${label}`);
    }
    console.log('\nTransitions:');
    for (const transition of wf.transitions) {
      for (const f of transition.fields) {
        console.log(`  ${transition.from + 1} → ${transition.to + 1}: .${f.sourceField} -> .${f.targetField} (${f.count}x)`);
      }
    }
    return;
  }

  if (action === 'run') {
    const name = parsed.positional[1];
    if (!name) {
      console.error('Usage: neo workflow run <name> [--params key=value]');
      process.exit(1);
    }
    const params = parseWorkflowParams(args);
    const workflows = loadWorkflowsFromDisk(parsed.flags.domain);
    const matched = workflows.filter(w => w.name === name || w.name.toLowerCase().startsWith(name.toLowerCase()));
    if (!matched.length) {
      console.log(`No workflow matching "${name}"`);
      return;
    }
    const exact = matched.filter(w => w.name === name);
    const wf = exact.length === 1 ? exact[0] : matched[0];
    const schemaData = loadSchemaFile(wf.domain);
    if (!schemaData) {
      console.error(`No schema for ${wf.domain}. Run: neo schema generate ${wf.domain}`);
      process.exit(1);
    }
    const schema = schemaData.schema;
    const endpointMap = new Map((schema.endpoints || []).map(ep => [endpointKey(ep.method, ep.path), ep]));

    const wsUrl = await findExtensionWs();
    const captures = await loadDependencyData(wsUrl, wf.domain);
    if (!captures.length) {
      console.error(`No captures for ${wf.domain}`);
      return;
    }
    captures.sort((a, b) => a.timestamp - b.timestamp);

    const findLatestCapture = (key) => {
      for (let i = captures.length - 1; i >= 0; i--) {
        if (captures[i].endpointKey === key) return captures[i];
      }
      return null;
    };

    let previousResponse = null;
    for (let i = 0; i < wf.steps.length; i++) {
      const step = wf.steps[i];
      const capture = findLatestCapture(step.endpointKey);
      if (!capture) {
        console.error(`Step ${i + 1}: no capture for ${step.endpointKey}`);
        return;
      }

      const schemaEndpoint = endpointMap.get(step.endpointKey);
      let requestPayload = schemaEndpoint?.requestBodyStructure
        ? JSON.parse(JSON.stringify(buildTemplateFromStructure(schemaEndpoint.requestBodyStructure)))
        : {};

      const transitions = wf.transitions.filter(t => t.from === i - 1);
      if (previousResponse && transitions.length > 0) {
        const responseData = parseMaybeJson(previousResponse);
        const responseValues = collectResponseValues(responseData);
        for (const transition of transitions.flatMap(t => t.fields)) {
          let value = undefined;
          if (responseData && typeof responseData === 'object') value = getByPath(responseData, transition.sourceField);
          if (value === undefined && responseValues && Object.prototype.hasOwnProperty.call(responseValues, transition.sourceField)) {
            value = responseValues[transition.sourceField];
          }
          if (value !== undefined) {
            setRequestField(requestPayload, transition.targetField, value);
          }
        }
      }

      for (const [k, v] of Object.entries(params)) {
        setRequestField(requestPayload, k, v);
      }

      const query = requestPayload.query;
      let url = appendQueryParams(capture.url, query);
      if (query && typeof requestPayload === 'object') delete requestPayload.query;

      const shouldSendBody = !(step.method === 'GET' || step.method === 'HEAD' || step.method === 'OPTIONS')
        && requestPayload && Object.keys(requestPayload).length > 0;
      const bodyText = shouldSendBody ? JSON.stringify(requestPayload) : null;
      const method = step.method || 'GET';
      const domain = wf.domain;

      console.log(`\nStep ${i + 1}/${wf.steps.length}: ${step.endpointKey}`);
      if (step.label) console.log(`  ${step.label}`);

      const tab = await findTab(domain);
      const probeUrl = (() => {
        try { return `${new URL(url).origin}/`; }
        catch { return tab.url; }
      })();
      const { liveHeaders, fallbackHeaders } = await collectExecutionAuthHeaders({
        wsUrl,
        tab,
        domain,
        probeUrl,
      });
      const execHeaders = {};
      applyAuthHeaders(execHeaders, liveHeaders, true);
      applyAuthHeaders(execHeaders, fallbackHeaders, false);
      stripForbiddenFetchHeaders(execHeaders);

      const fetchOpts = { method, headers: execHeaders, credentials: 'include' };
      if (bodyText) {
        fetchOpts.body = bodyText;
        if (!fetchOpts.headers['content-type'] && !fetchOpts.headers['Content-Type']) {
          fetchOpts.headers['Content-Type'] = 'application/json';
        }
      }

      const result = await cdpEval(tab.webSocketDebuggerUrl, `
        (async function() {
          try {
            var resp = await fetch(${JSON.stringify(url)}, ${JSON.stringify(fetchOpts)});
            var text = await resp.text();
            return JSON.stringify({
              status: resp.status,
              statusText: resp.statusText,
              headers: Object.fromEntries(resp.headers.entries()),
              body: text.length > 50000 ? text.slice(0, 50000) + '...[truncated]' : text
            });
          } catch (err) { return JSON.stringify({ error: err.message }); }
        })()
      `);
      const parsed = JSON.parse(result);
      if (parsed.error) {
        console.error(`Step ${i + 1} failed: ${parsed.error}`);
        return;
      }
      console.log(`  HTTP ${parsed.status} ${parsed.statusText}`);
      try { previousResponse = JSON.parse(parsed.body); } catch { previousResponse = parsed.body; }
    }
    return;
  }

  console.log(`Unknown workflow command: ${action}`);
};

// neo deps — discover API dependency chains (response fields → request fields)
commands.deps = async function(args) {
  const { positional, flags } = parseArgs(args || []);
  const domain = positional[0];
  if (!domain) {
    console.log(`Usage: neo deps <domain> [--window <ms>] [--min-confidence <n>]

Discover data flow between endpoints: which response fields feed into
subsequent request parameters. Analyzes temporal sequences of captures
to find value propagation patterns.

Options:
  --window <ms>          Max time between producer→consumer (default: 10000)
  --min-confidence <n>   Min occurrences to report a link (default: 2)`);
    return;
  }

  const windowMs = parseInt(flags.window) || 10000;
  const minConf = parseInt(flags['min-confidence'] || flags.minConfidence) || 2;

  const wsUrl = await findExtensionWs();

  const rawData = await loadDependencyData(wsUrl, domain);

  if (!rawData || rawData.length < 2) {
    console.error(`Not enough captures for ${domain} to detect dependencies`);
    return;
  }

  // Sort by timestamp
  rawData.sort((a, b) => a.timestamp - b.timestamp);
  const links = computeDependencyLinks(rawData, windowMs, minConf);

  // Filter by confidence and group by endpoint pair
  const pairLinks = new Map(); // "producer → consumer" → [{respField, reqField, count}]
  for (const link of links) {
    if ((link.count || 0) < minConf) continue;
    const pairKey = `${link.producerEndpoint} → ${link.consumerEndpoint}`;
    if (!pairLinks.has(pairKey)) pairLinks.set(pairKey, []);
    pairLinks.get(pairKey).push({
      respField: link.respField,
      reqField: link.reqField,
      count: link.count,
    });
  }

  if (pairLinks.size === 0) {
    console.log(`No dependency chains found for ${domain} (window: ${windowMs}ms, min: ${minConf}x)`);
    console.log(`  Analyzed ${rawData.length} captures`);
    console.log(`  Tip: try --window 30000 for slower workflows or --min-confidence 1`);
    return;
  }

  // Sort pairs by total evidence
  const sortedPairs = [...pairLinks.entries()]
    .map(([pair, fields]) => ({ pair, fields, total: fields.reduce((s, f) => s + f.count, 0) }))
    .sort((a, b) => b.total - a.total);

  console.log(`API dependency chains for ${domain} (${sortedPairs.length} links found)\n`);
  for (const { pair, fields, total } of sortedPairs.slice(0, 20)) {
    console.log(`  ${pair}  (${total}x)`);
    // Sort fields by count, show top 5
    fields.sort((a, b) => b.count - a.count);
    for (const f of fields.slice(0, 5)) {
      console.log(`    .${f.respField} → .${f.reqField}  (${f.count}x)`);
    }
    console.log();
  }
};

// neo export-skill — generate an agent-ready API reference from schema
commands['export-skill'] = async function(args) {
  const { positional, flags } = parseArgs(args || []);
  const domain = positional[0];
  if (!domain) {
    console.log(`Usage: neo export-skill <domain> [--output <file>]

Generate a concise, agent-friendly API reference from a captured schema.
Output is Markdown suitable for including in an OpenClaw SKILL.md or
system prompt. Includes endpoint signatures, auth requirements, and
example usage patterns.`);
    return;
  }

  const schemaFile = path.join(SCHEMA_DIR, `${domain}.json`);
  if (!fs.existsSync(schemaFile)) {
    console.error(`No schema for ${domain}. Run: neo schema generate ${domain}`);
    process.exit(1);
  }
  const schema = JSON.parse(fs.readFileSync(schemaFile, 'utf8'));

  const lines = [];
  lines.push(`# ${domain} API Reference`);
  lines.push(`> Auto-generated by Neo from ${schema.totalCaptures} captured API calls`);
  lines.push(`> Generated: ${schema.generatedAt}\n`);

  // Auth info
  const allAuthHeaders = new Set();
  for (const ep of schema.endpoints) {
    if (ep.authHeaders) ep.authHeaders.forEach(h => allAuthHeaders.add(h));
  }
  if (allAuthHeaders.size > 0) {
    lines.push(`## Authentication`);
    lines.push(`Required headers: ${[...allAuthHeaders].map(h => '`' + h + '`').join(', ')}`);
    lines.push(`Use \`neo exec <url> --auto-headers\` to auto-inherit auth from browser.\n`);
  }

  // Group endpoints by category
  const categories = {};
  for (const ep of schema.endpoints) {
    const cat = ep.category || 'uncategorized';
    if (!categories[cat]) categories[cat] = [];
    categories[cat].push(ep);
  }

  for (const [cat, endpoints] of Object.entries(categories).sort()) {
    lines.push(`## ${cat.charAt(0).toUpperCase() + cat.slice(1)}`);
    lines.push('');
    for (const ep of endpoints.sort((a, b) => b.callCount - a.callCount)) {
      const params = ep.queryParams?.length ? `?${ep.queryParams.join('&')}` : '';
      lines.push(`### \`${ep.method} ${ep.path}${params}\``);
      
      const meta = [];
      if (ep.callCount) meta.push(`${ep.callCount}x observed`);
      if (ep.avgDuration) meta.push(`avg ${ep.avgDuration}`);
      if (ep.responseType) meta.push(`returns ${ep.responseType}`);
      if (ep.source && typeof ep.source === 'string' && ep.source !== 'fetch') meta.push(`via ${ep.source}`);
      if (meta.length) lines.push(meta.join(' · '));

      // Request body structure
      if (ep.requestBodyStructure) {
        lines.push('\nRequest body:');
        lines.push('```json');
        lines.push(JSON.stringify(ep.requestBodyStructure, null, 2));
        lines.push('```');
        if (ep.bodyFieldVariability) {
          const varies = Object.entries(ep.bodyFieldVariability)
            .filter(([, v]) => v === 'variable')
            .map(([k]) => k);
          if (varies.length) lines.push(`Variable fields: ${varies.map(v => '`' + v + '`').join(', ')}`);
        }
      }

      // Response structure
      if (ep.responseKeys) {
        lines.push('\nResponse structure:');
        lines.push('```json');
        lines.push(JSON.stringify(ep.responseKeys, null, 2));
        lines.push('```');
      }

      // Triggers
      if (ep.triggers?.length) {
        const top = ep.triggers.slice(0, 3);
        lines.push(`\nTriggered by: ${top.map(t => `${t.event}(\`${t.text || t.selector}\`)`).join(', ')}`);
      }

      lines.push('');
    }
  }

  // Usage examples
  lines.push(`## Quick Usage`);
  lines.push('```bash');
  lines.push(`# List available endpoints`);
  lines.push(`neo schema show ${domain}`);
  lines.push(`# Call an endpoint (auto-auth from browser session)`);
  const exampleEp = schema.endpoints[0];
  if (exampleEp) {
    lines.push(`neo api ${domain} ${exampleEp.path.split('/').pop()}`);
  }
  lines.push(`# Search endpoints`);
  lines.push(`neo schema search ${domain} <keyword>`);
  lines.push('```');

  const output = lines.join('\n');

  if (flags.output) {
    fs.writeFileSync(flags.output, output);
    console.error(`Written to ${flags.output}`);
  } else {
    console.log(output);
  }
};

// neo mock — generate a mock HTTP server from schema
commands.mock = async function(args) {
  const { positional, flags } = parseArgs(args || []);
  const domain = positional[0];
  if (!domain) {
    console.log(`Usage: neo mock <domain> [--port <port>] [--latency <ms>]

Generate a local mock HTTP server from a domain's API schema.
Returns realistic responses based on captured response body structures.

Options:
  --port <port>       Port to listen on (default: 3456)
  --latency <ms>      Simulated response latency (default: 0)`);
    return;
  }

  const port = parseInt(flags.port) || 3456;
  const latency = parseInt(flags.latency) || 0;
  const schemaPath = path.join(SCHEMA_DIR, `${domain}.json`);
  if (!fs.existsSync(schemaPath)) {
    console.error(`No schema for ${domain}. Run: neo schema generate ${domain}`);
    process.exit(1);
  }

  const schema = JSON.parse(fs.readFileSync(schemaPath, 'utf-8'));
  const http = require('http');

  // Generate mock value from type string
  function mockValue(type, key) {
    const lk = key.toLowerCase();
    if (type === 'number') return lk.includes('count') || lk.includes('total') ? 42 : 3.14;
    if (type === 'boolean') return true;
    if (type === 'array') return [];
    if (type === 'object') return {};
    if (type === 'null') return null;
    // string
    if (lk.includes('id')) return 'mock-id-' + Math.random().toString(36).slice(2, 8);
    if (lk.includes('url') || lk.includes('href')) return 'https://example.com/mock';
    if (lk.includes('name')) return 'Mock Name';
    if (lk.includes('email')) return 'mock@example.com';
    if (lk.includes('date') || lk.includes('time') || lk.includes('created') || lk.includes('updated')) return new Date().toISOString();
    if (lk.includes('text') || lk.includes('body') || lk.includes('content') || lk.includes('description')) return 'Mock content';
    if (lk.includes('title')) return 'Mock Title';
    if (lk.includes('token') || lk.includes('key') || lk.includes('secret')) return 'mock-secret-xxxxx';
    if (lk.includes('status') || lk.includes('state')) return 'active';
    if (lk.includes('type') || lk.includes('kind')) return 'default';
    return 'mock-' + key;
  }

  // Build mock response from responseBodyStructure
  function buildMockBody(structure) {
    if (!structure || typeof structure !== 'object') return { ok: true };
    const result = {};
    for (const [key, type] of Object.entries(structure)) {
      if (typeof type === 'object' && type !== null) {
        result[key] = buildMockBody(type);
      } else {
        result[key] = mockValue(String(type), key);
      }
    }
    return result;
  }

  // Build route table: { "GET /path": endpoint }
  const routes = new Map();
  for (const ep of schema.endpoints || []) {
    // Convert parameterized paths to regex
    const pathRegex = ep.path
      .replace(/:[a-zA-Z_]+/g, '[^/]+')
      .replace(/\*/g, '.*');
    routes.set(`${ep.method} ${pathRegex}`, {
      ...ep,
      regex: new RegExp('^' + pathRegex + '(\\?.*)?$'),
    });
  }

  const server = http.createServer((req, res) => {
    const method = req.method;
    const urlPath = req.url;

    // CORS
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, PATCH, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', '*');
    if (method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

    // Find matching route
    let matched = null;
    for (const [, ep] of routes) {
      if (ep.method === method && ep.regex.test(urlPath)) {
        matched = ep;
        break;
      }
    }

    const respond = () => {
      if (matched) {
        const status = Object.keys(matched.statusCodes || {})[0] || 200;
        const body = buildMockBody(matched.responseBodyStructure);
        res.writeHead(parseInt(status), { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(body, null, 2));
      } else {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Not found', availableRoutes: schema.endpoints.map(e => `${e.method} ${e.path}`) }));
      }
    };

    if (latency > 0) {
      setTimeout(respond, latency);
    } else {
      respond();
    }
  });

  server.listen(port, () => {
    console.log(`Neo mock server for ${domain}`);
    console.log(`Listening on http://localhost:${port}`);
    console.log(`${routes.size} routes from schema\n`);
    for (const [, ep] of routes) {
      const status = Object.keys(ep.statusCodes || {})[0] || 200;
      console.log(`  ${ep.method.padEnd(6)} ${ep.path}  → ${status}`);
    }
    console.log(`\nPress Ctrl+C to stop`);
  });
};

// neo tabs — list open Chrome tabs
commands.tabs = async function(args) {
  const { positional } = parseArgs(args || []);
  const filter = positional[0];
  const tabs = await (await fetch(`${CDP_URL}/json/list`)).json();
  const pages = tabs.filter(t => t.type === 'page');
  const filtered = filter ? pages.filter(t => t.url.includes(filter) || (t.title || '').toLowerCase().includes(filter.toLowerCase())) : pages;
  if (!filtered.length) { console.log(filter ? `No tabs matching "${filter}"` : 'No tabs open'); return; }
  for (const t of filtered) {
    const title = (t.title || '').slice(0, 60);
    const url = t.url.length > 80 ? t.url.slice(0, 77) + '...' : t.url;
    console.log(`  ${title}`);
    console.log(`    ${url}`);
  }
  console.log(`\n${filtered.length} tab(s)${filter ? ` matching "${filter}"` : ''}`);
};

// neo reload — reload the Neo extension without toggling in chrome://extensions
commands.reload = async function() {
  const wsUrl = await findExtensionWs();
  await cdpEval(wsUrl, `chrome.runtime.reload()`);
  // Wait for the service worker to come back
  console.log('Reloading extension...');
  await new Promise(r => setTimeout(r, 2000));
  try {
    const newWs = await findExtensionWs();
    const count = await cdpEval(newWs, dbEval(`
      store.count().onsuccess = function(e) { resolve(String(e.target.result)); };
    `));
    console.log(`Extension reloaded. ${count} captures intact.`);
  } catch {
    console.log('Extension reloaded (reconnecting may take a moment).');
  }
};

// neo doctor — diagnose setup issues
commands.doctor = async function(args) {
  const fix = (args || []).includes('--fix');
  const config = loadNeoConfig();
  const cdpUrl = getConfiguredCdpUrl();
  const cdpPort = getConfiguredCdpPort();
  const checks = [];
  function check(name, fn, fixFn, opts) {
    checks.push({
      name,
      fn,
      fixFn,
      critical: !!(opts && opts.critical),
      optional: !!(opts && opts.optional),
      key: opts && opts.key ? opts.key : name,
    });
  }

  check('Chrome CDP endpoint', async () => {
    const resp = await fetch(`${cdpUrl}/json/version`);
    const info = await resp.json();
    return `${info.Browser} (${cdpUrl})`;
  }, async () => {
    process.stderr.write('  → Starting Chrome with neo start...\n');
    try {
      await commands.start([]);
    } catch (error) {
      const runningWithoutCdp = listChromeProcesses().filter((proc) => proc && !proc.remoteDebuggingPort);
      if (runningWithoutCdp.length) {
        throw new Error(`${error.message}. Chrome appears to be running without CDP; close it or retry with neo start --force`);
      }
      throw error;
    }
    await new Promise(r => setTimeout(r, 2000));
    const resp = await fetch(`${cdpUrl}/json/version`);
    const info = await resp.json();
    return `Fixed: ${info.Browser} (${cdpUrl})`;
  }, { critical: true });

  check('Browser tabs', async () => {
    const tabs = await (await fetch(`${cdpUrl}/json/list`)).json();
    const pages = tabs.filter(t => t.type === 'page');
    return `${pages.length} page(s), ${tabs.length} total targets`;
  }, null, { critical: false });

  check('Chrome profile', async () => {
    const configured = describeConfiguredChromeProfile(config);
    const activeProcess = listChromeProcesses().find((proc) => proc && (proc.remoteDebuggingPort === cdpPort || proc.profileDirectory || proc.userDataDir));
    const active = activeProcess
      ? `${activeProcess.profileDirectory || '(unknown profile)'} @ ${activeProcess.userDataDir || '(default user-data-dir)'}`
      : 'not detected';
    return `Configured: ${configured}; Active: ${active}`;
  }, null, { critical: false });

  check('CDP capture daemon', async () => {
    const state = readCaptureDaemonState(CDP_CAPTURE_STATE_FILE);
    if (!state) return 'Not running';
    if (!isProcessAlive(state.pid)) throw new Error(`Stale state file for pid ${state.pid}`);
    return `Running (pid ${state.pid}, tab ${state.tabId || '(unknown)'})`;
  }, async () => {
    const state = readCaptureDaemonState(CDP_CAPTURE_STATE_FILE);
    if (state && !isProcessAlive(state.pid) && fs.existsSync(CDP_CAPTURE_STATE_FILE)) {
      fs.unlinkSync(CDP_CAPTURE_STATE_FILE);
      return 'Removed stale state file';
    }
    return 'Not running';
  }, { critical: false });

  check('Neo extension service worker (optional - CDP capture available without extension)', async () => {
    const sw = await findExtensionServiceWorker({ cdpUrl });
    if (!sw) throw new Error('Not found');
    const extensionId = parseExtensionIdFromUrl(sw.url);
    if (!extensionId) return 'OK';
    return `OK (${extensionId.slice(0, 8)}…)`;
  }, null, { critical: false, optional: true, key: 'extension' });

  check('IndexedDB captures (extension)', async () => {
    const wsUrl = await findExtensionWs({ cdpUrl });
    if (!wsUrl) return 'Extension not active';
    const count = await cdpEval(wsUrl, dbEval(`
      store.count().onsuccess = function(e) { resolve(String(e.target.result)); };
    `));
    return `${count} captures stored`;
  }, null, { critical: false });

  check('CDP JSONL captures', async () => {
    const count = readLocalCaptureJsonl(LOCAL_CAPTURE_FILE).length;
    return `${count} captures stored in ${LOCAL_CAPTURE_FILE}`;
  });

  check('Schema directory', async () => {
    if (!fs.existsSync(SCHEMA_DIR)) throw new Error(`Missing: ${SCHEMA_DIR}`);
    const files = fs.readdirSync(SCHEMA_DIR).filter(f => f.endsWith('.json'));
    return `${files.length} schema(s) in ${SCHEMA_DIR}`;
  }, async () => {
    fs.mkdirSync(SCHEMA_DIR, { recursive: true });
    return `Fixed: created ${SCHEMA_DIR}`;
  });

  check('WebSocket bridge port', async () => {
    return new Promise((resolve, reject) => {
      const WebSocketCtor = getWebSocketCtor();
      const ws = new WebSocketCtor('ws://127.0.0.1:9234');
      const timer = setTimeout(() => { ws.close(); resolve('Not running (optional)'); }, 2000);
      ws.on('open', () => { clearTimeout(timer); ws.close(); resolve('Running on :9234'); });
      ws.on('error', () => { clearTimeout(timer); resolve('Not running (optional)'); });
    });
  });

  let fixed = 0;
  const status = {
    ready: true,
    chrome: false,
    extension: false,
    captures: null,
    cdpCaptures: 0,
    profile: {
      configured: String(config.defaultProfile || '').trim() || null,
      userDataDir: String(config.userDataDir || '').trim() || null,
      active: null,
    },
    actions: [],
  };
  for (const { name, fn, fixFn, critical, optional, key } of checks) {
    try {
      const result = await fn();
      console.log(`  ✓  ${name}: ${result}`);
      if (key === 'Chrome CDP endpoint') status.chrome = true;
      if (key === 'extension') status.extension = true;
      if (key === 'IndexedDB captures (extension)') {
        const m = String(result).match(/^(\d+)/);
        status.captures = m ? parseInt(m[1], 10) : 0;
      }
      if (key === 'CDP JSONL captures') {
        const m = String(result).match(/^(\d+)/);
        status.cdpCaptures = m ? parseInt(m[1], 10) : 0;
      }
      if (key === 'Chrome profile') {
        const activeMatch = String(result).match(/Active: (.+)$/);
        status.profile.active = activeMatch ? activeMatch[1] : null;
      }
    } catch (err) {
      if (optional) {
        console.log(`  ○  ${name}: ${err.message}`);
        continue;
      }
      if (critical) status.ready = false;
      if (fix && fixFn) {
        try {
          const result = await fixFn();
          console.log(`  ✓  ${name}: ${result}`);
          fixed++;
          if (key === 'Chrome CDP endpoint') status.chrome = true;
        } catch (fixErr) {
          console.log(`  ✗  ${name}: ${fixErr.message}`);
          if (key === 'Chrome CDP endpoint') {
            status.actions.push('Chrome CDP not reachable. Run: neo start');
          } else if (key === 'CDP capture daemon') {
            status.actions.push(`Stale CDP capture daemon state. Remove: ${CDP_CAPTURE_STATE_FILE}`);
          }
        }
      } else {
        console.log(`  ✗  ${name}: ${err.message}${fixFn ? ' (use --fix to auto-repair)' : ''}`);
        if (key === 'Chrome CDP endpoint') {
          status.actions.push('Chrome CDP not reachable. Run: neo start');
        } else if (key === 'CDP capture daemon') {
          status.actions.push(`Stale CDP capture daemon state. Remove: ${CDP_CAPTURE_STATE_FILE}`);
        }
      }
    }
  }
  if (fix && fixed > 0) console.log(`\n  Fixed ${fixed} issue(s).`);
  status.ready = status.chrome;
  console.log(`\nNEO_STATUS: ${JSON.stringify(status)}`);
};

async function main() {
  const rawArgv = process.argv.slice(2);
  const parsed = parseArgs(rawArgv);
  const sessionName = parsed.sessionName || DEFAULT_SESSION_NAME;
  const cleanArgv = stripGlobalSessionFlag(rawArgv);
  const [cmd, ...args] = cleanArgv;

  if (commands[cmd]) {
    await commands[cmd](args, { sessionName });
  } else {
    console.log(`Neo — Turn any web app into an API

Commands:
  neo status                              Overview of captured data
  neo capture start|stop|list|count|domains|detail|search|stats|summary|prune|clear|export|import
                                          Manage captured API traffic
  neo schema generate|show|search <domain>  API schema management
  neo exec <url> [options]                Execute fetch in browser context
  neo replay <id> [--tab pattern] [--auto-headers] Replay a captured API call
  neo eval "<js>" --tab <pattern>         Evaluate JS in page context
  neo open <url>                          Open URL in Chrome
  neo read <tab-pattern>                  Extract readable text from page
  neo setup [--profile <name>]             Setup ~/.neo config, extension, and schemas
  neo start [--profile <name>]             Start Chrome with configured Neo/system profile
  neo profile list                         List system Chrome/Chromium profiles
  neo profile use <name>                   Set default system Chrome profile
  neo profiles                             List configured profiles
  neo launch <app> [--port N]             Launch Electron app with CDP enabled
  neo connect [port]                      Connect to CDP and save active session
  neo connect --electron <app-name>       Auto-discover Electron CDP port and connect
  neo discover                            Discover reachable CDP endpoints on localhost
  neo sessions                            List saved active sessions
  neo tab                                 List CDP targets in the active session
  neo tab <index> | neo tab --url <pat>  Switch active tab target
  neo inject [--persist] [--tab pattern]  Inject Neo capture script into page target
  neo cookies list|export|import|clear     Manage browser cookies in the active session
  neo snapshot [-i] [-C] [--json] [--diff] Snapshot a11y tree with compact refs
  neo click <ref> [--new-tab]             Click element by ref
  neo fill <ref> "text"                    Clear then fill element by ref
  neo type <ref> "text"                    Type text without clearing
  neo press <key>                          Press keyboard key (supports Ctrl+a)
  neo hover <ref>                          Hover over element by ref
  neo scroll <dir> [px] [--selector css]   Scroll by direction and distance
  neo select <ref> "value"                 Select option value by ref
  neo screenshot [path] [--full] [--annotate] Capture screenshot to file
  neo get text <ref> | neo get url | neo get title Extract page/element info
  neo wait <ref> | neo wait --load networkidle | neo wait <ms> Wait for UI/load/time
  neo label <domain> [--dry-run]          Add semantic labels to schema endpoints
  neo workflow discover|show|run <name>    Discover and replay multi-step endpoint workflows
  neo tabs [filter]                       List open Chrome tabs
  neo api <domain> <search-term>          Smart API call (schema lookup + auto-auth)
  neo flows <domain> [--window ms]        Discover API call sequence patterns
  neo deps <domain> [--window ms]         Discover API dependency chains (response→request)
  neo suggest <domain>                    Suggest AI capabilities for a domain
  neo export-skill <domain>               Generate agent-ready API reference
  neo mock <domain> [--port N]            Generate mock server from schema
  neo bridge [port] [--json] [--quiet]    Start WebSocket bridge server
  neo doctor [--fix]                      Diagnose setup issues (--fix to auto-repair)
  neo reload                              Reload the Neo extension

Options (for exec):
  --method GET|POST|PUT|DELETE            HTTP method (default: GET)
  --header "Key: Value"                   Request header (repeatable)
  --body '{"key": "value"}'              Request body
  --tab <pattern>                         Match tab by URL pattern
  --auto-headers                          Auto-detect auth headers from live browser traffic
  --session <name>                        Global session name (default: __default__)

Environment:
  NEO_CDP_URL        Chrome DevTools URL (default: http://localhost:9222)
  NEO_EXTENSION_ID   Neo extension ID override
  NEO_SCHEMA_DIR     Schema storage directory`);
  }
}

main().catch(err => { console.error('Error:', err.message); process.exit(1); });
