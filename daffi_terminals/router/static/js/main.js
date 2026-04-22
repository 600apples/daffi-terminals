'use strict';

// Base WebSocket URL derived from the current page URL.
// Uses location.protocol so http → ws and https → wss without string tricks.
const WSURL = (() => {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const path  = location.pathname.endsWith('/') ? location.pathname : location.pathname + '/';
    return `${proto}//${location.host}${path}`;
})();

// Binary protocol command bytes sent as the first byte of each WS frame.
const CMD_DATA   = 0x01;  // keyboard input
const CMD_RESIZE = 0x02;  // terminal resize

// ── Host facts bar ───────────────────────────────────────────────────────────

function showHostFacts(facts) {
    const bar = document.getElementById('host-facts');
    if (!bar) return;
    if (!facts || !Object.keys(facts).length) { bar.innerHTML = ''; return; }

    const mem    = (facts.mem_total_mb && facts.mem_free_mb)
        ? `${facts.mem_free_mb} / ${facts.mem_total_mb} MB` : null;
    const distro = [facts.distro, facts.distro_version].filter(Boolean).join(' ') || null;
    const osName = distro || facts.os || null;

    // Each entry is [label, value] — falsy values are dropped automatically.
    const items = [
        ['host',   facts.hostname  || null],
        ['os',     osName],
        ['kernel', facts.kernel    || null],
        ['arch',   facts.arch      || null],
        ['cpu',    facts.cpu_count ? `${facts.cpu_count} cores` : null],
        ['mem',    mem],
        ['uptime', facts.uptime    || null],
    ].filter(([, v]) => v != null);

    bar.innerHTML = items.map(([label, value]) =>
        `<span class="dt-fact">
            <span class="dt-fact-label">${label}</span>
            <span class="dt-fact-value">${value}</span>
         </span>`
    ).join('');
}

function clearHostFacts() {
    const bar = document.getElementById('host-facts');
    if (bar) bar.innerHTML = '';
}

// ── Director socket ──────────────────────────────────────────────────────────
// Keeps the worker sidebar in sync.  Reconnects automatically on drop.

class DirectorSocket {
    constructor() {
        this._sock    = null;
        this._workers = [];   // full unfiltered list, kept for search re-renders
    }

    connect() {
        this._sock = new WebSocket(WSURL + 'director');

        this._sock.onopen = () => {
            document.querySelector('.disconnected-overlay').style.display = 'none';
        };

        this._sock.onmessage = ({ data }) => {
            this._workers = JSON.parse(data);
            console.log('[director] workers:', this._workers.map(w => ({ name: w.process_name, group: w.group })));
            this._renderWorkers(this._filterWorkers());
        };

        this._sock.onerror = (e) => console.error('[director] socket error:', e);

        this._sock.onclose = () => {
            document.querySelector('.disconnected-overlay').style.display = 'block';
            setTimeout(() => this.connect(), 3000);
        };

        // Search input — re-render on every keystroke.
        const searchEl = document.getElementById('worker-search');
        if (searchEl) {
            searchEl.addEventListener('input', () => {
                this._renderWorkers(this._filterWorkers());
            });
        }
    }

    send(obj) {
        if (this._sock && this._sock.readyState === WebSocket.OPEN) {
            this._sock.send(JSON.stringify(obj));
        }
    }

    /** Return workers whose name or MAC contains the current search query. */
    _filterWorkers() {
        const q = (document.getElementById('worker-search')?.value ?? '').trim().toLowerCase();
        if (!q) return this._workers;
        return this._workers.filter(w =>
            w.process_name.toLowerCase().includes(q) ||
            w.mac.toLowerCase().includes(q)
        );
    }

    // Deterministic color index 0-6 from a group name string.
    _groupColor(name) {
        let h = 0;
        for (const c of name) h = Math.imul(h * 31 + c.charCodeAt(0), 1) | 0;
        return Math.abs(h) % 7;
    }

    // Resolve the CSS color value for a group index.
    _groupColorVal(idx) {
        return getComputedStyle(document.documentElement)
            .getPropertyValue(`--group-${idx}`).trim();
    }

    // Build a single worker <li> card.
    _makeCard(worker, groupColor) {
        const iconColor = worker.active ? '#007bff' : '#c0706a';
        const li = document.createElement('li');
        li.id        = worker.process_name;
        li.className = 'terminal-worker-mdc' +
            (worker.active  ? '' : ' worker-inactive') +
            (groupColor     ? ' group-member'          : '');
        if (groupColor) li.style.borderLeftColor = groupColor;

        li.innerHTML = `
            <div class="terminal-worker-mdc-icon">
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24">
                    <g>
                        <path fill="none" d="M0 0h24v24H0z"/>
                        <path fill="${iconColor}"
                              d="M11 12l-7.071 7.071-1.414-1.414L8.172 12
                                 2.515 6.343 3.929 4.93 11 12zm0 7h10v2H11v-2z"/>
                    </g>
                </svg>
            </div>
            <div class="terminal-mdc-card">
                <h4>${worker.process_name}</h4>
                <p>host: ${worker.host}</p>
                <p>mac:  ${worker.mac}</p>
            </div>`;

        if (worker.active) {
            li.onclick = (ev) => TerminalSession.open(ev, worker.process_name);
        } else {
            const badge = document.createElement('span');
            badge.className   = 'worker-disconnected-badge';
            badge.textContent = 'disconnected';
            li.appendChild(badge);

            const btn = document.createElement('a');
            btn.className = 'close-terminal';
            btn.role      = 'button';
            btn.title     = 'Remove from list';
            btn.onclick   = (ev) => {
                ev.preventDefault();
                ev.stopPropagation();
                this.send({ command: 'delete_terminal', term_id: worker.process_name });
            };
            li.appendChild(btn);
        }
        return li;
    }

    _renderWorkers(workers) {
        const list     = document.getElementById('terminal-list');
        list.innerHTML = '';
        const activeId = TerminalSession._current?._workerId ?? null;

        // Split into ungrouped (top) and groups (bottom, alphabetically sorted).
        const ungrouped = workers.filter(w => !w.group);
        const groupMap  = new Map();
        for (const w of workers) {
            if (w.group) {
                if (!groupMap.has(w.group)) groupMap.set(w.group, []);
                groupMap.get(w.group).push(w);
            }
        }
        const sortedGroups = [...groupMap.keys()].sort();

        // ── Ungrouped workers (no section wrapper) ────────────────────────────
        for (const worker of ungrouped) {
            const card = this._makeCard(worker, null);
            if (worker.process_name === activeId) card.classList.add('selected');
            list.appendChild(card);
        }

        // ── Grouped workers — skip groups that are empty after search filtering ──
        for (const groupName of sortedGroups.filter(g => groupMap.get(g).length > 0)) {
            const colorIdx = this._groupColor(groupName);
            const color    = this._groupColorVal(colorIdx);
            const workers  = groupMap.get(groupName);

            const storageKey  = `group-collapsed:${groupName}`;
            const isCollapsed = localStorage.getItem(storageKey) === '1';

            // Section wrapper (a plain <li> acts as a structural container).
            const section = document.createElement('li');
            section.className    = 'group-section';
            section.dataset.group = groupName;

            // Group header.
            const header = document.createElement('div');
            header.className = 'group-header';
            header.style.borderLeftColor = color;
            header.innerHTML = `
                <span class="group-toggle${isCollapsed ? ' collapsed' : ''}"></span>
                <span class="group-label">${groupName}</span>
                <span class="group-count">${workers.length}</span>`;

            // Collapsible worker list.
            const subList = document.createElement('ul');
            subList.className = 'group-workers unstyled' + (isCollapsed ? ' collapsed' : '');

            for (const worker of workers) {
                const card = this._makeCard(worker, color);
                if (worker.process_name === activeId) card.classList.add('selected');
                subList.appendChild(card);
            }

            header.addEventListener('click', () => {
                const nowCollapsed = !subList.classList.contains('collapsed');
                subList.classList.toggle('collapsed', nowCollapsed);
                header.querySelector('.group-toggle').classList.toggle('collapsed', nowCollapsed);
                localStorage.setItem(storageKey, nowCollapsed ? '1' : '0');
            });

            section.appendChild(header);
            section.appendChild(subList);
            list.appendChild(section);
        }
    }
}

// ── Terminal session ─────────────────────────────────────────────────────────
// One live xterm.js instance connected to a worker PTY via WebSocket.

class TerminalSession {
    static _current = null;

    /**
     * Open a new terminal session for the given worker, closing any existing one.
     * @param {MouseEvent} ev
     * @param {string} workerId
     */
    static open(ev, workerId) {
        ev.preventDefault();
        ev.stopPropagation();

        const li = ev.target.closest('.terminal-worker-mdc');

        // No-op if this session is already the active one.
        if (li.classList.contains('selected')) return;
        if (TerminalSession._current?._workerId === workerId) return;

        // close() will deselect the old card via _cleanup().
        TerminalSession._current?.close();

        li.classList.add('selected');

        // Fetch host facts live from the worker so uptime/mem are always fresh.
        clearHostFacts();
        fetch(`/api/workers/${encodeURIComponent(workerId)}/facts`)
            .then(r => r.ok ? r.json() : {})
            .then(facts => { try { showHostFacts(facts); } catch (_) {} })
            .catch(() => {});

        const session = new TerminalSession(workerId);
        TerminalSession._current = session;
        session.start();
    }

    constructor(workerId) {
        this._workerId       = workerId;
        this._sock           = null;
        this._terminal       = null;
        this._resizeObserver = null;
    }

    start() {
        this._sock            = new WebSocket(`${WSURL}terminal?worker_id=${this._workerId}`);
        this._sock.binaryType = 'arraybuffer';

        // Build terminal without a theme first so xterm initialises its renderer,
        // then apply the theme via setOption (v4) or options (v5) to guarantee
        // the palette is wired up regardless of xterm.js version.
        this._terminal = new Terminal({ cursorBlink: true });
        if (typeof this._terminal.setOption === 'function') {
            this._terminal.setOption('theme', TERMINAL_THEMES[currentTheme()]);
        } else {
            this._terminal.options.theme = TERMINAL_THEMES[currentTheme()];
        }

        const fitAddon = new FitAddon.FitAddon();
        this._terminal.loadAddon(fitAddon);

        this._sock.onopen = () => {
            const container = document.getElementById('terminal');
            this._terminal.open(container);

            // ── Register ALL handlers BEFORE fitAddon.fit() ──────────────────
            // fitAddon.fit() calls terminal.resize() which fires onResize
            // synchronously.  If onResize is registered after fit(), the very
            // first resize event (which sets the initial PTY window size) is lost.

            this._terminal.onData((data) => {
                if (this._sock.readyState !== WebSocket.OPEN) return;
                const encoded = new TextEncoder().encode(data);
                const frame   = new Uint8Array(1 + encoded.length);
                frame[0]      = CMD_DATA;
                frame.set(encoded, 1);
                this._sock.send(frame.buffer);
            });

            this._terminal.onResize(({ rows, cols }) => {
                if (this._sock.readyState !== WebSocket.OPEN) return;
                const encoded = new TextEncoder().encode(`${rows},${cols}`);
                const frame   = new Uint8Array(1 + encoded.length);
                frame[0]      = CMD_RESIZE;
                frame.set(encoded, 1);
                this._sock.send(frame.buffer);
            });

            // Now fit — onResize fires here and the handler above catches it.
            fitAddon.fit();
            this._terminal.focus();

            // Refit whenever the container changes size (window resize, etc.).
            this._resizeObserver = new ResizeObserver(() => fitAddon.fit());
            this._resizeObserver.observe(container);
        };

        // PTY output → write raw bytes directly to xterm (no string conversion).
        this._sock.onmessage = ({ data }) => {
            this._terminal?.write(new Uint8Array(data));
        };

        this._sock.onclose = () => this._cleanup();
        this._sock.onerror = (e) => console.error('[terminal] socket error:', e);
    }

    close() {
        this._sock?.close();
        this._cleanup();
    }

    _cleanup() {
        this._resizeObserver?.disconnect();
        this._resizeObserver = null;

        this._terminal?.dispose();
        this._terminal = null;

        this._sock = null;

        // Only deselect this session's own card — leave other cards untouched.
        document.getElementById(this._workerId)?.classList.remove('selected');

        if (TerminalSession._current === this) {
            TerminalSession._current = null;
            try { clearHostFacts(); } catch (_) {}
        }
    }
}

// ── Theme switcher ───────────────────────────────────────────────────────────

const THEME_KEY = 'dt-theme';

// xterm.js colour palettes for each theme.
const TERMINAL_THEMES = {
    dark: {
        background:    '#2a2830',
        foreground:    '#d0d0e0',
        cursor:        '#a0a0c0',
        cursorAccent:  '#2a2830',
        selectionBackground: 'rgba(160,160,192,.3)',
    },
    light: {
        // Solarized Light
        background:    '#fdf6e3',
        foreground:    '#657b83',
        cursor:        '#586e75',
        cursorAccent:  '#fdf6e3',
        selectionBackground: '#eee8d5',
        black:         '#073642',
        red:           '#dc322f',
        green:         '#c0706a',  // pastel red  (normal green slot)
        yellow:        '#b58900',
        blue:          '#4a4a54',  // pastel black (normal blue slot)
        magenta:       '#d33682',
        cyan:          '#2aa198',
        white:         '#eee8d5',
        brightBlack:   '#002b36',
        brightRed:     '#cb4b16',
        brightGreen:   '#c0706a',  // pastel red  — bold green → user@hostname
        brightYellow:  '#657b83',
        brightBlue:    '#4a4a54',  // pastel black — bold blue  → ~/path$
        brightMagenta: '#6c71c4',
        brightCyan:    '#93a1a1',
        brightWhite:   '#fdf6e3',
    },
};

function currentTheme() {
    return document.documentElement.getAttribute('data-theme') || 'dark';
}

function applyTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem(THEME_KEY, theme);

    // Update the active xterm.js terminal in real-time.
    // xterm.js v4 uses setOption(); v5+ also accepts options.theme directly.
    // We try setOption first (v4), then fall back to options assignment (v5).
    const term = TerminalSession._current && TerminalSession._current._terminal;
    if (term) {
        if (typeof term.setOption === 'function') {
            term.setOption('theme', TERMINAL_THEMES[theme]);
        } else {
            term.options.theme = TERMINAL_THEMES[theme];
        }
        // Force a full redraw so existing buffer text adopts the new colours.
        term.refresh(0, term.rows - 1);
    }
}

function initTheme() {
    const saved = localStorage.getItem(THEME_KEY);
    applyTheme(saved === 'light' ? 'light' : 'dark');
}

// ── Bootstrap ────────────────────────────────────────────────────────────────

window.addEventListener('load', () => {
    // Apply stored / default theme before anything else renders.
    initTheme();

    // Wire up the toggle button.
    document.getElementById('theme-toggle').addEventListener('click', () => {
        const current = document.documentElement.getAttribute('data-theme');
        applyTheme(current === 'light' ? 'dark' : 'light');
    });

    // Populate the version badge — fallback to the in-HTML default "v1.0.0-debug"
    // if the endpoint is unreachable or returns an unexpected shape.
    fetch('/api/version')
        .then(r => { if (!r.ok) throw new Error(r.status); return r.json(); })
        .then(data => {
            const version = data && data.version;
            if (version) {
                const badge = document.getElementById('version-badge');
                if (badge) badge.textContent = `v${version}`;
            }
        })
        .catch(() => { /* keep the in-HTML default */ });

    new DirectorSocket().connect();
});
