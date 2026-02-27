/**
 * DBBench Dashboard Application
 * TPC-C Database Benchmark Tool - Web Interface
 */

// Global state
let ws = null;
let tpmcChart = null;
let cpuChart = null;
let networkChart = null;
let dbCpuChart = null;
let dbMemChart = null;
let dbDiskChart = null;
let dbNetChart = null;
let dbConnChart = null;
let chartTimeRange = 'all';
const chartDataBuffer = {
    tpmc:     { labels: [], timestamps: [], data: [] },
    cpu:     { labels: [], timestamps: [], data: [] },
    network: { labels: [], timestamps: [], recvData: [], sentData: [] },
    dbCpu:   { labels: [], timestamps: [], data: [] },
    dbMem:   { labels: [], timestamps: [], data: [] },
    dbDisk:  { labels: [], timestamps: [], readData: [], writeData: [] },
    dbNet:   { labels: [], timestamps: [], recvData: [], sentData: [] },
    dbConn:  { labels: [], timestamps: [], data: [] }
};
let currentConfig = null;
let currentProfileName = 'default';
let savedDbPassword = '';
let savedSshPassword = '';
let allLogs = [];
let statusPollInterval = null;
let lastStatus = null;

// DOM Ready
document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    connectWebSocket();
    loadInitialState();
    setupEventListeners();
    startStatusPolling();
});

// ==================== Initial State Loading ====================

async function loadInitialState() {
    try {
        // Load config, status, metrics, logs, and database types in parallel
        const [configRes, metricsRes, logsRes, dbTypesRes] = await Promise.all([
            fetch('/api/benchmark/config'),
            fetch('/api/metrics/current'),
            fetch('/api/benchmark/logs?limit=100'),
            fetch('/api/benchmark/database-types')
        ]);

        const config = await configRes.json();
        const metrics = await metricsRes.json();
        const logs = await logsRes.json();
        const dbTypes = await dbTypesRes.json();

        // Populate database type dropdown and merge URL templates
        populateDatabaseTypes(dbTypes);

        // Apply config
        currentConfig = config;
        displayConfig(config);

        // Restore form dropdown to match current config (populateDatabaseTypes resets it)
        if (config.database?.type) {
            document.getElementById('cfgFormDbType').value = config.database.type;
        }

        // Apply status
        updateStatus(metrics.status);
        lastStatus = metrics.status;

        // Apply metrics
        updateMetrics(metrics);

        // If running or has data, restore chart from history
        if (metrics.status === 'RUNNING' || metrics.status === 'STOPPED') {
            await restoreChartHistory();
        }

        // If loading, show progress
        if (metrics.loading) {
            updateLoadProgress(metrics.loadProgress || 0, metrics.loadMessage || 'Loading...');
        }

        // Apply logs
        allLogs = logs;
        const logEl = document.getElementById('log');
        logEl.innerHTML = '';
        logs.slice(-50).forEach(log => appendLogEntry(log));

        // Load hardware info
        loadHardwareInfo();

        addLog('Dashboard initialized', 'success');
    } catch (e) {
        console.error('Failed to load initial state:', e);
        addLog('Failed to load initial state: ' + e.message, 'error');
    }
}

async function restoreChartHistory() {
    try {
        const res = await fetch('/api/metrics/history');
        const history = await res.json();

        if (history && history.length > 0) {
            // Clear all buffers
            for (const buf of Object.values(chartDataBuffer)) {
                for (const key of Object.keys(buf)) { buf[key] = []; }
            }

            history.forEach(snapshot => {
                const time = new Date(snapshot.timestamp).toLocaleTimeString();
                const ts = new Date(snapshot.timestamp).getTime();
                const tx = snapshot.transactionMetrics || {};
                const client = snapshot.clientMetrics || {};
                const db = snapshot.databaseMetrics || {};
                const host = snapshot.dbHostMetrics || {};

                // TPS
                chartDataBuffer.tpmc.labels.push(time);
                chartDataBuffer.tpmc.timestamps.push(ts);
                chartDataBuffer.tpmc.data.push(tx.nopm || 0);

                // Client CPU
                chartDataBuffer.cpu.labels.push(time);
                chartDataBuffer.cpu.timestamps.push(ts);
                chartDataBuffer.cpu.data.push(client.cpuUsage || 0);

                // Client Network
                chartDataBuffer.network.labels.push(time);
                chartDataBuffer.network.timestamps.push(ts);
                chartDataBuffer.network.recvData.push(client.networkRecvBytesPerSec || 0);
                chartDataBuffer.network.sentData.push(client.networkSentBytesPerSec || 0);

                // DB CPU
                if (host.cpuUsage !== undefined) {
                    chartDataBuffer.dbCpu.labels.push(time);
                    chartDataBuffer.dbCpu.timestamps.push(ts);
                    chartDataBuffer.dbCpu.data.push(host.cpuUsage);
                }

                // DB Buffer Pool Hit Ratio
                const hitRatio = db.buffer_pool_hit_ratio || db.cache_hit_ratio || db.plan_cache_hit_ratio;
                if (hitRatio !== undefined) {
                    chartDataBuffer.dbMem.labels.push(time);
                    chartDataBuffer.dbMem.timestamps.push(ts);
                    chartDataBuffer.dbMem.data.push(hitRatio);
                }

                // DB Disk I/O
                if (host.diskReadBytesPerSec !== undefined) {
                    chartDataBuffer.dbDisk.labels.push(time);
                    chartDataBuffer.dbDisk.timestamps.push(ts);
                    chartDataBuffer.dbDisk.readData.push(host.diskReadBytesPerSec);
                    chartDataBuffer.dbDisk.writeData.push(host.diskWriteBytesPerSec || 0);
                }

                // DB Network I/O
                if (host.networkRecvBytesPerSec !== undefined) {
                    chartDataBuffer.dbNet.labels.push(time);
                    chartDataBuffer.dbNet.timestamps.push(ts);
                    chartDataBuffer.dbNet.recvData.push(host.networkRecvBytesPerSec);
                    chartDataBuffer.dbNet.sentData.push(host.networkSentBytesPerSec || 0);
                }

                // DB Connections
                const conn = db.active_connections || db.activeConnections;
                if (conn !== undefined) {
                    chartDataBuffer.dbConn.labels.push(time);
                    chartDataBuffer.dbConn.timestamps.push(ts);
                    chartDataBuffer.dbConn.data.push(conn);
                }
            });

            applyTimeRange();
            addLog(`Restored ${history.length} chart data points`, 'info');
        }
    } catch (e) {
        console.error('Failed to restore chart history:', e);
    }
}

// ==================== Status Polling ====================

function startStatusPolling() {
    // Poll status every 2 seconds as a fallback when WebSocket is disconnected
    statusPollInterval = setInterval(async () => {
        // Only poll if WebSocket is not connected
        if (ws && ws.readyState === WebSocket.OPEN) {
            return;
        }

        try {
            const res = await fetch('/api/metrics/current');
            const data = await res.json();
            updateMetrics(data);

            // Check for status changes
            if (data.status !== lastStatus) {
                lastStatus = data.status;
                addLog(`Status changed to: ${data.status}`, 'info');
            }
        } catch (e) {
            console.error('Status poll failed:', e);
        }
    }, 2000);
}

// ==================== Charts ====================

function initCharts() {
    // tpmcChart
    const tpmcCtx = document.getElementById('tpmcChart').getContext('2d');
    tpmcChart = new Chart(tpmcCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'tpmC',
                data: [],
                borderColor: '#00d9ff',
                backgroundColor: 'rgba(0, 217, 255, 0.1)',
                fill: true,
                tension: 0.4,
                pointRadius: 0,
                pointHoverRadius: 3,
                borderWidth: 1.5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { color: '#333' }, ticks: { color: '#888', maxTicksLimit: 10 } },
                y: { grid: { color: '#333' }, ticks: { color: '#888' }, beginAtZero: true, suggestedMax: 10 }
            }
        }
    });

    // CPU Chart
    const cpuCtx = document.getElementById('cpuChart').getContext('2d');
    cpuChart = new Chart(cpuCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'CPU %',
                data: [],
                borderColor: '#ff6b6b',
                backgroundColor: 'rgba(255, 107, 107, 0.1)',
                fill: true,
                tension: 0.4,
                pointRadius: 0,
                pointHoverRadius: 3,
                borderWidth: 1.5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { color: '#333' }, ticks: { color: '#888', maxTicksLimit: 10 } },
                y: { grid: { color: '#333' }, ticks: { color: '#888' }, beginAtZero: true, max: 100 }
            }
        }
    });

    // Network Chart
    const netCtx = document.getElementById('networkChart').getContext('2d');
    networkChart = new Chart(netCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Recv',
                    data: [],
                    borderColor: '#00ff88',
                    backgroundColor: 'rgba(0, 255, 136, 0.1)',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 0,
                    pointHoverRadius: 3,
                    borderWidth: 1.5
                },
                {
                    label: 'Sent',
                    data: [],
                    borderColor: '#ffa500',
                    backgroundColor: 'rgba(255, 165, 0, 0.1)',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 0,
                    pointHoverRadius: 3,
                    borderWidth: 1.5
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: { color: '#888', boxWidth: 12, padding: 10 }
                }
            },
            scales: {
                x: { grid: { color: '#333' }, ticks: { color: '#888', maxTicksLimit: 10 } },
                y: {
                    grid: { color: '#333' },
                    ticks: {
                        color: '#888',
                        callback: function(value) { return formatBytesShort(value) + '/s'; }
                    },
                    beginAtZero: true
                }
            }
        }
    });

    // Database CPU Chart
    const dbCpuCtx = document.getElementById('dbCpuChart').getContext('2d');
    dbCpuChart = new Chart(dbCpuCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'DB CPU %',
                data: [],
                borderColor: '#ff6b6b',
                backgroundColor: 'rgba(255, 107, 107, 0.1)',
                fill: true,
                tension: 0.4,
                pointRadius: 0,
                pointHoverRadius: 3,
                borderWidth: 1.5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { color: '#333' }, ticks: { color: '#888', maxTicksLimit: 10 } },
                y: { grid: { color: '#333' }, ticks: { color: '#888' }, beginAtZero: true, max: 100 }
            }
        }
    });

    // Buffer Pool Hit Ratio Chart
    const dbMemCtx = document.getElementById('dbMemChart').getContext('2d');
    dbMemChart = new Chart(dbMemCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Hit Ratio %',
                data: [],
                borderColor: '#2ecc71',
                backgroundColor: 'rgba(46, 204, 113, 0.1)',
                fill: true,
                tension: 0.4,
                pointRadius: 0,
                pointHoverRadius: 3,
                borderWidth: 1.5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { color: '#333' }, ticks: { color: '#888', maxTicksLimit: 10 } },
                y: { grid: { color: '#333' }, ticks: { color: '#888' }, beginAtZero: true, max: 100 }
            }
        }
    });

    // Database Disk I/O Chart
    const dbDiskCtx = document.getElementById('dbDiskChart').getContext('2d');
    dbDiskChart = new Chart(dbDiskCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Read',
                    data: [],
                    borderColor: '#3498db',
                    backgroundColor: 'rgba(52, 152, 219, 0.1)',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 0,
                    pointHoverRadius: 3,
                    borderWidth: 1.5
                },
                {
                    label: 'Write',
                    data: [],
                    borderColor: '#e74c3c',
                    backgroundColor: 'rgba(231, 76, 60, 0.1)',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 0,
                    pointHoverRadius: 3,
                    borderWidth: 1.5
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: { color: '#888', boxWidth: 12, padding: 10 }
                }
            },
            scales: {
                x: { grid: { color: '#333' }, ticks: { color: '#888', maxTicksLimit: 10 } },
                y: {
                    grid: { color: '#333' },
                    ticks: {
                        color: '#888',
                        callback: function(value) { return formatBytesShort(value) + '/s'; }
                    },
                    beginAtZero: true
                }
            }
        }
    });

    // Database Network I/O Chart
    const dbNetCtx = document.getElementById('dbNetChart').getContext('2d');
    dbNetChart = new Chart(dbNetCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Recv',
                    data: [],
                    borderColor: '#1abc9c',
                    backgroundColor: 'rgba(26, 188, 156, 0.1)',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 0,
                    pointHoverRadius: 3,
                    borderWidth: 1.5
                },
                {
                    label: 'Sent',
                    data: [],
                    borderColor: '#e67e22',
                    backgroundColor: 'rgba(230, 126, 34, 0.1)',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 0,
                    pointHoverRadius: 3,
                    borderWidth: 1.5
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: { color: '#888', boxWidth: 12, padding: 10 }
                }
            },
            scales: {
                x: { grid: { color: '#333' }, ticks: { color: '#888', maxTicksLimit: 10 } },
                y: {
                    grid: { color: '#333' },
                    ticks: {
                        color: '#888',
                        callback: function(value) { return formatBytesShort(value) + '/s'; }
                    },
                    beginAtZero: true
                }
            }
        }
    });

    // Database Connections Chart
    const dbConnCtx = document.getElementById('dbConnChart').getContext('2d');
    dbConnChart = new Chart(dbConnCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Connections',
                    data: [],
                    borderColor: '#00d9ff',
                    backgroundColor: 'rgba(0, 217, 255, 0.1)',
                    fill: true,
                    tension: 0.4,
                    pointRadius: 0,
                    pointHoverRadius: 3,
                    borderWidth: 1.5
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { color: '#333' }, ticks: { color: '#888', maxTicksLimit: 10 } },
                y: { grid: { color: '#333' }, ticks: { color: '#888' }, beginAtZero: true }
            }
        }
    });
}

// ==================== WebSocket ====================

function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(`${protocol}//${window.location.host}/ws/metrics`);

    ws.onopen = () => {
        document.getElementById('wsStatus').className = 'ws-status ws-connected';
        addLog('WebSocket connected', 'success');
    };

    ws.onclose = () => {
        document.getElementById('wsStatus').className = 'ws-status ws-disconnected';
        addLog('WebSocket disconnected, reconnecting...', 'warn');
        setTimeout(connectWebSocket, 3000);
    };

    ws.onerror = () => {
        console.error('WebSocket error');
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            handleWebSocketMessage(data);
        } catch (e) {
            console.error('Failed to parse WebSocket message:', e);
        }
    };
}

function handleWebSocketMessage(data) {
    // Handle log messages
    if (data.type === 'log' && data.log) {
        const log = data.log;
        allLogs.push(log);
        if (allLogs.length > 1000) allLogs.shift();
        appendLogEntry(log);
        return;
    }

    // Handle progress updates
    if (data.type === 'progress') {
        updateLoadProgress(data.progress, data.message);
        if (data.status) {
            updateStatus(data.status);
            lastStatus = data.status;
        }
        return;
    }

    // Handle status change notifications
    if (data.type === 'status') {
        console.log('Status change received:', data.status);
        updateStatus(data.status);
        lastStatus = data.status;
        return;
    }

    // Handle metrics updates
    updateMetrics(data);
}

// ==================== Metrics Update ====================

function updateMetrics(data) {
    if (data.transaction) {
        const tx = data.transaction;
        document.getElementById('tpmc').textContent = Math.round(tx.nopm || 0);
        document.getElementById('tpm').textContent = Math.round(tx.tpm || 0);
        document.getElementById('newOrderLatency').textContent = (tx.newOrderAvgLatencyMs?.toFixed(2) || '0.00') + ' ms';
        document.getElementById('totalTx').textContent = tx.totalTransactions || 0;
        document.getElementById('successRate').textContent = (tx.overallSuccessRate || 0).toFixed(1) + '%';
        document.getElementById('avgLatency').textContent = (tx.avgLatencyMs?.toFixed(2) || '0.00') + ' ms';
        document.getElementById('elapsed').textContent = (tx.elapsedSeconds || 0) + 's';

        // Update chart - only when benchmark is running
        if (data.status === 'RUNNING' && tx.nopm !== undefined) {
            const now = new Date().toLocaleTimeString();
            const ts = Date.now();
            chartDataBuffer.tpmc.labels.push(now);
            chartDataBuffer.tpmc.timestamps.push(ts);
            chartDataBuffer.tpmc.data.push(tx.nopm || 0);
            applyTimeRangeToChart('tpmc');
        }

        // Update transaction table
        if (tx.transactions && tx.transactions.length > 0) {
            const tbody = document.getElementById('txTable');
            tbody.innerHTML = tx.transactions.map(t => `
                <tr>
                    <td>${t.name}</td>
                    <td>${t.count}</td>
                    <td style="color: #00ff88">${t.success}</td>
                    <td style="color: #ffa500">${t.rollback || 0}</td>
                    <td style="color: #ff4757">${t.error || 0}</td>
                    <td>${t.successRate?.toFixed(1) || 0}%</td>
                    <td>${t.avgLatencyMs?.toFixed(2) || 0} ms</td>
                </tr>
            `).join('');
        }
    }

    if (data.client) {
        const client = data.client;
        document.getElementById('cpuUsage').textContent = (client.cpuUsage || 0).toFixed(1) + '%';
        document.getElementById('cpuBar').style.width = Math.min(client.cpuUsage || 0, 100) + '%';
        document.getElementById('memUsage').textContent = (client.memoryUsage || 0).toFixed(1) + '%';
        document.getElementById('memBar').style.width = Math.min(client.memoryUsage || 0, 100) + '%';
        document.getElementById('loadAvg').textContent = client.loadAvg1?.toFixed(2) || '0.00';

        // Always update CPU and Network charts for continuous monitoring
        const now = new Date().toLocaleTimeString();
        const ts = Date.now();

        // CPU chart
        chartDataBuffer.cpu.labels.push(now);
        chartDataBuffer.cpu.timestamps.push(ts);
        chartDataBuffer.cpu.data.push(client.cpuUsage || 0);
        applyTimeRangeToChart('cpu');

        // Network chart
        chartDataBuffer.network.labels.push(now);
        chartDataBuffer.network.timestamps.push(ts);
        chartDataBuffer.network.recvData.push(client.networkRecvBytesPerSec || 0);
        chartDataBuffer.network.sentData.push(client.networkSentBytesPerSec || 0);
        applyTimeRangeToChart('network');
    }

    if (data.database) {
        const db = data.database;
        document.getElementById('dbConnections').textContent = db.active_connections || db.activeConnections || 0;
        document.getElementById('bufferHit').textContent = (db.buffer_pool_hit_ratio || db.cache_hit_ratio || 0).toFixed(1) + '%';

        // Update database connections chart
        const now = new Date().toLocaleTimeString();
        const ts = Date.now();
        const connections = db.active_connections || db.activeConnections || 0;

        chartDataBuffer.dbConn.labels.push(now);
        chartDataBuffer.dbConn.timestamps.push(ts);
        chartDataBuffer.dbConn.data.push(connections);
        applyTimeRangeToChart('dbConn');

        // Buffer Pool Hit Ratio Chart
        const hitRatio = db.buffer_pool_hit_ratio || db.cache_hit_ratio || db.plan_cache_hit_ratio;
        if (hitRatio !== undefined) {
            chartDataBuffer.dbMem.labels.push(now);
            chartDataBuffer.dbMem.timestamps.push(ts);
            chartDataBuffer.dbMem.data.push(hitRatio);
            applyTimeRangeToChart('dbMem');
        }
    }

    // Database Host Metrics - Update Charts
    // Only use dbHost data (from SSH or SQL queries against the database server)
    // data.client is the local machine metrics and should NOT be used here
    const hostData = data.dbHost || {};

    {
        const now = new Date().toLocaleTimeString();
        const ts = Date.now();

        // CPU Chart - dbHost only
        const cpuVal = hostData.cpuUsage;
        if (cpuVal !== undefined) {
            chartDataBuffer.dbCpu.labels.push(now);
            chartDataBuffer.dbCpu.timestamps.push(ts);
            chartDataBuffer.dbCpu.data.push(cpuVal);
            applyTimeRangeToChart('dbCpu');
        }

        // Disk I/O Chart - dbHost only
        if (hostData.diskReadBytesPerSec !== undefined) {
            chartDataBuffer.dbDisk.labels.push(now);
            chartDataBuffer.dbDisk.timestamps.push(ts);
            chartDataBuffer.dbDisk.readData.push(hostData.diskReadBytesPerSec);
            chartDataBuffer.dbDisk.writeData.push(hostData.diskWriteBytesPerSec || 0);
            applyTimeRangeToChart('dbDisk');
        }

        // Update Database Metrics panel text fields
        // CPU Usage
        const dbCpuVal = hostData.cpuUsage;
        if (dbCpuVal !== undefined) {
            document.getElementById('dbCpuUsage').textContent = dbCpuVal.toFixed(1) + '%';
        }
        // Disk I/O - show read+write rate
        {
            let diskRead = 0, diskWrite = 0, hasDisk = false;
            if (hostData.diskReadBytesPerSec !== undefined) {
                diskRead = hostData.diskReadBytesPerSec || 0;
                diskWrite = hostData.diskWriteBytesPerSec || 0;
                hasDisk = true;
            }
            if (hasDisk) {
                document.getElementById('dbDiskIO').textContent =
                    formatBytesShort(diskRead) + '/s R | ' + formatBytesShort(diskWrite) + '/s W';
            }
        }
        // Network I/O - show recv+sent rate
        {
            let netRecv = 0, netSent = 0, hasNet = false;
            if (hostData.networkRecvBytesPerSec !== undefined) {
                netRecv = hostData.networkRecvBytesPerSec || 0;
                netSent = hostData.networkSentBytesPerSec || 0;
                hasNet = true;
            }
            if (hasNet) {
                document.getElementById('dbNetIO').textContent =
                    formatBytesShort(netRecv) + '/s ↓ | ' + formatBytesShort(netSent) + '/s ↑';
            }
        }

        // Network I/O Chart - dbHost only
        if (hostData.networkRecvBytesPerSec !== undefined) {
            chartDataBuffer.dbNet.labels.push(now);
            chartDataBuffer.dbNet.timestamps.push(ts);
            chartDataBuffer.dbNet.recvData.push(hostData.networkRecvBytesPerSec);
            chartDataBuffer.dbNet.sentData.push(hostData.networkSentBytesPerSec || 0);
            applyTimeRangeToChart('dbNet');
        }
    }

    // Update SSH connection status in real-time
    if (data.ssh) {
        const sshStatusEl = document.getElementById('cfgSshStatus');
        if (sshStatusEl) {
            if (data.ssh.enabled) {
                const host = data.ssh.host || '(auto-detect)';
                const connStatus = data.ssh.connected ? 'Connected' : 'Disconnected';
                sshStatusEl.textContent = `SSH: ${connStatus} (${host}:${data.ssh.port})`;
                sshStatusEl.style.color = data.ssh.connected ? '#00ff88' : '#ff4757';
            } else {
                sshStatusEl.textContent = 'SSH: Disabled';
                sshStatusEl.style.color = '#888';
            }
        }
    }

    if (data.status) {
        updateStatus(data.status);
        lastStatus = data.status;
    }

    // Handle loading progress from metrics endpoint
    if (data.loading && data.loadProgress !== undefined) {
        updateLoadProgress(data.loadProgress, data.loadMessage || 'Loading...');
    }
}

// ==================== Status ====================

function updateStatus(status) {
    const el = document.getElementById('status');
    const oldStatus = el.textContent;
    el.textContent = status;
    el.className = 'status status-' + status.toLowerCase();

    // Detect status changes and show notifications
    if (oldStatus && oldStatus !== status && oldStatus !== 'IDLE') {
        handleStatusChange(oldStatus, status);
    }

    const isRunning = status === 'RUNNING';
    const isLoading = status === 'LOADING';
    const isStopping = status === 'STOPPING';
    const canConfig = !isRunning && !isLoading && !isStopping;

    document.getElementById('btnStart').disabled = isRunning || isLoading;
    document.getElementById('btnStop').disabled = !isRunning;
    document.getElementById('btnLoad').disabled = isRunning || isLoading;
    document.getElementById('btnClean').disabled = isRunning || isLoading;
    document.getElementById('btnConfig').disabled = !canConfig;
    document.getElementById('btnReport').disabled = (status !== 'STOPPED' && status !== 'ERROR' && status !== 'RUNNING');

    // Show/hide progress container
    const progressContainer = document.getElementById('loadProgressContainer');
    if (isLoading) {
        progressContainer.classList.add('active');
        document.getElementById('btnCancelLoad').disabled = false;
    } else if (status === 'LOADED' || status === 'ERROR' || status === 'INITIALIZED' || status === 'CANCELLED') {
        // Hide after a delay
        setTimeout(() => {
            const currentStatus = document.getElementById('status').textContent;
            if (currentStatus !== 'LOADING') {
                progressContainer.classList.remove('active');
            }
        }, 3000);
    }
}

// ==================== Configuration ====================

function displayConfig(cfg) {
    document.getElementById('cfgProfileName').textContent = currentProfileName || 'default';
    if (cfg.database) {
        document.getElementById('cfgDbType').textContent = cfg.database.type?.toUpperCase() || '-';
        document.getElementById('cfgJdbcUrl').textContent = cfg.database.jdbcUrl || '-';
    }

    if (cfg.benchmark) {
        document.getElementById('cfgWarehouses').textContent = cfg.benchmark.warehouses || '-';
        document.getElementById('cfgTerminals').textContent = cfg.benchmark.terminals || '-';
        document.getElementById('cfgDuration').textContent = (cfg.benchmark.duration || '-') + 's';
        document.getElementById('cfgLoadConcurrency').textContent = cfg.benchmark.loadConcurrency || '-';
        document.getElementById('cfgLoadMode').textContent = cfg.benchmark.loadMode || 'auto';
    }

    if (cfg.transactionMix) {
        document.getElementById('cfgMixNewOrder').textContent = cfg.transactionMix.newOrder + '%';
        document.getElementById('cfgMixPayment').textContent = cfg.transactionMix.payment + '%';
        document.getElementById('cfgMixOrderStatus').textContent = cfg.transactionMix.orderStatus + '%';
        document.getElementById('cfgMixDelivery').textContent = cfg.transactionMix.delivery + '%';
        document.getElementById('cfgMixStockLevel').textContent = cfg.transactionMix.stockLevel + '%';
    }

    // SSH status
    const sshStatusEl = document.getElementById('cfgSshStatus');
    if (sshStatusEl && cfg.ssh) {
        if (cfg.ssh.enabled) {
            const host = cfg.ssh.host || '(auto-detect)';
            const connStatus = cfg.ssh.connected ? 'Connected' : 'Disconnected';
            sshStatusEl.textContent = `SSH: ${connStatus} (${host}:${cfg.ssh.port})`;
            sshStatusEl.style.color = cfg.ssh.connected ? '#00ff88' : '#ff4757';
        } else {
            sshStatusEl.textContent = 'SSH: Disabled';
            sshStatusEl.style.color = '#888';
        }
    }
}

function openConfigModal() {
    if (!currentConfig) {
        addLog('Configuration not loaded yet', 'error');
        return;
    }

    // Load profiles list
    loadProfiles();

    // Populate form with current config
    const cfg = currentConfig;

    // Database config
    document.getElementById('cfgFormDbType').value = cfg.database?.type || 'mysql';
    document.getElementById('cfgFormJdbcUrl').value = cfg.database?.jdbcUrl || '';
    document.getElementById('cfgFormDbUser').value = cfg.database?.username || '';
    document.getElementById('cfgFormDbPass').value = savedDbPassword;

    // Benchmark config
    document.getElementById('cfgFormWarehouses').value = cfg.benchmark?.warehouses || 10;
    document.getElementById('cfgFormTerminals').value = cfg.benchmark?.terminals || 50;
    document.getElementById('cfgFormDuration').value = cfg.benchmark?.duration || 60;
    document.getElementById('cfgFormLoadConcurrency').value = cfg.benchmark?.loadConcurrency || 4;
    document.getElementById('cfgFormThinkTime').checked = cfg.benchmark?.thinkTime || false;
    document.getElementById('cfgFormLoadMode').value = cfg.benchmark?.loadMode || 'auto';

    // Transaction mix
    document.getElementById('cfgFormMixNewOrder').value = cfg.transactionMix?.newOrder || 45;
    document.getElementById('cfgFormMixPayment').value = cfg.transactionMix?.payment || 43;
    document.getElementById('cfgFormMixOrderStatus').value = cfg.transactionMix?.orderStatus || 4;
    document.getElementById('cfgFormMixDelivery').value = cfg.transactionMix?.delivery || 4;
    document.getElementById('cfgFormMixStockLevel').value = cfg.transactionMix?.stockLevel || 4;

    // SSH config
    document.getElementById('cfgFormSshEnabled').checked = cfg.ssh?.enabled || false;
    document.getElementById('cfgFormSshHost').value = cfg.ssh?.host || '';
    document.getElementById('cfgFormSshPort').value = cfg.ssh?.port || 22;
    document.getElementById('cfgFormSshUser').value = cfg.ssh?.username || 'root';
    document.getElementById('cfgFormSshPass').value = savedSshPassword;
    document.getElementById('cfgFormSshKey').value = '';
    toggleSshFields();

    // Apply SQLite constraints if needed
    applySqliteConstraints(cfg.database?.type || 'mysql');

    // Clear connection test result
    document.getElementById('connectionTestResult').innerHTML = '';

    openModal('configModal');
}

async function saveConfig() {
    const dbType = document.getElementById('cfgFormDbType').value;
    const isSqlite = dbType === 'sqlite';

    const newConfig = {
        database: {
            type: dbType,
            jdbcUrl: document.getElementById('cfgFormJdbcUrl').value,
            username: document.getElementById('cfgFormDbUser').value
        },
        benchmark: {
            warehouses: parseInt(document.getElementById('cfgFormWarehouses').value),
            terminals: isSqlite ? 1 : parseInt(document.getElementById('cfgFormTerminals').value),
            duration: parseInt(document.getElementById('cfgFormDuration').value),
            loadConcurrency: isSqlite ? 1 : parseInt(document.getElementById('cfgFormLoadConcurrency').value),
            thinkTime: document.getElementById('cfgFormThinkTime').checked,
            loadMode: document.getElementById('cfgFormLoadMode').value
        },
        transactionMix: {
            newOrder: parseInt(document.getElementById('cfgFormMixNewOrder').value),
            payment: parseInt(document.getElementById('cfgFormMixPayment').value),
            orderStatus: parseInt(document.getElementById('cfgFormMixOrderStatus').value),
            delivery: parseInt(document.getElementById('cfgFormMixDelivery').value),
            stockLevel: parseInt(document.getElementById('cfgFormMixStockLevel').value)
        }
    };

    // Cache passwords
    const password = document.getElementById('cfgFormDbPass').value;
    if (password) {
        savedDbPassword = password;
        newConfig.database.password = password;
    }

    // SSH config
    const sshEnabled = document.getElementById('cfgFormSshEnabled').checked;
    newConfig.ssh = {
        enabled: sshEnabled,
        host: document.getElementById('cfgFormSshHost').value,
        port: parseInt(document.getElementById('cfgFormSshPort').value) || 22,
        username: document.getElementById('cfgFormSshUser').value
    };
    const sshPass = document.getElementById('cfgFormSshPass').value;
    if (sshPass) {
        savedSshPassword = sshPass;
        newConfig.ssh.password = sshPass;
    }
    const sshKey = document.getElementById('cfgFormSshKey').value;
    if (sshKey) {
        newConfig.ssh.privateKey = sshKey;
    }

    // Validate transaction mix
    const mixTotal = newConfig.transactionMix.newOrder +
                     newConfig.transactionMix.payment +
                     newConfig.transactionMix.orderStatus +
                     newConfig.transactionMix.delivery +
                     newConfig.transactionMix.stockLevel;

    if (mixTotal !== 100) {
        showToast('error', 'Invalid Configuration', `Transaction mix must total 100% (currently ${mixTotal}%)`);
        return;
    }

    try {
        const res = await fetch('/api/benchmark/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(newConfig)
        });
        const data = await res.json();

        if (data.success) {
            currentConfig = data.config;
            currentProfileName = document.getElementById('profileSelect').value || 'default';
            displayConfig(currentConfig);

            // Save to current profile
            const profileName = document.getElementById('profileSelect').value;
            if (profileName && profileName !== 'default') {
                try {
                    await fetch(`/api/benchmark/profiles/${encodeURIComponent(profileName)}`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(newConfig)
                    });
                } catch (ignored) {}
            }

            closeModal('configModal');
            addLog('Configuration saved successfully', 'success');
            showToast('success', 'Configuration Saved', 'Benchmark configuration has been updated');

            // Clear connection test result
            document.getElementById('connectionTestResult').innerHTML = '';
        } else {
            addLog('Failed to save config: ' + data.error, 'error');
            showToast('error', 'Save Failed', data.error);
        }
    } catch (e) {
        addLog('Failed to save config: ' + e.message, 'error');
        showToast('error', 'Save Failed', e.message);
    }
}

// ==================== Load Progress ====================

function updateLoadProgress(progress, message) {
    const container = document.getElementById('loadProgressContainer');
    const bar = document.getElementById('loadProgressBar');
    const percent = document.getElementById('loadProgressPercent');
    const msg = document.getElementById('loadProgressMessage');

    container.classList.add('active');

    if (progress >= 0) {
        bar.style.width = progress + '%';
        percent.textContent = progress + '%';
    } else {
        bar.style.width = '0%';
        percent.textContent = 'Error';
    }

    msg.textContent = message || '';
}

// ==================== Logs ====================

function addLog(message, type = 'info') {
    const logEl = document.getElementById('log');
    const entry = document.createElement('div');
    entry.className = 'log-entry ' + type;
    const timestamp = new Date().toLocaleTimeString();
    entry.innerHTML = `<span class="log-timestamp">[${timestamp}]</span>${message}`;
    logEl.appendChild(entry);
    logEl.scrollTop = logEl.scrollHeight;

    // Keep log size manageable
    while (logEl.children.length > 100) {
        logEl.removeChild(logEl.firstChild);
    }
}

function appendLogEntry(log) {
    const logEl = document.getElementById('log');
    const entry = document.createElement('div');
    entry.className = 'log-entry ' + (log.level || 'info').toLowerCase();
    entry.innerHTML = `<span class="log-timestamp">[${log.timestamp || new Date().toLocaleTimeString()}]</span>${escapeHtml(log.message)}`;
    logEl.appendChild(entry);
    logEl.scrollTop = logEl.scrollHeight;

    while (logEl.children.length > 100) {
        logEl.removeChild(logEl.firstChild);
    }
}

async function openLogViewer() {
    // Load full log history
    try {
        const res = await fetch('/api/benchmark/logs?limit=1000');
        allLogs = await res.json();
    } catch (e) {
        console.error('Failed to load logs:', e);
    }

    renderLogViewer();
    openModal('logModal');
}

function renderLogViewer(filter = '', level = 'all') {
    const viewer = document.getElementById('logViewerContent');
    const filterLower = filter.toLowerCase();

    const filteredLogs = allLogs.filter(log => {
        const matchesText = !filter || log.message.toLowerCase().includes(filterLower);
        const matchesLevel = level === 'all' || log.level?.toLowerCase() === level;
        return matchesText && matchesLevel;
    });

    if (filteredLogs.length === 0) {
        viewer.innerHTML = '<div class="log-entry text-muted text-center" style="padding: 20px;">No logs found</div>';
        return;
    }

    viewer.innerHTML = filteredLogs.map(log => `
        <div class="log-entry ${(log.level || 'info').toLowerCase()}">
            <span class="log-timestamp">[${log.timestamp || '-'}]</span>
            <span class="log-level">[${log.level || 'INFO'}]</span>
            ${escapeHtml(log.message)}
        </div>
    `).join('');

    viewer.scrollTop = viewer.scrollHeight;
}

function filterLogs() {
    const filter = document.getElementById('logFilter').value;
    const level = document.querySelector('.log-filter-btn.active')?.dataset.level || 'all';
    renderLogViewer(filter, level);
}

function setLogLevel(btn, level) {
    document.querySelectorAll('.log-filter-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    filterLogs();
}

async function clearLogs() {
    if (!confirm('Are you sure you want to clear all logs?')) return;

    try {
        await fetch('/api/benchmark/logs', { method: 'DELETE' });
        allLogs = [];
        document.getElementById('log').innerHTML = '<div class="log-entry">Logs cleared</div>';
        renderLogViewer();
        addLog('Logs cleared', 'info');
    } catch (e) {
        addLog('Failed to clear logs: ' + e.message, 'error');
    }
}

// ==================== API Calls ====================

async function apiCall(endpoint, method = 'POST', body = null) {
    try {
        const options = { method };
        if (body) {
            options.headers = { 'Content-Type': 'application/json' };
            options.body = JSON.stringify(body);
        }

        const res = await fetch(`/api/benchmark/${endpoint}`, options);
        const data = await res.json();

        if (data.success) {
            addLog(data.message, 'success');
            if (data.status) {
                updateStatus(data.status);
                lastStatus = data.status;
            }
        } else {
            addLog('Error: ' + data.error, 'error');
            showToast('error', 'Operation Failed', data.error);
        }
        return data;
    } catch (e) {
        addLog('Request failed: ' + e.message, 'error');
        showToast('error', 'Request Failed', e.message);
        return { success: false, error: e.message };
    }
}

// ==================== Confirm Dialog ====================

let confirmCallback = null;

function showConfirmDialog(title, message, btnClass, onConfirm) {
    document.getElementById('confirmTitle').textContent = title;
    document.getElementById('confirmMessage').textContent = message;
    const btn = document.getElementById('confirmBtn');
    btn.className = 'btn ' + btnClass;
    btn.textContent = 'Confirm';
    confirmCallback = onConfirm;
    openModal('confirmModal');
}

function closeConfirmModal() {
    closeModal('confirmModal');
    confirmCallback = null;
}

function executeConfirm() {
    if (confirmCallback) {
        confirmCallback();
    }
    closeConfirmModal();
}

// ==================== Operations ====================

function loadData() {
    showConfirmDialog('Load Data', 'Are you sure you want to load TPC-C test data?', 'btn-warning', doLoadData);
}

async function doLoadData() {
    addLog('Starting data load... this may take several minutes', 'info');
    showToast('info', 'Data Loading', 'Starting data load, this may take several minutes');
    document.getElementById('loadProgressContainer').classList.add('active');
    document.getElementById('btnCancelLoad').disabled = false;
    updateLoadProgress(0, 'Starting...');
    await apiCall('load');
}

function cancelLoad() {
    showConfirmDialog('Cancel Loading', 'Are you sure you want to cancel data loading? Partial data may remain in the database.', 'btn-danger', doCancelLoad);
}

async function doCancelLoad() {
    addLog('Cancelling data load...', 'warn');
    document.getElementById('btnCancelLoad').disabled = true;
    const result = await apiCall('load/cancel');
    if (result.success) {
        showToast('warning', 'Cancelling', 'Data loading is being cancelled...');
    }
}

function cleanData() {
    showConfirmDialog('Clean Data', 'Are you sure you want to clean all test data? This will drop all TPC-C tables.', 'btn-danger', doCleanData);
}

async function doCleanData() {
    addLog('Cleaning test data...', 'info');
    const result = await apiCall('clean');
    if (result.success) {
        showToast('success', 'Data Cleaned', 'All TPC-C tables have been dropped');
    }
}

function startBenchmark() {
    showConfirmDialog('Start Benchmark', 'Are you sure you want to start the benchmark?', 'btn-primary', doStartBenchmark);
}

async function doStartBenchmark() {
    // Clear TPS chart data for new run
    tpmcChart.data.labels = [];
    tpmcChart.data.datasets[0].data = [];
    tpmcChart.update();

    // Clear database charts for new run
    dbCpuChart.data.labels = [];
    dbCpuChart.data.datasets[0].data = [];
    dbCpuChart.update();

    dbMemChart.data.labels = [];
    dbMemChart.data.datasets[0].data = [];
    dbMemChart.update();

    dbDiskChart.data.labels = [];
    dbDiskChart.data.datasets[0].data = [];
    dbDiskChart.data.datasets[1].data = [];
    dbDiskChart.update();

    dbNetChart.data.labels = [];
    dbNetChart.data.datasets[0].data = [];
    dbNetChart.data.datasets[1].data = [];
    dbNetChart.update();

    dbConnChart.data.labels = [];
    dbConnChart.data.datasets[0].data = [];
    dbConnChart.update();

    addLog('Starting benchmark...', 'info');
    const result = await apiCall('start');
    if (result.success) {
        showToast('success', 'Benchmark Started', 'Benchmark is now running');
    }
}

function stopBenchmark() {
    showConfirmDialog('Stop Benchmark', 'Are you sure you want to stop the running benchmark?', 'btn-danger', doStopBenchmark);
}

async function doStopBenchmark() {
    addLog('Stopping benchmark...', 'info');
    const result = await apiCall('stop');
    if (result.success) {
        showToast('info', 'Benchmark Stopped', 'Benchmark has been stopped');
    }
}

// ==================== Time Range ====================

function getChartAndKeys(key) {
    switch (key) {
        case 'tpmc':     return { chart: tpmcChart, dataKeys: ['data'] };
        case 'cpu':     return { chart: cpuChart, dataKeys: ['data'] };
        case 'network': return { chart: networkChart, dataKeys: ['recvData', 'sentData'] };
        case 'dbCpu':   return { chart: dbCpuChart, dataKeys: ['data'] };
        case 'dbMem':   return { chart: dbMemChart, dataKeys: ['data'] };
        case 'dbDisk':  return { chart: dbDiskChart, dataKeys: ['readData', 'writeData'] };
        case 'dbNet':   return { chart: dbNetChart, dataKeys: ['recvData', 'sentData'] };
        case 'dbConn':  return { chart: dbConnChart, dataKeys: ['data'] };
        default: return null;
    }
}

function rangeToSeconds(range) {
    if (range === 'all') return Infinity;
    if (range === 'custom') return 'custom';
    const match = range.match(/^(\d+)(m|h|d)$/);
    if (!match) return Infinity;
    const val = parseInt(match[1]);
    switch (match[2]) {
        case 'm': return val * 60;
        case 'h': return val * 3600;
        case 'd': return val * 86400;
        default: return Infinity;
    }
}

function sliceBuffer(buffer, dataKeys, seconds) {
    const total = buffer.labels.length;
    const count = seconds === Infinity ? total : Math.min(seconds, total);
    const start = total - count;
    const labels = buffer.labels.slice(start);
    const datasets = dataKeys.map(k => buffer[k].slice(start));
    return { labels, datasets };
}

function sliceBufferByTimeRange(buffer, dataKeys, startTime, endTime) {
    const labels = [];
    const datasets = dataKeys.map(() => []);
    for (let i = 0; i < buffer.labels.length; i++) {
        const t = buffer.timestamps ? buffer.timestamps[i] : 0;
        if (t >= startTime && t <= endTime) {
            labels.push(buffer.labels[i]);
            dataKeys.forEach((k, di) => datasets[di].push(buffer[k][i]));
        }
    }
    return { labels, datasets };
}

// Max points to render on chart — keeps curves readable
const MAX_DISPLAY_POINTS = 100;

/**
 * Downsample using fixed-interval bucket averaging.
 * Stable for streaming: adding a new point only affects the last bucket,
 * so the chart doesn't visually jump on each update.
 */
function downsample(labels, datasets, maxPoints) {
    const len = labels.length;
    if (len <= maxPoints || maxPoints < 2) {
        return { labels, datasets };
    }

    const bucketSize = len / maxPoints;
    const newLabels = [];
    const newDatasets = datasets.map(() => []);

    for (let i = 0; i < maxPoints; i++) {
        const start = Math.floor(i * bucketSize);
        const end = Math.min(Math.floor((i + 1) * bucketSize), len);
        const count = end - start;

        // Use the last point's label in each bucket (most recent timestamp)
        newLabels.push(labels[end - 1]);

        // Average each dataset within the bucket
        datasets.forEach((ds, di) => {
            let sum = 0;
            for (let j = start; j < end; j++) sum += ds[j];
            newDatasets[di].push(sum / count);
        });
    }

    return { labels: newLabels, datasets: newDatasets };
}

function applyTimeRangeToChart(key) {
    const info = getChartAndKeys(key);
    if (!info) return;
    let result;
    if (chartTimeRange === 'custom' && customRangeStart && customRangeEnd) {
        result = sliceBufferByTimeRange(chartDataBuffer[key], info.dataKeys, customRangeStart, customRangeEnd);
    } else {
        const seconds = rangeToSeconds(chartTimeRange);
        result = sliceBuffer(chartDataBuffer[key], info.dataKeys, seconds);
    }
    // Downsample if too many points
    const ds = downsample(result.labels, result.datasets, MAX_DISPLAY_POINTS);
    info.chart.data.labels = ds.labels;
    ds.datasets.forEach((d, i) => { info.chart.data.datasets[i].data = d; });
    info.chart.update();
}

function applyTimeRange() {
    for (const key of Object.keys(chartDataBuffer)) {
        applyTimeRangeToChart(key);
    }
}

let customRangeStart = 0;
let customRangeEnd = 0;

function setTimeRange(btn) {
    document.querySelectorAll('.time-range-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    chartTimeRange = btn.dataset.range;
    if (chartTimeRange !== 'custom') {
        document.getElementById('customRangePicker').style.display = 'none';
    }
    applyTimeRange();
}

function toggleCustomRange(btn) {
    document.querySelectorAll('.time-range-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    chartTimeRange = 'custom';
    const picker = document.getElementById('customRangePicker');
    picker.style.display = 'flex';
    // Pre-fill with reasonable defaults if empty
    if (!document.getElementById('customRangeStart').value) {
        const now = new Date();
        const ago = new Date(now.getTime() - 600000); // 10 min ago
        document.getElementById('customRangeStart').value = toLocalISOString(ago);
        document.getElementById('customRangeEnd').value = toLocalISOString(now);
    }
}

function applyCustomRange() {
    const startVal = document.getElementById('customRangeStart').value;
    const endVal = document.getElementById('customRangeEnd').value;
    if (!startVal || !endVal) return;
    customRangeStart = new Date(startVal).getTime();
    customRangeEnd = new Date(endVal).getTime();
    if (customRangeStart >= customRangeEnd) {
        showToast('warning', 'Invalid Range', 'Start time must be before end time');
        return;
    }
    applyTimeRange();
}

function toLocalISOString(date) {
    const pad = n => String(n).padStart(2, '0');
    return date.getFullYear() + '-' + pad(date.getMonth() + 1) + '-' + pad(date.getDate())
        + 'T' + pad(date.getHours()) + ':' + pad(date.getMinutes()) + ':' + pad(date.getSeconds());
}

// ==================== SSH Config Toggle ====================

function toggleSshFields() {
    const enabled = document.getElementById('cfgFormSshEnabled').checked;
    document.getElementById('sshFields').style.display = enabled ? 'block' : 'none';
}

async function testSshConnection() {
    const btn = document.getElementById('btnTestSsh');
    const resultEl = document.getElementById('sshTestResult');
    btn.disabled = true;
    resultEl.textContent = 'Testing...';
    resultEl.style.color = '#888';

    const payload = {
        ssh: {
            host: document.getElementById('cfgFormSshHost').value,
            port: parseInt(document.getElementById('cfgFormSshPort').value) || 22,
            username: document.getElementById('cfgFormSshUser').value,
            password: document.getElementById('cfgFormSshPass').value,
            privateKey: document.getElementById('cfgFormSshKey').value
        },
        database: {
            jdbcUrl: document.getElementById('cfgFormJdbcUrl').value
        }
    };

    try {
        const resp = await fetch('/api/benchmark/test-ssh', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await resp.json();
        if (data.success) {
            resultEl.textContent = data.message;
            resultEl.style.color = '#00ff88';
        } else {
            resultEl.textContent = 'Failed: ' + (data.error || 'Unknown error');
            resultEl.style.color = '#ff4757';
        }
    } catch (e) {
        resultEl.textContent = 'Error: ' + e.message;
        resultEl.style.color = '#ff4757';
    } finally {
        btn.disabled = false;
    }
}

// ==================== Modal ====================

function openModal(modalId) {
    document.getElementById(modalId).classList.add('active');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

// ==================== Event Listeners ====================

function setupEventListeners() {
    // Close modal on overlay click (except configModal)
    document.querySelectorAll('.modal-overlay').forEach(overlay => {
        overlay.addEventListener('click', (e) => {
            // Don't close configModal when clicking outside
            if (e.target === overlay && overlay.id !== 'configModal') {
                overlay.classList.remove('active');
            }
        });
    });

    // Close modal on Escape key (except configModal)
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            document.querySelectorAll('.modal-overlay.active').forEach(modal => {
                // Don't close configModal with Escape
                if (modal.id !== 'configModal') {
                    modal.classList.remove('active');
                }
            });
        }
    });

    // Log filter input
    const logFilter = document.getElementById('logFilter');
    if (logFilter) {
        logFilter.addEventListener('input', filterLogs);
    }

    // Database type change - update JDBC URL template
    const dbTypeSelect = document.getElementById('cfgFormDbType');
    if (dbTypeSelect) {
        dbTypeSelect.addEventListener('change', onDbTypeChange);
    }

    // Handle page visibility change - refresh data when page becomes visible
    document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible') {
            loadInitialState();
        }
    });
}

// ==================== JDBC URL Templates ====================

const jdbcUrlTemplates = {
    mysql: 'jdbc:mysql://127.0.0.1:3306/tpcc?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    postgresql: 'jdbc:postgresql://127.0.0.1:5432/tpcc',
    oracle: 'jdbc:oracle:thin:@127.0.0.1:1521:orcl',
    sqlserver: 'jdbc:sqlserver://127.0.0.1:1433;databaseName=tpcc;encrypt=false',
    db2: 'jdbc:db2://127.0.0.1:50000/tpcc',
    dameng: 'jdbc:dm://127.0.0.1:5236/tpcc',
    oceanbase: 'jdbc:mysql://127.0.0.1:2881/tpcc',
    tidb: 'jdbc:mysql://127.0.0.1:4000/tpcc?useSSL=false&rewriteBatchedStatements=true',
    sqlite: 'jdbc:sqlite:./tpcc.db',
    yashandb: 'jdbc:yasdb://127.0.0.1:1688/tpcc',
    sybase: 'jdbc:jtds:sybase://127.0.0.1:5000/tpcc',
    hana: 'jdbc:sap://127.0.0.1:39017/?currentschema=tpcc'
};

function onDbTypeChange() {
    const dbType = document.getElementById('cfgFormDbType').value;
    const jdbcUrlInput = document.getElementById('cfgFormJdbcUrl');

    if (jdbcUrlTemplates[dbType]) {
        jdbcUrlInput.value = jdbcUrlTemplates[dbType];
    }

    // SQLite doesn't support concurrency - force terminals and load concurrency to 1
    applySqliteConstraints(dbType);

    // Clear connection test result when type changes
    document.getElementById('connectionTestResult').innerHTML = '';
}

function populateDatabaseTypes(types) {
    const select = document.getElementById('cfgFormDbType');
    select.innerHTML = '';
    types.forEach(t => {
        const option = document.createElement('option');
        option.value = t.value;
        option.textContent = t.builtin ? t.label : t.label + ' (plugin)';
        select.appendChild(option);
        // Merge plugin URL templates into the global map
        if (!t.builtin && t.urlTemplate) {
            jdbcUrlTemplates[t.value] = t.urlTemplate;
        }
    });
}

function applySqliteConstraints(dbType) {
    const terminalsInput = document.getElementById('cfgFormTerminals');
    const loadConcurrencyInput = document.getElementById('cfgFormLoadConcurrency');
    const isSqlite = dbType === 'sqlite';

    if (isSqlite) {
        terminalsInput.value = 1;
        loadConcurrencyInput.value = 1;
    }
    terminalsInput.disabled = isSqlite;
    loadConcurrencyInput.disabled = isSqlite;
}

// ==================== Utilities ====================

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatBytesShort(bytes) {
    if (bytes === 0) return '0';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + sizes[i];
}

function formatDuration(seconds) {
    if (seconds < 60) return seconds + 's';
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return mins + 'm ' + secs + 's';
}

// ==================== Toast Notifications ====================

function showToast(type, title, message, duration = 5000) {
    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;

    const icons = {
        success: '&#10004;',
        error: '&#10006;',
        warning: '&#9888;',
        info: '&#8505;'
    };

    toast.innerHTML = `
        <span class="toast-icon">${icons[type] || icons.info}</span>
        <div class="toast-content">
            <div class="toast-title">${escapeHtml(title)}</div>
            <div class="toast-message">${escapeHtml(message)}</div>
        </div>
        <button class="toast-close" onclick="this.parentElement.remove()">&times;</button>
    `;

    container.appendChild(toast);

    // Auto remove after duration
    if (duration > 0) {
        setTimeout(() => {
            toast.classList.add('hiding');
            setTimeout(() => toast.remove(), 300);
        }, duration);
    }

    return toast;
}

// ==================== Connection Test ====================

async function testConnection() {
    const btn = document.getElementById('btnTestConnection');
    const result = document.getElementById('connectionTestResult');

    // Get current form values
    const config = {
        database: {
            type: document.getElementById('cfgFormDbType').value,
            jdbcUrl: document.getElementById('cfgFormJdbcUrl').value,
            username: document.getElementById('cfgFormDbUser').value,
            password: document.getElementById('cfgFormDbPass').value
        }
    };

    // Show testing state
    btn.disabled = true;
    result.className = 'connection-test-result testing';
    result.innerHTML = '<span class="spinner"></span> Testing...';

    try {
        const res = await fetch('/api/benchmark/test-connection', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        const data = await res.json();

        if (data.success) {
            result.className = 'connection-test-result success';
            result.innerHTML = `&#10004; ${data.message}`;
            showToast('success', 'Connection Successful', `Connected to ${data.database}`);
        } else {
            result.className = 'connection-test-result error';
            result.innerHTML = `&#10006; ${data.error}`;

            let suggestion = data.suggestion || 'Check your connection settings';
            showToast('error', 'Connection Failed', `${data.error}\n\nSuggestion: ${suggestion}`, 8000);
        }
    } catch (e) {
        result.className = 'connection-test-result error';
        result.innerHTML = `&#10006; ${e.message}`;
        showToast('error', 'Connection Test Failed', e.message);
    } finally {
        btn.disabled = false;
    }
}

// ==================== Hardware Info ====================

async function loadHardwareInfo() {
    try {
        const res = await fetch('/api/metrics/hardware-info');
        const data = await res.json();

        if (data.client && Object.keys(data.client).length > 0) {
            displayHardwareInfo('client', data.client);
        }
        if (data.dbServer && Object.keys(data.dbServer).length > 0) {
            displayHardwareInfo('db', data.dbServer);
        }
    } catch (e) {
        console.error('Failed to load hardware info:', e);
    }
}

function displayHardwareInfo(prefix, hw) {
    const setVal = (id, val) => {
        const el = document.getElementById(id);
        if (el) el.textContent = val || '-';
    };

    setVal(prefix + 'HwOs', hw.os);
    setVal(prefix + 'HwCpu', hw.cpu);

    let cores = '';
    if (hw.cpuPhysicalCores) cores += hw.cpuPhysicalCores + 'P';
    if (hw.cpuLogicalCores) cores += (cores ? '/' : '') + hw.cpuLogicalCores + 'L';
    if (hw.cpuFreqMHz && hw.cpuFreqMHz > 0) cores += ' @ ' + hw.cpuFreqMHz + ' MHz';
    setVal(prefix + 'HwCores', cores || '-');

    setVal(prefix + 'HwMemory', hw.memoryTotalGB ? hw.memoryTotalGB + ' GB' : '-');
    setVal(prefix + 'HwDisk', hw.disks);
    setVal(prefix + 'HwNetwork', hw.network);
}

function toggleHwInfo(sectionId) {
    const section = document.getElementById(sectionId);
    if (!section) return;
    const grid = section.querySelector('.hw-info-grid');
    const isExpanded = section.classList.toggle('expanded');
    if (grid) grid.style.display = isExpanded ? 'grid' : 'none';
}

// ==================== Status Change Detection ====================

function handleStatusChange(oldStatus, newStatus) {
    // Handle specific status transitions
    if (oldStatus === 'LOADING' && newStatus === 'LOADED') {
        showToast('success', 'Data Loaded', 'TPC-C data has been loaded successfully');
        document.getElementById('loadProgressContainer').classList.remove('active');
    } else if (oldStatus === 'LOADING' && newStatus === 'CANCELLED') {
        showToast('warning', 'Load Cancelled', 'Data loading was cancelled by user');
        document.getElementById('loadProgressContainer').classList.remove('active');
    } else if (oldStatus === 'LOADING' && newStatus === 'ERROR') {
        showToast('error', 'Load Failed', 'Data loading failed, check logs for details');
    } else if (oldStatus === 'RUNNING' && newStatus === 'STOPPED') {
        showToast('info', 'Benchmark Complete', 'Benchmark has finished running');
    } else if (newStatus === 'ERROR') {
        showToast('error', 'Error', 'An error occurred, check logs for details');
    }

    // Re-fetch hardware info when initialized (SSH may have just connected)
    if (newStatus === 'INITIALIZED') {
        loadHardwareInfo();
    }
}

// ==================== Profile Management ====================

async function loadProfiles() {
    try {
        const res = await fetch('/api/benchmark/profiles');
        const profiles = await res.json();
        const select = document.getElementById('profileSelect');
        const currentVal = select.value || 'default';
        select.innerHTML = '<option value="default">default</option>';
        profiles.forEach(name => {
            if (name === 'default') return;
            const opt = document.createElement('option');
            opt.value = name;
            opt.textContent = name;
            select.appendChild(opt);
        });
        if (profiles.includes(currentVal)) {
            select.value = currentVal;
        } else {
            select.value = 'default';
        }
    } catch (e) {
        console.error('Failed to load profiles:', e);
    }
}

async function onProfileChange() {
    const select = document.getElementById('profileSelect');
    const name = select.value;
    if (!name || name === 'default') return;
    try {
        const res = await fetch(`/api/benchmark/profiles/${encodeURIComponent(name)}`);
        const data = await res.json();
        if (data.success && data.config) {
            fillConfigForm(data.config);
        }
    } catch (e) {
        console.error('Failed to load profile:', e);
    }
}

function fillConfigForm(cfg) {
    if (cfg.database) {
        document.getElementById('cfgFormDbType').value = cfg.database.type || 'mysql';
        document.getElementById('cfgFormJdbcUrl').value = cfg.database.jdbcUrl || '';
        document.getElementById('cfgFormDbUser').value = cfg.database.username || '';
        const dbPass = cfg.database.password || '';
        document.getElementById('cfgFormDbPass').value = dbPass;
        if (dbPass) savedDbPassword = dbPass;
    }
    if (cfg.benchmark) {
        document.getElementById('cfgFormWarehouses').value = cfg.benchmark.warehouses || 10;
        document.getElementById('cfgFormTerminals').value = cfg.benchmark.terminals || 50;
        document.getElementById('cfgFormDuration').value = cfg.benchmark.duration || 60;
        document.getElementById('cfgFormLoadConcurrency').value = cfg.benchmark.loadConcurrency || 4;
        document.getElementById('cfgFormThinkTime').checked = cfg.benchmark.thinkTime || false;
        document.getElementById('cfgFormLoadMode').value = cfg.benchmark.loadMode || 'auto';
    }
    if (cfg.transactionMix) {
        document.getElementById('cfgFormMixNewOrder').value = cfg.transactionMix.newOrder || 45;
        document.getElementById('cfgFormMixPayment').value = cfg.transactionMix.payment || 43;
        document.getElementById('cfgFormMixOrderStatus').value = cfg.transactionMix.orderStatus || 4;
        document.getElementById('cfgFormMixDelivery').value = cfg.transactionMix.delivery || 4;
        document.getElementById('cfgFormMixStockLevel').value = cfg.transactionMix.stockLevel || 4;
    }
    if (cfg.ssh) {
        document.getElementById('cfgFormSshEnabled').checked = cfg.ssh.enabled || false;
        document.getElementById('cfgFormSshHost').value = cfg.ssh.host || '';
        document.getElementById('cfgFormSshPort').value = cfg.ssh.port || 22;
        document.getElementById('cfgFormSshUser').value = cfg.ssh.username || 'root';
        const sshPass = cfg.ssh.password || '';
        document.getElementById('cfgFormSshPass').value = sshPass;
        if (sshPass) savedSshPassword = sshPass;
        document.getElementById('cfgFormSshKey').value = cfg.ssh.privateKey || '';
        toggleSshFields();
    }
    applySqliteConstraints(document.getElementById('cfgFormDbType').value);
}

async function saveProfile() {
    const name = prompt('Enter profile name (e.g. mysql-local, pg-prod):');
    if (!name || !name.trim()) return;

    const config = buildConfigFromForm();
    try {
        const res = await fetch(`/api/benchmark/profiles/${encodeURIComponent(name.trim())}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        const data = await res.json();
        if (data.success) {
            showToast('success', 'Profile Saved', data.message);
            addLog('Profile saved: ' + name.trim(), 'success');
            currentProfileName = name.trim();
            await loadProfiles();
            document.getElementById('profileSelect').value = name.trim();
        } else {
            showToast('error', 'Save Failed', data.error);
        }
    } catch (e) {
        showToast('error', 'Save Failed', e.message);
    }
}

async function deleteProfile() {
    const select = document.getElementById('profileSelect');
    const name = select.value;
    if (!name) {
        showToast('warning', 'No Profile', 'Please select a profile first');
        return;
    }
    if (!confirm(`Delete profile "${name}"?`)) return;

    try {
        const res = await fetch(`/api/benchmark/profiles/${encodeURIComponent(name)}`, {
            method: 'DELETE'
        });
        const data = await res.json();
        if (data.success) {
            showToast('success', 'Profile Deleted', data.message);
            addLog('Profile deleted: ' + name, 'info');
            if (currentProfileName === name) {
                currentProfileName = 'default';
                document.getElementById('cfgProfileName').textContent = 'default';
            }
            await loadProfiles();
        } else {
            showToast('error', 'Delete Failed', data.error);
        }
    } catch (e) {
        showToast('error', 'Delete Failed', e.message);
    }
}

function buildConfigFromForm() {
    const dbType = document.getElementById('cfgFormDbType').value;
    const isSqlite = dbType === 'sqlite';
    return {
        database: {
            type: dbType,
            jdbcUrl: document.getElementById('cfgFormJdbcUrl').value,
            username: document.getElementById('cfgFormDbUser').value,
            password: document.getElementById('cfgFormDbPass').value || savedDbPassword
        },
        ssh: {
            enabled: document.getElementById('cfgFormSshEnabled').checked,
            host: document.getElementById('cfgFormSshHost').value,
            port: parseInt(document.getElementById('cfgFormSshPort').value) || 22,
            username: document.getElementById('cfgFormSshUser').value,
            password: document.getElementById('cfgFormSshPass').value || savedSshPassword,
            privateKey: document.getElementById('cfgFormSshKey').value
        },
        benchmark: {
            warehouses: parseInt(document.getElementById('cfgFormWarehouses').value),
            terminals: isSqlite ? 1 : parseInt(document.getElementById('cfgFormTerminals').value),
            duration: parseInt(document.getElementById('cfgFormDuration').value),
            loadConcurrency: isSqlite ? 1 : parseInt(document.getElementById('cfgFormLoadConcurrency').value),
            thinkTime: document.getElementById('cfgFormThinkTime').checked,
            loadMode: document.getElementById('cfgFormLoadMode').value
        },
        transactionMix: {
            newOrder: parseInt(document.getElementById('cfgFormMixNewOrder').value),
            payment: parseInt(document.getElementById('cfgFormMixPayment').value),
            orderStatus: parseInt(document.getElementById('cfgFormMixOrderStatus').value),
            delivery: parseInt(document.getElementById('cfgFormMixDelivery').value),
            stockLevel: parseInt(document.getElementById('cfgFormMixStockLevel').value)
        }
    };
}

// ==================== Report ====================

let cachedReportMarkdown = '';

async function openReportModal() {
    openModal('reportModal');
    const contentEl = document.getElementById('reportContent');
    contentEl.innerHTML = '<div class="text-muted text-center" style="padding: 40px;">Loading report...</div>';

    try {
        const res = await fetch('/api/report/markdown');
        if (!res.ok) throw new Error('Failed to fetch report');
        cachedReportMarkdown = await res.text();
        contentEl.innerHTML = marked.parse(cachedReportMarkdown);
    } catch (e) {
        contentEl.innerHTML = '<div class="text-center" style="color: #ff4757; padding: 40px;">Failed to load report: ' + escapeHtml(e.message) + '</div>';
    }
}

function downloadReportMd() {
    window.open('/api/report/download/markdown', '_blank');
}

function printReport() {
    const content = document.getElementById('reportContent').innerHTML;
    const win = window.open('', '_blank');
    win.document.write('<!DOCTYPE html><html><head><meta charset="UTF-8">');
    win.document.write('<title>DBBench Report</title>');
    win.document.write('<style>');
    win.document.write('body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;');
    win.document.write('max-width:900px;margin:0 auto;padding:40px;color:#222;background:#fff;}');
    win.document.write('h1{color:#1a1a2e;border-bottom:2px solid #00b8d9;padding-bottom:10px;}');
    win.document.write('h2{color:#16213e;margin-top:30px;border-bottom:1px solid #ddd;padding-bottom:8px;}');
    win.document.write('h3{color:#333;margin-top:20px;}');
    win.document.write('table{width:100%;border-collapse:collapse;margin:15px 0;}');
    win.document.write('th,td{border:1px solid #ddd;padding:8px 12px;text-align:left;}');
    win.document.write('th{background:#f5f5f5;font-weight:600;}');
    win.document.write('tr:nth-child(even){background:#fafafa;}');
    win.document.write('strong{color:#1a1a2e;}');
    win.document.write('@media print{body{padding:20px;}}');
    win.document.write('</style></head><body>');
    win.document.write(content);
    win.document.write('</body></html>');
    win.document.close();
    setTimeout(() => win.print(), 300);
}
