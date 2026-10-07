const API_BASE = window.location.origin;

let currentCorrelationId = null;

// Escape untrusted values before interpolating into innerHTML.
// Event payloads and IDs originate from API callers, so they are
// attacker-controlled — rendering them raw is a stored-XSS vector.
function escapeHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

async function fetchStats() {
    try {
        const res = await fetch('/stats');
        const data = await res.json();
        if (data.success) {
            document.getElementById('stats-section').innerHTML = `
                <div class="stat-card">
                    <div class="stat-value">${data.stats.totalEvents}</div>
                    <div>Total Events</div>
                </div>
            `;
        }
    } catch (e) {
        console.error('Failed to fetch stats', e);
    }
}

async function fetchFlows() {
    try {
        const res = await fetch('/flows');
        const data = await res.json();
        const flowsList = document.getElementById('flows-list');
        
        if (data.success && data.flows.length > 0) {
            flowsList.innerHTML = data.flows.map(flow => `
                <div class="flow-item ${flow.has_failure ? 'failed' : ''}" onclick="showTimeline('${escapeHtml(flow.correlation_id).replace(/'/g, '')}')">
                    <div style="font-family: monospace; color: #718096; font-size: 0.9em;">${escapeHtml(flow.correlation_id)}</div>
                    <div style="margin-top: 5px;">
                        <strong>${escapeHtml(flow.event_count)} events</strong> across ${flow.services.map(s => `<span class="badge">${escapeHtml(s)}</span>`).join(' ')}
                    </div>
                    <div style="margin-top: 5px; font-size: 0.85em; color: ${flow.has_failure ? '#e53e3e' : '#48bb78'};">
                        ${flow.has_failure ? 'Failed' : 'Success'} - Duration: ${escapeHtml(flow.duration_ms)}ms
                    </div>
                </div>
            `).join('');
        } else {
            flowsList.innerHTML = '<p>No request flows found.</p>';
        }
    } catch (e) {
        document.getElementById('flows-list').innerHTML = '<p style="color:red;">Error loading flows. Make sure Debug API is running.</p>';
        console.error('Failed to fetch flows', e);
    }
}

async function showTimeline(correlationId) {
    currentCorrelationId = correlationId;
    document.getElementById('dashboard-controls').style.display = 'none';
    document.getElementById('flows-section').style.display = 'none';
    document.getElementById('timeline-section').style.display = 'block';
    document.getElementById('timeline-title').textContent = `Timeline: ${correlationId}`; // textContent — safe, no escaping needed
    
    document.getElementById('timeline-container').innerHTML = 'Loading timeline...';
    document.getElementById('replay-results').innerHTML = '';

    try {
        const res = await fetch(`/timeline/${correlationId}`);
        const data = await res.json();
        
        if (data.success) {
            const events = data.timeline;
            document.getElementById('timeline-container').innerHTML = events.map(event => `
                <div class="timeline-event ${event.is_failure ? 'failed' : ''}">
                    <div class="timeline-content">
                        <div style="display: flex; justify-content: space-between; margin-bottom: 10px;">
                            <strong>${escapeHtml(event.event_type)}</strong>
                            <span class="badge">${escapeHtml(event.service)}</span>
                        </div>
                        <p style="font-size: 0.9em; margin-bottom: 5px;">${escapeHtml(event.description)}</p>
                        <p style="font-size: 0.8em; color: #718096;">
                            Step ${escapeHtml(event.step_number)} - Time: ${new Date(parseInt(event.timestamp) || event.timestamp).toLocaleTimeString()}
                        </p>
                        <details style="margin-top: 10px;">
                            <summary style="cursor: pointer; font-size: 0.85em; color: #4a5568;">Show Payload</summary>
                            <pre style="background: #edf2f7; padding: 10px; border-radius: 4px; overflow-x: auto; font-size: 0.8em; margin-top: 5px;">${escapeHtml(JSON.stringify(event.payload, null, 2))}</pre>
                        </details>
                    </div>
                </div>
            `).join('');
        }
    } catch (e) {
        document.getElementById('timeline-container').innerHTML = '<p style="color:red;">Error loading timeline</p>';
    }
}

document.getElementById('back-btn').addEventListener('click', () => {
    currentCorrelationId = null;
    document.getElementById('dashboard-controls').style.display = 'flex';
    document.getElementById('flows-section').style.display = 'block';
    document.getElementById('timeline-section').style.display = 'none';
    fetchFlows(); // Refresh flows just in case
});

async function triggerReplay(mode) {
    if (!currentCorrelationId) return;
    
    const resultsDiv = document.getElementById('replay-results');
    resultsDiv.innerHTML = '<p>Replaying events...</p>';
    
    try {
        const res = await fetch('/replay', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ correlationId: currentCorrelationId, mode })
        });
        const data = await res.json();
        
        if (data.success) {
            let details = data.timeline?.map(item => `
                <div style="padding: 5px 0; border-bottom: 1px solid #e2e8f0; font-size: 0.9em;">
                    <strong>${escapeHtml(item.event_type)}</strong> (${escapeHtml(item.service)}) -
                    <span style="color: ${item.status === 'PROCESSED' ? 'green' : (item.status === 'FAILED' ? 'red' : 'orange')}">
                        ${escapeHtml(item.status)}
                    </span>
                </div>
            `).join('') || '';

            resultsDiv.innerHTML = `
                <div class="card" style="background-color: #f0fff4; border: 1px solid #9ae6b4;">
                    <h4 style="color: #276749;">Replay Completed (${data.session.processedCount}/${data.session.totalEvents} processed)</h4>
                    <p style="font-size: 0.9em; margin: 10px 0;">Duration: ${data.session.duration_ms}ms</p>
                    ${details}
                </div>
            `;
        } else {
            resultsDiv.innerHTML = `<p style="color: red;">Replay failed: ${escapeHtml(data.error)}</p>`;
        }
    } catch (e) {
        resultsDiv.innerHTML = `<p style="color: red;">Error triggering replay: ${escapeHtml(e.message)}</p>`;
    }
}

document.getElementById('dry-run-btn').addEventListener('click', () => triggerReplay('dry-run'));
document.getElementById('state-rebuild-btn').addEventListener('click', () => triggerReplay('state-rebuild'));

// Initialize
fetchStats();
fetchFlows();

// Add 'Create Live Order' button logic
async function createOrder(isIntentionalFailure, btnNode) {
    const originalText = btnNode.textContent;
    btnNode.disabled = true;
    btnNode.textContent = 'Creating...';
    
    try {
        const prefix = isIntentionalFailure ? 'fail-' : 'user-';
        const payload = {
            customerId: prefix + Math.floor(Math.random() * 1000),
            items: [{ id: 'product-A', name: 'Test Product', quantity: 1, price: 5000 }],
            paymentMethod: 'credit_card'
        };
        
        await fetch('http://localhost:3001/orders', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        
        // Wait for the distributed flow to complete (Bank delay + propagation)
        setTimeout(() => {
            fetchStats();
            fetchFlows();
            btnNode.disabled = false;
            btnNode.textContent = originalText;
        }, 2500);
        
    } catch (e) {
        console.error('Error creating order:', e);
        btnNode.disabled = false;
        btnNode.textContent = originalText;
        alert('Failed to create order! Check target order service.');
    }
}

document.getElementById('create-order-btn').addEventListener('click', function() {
    createOrder(false, this);
});

document.getElementById('create-fail-order-btn').addEventListener('click', function() {
    createOrder(true, this);
});
