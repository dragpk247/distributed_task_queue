//! # HTTP Server & Dashboard Module
//!
//! Provides an embedded web dashboard and Prometheus `/metrics` scraping endpoint:
//! - `/metrics`: Scrapes metrics from `QueueEngine::get_stats()` in Prometheus text format.
//! - `/` or `/dashboard`: Modern, responsive dark-mode HTML/CSS/JS dashboard.
//! - `/api/stats`: JSON endpoint returning queue metrics for real-time polling.
//! - `/api/dlq/requeue?queue=<name>`: Re-queues DLQ tasks back to the ready queue.
//! - `/api/dlq/purge?queue=<name>`: Purges DLQ tasks for a queue.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::engine::QueueEngine;

/// Spawns the HTTP server background task on the given listener address.
pub async fn start_http_server(bind_addr: String, engine: Arc<QueueEngine>) {
    let listener = match TcpListener::bind(&bind_addr).await {
        Ok(l) => {
            tracing::info!(
                "HTTP metrics and dashboard server listening on http://{}",
                bind_addr
            );
            l
        }
        Err(e) => {
            tracing::error!("Failed to bind HTTP server to {}: {:?}", bind_addr, e);
            return;
        }
    };

    loop {
        match listener.accept().await {
            Ok((socket, _addr)) => {
                let engine = Arc::clone(&engine);
                tokio::spawn(async move {
                    if let Err(e) = handle_http_connection(socket, engine).await {
                        tracing::debug!("HTTP connection error: {:?}", e);
                    }
                });
            }
            Err(e) => {
                tracing::warn!("HTTP listener accept error: {:?}", e);
            }
        }
    }
}

/// Handles a single HTTP request connection.
pub async fn handle_http_connection(
    mut socket: TcpStream,
    engine: Arc<QueueEngine>,
) -> tokio::io::Result<()> {
    let mut buffer = [0u8; 4096];
    let n = socket.read(&mut buffer).await?;
    if n == 0 {
        return Ok(());
    }

    let request_str = String::from_utf8_lossy(&buffer[..n]);
    let first_line = request_str.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    if parts.len() < 2 {
        let resp = build_http_response(400, "text/plain", "Bad Request");
        socket.write_all(resp.as_bytes()).await?;
        return Ok(());
    }

    let method = parts[0];
    let full_uri = parts[1];

    // Separate URI path and query string
    let (path, query) = match full_uri.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (full_uri, None),
    };

    let response = match (method, path) {
        ("GET", "/metrics") => {
            let body = generate_prometheus_metrics(&engine);
            build_http_response(200, "text/plain; version=0.0.4", &body)
        }
        ("GET", "/") | ("GET", "/dashboard") => {
            let body = get_dashboard_html();
            build_http_response(200, "text/html; charset=utf-8", &body)
        }
        ("GET", "/api/stats") => {
            let body = generate_json_stats(&engine);
            build_http_response(200, "application/json", &body)
        }
        ("POST", "/api/dlq/requeue") | ("GET", "/api/dlq/requeue") => {
            let params = parse_query_params(query.unwrap_or(""));
            if let Some(queue) = params.get("queue") {
                let count = engine.requeue_dlq(queue);
                let body = format!(
                    r#"{{"status":"ok","action":"requeue","queue":"{}","requeued":{}}}"#,
                    escape_json(queue),
                    count
                );
                build_http_response(200, "application/json", &body)
            } else {
                let body = r#"{"status":"error","message":"Missing 'queue' query parameter"}"#;
                build_http_response(400, "application/json", body)
            }
        }
        ("POST", "/api/dlq/purge") | ("GET", "/api/dlq/purge") => {
            let params = parse_query_params(query.unwrap_or(""));
            if let Some(queue) = params.get("queue") {
                let count = engine.purge_dlq(queue);
                let body = format!(
                    r#"{{"status":"ok","action":"purge","queue":"{}","purged":{}}}"#,
                    escape_json(queue),
                    count
                );
                build_http_response(200, "application/json", &body)
            } else {
                let body = r#"{"status":"error","message":"Missing 'queue' query parameter"}"#;
                build_http_response(400, "application/json", body)
            }
        }
        _ => build_http_response(404, "text/plain", "Not Found"),
    };

    socket.write_all(response.as_bytes()).await?;
    socket.flush().await?;
    Ok(())
}

/// Constructs a standard HTTP/1.1 response string.
pub fn build_http_response(status_code: u16, content_type: &str, body: &str) -> String {
    let status_text = match status_code {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Status",
    };
    format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{}",
        status_code,
        status_text,
        content_type,
        body.len(),
        body
    )
}

/// Generates Prometheus text exposition format metrics from current QueueEngine statistics.
pub fn generate_prometheus_metrics(engine: &QueueEngine) -> String {
    let stats = engine.get_stats();
    let mut out = String::new();

    // 1. Scheduled delayed tasks total
    out.push_str("# HELP dtq_delayed_tasks_total Number of scheduled delayed tasks\n");
    out.push_str("# TYPE dtq_delayed_tasks_total gauge\n");
    out.push_str(&format!(
        "dtq_delayed_tasks_total {}\n\n",
        stats.delayed_tasks_count
    ));

    // Gather sorted unique queue names across all metrics
    let mut all_queues: Vec<String> = stats
        .queue_lengths
        .keys()
        .chain(stats.in_flight_counts.keys())
        .chain(stats.dlq_counts.keys())
        .cloned()
        .collect();
    all_queues.sort();
    all_queues.dedup();

    // 2. Queue ready size
    out.push_str("# HELP dtq_queue_size Current ready task count in queue\n");
    out.push_str("# TYPE dtq_queue_size gauge\n");
    for q in &all_queues {
        let count = stats.queue_lengths.get(q).copied().unwrap_or(0);
        out.push_str(&format!(
            "dtq_queue_size{{queue=\"{}\"}} {}\n",
            escape_label(q),
            count
        ));
    }
    out.push('\n');

    // 3. In-flight leased tasks
    out.push_str("# HELP dtq_in_flight_tasks Current leased tasks in queue\n");
    out.push_str("# TYPE dtq_in_flight_tasks gauge\n");
    for q in &all_queues {
        let count = stats.in_flight_counts.get(q).copied().unwrap_or(0);
        out.push_str(&format!(
            "dtq_in_flight_tasks{{queue=\"{}\"}} {}\n",
            escape_label(q),
            count
        ));
    }
    out.push('\n');

    // 4. Dead letter queue size
    out.push_str("# HELP dtq_dead_letter_queue_size Tasks escalated to dead letter queue\n");
    out.push_str("# TYPE dtq_dead_letter_queue_size gauge\n");
    for q in &all_queues {
        let count = stats.dlq_counts.get(q).copied().unwrap_or(0);
        out.push_str(&format!(
            "dtq_dead_letter_queue_size{{queue=\"{}\"}} {}\n",
            escape_label(q),
            count
        ));
    }

    out
}

/// Generates JSON statistics for the dashboard UI polling endpoint `/api/stats`.
pub fn generate_json_stats(engine: &QueueEngine) -> String {
    let stats = engine.get_stats();

    let mut all_queues: Vec<String> = stats
        .queue_lengths
        .keys()
        .chain(stats.in_flight_counts.keys())
        .chain(stats.dlq_counts.keys())
        .cloned()
        .collect();
    all_queues.sort();
    all_queues.dedup();

    let mut total_ready: usize = 0;
    let mut total_in_flight: usize = 0;
    let mut total_dlq: usize = 0;

    let mut queues_json = Vec::new();
    for q in &all_queues {
        let ready = stats.queue_lengths.get(q).copied().unwrap_or(0);
        let in_flight = stats.in_flight_counts.get(q).copied().unwrap_or(0);
        let dlq = stats.dlq_counts.get(q).copied().unwrap_or(0);

        total_ready += ready;
        total_in_flight += in_flight;
        total_dlq += dlq;

        queues_json.push(format!(
            r#"{{"name":"{}","ready":{},"in_flight":{},"dlq":{}}}"#,
            escape_json(q),
            ready,
            in_flight,
            dlq
        ));
    }

    format!(
        r#"{{"total_queues":{},"total_ready":{},"total_in_flight":{},"total_delayed":{},"total_dlq":{},"queues":[{}]}}"#,
        all_queues.len(),
        total_ready,
        total_in_flight,
        stats.delayed_tasks_count,
        total_dlq,
        queues_json.join(",")
    )
}

fn parse_query_params(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            map.insert(k.to_string(), url_decode(v));
        } else if !pair.is_empty() {
            map.insert(pair.to_string(), String::new());
        }
    }
    map
}

fn url_decode(s: &str) -> String {
    let mut res = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            let h1 = chars.next().unwrap_or(' ');
            let h2 = chars.next().unwrap_or(' ');
            let hex_str = format!("{}{}", h1, h2);
            if let Ok(byte) = u8::from_str_radix(&hex_str, 16) {
                res.push(byte as char);
            } else {
                res.push('%');
                res.push(h1);
                res.push(h2);
            }
        } else if c == '+' {
            res.push(' ');
        } else {
            res.push(c);
        }
    }
    res
}

fn escape_label(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

/// Returns the embedded modern dark-mode responsive single-page web dashboard.
pub fn get_dashboard_html() -> String {
    r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Distributed Task Queue Dashboard</title>
  <style>
    :root {
      --bg: #0f172a;
      --card-bg: #1e293b;
      --card-border: #334155;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --primary: #38bdf8;
      --primary-hover: #0284c7;
      --accent: #818cf8;
      --success: #34d399;
      --warning: #fbbf24;
      --danger: #f87171;
      --danger-hover: #dc2626;
    }
    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
    }
    body {
      background-color: var(--bg);
      color: var(--text);
      padding: 2rem;
      min-height: 100vh;
    }
    .container {
      max-width: 1200px;
      margin: 0 auto;
    }
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 2rem;
      border-bottom: 1px solid var(--card-border);
      padding-bottom: 1.25rem;
    }
    .logo-group {
      display: flex;
      align-items: center;
      gap: 0.75rem;
    }
    .logo-badge {
      background: linear-gradient(135deg, var(--primary), var(--accent));
      color: #0f172a;
      font-weight: 800;
      padding: 0.4rem 0.75rem;
      border-radius: 8px;
      font-size: 1.1rem;
      letter-spacing: 0.5px;
    }
    h1 {
      font-size: 1.5rem;
      font-weight: 700;
      letter-spacing: -0.5px;
    }
    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      padding: 0.4rem 0.85rem;
      border-radius: 9999px;
      background-color: rgba(52, 211, 153, 0.15);
      color: var(--success);
      font-size: 0.875rem;
      font-weight: 500;
    }
    .pulse-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background-color: var(--success);
      box-shadow: 0 0 8px var(--success);
      animation: pulse 2s infinite;
    }
    @keyframes pulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(0.85); }
    }
    .cards-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 1.25rem;
      margin-bottom: 2rem;
    }
    .card {
      background-color: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 1.25rem;
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
      box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
    }
    .card-title {
      font-size: 0.875rem;
      color: var(--text-muted);
      font-weight: 500;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    .card-value {
      font-size: 2.25rem;
      font-weight: 700;
      letter-spacing: -1px;
    }
    .card-queues .card-value { color: var(--primary); }
    .card-inflight .card-value { color: var(--warning); }
    .card-delayed .card-value { color: var(--accent); }
    .card-dlq .card-value { color: var(--danger); }
    .card-ready .card-value { color: var(--success); }

    .table-container {
      background-color: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      overflow: hidden;
      box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
    }
    .table-header {
      padding: 1.25rem 1.5rem;
      border-bottom: 1px solid var(--card-border);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .table-title {
      font-size: 1.15rem;
      font-weight: 600;
    }
    .links-group {
      display: flex;
      gap: 0.75rem;
    }
    .btn-link {
      color: var(--primary);
      text-decoration: none;
      font-size: 0.875rem;
      padding: 0.35rem 0.75rem;
      border: 1px solid var(--card-border);
      border-radius: 6px;
      transition: all 0.15s ease;
    }
    .btn-link:hover {
      background-color: rgba(56, 189, 248, 0.1);
      border-color: var(--primary);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
    }
    th, td {
      padding: 1rem 1.5rem;
    }
    th {
      background-color: rgba(15, 23, 42, 0.6);
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.75px;
      color: var(--text-muted);
      font-weight: 600;
    }
    tr {
      border-bottom: 1px solid rgba(51, 65, 85, 0.5);
      transition: background-color 0.15s ease;
    }
    tr:last-child {
      border-bottom: none;
    }
    tbody tr:hover {
      background-color: rgba(51, 65, 85, 0.3);
    }
    .queue-name {
      font-weight: 600;
      color: #e2e8f0;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    }
    .badge-count {
      display: inline-block;
      min-width: 2rem;
      text-align: center;
      padding: 0.2rem 0.5rem;
      border-radius: 6px;
      font-weight: 600;
      font-size: 0.875rem;
    }
    .badge-ready { background-color: rgba(52, 211, 153, 0.15); color: var(--success); }
    .badge-inflight { background-color: rgba(251, 191, 36, 0.15); color: var(--warning); }
    .badge-dlq-zero { background-color: rgba(148, 163, 184, 0.1); color: var(--text-muted); }
    .badge-dlq-active { background-color: rgba(248, 113, 113, 0.2); color: var(--danger); font-weight: 700; }

    .actions-cell {
      display: flex;
      gap: 0.5rem;
    }
    .btn-action {
      background-color: transparent;
      border: 1px solid var(--card-border);
      border-radius: 6px;
      padding: 0.3rem 0.6rem;
      font-size: 0.8rem;
      cursor: pointer;
      color: var(--text-muted);
      transition: all 0.15s ease;
    }
    .btn-action:hover:not(:disabled) {
      color: var(--text);
      border-color: var(--text-muted);
    }
    .btn-requeue:hover:not(:disabled) {
      background-color: rgba(56, 189, 248, 0.15);
      border-color: var(--primary);
      color: var(--primary);
    }
    .btn-purge:hover:not(:disabled) {
      background-color: rgba(248, 113, 113, 0.15);
      border-color: var(--danger);
      color: var(--danger);
    }
    .btn-action:disabled {
      opacity: 0.35;
      cursor: not-allowed;
    }
    .empty-state {
      padding: 3rem 1.5rem;
      text-align: center;
      color: var(--text-muted);
      font-size: 0.95rem;
    }
    .last-updated {
      margin-top: 1.5rem;
      text-align: center;
      font-size: 0.8rem;
      color: var(--text-muted);
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="logo-group">
        <span class="logo-badge">DTQ</span>
        <h1>Queue Administration</h1>
      </div>
      <div class="status-badge">
        <span class="pulse-dot"></span>
        <span id="conn-status">Live Polling</span>
      </div>
    </header>

    <div class="cards-grid">
      <div class="card card-queues">
        <div class="card-title">Total Queues</div>
        <div class="card-value" id="val-queues">0</div>
      </div>
      <div class="card card-ready">
        <div class="card-title">Ready Tasks</div>
        <div class="card-value" id="val-ready">0</div>
      </div>
      <div class="card card-inflight">
        <div class="card-title">In-Flight Tasks</div>
        <div class="card-value" id="val-inflight">0</div>
      </div>
      <div class="card card-delayed">
        <div class="card-title">Scheduled Delayed</div>
        <div class="card-value" id="val-delayed">0</div>
      </div>
      <div class="card card-dlq">
        <div class="card-title">Dead-Letter Tasks</div>
        <div class="card-value" id="val-dlq">0</div>
      </div>
    </div>

    <div class="table-container">
      <div class="table-header">
        <div class="table-title">Active Queues</div>
        <div class="links-group">
          <a class="btn-link" href="/metrics" target="_blank">Prometheus /metrics</a>
          <a class="btn-link" href="/api/stats" target="_blank">JSON /api/stats</a>
        </div>
      </div>
      <table>
        <thead>
          <tr>
            <th>Queue Name</th>
            <th>Ready</th>
            <th>In-Flight</th>
            <th>DLQ</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody id="queues-table-body">
          <tr>
            <td colspan="5" class="empty-state">Loading queue metrics...</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="last-updated" id="last-updated">Updating...</div>
  </div>

  <script>
    async function fetchStats() {
      try {
        const resp = await fetch('/api/stats');
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        const data = await resp.json();
        renderStats(data);
        document.getElementById('conn-status').textContent = 'Live Polling';
        document.getElementById('conn-status').parentElement.style.color = 'var(--success)';
      } catch (err) {
        document.getElementById('conn-status').textContent = 'Disconnected';
        document.getElementById('conn-status').parentElement.style.color = 'var(--danger)';
      }
    }

    function renderStats(data) {
      document.getElementById('val-queues').textContent = data.total_queues || 0;
      document.getElementById('val-ready').textContent = data.total_ready || 0;
      document.getElementById('val-inflight').textContent = data.total_in_flight || 0;
      document.getElementById('val-delayed').textContent = data.total_delayed || 0;
      document.getElementById('val-dlq').textContent = data.total_dlq || 0;

      const tbody = document.getElementById('queues-table-body');
      if (!data.queues || data.queues.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="empty-state">No active queues found. Enqueue tasks via LPUSH.</td></tr>';
      } else {
        tbody.innerHTML = data.queues.map(q => {
          const dlqBadgeClass = q.dlq > 0 ? 'badge-dlq-active' : 'badge-dlq-zero';
          const disabledAttr = q.dlq === 0 ? 'disabled' : '';
          return `
            <tr>
              <td class="queue-name">${escapeHtml(q.name)}</td>
              <td><span class="badge-count badge-ready">${q.ready}</span></td>
              <td><span class="badge-count badge-inflight">${q.in_flight}</span></td>
              <td><span class="badge-count ${dlqBadgeClass}">${q.dlq}</span></td>
              <td>
                <div class="actions-cell">
                  <button class="btn-action btn-requeue" ${disabledAttr} onclick="requeueDlq('${escapeJs(q.name)}')">Requeue DLQ</button>
                  <button class="btn-action btn-purge" ${disabledAttr} onclick="purgeDlq('${escapeJs(q.name)}')">Purge DLQ</button>
                </div>
              </td>
            </tr>
          `;
        }).join('');
      }

      document.getElementById('last-updated').textContent = 'Last synced at ' + new Date().toLocaleTimeString();
    }

    async function requeueDlq(queueName) {
      try {
        const resp = await fetch(`/api/dlq/requeue?queue=${encodeURIComponent(queueName)}`, { method: 'POST' });
        const res = await resp.json();
        if (res.status === 'ok') {
          fetchStats();
        } else {
          alert('Failed to requeue: ' + (res.message || 'unknown error'));
        }
      } catch (err) {
        alert('Error requeuing DLQ: ' + err);
      }
    }

    async function purgeDlq(queueName) {
      if (!confirm(`Purge all dead-letter items for queue "${queueName}"?`)) return;
      try {
        const resp = await fetch(`/api/dlq/purge?queue=${encodeURIComponent(queueName)}`, { method: 'POST' });
        const res = await resp.json();
        if (res.status === 'ok') {
          fetchStats();
        } else {
          alert('Failed to purge: ' + (res.message || 'unknown error'));
        }
      } catch (err) {
        alert('Error purging DLQ: ' + err);
      }
    }

    function escapeHtml(str) {
      return String(str).replace(/[&<>"']/g, m => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
      })[m]);
    }

    function escapeJs(str) {
      return String(str).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
    }

    fetchStats();
    setInterval(fetchStats, 2500);
  </script>
</body>
</html>
"#
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_prometheus_metrics_format() {
        let engine = QueueEngine::new();
        engine.lpush("emails", b"email1".to_vec());
        engine.lpush("emails", b"email2".to_vec());
        engine.lpush_delayed("emails", Duration::from_secs(60), b"email_delayed".to_vec());

        // Lease one task
        let _ = engine.rpop_with_lease("emails", "task-1".to_string(), Duration::from_secs(30));

        // Create a DLQ task by failing 3 times
        engine.lpush("failing", b"poison".to_vec());
        let _ = engine.rpop_with_lease("failing", "t-fail".to_string(), Duration::from_secs(30));
        engine.task_nack("failing", "t-fail");
        let _ = engine.rpop_with_lease("failing", "t-fail2".to_string(), Duration::from_secs(30));
        engine.task_nack("failing", "t-fail2");
        let _ = engine.rpop_with_lease("failing", "t-fail3".to_string(), Duration::from_secs(30));
        engine.task_nack("failing", "t-fail3");

        let metrics = generate_prometheus_metrics(&engine);

        assert!(
            metrics.contains("# HELP dtq_delayed_tasks_total Number of scheduled delayed tasks")
        );
        assert!(metrics.contains("# TYPE dtq_delayed_tasks_total gauge"));
        assert!(metrics.contains("dtq_delayed_tasks_total 1"));

        assert!(metrics.contains("# HELP dtq_queue_size Current ready task count in queue"));
        assert!(metrics.contains("dtq_queue_size{queue=\"emails\"} 1"));

        assert!(metrics.contains("# HELP dtq_in_flight_tasks Current leased tasks in queue"));
        assert!(metrics.contains("dtq_in_flight_tasks{queue=\"emails\"} 1"));

        assert!(metrics
            .contains("# HELP dtq_dead_letter_queue_size Tasks escalated to dead letter queue"));
        assert!(metrics.contains("dtq_dead_letter_queue_size{queue=\"failing\"} 1"));
    }

    #[test]
    fn test_json_stats_output() {
        let engine = QueueEngine::new();
        engine.lpush("orders", b"order1".to_vec());

        let json = generate_json_stats(&engine);
        assert!(json.contains("\"total_queues\":1"));
        assert!(json.contains("\"total_ready\":1"));
        assert!(json.contains("\"name\":\"orders\""));
    }
}
