"""
web/views.py - Material Design 3 Dark Theme Dashboard and Reporting Hub for Korean Sunrise.
"""
from __future__ import annotations

import html
import json
import urllib.parse
from typing import Any, Dict, List, Optional


def render_login_html() -> str:
    return """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Вход | Korean Sunrise Intelligence</title>
<style>
:root {
  --bg-color: #0b0e17;
  --surface-color: #151c2c;
  --surface-border: rgba(255, 255, 255, 0.08);
  --primary-color: #38bdf8;
  --primary-glow: rgba(56, 189, 248, 0.25);
  --text-main: #f8fafc;
  --text-muted: #94a3b8;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
  background-color: var(--bg-color);
  color: var(--text-main);
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 100vh;
  padding: 16px;
}
.login-card {
  background: var(--surface-color);
  border: 1px solid var(--surface-border);
  border-radius: 20px;
  padding: 36px;
  width: 100%;
  max-width: 420px;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.6);
  position: relative;
  overflow: hidden;
}
.login-card::before {
  content: '';
  position: absolute;
  top: -50px;
  right: -50px;
  width: 120px;
  height: 120px;
  background: radial-gradient(circle, var(--primary-glow) 0%, transparent 70%);
  border-radius: 50%;
}
.logo-title {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
}
.logo-icon {
  font-size: 28px;
}
h1 {
  font-size: 22px;
  font-weight: 700;
  letter-spacing: -0.5px;
}
.subtitle {
  color: var(--text-muted);
  font-size: 14px;
  margin-bottom: 28px;
}
.form-group {
  margin-bottom: 20px;
}
label {
  display: block;
  font-size: 13px;
  color: var(--text-muted);
  margin-bottom: 8px;
  font-weight: 500;
}
input[type="password"] {
  width: 100%;
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid var(--surface-border);
  border-radius: 10px;
  padding: 12px 14px;
  color: #fff;
  font-size: 15px;
  outline: none;
  transition: border-color 0.2s, box-shadow 0.2s;
}
input[type="password"]:focus {
  border-color: var(--primary-color);
  box-shadow: 0 0 0 3px rgba(56, 189, 248, 0.15);
}
button {
  width: 100%;
  background: linear-gradient(135deg, #0284c7 0%, #2563eb 100%);
  border: none;
  border-radius: 10px;
  color: #fff;
  font-size: 15px;
  font-weight: 600;
  padding: 14px;
  cursor: pointer;
  transition: opacity 0.2s, transform 0.1s;
}
button:hover { opacity: 0.95; }
button:active { transform: scale(0.99); }
</style>
</head>
<body>
<div class="login-card">
  <div class="logo-title">
    <div class="logo-icon">🌅</div>
    <h1>Korean Sunrise</h1>
  </div>
  <p class="subtitle">Авторизация для доступа к квантовой телеметрии</p>
  <form method="GET" action="/">
    <div class="form-group">
      <label for="token">Секретный ключ авторизации:</label>
      <input type="password" id="token" name="token" placeholder="Введите API токен" required autofocus>
    </div>
    <button type="submit">Войти в панель управления</button>
  </form>
</div>
</body>
</html>"""


def render_dashboard_html(
    service_status: Dict[str, Any],
    pool_snapshot: Dict[str, Any],
    profile_data: Dict[str, Any],
    thresholds_doc: Dict[str, Any],
    reports_cards: List[Dict[str, Any]],
    auth_token: str,
) -> str:
    summary = pool_snapshot.get("summary", {})
    counters = service_status.get("counters", {})
    streams = service_status.get("streams", {})
    krw_rate = service_status.get("usdt_krw_rate", 1350.0)

    # Thresholds stats
    th_meta = thresholds_doc.get("metadata", {})
    th_class = th_meta.get("classifications", {})
    th_items = sorted(thresholds_doc.get("thresholds", {}).items())

    # Build active streams pills
    stream_pills_html = []
    for s_name, s_info in streams.items():
        is_ok = s_info.get("connected", False)
        st_color = "#10b981" if is_ok else "#ef4444"
        sub_cnt = s_info.get("subscribed", 0)
        seen_cnt = s_info.get("symbols_seen", 0)
        stream_pills_html.append(f"""
        <div class="stream-pill">
          <span class="status-dot" style="background: {st_color};"></span>
          <span class="stream-title">{s_name.upper()}</span>
          <span class="stream-meta">{seen_cnt}/{sub_cnt}</span>
        </div>
        """)
    streams_bar = "".join(stream_pills_html)

    # Build report cards HTML
    report_cards_html = []
    if reports_cards:
        for rc in reports_cards:
            pnl_val = rc.get("net_pnl_usd", 0.0)
            pnl_color = "#10b981" if pnl_val >= 0 else "#ef4444"
            sign = "+" if pnl_val > 0 else ""

            # venues
            v_rows = []
            for v in rc.get("venues_breakdown", [])[:6]:
                v_pnl = v.get("net", 0.0)
                vp_col = "#10b981" if v_pnl >= 0 else "#ef4444"
                v_rows.append(f"""
                <div class="venue-chip">
                  <span class="venue-name">{html.escape(v.get('pair', ''))}</span>
                  <span class="venue-trades">{v.get('trades', 0)} сд ({v.get('winrate', 0)}%)</span>
                  <span class="venue-pnl" style="color: {vp_col};">{sign}${v_pnl:.2f}</span>
                </div>
                """)
            venues_html = "".join(v_rows) if v_rows else "<div style='color:#64748b;font-size:12px;'>Нет связок</div>"

            report_cards_html.append(f"""
            <div class="report-card">
              <div class="rc-header">
                <div>
                  <div class="rc-title">{html.escape(rc.get('title', ''))}</div>
                  <div class="rc-date">{html.escape(rc.get('date_from', ''))} &bull; {rc.get('total_rows', 0):,} импульсов ({rc.get('total_mb', 0)} MB)</div>
                </div>
                <div class="rc-pnl" style="color: {pnl_color};">
                  {sign}${pnl_val:.2f}
                  <div class="rc-winrate">Win Rate: {rc.get('winrate_pct', 0)}% &bull; {rc.get('total_trades', 0):,} сд</div>
                </div>
              </div>
              <div class="rc-body">
                <div class="section-label">Топ связок с Upbit</div>
                <div class="venues-list">
                  {venues_html}
                </div>
              </div>
              <div class="rc-footer">
                <button class="btn-read" onclick="viewReport('{rc.get('id', '')}')">📖 Читать квантовый отчёт</button>
                <a href="/download?token={auth_token}&file={rc.get('filename_md', '')}" class="btn-dl">Скачать .md</a>
              </div>
            </div>
            """)
    else:
        report_cards_html.append("<div style='padding:24px;color:#64748b;'>Нет доступных отчётов. Нажмите 'Сгенерировать суточный отчёт'.</div>")
    reports_view_html = "".join(report_cards_html)

    # Build Thresholds rows
    th_rows = []
    for pk, cfg in th_items[:80]:
        st = cfg.get("status", "default_baseline")
        en = cfg.get("enabled", True)
        badge_cls = "badge-green" if st == "aggressive_clean" else ("badge-blue" if st == "balanced_standard" else ("badge-yellow" if st == "high_friction" else "badge-red"))
        status_label = st.replace("_", " ").upper()
        th_rows.append(f"""
        <tr>
          <td style="font-weight:600;">{html.escape(pk)}</td>
          <td><span class="badge {badge_cls}">{status_label}</span></td>
          <td style="text-align:right;font-family:monospace;font-weight:700;">{cfg.get('min_spread_bps', 20.0):.1f} bps</td>
          <td style="text-align:right;font-family:monospace;">{cfg.get('expected_winrate', 0.85)*100:.1f}%</td>
          <td style="text-align:right;font-family:monospace;">{cfg.get('sample_trades', 0)}</td>
          <td style="text-align:center;">
            <span style="color:{'#10b981' if en else '#ef4444'};font-weight:700;">{'АКТИВНА' if en else 'ОТКЛЮЧЕНА'}</span>
          </td>
        </tr>
        """)
    thresholds_table_html = "".join(th_rows)

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Korean Sunrise | Lead-Lag Intelligence Hub</title>
<style>
:root {{
  --bg-0: #07090e;
  --bg-1: #0f141f;
  --bg-2: #171f30;
  --bg-3: #1f2a40;
  --border: rgba(255, 255, 255, 0.08);
  --border-focus: #38bdf8;
  --primary: #38bdf8;
  --accent: #f59e0b;
  --green: #10b981;
  --red: #ef4444;
  --text: #f8fafc;
  --text-muted: #94a3b8;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  background: var(--bg-0);
  color: var(--text);
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  min-height: 100vh;
  display: flex;
  flex-direction: column;
}}
/* Top bar */
.topbar {{
  background: var(--bg-1);
  border-bottom: 1px solid var(--border);
  padding: 12px 24px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 16px;
}}
.topbar-left {{
  display: flex;
  align-items: center;
  gap: 12px;
}}
.brand-title {{
  font-size: 18px;
  font-weight: 700;
  letter-spacing: -0.5px;
  display: flex;
  align-items: center;
  gap: 8px;
}}
.rate-badge {{
  background: rgba(245, 158, 11, 0.15);
  color: var(--accent);
  border: 1px solid rgba(245, 158, 11, 0.3);
  padding: 4px 10px;
  border-radius: 8px;
  font-size: 13px;
  font-family: monospace;
  font-weight: 700;
}}
/* Navigation Tabs */
.tabs {{
  display: flex;
  gap: 8px;
}}
.tab-btn {{
  background: transparent;
  border: none;
  color: var(--text-muted);
  font-size: 14px;
  font-weight: 600;
  padding: 8px 16px;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.15s;
}}
.tab-btn:hover {{ color: #fff; background: rgba(255,255,255,0.05); }}
.tab-btn.active {{ color: #fff; background: var(--bg-3); border: 1px solid var(--border); }}

/* Stream ticker bar */
.streams-strip {{
  background: var(--bg-1);
  border-bottom: 1px solid var(--border);
  padding: 8px 24px;
  display: flex;
  align-items: center;
  gap: 12px;
  overflow-x: auto;
}}
.stream-pill {{
  background: var(--bg-2);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 4px 10px;
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  white-space: nowrap;
}}
.status-dot {{ width: 8px; height: 8px; border-radius: 50%; }}
.stream-title {{ font-weight: 700; }}
.stream-meta {{ color: var(--text-muted); font-size: 11px; }}

/* Content Area */
.main-content {{
  padding: 24px;
  max-width: 1400px;
  margin: 0 auto;
  width: 100%;
  flex: 1;
}}
.tab-pane {{ display: none; }}
.tab-pane.active {{ display: block; }}

/* Cards grid */
.metric-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 16px;
  margin-bottom: 24px;
}}
.metric-card {{
  background: var(--bg-1);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 18px 20px;
}}
.mc-label {{
  font-size: 13px;
  color: var(--text-muted);
  margin-bottom: 8px;
}}
.mc-val {{
  font-size: 26px;
  font-weight: 700;
  letter-spacing: -0.5px;
}}
.mc-sub {{
  font-size: 12px;
  color: var(--text-muted);
  margin-top: 4px;
}}

/* Report Cards */
.report-card {{
  background: var(--bg-1);
  border: 1px solid var(--border);
  border-radius: 16px;
  padding: 24px;
  margin-bottom: 20px;
}}
.rc-header {{
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  border-bottom: 1px solid var(--border);
  padding-bottom: 16px;
  margin-bottom: 16px;
}}
.rc-title {{ font-size: 18px; font-weight: 700; }}
.rc-date {{ font-size: 13px; color: var(--text-muted); margin-top: 4px; }}
.rc-pnl {{ font-size: 24px; font-weight: 800; text-align: right; font-family: monospace; }}
.rc-winrate {{ font-size: 12px; color: var(--text-muted); font-family: sans-serif; font-weight: normal; }}
.section-label {{ font-size: 12px; text-transform: uppercase; color: var(--text-muted); font-weight: 700; margin-bottom: 10px; }}
.venues-list {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 8px; margin-bottom: 16px; }}
.venue-chip {{
  background: var(--bg-2);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 8px 12px;
  display: flex;
  justify-content: space-between;
  font-size: 13px;
}}
.rc-footer {{ display: flex; justify-content: flex-end; gap: 12px; }}
.btn-read {{
  background: linear-gradient(135deg, #0284c7 0%, #2563eb 100%);
  color: #fff;
  border: none;
  padding: 10px 18px;
  border-radius: 8px;
  font-weight: 600;
  cursor: pointer;
}}
.btn-dl {{
  background: var(--bg-2);
  color: var(--text);
  border: 1px solid var(--border);
  padding: 10px 18px;
  border-radius: 8px;
  font-size: 13px;
  text-decoration: none;
}}

/* Table styles */
.data-table {{
  width: 100%;
  border-collapse: collapse;
  background: var(--bg-1);
  border: 1px solid var(--border);
  border-radius: 12px;
  overflow: hidden;
}}
.data-table th, .data-table td {{
  padding: 12px 16px;
  border-bottom: 1px solid var(--border);
  font-size: 13px;
}}
.data-table th {{ background: var(--bg-2); color: var(--text-muted); font-weight: 600; text-align: left; }}
.badge {{ padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 700; text-transform: uppercase; }}
.badge-green {{ background: rgba(16, 185, 129, 0.15); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.3); }}
.badge-blue {{ background: rgba(56, 189, 248, 0.15); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.3); }}
.badge-yellow {{ background: rgba(245, 158, 11, 0.15); color: #f59e0b; border: 1px solid rgba(245, 158, 11, 0.3); }}
.badge-red {{ background: rgba(239, 68, 68, 0.15); color: #ef4444; border: 1px solid rgba(239, 68, 68, 0.3); }}

/* Modal */
.modal-overlay {{
  display: none;
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.8);
  backdrop-filter: blur(4px);
  z-index: 1000;
  padding: 24px;
  align-items: center;
  justify-content: center;
}}
.modal-overlay.active {{ display: flex; }}
.modal-box {{
  background: var(--bg-1);
  border: 1px solid var(--border);
  border-radius: 16px;
  width: 100%;
  max-width: 900px;
  max-height: 85vh;
  display: flex;
  flex-direction: column;
}}
.modal-header {{
  padding: 16px 20px;
  border-bottom: 1px solid var(--border);
  display: flex;
  justify-content: space-between;
  align-items: center;
}}
.modal-content {{
  padding: 24px;
  overflow-y: auto;
  font-family: monospace;
  font-size: 13px;
  line-height: 1.6;
  white-space: pre-wrap;
}}
</style>
</head>
<body>

<div class="topbar">
  <div class="topbar-left">
    <div class="brand-title">🌅 Korean Sunrise Intelligence</div>
    <div class="rate-badge">KRW/USDT: {krw_rate:.1f}</div>
  </div>
  <div class="tabs">
    <button class="tab-btn active" onclick="switchTab('dashboard')">Мониторинг</button>
    <button class="tab-btn" onclick="switchTab('reports')">Суточная отчётность</button>
    <button class="tab-btn" onclick="switchTab('thresholds')">Матрица спредов ({len(th_items)})</button>
    <button class="tab-btn" onclick="switchTab('profile')">Депозит и слоты</button>
    <button class="tab-btn" onclick="switchTab('controls')">Управление</button>
  </div>
</div>

<div class="streams-strip">
  {streams_bar}
</div>

<div class="main-content">

  <!-- TAB: DASHBOARD -->
  <div id="tab-dashboard" class="tab-pane active">
    <div class="metric-grid">
      <div class="metric-card">
        <div class="mc-label">Боевой пул (Battle)</div>
        <div class="mc-val" style="color:var(--green);">{summary.get('battle_active', 0)} <span style="font-size:16px;color:var(--text-muted);">/ {summary.get('battle', 0)}</span></div>
        <div class="mc-sub">Инкубатор: {summary.get('incubator', 0)} монет</div>
      </div>
      <div class="metric-card">
        <div class="mc-label">Зафиксировано импульсов</div>
        <div class="mc-val" style="color:var(--primary);">{counters.get('signals_triggered', 0):,}</div>
        <div class="mc-sub">⚔️ Battle: {counters.get('signals_battle', 0)} &bull; 🧪 Inc: {counters.get('signals_incubator', 0)}</div>
      </div>
      <div class="metric-card">
        <div class="mc-label">Сходимость импульсов</div>
        <div class="mc-val" style="color:var(--green);">{counters.get('signals_converged', 0):,}</div>
        <div class="mc-sub">Таймауты: {counters.get('signals_timeout', 0)} &bull; Развороты: {counters.get('signals_reversed', 0)}</div>
      </div>
      <div class="metric-card">
        <div class="mc-label">Тиков стакана обработано</div>
        <div class="mc-val">{counters.get('ticks_upbit', 0) + counters.get('ticks_followers', 0):,}</div>
        <div class="mc-sub">Upbit: {counters.get('ticks_upbit', 0):,} &bull; Follower: {counters.get('ticks_followers', 0):,}</div>
      </div>
    </div>

    <div style="background:var(--bg-1);border:1px solid var(--border);border-radius:14px;padding:20px;">
      <h3 style="margin-bottom:12px;">Статус активного боевого пула</h3>
      <div style="display:flex;flex-wrap:wrap;gap:8px;">
        {_render_battle_tags(pool_snapshot)}
      </div>
    </div>
  </div>

  <!-- TAB: REPORTS -->
  <div id="tab-reports" class="tab-pane">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;">
      <h2>Архив суточных квантовых отчётов</h2>
      <button class="btn-read" onclick="triggerDailyReport()">⚡ Сгенерировать отчёт сейчас</button>
    </div>
    {reports_view_html}
  </div>

  <!-- TAB: THRESHOLDS -->
  <div id="tab-thresholds" class="tab-pane">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
      <div>
        <h2>Матрица калиброванных порогов спреда</h2>
        <p style="color:var(--text-muted);font-size:13px;margin-top:4px;">
          Классификация: 
          Aggressive Clean: {th_class.get('aggressive_clean', 0)} | 
          Balanced Standard: {th_class.get('balanced_standard', 0)} | 
          High Friction: {th_class.get('high_friction', 0)} | 
          Pruned: {th_class.get('toxic_pruned', 0)}
        </p>
      </div>
      <button class="btn-read" onclick="triggerCalibration()">🎯 Перекалибровать пороги</button>
    </div>
    <div style="overflow-x:auto;">
      <table class="data-table">
        <thead>
          <tr>
            <th>Связка (Монета:Лидер-Фолловер)</th>
            <th>Классификация</th>
            <th style="text-align:right;">Мин. Спред</th>
            <th style="text-align:right;">Ожидаемый WinRate</th>
            <th style="text-align:right;">Сделок в выборке</th>
            <th style="text-align:center;">Статус</th>
          </tr>
        </thead>
        <tbody>
          {thresholds_table_html}
        </tbody>
      </table>
    </div>
  </div>

  <!-- TAB: PROFILE -->
  <div id="tab-profile" class="tab-pane">
    <h2>Профиль депозита и сайзинга</h2>
    <div style="background:var(--bg-1);border:1px solid var(--border);border-radius:14px;padding:24px;margin-top:16px;max-width:700px;">
      <pre style="font-family:monospace;font-size:13px;line-height:1.5;">{json.dumps(profile_data, indent=2, ensure_ascii=False)}</pre>
    </div>
  </div>

  <!-- TAB: CONTROLS -->
  <div id="tab-controls" class="tab-pane">
    <h2>Управление и автономные пайплайны</h2>
    <div style="display:grid;grid-template-columns:repeat(auto-fit, minmax(300px, 1fr));gap:16px;margin-top:16px;">
      <div style="background:var(--bg-1);border:1px solid var(--border);border-radius:12px;padding:20px;">
        <h3>24-Часовой мастер-пайплайн</h3>
        <p style="color:var(--text-muted);font-size:13px;margin:8px 0 16px;">Запускает суточный скрининг 1440 свечей, оценку пула, калибровку спредов и отчётность.</p>
        <button class="btn-read" style="width:100%;" onclick="runPipeline()">🚀 Запустить суточный цикл</button>
      </div>
      <div style="background:var(--bg-1);border:1px solid var(--border);border-radius:12px;padding:20px;">
        <h3>Экспорт всех данных (.ZIP)</h3>
        <p style="color:var(--text-muted);font-size:13px;margin:8px 0 16px;">Выгрузка сигналов, отчётов и конфигураций в 1 клик без SSH.</p>
        <a href="/download?token={auth_token}" class="btn-dl" style="display:block;text-align:center;">📦 Скачать архив</a>
      </div>
    </div>
  </div>

</div>

<!-- MODAL REPORT VIEWER -->
<div id="reportModal" class="modal-overlay" onclick="closeModal(event)">
  <div class="modal-box" onclick="event.stopPropagation()">
    <div class="modal-header">
      <h3 id="modalTitle">Суточный отчёт</h3>
      <button style="background:none;border:none;color:#fff;font-size:20px;cursor:pointer;" onclick="closeModal(event)">&times;</button>
    </div>
    <div id="modalContent" class="modal-content">Загрузка...</div>
  </div>
</div>

<script>
const TOKEN = "{auth_token}";

function switchTab(name) {{
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  event.target.classList.add('active');
}}

async function viewReport(reportId) {{
  document.getElementById('modalTitle').innerText = 'Отчёт: ' + reportId;
  document.getElementById('modalContent').innerText = 'Загрузка...';
  document.getElementById('reportModal').classList.add('active');
  try {{
    const res = await fetch('/api/report/content?token=' + TOKEN + '&id=' + reportId);
    const text = await res.text();
    document.getElementById('modalContent').innerText = text;
  }} catch (e) {{
    document.getElementById('modalContent').innerText = 'Ошибка загрузки: ' + e;
  }}
}}

function closeModal(e) {{
  document.getElementById('reportModal').classList.remove('active');
}}

async function triggerDailyReport() {{
  if (!confirm('Сгенерировать суточный квантовый отчёт прямо сейчас?')) return;
  const res = await fetch('/api/report/generate?token=' + TOKEN, {{ method: 'POST' }});
  alert('Отчёт успешно сформирован!');
  location.reload();
}}

async function triggerCalibration() {{
  if (!confirm('Запустить алгоритмическую калибровку спредов по всем парам?')) return;
  const res = await fetch('/api/thresholds/calibrate?token=' + TOKEN, {{ method: 'POST' }});
  alert('Калибровка завершена!');
  location.reload();
}}

async function runPipeline() {{
  if (!confirm('Запустить 24-часовой автономный пайплайн?')) return;
  const res = await fetch('/api/daily-pipeline?token=' + TOKEN, {{ method: 'POST' }});
  alert('Пайплайн запущен в фоне!');
}}
</script>
</body>
</html>"""


def _render_battle_tags(pool_snapshot: Dict[str, Any]) -> str:
    coins_dict = pool_snapshot.get("coins", {})
    battle = [
        c for c in coins_dict.values()
        if isinstance(c, dict) and c.get("basket") == "battle"
    ]
    if not battle:
        return "<span style='color:#64748b;font-size:13px;'>Боевой пул пока пуст. Монеты находятся в инкубаторе и промоутятся после оценки.</span>"
    tags = []
    for c in sorted(battle, key=lambda x: x.get("mean_net_pnl_bps", 0.0), reverse=True):
        name = html.escape(str(c.get("coin", "")))
        en = c.get("enabled", True)
        col = "#10b981" if en else "#64748b"
        tags.append(f"""
        <div style="background:var(--bg-2);border:1px solid var(--border);border-radius:8px;padding:6px 12px;font-size:13px;display:flex;align-items:center;gap:6px;">
          <span style="color:{col};font-weight:700;">⚔️ {name}</span>
        </div>
        """)
    return "".join(tags)
