"""
SPECTRA‑SITE Builder (v1.0)
==========================
Static‑site generator for SPECTRA archives.  Produces a responsive, Tailwind‑styled
HTML dashboard with Chart.js visualisations.

Key features
------------
* **Self‑contained** – runs anywhere Python 3.10+ is available; no external
  CSS/JS files needed (CDNs used).
* **Tailwind UI** – modern cards, dark‑mode friendly.
* **Chart.js** – line + pie charts; data injected as JSON.
* **Navigation** – sticky header, sidebar for multi‑page expansion.
* **Pluggable** – drop‑in analytics functions for new charts.

Usage
-----
```bash
python3 build_site.py --db spectra.sqlite3 --out ./site
```
Generates `index.html`, `assets/…`, and copies media thumbnails for avatars.

MIT‑style licence.  © 2025 John (SWORD‑EPI) – codename *SPECTRA‑004*.
"""
from __future__ import annotations

# ── Stdlib ───────────────────────────────────────────────────────────────
import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# ── Third‑party ───────────────────────────────────────────────────────────
from jinja2 import Environment, FileSystemLoader, select_autoescape  # type: ignore
from rich.console import Console

# ── Local import ─────────────────────────────────────────────────────────
from spectra_004_db_handler import SpectraDB  # assumes same dir / installed pkg

console = Console()

# ── Templates embedded in‑file for portability ───────────────────────────
BASE_HTML = """<!DOCTYPE html>
<html lang="en" class="scroll-smooth">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>SPECTRA – {{ title }}</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    /* Custom scrollbar */
    ::-webkit-scrollbar { width: 8px; }
    ::-webkit-scrollbar-thumb { background: #4b5563; border-radius: 4px; }
  </style>
</head>
<body class="bg-gray-100 text-gray-900 dark:bg-gray-900 dark:text-gray-100">
  <!-- Header -->
  <header class="bg-blue-600 text-white sticky top-0 z-50 shadow">
    <div class="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between">
      <h1 class="text-xl font-semibold tracking-wide">SPECTRA Dashboard</h1>
      <nav class="space-x-6 hidden md:block">
        <a href="index.html" class="hover:underline">Overview</a>
        <!-- future links -->
      </nav>
    </div>
  </header>

  <main class="max-w-7xl mx-auto px-4 py-8">{% block content %}{% endblock %}</main>

  <footer class="text-center text-sm text-gray-500 pb-6">
    Generated {{ build_time }} UTC • SPECTRA‑004
  </footer>
</body>
</html>"""

OVERVIEW_HTML = """{% extends 'base.html' %}
{% block content %}
<section>
  <h2 class="text-2xl font-semibold mb-6">Channel Statistics</h2>

  <!-- Stat cards -->
  <div class="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-10">
    {% for card in stats_cards %}
      <div class="bg-white dark:bg-gray-800 p-4 rounded shadow">
        <div class="text-sm text-gray-500 dark:text-gray-400">{{ card.title }}</div>
        <div class="text-2xl font-bold">{{ card.value }}</div>
      </div>
    {% endfor %}
  </div>

  <!-- Charts -->
  <div class="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-12">
    <div class="bg-white dark:bg-gray-800 p-6 rounded shadow">
      <h3 class="text-lg font-semibold mb-4">Messages Over Time</h3>
      <canvas id="monthlyChart"></canvas>
    </div>
    <div class="bg-white dark:bg-gray-800 p-6 rounded shadow">
      <h3 class="text-lg font-semibold mb-4">Media Types</h3>
      <canvas id="mediaChart"></canvas>
    </div>
  </div>

  <!-- Top users -->
  <div class="bg-white dark:bg-gray-800 p-6 rounded shadow">
    <h3 class="text-lg font-semibold mb-4">Most Active Users</h3>
    <div class="overflow-x-auto">
      <table class="min-w-full text-sm">
        <thead>
          <tr class="bg-gray-50 dark:bg-gray-700">
            <th class="px-4 py-2 text-left">User</th>
            <th class="px-4 py-2 text-left">Messages</th>
          </tr>
        </thead>
        <tbody>
          {% for item in top_users %}
          <tr class="border-b border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700">
            <td class="px-4 py-2 flex items-center space-x-2">
              {% if item.user.avatar %}
                <img src="{{ item.user.avatar }}" alt="avatar" class="w-6 h-6 rounded-full" />
              {% endif %}
              <span class="whitespace-nowrap">@{{ item.user.username or item.user.id }}</span>
            </td>
            <td class="px-4 py-2">{{ item.message_count }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </div>
</section>

<script>
const monthlyData = {{ monthly_counts | tojson }};
const labels = monthlyData.map(x => x.date);
const counts = monthlyData.map(x => x.count);
new Chart(document.getElementById('monthlyChart'), {
  type: 'line',
  data: { labels, datasets: [{ data: counts, label: 'Messages', fill: false, borderColor: '#2563eb', tension: .2 }] },
  options: { responsive: true, plugins:{legend:{display:false}} }
});

const mediaStats = {{ media_stats | tojson }};
new Chart(document.getElementById('mediaChart'), {
  type: 'doughnut',
  data: { labels:Object.keys(mediaStats), datasets:[{ data:Object.values(mediaStats), backgroundColor:['#2563eb','#60a5fa','#bfdbfe','#1e40af'] }] },
  options: { responsive:true }
});
</script>
{% endblock %}"""

# ── Build functions ──────────────────────────────────────────────────────

def gather_stats(db: SpectraDB) -> Dict[str, Any]:
    """Query DB for stats required by dashboard."""
    total_messages = db.cur.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    total_users = db.cur.execute("SELECT COUNT(*) FROM users").fetchone()[0]

    media_counts = {
        typ: db.cur.execute("SELECT COUNT(*) FROM media WHERE type=?", (typ,)).fetchone()[0]
        for typ in ("photo", "video", "document", "audio")
    }

    # Monthly counts (last 12 months)
    monthly = [
        {"date": m.slug, "count": m.count}
        for m in list(db.months())[-12:]
    ]

    # Top users
    top_users_raw = db.cur.execute(
        """SELECT user_id, COUNT(*) AS c FROM messages GROUP BY user_id ORDER BY c DESC LIMIT 10"""
    ).fetchall()
    top_users = []
    for uid, c in top_users_raw:
        urow = db.cur.execute("SELECT username, avatar FROM users WHERE id=?", (uid,)).fetchone()
        top_users.append({"user": DBUser(uid, *(urow or (None, None))), "message_count": c})

    return {
        "total_messages": total_messages,
        "total_users": total_users,
        "media_stats": media_counts,
        "monthly_counts": monthly,
        "top_users": top_users,
    }

# Lightweight user struct for template access
class DBUser:
    def __init__(self, uid, username, avatar):
        self.id = uid
        self.username = username
        self.avatar = avatar

# ── Main builder ─────────────────────────────────────────────────────────

def build_site(db_path: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    with SpectraDB(db_path) as db:
        data = gather_stats(db)

    # Prepare Jinja env from in‑memory dict
    env = Environment(loader=FileSystemLoader("."), autoescape=select_autoescape(["html"]))
    env.filters["tojson"] = json.dumps  # naive but ok for simple content

    # Register templates
    tmpl_dir = out_dir / "_tmpl_tmp"  # temp folder to satisfy FileSystemLoader
    tmpl_dir.mkdir(exist_ok=True)
    (tmpl_dir / "base.html").write_text(BASE_HTML)
    (tmpl_dir / "index.html").write_text(OVERVIEW_HTML)
    env.loader = FileSystemLoader(str(tmpl_dir))

    ctx = {**data, "title": "Overview", "build_time": datetime.utcnow().strftime("%Y‑%m‑%d %H:%M:%S")}
    rendered = env.get_template("index.html").render(ctx)
    (out_dir / "index.html").write_text(rendered)

    shutil.rmtree(tmpl_dir)
    console.print(f"[green]✓ Site built at [bold]{out_dir}[/]")

# ── CLI ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Build static HTML dashboard from SPECTRA DB")
    ap.add_argument("--db", required=True, type=Path, help="Path to spectra.sqlite3")
    ap.add_argument("--out", default=Path("site"), type=Path, help="Output directory for html files")
    args = ap.parse_args()
    build_site(args.db, args.out)
