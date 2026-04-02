#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Homeland – Serveur Rapport de Gestion
======================================
Fonctionne en local (port 5055) ET en production (Railway/Render).
Les credentials sont lus depuis :
  1. Variables d'environnement  (déploiement cloud)
  2. config.json                (usage local)
"""

import json, os, sys
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import requests
from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS

BASE_DIR = Path(__file__).parent
app = Flask(__name__, static_folder=str(BASE_DIR))
CORS(app)

# ─────────────────────────────────────────────
# CONFIG  (env vars > config.json)
# ─────────────────────────────────────────────

def load_config():
    """Charge la config depuis variables d'env ou config.json."""
    cfg = {}

    # ── Variables d'environnement (Railway / Render) ──
    if os.environ.get("HBO_EMAIL"):
        cfg["hbo"] = {
            "base_url": os.environ.get("HBO_BASE_URL", "https://hbo.homeland.immo/api"),
            "email":    os.environ["HBO_EMAIL"],
            "password": os.environ.get("HBO_PASSWORD", ""),
        }
    if os.environ.get("RINGOVER_API_KEY"):
        cfg["ringover"] = {
            "base_url": os.environ.get("RINGOVER_BASE_URL", "https://public-api.ringover.com/v2"),
            "api_key":  os.environ["RINGOVER_API_KEY"],
        }
    if os.environ.get("FRONT_TOKEN"):
        cfg["front"] = {
            "base_url": os.environ.get("FRONT_BASE_URL", "https://api2.frontapp.com"),
            "token":    os.environ["FRONT_TOKEN"],
        }

    # ── Fallback config.json ──
    config_path = BASE_DIR / "config.json"
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            file_cfg = json.load(f)
        for key in ("hbo", "ringover", "front", "rapport"):
            if key not in cfg:
                cfg[key] = file_cfg.get(key, {})

    return cfg

# ─────────────────────────────────────────────
# HBO  (JWT cache 8h)
# ─────────────────────────────────────────────

_token_cache = {"token": None, "expires": datetime.min}

def hbo_token(cfg):
    now = datetime.now()
    if _token_cache["token"] and now < _token_cache["expires"]:
        return _token_cache["token"]
    r = requests.post(
        f"{cfg['hbo']['base_url']}/v2/login_check",
        json={"email": cfg["hbo"]["email"], "password": cfg["hbo"]["password"]},
        timeout=15
    )
    r.raise_for_status()
    t = r.json().get("token")
    _token_cache["token"]   = t
    _token_cache["expires"] = now + timedelta(hours=8)
    return t

def hbo(cfg, path, params=None, method="GET", body=None):
    headers = {"Authorization": f"Bearer {hbo_token(cfg)}", "Content-Type": "application/json"}
    url = f"{cfg['hbo']['base_url']}{path}"
    try:
        r = (requests.post(url, headers=headers, json=body or {}, timeout=20)
             if method == "POST"
             else requests.get(url, headers=headers, params=params, timeout=20))
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  ⚠ HBO {method} {path}: {e}")
        return None

def list_items(data):
    if data is None: return []
    if isinstance(data, list): return data
    for key in ("hydra:member", "member", "data", "items"):
        if key in data: return data[key]
    return []

# ─────────────────────────────────────────────
# RINGOVER
# ─────────────────────────────────────────────

def ringover_calls(cfg, date_start, date_end):
    base, api_key = cfg["ringover"]["base_url"], cfg["ringover"]["api_key"]
    calls, offset = [], 0
    while True:
        try:
            r = requests.get(f"{base}/calls",
                headers={"Authorization": api_key},
                params={"limit_count": 100, "limit_offset": offset,
                        "period_start": f"{date_start}T00:00:00",
                        "period_end":   f"{date_end}T23:59:59",
                        "call_type":    "inbound"},
                timeout=20)
            r.raise_for_status()
            batch = r.json().get("callList", r.json().get("calls", []))
            if not batch: break
            calls.extend(batch)
            if len(batch) < 100: break
            offset += 100
        except Exception as e:
            print(f"  ⚠ Ringover: {e}"); break
    return calls

# ─────────────────────────────────────────────
# FRONT
# ─────────────────────────────────────────────

def front_convs(cfg, date_start, date_end):
    base, token = cfg["front"]["base_url"], cfg["front"]["token"]
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    start_ts = int(datetime.strptime(date_start, "%Y-%m-%d").timestamp())
    end_ts   = int(datetime.strptime(date_end,   "%Y-%m-%d").timestamp())
    convs, page_token = [], None
    while len(convs) < 5000:
        params = {"q": f"after:{start_ts} before:{end_ts}", "limit": 100}
        if page_token: params["page_token"] = page_token
        try:
            r = requests.get(f"{base}/conversations", headers=headers, params=params, timeout=20)
            r.raise_for_status()
            data  = r.json()
            items = data.get("_results", [])
            if not items: break
            convs.extend(items)
            nxt = data.get("_pagination", {}).get("next", "")
            if not nxt or "page_token=" not in nxt: break
            page_token = nxt.split("page_token=")[-1].split("&")[0]
        except Exception as e:
            print(f"  ⚠ Front: {e}"); break
    return convs

# ─────────────────────────────────────────────
# PROCESSING → format attendu par rapport_v3.html
# ─────────────────────────────────────────────

CATEGORY_KEYWORDS = {
    "sinistre": "SINISTRES", "dde": "SINISTRES", "mri": "SINISTRES",
    "infiltration": "SINISTRES", "fuite": "SINISTRES", "dégât": "SINISTRES",
    "travaux": "TRAVAUX", "réfection": "TRAVAUX", "ravalement": "TRAVAUX",
    "dtg": "TRAVAUX", "remplacement": "TRAVAUX", "rénovation": "TRAVAUX",
    "mutation": "MUTATIONS", "cession": "MUTATIONS", "vente": "MUTATIONS",
    "litige": "LITIGES",
}
CLOSED_WORDS = {"closed","terminé","termine","done","completed","clos","archivé","archive","resolved"}

def categorize(p):
    txt = " ".join([
        str(p.get("title") or ""), str(p.get("name") or ""),
        str(p.get("subject") or ""), str(p.get("type") or ""),
        str(p.get("category") or "")
    ]).lower()
    for kw, cat in CATEGORY_KEYWORDS.items():
        if kw in txt: return cat
    return "GESTION"

def is_closed(p):
    st = str(p.get("status") or p.get("state") or "").lower()
    return any(s in st for s in CLOSED_WORDS)

def to_projects_list(raw_items):
    """Convertit les projets/incidents HBO en liste plate pour le HTML."""
    result = []
    for p in raw_items:
        cat = p.get("category") or categorize(p)
        result.append({
            "name":       p.get("title") or p.get("name") or p.get("subject") or "—",
            "category":   cat,
            "status":     "Clôturé" if is_closed(p) else "En cours",
            "start_date": (p.get("createdAt") or p.get("startDate") or "")[:10],
        })
    return result

def to_incidents_list(raw_items):
    """Incidents avec catégorie et statut pour les pages Sinistres/Gestion."""
    result = []
    for i in raw_items:
        cat = i.get("category") or categorize(i)
        result.append({
            "name":     i.get("title") or i.get("name") or i.get("subject") or "—",
            "category": cat,
            "status":   "Clôturé" if is_closed(i) else "En cours",
            "date":     (i.get("createdAt") or i.get("date") or "")[:10],
        })
    return result

def process_calls_v3(calls):
    total  = len(calls)
    in_cnt = sum(1 for c in calls if str(c.get("type") or c.get("direction") or "in").lower() in ("in","inbound","incoming"))
    out_cnt = total - in_cnt
    dur_sec = [int(c.get("duration") or c.get("duration_seconds") or 0) for c in calls]
    avg_dur = (sum(dur_sec) / len(dur_sec)) if dur_sec else 0
    by_month = defaultdict(int)
    for c in calls:
        ds = c.get("startedAt") or c.get("started_at") or c.get("date") or ""
        if ds: by_month[ds[:7]] += 1
    return {
        "total":              total,
        "inbound":            in_cnt,
        "outbound":           out_cnt,
        "avg_duration_seconds": round(avg_dur),
        "by_month":           dict(sorted(by_month.items())),
    }

def process_emails_v3(convs):
    by_month = defaultdict(int)
    resp_times = []
    for c in convs:
        ts = c.get("created_at") or c.get("last_message_at")
        if ts:
            by_month[datetime.fromtimestamp(ts).strftime("%Y-%m")] += 1
        elif c.get("createdAt"):
            by_month[c["createdAt"][:7]] += 1
        # Délai de réponse approximatif
        rt = c.get("response_time")
        if rt: resp_times.append(rt)
    avg_h = (sum(resp_times) / len(resp_times) / 3600) if resp_times else None
    return {
        "total":               len(convs),
        "avg_response_hours":  round(avg_h, 1) if avg_h else None,
        "response_rate":       None,
        "by_month":            dict(sorted(by_month.items())),
    }

def process_assemblies_v3(raw):
    result = []
    for a in raw:
        dt = a.get("date") or a.get("scheduledAt") or a.get("heldAt") or ""
        tp = a.get("type", {})
        t  = (tp.get("name") or tp.get("label") if isinstance(tp, dict) else str(tp)) or "AG"
        result.append({
            "name":   a.get("name") or a.get("title") or f"Assemblée {t}",
            "type":   t,
            "date":   dt[:10] if dt else "",
            "status": "Tenu" if is_closed(a) else "Planifié",
        })
    return sorted(result, key=lambda x: x["date"], reverse=True)

def satisfaction_to_v3(sat):
    """Convertit la satisfaction HBO en format CSAT attendu par le HTML."""
    if not sat: return {}
    score = sat.get("score") or sat.get("satisfactionScore")
    notes = sat.get("notes") or sat.get("satisfactionNotes") or []

    # Si notes est une liste de scores individuels, calculer la distribution
    if notes and isinstance(notes[0], (int, float)):
        dist = Counter(int(n) for n in notes if 1 <= n <= 5)
        distribution = {i: dist.get(i, 0) for i in range(1, 6)}
    else:
        distribution = {}

    # Convertir score sur 5 si c'est sur 100
    if score:
        score = float(score)
        if score > 5: score = round(score / 20, 1)

    return {"score": score, "distribution": distribution}

# ─────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(str(BASE_DIR), "rapport_v3.html")

@app.route("/api/buildings")
def get_buildings():
    try:
        cfg  = load_config()
        data = hbo(cfg, "/building/search", method="POST", body={})
        rows = list_items(data)
        buildings = [
            {"id": b.get("id"), "name": b.get("name") or b.get("address") or f"#{b.get('id')}"}
            for b in rows
        ]
        return jsonify({"ok": True, "buildings": buildings})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "buildings": []}), 200

@app.route("/api/building/<int:bid>/data")
def get_building_data(bid):
    try:
        cfg = load_config()
        mois = int(cfg.get("rapport", {}).get("periode_mois", 15))
        now  = datetime.now()

        # Période depuis les paramètres URL ou config
        ds_param = request.args.get("date_start")
        de_param = request.args.get("date_end")
        date_end   = de_param or now.strftime("%Y-%m-%d")
        date_start = ds_param or (now - timedelta(days=mois * 30)).strftime("%Y-%m-%d")

        # HBO
        b       = hbo(cfg, f"/building/{bid}") or {}
        projs   = list_items(hbo(cfg, f"/projects/{bid}", {"order": "desc"}))
        incs    = list_items(hbo(cfg, "/incidents", {"building_id": bid}))
        assembs = list_items(hbo(cfg, "/assemblies", {"building_id": bid}))

        # Communications
        all_calls  = ringover_calls(cfg, date_start, date_end)
        all_emails = front_convs(cfg, date_start, date_end)

        # Filtrer par bâtiment si possible
        b_name = b.get("name", "")
        bcalls  = [c for c in all_calls  if str(c.get("building_id","")) == str(bid)] or all_calls
        bemails = [e for e in all_emails if str(e.get("building_id","")) == str(bid)
                   or b_name[:6].lower() in str(e.get("subject","")).lower()] or all_emails

        # Satisfaction
        sat_raw = {
            "score": b.get("satisfactionScore") or b.get("satisfaction"),
            "notes": b.get("satisfactionNotes") or [],
        }

        result = {
            "building": {
                "name":    b.get("name") or b.get("address") or f"#{bid}",
                "address": b.get("address") or "",
                "city":    b.get("city") or "",
                "lots":    b.get("lots") or b.get("lotsCount") or 0,
            },
            "csat":       satisfaction_to_v3(sat_raw),
            "projects":   to_projects_list(projs),
            "incidents":  to_incidents_list(incs),
            "calls":      process_calls_v3(bcalls),
            "emails":     process_emails_v3(bemails),
            "assemblies": process_assemblies_v3(assembs),
            "period": {
                "start": date_start,
                "end":   date_end,
            }
        }
        return jsonify(result)

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/api/demo")
def get_demo():
    """Données de démo réalistes."""
    return jsonify({
        "building": {
            "name":    "34, 36 et 36 bis Boulevard de l'Hôpital",
            "address": "75005 Paris",
            "lots":    42,
        },
        "csat": {
            "score": 4.1,
            "distribution": {1: 1, 2: 3, 3: 6, 4: 14, 5: 18}
        },
        "projects": [
            {"name": "Ravalement de façade",               "category": "TRAVAUX",   "status": "En cours",  "start_date": "2024-03-01"},
            {"name": "Remplacement chaudière collective",   "category": "TRAVAUX",   "status": "En cours",  "start_date": "2024-06-15"},
            {"name": "Audit énergétique DPE collectif",     "category": "GESTION",   "status": "Clôturé",   "start_date": "2023-11-01"},
            {"name": "Mise en conformité cage d'escalier",  "category": "TRAVAUX",   "status": "Clôturé",   "start_date": "2023-09-01"},
            {"name": "Installation boîtes aux lettres",     "category": "GESTION",   "status": "Clôturé",   "start_date": "2024-01-10"},
            {"name": "Litige étanchéité terrasse lot 12",   "category": "LITIGES",   "status": "En cours",  "start_date": "2024-04-01"},
        ],
        "incidents": [
            {"name": "Dégât des eaux appartement 3B",        "category": "SINISTRES",  "status": "Clôturé", "date": "2024-02-14"},
            {"name": "Infiltration toiture — cage B",         "category": "SINISTRES",  "status": "En cours","date": "2024-08-05"},
            {"name": "Fissures parking sous-sol",             "category": "SINISTRES",  "status": "En cours","date": "2024-09-22"},
            {"name": "Ravalement de façade côté boulevard",   "category": "TRAVAUX",    "status": "En cours","date": "2024-03-01"},
            {"name": "Remplacement canalisations eaux usées", "category": "TRAVAUX",    "status": "Clôturé", "date": "2023-12-10"},
            {"name": "Mutation lot 7 — acte signé",           "category": "MUTATIONS",  "status": "Clôturé", "date": "2024-05-30"},
            {"name": "Mutation lot 19 — en cours",            "category": "MUTATIONS",  "status": "En cours","date": "2024-11-03"},
            {"name": "Contentieux charges impayées lot 24",   "category": "LITIGES",    "status": "En cours","date": "2024-04-15"},
            {"name": "Réparation portail électrique",         "category": "GESTION",    "status": "Clôturé", "date": "2024-07-08"},
            {"name": "Remplacement visiophone",               "category": "GESTION",    "status": "Clôturé", "date": "2024-09-14"},
        ],
        "emails": {
            "total": 387, "avg_response_hours": 3.2, "response_rate": 0.96,
            "by_month": {
                "2024-04":28,"2024-05":31,"2024-06":29,"2024-07":22,"2024-08":18,
                "2024-09":33,"2024-10":35,"2024-11":38,"2024-12":30,
                "2025-01":32,"2025-02":34,"2025-03":37
            }
        },
        "calls": {
            "total": 214, "inbound": 168, "outbound": 46, "avg_duration_seconds": 187
        },
        "assemblies": [
            {"name": "Assemblée Générale Ordinaire 2024",           "type": "AGO", "date": "2024-06-12", "status": "Tenu"},
            {"name": "Assemblée Générale Extraordinaire — Ravalement","type": "AGE","date": "2024-11-20", "status": "Tenu"},
        ]
    })

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

# ─────────────────────────────────────────────
# START
# ─────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5055))
    host = "0.0.0.0" if os.environ.get("PORT") else "127.0.0.1"

    print("=" * 52)
    print("  Homeland · Rapport de Gestion")
    print("=" * 52)
    print(f"\n  Dashboard : http://localhost:{port}")
    print(f"  Arrêt     : Ctrl+C\n")

    if not os.environ.get("PORT"):
        # Local : tenter la connexion HBO
        try:
            cfg = load_config()
            t   = hbo_token(cfg)
            print("  ✅ HBO connecté" if t else "  ⚠  HBO: vérifiez config.json")
        except Exception as e:
            print(f"  ⚠  HBO: {e}")
        import webbrowser, threading
        threading.Timer(1.5, lambda: webbrowser.open(f"http://localhost:{port}")).start()

    app.run(host=host, port=port, debug=False)
