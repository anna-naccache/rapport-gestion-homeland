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
        json={"username": cfg["hbo"]["email"], "password": cfg["hbo"]["password"]},
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

def ringover_calls(cfg, date_start, date_end, building_name="", building_tags=None):
    """Récupère tous les appels (entrants et sortants) sur la période."""
    base, api_key = cfg["ringover"]["base_url"], cfg["ringover"]["api_key"]
    calls = []
    for call_type in ["inbound", "outbound"]:
        offset = 0
        while True:
            try:
                r = requests.get(f"{base}/calls",
                    headers={"Authorization": api_key},
                    params={"limit_count": 100, "limit_offset": offset,
                            "period_start": f"{date_start}T00:00:00",
                            "period_end":   f"{date_end}T23:59:59",
                            "call_type":    call_type},
                    timeout=20)
                r.raise_for_status()
                batch = r.json().get("callList", r.json().get("calls", []))
                if not batch: break
                calls.extend(batch)
                if len(batch) < 100: break
                offset += 100
            except Exception as e:
                print(f"  ⚠ Ringover ({call_type}): {e}"); break
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

# Mots-clés pour identifier le service depuis les tags/labels Ringover
SERVICE_KEYWORDS = {
    "gestion":       ["gestion", "gestionnaire", "copro", "copropriété"],
    "comptabilité":  ["compta", "comptabilité", "comptable", "finance"],
    "juridique":     ["juridique", "juriste", "contentieux", "avocat"],
    "support":       ["support", "technique", "urgence", "dépannage"],
}

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

def get_call_service(call):
    """Détermine le service d'un appel depuis ses tags/labels."""
    tags = []
    # Ringover peut retourner des tags dans différents champs
    for field in ("tags", "labels", "team", "ivr_option", "via_number_label"):
        val = call.get(field)
        if val:
            if isinstance(val, list):
                tags.extend([str(t).lower() for t in val])
            else:
                tags.append(str(val).lower())
    # Aussi regarder le numéro appelé / destination
    for field in ("to_number", "called_number", "to"):
        val = call.get(field)
        if val: tags.append(str(val).lower())

    tags_str = " ".join(tags)
    for service, keywords in SERVICE_KEYWORDS.items():
        if any(kw in tags_str for kw in keywords):
            return service
    return "autre"

def process_calls_v3(calls):
    total  = len(calls)
    in_cnt  = sum(1 for c in calls if str(c.get("type") or c.get("direction") or "in").lower() in ("in","inbound","incoming"))
    out_cnt = total - in_cnt
    dur_sec = [int(c.get("duration") or c.get("duration_seconds") or 0) for c in calls]
    total_sec = sum(dur_sec)
    avg_dur   = (total_sec / len(dur_sec)) if dur_sec else 0

    by_month = defaultdict(int)
    by_service = defaultdict(lambda: {"count": 0, "duration_seconds": 0})
    for c in calls:
        ds = c.get("startedAt") or c.get("started_at") or c.get("date") or ""
        if ds: by_month[ds[:7]] += 1
        svc = get_call_service(c)
        by_service[svc]["count"] += 1
        by_service[svc]["duration_seconds"] += int(c.get("duration") or c.get("duration_seconds") or 0)

    return {
        "total":                total,
        "inbound":              in_cnt,
        "outbound":             out_cnt,
        "avg_duration_seconds": round(avg_dur),
        "total_duration_hours": round(total_sec / 3600, 1),
        "by_month":             dict(sorted(by_month.items())),
        "by_service":           {k: dict(v) for k, v in by_service.items()},
    }

def process_emails_v3(convs):
    by_month = defaultdict(int)
    sent_count = 0
    received_count = 0
    for c in convs:
        ts = c.get("created_at") or c.get("last_message_at")
        if ts:
            by_month[datetime.fromtimestamp(ts).strftime("%Y-%m")] += 1
        elif c.get("createdAt"):
            by_month[c["createdAt"][:7]] += 1
        # Direction de la conversation (inbound = reçu, outbound = envoyé)
        direction = str(c.get("status") or "").lower()
        if direction in ("assigned", "unassigned", "open", ""):
            # Conversation initiée par le client = reçu
            received_count += 1
        else:
            sent_count += 1
    # Si on ne peut pas distinguer, total = received
    if sent_count == 0 and received_count == 0:
        received_count = len(convs)

    return {
        "total":          len(convs),
        "sent":           sent_count,
        "received":       received_count,
        "by_month":       dict(sorted(by_month.items())),
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

def process_visits_v3(assembs_raw, visits_raw=None):
    """Combine assemblées et visites en une seule liste."""
    result = process_assemblies_v3(assembs_raw)
    if visits_raw:
        for v in visits_raw:
            dt = v.get("date") or v.get("scheduledAt") or v.get("visitedAt") or ""
            tp = v.get("type", {})
            t  = (tp.get("name") or tp.get("label") if isinstance(tp, dict) else str(tp)) or "Visite"
            result.append({
                "name":   v.get("name") or v.get("title") or f"Visite {t}",
                "type":   t,
                "date":   dt[:10] if dt else "",
                "status": "Effectuée" if is_closed(v) else "Planifiée",
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
        cfg = load_config()
        tok = hbo_token(cfg)
        headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
        base_url = cfg['hbo']['base_url']

        buildings = []
        # Essai 1 : POST /building/search avec pagination
        try:
            r = requests.post(f"{base_url}/building/search",
                headers=headers,
                json={"page": 1, "itemsPerPage": 50},
                timeout=20)
            if r.status_code == 200:
                data = r.json()
                buildings = list_items(data)
        except Exception as e:
            print(f"  ⚠ /building/search: {e}")

        # Essai 2 : GET /buildings
        if not buildings:
            try:
                r = requests.get(f"{base_url}/buildings", headers=headers, timeout=20)
                if r.status_code == 200:
                    data = r.json()
                    buildings = list_items(data)
            except Exception as e:
                print(f"  ⚠ /buildings: {e}")

        # Essai 3 : GET /coproprietes ou /syndic/buildings
        if not buildings:
            for endpoint in ["/coproprietes", "/syndic/buildings", "/building/list"]:
                try:
                    r = requests.get(f"{base_url}{endpoint}", headers=headers, timeout=20)
                    if r.status_code == 200:
                        buildings = list_items(r.json())
                        if buildings: break
                except Exception:
                    pass

        result = []
        for b in buildings:
            bid = b.get("id") or b.get("buildingId")
            if not bid: continue
            result.append({
                "id":      bid,
                "name":    b.get("name") or b.get("address") or f"#{bid}",
                "address": b.get("address") or "",
                "city":    b.get("city") or "",
            })
        return jsonify({"ok": True, "buildings": result})
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

        # HBO — infos bâtiment
        b       = hbo(cfg, f"/building/{bid}") or {}
        projs   = list_items(hbo(cfg, f"/projects/{bid}", {"order": "desc"}))
        incs    = list_items(hbo(cfg, "/incidents", {"building_id": bid}))
        assembs = list_items(hbo(cfg, "/assemblies", {"building_id": bid}))
        visits  = list_items(hbo(cfg, "/visits", {"building_id": bid}))

        # Extraire gestionnaire et comptable
        manager_field = b.get("manager") or b.get("gestionnaire") or {}
        accountant_field = b.get("accountant") or b.get("comptable") or {}
        if isinstance(manager_field, dict):
            manager_name = (manager_field.get("firstName") or manager_field.get("first_name") or "") + " " + (manager_field.get("lastName") or manager_field.get("last_name") or "")
            manager_name = manager_name.strip() or manager_field.get("name") or manager_field.get("email") or ""
        else:
            manager_name = str(manager_field)
        if isinstance(accountant_field, dict):
            accountant_name = (accountant_field.get("firstName") or accountant_field.get("first_name") or "") + " " + (accountant_field.get("lastName") or accountant_field.get("last_name") or "")
            accountant_name = accountant_name.strip() or accountant_field.get("name") or accountant_field.get("email") or ""
        else:
            accountant_name = str(accountant_field)

        # Date de création / début de gestion
        created_at = b.get("createdAt") or b.get("created_at") or b.get("managedSince") or b.get("dateCreation") or ""
        if created_at:
            created_at = created_at[:10]

        # Communications
        b_name = b.get("name", "")
        building_tags = [b_name[:6].lower()] if b_name else []

        all_calls  = ringover_calls(cfg, date_start, date_end, building_name=b_name, building_tags=building_tags)
        all_emails = front_convs(cfg, date_start, date_end)

        # Filtrer par bâtiment si possible
        bcalls  = [c for c in all_calls  if str(c.get("building_id","")) == str(bid)] or all_calls
        bemails = [e for e in all_emails if str(e.get("building_id","")) == str(bid)
                   or b_name[:6].lower() in str(e.get("subject","")).lower()] or all_emails

        # Satisfaction
        sat_raw = {
            "score": b.get("satisfactionScore") or b.get("satisfaction"),
            "notes": b.get("satisfactionNotes") or [],
        }

        # Lots count
        lots_count = b.get("lots") or b.get("lotsCount") or b.get("lotsPrincipaux") or 0

        result = {
            "building": {
                "name":       b.get("name") or b.get("address") or f"#{bid}",
                "address":    b.get("address") or "",
                "city":       b.get("city") or "",
                "lots":       lots_count,
                "manager":    manager_name,
                "accountant": accountant_name,
                "created_at": created_at,
            },
            "csat":       satisfaction_to_v3(sat_raw),
            "projects":   to_projects_list(projs),
            "incidents":  to_incidents_list(incs),
            "calls":      process_calls_v3(bcalls),
            "emails":     process_emails_v3(bemails),
            "visits":     process_visits_v3(assembs, visits),
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
            "name":       "34, 36 et 36 bis Boulevard de l'Hôpital",
            "address":    "75005 Paris",
            "lots":       42,
            "manager":    "Sophie Martin",
            "accountant": "Pierre Dubois",
            "created_at": "2019-03-15",
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
            "total": 387, "sent": 205, "received": 182,
            "by_month": {
                "2024-04":28,"2024-05":31,"2024-06":29,"2024-07":22,"2024-08":18,
                "2024-09":33,"2024-10":35,"2024-11":38,"2024-12":30,
                "2025-01":32,"2025-02":34,"2025-03":37
            }
        },
        "calls": {
            "total": 214, "inbound": 168, "outbound": 46,
            "avg_duration_seconds": 187,
            "total_duration_hours": 11.1,
            "by_service": {
                "gestion":      {"count": 89, "duration_seconds": 18200},
                "comptabilité": {"count": 52, "duration_seconds": 9400},
                "support":      {"count": 43, "duration_seconds": 7800},
                "juridique":    {"count": 18, "duration_seconds": 5600},
                "autre":        {"count": 12, "duration_seconds": 1600},
            }
        },
        "visits": [
            {"name": "Assemblée Générale Ordinaire 2024",           "type": "AGO",    "date": "2024-06-12", "status": "Tenu"},
            {"name": "Assemblée Générale Extraordinaire — Ravalement","type": "AGE",  "date": "2024-11-20", "status": "Tenu"},
            {"name": "Visite technique — état des façades",          "type": "Visite","date": "2024-09-05", "status": "Effectuée"},
            {"name": "Visite conseil syndical",                      "type": "Visite","date": "2025-01-14", "status": "Effectuée"},
        ]
    })

@app.route("/api/debug")
def debug_apis():
    """Test toutes les APIs et retourne les réponses brutes."""
    cfg = load_config()
    out = {"hbo": {}, "ringover": {}, "front": {}}

    # ── HBO ──
    try:
        tok = hbo_token(cfg)
        headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
        base = cfg["hbo"]["base_url"]
        out["hbo"]["auth"] = "ok"

        # Essais endpoints bâtiments
        for label, method, path, body in [
            ("POST /building/search {page}", "POST", "/building/search", {"page": 1, "itemsPerPage": 20}),
            ("POST /building/search {}", "POST", "/building/search", {}),
            ("GET /buildings", "GET", "/buildings", None),
            ("GET /building", "GET", "/building", None),
            ("GET /coproprietes", "GET", "/coproprietes", None),
        ]:
            try:
                if method == "POST":
                    r = requests.post(f"{base}{path}", headers=headers, json=body, timeout=15)
                else:
                    r = requests.get(f"{base}{path}", headers=headers, timeout=15)
                out["hbo"][label] = {"status": r.status_code, "body": r.text[:300]}
            except Exception as e:
                out["hbo"][label] = {"error": str(e)}
    except Exception as e:
        out["hbo"]["auth"] = f"ERREUR: {e}"

    # ── Ringover ──
    try:
        from datetime import date
        today = date.today().isoformat()
        month_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        r = requests.get(f"{cfg['ringover']['base_url']}/calls",
            headers={"Authorization": cfg["ringover"]["api_key"]},
            params={"limit_count": 5, "limit_offset": 0,
                    "period_start": f"{month_ago}T00:00:00",
                    "period_end": f"{today}T23:59:59",
                    "call_type": "inbound"},
            timeout=15)
        out["ringover"] = {"status": r.status_code, "body": r.text[:500]}
    except Exception as e:
        out["ringover"] = {"error": str(e)}

    # ── Front ──
    try:
        r = requests.get(f"{cfg['front']['base_url']}/conversations",
            headers={"Authorization": f"Bearer {cfg['front']['token']}", "Accept": "application/json"},
            params={"limit": 5},
            timeout=15)
        out["front"] = {"status": r.status_code, "body": r.text[:500]}
    except Exception as e:
        out["front"] = {"error": str(e)}

    return jsonify(out)

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
