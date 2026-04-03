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
from concurrent.futures import ThreadPoolExecutor, as_completed
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

_token_cache     = {"token": None, "expires": datetime.min}
_buildings_cache = {"data": None, "expires": datetime.min}

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

def ringover_calls(cfg, date_start, date_end, building_name="", building_tags=None, max_calls=2000):
    """Récupère les appels Ringover sur la période (plafonné à max_calls)."""
    base, api_key = cfg["ringover"]["base_url"], cfg["ringover"]["api_key"]
    calls, offset = [], 0
    while len(calls) < max_calls:
        try:
            r = requests.get(f"{base}/calls",
                headers={"Authorization": api_key},
                params={"limit_count": 100, "limit_offset": offset,
                        "period_start": f"{date_start}T00:00:00",
                        "period_end":   f"{date_end}T23:59:59"},
                timeout=15)
            r.raise_for_status()
            data = r.json()
            batch = data.get("call_list", data.get("callList", data.get("calls", [])))
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

def _building_summary(b):
    """Extrait les champs affichés dans la liste déroulante."""
    bid = b.get("id")
    if not bid:
        return None
    return {
        "id":      bid,
        "name":    b.get("name") or b.get("address") or f"#{bid}",
        "address": b.get("address") or "",
        "city":    b.get("city") or "",
    }

def _is_homeland_client(b):
    """Retourne True si le bâtiment appartient au syndic Homeland et est actif (client)."""
    status = str(b.get("status") or "").lower()
    if status not in ("client", ""):   # accepter aussi "" si l'API ne filtre pas
        return False
    # Chercher "homeland" dans tous les champs syndic possibles
    for field in ("syndicName", "syndic_name", "syndic"):
        val = b.get(field)
        if isinstance(val, dict):
            val = val.get("name", "")
        if val and "homeland" in str(val).lower():
            return True
    return False

_scan_thread_running = False

def _search_paged(cfg, criteria, page_size=50):
    """POST /building/search avec critères donnés, paginé. Retourne [] si refusé."""
    tok = hbo_token(cfg)
    headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
    base = cfg["hbo"]["base_url"]
    all_items, page = [], 1
    while True:
        try:
            r = requests.post(
                f"{base}/building/search",
                headers=headers,
                json={"building": criteria, "page": page, "itemsPerPage": page_size},
                timeout=25
            )
            if r.status_code != 200:
                return []
            data = r.json()
            items = list_items(data)
            if not items:
                break
            all_items.extend(items)
            total = (data.get("hydra:totalItems") or data.get("totalItems")
                     or data.get("total") or 0)
            if total and len(all_items) >= int(total):
                break
            if len(items) < page_size:
                break
            page += 1
        except Exception as e:
            print(f"  ⚠ /building/search page {page}: {e}")
            break
    return all_items

def _run_id_scan(cfg):
    """Scan parallèle IDs 51–978 en arrière-plan, met à jour le cache."""
    global _scan_thread_running
    print("  🔍 Background scan HBO IDs 51–978…")
    found = []
    tok = hbo_token(cfg)
    headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
    base = cfg["hbo"]["base_url"]

    def fetch(bid):
        try:
            r = requests.get(f"{base}/building/{bid}", headers=headers, timeout=5)
            if r.status_code != 200:
                return None
            b = r.json()
            return b if (b and b.get("id")) else None
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=40) as ex:
        futures = {ex.submit(fetch, bid): bid for bid in range(51, 979)}
        for fut in as_completed(futures):
            b = fut.result()
            if b and b.get("id"):
                # L'API est scopée au compte Homeland — on garde tout bâtiment valide
                # (pas de filtre sur status : le champ peut varier selon la version API)
                s = _building_summary(b)
                if s:
                    found.append(s)

    found.sort(key=lambda x: x["id"])
    print(f"  ✅ Scan background terminé : {len(found)} bâtiments")
    _buildings_cache["data"]    = found
    _buildings_cache["expires"] = datetime.now() + timedelta(hours=24)
    _scan_thread_running = False

def _scan_homeland_buildings(cfg):
    """
    Lance le scan ID 51–978 en background au premier appel.
    Retourne le cache immédiatement (vide si scan pas encore terminé).
    Cache 24h.
    """
    global _scan_thread_running
    now = datetime.now()

    # Cache valide → retour immédiat
    if _buildings_cache["data"] is not None and now < _buildings_cache["expires"]:
        return _buildings_cache["data"]

    # Lancer le scan en background si pas déjà en cours
    if not _scan_thread_running:
        _scan_thread_running = True
        import threading
        t = threading.Thread(target=_run_id_scan, args=(cfg,), daemon=True)
        t.start()
        print("  🔄 Scan background lancé (IDs 51–978, 40 workers)")

    return _buildings_cache["data"] or []

@app.route("/api/buildings")
def get_buildings():
    try:
        cfg = load_config()
        buildings = _scan_homeland_buildings(cfg)
        return jsonify({
            "ok":       True,
            "buildings": buildings,
            "scanning": _scan_thread_running,   # true = scan en cours, refaire un appel dans 15s
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "buildings": [], "scanning": False}), 200

@app.route("/api/buildings/raw")
def get_buildings_raw():
    """Teste plusieurs critères /building/search et retourne les résultats bruts."""
    try:
        cfg  = load_config()
        tok  = hbo_token(cfg)
        headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
        base = cfg["hbo"]["base_url"]
        results = {}
        for label, body in [
            ("status_client",   {"building": {"status": "client"}, "page": 1, "itemsPerPage": 3}),
            ("name_empty",      {"building": {"name": ""}, "page": 1, "itemsPerPage": 3}),
            ("name_space",      {"building": {"name": " "}, "page": 1, "itemsPerPage": 3}),
            ("city_paris",      {"building": {"city": "Paris"}, "page": 1, "itemsPerPage": 3}),
            ("empty_criteria",  {"building": {}, "page": 1, "itemsPerPage": 3}),
        ]:
            try:
                r = requests.post(f"{base}/building/search", headers=headers, json=body, timeout=20)
                d = r.json() if r.status_code == 200 else {}
                results[label] = {
                    "status": r.status_code,
                    "total":  d.get("hydra:totalItems") or d.get("totalItems") or d.get("total"),
                    "count":  len(list_items(d)),
                    "first":  list_items(d)[0].get("name","?") if list_items(d) else None,
                    "raw":    r.text[:200],
                }
            except Exception as e:
                results[label] = {"error": str(e)}
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/building/<int:bid>/data")
def get_building_data(bid):
    try:
        cfg = load_config()
        mois = int(cfg.get("rapport", {}).get("periode_mois", 15))
        now  = datetime.now()

        # Période depuis les paramètres URL ou config
        # Normalise YYYY-MM (input type=month) → YYYY-MM-DD
        def norm_date(d, end=False):
            if not d: return d
            if len(d) == 7:  # YYYY-MM
                if end:
                    from calendar import monthrange
                    y, m = int(d[:4]), int(d[5:])
                    return f"{d}-{monthrange(y,m)[1]:02d}"
                return f"{d}-01"
            return d

        ds_param = norm_date(request.args.get("date_start"), end=False)
        de_param = norm_date(request.args.get("date_end"),   end=True)
        date_end   = de_param or now.strftime("%Y-%m-%d")
        date_start = ds_param or (now - timedelta(days=mois * 30)).strftime("%Y-%m-%d")

        # HBO — infos bâtiment
        b = hbo(cfg, f"/building/{bid}") or {}

        # ── Gestionnaire et Comptable via admin_users ──
        def admin_user_name(user_id):
            """Retourne 'Prénom NOM' depuis /admin_users/{id}."""
            if not user_id:
                return ""
            try:
                u = hbo(cfg, f"/admin_users/{user_id}")
                if not u:
                    return ""
                fname = u.get("firstname") or u.get("firstName") or u.get("first_name") or ""
                lname = u.get("name") or u.get("lastName") or u.get("last_name") or ""
                full  = f"{fname} {lname}".strip()
                return full or u.get("email", "")
            except Exception:
                return ""

        manager_id    = b.get("referentAdminUserId") or b.get("referent_admin_user_id")
        accountant_id = b.get("accountantAdminUserId") or b.get("accountant_admin_user_id")

        b_name = b.get("name", "")

        # Tout en parallèle : HBO + Ringover + Front
        with ThreadPoolExecutor(max_workers=10) as ex:
            f_mgr     = ex.submit(admin_user_name, manager_id)
            f_acct    = ex.submit(admin_user_name, accountant_id)
            f_works   = ex.submit(lambda: list_items(hbo(cfg, f"/building/works/{bid}")))
            f_projs   = ex.submit(lambda: list_items(hbo(cfg, f"/projects/{bid}", {"order": "desc"})))
            f_assembs = ex.submit(lambda: list_items(hbo(cfg, "/assemblies", {"building_id": bid})))
            f_visits  = ex.submit(lambda: list_items(hbo(cfg, "/visits",     {"building_id": bid})))
            f_incs    = ex.submit(lambda: list_items(hbo(cfg, "/incidents",  {"building_id": bid})))
            f_calls   = ex.submit(ringover_calls, cfg, date_start, date_end, b_name)
            f_emails  = ex.submit(front_convs,    cfg, date_start, date_end)

            manager_name    = f_mgr.result()
            accountant_name = f_acct.result()
            works   = f_works.result()
            projs   = f_projs.result() if not works else works
            assembs = f_assembs.result()
            visits  = f_visits.result()
            incs    = f_incs.result()
            all_calls  = f_calls.result()
            all_emails = f_emails.result()

        # Fallback noms si admin_users vide
        if not manager_name:
            mf = b.get("manager") or b.get("gestionnaire") or {}
            manager_name = ((mf.get("firstName","")+" "+mf.get("name","")).strip()
                            if isinstance(mf, dict) else str(mf))
        if not accountant_name:
            af = b.get("accountant") or b.get("comptable") or {}
            accountant_name = ((af.get("firstName","")+" "+af.get("name","")).strip()
                               if isinstance(af, dict) else str(af))

        # Date de création / début de gestion
        created_at = (b.get("createdAt") or b.get("created_at") or
                      b.get("managedSince") or b.get("dateCreation") or "")
        if created_at: created_at = created_at[:10]

        # Lots
        lots_count = (b.get("lots") or b.get("lotsCount") or b.get("lotsPrincipaux")
                      or b.get("numberOfLots") or b.get("nb_lots") or 0)

        # Filtrer par bâtiment si possible
        bcalls  = ([c for c in all_calls if str(c.get("building_id","")) == str(bid)]
                   or all_calls)
        bemails = ([e for e in all_emails if str(e.get("building_id","")) == str(bid)
                    or b_name[:6].lower() in str(e.get("subject","")).lower()]
                   or all_emails)

        # Satisfaction
        sat_raw = {
            "score": b.get("satisfactionScore") or b.get("satisfaction"),
            "notes": b.get("satisfactionNotes") or [],
        }

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

@app.route("/api/buildings/status")
def buildings_status():
    return jsonify({
        "scanning":      _scan_thread_running,
        "cached":        _buildings_cache["data"] is not None,
        "count":         len(_buildings_cache["data"] or []),
        "cache_expires": str(_buildings_cache["expires"]),
        "sample":        (_buildings_cache["data"] or [])[:3],
    })

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

# ─────────────────────────────────────────────
# START
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
# WARMUP : lancer le scan bâtiments au démarrage
# Comme ça quand la première requête arrive, le cache est déjà chaud.
# ─────────────────────────────────────────────

def _warmup():
    try:
        cfg = load_config()
        if not cfg.get("hbo"):
            return
        print("  🔥 Warmup: scan bâtiments au démarrage…")
        _run_id_scan(cfg)
    except Exception as e:
        print(f"  ⚠ Warmup error: {e}")

import threading as _threading
_threading.Thread(target=_warmup, daemon=True).start()

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
