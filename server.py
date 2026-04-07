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

_token_cache      = {"token": None, "expires": datetime.min}
_buildings_cache  = {"data": None, "expires": datetime.min}
_admin_users_cache = {"data": None, "expires": datetime.min}

BUILDINGS_CACHE_FILE = BASE_DIR / "buildings_cache.json"

def _load_disk_cache():
    """Charge le cache bâtiments depuis le disque si valide (< 24h)."""
    try:
        if BUILDINGS_CACHE_FILE.exists():
            with open(BUILDINGS_CACHE_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            expires = datetime.fromisoformat(d.get("expires", "2000-01-01"))
            if datetime.now() < expires and d.get("data"):
                _buildings_cache["data"]    = d["data"]
                _buildings_cache["expires"] = expires
                print(f"  💾 Cache disque chargé : {len(d['data'])} bâtiments")
                return True
    except Exception as e:
        print(f"  ⚠ Lecture cache disque: {e}")
    return False

def _save_disk_cache(data):
    """Sauvegarde le cache bâtiments sur disque."""
    try:
        expires = datetime.now() + timedelta(hours=24)
        with open(BUILDINGS_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"data": data, "expires": expires.isoformat()}, f, ensure_ascii=False)
        print(f"  💾 Cache disque sauvegardé : {len(data)} bâtiments")
    except Exception as e:
        print(f"  ⚠ Écriture cache disque: {e}")

# Charger le cache disque au démarrage
_load_disk_cache()

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
    while len(convs) < 500:   # plafonné à 500 pour éviter timeout
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

# Mapping direct des types HBO → catégories rapport
HBO_TYPE_MAP = {
    "gestion":  "GESTION",
    "travaux":  "TRAVAUX",
    "litige":   "LITIGES",
    "mutation": "MUTATIONS",
    "sinistre": "SINISTRES",
}

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
    # HBO projects: status == "inactif" → clôturé
    if str(p.get("status") or "").lower() == "inactif":
        return True
    st = str(p.get("status") or p.get("state") or "").lower()
    return any(s in st for s in CLOSED_WORDS)

def to_projects_list(raw_items):
    """Convertit les projets HBO en liste plate pour le HTML."""
    result = []
    for p in raw_items:
        # Utiliser le champ `type` HBO directement (gestion/travaux/litige/mutation/sinistre)
        hbo_type = str(p.get("type") or "").lower()
        cat = HBO_TYPE_MAP.get(hbo_type) or p.get("category") or categorize(p)
        closed = is_closed(p)
        result.append({
            "name":       p.get("description") or p.get("title") or p.get("name") or p.get("subject") or "—",
            "category":   cat,
            "status":     "Clôturé" if closed else "En cours",
            "start_date": (p.get("start_date") or p.get("createdAt") or p.get("startDate") or "")[:10],
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

def get_call_service_from_tags(call):
    """Fallback: détermine le service depuis les tags/labels de l'appel."""
    tags = []
    for field in ("tags", "labels", "team", "ivr_option", "via_number_label"):
        val = call.get(field)
        if val:
            if isinstance(val, list):
                tags.extend([str(t).lower() for t in val])
            else:
                tags.append(str(val).lower())
    tags_str = " ".join(tags)
    for service, keywords in SERVICE_KEYWORDS.items():
        if any(kw in tags_str for kw in keywords):
            return service
    return "support"

def get_admin_users_map(cfg):
    """Retourne dict email→service depuis HBO /admin_users. Cache 8h."""
    global _admin_users_cache
    now = datetime.now()
    if _admin_users_cache["data"] is not None and now < _admin_users_cache["expires"]:
        return _admin_users_cache["data"]
    try:
        data  = hbo(cfg, "/admin_users", {"itemsPerPage": 200})
        users = list_items(data) if data else []
        email_map = {}
        for u in users:
            email = (u.get("email") or "").lower().strip()
            if not email:
                continue
            # Chercher le champ service dans différents noms possibles
            service = (u.get("service") or u.get("team") or u.get("department")
                       or u.get("role") or u.get("poste") or "")
            email_map[email] = str(service).lower() if service else ""
        _admin_users_cache["data"]    = email_map
        _admin_users_cache["expires"] = now + timedelta(hours=8)
        print(f"  ✅ Admin users map: {len(email_map)} entrées")
        return email_map
    except Exception as e:
        print(f"  ⚠ Admin users map: {e}")
        _admin_users_cache["data"]    = {}
        _admin_users_cache["expires"] = now + timedelta(minutes=10)
        return {}

def get_service_for_call(call, admin_email_map):
    """Service de l'appel: email interne → profil HBO → fallback tags → 'support'."""
    user  = call.get("user") or {}
    email = (user.get("email") or "").lower().strip()
    if email and admin_email_map:
        svc = admin_email_map.get(email, "")
        if svc:
            # Normaliser vers les services connus
            for key, kws in SERVICE_KEYWORDS.items():
                if any(kw in svc for kw in kws):
                    return key
            # Retourner brut si non reconnu
            return svc
    return get_call_service_from_tags(call)

def process_calls_v3(calls, admin_email_map=None):
    total    = len(calls)
    in_cnt   = sum(1 for c in calls if str(c.get("direction") or c.get("type") or "in").lower()
                   in ("in", "inbound", "incoming"))
    out_cnt  = total - in_cnt
    # incall_duration = durée réelle de conversation (sans sonnerie)
    dur_sec  = [int(c.get("incall_duration") or c.get("duration") or c.get("duration_seconds") or 0)
                for c in calls]
    total_sec = sum(dur_sec)
    avg_dur   = (total_sec / len(dur_sec)) if dur_sec else 0

    by_month   = defaultdict(int)
    by_service = defaultdict(lambda: {"count": 0, "duration_seconds": 0})
    for c in calls:
        ds = c.get("startedAt") or c.get("started_at") or c.get("date") or ""
        if ds:
            by_month[ds[:7]] += 1
        svc = get_service_for_call(c, admin_email_map or {})
        dur = int(c.get("incall_duration") or c.get("duration") or c.get("duration_seconds") or 0)
        by_service[svc]["count"]            += 1
        by_service[svc]["duration_seconds"] += dur

    return {
        "total":                total,
        "inbound":              in_cnt,
        "outbound":             out_cnt,
        "avg_duration_seconds": round(avg_dur),
        "total_duration_hours": round(total_sec / 3600, 1),
        "by_month":             dict(sorted(by_month.items())),
        "by_service":           {k: dict(v) for k, v in by_service.items()},
    }

# ── Front : tag par bâtiment ──────────────────────────────────────

import re as _re

def find_front_tag(cfg, bid, b_name=""):
    """Trouve le tag Front du bâtiment (format 'NOM - BID'). Regex whole-number."""
    if not cfg.get("front"):
        return None
    base, token = cfg["front"]["base_url"], cfg["front"]["token"]
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        r = requests.get(f"{base}/tags", headers=headers, timeout=15)
        r.raise_for_status()
        tags = r.json().get("_results", [])
        # Regex: le BID apparaît comme nombre entier (pas collé à d'autres chiffres)
        pattern = _re.compile(r'(?<!\d)' + _re.escape(str(bid)) + r'(?!\d)')
        matching = [t for t in tags if pattern.search(t.get("name", ""))]
        if not matching:
            return None
        if len(matching) == 1:
            return matching[0]
        # Plusieurs correspondances → préférer celle qui contient des mots du nom du bâtiment
        if b_name:
            words = [w.lower() for w in b_name.split() if len(w) > 3]
            for t in matching:
                if any(w in t["name"].lower() for w in words):
                    return t
        return matching[0]
    except Exception as e:
        print(f"  ⚠ Front find_tag({bid}): {e}")
        return None

def front_convs_for_tag(cfg, tag_id, date_start, date_end):
    """Conversations Front pour un tag, filtrées par date (pagination arrêtée si trop vieille)."""
    base, token = cfg["front"]["base_url"], cfg["front"]["token"]
    headers  = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    start_ts = int(datetime.strptime(date_start, "%Y-%m-%d").timestamp())
    end_ts   = int(datetime.strptime(date_end,   "%Y-%m-%d").timestamp())
    convs, page_token, done = [], None, False
    while len(convs) < 1000 and not done:
        params = {"limit": 100}
        if page_token:
            params["page_token"] = page_token
        try:
            r = requests.get(f"{base}/tags/{tag_id}/conversations",
                             headers=headers, params=params, timeout=20)
            r.raise_for_status()
            data  = r.json()
            items = data.get("_results", [])
            if not items:
                break
            for c in items:
                ts = c.get("created_at") or c.get("last_message_at") or 0
                if ts and ts < start_ts:
                    done = True   # plus vieilles que la période → stop
                    break
                if not ts or ts <= end_ts:
                    convs.append(c)
            nxt = data.get("_pagination", {}).get("next", "")
            if not nxt or "page_token=" not in nxt:
                break
            page_token = nxt.split("page_token=")[-1].split("&")[0]
        except Exception as e:
            print(f"  ⚠ Front tag convs ({tag_id}): {e}")
            break
    return convs

def front_csat_from_convs(convs):
    """Extrait les scores CSAT des conversations Front (metadata.satisfaction.score)."""
    SCORE_MAP = {
        "amazing": 5, "great": 5, "good": 4, "neutral": 3, "bad": 2, "awful": 1,
        "thumbs_up": 4, "thumbs_down": 2,
        "very_good": 5, "positive": 4, "negative": 2,
        "5": 5, "4": 4, "3": 3, "2": 2, "1": 1,
    }
    scores = []
    for c in convs:
        meta = c.get("metadata") or {}
        sat  = meta.get("satisfaction") or {}
        raw  = sat.get("score") or sat.get("rating")
        if raw is None:
            # Essayer custom_fields
            for cf in (c.get("custom_fields") or []):
                fname = str(cf.get("name") or "").lower()
                if "csat" in fname or "satisfaction" in fname:
                    raw = cf.get("value")
                    break
        if raw is not None:
            if isinstance(raw, (int, float)):
                score = float(raw)
                if score > 5:
                    score = round(score / 20, 1)
                scores.append(min(max(score, 1), 5))
            else:
                mapped = SCORE_MAP.get(str(raw).lower().strip())
                if mapped:
                    scores.append(mapped)
    if not scores:
        return {}
    avg  = round(sum(scores) / len(scores), 1)
    dist = Counter(round(s) for s in scores)
    return {
        "score":        avg,
        "distribution": {i: dist.get(i, 0) for i in range(1, 6)},
        "count":        len(scores),
    }

def fetch_front_for_building(cfg, bid, b_name, date_start, date_end):
    """Récupère conversations + CSAT Front pour un bâtiment (via son tag)."""
    if not cfg.get("front"):
        return {"convs": [], "csat": {}}
    try:
        tag = find_front_tag(cfg, bid, b_name)
        if not tag:
            print(f"  ⚠ Front: aucun tag trouvé pour building {bid}")
            return {"convs": [], "csat": {}}
        convs = front_convs_for_tag(cfg, tag["id"], date_start, date_end)
        csat  = front_csat_from_convs(convs)
        print(f"  ✅ Front '{tag['name']}': {len(convs)} convs, CSAT={csat.get('score')}")
        return {"convs": convs, "csat": csat}
    except Exception as e:
        print(f"  ⚠ fetch_front_for_building({bid}): {e}")
        return {"convs": [], "csat": {}}

# ── HBO : projets avec détails + cache 1h par bâtiment ───────────

_projects_cache = {}   # {bid: {"data": [...], "expires": datetime}}

def fetch_projects_hbo(cfg, bid, max_workers=40):
    """
    Récupère TOUS les projets HBO d'un bâtiment :
      /projects/{bid} → liste d'IDs → /project/{pid} en parallèle (40 workers).
    Cache 1h par bâtiment pour éviter de rescanner à chaque rapport.
    """
    global _projects_cache
    now = datetime.now()

    # Cache valide ?
    cached = _projects_cache.get(bid)
    if cached and now < cached["expires"]:
        return cached["data"]

    try:
        raw = list_items(hbo(cfg, f"/projects/{bid}"))
        if not raw:
            return []

        # Normaliser en liste d'entiers
        ids = []
        for p in raw:
            if isinstance(p, int):
                ids.append(p)
            elif isinstance(p, dict) and p.get("id"):
                ids.append(int(p["id"]))

        print(f"  🔍 Projets {bid}: {len(ids)} IDs à récupérer…")

        def get_detail(pid):
            try:
                r = hbo(cfg, f"/project/{pid}")
                return r if (r and r.get("id")) else None
            except Exception:
                return None

        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(get_detail, pid): pid for pid in ids}
            for fut in as_completed(futures):
                r = fut.result()
                if r:
                    results.append(r)

        actifs   = sum(1 for p in results if p.get("status") != "inactif")
        inactifs = len(results) - actifs
        print(f"  ✅ Projets {bid}: {actifs} actifs / {inactifs} inactifs")

        _projects_cache[bid] = {"data": results, "expires": now + timedelta(hours=1)}
        return results
    except Exception as e:
        print(f"  ⚠ fetch_projects_hbo({bid}): {e}")
        return []

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

# Mapping type_id assemblée HBO → label lisible
ASSEMBLY_TYPE_MAP = {
    8:  "AGO",   # Assemblée Générale Ordinaire
    11: "AGE",   # Assemblée Générale Extraordinaire
}

def process_assemblies_v3(raw):
    """
    Convertit les assemblées HBO en liste de visites.
    Structure HBO : meeting_date = {"date": "2024-03-21 18:00:00", ...}
                    type_id = 8 (AGO) ou 11 (AGE)
                    status  = "done" | "planned" | ...
    """
    import re as _re2
    result = []
    for a in raw:
        # Extraire la date depuis l'objet imbriqué HBO
        md = a.get("meeting_date")
        if isinstance(md, dict):
            dt = (md.get("date") or "")[:10]
        else:
            dt = str(md or "")[:10]
        # Ignorer les dates epoch invalides
        if dt == "1970-01-01":
            dt = ""

        type_id = a.get("type_id")
        t = ASSEMBLY_TYPE_MAP.get(type_id) or a.get("type") or "AG"

        # Nettoyer la description HTML si présente
        desc = a.get("description") or ""
        desc_clean = _re2.sub(r'<[^>]+>', '', desc).strip() if desc else ""
        name = desc_clean or f"Assemblée {t}"

        result.append({
            "name":   name,
            "type":   t,
            "date":   dt,
            "status": "Tenu" if a.get("status") == "done" else "Planifié",
        })
    return sorted(result, key=lambda x: x["date"] or "0000", reverse=True)

def process_visits_v3(assembs_raw, visits_raw=None):
    """Assemblées HBO = visites (AGO / AGE). visits_raw ignoré (endpoint inexistant)."""
    return process_assemblies_v3(assembs_raw)

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
    try:
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
                    s = _building_summary(b)
                    if s:
                        found.append(s)

        found.sort(key=lambda x: x["id"])
        print(f"  ✅ Scan terminé : {len(found)} bâtiments")
        _buildings_cache["data"]    = found
        _buildings_cache["expires"] = datetime.now() + timedelta(hours=24)
        _save_disk_cache(found)
    except Exception as e:
        print(f"  ⚠ Scan error: {e}")
    finally:
        _scan_thread_running = False  # toujours resetter même si exception

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

        # HBO — infos bâtiment (synchrone, nécessaire pour b_name)
        b      = hbo(cfg, f"/building/{bid}") or {}
        b_name = b.get("name", "")

        # Log tous les champs du bâtiment pour diagnostiquer gestionnaire/comptable
        print(f"  🏢 Building {bid} keys: {list(b.keys())}")
        for k, v in b.items():
            if any(w in k.lower() for w in ["user", "admin", "gest", "compt", "referent", "manager", "account", "responsable"]):
                print(f"    → {k}: {v}")

        # Essai exhaustif des noms de champs possibles pour gestionnaire et comptable
        manager_id = (
            b.get("referentAdminUserId") or b.get("referent_admin_user_id") or
            b.get("managerId") or b.get("manager_id") or
            b.get("gestionnaire_id") or b.get("gestionnaireId") or
            b.get("responsableId") or b.get("responsable_id") or
            (b.get("manager") or {}).get("id") if isinstance(b.get("manager"), dict) else None or
            (b.get("gestionnaire") or {}).get("id") if isinstance(b.get("gestionnaire"), dict) else None
        )
        accountant_id = (
            b.get("accountantAdminUserId") or b.get("accountant_admin_user_id") or
            b.get("accountantId") or b.get("accountant_id") or
            b.get("comptable_id") or b.get("comptableId") or
            (b.get("accountant") or {}).get("id") if isinstance(b.get("accountant"), dict) else None or
            (b.get("comptable") or {}).get("id") if isinstance(b.get("comptable"), dict) else None
        )
        print(f"  👤 manager_id={manager_id}, accountant_id={accountant_id}")

        def admin_user_name(user_id):
            if not user_id: return ""
            try:
                u = hbo(cfg, f"/admin_users/{user_id}")
                if not u: return ""
                fname = u.get("firstname") or u.get("firstName") or u.get("first_name") or ""
                lname = u.get("name") or u.get("lastName") or u.get("last_name") or ""
                return f"{fname} {lname}".strip() or u.get("email", "")
            except Exception: return ""

        # Tout en parallèle : HBO + Ringover + Front + admin map
        with ThreadPoolExecutor(max_workers=12) as ex:
            f_mgr     = ex.submit(admin_user_name, manager_id)
            f_acct    = ex.submit(admin_user_name, accountant_id)
            f_works   = ex.submit(lambda: list_items(hbo(cfg, f"/building/works/{bid}")))
            f_projs   = ex.submit(fetch_projects_hbo, cfg, bid)
            f_assembs = ex.submit(lambda: list_items(hbo(cfg, "/assemblies", {"building_id": bid})))
            f_incs    = ex.submit(lambda: list_items(hbo(cfg, "/incidents",  {"building_id": bid})))
            f_calls   = ex.submit(ringover_calls, cfg, date_start, date_end)
            f_front   = ex.submit(fetch_front_for_building, cfg, bid, b_name, date_start, date_end)
            f_admins  = ex.submit(get_admin_users_map, cfg)

            manager_name    = f_mgr.result()
            accountant_name = f_acct.result()
            works     = f_works.result()
            projs     = f_projs.result() or to_projects_list(works)
            assembs   = f_assembs.result()
            incs      = f_incs.result()
            all_calls = f_calls.result()
            front_data= f_front.result()   # {"convs": [...], "csat": {...}}
            admin_map = f_admins.result()

        # Fallback noms gestionnaire/comptable — essai exhaustif des champs possibles
        def _extract_name(obj):
            """Extrait un nom depuis un objet ou une string."""
            if not obj: return ""
            if isinstance(obj, str): return obj.strip()
            if isinstance(obj, dict):
                # Essayer prénom + nom
                fn = obj.get("firstname") or obj.get("firstName") or obj.get("first_name") or ""
                ln = obj.get("name") or obj.get("lastName") or obj.get("last_name") or obj.get("surname") or ""
                full = f"{fn} {ln}".strip()
                if full: return full
                # Fallback sur fullName / displayName / email
                return (obj.get("fullName") or obj.get("full_name") or obj.get("displayName")
                        or obj.get("display_name") or obj.get("email") or "")
            return ""

        if not manager_name:
            # Chercher dans plusieurs champs possibles
            for field in ["manager", "gestionnaire", "responsable", "referent",
                          "referentAdminUser", "referent_admin_user"]:
                val = b.get(field)
                if val:
                    manager_name = _extract_name(val)
                    if manager_name: break

        if not accountant_name:
            for field in ["accountant", "comptable", "accountantAdminUser", "accountant_admin_user"]:
                val = b.get(field)
                if val:
                    accountant_name = _extract_name(val)
                    if accountant_name: break

        print(f"  👤 manager='{manager_name}', accountant='{accountant_name}'")

        created_at = (b.get("createdAt") or b.get("created_at") or
                      b.get("managedSince") or b.get("dateCreation") or "")
        if created_at: created_at = created_at[:10]

        lots_count = (b.get("lots") or b.get("lotsCount") or b.get("lotsPrincipaux")
                      or b.get("numberOfLots") or b.get("nb_lots") or 0)

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
            # CSAT depuis les conversations Front du bâtiment
            "csat":      front_data.get("csat", {}),
            # Projets depuis HBO /projects/{bid} + détails
            "projects":  to_projects_list(projs),
            # Incidents depuis HBO /incidents?building_id={bid}
            "incidents": to_incidents_list(incs),
            # Appels Ringover : service déduit via email interne → profil HBO
            "calls":     process_calls_v3(all_calls, admin_map),
            # Emails/CSAT Front filtrés par tag du bâtiment
            "emails":    process_emails_v3(front_data.get("convs", [])),
            # Visites HBO (assemblées + visites)
            "visits":    process_visits_v3(assembs),
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
        # Cache disque déjà valide → pas besoin de rescan
        if _buildings_cache["data"] and datetime.now() < _buildings_cache["expires"]:
            print("  ✅ Cache disque valide, pas de rescan au démarrage")
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
