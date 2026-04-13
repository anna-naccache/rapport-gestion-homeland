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

_token_cache         = {"token": None, "expires": datetime.min}
_buildings_cache     = {"data": None, "expires": datetime.min}
_admin_users_cache   = {"data": None, "expires": datetime.min}
_admin_users_id_cache= {"data": None, "expires": datetime.min}   # id → "Prénom Nom"
_front_tags_cache    = {"data": None, "expires": datetime.min}   # toutes les pages de tags Front
_front_tags_lock     = None   # initialisé après import threading (voir bas du fichier)

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

def _extract_str(val):
    """Extrait une string depuis un champ qui peut être string ou objet HBO."""
    if not val:
        return ""
    if isinstance(val, str):
        return val.lower().strip()
    if isinstance(val, dict):
        # HBO retourne souvent {"id":1,"name":"Inactif"} ou {"id":1,"label":"inactif"}
        return (str(val.get("name") or val.get("label") or val.get("slug") or "")).lower().strip()
    return str(val).lower().strip()

def _extract_date(val):
    """Extrait YYYY-MM-DD depuis : str, dict HBO {"date":"2026-03-06 00:00:00..."},
    ou None. Retourne "" si vide ou invalide."""
    if not val:
        return ""
    if isinstance(val, dict):
        v = val.get("date") or ""
        return str(v)[:10]
    return str(val)[:10]

def is_closed(p):
    """Détecte si un projet HBO est clôturé/inactif.

    Règle stricte : on cherche explicitement "actif" pour dire "En cours".
    Si le champ est absent ou inconnu → Clos par défaut (évite le bruit).
    Valeurs HBO confirmées : status="actif" | "inactif"
    """
    for field in ("projet_statut", "status", "state"):
        val = p.get(field)
        if val is None:
            continue
        s = _extract_str(val)   # gère str ET dict HBO {id, name, label…}
        if not s:
            continue
        # Explicitement actif → En cours
        if s in ("actif", "active", "en cours", "open", "in_progress", "in progress", "progress"):
            return False
        # Explicitement clos → Clos
        if s in ("inactif", "inactive", "closed", "clôturé", "cloture",
                 "terminé", "termine", "done", "completed",
                 "archivé", "archive", "resolved", "clos"):
            return True

    # Booléens explicites
    if p.get("active") is True:
        return False
    if p.get("active") is False:
        return True
    if p.get("closed") is True:
        return True

    # Aucun champ de statut reconnu → Clos par défaut
    # (les projets sans statut clair ne doivent pas polluer la liste "En cours")
    return True

def _extract_hbo_type(p):
    """Extrait le type de projet (string normalisée) depuis un champ string ou objet.
    Priorité : champ natif HBO 'projet_type', fallback sur 'type'.
    """
    raw = p.get("projet_type") or p.get("type")
    if not raw:
        return ""
    if isinstance(raw, dict):
        # {"id":1,"name":"Gestion"} ou {"id":1,"label":"gestion"}
        return (str(raw.get("name") or raw.get("label") or raw.get("slug") or "")).lower().strip()
    return str(raw).lower().strip()

def to_projects_list(raw_items, date_start=None, date_end=None):
    """Convertit les projets HBO en liste filtrée par période du rapport.

    Règles de filtrage (spec utilisateur) :
      - En cours (actif)  : TOUS les projets actifs, sans filtre de date
      - Clos (inactif)    : UNIQUEMENT si projet_end_date est dans la période
                            (si end_date absente → inclus par défaut)
    """
    result = []
    for p in raw_items:
        # ── Type / catégorie ───────────────────────────────────────────
        hbo_type = _extract_hbo_type(p)
        cat = HBO_TYPE_MAP.get(hbo_type) or p.get("category") or categorize(p)

        # ── Statut (projet_statut natif HBO prioritaire) ───────────────
        closed = is_closed(p)

        # ── Dates ─────────────────────────────────────────────────────
        # _extract_date() gère str ET dict HBO {"date":"2026-03-06 00:00:00..."}
        start_date = _extract_date(
            p.get("projet_start_date") or p.get("start_date")
            or p.get("startDate") or p.get("createdAt")
        )
        end_date = _extract_date(
            p.get("projet_end_date") or p.get("end_date")
            or p.get("endDate") or p.get("closedAt")
        )
        # Fallback endDate pour les projets clôturés : lastUpdate.updateDate
        if not end_date and closed:
            lu = p.get("lastUpdate") or {}
            if isinstance(lu, dict):
                end_date = _extract_date(lu.get("updateDate"))
            if not end_date:
                end_date = _extract_date(
                    p.get("updateDate") or p.get("updatedAt") or p.get("updated_at")
                )

        # ── Filtrage par période du rapport ────────────────────────────
        if date_start and date_end:
            if closed:
                # Clôturé : filtrer par end_date (ou start_date si vide).
                ref_date = end_date or start_date
                if ref_date and (ref_date < date_start or ref_date > date_end):
                    continue
            # Actif : aucun filtre de date → tous les projets actifs sont inclus

        # ── Nom du projet (projet_description natif HBO prioritaire) ───
        name = (p.get("projet_description") or p.get("description")
                or p.get("title") or p.get("name") or p.get("subject") or "—")

        result.append({
            "name":       name,
            "category":   cat,
            "status":     "Clos" if closed else "En cours",
            "start_date": start_date,
            "end_date":   end_date,
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
            "status":   "Clos" if is_closed(i) else "En cours",
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

def get_admin_users_id_map(cfg):
    """Retourne dict id→'Prénom Nom' depuis HBO /admin_users. Cache 8h.
    Utilisé pour résoudre added_by dans les buildings_events."""
    global _admin_users_id_cache
    now = datetime.now()
    if _admin_users_id_cache["data"] is not None and now < _admin_users_id_cache["expires"]:
        return _admin_users_id_cache["data"]
    try:
        data  = hbo(cfg, "/admin_users", {"itemsPerPage": 200})
        users = list_items(data) if data else []
        id_map = {}
        for u in users:
            uid = u.get("id")
            if uid is None: continue
            fn  = u.get("firstname") or u.get("firstName") or u.get("first_name") or ""
            ln  = u.get("name") or u.get("lastName") or u.get("last_name") or ""
            full = f"{fn} {ln}".strip() or u.get("email", f"#{uid}")
            id_map[uid]       = full   # clé int
            id_map[str(uid)]  = full   # clé str (robustesse)
        _admin_users_id_cache["data"]    = id_map
        _admin_users_id_cache["expires"] = now + timedelta(hours=8)
        return id_map
    except Exception as e:
        print(f"  ⚠ Admin users id map: {e}")
        _admin_users_id_cache["data"]    = {}
        _admin_users_id_cache["expires"] = now + timedelta(minutes=10)
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
    """Trouve le tag Front du bâtiment via le cache global (_get_all_front_tags).
    Le cache est chargé une fois au warmup ou au premier appel (4h TTL).
    Format: '000632 16 RUE LEON JOST' (ID zero-paddé + adresse).
    """
    if not cfg.get("front"):
        return None

    bid_str = str(bid)
    bid_pad = bid_str.zfill(6)   # ex: "000632"
    pattern_id = _re.compile(
        r'(?:^|[^0-9])(' + _re.escape(bid_pad) + r'|' + _re.escape(bid_str) + r')(?:[^0-9]|$)'
    )
    words_fb = [w.lower() for w in b_name.split() if len(w) > 3] if b_name else []

    try:
        all_tags = _get_all_front_tags(cfg)
    except Exception as e:
        print(f"  ⚠ Front get_all_tags({bid}): {e}", flush=True)
        return None

    # Chercher par ID (format zero-paddé prioritaire)
    for t in all_tags:
        if pattern_id.search(t.get("name", "")):
            print(f"  🏷 Front: tag '{t.get('name')}' (building {bid})", flush=True)
            return t

    # Fallback : correspondance par mots du nom de bâtiment
    if words_fb:
        for t in all_tags:
            if any(w in t.get("name","").lower() for w in words_fb):
                print(f"  🏷 Front (nom): tag '{t.get('name')}' (building {bid})", flush=True)
                return t

    print(f"  ⚠ Front: aucun tag pour building {bid} (b_name={b_name!r})", flush=True)
    return None


def _get_all_front_tags(cfg):
    """Charge toutes les pages de tags Front avec cache 4h.
    Utilise un Lock pour éviter la double-pagination si warmup + requête arrivent en même temps.
    """
    global _front_tags_cache, _front_tags_lock
    now = datetime.now()
    # Lecture rapide sans lock (hot path)
    if _front_tags_cache["data"] is not None and now < _front_tags_cache["expires"]:
        return _front_tags_cache["data"]

    # Double-checked locking : un seul thread pagine, les autres attendent puis lisent le cache
    with _front_tags_lock:
        now = datetime.now()
        if _front_tags_cache["data"] is not None and now < _front_tags_cache["expires"]:
            return _front_tags_cache["data"]

        import time as _time
        base, token = cfg["front"]["base_url"], cfg["front"]["token"]
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        all_tags, page_token = [], None
        page_count = 0
        while True:
            params = {"limit": 200}
            if page_token:
                params["page_token"] = page_token
            # Retry avec backoff exponentiel sur 429.
            # Fenêtre rate-limit Front ~60s → on tente jusqu'à 7 fois (max wait 60s).
            for attempt in range(7):
                r = requests.get(f"{base}/tags", headers=headers, params=params, timeout=30)
                if r.status_code == 429:
                    # Respecter le header Retry-After si présent
                    retry_after = r.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait = int(retry_after) + 2
                        except Exception:
                            wait = min(2 ** attempt, 60)
                    else:
                        wait = min(2 ** attempt, 60)  # 1,2,4,8,16,32,60s
                    print(f"  ⏳ Front tags 429 (page {page_count+1}), retry dans {wait}s…", flush=True)
                    _time.sleep(wait)
                    continue
                r.raise_for_status()
                break
            else:
                raise Exception(f"Front tags: 429 persistant après 7 tentatives (page {page_count+1})")
            data = r.json()
            all_tags.extend(data.get("_results", []))
            page_count += 1
            nxt = data.get("_pagination", {}).get("next", "")
            if not nxt or "page_token=" not in nxt:
                break
            page_token = nxt.split("page_token=")[-1].split("&")[0]
            # Pause inter-page : 1.5s ≈ 40 req/min, sous la limite Front (~50 req/min)
            _time.sleep(1.5)

        _front_tags_cache["data"]    = all_tags
        _front_tags_cache["expires"] = now + timedelta(hours=4)
        print(f"  ✅ Front tags cache: {len(all_tags)} tags en {page_count} pages", flush=True)
        return all_tags

def front_convs_for_tag(cfg, tag_id, date_start, date_end):
    """Conversations Front pour un tag avec activité dans la période.

    L'API /tags/{id}/conversations trie par last_message_at DESC.
    On s'arrête quand last_message_at < start_ts (aucune activité possible ensuite).
    Bug corrigé : l'ancienne version utilisait created_at comme critère d'arrêt,
    ce qui coupait les conversations créées avant la période mais actives pendant.
    """
    base, token = cfg["front"]["base_url"], cfg["front"]["token"]
    headers  = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    start_ts = int(datetime.strptime(date_start, "%Y-%m-%d").timestamp())
    end_ts   = int(datetime.strptime(date_end,   "%Y-%m-%d").timestamp()) + 86399
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
                # L'API trie par last_message_at DESC → utiliser ce champ pour l'arrêt
                last_ts = c.get("last_message_at") or c.get("created_at") or 0
                if last_ts and last_ts < start_ts:
                    done = True   # Toutes les suivantes sont encore plus vieilles → stop
                    break
                # Inclure si la dernière activité est dans/avant la fin de fenêtre
                if not last_ts or last_ts <= end_ts:
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
    """Extrait les scores CSAT des conversations Front.

    Priorité d'extraction (Homeland stocke le rating dans les tags) :
      1. Tags "Rating X/5"  (ex. tag name = "Rating 1/5")
      2. metadata.satisfaction.survey_rating / score / rating
      3. custom_fields contenant "csat" ou "satisfaction"
    """
    import re as _re
    SCORE_MAP = {
        "amazing": 5, "great": 5, "good": 4, "neutral": 3, "bad": 2, "awful": 1,
        "thumbs_up": 4, "thumbs_down": 2,
        "very_good": 5, "positive": 4, "negative": 2,
        "5": 5, "4": 4, "3": 3, "2": 2, "1": 1,
    }
    _tag_rating_re = _re.compile(r"rating\s*(\d+)\s*/\s*5", _re.IGNORECASE)

    scores = []
    survey_sent_count  = 0
    no_response_count  = 0

    for c in convs:
        raw = None

        # ── 1. Tags "Rating X/5" (Homeland) ──────────────────────────────────
        tags_names = [t.get("name", "") for t in (c.get("tags") or [])]
        has_survey = any("survey sent" in tn.lower() for tn in tags_names)
        if has_survey:
            survey_sent_count += 1
        for tn in tags_names:
            m = _tag_rating_re.search(tn)
            if m:
                raw = int(m.group(1))
                break

        # ── 2. metadata.satisfaction (CSAT natif Front) ───────────────────────
        if raw is None:
            meta = c.get("metadata") or {}
            sat  = meta.get("satisfaction") or {}
            raw  = sat.get("survey_rating") or sat.get("score") or sat.get("rating")

        # ── 3. custom_fields ──────────────────────────────────────────────────
        if raw is None:
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
        elif has_survey:
            no_response_count += 1

    result = {
        "count":         len(scores),
        "survey_sent":   survey_sent_count,
        "no_response":   no_response_count,
    }
    if not scores:
        return result
    avg  = round(sum(scores) / len(scores), 1)
    dist = Counter(round(s) for s in scores)
    result.update({
        "score":        avg,
        "distribution": {i: dist.get(i, 0) for i in range(1, 6)},
    })
    return result

def fetch_hbo_csat(cfg, account_name, date_start, date_end):
    """Récupère le CSAT directement depuis l'API HBO /front_csat.

    Champs HBO : account_name, survey_rating, message_date
    Filtre côté API sur account_name + message_date pour éviter de lire les 30k entrées.
    Pagination sur les pages filtrées uniquement → rapide.
    Retourne le même format que front_csat_from_convs().
    """
    if not cfg.get("hbo"):
        return {}
    try:
        scores = []
        page   = 1
        while True:
            data = hbo(cfg, "/front_csat", params={
                "account_name":         account_name,   # filtre côté API
                "message_date[after]":  date_start,
                "message_date[before]": date_end,
                "itemsPerPage": 200,
                "page": page,
            })
            items = list_items(data)
            if not items:
                break
            for item in items:
                raw = item.get("survey_rating")
                if raw is not None:
                    try:
                        scores.append(min(max(float(raw), 1), 5))
                    except (TypeError, ValueError):
                        pass
            if len(items) < 200:
                break
            page += 1

        result = {"count": len(scores), "survey_sent": len(scores), "no_response": 0}
        if scores:
            avg  = round(sum(scores) / len(scores), 1)
            dist = Counter(round(s) for s in scores)
            result.update({
                "score":        avg,
                "distribution": {i: dist.get(i, 0) for i in range(1, 6)},
            })
        print(f"  ✅ HBO CSAT '{account_name}': {len(scores)} notes, score={result.get('score')}", flush=True)
        return result
    except Exception as e:
        print(f"  ⚠ fetch_hbo_csat('{account_name}'): {e}", flush=True)
        return {}


def fetch_front_for_building(cfg, bid, b_name, date_start, date_end):
    """Récupère conversations + CSAT Front pour un bâtiment (via son tag).
    CSAT vient maintenant de l'API HBO /front_csat (plus fiable que les tags).
    """
    if not cfg.get("front"):
        return {"convs": [], "csat": {}}
    try:
        tag = find_front_tag(cfg, bid, b_name)
        if not tag:
            print(f"  ⚠ Front: aucun tag trouvé pour building {bid}")
            return {"convs": [], "csat": {}}
        convs = front_convs_for_tag(cfg, tag["id"], date_start, date_end)
        # CSAT via HBO directement (account_name = b_name)
        csat  = fetch_hbo_csat(cfg, b_name, date_start, date_end) if cfg.get("hbo") else front_csat_from_convs(convs)
        print(f"  ✅ Front '{tag['name']}': {len(convs)} convs, CSAT={csat.get('score')}")
        return {"convs": convs, "csat": csat}
    except Exception as e:
        print(f"  ⚠ fetch_front_for_building({bid}): {e}")
        return {"convs": [], "csat": {}}

_front_accounts_cache = {"data": None, "expires": datetime.min}

_front_accounts_lock = None   # initialisé après import threading

def _get_all_front_accounts(cfg):
    """Charge tous les comptes Front avec cache 4h (pagination côté serveur).
    Le paramètre ?q= de l'API /accounts ne filtre pas par nom — on pagine tout
    et on cherche localement.
    Gère les 429 avec Retry-After + backoff exponentiel, pause 1.5s inter-page.
    """
    global _front_accounts_cache, _front_accounts_lock
    import time as _time
    now = datetime.now()
    if _front_accounts_cache["data"] is not None and now < _front_accounts_cache["expires"]:
        return _front_accounts_cache["data"]

    with _front_accounts_lock:
        now = datetime.now()
        if _front_accounts_cache["data"] is not None and now < _front_accounts_cache["expires"]:
            return _front_accounts_cache["data"]

        base, token = cfg["front"]["base_url"], cfg["front"]["token"]
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        all_accounts, page_token, page_count = [], None, 0

        while len(all_accounts) < 10000:
            params = {"limit": 100}
            if page_token:
                params["page_token"] = page_token

            # Retry avec backoff exponentiel sur 429
            for attempt in range(7):
                r = requests.get(f"{base}/accounts", headers=headers, params=params, timeout=20)
                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    wait = int(retry_after) + 2 if retry_after and retry_after.isdigit() \
                           else min(2 ** attempt, 60)
                    print(f"  ⏳ Front accounts 429 (page {page_count+1}), retry dans {wait}s…", flush=True)
                    _time.sleep(wait)
                    continue
                r.raise_for_status()
                break
            else:
                raise Exception(f"Front accounts: 429 persistant après 7 tentatives (page {page_count+1})")

            data  = r.json()
            items = data.get("_results", [])
            if not items:
                break
            all_accounts.extend(items)
            page_count += 1
            nxt = data.get("_pagination", {}).get("next", "")
            if not nxt or "page_token=" not in nxt:
                break
            page_token = nxt.split("page_token=")[-1].split("&")[0]
            _time.sleep(1.5)   # pause inter-page ≈ 40 req/min

        _front_accounts_cache["data"]    = all_accounts
        _front_accounts_cache["expires"] = now + timedelta(hours=4)
        print(f"  ✅ Front accounts cache: {len(all_accounts)} comptes en {page_count} pages", flush=True)
        return all_accounts


def _fetch_account_convs_direct(base, headers, account_id, start_ts, end_ts):
    """Tente GET /accounts/{id}/conversations (module CRM Front).
    Retourne la liste de conversations filtrées, ou None si 404."""
    import time as _time
    convs, page_token, done = [], None, False
    while len(convs) < 500 and not done:
        params = {"limit": 100}
        if page_token:
            params["page_token"] = page_token
        try:
            r = requests.get(f"{base}/accounts/{account_id}/conversations",
                             headers=headers, params=params, timeout=20)
            if r.status_code == 404:
                return None        # endpoint non disponible (plan sans CRM)
            if r.status_code == 429:
                retry = r.headers.get("Retry-After", "10")
                _time.sleep(int(retry) if retry.isdigit() else 10)
                continue
            r.raise_for_status()
        except requests.exceptions.HTTPError as he:
            if he.response is not None and he.response.status_code == 404:
                return None
            raise
        data  = r.json()
        items = data.get("_results", [])
        if not items:
            break
        for c in items:
            ts = c.get("created_at") or c.get("last_message_at") or 0
            if ts and ts < start_ts:
                done = True
                break
            if ts and ts <= end_ts:
                convs.append(c)
        nxt = data.get("_pagination", {}).get("next", "")
        if not nxt or "page_token=" not in nxt:
            break
        page_token = nxt.split("page_token=")[-1].split("&")[0]
    return convs


def _fetch_account_convs_via_contacts(base, headers, account_id, start_ts, end_ts,
                                       use_updated_at=False, extra_lookback_days=0):
    """Fallback : compte → contacts → conversations (sans module CRM).

    use_updated_at      : True → utilise updated_at pour l'inclusion (conversations
                          créées avant la fenêtre mais mises à jour pendant, ex. CSAT).
    extra_lookback_days : repousse la fenêtre d'early-stop de N jours en arrière.
                          Utile avec use_updated_at pour ne pas stopper trop tôt.
    """
    import time as _time
    # Limite inférieure pour l'early-stop (eviter de parcourir toute l'histoire)
    fetch_start = start_ts - extra_lookback_days * 86400

    # 1. Récupérer les contacts du compte
    contacts, page_token = [], None
    while len(contacts) < 200:
        params = {"limit": 100}
        if page_token:
            params["page_token"] = page_token
        rc = requests.get(f"{base}/accounts/{account_id}/contacts",
                          headers=headers, params=params, timeout=20)
        if rc.status_code == 429:
            _time.sleep(int(rc.headers.get("Retry-After", "10")))
            continue
        rc.raise_for_status()
        data  = rc.json()
        items = data.get("_results", [])
        if not items:
            break
        contacts.extend(items)
        nxt = data.get("_pagination", {}).get("next", "")
        if not nxt or "page_token=" not in nxt:
            break
        page_token = nxt.split("page_token=")[-1].split("&")[0]

    print(f"  👥 {len(contacts)} contacts pour account {account_id}", flush=True)

    # 2. Récupérer les conversations de chaque contact en parallèle (max 5 workers)
    def _get_contact_convs(cid):
        result, cpage, done = [], None, False
        while not done:
            params = {"limit": 100}
            if cpage:
                params["page_token"] = cpage
            try:
                rv = requests.get(f"{base}/contacts/{cid}/conversations",
                                  headers=headers, params=params, timeout=20)
                if rv.status_code == 429:
                    _time.sleep(int(rv.headers.get("Retry-After", "10")))
                    continue
                if rv.status_code in (404, 403):
                    break
                rv.raise_for_status()
            except Exception:
                break
            data  = rv.json()
            items = data.get("_results", [])
            if not items:
                break
            for c in items:
                created_ts = c.get("created_at") or 0
                # active_ts : timestamp représentatif de l'activité récente
                active_ts  = (c.get("updated_at") or created_ts) if use_updated_at else created_ts
                # Early stop sur created_at (borné par fetch_start)
                if created_ts and created_ts < fetch_start:
                    done = True
                    break
                # Inclusion : actif pendant [start_ts, end_ts]
                if active_ts and start_ts <= active_ts <= end_ts:
                    result.append(c)
            nxt = data.get("_pagination", {}).get("next", "")
            if not nxt or "page_token=" not in nxt:
                break
            cpage = nxt.split("page_token=")[-1].split("&")[0]
        return result

    from concurrent.futures import ThreadPoolExecutor as _TPE
    all_conv_ids, convs = set(), []
    contact_ids = [c.get("id") for c in contacts if c.get("id")]
    with _TPE(max_workers=5) as ex:
        futures = {ex.submit(_get_contact_convs, cid): cid for cid in contact_ids}
        for fut in futures:
            for c in fut.result():
                if c.get("id") not in all_conv_ids:
                    convs.append(c)
                    all_conv_ids.add(c.get("id"))

    return convs


def fetch_front_csat_by_account(cfg, account_name, date_start, date_end):
    """Récupère CSAT via l'API HBO /front_csat (account_name + message_date).

    Plus fiable que la détection via tags Front : source directe.
    Retourne : {"convs": [], "csat": {...}, "account": {"name": account_name}}
    """
    csat = fetch_hbo_csat(cfg, account_name, date_start, date_end)
    return {
        "convs":   [],
        "csat":    csat,
        "account": {"name": account_name},
    }


# ── Front : comptage emails (messages) par copropriété ────────────

def find_front_tag_by_account_name(cfg, account_name):
    """Trouve le tag Front correspondant à un compte (copropriété) par son nom.

    Les tags Homeland ont la forme : '{account_name} - {building_id}'
    ex. 'SDC 92300 18 RUE GREFFULHE - 5 RUE JEAN GABIN - 671'
    """
    try:
        all_tags = _get_all_front_tags(cfg)
    except Exception:
        return None
    name_up = account_name.upper().strip()

    # Correspondance exacte ou prefix (tag = account_name + ' - NNN')
    for t in all_tags:
        tag_up = (t.get("name") or "").upper().strip()
        if tag_up == name_up or tag_up.startswith(name_up + " -"):
            return t

    # Fallback : tous les mots significatifs présents dans le tag
    words = [w for w in name_up.split() if len(w) > 3]
    if words:
        for t in all_tags:
            tag_up = (t.get("name") or "").upper()
            if all(w in tag_up for w in words):
                return t
    return None


def _count_messages_in_conv(base, headers, conv_id, start_ts, end_ts):
    """Compte les messages (inbound/outbound) dans [start_ts, end_ts] pour une conversation.

    Messages Front triés du plus récent au plus ancien.
    Bug corrigé : l'ancienne version s'arrêtait si found_any=False (pages de messages
    plus récents que end_ts faisaient couper la pagination prématurément).
    """
    import time as _time
    sent, received = 0, 0
    page_token = None
    while True:
        params = {"limit": 100}
        if page_token:
            params["page_token"] = page_token
        try:
            r = requests.get(f"{base}/conversations/{conv_id}/messages",
                             headers=headers, params=params, timeout=20)
            if r.status_code == 429:
                _time.sleep(int(r.headers.get("Retry-After", "10")))
                continue
            if not r.ok:
                break
        except Exception:
            break
        data  = r.json()
        items = data.get("_results", [])
        if not items:
            break
        hit_old = False
        for m in items:
            msg_ts = m.get("created_at") or 0
            if msg_ts and msg_ts < start_ts:
                # Message plus vieux que la fenêtre → arrêt (les suivants sont encore plus vieux)
                hit_old = True
                break
            if msg_ts and msg_ts <= end_ts:
                if m.get("is_inbound"):
                    received += 1
                else:
                    sent += 1
            # msg_ts > end_ts → trop récent, on passe (ne pas arrêter la pagination)
        if hit_old:
            break
        nxt = data.get("_pagination", {}).get("next", "")
        if not nxt or "page_token=" not in nxt:
            break
        page_token = nxt.split("page_token=")[-1].split("&")[0]
    return sent, received


def fetch_front_email_count_by_account(cfg, account_name, date_start, date_end):
    """Compte les emails échangés pour une copropriété sur une période.

    Méthode :
      1. Conversations portant le tag de la copropriété (créées ou mises à jour pendant la période)
      2. Conversations via contacts du compte Front (sans le tag, ou pour compléter)
      Union dédupliquée → pour chaque conv, compte messages [start, end] inbound/outbound.

    Retourne dict avec sent, received, total, convs_counted, tag_name, account_matched.
    """
    if not cfg.get("front"):
        return {"error": "Front non configuré"}
    import time as _time
    from concurrent.futures import ThreadPoolExecutor as _TPE

    base, token = cfg["front"]["base_url"], cfg["front"]["token"]
    headers  = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    start_ts = int(datetime.strptime(date_start, "%Y-%m-%d").timestamp())
    end_ts   = int(datetime.strptime(date_end,   "%Y-%m-%d").timestamp()) + 86399

    # ── 1. Trouver le compte ──────────────────────────────────────────────────
    all_accounts = _get_all_front_accounts(cfg)
    name_up  = account_name.upper().strip()
    account  = None
    for acc in all_accounts:
        if (acc.get("name") or "").upper().strip() == name_up:
            account = acc; break
    if not account:
        words = [w for w in name_up.split() if len(w) > 3]
        for acc in all_accounts:
            if words and all(w in (acc.get("name") or "").upper() for w in words):
                account = acc; break
    if not account:
        return {"error": f"Compte non trouvé : {account_name}"}

    account_id   = account["id"]
    account_name_matched = account.get("name", account_name)

    # ── 2. Conversations via le tag copropriété ───────────────────────────────
    tag = find_front_tag_by_account_name(cfg, account_name_matched)
    tag_convs = []
    tag_name  = None
    if tag:
        tag_name = tag.get("name")
        tag_id   = tag["id"]
        # front_convs_for_tag filtre sur created_at — on étend de 7j pour mails en cours
        raw_tag_convs = front_convs_for_tag(cfg, tag_id,
            (datetime.fromtimestamp(start_ts - 7*86400)).strftime("%Y-%m-%d"), date_end)
        for c in raw_tag_convs:
            # Garder si un message existe dans la fenêtre (on vérifie plus loin)
            tag_convs.append(c)
        print(f"  📌 Email count: {len(tag_convs)} convs via tag '{tag_name}'", flush=True)

    # ── 3. Conversations via contacts du compte ───────────────────────────────
    account_convs = _fetch_account_convs_via_contacts(
        base, headers, account_id, start_ts, end_ts,
        use_updated_at=False, extra_lookback_days=7
    )
    print(f"  📌 Email count: {len(account_convs)} convs via account contacts", flush=True)

    # ── 4. Dédupliquer ────────────────────────────────────────────────────────
    seen_ids, all_convs = set(), []
    for c in tag_convs + account_convs:
        cid = c.get("id")
        if cid and cid not in seen_ids:
            seen_ids.add(cid)
            all_convs.append(c)
    print(f"  📌 Email count: {len(all_convs)} convs uniques au total", flush=True)

    # ── 5. Compter messages pour chaque conv (en parallèle, max 8 workers) ────
    def _count_conv(c):
        cid  = c.get("id")
        s, r = _count_messages_in_conv(base, headers, cid, start_ts, end_ts)
        return {"id": cid, "subject": (c.get("subject") or "")[:60], "sent": s, "received": r}

    per_conv = []
    with _TPE(max_workers=8) as ex:
        futs = [ex.submit(_count_conv, c) for c in all_convs]
        for fut in futs:
            per_conv.append(fut.result())

    per_conv = [p for p in per_conv if p["sent"] + p["received"] > 0]
    total_sent     = sum(p["sent"]     for p in per_conv)
    total_received = sum(p["received"] for p in per_conv)

    return {
        "account_matched": account_name_matched,
        "tag_name":        tag_name,
        "date_range":      f"{date_start} → {date_end}",
        "sent":            total_sent,
        "received":        total_received,
        "total":           total_sent + total_received,
        "convs_with_msgs": len(per_conv),
        "convs_total":     len(all_convs),
        "per_conv":        per_conv,
    }


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

        inactifs = sum(1 for p in results if is_closed(p))
        actifs   = len(results) - inactifs
        print(f"  ✅ Projets {bid}: {actifs} actifs / {inactifs} inactifs (total={len(results)})")
        # Log un sample pour diagnostique
        for p in results[:3]:
            print(f"    sample: status={p.get('status')!r} active={p.get('active')!r} closed={p.get('closed')!r} type={p.get('type')!r}")

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
# Tous les types connus — les inconnus sont affichés tels quels
ASSEMBLY_TYPE_MAP = {
    1:  "Conseil syndical",
    2:  "Visite",
    3:  "Réunion",
    4:  "Visite technique",
    5:  "Conseil syndical",
    6:  "Visite de l'immeuble",
    7:  "Réunion de copropriétaires",
    8:  "AGO",    # Assemblée Générale Ordinaire
    9:  "Réunion préparatoire",
    10: "Réunion",
    11: "AGE",    # Assemblée Générale Extraordinaire
    12: "Visite technique",
    13: "Réunion de travail",
}

def process_assemblies_v3(raw):
    """
    Convertit TOUTES les assemblées/visites HBO en liste.
    Structure HBO : meeting_date = {"date": "2024-03-21 18:00:00", ...}
                    type_id = entier (8=AGO, 11=AGE, autres types possibles)
                    status  = "done" | "planned" | ...
    Tous les type_ids sont inclus — pas de filtre.
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
        # Label : mapping connu → champ type HBO → "Assemblée" par défaut
        t = (ASSEMBLY_TYPE_MAP.get(type_id)
             or str(a.get("type") or a.get("type_name") or "").strip()
             or "Assemblée")

        # Nettoyer la description HTML si présente
        desc = a.get("description") or a.get("title") or a.get("name") or ""
        desc_clean = _re2.sub(r'<[^>]+>', '', str(desc)).strip() if desc else ""
        name = desc_clean or f"{t} {dt[:4]}".strip() if dt else t

        result.append({
            "name":   name,
            "type":   t,
            "date":   dt,
            "status": "Tenu" if a.get("status") == "done" else "Planifié",
        })
    return sorted(result, key=lambda x: x["date"] or "0000", reverse=True)

def process_visits_v3(events_raw, admin_id_map=None, date_start=None, date_end=None):
    """Traite les building_events HBO pour la page Visites & Assemblées.

    Champs utilisés :
      - meeting_type  → Type de visite (colonne 1)
      - event_date    → Date (colonne 2)
      - added_by      → id admin → résolu en 'Prénom Nom' (colonne 3)

    Fallback si meeting_type/event_date absent : utilise les champs /assemblies
    (date, type) pour compatibilité avec l'ancien endpoint.
    """
    admin_id_map = admin_id_map or {}
    result = []
    for e in events_raw:
        # ── Date ──────────────────────────────────────────────────────
        # Essai dans l'ordre : event_date (Buildings_events), meeting_date, date (/assemblies)
        dt = e.get("event_date") or e.get("meeting_date") or e.get("date") or ""
        if isinstance(dt, dict):                # meeting_date = {"date": "2024-..."} (ancien format)
            dt = dt.get("date") or ""
        dt = str(dt or "")[:10]
        if dt == "1970-01-01":
            dt = ""

        # ── Filtre période ─────────────────────────────────────────────
        if date_start and date_end and dt:
            if dt < date_start or dt > date_end:
                continue

        # ── Type de visite ─────────────────────────────────────────────
        meeting_type = str(e.get("meeting_type") or e.get("type") or "").strip()
        if not meeting_type:
            type_id = e.get("type_id")
            meeting_type = ASSEMBLY_TYPE_MAP.get(type_id, "Assemblée") if type_id else "Assemblée"

        # ── Intervenant : added_by → nom admin ────────────────────────
        added_by = e.get("added_by")
        intervenant = ""
        if added_by is not None:
            intervenant = admin_id_map.get(added_by) or admin_id_map.get(str(added_by)) or ""

        result.append({
            "type":        meeting_type,
            "date":        dt,
            "intervenant": intervenant,
        })
    return sorted(result, key=lambda x: x["date"] or "0000", reverse=True)

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

        # Log tous les champs du bâtiment pour diagnostic
        print(f"  🏢 Building {bid} keys: {list(b.keys())}", flush=True)
        for k, v in b.items():
            if any(w in k.lower() for w in ["referent", "accountant", "assistant", "user", "admin",
                                             "manager", "lot", "copro"]):
                print(f"    → {k}: {v}", flush=True)

        def _field_id(obj):
            """Extrait l'ID depuis un objet ou un entier."""
            if not obj: return None
            if isinstance(obj, int): return obj
            if isinstance(obj, dict): return obj.get("id")
            return None

        # Priorité : champs confirmés par l'utilisateur (referent, accountant, assistant)
        manager_id = (
            _field_id(b.get("referent")) or
            b.get("referentAdminUserId") or b.get("referent_admin_user_id") or
            b.get("managerId") or b.get("manager_id")
        )
        accountant_id = (
            _field_id(b.get("accountant")) or
            b.get("accountantAdminUserId") or b.get("accountant_admin_user_id") or
            b.get("accountantId") or b.get("accountant_id")
        )
        assistant_id = (
            _field_id(b.get("assistant")) or
            b.get("assistantId") or b.get("assistant_id")
        )
        print(f"  👤 referent_id={manager_id}, accountant_id={accountant_id}, assistant_id={assistant_id}")

        def admin_user_name(user_id):
            if not user_id: return ""
            try:
                u = hbo(cfg, f"/admin_users/{user_id}")
                if not u: return ""
                fname = u.get("firstname") or u.get("firstName") or u.get("first_name") or ""
                lname = u.get("name") or u.get("lastName") or u.get("last_name") or ""
                return f"{fname} {lname}".strip() or u.get("email", "")
            except Exception: return ""

        def _fetch_building_events(bid_):
            """Essai /building/{bid}/events, fallback /assemblies."""
            ev = list_items(hbo(cfg, f"/building/{bid_}/events"))
            if not ev:
                ev = list_items(hbo(cfg, "/assemblies", {"building_id": bid_}))
            return ev

        def _fetch_parcels(bid_):
            """Essaie /building_parcels/{bid} pour récupérer nbLotsMain."""
            data = hbo(cfg, f"/building_parcels/{bid_}")
            if data is None:
                return {}
            if isinstance(data, list) and data:
                return data[0] if isinstance(data[0], dict) else {}
            return data if isinstance(data, dict) else {}

        # Tout en parallèle : HBO + Ringover + Front + admin maps
        with ThreadPoolExecutor(max_workers=15) as ex:
            f_mgr       = ex.submit(admin_user_name, manager_id)
            f_acct      = ex.submit(admin_user_name, accountant_id)
            f_assist    = ex.submit(admin_user_name, assistant_id)
            f_works     = ex.submit(lambda: list_items(hbo(cfg, f"/building/works/{bid}")))
            f_projs     = ex.submit(fetch_projects_hbo, cfg, bid)
            f_events    = ex.submit(_fetch_building_events, bid)
            f_incs      = ex.submit(lambda: list_items(hbo(cfg, "/incidents",  {"building_id": bid})))
            f_calls     = ex.submit(ringover_calls, cfg, date_start, date_end)
            f_front     = ex.submit(fetch_front_for_building, cfg, bid, b_name, date_start, date_end)
            f_admins    = ex.submit(get_admin_users_map, cfg)
            f_admin_ids = ex.submit(get_admin_users_id_map, cfg)
            f_copro     = ex.submit(lambda: hbo(cfg, f"/copropriete/{bid}") or {})
            f_parcels   = ex.submit(_fetch_parcels, bid)

            manager_name    = f_mgr.result()
            accountant_name = f_acct.result()
            assistant_name  = f_assist.result()
            works       = f_works.result()   # réservé pour usage futur
            projs       = f_projs.result() or []   # ne jamais mélanger avec works
            events      = f_events.result()
            incs        = f_incs.result()
            all_calls   = f_calls.result()
            front_data  = f_front.result()   # {"convs": [...], "csat": {...}}
            admin_map   = f_admins.result()
            admin_id_map= f_admin_ids.result()
            copro       = f_copro.result()
            parcels     = f_parcels.result()   # /building_parcels/{bid} → nbLotsMain ?
            print(f"  🏢 Copropriete {bid} keys: {list(copro.keys())}", flush=True)
            print(f"  📦 Parcels {bid} keys: {list(parcels.keys()) if isinstance(parcels, dict) else type(parcels).__name__}", flush=True)
            print(f"  📅 Events {bid}: {len(events)} entrées", flush=True)
            for k, v in copro.items():
                if any(w in k.lower() for w in ["lot", "ppx", "copro"]):
                    print(f"    → copro.{k}: {v}", flush=True)

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

        # Fallbacks : si l'ID ne donnait pas de résultat, essayer le champ objet directement
        if not manager_name:
            for field in ["referent", "manager", "gestionnaire", "responsable",
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

        if not assistant_name:
            for field in ["assistant", "assistantAdminUser", "assistant_admin_user"]:
                val = b.get(field)
                if val:
                    assistant_name = _extract_name(val)
                    if assistant_name: break

        print(f"  👤 manager='{manager_name}', accountant='{accountant_name}', assistant='{assistant_name}'")

        created_at = (b.get("createdAt") or b.get("created_at") or
                      b.get("managedSince") or b.get("dateCreation") or "")
        if created_at: created_at = created_at[:10]

        # Lots : /building_parcels/{bid} → nbLotsMain prioritaire (confirmé via session web)
        # Fallback : /copropriete/{bid} → copropriété_lotsppx
        # Fallback final : /building/{bid} (en général absent de l'API JWT)
        lots_count = (
            # /building_parcels (essai JWT)
            (parcels.get("nbLotsMain") or parcels.get("nbLotsTotal")
             or parcels.get("nb_lots_main") or parcels.get("lotsMain"))
            # /copropriete
            or copro.get("copropriété_lotsppx") or copro.get("copropriete_lotsppx")
            or copro.get("lotsppx") or copro.get("lots_ppx") or copro.get("lots")
            or copro.get("nbLotsMain") or copro.get("nbLotsTotal")
            # /building (en général vide côté JWT)
            or b.get("nbLotsMain") or b.get("nbLotsTotal")
            or b.get("copropriété_lotsppx") or b.get("copropriete_lotsppx")
            or b.get("lotsppx") or b.get("lots") or b.get("lotsCount")
            or b.get("lotsPrincipaux") or b.get("numberOfLots") or b.get("nb_lots") or 0
        )
        print(f"  🏢 lots_count={lots_count} (parcels={list(parcels.keys())[:5]}, copro_keys={list(copro.keys())[:10]})", flush=True)

        result = {
            "building": {
                "name":       b.get("name") or b.get("address") or f"#{bid}",
                "address":    b.get("address") or "",
                "city":       b.get("city") or "",
                "lots":       lots_count,
                "manager":    manager_name,
                "accountant": accountant_name,
                "assistant":  assistant_name,
                "created_at": created_at,
            },
            # CSAT depuis les conversations Front du bâtiment
            "csat":      front_data.get("csat", {}),
            # Projets depuis HBO /projects/{bid} + détails
            "projects":  to_projects_list(projs, date_start, date_end),
            # Incidents depuis HBO /incidents?building_id={bid}
            "incidents": to_incidents_list(incs),
            # Appels Ringover : service déduit via email interne → profil HBO
            "calls":     process_calls_v3(all_calls, admin_map),
            # Emails/CSAT Front filtrés par tag du bâtiment
            "emails":    process_emails_v3(front_data.get("convs", [])),
            # Visites & Assemblées (building_events + fallback assemblies)
            "visits":    process_visits_v3(events, admin_id_map, date_start, date_end),
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
            {"name": "Audit énergétique DPE collectif",     "category": "GESTION",   "status": "Clos",   "start_date": "2023-11-01"},
            {"name": "Mise en conformité cage d'escalier",  "category": "TRAVAUX",   "status": "Clos",   "start_date": "2023-09-01"},
            {"name": "Installation boîtes aux lettres",     "category": "GESTION",   "status": "Clos",   "start_date": "2024-01-10"},
            {"name": "Litige étanchéité terrasse lot 12",   "category": "LITIGES",   "status": "En cours",  "start_date": "2024-04-01"},
        ],
        "incidents": [
            {"name": "Dégât des eaux appartement 3B",        "category": "SINISTRES",  "status": "Clos", "date": "2024-02-14"},
            {"name": "Infiltration toiture — cage B",         "category": "SINISTRES",  "status": "En cours","date": "2024-08-05"},
            {"name": "Fissures parking sous-sol",             "category": "SINISTRES",  "status": "En cours","date": "2024-09-22"},
            {"name": "Ravalement de façade côté boulevard",   "category": "TRAVAUX",    "status": "En cours","date": "2024-03-01"},
            {"name": "Remplacement canalisations eaux usées", "category": "TRAVAUX",    "status": "Clos", "date": "2023-12-10"},
            {"name": "Mutation lot 7 — acte signé",           "category": "MUTATIONS",  "status": "Clos", "date": "2024-05-30"},
            {"name": "Mutation lot 19 — en cours",            "category": "MUTATIONS",  "status": "En cours","date": "2024-11-03"},
            {"name": "Contentieux charges impayées lot 24",   "category": "LITIGES",    "status": "En cours","date": "2024-04-15"},
            {"name": "Réparation portail électrique",         "category": "GESTION",    "status": "Clos", "date": "2024-07-08"},
            {"name": "Remplacement visiophone",               "category": "GESTION",    "status": "Clos", "date": "2024-09-14"},
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

@app.route("/api/debug/find_building")
def debug_find_building():
    """Recherche un bâtiment par nom dans la liste scannée ou via /building/{id}."""
    name = (request.args.get("name") or "").lower().strip()
    if not name:
        return jsonify({"error": "param ?name= requis"}), 400
    try:
        cfg = load_config()
        # Chercher dans le cache bâtiments
        cached = _buildings_cache.get("data") or []
        matches = [b for b in cached if name in b.get("name", "").lower()]
        return jsonify({"query": name, "matches": matches, "total_cached": len(cached)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/debug/building/<int:bid>")
def debug_building(bid):
    """Retourne tous les champs bruts d'un bâtiment HBO."""
    try:
        cfg = load_config()
        b = hbo(cfg, f"/building/{bid}") or {}
        return jsonify({"building_id": bid, "keys": list(b.keys()), "data": b})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/debug/project/<int:bid>")
def debug_project(bid):
    """Retourne les champs bruts d'un projet HBO pour diagnostiquer status/type."""
    try:
        cfg = load_config()
        # Récupère la liste des IDs
        raw_ids = list_items(hbo(cfg, f"/projects/{bid}"))
        ids = []
        for p in raw_ids:
            if isinstance(p, int): ids.append(p)
            elif isinstance(p, dict) and p.get("id"): ids.append(int(p["id"]))
        # Prend les 5 premiers pour inspecter
        samples = []
        for pid in ids[:10]:
            proj = hbo(cfg, f"/project/{pid}")
            if proj:
                samples.append({
                    "id": pid,
                    "status": proj.get("status"),
                    "state":  proj.get("state"),
                    "type":   proj.get("type"),
                    "active": proj.get("active"),
                    "closed": proj.get("closed"),
                    "title":  (proj.get("description") or proj.get("title") or "")[:50],
                    "all_keys": list(proj.keys()),
                })
        return jsonify({"building_id": bid, "total_ids": len(ids), "samples": samples})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/debug/front_tags")
def debug_front_tags():
    """Liste tous les tags Front avec pagination (pour diagnostiquer CSAT manquant)."""
    try:
        cfg = load_config()
        if not cfg.get("front"):
            return jsonify({"error": "Front non configuré"})
        all_tags = _get_all_front_tags(cfg)
        # Filtrer pour ne montrer que les tags de bâtiments (pas "Inbox")
        bldg_tags = [t for t in all_tags if t.get("name","") not in ("Inbox","Starred","")]
        return jsonify({
            "total_all": len(all_tags),
            "total_building_tags": len(bldg_tags),
            "sample_building": [{"id": t.get("id"), "name": t.get("name")} for t in bldg_tags[:30]],
            "all_building_names": [t.get("name","") for t in bldg_tags[:200]],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/debug/copropriete/<int:bid>")
def debug_copropriete(bid):
    """Sonde plusieurs variantes de chemin HBO + immat pour trouver les données copropriété (lots)."""
    try:
        cfg = load_config()
        # Récupère l'immat du bâtiment (ex: "AE1511005")
        b = hbo(cfg, f"/building/{bid}") or {}
        immat = b.get("immat") or ""

        # Tous les chemins à tester
        candidates = [
            (f"/copropriete/{bid}", None),
            (f"/coproprietes/{bid}", None),
            (f"/building/{bid}/copropriete", None),
            (f"/syndic/copropriete/{bid}", None),
            (f"/coproprietes", {"building_id": bid}),
            (f"/coproprietes", {"id": bid}),
        ]
        if immat:
            candidates += [
                (f"/copropriete/{immat}", None),
                (f"/coproprietes/{immat}", None),
                (f"/coproprietes", {"immat": immat}),
                (f"/coproprietes", {"immatriculation": immat}),
            ]

        results = {}
        for p, params in candidates:
            key = f"{p}?{params}" if params else p
            raw = hbo(cfg, p, params=params)
            if raw is not None:
                data = raw if isinstance(raw, dict) else {}
                items = list_items(raw) if isinstance(raw, (list, dict)) else []
                lot_fields = {k: v for k, v in data.items() if any(w in k.lower() for w in ["lot", "ppx"])}
                # If it's a list, scan items for lot fields too
                if items:
                    for item in items[:3]:
                        if isinstance(item, dict):
                            for k, v in item.items():
                                if any(w in k.lower() for w in ["lot", "ppx", "copro"]):
                                    lot_fields[f"item.{k}"] = v
                results[key] = {
                    "type": type(raw).__name__,
                    "keys": list(data.keys())[:20] if data else None,
                    "lot_fields": lot_fields,
                    "items_count": len(items),
                    "first_item_keys": list(items[0].keys())[:20] if items and isinstance(items[0], dict) else None,
                }
            else:
                results[key] = None
        return jsonify({"building_id": bid, "immat": immat, "paths_tried": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/debug/building_full/<int:bid>")
def debug_building_full(bid):
    """Retourne TOUS les champs bruts du bâtiment HBO + multiples essais pour trouver lots."""
    try:
        cfg = load_config()
        base = cfg["hbo"]["base_url"]
        tok = hbo_token(cfg)
        headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}

        # 1. Tous les champs bruts du bâtiment (valeurs incluses)
        b_raw = hbo(cfg, f"/building/{bid}") or {}
        immat = b_raw.get("immat", "")
        syndic_id = b_raw.get("syndic_id")

        # 2. POST /building/search avec immat (retourne-t-il plus de champs ?)
        search_result = None
        try:
            r = requests.post(f"{base}/building/search", headers=headers,
                              json={"immat": immat} if immat else {"id": bid}, timeout=15)
            if r.status_code == 200:
                items = list_items(r.json())
                search_result = {"count": len(items), "first_keys": list(items[0].keys())[:30] if items else None,
                                 "lot_fields": {k: v for item in items[:3] for k, v in item.items() if any(w in k.lower() for w in ["lot", "ppx", "copro", "unit"])}}
            else:
                search_result = {"status": r.status_code}
        except Exception as e:
            search_result = {"error": str(e)}

        # 3. Essais paths alternatifs lots
        lot_paths = [
            f"/building/{bid}/lots",
            f"/lots",
            f"/lots?building_id={bid}",
            f"/building/{bid}/copropriete",
        ]
        if syndic_id:
            lot_paths += [
                f"/syndic/{syndic_id}/coproprietes",
                f"/syndic/{syndic_id}/buildings/{bid}",
            ]
        lot_results = {}
        for p in lot_paths:
            r2 = hbo(cfg, p)
            if r2 is not None:
                items2 = list_items(r2)
                lot_results[p] = {
                    "type": type(r2).__name__,
                    "keys": list(r2.keys())[:20] if isinstance(r2, dict) else None,
                    "items_count": len(items2),
                    "lot_fields": {k: v for k, v in (r2.items() if isinstance(r2, dict) else {}.items()) if any(w in k.lower() for w in ["lot", "ppx"])}
                }
            else:
                lot_results[p] = None

        return jsonify({
            "building_id": bid,
            "building_raw": b_raw,
            "building_lot_fields": {k: v for k, v in b_raw.items() if any(w in k.lower() for w in ["lot", "ppx", "copro", "unit"])},
            "search_result": search_result,
            "lot_paths": lot_results,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/debug/front_tag/<int:bid>")
def debug_front_tag(bid):
    """Vérifie si un tag Front est trouvé pour ce bâtiment."""
    try:
        cfg = load_config()
        if not cfg.get("front"):
            return jsonify({"error": "Front non configuré"})
        b = hbo(cfg, f"/building/{bid}") or {}
        b_name = b.get("name", "")
        tag = find_front_tag(cfg, bid, b_name)
        cache_size = len(_front_tags_cache.get("data") or [])
        return jsonify({
            "building_id": bid,
            "building_name": b_name,
            "tag_found": tag is not None,
            "tag": {"id": tag.get("id"), "name": tag.get("name")} if tag else None,
            "cache_size": cache_size,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/debug/admin_user/<int:uid>")
def debug_admin_user(uid):
    """Retourne les champs bruts d'un admin user HBO."""
    try:
        cfg = load_config()
        u = hbo(cfg, f"/admin_users/{uid}") or {}
        return jsonify({"user_id": uid, "keys": list(u.keys()), "data": u})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/debug/front_convs/<int:bid>")
def debug_front_convs(bid):
    """Retourne les 3 premières conversations Front d'un bâtiment pour diagnostiquer CSAT."""
    try:
        cfg = load_config()
        if not cfg.get("front"):
            return jsonify({"error": "Front non configuré"})
        b = hbo(cfg, f"/building/{bid}") or {}
        b_name = b.get("name","")
        tag = find_front_tag(cfg, bid, b_name)
        if not tag:
            return jsonify({"error": f"Aucun tag Front pour building {bid}", "building_name": b_name})
        base, token = cfg["front"]["base_url"], cfg["front"]["token"]
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        r = requests.get(f"{base}/tags/{tag['id']}/conversations", headers=headers,
                         params={"limit": 3}, timeout=15)
        r.raise_for_status()
        data = r.json()
        convs = data.get("_results", [])
        # Montrer tous les champs metadata/satisfaction pour comprendre CSAT
        samples = []
        for c in convs:
            samples.append({
                "id": c.get("id"),
                "subject": c.get("subject","")[:60],
                "metadata": c.get("metadata"),
                "custom_fields": c.get("custom_fields"),
                "all_keys": list(c.keys()),
            })
        return jsonify({"tag": tag["name"], "total_convs_page": len(convs), "samples": samples})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/debug/csat/<int:bid>")
def debug_csat(bid):
    """Diagnostic CSAT détaillé : tente conv detail + ratings endpoint."""
    try:
        cfg = load_config()
        if not cfg.get("front"):
            return jsonify({"error": "Front non configuré"})
        b = hbo(cfg, f"/building/{bid}") or {}
        b_name = b.get("name", "")
        tag = find_front_tag(cfg, bid, b_name)
        if not tag:
            return jsonify({"error": f"Aucun tag Front pour building {bid}"})
        base, token = cfg["front"]["base_url"], cfg["front"]["token"]
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        # 1. Conversations de la dernière semaine (pour trouver celles avec CSAT)
        r1 = requests.get(f"{base}/tags/{tag['id']}/conversations",
                          headers=headers, params={"limit": 5}, timeout=15)
        r1.raise_for_status()
        convs = r1.json().get("_results", [])

        # 2. Détail d'une conversation (plus de champs que la liste)
        conv_detail = None
        if convs:
            cid = convs[0]["id"]
            try:
                r2 = requests.get(f"{base}/conversations/{cid}", headers=headers, timeout=15)
                r2.raise_for_status()
                conv_detail = r2.json()
            except Exception as e:
                conv_detail = {"error": str(e)}

        # 3. Essai endpoint /ratings (CSAT surveys Front)
        ratings_result = None
        try:
            r3 = requests.get(f"{base}/ratings", headers=headers,
                              params={"limit": 10}, timeout=15)
            ratings_result = {"status": r3.status_code, "data": r3.json() if r3.ok else r3.text[:200]}
        except Exception as e:
            ratings_result = {"error": str(e)}

        # 4. Scan les conversations pour trouver celles avec metadata.satisfaction non vide
        rated = [c for c in convs if c.get("metadata", {}).get("satisfaction")]
        return jsonify({
            "tag": tag["name"],
            "total_convs_sample": len(convs),
            "convs_with_satisfaction": len(rated),
            "conv_detail_keys": list(conv_detail.keys()) if isinstance(conv_detail, dict) and "error" not in conv_detail else conv_detail,
            "conv_detail_metadata": conv_detail.get("metadata") if isinstance(conv_detail, dict) else None,
            "ratings_endpoint": ratings_result,
            "sample_conv_metadata": [c.get("metadata") for c in convs[:3]],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/debug/front_accounts")
def debug_front_accounts():
    """Explore tous les comptes Front disponibles.
    Params : ?q=ANATOLE (filtre côté serveur, insensible à la casse)
             ?limit=50  (max comptes à retourner, défaut 50)
    Pagine toutes les pages Front et filtre localement.
    """
    q        = (request.args.get("q") or "").strip().lower()
    max_ret  = int(request.args.get("limit") or 50)
    try:
        cfg = load_config()
        if not cfg.get("front"):
            return jsonify({"error": "Front non configuré"})
        base, token = cfg["front"]["base_url"], cfg["front"]["token"]
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        all_accounts, page_token, pages = [], None, 0
        while len(all_accounts) < 5000:  # sécurité
            params = {"limit": 100}
            if page_token:
                params["page_token"] = page_token
            r = requests.get(f"{base}/accounts", headers=headers, params=params, timeout=20)
            r.raise_for_status()
            data  = r.json()
            items = data.get("_results", [])
            if not items:
                break
            all_accounts.extend(items)
            pages += 1
            nxt = data.get("_pagination", {}).get("next", "")
            if not nxt or "page_token=" not in nxt:
                break
            page_token = nxt.split("page_token=")[-1].split("&")[0]

        # Filtrage local
        if q:
            filtered = [a for a in all_accounts if q in (a.get("name") or "").lower()]
        else:
            filtered = all_accounts

        return jsonify({
            "total_accounts":   len(all_accounts),
            "pages_fetched":    pages,
            "filter_q":         q or "(none)",
            "matched":          len(filtered),
            "results":          [{"id": a.get("id"), "name": a.get("name")}
                                 for a in filtered[:max_ret]],
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/debug/front_account_contacts/<account_id>")
def debug_front_account_contacts(account_id):
    """Contacts d'un compte Front + premières conversations de chaque contact."""
    try:
        cfg = load_config()
        if not cfg.get("front"):
            return jsonify({"error": "Front non configuré"})
        base, token = cfg["front"]["base_url"], cfg["front"]["token"]
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        # Contacts du compte
        rc = requests.get(f"{base}/accounts/{account_id}/contacts",
                          headers=headers, params={"limit": 10}, timeout=20)
        contacts_status = rc.status_code
        contacts = rc.json().get("_results", []) if rc.ok else []

        # Premières conversations du 1er contact (pour voir la structure CSAT)
        conv_samples = []
        if contacts:
            first_cid = contacts[0].get("id")
            rv = requests.get(f"{base}/contacts/{first_cid}/conversations",
                              headers=headers, params={"limit": 5}, timeout=20)
            if rv.ok:
                for c in rv.json().get("_results", []):
                    sat = (c.get("metadata") or {}).get("satisfaction") or {}
                    conv_samples.append({
                        "id": c.get("id"),
                        "subject": (c.get("subject") or "")[:60],
                        "created_at": c.get("created_at"),
                        "satisfaction": sat,
                        "survey_rating": sat.get("survey_rating"),
                    })

        return jsonify({
            "account_id":       account_id,
            "contacts_status":  contacts_status,
            "contacts_count":   len(contacts),
            "contacts":         [{"id": c.get("id"), "name": c.get("name")} for c in contacts],
            "conv_samples":     conv_samples,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/debug/front_account_convs/<account_id>")
def debug_front_account_convs(account_id):
    """Conversations d'un compte Front avec leur metadata CSAT.
    Params : ?limit=10 (défaut 10 premières convs)
    """
    limit = int(request.args.get("limit") or 10)
    try:
        cfg = load_config()
        if not cfg.get("front"):
            return jsonify({"error": "Front non configuré"})
        base, token = cfg["front"]["base_url"], cfg["front"]["token"]
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        r = requests.get(f"{base}/accounts/{account_id}/conversations",
                         headers=headers, params={"limit": limit}, timeout=20)
        r.raise_for_status()
        data  = r.json()
        convs = data.get("_results", [])

        # Pour chaque conv, récupérer les détails (metadata complète)
        samples = []
        for c in convs:
            meta = c.get("metadata") or {}
            sat  = meta.get("satisfaction") or {}
            samples.append({
                "id":            c.get("id"),
                "subject":       (c.get("subject") or "")[:80],
                "status":        c.get("status"),
                "created_at":    c.get("created_at"),
                "last_msg":      c.get("last_message_at"),
                "metadata":      meta,
                "satisfaction":  sat,
                "survey_rating": sat.get("survey_rating"),
                "all_keys":      list(c.keys()),
            })

        return jsonify({
            "account_id":   account_id,
            "total_in_page": len(convs),
            "pagination":   data.get("_pagination"),
            "conversations": samples,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/debug/front_conv/<conv_id>")
def debug_front_conv(conv_id):
    """Détails complets d'une conversation Front (tous les champs)."""
    try:
        cfg = load_config()
        if not cfg.get("front"):
            return jsonify({"error": "Front non configuré"})
        base, token = cfg["front"]["base_url"], cfg["front"]["token"]
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        r = requests.get(f"{base}/conversations/{conv_id}", headers=headers, timeout=15)
        r.raise_for_status()
        c = r.json()
        return jsonify({
            "id":          c.get("id"),
            "subject":     c.get("subject"),
            "status":      c.get("status"),
            "metadata":    c.get("metadata"),
            "custom_fields": c.get("custom_fields"),
            "created_at":  c.get("created_at"),
            "all_keys":    list(c.keys()),
            "full":        c,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/debug/front_csat_account")
def debug_front_csat_account():
    """Test CSAT Front par account_name (copropriété).

    Params GET :
      ?name=SDC+93600+...  (défaut: SDC 93600 98-100 AVENUE ANATOLE FRANCE)
      ?date_start=2026-03-02
      ?date_end=2026-03-08
    """
    account_name = request.args.get("name", "SDC 93600 98-100 AVENUE ANATOLE FRANCE")
    date_start   = request.args.get("date_start", "2026-03-02")
    date_end     = request.args.get("date_end",   "2026-03-08")
    try:
        cfg = load_config()
        if not cfg.get("front"):
            return jsonify({"error": "Front non configuré"})

        import re as _re
        _tag_re = _re.compile(r"rating\s*(\d+)\s*/\s*5", _re.IGNORECASE)

        result = fetch_front_csat_by_account(cfg, account_name, date_start, date_end)
        convs  = result["convs"]

        # Conversations avec satisfaction renseignée (via tag Rating X/5 ou metadata)
        def _conv_rating(c):
            for tn in [t.get("name","") for t in (c.get("tags") or [])]:
                m = _tag_re.search(tn)
                if m:
                    return int(m.group(1))
            sat = (c.get("metadata") or {}).get("satisfaction") or {}
            return sat.get("survey_rating") or sat.get("score") or sat.get("rating")

        rated = [c for c in convs if _conv_rating(c) is not None]
        # 10 premiers pour diagnostic
        samples = []
        for c in rated[:10]:
            rating = _conv_rating(c)
            tags_names = [t.get("name","") for t in (c.get("tags") or [])]
            samples.append({
                "id":          c.get("id"),
                "subject":     (c.get("subject") or "")[:80],
                "created_at":  c.get("created_at"),
                "rating":      rating,
                "tags":        tags_names,
                "survey_sent": any("survey sent" in tn.lower() for tn in tags_names),
            })

        # Aussi lister les 5 premiers comptes trouvés pour valider la correspondance
        base, token = cfg["front"]["base_url"], cfg["front"]["token"]
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        try:
            r_acc = requests.get(f"{base}/accounts", headers=headers,
                                 params={"q": account_name, "limit": 5}, timeout=15)
            r_acc.raise_for_status()
            candidates = [{"id": a.get("id"), "name": a.get("name")}
                          for a in r_acc.json().get("_results", [])]
        except Exception as ae:
            candidates = [{"error": str(ae)}]

        # Toutes les convs avec leur tag list (pour diagnostiquer la 2ème note manquante)
        all_tags_summary = []
        for c in convs:
            tag_names = [t.get("name","") for t in (c.get("tags") or [])]
            all_tags_summary.append({
                "id":       c.get("id"),
                "subject":  (c.get("subject") or "")[:60],
                "updated":  c.get("updated_at"),
                "rating":   _conv_rating(c),
                "tags":     tag_names,
            })

        return jsonify({
            "query_name":      account_name,
            "date_range":      f"{date_start} → {date_end}",
            "account_matched": result["account"],
            "account_candidates": candidates,
            "total_convs":     len(convs),
            "rated_convs":     len(rated),
            "survey_sent":     result["csat"].get("survey_sent", 0),
            "no_response":     result["csat"].get("no_response", 0),
            "csat":            result["csat"],
            "samples":         samples,
            "all_convs_tags":  all_tags_summary,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/debug/front_email_count")
def debug_front_email_count():
    """Compte les emails (sent + received) pour une copropriété sur une période.

    Params GET :
      ?name=SDC+92300+18+RUE+GREFFULHE+-+5+RUE+JEAN+GABIN
      ?date_start=2026-04-02
      ?date_end=2026-04-08
    """
    account_name = request.args.get("name", "SDC 93600 98-100 AVENUE ANATOLE FRANCE")
    date_start   = request.args.get("date_start", "2026-03-02")
    date_end     = request.args.get("date_end",   "2026-03-08")
    try:
        cfg = load_config()
        if not cfg.get("front"):
            return jsonify({"error": "Front non configuré"})
        result = fetch_front_email_count_by_account(cfg, account_name, date_start, date_end)
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/debug/front_conv_raw")
def debug_front_conv_raw():
    """Inspecte le JSON brut d'une conversation Front pour trouver les champs CSAT disponibles.

    Params GET :
      ?conv_id=cnv_1iltrkiq   (ID de la conversation à inspecter)

    Retourne l'objet complet de la conversation + les messages pour chercher le type 'survey'.
    """
    conv_id = request.args.get("conv_id", "cnv_1ilqop0i")  # défaut: conv avec Rating 1/5 connu
    try:
        cfg = load_config()
        if not cfg.get("front"):
            return jsonify({"error": "Front non configuré"})

        base, token = cfg["front"]["base_url"], cfg["front"]["token"]
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        # 1. Objet conversation complet
        r_conv = requests.get(f"{base}/conversations/{conv_id}", headers=headers, timeout=15)
        r_conv.raise_for_status()
        conv = r_conv.json()

        # Extraire tous les champs de premier niveau + metadata complète
        top_level_keys = list(conv.keys())
        metadata = conv.get("metadata") or {}
        satisfaction = metadata.get("satisfaction")

        # 2. Messages de la conversation (chercher type 'survey' ou similaire)
        r_msgs = requests.get(f"{base}/conversations/{conv_id}/messages",
                              headers=headers, params={"limit": 50}, timeout=15)
        r_msgs.raise_for_status()
        msgs_raw = r_msgs.json().get("_results", [])

        # Résumé des messages avec tous leurs champs de premier niveau
        msg_summary = []
        for m in msgs_raw:
            msg_summary.append({
                "id":          m.get("id"),
                "type":        m.get("type"),
                "created_at":  m.get("created_at"),
                "is_inbound":  m.get("is_inbound"),
                "body_preview": (m.get("body") or m.get("text") or "")[:120],
                "all_keys":    list(m.keys()),
                # Chercher champs CSAT potentiels
                "satisfaction": m.get("satisfaction"),
                "survey":       m.get("survey"),
                "rating":       m.get("rating"),
                "metadata":     m.get("metadata"),
            })

        return jsonify({
            "conv_id":         conv_id,
            "top_level_keys":  top_level_keys,
            "status":          conv.get("status"),
            "subject":         conv.get("subject"),
            "created_at":      conv.get("created_at"),
            "updated_at":      conv.get("updated_at"),
            "metadata":        metadata,
            "satisfaction_raw": satisfaction,
            "tags":            [t.get("name") for t in (conv.get("tags") or [])],
            "custom_fields":   conv.get("custom_fields"),
            # Champs CSAT potentiels au niveau racine
            "root_satisfaction": conv.get("satisfaction"),
            "root_survey":       conv.get("survey"),
            "root_rating":       conv.get("rating"),
            "root_score":        conv.get("score"),
            # Messages
            "messages_count":  len(msgs_raw),
            "messages":        msg_summary,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/debug/hbo_csat")
def debug_hbo_csat():
    """Test de la route HBO /front_csat.

    Params GET :
      ?name=SDC+92300+18+RUE+GREFFULHE+-+5+RUE+JEAN+GABIN
      ?date_start=2026-04-02
      ?date_end=2026-04-08
    """
    account_name = request.args.get("name", "SDC 92300 18 RUE GREFFULHE - 5 RUE JEAN GABIN")
    date_start   = request.args.get("date_start", "2026-04-02")
    date_end     = request.args.get("date_end",   "2026-04-08")
    try:
        cfg = load_config()
        if not cfg.get("hbo"):
            return jsonify({"error": "HBO non configuré"})

        # 1. Appel brut pour voir la structure
        raw_page1 = hbo(cfg, "/front_csat", params={
            "account_name":         account_name,
            "message_date[after]":  date_start,
            "message_date[before]": date_end,
            "itemsPerPage": 10,
            "page": 1,
        })
        items = list_items(raw_page1)

        # 2. CSAT calculé
        csat = fetch_hbo_csat(cfg, account_name, date_start, date_end)

        return jsonify({
            "account_name": account_name,
            "date_range":   f"{date_start} → {date_end}",
            "raw_total":    (raw_page1.get("hydra:totalItems") or raw_page1.get("total")
                             if isinstance(raw_page1, dict) else len(items)),
            "sample_keys":  list(items[0].keys()) if items else [],
            "sample":       items[:3],
            "csat":         csat,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/debug/building_parcels/<int:bid>")
def debug_building_parcels(bid):
    """Retourne la réponse brute de /building_parcels/{bid} (pour diagnostiquer lots)."""
    try:
        cfg  = load_config()
        data = hbo(cfg, f"/building_parcels/{bid}")
        return jsonify({
            "building_id": bid,
            "raw":         data,
            "type":        type(data).__name__,
            "keys":        list(data.keys()) if isinstance(data, dict) else None,
            "count":       len(data) if isinstance(data, list) else None,
            "lot_fields":  ({k: v for k, v in data.items()
                             if any(w in k.lower() for w in ["lot", "ppx", "main", "unit"])}
                            if isinstance(data, dict) else None),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health")
def health():
    return jsonify({"status": "ok", "version": "csat-tag-union-v6"})

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
            print("  ✅ Cache disque valide, pas de rescan au démarrage", flush=True)
        else:
            print("  🔥 Warmup: scan bâtiments au démarrage…", flush=True)
            _run_id_scan(cfg)
        # Pre-warm Front tags + accounts en séquence (évite les 429 concurrents)
        if cfg.get("front"):
            print("  🔥 Warmup: chargement tags Front (cache 4h)…", flush=True)
            try:
                tags = _get_all_front_tags(cfg)
                print(f"  ✅ {len(tags)} tags Front en cache", flush=True)
            except Exception as fe:
                print(f"  ⚠ Front tags warmup: {fe}", flush=True)
            # Comptes APRÈS les tags pour ne pas déclencher 429 concurrent
            print("  🔥 Warmup: chargement comptes Front (cache 4h)…", flush=True)
            try:
                accounts = _get_all_front_accounts(cfg)
                print(f"  ✅ {len(accounts)} comptes Front en cache", flush=True)
            except Exception as ae:
                print(f"  ⚠ Front accounts warmup: {ae}", flush=True)
    except Exception as e:
        print(f"  ⚠ Warmup error: {e}", flush=True)

import threading as _threading
_front_tags_lock      = _threading.Lock()
_front_accounts_lock  = _threading.Lock()
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
