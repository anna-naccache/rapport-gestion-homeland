#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exploration de l'API HBO pour trouver la route CSAT.
Lance avec : python debug_hbo_csat.py
"""

import requests, json
from pathlib import Path
from datetime import datetime

# ─── Charger credentials depuis config.json ───────────────────────────────────
config_path = Path(__file__).parent / "config.json"
with open(config_path, encoding="utf-8") as f:
    cfg = json.load(f)

HBO_BASE = cfg["hbo"]["base_url"]   # https://hbo.homeland.immo/api
EMAIL    = cfg["hbo"]["email"]
PASSWORD = cfg["hbo"]["password"]

# ─── Authentification ─────────────────────────────────────────────────────────
print("🔑 Connexion HBO...")
r = requests.post(f"{HBO_BASE}/v2/login_check",
                  json={"username": EMAIL, "password": PASSWORD}, timeout=15)
r.raise_for_status()
TOKEN   = r.json()["token"]
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
print("✅ Connecté\n")

# ─── Découverte : tester les variantes de la route ──────────────────────────
print("🔍 Recherche de la bonne route...\n")
VARIANTS = [
    "/front_csat", "/front-csat", "/frontCsat", "/front_csats",
    "/front_csat_ratings", "/csat", "/csats", "/survey_ratings",
    "/satisfaction_ratings", "/front_satisfaction", "/satisfaction",
    "/ratings", "/notes_satisfaction", "/avis_csat",
    "/v1/front_csat", "/v2/front_csat",
    "/building/front_csat", "/copropriete/front_csat",
    "/front_csat/list", "/api/front_csat",
    "/front_survey", "/front_surveys", "/survey", "/surveys",
]
for v in VARIANTS:
    try:
        r = requests.get(f"{HBO_BASE}{v}", headers=HEADERS,
                         params={"itemsPerPage": 1}, timeout=8)
        if r.status_code != 404:
            print(f"  ✅ {v} → {r.status_code} : {r.text[:120]}")
    except Exception as e:
        pass
print()

# ─── Test direct de la route /front_csat ─────────────────────────────────────
ACCOUNT_NAME = "SDC 92300 18 RUE GREFFULHE - 5 RUE JEAN GABIN"
DATE_START   = "2026-04-02"
DATE_END     = "2026-04-08"

print(f"🔍 Test route /enum/front_csats")
print(f"   Copropriété : {ACCOUNT_NAME}")
print(f"   Période     : {DATE_START} → {DATE_END}\n")

# Page 1 sans filtre pour voir la structure + total
r0 = requests.get(f"{HBO_BASE}/enum/front_csats", headers=HEADERS,
                  params={"itemsPerPage": 3}, timeout=15)
print(f"GET /enum/front_csats → {r0.status_code}")
if r0.ok:
    d0 = r0.json()
    items0 = (d0.get("hydra:member") or d0.get("member") or
              d0.get("data") or d0.get("items") or (d0 if isinstance(d0, list) else []))
    total = d0.get("hydra:totalItems") or d0.get("total") or "?"
    print(f"Total enregistrements (toutes copros) : {total}")
    if items0:
        print(f"Champs disponibles : {list(items0[0].keys())}")
        print(f"Exemple :\n{json.dumps(items0[0], indent=2, ensure_ascii=False)}\n")
else:
    print(f"Erreur : {r0.text[:300]}")

# Test avec filtres camelCase
print(f"─── Filtre accountName + messageDate ───")
from datetime import datetime as _dt
start_dt = _dt.strptime(DATE_START, "%Y-%m-%d")
end_dt   = _dt.strptime(DATE_END,   "%Y-%m-%d")
name_up  = ACCOUNT_NAME.upper().strip()
scores = []
page = 1
while True:
    r = requests.get(f"{HBO_BASE}/enum/front_csats", headers=HEADERS, params={
        "accountName":         ACCOUNT_NAME,
        "messageDate[after]":  DATE_START,
        "messageDate[before]": DATE_END,
        "itemsPerPage": 200,
        "page": page,
    }, timeout=15)
    print(f"  Page {page} → {r.status_code}")
    if not r.ok:
        print(f"  Erreur : {r.text[:200]}")
        break
    d = r.json()
    items = (d.get("hydra:member") or d.get("member") or
             d.get("data") or d.get("items") or (d if isinstance(d, list) else []))
    print(f"  {len(items)} résultats cette page")
    if items and page == 1:
        print(f"  Exemple : {json.dumps(items[0], indent=2, ensure_ascii=False)}")
    matched = 0
    for item in items:
        # Filtre local aussi (au cas où les params API ne filtrent pas)
        an = (item.get("accountName") or "").upper().strip()
        if an and an != name_up:
            continue
        md = item.get("messageDate") or ""
        if md:
            try:
                item_dt = _dt.fromisoformat(md[:10])
                if not (start_dt <= item_dt <= end_dt):
                    continue
            except: pass
        raw = item.get("surveyRating")
        if raw is not None:
            try:
                scores.append(float(raw))
                matched += 1
            except: pass
    print(f"  → {matched} notes retenues après filtre local")
    if len(items) < 200:
        break
    page += 1

print(f"\n{'='*60}")
print(f"  Notes trouvées : {len(scores)}")
if scores:
    avg = round(sum(scores)/len(scores), 2)
    print(f"  Score moyen   : {avg}/5")
    from collections import Counter
    dist = Counter(round(s) for s in scores)
    for i in range(1, 6):
        print(f"    {i}★ : {dist.get(i, 0)}")
print(f"{'='*60}")

input("\nAppuyez sur Entrée pour quitter...")
