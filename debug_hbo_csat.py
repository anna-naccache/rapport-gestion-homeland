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

# ─── Test direct de la route /front_csat ─────────────────────────────────────
ACCOUNT_NAME = "SDC 92300 18 RUE GREFFULHE - 5 RUE JEAN GABIN"
DATE_START   = "2026-04-02"
DATE_END     = "2026-04-08"

print(f"🔍 Test route /front_csat")
print(f"   Copropriété : {ACCOUNT_NAME}")
print(f"   Période     : {DATE_START} → {DATE_END}\n")

# Page 1 sans filtre pour voir la structure
r0 = requests.get(f"{HBO_BASE}/front_csat", headers=HEADERS,
                  params={"itemsPerPage": 3}, timeout=15)
print(f"GET /front_csat → {r0.status_code}")
if r0.ok:
    d0 = r0.json()
    items0 = (d0.get("hydra:member") or d0.get("member") or
              d0.get("data") or d0.get("items") or d0 if isinstance(d0, list) else [])
    total = d0.get("hydra:totalItems") or d0.get("total") or "?"
    print(f"Total enregistrements : {total}")
    if items0:
        print(f"Champs disponibles : {list(items0[0].keys())}")
        print(f"Exemple :\n{json.dumps(items0[0], indent=2, ensure_ascii=False)}\n")

# Test avec filtre account_name + date
print(f"─── Filtre account_name + date ───")
scores = []
page = 1
while True:
    r = requests.get(f"{HBO_BASE}/front_csat", headers=HEADERS, params={
        "account_name":         ACCOUNT_NAME,
        "message_date[after]":  DATE_START,
        "message_date[before]": DATE_END,
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
    for item in items:
        raw = item.get("survey_rating")
        if raw is not None:
            try: scores.append(float(raw))
            except: pass
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
