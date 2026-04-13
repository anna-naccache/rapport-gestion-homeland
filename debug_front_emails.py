#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de diagnostic : compte les emails envoyés/reçus pour un tag de copropriété.
Lance avec : python debug_front_emails.py
"""

import requests
import json
from datetime import datetime

# ─── CONFIG ──────────────────────────────────────────────────────────────────
TOKEN      = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NzUxNDE1MDcsImlzcyI6ImZyb250Iiwic3ViIjoiaG9tZWxhbmRpbW1vIiwianRpIjoiNjI0NzY0ODFkMzYzMjllZiJ9.9upML0DoEjzUlAu9oUc7O4IlSi8Eb2vj6PE8eq1hBf8"
BASE       = "https://api2.frontapp.com"
TAG_NAME   = "SDC 92300 18 RUE GREFFULHE - 5 RUE JEAN GABIN - 671"
DATE_START = "2026-04-02"
DATE_END   = "2026-04-08"
# ─────────────────────────────────────────────────────────────────────────────

HEADERS  = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}
START_TS = int(datetime.strptime(DATE_START, "%Y-%m-%d").timestamp())
END_TS   = int(datetime.strptime(DATE_END,   "%Y-%m-%d").timestamp()) + 86399

def find_tag(name):
    """Cherche le tag par nom exact ou partiel."""
    page_token = None
    name_up = name.upper()
    for _ in range(50):
        params = {"limit": 100}
        if page_token:
            params["page_token"] = page_token
        r = requests.get(f"{BASE}/tags", headers=HEADERS, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        for t in data.get("_results", []):
            if (t.get("name") or "").upper() == name_up:
                return t
        nxt = data.get("_pagination", {}).get("next", "")
        if not nxt or "page_token=" not in nxt:
            break
        page_token = nxt.split("page_token=")[1].split("&")[0]
    return None

def fetch_convs_for_tag(tag_id):
    """Conversations avec activité dans la fenêtre (triées par last_message_at DESC)."""
    convs, page_token, stop = [], None, False
    while not stop and len(convs) < 2000:
        params = {"limit": 100}
        if page_token:
            params["page_token"] = page_token
        r = requests.get(f"{BASE}/tags/{tag_id}/conversations", headers=HEADERS, params=params, timeout=20)
        r.raise_for_status()
        data  = r.json()
        items = data.get("_results", [])
        if not items:
            break
        for c in items:
            last_ts = c.get("last_message_at") or c.get("created_at") or 0
            if last_ts and last_ts < START_TS:
                stop = True
                break
            if not last_ts or last_ts <= END_TS:
                convs.append(c)
        nxt = data.get("_pagination", {}).get("next", "")
        if not nxt or "page_token=" not in nxt:
            break
        page_token = nxt.split("page_token=")[1].split("&")[0]
    return convs

def count_messages(conv_id):
    """Compte inbound/outbound dans [START_TS, END_TS] pour une conversation."""
    sent, received, page_token = 0, 0, None
    while True:
        params = {"limit": 100}
        if page_token:
            params["page_token"] = page_token
        r = requests.get(f"{BASE}/conversations/{conv_id}/messages", headers=HEADERS, params=params, timeout=20)
        if r.status_code == 429:
            import time; time.sleep(10)
            continue
        if not r.ok:
            break
        data  = r.json()
        items = data.get("_results", [])
        if not items:
            break
        hit_old = False
        for m in items:
            msg_ts = m.get("created_at") or 0
            if msg_ts and msg_ts < START_TS:
                hit_old = True
                break
            if msg_ts and msg_ts <= END_TS:
                if m.get("is_inbound"):
                    received += 1
                else:
                    sent += 1
        if hit_old:
            break
        nxt = data.get("_pagination", {}).get("next", "")
        if not nxt or "page_token=" not in nxt:
            break
        page_token = nxt.split("page_token=")[1].split("&")[0]
    return sent, received

# ─── MAIN ────────────────────────────────────────────────────────────────────
print(f"\n🔍 Recherche du tag : {TAG_NAME}")
tag = find_tag(TAG_NAME)
if not tag:
    print("❌ Tag non trouvé !")
    input("Appuyez sur Entrée pour quitter...")
    exit(1)

print(f"✅ Tag trouvé : {tag['name']} (id={tag['id']})")
print(f"\n📬 Récupération des conversations du {DATE_START} au {DATE_END}...")

convs = fetch_convs_for_tag(tag["id"])
print(f"✅ {len(convs)} conversations trouvées\n")

total_sent, total_received = 0, 0
per_conv = []

for i, c in enumerate(convs, 1):
    cid = c.get("id", "?")
    subj = (c.get("subject") or "(sans objet)")[:60]
    s, r = count_messages(cid)
    total_sent     += s
    total_received += r
    per_conv.append({"id": cid, "subject": subj, "sent": s, "received": r})
    print(f"  [{i:2d}/{len(convs)}] {cid} | S={s} R={r} | {subj}")

print(f"\n{'─'*60}")
print(f"  TOTAL  →  Envoyés : {total_sent}   Reçus : {total_received}   Total : {total_sent + total_received}")
print(f"{'─'*60}")
print(f"\n  Conversations avec messages : {sum(1 for p in per_conv if p['sent']+p['received']>0)}")
print(f"  Conversations vides         : {sum(1 for p in per_conv if p['sent']+p['received']==0)}")

# Sauvegarder le résultat
output = {
    "tag": TAG_NAME,
    "date_range": f"{DATE_START} → {DATE_END}",
    "sent": total_sent,
    "received": total_received,
    "total": total_sent + total_received,
    "convs_total": len(convs),
    "per_conv": per_conv,
}
with open("debug_front_result.json", "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)
print(f"\n💾 Résultat sauvegardé dans debug_front_result.json")
input("\nAppuyez sur Entrée pour quitter...")
