#!/usr/bin/env python3
"""
Mio - Pipeline quotidien de detection de PME a potentiel d'export.
Concu pour tourner dans GitHub Actions (cloud, pas de machine locale requise).

Signaux V1 (verifies et gratuits, sans cle pour l'un des deux) :
  - France Travail  : offres d'emploi liees a l'export (declencheur)
  - recherche-entreprises.api.gouv.fr : profil SIRENE/RNE (prior, sans cle)

BODACC et les autres signaux du plan (turmn 1) s'ajoutent une fois que
cette base tourne proprement pendant quelques jours - volontairement
laisses de cote ici pour livrer quelque chose de fiable des le premier run.
"""

import os
import sys
import json
import time
from datetime import datetime, timezone

import requests

FT_CLIENT_ID = os.environ.get("FRANCETRAVAIL_CLIENT_ID")
FT_CLIENT_SECRET = os.environ.get("FRANCETRAVAIL_CLIENT_SECRET")

TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"
SEARCH_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
SIRENE_URL = "https://recherche-entreprises.api.gouv.fr/search"

# Requetes ciblees sur les postes qui trahissent une activite export en cours
# (ADV export = le poste qu'on cree quand les commandes etrangeres arrivent
# reellement et que le back-office ne suit plus)
SEARCH_QUERIES = [
    "ADV export",
    "responsable export",
    "commercial export",
    "export area manager",
]


def get_ft_token() -> str:
    if not FT_CLIENT_ID or not FT_CLIENT_SECRET:
        raise RuntimeError(
            "FRANCETRAVAIL_CLIENT_ID / FRANCETRAVAIL_CLIENT_SECRET manquants "
            "(a definir comme secrets GitHub Actions)."
        )
    data = {
        "grant_type": "client_credentials",
        "client_id": FT_CLIENT_ID,
        "client_secret": FT_CLIENT_SECRET,
        "scope": "api_offresdemploiv2 o2dsoffre",
    }
    r = requests.post(
        TOKEN_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def fetch_export_jobs(token: str) -> list[dict]:
    headers = {"Authorization": f"Bearer {token}"}
    all_jobs = {}
    for query in SEARCH_QUERIES:
        params = {"motsCles": query, "sort": 1}
        try:
            r = requests.get(SEARCH_URL, headers=headers, params=params, timeout=20)
        except requests.RequestException as exc:
            print(f"[WARN] echec requete France Travail pour '{query}': {exc}", file=sys.stderr)
            continue
        if r.status_code not in (200, 206):
            print(f"[WARN] France Travail a repondu {r.status_code} pour '{query}'", file=sys.stderr)
            continue
        for offer in r.json().get("resultats", []):
            all_jobs[offer.get("id")] = offer  # dedup par id d'offre
        time.sleep(0.3)  # ne pas marteler l'API
    return list(all_jobs.values())


def enrich_with_sirene(company_name: str) -> dict | None:
    try:
        r = requests.get(SIRENE_URL, params={"q": company_name, "per_page": 1}, timeout=15)
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    results = r.json().get("results", [])
    return results[0] if results else None


def build_leads(jobs: list[dict]) -> list[dict]:
    leads = []
    seen_sirens = set()
    for job in jobs:
        company_name = (job.get("entreprise") or {}).get("nom")
        if not company_name:
            continue
        profile = enrich_with_sirene(company_name)
        siren = profile.get("siren") if profile else None
        if siren:
            if siren in seen_sirens:
                continue
            seen_sirens.add(siren)
        leads.append(
            {
                "entreprise": company_name,
                "siren": siren,
                "naf": (profile or {}).get("activite_principale"),
                "effectif": (profile or {}).get("tranche_effectif_salarie"),
                "declencheur": job.get("intitule"),
                "date_offre": job.get("dateCreation"),
                "lieu": ((job.get("lieuTravail") or {}).get("libelle")),
                "url_offre": (job.get("origineOffre") or {}).get("urlOrigine", ""),
            }
        )
        time.sleep(0.2)
    return leads[:10]


def main() -> None:
    token = get_ft_token()
    jobs = fetch_export_jobs(token)
    leads = build_leads(jobs)

    os.makedirs("leads", exist_ok=True)
    out_path = f"leads/leads_{datetime.now(timezone.utc):%Y-%m-%d}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(
            {"date_utc": datetime.now(timezone.utc).isoformat(), "nb_leads": len(leads), "leads": leads},
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(f"{len(leads)} leads generes -> {out_path}")
    if not leads:
        print("[WARN] 0 lead genere - verifier les requetes ou la disponibilite des APIs.", file=sys.stderr)


if __name__ == "__main__":
    main()
