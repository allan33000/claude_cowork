#!/usr/bin/env python3
"""
Mio - Pipeline quotidien de detection de PME a potentiel d'export.
Concu pour tourner dans GitHub Actions (cloud, pas de machine locale requise).

Signaux V1 (verifies et gratuits) :
  - France Travail  : offres d'emploi liees a l'export (declencheur)
  - recherche-entreprises.api.gouv.fr : profil SIRENE/RNE (prior, sans cle)

BODACC et les autres signaux du plan s'ajoutent une fois que cette base
tourne proprement pendant quelques jours.

Repartition : 2 destinataires, 10 leads chacun, jamais les memes entre eux
le meme jour, et jamais un lead deja envoye a qui que ce soit un jour
precedent (historique persiste dans leads/sent_sirens.json, committe par
le workflow a chaque run).
"""

import os
import sys
import json
import time
import urllib.parse
from datetime import datetime, timezone

import requests

FT_CLIENT_ID = os.environ.get("FRANCETRAVAIL_CLIENT_ID")
FT_CLIENT_SECRET = os.environ.get("FRANCETRAVAIL_CLIENT_SECRET")

TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"
SEARCH_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
SIRENE_URL = "https://recherche-entreprises.api.gouv.fr/search"
HISTORY_PATH = "leads/sent_sirens.json"

LEADS_PER_RECIPIENT = 10

NOTE_TEMPLATE = """Bonjour {prenom},

J'ai vu que {entreprise} recrute actuellement sur un poste lie a l'export \
({declencheur}) — signe que votre developpement a l'international s'accelere.

Mio aide les PME francaises a structurer cette phase (conformite, volet \
commercial, suivi), pour eviter que ce genre de recrutement absorbe tout le \
temps de l'equipe. Ouvert a un echange de 15 minutes cette semaine ?

[Ton prenom]"""

# Requetes ciblees sur les postes qui trahissent une activite export en cours
# (ADV export = le poste qu'on cree quand les commandes etrangeres arrivent
# reellement et que le back-office ne suit plus). A elargir si le pool de
# leads frais se reduit trop au fil du temps (cf. avertissement en fin de run).
SEARCH_QUERIES = [
    "ADV export",
    "responsable export",
    "commercial export",
    "export area manager",
    "chef de zone export",
    "assistant export",
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


def load_history() -> set:
    if not os.path.exists(HISTORY_PATH):
        return set()
    try:
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except (json.JSONDecodeError, OSError):
        return set()


def save_history(sirens: set) -> None:
    os.makedirs("leads", exist_ok=True)
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(sorted(sirens), f, ensure_ascii=False, indent=2)


def build_fresh_leads(jobs: list[dict], already_sent: set, limit: int) -> list[dict]:
    """Construit une liste de leads dont le SIREN n'a JAMAIS ete envoye
    (ni aujourd'hui, ni un jour precedent). S'arrete a `limit` resultats."""
    leads = []
    seen_this_run = set()
    for job in jobs:
        if len(leads) >= limit:
            break
        company_name = (job.get("entreprise") or {}).get("nom")
        if not company_name:
            continue
        profile = enrich_with_sirene(company_name)
        siren = profile.get("siren") if profile else None
        # Sans SIREN identifie on ne peut pas garantir la non-repetition -> on ecarte
        if not siren or siren in already_sent or siren in seen_this_run:
            continue
        seen_this_run.add(siren)
        # Dirigeant : donnee publique et gratuite (RNE/INSEE), pas de LinkedIn
        # direct disponible gratuitement -> on construit un lien de recherche
        # cible (verification humaine en 5-10 secondes) plutot que de deviner
        dirigeants = (profile or {}).get("dirigeants") or []
        dirigeant = dirigeants[0] if dirigeants else {}
        prenom = (dirigeant.get("prenoms") or "").split(",")[0].strip()
        nom = dirigeant.get("nom") or ""
        contact_display = f"{prenom} {nom}".strip() or "dirigeant non identifie"

        linkedin_query = f'"{prenom} {nom}" "{company_name}"' if (prenom or nom) else f'"{company_name}" export'
        linkedin_search_url = "https://www.google.com/search?q=" + urllib.parse.quote(
            linkedin_query + " site:linkedin.com/in"
        )

        note = NOTE_TEMPLATE.format(
            prenom=prenom or "",
            entreprise=company_name,
            declencheur=job.get("intitule") or "un poste export",
        )

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
                "contact_probable": contact_display,
                "linkedin_search_url": linkedin_search_url,
                "note_prospection": note,
            }
        )
        time.sleep(0.2)
    return leads


def render_email_html(leads: list[dict], recipient_label: str) -> str:
    lines = [f"<h2>Mio - {len(leads)} lead(s) du {datetime.now(timezone.utc):%d/%m/%Y} pour {recipient_label}</h2>"]
    if not leads:
        lines.append("<p>Aucun lead frais disponible aujourd'hui pour ce destinataire.</p>")
    for lead in leads:
        note_html = lead["note_prospection"].replace("\n", "<br>")
        lines.append(
            f"<p><b>{lead['entreprise']}</b> (SIREN {lead['siren']})<br>"
            f"NAF: {lead['naf']} — Effectif: {lead['effectif']}<br>"
            f"Declencheur: {lead['declencheur']} ({lead['date_offre']})<br>"
            f"<a href=\"{lead['url_offre']}\">Voir l'offre</a><br>"
            f"Contact probable (dirigeant, source RNE): <b>{lead['contact_probable']}</b> — "
            f"<a href=\"{lead['linkedin_search_url']}\">chercher son LinkedIn</a> (a confirmer, 1 clic)<br>"
            f"<details><summary>Note prete a copier</summary><p>{note_html}</p></details>"
            f"</p><hr>"
        )
    return "\n".join(lines)


def build_for_recipient(recipient: str, jobs: list[dict], history: set) -> tuple[list[dict], set]:
    """Construit les leads d'UN destinataire en excluant l'historique global
    (qui inclut deja ce que l'autre destinataire a pu recevoir plus tot dans
    la meme journee, si son run est passe avant)."""
    leads = build_fresh_leads(jobs, history, limit=LEADS_PER_RECIPIENT)
    if len(leads) < LEADS_PER_RECIPIENT:
        print(
            f"[WARN] {recipient}: seulement {len(leads)} leads frais sur {LEADS_PER_RECIPIENT} "
            "(pool de leads jamais-envoyes en baisse -> elargir SEARCH_QUERIES si ca persiste).",
            file=sys.stderr,
        )
    return leads, {lead["siren"] for lead in leads}


def write_outputs(recipient: str, leads: list[dict]) -> None:
    os.makedirs("leads", exist_ok=True)
    date_str = f"{datetime.now(timezone.utc):%Y-%m-%d}"
    payload = {
        "date_utc": datetime.now(timezone.utc).isoformat(),
        "recipient": recipient,
        "leads": leads,
    }
    with open(f"leads/leads_{recipient}_{date_str}.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    with open(f"leads/latest_{recipient}.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    with open(f"leads/email_body_{recipient}.html", "w", encoding="utf-8") as f:
        f.write(render_email_html(leads, recipient.capitalize()))


def main() -> None:
    # Usage : python daily_leads.py allan | esteban | both
    # "both" sert uniquement aux tests manuels (workflow_dispatch) ; en
    # production chaque destinataire est construit par son propre run,
    # declenche a son propre horaire (cf. mio-leads.yml).
    target = sys.argv[1] if len(sys.argv) > 1 else "both"
    if target not in ("allan", "esteban", "both"):
        raise SystemExit(f"Argument invalide: {target!r} (attendu: allan, esteban, ou both)")

    token = get_ft_token()
    jobs = fetch_export_jobs(token)
    history = load_history()

    recipients = ["allan", "esteban"] if target == "both" else [target]
    all_new_sirens = set()

    for recipient in recipients:
        leads, new_sirens = build_for_recipient(recipient, jobs, history)
        write_outputs(recipient, leads)
        history |= new_sirens  # visible immediatement pour le destinataire suivant dans cette meme execution
        all_new_sirens |= new_sirens
        print(f"{len(leads)} leads -> {recipient}")

    save_history(history)
    print(f"Historique total: {len(history)} SIREN jamais repetes (+{len(all_new_sirens)} aujourd'hui)")


if __name__ == "__main__":
    main()
