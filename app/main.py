import asyncio
import json
import logging
import os
import re
import sqlite3
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

# Désactive uniquement les logs d'accès HTTP Uvicorn répétitifs.
# Les logs applicatifs ForcedFR restent inchangés.
logging.getLogger("uvicorn.access").disabled = True

try:
    import discord
except ImportError:
    discord = None


# ============================================================
# CONFIGURATION
# ============================================================

QB_HOST = os.getenv(
    "QB_HOST",
    "http://192.168.1.42:8080",
).rstrip("/")

QB_USERNAME = os.getenv("QB_USERNAME", "")
QB_PASSWORD = os.getenv("QB_PASSWORD", "")

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "").strip()
DISCORD_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID", "").strip()

RADARR_URL = os.getenv("RADARR_URL", "http://192.168.1.42:7878").rstrip("/")
RADARR_API_KEY = os.getenv("RADARR_API_KEY", "").strip()

SONARR_URL = os.getenv("SONARR_URL", "http://192.168.1.42:8989").rstrip("/")
SONARR_API_KEY = os.getenv("SONARR_API_KEY", "").strip()

# Vérification très fréquente des nouveaux torrents
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "1"))
ANALYSIS_POLL_SECONDS = 0.5

# Temps maximum consacré à la recherche d'un MKV analysable
ANALYSIS_TIMEOUT = int(
    os.getenv("ANALYSIS_TIMEOUT", "900")
)

LOG_LEVEL = os.getenv(
    "LOG_LEVEL",
    "INFO",
).upper()

# Recherche du contexte Radarr/Sonarr avant notification Discord
# 1 tentative toutes les 5 secondes pendant 1 minute maximum.
RELEASE_CONTEXT_RETRY_SECONDS = 5
RELEASE_CONTEXT_TIMEOUT = 60

# Stabilisation de qBittorrent au démarrage : évite de considérer comme nouveaux
# les torrents restaurés progressivement après un redémarrage du serveur.
STARTUP_STABILITY_CHECK_SECONDS = 5
STARTUP_STABLE_CHECKS_REQUIRED = 3

# Base SQLite persistante pour les analyses de bibliothèque.
SQLITE_PATH = os.getenv("SQLITE_PATH", "/app/data/forcedfr.db")

# IA : Brave Search + Groq
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b").strip()
BRAVE_SEARCH_API_KEY = os.getenv("BRAVE_SEARCH_API_KEY", "").strip()
BRAVE_RESULTS_COUNT = int(os.getenv("BRAVE_RESULTS_COUNT", "5"))
AI_ENABLED = os.getenv("AI_ENABLED", "1") == "1"
AI_RETRIES = int(os.getenv("AI_RETRIES", "4"))


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("forcedfr")


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="ForcedFR",
    description="Détection automatique des pistes françaises forcées.",
    version="2.6.13",
)

# Logos PNG fournis dans app/templates. Les fichiers restent externes au code.
# Cherche le dossier à côté de ce fichier, puis dans le dossier app/ du projet.
_APP_DIR = Path(__file__).resolve().parent
_PROJECT_DIR = _APP_DIR.parent
_TEMPLATES_CANDIDATES = (
    _APP_DIR / "templates",
    _APP_DIR / "app" / "templates",
    _PROJECT_DIR / "app" / "templates",
)
TEMPLATES_DIR = next((candidate for candidate in _TEMPLATES_CANDIDATES if candidate.is_dir()), _APP_DIR / "templates")
if TEMPLATES_DIR.is_dir():
    app.mount("/templates", StaticFiles(directory=str(TEMPLATES_DIR), check_dir=True), name="templates")
else:
    log.warning("Dossier des logos introuvable. Dossiers vérifiés : %s", ", ".join(str(p) for p in _TEMPLATES_CANDIDATES))


# ============================================================
# SESSION qBITTORRENT
# ============================================================

qb_session = requests.Session()


# ============================================================
# ÉTAT DE LA SURVEILLANCE
# ============================================================

previous_torrents: set[str] = set()

processing_torrents: set[str] = set()

MAIN_EVENT_LOOP: asyncio.AbstractEventLoop | None = None
discord_bot: Any = None

# Évite qu'une même notification Discord soit traitée plusieurs fois.
resolved_discord_actions: set[str] = set()
discord_lifecycle_task: asyncio.Task | None = None


# ============================================================
# AUTHENTIFICATION qBITTORRENT
# ============================================================

def qb_login() -> None:
    response = qb_session.post(
        f"{QB_HOST}/api/v2/auth/login",
        data={
            "username": QB_USERNAME,
            "password": QB_PASSWORD,
        },
        timeout=10,
    )

    if response.status_code == 403:
        raise RuntimeError("IP bannie par qBittorrent après trop de tentatives d'authentification. Ne réessaie pas pendant le bannissement.")
    if response.status_code not in (200, 204):
        raise RuntimeError(f"Authentification qBittorrent échouée : HTTP {response.status_code} {response.text}")

    log.info(
        "Connexion qBittorrent réussie : %s",
        QB_HOST,
    )


# ============================================================
# REQUÊTES qBITTORRENT
# ============================================================

def qb_request(method: str, endpoint: str, *, params: dict[str, Any] | None = None, data: dict[str, Any] | None = None) -> Any:
    response=qb_session.request(method,f"{QB_HOST}{endpoint}",params=params,data=data,timeout=15)
    if response.status_code==403:
        raise RuntimeError("qBittorrent a répondu HTTP 403 : accès refusé ou IP bannie. Aucun nouvel essai d'authentification automatique n'est effectué.")
    response.raise_for_status()
    if not response.text: return None
    if "application/json" in response.headers.get("content-type",""): return response.json()
    return response.text

# ============================================================
# INFORMATIONS TORRENTS
# ============================================================

def get_torrents() -> list[dict[str, Any]]:

    return qb_request(
        "GET",
        "/api/v2/torrents/info",
    )


def get_torrent(
    torrent_hash: str,
) -> dict[str, Any]:

    torrents = qb_request(
        "GET",
        "/api/v2/torrents/info",
        params={
            "hashes": torrent_hash,
        },
    )

    if not torrents:
        raise HTTPException(
            status_code=404,
            detail="Torrent introuvable.",
        )

    return torrents[0]


def get_torrent_files(
    torrent_hash: str,
) -> list[dict[str, Any]]:

    return qb_request(
        "GET",
        "/api/v2/torrents/files",
        params={
            "hash": torrent_hash,
        },
    )


def get_piece_states(
    torrent_hash: str,
) -> list[int]:

    return qb_request(
        "GET",
        "/api/v2/torrents/pieceStates",
        params={
            "hash": torrent_hash,
        },
    )


# ============================================================
# ACTIONS qBITTORRENT
# ============================================================

def toggle_first_last_piece_priority(
    torrent_hash: str,
) -> None:

    qb_request(
        "POST",
        "/api/v2/torrents/toggleFirstLastPiecePrio",
        data={
            "hashes": torrent_hash,
        },
    )


def stop_torrent(
    torrent_hash: str,
) -> None:

    qb_request(
        "POST",
        "/api/v2/torrents/stop",
        data={
            "hashes": torrent_hash,
        },
    )

    log.info(
        "[%s] Torrent mis en pause.",
        torrent_hash,
    )


def start_torrent(
    torrent_hash: str,
) -> None:

    qb_request(
        "POST",
        "/api/v2/torrents/start",
        data={
            "hashes": torrent_hash,
        },
    )

    log.info(
        "[%s] Torrent relancé.",
        torrent_hash,
    )


# ============================================================
# DISCORD / RADARR / SONARR
# ============================================================

def arr_item_url_lookup(
    base_url: str,
    api_key: str,
    source: str,
    item_id: Any,
) -> str:
    """
    Construit l'URL de la page web Radarr/Sonarr.

    Les IDs présents dans l'historique sont des IDs internes Arr.
    L'interface web utilise :
    - Radarr : tmdbId
    - Sonarr : titleSlug (slug lisible de la série)

    En cas d'échec de la requête complémentaire, on conserve
    l'URL basée sur l'ID interne afin de ne jamais bloquer
    la notification Discord.
    """

    if item_id is None:
        return base_url

    fallback = (
        f"{base_url}/movie/{item_id}"
        if source == "Radarr"
        else f"{base_url}/series/{item_id}"
    )

    try:
        endpoint = (
            f"{base_url}/api/v3/movie/{item_id}"
            if source == "Radarr"
            else f"{base_url}/api/v3/series/{item_id}"
        )

        response = requests.get(
            endpoint,
            headers={"X-Api-Key": api_key},
            timeout=10,
        )
        response.raise_for_status()

        item = response.json()

        if source == "Radarr":
            external_id = item.get("tmdbId")
            if external_id:
                return f"{base_url}/movie/{external_id}"

        else:
            # Sonarr n'utilise pas le TVDB ID dans l'URL de sa page série.
            # L'interface web attend le titleSlug, par exemple :
            # /series/baron-noir
            title_slug = item.get("titleSlug")
            if title_slug:
                return f"{base_url}/series/{title_slug}"

        log.warning(
            "[%s] Identifiant externe introuvable pour %s ID %s. "
            "Utilisation de l'URL de secours.",
            source,
            source,
            item_id,
        )

    except Exception as exc:
        log.warning(
            "Impossible de récupérer l'identifiant externe %s "
            "(ID interne %s) : %s. Utilisation de l'URL de secours.",
            source,
            item_id,
            exc,
        )

    return fallback


def arr_history_lookup(
    base_url: str,
    api_key: str,
    torrent_hash: str,
    source: str,
) -> dict[str, Any] | None:

    if not base_url or not api_key:
        return None

    try:

        response = requests.get(
            f"{base_url}/api/v3/history",
            headers={"X-Api-Key": api_key},
            params={"pageSize": 1000},
            timeout=10,
        )

        response.raise_for_status()

        records = response.json().get("records", [])

        matches = [
            item
            for item in records
            if str(item.get("downloadId", "")).lower()
            == torrent_hash.lower()
        ]

        if not matches:
            return None

        grabbed = next(
            (
                item
                for item in matches
                if item.get("eventType") == "grabbed"
            ),
            matches[0],
        )

        data = grabbed.get("data", {}) or {}

        item_id = (
            grabbed.get("movieId")
            if source == "Radarr"
            else grabbed.get("seriesId")
        )

        arr_item_url = arr_item_url_lookup(
            base_url,
            api_key,
            source,
            item_id,
        )

        poster_url = None
        try:
            endpoint = f"{base_url}/api/v3/movie/{item_id}" if source == "Radarr" else f"{base_url}/api/v3/series/{item_id}"
            item_response = requests.get(endpoint, headers={"X-Api-Key": api_key}, timeout=10)
            item_response.raise_for_status()
            item_json = item_response.json() or {}
            images = item_json.get("images") or []
            poster = next((x for x in images if str(x.get("coverType") or "").lower() == "poster"), None)
            if poster:
                poster_url = poster.get("remoteUrl") or poster.get("url")
        except Exception:
            poster_url = None

        return {
            "source": source,
            "title": grabbed.get("sourceTitle"),
            "indexer": data.get("indexer"),
            "tracker_url": (
                data.get("nzbInfoUrl")
                or data.get("infoUrl")
            ),
            "arr_item_url": arr_item_url,
            "item_id": item_id,
            "poster_url": poster_url,
            "event_type": grabbed.get("eventType"),
        }

    except Exception as exc:

        log.warning(
            "[%s] Impossible de récupérer l'historique %s : %s",
            torrent_hash,
            source,
            exc,
        )

        return None


def get_release_context(
    torrent_hash: str,
) -> dict[str, Any]:

    radarr = arr_history_lookup(
        RADARR_URL,
        RADARR_API_KEY,
        torrent_hash,
        "Radarr",
    )

    if radarr:
        return radarr

    sonarr = arr_history_lookup(
        SONARR_URL,
        SONARR_API_KEY,
        torrent_hash,
        "Sonarr",
    )

    if sonarr:
        return sonarr

    return {
        "source": None,
        "title": None,
        "indexer": None,
        "tracker_url": None,
        "arr_item_url": None,
        "item_id": None,
        "poster_url": None,
        "event_type": None,
    }


def wait_for_release_context(
    torrent_hash: str,
) -> dict[str, Any] | None:
    """
    Attend que Radarr ou Sonarr expose l'événement "grabbed"
    contenant l'URL de la page du torrent.

    Une tentative toutes les 5 secondes pendant 1 minute.
    Aucune notification Discord n'est envoyée sans tracker_url.
    """

    started_at = time.time()
    attempt = 0

    while True:
        attempt += 1

        release = get_release_context(
            torrent_hash
        )

        if release.get("tracker_url"):
            log.info(
                "[%s] URL du torrent récupérée via %s.",
                torrent_hash,
                release.get("source") or "Arr",
            )
            return release

        elapsed = time.time() - started_at

        if elapsed >= RELEASE_CONTEXT_TIMEOUT:
            log.warning(
                "[%s] URL du torrent introuvable après %ss "
                "(%d tentative(s)). Notification Discord annulée.",
                torrent_hash,
                RELEASE_CONTEXT_TIMEOUT,
                attempt,
            )
            return None

        remaining = max(
            0,
            RELEASE_CONTEXT_TIMEOUT - int(elapsed),
        )

        log.info(
            "[%s] URL du torrent pas encore disponible "
            "(tentative %d, nouvelle tentative dans %ss, "
            "%ss restantes).",
            torrent_hash,
            attempt,
            RELEASE_CONTEXT_RETRY_SECONDS,
            remaining,
        )

        time.sleep(
            RELEASE_CONTEXT_RETRY_SECONDS
        )


def build_qbittorrent_url(
    torrent_hash: str,
) -> str:

    return f"{QB_HOST}/#torrent={torrent_hash}"


async def _send_discord_bot_message(*, embeds: list[dict[str, Any]], torrent_hash: str, release: dict[str, Any], decision_required: bool = False) -> Any:
    if discord_bot is None or not discord_bot.is_ready(): raise RuntimeError("Bot Discord non prêt.")
    if not DISCORD_CHANNEL_ID: raise RuntimeError("DISCORD_CHANNEL_ID non configuré.")
    channel=discord_bot.get_channel(int(DISCORD_CHANNEL_ID))
    if channel is None: channel=await discord_bot.fetch_channel(int(DISCORD_CHANNEL_ID))
    enriched=[]; poster_url=str(release.get("poster_url") or "").strip() if release else ""
    for embed in embeds:
        item=dict(embed)
        if poster_url and not item.get("image"): item["image"]={"url":poster_url}
        enriched.append(item)
    return await channel.send(embeds=[discord.Embed.from_dict(e) for e in enriched], view=ForcedFRView(torrent_hash,release,decision_required=decision_required))


def _discord_record_message(message: Any, torrent_hash: str, decision_required: bool) -> None:
    if message is None or not DISCORD_CHANNEL_ID: return
    try:
        with _db_connect() as conn:
            conn.execute("INSERT INTO discord_messages(message_id,channel_id,torrent_hash,decision_required,sent_at,reminder_sent,resolved,deleted_at) VALUES (?,?,?,?,?,?,?,NULL)",(str(message.id),str(DISCORD_CHANNEL_ID),str(torrent_hash),int(bool(decision_required)),time.time(),0,0))
    except Exception: log.exception("Impossible d'enregistrer le message Discord %s.",getattr(message,"id","?"))


def send_discord_message(*, embeds: list[dict[str, Any]], torrent_hash: str | None = None, release: dict[str, Any] | None = None, decision_required: bool = False) -> None:
    if discord_bot is not None and MAIN_EVENT_LOOP is not None and torrent_hash and release:
        try:
            future=asyncio.run_coroutine_threadsafe(_send_discord_bot_message(embeds=embeds,torrent_hash=torrent_hash,release=release,decision_required=decision_required),MAIN_EVENT_LOOP)
            message=future.result(timeout=15); _discord_record_message(message,torrent_hash,decision_required); log.info("Notification Discord envoyée%s."," avec prise de décision" if decision_required else ""); return
        except Exception: log.exception("Impossible d'envoyer la notification via le bot Discord.")
    log.warning("Bot Discord indisponible : notification non envoyée.")


class ForcedFRView(discord.ui.View if discord else object):
    def __init__(self,torrent_hash: str,release: dict[str,Any],decision_required: bool=False)->None:
        if discord is None: return
        super().__init__(timeout=None); tracker_url=release.get("tracker_url"); arr_item_url=release.get("arr_item_url"); source=release.get("source")
        if tracker_url: self.add_item(discord.ui.Button(label="🌐 Voir le torrent",style=discord.ButtonStyle.link,url=tracker_url))
        self.add_item(discord.ui.Button(label="🖥️ Ouvrir qBittorrent",style=discord.ButtonStyle.link,url=build_qbittorrent_url(torrent_hash)))
        if arr_item_url:
            label="🎬 Ouvrir Radarr" if source=="Radarr" else "📺 Ouvrir Sonarr"; self.add_item(discord.ui.Button(label=label,style=discord.ButtonStyle.link,url=arr_item_url))
        if decision_required:
            self.add_item(discord.ui.Button(label="▶️ Continuer le téléchargement",style=discord.ButtonStyle.success,custom_id=f"forcedfr:resume:{torrent_hash}"))
            self.add_item(discord.ui.Button(label="⏸️ Laisser en pause",style=discord.ButtonStyle.secondary,custom_id=f"forcedfr:pause:{torrent_hash}"))


def build_disabled_decision_view(message: Any) -> Any:
    if discord is None: return None
    view=discord.ui.View(timeout=None)
    for row in getattr(message,"components",[]):
        for component in getattr(row,"children",[]):
            custom_id=getattr(component,"custom_id",None); url=getattr(component,"url",None)
            view.add_item(discord.ui.Button(label=getattr(component,"label",None),style=getattr(component,"style",discord.ButtonStyle.secondary),custom_id=custom_id,url=url,emoji=getattr(component,"emoji",None),disabled=bool(custom_id and str(custom_id).startswith("forcedfr:"))))
    return view

class ForcedFRTestView(discord.ui.View if discord else object):
    def __init__(self) -> None:
        if discord is None:
            return
        super().__init__(timeout=300)
        self.add_item(discord.ui.Button(label="🔗 Test lien", style=discord.ButtonStyle.link, url=build_qbittorrent_url("test")))
        self.add_item(discord.ui.Button(label="▶ Tester Continuer", style=discord.ButtonStyle.success, custom_id="forcedfr:test:resume"))
        self.add_item(discord.ui.Button(label="⏸ Tester Pause", style=discord.ButtonStyle.secondary, custom_id="forcedfr:test:pause"))


async def send_discord_test_message() -> None:
    if discord_bot is None or not discord_bot.is_ready():
        raise RuntimeError("Bot Discord non prêt. Vérifie le token, l'ID du salon et redémarre ForcedFR si nécessaire.")
    if not DISCORD_CHANNEL_ID:
        raise RuntimeError("ID du salon Discord non configuré.")
    channel = discord_bot.get_channel(int(DISCORD_CHANNEL_ID))
    if channel is None:
        channel = await discord_bot.fetch_channel(int(DISCORD_CHANNEL_ID))
    embed = discord.Embed(
        title="🧪 Test ForcedFR",
        description="Message de test du bot Discord. Les boutons ci-dessous sont sans effet sur qBittorrent.",
    )
    embed.add_field(name="Connexion", value="✅ Bot connecté et salon accessible.", inline=False)
    embed.add_field(name="Boutons", value="▶ Continuer et ⏸ Pause simulent les actions Discord.", inline=False)
    await channel.send(embed=embed, view=ForcedFRTestView())


def build_discord_bot() -> Any:
    if discord is None:
        return None

    intents = discord.Intents.none()
    bot = discord.Client(intents=intents)

    @bot.event
    async def on_ready() -> None:
        log.info("Bot Discord connecté : %s", bot.user)
        if DISCORD_CHANNEL_ID:
            log.info("Salon Discord configuré : %s", DISCORD_CHANNEL_ID)

    @bot.event
    async def on_interaction(interaction: Any) -> None:
        try:
            data = interaction.data or {}
            custom_id = str(data.get("custom_id", ""))

            if not custom_id.startswith("forcedfr:"):
                return

            parts = custom_id.split(":", 2)
            if len(parts) != 3:
                return

            _, action, torrent_hash = parts

            await interaction.response.defer(ephemeral=True)

            if torrent_hash == "test":
                if action == "resume":
                    await interaction.followup.send("🧪 Test réussi : le bouton « Continuer » fonctionne. Aucun torrent n’a été modifié.", ephemeral=True)
                elif action == "pause":
                    await interaction.followup.send("🧪 Test réussi : le bouton « Pause » fonctionne. Aucun torrent n’a été modifié.", ephemeral=True)
                else:
                    await interaction.followup.send("🧪 Action de test inconnue.", ephemeral=True)
                return

            if torrent_hash in resolved_discord_actions:
                await interaction.followup.send(
                    "ℹ️ Une décision a déjà été prise pour ce torrent.",
                    ephemeral=True,
                )
                return

            # Vérifie que le torrent existe toujours avant toute action.
            await asyncio.to_thread(get_torrent, torrent_hash)

            if action == "resume":
                await asyncio.to_thread(start_torrent, torrent_hash)
                response_message = (
                    "▶️ Le téléchargement a été repris dans qBittorrent."
                )
                log.info(
                    "[%s] Reprise demandée depuis Discord par %s.",
                    torrent_hash,
                    interaction.user,
                )

            elif action == "pause":
                await asyncio.to_thread(stop_torrent, torrent_hash)
                response_message = (
                    "⏸️ Le téléchargement reste en pause dans qBittorrent."
                )
                log.info(
                    "[%s] Maintien en pause demandé depuis Discord par %s.",
                    torrent_hash,
                    interaction.user,
                )

            else:
                await interaction.followup.send(
                    "⚠️ Action Discord inconnue.",
                    ephemeral=True,
                )
                return

            resolved_discord_actions.add(torrent_hash)

            record_torrent_action(
                torrent_hash,
                action,
                source="discord",
                actor=str(interaction.user),
                details=response_message,
            )

            try:
                with _db_connect() as conn:
                    conn.execute("UPDATE discord_messages SET resolved=1 WHERE message_id=?",(str(getattr(interaction.message,"id","")),))
            except Exception: pass
            if interaction.message is not None:
                try: await interaction.message.delete()
                except Exception: log.exception("Impossible de supprimer le message Discord après décision.")
            await interaction.followup.send(response_message,ephemeral=True)

            log.info(
                "[%s] Décision Discord enregistrée : %s.",
                torrent_hash,
                action,
            )

        except HTTPException:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "⚠️ Ce torrent n'existe plus dans qBittorrent.",
                    ephemeral=True,
                )
            else:
                await interaction.followup.send(
                    "⚠️ Ce torrent n'existe plus dans qBittorrent.",
                    ephemeral=True,
                )

        except requests.RequestException:
            log.exception(
                "Erreur qBittorrent lors d'une interaction Discord."
            )
            await interaction.followup.send(
                "⚠️ Impossible de communiquer avec qBittorrent.",
                ephemeral=True,
            )

        except Exception as exc:
            log.exception(
                "Erreur lors d'une interaction Discord : %s",
                exc,
            )
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "⚠️ Impossible d'exécuter cette action.",
                        ephemeral=True,
                    )
                else:
                    await interaction.followup.send(
                        "⚠️ Impossible d'exécuter cette action.",
                        ephemeral=True,
                    )
            except Exception:
                pass

    return bot


def _discord_media_name(torrent: dict[str, Any], release: dict[str, Any] | None) -> str:
    rel=str((release or {}).get("title") or "").strip(); raw=str(torrent.get("name") or "").strip(); title=rel or raw or "Média inconnu"
    if (release or {}).get("source")=="Sonarr":
        m=re.search(r"(S\d{1,2}E\d{1,3}(?:[- &]?E\d{1,3})?)",raw,re.I)
        if m and m.group(1).upper() not in title.upper(): title=f"{title} — {m.group(1).upper()}"
    return title


def build_release_fields(torrent: dict[str, Any], release: dict[str, Any]) -> list[dict[str, Any]]:
    fields=[{"name":"Progression","value":f"{float(torrent.get('progress',0))*100:.2f}%","inline":True}]
    if release.get("source"): fields.append({"name":"Source","value":str(release["source"]),"inline":True})
    if release.get("indexer"): fields.append({"name":"Indexeur","value":str(release["indexer"])[:1024],"inline":True})
    return fields


def notify_no_french_forced(
    torrent: dict[str, Any],
) -> None:
    if not _setting_bool("notify_no_forced", True):
        return

    torrent_hash = str(
        torrent.get("hash", "")
    )

    release = wait_for_release_context(
        torrent_hash
    )

    # Aucun message Discord sans URL de la page du torrent.
    if release is None:
        log.warning(
            "[%s] Notification Discord non envoyée : "
            "URL du torrent indisponible.",
            torrent_hash,
        )
        return

    update_torrent_release_context(torrent_hash, release)

    fields = build_release_fields(
        torrent,
        release,
    )

    fields.extend(
        [
            {
                "name": "Action effectuée",
                "value": (
                    "⏸️ Le téléchargement a été mis en pause automatiquement."
                ),
                "inline": False,
            },
            {
                "name": "Que faire ?",
                "value": (
                    "Vérifie le torrent puis décide dans qBittorrent "
                    "si tu souhaites reprendre ou supprimer le téléchargement."
                ),
                "inline": False,
            },
        ]
    )

    send_discord_message(
        embeds=[
            {
                "title": "🚨 Aucune piste FR Forced détectée",
                "description": (
                    "ForcedFR a pu analyser le fichier, mais aucune piste "
                    "de sous-titres français forcés n'a été trouvée."
                ),
                "color": 15158332,
                "fields": fields,
                "footer": {
                    "text": (
                        "ForcedFR • Vérification manuelle recommandée"
                    ),
                },
            }
        ],
        torrent_hash=torrent_hash,
        release=release,
    )


def notify_profile_decision(torrent: dict[str, Any], title: str, description: str, action_text: str, release: dict[str, Any] | None = None, *, decision_required: bool = False) -> None:
    torrent_hash=str(torrent.get("hash",""))
    if release is None:
        try: release=wait_for_release_context(torrent_hash)
        except Exception: release=None
    if release is None: release={"source":None,"title":torrent.get("name"),"indexer":None,"tracker_url":None,"arr_item_url":None,"item_id":None}
    else: update_torrent_release_context(torrent_hash,release)
    fields=build_release_fields(torrent,release); fields.append({"name":"Action ForcedFR","value":action_text,"inline":False})
    media_name=_discord_media_name(torrent,release)
    embed={"title":f"{title} — {media_name}","description":description,"color":5763719,"fields":fields,"footer":{"text":"ForcedFR • Décision nécessaire" if decision_required else "ForcedFR • Notification"}}
    if release.get("poster_url"): embed["image"]={"url":str(release["poster_url"])}
    send_discord_message(embeds=[embed],torrent_hash=torrent_hash,release=release,decision_required=decision_required)


def notify_analysis_error(torrent: dict[str, Any], error: str, *, action_text: str, attempt: int | None = None, release: dict[str, Any] | None = None, decision_required: bool = False) -> None:
    torrent_hash=str(torrent.get("hash",""))
    if release is None:
        try: release=wait_for_release_context(torrent_hash)
        except Exception: release=None
    if release is None: release={"source":None,"title":torrent.get("name"),"indexer":None,"tracker_url":None,"arr_item_url":None,"item_id":None}
    else: update_torrent_release_context(torrent_hash,release)
    fields=build_release_fields(torrent,release); text=str(error).strip() or "Erreur inconnue"
    fields += [{"name":"Erreur d'analyse","value":f"```{text[:1000]}```","inline":False},{"name":"Action ForcedFR","value":action_text,"inline":False}]
    if attempt is not None: fields.append({"name":"Tentative","value":str(attempt),"inline":True})
    media_name=_discord_media_name(torrent,release)
    embed={"title":f"⚠️ Erreur d'analyse — {media_name}","description":"ForcedFR n'a pas pu déterminer le résultat de manière fiable.","color":16776960,"fields":fields,"footer":{"text":"ForcedFR • Erreur d'analyse"}}
    send_discord_message(embeds=[embed],torrent_hash=torrent_hash,release=release,decision_required=decision_required)


def apply_analysis_error_policy(torrent: dict[str, Any], error: str, error_count: int) -> bool:
    profile,release=_error_profile_for_torrent(torrent); policy=str(profile.get("error_action") or "resume")
    retries=max(1,min(20,int(profile.get("error_retries") or 5))); delay=max(1.0,min(3600.0,float(profile.get("error_retry_delay") or 30)))
    if policy in {"retry_pause_decision","retry_pause"} and error_count <= retries:
        notify_analysis_error(torrent,error,action_text=f"🔄 Nouvelle tentative dans {delay:g} seconde(s) ({error_count}/{retries}).",attempt=error_count,release=release); time.sleep(delay); return True
    if policy in {"retry_pause_decision","retry_pause"}:
        stop_torrent(str(torrent.get("hash",""))); text="⏸️ Après les tentatives prévues, le téléchargement reste en pause. Une décision est demandée dans Discord."; notify_analysis_error(torrent,error,action_text=text,attempt=error_count,release=release,decision_required=True)
    elif policy in {"retry_resume","retry_continue"}:
        start_torrent(str(torrent.get("hash",""))); text="▶️ Après les tentatives prévues, le téléchargement a été repris automatiquement."; notify_analysis_error(torrent,error,action_text=text,attempt=error_count,release=release)
    elif policy=="pause_decision":
        stop_torrent(str(torrent.get("hash",""))); text="⏸️ Le téléchargement reste en pause. Une décision est demandée dans Discord."; notify_analysis_error(torrent,error,action_text=text,attempt=error_count,release=release,decision_required=True)
    else:
        start_torrent(str(torrent.get("hash",""))); text="▶️ Le téléchargement a été repris automatiquement malgré l'erreur d'analyse."; notify_analysis_error(torrent,error,action_text=text,attempt=error_count,release=release)
    return False


# ============================================================
# RECHERCHE DES MKV
# ============================================================

def find_mkv_files(
    files: list[dict[str, Any]],
) -> list[dict[str, Any]]:

    mkvs = []

    for file in files:

        name = str(
            file.get(
                "name",
                "",
            )
        )

        if name.lower().endswith(".mkv"):
            mkvs.append(file)

    # Le plus gros MKV en premier
    return sorted(
        mkvs,
        key=lambda item: item.get(
            "size",
            0,
        ),
        reverse=True,
    )


def resolve_mkv_path(
    torrent: dict[str, Any],
    file_info: dict[str, Any],
) -> Path:

    content_path = Path(
        torrent.get(
            "content_path",
            "",
        )
    )

    save_path = Path(
        torrent.get(
            "save_path",
            "",
        )
    )

    file_name = Path(
        file_info["name"]
    )

    # --------------------------------------------------------
    # CAS 1
    # content_path pointe directement vers le MKV
    # --------------------------------------------------------

    if content_path.suffix.lower() == ".mkv":
        return content_path

    # --------------------------------------------------------
    # CAS 2
    # save_path + chemin relatif du fichier
    # --------------------------------------------------------

    candidate = save_path / file_name

    if candidate.exists():
        return candidate

    # --------------------------------------------------------
    # CAS 3
    # content_path + nom du MKV
    # --------------------------------------------------------

    candidate = content_path / file_name.name

    if candidate.exists():
        return candidate

    # --------------------------------------------------------
    # CAS 4
    # Recherche récursive dans le dossier
    # --------------------------------------------------------

    if content_path.is_dir():

        matches = list(
            content_path.rglob(
                file_name.name
            )
        )

        if matches:
            return matches[0]

    # --------------------------------------------------------
    # Dernier recours
    # --------------------------------------------------------

    return save_path / file_name


# ============================================================
# ERREUR TEMPORAIRE FFPROBE
# ============================================================

class IncompleteMKVError(RuntimeError):
    """Le MKV est encore incomplet et doit être réessayé."""


# ============================================================
# FFPROBE
# ============================================================

def run_ffprobe(
    file_path: Path,
) -> dict[str, Any]:

    if not file_path.exists():
        raise FileNotFoundError(
            f"Fichier introuvable : {file_path}"
        )

    if not file_path.is_file():
        raise RuntimeError(
            f"Le chemin n'est pas un fichier : {file_path}"
        )

    command = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        (
            "stream=index,codec_type:"
            "stream_disposition:"
            "stream_tags=language,title"
        ),
        "-of",
        "json",
        str(file_path),
    ]

    log.info(
        "[FFPROBE] Analyse : %s",
        file_path,
    )

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=60,
    )

    if result.returncode != 0:

        error = result.stderr.strip()

        # Erreurs normales pendant le téléchargement :
        # le fichier existe déjà mais son en-tête MKV n'est
        # pas encore entièrement disponible.
        temporary_markers = (
            "EBML header parsing failed",
            "Invalid data found when processing input",
            "invalid as first byte of an EBML number",
            "End of file",
            "Input/output error",
        )

        if any(
            marker.lower() in error.lower()
            for marker in temporary_markers
        ):
            raise IncompleteMKVError(
                error or "MKV encore incomplet."
            )

        # Toute autre erreur est considérée comme une
        # vraie erreur d'analyse.
        raise RuntimeError(
            error or "ffprobe a échoué."
        )

    return json.loads(
        result.stdout or '{"streams":[]}'
    )


# ============================================================
# DÉTECTION FR FORCED
# ============================================================

def detect_french_forced(
    probe: dict[str, Any],
) -> dict[str, Any]:

    subtitles = []
    forced_french = []

    for stream in probe.get(
        "streams",
        [],
    ):

        if stream.get(
            "codec_type"
        ) != "subtitle":
            continue

        tags = stream.get(
            "tags"
        ) or {}

        disposition = stream.get(
            "disposition"
        ) or {}

        language = str(
            tags.get(
                "language",
                "",
            )
        ).lower()

        title = str(
            tags.get(
                "title",
                "",
            )
        )

        title_lower = title.lower()

        french = language in {
            "fr",
            "fra",
            "fre",
        }

        forced_flag = (
            int(
                disposition.get(
                    "forced",
                    0,
                )
            )
            == 1
        )

        forced_title = any(
            marker in title_lower
            for marker in (
                "forced",
                "force",
                "forcé",
            )
        )

        subtitle = {
            "index": stream.get(
                "index"
            ),
            "language": language or None,
            "title": title or None,
            "french": french,
            "forced_flag": forced_flag,
            "forced_title": forced_title,
        }

        subtitles.append(
            subtitle
        )

        if french and (
            forced_flag
            or forced_title
        ):
            forced_french.append(
                subtitle
            )

    return {
        "forced_french": bool(
            forced_french
        ),
        "subtitles": subtitles,
        "forced_tracks": forced_french,
    }


# ============================================================
# ANALYSE D'UN TORRENT
# ============================================================

def analyze_torrent(
    torrent_hash: str,
) -> dict[str, Any]:

    torrent = get_torrent(
        torrent_hash
    )

    files = get_torrent_files(
        torrent_hash
    )

    mkvs = find_mkv_files(
        files
    )

    if not mkvs:
        raise RuntimeError(
            "Aucun fichier MKV trouvé."
        )

    results = []

    for mkv in mkvs:

        path = resolve_mkv_path(
            torrent,
            mkv,
        )

        # Le fichier peut ne pas encore exister.
        if not path.exists():
            raise FileNotFoundError(
                f"MKV pas encore disponible : {path}"
            )

        if not path.is_file():
            raise RuntimeError(
                f"Le chemin du MKV est un dossier : {path}"
            )

        probe = run_ffprobe(
            path
        )

        detection = detect_french_forced(
            probe
        )

        results.append(
            {
                "path": str(path),
                "size": mkv.get("size"),
                "progress": mkv.get(
                    "progress",
                    0,
                ),
                "detection": detection,
                "ffprobe": probe,
            }
        )

    forced_french = any(
        result["detection"]["forced_french"]
        for result in results
    )

    return {
        "torrent": {
            "hash": torrent["hash"],
            "name": torrent["name"],
            "progress": torrent["progress"],
            "content_path": torrent["content_path"],
            "f_l_piece_prio": torrent.get(
                "f_l_piece_prio"
            ),
        },
        "forced_french": forced_french,
        "files": results,
    }


# ============================================================
# ANALYSE AVEC PRIORITÉ TEMPORAIRE
# ============================================================

def analyze_with_temporary_priority(
    torrent_hash: str,
) -> dict[str, Any]:

    torrent = get_torrent(
        torrent_hash
    )

    initial_priority = bool(
        torrent.get(
            "f_l_piece_prio",
            False,
        )
    )

    priority_changed = False

    try:

        # ----------------------------------------------------
        # Activation IMMÉDIATE
        # ----------------------------------------------------

        if not initial_priority:

            log.info(
                "[%s] Activation immédiate "
                "f_l_piece_prio.",
                torrent_hash,
            )

            toggle_first_last_piece_priority(
                torrent_hash
            )

            priority_changed = True

        # ----------------------------------------------------
        # Analyse
        # ----------------------------------------------------

        return analyze_torrent(
            torrent_hash
        )

    finally:

        # ----------------------------------------------------
        # Toujours désactiver après analyse
        # ----------------------------------------------------

        if priority_changed:

            log.info(
                "[%s] Désactivation f_l_piece_prio.",
                torrent_hash,
            )

            try:

                toggle_first_last_piece_priority(
                    torrent_hash
                )

            except Exception:

                log.exception(
                    "[%s] Impossible de désactiver "
                    "f_l_piece_prio.",
                    torrent_hash,
                )


# ============================================================
# TRAITEMENT NOUVEAU TORRENT
# ============================================================

# ============================================================
# IA : BRAVE SEARCH + GROQ
# ============================================================

def _ai_cache_key(media_type: str, title: str) -> str:
    import hashlib
    base = re.sub(r"\s+", " ", f"{media_type}|{title}".strip().lower())
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _ai_cache_get(media_type: str, title: str) -> dict[str, Any] | None:
    key = _ai_cache_key(media_type, title)
    with _db_connect() as conn:
        row = conn.execute("SELECT * FROM ai_research_cache WHERE cache_key=?", (key,)).fetchone()
    if not row:
        return None
    data = dict(row)
    try:
        data["sources"] = json.loads(data.get("sources") or "[]")
    except Exception:
        data["sources"] = []
    recommendation = str(data.get("recommendation") or data.get("result") or "").strip().upper()
    model = str(data.get("model") or "").strip()
    researched_at = float(data.get("researched_at") or data.get("searched_at") or 0)
    current_model = str(GROQ_MODEL or _setting("config_GROQ_MODEL", "openai/gpt-oss-120b")).strip()
    if recommendation not in {"NÉCESSAIRE", "NON NÉCESSAIRE", "INDETERMINÉ"}:
        return None
    if researched_at <= 0:
        return None
    # Les anciens résultats Gemini restent dans l'historique mais ne doivent pas
    # être réutilisés après le passage à Groq.
    if model and current_model and model != current_model:
        log.info("Cache IA ignoré : ancien modèle %s (modèle actuel %s).", model, current_model)
        return None
    data["recommendation"] = recommendation
    data["researched_at"] = researched_at
    return data



def _ai_cache_put(media_type: str, title: str, result: dict[str, Any]) -> None:
    key = _ai_cache_key(media_type, title)
    recommendation = str(result.get("recommendation") or "INDETERMINÉ")
    confidence = float(result.get("confidence") or 0)
    justification = str(result.get("justification") or "")
    sources = json.dumps(result.get("sources") or [], ensure_ascii=False)
    model = str(result.get("model") or GROQ_MODEL)
    now = time.time()
    year = str(result.get("year") or "")
    brave_queries = json.dumps(result.get("brave_queries") or [], ensure_ascii=False)

    # Compatibilité avec toutes les structures ai_research_cache rencontrées
    # pendant les versions 2.6.x. Certaines anciennes bases imposent encore
    # result, year, researched_at et brave_queries en NOT NULL.
    values = {
        "cache_key": key,
        "media_type": media_type,
        "title": title,
        "year": year,
        "result": recommendation,
        "recommendation": recommendation,
        "confidence": confidence,
        "justification": justification,
        "sources": sources,
        "model": model,
        "researched_at": now,
        "searched_at": now,
        "brave_queries": brave_queries,
    }

    with _db_connect() as conn:
        columns_info = conn.execute("PRAGMA table_info(ai_research_cache)").fetchall()
        if not columns_info:
            raise RuntimeError("Table ai_research_cache introuvable après initialisation SQLite.")

        columns = [row[1] for row in columns_info]
        # Toute colonne NOT NULL sans valeur explicite doit recevoir une valeur
        # sûre, afin qu'une évolution de schéma future ne casse pas le cache.
        for row in columns_info:
            name = row[1]
            notnull = bool(row[3])
            default = row[4]
            pk = bool(row[5])
            if name not in values and notnull and not pk and default is None:
                ctype = (row[2] or "").upper()
                values[name] = 0 if any(t in ctype for t in ("INT", "REAL", "NUM", "FLOA", "DOUB")) else ""

        insert_columns = [c for c in columns if c in values]
        if "cache_key" not in insert_columns:
            raise RuntimeError("Table ai_research_cache sans colonne cache_key.")
        placeholders = ",".join("?" for _ in insert_columns)
        quoted = ",".join(f'"{c}"' for c in insert_columns)
        update_columns = [c for c in insert_columns if c != "cache_key"]
        if update_columns:
            updates = ",".join(f'"{c}"=excluded."{c}"' for c in update_columns)
            sql = f"INSERT INTO ai_research_cache ({quoted}) VALUES ({placeholders}) ON CONFLICT(cache_key) DO UPDATE SET {updates}"
        else:
            sql = f"INSERT OR IGNORE INTO ai_research_cache ({quoted}) VALUES ({placeholders})"
        conn.execute(sql, tuple(values[c] for c in insert_columns))



def _brave_search(title: str, media_type: str) -> list[dict[str, str]]:
    key=BRAVE_SEARCH_API_KEY or _setting("config_BRAVE_SEARCH_API_KEY","")
    if not key: raise RuntimeError("Clé Brave Search non configurée.")
    count=max(1,min(10,int(_setting("brave_results_count",str(BRAVE_RESULTS_COUNT)) or BRAVE_RESULTS_COUNT)))
    queries=[f'"{title}" Blu-ray France subtitles forced',f'"{title}" DVD France sous-titres forced',f'"{title}" "French forced subtitles" Blu-ray',f'"{title}" Blu-ray.com subtitles']
    preferred=("blu-ray.com","dvdcompare.net")
    results=[]; seen=set()
    for q in queries:
        r=requests.get("https://api.search.brave.com/res/v1/web/search",headers={"Accept":"application/json","X-Subscription-Token":key},params={"q":q,"count":max(count,8)},timeout=15); r.raise_for_status(); data=r.json()
        batch=data.get("web",{}).get("results",[]) or []
        batch=sorted(batch,key=lambda x:0 if any(d in str(x.get("url") or "").lower() for d in preferred) else 1)
        for item in batch:
            url=str(item.get("url") or "").strip()
            if not url or url in seen: continue
            seen.add(url); results.append({"title":str(item.get("title") or ""),"url":url,"description":str(item.get("description") or "")[:1200]})
            if len(results)>=count: return results
    if not results: raise RuntimeError("Brave n'a retourné aucun résultat pertinent.")
    return results

def _groq_generate(media_type: str, title: str, search_results: list[dict[str,str]], ffprobe_summary: dict[str,Any] | None = None) -> dict[str,Any]:
    key=GROQ_API_KEY or _setting("config_GROQ_API_KEY", "")
    model=GROQ_MODEL or _setting("config_GROQ_MODEL", "openai/gpt-oss-120b")
    if not key: raise RuntimeError("Clé Groq non configurée.")
    sources_text="\n\n".join(f"SOURCE {i+1}\nTitre: {x['title']}\nURL: {x['url']}\nRésumé: {x['description']}" for i,x in enumerate(search_results))
    probe=json.dumps(ffprobe_summary or {}, ensure_ascii=False)[:5000]
    prompt=f"""Analyse si l'œuvre « {title} » ({media_type}) nécessite normalement des sous-titres français Forced. Appuie-toi en priorité sur les éditions physiques françaises (Blu-ray/DVD), Blu-ray.com, DVDCompare et autres bases spécialisées home-cinéma. Les sites de streaming ne doivent pas être considérés comme preuve principale. Recherche la langue des dialogues, les passages en langue étrangère, les sous-titres français de ces passages, les informations d’édition et les différences éventuelles entre versions. Ne confonds pas une absence de piste Forced sur une release avec une absence de besoin de Forced. En cas d’informations contradictoires ou d’édition impossible à identifier, réponds INDETERMINÉ.\n\nInformations FFprobe:\n{probe}\n\nRésultats Brave:\n{sources_text}\n\nRetourne une décision parmi NÉCESSAIRE, NON NÉCESSAIRE ou INDETERMINÉ avec une confiance de 0 à 1 et une justification courte."""
    schema={"type":"object","properties":{"recommendation":{"type":"string","enum":["NÉCESSAIRE","NON NÉCESSAIRE","INDETERMINÉ"]},"confidence":{"type":"number","minimum":0,"maximum":1},"justification":{"type":"string"},"sources":{"type":"array","items":{"type":"object","properties":{"title":{"type":"string"},"url":{"type":"string"}},"required":["title","url"],"additionalProperties":False}}},"required":["recommendation","confidence","justification","sources"],"additionalProperties":False}
    payload={"model":model,"messages":[{"role":"system","content":"Tu es un analyste expert des sous-titres Forced. Réponds uniquement avec le JSON demandé."},{"role":"user","content":prompt}],"temperature":0,"response_format":{"type":"json_schema","json_schema":{"name":"forcedfr_decision","strict":True,"schema":schema}},"max_tokens":700}
    last=None
    for attempt in range(1,max(1,AI_RETRIES)+1):
        try:
            r=requests.post("https://api.groq.com/openai/v1/chat/completions",headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"},json=payload,timeout=60)
            if r.status_code in (429,500,502,503,504):
                last=RuntimeError(f"Groq HTTP {r.status_code}")
                if attempt < AI_RETRIES:
                    delay=min(30,2**(attempt-1)); log.warning("Groq temporairement indisponible (%s), nouvelle tentative dans %ss (%s/%s).",r.status_code,delay,attempt,AI_RETRIES); time.sleep(delay); continue
            r.raise_for_status(); content=r.json()["choices"][0]["message"]["content"]
            data=json.loads(content)
            data["model"]=model
            return data
        except Exception as exc:
            last=exc
            if attempt < AI_RETRIES:
                delay=min(30,2**(attempt-1)); log.warning("Erreur Groq (%s), nouvelle tentative dans %ss (%s/%s).",exc,delay,attempt,AI_RETRIES); time.sleep(delay)
    raise RuntimeError(f"Groq indisponible après {AI_RETRIES} tentative(s): {last}")


def research_forced_fr_with_ai(torrent: dict[str,Any], media_type: str, title: str, ffprobe_result: dict[str,Any] | None = None, ignore_cache: bool = False) -> dict[str,Any]:
    cached=None if ignore_cache else _ai_cache_get(media_type,title)
    if cached:
        log.info("[%s] 🤖 Décision IA récupérée depuis le cache : %s",torrent.get("hash",""),cached.get("recommendation"))
        return cached
    if not AI_ENABLED or not _setting_bool("ai_enabled", True):
        raise RuntimeError("IA désactivée dans ForcedFR.")
    log.info("[%s] 🌐 Recherche Brave pour la décision IA : %s",torrent.get("hash",""),title)
    sources=_brave_search(title,media_type)
    log.info("[%s] 🌐 Brave : %s résultat(s).",torrent.get("hash",""),len(sources))
    result=_groq_generate(media_type,title,sources,ffprobe_result)
    result["sources"]=result.get("sources") or [{"title":x["title"],"url":x["url"]} for x in sources[:5]]
    _ai_cache_put(media_type,title,result)
    log.info("[%s] 🤖 Groq : %s (confiance %.0f%%).",torrent.get("hash",""),result.get("recommendation"),float(result.get("confidence") or 0)*100)
    return result


def _ai_confidence_threshold_key(media_type: str, decision: str) -> str | None:
    if decision not in {"NÉCESSAIRE", "NON NÉCESSAIRE"}:
        return None
    suffix = "series" if media_type == "Série" else "films"
    prefix = "ai_necessary_min_confidence" if decision == "NÉCESSAIRE" else "ai_not_necessary_min_confidence"
    return f"{prefix}_{suffix}"


def _normalize_ai_decision(media_type: str, result: dict[str, Any]) -> tuple[str, float, bool]:
    decision = str(result.get("recommendation") or "INDETERMINÉ").strip().upper()
    if decision not in {"NÉCESSAIRE", "NON NÉCESSAIRE", "INDETERMINÉ"}:
        decision = "INDETERMINÉ"
    confidence = max(0.0, min(1.0, float(result.get("confidence") or 0)))
    key = _ai_confidence_threshold_key(media_type, decision)
    if key:
        threshold = max(0.0, min(100.0, float(_setting(key, "70") or 70))) / 100.0
        if confidence < threshold:
            log.info("Décision IA %s convertie en INDETERMINÉ : confiance %.0f%% < seuil %.0f%%.", decision, confidence*100, threshold*100)
            return "INDETERMINÉ", confidence, False
    return decision, confidence, True


def _apply_ai_action(torrent: dict[str,Any], profile: dict[str,Any], decision: str, ai_result: dict[str,Any], release: dict[str,Any] | None) -> str:
    action_key={"NÉCESSAIRE":"ai_necessary_action","NON NÉCESSAIRE":"ai_not_necessary_action","INDETERMINÉ":"ai_indeterminate_action"}.get(decision,"ai_indeterminate_action")
    action=str(profile.get(action_key) or "pause_decision"); h=str(torrent.get("hash","")); confidence=float(ai_result.get("confidence") or 0); justification=str(ai_result.get("justification") or "")
    if action=="resume": start_torrent(h); text=f"▶️ Téléchargement repris après décision IA : **{decision}** ({confidence:.0%})."; record_torrent_action(h,"resume",source="ai",details=f"Décision IA {decision}: {justification[:500]}"); notify=False
    elif action=="resume_notify": start_torrent(h); text=f"▶️ Téléchargement repris après décision IA : **{decision}** ({confidence:.0%})."; record_torrent_action(h,"resume",source="ai",details=f"Décision IA {decision}: {justification[:500]}"); notify=True
    else: stop_torrent(h); text=f"⏸️ Téléchargement laissé en pause après décision IA : **{decision}** ({confidence:.0%})."; record_torrent_action(h,"auto_pause",source="ai",details=f"Décision IA {decision}: {justification[:500]}"); notify=True
    if notify:
        fields=f"**Décision :** {decision}\n**Confiance :** {confidence:.0%}\n\n{justification or 'Aucune justification fournie.'}"; notify_profile_decision(torrent,"🤖 Décision IA ForcedFR",fields,text,release,decision_required=(action=="pause_decision"))
    return text


def _apply_ai_failure(torrent: dict[str,Any], error: str, release: dict[str,Any] | None) -> str:
    h=str(torrent.get("hash","")); stop_torrent(h)
    text="⏸️ Le téléchargement reste en pause. Une décision est demandée dans Discord après l'échec de l'analyse IA/Web."
    record_torrent_action(h,"auto_pause",source="ai",details=str(error)[:1000])
    notify_analysis_error(torrent,error,action_text=text,release=release,decision_required=True)
    return text

def process_new_torrent(
    torrent_hash: str,
) -> None:

    if torrent_hash in processing_torrents:
        return

    processing_torrents.add(
        torrent_hash
    )

    log.info(
        "========================================"
    )

    log.info(
        "[%s] 🆕 Nouveau torrent détecté.",
        torrent_hash,
    )

    started_at = time.time()
    real_error_count = 0

    # --------------------------------------------------------
    # ACTIVER LA PRIORITÉ IMMÉDIATEMENT
    # --------------------------------------------------------

    priority_enabled = False

    try:

        torrent = get_torrent(
            torrent_hash
        )

        ignored_tag = torrent_has_ignored_tag(torrent)
        if ignored_tag:
            log.info(
                "[%s] Analyse ignorée : étiquette qBittorrent « %s » configurée dans ForcedFR.",
                torrent_hash,
                ignored_tag,
            )
            return

        if not torrent.get(
            "f_l_piece_prio",
            False,
        ):

            toggle_first_last_piece_priority(
                torrent_hash
            )

            priority_enabled = True

            log.info(
                "[%s] ⚡ Priorité premières/dernières "
                "pièces activée.",
                torrent_hash,
            )

        # ----------------------------------------------------
        # Boucle d'analyse
        # ----------------------------------------------------

        while True:

            elapsed = (
                time.time()
                - started_at
            )

            if elapsed >= ANALYSIS_TIMEOUT:

                log.error(
                    "[%s] ⚠️ ERREUR RÉELLE : timeout d'analyse "
                    "après %ss. Application de la politique du profil.",
                    torrent_hash,
                    ANALYSIS_TIMEOUT,
                )

                error_text = f"Timeout après {ANALYSIS_TIMEOUT} secondes."
                record_analysis_history(torrent_hash, str(torrent.get("name", "")), "error", error_text)

                # Le timeout est une véritable erreur d'analyse : il passe
                # donc lui aussi par la politique configurée dans le profil
                # Films / Séries, exactement comme les autres erreurs FFprobe.
                if apply_analysis_error_policy(torrent, error_text, real_error_count + 1):
                    real_error_count += 1
                    continue
                real_error_count += 1
                return

            try:

                torrent = get_torrent(
                    torrent_hash
                )

                log.info(
                    "[%s] %s — %.2f%%",
                    torrent_hash,
                    torrent.get(
                        "name",
                        "",
                    ),
                    float(
                        torrent.get(
                            "progress",
                            0,
                        )
                    ) * 100,
                )

                # ------------------------------------------------
                # Tentative immédiate de ffprobe
                # ------------------------------------------------

                result = analyze_torrent(
                    torrent_hash
                )

                if result[
                    "forced_french"
                ]:

                    log.info(
                        "[%s] ✅ FR Forced détecté.",
                        torrent_hash,
                    )
                    profile, release = _torrent_profile_for_torrent(torrent)
                    found_action = str(profile.get("found_action") or "resume")
                    name = str(torrent.get("name", ""))
                    if found_action == "pause_decision":
                        stop_torrent(torrent_hash); action_text="⏸️ Téléchargement laissé en pause. Une décision est demandée dans Discord."
                        record_torrent_action(torrent_hash,"auto_pause",source="forcedfr",details="Pause après détection d'une piste FR Forced.")
                        notify_profile_decision(torrent,"🇫🇷 Forced FR détecté","Une piste française Forced a été détectée.",action_text,release,decision_required=True)
                    elif found_action == "resume_notify":
                        start_torrent(torrent_hash); action_text="▶️ Téléchargement repris. Forced FR détecté."
                        record_torrent_action(torrent_hash,"resume",source="forcedfr",details="Téléchargement poursuivi après détection d'une piste FR Forced.")
                        notify_profile_decision(torrent,"🇫🇷 Forced FR détecté","Une piste française Forced a été détectée.",action_text,release)
                    else:
                        start_torrent(torrent_hash); action_text="▶️ Téléchargement repris. Forced FR détecté."
                        record_torrent_action(torrent_hash,"resume",source="forcedfr",details="Téléchargement poursuivi après détection d'une piste FR Forced.")
                    record_analysis_history(torrent_hash,name,"forced_found",action_text)
                    return

                # ------------------------------------------------
                # MKV lisible mais pas de FR Forced
                # ------------------------------------------------

                log.warning(
                    "[%s] ❌ Aucune piste "
                    "FR Forced détectée.",
                    torrent_hash,
                )

                profile, release = _torrent_profile_for_torrent(torrent)
                name = str(torrent.get("name", ""))

                # La mécanique v2.6.5 reste inchangée jusqu'à l'analyse FFprobe.
                # Une fois le MKV lisible et sans FR Forced, on met le torrent en pause
                # avant la recherche externe afin que Brave/Groq ne laisse jamais le
                # téléchargement se poursuivre pendant la décision.
                stop_torrent(torrent_hash)
                record_torrent_action(torrent_hash, "auto_pause", source="ai", details="Pause avant recherche Brave/Groq après absence de FR Forced.")
                log.info("[%s] ⏸️ Torrent mis en pause avant la décision IA.", torrent_hash)

                media_type = str(profile.get("media_type") or ("Série" if re.search(r"S\d{1,2}E\d{1,3}", name, re.I) else "Film"))
                missing_action = str(profile.get("torrent_missing_action") or "scan_ai")
                if missing_action != "scan_ai":
                    if missing_action == "resume":
                        start_torrent(torrent_hash); action_text="▶️ Téléchargement repris. Aucune piste FR Forced détectée."; record_torrent_action(torrent_hash,"resume",source="forcedfr",details="Reprise après absence de FR Forced."); record_analysis_history(torrent_hash,name,"no_forced",action_text); return
                    if missing_action == "resume_notify":
                        start_torrent(torrent_hash); action_text="▶️ Téléchargement repris. Aucune piste FR Forced détectée."; record_torrent_action(torrent_hash,"resume",source="forcedfr",details="Reprise après absence de FR Forced."); notify_profile_decision(torrent,"🇫🇷 Forced FR absent","Aucune piste française Forced n'a été détectée.",action_text,release); record_analysis_history(torrent_hash,name,"no_forced",action_text); return
                    stop_torrent(torrent_hash); action_text="⏸️ Téléchargement laissé en pause. Une décision est demandée dans Discord."; record_torrent_action(torrent_hash,"auto_pause",source="forcedfr",details="Pause après absence de FR Forced."); notify_profile_decision(torrent,"🇫🇷 Forced FR absent","Aucune piste française Forced n'a été détectée.",action_text,release,decision_required=True); record_analysis_history(torrent_hash,name,"no_forced",action_text); return
                stop_torrent(torrent_hash); record_torrent_action(torrent_hash,"auto_pause",source="ai",details="Pause avant recherche Brave/Groq après absence de FR Forced."); log.info("[%s] ⏸️ Torrent laissé en pause pendant la décision IA.",torrent_hash)

                try:
                    ai_result = research_forced_fr_with_ai(torrent, media_type, name, result)
                    decision, normalized_confidence, accepted = _normalize_ai_decision(media_type, ai_result)
                    ai_result["effective_recommendation"] = decision
                    ai_result["confidence"] = normalized_confidence
                    action_text = _apply_ai_action(torrent, profile, decision, ai_result, release)
                    record_analysis_history(torrent_hash, name, "no_forced", f"🤖 {decision} — {action_text}")
                except Exception as ai_exc:
                    log.error("[%s] ⚠️ Recherche IA/Web impossible : %s", torrent_hash, ai_exc)
                    action_text = _apply_ai_failure(torrent, str(ai_exc), release)
                    record_analysis_history(torrent_hash, name, "error", f"IA/Web indisponible — {action_text}")
                return

            except FileNotFoundError:

                log.info(
                    "[%s] MKV pas encore disponible. "
                    "Nouvelle tentative.",
                    torrent_hash,
                )

            except IncompleteMKVError as exc:

                log.info(
                    "[%s] MKV encore incomplet : %s",
                    torrent_hash,
                    exc,
                )

            except subprocess.TimeoutExpired as exc:

                log.error(
                    "[%s] ⚠️ ERREUR RÉELLE : ffprobe timeout. "
                    "Le téléchargement continue.",
                    torrent_hash,
                )

                real_error_count += 1
                error_text = "ffprobe a dépassé son délai de 60 secondes."
                record_analysis_history(torrent_hash, str(torrent.get("name", "")), "error", error_text)
                if apply_analysis_error_policy(torrent,error_text,real_error_count): continue
                return

            except RuntimeError as exc:

                log.error(
                    "[%s] ⚠️ ERREUR RÉELLE D'ANALYSE : %s",
                    torrent_hash,
                    exc,
                )

                real_error_count += 1
                error_text = str(exc)
                record_analysis_history(torrent_hash, str(torrent.get("name", "")), "error", error_text)
                if apply_analysis_error_policy(torrent,error_text,real_error_count): continue
                return

            except Exception as exc:

                log.exception(
                    "[%s] ⚠️ ERREUR RÉELLE INATTENDUE : %s "
                    "Le téléchargement continue.",
                    torrent_hash,
                    exc,
                )

                real_error_count += 1
                error_text = str(exc)
                record_analysis_history(torrent_hash, str(torrent.get("name", "")), "error", error_text)
                if apply_analysis_error_policy(torrent,error_text,real_error_count): continue
                return

            time.sleep(ANALYSIS_POLL_SECONDS)

    finally:

        # --------------------------------------------------------
        # TOUJOURS désactiver la priorité
        # --------------------------------------------------------

        if priority_enabled:

            try:

                # Vérification avant toggle pour éviter
                # de changer un état déjà modifié ailleurs.

                torrent = get_torrent(
                    torrent_hash
                )

                if torrent.get(
                    "f_l_piece_prio",
                    False,
                ):

                    toggle_first_last_piece_priority(
                        torrent_hash
                    )

                    log.info(
                        "[%s] 📴 f_l_piece_prio désactivé.",
                        torrent_hash,
                    )

            except Exception:

                log.exception(
                    "[%s] Impossible de désactiver "
                    "f_l_piece_prio.",
                    torrent_hash,
                )

        processing_torrents.discard(
            torrent_hash
        )

        log.info(
            "[%s] Fin du traitement.",
            torrent_hash,
        )

        log.info(
            "========================================"
        )



# ============================================================
# SURVEILLANCE qBITTORRENT
# ============================================================

async def monitor_qbittorrent() -> None:

    global previous_torrents

    # --------------------------------------------------------
    # Stabilisation de qBittorrent au démarrage
    # --------------------------------------------------------
    # Après un redémarrage du serveur, qBittorrent peut restaurer
    # ses torrents progressivement. On attend donc que la liste
    # soit inchangée pendant 3 vérifications espacées de 5 secondes
    # avant d'enregistrer la référence initiale.

    log.info(
        "Attente de stabilisation de qBittorrent..."
    )

    stable_snapshot: set[str] | None = None
    stable_checks = 0

    while stable_checks < STARTUP_STABLE_CHECKS_REQUIRED:

        try:

            torrents = get_torrents()

            current_snapshot = {
                torrent["hash"]
                for torrent in torrents
                if torrent.get("hash")
            }

            if stable_snapshot is None:

                stable_snapshot = current_snapshot
                stable_checks = 0

                log.info(
                    "Liste détectée : %d torrent(s). "
                    "Vérification de stabilité en cours...",
                    len(current_snapshot),
                )

            elif current_snapshot == stable_snapshot:

                stable_checks += 1

                log.info(
                    "Liste stable (%d/%d) : %d torrent(s).",
                    stable_checks,
                    STARTUP_STABLE_CHECKS_REQUIRED,
                    len(current_snapshot),
                )

            else:

                log.info(
                    "Liste modifiée (%d → %d torrents). "
                    "Nouvelle période de stabilisation.",
                    len(stable_snapshot),
                    len(current_snapshot),
                )

                stable_snapshot = current_snapshot
                stable_checks = 0

        except Exception:

            log.warning(
                "qBittorrent pas encore prêt. "
                "Nouvelle tentative dans %ss.",
                STARTUP_STABILITY_CHECK_SECONDS,
            )

            stable_snapshot = None
            stable_checks = 0

        if stable_checks < STARTUP_STABLE_CHECKS_REQUIRED:
            await asyncio.sleep(
                STARTUP_STABILITY_CHECK_SECONDS
            )

    previous_torrents = stable_snapshot or set()

    log.info(
        "qBittorrent stabilisé. %d torrent(s) présents au démarrage "
        "et ignorés.",
        len(previous_torrents),
    )

    log.info(
        "Surveillance qBittorrent démarrée "
        "(intervalle : %ss).",
        POLL_SECONDS,
    )

    # --------------------------------------------------------
    # Surveillance permanente
    # --------------------------------------------------------

    while True:

        try:

            torrents = get_torrents()

            current_torrents = {
                torrent["hash"]
                for torrent in torrents
                if torrent.get("hash")
            }

            new_torrents = (
                current_torrents
                - previous_torrents
            )

            if new_torrents:

                log.info(
                    "🔎 %d nouveau(x) torrent(s) détecté(s).",
                    len(new_torrents),
                )

                for torrent_hash in new_torrents:
                    torrent = next((t for t in torrents if t.get("hash") == torrent_hash), None)
                    ignored_tag = torrent_has_ignored_tag(torrent or {})
                    if ignored_tag:
                        log.info(
                            "[%s] Torrent ignoré : étiquette qBittorrent « %s » configurée dans ForcedFR.",
                            torrent_hash,
                            ignored_tag,
                        )
                        continue

                    asyncio.create_task(
                        asyncio.to_thread(
                            process_new_torrent,
                            torrent_hash,
                        )
                    )

            previous_torrents = (
                current_torrents
            )

        except Exception:

            log.exception(
                "Erreur dans la surveillance "
                "qBittorrent."
            )

        await asyncio.sleep(
            POLL_SECONDS
        )



async def _discord_lifecycle_loop() -> None:
    while True:
        try:
            if discord_bot is not None and discord_bot.is_ready() and DISCORD_CHANNEL_ID:
                now=time.time()
                with _db_connect() as conn: rows=conn.execute("SELECT * FROM discord_messages WHERE resolved=0 AND deleted_at IS NULL ORDER BY sent_at ASC LIMIT 200").fetchall()
                channel=discord_bot.get_channel(int(DISCORD_CHANNEL_ID))
                if channel is None: channel=await discord_bot.fetch_channel(int(DISCORD_CHANNEL_ID))
                for row in rows:
                    age=now-float(row["sent_at"])
                    try: msg=await channel.fetch_message(int(row["message_id"]))
                    except Exception:
                        with _db_connect() as conn: conn.execute("UPDATE discord_messages SET deleted_at=? WHERE message_id=?",(now,str(row["message_id"])))
                        continue
                    if int(row["decision_required"]):
                        if age>=48*3600:
                            try: await msg.delete()
                            except Exception: pass
                            with _db_connect() as conn: conn.execute("UPDATE discord_messages SET deleted_at=? WHERE message_id=?",(now,str(row["message_id"])))
                        elif age>=24*3600 and not int(row["reminder_sent"]):
                            try:
                                embeds=[]
                                for e in msg.embeds:
                                    d=e.to_dict(); d["description"]=(d.get("description") or "")+"\n\n⏰ **Rappel : une décision est toujours nécessaire.**"; embeds.append(d)
                                await msg.edit(embeds=embeds or None)
                            except Exception: log.exception("Impossible de relancer le message Discord %s.",row["message_id"])
                            with _db_connect() as conn: conn.execute("UPDATE discord_messages SET reminder_sent=1 WHERE message_id=?",(str(row["message_id"]),))
                    elif age>=24*3600:
                        try: await msg.delete()
                        except Exception: pass
                        with _db_connect() as conn: conn.execute("UPDATE discord_messages SET deleted_at=? WHERE message_id=?",(now,str(row["message_id"])))
        except asyncio.CancelledError: raise
        except Exception: log.exception("Erreur dans le nettoyage des messages Discord.")
        await asyncio.sleep(300)

# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
async def startup() -> None:

    global MAIN_EVENT_LOOP, discord_bot, discord_lifecycle_task
    MAIN_EVENT_LOOP = asyncio.get_running_loop()

    log.info(
        "========================================"
    )

    log.info(
        "ForcedFR démarrage."
    )

    log.info(
        "qBittorrent : %s",
        QB_HOST,
    )

    if not QB_USERNAME or not QB_PASSWORD:

        log.warning(
            "QB_USERNAME ou QB_PASSWORD "
            "non configuré."
        )

    else:

        try:

            qb_login()

        except Exception:

            log.exception(
                "Connexion initiale qBittorrent échouée."
            )

    if not DISCORD_BOT_TOKEN:
        log.warning("DISCORD_BOT_TOKEN non configuré : les boutons interactifs Discord sont indisponibles.")
    elif not DISCORD_CHANNEL_ID:
        log.warning("DISCORD_CHANNEL_ID non configuré : les notifications Discord sont indisponibles.")
    elif discord is None:
        log.error("Le module discord.py est absent. Ajoute discord.py aux dépendances du conteneur.")
    else:
        discord_bot = build_discord_bot()
        asyncio.create_task(discord_bot.start(DISCORD_BOT_TOKEN))

    discord_lifecycle_task=asyncio.create_task(_discord_lifecycle_loop())

    asyncio.create_task(
        monitor_qbittorrent()
    )

    log.info(
        "========================================"
    )


# ============================================================
# SHUTDOWN
# ============================================================

@app.on_event("shutdown")
async def shutdown() -> None:
    global discord_bot, discord_lifecycle_task
    if discord_lifecycle_task is not None:
        discord_lifecycle_task.cancel()
        discord_lifecycle_task=None
    if discord_bot is not None:
        try:
            await discord_bot.close()
        except Exception:
            log.exception("Erreur lors de l'arrêt du bot Discord.")


# ============================================================
# API
# ============================================================

@app.get("/health")
def health() -> dict[str, Any]:

    return {
        "status": "ok",
        "version": "2.8.6",
        "qbittorrent": QB_HOST,
        "monitoring": True,
        "poll_seconds": POLL_SECONDS,
    }


@app.get("/torrents")
def torrents() -> Any:

    try:

        return get_torrents()

    except Exception as exc:

        raise HTTPException(
            status_code=502,
            detail=f"Erreur qBittorrent : {exc}",
        )


@app.get(
    "/torrent/{torrent_hash}/inspect"
)
def inspect(
    torrent_hash: str,
) -> dict[str, Any]:

    try:

        torrent = get_torrent(
            torrent_hash
        )

        files = get_torrent_files(
            torrent_hash
        )

        piece_states = get_piece_states(
            torrent_hash
        )

        return {
            "torrent": torrent,
            "files": files,
            "piece_states": piece_states,
        }

    except HTTPException:
        raise

    except Exception as exc:

        raise HTTPException(
            status_code=502,
            detail=f"Erreur : {exc}",
        )


@app.get(
    "/torrent/{torrent_hash}/analyze"
)
def analyze(
    torrent_hash: str,
) -> dict[str, Any]:

    try:

        return analyze_with_temporary_priority(
            torrent_hash
        )

    except HTTPException:
        raise

    except FileNotFoundError as exc:

        raise HTTPException(
            status_code=404,
            detail=str(exc),
        )

    except subprocess.TimeoutExpired:

        raise HTTPException(
            status_code=504,
            detail="ffprobe a dépassé 60 secondes.",
        )

    except Exception as exc:

        raise HTTPException(
            status_code=502,
            detail=f"Analyse impossible : {exc}",
        )


@app.post(
    "/torrent/{torrent_hash}/pause"
)
def pause(
    torrent_hash: str,
) -> dict[str, bool]:

    try:

        stop_torrent(
            torrent_hash
        )

        return {
            "ok": True
        }

    except Exception as exc:

        raise HTTPException(
            status_code=502,
            detail=f"Erreur qBittorrent : {exc}",
        )


@app.post(
    "/torrent/{torrent_hash}/resume"
)
def resume(
    torrent_hash: str,
) -> dict[str, bool]:

    try:

        start_torrent(
            torrent_hash
        )

        return {
            "ok": True
        }

    except Exception as exc:

        raise HTTPException(
            status_code=502,
            detail=f"Erreur qBittorrent : {exc}",
        )

# ============================================================
# ============================================================
# V2.1 — SCAN DE BIBLIOTHÈQUE VIA RADARR / SONARR
# ============================================================

# Le scan de bibliothèque ne parcourt plus les volumes Docker. Radarr et Sonarr
# sont les sources de vérité : seuls les médias réellement connus par les *Arr
# sont proposés à FFprobe.
#
# Si les chemins renvoyés par Radarr/Sonarr ne correspondent pas directement aux
# chemins visibles dans le conteneur ForcedFR, un remappage optionnel peut être
# défini via ARR_PATH_MAPPINGS, au format JSON :
# {"/movies":"/data/Films", "/tv":"/data/Séries"}
ARR_PATH_MAPPINGS_RAW = os.getenv("ARR_PATH_MAPPINGS", "").strip()


def _load_arr_path_mappings() -> list[tuple[str, str]]:
    if not ARR_PATH_MAPPINGS_RAW:
        return []
    try:
        data = json.loads(ARR_PATH_MAPPINGS_RAW)
        if not isinstance(data, dict):
            raise ValueError("ARR_PATH_MAPPINGS doit être un objet JSON")
        mappings = [(str(k).rstrip("/"), str(v).rstrip("/")) for k, v in data.items()]
        return sorted(mappings, key=lambda x: len(x[0]), reverse=True)
    except Exception as exc:
        log.warning("[SCAN] ARR_PATH_MAPPINGS invalide : %s", exc)
        return []


ARR_PATH_MAPPINGS = _load_arr_path_mappings()
SERVICE_STARTED_AT = time.time()

# ============================================================
# SQLITE - CACHE DES ANALYSES DE BIBLIOTHÈQUE
# ============================================================

def _db_connect() -> sqlite3.Connection:
    db_path = Path(SQLITE_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_database() -> None:
    with _db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS library_analysis (
                media_key TEXT PRIMARY KEY,
                media_type TEXT NOT NULL,
                path TEXT NOT NULL,
                size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                forced_french INTEGER NOT NULL,
                forced_tracks TEXT NOT NULL DEFAULT '[]',
                subtitles TEXT NOT NULL DEFAULT '[]',
                analyzed_at REAL NOT NULL,
                review_status TEXT NOT NULL DEFAULT 'pending',
                reviewed_at REAL,
                review_note TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_library_analysis_path ON library_analysis(path)")
        # Migration automatique depuis les versions antérieures à la v2.4.
        library_columns = {row["name"] for row in conn.execute("PRAGMA table_info(library_analysis)").fetchall()}
        for column, definition in (
            ("review_status", "TEXT NOT NULL DEFAULT 'pending'"),
            ("reviewed_at", "REAL"),
            ("review_note", "TEXT"),
        ):
            if column not in library_columns:
                conn.execute(f"ALTER TABLE library_analysis ADD COLUMN {column} {definition}")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_library_analysis_review_status ON library_analysis(review_status)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS torrent_analysis (
                torrent_hash TEXT PRIMARY KEY,
                torrent_name TEXT NOT NULL,
                result TEXT NOT NULL,
                details TEXT,
                forced_french INTEGER,
                first_detected_at REAL NOT NULL,
                analyzed_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                last_action TEXT,
                last_action_at REAL,
                tracker_url TEXT,
                arr_url TEXT,
                arr_source TEXT
            )
        """)
        # Migration automatique des bases créées avant la v2.3.1.
        existing_columns = {row["name"] for row in conn.execute("PRAGMA table_info(torrent_analysis)").fetchall()}
        for column, definition in (
            ("tracker_url", "TEXT"),
            ("arr_url", "TEXT"),
            ("arr_source", "TEXT"),
        ):
            if column not in existing_columns:
                conn.execute(f"ALTER TABLE torrent_analysis ADD COLUMN {column} {definition}")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_torrent_analysis_analyzed_at ON torrent_analysis(analyzed_at DESC)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS torrent_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                torrent_hash TEXT NOT NULL,
                action TEXT NOT NULL,
                source TEXT NOT NULL,
                actor TEXT,
                details TEXT,
                created_at REAL NOT NULL,
                FOREIGN KEY (torrent_hash) REFERENCES torrent_analysis(torrent_hash)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_torrent_actions_hash ON torrent_actions(torrent_hash)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_torrent_actions_created_at ON torrent_actions(created_at DESC)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_research_cache (
                cache_key TEXT PRIMARY KEY,
                media_type TEXT NOT NULL DEFAULT 'Film',
                title TEXT NOT NULL DEFAULT '',
                recommendation TEXT NOT NULL DEFAULT 'INDETERMINÉ',
                confidence REAL DEFAULT 0,
                justification TEXT DEFAULT '',
                sources TEXT NOT NULL DEFAULT '[]',
                model TEXT DEFAULT '',
                searched_at REAL NOT NULL DEFAULT 0
            )
        """)
        # Migration robuste des bases créées par une version 2.6.x de test.
        # CREATE TABLE IF NOT EXISTS ne modifie pas une table existante : on
        # ajoute donc explicitement les colonnes manquantes avant toute lecture.
        ai_cache_columns = {row[1] for row in conn.execute("PRAGMA table_info(ai_research_cache)").fetchall()}
        ai_cache_migrations = (
            ("media_type", "TEXT NOT NULL DEFAULT 'Film'"),
            ("title", "TEXT NOT NULL DEFAULT ''"),
            ("year", "TEXT NOT NULL DEFAULT ''"),
            ("result", "TEXT NOT NULL DEFAULT 'INDETERMINÉ'"),
            ("recommendation", "TEXT NOT NULL DEFAULT 'INDETERMINÉ'"),
            ("confidence", "REAL DEFAULT 0"),
            ("justification", "TEXT DEFAULT ''"),
            ("sources", "TEXT NOT NULL DEFAULT '[]'"),
            ("model", "TEXT DEFAULT ''"),
            ("researched_at", "REAL NOT NULL DEFAULT 0"),
            ("searched_at", "REAL NOT NULL DEFAULT 0"),
            ("brave_queries", "TEXT NOT NULL DEFAULT '[]'"),
        )
        for column, definition in ai_cache_migrations:
            if column not in ai_cache_columns:
                conn.execute(f"ALTER TABLE ai_research_cache ADD COLUMN {column} {definition}")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ai_research_cache_title ON ai_research_cache(title)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS library_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                media_key TEXT UNIQUE NOT NULL,
                media_type TEXT NOT NULL,
                title TEXT,
                path TEXT,
                error TEXT NOT NULL,
                first_seen REAL NOT NULL,
                last_seen REAL NOT NULL,
                resolved_at REAL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_library_errors_last_seen ON library_errors(last_seen DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_library_errors_resolved ON library_errors(resolved_at)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS forcedfr_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
        """)
        defaults = {
            "notify_no_forced": "1",
            "notify_errors": "1",
            "library_profile_films": "strict",
            "library_profile_series": "strict",
            "qb_ignored_tags": "",
            "ai_enabled": "1",
            "brave_results_count": "5",
            "ai_necessary_min_confidence_films": "70",
            "ai_not_necessary_min_confidence_films": "70",
            "ai_necessary_min_confidence_series": "70",
            "ai_not_necessary_min_confidence_series": "70",
        }
        for key, value in defaults.items():
            conn.execute(
                "INSERT OR IGNORE INTO forcedfr_settings(key,value,updated_at) VALUES (?,?,?)",
                (key, value, time.time()),
            )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS discord_messages (
                message_id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, torrent_hash TEXT NOT NULL, decision_required INTEGER NOT NULL DEFAULT 0, sent_at REAL NOT NULL, reminder_sent INTEGER NOT NULL DEFAULT 0, resolved INTEGER NOT NULL DEFAULT 0, deleted_at REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS forcedfr_profiles (
                name TEXT PRIMARY KEY,
                media_type TEXT NOT NULL,
                forced_required INTEGER NOT NULL DEFAULT 1,
                missing_action TEXT NOT NULL DEFAULT 'review',
                error_action TEXT NOT NULL DEFAULT 'resume',
                enabled INTEGER NOT NULL DEFAULT 1,
                updated_at REAL NOT NULL
            )
        """)
        for name, media_type in (("strict", "Film"), ("strict_series", "Série")):
            conn.execute(
                "INSERT OR IGNORE INTO forcedfr_profiles(name,media_type,forced_required,missing_action,error_action,enabled,updated_at) VALUES (?,?,?,?,?,?,?)",
                (name, media_type, 1, "review", "resume", 1, time.time()),
            )
        cols = {row[1] for row in conn.execute("PRAGMA table_info(forcedfr_profiles)").fetchall()}
        if "error_retries" not in cols:
            conn.execute("ALTER TABLE forcedfr_profiles ADD COLUMN error_retries INTEGER NOT NULL DEFAULT 5")
        if "error_retry_delay" not in cols:
            conn.execute("ALTER TABLE forcedfr_profiles ADD COLUMN error_retry_delay REAL NOT NULL DEFAULT 30")
        if "found_action" not in cols:
            conn.execute("ALTER TABLE forcedfr_profiles ADD COLUMN found_action TEXT NOT NULL DEFAULT 'resume'")
        if "torrent_missing_action" not in cols:
            conn.execute("ALTER TABLE forcedfr_profiles ADD COLUMN torrent_missing_action TEXT NOT NULL DEFAULT 'scan_ai'")
        if "ai_necessary_action" not in cols:
            conn.execute("ALTER TABLE forcedfr_profiles ADD COLUMN ai_necessary_action TEXT NOT NULL DEFAULT 'pause_decision'")
        if "ai_not_necessary_action" not in cols:
            conn.execute("ALTER TABLE forcedfr_profiles ADD COLUMN ai_not_necessary_action TEXT NOT NULL DEFAULT 'resume_notify'")
        if "ai_indeterminate_action" not in cols:
            conn.execute("ALTER TABLE forcedfr_profiles ADD COLUMN ai_indeterminate_action TEXT NOT NULL DEFAULT 'pause_decision'")
        conn.execute("UPDATE forcedfr_profiles SET found_action='resume' WHERE found_action='validate'")
        conn.execute("UPDATE forcedfr_profiles SET found_action='resume_notify' WHERE found_action='validate_notify'")
        conn.execute("UPDATE forcedfr_profiles SET found_action='pause_decision' WHERE found_action='pause_notify'")
        conn.execute("UPDATE forcedfr_profiles SET torrent_missing_action='scan_ai' WHERE torrent_missing_action IN ('pause_notify','continue_notify')")
        conn.execute("UPDATE forcedfr_profiles SET ai_necessary_action='pause_decision' WHERE ai_necessary_action IN ('pause_notify','validate_notify')")
        conn.execute("UPDATE forcedfr_profiles SET ai_necessary_action='resume_notify' WHERE ai_necessary_action='continue_notify'")
        conn.execute("UPDATE forcedfr_profiles SET ai_not_necessary_action='resume_notify' WHERE ai_not_necessary_action='continue_notify'")
        conn.execute("UPDATE forcedfr_profiles SET ai_not_necessary_action='pause_decision' WHERE ai_not_necessary_action='pause_notify'")
        conn.execute("UPDATE forcedfr_profiles SET ai_indeterminate_action='pause_decision' WHERE ai_indeterminate_action IN ('pause_notify','validate_notify')")
        conn.execute("UPDATE forcedfr_profiles SET ai_indeterminate_action='resume_notify' WHERE ai_indeterminate_action='continue_notify'")
        conn.execute("UPDATE forcedfr_profiles SET error_action='resume' WHERE error_action='notify_continue'")
        conn.execute("UPDATE forcedfr_profiles SET error_action='retry_resume' WHERE error_action='retry_continue'")
        conn.execute("UPDATE forcedfr_profiles SET error_action='retry_pause_decision' WHERE error_action='retry_pause'")
    log.info("SQLite initialisée : %s", SQLITE_PATH)



def _setting(key: str, default: str = "") -> str:
    try:
        with _db_connect() as conn:
            row = conn.execute("SELECT value FROM forcedfr_settings WHERE key=?", (key,)).fetchone()
        return str(row["value"]) if row else default
    except Exception:
        return default


def _setting_bool(key: str, default: bool = True) -> bool:
    return _setting(key, "1" if default else "0") == "1"


def set_setting(key: str, value: str) -> None:
    with _db_connect() as conn:
        conn.execute(
            "INSERT INTO forcedfr_settings(key,value,updated_at) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, str(value), time.time()),
        )


def get_qbittorrent_tags() -> list[str]:
    try:
        r=qb_session.get(f"{QB_HOST}/api/v2/torrents/tags",timeout=5); r.raise_for_status(); data=r.json()
        return sorted({str(x).strip() for x in data if str(x).strip()}, key=str.casefold) if isinstance(data,list) else []
    except Exception as exc:
        log.warning("Impossible de récupérer les étiquettes qBittorrent : %s",exc); return []


def get_ignored_qb_tags() -> set[str]:
    raw = _setting("qb_ignored_tags", "")
    return {tag.strip().casefold() for tag in raw.replace(";", ",").split(",") if tag.strip()}


def torrent_has_ignored_tag(torrent: dict[str, Any]) -> str | None:
    ignored = get_ignored_qb_tags()
    if not ignored:
        return None
    raw_tags = torrent.get("tags") or ""
    tags = [t.strip() for t in str(raw_tags).split(",") if t.strip()]
    for tag in tags:
        if tag.casefold() in ignored:
            return tag
    return None


def record_library_error(item: dict[str, Any], file_path: Path | None, error: str) -> None:
    key = _media_cache_key(item, file_path) if file_path else f"{item.get('type','')}|{item.get('raw_path') or item.get('title','')}"
    now = time.time()
    with _db_connect() as conn:
        conn.execute("""
            INSERT INTO library_errors(media_key,media_type,title,path,error,first_seen,last_seen,resolved_at)
            VALUES (?,?,?,?,?,?,?,NULL)
            ON CONFLICT(media_key) DO UPDATE SET
                media_type=excluded.media_type,title=excluded.title,path=excluded.path,
                error=excluded.error,last_seen=excluded.last_seen,resolved_at=NULL
        """, (key, item.get("type", "inconnu"), item.get("title"), str(file_path) if file_path else item.get("raw_path"), str(error), now, now))


def resolve_library_error(item: dict[str, Any], file_path: Path | None) -> None:
    key = _media_cache_key(item, file_path) if file_path else f"{item.get('type','')}|{item.get('raw_path') or item.get('title','')}"
    with _db_connect() as conn:
        conn.execute("UPDATE library_errors SET resolved_at=? WHERE media_key=? AND resolved_at IS NULL", (time.time(), key))


def get_library_errors(include_resolved: bool = False, limit: int = 200) -> list[dict[str, Any]]:
    with _db_connect() as conn:
        where = "" if include_resolved else "WHERE resolved_at IS NULL"
        rows = conn.execute(f"SELECT * FROM library_errors {where} ORDER BY last_seen DESC LIMIT ?", (max(1, min(limit, 1000)),)).fetchall()
    return [dict(r) for r in rows]


def get_dashboard_stats() -> dict[str, Any]:
    with _db_connect() as conn:
        lib = conn.execute("SELECT COUNT(*) total, SUM(CASE WHEN forced_french=1 THEN 1 ELSE 0 END) forced, SUM(CASE WHEN forced_french=0 THEN 1 ELSE 0 END) no_forced FROM library_analysis").fetchone()
        pending = conn.execute("SELECT COUNT(*) FROM library_analysis WHERE forced_french=0 AND review_status='pending'").fetchone()[0]
        waiting = conn.execute("SELECT COUNT(*) FROM library_analysis WHERE forced_french=0 AND review_status='waiting_replacement'").fetchone()[0]
        validated = conn.execute("SELECT COUNT(*) FROM library_analysis WHERE forced_french=0 AND review_status='validated'").fetchone()[0]
        errors = conn.execute("SELECT COUNT(*) FROM library_errors WHERE resolved_at IS NULL").fetchone()[0]
        torrent_total = conn.execute("SELECT COUNT(*) FROM torrent_analysis").fetchone()[0]
        torrent_forced = conn.execute("SELECT COUNT(*) FROM torrent_analysis WHERE forced_french=1").fetchone()[0]
        torrent_no_forced = conn.execute("SELECT COUNT(*) FROM torrent_analysis WHERE result='no_forced'").fetchone()[0]
        torrent_errors = conn.execute("SELECT COUNT(*) FROM torrent_analysis WHERE result='error'").fetchone()[0]
        recent = conn.execute("""SELECT a.action, a.details, a.created_at, t.torrent_name, t.result
                                   FROM torrent_actions a LEFT JOIN torrent_analysis t ON t.torrent_hash=a.torrent_hash
                                   ORDER BY a.created_at DESC LIMIT 8""").fetchall()
    return {
        "library": {"total": int(lib["total"] or 0), "forced": int(lib["forced"] or 0), "no_forced": int(lib["no_forced"] or 0), "pending": pending, "waiting": waiting, "validated": validated, "errors": errors},
        "torrents": {"total": torrent_total, "forced": torrent_forced, "no_forced": torrent_no_forced, "errors": torrent_errors},
        "recent": [dict(r) for r in recent],
    }


def _media_cache_key(item: dict[str, Any], file_path: Path) -> str:
    # Le chemin résolu identifie physiquement le média. Le type évite tout conflit.
    return f"{item.get('type','')}|{str(file_path.resolve())}"


def _cached_analysis(item: dict[str, Any], file_path: Path) -> dict[str, Any] | None:
    stat = file_path.stat()
    key = _media_cache_key(item, file_path)
    with _db_connect() as conn:
        row = conn.execute("SELECT * FROM library_analysis WHERE media_key = ?", (key,)).fetchone()
    if not row:
        return None
    if int(row["size"]) != int(stat.st_size) or int(row["mtime_ns"]) != int(stat.st_mtime_ns):
        return None
    return {
        "forced_french": bool(row["forced_french"]),
        "forced_tracks": json.loads(row["forced_tracks"] or "[]"),
        "subtitles": json.loads(row["subtitles"] or "[]"),
        "analyzed_at": row["analyzed_at"],
        "review_status": (row["review_status"] if "review_status" in row.keys() else "pending") or "pending",
        "reviewed_at": (row["reviewed_at"] if "reviewed_at" in row.keys() else None),
        "review_note": (row["review_note"] if "review_note" in row.keys() else None),
    }


def _save_cached_analysis(item: dict[str, Any], file_path: Path, detection: dict[str, Any]) -> None:
    stat = file_path.stat()
    key = _media_cache_key(item, file_path)
    with _db_connect() as conn:
        conn.execute("""
            INSERT INTO library_analysis
            (media_key, media_type, path, size, mtime_ns, forced_french, forced_tracks, subtitles, analyzed_at, review_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(media_key) DO UPDATE SET
                media_type=excluded.media_type, path=excluded.path, size=excluded.size,
                mtime_ns=excluded.mtime_ns, forced_french=excluded.forced_french,
                forced_tracks=excluded.forced_tracks, subtitles=excluded.subtitles,
                analyzed_at=excluded.analyzed_at,
                review_status=CASE WHEN excluded.forced_french=1 THEN 'pending' ELSE library_analysis.review_status END,
                reviewed_at=CASE WHEN excluded.forced_french=1 THEN NULL ELSE library_analysis.reviewed_at END,
                review_note=CASE WHEN excluded.forced_french=1 THEN NULL ELSE library_analysis.review_note END
        """, (
            key, item.get("type", "inconnu"), str(file_path.resolve()), int(stat.st_size),
            int(stat.st_mtime_ns), int(bool(detection.get("forced_french"))),
            json.dumps(detection.get("forced_tracks", []), ensure_ascii=False),
            json.dumps(detection.get("subtitles", []), ensure_ascii=False), time.time(), "pending",
        ))


def _media_review_key(item: dict[str, Any], file_path: Path) -> str:
    return _media_cache_key(item, file_path)


def set_media_review(media_key: str, review_status: str, review_note: str | None = None) -> bool:
    allowed = {"pending", "validated", "waiting_replacement"}
    if review_status not in allowed:
        return False
    with _db_connect() as conn:
        cur = conn.execute(
            "UPDATE library_analysis SET review_status=?, reviewed_at=?, review_note=? WHERE media_key=?",
            (review_status, None if review_status == "pending" else time.time(), review_note, media_key),
        )
        return cur.rowcount > 0


init_database()

# Configuration persistante modifiable depuis l’interface web.
CONFIG_FALLBACKS = {
    "DISCORD_BOT_TOKEN": DISCORD_BOT_TOKEN, "DISCORD_CHANNEL_ID": DISCORD_CHANNEL_ID,
    "DISCORD_WEBHOOK_URL": DISCORD_WEBHOOK_URL, "QB_HOST": QB_HOST,
    "QB_PASSWORD": QB_PASSWORD, "QB_USERNAME": QB_USERNAME,
    "RADARR_API_KEY": RADARR_API_KEY, "RADARR_URL": RADARR_URL,
    "SONARR_API_KEY": SONARR_API_KEY, "SONARR_URL": SONARR_URL,
    "TZ": os.getenv("TZ", "Europe/Paris"),
    "GROQ_API_KEY": GROQ_API_KEY, "GROQ_MODEL": GROQ_MODEL,
    "BRAVE_SEARCH_API_KEY": BRAVE_SEARCH_API_KEY,
}

def load_runtime_configuration() -> None:
    global DISCORD_BOT_TOKEN, DISCORD_CHANNEL_ID, DISCORD_WEBHOOK_URL
    global QB_HOST, QB_PASSWORD, QB_USERNAME, RADARR_API_KEY, RADARR_URL, SONARR_API_KEY, SONARR_URL, GROQ_API_KEY, GROQ_MODEL, BRAVE_SEARCH_API_KEY
    for key, fallback in CONFIG_FALLBACKS.items():
        value = _setting("config_" + key, fallback)
        if key == "DISCORD_BOT_TOKEN": DISCORD_BOT_TOKEN = value
        elif key == "DISCORD_CHANNEL_ID": DISCORD_CHANNEL_ID = value
        elif key == "DISCORD_WEBHOOK_URL": DISCORD_WEBHOOK_URL = value
        elif key == "QB_HOST": QB_HOST = value.rstrip("/")
        elif key == "QB_PASSWORD": QB_PASSWORD = value
        elif key == "QB_USERNAME": QB_USERNAME = value
        elif key == "RADARR_API_KEY": RADARR_API_KEY = value
        elif key == "RADARR_URL": RADARR_URL = value.rstrip("/")
        elif key == "SONARR_API_KEY": SONARR_API_KEY = value
        elif key == "SONARR_URL": SONARR_URL = value.rstrip("/")
        elif key == "GROQ_API_KEY": GROQ_API_KEY = value
        elif key == "GROQ_MODEL": GROQ_MODEL = value or "openai/gpt-oss-120b"
        elif key == "BRAVE_SEARCH_API_KEY": BRAVE_SEARCH_API_KEY = value
        elif key == "TZ" and value:
            os.environ["TZ"] = value
            if hasattr(time, "tzset"): time.tzset()

load_runtime_configuration()

# ============================================================
# SQLITE - HISTORIQUE PERSISTANT DES ANALYSES qBITTORRENT
# ============================================================
ANALYSIS_HISTORY_LIMIT = int(os.getenv("ANALYSIS_HISTORY_LIMIT", "500"))


def _torrent_forced_value(result: str) -> int | None:
    if result == "forced_found":
        return 1
    if result == "no_forced":
        return 0
    return None


def record_analysis_history(
    torrent_hash: str,
    torrent_name: str,
    result: str,
    details: str | None = None,
) -> None:
    now = time.time()
    forced_value = _torrent_forced_value(result)
    with _db_connect() as conn:
        conn.execute("""
            INSERT INTO torrent_analysis
            (torrent_hash, torrent_name, result, details, forced_french,
             first_detected_at, analyzed_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(torrent_hash) DO UPDATE SET
                torrent_name=excluded.torrent_name,
                result=excluded.result,
                details=excluded.details,
                forced_french=COALESCE(excluded.forced_french, torrent_analysis.forced_french),
                analyzed_at=excluded.analyzed_at,
                updated_at=excluded.updated_at
        """, (
            torrent_hash, torrent_name, result, details, forced_value,
            now, now, now,
        ))
        conn.execute("""
            INSERT INTO torrent_actions
            (torrent_hash, action, source, actor, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (torrent_hash, "analysis", "forcedfr", None, details or result, now))


def record_torrent_action(
    torrent_hash: str,
    action: str,
    *,
    source: str,
    actor: str | None = None,
    details: str | None = None,
) -> None:
    now = time.time()
    with _db_connect() as conn:
        conn.execute("""
            INSERT INTO torrent_actions
            (torrent_hash, action, source, actor, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (torrent_hash, action, source, actor, details, now))
        conn.execute("""
            UPDATE torrent_analysis
            SET last_action=?, last_action_at=?, updated_at=?
            WHERE torrent_hash=?
        """, (action, now, now, torrent_hash))


def update_torrent_release_context(
    torrent_hash: str,
    release: dict[str, Any] | None,
) -> None:
    if not release:
        return
    with _db_connect() as conn:
        conn.execute(
            """
            UPDATE torrent_analysis
            SET tracker_url=COALESCE(?, tracker_url),
                arr_url=COALESCE(?, arr_url),
                arr_source=COALESCE(?, arr_source),
                updated_at=?
            WHERE torrent_hash=?
            """,
            (
                release.get("tracker_url"),
                release.get("arr_item_url"),
                release.get("source"),
                time.time(),
                torrent_hash,
            ),
        )


def _resolve_missing_history_context(item: dict[str, Any]) -> dict[str, Any]:
    # Les anciennes analyses v2.3.0 ne possèdent pas encore les URLs.
    # On les récupère une seule fois depuis Radarr/Sonarr puis on les mémorise.
    if item.get("tracker_url") and item.get("arr_url"):
        return item
    release = get_release_context(str(item.get("torrent_hash", "")))
    if release.get("source") or release.get("tracker_url") or release.get("arr_item_url"):
        update_torrent_release_context(str(item.get("torrent_hash", "")), release)
        item["tracker_url"] = release.get("tracker_url") or item.get("tracker_url")
        item["arr_url"] = release.get("arr_item_url") or item.get("arr_url")
        item["arr_source"] = release.get("source") or item.get("arr_source")
    return item


def get_analysis_history(limit: int | None = None) -> list[dict[str, Any]]:
    history_limit = max(1, min(int(limit or ANALYSIS_HISTORY_LIMIT), 5000))
    with _db_connect() as conn:
        rows = conn.execute("""
            SELECT torrent_hash, torrent_name, result, details,
                   analyzed_at AS timestamp, forced_french,
                   last_action, last_action_at,
                   tracker_url, arr_url, arr_source
            FROM torrent_analysis
            ORDER BY analyzed_at DESC
            LIMIT ?
        """, (history_limit,)).fetchall()
    # IMPORTANT : ne jamais interroger Radarr/Sonarr ici.
    # L'historique doit s'ouvrir immédiatement depuis SQLite.
    # Les URLs sont récupérées et enregistrées au moment de l'analyse ;
    # les anciennes entrées sans URL restent donc affichées sans bloquer
    # l'ouverture de la page.
    return [dict(row) for row in rows]


scan_lock = threading.Lock()
scan_state: dict[str, Any] = {
    "running": False,
    "requested_scope": None,
    "mode": "incremental",
    "started_at": None,
    "finished_at": None,
    "current_file": None,
    "total_files": 0,
    "processed_files": 0,
    "files_with_forced_fr": 0,
    "files_without_forced_fr": 0,
    "errors": 0,
    "cache_hits": 0,
    "reanalyzed": 0,
    "results": [],
    "last_error": None,
    "source_errors": [],
}


def _scan_reset(scope: str, mode: str = "incremental") -> None:
    existing_results = list(scan_state.get("results", []))
    if scope == "films":
        existing_results = [r for r in existing_results if r.get("type") != "Film"]
    elif scope == "series":
        existing_results = [r for r in existing_results if r.get("type") != "Série"]
    else:
        existing_results = []

    scan_state.update({
        "running": True,
        "requested_scope": scope,
        "mode": mode,
        "started_at": time.time(),
        "finished_at": None,
        "current_file": None,
        "total_files": 0,
        "processed_files": 0,
        "files_with_forced_fr": 0,
        "files_without_forced_fr": 0,
        "errors": 0,
        "cache_hits": 0,
        "reanalyzed": 0,
        "results": existing_results,
        "last_error": None,
        "source_errors": [],
    })


def _arr_headers(api_key: str) -> dict[str, str]:
    return {"X-Api-Key": api_key, "Accept": "application/json"}


def _arr_request(base_url: str, api_key: str, endpoint: str, params: dict[str, Any] | None = None) -> Any:
    if not base_url or not api_key:
        raise RuntimeError("URL ou clé API Arr non configurée")
    response = requests.get(
        f"{base_url}/api/v3/{endpoint.lstrip('/')}",
        headers=_arr_headers(api_key),
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def _arr_connection_status(base_url: str, api_key: str) -> dict[str, Any]:
    if not base_url or not api_key:
        return {"status": "not_configured", "configured": False, "url": base_url or None}
    try:
        data = _arr_request(base_url, api_key, "system/status")
        return {
            "status": "connected",
            "configured": True,
            "url": base_url,
            "version": data.get("version"),
        }
    except Exception as exc:
        return {
            "status": "error",
            "configured": True,
            "url": base_url,
            "error": str(exc),
        }


def _resolve_arr_media_path(raw_path: str | None) -> Path | None:
    if not raw_path:
        return None
    candidate = Path(raw_path)
    if candidate.exists():
        return candidate
    raw = str(raw_path).rstrip("/")
    for source_prefix, target_prefix in ARR_PATH_MAPPINGS:
        if raw == source_prefix or raw.startswith(source_prefix + "/"):
            mapped = Path(target_prefix + raw[len(source_prefix):])
            if mapped.exists():
                return mapped
            return mapped
    return candidate


def _radarr_scan_items() -> list[dict[str, Any]]:
    movies = _arr_request(RADARR_URL, RADARR_API_KEY, "movie")
    items: list[dict[str, Any]] = []
    for movie in movies if isinstance(movies, list) else []:
        movie_file = movie.get("movieFile") or {}
        raw_path = movie_file.get("path") or movie.get("path")
        if not movie.get("hasFile") or not raw_path:
            continue
        tmdb_id = movie.get("tmdbId")
        poster_url = next((img.get("remoteUrl") or img.get("url") for img in (movie.get("images") or []) if img.get("coverType") == "poster"), None)
        items.append({
            "type": "Film",
            "title": movie.get("title") or Path(raw_path).stem,
            "poster_url": poster_url,
            "raw_path": raw_path,
            "path": _resolve_arr_media_path(raw_path),
            "arr_source": "Radarr",
            "arr_url": f"{RADARR_URL}/movie/{tmdb_id}" if tmdb_id else RADARR_URL,
            "arr_id": movie.get("id"),
        })
    return items


def _sonarr_scan_items() -> list[dict[str, Any]]:
    """
    Construit la liste des épisodes à analyser directement depuis Sonarr.

    Important : les métadonnées saison/épisode ne sont pas toujours présentes
    dans /episodefile. Elles sont récupérées depuis /episode et associées via
    episodeFileId. Cela évite toute tentative de déduction depuis le nom du
    fichier.
    """
    series_list = _arr_request(SONARR_URL, SONARR_API_KEY, "series")
    items: list[dict[str, Any]] = []

    for series in series_list if isinstance(series_list, list) else []:
        series_id = series.get("id")
        if series_id is None:
            continue

        try:
            # URL native de Sonarr : /series/<titleSlug>
            # Exemple : /series/baron-noir
            title_slug = series.get("titleSlug")
            if title_slug:
                series_url = f"{SONARR_URL}/series/{title_slug}"
            else:
                # Même logique de secours que le bot Discord.
                series_url = arr_item_url_lookup(
                    SONARR_URL,
                    SONARR_API_KEY,
                    "Sonarr",
                    series_id,
                )

            episode_files = _arr_request(
                SONARR_URL,
                SONARR_API_KEY,
                "episodefile",
                {"seriesId": series_id},
            )
            episodes = _arr_request(
                SONARR_URL,
                SONARR_API_KEY,
                "episode",
                {"seriesId": series_id},
            )
        except Exception as exc:
            log.warning(
                "[SCAN] Impossible de récupérer les épisodes de la série '%s' : %s",
                series.get("title"),
                exc,
            )
            continue

        # Association episodeFileId -> métadonnées épisode Sonarr.
        episodes_by_file_id: dict[Any, list[dict[str, Any]]] = {}
        for episode in episodes if isinstance(episodes, list) else []:
            file_id = episode.get("episodeFileId")
            if file_id is not None and str(file_id) not in ("", "0"):
                # Normalisation en chaîne pour éviter les différences int/string
                # selon les versions de l'API Sonarr.
                episodes_by_file_id.setdefault(str(file_id), []).append(episode)

        for episode_file in episode_files if isinstance(episode_files, list) else []:
            raw_path = episode_file.get("path")
            if not raw_path:
                continue

            linked_episodes = episodes_by_file_id.get(str(episode_file.get("id")), [])
            linked_episodes.sort(
                key=lambda ep: (ep.get("seasonNumber", 0), ep.get("episodeNumber", 0))
            )

            season_number = None
            episode_number = None
            episode_label = None

            if linked_episodes:
                first_episode = linked_episodes[0]
                season_number = first_episode.get("seasonNumber")
                numbers = [ep.get("episodeNumber") for ep in linked_episodes if ep.get("episodeNumber") is not None]
                if len(numbers) == 1:
                    episode_number = numbers[0]
                    episode_label = f"E{int(numbers[0]):02d}"
                elif numbers:
                    episode_number = numbers[0]
                    episode_label = " / ".join(f"E{int(n):02d}" for n in numbers)

            # Compatibilité avec certaines versions de Sonarr qui peuvent
            # fournir seasonNumber/episodes directement dans episodefile.
            if season_number is None:
                season_number = episode_file.get("seasonNumber")
            if episode_label is None:
                embedded = episode_file.get("episodes") or []
                numbers = [ep.get("episodeNumber") for ep in embedded if ep.get("episodeNumber") is not None]

                # Certaines réponses Sonarr exposent directement une liste
                # episodeNumbers au lieu d'un tableau episodes.
                if not numbers:
                    raw_numbers = episode_file.get("episodeNumbers") or []
                    if isinstance(raw_numbers, list):
                        numbers = [n for n in raw_numbers if n is not None]

                if len(numbers) == 1:
                    episode_number = numbers[0]
                    episode_label = f"E{int(numbers[0]):02d}"
                elif numbers:
                    episode_number = numbers[0]
                    episode_label = " / ".join(f"E{int(n):02d}" for n in numbers)

            poster_url = next((img.get("remoteUrl") or img.get("url") for img in (series.get("images") or []) if img.get("coverType") == "poster"), None)
            items.append({
                "type": "Série",
                "title": series.get("title") or Path(raw_path).stem,
                "poster_url": poster_url,
                "season": f"S{int(season_number):02d}" if season_number is not None else None,
                "episode": episode_label,
                "season_number": season_number,
                "episode_number": episode_number,
                "raw_path": raw_path,
                "path": _resolve_arr_media_path(raw_path),
                "arr_source": "Sonarr",
                "arr_url": series_url,
                "arr_id": series_id,
            })
    return items

def _scan_items(scope: str, *, record_errors: bool = False) -> list[dict[str, Any]]:
    """Récupère les médias indépendamment depuis Radarr et Sonarr.

    Une erreur sur un service ne doit pas empêcher l'affichage des médias
    provenant de l'autre service. Les erreurs sont conservées pour le scan
    actif et affichées dans l'interface.
    """
    items: list[dict[str, Any]] = []
    source_errors: list[str] = []
    if scope in ("films", "all"):
        try:
            items.extend(_radarr_scan_items())
        except Exception as exc:
            message = f"Radarr : {exc}"
            source_errors.append(message)
            log.exception("[SCAN] Impossible de récupérer la bibliothèque Radarr.")
    if scope in ("series", "all"):
        try:
            items.extend(_sonarr_scan_items())
        except Exception as exc:
            message = f"Sonarr : {exc}"
            source_errors.append(message)
            log.exception("[SCAN] Impossible de récupérer la bibliothèque Sonarr.")
    if record_errors:
        scan_state["source_errors"] = source_errors
        if source_errors:
            scan_state["last_error"] = " ; ".join(source_errors)
        elif not items:
            scan_state["last_error"] = "Radarr/Sonarr n'ont retourné aucun média. Vérifie les connexions et les bibliothèques configurées."
    return items


def get_cached_library_results(scope: str) -> list[dict[str, Any]]:
    """Retourne immédiatement les médias déjà analysés en SQLite, enrichis par Radarr/Sonarr."""
    items = _scan_items(scope)
    results: list[dict[str, Any]] = []
    for item in items:
        file_path = item.get("path")
        if not file_path:
            continue
        try:
            cached = _cached_analysis(item, file_path)
        except Exception:
            cached = None
        if cached is None:
            continue
        ai_cached = None
        ai_decision = None
        ai_confidence = None
        try:
            ai_cached = _ai_cache_get(str(item.get("type", "Film")), str(item.get("title") or Path(file_path).stem))
            if ai_cached:
                ai_decision, ai_confidence, _ = _normalize_ai_decision(str(item.get("type", "Film")), ai_cached)
        except Exception as exc:
            # Le cache IA est facultatif : une erreur dans sa table ne doit
            # jamais empêcher d'afficher les médias déjà analysés.
            log.warning("[BIBLIOTHÈQUE] Cache IA indisponible pour %s : %s", item.get("title"), exc)
        results.append({
            "path": str(file_path),
            "relative_path": item.get("raw_path") or item.get("title"),
            "type": item.get("type", "inconnu"),
            "title": item.get("title"),
            "poster_url": item.get("poster_url"),
            "arr_source": item.get("arr_source"),
            "arr_url": item.get("arr_url"),
            "season": item.get("season"),
            "episode": item.get("episode"),
            "season_number": item.get("season_number"),
            "episode_number": item.get("episode_number"),
            "forced_french": cached["forced_french"],
            "status": "ok",
            "error": None,
            "forced_tracks": cached.get("forced_tracks", []),
            "subtitles": cached.get("subtitles", []),
            "media_key": _media_review_key(item, file_path),
            "review_status": cached.get("review_status", "pending"),
            "reviewed_at": cached.get("reviewed_at"),
            "review_note": cached.get("review_note"),
            "from_cache": True,
            "ai_recommendation": ai_decision,
            "ai_confidence": ai_confidence,
            "ai_analyzed": bool(ai_cached),
        })
    return results


def _profile_for_media(media_type: str) -> dict[str, Any] | None:
    name = "strict_series" if media_type == "Série" else "strict"
    with _db_connect() as conn:
        row = conn.execute("SELECT * FROM forcedfr_profiles WHERE name=? AND enabled=1", (name,)).fetchone()
    return dict(row) if row else None


def _torrent_profile_for_torrent(torrent: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    release = None
    try:
        release = wait_for_release_context(str(torrent.get("hash", "")))
    except Exception:
        release = None
    media_type = str((release or {}).get("media_type") or "")
    if not media_type:
        media_type = "Série" if re.search(r"S\d{1,2}E\d{1,3}", str(torrent.get("name", "")), re.I) else "Film"
    return (_profile_for_media(media_type) or {"media_type": media_type, "found_action": "resume", "torrent_missing_action": "scan_ai"}, release)


def _error_profile_for_torrent(torrent: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    release=None
    try: release=wait_for_release_context(str(torrent.get("hash","")))
    except Exception: pass
    media_type="Série" if release and release.get("source")=="Sonarr" else "Film"
    return (_profile_for_media(media_type) or {"media_type":media_type,"error_action":"resume","error_retries":5,"error_retry_delay":30},release)


def _review_status_from_profile(media_type: str) -> str:
    profile = _profile_for_media(media_type)
    action = (profile or {}).get("missing_action", "review")
    return {
        "validated": "validated",
        "waiting_replacement": "waiting_replacement",
        "review": "pending",
    }.get(action, "pending")


def run_library_scan(scope: str, mode: str = "incremental") -> None:
    if not scan_lock.acquire(blocking=False):
        log.warning("Un scan de bibliothèque est déjà en cours.")
        return
    try:
        _scan_reset(scope, mode)
        log.info("[SCAN] Récupération de la bibliothèque '%s' via Radarr/Sonarr (mode=%s)...", scope, mode)
        items = _scan_items(scope, record_errors=True)
        scan_state["total_files"] = len(items)
        log.info("[SCAN] %d média(s) référencé(s) par Radarr/Sonarr.", len(items))

        for item in items:
            raw_path = item.get("raw_path")
            file_path = item.get("path")
            scan_state["current_file"] = raw_path or item.get("title")
            result: dict[str, Any] = {
                "path": str(file_path) if file_path else raw_path,
                "relative_path": raw_path or item.get("title"),
                "type": item.get("type", "inconnu"),
                "title": item.get("title"),
                "poster_url": item.get("poster_url"),
                "arr_source": item.get("arr_source"),
                "arr_url": item.get("arr_url"),
                "season": item.get("season"),
                "episode": item.get("episode"),
                "season_number": item.get("season_number"),
                "episode_number": item.get("episode_number"),
                "forced_french": False,
                "status": "ok",
                "error": None,
                "forced_tracks": [],
                "subtitles": [],
                "media_key": _media_review_key(item, file_path) if file_path else None,
                "review_status": _review_status_from_profile(str(item.get("type", "Film"))),
                "reviewed_at": None,
                "review_note": None,
            }
            try:
                if not file_path or not file_path.exists():
                    raise FileNotFoundError(
                        f"Fichier référencé par {item.get('arr_source')} introuvable dans ForcedFR : {raw_path}"
                    )
                cached = _cached_analysis(item, file_path) if mode == "incremental" else None
                if cached is not None:
                    result["forced_french"] = cached["forced_french"]
                    result["forced_tracks"] = cached["forced_tracks"]
                    result["subtitles"] = cached["subtitles"]
                    result["review_status"] = cached.get("review_status", "pending")
                    result["reviewed_at"] = cached.get("reviewed_at")
                    result["review_note"] = cached.get("review_note")
                    result["from_cache"] = True
                    scan_state["cache_hits"] += 1
                else:
                    probe = run_ffprobe(file_path)
                    detection = detect_french_forced(probe)
                    result["forced_french"] = bool(detection.get("forced_french"))
                    result["forced_tracks"] = detection.get("forced_tracks", [])
                    result["subtitles"] = detection.get("subtitles", [])
                    result["from_cache"] = False
                    _save_cached_analysis(item, file_path, detection)
                    saved = _cached_analysis(item, file_path)
                    if saved:
                        result["review_status"] = saved.get("review_status", result["review_status"])
                        result["reviewed_at"] = saved.get("reviewed_at")
                        result["review_note"] = saved.get("review_note")
                    if not result["forced_french"]:
                        profile_status = _review_status_from_profile(str(item.get("type", "Film")))
                        if profile_status != "pending":
                            set_media_review(result["media_key"], profile_status, "Décision appliquée par le profil ForcedFR.")
                            result["review_status"] = profile_status
                            result["reviewed_at"] = time.time()
                            result["review_note"] = "Décision appliquée par le profil ForcedFR."
                    scan_state["reanalyzed"] += 1
                if result["forced_french"]:
                    scan_state["files_with_forced_fr"] += 1
                else:
                    scan_state["files_without_forced_fr"] += 1
                resolve_library_error(item, file_path)

                scan_state["results"].append(result)
            except Exception as exc:
                result["status"] = "error"
                result["error"] = str(exc)
                scan_state["errors"] += 1
                record_library_error(item, file_path, str(exc))
                scan_state["results"].append(result)
                log.warning("[SCAN] Erreur sur %s : %s", raw_path or item.get("title"), exc)
            finally:
                scan_state["processed_files"] += 1

        log.info("[SCAN] Terminé : %d analysé(s), %d sans FR Forced, %d erreur(s).",
                 scan_state["processed_files"], scan_state["files_without_forced_fr"], scan_state["errors"])
    except Exception as exc:
        scan_state["last_error"] = str(exc)
        log.exception("[SCAN] Erreur générale.")
    finally:
        scan_state["running"] = False
        scan_state["current_file"] = None
        scan_state["finished_at"] = time.time()
        scan_lock.release()


def start_library_scan(scope: str, mode: str = "incremental") -> dict[str, Any]:
    if scan_state.get("running"):
        raise HTTPException(status_code=409, detail="Un scan de bibliothèque est déjà en cours.")
    if mode not in ("incremental", "full"):
        raise HTTPException(status_code=400, detail="Mode de scan invalide.")
    thread = threading.Thread(target=run_library_scan, args=(scope, mode), daemon=True, name=f"forcedfr-scan-{scope}-{mode}")
    thread.start()
    return {"ok": True, "message": f"Scan '{scope}' ({mode}) démarré.", "scope": scope, "mode": mode}

def _discord_status() -> str:
    if discord_bot is None:
        return "disabled"
    try:
        return "connected" if discord_bot.is_ready() else "connecting"
    except Exception:
        return "unknown"


def _qbittorrent_status() -> tuple[str, int | None]:
    try:
        count = len(get_torrents())
        return "connected", count
    except Exception:
        return "error", None


@app.get("/", response_class=HTMLResponse)
def web_dashboard() -> str:
    return """<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ForcedFR v2.8.9</title>
<style>
:root{color-scheme:dark;--bg:#080c12;--surface:#101722;--surface2:#151e2b;--surface3:#1b2635;--border:#263345;--text:#f3f6fa;--muted:#8d9aac;--accent:#5b8cff;--accent2:#7b68ee;--green:#35c98a;--yellow:#f0b85a;--red:#ef6b73;--shadow:0 14px 40px rgba(0,0,0,.22);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
*{box-sizing:border-box}html{background:var(--bg)}body{margin:0;background:radial-gradient(circle at 50% -10%,#1a2638 0,#080c12 42%);color:var(--text);min-height:100vh}main{max-width:1440px;margin:auto;padding:30px 28px 55px}h1,h2,h3,p{margin-top:0}h1{font-size:1.72rem;letter-spacing:-.035em;margin-bottom:3px}h2{font-size:1.12rem;letter-spacing:-.015em;margin-bottom:5px}.sub,.small{color:var(--muted)}.sub{font-size:.88rem;line-height:1.45}.small{font-size:.78rem}
.app-header{display:flex;align-items:center;justify-content:space-between;gap:20px;margin-bottom:20px}.brand{display:flex;align-items:center;gap:13px}.brand-logo{width:64px;height:64px;flex:0 0 64px;object-fit:contain;border-radius:50%;filter:drop-shadow(0 4px 12px rgba(0,145,255,.22))}.brand .sub{margin:0}.version{border:1px solid var(--border);background:var(--surface3);color:var(--text);padding:7px 12px;border-radius:999px;font-size:.76rem;font-weight:850;white-space:nowrap}
.services{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px;margin-bottom:23px}.service-card{display:flex;align-items:center;gap:11px;min-width:0;padding:12px 14px;background:rgba(16,23,34,.9);border:1px solid var(--border);border-radius:13px;box-shadow:var(--shadow)}.service-icon{display:grid;place-items:center;width:34px;height:34px;flex:0 0 34px;border-radius:9px;background:#fff;overflow:hidden}.service-icon img{width:25px;height:25px;object-fit:contain}.service-name{font-size:.82rem;font-weight:850}.service-meta{margin-top:3px;font-size:.73rem}.status-dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:5px;background:var(--muted)}.status-dot.online{background:var(--green);box-shadow:0 0 0 3px rgba(53,201,138,.1)}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:11px}.card,.tablewrap,.settings-card{background:rgba(16,23,34,.93);border:1px solid var(--border);border-radius:15px;box-shadow:var(--shadow)}.card{padding:17px}.label{color:var(--muted);font-size:.7rem;text-transform:uppercase;letter-spacing:.06em;font-weight:850}.value{font-weight:750;margin-top:6px}.ok{color:var(--green)}.warn{color:var(--yellow)}.bad{color:var(--red)}
.tabs{display:flex;gap:5px;flex-wrap:wrap;margin:25px 0 21px;padding:5px;background:rgba(13,19,28,.9);border:1px solid var(--border);border-radius:13px;box-shadow:var(--shadow)}.tab,.subtab,button{font:inherit;border:1px solid transparent;border-radius:9px;padding:9px 13px;font-size:.82rem;font-weight:760;cursor:pointer;background:transparent;color:#b9c4d1;transition:.16s}.tab:hover,.subtab:hover,button:hover{background:var(--surface3);color:#fff}.tab.active,.subtab.active{background:#202d40;border-color:#304158;color:#fff;box-shadow:0 3px 12px rgba(0,0,0,.18)}button:active{transform:translateY(1px)}button.primary{background:var(--accent)!important;border-color:#719bff!important;color:#fff!important}.panel{display:none}.panel.active{display:block}.toolbar,.filters,.subtabs{display:flex;gap:8px;flex-wrap:wrap}.toolbar{padding:13px;background:var(--surface);border:1px solid var(--border);border-radius:12px}.toolbar button{background:var(--surface3);border-color:var(--border)}.progress{height:8px;background:#202b39;border-radius:999px;overflow:hidden;margin-top:13px}.progress div{height:100%;width:0;background:linear-gradient(90deg,var(--accent),var(--accent2));transition:.3s}.toolbar+.small,.progress+.small{display:block;margin-top:9px}
select,input{font:inherit;background:#0d131c;color:var(--text);border:1px solid var(--border);border-radius:9px;padding:9px 11px;outline:none}select:focus,input:focus{border-color:#527fd6;box-shadow:0 0 0 3px rgba(91,140,255,.1)}input{min-width:220px;flex:1}.filters{margin:12px 0}.filters select{min-width:180px}
table{width:100%;border-collapse:collapse;min-width:650px}th,td{padding:12px 14px;text-align:left;border-bottom:1px solid var(--border);vertical-align:middle}th{color:var(--muted);font-size:.68rem;text-transform:uppercase;letter-spacing:.06em;font-weight:850;background:#0d141e}tbody tr:hover{background:rgba(255,255,255,.018)}tbody tr:last-child td{border-bottom:0}.tablewrap{padding:0;overflow:auto;margin-top:13px}.badge{font-weight:760}.yes{color:var(--green)}.no{color:var(--red)}.err{color:var(--yellow)}a.btn,.review-btn{display:inline-flex;align-items:center;justify-content:center;background:var(--surface3);color:#dce5ee;text-decoration:none;padding:7px 10px;border:1px solid var(--border);border-radius:8px;font-size:.75rem;font-weight:720;margin:2px 4px 2px 0;white-space:nowrap}.review-btn:hover,a.btn:hover{background:#26364a;border-color:#40536c;color:#fff}.empty{color:var(--muted);text-align:center;padding:28px}
.section-head{display:flex;align-items:flex-end;justify-content:space-between;gap:16px;margin:25px 0 13px}.section-head h2{margin:0}.section-head .sub{margin:5px 0 0}.activity{padding:0;overflow:hidden}.activity-row{display:grid;grid-template-columns:105px 1fr auto;gap:15px;align-items:center;padding:14px 17px;border-bottom:1px solid var(--border)}.activity-row:last-child{border-bottom:0}.activity-date{color:var(--muted);font-size:.74rem;white-space:nowrap}.activity-main{min-width:0}.activity-action{font-weight:780;font-size:.81rem}.activity-details{color:#9eabb9;font-size:.76rem;margin-top:3px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.activity-dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--green);margin-right:8px;box-shadow:0 0 0 3px rgba(53,201,138,.08)}
.subtabs{margin:18px 0 14px;padding:4px;width:max-content;background:var(--surface);border:1px solid var(--border);border-radius:11px}.subtab{padding:8px 12px}
.media-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:14px}.media-card{background:rgba(16,23,34,.92);border:1px solid var(--border);border-radius:14px;overflow:hidden;box-shadow:var(--shadow);transition:.16s}.media-card:hover{border-color:#3a4d67;transform:translateY(-1px)}.poster{width:100%;aspect-ratio:2/3;object-fit:cover;background:#0c121a;display:block}.poster-placeholder{width:100%;aspect-ratio:2/3;display:grid;place-items:center;background:linear-gradient(145deg,#172233,#0c121a);color:#718095;font-size:2rem}.media-info{padding:12px}.media-title{font-weight:850;line-height:1.25;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.media-meta{color:var(--muted);font-size:.74rem;margin-top:5px}.media-actions{display:flex;flex-wrap:wrap;gap:5px;margin-top:10px}.media-actions a.btn{margin:0}.series-card{cursor:pointer}.series-summary{display:flex;gap:7px;flex-wrap:wrap;margin-top:9px}.mini-pill{padding:4px 7px;border-radius:999px;background:#1c2735;font-size:.68rem;font-weight:800;color:#aeb9c6}.mini-pill.y{color:var(--green)}.mini-pill.n{color:var(--red)}.mini-pill.w{color:var(--yellow)}.series-detail{display:none}.series-detail.active{display:block}.series-detail-head{display:grid;grid-template-columns:180px 1fr;gap:24px;align-items:start;margin:20px 0}.series-detail-poster{width:180px;aspect-ratio:2/3;object-fit:cover;border-radius:14px;border:1px solid var(--border);box-shadow:var(--shadow)}.series-detail-title{font-size:1.6rem;font-weight:900}.series-detail-meta{color:var(--muted);margin-top:7px}.series-detail-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:16px}.episode-list{display:grid;gap:8px}.episode-card{display:grid;grid-template-columns:90px 1fr auto;gap:15px;align-items:center;padding:14px 16px;background:var(--surface);border:1px solid var(--border);border-radius:12px}.episode-number{font-weight:850}.episode-title{font-weight:750}.episode-status{font-size:.78rem;margin-top:4px}.episode-actions{display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end}.episode-ok{color:var(--green);font-weight:800}.episode-no{color:var(--red);font-weight:800}.episode-err{color:var(--yellow);font-weight:800}.config-group{padding:13px 0;border-top:1px solid var(--border)}.config-group:first-of-type{border-top:0}.config-group h4{margin:0 0 10px;font-size:.82rem}.config-field{display:grid;grid-template-columns:125px 1fr;align-items:center;gap:12px;margin:8px 0}.config-field>span{color:var(--muted);font-size:.76rem}.config-field input{width:100%;min-width:0}.config-note{margin-top:12px;line-height:1.5}.test-btn{margin-top:8px;background:var(--surface3);border-color:var(--border)}.test-result{font-size:.76rem;margin-top:7px;min-height:1em}.test-result.ok{color:var(--green)}.test-result.bad{color:var(--red)}
.settings-grid{display:grid;grid-template-columns:minmax(290px,.8fr) minmax(0,1.5fr);gap:18px;align-items:start}.settings-card{padding:19px}.settings-card h3{margin:0 0 6px}.settings-card .desc{color:var(--muted);font-size:.84rem;line-height:1.5;margin:0 0 16px}.setting-item{display:flex;align-items:center;justify-content:space-between;gap:15px;padding:14px 0;border-top:1px solid var(--border)}.setting-item:first-of-type{border-top:0}.setting-copy strong{display:block;font-size:.85rem}.setting-copy span{display:block;color:var(--muted);font-size:.76rem;margin-top:4px}.switch{position:relative;width:44px;height:24px;flex:0 0 auto}.switch input{display:none}.switch span{position:absolute;inset:0;background:#2b3542;border-radius:999px;cursor:pointer;transition:.2s}.switch span:before{content:"";position:absolute;width:18px;height:18px;left:3px;top:3px;background:#fff;border-radius:50%;transition:.2s}.switch input:checked+span{background:var(--green)}.switch input:checked+span:before{transform:translateX(20px)}.savebar{display:flex;justify-content:flex-end;align-items:center;flex-wrap:wrap;gap:8px;margin-top:17px}.profile-list{display:grid;gap:12px}.profile-card{background:rgba(21,29,39,.76);border:1px solid var(--border);border-radius:13px;padding:17px}.profile-top{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px}.profile-name{font-size:.98rem;font-weight:850}.profile-type{color:var(--muted);font-size:.76rem;margin-top:3px}.profile-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}.profile-field{display:flex;flex-direction:column;gap:7px}.profile-field label{color:var(--muted);font-size:.68rem;text-transform:uppercase;font-weight:800;letter-spacing:.05em}.profile-field select{width:100%;min-width:0}.profile-field input{width:100%;min-width:0;flex:initial}.profile-field label,.profile-field span{overflow-wrap:anywhere;line-height:1.35}.profile-footer{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-top:15px;padding-top:14px;border-top:1px solid var(--border)}.status-pill{display:inline-flex;align-items:center;gap:6px;border-radius:999px;padding:5px 9px;background:#202a36;font-size:.72rem;font-weight:800}.status-pill.on{color:var(--green)}.status-pill.off{color:var(--muted)}.settings-title{margin-top:0;margin-bottom:12px}.settings-title h2{margin-bottom:4px}.settings-title p{margin:0}.tag-list{display:flex;flex-wrap:wrap;gap:7px;margin-top:9px}.tag-chip{border:1px solid var(--border);background:#202a36;color:#e8edf2;border-radius:999px;padding:6px 10px;cursor:pointer;font-size:.75rem}.tag-chip:hover{border-color:var(--accent)}.tag-chip.selected{background:rgba(72,184,128,.14);border-color:var(--green);color:#fff;box-shadow:0 0 0 1px rgba(72,184,128,.12) inset}.retry-fields{grid-template-columns:1fr 1fr;gap:8px;margin-top:9px}.retry-fields label{font-size:.68rem;color:var(--muted);text-transform:uppercase;font-weight:800}.retry-fields input{margin-top:5px}
@media(max-width:1000px){.services{grid-template-columns:repeat(3,1fr)}.settings-grid{grid-template-columns:1fr}.profile-grid{grid-template-columns:repeat(3,1fr)}}@media(max-width:720px){main{padding:22px 15px 40px}.services{grid-template-columns:1fr 1fr}.app-header{align-items:flex-start}.tabs{overflow:auto;flex-wrap:nowrap}.tab{white-space:nowrap}.activity-row{grid-template-columns:1fr;gap:5px}.profile-grid{grid-template-columns:1fr 1fr}.profile-footer{align-items:flex-start;flex-direction:column}.savebar{justify-content:stretch}.savebar button{width:100%}.media-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}@media(max-width:480px){.services{grid-template-columns:1fr}.profile-grid{grid-template-columns:1fr}.version{display:none}.media-grid{grid-template-columns:1fr 1fr}}

/* ForcedFR UI refresh — inspired by modern media-library products. */
:root{--bg:#090d14;--surface:#111824;--surface2:#161f2d;--surface3:#202b3b;--border:#293548;--text:#f4f6fb;--muted:#929eb0;--accent:#8b7bff;--accent2:#5c8dff;--green:#3bd6a0;--yellow:#f5bd61;--red:#ff727f;--shadow:0 18px 45px rgba(0,0,0,.16)}
html,body{min-height:100%;background:var(--bg)}body{background:radial-gradient(ellipse at 70% -10%,rgba(94,112,255,.13),transparent 40%),var(--bg);color:var(--text);transition:background .2s,color .2s}
.app-shell{min-height:100vh;display:grid;grid-template-columns:248px minmax(0,1fr)}
.sidebar{position:sticky;top:0;height:100vh;padding:25px 16px 18px;background:rgba(13,18,28,.94);border-right:1px solid var(--border);display:flex;flex-direction:column;gap:25px;z-index:5;backdrop-filter:blur(18px)}
.sidebar-brand{display:flex;align-items:center;gap:11px;padding:0 9px}.sidebar-brand .brand-logo{width:42px;height:42px;flex-basis:42px}.sidebar-brand h1{font-size:1.12rem;letter-spacing:-.04em;margin:0}.sidebar-brand small{display:block;color:var(--muted);font-size:.69rem;margin-top:3px}
.nav-label{padding:0 12px;color:var(--muted);font-size:.65rem;font-weight:850;letter-spacing:.13em;text-transform:uppercase;margin-bottom:8px}.tabs{display:flex;flex-direction:column;gap:5px;margin:0;padding:0;border:0;background:transparent;box-shadow:none}.tab{display:flex;align-items:center;gap:12px;text-align:left;width:100%;padding:12px 13px;border:1px solid transparent;border-radius:11px;color:#aeb9ca;font-size:.83rem;font-weight:700}.tab .nav-icon{width:22px;text-align:center;font-size:1rem}.tab:hover{background:var(--surface2);color:var(--text)}.tab.active{background:linear-gradient(100deg,rgba(139,123,255,.2),rgba(92,141,255,.08));border-color:rgba(139,123,255,.25);color:var(--text);box-shadow:none}.sidebar-bottom{margin-top:auto;border:1px solid var(--border);background:var(--surface);border-radius:13px;padding:13px}.sidebar-bottom strong{font-size:.78rem}.sidebar-bottom p{font-size:.72rem;line-height:1.45;color:var(--muted);margin:5px 0 0}
main.main-content{max-width:none;width:100%;min-width:0;margin:0;padding:30px clamp(20px,3vw,46px) 56px}.app-header{margin-bottom:20px;align-items:center}.app-header .brand-logo{display:none}.app-header h1{font-size:1.8rem}.app-header .sub{margin-top:7px}.header-actions{display:flex;align-items:center;gap:10px}.theme-toggle{display:inline-flex;align-items:center;gap:8px;border:1px solid var(--border);background:var(--surface);color:var(--text);border-radius:10px;padding:9px 12px;font-size:.78rem;font-weight:750}.version{border-radius:999px;padding:7px 11px}.services{grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;margin:24px 0 28px}.service-card{background:linear-gradient(145deg,var(--surface),rgba(20,28,41,.75));border-radius:15px;padding:14px;box-shadow:none;transition:transform .18s,border-color .18s}.service-card:hover{transform:translateY(-2px);border-color:#475a78}.service-icon{border-radius:10px}.service-name{font-size:.8rem}.service-meta{font-size:.72rem}.panel{animation:panelIn .22s ease both}@keyframes panelIn{from{opacity:.4;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
.grid{gap:13px}.card,.tablewrap,.settings-card,.media-card{background:var(--surface);border-color:var(--border);box-shadow:0 8px 28px rgba(0,0,0,.08);border-radius:16px}.card{padding:19px}.label{letter-spacing:.09em}.value{font-variant-numeric:tabular-nums}.section-head{margin-top:30px}.section-head h2{font-size:1.18rem}.activity-row{padding:16px 19px}.activity-dot{background:var(--accent2)}.media-grid{grid-template-columns:repeat(auto-fill,minmax(176px,1fr));gap:17px}.media-card{transition:transform .2s,border-color .2s,box-shadow .2s}.media-card:hover{transform:translateY(-4px);border-color:rgba(139,123,255,.65);box-shadow:0 16px 34px rgba(0,0,0,.2)}.poster{background:var(--surface2)}.poster-placeholder{background:linear-gradient(145deg,var(--surface3),var(--surface));color:var(--muted)}.media-info{padding:14px}.media-title{font-size:.88rem}.media-meta{line-height:1.5}.mini-pill,.status-pill,.tag-chip{background:var(--surface3);color:var(--muted)}.toolbar{background:var(--surface);border-radius:14px;padding:15px}.toolbar button{background:var(--surface2);border-color:var(--border)}.progress{background:var(--surface3)}select,input,textarea{background:var(--bg);color:var(--text);border-color:var(--border)}th{background:var(--surface2)}tbody tr:hover{background:rgba(130,145,170,.07)}a.btn,.review-btn{background:var(--surface2);color:var(--text);border-color:var(--border)}.profile-card{background:var(--surface2)}.settings-card{padding:22px}
body[data-theme="light"]{color-scheme:light;--bg:#f4f6fb;--surface:#ffffff;--surface2:#f0f3f9;--surface3:#e8edf6;--border:#dce2ed;--text:#20283a;--muted:#6d778a;--accent:#6556dc;--accent2:#416fe0;--green:#16845e;--yellow:#a86a08;--red:#c53f52;--shadow:0 12px 32px rgba(38,53,83,.07);background:var(--bg)}
body[data-theme="light"]{background:radial-gradient(ellipse at 70% -10%,rgba(101,86,220,.08),transparent 42%),var(--bg)}body[data-theme="light"] .sidebar{background:rgba(255,255,255,.92)}body[data-theme="light"] .sidebar-bottom,body[data-theme="light"] .service-card{background:var(--surface)}body[data-theme="light"] .tab{color:#59647a}body[data-theme="light"] .tab.active{color:var(--text);background:#eeebff;border-color:#ded8ff}body[data-theme="light"] .service-icon{background:#f1f4fa}body[data-theme="light"] .activity-details{color:var(--muted)}body[data-theme="light"] .mini-pill,body[data-theme="light"] .status-pill,body[data-theme="light"] .tag-chip{background:var(--surface3);color:var(--muted)}body[data-theme="light"] .tag-chip.selected{color:var(--green)}body[data-theme="light"] .poster-placeholder{background:linear-gradient(145deg,#e8edf6,#f8faff)}body[data-theme="light"] .profile-card{background:var(--surface2)}body[data-theme="light"] .progress{background:#e2e7f0}body[data-theme="light"] .tab:hover{background:var(--surface2);color:var(--text)}
body[data-theme="light"] .version{background:#e5e9f2;color:#30394b;border-color:#cbd3e1}
body[data-theme="light"] .toolbar button,body[data-theme="light"] .subtab,body[data-theme="light"] .tab{color:#344054;border-color:#cbd5e4}
body[data-theme="light"] .toolbar button{background:#e9edf5}
body[data-theme="light"] .toolbar button:hover,body[data-theme="light"] .subtab:hover,body[data-theme="light"] button:hover{background:#dce4f2;color:#172033}
body[data-theme="light"] .toolbar button:disabled,body[data-theme="light"] button:disabled{background:#e8ecf3!important;color:#657187!important;border-color:#d2d9e5!important;opacity:1}
body[data-theme="light"] .service-card{background:#fff}
body[data-theme="light"] .service-card:first-child .service-icon{background:#edf1f8;color:#35415a}
body[data-theme="light"] .service-card:first-child .service-icon svg{color:#35415a}
body[data-theme="light"] .test-btn,body[data-theme="light"] .settings-card button:not(.primary){background:#e8edf6;color:#283449;border-color:#cbd5e4}
body[data-theme="light"] .test-btn:hover,body[data-theme="light"] .settings-card button:not(.primary):hover{background:#dce5f3;color:#172033}
body[data-theme="light"] .profile-card{background:#f4f6fb}
body[data-theme="light"] .profile-field label,body[data-theme="light"] .profile-field span{overflow-wrap:anywhere;line-height:1.35}
body[data-theme="light"] .profile-grid{grid-template-columns:repeat(2,minmax(0,1fr))}
body[data-theme="light"] .profile-field input{width:100%;min-width:0;flex:initial}
body[data-theme="light"] .config-field input{min-width:0}
@media(max-width:760px){.profile-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.config-field{grid-template-columns:1fr;gap:5px}.savebar{display:flex;flex-wrap:wrap;align-items:center;gap:8px}.savebar button{max-width:100%;white-space:normal}.savebar .test-result{flex-basis:100%}.settings-card .profile-card{min-width:0}.profile-field label{overflow-wrap:anywhere;line-height:1.35}}
@media(max-width:520px){.profile-grid{grid-template-columns:1fr}.profile-footer{gap:8px}.services .service-card{align-items:center}.service-card:first-child .service-icon{flex:0 0 34px}.app-header .version{display:inline-flex}}
@media(max-width:1100px){.app-shell{grid-template-columns:210px minmax(0,1fr)}.services{grid-template-columns:repeat(3,minmax(0,1fr))}.media-grid{grid-template-columns:repeat(auto-fill,minmax(155px,1fr))}}
@media(max-width:760px){.app-shell{grid-template-columns:1fr}.sidebar{position:sticky;top:0;height:auto;padding:11px 14px;gap:10px;border-right:0;border-bottom:1px solid var(--border)}.sidebar-brand{padding:0 3px}.sidebar-brand .brand-logo{width:34px;height:34px;flex-basis:34px}.sidebar-brand h1{font-size:1rem}.sidebar-brand small,.nav-label,.sidebar-bottom{display:none}.tabs{flex-direction:row;overflow-x:auto;gap:5px;padding-bottom:2px}.tab{width:auto;flex:0 0 auto;padding:9px 11px;font-size:.75rem;white-space:nowrap}.tab .nav-icon{width:auto}.main-content{padding:23px 15px 38px!important}.app-header{align-items:flex-start;gap:10px}.app-header h1{font-size:1.45rem}.header-actions{flex-wrap:wrap;justify-content:flex-end}.theme-toggle{font-size:0;padding:9px 11px}.theme-toggle .theme-icon{font-size:1rem}.services{grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.service-card{padding:11px}.media-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.media-info{padding:10px}.settings-grid{grid-template-columns:1fr}.activity-row{grid-template-columns:1fr auto;gap:7px}.activity-date{grid-column:1/-1}.activity-main{min-width:0}.activity-details{white-space:normal}.series-detail-head{grid-template-columns:110px 1fr;gap:14px}.series-detail-poster{width:110px}.episode-card{grid-template-columns:1fr;gap:8px}.episode-actions{justify-content:flex-start}}
@media(max-width:430px){.services{grid-template-columns:1fr 1fr}.service-icon{width:29px;height:29px;flex-basis:29px}.service-icon img{width:21px;height:21px}.service-card{gap:8px}.service-name{font-size:.74rem}.service-meta{font-size:.67rem}.version{display:none}.media-title{font-size:.8rem}.media-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.profile-grid{grid-template-columns:1fr}.config-field{grid-template-columns:1fr;gap:5px}}

/* Responsive correction v2.7.1 */
*,*::before,*::after{box-sizing:border-box}
html,body{width:100%;max-width:100%;overflow-x:hidden}
.app-shell{width:100%;max-width:100%;min-width:0;grid-template-columns:248px minmax(0,1fr)}
.main-content{min-width:0;width:100%;max-width:100%;overflow-x:clip}
.main-content>*{min-width:0;max-width:100%}
#dashCards{grid-template-columns:repeat(auto-fit,minmax(min(100%,190px),1fr))}
.services{grid-template-columns:repeat(auto-fit,minmax(min(100%,190px),1fr))}
.activity,.activity-row,.activity-main,.activity-details,.tablewrap{min-width:0;max-width:100%}
@media(max-width:760px){
 .app-shell{display:flex;flex-direction:column;gap:0}
 .sidebar{position:relative;top:auto;width:100%;height:auto;min-width:0;overflow:hidden;padding:10px 12px;gap:10px}
 .sidebar-brand{min-width:0}
 .tabs{width:100%;max-width:100%;overflow-x:auto;overflow-y:hidden;flex-wrap:nowrap;scrollbar-width:thin;-webkit-overflow-scrolling:touch}
 .tab{flex:0 0 auto}
 main.main-content{width:100%;min-width:0;max-width:100%;padding:20px 14px 34px!important;margin:0}
 .app-header{width:100%;min-width:0;align-items:flex-start}
 .app-header>div:first-child{min-width:0;flex:1}
 .app-header h1{font-size:1.35rem;overflow-wrap:anywhere}
 .app-header .sub{font-size:.82rem}
 .header-actions{flex:0 0 auto;max-width:100%}
 .theme-toggle{min-height:40px}
 .services{grid-template-columns:repeat(2,minmax(0,1fr));width:100%;gap:9px}
 .service-card{min-width:0;overflow-wrap:anywhere;padding:10px}
 #dashCards{grid-template-columns:repeat(2,minmax(0,1fr));gap:9px;width:100%}
 #dashCards .card{min-width:0;padding:14px}
 .grid{grid-template-columns:repeat(2,minmax(0,1fr))}
 .section-head{align-items:flex-start;flex-direction:column;gap:8px}
 .activity-row{grid-template-columns:minmax(0,1fr);gap:6px;padding:13px}
 .activity-date{white-space:normal}
 .activity-details{white-space:normal;overflow-wrap:anywhere;line-height:1.45}
 .media-grid{grid-template-columns:repeat(2,minmax(0,1fr))}
 .tablewrap{overflow-x:auto}
}
@media(max-width:420px){
 main.main-content{padding-left:11px!important;padding-right:11px!important}
 .services{grid-template-columns:minmax(0,1fr)}
 #dashCards{grid-template-columns:repeat(2,minmax(0,1fr))}
 #dashCards .card{padding:11px}
 .value{overflow-wrap:anywhere}
 .media-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
 .header-actions .version{display:none}
 .theme-toggle{padding:8px 10px}
 .tab{padding:8px 10px;font-size:.72rem}
}

.mobile-nav-toggle{display:none;position:fixed;z-index:1002;top:12px;left:12px;border:1px solid var(--border,#dce2ee);border-radius:12px;background:var(--card,#fff);color:var(--text,#20283a);padding:10px 13px;font-weight:700;box-shadow:0 5px 20px #0001}.nav-backdrop{display:none}
.pending-card{display:flex;gap:12px;align-items:flex-start;padding:14px;min-width:0}.pending-card .poster,.pending-card .poster-placeholder{width:70px;min-width:70px;height:100px;object-fit:cover;border-radius:9px}.pending-card-body{min-width:0;flex:1}.pending-card-body strong{overflow-wrap:anywhere}.pending-card .media-actions{display:flex;flex-wrap:wrap;gap:6px}
@media(max-width:760px){.mobile-nav-toggle{display:block}.app-header{padding-top:54px}.sidebar{position:fixed;z-index:1001;inset:0 auto 0 0;width:min(82vw,310px);height:100dvh;min-height:100vh;display:flex;flex-direction:column;align-items:stretch;overflow-y:auto;overflow-x:hidden;transform:translateX(-105%);transition:transform .22s ease;box-shadow:0 0 35px #0003;padding:82px 14px 20px;gap:22px}.sidebar>div{width:100%;min-width:0}.sidebar .nav-label{display:block}.sidebar .sidebar-brand small{display:block}.sidebar .sidebar-bottom{display:block;margin-top:auto}.sidebar .tabs{display:flex;flex-direction:column;align-items:stretch;width:100%;max-width:100%;overflow:visible;overflow-x:visible;overflow-y:visible;flex-wrap:nowrap;gap:5px;padding:0}.sidebar .tab{display:flex;flex:0 0 auto;width:100%;min-width:0;justify-content:flex-start;white-space:normal;text-align:left;padding:12px 13px;font-size:.83rem}.sidebar .tab .nav-icon{width:22px;flex:0 0 22px;text-align:center}.nav-open .sidebar{transform:translateX(0)}.nav-open .nav-backdrop{display:block;position:fixed;z-index:1000;inset:0;background:#10152280}.app-shell{display:block}.main-content{width:100%;min-width:0;padding:16px}.services{grid-template-columns:1fr!important}.grid{grid-template-columns:repeat(2,minmax(0,1fr))!important}.media-grid{grid-template-columns:repeat(1,minmax(0,1fr))!important}.filters{align-items:stretch;flex-wrap:wrap}.filters input{min-width:0;flex:1 1 100%}.filters label{display:flex;align-items:center;gap:8px}.pending-card{width:100%}.header-actions .version{display:none}}

/* ForcedFR 2.8.4 — harmonisation visuelle */
.services{margin-bottom:26px;padding-bottom:24px;border-bottom:1px solid var(--border)}
.service-card{min-height:92px;display:flex;align-items:center;gap:12px}
.service-card .service-icon{width:42px;height:42px;flex:0 0 42px;display:flex;align-items:center;justify-content:center;border-radius:13px}
.service-card .service-icon img{width:30px;height:30px;object-fit:contain}
.service-card:first-child .service-icon{background:var(--surface3);color:var(--text);font-size:1.35rem}
.service-card>div:last-child{min-width:0}
.service-name{line-height:1.25}
.service-meta{line-height:1.3;overflow-wrap:anywhere}
.profile-card .profile-grid{align-items:stretch}
.profile-field{display:flex;flex-direction:column;align-items:stretch;gap:8px;min-width:0}
.profile-field label{display:block;min-height:2.7em;line-height:1.35;overflow-wrap:anywhere}
.profile-field select,.profile-field input{width:100%;min-width:0;box-sizing:border-box}
.confidence-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}
.confidence-grid .profile-field label{min-height:3.2em}
.connection-tests{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;align-items:center}
.connection-tests button{width:100%;min-width:0;white-space:nowrap}
.connection-tests .test-result{grid-column:1/-1;display:none}
.connection-tests .test-result:not(:empty){display:block}
.connection-results{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:12px;padding:12px;border:1px solid var(--border);border-radius:12px;background:var(--surface2)}
.connection-result-item{display:flex;gap:8px;align-items:flex-start;min-width:0;overflow-wrap:anywhere;font-size:.9rem}
.connection-result-item .test-result{min-width:0;overflow-wrap:anywhere}
body[data-theme="light"] .connection-results{background:#f6f8fc}
body[data-theme="light"] .connection-tests button{background:#e8edf6;color:#283449;border-color:#cbd5e4}
body[data-theme="dark"] .connection-tests button{background:var(--surface2);color:var(--text);border-color:var(--border)}
@media(max-width:760px){
  .connection-tests{grid-template-columns:repeat(2,minmax(0,1fr))}
  .connection-results{grid-template-columns:1fr}
  .profile-field label{min-height:0}
  .confidence-grid{grid-template-columns:repeat(2,minmax(0,1fr))}
}
@media(max-width:430px){
  .service-card{min-height:76px}
  .service-card .service-icon{width:34px;height:34px;flex-basis:34px}
  .service-card .service-icon img{width:25px;height:25px}
  .confidence-grid{grid-template-columns:1fr}
  .connection-tests{grid-template-columns:repeat(2,minmax(0,1fr))}
}


/* ForcedFR 2.8.6 — seuils IA, logo et indicateurs de service */
.profile-card .profile-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px;align-items:start}
.profile-card .profile-field{display:flex;flex-direction:column;align-items:stretch;gap:9px;min-width:0}
.profile-card .profile-field span{display:block;min-height:4.1em;line-height:1.35;overflow-wrap:normal;word-break:normal;hyphens:none}
.profile-card .profile-field input{width:100%;min-width:0;box-sizing:border-box}
body[data-theme="dark"] .settings-card .profile-card{background:#151e2d}
body[data-theme="dark"] .settings-card .profile-field span{color:#e5eaf3}
body[data-theme="dark"] .settings-card .profile-field input{background:#080d15;color:#f3f6fb;border-color:#344258}
.services{padding-top:20px;border-top:1px solid var(--border)}
.service-card{min-height:66px;padding:10px 14px;gap:10px}
.service-card .service-icon{width:54px;height:54px;flex:0 0 54px;background:transparent;border-radius:0;overflow:visible}
.service-card .service-icon img{width:54px;height:54px;object-fit:contain;border-radius:50%}
.service-card:first-child .service-icon{width:54px;height:54px;flex-basis:54px;background:transparent}
.service-card:first-child .service-icon img{width:54px;height:54px;object-fit:contain;border-radius:50%}
.service-card .service-name{font-size:.9rem;font-weight:850}
.service-card .service-meta{display:none!important}
.service-card .service-name{display:flex;align-items:center;gap:9px}
/* Indicateurs ::before supprimés : conserver uniquement les pastilles de statut à droite. */
@media(max-width:760px){
 .profile-card .profile-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
 .profile-card .profile-field span{min-height:5.2em;font-size:.9rem}
 .brand-logo{width:52px;height:52px;flex-basis:52px}
}
@media(max-width:430px){
 .profile-card .profile-grid{grid-template-columns:1fr}
 .profile-card .profile-field span{min-height:0}
}


/* ForcedFR 2.8.9 — barre d'état compacte */
.service-status-row{display:grid!important;grid-template-columns:repeat(5,minmax(0,1fr));gap:14px;margin:20px 0 28px;padding:0 0 26px;border-bottom:1px solid var(--border)}
.service-status-row .service-card{display:flex;align-items:center;gap:10px;min-width:0;min-height:76px;padding:12px 14px;border:1px solid var(--border);border-radius:18px;background:var(--surface);box-shadow:0 4px 14px rgba(0,0,0,.05)}
.service-status-row .service-icon{width:44px;height:44px;flex:0 0 44px;display:flex;align-items:center;justify-content:center;background:transparent!important;border:0!important;border-radius:0!important;overflow:visible}
.service-status-row .service-icon img{width:44px;height:44px;object-fit:contain;background:transparent!important;border-radius:0!important}
.service-status-row .service-name{flex:1;min-width:0;font-size:.95rem;font-weight:800;line-height:1.2;overflow-wrap:anywhere}
.service-status-row .service-status-dot{width:11px;height:11px;flex:0 0 11px;border-radius:50%;background:#22c55e;box-shadow:0 0 0 3px rgba(34,197,94,.12)}
.service-status-row .service-status-dot.warning{background:#f59e0b;box-shadow:0 0 0 3px rgba(245,158,11,.14)}
.service-status-row .service-status-dot.offline{background:#ef4444;box-shadow:0 0 0 3px rgba(239,68,68,.12)}
.service-status-row .service-meta,.service-status-row .service-card .service-meta{display:none!important}
body[data-theme="light"] .service-status-row .service-card{background:#fff;border-color:#dbe2ef;box-shadow:0 4px 14px rgba(30,41,59,.06)}
body[data-theme="dark"] .service-status-row .service-card{background:#111a28;border-color:#29374c;box-shadow:none}
@media(max-width:1000px){.service-status-row{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(max-width:650px){.service-status-row{grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.service-status-row .service-card{min-height:64px;padding:9px}.service-status-row .service-icon,.service-status-row .service-icon img{width:36px;height:36px;flex-basis:36px}.service-status-row .service-name{font-size:.88rem}}
@media(max-width:380px){.service-status-row{grid-template-columns:1fr}}


.service-status-row{display:grid!important;grid-template-columns:repeat(5,minmax(0,1fr));gap:14px;margin:20px 0 28px;padding:0 0 26px;border-bottom:1px solid var(--border)}
.service-status-row .service-card{display:flex;align-items:center;gap:10px;min-width:0;min-height:76px;padding:12px 14px;border:1px solid var(--border);border-radius:18px;background:var(--surface);box-shadow:0 4px 14px rgba(0,0,0,.05)}
.service-status-row .service-icon{width:44px;height:44px;flex:0 0 44px;display:flex;align-items:center;justify-content:center;background:transparent!important;border:0!important;border-radius:0!important;overflow:visible}
.service-status-row .service-icon img{width:44px;height:44px;object-fit:contain;background:transparent!important;border-radius:0!important}
.service-status-row .service-name{flex:1;min-width:0;font-size:.95rem;font-weight:800;line-height:1.2;overflow-wrap:anywhere}
.service-status-row .service-status-dot{width:11px;height:11px;flex:0 0 11px;border-radius:50%;background:#22c55e;box-shadow:0 0 0 3px rgba(34,197,94,.12)}
.service-status-row .service-status-dot.warning{background:#f59e0b;box-shadow:0 0 0 3px rgba(245,158,11,.14)}
.service-status-row .service-status-dot.offline{background:#ef4444;box-shadow:0 0 0 3px rgba(239,68,68,.12)}
.service-status-row .service-meta{display:none!important}
body[data-theme="light"] .service-status-row .service-card{background:#fff;border-color:#dbe2ef;box-shadow:0 4px 14px rgba(30,41,59,.06)}
body[data-theme="dark"] .service-status-row .service-card{background:#111a28;border-color:#29374c;box-shadow:none}
@media(max-width:1000px){.service-status-row{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(max-width:650px){.service-status-row{grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.service-status-row .service-card{min-height:64px;padding:9px}.service-status-row .service-icon,.service-status-row .service-icon img{width:36px;height:36px;flex-basis:36px}.service-status-row .service-name{font-size:.88rem}}
@media(max-width:380px){.service-status-row{grid-template-columns:1fr}}


/* Final status-bar normalization */
.service-status-row .service-card{position:relative;display:flex!important;flex-direction:row!important;align-items:center!important;gap:10px!important;min-width:0!important}
.service-status-row .service-icon,.service-status-row .service-icon img{width:44px!important;height:44px!important;flex:0 0 44px!important;max-width:44px!important;max-height:44px!important}
.service-status-row .service-icon img{object-fit:contain!important}
.service-status-row .service-name{display:block!important;writing-mode:horizontal-tb!important;word-break:normal!important;overflow-wrap:normal!important;white-space:normal!important;flex:1 1 auto!important;min-width:0!important}
.service-status-row .service-meta,.service-status-row .service-card .value,.service-status-row .service-card .status-dot{display:none!important;visibility:hidden!important}
.service-status-row .service-status-dot{display:block!important;visibility:visible!important;position:static!important;width:11px!important;height:11px!important;flex:0 0 11px!important;border-radius:50%!important;background:#22e58b!important;box-shadow:0 0 8px rgba(34,229,139,.85),0 0 0 3px rgba(34,229,139,.13)!important}
.service-status-row .service-status-dot.warning{background:#ffad33!important;box-shadow:0 0 8px rgba(255,173,51,.85)!important}
.service-status-row .service-status-dot.offline{background:#ff4d5e!important;box-shadow:0 0 8px rgba(255,77,94,.85)!important}


/* ForcedFR 2.8.9 — ajustements navigation mobile et indicateurs */
.service-status-row .service-card .status-dot,
.service-status-row .service-card .status-indicator,
.service-status-row .service-card::before{display:none!important;content:none!important}
.mobile-brand{display:none}
@media(max-width:760px){
  .mobile-brand{display:flex;position:fixed;z-index:1003;top:10px;left:12px;align-items:center;gap:8px;max-width:calc(100vw - 110px);height:42px;color:var(--text);font-weight:850;letter-spacing:-.02em}
  .mobile-brand img{width:34px;height:34px;object-fit:contain;flex:0 0 34px}
  .mobile-nav-toggle{display:flex!important;align-items:center;justify-content:center;gap:7px;position:fixed;z-index:1004;top:10px;left:auto!important;right:12px!important;min-height:42px;border:1px solid rgba(130,155,190,.5)!important;border-radius:12px;background:#151c2b!important;color:#fff!important;padding:9px 12px!important;font-weight:800;box-shadow:0 5px 18px rgba(0,0,0,.28)!important}
  body[data-theme="light"] .mobile-nav-toggle{background:#fff!important;color:#172033!important;border-color:#cbd5e1!important}
  .sidebar{inset:0 0 0 auto!important;transform:translateX(105%)!important;width:min(84vw,320px)!important;padding:72px 16px 20px!important;box-sizing:border-box;align-items:stretch!important}
  .nav-open .sidebar{transform:translateX(0)!important}
  .sidebar-brand{display:flex!important;align-items:center!important;gap:10px!important;padding:0 6px 12px!important;border-bottom:1px solid var(--border)}
  .sidebar-brand img{width:38px!important;height:38px!important;flex:0 0 38px!important}
  .sidebar-brand>div{min-width:0}
  .sidebar-brand h1{line-height:1.15}
  .sidebar .tabs{display:flex!important;flex-direction:column!important;align-items:stretch!important;width:100%!important;gap:6px!important}
  .sidebar .tab{display:flex!important;align-items:center!important;gap:11px!important;width:100%!important;min-height:44px;box-sizing:border-box;padding:11px 12px!important;border-radius:10px;text-align:left!important}
  .sidebar .tab .nav-icon{display:inline-flex;align-items:center;justify-content:center;width:22px!important;flex:0 0 22px!important}
  .sidebar .sidebar-bottom{margin-top:auto!important}
  .nav-open .nav-backdrop{display:block!important;position:fixed;z-index:1000;inset:0;background:rgba(5,10,20,.62);backdrop-filter:blur(2px)}
  .app-header{padding-top:58px!important}
}


/* Sidebar correction: fixed to viewport on desktop; main content scrolls independently. */
@media(min-width:761px){
  .sidebar{position:fixed!important;top:0!important;left:0!important;right:auto!important;width:248px!important;height:100vh!important;height:100dvh!important;max-height:100vh;overflow-y:auto!important;overflow-x:hidden!important;overscroll-behavior:contain;}
  .app-shell{display:block!important;min-height:100vh;}
  .main-content{margin-left:248px!important;min-width:0;}
}
@media(min-width:761px) and (max-width:1100px){
  .sidebar{width:210px!important;}
  .main-content{margin-left:210px!important;}
}

/* Layout fix: dashboard pending cards and full-height sidebar */
#dashPending.media-grid{display:grid!important;grid-template-columns:repeat(5,minmax(0,1fr))!important;gap:14px;width:100%;align-items:stretch;}
#dashPending.media-grid>.pending-card{display:flex;flex-direction:column;min-width:0;width:100%;max-width:100%;overflow:hidden;}
#dashPending .pending-card-body{display:flex;flex:1 1 auto;flex-direction:column;min-width:0;width:100%;overflow-wrap:anywhere;word-break:normal;}
#dashPending .pending-card-body>strong{display:block;min-width:0;max-width:100%;overflow-wrap:anywhere;word-break:break-word;line-height:1.35;}
#dashPending .pending-card-body .small{min-width:0;overflow-wrap:anywhere;}
#dashPending .media-actions{display:flex;flex-direction:column;align-items:stretch;gap:7px;width:100%;min-width:0;margin-top:auto;}
#dashPending .media-actions>*{box-sizing:border-box;min-width:0;max-width:100%;width:100%;white-space:normal;overflow-wrap:anywhere;}
.app-shell{align-items:stretch;min-height:100vh;min-height:100dvh;}
.app-shell>.sidebar{align-self:stretch;min-height:100vh;min-height:100dvh;height:100vh;height:100dvh;position:sticky;top:0;overflow-y:auto;}
@media(max-width:1200px){#dashPending.media-grid{grid-template-columns:repeat(3,minmax(0,1fr))!important;}}
@media(max-width:760px){#dashPending.media-grid{grid-template-columns:repeat(2,minmax(0,1fr))!important;gap:10px;}.app-shell>.sidebar{position:fixed;top:0;bottom:0;left:auto;right:0;height:100dvh;min-height:100vh;min-height:100dvh;}}
@media(max-width:480px){#dashPending.media-grid{grid-template-columns:minmax(0,1fr)!important;}}


/* Responsive main-content sizing paired with the fixed sidebar. */
@media(min-width:761px){
  .app-shell{display:block!important;width:100%;max-width:100%;min-height:100vh;min-height:100dvh;}
  .app-shell>.sidebar{position:fixed!important;top:0!important;right:auto!important;bottom:0!important;left:0!important;width:248px!important;height:100vh!important;height:100dvh!important;max-height:100dvh;overflow-y:auto!important;overflow-x:hidden!important;}
  main.main-content{box-sizing:border-box;width:calc(100% - 248px)!important;max-width:calc(100% - 248px)!important;min-width:0;margin:0 0 0 248px!important;overflow-x:clip;}
}
@media(min-width:761px) and (max-width:1100px){
  .app-shell>.sidebar{width:210px!important;}
  main.main-content{width:calc(100% - 210px)!important;max-width:calc(100% - 210px)!important;margin-left:210px!important;}
}
@media(max-width:760px){
  .app-shell{display:block!important;width:100%;max-width:100%;}
  .app-shell>.sidebar{position:fixed!important;top:0!important;right:0!important;bottom:0!important;left:auto!important;width:min(82vw,310px)!important;height:100vh!important;height:100dvh!important;max-height:100dvh;transform:translateX(105%)!important;overflow-y:auto!important;}
  .nav-open .app-shell>.sidebar{transform:translateX(0)!important;}
  main.main-content{box-sizing:border-box;width:100%!important;max-width:100%!important;min-width:0;margin:0!important;overflow-x:clip;padding:16px!important;}
  .mobile-nav-toggle{left:auto!important;right:12px!important;}
}

</style></head><body><div class="mobile-brand" aria-label="ForcedFR"><img src="data:image/webp;base64,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" alt="ForcedFR"><span>ForcedFR</span></div><button class="mobile-nav-toggle" id="mobileNavToggle" aria-label="Afficher la navigation" aria-expanded="false">☰ <span>Menu</span></button><div class="nav-backdrop" id="navBackdrop"></div><div class="app-shell"><aside class="sidebar"><div class="sidebar-brand"><img class="brand-logo" src="data:image/webp;base64,UklGRmhIAABXRUJQVlA4WAoAAAAQAAAA/wAA/wAAQUxQSPoKAAABDMZtGzmy+m/bnrB74RsRE6APiZ+j32B0j9KD0qzQ1EB7jVzA0a5Md+QXJiV25ffqZ+kpGU//1gK2f6qcVrO+cXdrLg4JFdwd0uLu7pZSd8XqReruLqFe3B1SDR7PQtyT3WzO/K6snXPmzFx/ImICILltJEn6/5cngr5UVc8SlTHXiJgA8t+3DX5hCW1vHHLLhJkLV65ctXrl6uULpk+bOHJIr84psaF+RqExh7cdsnjTuz8eOVdQVt/scLZKVJIkKrW2tDga6yptl3498tMHWxb1TvLXCYcuuM3AJU98dTy3thVuqbfw0Fl57seX7x3XPlQvCvqwbgtf3Jdb04p/pErEPzbmHnx5UVqYnvsC0xe9dqTUCQBU4QDgLD76yvweEfxmiO235vsSJwCqWgDOssNPjEgw8lj8+FeyqyWAqh5A7e9vTE4x8lXooPVnGgFQRgJoznlpaCQ3mdLu2lsFgDIVQPWxB2+08lDctHfzKUAZDNDiT6bEco6py32nmwHKbKDp9PobjfwSMOSNfApQpgMofLGnmU8Cx2ZVAlQDAdvbQ/z5I3jsjjqAaiRQ+WlGAF9Yx3xXD1ANBaq/uMWPH4z9P6gGqMYC1e/3MHBC+ovXAKrBQNHzaTwQf/clgGo0cOmeRK0LmHa0FVTD0XpimkXTbni/AVTjUftSR+0KufMKQDUfyFnkr1G9P3eAciEa3uum06DQuwoByolAwd0hmpP2aQsoR6LlwzRtsczJAeVL4Nxsk4bEb60D5U5UPxGjGb13UVAOhfRTujbop14E5VRkZ+g0wLKuApRbYVvrz7yYLY2gHIum7dGM65BFQdmMERT0yzZM63sClM2EnTjYg2Ej/wTlYPw2mFlj80C5GFcmsUk3rQBUiZyPomS6jkG6uWWg3IxriwzM0S8rB1Wl+9NQVGUaGaNfVgWqSjRyXtSsNDBFt7wKVMGe0N3kxKheZWCIbm45qArRVeTQqF6hZ8esUlCF4gO6quXcqJjHjHHFoMqEPKKr5Oi4NoURQ3JBFemaW3TXV8nhcaUfE27IBlWg+9zSVV/VOT+Od2BAwk5QxeQKXVUtG8S38aoLeJUq0H2udNdXlSwRrweqzHCfHfK5TxK6qqpljXA8aFTXrCpQuT0mCV3VskrUzFHVgFxQuZEgCV3Vsk1c7qWi6w6BygwJklDVslDsTlCN/5ugMiMJkuhqWerTRrVkNslFkiChq7NVVM9QyYAiUHnJJ11lLRSXe6kidjeovCSJkP1iV4wK9OslKq/cy4qlxw3KG1UKmZaNikmKSzgMKpQ4laww/QYqmtJ6vbIySiEauDZKUdH7QYUTeyKUdLuTCohjtYLSLkJAKC5frxjLq6BCildMShlfIyrlIxQSvhdUUPFjqDKW2KmwNC9URNxJCAvFiRglZDqpwDqWKKDN7xAZHImU70EqtLDPk63dnxAaioMRcj0gUcF1LJQpIRuig8OR8ixzUOGxz5clbB+Eh+JwpBxj60WoeZYMlo9BBRhfWX3X86oY2W7y3SZQIcb9Pkv6XZSOR/tqjoMKUtNUH5k+gCBRfOnnm7QCzVLdhylK902mRIVZWuOT4J8hTvjK4osepSJV1MUXd1GVejDBCGygrat8EPADVBIkkV0jy8+79GLVXOXEMCivo3fznZS3Z1HHdK8Mr0Ks8KxXMWf5KwmDjoZ707OCwwRzKnp7s0KiXC5zpZVeGN8Dn03G2wbP4n4TrzNRnvWtEq/SGz1b4KTC7Zjh2fMQLzzjUfB+EdsT5Em7PBHLa+PJoDoRqx/myTwnFfFVnjwBEcOzHpg+5y8fz/nE5C7kEN8ZcyjUXUIOf32MKZeS3XWx8Y3fikwp7uSuf43qwC55bVT1dTfRTlXuyl+0eYy7W6nK3fOXtNzdE1AXWToecWN4S9Sec+P3k7pktqO9a3AVelTLjo6vLa4iT6vr0cZ+8HcVk82EjWN3oKvE80Jgwp4gV6m5QhADfg5w1SZfbZyLTwZiV6CrdgUqI1cL2x/KlkNPC2ED2Vuoq45FKsvfxKp7GHHQTftCdf3Vl8u01Muqcga/FhmxM9BV/B+q+rOvNxM/+ruKPKEyp+FFrK6C96vLcV4UWWZXAT+pLCd4aXxkdGX8QNTe0Lsi61Uli9tI3K6hGrgSmuluikPMHFPdDaiBiKF2kLs0m5jZ0tylXBazi0nuwo+L2YkId+YvVGVv31ndkS1qsriXdB6sE7O7iYcT7GrK2ponetK9HGNkbbCleRL3G/sWiuwYTyxZqrG5ryyekMfUk8U9TDyeaJ+yeDTe4lnnYgjX1XTPQvYrif/BmWjPyJNKujqB2xf70OjFZLuSdB8h8upSJvGyQz6Uo2WyP/TuKEjzJuBn5bRM5RJ28J2fN2STYshgl/BDDvMw8XpkHZRB/oWoH+ldbLZCRBHnk70zviVWH5m9I/NbRMo+k/iwfS7ECX8k+8L0nki9rvcFmWEXJ/s04tP4bIgScpJ9QzZzAU7wksFHA6sgRqgcQnwc8C0PRA7wXYCvyKIWHjihcwnxeWoORAiX2vhO94QYbTH4jnQrgeZ5PxTcQGQ0v6V9B8RrBjlIv1KIDmpGEFktn4jP14HykFGVEBuUDyUyWz8UnQ8scpEhlRAZlA8mslveF5t3zPKRftcgLrjalyjQuF1kthmVQLpcgqjgUheizHtaRaX1PqLQuMMQExyJUwqZWg8RQdMsolj/D8Tk00DlkBuuQDyQ240oeV2LeEj3EkWHfQfRwIlEZZFBNogFyscThevvdwrGUwalkaid8MzycCqVKL9vLjxaPopGEjUuaYAowHErUaX1RXH4JFQdpM0piAFOdyBqHZYLEcDVDKLeOdXYH+pW6FRkXu/kP+kpC1Fz5A7wHrIiibq7nAXf4WQnovZBF8FzON+PqH9sCfgN1yYQBuoWlIPXUL5QxwJiWFUDPkPdSj1hoymzFjyGpgdMhJXGuxrAX2jZ4EfYaX2oEbwF6bVgwlK/zc3gK9i3RBC2Bj3RDJ6C48kAwtqAx5vAT7BvDiTs9X+kFryE+vUBhMXmNdXgI1SvtRA2GxcVgYdQvsJAmD3uCvgHRbP1hOHDT1BwDrIzCNvbvNYMnoH04/WE9YH3VoBfUP1EDGG/YcZl8ApsC01EEwcdpuAR4PTNOqKRKS82gj9gf6ct0c6AVfngDZTcEUI0dchBCTwBnBqlIxqb9EQZwA2o2t6WaK8p49tm8AGkI+PNRJPDMi8D2gcUP5ZINPvGN2uhdWj+doCeaLjf7JMStAz4c3kY0fiUxwoArQJKt3Qk2m+48eliQIuA6o8GmQgXGvp+UAtoDeDYO9afcGPA5O9qAS0Bmg8ujSJcGTx+Rz2gFUDTzjlRhDuDx39sA7QAqNs5K5xwqbXnppxWgHGA7ZNxoYRbde3vOFAHsAto+fPpHn6Eb8PHvZErASwCUJq18Do94V9j13Vf2CSALQDqjjza05/wsl+3O3eWUoAVAOrOPpMRQfg6pOfdP9gkAGoDgJozz90cpyMcHtjt1vdOV1IAagFAG87veGBwrI5wuyl24N1Z56pbAUBZAEDrLmfdmZFqJdzvnzJwxdafc8rtcCkXXDrr8ve/fvvwv/gTYdQFJfee8/SOU1fKGloo5JSaKgvOZm1aPKxjmIGIpz4wtl2vmxc9tOWtz3YeOfXruQuXruSXlJYWFeZd+uPM4V1fvr39vuVje3eICyCiqzNag6Nik1JT23TocmP3bulpndokxUYEWQw68v84AlZQOCBIPQAA0LoAnQEqAAEAAT49GIpDIiGhFlmuwCADxLYEOADKidF+idP5b3sn9a/an2LKt/Yf7L+lP7T+1fzG6SuqPqA923nX/k/5b/Lfsn8tP89/zf857oPzt/x/z/+gL9Uv+R/jf9P8H/9H+0fvI/vf+w/Kf4I/0v/Cf9z/Jfv/8yv+z/b73X/3//S/739o/kE/rP+M/6nri+xn/lv+f/8/cI/oX+L/9ntAf8v9rP+f8pf9T/1v7U/8f5Gf2C/+H5//IB/4fUA/73FC/3P6APlj4//pvya86fxr6f/I/3j9t/8T/6vfH/2PItE++Xfjr9T+Wn5T/On/F/ML0n/Lv43/gfmp8Bf43/NP8l+WP+G/bj68vpvFI3P/cehH7o/Yf87/kf3b/xXp+68fi3/ee4F+sX+i/Nn/F9CRQH/m392/7f+E/K76Z/7b/u/7L8rfdt+c/53/y/5z4Df5f/X/+N/ev89/7f9L///rI9mHoxftk5550E98ZoES+9v+pVFKdd+8NH11irPDWwmgkGssMaNXVd/RYTYBTFnH4u0zD8ZP3pyl//8vhQhwiV///WuR2vvEQZuZzH96NvLjohA8diVY4NfS8RNmngRaQjLDSIBxa3kQdODa0bOekQR+oK//5I8mGQr3qDgw4Q0EbPs3Lm6ira8o5u1G9QaP+i3ctO+oglrboBaHkzAxkHSXgrq2qaacF+0Y7QxMws5ZkDeRDb9BWQSBW+DahzHRNd8cvbq0A5kZIUlWDcPh/qPH2XCIYkkfZMg288nOcSne20dXRsNnVex3d6pGL4WElZRJg3UFCxQUw8jbxHp3rWXhJ/BjWwlyEjAGqsOCAOLRESkMf6k5TyZJkrSgyPBkIfZkKkitXScr0SIwW5jd7xapHqV7Msz24vpOEiFrqzEv1q6i/tGuVOKcFfo9mdirsjHs4H0JEgvzdM8iu2Iesbsc2NjtvZUYMqKFhfqu4VD+0wJ6v24CaT9ydioTKYjsA8DSTn1V4NviQf6R3aJH3LSmYc7KnNO+gy33L1C2sID+cZuDhKqqZ9Pv0EuXSc17W0KdRvQmEkP28fyupMaZrfEEEP146zLfbsKDeNXAL1/AmlIh9pEzUWEfpGBDgH679hUSb+SrhVaHuabxBjhLB6JOS2P8C+ojemlt+9Ga5mwW5N0Nu8UA4Hvr51rCrqxtk7TTp1W7w6QoIh6UPhGqvxsZ6mA78NG77TapgFIVTRuoCc3upuGwZyjOeK+cYwX3Q1WaN22GVyVTBqeeed4dBNopcjA3/SbdhDuTSx5fO9Mo9WK0sHZizvYzoD8wqFpnkh7JOBpevuzpNdTbCJ7whgLvsfyuzsrXjg2W9WrnIxioTAxWx8fM4CQdvTg32bBNAIqo1ZRsdir0DXSDOWlUmUPmrDFm/cM8JccliHJOr+0tQTXhykZ5LbFBm8HrUEGHDj6G9U+NEJ7occ+r2xEoF6WoDv6tD7cvrELxsE9vsixgiVXTcf5Db/WFyebXh+YRg8tjN3n4tKT34FHCsvzmBU+63Q2zqpgiHC8G+EdTMApO6FZ5YG7tvlpfOtEulcb3BKQXaZ7oiFWXZrzHWVQONKa4j0g/n1Yyp3mEbTcugb9h8tjLZgrNYocogGrUoN9RHlzOmZj31oVPYLeWyaC2PfDmIHvB5tHdGN8Uc0bc52fn97GBCyx4bc9cOuS8UWXCVrtZZeEoH6b96SoIiPdk6yZZoTsVCKIS+imtAKmiPYNyENF2u+2ynXZFI6GdrzftTEUO28hFatRyCuKpfErLILYABgTEYtcuKmY0mMGWzVXzN4xlN99dg73D74DOE/OrOZfT+BpoaWuc1soW0iVoVYToKVZnS7kJbigQrpPNvjST//Kp8S7CwGow9vlmCXu2lDKbZIR8DzWxHCe4Ek6FmyjNRrnRP0rjyJxwYippfqtQ3Zj/vyI0m90ousbYXGOlpRuHnDmiuMoR2QtuqgjO+k/XORQh77ZRNohTety/UTJNeLTHh9cG58OAAP72kaCv/lImPNwfSrELp4yQW8b5kGYOdEszzuvu+fsgqHFbok7nEP+5AFuYk1LcwnUAf/O2MWRX/sfWFUbt9Bi+USpALpQrxvTiUYZHhoaMeot7JjLnZjfSa42mkUtUj77PCbUL7L3MNBCG7ecOYRXkfkpsCXhtLaHx+HCG+ZoGHSXcRP2bXKh2ecDHRx+20YglEG/Ap8Gb0iGlZnszG23G03KamN8iG12dOeTJ8Ao5dBuW60n0bfTEKVcpRnB1pO5VtLtkuiO57xFyXoKt+XYi0GSo3WszT7r4gywHKhh7d6rxolNG+WMuMPU1CKKeB/MNK7T9U/jam3shf5Kf5ZMvNLex8wuyNYDhMqkulMvGe2vDnSl/OYgQFtQoIoKEYa4GzZMp4CVCB8BQncpIp7GFfWvFTla99jjectRK7Ocyh3N1p3ZYB+3zLzqMpsGOQR22Nh1dxcQccslREvGepuJlPYQQPyTu628PjITfoUWtrTfGcegTNMN7DRVAX/LoG3ITWrJHZKf5CC1KFPTc83+zCISs9MJYypGNW+q3SdjBYCJJwuPwZTHO7nYyIqnF0Cb1GlgotoRomPebe8xQJPvvl9etn7SEWNjwIRxhhEhJ+DLLdkl+4MvG3//+Dq//hGL//wdyFkNpXnjqac6k/6uADkqFVAp/eZAABklwXQmTfdPAUtvV0i4+flfRNRkd+/zX6SX1Xe/1MNfR4e6ZSRmlx3InMx4JedbmZ0pVPEIJ5ii2Xcd971a1GXPvDahfBQgaZ34YguArHGDLgKKcyF4byc7ezQ3mjW1WmaZanA0uR5/VD/PrLi/U2j+aWAO5SBsO6EhCka3ipmpPIKMk5Ic3n6nY41cnLmb846xde26J4xBGXUGV+h/5jM+h9/Wtl8Qo15XHSHct+KU7OIo5D7KqVY72f4TvR4Hc5v298EPKBdDb2Zeqp3/dK9jYQ6HlycIWonbiAzTBSPuglUkw/kXVgPBbdsQXSv1ee48lNGKlMdwxYfROT8GvSixGeZne9n7AZRKmDYI4v/NzIQp51K6zPABr82hLYREaPqPn5KngODDz9R0GCuuATRBPT9nVhNhAnsU/U3igwGz2ZueDkv8hnY9BLwAX8xLkQ7zeWUhmph7JCGTb0rdIiHzrCJxZwAO90PGwRjprW5yoBBoLeLIUBfNvAJwUVF8TY1rQT4+rpB1omIkrFLUjt0vge1j6Scr7F08jeZ7Hn0yarSoXT1L3BGBDsDRQoDbpuGwoEQOzEMCLtc4wU++vIDF7Op037619weiYmBSqxG+gbvrEOu/64KeN5DUwtKqB6dP4LsxuPZEQw3fSkbJ+j0WQ458cWMGrLMS1ocJLM+HpL3w1JMixFNIVnavN+A9r50NM4CbiYUn58/QeOWIO473nQnxaqqr2H6dQbAJ2PmjXuBpbLrPe7ne77raJ8E/51cJKADRHmu9bcW47qcPiagn+td+w4OtdglwiEJWrr7sLKAQfH0dB/B7zDL9IMTWK6APwBMLazVhTu9IM7DzcV2gW1gnNQesAdnjHyUARs4drgfPICY52CqJqkwBg+VTrogiyLnLtaqdVAFB6JUtQxup/UJbHNsEd3aN2ulnkeazpm0tDirBGh2Pb2Uf533mriDRADZVY4AStUJmPFhEkNYEC1XZKCWjOFlRiMSkmDkUib7cNCQEwsu2LRZc4+ct1rgupiQWFvl9L/KGrzFAjVyMJRom6XDl2qBcy83AHOvx2kRVvKefpd/nfrPN9dSZHyHvib5x9I7vvc68wv3Hc7nx5wXh/i2bm6sEK3whvThATVAaG/dw+OfJ5L+JPnebNQL0+x60oCV2LZu3ZJiGId1YxYHH/zuzU8nJUjdaQk6XGbWljrrH61NXPZ8w1a+0lSWfibqeAPBgnZAw9VdlbyfPF6k6uZC+2yp1IAgPffiUyKqRmc02OOeN3bX6wpQjhFGWiU+GYMFnk/30jwiZ8nKF9E6fBDMzX+wy1140rVvsbjQRSIPljIl30a7GmE+AQHTicr4ztcP19/1lFcgrn80k5kZpO6dsF9VJ5J42Wwg8B1UUey3+C7ZBBnynk+b7yTXRz3l9K4OGshP90T5eI3MrAeRLJ6AZ87Ql54agpK4k5bBtJrUGVhfehxzDWNXDNapIRJTjCT5h/e1CdjPhOb+yx8Vd+7wIsmzh2H2ISXrG+9++CvuLPMnFSTcNdp3/qgK6C0TFljsduNIzuVQsq5pl8OaWaEbEzxtgPSxTvT4WL0/h8wcJ3jgVUqSzB+TSsXkWPGYI/AXdXL+2PfZIeU3PR3qsa+fdlIoXRdURMRui6NZflUgQ2aq1RSieet6abHR9R6XtdJGjmunrNRbTd4VhglE6hX9X0ow0Rw0QhpB4X4A/4bfjaj5i/zCN4A8+LQyuMmIwjLe599dhYANh/NkD0jSl14W1KEu6s6GwpWYwx83hHkTiN9gscKObKqiVdw9fVgdYI+xrvGxGJiG/vBPiFH01QP52l6weE0RR9vNWtTiIT5ltyCn52NGed9QE3tIQjRqGcu7MwdIC3WCUflSf71r3lbKO+MvYJd0zxddgKyf+K8bUJnYnyPHLkyefIO+RjRzWqebUq2txwnkWODcXW1sr3sMLCeJBoF48+hnp/z2+BJuxSrrKBaWsyIuLjRHMg+U/4g5Ih2DV1a2wTFJ2OGxULklnlNZuyF/1UBkn/cXPU8qaDmc30+WOzafoKO2izep1o//6aeIVYhkeRo0sB1vl3HGhMCHRRtApuCbybzLi7r98hmy6JsBg2I/KCGFj2bdfYdKgllVn/SmHnT0wzWEB2bT65IJUlifDPnIAjGo65kT1EuD8IF6gORl5Md8rAJZgd+mtQ2Nl4HFXJhLxmcDYttJE3Bm7P78ydatZeqpmM/HaQI9Aqud4tjxrls1dDyUP2gVKTKKf3YVPVTkXUMUWoOJE3ZsuY0S61POvxSuVl7gA++Yu8lKntPf0HbwHrTWCTydQct7wJMPHH6sSP2R9k5fQW0g1SNl5nwkExi2UbgOtXvWKq98SXJnjKaRputYC4Kd44lR/yxAhCdpYUmwK2zBdWEhdBp2SuvncBWniKNNa0kq3kcXT8BwGkXGVQ9MDykImv3Ew/1VDMkvgGdnWOtSEZ4TcPWGlCGkhYKEgvO/qsIxbXPopwWDiG0WTacVBa0mm5bx8NYQ1uXzvHRlfvDw1Lm89lgn4udaeZZE5lPRDc3IHl5Cgjlylv1l3jMYAnu4UUckoOKf7PVmaSY/HJ9AA2Um8ZwknusdxDlxdi0Qyv72C4qPB1nNcSd5xiNaXqExIHr6cUPRwEq4tZSZB7yLhK3JGGlFbeAOTDTXAE/G22D/aWR2j+1aSp9exR9Nzt/roAHlgdlTmn9yL1/y1nOj9hj2t1g9WnLX6OmMSCvyRnSpzJL99NsyJYHx9Ro6JMqGFKaLQFeAhloagTu/MIXOsAQ7iXsuv7o0NR3jHlJfS7upCHIYF1eF+QV+IqJg56cqaI0JE2pJnsFe/PkxQlYBnbaoJQjF4K3HQ1Xnm8nmQEr8EvHvEMlX9so8ZJq23cWZZUerry0wotvlNWNlPUn0Dd3VV0BONxFNCP7nup2hLmUCdwxNynzgViHs+9d1lY43ip1RpKVECyNb0oMLH8gkgUUJ1C5i2V284qUn+QuniuTV0ALgROawOtKFnQluxkOOajFyKzNmlbL94Zy48IXmfzJeTVSyEIxpt/dtYOb3E8MHIx1PHJ97/dZrNLLWKatb//9W/7AyQuV5VuAMDNaB5PmCJ87ptdcnCpWIIgfVGm8j7vwaaRERjoEG2JmdtSxQlpKufn8iGmCqhIgLEwokJ7WpOOOOsX9J41Vjdc5GSjzY2nT+OU6bQnpkgOTnbIgKfvw6say6WWEqTKRMKdLWo6qvq8q1gZ4+IHSc/x8zlJFqtqIbgkjw78KXQXwhPpCHvBlhJoXZz+0XpyCanteAG/ihUXRAio9Fry//CDFTcK7h74ZvMWIFmVVSBn4GZl7wZdpMz/cBrdGIh/+oEQ1ryWAZldE/yDd8HMk7KdbujkAXO8c4WMUZSS7XSO2r8d0ilGb5rzKQdXuwD1KJihiFCA8n6FtFq9vteowIR9pubs23Oo3n613wm1Cyqo5fZIaT8zUwRsbtQb7QnqqmL7gc+E8FLHsEV2xXZ9XKCoznUGv3FakxDuA4ozE9S+gnx/PSJXD1A3exmzFQCcCVFhfw3EzSZPUHIzzzDjx0bGoNTgzKn6HTSdBQjUXBv/pVGYSE8jby7sYeonrFTgQgVbY9hGt//tLFf4kHlgukp3AfBH49NXAz01n57Efq5cx91cnSEhEEZ+c/6gMlWwcx3QYLWYOzX14CMfEZD7Xafr4mgOhnucCbnqtafvdduVTqxMc+M45YvHq2rR5eT2xv8H0M9xJgp2DtjTbAsMpAP50+oZFK31bEZ5OVOMDkQjkHjKJomUy2xvH40m4NohhS1oUzf+a9Fyz6i29BwPl6rop3EyxlcHBt4Kyi3JzI7OqDlpLPnBLyhRib7M6/50GhQB6xvsVsgzxldYU4ejV0uzpdtFmoXw2PrQC5qeX7zf/uP07OH+Mrh2V9BfAvihtqPG+TX7JLl349YzsYgSHeJW4rmIhhDLvu1lmODMLHqLQyetRjA0t3gn3YZv4ygt9SV5fmu3pDv80RDoobBIRiHUu+XtkWtiSPrRffTxFFvLDVfBSWJ68jSMjziI9yiswlCF1XEieLJn5Mgg63giZaHpe3L46TmDH/iMqgRjaZgAW36LeHKzM3m+Zn7cHo4MIX6hNY0P8BN9A5bCuRoOiQ0YBWQ2799CyPziRqMZHUMzZNQfA2qVFZdZLwPJNQc2sWOlpzZarJCkE6cz8j+RgX8qjAEznfPkGAMheNmDvNN74tVB1OgcwEazYN3jNLeHWWcLGtKwwFCMdWIjtbZXlgcCeoKVNDQv1eYxfvSFlNuUgO+xjU8ZOfkvAgUHjMd3kK2iAhdmhxijHqzHGtJrCQJXm0T9GZ2k4oaQLUvRw2qB/+S1J9fh49TZZlBzpnT/dxkhpo3lvY7pDgDgEq0myynl5RdDWMEjYIFs/9jidk0AlGl1iU8SHDraBI9tCO8W9tx3Bmx3nI9OZtWmXapuR6X78k33M5hg1r3JMHVEx06I2eSFM9XD0+F4MroZy6206eOwj5TraXHuQPXrQGdktpPqj0a57Bx5RCYSytgMzi5SJ6RO0Z9wJVVXrquYac1Z/Rz+6aUKPwToH58E6420WOp/UsXXedaPGLaQndOpCcSDX19iFDTFLMsPInzLfIElYdkXs5dGGYpJVIgvFBfhJZV8UAuMIVqXAvtnnH+DWMrNT5MgsuwiaD6H/Bda65IGo/prGUc5zQDDvDAbL5mlsRmxYPtNbjFVYAgZYPk+HYIKZdh5xhqnQpUxubY18hlCbz6BGzn+pv1u8JN+q7yKrXc/ThZfJ3ybJCIeu13KoFScLXIeomv2Kzstbg1W41arcQIkJq6DhNJH87a9fyh8SflkiTx/Nfd7CHhYYMHq4NbCpTaqkeUsF936mJWjl/lfK1WDC0f5SzYAyOGXOED9K4kS70pqqp9iIxVyqzQh9U7KQUqrP4Oo9KLiZq5gf9YgrTQydKeZ4k+HpL1KMkg9kXYJ2nNHkXrXojFYO9qbxHCuIiUu9L3ndlcYdVcJrDYbxg0XB+4qaZreixkVJAqtAJUl5B9Mxpms+oJqOsXoLYTLz6RAMSjOPwIj6GgPO0g0TlHkQ26i13MlogCFg5gnw71JL3WiklqA0zJwqk4bGF+bLtryoEW15L0PmRlOCaGSAQngJ539SetzCIL4f6NFmeV+CtbAJRhalkndZwR5WtWfsO71ffsfKvOKdLrQsqZ2ayongimNhbqtH19ZohRnVJ7BlHqlsM4NvExRI3u1v1wiSyNvbV/uskY84dzUdZJHClmN0ur7ZavI47REU1IlYvTbqSQ+96bZ+LSDI4wi8ldXX0E4zRxZBsHNOqgLOfjVN0k2tUn0+BgJ3ouviOakRoPD9kF6nkqJaDa6+r/JL2giaMbuVqJJC5PtO4zLL3P4KvuzpMw02ggoG6BUFUd+v+zQtgXQKY5FcF2wEM4cYvV2JBF66Ut3Swg5akirU6lkMtquKIYIYHFQd2PJLX7j6KAfFSBAyY9Nl4jleM/Pp2vh10ddFi+oDoY3+Gvhwfp7f4Cfpj7qUnaPcqPL5x98B1LyCLeMHA+eCgbzdD6aM28AspOzYXBitV47s1fuqRLKfqmdXuzxVqAVR8jW8uI5K4jRErdkAQ8B968runsHmvOqKgxCoW3VI2KlTEYj2Mt5DTZdqWkydW+feHCbQl7oHB/4MiY1nBiB5WZE6Q1mWEpEpX3dcw/hfB7fFc0ukjrO0AUj/n+XYJTk4HSvdxw4vmHM+w7kjrHNaeZUVBEMbABlGdroC96I5DTWXhmX8RY780Uph6WHnjBV0n+vlUswPsfvXho7A+rf2a7tRjmauBBqFYzmnGJvigGmfeXK/6q/H/ZYfT4tDx4M0wsx3/1eN8FQTnHUMdBFgFUY5alTOooZVyFoseWi91ukbfMQaADuYQhOqHJ3/OsdYMW+SxHSQ/1r5dQvp0q4gUqLj0HRB9CnydfCLUaitZH2pef4dnGDBlU7zPoIUaz7zcUbvvp/5kRUrgyGVhZNqxQJLGDtaQ3bWUMiYbRlStO7Gx1cRdx7JVAGqvJ1dc5FJ+mpohqv9J/JnM5yXeRaRBM/9is+GDD4yGzYLWMxIoejBswypREbM+5AwuZhw+XTsmpuEoUgNQfcD4xYNFpkNSLQI+BaP4GUWhLjHVSNHABKJXIM1t0+eHNjSly2H5cj9FEpUB/MiSKK4RDqdJ79bmGFWBVlzfZ78Af4z4X8R92r2RVEuD4niSYL8dxvSWGka+WI9zb9bHuuC7ZYu29xJbumjsNBvONj7xOTZ5fHqpsC/89rgAQIqJRPQ59yQgk+mqHI+gI5eNe97J3Hh+q1TrPR8Z5DZbaMJhe2axTWx1z5sV4+mFNkIZjqMBLfXQ5wyCobDhFNoiqtfwNfBi+wucurOUa64/nY943vOwfd7Sc1R4VF+29o1/jKo2EfISl2hj6mU77OtPpJPNtMqXXfDtCo8+reLPUD/h8CXeQx9udqa5rsrwAsu2RDFCP98GcYyaXebI9gqqup+VcWjQDHQERkZnCTgm8bK3EFYdkWVEWzWmCdYhjKUsUNQdLhvepuolQUuPwvuY8cukhYKxIlzg2Jh1XcKr7K944skMkywNvn7K5sjeY2/Vog+sjmUdKXSCtIe2KujvTNoGCqL9Dr8Wyidie9O4XY9A201QTd397D7MQyoRMtav6J0e5kX7qKkF5qC8UMlt2C4ypAuVYTekU+hCzzKSU+zcXX5Z7VzZBYtC7N03JkiZ83PffTpZSWqMgciVvI8hynq7RLFK9SJ10N/U3GMdaLmNSvjkAgOfAAVoPEaVrjuARms3uWrjQkCnkc1cFHSE+SuSP56y5BLzsQnkpGQhKwKvR90cJ6LB7sTgDRN2NMaymWmTEumcE34eOVf7jqWtlVmndaxZgJk+cP//5dbzNTIkpB9TdhNvmicuVXzMPSkpa3ROykJQ9PHBX1JE1TLEPnKVEX4OChGAd5H45QnTPEv4cOGPa8pAzPvBKU1EnGW2xQ6bJU5b4LAj8lQhB18AirylxjmP3JmdzijtYwj55TtlltazBjIaoaL5gVbC2QCfO+wqCOgSBC7nHuyV/v9NNs/8Dd4X+Kgm5cYsJikqK713OUS+7BfGM+BvdW/gOxs+8ljtex4q71vytP0MpA5E20zIw4Dyk/DbmwN+oxHpPvzPWwFBUikxvTAdkssKfZ6JwRbsdyTDXj01arZweuXvRCcNB1T6zhJtsTGJsDMucsKmuhzWKu2Wok2V/KvF5ktrnFQOAfhdgcOMlKflCj0TqT+5+YcrtjglMZtax9+fLzPgGNMr6LkXalkJB5kAVGkV7vzi5o6etECCdR5Lk//oPBOZd+HQxYDkteJKjmx0yZfv0VBrxWwpE6XX9VmNzOBzXwsyrco98fy9+0Ui67Nht7Af8i35P2Fyuj2eCOXxPrDhRsSQuYoO08BUk55y2MeDIQ07FjH/0nA9unY+CIUkuB4v7iKn49R5r4HBulsbKTh2MjXoZG8iwRUOqVEtyTHpt1XlAvVGjqPHA93v6Fzp0WmV0gZ9IptXnlVroM6k/JfAA4GnFBJqow+HGozWuXYii3jJexOLMWiTL93tFqI/meFmMQG2gQE6O3G4NNw9qqN0Oun8gJKYZT3VWpohiRBemJWaS5DHb3MCDxxaX1Zivb6JGALGwGEVg5yP0S0BWrDDGZ3mMd4s7ddapfsk9zm5HnrlQQ7Gf7QbgSuZftY55I1oXl+Hv3Ns6AM/W4b2hJH1t3RFWtI5f0kYVRTfN+X4/UiewnfFfxeanZfGl+ZhUXCrp4PFkTyt0w7NyMQTW7BRaoIl7VjVddzjO7/MuxbpznGbtIGn0FgnSLeUxDG3S7ZLyjM0UnDvnkbG8tWtJhjBCuJLcFYUTpipdREoNMWKis8IO2ZCtRa4DTKUmgfESVQSH6fHhC1jeGnmOHL5zcCXZKl6bvc+HIvo7x/aKR8UOGPWTjRSgjsFCDsixHxIyRVTlbr4kckNl+e2Qka1ljv6/Y1o/k+6Na1rntplu6EEExoq/i59XWN3Gj9Gwk611BqfrfGqwPdzpTDMo7pPYqnoMEPQlCEKPvwRCjJtlbuSUmT3CqLmnw2il+TEFmHC9xIJpfaHs0GEg4qk9QkrystCthY3UFqdQ11OajVrWM07fYdL9oZQruT7+Ec7bq+z0TgFt03F1ZUJMzITULLA4REAuw12v4xHUAFaco2E+n9lbzwxNVzS3XoJn6UTrN/pd+b8dr4+UakOzt+ixWvmDBZfbwN5ZhBE2FSzZFVvHMVoaGUI9ps0ImGKK6YuvXdGfWbzNC4/rFA7WeZQbnyvh/tI17yjtRXz/PjflQTygWxeKn0xP/WvX/Id1Ijr/g7nZMj+m7vKKCFzwU7j6hnCgxZP43NleLcdLi42WtucjGN/LEF9WWmYgUeGqAn0ob3/SDXOBSU7ph/qs/GzeYr9w68YxYl4Gcs6f/b8DXCzMQ8mOcmz1/8+/q/eOtI4gcAM8KSpB/Hy5vcX/4YVh2siqA7WGta1RmQVfRiE/lwoSCafsYsdDpKbO/efZG5+EwukwBwE2K9DY2V3Wjy4Rv7O5aDhr06bJ1ome7fQHyGKzqE9/cIO25OSjpfJQamMnYhZnaWCI83ojXI11CglGY1Oz4ZDURqhilWN2zlfw7tFmG8Rmp1qVQNOMpt9pWgGLhdr1aauYuSnoCzb2hgo46QGWQFDdjFivULqMou6lIY/TcfxmqpAmtXeciEzq8zoBWEJyHHiJxJHkdB8hz2cWrKbp8saj3BUG2CiNzFF+xTj0LUyfvlu/FFHMFz08qwUNA25UXxm460whS5wkkHDcLa0iAwV9wGZOiXV9zp/CfzOJV2LYbQpF+m2Rz25diTwtYI/D8jqgF5Ey3agJPWpAN9qzG6lLvYqo1F7WN1z6ZOSi/twrtoEJp4qw99jwhSkFPDkQmyjJ+0i98ANjU0riJN6EtgAzsdaJ+BgCbPxIxVMoQymlUhsGrFXtP5E/lWOXx7pbNfdxysQ5HM0/4WTgoZUAhANiTSgbbzdDoqFsttbmD6GeDGF0sEEQTmlq+SK8TsDV8SoKge9rn576j2p1p87P13MyPu36alkbp/qzYA+jLmncz7YzI9ANkThVURWWYmZg2lYHjP2R+tMKLTw+g2SSkeJFBEkx1RbP3HsEuZ39vihs/c6TwpeqpBraW3bhFOszhCGHEWy5a2XRfqmeSt8FK76Xo/5xZSyJIAJBiMLkOuj9KxNLmNJ7NZUMPqp9jMQKzj+uCwVwtHSbtHpba4mKUZDvzqgruipDEnLBUKx9bA7JWE5GiZDOtjtdsbAtDHDspemWKDm/uGu/QiO7IYbE+lQlgld1oWrM8RlDaGFVJ3qUtUcSr5XQy8PzL7Mr1sz5AOd9GK7tUoZYrZCYEqHqxXa6PA7uFx8AhgdZDY6SNj5zH4UD7/e9Us8I9UYd9IT2WZ2vi1ZcI6GS99GYBxwUdhvOS9V8jDRVnkv0bPoPcAkR0GRirW2/9/XEuo31Yq3hc5CdMPpYgW3UajtEfdiTbcqNOI2qMPso6gfy4YVV+IgUhN+zdnR+9TmumpeGWcrwIt4UZuKAO5MhAlkBZ8nh1u8ksbMFhYhArDgbyF8dF86kPhCHkwLcvOD28yzjgAVw/dxfnnOKHPlWyaZSmIorZSYjHvojmh6DuKTdPIDbmdLuqCtKcCm0IlXXwnpVKFiSVbpSYnuUSEowLG3EcjtMK989BL8ByLaxBOmKesC6aDkeVWe+R574a4NOBQ26EjV0mJ72SbmYH5CKtSwTHalKKruPzDaIzE7K4qPVS5hbyRY2ijZ6ppp/PQs4tMkz+rQUEnQrZEFy8ywlB7ooOCM8ZzvE5B0N0RLQrPXEy4pQEE9+/M31WVw+LZOrTS1UNSRwO1U0c2W5P/4OFzF7vzGD9cWJGu4JWalRk9PkfAcFTt8IVk+uFusYhb5vJd/+NKxYdGRhZ0k28PCWQ7TWdlHyQEMM6shmD28RmFOKXRpwi7S8fT4InLX5NlrQvb+iAN1hgospUV55Qu8sk4aUGrVcKrt+V5zUg8Uehk8q6DRhd3K9WQIETzwDemgb7lagICc299szDWQNz/nhizCIuBjfGZ76d5zj2LbFMK7Y+M3ih7ldH4Uy59YaT/5Su4f/wN9nfqqQFfzwK5NxQnqVSRWmV33wwQ7VeBoUknz/rLXh3VYPdPAYX4odQgp3Rk1GNQ5qsbW1lN1+MUX1wAsyDsdHYEy5+IWt2AhSty/2DeQgGqX7tMGkZ+G6LW6mHxb/wXLF7+E5VZ+29/EBwagcrf7m81gKsrgiqY9F06ayRESzZg/V0Xta4E0WOlv7niHNvrpAqDMsgjSU4mzqULaImALIc0s9lgIgY1rZiOCAHtZRoC41cTFuQb/Px70xExEU47ios3lqEdaPiSGosp+8/IeYqK2JL26iKDHpT20XcpvQYpmwdJxSx3pciXnKkmzrlSj8jevAiYshECs+k1rSlP59cLVps4AsF15h253ihGX36hjQA3gwZ0ChcoweVRkBc30HTH2GaAAaYYxo8IVGg18L92s+ld0IibyoWwk4EKf/GwftNxCzVQQf8oPbSc/N5dug96tEtS/Qwkpa77u9hbNYo+mD3dd+wgc5xMokvZjPEuRTrtRRcNZPoqLj5f10NpASp2Ym6hcFBW++MIksRPorHY9677d4kkXZCEDQqeipQNHRYh5jVBZCXvv8Rn2SnfPZEeJwJYlh/tUATPG3iyNZrLGU9LZuLPjvoyHY3CrL4g474HHg2QGEHTPBhFw9vZoQIE9+L0EZKaIFz9plITzXqfux738NTxhbJt6k5QCcCaKvnlbkCaFE3/zxZd60xymOpnFhbPsVPW2p0EsR9Bo3n4piWt9bd353zV+oxQCBe8MyB4pZpMdB2HG35r0IvPYQ7G8B9Nqa7gI6UGj5noB/PqvlinPi/Ncl83EvQaZ+WIfq5E30jzE0yS+1Kuo2OuLMKsE0lTE9at5lDevCcWb00NonyqL/mzidRmziQEjKQ7IPP1vrp55yMBGmFXfc0bi5wAHK30p2hgVVn+CLnWsS23RGNhCYqN6hsEwH/0UhVJjo9etx+mrHMFT1SySPaCjzvLttNgTI/edmXJzVT8VKGBxEW0yKYUtbzE7kBiVHAJiTxlnMCJJaHGjpa6Er4jQfXrN4fGZKtKVWHJxcETa7CT9P9c8L61lRgp8xFvkQiAi7QeppCp/MJ5RLjxWCUGFwXtlydwCQWOmoebZ9yGpwjYcK8EIhHgnmf38LLlsZQ/mlsoS/6W8VvQmlBotzs78hwHN4qkCEBbHzEFQp9riemo0QAAkQnwur19WHztvlv9kU6vz9WGtqrUz20AtQdbTT1yr+gGV3BurXuNYbXLKmHcgeEGyQVFW2R7CyiIFoZFs2630Lm83X1F7LHWr8pSMqXzoOcKOQIy+dhvkYHkqdj7nHqLl56X+WYWbeROu8VyQg9+Wp8q+tJtVdVYGhFGkX52tJqjzZTgkQKcXIlQwyoTV0O0mmWLDcVfsq3Go3m8VTlR2an+VBdgfdIhYJE2TpaemV/A0jNBzTluXYUhxRqLFLGoE3gQMnRX/x2VyELfZ8HC9AHeKuM41mBh1Kdi6aFEuZSo6XgwTdkn+g7WaiLx71Yxgfs/wKdewYdo7wcHkWnn2CUiQNX/wk3K+kN0m+3J97uOMhGi7bW2OpGDR7SrIJCVEbp7oy2M5hvmF+UqYeD2MzI4n4oZmR+rMchhYpzFyCgbNri6T1j1RPvIVeHpqfht8khHqvqa0F5WAncOg3HxW8D94JkF7QUsXCRK55Vr0ff0UJdLkse5l3f9EoWUQw8EU18eMN3+S42WGdQtC+c+WRYIfQu6gP6pongTVWSoXCYy5UDXJEydjjiSlG7FeTTOET2XxTCbXhiGBVaWr/1VTvbYaT+DWIygg6xxna/FUjVIy6UUHzlA17InymLQjghnqU8fb/vkg7cAkCohU9WQbRBm6QHS6y7ccn5g7sBA+Sgoo/9iLTF1PsHO27vhuwMwZ3hTszl/LIrdnoXaQMCmmQiNf0icyJOqMAKOF6gl20ow/m5oJOjsq6VhP59Us6wxHIbssn/K6AoW/6/qns1kp4WtcDGN/tIkfZxD1lg3nLybx7WYOzIWMMAqP1l7LD5tGBupMQSuzAKScmPfjrDAQLA+ceOXQX1tZO1ovj9gciXTdqSXffdCGRHDBJAepzol2J5+/Izdh5ggLIUPS5I3T8YEIwcUP7nvZ6jTLi11v6eoIXI67g79Y6/M+qrXec3fuz328NBS7i8p3f/19ZReDa/+w1JqVB/BklnBNch2HKf/D89+ZrYctv7xnDySWOcZjogXxeHD/emEX1VefUtqXBKhHbgH294HK4ebGVe9HncxBfD+g7XGTjHwM6KVNEmWt36ncQylu3YJ4hq/7SGPbPG2zNJDOBExAWhnTmNOWdzK7e8qrHKP9cW0SC67J7Po6ze0IBhdZrBv85oVpnwS8NHwlm9AxdeIrbPlJp+s9oMPRbpZT3NHad6aVjzUDlLujYXCtNisgcaUK56fBvb9mm+EoH28KYF7A0Is6bIyqnaFqP/O77cpr3oa3mqAlc3Rpw9JQbtynF0kaIkjiHrtudbdJQUE5kydg7GpSHB3vn+ewmDIj5mAYG+foN2dx9GnNPbHbiOs+ZlYuKYqPVIuXk0Cg6BPtl6QDS9r+PAeCL1qY65+Z6JpOMDJfO4fIQJUhTghvJB4EO9ZHq6mAvoXb92HEK1jKZD6KMSLgkscfKkWcK38FJjSPIah1wvaa+YMjzmrHB28nvbAHDmTUp9s0x9p1TDWu9bPolN9Si4p/VE/sgtXWZGhuad3sJD4tgryu8sue0nYAtG+0VIs4TzDIluB+oLBhPdOONZj0MacCV8FDjS7dE7sPMNV/rZlVRywZCfPl9VMrw6gMiGO2eM6Nfft/d3bZdxjTs2GdrILZKFRneKQ/7OB7tpL2xGDJq3U6/oorvO+nQFYlQknSuweptfhKbB8kzkOOwLq9Ty1w/gKY/Q5PXai8otu2qLIy4ZHV3H/hxIxtOPu+Um/gydsBQUJiv4vBAcGqzAiR0dQexVjnkE1C9so2bZPc/OPIIMOLjxXnT1VdeIoCA5sHoTgTjUI4w6onSBcilRtN/hKbDFqdpC+SqllM0DMdjqfjVVd8b7jO3xspGG9nf+L8xcloQOjLiOlnbWZLx/FDPidkoqoib146f5uVV86Wi4Jc1fOKeiTAec3SDHQg0U2CfoRFmStUGTSmt6F8zGpiDWYvYysvPzfDONjBNC3yq0IDqldy3/r0iHiJtVbznLOtcFUY4g+mhuD8oX/ASm9Dl3vxsGfZnnxXY4mMTFrLOZqK95f0NfCB+e04eo3SyMK3FAQe5deKjWo2mcSPNgYGQ2z1G2wi8L/0sIJ+U532D2yayTI7xwmE7IrlHyYsr7eApijALxRgYiyOehiuY21ZUAoIuf/Z4f2A/TXVLqHijJ2QhN9JDIGoga5IsmsLdodkF2m4poAThXyQLOgZyc4Rv0cbDLBl5jkqNnZMrMJFE94s11bPchE35REbUeU1QbD5n9Wa2XgWQettSLJrBwIuBvRbe2EFU0ajIGBDc0yMOOxE/bN4sXm5nYgGn71tabf47IgHs7SNm/Uwq0pPsxKdSzrvw6OxuzC1YIPfQh8TM2KG1pzuopEZb6i5ulSf1BzfacbgeAeJEmyNoY7m7GTkMC80+6/cFcRu2FgogklUU589GX1BrNVSRyDcGbj9DcRmVJolHo8U72+I31N+NxoYmSkNFHj0Gyf7hzNIO9HRwIv3LS7J12qtoJnun9DGpJbiaCZoahR2zz8qrnain8eAmf0z9SmfdMkVrFkgijMSU1Su1EfxXNHQyOh1DLJU0Iv/dYhftCX8THbSPq1W1Jejg4IVaHrb4T50Sxj3WUmQt4RGAkBZYQq9a5QkIvoKWEIBR7Dh/Ku94d0qgHlUXP6INS0JL4M03o4TnuX3i3/bYbtUxVDTN/k0Dx2l0LHsdmr8b3q6WedeAKgrUMeeqPxsvsFXW1DQcUhy3PdzDGCDpnwznjAZJpDm4C4yaxkb/EwkzsyaJ67wL0//cLwq/GVzmwTdpZ/YiwF7ShSvtmylTHG+9HrD0DJNmtjV6UD5iASCu3gFWWJM+1lDElh6jjcCJSPkT3NqDjwYkXlzQKPx0tri92jIS6HGaiAKy9mQzAbU7Nbm79UA5fKkvN6tO1EKv42KztlV3/NWBKsaD+Rd+PACdp+9ygUW9Q7+D4a8KtBSVr0HqNPGWzJzY+iKM8eL9+mfEzvOp1hBFqVeAsMisE6hBeoAlbVgRiiMPj8n970MOAMrabhsxEgN4voZesIHmKCAbKst8Li3F7rm/0CrzQQee8bOamxsCQlF1qaia7weVyDfwpPAFpsGPxa2i7moy4nl5xEhaXskxXMNshqt+8o7aP89gdxMex1thTZe0MROdDlGQNuJloe3IjPwz4U2f8WLFhOEFcNlH+bWW1Qw4q8wNXcPAH4UF4XEuhl/ClzC0Icn7ir211sEwbz8d7jK7RaDQ9cNDllTMTVFrnKGrzEGsYaKsCPQq6bi3elqgIS2O46Hxnh5pFdFC1GtFszRR/yIowWgUIaJwFUIA60eRFKyMIiPcksJZBTYAxwkUd0oemztN6DnFCoTLXjf9yPK26xGCvZeKB43vr1yrAMFZJMXo1aqKgnbwign5T32Ojwh+YB0uM5FAUfzjHbctD0E5FniUiLbwdo4SS1bXKwJh9rDM78ft6a98PaynWIS3TSv5WtQJLxFXkzI0bdfz351EIWGmsgOrdMoSP5ytdZt9thE8bWhEGmLM1j7YgHJ0m0aji/otROUvfIOB1RzTpqPwcRb0AbhHQfYiEtBRsp43AQATnjuIT90u7ih6SgofvafXJWdJTkcjarixL9kXItxdrya90Peg/Kki0bTUH+mJrcPQLcIZ8VOYkzQQmwWUq9dLHJZw4ocBC86nqBXoyoBSTYpQ2wQM/8fi/Ss6bnXJBi4qeiU5fOV9vzWmm3WA1xyIQ6UhOP5EnI1u3H3UscIAQBFS3s1eurrKm1z5bJXCKPfZMSDEraeNfVOd22fW3+1tHFKDwA1ZECW95x6hrNJgBTCoq421jh89Fz0Al0SQBTBz2AN0pY935SAr1owSOLsgK4cKvoOWkGWgNriPjQxTMyY9IYTYohRSu15xscNipzX6todBJbDbtDTh7HGVy24iDlzZfzxydv9o9oGdY/39qEDuqDWaUVpqPHww8qTKXLxJDftEGdjGqoQEJUeIv66/RgW3a7GZ4ya9ZnMXPAkMqR7uoubMxKSAEEOmsrqSJW2kG1sLfYui62VqTptj2KSoWfLSvJIRwDXm/84eZry7vZ4TquLsuUtNfL7CTpWBZUu9OxSGQxkwOzUBoUzYJ2bXVIyT4Lk6z1j1UQRP51tr/zdZ4y80y5zR8UBraGOjSNlV3yZTie4dyL6AxeuiKkmC8SP8VIq206NIAcj59CrBaLIph9XWtKOF9ihZxFD85dW+LjTzJe4fvdpuabPu15d4mnoJsC8eXSGfT97OW2meHI1PYauVM9IT748Q/f3Fe7ZpGul1hqdUvAORcONiGSCRoAn+5goFn+p1VJGrBmDjPaReC7U5MH8sK+jgYqxiwc0QutpgOzGuJNNFEaMukv/n5BU1aoxRq+KGILdDHBVT70JKxdQJFFogm0RBieGmUak+TLsdfUQJP+9ZB7b5B9Ymd3JHeKtf9YfkILnIal4snSVI4PRTSTO8dyBbajh0HT0001Dk38ar1yOnULIe0HGmzv5IcW6JHoYDIhP3mnYWFwzRD/+zKr2Y+jmZ44roz1vQDdc8F17nwFubacrYv21iCpZIhEftYrsD+H1QoeNluzDmEkr4MGz1YXNLK6iXX1CZAUuILyntjqLHC1yiVP/fI1nF/XKoSdhMaFmHT2QvurpguT2aIPwZwKstpPp2Cro+fTyWfXIrtEudSoNLhzEW46whXYaOuW6Kf8OfD/NBgg7m/Iw++uk46vLoNs4woD6CvMFBWr+C4CavrWx11kZfCLvBIAAACFwFUcoVY5emb3NOMXc9pl0WgApkRpe/pZA0dVMLwh+s3AiG8CsEAvBCuIVt5/vw09kohmdB8+2SuMEWbuR194u+lNoe3bQglozNhvr/kyjwMfT2aLLDKABi1tG72uyQDTVGK7foGNp2gXC2miOkMwWr1J7lxbeWfB2RFNiYp2hVMoPuhpevVBBXIOkeaX9n0FV/tWR32qxEKmErDyMMzTgj6joqy+mzMds59F3mdLLx+zC0mv8aICA9wr3VKJ8anKKEO94I+Ax03YDifSXCV48ReJ9XlNKpCrhqxcInBnWkusDZoviNdnwZ/i32PIbv3LePmH++doFJSGY6TYmIQpCz8N3+feI/kj/hUUxsm9iZ/2RxsEwMxIRp75rC/bddnt+zc/8m1YeV/TzqSULT881QRb9um6rmEyMp1n8DxSDJvV+aYYy38kFc1bUjvqNJciza8sZa1TgeM85ZqpOO85dx2gQNtqlV3/lbPC3r3cTDDFPr+ELEe1U4C00CjWQDUeKrFLX7EydyFIWOvVV42XfqPSLhFjcETKKqNNuy3JluiPLRocL3uO0PSl+zzatkX36lKtj35megJLxtB9/tLdHlrF5UVtxFuJ8fnpwYah2vRdnM/M+HVONKiU66nNRuQMTdio816JbATCsK3BjelRPfMxbEsCRXalu0qs3HHL/VvMGmcKHJkJz5J++ew5VQSfltvAH9SbSacwZjTs/Mxh4FgpdY31RSJaqPcHY8yD8GROhzZ22l5XsESPlIK4VI8OxB2PmvMpRv32AGGtSYqoRX3X2CUdKBiqN4vq+5/LAMoAAIyuhVvydrpARpZu8WMM8xq2niIphn3VnKUhmzZRMgWk+oPjJsatdV3G0JzaUJKru8MFqvhkF9VzDwTWlN8YaLndG50PCpQXoWW60r+D3Wa0PQj6n29M5QVJVCOMvorejF+P7H8GWg0+P92NvU8u72vr21N73McO0eJ9Rs7msGBHkTqe/t8uk+XGqsua07NDiBscT6yk4QflNAPvxgzE8mWP2YJufNln/KFQPYMR42Zl6Bh9oT3ElkeBYfcHFDLJ8Ny6P1Gps7oJcS7Z8W/u0foHzDhrWqyz4EgFcZqflnrbQTteLX1PkdR0mR/0RsjWmfD1VpKPd+EI37Pv5dwgiXYsMMahIYp8r9DxcUAeLq46FTtyeg+NfSketiT8zgyr2CYyBDFjRjdJF26Tlje6mwivAFRr1w/9RUMNxMbT2zs0a+XLjqD3tv8ZWIzqbNpXomYo5IwnMmgiQNRhIV8GX1yL/acgzzE3DKGZgST78PmGRkcL82nNOoxuBAiQCPWY/e2fH+nArbAiQhWkZsglL/iPn5xmTOuPZZTwb7HT8DJznVSBSLY7rkkL+P/FOK3RxQ0B8hZOUmwORrbc0sLe+6qXShb2XvD4m1PAdhiz0Byht51QfCAJO8OKM9zicZsU2gTNFcnffjV5uB+01G1HICbEOtaT5O4Q82o7JWqsbJo6AZ0jxr45pSJxRL9wJy8drJ18L6Yv24j6IaCGjY8HYu8cc/pzqgcJaQ2KQYD85CuFKMx+2DD3uM6biu74XVMp4Dylc2JLMxPafBD9iBQdiGm3WLv5NmWYRQzPA/JySiR6lJzMFpDLwtLS0Yhw7P9vGuyE0ODrtBmD5SkA6TMRVGA198Tma7h8ZEmbJ6A0XtdswwQJUnz4zfag6lNm97e7We5E5cC20Lq4g4R+j45y/cODOrNf//9vjaQwUAAAAO2PEV53MfLC7Hl5MtKWr3grRBbE5Ga2Cqa2NC4aUjkcUV4vDKanttAmqbQc1tD5RxU3qh+yyMMIhf/1nIq5EjhrPkvxRB62JbbuD8kfi1DpoxI1nrIY0Y1Xe2LNdWewsC+8FKm+KPmd5iwaUQuvW7CHjbNCmqn1mrTH2D8CR9O2D4AbgdvJ3+LSjxlFVay7qbq3b1A9NltJbvSwWSc1idvNM540IAGOt7DGJyM72iLTlucM14mQOjl3MrDhZy6XOWSKyHxY6MCSH6IoslRH8FJnVgYIaEaPNuhhfLyBJFP14pPkeEdgllwQ4jnqi6igHYEk5lq7JgxCAdA55mZSny2BR8WcMv5KQjJQD/w+5//+N4coVgZOcD0VG1gtkBDAAAAAA==" alt="Logo ForcedFR"><div><h1>ForcedFR</h1><small>Media intelligence</small></div></div><div><div class="nav-label">Espace de travail</div><nav class="tabs"><button class="tab active" data-tab="dashboard"><span class="nav-icon">⌂</span>Vue d’ensemble</button><button class="tab" data-tab="scan"><span class="nav-icon">▦</span>Bibliothèque</button><button class="tab" data-tab="history"><span class="nav-icon">◷</span>Historique</button><button class="tab" data-tab="pending"><span class="nav-icon">⚠</span>Fichiers à traiter</button><button class="tab" data-tab="settings"><span class="nav-icon">⚙</span>Paramètres</button></nav></div><div class="sidebar-bottom"><strong>Surveillance active</strong><p>ForcedFR surveille les téléchargements et vérifie les pistes de sous-titres.</p></div></aside><main class="main-content">
<section id="p-dashboard" class="panel active"><header class="app-header"><div><h1>Vue d’ensemble</h1><p class="sub">Suivez votre bibliothèque et l’activité de ForcedFR.</p></div><div class="header-actions"><button class="theme-toggle" id="themeToggle" type="button" title="Changer le thème"><span class="theme-icon" id="themeIcon">☼</span><span id="themeLabel">Mode clair</span></button><div class="version">v2.8.9</div></div></header>
<div class="services service-status-row">
<div class="service-card" id="service-forcedfr"><div class="service-icon"><img src="data:image/webp;base64,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" alt="ForcedFR"></div><div class="service-name">ForcedFR</div><span class="service-status-dot online" aria-label="État de ForcedFR"></span></div>
<div class="service-card" id="service-qbittorrent"><div class="service-icon"><img src="data:image/webp;base64,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" alt="qBittorrent"></div><div class="service-name">qBittorrent</div><span class="service-status-dot online" aria-label="État de qBittorrent"></span><span class="service-meta value" id="qb" aria-hidden="true"></span></div>
<div class="service-card" id="service-discord"><div class="service-icon"><img src="data:image/webp;base64,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" alt="Discord"></div><div class="service-name">Discord</div><span class="service-status-dot online" aria-label="État de Discord"></span><span class="service-meta value" id="discord" aria-hidden="true"></span></div>
<div class="service-card" id="service-radarr"><div class="service-icon"><img src="data:image/webp;base64,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" alt="Radarr"></div><div class="service-name">Radarr</div><span class="service-status-dot online" aria-label="État de Radarr"></span><span class="service-meta value" id="radarr" aria-hidden="true"></span></div>
<div class="service-card" id="service-sonarr"><div class="service-icon"><img src="data:image/webp;base64,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" alt="Sonarr"></div><div class="service-name">Sonarr</div><span class="service-status-dot online" aria-label="État de Sonarr"></span><span class="service-meta value" id="sonarr" aria-hidden="true"></span></div>
</div>

<div class="grid" id="dashCards"></div>
<div class="section-head"><div><h2>Les 5 dernières activités</h2><p class="sub">Les dernières actions enregistrées par ForcedFR.</p></div><button class="btn" onclick="switchTab('history')">Tout l’historique →</button></div><div class="card activity"><div id="recent"></div></div><div class="section-head" style="margin-top:24px"><div><h2>Les 5 derniers fichiers à traiter</h2><p class="sub">Films et séries sans Forced FR nécessitant une décision.</p></div><button class="btn" onclick="switchTab('pending')">Voir tous les fichiers →</button></div><div id="dashPending" class="media-grid"></div>
</section>

<section id="p-scan" class="panel">
<div class="card"><div class="toolbar"><button onclick="startScan('films','incremental')">⚡ Films incrémental</button><button onclick="startLibraryAI('films',false)">🤖 IA Films sans Forced FR</button><button onclick="startLibraryAI('films',true)">🔄 Réexaminer Films IA</button><button onclick="startScan('films','full')">🎬 Films complet</button><button onclick="startScan('series','incremental')">⚡ Séries incrémental</button><button onclick="startLibraryAI('series',false)">🤖 IA Séries sans Forced FR</button><button onclick="startLibraryAI('series',true)">🔄 Réexaminer Séries IA</button><button onclick="startScan('series','full')">📺 Séries complet</button><button onclick="startScan('all','incremental')">⚡ Toute la bibliothèque</button></div><div class="small" id="scanLabel">Aucun scan en cours.</div><div class="progress"><div id="bar"></div></div><div class="small" id="stats"></div></div>
<div class="subtabs"><button class="subtab active" data-kind="films">🎬 Films</button><button class="subtab" data-kind="series">📺 Séries</button></div>
<div id="k-films"><div class="section-head"><div><h2>Films</h2><p class="sub">Les films déjà analysés sont affichés immédiatement. Un scan incrémental est lancé à l'ouverture.</p></div></div><div class="filters"><select id="ff"><option value="all">Tous</option><option value="yes">Avec Forced FR</option><option value="no">Sans Forced FR</option><option value="error">Erreurs</option><option value="pending">🔴 À traiter</option><option value="validated">🟢 Absence normale</option><option value="waiting">🟠 En attente</option></select><input id="fs" placeholder="Rechercher un film…"></div><div id="films" class="media-grid"></div></div>
<div id="k-series" style="display:none"><div class="section-head"><div><h2>Séries</h2><p class="sub">Une carte par série. Clique sur une série pour afficher ses épisodes.</p></div></div><div class="filters"><select id="sf"><option value="all">Toutes</option><option value="yes">Avec Forced FR</option><option value="no">Sans Forced FR</option><option value="error">Erreurs</option><option value="pending">🔴 À traiter</option><option value="validated">🟢 Absence normale</option><option value="waiting">🟠 En attente</option></select><input id="ss" placeholder="Rechercher une série…"></div><div id="seriesGrid" class="media-grid"><div id="series" style="display:contents"></div></div><div id="seriesDetail" class="series-detail"></div></div>
</section>

<section id="p-pending" class="panel"><div class="section-head"><div><h2>Fichiers à traiter</h2><p class="sub">Films et séries sans Forced FR, en attente d’une décision.</p></div><div class="toolbar"><button onclick="startScan('all','incremental')">↻ Actualiser la bibliothèque</button></div></div><div class="filters"><input id="pendingSearch" placeholder="Rechercher un film ou une série…"><select id="pendingType"><option value="all">Films et séries</option><option value="Film">Films</option><option value="Série">Séries</option></select><label class="small">Afficher <select id="pendingLimit"><option>25</option><option>50</option><option>100</option></select></label></div><div id="pendingList" class="media-grid"></div><div class="section-head" style="margin-top:28px"><div><h2>Erreurs persistantes</h2><p class="sub">Les erreurs sont conservées ici pour diagnostic.</p></div></div><div class="tablewrap"><table><thead><tr><th>Dernière détection</th><th>Média</th><th>Erreur</th><th>Action</th></tr></thead><tbody id="errors"></tbody></table></div></section>

<section id="p-settings" class="panel">
<div class="settings-title"><h2>⚙ Paramètres ForcedFR</h2><p class="sub">Une seule page pour les connexions, l’intelligence artificielle et les règles Films / Séries.</p></div>
<div class="settings-grid">
<div>
<div class="settings-card"><h3>🤖 Intelligence artificielle</h3><p class="desc">Brave effectue la recherche Web. Groq analyse les sources et fournit la décision.</p>
<div class="setting-item"><div class="setting-copy"><strong>Activer l’analyse IA</strong><span>Si désactivée, ForcedFR utilise uniquement le comportement classique.</span></div><label class="switch"><input type="checkbox" id="setAIEnabled"><span></span></label></div>
<label class="config-field"><span>Clé API Groq</span><input id="cfgGroqKey" autocomplete="off"></label>
<label class="config-field"><span>Modèle Groq</span><input id="cfgGroqModel"></label>
<div class="savebar"><button class="primary" onclick="testConnection('groq')">🧪 Tester Groq</button><span id="test-groq" class="test-result"></span></div>
<label class="config-field"><span>Clé API Brave Search</span><input id="cfgBraveKey" autocomplete="off"></label>
<label class="config-field"><span>Nombre de résultats Web</span><input id="cfgBraveResults" type="number" min="1" max="10"></label>
<div class="savebar"><button class="primary" onclick="testConnection('brave')">🧪 Tester Brave</button><span id="test-brave" class="test-result"></span></div>
</div>
<div class="settings-card" style="margin-top:18px"><h3>🎯 Confiance IA</h3><p class="desc">Si Groq propose NÉCESSAIRE ou NON NÉCESSAIRE sous le seuil choisi, ForcedFR transforme automatiquement la décision en INDETERMINÉ. INDETERMINÉ n’a pas de seuil.</p>
<div class="profile-card"><div class="profile-name">🎬 Films</div><div class="profile-grid"><label class="profile-field"><span>🟢 NÉCESSAIRE — confiance minimale (%)</span><input id="confFilmsNecessary" type="number" min="0" max="100"></label><label class="profile-field"><span>🔵 NON NÉCESSAIRE — confiance minimale (%)</span><input id="confFilmsNotNecessary" type="number" min="0" max="100"></label></div></div>
<div class="profile-card" style="margin-top:12px"><div class="profile-name">📺 Séries</div><div class="profile-grid"><label class="profile-field"><span>🟢 NÉCESSAIRE — confiance minimale (%)</span><input id="confSeriesNecessary" type="number" min="0" max="100"></label><label class="profile-field"><span>🔵 NON NÉCESSAIRE — confiance minimale (%)</span><input id="confSeriesNotNecessary" type="number" min="0" max="100"></label></div></div>
<div class="savebar"><button class="primary" type="button" onclick="saveAISettings()">💾 Enregistrer l’IA</button></div></div>

</div>
<div>
<div class="settings-card"><h3>🎬 Profil Films</h3><p class="desc">Chaque décision IA possède sa propre action.</p><div id="profileFilm"></div></div>
<div class="settings-card" style="margin-top:18px"><h3>📺 Profil Séries</h3><p class="desc">Les règles des séries sont totalement indépendantes des films.</p><div id="profileSeries"></div></div>
<div class="settings-card" style="margin-top:18px"><h3>⚠️ En cas d’erreur d’analyse</h3><p class="desc">Le comportement reste configurable séparément dans chaque profil.</p><div id="profileErrors"></div></div>
<div class="settings-card" style="margin-top:18px"><h3>🔌 Connexions</h3><p class="desc">Toutes les valeurs sont affichées en clair et enregistrées dans la base SQLite. Un redémarrage de ForcedFR est nécessaire après modification du bot Discord.</p>
<label class="config-field"><span>qBittorrent</span><input id="cfgQBHost"></label><label class="config-field"><span>Utilisateur qBittorrent</span><input id="cfgQBUsername"></label><label class="config-field"><span>Mot de passe qBittorrent</span><input id="cfgQBPassword" type="password" autocomplete="current-password"></label>
<label class="config-field"><span>Radarr URL</span><input id="cfgRadarrUrl"></label><label class="config-field"><span>Radarr API Key</span><input id="cfgRadarrKey"></label>
<label class="config-field"><span>Sonarr URL</span><input id="cfgSonarrUrl"></label><label class="config-field"><span>Sonarr API Key</span><input id="cfgSonarrKey"></label>
<label class="config-field"><span>Discord Bot Token</span><input id="cfgDiscordToken"></label><label class="config-field"><span>ID du salon Discord</span><input id="cfgDiscordChannel"></label><label class="config-field"><span>Discord Webhook URL</span><input id="cfgDiscordWebhook"></label><label class="config-field"><span>Fuseau horaire</span><input id="cfgTZ"></label>
<div class="savebar connection-actions"><button class="primary" type="button" onclick="saveConnectionSettings()">💾 Enregistrer les connexions</button></div>
<div class="savebar connection-tests">
<button type="button" onclick="testConnection('qbittorrent')">🧪 qBittorrent</button>
<button type="button" onclick="testConnection('radarr')">🧪 Radarr</button>
<button type="button" onclick="testConnection('sonarr')">🧪 Sonarr</button>
<button type="button" onclick="testConnection('discord')">🧪 Discord</button>
</div>
<div class="connection-results" aria-live="polite">
<div class="connection-result-item">qBittorrent <span id="test-qbittorrent" class="test-result"></span></div>
<div class="connection-result-item">Radarr <span id="test-radarr" class="test-result"></span></div>
<div class="connection-result-item">Sonarr <span id="test-sonarr" class="test-result"></span></div>
<div class="connection-result-item">Discord <span id="test-discord" class="test-result"></span></div>
</div></div>
<div class="settings-card" style="margin-top:18px"><h3>🏷️ Étiquettes qBittorrent ignorées</h3><p class="desc">Les torrents portant ces étiquettes ne sont pas analysés.</p><input id="cfgQBIgnoredTags" placeholder="cross-seed, autre-etiquette"><div class="small" style="margin-top:10px">Étiquettes disponibles :</div><div id="qbTagsAvailable" class="tag-list"></div><div class="savebar"><button class="primary" type="button" onclick="saveSettings()">💾 Enregistrer</button></div></div>
</div></div></section>

<section id="p-history" class="panel"><h2>Historique des analyses</h2><p class="sub">Historique persistant des analyses et décisions prises sur les téléchargements.</p><div class="filters"><select id="historyFilter"><option value="all">Toutes les analyses</option><option value="forced_found">Forced FR détecté</option><option value="no_forced">Pas de Forced FR</option><option value="error">Erreurs</option></select><input id="historySearch" placeholder="Rechercher un torrent…"><label class="small">Afficher <select id="historyLimit"><option>25</option><option>50</option><option>100</option></select></label></div>
<div class="tablewrap"><table><thead><tr><th>Date</th><th>Torrent</th><th>Résultat</th><th>Décision</th><th>Action</th></tr></thead><tbody id="history"></tbody></table></div></section>

<script>
const $=x=>document.getElementById(x);let data=[],prev=false,loaded=false,timer;
async function api(u,o={}){const r=await fetch(u,o),d=await r.json();if(!r.ok)throw Error(d.detail||'Erreur');return d}
function esc(v){return String(v??'').replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#039;'}[c]))}
function st(id,s,e=''){const m={connected:['● Connecté','ok'],connecting:['● Connexion…','warn'],disabled:['● Désactivé','warn'],error:['● Indisponible','bad'],not_configured:['● Non configuré','warn'],unknown:['● Inconnu','warn']}[s]||['● '+s,'warn'];$(id).textContent=m[0]+(e?' '+e:'');$(id).className='value '+m[1]}
document.addEventListener('click',e=>{const b=e.target.closest('.review-btn');if(b&&b.dataset.reviewKey&&b.dataset.reviewStatus){setReviewByKey(b.dataset.reviewKey,b.dataset.reviewStatus);return}const card=e.target.closest('.series-card');if(card&&!e.target.closest('a,button'))openSeries(card.dataset.seriesKey)});
function switchTab(tab){const b=document.querySelector('.tab[data-tab="'+tab+'"]');if(!b)return;document.querySelectorAll('.tab').forEach(x=>x.classList.toggle('active',x===b));document.querySelectorAll('.panel').forEach(x=>x.classList.remove('active'));const panel=$('p-'+tab);if(panel)panel.classList.add('active');const headings={dashboard:['Vue d’ensemble','Résumé de la bibliothèque et de l’activité récente.'],scan:['Bibliothèque','Films, séries et analyse des pistes de sous-titres.'],history:['Historique','Retrouvez les analyses et les décisions prises par ForcedFR.'],pending:['Fichiers à traiter','Identifiez les films et séries nécessitant une décision.'],settings:['Paramètres','Configurez les connexions, l’intelligence artificielle et les profils.']};const h=headings[tab];if(h){document.querySelector('.app-header h1').textContent=h[0];document.querySelector('.app-header .sub').textContent=h[1]}if(tab==='history')loadHistory();if(tab==='dashboard')loadDashboard();if(tab==='pending'){loadCached('all').then(()=>{renderPending();loadErrors()})}if(tab==='settings')loadSettings();if(tab==='scan'){loadCached('all').then(()=>startScan(document.querySelector('.subtab.active')?.dataset.kind||'films','incremental'))}closeMobileNav()}document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>switchTab(b.dataset.tab));
function closeMobileNav(){document.body.classList.remove('nav-open');const t=$('mobileNavToggle');if(t)t.setAttribute('aria-expanded','false')};$('mobileNavToggle').onclick=()=>{document.body.classList.toggle('nav-open');$('mobileNavToggle').setAttribute('aria-expanded',document.body.classList.contains('nav-open')?'true':'false')};$('navBackdrop').onclick=closeMobileNav;
document.querySelectorAll('.subtab').forEach(b=>b.onclick=async()=>{document.querySelectorAll('.subtab').forEach(x=>x.classList.toggle('active',x===b));['films','series'].forEach(k=>$('k-'+k).style.display=k===b.dataset.kind?'block':'none');$('seriesDetail').classList.remove('active');$('seriesGrid').style.display='grid';await loadCached(b.dataset.kind);await startScan(b.dataset.kind,'incremental')});
async function analyzeLibraryAI(key,force=false){try{const d=await api('/library/ai',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({media_key:key,force})});const r=d.result||{};const i=data.find(x=>x.media_key===key);if(i){i.ai_analyzed=true;i.ai_recommendation=r.recommendation;i.ai_confidence=r.confidence}render('films');render('series');alert('IA : '+(r.recommendation||'INDETERMINÉ')+' — '+Math.round((r.confidence||0)*100)+'%')}catch(e){alert(e.message)}}
async function startLibraryAI(scope,force=false){try{const d=await api('/library/ai/bulk',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({scope,force})});alert(d.message||'Analyse IA lancée.')}catch(e){alert(e.message)}}
async function startScan(s,m='incremental'){try{await api('/scan/'+s+'?mode='+encodeURIComponent(m),{method:'POST'});loaded=false;refresh()}catch(e){alert(e.message)}}
function rows(kind){const f=$(kind==='films'?'ff':'sf').value,q=$(kind==='films'?'fs':'ss').value.toLowerCase();return data.filter(i=>(kind==='films'?i.type==='Film':i.type==='Série')).filter(i=>{const rs=i.review_status||'pending';if(f==='all')return true;if(f==='error')return i.status==='error';if(f==='yes')return i.status!=='error'&&i.forced_french;if(f==='no')return i.status!=='error'&&!i.forced_french;if(f==='pending')return i.status!=='error'&&!i.forced_french&&rs==='pending';if(f==='validated')return i.status!=='error'&&!i.forced_french&&rs==='validated';if(f==='waiting')return i.status!=='error'&&!i.forced_french&&rs==='waiting_replacement';return true}).filter(i=>!q||(i.title+' '+(i.season||'')+' '+(i.episode||'')).toLowerCase().includes(q))}
function badge(i){return i.status==='error'?'<span class="badge err">⚠ Erreur</span>':i.forced_french?'<span class="badge yes">✅ Forced FR</span>':'<span class="badge no">❌ Sans Forced FR</span>'}
function reviewBadge(i){if(i.forced_french||i.status==='error')return '';const m={pending:'<span class="badge no">🔴 À traiter</span>',validated:'<span class="badge yes">🟢 Absence normale</span>',waiting_replacement:'<span class="badge err">🟠 En attente</span>'};return '<div class="small" style="margin-top:6px">'+(m[i.review_status||'pending']||m.pending)+'</div>'}
function mediaActions(i){let a=[];if(i.arr_url)a.push('<a class="btn" target="_blank" href="'+esc(i.arr_url)+'">Ouvrir '+esc(i.arr_source)+'</a>');if(i.status!=='error'&&!i.forced_french&&i.media_key){const key=encodeURIComponent(i.media_key);a.push(`<button class="review-btn" onclick="analyzeLibraryAI(decodeURIComponent('${key}'),false)">🤖 Analyser avec l’IA</button>`);if(i.ai_analyzed)a.push(`<button class="review-btn" onclick="analyzeLibraryAI(decodeURIComponent('${key}'),true)">🔄 Réexaminer avec l’IA</button>`);const rs=i.review_status||'pending';if(rs==='pending'){a.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="validated">✓ C’est normal</button>');a.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="waiting_replacement">⏳ Attendre</button>')}else{a.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="pending">↩ À traiter</button>')}}return a.join(' ')}
async function setReviewByKey(encodedKey,status){const key=decodeURIComponent(encodedKey);const i=data.find(x=>x.media_key===key);if(!i)return alert('Média introuvable dans les résultats actuels.');try{await api('/library/review',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({media_key:key,review_status:status})});i.review_status=status;i.reviewed_at=status==='pending'?null:Date.now()/1000;render('films');render('series')}catch(e){alert(e.message)}}
function openSeries(title){const items=data.filter(i=>i.type==='Série'&&i.title===title).slice().sort((a,b)=>(a.season_number||0)-(b.season_number||0)||(a.episode_number||0)-(b.episode_number||0));if(!items.length)return;const first=items[0],panel=$('seriesDetail');const episodes=items.map(i=>{const status=i.status==='error'?'<span class="episode-err">⚠ Erreur</span>':i.forced_french?'<span class="episode-ok">✅ Forced FR</span>':'<span class="episode-no">❌ Sans Forced FR</span>';const actions=[];if(!i.forced_french&&i.media_key){const key=encodeURIComponent(i.media_key);actions.push(`<button class="review-btn" onclick="analyzeLibraryAI(decodeURIComponent('${key}'),false)">🤖 IA</button>`);if(i.ai_analyzed)actions.push(`<span class="small">${esc(i.ai_recommendation||'INDETERMINÉ')} ${Math.round((i.ai_confidence||0)*100)}%</span>`)}if(i.arr_url)actions.push('<a class="btn" target="_blank" href="'+esc(i.arr_url)+'">Ouvrir Sonarr</a>');if(i.status!=='error'&&!i.forced_french&&i.media_key){const key=encodeURIComponent(i.media_key),rs=i.review_status||'pending';if(rs==='pending'){actions.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="validated">✓ C’est normal</button>');actions.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="waiting_replacement">⏳ Attendre</button>')}else actions.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="pending">↩ À traiter</button>')}return '<div class="episode-card"><div class="episode-number">'+esc(i.season||'—')+' · '+esc(i.episode||'—')+'</div><div><div class="episode-title">'+esc(i.episode_title||i.episode_name||'Épisode')+'</div><div class="episode-status">'+status+(i.error?' — '+esc(i.error):'')+'</div></div><div class="episode-actions">'+actions.join('')+'</div></div>'}).join('');panel.innerHTML='<button class="btn" onclick="closeSeries()">← Retour aux séries</button><div class="series-detail-head"><div>'+poster(first).replace('class="poster"','class="series-detail-poster"')+'</div><div><div class="series-detail-title">'+esc(title)+'</div><div class="series-detail-meta">'+items.length+' épisode(s) · '+items.filter(i=>i.forced_french).length+' avec Forced FR · '+items.filter(i=>i.status!=='error'&&!i.forced_french).length+' sans Forced FR'+(items.filter(i=>i.status==='error').length?' · '+items.filter(i=>i.status==='error').length+' erreur(s)':'')+'</div><div class="series-detail-actions">'+(first.arr_url?'<a class="btn" target="_blank" href="'+esc(first.arr_url)+'">Ouvrir Sonarr</a>':'')+'</div></div></div><div class="episode-list">'+episodes+'</div>';$('seriesGrid').style.display='none';panel.classList.add('active')}
function closeSeries(){$('seriesDetail').classList.remove('active');$('seriesGrid').style.display='grid'}
function poster(i){return i.poster_url?'<img class="poster" loading="lazy" src="'+esc(i.poster_url)+'" alt="">':'<div class="poster-placeholder">'+(i.type==='Film'?'🎬':'📺')+'</div>'}
function filmCard(i){return '<article class="media-card"><a target="_blank" href="'+esc(i.arr_url||'#')+'" style="display:block">'+poster(i)+'</a><div class="media-info"><div class="media-title" title="'+esc(i.title)+'">'+esc(i.title)+'</div><div class="media-meta">'+badge(i)+'</div>'+reviewBadge(i)+(i.error?'<div class="small" style="margin-top:6px">'+esc(i.error)+'</div>':'')+'<div class="media-actions">'+mediaActions(i)+'</div></div></article>'}
function seriesCard(title,items){const first=items[0]||{};const yes=items.filter(i=>i.status!=='error'&&i.forced_french).length,no=items.filter(i=>i.status!=='error'&&!i.forced_french).length,err=items.filter(i=>i.status==='error').length;const needs=items.some(i=>i.status==='error'||(!i.forced_french&&(i.review_status||'pending')==='pending'));const q=($('ss')?.value||'').toLowerCase();if(q&&!title.toLowerCase().includes(q))return '';const sonarr=first.arr_url?'<a class="btn" target="_blank" href="'+esc(first.arr_url)+'">Ouvrir Sonarr</a>':'';const actionInfo=needs?'<span class="series-attention">⚠ '+items.filter(i=>i.status==='error'||(!i.forced_french&&(i.review_status||'pending')==='pending')).length+' à traiter</span>':'';return '<article class="media-card series-card" data-series-key="'+esc(title)+'"><div>'+poster(first)+'</div><div class="media-info"><div class="media-title" title="'+esc(title)+'">'+esc(title)+'</div><div class="media-meta">'+items.length+' épisode(s)</div><div class="series-summary"><span class="mini-pill y">'+yes+' avec FR</span><span class="mini-pill n">'+no+' sans FR</span>'+(err?'<span class="mini-pill w">'+err+' erreur(s)</span>':'')+'</div><div class="media-actions">'+sonarr+'</div></div></article>'}
function openSeries(title){const items=data.filter(i=>i.type==='Série'&&i.title===title).slice().sort((a,b)=>(a.season_number||0)-(b.season_number||0)||(a.episode_number||0)-(b.episode_number||0));if(!items.length)return;const first=items[0],panel=$('seriesDetail');const episodes=items.map(i=>{const status=i.status==='error'?'<span class="episode-err">⚠ Erreur</span>':i.forced_french?'<span class="episode-ok">✅ Forced FR</span>':'<span class="episode-no">❌ Sans Forced FR</span>';const actions=[];if(i.arr_url)actions.push('<a class="btn" target="_blank" href="'+esc(i.arr_url)+'">Ouvrir Sonarr</a>');if(i.status!=='error'&&!i.forced_french&&i.media_key){const key=encodeURIComponent(i.media_key),rs=i.review_status||'pending';if(rs==='pending'){actions.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="validated">✓ C’est normal</button>');actions.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="waiting_replacement">⏳ Attendre</button>')}else actions.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="pending">↩ À traiter</button>')}return '<div class="episode-card"><div class="episode-number">'+esc(i.season||'—')+' · '+esc(i.episode||'—')+'</div><div><div class="episode-title">'+esc(i.episode_title||i.episode_name||'Épisode')+'</div><div class="episode-status">'+status+(i.error?' — '+esc(i.error):'')+'</div></div><div class="episode-actions">'+actions.join('')+'</div></div>'}).join('');panel.innerHTML='<button class="btn" onclick="closeSeries()">← Retour aux séries</button><div class="series-detail-head"><div>'+poster(first).replace('class="poster"','class="series-detail-poster"')+'</div><div><div class="series-detail-title">'+esc(title)+'</div><div class="series-detail-meta">'+items.length+' épisode(s) · '+items.filter(i=>i.forced_french).length+' avec Forced FR · '+items.filter(i=>i.status!=='error'&&!i.forced_french).length+' sans Forced FR'+(items.filter(i=>i.status==='error').length?' · '+items.filter(i=>i.status==='error').length+' erreur(s)':'')+'</div><div class="series-detail-actions">'+(first.arr_url?'<a class="btn" target="_blank" href="'+esc(first.arr_url)+'">Ouvrir Sonarr</a>':'')+'</div></div></div><div class="episode-list">'+episodes+'</div>';$('seriesGrid').style.display='none';panel.classList.add('active')}
function closeSeries(){$('seriesDetail').classList.remove('active');$('seriesGrid').style.display='grid'}
function render(kind){const r=rows(kind),target=$(kind);if(kind==='films'){target.innerHTML=r.length?r.map(filmCard).join(''):'<div class="empty">Aucun film correspondant.</div>';return}const groups={};r.forEach(i=>(groups[i.title]??=[]).push(i));const html=Object.entries(groups).sort((a,b)=>a[0].localeCompare(b[0],'fr')).map(([title,items])=>seriesCard(title,items)).join('');target.innerHTML=html||'<div class="empty">Aucune série correspondante.</div>'}
async function loadCached(scope='all'){try{const d=await api('/library/cached?scope='+scope);if(scope==='all')data=d.results||[];else{const other=data.filter(i=>i.type!==(scope==='films'?'Film':'Série'));data=other.concat(d.results||[])}render('films');render('series')}catch(e){console.error(e)}}
async function loadResults(){data=(await api('/scan/results')).results||[];render('films');render('series');loaded=true}
['ff','fs'].forEach(id=>$(id).oninput=()=>render('films'));['sf','ss'].forEach(id=>$(id).oninput=()=>render('series'));
function historyResult(i){const m={forced_found:'<span class="badge yes">✅ Forced FR détecté</span>',no_forced:'<span class="badge no">❌ Pas de Forced FR</span>',error:'<span class="badge err">⚠ Erreur d’analyse</span>'};return m[i.result]||'<span class="badge">'+esc(i.result||'—')+'</span>'}
function historyDecision(i){const m={auto_pause:'<span class="badge no">⏸ Pause automatique</span>',pause:'<span class="badge no">⏸ Maintenu en pause</span>',resume:'<span class="badge yes">▶ Téléchargement repris</span>'};if(m[i.last_action])return m[i.last_action];if(i.result==='no_forced')return '<span class="badge no">⏸ Pause automatique</span>';return '—'}
async function loadHistory(){const d=await api('/history');window.historyData=d.results||[];renderHistory()}
function renderHistory(){const q=($('historySearch')?.value||'').toLowerCase(),f=$('historyFilter')?.value||'all';const r=(window.historyData||[]).filter(i=>!q||(i.torrent_name||'').toLowerCase().includes(q)).filter(i=>f==='all'||i.result===f).slice(0,Number($('historyLimit')?.value||25));$('history').innerHTML=r.length?r.map(i=>{const actions=[];if(i.tracker_url)actions.push('<a class="btn" target="_blank" href="'+esc(i.tracker_url)+'">Voir le torrent</a>');if(i.qb_url)actions.push('<a class="btn" target="_blank" href="'+esc(i.qb_url)+'">qBittorrent</a>');if(i.arr_url)actions.push('<a class="btn" target="_blank" href="'+esc(i.arr_url)+'">'+esc(i.arr_source==='Sonarr'?'Sonarr':'Radarr')+'</a>');return '<tr><td>'+new Date(i.timestamp*1000).toLocaleString('fr-FR',{dateStyle:'short',timeStyle:'short'})+'</td><td title="'+esc(i.torrent_name)+'">'+esc(i.torrent_name)+'</td><td>'+historyResult(i)+'</td><td>'+historyDecision(i)+'</td><td>'+actions.join(' ')+'</td></tr>'}).join(''):'<tr><td colspan="5" class="empty">Aucune analyse correspondante.</td></tr>'}

function render(kind){const r=rows(kind),target=$(kind);if(kind==='films'){target.innerHTML=r.length?r.map(filmCard).join(''):'<div class="empty">Aucun film correspondant.</div>';return}const groups={};r.forEach(i=>(groups[i.title]??=[]).push(i));const html=Object.entries(groups).sort((a,b)=>a[0].localeCompare(b[0],'fr')).map(([title,items])=>seriesCard(title,items)).join('');target.innerHTML=html||'<div class="empty">Aucune série correspondante.</div>'}
async function loadCached(scope='all'){try{const d=await api('/library/cached?scope='+scope);if(scope==='all')data=d.results||[];else{const other=data.filter(i=>i.type!==(scope==='films'?'Film':'Série'));data=other.concat(d.results||[])}render('films');render('series')}catch(e){console.error('Chargement bibliothèque:',e);const label=$('scanLabel');if(label)label.textContent='⚠ Chargement du cache impossible : '+e.message}}
async function loadResults(){
  const response=await api('/scan/results');
  const incoming=Array.isArray(response.results)?response.results:[];
  // Ne jamais effacer la bibliothèque SQLite au démarrage si le scan en mémoire est vide.
  // Fusionner les résultats du scan avec le cache déjà chargé ; les résultats récents remplacent les anciens pour un même média.
  const keyOf=i=>String(i.media_key||i.path||i.relative_path||((i.type||'')+'|'+(i.title||'')));
  const merged=new Map();
  for(const item of data) merged.set(keyOf(item),item);
  for(const item of incoming) merged.set(keyOf(item),{...(merged.get(keyOf(item))||{}),...item});
  data=Array.from(merged.values());
  render('films');render('series');loaded=true
}
['ff','fs'].forEach(id=>$(id).oninput=()=>render('films'));['sf','ss'].forEach(id=>$(id).oninput=()=>render('series'));
function historyResult(i){const m={forced_found:'<span class="badge yes">✅ Forced FR détecté</span>',no_forced:'<span class="badge no">❌ Pas de Forced FR</span>',error:'<span class="badge err">⚠ Erreur d’analyse</span>'};return m[i.result]||'<span class="badge">'+esc(i.result||'—')+'</span>'}
function historyDecision(i){const m={auto_pause:'<span class="badge no">⏸ Pause automatique</span>',pause:'<span class="badge no">⏸ Maintenu en pause</span>',resume:'<span class="badge yes">▶ Téléchargement repris</span>'};if(m[i.last_action])return m[i.last_action];if(i.result==='no_forced')return '<span class="badge no">⏸ Pause automatique</span>';return '—'}
async function loadHistory(){const d=await api('/history');window.historyData=d.results||[];renderHistory()}
function renderHistory(){const q=($('historySearch')?.value||'').toLowerCase(),f=$('historyFilter')?.value||'all';const r=(window.historyData||[]).filter(i=>!q||(i.torrent_name||'').toLowerCase().includes(q)).filter(i=>f==='all'||i.result===f).slice(0,Number($('historyLimit')?.value||25));$('history').innerHTML=r.length?r.map(i=>{const actions=[];if(i.tracker_url)actions.push('<a class="btn" target="_blank" href="'+esc(i.tracker_url)+'">Voir le torrent</a>');if(i.qb_url)actions.push('<a class="btn" target="_blank" href="'+esc(i.qb_url)+'">qBittorrent</a>');if(i.arr_url)actions.push('<a class="btn" target="_blank" href="'+esc(i.arr_url)+'">'+esc(i.arr_source==='Sonarr'?'Sonarr':'Radarr')+'</a>');return '<tr><td>'+new Date(i.timestamp*1000).toLocaleString('fr-FR',{dateStyle:'short',timeStyle:'short'})+'</td><td title="'+esc(i.torrent_name)+'">'+esc(i.torrent_name)+'</td><td>'+historyResult(i)+'</td><td>'+historyDecision(i)+'</td><td>'+actions.join(' ')+'</td></tr>'}).join(''):'<tr><td colspan="5" class="empty">Aucune analyse correspondante.</td></tr>'}

function pendingItems(){const q=($('pendingSearch')?.value||'').toLowerCase(),type=$('pendingType')?.value||'all';return data.filter(i=>i.status!=='error'&&!i.forced_french&&(i.review_status||'pending')==='pending').filter(i=>type==='all'||i.type===type).filter(i=>(i.title||'').toLowerCase().includes(q)).sort((a,b)=>(b.updated_at||b.last_seen||0)-(a.updated_at||a.last_seen||0))}
function pendingCard(i){return '<div class="card pending-card">'+poster(i)+'<div class="pending-card-body"><strong>'+esc(i.title||'Sans titre')+'</strong><div class="small">'+esc(i.type||'Média')+(i.season?' · '+esc(i.season):'')+(i.episode?' · '+esc(i.episode):'')+'</div><div style="margin:8px 0"><span class="badge no">🔴 À traiter</span></div><div class="media-actions">'+mediaActions(i)+'</div></div></div>'}
function renderPending(){const r=pendingItems().slice(0,Number($('pendingLimit')?.value||25));$('pendingList').innerHTML=r.length?r.map(pendingCard).join(''):'<div class="empty">Aucun fichier à traiter.</div>';renderDashboardPending()}
function renderDashboardPending(){const r=data.filter(i=>i.status!=='error'&&!i.forced_french&&(i.review_status||'pending')==='pending').slice(0,5);const el=$('dashPending');if(el)el.innerHTML=r.length?r.map(pendingCard).join(''):'<div class="empty">Aucun fichier à traiter.</div>'}
$('pendingSearch')?.addEventListener('input',renderPending);$('pendingType')?.addEventListener('change',renderPending);$('pendingLimit')?.addEventListener('change',renderPending);$('historyLimit')?.addEventListener('change',renderHistory);
async function loadDashboard(){const d=await api('/dashboard/stats');loadCached('all').then(()=>renderDashboardPending());if(!window.historyData)loadHistory();const l=d.library,t=d.torrents;const cards=[['Bibliothèque',l.total,'médias'],['Avec Forced FR',l.forced,'validés'],['Sans Forced FR',l.no_forced,'médias'],['🔴 À traiter',l.pending,'médias'],['🟠 En attente',l.waiting,'médias'],['🟢 Absence normale',l.validated,'médias'],['⚠ Erreurs',l.errors,'à traiter'],['Torrents analysés',t.total,'analyses']];$('dashCards').innerHTML=cards.map(c=>'<div class="card"><div class="label">'+c[0]+'</div><div class="value" style="font-size:1.5rem">'+c[1]+'</div><div class="small">'+c[2]+'</div></div>').join('');$('recent').innerHTML=d.recent.length?d.recent.slice(0,5).map(i=>{const name=i.torrent_name||'Torrent';const texts={analysis:i.result==='forced_found'?'Analyse terminée : Forced FR détecté':i.result==='no_forced'?'Analyse terminée : aucun Forced FR':'Analyse terminée',auto_pause:'Téléchargement mis en pause automatiquement',pause:'Téléchargement maintenu en pause',resume:'Téléchargement repris'};const label=texts[i.action]||'Action effectuée';return '<div class="activity-row"><div class="activity-date">'+new Date(i.created_at*1000).toLocaleString('fr-FR',{dateStyle:'short',timeStyle:'short'})+'</div><div class="activity-main"><div class="activity-action"><span class="activity-dot"></span>'+esc(label)+'</div><div class="activity-details">'+esc(name)+(i.details?' — '+esc(i.details):'')+'</div></div><div>'+ (i.result==='forced_found'?'<span class="badge yes">OK</span>':i.result==='no_forced'?'<span class="badge no">À traiter</span>':'<span class="badge err">Info</span>') +'</div></div>'}).join(''):'<div class="activity-empty">Aucune activité enregistrée pour le moment.</div>'}
async function loadErrors(){const d=await api('/errors');$('errors').innerHTML=d.results.length?d.results.map(i=>'<tr><td>'+new Date(i.last_seen*1000).toLocaleString('fr-FR',{dateStyle:'short',timeStyle:'short'})+'</td><td><strong>'+esc(i.title||'—')+'</strong><div class="small">'+esc(i.media_type)+' · '+esc(i.path||'')+'</div></td><td>'+esc(i.error)+'</td><td><button class="review-btn" onclick="resolveError('+i.id+')">✓ Marquer traité</button></td></tr>').join(''):'<tr><td colspan="4" class="empty">Aucune erreur à traiter.</td></tr>'}
async function resolveError(id){try{await api('/errors/resolve',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})});loadErrors();loadDashboard()}catch(e){alert(e.message)}}
async function testConnection(kind){const out=$('test-'+kind);if(out){out.textContent='Test en cours…';out.className='test-result pending'}const buttons=[...document.querySelectorAll('.connection-tests button')];const button=buttons.find(b=>b.getAttribute('onclick')?.includes("'"+kind+"'"));if(button){button.disabled=true;button.textContent='⏳ '+kind}try{const d=await api('/test/'+kind,{method:'POST'});if(out){out.textContent=(d.ok?'✓ ':'✗ ')+(d.message||'Test terminé');out.className='test-result '+(d.ok?'ok':'error')}return d}catch(e){if(out){out.textContent='✗ '+e.message;out.className='test-result error'}return {ok:false,message:e.message}}finally{if(button){button.disabled=false;button.textContent=({qbittorrent:'🧪 qBittorrent',radarr:'🧪 Radarr',sonarr:'🧪 Sonarr',discord:'🧪 Discord'})[kind]||'🧪 Tester'}}}
async function loadQbTags(){
try{
const d=await api('/qbittorrent/tags');
const tags=Array.isArray(d.results)?d.results:[];
const selected=new Set(String($('cfgQBIgnoredTags').value||'').split(',').map(x=>x.trim().toLowerCase()).filter(Boolean));
$('qbTagsAvailable').innerHTML=tags.length
?tags.map(tag=>{
const safe=esc(tag);
const isSelected=selected.has(String(tag).trim().toLowerCase());
return '<button type="button" class="tag-chip '+(isSelected?'selected':'')+'" data-tag="'+safe+'">'+safe+'</button>';
}).join('')
:'<span class="small">Aucune étiquette disponible.</span>';
document.querySelectorAll('#qbTagsAvailable .tag-chip').forEach(btn=>btn.onclick=()=>{
const tag=btn.getAttribute('data-tag')||'';
const current=String($('cfgQBIgnoredTags').value||'').split(',').map(x=>x.trim()).filter(Boolean);
const index=current.findIndex(x=>x.toLowerCase()===tag.toLowerCase());
if(index>=0){current.splice(index,1);btn.classList.remove('selected')}
else{current.push(tag);btn.classList.add('selected')}
$('cfgQBIgnoredTags').value=current.join(', ');
});
}catch(e){
console.error('Impossible de charger les étiquettes qBittorrent:',e);
$('qbTagsAvailable').innerHTML='<span class="small">Impossible de récupérer les étiquettes qBittorrent.</span>';
}
}

async function loadSettings(){const [s,p,c]=await Promise.all([api('/settings'),api('/profiles'),api('/configuration')]);
$('setAIEnabled').checked=s.ai_enabled;$('cfgBraveResults').value=s.brave_results_count;$('confFilmsNecessary').value=s.ai_confidence.films.necessary;$('confFilmsNotNecessary').value=s.ai_confidence.films.not_necessary;$('confSeriesNecessary').value=s.ai_confidence.series.necessary;$('confSeriesNotNecessary').value=s.ai_confidence.series.not_necessary;
$('cfgQBHost').value=c.values.QB_HOST||'';$('cfgQBUsername').value=c.values.QB_USERNAME||'';$('cfgQBPassword').value=c.values.QB_PASSWORD||'';$('cfgRadarrUrl').value=c.values.RADARR_URL||'';$('cfgRadarrKey').value=c.values.RADARR_API_KEY||'';$('cfgSonarrUrl').value=c.values.SONARR_URL||'';$('cfgSonarrKey').value=c.values.SONARR_API_KEY||'';$('cfgDiscordToken').value=c.values.DISCORD_BOT_TOKEN||'';$('cfgDiscordChannel').value=c.values.DISCORD_CHANNEL_ID||'';$('cfgDiscordWebhook').value=c.values.DISCORD_WEBHOOK_URL||'';$('cfgTZ').value=c.values.TZ||'';$('cfgGroqKey').value=c.values.GROQ_API_KEY||'';$('cfgGroqModel').value=c.values.GROQ_MODEL||'';$('cfgBraveKey').value=c.values.BRAVE_SEARCH_API_KEY||'';$('cfgQBIgnoredTags').value=s.qb_ignored_tags||'';loadQbTags();
const film=p.results.find(i=>i.media_type==='Film')||p.results[0];const series=p.results.find(i=>i.media_type==='Série')||p.results[1]||p.results[0];if(film)$('profileFilm').innerHTML=profileEditor(film);if(series)$('profileSeries').innerHTML=profileEditor(series);$('profileErrors').innerHTML='<div class="small">Les options d’erreur sont disponibles dans chaque profil ci-dessus.</div>';
}
function profileEditor(i){const n=esc(i.name);return '<div class="profile-grid"><div class="profile-field"><label>Si FR Forced trouvée</label><select id="found-'+n+'"><option value="resume" '+(i.found_action==='resume'?'selected':'')+'>▶️ Reprendre</option><option value="resume_notify" '+(i.found_action==='resume_notify'?'selected':'')+'>▶️ Reprendre + notif Discord</option><option value="pause_decision" '+(i.found_action==='pause_decision'?'selected':'')+'>⏸️ Laisser en pause + prise de décision</option></select></div><div class="profile-field"><label>Si FR Forced absente</label><select id="tm-'+n+'"><option value="scan_ai" '+(i.torrent_missing_action==='scan_ai'?'selected':'')+'>🤖 Scan IA</option><option value="resume" '+(i.torrent_missing_action==='resume'?'selected':'')+'>▶️ Reprendre</option><option value="resume_notify" '+(i.torrent_missing_action==='resume_notify'?'selected':'')+'>▶️ Reprendre + notif Discord</option><option value="pause_decision" '+(i.torrent_missing_action==='pause_decision'?'selected':'')+'>⏸️ Laisser en pause + prise de décision</option></select></div><div class="profile-field"><label>Après scan bibliothèque</label><select id="miss-'+n+'"><option value="review" '+(i.missing_action==='review'?'selected':'')+'>À traiter</option><option value="validated" '+(i.missing_action==='validated'?'selected':'')+'>Absence normale</option><option value="waiting_replacement" '+(i.missing_action==='waiting_replacement'?'selected':'')+'>Attendre une meilleure release</option></select></div></div><div class="profile-card" style="margin-top:12px"><div class="profile-name">🤖 Décisions IA</div><div class="profile-grid confidence-grid"><div class="profile-field"><label>🟢 NÉCESSAIRE</label><select id="aiN-'+n+'">'+aiActionOptions(i.ai_necessary_action)+'</select></div><div class="profile-field"><label>🔵 NON NÉCESSAIRE</label><select id="aiNN-'+n+'">'+aiActionOptions(i.ai_not_necessary_action)+'</select></div><div class="profile-field"><label>🟠 INDETERMINÉ</label><select id="aiI-'+n+'">'+aiActionOptions(i.ai_indeterminate_action)+'</select></div></div></div><div class="profile-card" style="margin-top:12px"><div class="profile-name">⚠️ Erreur d’analyse</div><div class="profile-field"><label>Action</label><select id="err-'+n+'" data-profile-name="'+n+'" onchange="toggleRetryFields(this.dataset.profileName)"><option value="resume" '+(i.error_action==='resume'?'selected':'')+'>▶️ Reprendre</option><option value="pause_decision" '+(i.error_action==='pause_decision'?'selected':'')+'>⏸️ Laisser en pause + prise de décision</option><option value="retry_resume" '+(i.error_action==='retry_resume'?'selected':'')+'>🔄 Réessayer puis Reprendre</option><option value="retry_pause_decision" '+(i.error_action==='retry_pause_decision'?'selected':'')+'>🔄 Réessayer puis laisser en pause + prise de décision</option></select><div class="retry-fields" id="retry-'+n+'" style="display:'+(i.error_action==='retry_pause_decision'||i.error_action==='retry_resume'?'grid':'none')+'"><label>Nombre de tentatives<input type="number" min="1" max="20" id="retries-'+n+'" value="'+(i.error_retries||5)+'"></label><label>Délai entre tentatives (s)<input type="number" min="1" max="3600" id="delay-'+n+'" value="'+(i.error_retry_delay||30)+'"></label></div></div></div><div class="profile-footer"><label><input type="checkbox" id="ena-'+n+'" '+(i.enabled?'checked':'')+'> Profil actif</label><button type="button" class="primary" data-profile-name="'+n+'" data-media-type="'+esc(i.media_type)+'" data-forced-required="'+(i.forced_required?'true':'false')+'" onclick="saveProfile(this)">💾 Enregistrer le profil</button></div>'}
function aiActionOptions(v){return '<option value="resume" '+(v==='resume'?'selected':'')+'>▶️ Reprendre</option><option value="resume_notify" '+(v==='resume_notify'?'selected':'')+'>▶️ Reprendre + notif Discord</option><option value="pause_decision" '+(v==='pause_decision'?'selected':'')+'>⏸️ Laisser en pause + prise de décision</option>'}
function toggleRetryFields(name){const v=$('err-'+name).value;const box=$('retry-'+name);if(box)box.style.display=(v==='retry_pause_decision'||v==='retry_resume')?'grid':'none'}
async function saveProfile(button){
  const name=button?.dataset?.profileName;
  const mediaType=button?.dataset?.mediaType;
  const forcedRequired=button?.dataset?.forcedRequired==='true';
  if(!name||!mediaType){alert('Impossible d’identifier le profil à enregistrer. Recharge la page et réessaie.');return}
  const oldText=button.textContent;
  const get=id=>document.getElementById(id+'-'+name);
  try{
    button.disabled=true;
    button.textContent='⏳ Enregistrement…';
    const required=['miss','found','tm','err','aiN','aiNN','aiI','ena'];
    const missing=required.filter(prefix=>!get(prefix));
    if(missing.length)throw new Error('Champs introuvables : '+missing.join(', ')+'. Recharge la page puis réessaie.');
    const payload={
      name,media_type:mediaType,forced_required:forcedRequired,
      missing_action:get('miss').value,found_action:get('found').value,
      torrent_missing_action:get('tm').value,error_action:get('err').value,
      error_retries:parseInt(get('retries')?.value||'5',10),
      error_retry_delay:parseFloat(get('delay')?.value||'30'),
      ai_necessary_action:get('aiN').value,
      ai_not_necessary_action:get('aiNN').value,
      ai_indeterminate_action:get('aiI').value,
      enabled:get('ena').checked
    };
    const result=await api('/profiles',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
    if(!result||result.ok!==true)throw new Error(result?.detail||result?.message||'Le serveur n’a pas confirmé la sauvegarde.');
    const verify=await api('/profiles');
    const saved=(verify.results||[]).find(p=>p.name===name);
    if(!saved)throw new Error('Profil introuvable après sauvegarde : la persistance n’a pas pu être confirmée.');
    const fields=['media_type','missing_action','found_action','torrent_missing_action','error_action','ai_necessary_action','ai_not_necessary_action','ai_indeterminate_action'];
    const mismatch=fields.find(k=>String(saved[k]??'')!==String(payload[k]??''));
    if(mismatch||Boolean(saved.enabled)!==payload.enabled)throw new Error('La valeur « '+(mismatch||'enabled')+' » relue ne correspond pas à celle envoyée.');
    button.textContent='✓ Profil enregistré';
    await loadSettings();
    const refreshed=[...document.querySelectorAll('.profile-footer button[data-profile-name]')].find(b=>b.dataset.profileName===name);
    if(refreshed){refreshed.textContent='✓ Profil enregistré';setTimeout(()=>{if(refreshed.isConnected)refreshed.textContent='💾 Enregistrer le profil'},2500)}
  }catch(e){
    console.error('ForcedFR saveProfile error:',e);
    alert('Échec de l’enregistrement du profil '+name+' : '+(e?.message||String(e)));
  }finally{
    const refreshed=[...document.querySelectorAll('.profile-footer button[data-profile-name]')].find(b=>b.dataset.profileName===name);
    if(refreshed)refreshed.disabled=false;
    if(button&&button.isConnected){button.disabled=false;if(button.textContent==='⏳ Enregistrement…')button.textContent=oldText}
  }
}
async function saveAISettings(){try{const numericIds=['cfgBraveResults','confFilmsNecessary','confFilmsNotNecessary','confSeriesNecessary','confSeriesNotNecessary'];for(const id of numericIds){const el=$(id);const n=Number(el.value);if(el.value.trim()===''||!Number.isFinite(n)){el.focus();throw new Error('Valeur numérique invalide : '+id)}if(id!=='cfgBraveResults'&&(n<0||n>100)){el.focus();throw new Error('Les seuils de confiance doivent être compris entre 0 et 100 %.')}if(id==='cfgBraveResults'&&(n<1||n>10)){el.focus();throw new Error('Le nombre de résultats Brave doit être compris entre 1 et 10.')}}await api('/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ai_enabled:$('setAIEnabled').checked,brave_results_count:Number($('cfgBraveResults').value),ai_necessary_min_confidence_films:Number($('confFilmsNecessary').value),ai_not_necessary_min_confidence_films:Number($('confFilmsNotNecessary').value),ai_necessary_min_confidence_series:Number($('confSeriesNecessary').value),ai_not_necessary_min_confidence_series:Number($('confSeriesNotNecessary').value),qb_ignored_tags:$('cfgQBIgnoredTags').value})});await api('/configuration',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({GROQ_API_KEY:$('cfgGroqKey').value,GROQ_MODEL:$('cfgGroqModel').value,BRAVE_SEARCH_API_KEY:$('cfgBraveKey').value})});alert('Paramètres IA enregistrés.');await loadSettings()}catch(e){alert(e.message)}}
async function saveConnectionSettings(){try{await api('/configuration',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({QB_HOST:$('cfgQBHost').value,QB_USERNAME:$('cfgQBUsername').value,QB_PASSWORD:$('cfgQBPassword').value,RADARR_URL:$('cfgRadarrUrl').value,RADARR_API_KEY:$('cfgRadarrKey').value,SONARR_URL:$('cfgSonarrUrl').value,SONARR_API_KEY:$('cfgSonarrKey').value,DISCORD_BOT_TOKEN:$('cfgDiscordToken').value,DISCORD_CHANNEL_ID:$('cfgDiscordChannel').value,DISCORD_WEBHOOK_URL:$('cfgDiscordWebhook').value,TZ:$('cfgTZ').value})});alert('Connexions enregistrées. Redémarre ForcedFR pour appliquer le nouveau token Discord.');loadSettings();refresh()}catch(e){alert(e.message)}}
async function saveSettings(){try{await api('/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({qb_ignored_tags:$('cfgQBIgnoredTags').value})});alert('Paramètres enregistrés.')}catch(e){alert(e.message)}}
$('historyFilter').onchange=renderHistory;$('historySearch').oninput=renderHistory;
async function refresh(){try{const [s,x]=await Promise.all([api('/status'),api('/scan/status')]);st('qb',s.qbittorrent.status,s.qbittorrent.torrents!=null?'('+s.qbittorrent.torrents+')':'');st('discord',s.discord.status);st('radarr',s.radarr.status,s.radarr.version?'v'+s.radarr.version:'');st('sonarr',s.sonarr.status,s.sonarr.version?'v'+s.sonarr.version:'');const p=x.total_files?Math.round(x.processed_files/x.total_files*100):0;$('bar').style.width=p+'%';$('scanLabel').textContent=x.running?'Scan en cours : '+p+'%'+(x.current_file?' — '+x.current_file:''):(x.last_error?'⚠ '+x.last_error:(x.finished_at?'Dernier scan terminé.':'Aucun scan en cours.'));$('stats').textContent='Analysés : '+x.processed_files+'/'+x.total_files+' • Avec FR Forced : '+x.files_with_forced_fr+' • Sans FR Forced : '+x.files_without_forced_fr+' • Cache : '+(x.cache_hits||0)+' • FFprobe : '+(x.reanalyzed||0)+' • Erreurs : '+x.errors;if(!loaded||(prev&&!x.running))await loadResults();prev=x.running;clearTimeout(timer);timer=setTimeout(refresh,x.running?5000:15000)}catch(e){console.error(e);clearTimeout(timer);timer=setTimeout(refresh,15000)}}(async()=>{try{await loadCached('all')}catch(e){console.error(e)}loadDashboard();refresh()})();

const themeSaved=(()=>{try{return localStorage.getItem('forcedfr-theme')}catch(e){return null}})();
function applyTheme(theme){document.body.dataset.theme=theme;const light=theme==='light';if($('themeIcon'))$('themeIcon').textContent=light?'☾':'☼';if($('themeLabel'))$('themeLabel').textContent=light?'Mode sombre':'Mode clair';if($('themeToggle'))$('themeToggle').title=light?'Passer au thème sombre':'Passer au thème clair';try{localStorage.setItem('forcedfr-theme',theme)}catch(e){}}
applyTheme(themeSaved==='light'?'light':'dark');$('themeToggle')?.addEventListener('click',()=>applyTheme(document.body.dataset.theme==='light'?'dark':'light'));
</script></main></div><script>\n// Synchronise les pastilles de la barre de services avec les statuts existants.\n(function(){const pairs=[['qb','service-qbittorrent'],['discord','service-discord'],['radarr','service-radarr'],['sonarr','service-sonarr']];function update(id,cardId){const el=document.getElementById(id),card=document.getElementById(cardId);if(!el||!card)return;const dot=card.querySelector('.service-status-dot');const t=(el.textContent||'').toLowerCase();if(!t)return;if(/erreur|échec|failed|offline|déconnect|indisponible|injoignable/.test(t)){dot.classList.remove('online','warning');dot.classList.add('offline')}else if(/connect|ok|succès|version|torrent/.test(t)){dot.classList.remove('offline','warning');dot.classList.add('online')}else{dot.classList.remove('online','offline');dot.classList.add('warning')}}function all(){pairs.forEach(([id,c])=>update(id,c))}document.addEventListener('DOMContentLoaded',()=>{all();pairs.forEach(([id])=>{const el=document.getElementById(id);if(el)new MutationObserver(all).observe(el,{childList:true,subtree:true,characterData:true})});setInterval(all,1500)})})();\n</script>
</body></html>"""


@app.get("/status")
def status() -> dict[str, Any]:
    qb_status, qb_count = _qbittorrent_status()
    radarr_status = _arr_connection_status(RADARR_URL, RADARR_API_KEY)
    sonarr_status = _arr_connection_status(SONARR_URL, SONARR_API_KEY)

    return {
        "status": "ok",
        "version": "2.8.6",
        "uptime_seconds": int(time.time() - SERVICE_STARTED_AT),
        "qbittorrent": {
            "status": qb_status,
            "host": QB_HOST,
            "torrents": qb_count,
        },
        "discord": {
            "status": _discord_status(),
            "channel_id": DISCORD_CHANNEL_ID or None,
        },
        "radarr": radarr_status,
        "sonarr": sonarr_status,
        "monitoring": {
            "enabled": True,
            "poll_seconds": POLL_SECONDS,
            "known_torrents": len(previous_torrents),
            "processing_torrents": len(processing_torrents),
        },
        "scan": {
            "running": scan_state.get("running", False),
            "scope": scan_state.get("requested_scope"),
            "processed_files": scan_state.get("processed_files", 0),
            "total_files": scan_state.get("total_files", 0),
            "duplicates_skipped": scan_state.get("duplicates_skipped", 0),
        },
        "libraries": {
            "source": "Radarr / Sonarr",
            "radarr": {"configured": bool(RADARR_URL and RADARR_API_KEY), "url": RADARR_URL},
            "sonarr": {"configured": bool(SONARR_URL and SONARR_API_KEY), "url": SONARR_URL},
            "path_mappings_configured": len(ARR_PATH_MAPPINGS),
        },
    }


@app.get("/dashboard/stats")
def dashboard_stats() -> dict[str, Any]:
    return get_dashboard_stats()


@app.get("/errors")
def library_errors(include_resolved: bool = False) -> dict[str, Any]:
    return {"results": get_library_errors(include_resolved=include_resolved)}


@app.post("/errors/resolve")
def resolve_error(payload: dict[str, Any]) -> dict[str, Any]:
    error_id = int(payload.get("id") or 0)
    if error_id <= 0:
        raise HTTPException(status_code=400, detail="Identifiant d'erreur invalide.")
    with _db_connect() as conn:
        cur = conn.execute("UPDATE library_errors SET resolved_at=? WHERE id=?", (time.time(), error_id))
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Erreur introuvable.")
    return {"ok": True}


@app.post("/test/qbittorrent")
def test_qbittorrent() -> dict[str, Any]:
    try:
        load_runtime_configuration()
        qb_login()
        torrents = get_torrents()
        return {"ok": True, "message": f"qBittorrent connecté — {len(torrents)} torrent(s)."}
    except Exception as exc:
        text=str(exc)
        if "IP bannie" in text or "HTTP 403" in text:
            return {"ok": False, "message": f"qBittorrent : {text} Aucun nouvel essai automatique ne sera effectué."}
        return {"ok": False, "message": f"qBittorrent : {text} — vérifie l'URL, l'utilisateur et le mot de passe."}


@app.post("/test/radarr")
def test_radarr() -> dict[str, Any]:
    result = _arr_connection_status(RADARR_URL, RADARR_API_KEY)
    if result.get("status") == "connected":
        return {"ok": True, "message": f"Radarr connecté — version {result.get('version') or 'inconnue'}."}
    return {"ok": False, "message": "Radarr : " + (result.get("error") or "connexion impossible")}


@app.post("/test/sonarr")
def test_sonarr() -> dict[str, Any]:
    result = _arr_connection_status(SONARR_URL, SONARR_API_KEY)
    if result.get("status") == "connected":
        return {"ok": True, "message": f"Sonarr connecté — version {result.get('version') or 'inconnue'}."}
    return {"ok": False, "message": "Sonarr : " + (result.get("error") or "connexion impossible")}


@app.post("/test/discord")
def test_discord() -> dict[str, Any]:
    if discord is None:
        return {"ok": False, "message": "Le module Discord n'est pas installé."}
    if discord_bot is None or not discord_bot.is_ready():
        return {"ok": False, "message": "Bot Discord non connecté. Vérifie la configuration et redémarre ForcedFR après un changement de token."}
    try:
        future = asyncio.run_coroutine_threadsafe(send_discord_test_message(), MAIN_EVENT_LOOP)
        future.result(timeout=15)
        return {"ok": True, "message": "Message de test envoyé dans Discord avec les boutons d'action."}
    except Exception as exc:
        return {"ok": False, "message": f"Discord : {exc}"}


@app.post("/test/groq")
def test_groq() -> dict[str, Any]:
    try:
        load_runtime_configuration()
        key = GROQ_API_KEY or _setting("config_GROQ_API_KEY", "")
        if not key:
            return {"ok": False, "message": "Clé Groq non configurée."}
        result = _groq_generate("Film", "ForcedFR test", [{"title":"Test","url":"https://example.com","description":"Test technique uniquement."}], {})
        return {"ok": True, "message": f"Groq connecté — modèle {result.get('model') or GROQ_MODEL}."}
    except Exception as exc:
        return {"ok": False, "message": f"Groq : {exc}"}


@app.post("/test/brave")
def test_brave() -> dict[str, Any]:
    try:
        load_runtime_configuration()
        results = _brave_search("ForcedFR test", "Film")
        return {"ok": True, "message": f"Brave Search connecté — {len(results)} résultat(s)."}
    except Exception as exc:
        return {"ok": False, "message": f"Brave : {exc}"}


@app.get("/configuration")
def configuration() -> dict[str, Any]:
    secret_keys = {"DISCORD_BOT_TOKEN", "DISCORD_WEBHOOK_URL", "QB_PASSWORD", "RADARR_API_KEY", "SONARR_API_KEY", "GROQ_API_KEY", "BRAVE_SEARCH_API_KEY"}
    values: dict[str, Any] = {}
    configured: dict[str, bool] = {}
    for key, fallback in CONFIG_FALLBACKS.items():
        value = _setting("config_" + key, fallback)
        configured[key] = bool(value)
        # Les clés/API sont volontairement affichées en clair dans les paramètres,
        # conformément au choix utilisateur.
        values[key] = value
    return {"values": values, "configured": configured}


@app.post("/configuration")
def update_configuration(payload: dict[str, Any]) -> dict[str, Any]:
    global DISCORD_BOT_TOKEN, DISCORD_CHANNEL_ID, DISCORD_WEBHOOK_URL, QB_HOST, QB_PASSWORD, QB_USERNAME
    global RADARR_API_KEY, RADARR_URL, SONARR_API_KEY, SONARR_URL, GROQ_API_KEY, GROQ_MODEL, BRAVE_SEARCH_API_KEY
    secret_keys = {"DISCORD_BOT_TOKEN", "DISCORD_WEBHOOK_URL", "QB_PASSWORD", "RADARR_API_KEY", "SONARR_API_KEY", "GROQ_API_KEY", "BRAVE_SEARCH_API_KEY"}
    for key in CONFIG_FALLBACKS:
        if key not in payload: continue
        value = str(payload.get(key) or "").strip()
        if key in secret_keys and not value: continue
        set_setting("config_" + key, value)
    load_runtime_configuration()
    return configuration()



@app.post("/ai/test")
def ai_test() -> dict[str, Any]:
    title="ForcedFR test"
    try:
        sources=_brave_search(title,"Film")
        result=_groq_generate("Film",title,sources,{})
        return {"ok":True,"brave_results":len(sources),"groq":result}
    except Exception as exc:
        return {"ok":False,"message":str(exc)}

@app.get("/qbittorrent/tags")
def qbittorrent_tags() -> dict[str, Any]:
    return {"results": get_qbittorrent_tags()}


@app.get("/settings")
def settings() -> dict[str, Any]:
    return {
        "qb_ignored_tags": _setting("qb_ignored_tags", ""),
        "ai_enabled": _setting_bool("ai_enabled", True),
        "brave_results_count": int(_setting("brave_results_count", "5") or 5),
        "ai_confidence": {
            "films": {
                "necessary": float(_setting("ai_necessary_min_confidence_films", "70") or 70),
                "not_necessary": float(_setting("ai_not_necessary_min_confidence_films", "70") or 70),
            },
            "series": {
                "necessary": float(_setting("ai_necessary_min_confidence_series", "70") or 70),
                "not_necessary": float(_setting("ai_not_necessary_min_confidence_series", "70") or 70),
            },
        },
    }


@app.post("/settings")
def update_settings(payload: dict[str, Any]) -> dict[str, Any]:
    if "qb_ignored_tags" in payload:
        set_setting("qb_ignored_tags", str(payload.get("qb_ignored_tags") or ""))
    if "ai_enabled" in payload:
        set_setting("ai_enabled", "1" if bool(payload["ai_enabled"]) else "0")
    if "brave_results_count" in payload:
        set_setting("brave_results_count", str(max(1, min(10, int(payload.get("brave_results_count") or 5)))))
    for key in ("ai_necessary_min_confidence_films", "ai_not_necessary_min_confidence_films", "ai_necessary_min_confidence_series", "ai_not_necessary_min_confidence_series"):
        if key in payload:
            set_setting(key, str(max(0.0, min(100.0, float(payload.get(key) or 70)))))
    return settings()


@app.get("/profiles")
def profiles() -> dict[str, Any]:
    with _db_connect() as conn:
        rows = conn.execute("SELECT * FROM forcedfr_profiles ORDER BY media_type, name").fetchall()
    return {"results": [dict(r) for r in rows]}


@app.post("/profiles")
def update_profile(payload: dict[str, Any]) -> dict[str, Any]:
    name = str(payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Nom de profil manquant.")
    media_type = str(payload.get("media_type") or "Film")
    if media_type not in {"Film", "Série"}:
        raise HTTPException(status_code=400, detail="Type de média invalide.")
    with _db_connect() as conn:
        conn.execute("""
            INSERT INTO forcedfr_profiles(name,media_type,forced_required,missing_action,error_action,enabled,updated_at,error_retries,error_retry_delay,found_action,torrent_missing_action,ai_necessary_action,ai_not_necessary_action,ai_indeterminate_action)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(name) DO UPDATE SET media_type=excluded.media_type,forced_required=excluded.forced_required,missing_action=excluded.missing_action,error_action=excluded.error_action,enabled=excluded.enabled,updated_at=excluded.updated_at,error_retries=excluded.error_retries,error_retry_delay=excluded.error_retry_delay,found_action=excluded.found_action,torrent_missing_action=excluded.torrent_missing_action,ai_necessary_action=excluded.ai_necessary_action,ai_not_necessary_action=excluded.ai_not_necessary_action,ai_indeterminate_action=excluded.ai_indeterminate_action
        """, (name,media_type,int(bool(payload.get("forced_required",True))),str(payload.get("missing_action") or "review"),str(payload.get("error_action") or "resume"),int(bool(payload.get("enabled",True))),time.time(),max(1,min(20,int(payload.get("error_retries") or 5))),max(1.0,min(3600.0,float(payload.get("error_retry_delay") or 30))),str(payload.get("found_action") or "resume"),str(payload.get("torrent_missing_action") or "scan_ai"),str(payload.get("ai_necessary_action") or "pause_decision"),str(payload.get("ai_not_necessary_action") or "resume_notify"),str(payload.get("ai_indeterminate_action") or "pause_decision")))
    return {"ok": True}



@app.get("/history")
def history() -> dict[str, Any]:
    results = get_analysis_history()
    for item in results:
        item["qb_url"] = build_qbittorrent_url(str(item.get("torrent_hash", "")))
    return {
        "results": results,
        "persistent": True,
        "storage": "sqlite",
    }


def _find_library_item_by_media_key(media_key: str) -> tuple[dict[str, Any], Path] | None:
    for scope in ("films", "series"):
        for item in _scan_items(scope):
            path = item.get("path")
            if not path:
                continue
            if _media_review_key(item, path) == media_key:
                return item, Path(path)
    return None


def _library_ai_result(item: dict[str, Any], file_path: Path, force: bool = False) -> dict[str, Any]:
    cached = _cached_analysis(item, file_path)
    if not cached or cached.get("forced_french"):
        raise RuntimeError("Ce média ne peut pas être analysé par l'IA : il est absent du cache ForcedFR ou possède déjà une piste FR Forced.")
    media_type = str(item.get("type") or "Film")
    title = str(item.get("title") or file_path.stem)
    if not force:
        existing = _ai_cache_get(media_type, title)
        if existing:
            decision, confidence, _ = _normalize_ai_decision(media_type, existing)
            existing["recommendation"] = decision
            existing["confidence"] = confidence
            return existing
    probe_summary = {
        "forced_french": bool(cached.get("forced_french")),
        "forced_tracks": cached.get("forced_tracks", []),
        "subtitles": cached.get("subtitles", []),
    }
    dummy = {"hash": f"library:{_ai_cache_key(media_type,title)}", "name": title, "progress": 1.0}
    result = research_forced_fr_with_ai(dummy, media_type, title, probe_summary, ignore_cache=force)
    decision, confidence, _ = _normalize_ai_decision(media_type, result)
    result["recommendation"] = decision
    result["confidence"] = confidence
    return result


@app.post("/library/ai")
def library_ai(payload: dict[str, Any]) -> dict[str, Any]:
    media_key = str(payload.get("media_key") or "").strip()
    force = bool(payload.get("force", False))
    if not media_key:
        raise HTTPException(status_code=400, detail="media_key manquant.")
    found = _find_library_item_by_media_key(media_key)
    if not found:
        raise HTTPException(status_code=404, detail="Média introuvable dans Radarr/Sonarr.")
    item, path = found
    try:
        result = _library_ai_result(item, path, force=force)
        return {"ok": True, "media_key": media_key, "title": item.get("title"), "type": item.get("type"), "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/library/ai/bulk")
def library_ai_bulk(payload: dict[str, Any]) -> dict[str, Any]:
    scope = str(payload.get("scope") or "films")
    force = bool(payload.get("force", False))
    if scope not in {"films", "series", "all"}:
        raise HTTPException(status_code=400, detail="Scope invalide.")
    items = _scan_items(scope)
    eligible=[]
    for item in items:
        path=item.get("path")
        if not path:
            continue
        try:
            cached=_cached_analysis(item,path)
            if cached and not cached.get("forced_french"):
                if force or not _ai_cache_get(str(item.get("type") or "Film"), str(item.get("title") or Path(path).stem)):
                    eligible.append((item,Path(path)))
        except Exception:
            continue
    if not eligible:
        return {"ok":True,"started":False,"count":0,"message":"Aucun média nouveau à analyser avec l'IA."}
    def worker():
        for item,path in eligible:
            try:
                _library_ai_result(item,path,force=force)
                log.info("[AI-LIBRARY] Analyse IA terminée : %s",item.get("title"))
            except Exception as exc:
                log.error("[AI-LIBRARY] Erreur sur %s : %s",item.get("title"),exc)
    threading.Thread(target=worker,daemon=True,name="forcedfr-ai-library").start()
    return {"ok":True,"started":True,"count":len(eligible),"message":f"{len(eligible)} média(s) placé(s) dans la file d'analyse IA."}


@app.post("/library/review")
def library_review(payload: dict[str, Any]) -> dict[str, Any]:
    media_key = str(payload.get("media_key") or "").strip()
    review_status = str(payload.get("review_status") or "").strip()
    review_note = payload.get("review_note")
    if not media_key:
        raise HTTPException(status_code=400, detail="media_key manquant.")
    if review_status not in {"pending", "validated", "waiting_replacement"}:
        raise HTTPException(status_code=400, detail="Statut de décision invalide.")
    if not set_media_review(media_key, review_status, review_note if isinstance(review_note, str) else None):
        raise HTTPException(status_code=404, detail="Média introuvable dans la base d'analyse.")
    for item in scan_state.get("results", []):
        if item.get("media_key") == media_key:
            item["review_status"] = review_status
            item["reviewed_at"] = None if review_status == "pending" else time.time()
            item["review_note"] = review_note if isinstance(review_note, str) else None
    return {"ok": True, "media_key": media_key, "review_status": review_status}


@app.get("/library/cached")
def library_cached(scope: str = "all") -> dict[str, Any]:
    if scope not in {"films", "series", "all"}:
        raise HTTPException(status_code=400, detail="Scope invalide.")
    return {"results": get_cached_library_results(scope)}


@app.get("/scan/status")
def scan_status() -> dict[str, Any]:
    return dict(scan_state)


@app.get("/scan/results")
def scan_results() -> dict[str, Any]:
    return {
        "running": scan_state.get("running", False),
        "results": list(scan_state.get("results", [])),
    }


@app.post("/scan/films")
def scan_films(mode: str = "incremental") -> dict[str, Any]:
    return start_library_scan("films", mode)


@app.post("/scan/series")
def scan_series(mode: str = "incremental") -> dict[str, Any]:
    return start_library_scan("series", mode)


@app.post("/scan/all")
def scan_all(mode: str = "incremental") -> dict[str, Any]:
    return start_library_scan("all", mode)
