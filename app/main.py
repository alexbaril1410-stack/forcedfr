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
<title>ForcedFR v2.8.7</title>
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
.service-card .service-name::before{content:"";display:inline-block;width:11px;height:11px;flex:0 0 11px;border-radius:50%;background:var(--green)}
.service-card.service-offline .service-name::before{background:var(--red)}
.service-card.service-warning .service-name::before{background:var(--yellow)}
@media(max-width:760px){
 .profile-card .profile-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
 .profile-card .profile-field span{min-height:5.2em;font-size:.9rem}
 .brand-logo{width:52px;height:52px;flex-basis:52px}
}
@media(max-width:430px){
 .profile-card .profile-grid{grid-template-columns:1fr}
 .profile-card .profile-field span{min-height:0}
}


/* ForcedFR 2.8.7 — barre d'état compacte */
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

</style></head><body><button class="mobile-nav-toggle" id="mobileNavToggle" aria-label="Afficher la navigation" aria-expanded="false">☰ <span>Menu</span></button><div class="nav-backdrop" id="navBackdrop"></div><div class="app-shell"><aside class="sidebar"><div class="sidebar-brand"><img class="brand-logo" src="data:image/png;base64,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" alt="Logo ForcedFR"><div><h1>ForcedFR</h1><small>Media intelligence</small></div></div><div><div class="nav-label">Espace de travail</div><nav class="tabs"><button class="tab active" data-tab="dashboard"><span class="nav-icon">⌂</span>Vue d’ensemble</button><button class="tab" data-tab="scan"><span class="nav-icon">▦</span>Bibliothèque</button><button class="tab" data-tab="history"><span class="nav-icon">◷</span>Historique</button><button class="tab" data-tab="pending"><span class="nav-icon">⚠</span>Fichiers à traiter</button><button class="tab" data-tab="settings"><span class="nav-icon">⚙</span>Paramètres</button></nav></div><div class="sidebar-bottom"><strong>Surveillance active</strong><p>ForcedFR surveille les téléchargements et vérifie les pistes de sous-titres.</p></div></aside><main class="main-content">
<header class="app-header"><div><h1>Vue d’ensemble</h1><p class="sub">Suivez votre bibliothèque et l’activité de ForcedFR.</p></div><div class="header-actions"><button class="theme-toggle" id="themeToggle" type="button" title="Changer le thème"><span class="theme-icon" id="themeIcon">☼</span><span id="themeLabel">Mode clair</span></button><div class="version">v2.8.7</div></div></header>
<div class="services service-status-row">
<div class="service-card" id="service-forcedfr"><div class="service-icon"><img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAQAAAAEACAYAAABccqhmAAEAAElEQVR42uz9d6Bt11Xfi3/GmHOt3U4/t+vqql1JlmRZlmVbAveCDQEcAiGNhJe8dCCBAIkfBMeYEgiQlwSSQAq8BAj5JU4ChGLANrGwjYvc1buudHX7PXXXtdac4/fHXLsdyWATF5ncaR3fs/fZZe211xhzjO/4ju+AS+vSurQurUvr0rq0Lq1L69K6tC6tS+vSurQurUvr0rq0Lq1L69K6tC6tS+vSurQurUvr0rq0Lq1L69K6tC6tS+vSurQurUvr0rq0Lq1L69K6tC6tS+vSurQurUvr0rq0Lq1L69K6tC6tS+vSurSea0v+GB27fZaPlT/g9ufq3AncpunXhT3H1xVuA7a3lfIyIYyEsq9UC+nxvhvJ2hHXMLLW9LnlID12vFwj/S2MhHJRJ/drw8h3I50DEYBiVygH6XnhwPT5ob7PtQx3Lr1W1jKWj0X46PRwB1cLrceeeY4XFoxuV1ioP9+d9f23ddPrfnThWb6XAwbnZP723vX2T/d92mfxHY2/V7tk6n+8HID8ES4K+Swe90c9jtn3qP/2qmd5vdo4bhwIRSEM9yllX4mrQhgoa8B2K6K5oRtGvhDnnh5GQthXG+6F2gHsE2LtGA4CZ4FmEZ7hPMYOYGz4++vnnG9MHzfrOD7dejZn8OnWszqBT7fGzuCP7AAuGfv/YRGAfRHPifzBz/s0xj9r+KEUqoESFpSVIFQtJVaCa0QsCOKMbBDZWIO1DXDeuACsLgmxEtSnzx8rIZQCK2AXBVk3mhbYGiRDXl+RaaTQUKq+0h4qsZXuV2/0y4h4IxtFer7C1c5jHBnMrmdzLM/6mEcgz6ePbdXPmwQXny5CALjR4G2XHMD/wQ7gDzPy/51d3f4I50H+8PteJWmHnzGKG+t/i0IoLxOKrj7D6MuBw1pCLr4V1CHOLJaKRUHUaDamrxeDmEURl0VEjVCqRXWgBjE9TnwUb0GKGGi303NiqebMW+kzojhz5pGQm2hGlEqzuCuVBbG8cCajXmaBoQXyEJIjmDHkjW1DcyMWwtqyEIo6ItkxNrL0OL9YRxGnIa6lv2e707Qmf/LZv7v7ZpwLV8dniQj+d43cvoDX6CUH8Hk+uZ+JE5DP0/mS+V2+NvzjhVCW0/uzzCgvS7eHm46y6VjI3cToLQjNSltV20ePJ6qzGFI+n2dCrBRzk/zezIQ8R0oxAPNBieIgAysEEUNdlBgLcRaggYXC4cVZwJtIjsTMKhrOaQuhgTFSke0qxlLEDdTRp/DRtYajvi6N6PYNqaMN1zPUGy63hFs0lFgKmhnSTY8ZRy8uM0IplH0HQHO5YrBZptfJbA7DCKXgMqN5IRl9nltyBgs2kxrsvR7si+wAPpuN6JID+COcwM/VyZX/zeftCfdfNTV4CuGKUghBCAeFWD7zvWIlhAVlIXcMy7zpQ24VmXlJBu8b3qCJSMNimeF90wkZUYMoFXhMYm5mLaLkqDhVFUwUsQxRn45MjGASxKKIdEX8AKJNnYc4sCbQMLShai2FRjQqIn0RCxEKYEfEVVKxG7K4TbAgagGJkUoNtRQVRHH42llVIYplI6SyFIRQMQJipZabSOmi4gYD0yHaN3YHEW0kY48jZakSdryhjYjvRjQzOtuBR5bjfLpwp32aa8K+SA7gUgrwedrpP9fhlfxvGL7AbfXvM0Y/XHZUIyWWSlyUFIJrfbxRWE4hO6GlVOpbufhgsRM1WwJrm1lb1S0humxmLRVpmEkHkf0podeRiJ6MYiLImsEBjAURGoiqiGaIZIK6+liVGNXMoql2Ee2JUAEjkNJSBOFBvIo4AwV8/QNmCmBCqUaBsCvGLlhpMMKsrM9GFc0imIIZ0UywAmU7RhkIMhC1XWKMEC2YFlgsFLbV2EJjGMWywIdAD2hFoV+fafURLSJZERhJQXM1kD1t8xHBnX+QA7iEE/wxiQDki2T848fL1PBroz8OlPVuP1pyFJnHSqXTSk7AouAaAVFLTqGhKU8vXNPnuXnLYpTFaHGfc9oxdM1M9ouyAJIjNARZNnRdRNoIpUTZNLEgyKKJ7BekCZKh4kXEIyoiKma4iDmiYRBUZCQihYlUZgyACkRAQMWL4UEUTDHSvyAIhhHBKrChifYxi2JWIQSIRCNgKQoQixUYGBXYlpkNUOmL2aahAyRUGJWIDUKIGyp2UcwVYjaSUBVDoAkkvGNkQ1FD1Cj7JWVzSDYMk2hgzgnMrjufzfjtkhP40sUA5HP4hcln+ZhnGj6lcLhQiiVHLJUq93QqpfKeTDOid0Tn8lh6a/oGUb1ZVMwJTp2DtpksmkgzGTf7gZaorAu6GqEpIpmhDRFpp79pyptNKkQC0ABdQPCISvpxgiEigokoNvkcMRmrJqAQChMiERUEVAURh0Uxk3SfCEYULCJmBgZCZVACKGZYtJj+ZILFaDGq2cjEKoxoxBFmhUAlMDRshNkIbIgxxOI5Q06DjSTGXdG4E6IFxALBAhqDRCmRGCRqUUjZo3IFrgqMXEE2DDS3A51OnAcL56KCz6aEaJccwJfGcdnn+DPKpw/5XwWcVyiEo0OlLJVqJe3whTVZyNoE58GEqD5T3zAnGVG8ERfFuSsxWUJFEDEx6SC6ZrCMSkeiroqwIGI54lop99cM0RwRL4agaoggpoqogoioqImogIg4M1FDXH3kKvUvZiIImjZ7EkwAYrVfEywCijhNgKKICGqGmFja/IWgZiZSb+3YOO2ORgzT29FihJERSsyiWRCxaPXXpVhUsBJjIGaVEbcNzmE2EOyimW1goTBsBHFAlL5I3BFsGMyGEqsdEekL9IvCdsi1YCgljVCRX4hkmfFIbjB2BrORwWcVFdglB/DcPTb7I6YGn4nx7/n9Rp3k90WRwK1iyVENHdZRJCxlDVsiqodMTMlQa5ipQ1xuMa478dfhZDVZnoKoF2XFkHWMpqg0QJoi2ohIE2iIpHBexCuiQm3AaWd2glNBVImiiII4cLUDQCUZex0AiBjiUlRQ3xbJ0u+1aUp9d3Igmk6nJezCLCDE5GeIhgWwaGYVYjE5DTOQlPubhdIsVMQAMZC8Q0whPQYxRMNGgpWYDc2si8SBWdyQGDexODLiABgKNsBs28y6AeuJxW0q+qrsOqrzw+BH+FgxlJJsGMjakeaFyCMA4zLlXmfwDNDwkhP4EnAAfxAe8Ic5APkM/jalzXKbwLZydKiMlhyhUKqhp5llVAsZofDk3hFGDa+yz9StG77tRDJMc4M2TtoStY2yH/QoIm0RdYgIiMN0wVSWRMQh6gTJUHWgThBFRNOvTkQdRrqLyY8TcIJpwvs0Y2q8HhMREUf6m1pt9YAjHYbHRIkxMg73MUnFAxHMDCQiKibRBAsgycYhmFmEWCYDt4jFKkUSFg2rAhZism+DOqI3i/W/AYjBYjRiCMRQIVZBLIhhiMXCzEYII7DSYtgRi70YY0+wsxLZiWJdJTwVzTYlsF3m2QallDS6JSMp0CyVHDmZvlLnjBOZJacwBxzaZ2H0dskBPPeO1f6Qv32Gxl/v9ACHey7t8gNPyD2t3FFUrQxbtlwWLIpPliUdE9kPHBTVVcR5kAbCkoquiLEchSVwHVHxiHMgDlWvJhkqmahTIYX1IoqlHdxMVGT8NjjMJaNNO3yWqn3ixCQD5xGXMgZLDkLAY9Fj5jBTLDqMDJM6s3A5iEckhf4JWnCgHosRgiXYQMdl/BKqEqxArAArQSukhiMSJBHS/RYsOYcEA0io0v1EiMXUIcSqdiCVWX1fSiliMGJIAEMIZnFIjCUWSol22gjbhvXFOGUWz0SxJxw8JNENC6l69ItdXCuVJdkAcYZvRLKNOHUEy/EPwQn+j4wK/JfY8dpnEc7/Acj+bQLbCdg7OHQMO57Qc7RyT0lOUeaodsy5JTNbQl1uJh6JbdB1QdcRWUA0E6QhsIjJUhRZRKQl4nJEvYgmq1XvFHWmogm0c4iqoV5IRq9CMsa0s2f173n612WI5KA5FlVizIjWwEIDpIH6Jq7ZwS908AtN/FKTbLlJttIkW8zI2jnS9mQtR950IplO4IAgyf5jYRKDISFCiMRhJPQqyt3Cim5BuTmi2O5T7Y6Ig4Ky2yMM+2B9LAxEpDKXlagUAgUaK7ACkh2DlUgMZrGsnUaZKotWRTGDGEUIRKmUGHMkqlnlkbgiJg6LTbM4ElFUGEY4KwSfi4ai0R7hqlR5YRV6w4j1A6wB7cgVp+HEtsLA4LZxNLD3mvpCVKIuRQBfxM+hqTOvDvX7/Yyw4Kh8Rk6ToBmeBqIdh3QwWTXV/YgsCSwg2sSkISJLiKwgbkFEMkRyMxZQXVFcK5XmNG2zqTyniFMZ31YvaIaJB8kE9Zh4RDNwTRHfSLu1ZkTLCCEnhCZIB9dcxncWxC+3aawv0L5s0RYOd2gdapKtN9AljzYRctDaxomYFUgcRWIZCSFCFSFGYoxEG2fsgmWOPHNkuSCZopkiWSoOOgENEEcQelBsljY4O2J4bkD/TJf+mS7Ds12K7S5hsCVCD+9HpjpApERiiVVDs2qIhQKxyrDCsDIVKyyYhcqIJRarQKwiMZhZVZnFMqUJYdeMoVi8iMW7TKwbjbNq4UGBAVEKcVIUYVRSuRENP6JXBPKywrci+YU4TQvGWMFHx2nBZ8MutEsO4HOf1/O/4X0/E7BP4bjj4K5n2MyotEGuOYEGqh2UhhPtYLKiTpcM22/mDiKyKOgKSktwGWKNiHYQbYmoJ4F3LcUtidMMcePyHOB0kr+rR1wuojmmHiTDJAfXQHxT8E3QFjE2iKGJ6DKuvSyNA+vWOrIkS1ct07lygeZlLbIVyJrJyKsCqp2SwcaA/tk+o3O7FBtdio0eodsl9odU/SFh0CcWRZ2uV1gN2MXEDEqFBu9Q53DeI3mOa7dxrSZ+cYHW2iLN9QXy9QUa+xdorLfJVjuWLYDLkaqCwQYMTlf0ntil++g2g6e7DE9ftKq7BXEbkR1EBgilEUZQDYHSJIwEq1IKEUZGLA3KOJMyJCDRYmEWzWIYYvaIWexhPCVqHwqx6kpkR0K1LT4MRegVgzDAU1C4Eb6o8I3ELnRnbT41eFYn8H9EWvClwgT8TMJ/Pn1Zrw77D5/KGHU8RS8na7WpYguXN7xaC/UNM11WiftNZdnM7wM7KKLLiC6ZaBsRX8fiTdBcRLyoeBGfq2obdS5V8VQEj4kqmoHLBM0RbSCuKaYNcDn4BpEOIS5guoTvrNHctyKdK9ZZvHGd5esX6FzmyDpACf3NwPD0Dv2Tuwyf2mB4coPu6fMUG5vJwEcDrBwiViEaEUogcXdEI4glTJKYtn2TVNMXROpKQALxNPlLAzMP0ZlJIguKy9Csibba+IVFGvtWaB5ek9bl63SuWKd9xSr5gTa+maKF3inYeWjXdh/cYvfhcwxOn6XqXUDp4bORiYyQaghhALHAqgFYmQDFWBghYQtQRkJVgYUY4girzlkMXcGejDF+BGI/mm0K4YJgQ6nCrpj0Ci8FsRwmoLAX0SyizqYRQSfC/linBJ8rfOBLJm2Q5+Ax2GdxEuUPzvVn6bsXHOvNjLKXUbmcLD+EsOhEOqZuP9giqsuCWxeRNZBDhu4jlewaiXKrXkSzGt13afd3HnUikowfcWKTPD5Phu+a4JsirgW+SaRFCAuYrpKvHmTh6oPsv2VdVm5conU0J7ah6MPodJf+I+fYfeic9Z84K4MzFyi3drDRLhZ6QIlIhbqYDF7ieB9LpbsYJsYuqYZPXecjsXjj+AxKXXI0I5UeReqgqS45irpUZpTaMaDpLSpJgKM5JGvjllZp7t9P55qjsnjTZbSvO8TCsWUaCyl16D5WsPXJi2zct2Xdxy9S7p7FySbedyEOoBwY1QALQyMWWBgmIDGWiMRIrKKFMsRYDY1YEeMOMZwwi30hnjILjxGtaxLPCHaRYJX4OBCsX1g1JPohQyvRXsQ3A34zkue1IxiXDz866wg+W3LRs21mz9mOweeyA/ijHv8sdz/l/GMWXz9rEHJPFlsuZlfj4qqRraAcFXMrqC2J6LKIrJq4/aCrgENVRcSJOi84B6qioskheJ2Ad5oL4jFtGK4haBNcA80XMNeWULUxVslWj7B47RFZvXUfKy9YoXMUMoXB6YLN+8+y8YkT7Dx4ktGZs1Q7F6AamrqIuoA4Q6UCm4bx9S4vWCBt6ZNSHVDv+pNLMNYGHOsSHyAqqXwyCQcws9oJSF2JGBOLtL4vVShMtHYOgqGE6LBSicGDb4nrrNG67AgrN17F8s3HWLjpEK3LmkiA3cdh62MbbH7iPDuPn7XQPYXqFqp9o+pB2UsOIAwhjhArjVgZVkRCGSwGM0IZYrVNCCMhnCbEh5DYtWgnzKpTCBUWh4INqrLq4WSDWA7wzYpBqHCDgMsjjZ3ASWcwixHcF/c4gD9qSnDJAXwWx2GfO+MfCGx69ldKtegYhRa5NJDmohOuM9H9qK6K6kGMFUn5/pKoLkZkXUSXRERRVVARnEN9Mn71inkV51U0B5cLkmOuibk2krUQ3yaEllSski1fzvLxo6y8cD8rL1qV9lEIJfSe2GX7EyfZ+dgJdh97gmLjrFHs4rJC1EcTQjolsUp1eEsUfbEy7eK1sYvFGvGLM6F8nDD2RKQO++sooI6vRGz+tNW1f5s9pULNLIRxhIAo1HyF5CBq51BXMEQ9pp5oSAwOCw2MJo21QyzedB3rtx1n5dYraF3VgQCbj0a78IGLbN71FINTTxvlebzfQRlhxS5W9SEOkTi02hEEC4VZLKsYir7FUFqsNsTiU0boSgwnLcYTUaSHxYIYSsEGUpWnK41bVDIik4JBVaGDQN6p8JupbHiyWXcfzuED/BGiged8WiBfhPezz8JDCn+4cMezdO3dqLDjWB94iv05oZ+TNVo4O+DMLRusm3Az6ldV3II4t2LGgqCLqC6h0lDRFkgjcXYkEXTEgXhFspqwkwmuUZfrWmK+I5J3wLUIcQVzh6R58DIO3X6Ewy9bp3V1zhDYfXiH7Y8+bZsfe0R6Dz9KtXMOGJrLCxGpkBhMYs2VsZTHJzZeMvax0U9+mDX28e36JnF6ao0kGGKMt/e04T+DPTFz2sekXpH0kHEUIDVOUDuFdLuOFup0QcRh4kgOMiMiQvCEKjezBo3Vw7Jww/Us334t67dfTfvyNmELLn58l3PvO8f2faet7D6J91s4+ljZhbJnEvuYlZEwhLLAbBSwEC0UhcXQsxgqrDqPxVNmcYjFrsXYxaotgwc0VKcEuqWylaKBqmLQqiYYgd+KnM0iNGNyApO0wP43I4FLGMBn4Rg+E2DwmUDfeNdfbWWUg4ys3aKKrUx1MTo9pur2mem6CTcauoK4lni3BNoUpCPi2qKSibgcHaP6rm63d4J4Sbl9hrim4FqYa0LWEcuWiOUqZIdl6XlXcfA1l7H/JYu09sHgTMXFu05y5s4H2bnvQat2z6M6EOdHKCMIwWIcCbFErDQhIOOGvHEuH+Mkpzezeqc2k7EK0Oz1WfN5koOYPYn2DLrUbIJgjF3D/OlPT5FJf0Hd44BhUtONmfQijdmLsw4hOU+QDHEN0AwzTyg9MTalefAYay++hfVXP5/lWw4iLRg+HDh75zk7/6GnGJ4/ibpzKLtI0cOqvlnVhzBA4siw0ghFFa0sLVTBYtk1C5tqYWQx7lqstjHbQsKnrLKTEHdV9ExJ1aWiIJOCfhHQQcA3QwIKNyOn8whjfGDiBOIlB/DFwwXk2cG+GxV6CoUm4+/n5Is5ZdXJ1HWidysqerXBIRNdF9w1lur5bRFdMNVcRRoiriGiHnW5iHNQE3JcqtubZiJal+38ApK3BW1TVmvowjFWb76GtVcflvUXtcgb0Htwh1PveICLH7qb0fknUfrmfJEQ+lhAKCAOsTiqu2/TTp9Y/DGF/jNhvdT/GmZp57Ya5CNlKVbbfdrt059nTHjvKZ25d+yERebOtMy4hnq3r5uOxs+bTRfG9OW6AWlKZ0anjqAmOYmkakhiLKrEKkf8KgvXPo+Dr7uN/a8+TvOwZ+cp7Oy7LnDugycYPPk4ykWc7kK5i5V9k9A3Qh9iWVkYVVhhMVbDGIuexFBZrHpisYfFPjF8yrAnY7SuEJ9QLbdlFPuF0cVbyaAIuKJCfcTvBryfcQL3zYRYfzyiAPkSPp55499/PmeYNQhZRjPzhCIjau6zbNlM1kz0oKh/vohcLirrEd2H6IKINEVdLjiv6ib8fDRLIX5C8zGXI66BaUtMm2hjUSRbpqxWRVuXsXrbNRx9w2EWbm4yGMLuB0+z8bv3sHHPA1Tbp/C+b6qJEEMYIXGE2bA2+BKJVeLMW91pN4PcT4w//V9tj+N92vaUTWaUsO3ZuS1SG61Y3e83Y8vybN/BeNf/NK5k+lWA1ZHA1K3oFDOY9jXUTsCnHgWSIxCf2iKqqoHZKq1DV7P2yptZf/3zWbqmQ3kRztx5gbPvfpreU4+Zd6dR62GjHSP26spB3ySMzGJhxCpaqiAELAQLIUYLT2DxSWLYgPhxM84Qw7Zaea6MMqIqRmQ6ZBhL3Kgi71ToReNsM8C+8BmmAl8yzuBL1QHs2fl3HMuuQRHb5NIgek/mfBasGb2umuhBFXcU0ecLejTt/Lok6tqi2lBTh/NOnEskfM3UNEvsPG1KigCakHXAL0LWlqpaQVtXy+otV3Pkq46wcmuDqgfn33OSp3/zI/Qfvh/CBbwvTazAqmECsWxUa2qUpItzXKqrG2cYt+HbxIht2uQ/2c+pG3XBnhneM37+3tDJxk+ZMfoZ0xax+cyg7ima9k2JzSYKlvoFJw5Ipi3IMokWmKQGgmAiM6Ch1KzHPGEFoqCZ4BfNfItYCmEkkq1dwb5XvIwjf/L5LF3fZnQezr57g5O//Zj1Tz+Mzy+gsWdW9KDaRapekiWIpaXqQWHEMloIVsXqjMXqvFi1a8ZdEuNT0apNFTtdVaFLjH0m0UBVkVUloka2Gzi7WKUKwX2Bz0x9yJ7rVQH3pe0AbnRTsK/ZwrlFnLZA2l7coqlbMeSoirtC1F0u6NWmegB1CyLaEucbKj4T9V6cd5A5XC7pp4m4tuDb4DuQL4u21iSwLhVXsnrzbVz9l58vl33jfvyi59w7n+TRf/27nP6NdxEu3o/XTXO2C9VO+ok9JPQgDpAwmvLkY5maYmJqtRWzGpUbG3+6TmRieLZ3063LeTZ7V1IFkHEhz+bBkhrYl7GzSQCfTbzqGPCzGZcgk4Bi3GQ8yRDqusDkxcdYgUxwg6kDkrFzsphSHBJQmX5PnYpqQDVAbIhviljYZff+hzn/3hMMToxoXbbK2mtWWL3tiDi/n96ZjHJoog2XeBciiY05KWnWt5O3iqS2ybYIlZmoiuSGFEoicEczQ8UhpSYZFXVEhZU+9Aw4Dpy2Z25GX3pLvkSPpeb1n/Ws5jmlz5BqFc8BJ9oB3zHcoorsR3i+iFyG6qKJu0zRFioq4h3qVJxziHdIHfJrU3CJvGO+I7gFJG9jboGqPCStK67n8q+5msOvWWJkcO5/neTsr3yY7sP3msgmzo+gGkDVR+KgbnpJ4b3EOsePwaaqWyazm4nM7h6TvLvOy81suvnLHuh0EvLLXjhvHvmzSQowTRGmSMF49yYRBSHWYJ+rPYGNiwEzgODEyKdg4Hi3l4mkSJ29jEuJqiSoJQMzKcsRZVFQVUXqFxj1BYTm6uW45SuRrE2s1MpBA7dyOQde/2Vy7BtexMKVTXbuK3n8V05z7sMPm4bHyfwONtw1K7eR0IcwNAvJ8VooI5QxhlCZlWeJ4SIxXogWPmoWzhLDthjnhapXWexjYUAZKrwOKHtDfLNiM4zgRMVnTh9+zqYC7nNovPIFMv66DrWqHO47RpYTYkauS07ydZAlU1lTdN1E94u4YyKybuoWRXVRxeWimYpzTpx3aK5opuJagmsLvoVkHSxbFslX0c4awY6IdG6Qo197G9f9reMsv6DB5l0bPPxT7+XMr76TcuMBy7JtJO5CsQthB2IfwghigcQi5f5xppyX2m8mobiMDZ49dju16Gc/XTZrf2nTs7GBTsE7mYn9GW/M41eY//Jk7vckGmCylxgwjhIm0cLU/msnMA7/awlCVZz3qc8AIYTIcDigf/Ecw/MnJVQDVpcXOXL4iFx55TVy7IprWV5c4PzZJ7HBBmkfV1zTsNBj576H5MJ7n6Tstlh40UEOvXaFhcsOSv/UCsOLJZpVqf/CBFERSFyumvMooihIkYgMZGaMRCwiUolJgUXvcOJUQsQJMRp5yygEFjNjsGpwFXCaL+Uln+PXsC+A8U9D/9U8Z+gWyGIL1cPq5ErErYrpPkTWxLk1Qa6MuFVx0na4RVHvcU5xXtKun6uJV3EtxLUkkXgWRVorRFklcJT1F14vx77uKIs3Z3QfH9mJ//RRzt35QaQ6i89HaZepEsicAL6i5q/XiH6sxTEmNhUn0f1eYzeblvNkT3XU+PTCB3UuLuMood52p4F77QMkpREyzSXSlj551GRnnx6LzAQfc8a9JyGQmhUoTms1tJTrh1AxGg0Z9fsw7IMFWVxe4NjRI9x4/TX2khfdIp860WN7CA0vnD1ziuGgy+HLriJWI3771/4LkuWWrV6DaTtVZXxOqESqYoWF617Ild/0ava99gijDTj1K+fs5G89gA0fw/uLUHSRqiYThb5R9c3iKFooejGWI0IYYdVJi9UZw05KDA9Hi32JZY/Aprg4EIpuObIuLpZUeY+F7UGqDjwjEviSigC+mA7gs53GMwX9Dl7MKDqeYdmhma17cytR3dUmHFfRNVG3H9FVUV02dJ+oa6ed3+WJyedrnn5q5zeXi2hLUq6/KtpaIcSDZPtukCNfcyWXvX6JOIKn/udDnPqV99jo4uNkzT4SR1D2EOunzpeas66hNEs5rUjNzpuw6yYAve1xAGMx3nHkb89yFZnI3DmWPVu2TFOHZ1QB/pCvyOpK/4yBT/8Ua39U/0118qYJtBSxUBHKEaEaUVUVVlZQVYCaNDMO71+X6689zotvvZk7bn8JN1x/nEOHj9DpLJBnylOnzvN/fev3cf+DD7K80MbMrKhKufX21/PwvXdxz8d/n86h60zyJWKtZC7agqxNFVqYOywHX/XlHPuml9K8qsnGx0Y88UtPs/PgvZblTyPVDhTbUG2bVV2Tqg+xiBaLaFUVgpW7FqoBMZwX4t0W43mzcodou2A7QjinFi6UQYZIuY3FXfxu4PxyCY+ECZjxmYOC8lxwFM91B/AsRJ8LjrUs5f0al7xyxNTtF+QaE7lKRNZQ3S/ilkxcS8SvqnMtEZeJc94kUxEvuAamTZHE2RfzC0hzRcyvU4XL2XfbjXLFN11B80pl56PbPPELH2T73o9Y7i4kemo1xGIfif2aq15OSDtiwVJkPeWLjIE6mzO4hADI7L5un/ZUClMU30RkPqSfggM2YQLOYwrzrzuXbsg0DRGVPelD2tknzMGkIBKrEeVoSDHoYsO+EEoQJVtcYXV5iUPri9xw48287Mtfys0338y1x6/h0MHDuDrpHIwqhsPaWZixvLTAyVNn+Wvf/hYefuxJ1leXbVQMpdleptVq88H3/E9aa5eZa62lT6V1O7W2wLcxbVKVTWnsfx6H/sxrOfp1x2EIj/+Xi5z6rfuR+BDOLhrDLazcMQk9oxqZ2cgIwxCroleTiLbFwt0Wq00s7pjYJiH2iXZapTxVWexTxk2i7pBVJe1hycmDVV0ejJ8lHiB/3BzAZ/MhPlMHUP8cd0nIo5nhyjZFo+U1HIrib0D1sIgeFfSgIItRdAXnO+p8U8QvqbocVSfinaWwn5rBJ+Lbgl8UaS5TxYPoyvVy7E3P49BXrRGG8OQvfYLTv3knFCctz4ZY0YfQR2yAhVFN5ClmcnszGbfbjsHxiV3HeYivRsXk0xs9kyp9HTDUdmsiz34uZepapg5gjt6z1ymYyKRWL6ntYQLeWWIfWiSEktFwQDHow7ALsRBU6Syvse/QMQ5ffg3rh46SNZfROOQHv/MvceMN1wFQVDAaDanKihADBihCGmyUjqYsKxqNJltbW3zH9/4IH/3kvawstimqioNHruGDv/friG9avrgvpTPiaqnFDNMmSAvJmhLICcUaB17xGq78m6+keXWDC+8Z8NjPP8DwzAPk2Vmz4RaUO0bZM0LfLA7MqtHI4ihYVQ6x6gms2rUYN4x4mhj7ZtVJIT4qZr0qsAm6y6hX0NBhcgJLYaZ56A8SF3lOpQNfjDLgZwIYyjNq/Qe3MoatjCK2UL/ulbWoeq2If5Gou0rUHRV1+0zdioguiPqOOr8g6puo94JXNFdxTcG3hKwtki1CtizaWpeiOEz72hfJ9X/nFg69ZoGtu3d58J++iwu/924yTpmjC8UOhB0k9rAwGIN8NkH5pWbvTXZfm4Dqc199Xc6rAbQaOn+2FLKuqU02bZuF2sYkfnm2TWZC3R3X6fYQ/FKlTEUliYB473HOoxIhlJTDPoPdLelfPC/DzSel2N2UrNGUfQcOyZXX3SQ3vvhV3PLlX8nxF7ycg8eux+Utti5ucOrJR+yxRx/hzt//mNz2ghtYXl5hc3O7ViGuHcy0CDnuT0IEiqJgabHDm77yNdx774Pc98BDNHLPyr7DbF44S6+3S97s1JFSrSk4cbwhdVeJkTUj3cdPyMUPnqGxcpC1V62xcvMBhmeX6J4s0UYgDVJJaRqpFuhU8Gk2gzXNWBaxJkaJSoMoQ4FthOiREBkJTVWGFoneWNmB7jGD819SIOAXywF8lmH/tqPRyqiyjCy2MseSoUuKO4LqlSLaTiG/LqOuifiWONdQ8V5cloFX0VzEtwTfEbK2kC2LNFeFfF3K6hoOvOoOueZbryU74Dj13x7nkZ/+TYozHyfLuka5g4StVMuPw7pFtSAp16SLT8XqgnMN4NkUMZ8W9G0WPZuF8vbQ720mt5/EALNNvXuetQfxn9QDxrau49xeVFVUnag6caoogVgMGPZ26G6el8H50zLcPi1ld0sajYzrn3cdX/mVX8kL73gdx258Gc+/9Q5aywcZlYEL589w+uQTnDn5GBfPnWLQ3bFYBZqNnItbW/KOd97Ji15wA9cev4p+f5hExuM8qIkZMU0UQUUpyhIRx1e89hU88fhTfOLeBzly9EqG3S6b506Stzr1k+OETzDphpSaTBUiPoeyv8n5Ox+lPO9l9Y5jHH7lCoRV2XygAh2gGtNpUiSNVRgTntQw87VQyqh2WDuGbYgoWAxRJRI8uBCo8og5GISwpzIgn4eK2Zc0D+CzDPtvEzjrWWtmFDTIJUdk0Ys7YKbLIlxjqjeL+FVRPWIiB0R8Q9Q1RFwm6hySe1yecn3XFrIOZItIY1kC+5DGdVz5p1/AoTetSf9syWM/8wHb+ND7yPMtpOpixQ4Su1gYJnTfqmk9n2gTdJ+ZXX/cWl/H6jKpwu+N9e3ZSWGT7XsP1m/PwP9qON+YlvlkZtPXOn9PBkNMityxHDEadIn9XlL0IIDPuezIIZ7//Odz24tu49YX3szzn38TR48cYWFxgZ3eiL/2HT/EBz/yCRY7OSFUOO9RUWIMhKokVJWlMYERJ8JgMKCZZ/zcv/pRvuzLXsLZcxcl834mV7Ga9TxDdzIjhIA4T7vV4Pve9mO8+wN302nkfPwD77D22mG00amdndbudkwvzjCXp4YjXQDfIWqDcrDMyq13yDXf/hUsX9/hzDt2efDn7rbYu48suwCjXSh3I6GHVf1g1XBo1TCYFYXFsEsoy2j2oMbyA9HCUCKnhHCuMhugbGKhzzCWNHVEu1clcZHPSEvALjmAZ/9bXeu/IgF+BQ0ya+Lypre4bKIHDF1S0Stw/nmIriN6TMQdFHWZiMvFZQ5xziQX8SnXN7cA+Yq49jJFuZ9s/81c99duYv2OFpt37chDP/1O6z/xcRqNHowSe4/QgyRdX7P1woS1Vl+4NoXj4gzpbYaxV9tn6tWZueJnH7AnkZ+m6tMg4pmF/7n6XQ3Uae0aAhYCoSwoBt2k4BuqelBwn0arw7XXHufW217Kl91+O7e84EauueZa9q3vGwN1NiwCo+GIUVFIp91gZ3fAt775R/no3fezvrpIURTEkOS+qyrR7YkxUZxCQJ1SlIHce37qx79PXv2qV7CxsYn3fpLSxLqzccpfSsFSjBHvPa1Gzo/8xE/zP3/nfTxyz3stay/TWFyvdQ9keubqHoOkxuRrenEjaTPky5RFh+bBG+S6v/u17HvNATY/MuL+f/WgDU9/irxxHutvmJU7RuhGql5hYVRZNTLiyCxEZ1Y+aSG8nxh6JuFxIT4ulfQqP7qA0KUIFdVgQL5Qcn4nwDMwgf+jHcAfAfGvy33DRk4pDdrtRSptex+XTfxBFbccRa8U0etQXUL0ClW3X1AnPstwDQdemQB9C5hfEWmvUpSXsXjtC+V533INzSszTv63J3nyl96FDR81Z9tQ7CDWTTp1IQ3ElXoghozVdiY02tndfG9ZbQadH9u11eO1niV9l2fh4sizRAgTVH7sOyxiaXeXaorMQxwC0FhYYd/hY+w/chX79+3jL3/D63nRrbdw+bEr6LSbAFQBBsNhbdSxJu+4MUYnIRp5njMaFfy9f/hPeN+HP8nKUovRcEgIyfgtppJn+jeBkE6VoiixWMk/+ydv5Wv+xBu4cGEDl8aSYbOdy3OBjKTBwiIsLXT4uf/wn/ne/+e7TRpNmqtHZkuQ4zFHtUNI8mVJddmDb2HSgcYKURYEfyXX/pWv4uifu4b+ycj9//yEbd7/IbLsrFn/IoTtaEW3lNgLVg2NMDJiJSGWZwjVRyyGbZH4uMX4GJEdtep8KWGXMpZUoU9uBb4ROduvakGR+FwGBd1zxAHsMf7jDnY8eTujlAZZo4W5RZ+xYOIPiOo1Jv4KEb1cRPeL6JI6ty7iFsRlDpc7JFd8O+X8flHIVkXa6xThOPu/7CXc8O3XSLbqeOynP8WT/79fN88TuLiLFNtI3KmNP+nRaT0UV8Zl3llUb5pzP5uDl2cy+mZoejanyzHpj0nJ+5hMk4AzVY+qS0CdChIKwrDPoLvFYOM8xeZZKXfPE6qC1dUVrrzuJm687ZW8+JV/gttf+3Vcc+NL6awcZKdXsL6+n6//mtdSloGt7S69/pCiGE2iFHU6UxkcQ4hCWVY0Gw3e9MZX8tAjJ/jEPffTauaEKiQBIktS4xbjJEgJMeC8YCLya+94F0cP7efWW29hZ7eXjNzGEwfH0cC8RUQzBsOCF77gBn7tf/4PLpw+TWNpHVGdRg7CDDlp3EkZJ92UIobEmMYt0uPcBx6lupjL+isvZ+2OVRmeWmL3qaG4VlX3PljyJJgbFyxqTKCjIofF2IkiT4sZaBzEQEmm4CVQesMGsNKDrjc4JnD6D5pdIf+nO4A9J+e453DPkbUySs3JaaJ0PH7J1JZM3GHE3STqLhN1+0R12VQXRbMlcVlLtOFwU+M3vyTka2hzH0U4zmVf8WJ53t86KqGEB/7p73Pu3b9No3EOKXag2obYw6oBEpPxi40ZfTbh2UxbdWdDdpuhxtozggFJlNppfl5L8di4Ty51sCS1MedQ50WdF6cqYlGsGkk52JXe1nkZnntKhtunpehtSOadXH7ltfK8F97OrS97A3e8+k9y00tfz8FjN+DyRbZ3ejz1+OM89vADnH36BFXR58Mfu5dHHn+KV73sxcmQMHQi9llnCUyI+3MkobKsMBHe+NqX8dhjJ/jEPQ+wuNCWEEJKB+roaNqabIQYUU0jD3/jt97N/vVVefGLX0S3269f3lK1cWZLTI4k6ZkMRwXNVpuPfvQueeDeT9FcXEdcVqcBzEQAe1iV80NNISawULOCi3c/TvGUl/Uvv5J9L1+WuLsmO48McdlQzEICdAWRxOdQwZyYtBBbN2NDxR6JaVBrz4mVMahRBSMHCgVtRfb3jO1SPgPKsPxxdwB/0IeUac5/m8K2I29nVIOMPG+gvuPxy+Z0HckWULlMRW8U0UVxbsHELSO+rc63cc1MNJex8YtfhGxFpLlGWR2XY1//UrnmLx+kf6bg/h99F1sffy95ZxMGddde7I4VZmpW37hVdxxh2rStZjYQSBeazIlvzoXs07hBZ6B7qcN5dV6cy3DqUhkuVoSiL8PdDXoXzjC8+CSjnXNU/R7Liw1uvvlGvuZr38T/9Vf+BgevfzlXXP8SltYvo9svOX36DCdPPMqZpx5n48JZhv0uFivUZebzHEBazYxP3P0ADzz0BG94zR3kecaoLGuSj82AczMiwzO7c1lWRIM/8cZXc+HcBe76+D20W7mEqiLaNO2dGGAaIowK+Czjt971HlYX23LH7S9hZ7c3yfvHUEeMkaqsqEIgxmiIcPDAAdnd2eIdv/Fr4puLaNbEYpyjLk9apGXqfMYVg/QNRJK8GmRtx86J09J9aMTKi46x/oolqt6KbN4/wGVDiHEGTR17RImCKRIvGpwRE2cWh4grHSZRQ6QCGgrVICIjY70eQsWxsRMwnkNVgC90GfDTqPncppB7Dm95smZOYU1cawEnB53pQdD9Jm4/qvtV3XWGex5OV1FdE+eXVbMmmjdwDadZO+X8bhFprIm09lHGG+Tyb3wJx/78PnYfHXL/j/4OvUfeT55vYr0dk7ANoZt2/TBKHXxxXF/eG99P6vt7U4FxyX+WrSu15EYNz9VUW3Up1BYVURErC6r+Fv3NswzOn5bR9ikpdi9iIXD1VUd59atfzTd/8zfz99/89/meN7+Zb/nWb+Pr/tSf5I6XvIj15Q4/8+9+nidPPEYx6FFVBU4V7/1cJ16sDQsqqUKg1cp58JEnuP+BR3n9K28nz3OGo9GEdjBG58eGH2MyYqsjoRgiIUbe+PpXsHlxk9+/62O0202xGgSEmeonUx6DquDzjN9+13vIVOT2229nMBgSo9U5P6g6Gq0WiwsLtFotQlVw+ukn5dd/9Vf4+Mc/Zr65gGu00/vMKBXbZO7BNFjD5vUSbTrWHNcM0n3qaXY+uc3yLdew/upldLQqG/eNcM1R2vxTubEeoW5O0pCElggHBLvS0EbSUpdFNTpOWIi4HOeHFKtGEKHfqy+Y8/ZciwLki/ieM4M664k9Rcencl+zCWHZOb3c8MsOFsy5dRO/Kui1OL0JdS1V1xJ8LtpwljUc2lL1LcEtijVWRVr7Ke0mjv3ZW+XQ1y+x+8kej/6zd1Je+DiZbJsNtiDuQGoZTR17FmrJ7TjHmjOR+a68maadSevtLJ9nFsYbj9+OAQulhGJAOegRikEa00UAG7G4tMKNz7+ZF97yAl7y4tu45QUv5Kqrr2R1bbVmyyVG3Wg0SsAbsG9tjfd/6GN851v+OQFoZI6yLGuEfVqiDFHq/oQgaZeP5HnGzk6PW264hn/xo9/LwsIC/cEANy4DmM3LC9r81zgGQhc6Lf7J//uv+Xe/8F9YXlogVKVZtMkuPH4dkZTTp1MW2Ly4Kd/5bX+Tb//2b6MMkcwpw9GIC+fP8dgjD3Hf/ffxqU9+3O695z6efOopBt0dcBmtfcfwrSUshhkNQtnTAT3bWCVT9qDW05hcG3NtIV+gLJdZuOp2bvyHX03z6hZP/McLnPjvH7G88Rg2uGBWbEDZNaqeiQ0tVsNBjOUusRwRq08R46ewuB1j2IXYFbONUIweogo93KiiXRSc7oRnAQW/6JUBeW5EAEczVvOcKvf4so26jje3HNVd4dStmMmSqTuoqos4f8yQ60V8W13WEM0yfFNxTYe2VfyCWL4i0tpHEZ7H1X/+djn8Z5a4+JFdHv3xX6faupdMd2C0axJ2IPSwuQ6+CBZq/b0/iPBsk9ByPAFcVCc4AGbEqiCUhYRiaOWwiw22U80dpbW4ytqBo6wfOsbyygp//c9/NS9/2ctYX19jabHNqErMuOFwNDFoEcE5ndnlhCoEVleW+PinHuC7/tE/Z3t3hzz3iWM/vvjj2AZDuhHNLM0EI/Oeje0duenaK/mXP/H9rK0u0+sPapSeyY5pz9ASTRFyCBEzWFtZ5J/91L/lX/yb/8Dy0iIxVU1s4gRichlxputQ1clut8ff+st/lhuuvYIPfugu7r37kzz48EOcP3sOrLLJHpG1cOpwrQXypQPz0OpebYKZzspJ5WUy6MRhmuTdcE1MFtHWCpWtSPvoC3neP/xqsivanPiPF3j6Vz9iWfYwNrhgFJtQ7ZqEoVkYjmIY9GIoCgnhIax6IBK3JYZzMYYdoboQgj6IFLuMKGj5Ec3tMDOF6DlTGnRfIKOXZyL945y/41jzqdSXWwPTNe/cmsF+FXfMVA+hsh+VVRFdFfWHUL9PXZaL+hzXyMQ1nPiWStYR8lWR1hqj8lqOfd1tctVfWKV7T5+Hfuw3qS5+yjw7yGgLwjZUXSSMkFq0YyzNZTbTjjvVwJxMzJmAfaLponSaZoeEUuKwK0V3i/6FkxTbT0vVu0gMFQudDpdddZ1c/8KXc+NLXsvxm7+MtYNXgChnzp3j4kbXvuZPvEGcc5w+u8FwOKQsS1IKLJP3m/iemZC63x9y1RWXcfuLbuKd7/kAWzu7NDJPjAGL0SykdMYmwFrtvKJRhUCrkfPUqTO8/4Mf47WvuF2WlpcSc6/uLo5xvsFwPE9wWsOH/mDEy7/8dhTjd9/zPrKsrlZIqmA4l6GqmMWam7DLoLdDOexx5++8g1/9lbfz0bs+xJNPPkG/O0TyBlmjjcs7+NYCWWuJbGGVrLPCZMLRzO4/G5/NSJxNIoIxbjOG+Ov6YcIFguG8MdzYYOvjW6zeclz2v2aZuLssG/f38K2REE2o1ZpEoohFL0JuQp6mQ7Mu0BehsCjRi16MVVXR9JFYVejQ2BFgJHAjcMKeC5uy+yJEGTN5/7ZyuOcYthvksYnLW97imomum7o1RC9DZJ/glwS3LE6XUbdPNFsRyTLxDY/kTlxL8Ati+Rq6cEBG4VqOftWL5Kq/fIDeQ0Pu+9Hfpjz/CfOyC8VWKvNVqYV3GvbXgB9jDq3NDM+QujzmUOdxKohFkaqQarAr/a2LMjx/ktHOKcreJiGYHDl2hRy9+gauv/VVXP/CV3DkmptZXL+Moqjk3NlTPPX4w5w68Qgb5542CyUPP3qCD3zwo7zmlV/G4tKylGU5KXWNa+oprJ9mH5MdWoRub8DhQ/u5/bYX8J47P8DWbpdGnlFVVY3O18Y6rtXX48HMEmmn2cw5deY8733fh3j1y14sK2tr9Pt9QGqEfhzK2+S1QgiEBNRhQH9U8hWvey2HD6zxobs+hs8aEkNg2Nuhv3OR7sYZ2b14WnpbZ2XQ26EY9qhGA9QL6hv4vI1vLiT4p7mAayzgmov45hKu0UScS8Df3p1fZIY1MW4wkmdcgVN5MpMpSFmTukLAZTDa2mH7noHsu+1q9r1yifLMomw/2pesVWAxSiorjmnIiYhpgirkmG2g0hckQHkh4ksCERuW6Lqxsi3JCegfRhf+Y+sAZqKBVU21/o4n0xaatbIqtE2yNfPZmorfp6IHUVnBuSURXRP1q2i24jTriG84XHNi/OQraGe/FNUVHHj5i7n2bxyS/qmCe//x7zA69RHLZAsptkiAX79G+au6oSQy38lZ7y+aSnJOJE2uLfoU3S1650/JaONJit0LVIMtOu0mN910Pa973VfwLd/yLbz6DV/Hma7SbC+ztbnB6ZOPc/bpx7lw+il2ty9QjIYiIrgsm7DiOp0GJ0+d5s4738erXnEHa+vrMugP6tx2CsZNw3GbQezT8XZ7fQ4d2Mcr7ngR7/v9D3N+c4tGnsm45TY5khkgzKZ5eqgjgbPnL/Ke3/t97njpC2Xf/gP0ewNEpQbpAlVVpRHjouR5g3arw8JCJ0Fc587y0Y98iPNnnuITH/kQJ088ys7WOXpb5xj1tqQq+sQY6qjAIT5DsxzNW7jmAtpcxDUXcXkL8fkEOxlXE2JKt2zSWTQ/p2AGc5lvlphgEDV72tiD35AGp1oE31BGGxdl576C5TuuZvXLlhk80ab79C6+UWJVkmkXqR1HJFgidgliQ0ltIBlRznjTEGVoVI0hUhiyQnICgbo0aF9sJyBfYOOvE7EblcObnn6eU7mcjlvIkAWzbCGaO6bqjphySEQOg66J6HoiAGVN9VlLXN7At1RcW8R1xPJVkfa6VPEwnZvu4Ka/d7XEInLvD7/HuvffSe42YLQJYRsreynfH6v1MDNdZ5ZUEgKx6FL1dyl3N4Fy8kEOHzkqL7j5+bzottu49dYXcfza61hZO4BvtFBJCjj/+Ed+jJ//pf/G+v79mAUbz8+bphh171CiFRtmZFnGzm6Xqy+/jH//0/+cA4cOSbfbZcqfnyLc05b9GSUhgRACS0uLnDl1mm/57rfx2MnTLHZaVGVZ18ZkkhfbnPBoIvJ4VXa7PQ4dWJV/8WM/wLErrqTf79Fqtch8hjgHFunu7vDUUyd46IH7ufeee/jkJz/BQw8/wsWLm4lEVffso2JpjqqKOIe4HPG5qcvqOQEyVSedYznXCkZjFWGdUR2ayIvP1l1lKpci0/mGM2XaqRyDjDdvAVxSf3YNMd8CWUCaKxTFCssvepVc95Y3olXkgR95nJ1HPmjen8b6G0i5aVRds6o3jNVwZKEKRrVjsRoQw64Rfl5i9bhY2KosnmAoBa4XcXlMA0dWqz3qws+GAdiXsgOQT1/vP66su5xh2UpEH9/2ZItmuizeHxeVKw05ALomqkuGrov6A+qzXDXPLWt6fEdFO0K2JNo5KJUeJj94szzvu28kW/E8+KMftK2PvpvcX8AGF5FqCwu9WpG3nBmrVdeSx9eGy4ijXYqNpwnDXRHgBS+4mZtvfj43v+BWbr7lVo5dcTUra+uo8wxGJd1ul15vgGF4l8QulxYW+Lf/9mf5uV96O0uLbQtVmWrkY6Q6TdiVCfWmNkTvPf3egGOXHeJf/4sfl2NXXsHO9k5yAnubBWSPlNeYPRcCnYUOZ8+c5+9891t5+MRTLC8uWFmU890Hkwhg6gCs5uAPykJWFtr85I//MDfdcC1nzpzikUcf4Z677+buT32Ke++5mxNPPkUx2J0ekzbQzOPUpa4IUdTnJlmOqJ+RDR+X58McHVJE5hokk33PSo3XDU46kRWXaXVGMJltgZb68820as3LpU32IxGH1SPczbVAl9DmMkWxTw68/qs5/ubbKU9V3PND9zO68HFzdhYGF8zKLbNqp6IaFBaKYKHsWyxLsziQGH7JrLpXKrlQheHjNPyIfhHwRTWlCt9awdv/WDqAT0d3TCW/wz1Hb7WJG3VwWSuDxeh02fD7BX+ziF4pIqsIS+Ca5tyy03xdXJ7jmw7fUvELIm5Bae6H5iGx1gu48dtvlvYNDR75F5+0s7/1GzTbG1j/IlZuGVUX4kDEKsMqmZunN2bvqaPavUix8aSA8bxb7uD2V3wFX/H613H98Wu46oojnDt/kd1ul7IYWYwmzmmiztYfUTWRacpg7Ftb4ed+9j/wb/7Df2JxoU2MlY3R/HFt0Wb1weqAN/eOXn/A4f37+emf/DG54qqr2dlJTmCiwz/7OuPeX5mW5qoq0Gw16e7s8B1///vtUw88zOrKMmVR1gDieDy4Ik5FUNM65I4xYFZKt9tjuZ1z2XqT+x+4P+3usaiP1Bt5jq9nqZg6EXWg3tRliMtmtAVtMrS01kKwMQY37upD5uVIGXcxonNEH5Ex1z9JKtgEAJytBIxPypyo+hi/MJnjKIyf60AyzDWSFLwuou01yvKQXP7n3sTRv/F8du4acd+PfwIN9xrFeWN4zig3o5W9QBxVFoaDWJWFWRgSq9/SWN0do51yzh4uhkWPzEqK7gjfDKlr8MpqpiLwbE7g8+4A3BfQ+JOY5+Edz7CV4aWN6AIiC6J+3ZDDKnpM0BtE9UgUXTXRVVG3qM4vqs87oo1MXEvxbdF8USRfE22tSRmPy/FvfoHse0WbJ//TE5z6779Oo3kOG2wj1U7SfwtDIE3gqcPuCRwkgKgj9rcYXTwhS6tr/IW/+4+5/Prb7fETp/m9939Ifvt/vZ/tbperjl3GaDRMeLBNSTOpHGaEEAkx3dnt9bnjjtvJ1Hjv+36fvNGsK4RThppNY1EZtxPHGGk0G2xu7/Cud9/Jl73kVjl4+DDdbh9VrWvpY0CwBgeZIvMhpDy7KEpanQ6vfeWXyafuvlcee/JpaTQzAatFkTNEVYxIWQxk2N2ku3WOrQsn2Tr3lIx6W2ycO82TTzzMYFDiGi18cwHfSEDdGLDT5gIub6NZC/EZJAn+eo7RZLrolD4wm2pNaVIzhqwzUmTjkJ+p5PiUdFU7wTmOdS1HPt9O9Qy3IJ9mw431pEMxiODyyManzki2cIQDb9hH5pc5d9cI19gVC4ZYEMRUCI6YFJSTbFMcGHgRc4ZuO62II2dEKfEEsjKy7Q2uA07MHpF9gSL0L4YDWHa0FjzV0EOzhY8LPkgTs1XE78Nkv+AuF5VV1DVFXEvU5erypro8xzUE1xRcJ4F+zVUdFZdz+KtfLJf96VXO/u4mj/7bd+Ddk1jd1CPluCuumAB+aTZEatkdS+IJUFx4AiHIN/ytH6RfKB973zulu3ORGEYy6m/zoQ98yBaXFnne9dfR7faFml03ZrGFGCXEKJNuuGjs7va49UUvopVnfPDDd5E1cmTMdZ+lGsi8iGeMkUaesdPt8bv/6/fktluez5GjR+n1e0lYY1xht2mdvQoJpKuqhMyrc4RorO/bzze86au4/4EHefTxJ8kURsMuvZ0LsnPhabbOn2T34hl6uxcZ9XcJVcVYb8hlGb65iG8t4hoLaLODNmqD941a+dfM6vM64UZM9966djqlREpt3JOcnunkYVFlRsBkbNwiKuhEm1DGddiZqEFqqUTZgyGwh5w5zxyYH6M2HspSOy4xLArqS9n6xCadq67h8BtWqDY7snHfFq41FIkBjVXtVqKJRTOCibGDWGVIhcbzRKkiZUVVDtEq4suYwEBlxgn8sSgDfprc/1XCFRuOIZ7Ycog1UN9S1aa5bFVN9yN+v6gcU3XLqDZFfVPE56o+lfy0pfgFyBZFG6tSVIdk+Zbb5Lq/fZTe40Pu/7HfgdGDptUOUm1B2EWqIdQz+GSc84/RsPHlpQ4rhhQ7Z+SqG27j8htexife/9u0Oh0sRjTxdvHey10f+ZjkeUOuu+46+r1eEsQIdRdcqpknpl3izmLATrfHbS9+EfvXlnnf798l6rQOR6f2YRM0enraYozkeU63P+B33vm7ctstN3L02BV0e6mTrqpC/d6BKkZElKzRoNPpTCi0F86d4YMfeD+/+sv/nQfv+wSPPHg/u1tn2bl4lmF308piIBYjqKAuE5c1cY02rrmQjL65iMs7aNZEXJYM12aabGzaISnzOuXPAH/Gof3cHIMZc5w1fBlHBboH7Z8MHJ2nlcxvmzLpqpxTX5oFTOf7OOaO2eYgleSQoo1k574Bq3dcx+rtS+ze52RwfhefR7AgklKqlF5alAilWsyQqBrlohnmnKui+D4VERrGkRDZGGcAp78oPQL+C2b8nFeKQhOdulJaIkQFE0+kbSqLIiyZyHJUXRB1mahrqPhcXO4hF7QBviXaWCXKquQHb+Tqv34lEuDRn77LyoufJPc7SLWddPvKwrA0dTd19e1NrWrgSIRYDgXg6DXP5/ypx0GhrBJvbWd7G59lNemmx7//2Z/DiLzuNa+h2+uaU5UZ5uBEblfrC9CpcPHiFl/9NW/CO8c/+af/QprtpjlVs4kjsD3gVDpzVVWR557+aGTf/g/+kfzYD72FW259ETu7uzSbzdQvpY6qHLG1cYETJx7n/gfu5767P8X9Dz7MU0+fpnvxAjBKptFcSqxCVVOXoy5DfJp+rC5DtJ7uO8Ohr2XB5+YHi8wzgqeo++yUoGcU4ueeJLNGPBkpNn8Z2Z6pQ/PjkCaCCHO6Cakih6E2Gag0fu3pIc5UCWYEUxGrZzdWiBSgfazy+FbDhmc+KY/8s31c9wOv45q/cTX3/NBQYrdA8tIIheBLhxVpvgi2bvi2ROngqqdELLcQBLEtljJhN9pcaYlXSY0H8McpAqi/sX0KlbA4VIo1T6dwxLzlIwumbgnnDwj+gKiui7rLRNxCPa8vU80dvqVkHcUvquRrSHNFKq7nmv/7Zll5QZPH/s1Ddvb3fpdGI5X7JOxCOTTq0VxiYTz6bgzCy6wMn6jDRj0Jg22ue8EdBPPsbl/Eu4zBcMALnneV/MxPvIVv/rNv4umnnuKxJ09x7333kfmM48ePy2A4SDhADFR1k0ydEhBqyq2opBLfdTdwcN8KH/zgh1CfoVJHCzwTzR8bSqgCeZ5TlBW/8853y43XHefIZYc58cRjfPSuD/HL/+O/8v/9+5/hZ/7Nv+Hn/9N/5vfe+S7uv+8eLpy7QBECrtHENzu45oKpa+CbHfGtRfGtJVyzg2SNGlmfpzhPxDun4fhc4DyNvmd3Z+b/HWcAImBazyuU8bieyQ47MeDZ15+ZMcgMC3Nm/BDzE4sn8gVj5yHPiEJkT/DAjBagPfManuAFUXA5dJ/YRDkgB77yAL6xyPm7urhsq6YFVIiEhH5YCFaPdBazcyCFCT1TzhGJ5L4iLlYc2GUaBcylAl+SEcAfsAoh7hfiyFF5T8wallkH0Y6aLIpax0SbUI/rVucU78lyh28okqv4BdHGEqPhIY78ievl4GsXOfvbF3nqN95D3jgLo6TfJ9XQLA7Hxj9Lu56T059eEFOjCyFM3GIMBSKRt33P3+GWF9xId1jxlu/9Tj765/86/eGIn//F/8SoCrzxK17PbncnkXr2oPOgmE+GEIELFy/yujd+FUsLbfnHP/bPcD7De7Vxg854vHaaZZm6Bk2NGCJCxW63y3e++XvYv9Lk8SeepOgNahqzQt7AZRluaTnl5qKY1spo6uv7JjunJQwjMMvyH1cZZkN0mR8t+CzSpGO9gFqAY+5kT8MFmYPljNROv8fkxu28Mk/unZVWnjT92GSa+YTpY/PWOwkNbO/As/k7Z4YkjAHaAHFMvx4m3cGhkLVyTv7ye+jccICDX7lO9+Hn8fS7dmg0KyH0wUaYq5xY2VQzbxGH2mEJFtVkx6nPyxEBqoJi0xOWA8e3A4+07Fk2UvtSTwHq0KYLV5TCbt/RanrENbJMW9GkbUgbZRFkUURaiGsKLhfxis+duKaiTTHXgaxDGVZpX/U8ufzPHWbwVMVjv/Bhc+5ppOwhYQcJfZLxF5CaXxIaN9XQk2ce6MyAzmhJODNGIhGxSoaV8fDJLc5vbBOKPsRIqCKdhQ5vf/t/xWLkta97Pb3eDnnmJzmyAapGVcnkrVWF8+fP8+Ivezlv/q7IP/upnyHWg3y1RrxNzCxGqjAglCVlWRDKIVU5EnGO4cCxcWaAZp58cbHOzd2UAltTlgVNxHf79MByMluxad49O4Fc5hp/9nbaTfvsZL6kVk88GO+sIs92PcszswQbP+WZUcDY10ynJs2E/JMuzPEA0rlBK7OzzOcTwHFKYHE2f6jZvSZGrBvE6jZpcUjVROWEnfjZ35Pl572Jq7/pkOw8eAP9pzfNZX2IIxEXFEJDsTxWMYumh02dN+hi1sma0coqL8hzT68Lbp/Bzhes9Pf5TAGeBQO4UmBbWRwqw+UMtNmQKo9kC4isga44laOi/hDi1kWzfao+F59p6vJri/gFIVsVy1YkyHGu+Zu3SOfqnId/8m527n8/Xreg2EbjTi3ZPRSsGgMzkwigzlGF2fyzFqW3UU/CcJcrrr+VCkd386JlPqO72+WhB+6Va45fz3BU8rP/9qf5xKfuY2FxiRgN55RPfOJj1mo05LrrrqffH9S19OR3YoyUZaAMEecczWaTZrvNsKy46abns2+5xfve9z5iVVAWA4aDXQbdHRn0dhgNepTFUGKoxGIUUYf3Gd5lZO0WvtFEsxaat9BGE+ezNIDTuRp/mHq7CbhWZ9Uzd1HLEkwMTnUsRSY453CaZqmq1jJlmnojnArOJeRexz8zEcS4h2J2Z59oltevQc09UEnvpaq4moKtmm6rczPTiaxOGZjvCJwLIGwuNdjraybuw2amJMm0YinMZB42E0akNAaXe4ZbPSk3V1h7wyGypQ4XPtQVdVv1IJUKCAZBsBij0ZMYTUR2LdqjAsMYyoK4UFFEwxPpVhFW7VlSAOHzOIL8cx0BGM/s94crespwTbFSMVGzTE1tRZDLFfYj7ipTd7ngl0V8bj5T0VxS73YT8x2k0ZHR6BD7v+J6Vu5oce63NrnwwY9bnm8T+9uo7abBnKFIQzqmuexEsMvGWaHJXpnuCSMubQjJcVRlQbvd5oMfvcfu/tt/S7z3nL+4yfLqPsqyTFxwi9ZqNvmvb387/UHBq17zOvqDIY3c4bMM53MyUWKsGA16nDt1gscefZiHHriPxx66j/MXLtLf7hKqYgxIgKjVPPjpVPAakKu6dbg/17swdnKafLpofRGmY0ziKA2sGqUwZAbMm3KKo9TooOAyVD1x0IU4APJPszntvS5r3YPcT8chqa+37nG1wFnNwkw4vW+gPif0e1D2wTVBND1mVhG5Jgz5pWXyVgeXt2rFJp4BAk5mpM4qJ4vMEY3nphPInKj6bHmgJoxVmGnNIO0Ti5y8k3PuPb/H8m2H2ffGgxz40PWc/V8XyZol2EiwQtDKxAWVGJfw0WPltSLupRhnnM+eCOXgCRbiiD67HC8Cj3QFXgXc+QWLBD7fGIBAVyj2KXEgLHQkffGlWNBldXLETPdr0vlbF3VNnPPJ+BuC5OA7ItmyRFuhefi4XP6NhymerHjiP38Ip09D2UfDbj2ddzST90fYOxNbxulgyh1t7hKWOSDOLBCjYBZZXdvPYNC3oqhY23eIECrimFNQy8E0Gw1+7X/+shXBeO1rXiMixsaF8zzx2COcfPIJTp48xanTT3Px4nlGoxLQ1ELWaLF2eB+q092NWp9PiKi6pIkaIxYrvvnPvomVpcW0a6pOFYpJ4qETjkCMxFBKs9G0T93/mLzv44/Qaub1+C83Zy42frylllfvPUWAKw8u8/qX3USoZ4PW6kYzxzk9ZouJhNTptPj13/l9njh1kWarSQhGtFCrCgViiImvGENiPTZaDEvl2mP7eOMrXkBRhXrwWZwzyCzLeOKJJ/l3P/cf6V48SWv9MnNZq9ZumFMAEJvOUJxJ7W2G/zcZtjJn8jOTzutg0cyIkigaVRoGg4L0sVGO6kme+sX3sXTLn+TyP3+Q7QeeR7GxIz4rsFAIroJYiWhoGzEjmEB1azTZFJV2lunFMuYZjTCiLEcw2Dv82T5NlG3PVQewJ0T5RuBeIXohLCiu0IaZRhqZUzJwDVVdQCRH1Il6FclENBNcjklTRTtIa4VYHOOKr7+G1iHHIz9+P8NT91gz38HKpOgzEfUYG7/FGWTYZoihs3XhSa/ZDD89Egl1t10kWqqz+7oNOIZyUv+etummuxrNjN9952+wvXmRJx74GE+eeJT+YIhpRt5aJM9z8uYirYUxL97MbCwHmqTCxuQEIZUWEcxlTdna3uJ7vuNv8c1/6S+y2+3TyP1cqD4hwEzYgbC42OTM6Qv8l9/8DgbD4aTkOf6cs+MJpNbVVzG2d3ZZ6rT4gX/0I1x37ZUMR+MLf+bs2WRqMCIwGpUsr3b4zd98J/c89EvkrQ5Vf0CoqhoL0YSrVFVqG3AZAuxcOM/q0iLf/73fxVVXXcFut080mziz8aqqQKPZ5ODhI7z5u/4uob8tfqVVC5tIncrN5wIynqc8azO2B3IUm7h8G9NDJpPTZRJpiQXMSoguNTsV3rTppP/U/Tz1C9fwvO99IVf9mavl/n99wSzvQzUUbITpSEQzb7HCJHqRkCGaYTZNv60lhKZw/II8Cxj4JV4FuBG4CISREpuKVY5cPFHaJrqE6DLiWuA84lwa2Z0LNBLjL1ukLFZk6ZZr5eDrltj84A7n33uX5Y1t4nAHCd26r79ikvPPDNGbLwDvcZ8yxf/G6V8IFcGqdIGLTdVpJ/34s/nluD8+2pgHIBgf//hdljkvw8GQRnuJRrNT571JEKMqRnXzmjChnomk+VQiialY69x57+Tppx7nr/7FP8PXf93X8vgTJ6ZCG5J6D8bGMmkZFmg2czY2Kv7q3/4u7nvoUZYWF+iOes9OOGU+JQhlwY+/9TtYWV3mkceexHs3M9RkVocgHXdVJcGTO3/tfXzb3/seRB3FaIDNcgYm7cjzAJxVBd/7tjfTWljk4ceewKlO257HA9Nr4RJQXnjb7SytH2CnN7SGxRkSldWpnT1j+zTb+83PyIPMTGuY2PwYNrQpo3FcFYAiVVeCg5Eja17k3Ls/yKFXXcHBr1jl/O8flwsfO2NZY4SFgeBGKrHK1CqNFlombgFsgIkijcQWi6VSjhRXWooCJpyAz3slQD9/9f9vFDgnFIUQK2GxpTQKZ5l4zKmpthBZSmG/ZgllciLqMd/AtAW+I+YWRRrHuPxPHUEDnHz7fVh5Eil3U72/7u2f9PXbnr7PZxyhzIr8zOX/4zJgrCpirGTahx9TSkAKkSc9+EatlhNrTfyUk1pVEGNlV1z7Qiwao9EgcQJCHNfHbLZSONmVE3BhghOzpKC7ub3La172Ur7tW7+Fze1dnNMUjbi6p34yJGQKxokIjbzB2374J7jngYdZXV60pCyUJO6llsaS9HwT1ETUVB3b21u85e9/Gy9/xSvY2elOxEXHVYoEyo0BP63D/g5PP32St/7gj+GynGYzT50Kc0jjlOGnNd233+vy/d/7Xbz0ji9jZ2cH5zyimj5bDQiOHaGq4pwjzz153pzwFWym4mJzPf71FWBThWKbZf1NPH+cYwPKTDlxyuwysGhiwYhljTUNIYxMQh+pnuTEf/oY1Qgu/4b9+OYVYrok+CXEdQTXcqK5E80yE99EtY1zOaYuj5Wj1Uw2UuzT+ej587/08xP6z6zyMqFqOaqho1JPVGeKB9cS0Y6INoRx+O8V9bVwYxvJFymLAxx4xTWy74UNTr/jPJv334d3XazYTWO7YmL6jSf0GrFm5dXUWrOZ6s5MyD9JCWcprTNOoB5wEWMgknj1Xl3Nu58OwEgttFP56zQUQxkN+wyGPbvsyuuJVUlZDFPeW9fdx5dhnEhuTQU+wPDesbOzywtuuIa3vOX72OmPks5fTThKRKNAiJGyCjUt2KiqiqXFRf75T/40v/7b77bV1SWKssCsshAqizH9mFVmsbJogWgV6oSNzU3+9l/5Jr72a7+Gs+cumHOJjhxCoAqBECJVqHsfSL0H3nm6uzt89/d8Pxc2Uxm0qqr6fASzWKWyqoWkKhwCgrG1tcG3/o3/i9e/4Y1cuLCR+hZCIFQ1mSoEyqqWBrfEg2i1mpw9e5bu9kVUpx2NNo38piXY2Yg/Wh0dMquBMAkcpg6jnvlgTH7mwiWLqZkslvWmM4RiZFnWZ/veT3Lmf55g6eac/S+/mrLYh+QLCcPyDcw1FM28Ot8Q9YuG7osiyzFrtSljRlwRYikcL76gdGD9PBj/zO2uEEZCLJRSskZoefPqwLXF6YqoW0LcIuKaIuqRTC0BgEhjgSiLZOvH5NDXrrNzouLJX/kE3p8xyh7YeDx3OR7WORWzn4dy5+6SSWP4jPEzTwQay1+HGBBBYkQuXNiU8xc2KIoqBcPj5hf2pASiYmYSTaQsC+n1tjl89GpiOaKqimmYPodPzjQD1/nvaFSxf22JH3jr95E1WhSj0UQdKBoTPb4Yp30IRVmwsrrC//jlX+XnfvG/sLqyTFWMJhN7xsZi41FiMQ3zcM5xYWODr33jq/nWv/3X2djcxhAJcfz6EKLVDMcUjseQFHmdCt/3/T/M/Q8/TqedW1UVNqYPJ0dZWYzBrP5xTtnYuMjXf/Ub+Et/8S9y7vwGBoQqEKMxfs80ZnD6GX2jwfbWBj/4j/8Jw8EQZXZowYzxR6z+eifCpxOcZiYpkFrfzybNP8xFDzNzBWqc1OoO0lhLyJX15tPHRn1TPWsnf/XD9J4uOfymfTT2X0WQJcEvY76l4nMnmmeI34/4y1T989T560ziFZnKCh31FJlnuE+hK39Ipe05mQLMgn8zFlUK7dzRcM483kQbJtLAdAHcgqWmHw9ezeWauv3aIo0FqngZh7/iKlpXOk7+2kmGZx/BWa8O+4f1l1DWxh/HGjsmzzhfEx6A2exUH9nDapmUAG0ydSaY0Wk3ePO3/1V+6B/+XQ7uW2U4KvZkjlO++WAwpBj2WWw3GI4qisGQ4XDAoaPHsVARqip1ztmeppRxCCoqVYioRN72lu9hff9hdnd3QJSqivXuOP63NhiDoqhYWl7ife9/Pz/yT3+KTqdNFaqxMChYAjTH0UuMkRgi6pTN7V15yQtvlu/9B9/J5nZ3auj1+1W19l8YRx0hUpQVCwttfuwn/rm9687329JSx4piKkceawA1RTYpYnFO2NzatC9/6a1813f+Pbu4uZOGgIzp03U0NHnPGFNkYxBDyT9669t4/ImT+Ea7nnFQzxKY2cEnEiCzKqZTUtB85cNgmozNVIVnrpGZycsyaR2wGhMIBcQBFno4egxPPczpX32YxWuUI19xjFgdgnxBTJfVpC24zKtmHZxbRvSAqBwW0/3mWGRY5nQyR7mo3DhI6fMXYJS4fv5e+pxw4yCFNTFXgnMmNC3oshNdFpVVE10wtIE4RTPFZWKuhWQdzJalc8U1HHnjGr0HCs6/5x5ctmGMuhD79eiusi4DhZkLYI7SPe6ZnTH5edR/LEdlk5r71LBFobe7ZW9787fyljf/Xf7et/01furH34aTce/+NIsQEQbDAcevPMJv/fIvctfv/RY/+tbvTg09ZUEIgbUDR6mKYdp5Y6iv0VnRChUQBr1d3vyd38YNN72Qza1tE1GqUFHWhlFVodYdSD9FUdJstXjooQf5vrf+8BgVt5TK2HSgh9nEIcQYERV6vQFHD+7nh976PVSmDIcjQrBJGD5+v6qKVFVyPqOiZGl5iZ//hV+yX/yvv8zqygplUexJjWxSXjQznAq73S7XXX2Mt/2j77NBUUkM1VxkEWIkWKw/Wwr7QxVoNjJ+/Mf+KXd9/G5W19Yn+XkMswKnk11bxi0IUw6FzXUwTgVfZ3f66RykaTBgewaLjDs2a4ZgLOv5kX2sGJBl23b+nR+l+1CfA1+5SvPyK6jiArg2pg0x11Q08yJZZuLaZrqK+lWzrEPWzMjFU/WUokjl82fHAeRLwAGcq+v/hVAtKA3v8qY2o9pBUa43s5sNuRKRRU0tv968VzRX1bbiF6Us12XfKw/jVuHpX36Ccutxc6FbT/Dp18Zfjb+cKaZn8yTRBPXIzJfPdHqPMduTX6MCcUKTi9Gk3XBcfc11dv/jZ/j9jz9Cvrif/auLlEUxxzxTFUbDId/yN/5vbnvJbfRL40/96W/klpuuYzAaUVYjAFNVqcpS5oQ+a+zPuQTC/ZW/9Bd49eu+kosXNxBUYjAsjHfvaWUiWsr5XZaztbXJm7/3rWzv9CzzaiHECWCJicR6Z05d60lZtygraWVefuj7v4eF5TV6vR4mTJqZqpAoz+MduYqRwahgYXGR33rHb9n/+1M/w/LKMqEqJyXTWYB0fJ9omu+3vNDhH//g99PsLDIajequw4QL2KSdeuqsqrJicXnJfu7nfs7e8TvvtNWV5YQvIHNlzD2TGCfzCecmASXsdSq7MI0QzOqppJP/JbEFkz0tRbMTj5KmZAmhBgXjAGFEsXWSp/7LveRryMHXXCYhHILGArgFRBqC5plIlon6ZcTdhOiLBbm6odpgoE2W2wrHP69G/4WJAG4cCBwHq4QyZhbVgbZVdT/iDojokqjzoj7DZU5cU9CW4DtElqR59Er2v2pFtu8vuPihB/H5NhRp95+M75oo+toclctmDHs6w29mv04X6KRDcFZlMxGA0gXpVKw/Cvz2O3+X3tDYHhr/687f49SZM2Q+m3TxjfFj75SNrS4fu/cU9z3yNA8+fppevz/ZWMqyqJuMyunoLagpxc42Njb4mje+jj//TX+JM2fPpRJbCJTjHb8GycY7f1VVRBOwwFvf9oM8fuKktVoZZTnBGsQMiXEeuKQe0FGOCt7yvd/NNcevZ3NziwiJtlxWCVgMgTJUyQHEyGhU0Ol0uPuTn+T7f+ifkGVZAjbnqiU2lRyrjSeESChL+YG3fK/sO3yU7e0dRDQRqkLAYlIxqsqKqkwRx3A4Yml5mV//tV/lP/7CL7K8vFiPLq+wWE14HuMvfKqObJMJwbZnfuNEmmwW9JuCf8JY4kmsRgdm9QNSg8P0NeLMFKkyVfaKLllzhwu/fw/9e7ocee0SC5dfQYwd0XwRydqIy8cljtxE1wQOobpummVk4qmGSjmQaRrwjfJpooDPSXrw+eUBlANhZUmoRk5EoouKKU0xaZk6r+IUVJN6bAN8C8k7xHCQQ6+9guY+OPEfniL2nrLM9bHYTySMWGBzkt5T8G9c6TPb08H2jEEsz94gY+OQOZqFqqDVbvMvf+bf8IG7PmLqHB/+wPsF10gAoEza4YkxkOUN/t3P/pxVeFk/cJgPvO9/cc/9D9NZWKwn9SAhlHPvbzEh/tvbO3LzDcf5lm/9NrZ2dusxWnFmBoDW7e2CREE1RQAry21+4id+gg/c9bGk+VeWYyqwMWH4zxunc5lsbW7y5u/6O7zs5a/g/IULeOcJVZjpvpPJTixihDLSbDY5e/oU/+gHfoTKoOE0PefTTeZNzb7s7u7wln/wXTz/ltvs4oVNMqdSxmoOi4l163SSVwusrq7y8Y/dxU/+y39NZ2ExhftgNjY8dG4G8ZjTYzPCojJzAcw5g/TA6SQxeRaSnTBfC5wWjmXaX5VSAdEqVQXiAELfwvApOfWrD3H8H76Ig687JI/+x/3msx2xso25nhK9SXBZlNA2cRkxdkyqnFEjJopo7wtWCfj8OoAwEkCa6mMUDRAdJh1Tt4RoA5wiXtE83fRtQmxLfvAo+16zLDsPjNi46wF8oyvWH5hY6vKzWNN9E+vP5uWkZ743M6aBXE2wE9nDtJyMuxFqQ46a8PxoCS3WrMGd730voQp0FhYt825G2nusqx/JMs/p8xvygz/w/bQ7bXZ2duksrtQSW+m1q2KEz5uYRQHFec9gOGLf6iLf9d3/gEEZCVWBd44Q40ybbiTUU3lUFSsjK6sr/PzP/wK/9o53s7a6mtikY5BqioRNJDswcJm3jYsX+XPf8Ca++qvfxNmz5/BjQ56EyjIjN0b92TJ63V3+n3/4Vjtz/gKddpOqKmGOYDttvEuaI46LG+ftb/7lv8Qbv/pr7cy5C2TOjZ0h44GpyLgcmt5rcXGBJ598lB/8kR+r2YlCCEnuy1BAZ3oCZ0hAMqM/aDbXiTg/11Hm2ofr59ikXWACBE6JQTNOoe6mqNmHBIhluibjACt7ZI2Wnf/A3bL/nus48JpFzrz7KMXT5xDfg9gSwgjxpVeLS2nYiB20qO2sESljyBguKr2GcVvX+OiYE/B2+1JwAFPPVRwT2AILgstS5C14QTuIbyPeo05Mc0QzMW0geZuqOMCRLz8ijf1w4heeptp9mtz3bIL8T0g/YQa9t0nrBvOE1UkRUPZqRu2d8VnvYlneSIOBiYh6zCIqytrafgyo6nw3Gf9sjpmGZzQbDaPZkhADy6v7kr5AGi/LsLudHukmp12qqmJleYkf+oG3sX7oKMWgT6vVSWHmLHmmPr5ogiG2vNiRd/3OO/jZ/+8XWFxeIobKVHS2B25GoSgdn/OOwXDA17/pT/B3v+O72N7u0sibEyrxvC7AFBANZuSZ44d/+Ift3vvvZ33f/iQ8qjoTao9Vd5IbcKrsbG/xjV/3Jvkrf/1b2Nrt2sLCYsJY6gqLTf3FpKzZzhr0ulu89W0/zPb2Lp1OS6qqNFGpR/TObI4xjFGeydCvOYYXs5WWuRZiqcNEnlGIqSUIZewCJh2kU28KYmMR8hSRpD4B0yGEPhr7FL2nOP+bj3LD99zCoZcf4fFfeoLcbxNDC9xQJBROXOUw50xDx4JrIlQEHFVPCTEyGAgs2OejB+DzGAF8o8C5aQkQhBgEyxS0bcKKCG1RdSJe0EzQDPEtMVuUxr6jHHz1Kv3HSzY+/JA5v4kVPSQOE/Jv1RT5t/hsnNaZGbHyrOUhm/Z42qRhoN7Sz518lMV9RwnF0IgZY5nsqrQxuD5DJpnmmuPdMhJwTk1FCNUIMyHEilG/Sznqo1ljon8fQ8Vw0OcbvvarcM5x36c+nuYK1Lp54408CWVSI/pied5g88JZ+8mf+leSN3Kq0dBMUg3BZumwE9HMNFFnWFQcXF/hK9/werv37rvpDwYA4jThF8yE/VOSUqTT6fAbv/Gb3Pn+D7C8vMyw361ZiDJD7x2DLoqo2HBYcvTQurz6Va/iox/5MFVV4n2WKMV7GXxTllZKo/7tT/P4Y4/ayto6RVmPascQcVYOu8RqhPjGbNvOnC7InLnPmO7MeKD01c/yQGbHh9m4xcumPUQyQw+0OqkQZDL1mQIJPkmIFT2yxhYbH7yXnUefx75XrsrTv3OZha1zItoBNzBzQxUrMrRSgluOvjxIkByVDVaXhO2BozgWue28pCjg80MJls/xa9SgxWPKFQuOPh5Ps5H5peilI+jrQL9exC/hskPqWgtkC2r5stPmQS25Wg684cu5/lsO8vh/fFqe+MVft0Z+EgYXkPI8lDsQBljN/U87wORbml6IE27tbA3QZsBCmwsFTJzE4S7lxpOA4RoLIs7bVC5qKipps7p9dXxpFm12LLjM6NOn9uL6AlY/pt/Wf48pJ7GKsqzIGi1Lj52iCzN6/4nYUr9+WVRkeY5zPiHte3Zwm/GF4152M0MJpLmDGSoiFqs9Ofy0/BWt7vqzYCEYjfZCzWb8wyZcS136M8oq1P41TmjPs9/BtOSaKMxVWUKsyFudOtxnUuYV9cRyRBh2cc0OfvEgqe+eWV0iSEJrNhvwzYmDzmiZjQPHZN9jNYMxj3lOkNBmZxdMdIrqJirEg2uA70C2irbWpKyOceSb/hTH/upxHv2pc5x95wfN6wnonzWpNiJFt7TQJ4TiIaH6DYvVGYm8v6R4IkXR3YLVQcV9LYOrI7w9fq6JQZ8fDOA24OxIWFOhaDpzWdNi1ZAoC6h2orhU+tNMxOUqriUmHXHNAxz48lVGF+Hc750wJxvYqItUvST0Ubf6jvndqDCZNz2T/c83ccykhDbfEDKRhYoVkrXQxgJWdGmvHcaiUBb9egOp6aZxtn4MTCEFebYB2qT2HiCvd+SxFHYyTJUMMyGaJ/dJaANpzL/CnMD9dB5Y1hjfq2l2IdNZATJTnrSZUVmAxBgs0zxNM57RAbQ9/fdmYwBl3E1ZRwdZVtPoa8q1jLMvmeHaj/ERyPIZCb85RH7soOb2anzWnMp4pZqcIA7MLG8vSihL6w97Sch0lugzHnomY2HPOZ3APcCPMS8QML5iJm1IU3RxNlmop5FMry4zLNafLKZIJVYQh1hZmvotuXDnfRx509UcfPkqF957CKqz4BpYyATnlehBq4xKFkBWzMo2JR7XiFQovQXlxvOR+577IKDAW4H7YHC1EAZCqKTlyEqzZTVdN9EDIuSI5CLOoV7NNUTdggRbpHP8EJ3rc87/7iaDUyfIfB96vVT6q4d5moX5BH5OxUpkr9CbzErC79HeF7OaQlA3yjhHNOPAoSsZlSXbF0+nqTcxzOT7s7nlLIWXidTI9LBmKjWzAy5q5tBspDCGKmQ2HB3LZNcKRqIyNsQEUM715Y9nYc1qYtX0mPq+sROYAmQyw2JkzhwsVDMqOMiYvTg+htluyLFzGvdhxRoMlJnS3FRfkD1NOelDT6cV1Xek16uzivQldhZXKfrb0rdqQvOdGnLNzaw9i8i8I0z/1UDf9L/JHfKMRNKmCmkz0YPNVs4noUwU6mGhKUUtsWqIZEMbnnpUtj5whgNvPMLiNYdk655HzbtmEj6JQ0EdEl3LtDooUdri/JEGnB6FUUmbSNGNZMdshhloz1UHMCUBFa2U95eFxqzVUDgAerWIHjIkE3FexAnqBdeSVPo7wOpL9xMUzr3vJML51OZrI4ijGeQ/jr/nucEOwmc6VE1maJ7zlMDxhX3o6DV0ez0Z9DZN1acuvz215omc94wmwHR/mCWjihCKJEtuMs8eHEuRjKX2Q6xvyyT3NzMGZUy6f6oTXNPMBLGZdENmUbBJKpRShzgfEMszOyFTqF93NIoircaEI1kr+cyD4eMBm9iULKMOMyQDWqIY0eI4QtM9ahs2K872zNRsptptIoKFiPMZWeaZlf8YpzmzHNC9moHUCPQcC7TWDpkEIGPcYwL/ybMY+hjPnXGAM9HXZMx8LDEdINUQJ7ucf++jrH/lEdZetsbmvfvAb0BoQsgU9SaiC4JcbUIZoz2NcSbzvleqlcS1kuGWclsZayzguRoBvHX+5kpL6YkaVW7OraKyJiaLiMtNnTP1KtoQdS2q0JR8bT8HXrokw4dGdO971JxswWgANqq7r6o6xKon+8xt5TI3dHPq0efDg6kXHzeRyzR1nkGEW602lSlZ1hTnMosxiYTauDoUK4vjkDnO0lBny0oqFkdY1UNWjmOtfSmCSXU81M0OsawzGlcr/NQdhagjCxU3rpU0Wk1SxcjwXtF6ZHeoyUjPCHSnZiCCpMGkGE4VGYtzhFBHFqntNg0YKckqI3/6zKS9WWpAcFbxeJbFmO6bTj1+2oxHq4o2Il7HUcjEKc06rFRTm4VsZuLJ2WiqIorPGkjVrIMXsfkR31OQz5gF/pghJjEjGipzaeJkRNxMj5HMiAMIM4NGbQZbmJQVahFRC7WzH0E1xLX7dB94lN37XsjKixdp/eoByvOnUN3BNEPUi4nzUbQtpkOz2EbjAhYjlXPEQmowvQbY3/45BQQ/PxhAORDKgaPZcmbSsiirIrqEyhLq24LPRJtOfEtwbanKRdZvXJfWYTj16+eoeqfJfT8RK6xIckwWJ1JfdQ/XRNlvRuDJ9gqATBRebDoVeq5BZHwNyARfkSzP8UWF1EKVKQYN04tJRMTCRHlArBoHA2OCpVgs0cYCxfO/i2rl5SAtYqjMoomoS8YuOu1cMk2RQAyT0dUxGOtt4yff3GKpGRgVIYln+lp8MxpV3VOQSoWJIzCLB1B37MkzpiE8c3AmZlizQeupk2R/5duonOIyP7dDjmeYJCc1jSaC1WCnwNB5fmdU8rbBLpWY+ImLnEr/TiYMzRi8ykzqvSdME1G895jqHl7OswzUmfRn1NGB7XmEJfxoThewvpqYSSimMMBc5DCVCVWZGT86EwWM+wR0BGFIOTjNhfc+xZXfeiMrtxzi7G8ti7oNTBuIZibqPdEvRqpchaUY6RhW4kLGYuborRlsPNcxgLcZvFX4xgPGh8/VqsjmzWdt07CsIkuIW1DRprjM4TNn0k7j/2Sd1dvXKLbh4seeNpd1oRjN1/1rmS6xSTOHTIo2c8MhPl3gP/N1z6o+1ZxxsclwClNVEVHUedQ5MQIuaxLHbbDisSgSY0ScgWXEUE3y53Ej0fCFb6Fwd6C9cyC7ZibEOtWYUdeztAMjqcNtTOOx1O2nnlh5hqOSqkqhtlaTEqHFpCnG7FwN2YN2VzPViTgTfo9343F+r3WoHnp98kGfmCsSywRm6gx8MOHNM1cZsXEqVRV8M45We4G/M+jiZ3dsA1GfXki17qxLr5Gcl9S9CrWZyQTdMZmbLT7TvyN7O/5m9uYYp0NI2cP42zsibEYNaOoQppjNbM1xKiEsM9UMm9CDiVWSpi+Hpn5XNj/yBEc2b2T9pWty9j37zcIZEWlh9EG8V3WdGLVpomuq0onBCqqQoaVSRU2cgC8FJuDbgSuAprqmNXxpoaGii4ZbVtEm6urafxNxDUJo0zh6hOXnd9i6u8vgiRN4HdT6fqndV6yiHrLyzALkDPNL9mC3da4+ZQHZXp2gSXnAZvUfQpyWwEQdTh3dzTPsnj9JDGPNQSbjrTqrRyTvrFgIZRLIHG1bddnrpGh+OdI9C97Xu5+bTLdNWLOa6iRgscRGlCk9NYB4wTmlkWd4b7Xurs0L2AgT3MBmmyJmma2zBiB7y+PTyTzBeTLvaDVzopsXeB7v9nHSXs3c9zLplzM4Y5E3ec8vquODoWRRrC6XifQuPk057E4px3WrtnOehfXLxOWtRPWeG0Yw7mqMM8ZuKR00ne0CE5vrDttLD5lB9mqy1VxZaDwHRQxMp6PEzGaGlTyDXFj3h8ZEEYoBpErYVRihWWnFmVPS/dQmKy9cpXVoTYZPdUxdC3GZWPRKFC+i3kTbZlUL7wYgSliA1SBw2ZcYFTi2xJxkLloHkdX045qiXhGvol4kaxHjMis3r5MvwcUPXzQrLyJumBR+47DOp0Kaq7OH62t7FH+nTb82T+meOOspVM0cBWUOjxdTV6O9YuIyiaFi8+QjhGrwrB919/yTrLWXZnIQIyzcYEQn4vJJhm51zXgsT65JH6tmSdYOrA6NZdwdrGaqKpkXNM4MuITZWRfMT8+Zv+b3VkXGGW29n5ng0tuGKKiQZQ6f6SSFmNTY6zDBSa2LURvFLARbcySJgMd4ngjvqym/5hzFoMdg5/wefm1yeYFI9yIsHzk+03nHTNEt0YVn6huTCGBC9X4GpC9zlEORecDR2Av4me3N+uegpWfrG5jwyGrsxmK9eRWgBUIB1Xm27zrJ+stXWbxpjd7jy/9/6t473rKsqvf9jjnX2unEip0j3Q00OUfJXLL3ApIEA9hXQUSQYICrwvWqFxVERBSQIIgISFAUBAHJOUhqGrqhU3VXV66TdlhrzjneH3OutM/hvs/7vOZZrz6f6j514j57zzDGb/wCJjuBlj3EZAayXEQkiOwS5LQQ6XH9MpQCBooNgaVT8QD43e2lid8vDJ3VWcjUyiKYXUbMAsZkkNh/diDYvpCvsucOi+JOwPq3D2DtFlrOYqJv7fZTlYQNojy3z9vLvlXtt75GWmGQ2g6c0h2ZTdZajFgMVn0opFranWlCbcQZCGURE3kgkX16YKwiJnbBSRpTA38mLS3TrC6lYe6pUNv8W2PJbah7zTm347qd6BB/Wl3s/H5oN07t8R8INjdkxpApBCPdr5aGPEmKLGsDgtq01+n4rI4+Ew+FECS4IlZCRipFn1bohCa/ghC8tjUGtBiXtXVb2/6rfuVlB41Xd2TcYvZ2TJmUhv3b8Cdam1vk/9Bm1qOIdIAEkRBdl0SjOajYDTa+ex1h/XbsvuMKhz+yC+UmxAxAMyPWWA1GBFlW4Yx43maDJDiSmK9wWOB3FV52ClcA9z8sbC7J4KTNQiYWZQiyjNgckxtMZsT2hWyIdyP6Z+1l6ZIhJ769weTgTeRmhvopEiaYUKaQj9Bc3O2Zr7Q3TxfRmbMB2zYeFIxUq1Y66ACIhibRBiX4khDc/+GXbmoINa2bOCAqJmXydZNwMaa5VYxouzTVyiw4TQWtjW1AqG1Dumy/htfchB9Ka6a/Q0eQzDbrkrkmKGmyPxdJNz1Nb18/xKCtCK8IRvrKGzExMUL6eiOVPkCl2uARUK0OxfRamajOjhkDoePRqDUiGFqvdJQ4Sx2I0urVmwN+jiY8H3DYsHmpzJm7NIptY8kOwayuqlqG7FEglFKBklTYTTC9GZNDN7F11Tqrt1xmcObpTA9cI9bkcXAalbGimJ6KXRL1MzUh7wDr9wc+ebmwHdY8RQ6A+3/CcGS/wc+sZlhVmxthEG2/bKbGitieYnqK6Yv3S+y+eFXsKhz/6lEoj6I2+vvHm7/sRHu1eWTCHBt13gBY2iSxttmLdjAgpU1Gi19grQXjak6bL0vVEKQi5rRJQPXPsVkHbNI414t9r7V0jqOK6FKtKJOWqq0ubKkqAMUq1lrJbKituE0bC9OmH29+vjZrXnaQs0EdXR6/hzQMCRO/v5XU/5rtPPv6sms92SZteklVikGwFQMxbmbFiGhoqQ4TCFgRg+JGinhECK3+PFENGh5Gw7KoSu+6idN2xdfgoVLrg7VmhNb7VlrskLlk0Q6I3MoU0bZRiLbDVFuPK3EC8E4JTrRc4/iXD7Lrbsss3WKZ8bWLWNtHjRXx1mCsSDB9VBZUjLdBR26UG6aqhF3C5g8F9iu89GYjBN38hiDFhlD2Da6XYUyuxvQE6SE2w2RCFuO+sAPIVlm5wyqzTVj/3mFsNo7gH2XEruPQfW5A3OJd7FTqtbv59kvYYH2tmrkNXTWnSN7LsTZKfoPz9EfL5P1BMgtxaOVArJEh2B8u0hssJM56KlONgLUqmdH6MYmNUVlimu4ytglg4l/JrJrcKplVKrsEiRVAtASPHABrquw+U1tmW5s+t87qa72dAkSMsel9rfw/U32MlM0XN7Q1ghXS3/i2YXtYnUhjaFZ/XCv3HU0lvUcIOlhYSU9RiabKqnkuPaPFlRhLptpyeY8byvsyBbNQGXi0zD9CKyVpzsqrcSmSubq/zRT4UXtKWt1M20q4dRFowzKt3Wgq05B0kZWlGrvF+ncO4MawcptFxCyBzUVMjkpObI9tjpgFMAuqjJBpj57PKE8aJhdWXoE7t97/ORVAGv9xecsItBDtjzIVzUS1H1UwNkdyK/RETS7KiN6uVVZu1WdydUFx6Bg2K6Ao4vgk9f6tiK8W0Ve3HwqtgkznX9wWVVi0a1jRKQgrJktT36qqx9oel9z1kZw8fE26wNuGwsLC8umcPHlUNFn21mWktVBK0qakGjoTFUwC3NLhIE00dxO3lc5nCRVgGDdtXaE2bDiVSnMwZzbcjvrWOUS//a7WGSpVAKg0KkTV0LQBqjsoUaTF76d2OVKb7LjTx7wr6Q2WOOviuzNePxLDTayJJX4IWJszXN7P+tqRBkoM8f8hBLwro7yaLsYjLSpOA9szVxG0BgGtBaIqOzBHOyMWrcyjtNV+dXxmZIdGs3YNquTCM0xvqtObjsrswJTlWy2QLS2jW33EDhBjRY1FVPqIrABWrRkOyjybuvLHFg5y87YAm7cUyu8Jy7tFXbDWhBHeLmAi20GMtZhMJB+IcwMWz1qW0V7L4U8ex09OYLMp+DgBkOjx3wh/NNQBX9pi93TLr+QENL84pCnzNclWRNtzn+7Cdl5xPi4NMYZyNgZUV0+/pGaVRZ2Kw5cFa2vHCL6MuoE6BCMh+pFIhNDe7AKZ7W7+zsirag2ShoAYM25Ce5LVGs3NH2SV912H+NydGHQ+Yio2YqilcdLGMlrx2yppNt8ZvescuTqW5QFLiK1ELb2fTta1N1iU4eoZsTKQCBLGaqvk5PGDHcCishbT4NU7X3ElEngR4vc1zLH8ur2htOY9dbsj8n9LGp+n3jdtFY3WgZaeovMER99AqSoALaJn4OZR1r97nDMeeSaj01fZuHKIsblgckWsInaImHMhlKq6V31pyDVjBgx/qNx/MWWHVlXAy/6z1YBVKXKpwkHgfPBjAdtDe31Eh2AyxBqS/l+yvmixJMsXraDAycuPgJ5QylkK+EyjvxBa07pG0dF93RRV0VaCfEu0I3MewF26bLR1CHPWT7TspNI+tBmunLF29IakC6gyA10EomyGzbLUYZjWroy0W8W2dq50U3lljvtekW3aj9PETSdGmwqgZdjRdb1pxnzS6s/buEUH/Z9f62lAYaTVB6t0iDm1lYJu3yiRztAy2PQumn5aUx/W5WRTCxrH4A5KUSkU6xJO67GoJFQ0xoZXRilI/cTUxGdNTYlopypS7daKnWBQbfattOos7U6c5+nAXaq51mNcDQGR0CIGFeBmiD+pa985JKc/9kwWbrHC+veWYgI2vcSTMJkYMySYkSjL5GpxVjmtJMpsv3eqVgCXC+yC/JDCEiqSa1ArRq1CjjFGyATpiTLA9JdZvGSR8TGYXntMMzuDokA0uf2GSl0Vdni6dR7vkRYDqLO0dUcBldCpV2uUO36VCUmiUs+4UxafzVExAgGjTiVEPn5k2AXaOcMqLcO5tj9ASBphaZ8ysgNyWSFwTaEgnU8VQvDp9owEGZvAxrnbKLUWFflFa02+tEZt0qpOTGIrhtjaagOQtXRx0hrDNrEahEQEin+DknASVDDWUhQF0+m0LVpMrVY8iIOGDlxZq/40yMxBOT4JwRHGJ7cNN3e4wYVsQG+4ADZXiRwMSa6fHSKFtokEKp0CQVp3epuMpnPTgpCEwyIhVkq1c5WPuJYvMWbC1g+OqttAli5d4uBHllUlR01PRKzGyCOTwCFGGhj2bTmdzYaG8XUGNsP/D4hAyyBT9RlYlQwxPTFZJiYzZLkE7YldWWF0bp/x9RPKk+sYmaG+QLSIyCm+Ra/U+ubpzvGbTDhpq2Gr+0q22z7BtrEh28hd6tDgpQoSactX48qwiIgE9Yh4rZxm4+caoZ0+2N69KqmW78AMc1WAbGP4NGCbdPjyq7uWGW9u0e8NMVnOybX1lN8n2+TK0npO2rHoXXZb2xoMzBz3UhodlbSGgPUoMqRBXUDxKE4CaiyYGPt14uQau5ZHculF5+nS8rKEUPMWpHERNq2DvVFgxkrM4ospvpxgjN0B/ZF0MIZawHT11Vdz5NBBsAPJR8tKdBJqRmmNJkw76sfWL7hjziTb+AbpSRFph5KKhniZhTgSlKxgdmJNyoMzFs7rYxcWRTdzxWSIyUGtgDXgcozZ64PfpyHfYuKPIZMyAoGXS6y4T8UDoDxNyArBgvWSY0xfjLURXcoEkxFCj4UzFuntgcOfGuNna1hToMnuWxLxh/kLUUILudKO+VdTCpvaTw/dOQSXzuFBhzaT5lXt/aBxSA2uHEfOf1ooIfhYG2S5Vn71lSmXxJUglYFlvIBNt9zsNubb+5RkbqNtDW96TkbDPm9849/wV3/9d5x5+j6e9+zLeMhDHsB4WjIej7E228ZVkTkdxM4db0WCMen2naMOh+bxVZZhoTUhCxpVGyUwDSoaSmU2ZeYKXvCcX5LHPe7xnHX2ObK8OIpAIbpDP94lH6lWx6oQUIyYOcCt0TpU1mk+KAZl7cQR3vn37+BlL3sps/FJyRZ2aXJ1buC8NrOquebT0yVtdsmcliCV/F1GY3OIVtOJGF0XcQAtcePjrP9wg7333ctg7zKTjUE80MSiYqm8D1X9klVZ9UbBhozpfrm5GYHZzVP6E41Lv3NC4rQnE8QMVHTFiCzWlDaxqMkIvsfCWUOMha0frCG6Ge2VcW3Nf0ur3vaW0C7IC3Npv43CXNtVtXRZwtrxEZiDsFKUVT13FtHjN1whG0ev77YP6UEs7jozuQg1KJ0Y0WYmJm06cgeZ3ilaUeZYau1KIXjPaLTA1//j6/zKi35HJR/wze99j4995gv8zJMeJy9+4a9w/vnncmJtC+8D1ho6mfc/AvrqbkGtJ2bxsGqXveljAdQH9cETGiYMQZUyKDNgEpSydFhf8NpXv0Ke9rNP5uQW/PCHP+TGgzem/MXQULm1IRNX72trFiTpELZFv2mLpZhyE0WEQS/jVpdczIt+40Wcfc55POPpP0uYbogZoGT91qXReBq3+kKdJw21Lx+qKU3XU1C2XzdViEgMsAUH5Zpu/uCE7H/IXnqnLzL+wTAa50qGiE3WUEZV/QLIKhqmOGPxN78g6OarAA4fFtifvmsvM8Yth2BPB1lFLBJ/KVHJRc0iC+eMcBMYH1xHzARc0QAm1Ty3ne82B8jqDqyfdl/brhK247n/h50A6pyTmMSrKmIpZ1usH742rQDTpZcS2Dp5SAarp6skqWrNXJP5ErUFwc2Lc+opxVzBOff4gip5z3D1tTegkrGyMCSEPiqGN7z93frBD3+U33zes+TpP/80eosLrK2v14ScoNvvWZ0jvdTj7Cr0ZG6qoulI6CYNpbRgwAXFEzRoIJhc1jc25OnP/kWe9rNP5rtX3cCrXv2XfOqzXyT4oEGD+JSw3BB5mGu5tC7NtZZQhmoTVuruukKqqpYszwlB+S8PuDe/9esv4PFPfCJbWxs88xcvq6PO1ebNIdBMk0TnpqntUlOlLRBsvVIt5XHtJdDmBgTfiNuYMLn2KBIuZnTWiBM6iCanJhecFTUY8Sqgy2p0v1FxPnM5KyqsFTdrBXDzEoGKDeG0XYJ1mXoWRHSoQo6IUTWikqP0sP1lGZ3bZ3rM6+zYuhpTpCemTMKflu/fNrcY/RFGwI1gZKd2rcPZoJsF0T00wIeuI2+7Jxfp4u1gWvhDEypK0ERj0MpismakdaTsQZGgbXHO9t+tyztFgV6egar4lNbrnWP36hIntiY859d/Vx/2k0/k05/8JHt3LZP3epTOtb6PdnAQbb1RHw6txCUlpQNrReet5cDSAH4xG6EkUKBSqufYZIIuj3jSE57AxlbJK//0Nbz9799LOSu1LEtc6bROH/aKd630I+/w3qlzLqYTxfepr2PKvXrnUoS4U+dKyjJ+nvOlzqZTvPe8/V3v59vf+Q6bGxOe/gu/wG//zstwsy20HCPB0Rb5zrX0XbbZTmlCHQ9E7ZjQKu248gRmh3QAmBnTQ8dwa7B0/gKSLyuSpSmATRRQI4LJJbICl/q2n+EKwe8/1bIB22DExVCOBacWkRxEjIqoEudhNhPok68u0tufs3XDDLd+AqGIunP1CTChq/KotmDHyEM7Ka7zh4J0objtnm/zXyM7lW4to4cW9VZb6TlN6k4Q2nbXNRGtniO05DnaSQfWbdTG+Z3ZUp1WPjVp1NRWtRWlJ7OWXfv28sVvXK6PfMLP639/1q/psSOH2bdnNfbn3nenEto1Sa1v9dbH2nTjeC7Em7aSaHmt/gacKoUPTBWu3drQ4S0u0DPOPI/Lv3cNn/78lznttP2p3KlGe2j9wop2ARnR1vRDq7+KpFqgha9WyGSjfgyqwZNJoCxm9HoZBw8e5UW/9Ts847JnSjleQ4utOJZp06c7iy7RjFuqQGnTkTuswzZYHFps1er9oWY7iniKE+s6OTJhcFqOGS4S6IFkqqlVTm7DAqaHmsFMg5D10g8+LKdYBVAdAlcC0M+NQbComlDhySY3IrnR0Jf+rgWyRRhftwXlRi35RUskVIpz3YbV61ywZ31Qo9sEGvVWU92JwNZhDOsc7TOuhxTG6aMdWDSXyRI7zjTAlUR//2pDb+eXVLdtFZWdDi5VJGjFM/sR1U2z7bdZm6dF31Y3SxLquNKxMBrSHy3w1297F/d9yH/VV//56xj2c5aWlijKso7Xbqv45g8flSBBAyHGb8TkpCrIA6nbAK+BMihFUGYhMPHKNCjjoIxGIxaGfTY3NynLMlYTkackgdamUR9TOtU3xqbacB1U28nNTZ6ABm1lDIT0PSoKsiN6NFCn/R4/fpw/fuWrePRjfpJyvAbFuLNZ0bmKQLvUng7cp/Px49rKVZjPKUzAtkZ+QJhOmB2akK9a7EIf1R4qNRdAIilIDMb0EMkRo+R9heuJeoBTsQUAcDPBkQT1SfcqVhArYnMIOf3dPTRDJjdsikghqE+Bj4kXTtd/bge6b1dYMy/kUDrqQe3I//X/fOkDYmI5piqUZUF/tMjK3jMkjQc73HVU2X36+fSGC6q1YrCBGSNEnizFQ6hbAg1BGm/8+lJs3SDNzmyn7lYc+dBqGzQ0XWv1ed45gitZWV3hyMk1nvvCl/Con3wCX/nylzh93y6szZjNCiovv+5G04oygMagzIT0azIODXHzC3gxoqrqNWipShk8pXrGPsjYBynLMmqarKR48lacdyWlrjGfbtvRrVAkXcBVldVkHdL6P4kToTEIVRET49XTY9YQmExmvP6Nb+Xe974vxVYKnpFOJJjQFBlzB3mrXdNuBVAdXKptwZ40VUnwyTPQo27C7KYtBsvQ37uA93m0OxPTIpOZHugwXqZBODRUBqth59Tw/2wQcP9+5YbToThuNBODiBMrMzXGixgbSUBWlD690/sEYHpkDLZEgk++fx4VL6INQ3s797/SbTZ+bLrtGtdORJzoPE2VuVN67sZV0qgpfiw4zzm3ux+nT8fqnYtj4xRoKSYDDMcPXZtGkNXJmtDfWsxUld6GGrpCJRWuHWA5copqUj2oaemZKhetbuZ9t6dvFmZZlmTW0N+7Vz/5ha/KQx/9RC572hN50Yt+jdPPOotjx04ksZGNWFVyQ6oOAdLktRFTRsDPayCRtQki4lRxBOIhEJipMg2ekDZSlW6Msc1BRRffmG94mo83Et+uy0/osD21Re6qgMWkbdAQgoSg2Nwwm80YDIf89VveLo95xIP5wQ+u0p4xQj6qsopanJC6G0kLyTR4Uxv6lxgu3DUk1jgF7mgDUhugBdObtpAe9Pf0QQdxLVkTmzwjqNeBwpKoLkSTipHCRvIFPRUrgHJdWMgMA6tiZAJ2C4wTY4yKkYBBpcfo9L4wA3dyijEx7LMi/qSSKRLQ2jZgLSdXaQkypB7baofq2pCCuhh6U/a2b6H5lRVqqxARQ/COEzfdqNPpjKIsmM1mOp3GvxtrJ/T4oWu04s7XWgFN4F+6lSTurhSTFhoEUFOgRKVmC6FTKeBV6o0Zmrl7+g2klWkfHZMTqt7eYCHEg2BxcUElz/Q1r/1rvd+DH8Hf/93fsbK8yGg0oixds/F9iH9DNetvbj2fcIKgmhB/xUuc/pYhHnlOwUVcQI0kG83QAsUiKqrbxreNkEZTBlsLnFRUfXUq1mCrtqqjVktTn+km+S54H1uDaHsurK9vyOLKbt7y9neyf//pUmyuoeWkOtuZJzx1x46VdYtpubuYyuOplTvUbhO6wjakZHp0Ew3Q390HGYDJk3eEib+aIQNZgLDUD8awMA43d8F+87cAgJS9ABJpjWLRyG4CtYjNyFctxXrAb4wxlIp3ddZfxwGmhQNIpZCTuQGZ6o49d/f10kYN3F5c2lQM3dMigIq2jTdUAsVknWK6xWy6yXS8xmx8AjfbSP79lSNNw3HHq6gPoi7+JTT9oFYlqa/L1bT5fXMQtDL6nA+EoBq8xqjsNuicNn1I/68Pk/oQiP92LmoXFvbu4fpDR/Tnfv6X9AlPeApXXfk9FpeXKGZlKu2l2sD4qs/3vpoEpI/F5exCwAWHU49Lt3+pAQ/MNOICzocW1yJou3yurbTaQOjcx9MJ0hCV6MaKVa2E1lZFTd6C957NrRnOhzilcAHnPMYYNtbXuPiWt+ENb3wLw0GG2zqJulkVV1zHErCN6lMTD+pKQTSodEHMOffpqhKIbaRQMju6TpjBYF9PxOSRCShZUmcJomSioYdKrsEatg4byomcmgfA4UuFvX1DKIWeF7WmB6YXQ0CjM46SYfp9zKrV2XGvfjJVJCBUibGhJgFJB5UNneGwVmaflXXWDudC5wVrzd2qBaet1kHbI5wk/lF1EpLNdU3QqeysaVldiam9wgkqlaK4aqI13fDx/zRlffDNzR+avpbKDad1GIQKdQ8+gnLV51eym3oDtPGCdAhUB0z1e6viypJeL2e0Zw8f/Mi/c9/7PoB3veMdLC0vUxTxEPAacCHgCZHaq8TNH2IV4CXUAKDXoEG9+nhcoCjrIXDUe2bBSw18VoYh2h57anNAdxgHYb7F0W6Vt2MMjLTB0mgs4vnwhz+EzXriFIrS4ZyLKc/ADTcc5PZ3vTeveNVrsAbCdBP8trzEdsBA6vO7P78SF8hcvZAul0bOpj4Kg6SkXJ8QZoH+3l6MC0Mkch4jEBhhNKyKDFS0z0rPUiwZ7n+qTQF+F9g8KJTjuNV8YRAzMIY+aFb7wJORLQ7oLWW4kzO0nCT6SKKXJCMFaczkNBX47dq+LgZaQJ9qB9JuvTQ7zPmr8lvQjpCFFnPeV2zAUJldZNisX7PTjBiM7YnJBhHb0EAIbdKSNKW/xKpDU7CJaBDRgPh4CEgrVaYqwasQFLxLI7doEx5CwIc64TgBgK1NX1cBWktp51ONKhqvd14WlhcZF47f+4M/ZDrewGYZrnSxA0l3s09zGach/U3VQdzs6ohVQUDZ9IFDpeOE9zhSYFbL209bm7+JW4tPR3eA22AAUX4d2MbJbwmjGgQvvbYSb//FhUXe/Z738Oo//RMWhj327Vllz55dLC2vMFpYZNfu3Zw8eZLH/tRP88vPeR6+2AJfdA8A1bbJUg0+N5iyEG1TGv5PXXtqu6wJdW6AsUFCMYOJp7eaPCSJAS2RC5Cy46I8cqiZGTDeb1k2ls3Nrv/GfzoI+DKiUtHNBJuDGalRnyvk0epGFJMJkpONeuQjcCdL1E2Q3NXmH9IhVjTkTNHKZ5b2pd+Zzc8LPJvx4E4jrsY2RGqf2AaFmk7GuLJsjDeNwbuSyYnDODeTOHqiBpl6w+X4OgXXjJKCR32JBJ/a1ZDy80yjPAyVNZ9HTLoXqwooZpEIGg8j5xTn48ed93jfAEtBdY4wpJWdYH2fznMZap6viOA8vcEgDnGcxwbFe68x6kslTdbEq6SWIIl9YkUgAaFQ5dCs1MOlY0zAmaiQbGzB0uRCuiMzaYJL62GbtZl471ANalOKUXvXG2Pw3teiJx881trq87RSRGoSG42WVuUVr34NH//Up7nVJReh3lO6Eh8Ce/bs42lPeyonhws85rFP4o1v+CvGszG9/mjuUojJgh2bMtG2K3iLdFJ5U0iNJko1CtTYHYuB4EvchsMs9DC5RYrkGFWbQRhTkwKcUfIN3ZGG/59+ADzhcuGHu9I/VsB5qyIjrFkRZIAIxhrxWLGLfcigXC9bZV5FAe7Eabai6nUH5l9bvaHbVH66E9unQ/EWfpQNVBt/IpWRh6/5D8rpxo6/fm+4zPIZl6C1GVEC/LxDfdmsjkqWKy3KSrIjkJCE7CE0Pzu9GYKNKHpQ0dRT1yOzut9PeHjHKFV3QNWT5NnGjbV19DjnnX82r/jff4BKLyLkKF4Cmkz+QvAoJrUCsQIIafboETnmSo7NCjZKJ16hQJmGWNt193ZoUhhS3Ld3TspiSjGb4b1jZXUXG1sTcmtQDVKWjpXlRcZbmxSzCT7EMe2uXatMZzOK2Yw8M5zYmrC0tIQrZ7iiwAdPL+8hxhI0YPIBX/ri1/jSV78BbhYrr6ynWjguv+L78spXvYbR0gr7TjtTr/nB1bRHtLUoaY4+nZRVGqHPypjUNEWBSIc+0FSc8cUK5UzGJ2cMV4aYPEdnGWKTJFiMKuLBFghTyd2MxeApZ8JXF0/haDBfiGaaiZglEZYR6UuShILFDnMwSLHhFC2bYIlWdpQq28kVbdMLab8I22sBqQsHmTcO3K4KUJ33zFVrjET0OH6jcrZFOd0USV5+XZsRxc0m+HKmGKnUXAnBj753jfGnpePoIcwTmiT1yvEHeAEXx40uBLxXNHic861bHzquB3Vc2bztWZMIZLOM8ckTZJnyzGc+gxf9+osYjJY4fuIkMhrhQ3Qxc+qj02dsGhI20OAoG85xxAXWvMP5JAeWBFIaGrBQW8SuVH0YY1hb35Cf/qnH8Mu/8DOoeibjCU/+hefw+J/8SZ7/nP8OCq9749/ymte/mVf8/v/g7ne9IyKG9//zh3nla9/EnW53KX/wu7/OeeeexUc++gn+4q/exP/+X7/Nvr17GA77vPxPXsPtb3cpD33w/SmKkre9/Z28/s1vpb+6Tysyl/eB717xfT1y5Ijs27e7xnjquPS2S3D9v3ayaFtiKfPDwS6w3PYqDA4NM4r1gqU+mH4PtxEdpDWSgAQRj4hDKYRewdFRiGY7h08hHsAT2mXIPggbAnmusCDB5mTR7hhBNFhMP+bhufVZkkf6pluX0DRVbWpWF+Fp7ZjQofbFrRO0leS0HShS3T5n1rmoKNVkipEWfgQDq+hpmXcTlMwm5D7afcd616t6J+J9nOmqSToibSjE0rLmaifdpts8toyKK3uUZcAH1RBCxxevKue1o2RtHXWt/ttay6womZ08zn3uey9e/JKXyF3udg9Orq0zPrmGEcF7T0Ai0Be8Bq28eGLpb4AyBI66gjXn8aoaglKoUiritfIFSO1CiOh7hY+oNLFk08mES25xPnv27OLZv/YSjMm4333uyav/6KW88LdehhJ41R//Plf+4Crucqc78L3vXckHPvgR3vG2N/Cd736XP3zpizlw40Fe8rt/yJN+6r9x9pmnca+735kX/+7vc8PBQ1z5g2v5jRc+h299+7t87nOf5xV/9L8oyxl/9eZ3sLp7V605yIdDImaQXuu2SawwZzfSANFxRbTSJXQuH7rNS+nQLQOVZbjMyuiRO8xxMUStvaaFOekKdqTRHvxU9APwU2GYGesYqNEBRvsIPTHWqFhRMuzQCh504qLFUwhzZP1ubktXUSfb5/01OlN5VdSg0g6iIJFI0dG5/qCr0AsVel7RmJNAoxUf0VIhVl9uWj78AQ1OamPTCt8ITTxV7YjTyp8Xahp5TJr1Ci7g/UAjBwDRENT5kNIqQs186xCg5lh0YqwgwuTYUfaetofffNnLefJTnsq0DNxw401kmU1QgBJsxBiCD+JrfUCy6FTluCvZcE6LxJ9wCZ9RVRxKmcxAfDpEavirDvRoDFystWxtTcizjIc95AF89Rvf5iEP/Ak+9olP86o//QsAHv7QB/HExz2aI0cPMxwOWVle5siRo1x0/tns2r1LHvVTP6vXXH8j73jvB7ntrS8iqPKoRzyU9Y0tfvt//hGbG5t89otf4dV/+nJud7vb8rNPewqve8s7qZIGJQW0+pY9Wb2spAlxb4r+jspkLnq0aRJUpZZSa4dInHghqSLwE42wWT+nSnzXWvqgpilVsPH2p+UJeModALOYfmlyqyI25TnG+RkiopZsYLGAFqEir3e9/+fGONrpvJSdHIF1JwmdiGy/+dsgoKDa1XK2N07kjcTHleX9Vs+9/QeHUGJsjg9lqxQPKd/OtUw0UoVQ0ZdD18yiCZ3UpmoEfAgSMYAo0403VdhezXR9/0QENTZjsrkJwfPkpz6RX/u153PGWedx5Phx0FgVOBcPO6Maq4tZifGgtooqUNa9Z610zHwjE55pNP5wQaUaCVZtgtPQDAXn05hTiS3JPEmMsLo8YnWhz9bmBre8xbmQZ2By9u7dyzXXXktmDPe61z157H97NH/48lfKP/3zh/mD33spw34Pt7XB7tNPj+nBqnz5K//B1ddcy7EjMeJ8a3MNgDPPPIPxeJz8AlKCU5qkBJ/am+5MuQP41TkLnUBSdlpCLQNSle3fUklEaoppbBFNL5vzWIz3PaoWRAoNgp8JdnizOgTfvAeA7StF6Z0xhWR4keAI6mOZZFUR8p6QCWjpadx+Q6cElzlIT6Q1pu/kv7fznnQHcn/z4s2TSLrk/45RZ4IkKjdaR3+0wpkX35ljB74njYV43K4my9hz9q2ZFR7nptHbP1ZvkcLnS8GkxaASab3G1FejdqxoWndKzSPP8CFQlJGIE0KQUQhaHQDbOOlpc2Z5Tlk6mZ04zKW3uw2//T9ezE884CEcP7nBjQdvIs9jkIl3AR98ddDgjaGcFfR9QAzMgrJeOjZ83NwSmX7iUiR4qUGdKmXiCJTVSFCTT2Kb998h7wTKomD36jJHjhzjaT/98zBc4t73ujtPfdLf88/vezshwO1ucyt+8dkv4O1veg1/8Vdv4tjx47z8916if/G6N/Oxj39SPvCet/Hhj3yM+/3Effhf//uVLCwscP7557OwuMT1Bw6QZZZnXvZzPOIhD+LB978Pj3vqZdob9Gu7uHqZhEDwoRrtqLTQId3W/tOeonQqUml7CUnbTKTdujZYQJiVeIiWGdgqwjKdjGKAARqGeY6UN/Pmv3kOgHdfqjUO0C8C+chJUUzAzlTFCXhUPIpRMcggJzjwRYrTDs1mblEwO2nN2xSyWplPzR0E6ajo+N9I0yPDnLKrVc/LXKfQLvOn4zUGS/u44E7nxaWrgeCDqCpiLJPNNcriqBhjE5exyscqYxUQtCYSqUpC103LZlq7crPG9TyOrMo+hY0HAKq4EFJH0mb7xefNmJhoPDl2VJd375Jf+R8v5slP/RlMNuCa624kz6I7W+l8az1WDr4BJw4bPNPgmBTKhos3uU0jN6ehMvyMpX4iCFW3p9c26VXao5wWWy/228NBXz/5mS/JjTcdZfWs8xmNhnzz8it51ON/nl98xtMIvuBhj3ky3//Btfztu/6R71zxff79E5/itpfeilve6pY8+Wcu0xf+2nO49Da35tV/9Ra+/u3v86rXvIGVpQX2793FaGmVv/rrt3Ove96NyXTKgx71BP3yf3ybpaXFyPFoTUZCDW7KNrloHRfReYnaPkUNIK2puts5K2A+hEYRr2Q52DyxSY2tLgnFMVANe0RRLcwAO1KKYyZiAKdiBTDaFzBrQXzfa3Ae1KSmqQ6fMpkk+rtvzUfShlfT3o6i2nFkmJvqmc5IrzrQ6+LsRwCAMs8a7goMYxsealYK3ns0eDbXjsnm2vGoAgwu0ohUUR8rmcge9M0BEgL4AvGxK65VXrWJfTf6WqQJn6gNqoJCKYm5FuoDYFZ4SbmaDfVXBJtnTLfGMJvy6P/6CPm1X3shZ59/MYcOH8H7TfI8xyXikK9aiAr3CImWLHHGvl6WTETI0jNfBK3xkYokFBmBCekXSTYOlQSqtlOrDwJt6TNUlcXFBT7yic/wL//27ywvjbQoZoxGffn6ty7n5575fMrZFJH4eX/w8ldhMstwOOS5v/5SFpdGBA8veenLY0vVG7C8tMhv/68/oZhsKUEZrq6KKvz1375TCZ7+aMTi4gLe+Rb3QGpTkNoDMa29Rv4rP6LVnDOmbLWQ1SHQTrBHtP5YYjiLLxJ+Y9LaaGdHiFqUYUBXhaIfcfaZnFoVQHsKUG4JdkE0lCZ54luQDIkB9BJUxDQb2RildpTUjgOt0KL7/ijJgbR861pcy7mowHnXj8pEJBWlqu2aQAFJphlxEQQXk75snoQ1AdSm40dRW5EQfXpP2uDBq4YSUZd0EGlWXJtIpoFRZSOmDRBZTzuCoiEeAL70OB+/ypUls1kh0W4lfk0IQaYnT3LBxRfw6y94AQ95xGM4cmyNa6+7Ps7DEcqijAtNqenE7aScsvSMC0cYzyhUcWmTiwZca+od2YFKmcTbZTS7wtXMQRWfzEGrm9/X+oUmx8B7z3DQYzTs430JkTykg35Gr7cou1bP4qwz9rO1sV5XZXlvwNrGmI3NDS668FyCi6Qem/U4dOgQ10022Hv66Vhj8N4pCIsLwyRFrvz6pZPFDEgIEfxU1e2q0laN2I0KV91mFDB3IGjbAarNKa7SUoJSJtInYtvOo9GdVjQjaI6xGf7m1+3c/DyAmTGIyYwRwAYRKyJiISSfAxPp7vMnp7Zx/raxV3Nny/Z4htr2u6nmg0rH63obM6hFoGkseTRlzdX9YAhJkmCaz6/e19IXyLweqWr5fJkIJ6WKCRJ9Ak0iirRWTDAd9mJn7QRQL5QuUFov3sevnExmLC0toc7JrHDoZEyeBZ75zGdw2bOeg2QjfnD1DeSZwRpLWZatu8zXB05oVUbTwjGeztBeD3GBwocYDy71aFGqmz0CfSQKMB16cKmBMuEDJPNQV9+48247WkeB1xZs6fI7cfw4D3/QfXnrX78G5zxZ1tiAv/LPX8+73vfPfOwD78R7n/IQYGtzk9e96W287H+/il6/Fzd8JRlut4otIbkm4lfpfJ3x0Ob5NK+JdpfRPBhYK1TnbVWruRSyzW40BJXcJNe5OCkyUnFelZoXbEyuobo5TiOagtw81uA3/wGQTZTSCEgfa1KcLoYQvW8aGm31ZG1n5LW9M3Wb2i+k3qByBNAWJtge5Uh3VMecp+DccEDm+ITeFYmSa2q+QaAlT67DreNZE3vKSgCaSECuQKuE4xrlTxOAajiirVaoIpK0qON4KF1JmamGoCJGOLG2xi0uuTXPfNZlvO+97+XSe9yZX/mVX+G8S27DoUPH0LBOlmeUpW8ZWDSPOahiU+UxnhaMJ0VNYPHq8NMipoVpmDdHi/KEJAv2qTUoaVSCRQJ8okloGiP6qMBrv9baqqCZfzvejupcKQBXfO/7vPNd76U/GGKs4WP//inEFwAcOXKU17zuzVgLz7rs6Tz/V5/Fl77yH/K+f/mILi8u4Ld5Ks6lyraMTevRbCtIRbYh0o3CpIMJtGEo6jK/Gyc+F9EeQ2FbzgPq0v4ItCVo9ef7seCQU7sCsH3NvQZPyFJKdAr0Sr2mr/aM6YisIhc2vgrx/q+ncB3jZmnKeEW389xlh4ngtounox/c4WuTFZhqqOuLrWMHmG4c2VZVqKgMlvZqNtot6n3DP1Qfk4617Cy6GEcmyQN+zoW6fRClRludUhSOzFboWdyYh4+d5Gef8Uye8MSnsrC4wnhWcs11B+o0YF+Uc94UlRsPGCPMQslkUjCZlnglJQErpQHKIo7/NGECmnBLVYmhH0kZGDe+eA3qgFKjF0AEB6OkuPIPrCYNnddA6fz+Wl27JlYZMWsBfnj11fyv3/8jFvfsR0TZWFvjfve9FwAHb7qJ33/pS4EZt73VLXnCTz2W8845k2I6QZYW64NFdb6KbBPLtOYqSMt1bsfc+XZUmHbne9JapXOTJumE1FYfNkbIUkXsNTVUPrrSBp/UrpJmtFiWsRQbJoqB9p9iRKDFM5TZunJiTct+zxtryvrVr6ekUke5i40JO804TOvoqe3KHubMP0UgqMocUbCmbc2PCKHr/t8dMcwfIoTQYn8qZTnVzWPX/ciTd3ziJlnqr1DFy9S3bSggTIFeJ/gzMsR9t5lJIhFFGvaMV/CGWenJrJf6aUz4ybU3HMLanMNrN2EEMptF7b1rvPZpmVL4pCMoS8d05mLJmyob7yL9N2RgnYsWWlJbGIjX2jBMojGIilO0VFWvSqFIoRrZy0pdFQjx53rf5XG0GYpdL8doP4YINovL82EPfTAnj11HbjLG0zF3f8CjqCris88+ize++Q2srq7wqEc8DIDPff6LqQVwqiFIIyLVLv7SzJfxIYqZd5CONqyKBp9uUwJbLYXMzQJpLqsWmT1qzlXwEbglgJZBBB+j1FqRo8Sz1xt8z2/5DLukTJbk1BEDtceAB48rCwuKFBqCLYyxZRP2lmKencdkYPKMoGDSqStiGlbO9sie1iJpnapaFRfSmAJJOw48zBMM2xTZnYKda9KGyfIGIXeFxLGdNDdJmzImkso2E1Oqq/lmKCKhPxR0MwLnrSbojCPrfjUoBIMrCwob7bNNJUqReIGE9L0DSumKFI2ljaKwDc8UJePJDO/DtoASTSQjDUrPxzLNNRqFqD1IR1SUBqMutQJlIqzHyiBOK4KJAKJN/oQRYAuoNhbq7TFkI+6iRuOtzQE4eXKN71x+BYKwubXBrHBk1kIIrCwt84yf/5mIQZeOX/jFX+GLX/2mLq0sqQ+eSmLRZly3b/2K7elKR1mU8bnbNoHeMcxF2AbvSf36Sf1jDd3cYGl9rpD1LeIjL0ZEGw9Jkg1T0GA0lGJC5Jp74oVyyrYAABSIDKZoKDASmmy7QDmL2L5kTVBllNIakLaYYh7El23M3eaf0mESKrotcm9e2jk3UGjvwljEh2Tj3EooVpEdMIqKr6GoBq3GfBo8GqbADCHrpAZrPcak0TO0mYDxXo7qgyAUhcOYOKozFS5iBGuk2fBQf6zFVo+WZskIsyi9GkPt7Y+GeKBU51WIt08oSiRVABVmoKmpDa1+P1Dd9mkkmP5Gs9BEZAyBoiwpnWvbuWxP7O0Eu4Q6YM17z6c/81me8NO/wGhlNY2PBV+eiVflBz/8IY95/NN40+tezX3uc884zZjNMLKCV9eWQjV3fwuJi5VIJEMVZTRDqTAA6aytVqBq15h2brLcblOlxTCXVnuQ5L6SYRcyvANXeBWTqPHiNPYEwQthBrruVcZko4AvmiSuw4fl/y0f4GYWA0U9gEgesN4hthrwJ1N/1TAL4gWwJolnsprZJ9WMFOlQK2NlFnYY7UuXZlXN37txTTt4AzaaQp07QgC887H/1NCGIRqCh3RJhJELYOo5b2woC/CFCGW0xaimACKdXrQdXVYnCFROplGLz6woEBsR9Qqcq91mxdSHpqlTiiMhyAdlVjiKosT5kGaxjaefqMYqohrPJWaimzlsradPI7taphDtTR2JDUigRPCSDoDQHAQVBl5G/wLtjEtqPX1IJ2v7cBSCLzESE4937VrV3miBpeVlUDhx/JiQ0pCXlhb54TXX8OLf+Z985t//lTe+/i/43lU/lK9843IWRkP1IVQGJCoY6ZDCam2O1rZrGuZNSuvJgYq0bv4OPFAT0OLB1U4K0+3p8/HwzxSTixn2CCX4SVEdnypEA0hVLUR1rOgaXsbks4ApBHopieuUrQAgcphVqjI8+sc6/FYJFuxC1q2iag5ds8m07dnXybXuILnSJN1o+6VoZrfSdg/Sbt+vDURRPRrvHSEkJx5f0h8t0h8uM5us7Sg+GC6ukPX6FLNJE1NWG51Woic6RCBtk5ho0xG0yeRLQGBZFjF2Pszzh6QJxWzdcsYIZTljNispXd1TpstVNYhI7RpUfatqswfF+kCW0n6q2san9qeKAPMpv8UrOEKEKwjqQEJLoWhSBRJapp3drqfb41Uz+uFoyLe+9R1+7/f/iKuuvpb+cIhL48z+YMBNR47ze3/wxxy86SZWVpb56je+w8/992dzq4svYt/efRhj02owaaDeciHulPNxJZSllzip6DrJVB4g2qn8dCe4uen0pV0PzPezkQWqapAsY7CSowXobIbiomFI7eQUSlSnoEHExsVge6ewFsCPhcVdRmc+x2Q9CZJpUNFolgfqcVMPBvLFPoLdfjOnqGZtDN/T02ka6V0TW9sq8ON8Udu+rPNcA9GdyoEuAlUNr0P0vLOxt9czb3V3mawdIfhSI+XWSgw87pGPVjl203UVC7nBe0OpaCk1PVRNm1ASxV+t8pCWrTXpAFA1lGVJ1ZW0xSWVPVlrhKWqSuFKKZ2rR1EhND2/VoYjCbcIaXxahXN6H8iLWQQN02Mxlb0BzQiw5gPUXIDm/W1GnCbqcmgCPTogvIgRJYaEVvz8oMpgMOLKqw/wO7/3x9h+XxaXFnHOqYiVLOtx8MhxfudlLwcrLK3sYrAw4K1//z6YTuktr0SnY1c27UWafhhjIud/7hQvy4Dz9XGY6q/2MpJq+tyyCJfOiK8TICo7tZiCisFEyy8ky+kt9SjHnrJwGGkMXOuJbRolqZSWaWaxPd22bf7TQUCA+yOcfTasr1vt+b4EehjJwRuCU9QJ4tFpIBPIl7LEea76IVtDJU2edotuo9v1/K1TtgMlSxsI3CFTsBssOh/rlGj89cxfKIst1suJ5r0RkjcvZwzI9WwcvqHaVFLPcNVBmAAlqE0efkpKfq1uO43/0Z25CgkEnM6KqrIRre2+tKWLiio+l7L1gki0y2r9zhX5SjWIVVU1Eu1Y5/CD0gW0cORJ1hsQjCajjxYLMGgFBmrtGRhSzRuqMSC1ZKE1jAj1VSkmYzKZiFXHwuICmAyJnoaqovQXF1ldWcKXM5x3GJuJGIOGQNbLWD7nLJRoeR58yd7VRaxdRTW+flluWwapUDrHdObpDwbJKLVxFA4hRDKQttfRXJqEdKxVEK2IAkHr1nUevdb05DZhH8nzz2J7fcxixuykJxQzTC2Pp12uelUmBLHks5zpxDNYOZV5AD3FzRSbkCYTEpYRO0M/LcFBtpwDeZqHZ7UTS1MZy9yIPMxJ+KurPdSRm9qetfIjRnytQ0HnX2yRFhzVKhE1EJyTYjquStS0bpLW39SlvVb+d/gZhAKhRMXXYF9UfGexPA3p8xu/ww54REoBms6KxHtorARVkQpfLl3JrJjhfXTaMcao64ytKq5/ulYkDvaj8XAVVxU/5kyGekeenoUgQkHEIqSJOGkxAmu3xmo6UFOIK0uGUIOOKTA1lXnT2ZTb3ebWPP+5z2E0WiDPM1CYFoU45xEgy6IawXvfSicCayzGCsEHZmWMbzNxNBJNPRQxRhIuUq8LfevfvJVPfPpTDPr9lPIUaqqyd759YUht+tdEy4t2PAHbHMNmsiOtSU8dYCNJABYz/whqyUcjsgVLce0s6kYk5WPWXvYhuq4FKQFLyA3M/KnZAuy/XPnSSMmdN73eGHRdRaZGEwnce5Es4McOnSK91b5iB1SpqDqvhqNxy1HROfcvkYZ2bVrs+dBNe/0RyWLbtQLbD4ja66Py7EcwNmtN/jSiQsHQ2PNKixLqo2mUn8Vw5NT7xRKxoDYHVJmrahKOnKB59WgxnVXHS1TlxW5Iy+CZlUVtkIkaCudjYVH10y1JS9wkEcRr5+2hPgmgBM1BvUssPsFXepe5DKJQH1uiGo+4iAdU/C1pxEOlcxGLSDtBNWDEMNna5Gee8mTucrd7csONB+NoT5VFjS1BZQZqUvvig+5Y+y0GrTkYvnJNrg1H0+hUA73BSJ78lKfqZz/7abz39aEXgUoXBVLMlfCtfr9q8LQV+dGdULXbA6TrYZVe/+QHoWrFLo2wfUN5tCD4AqyLCIumWYqGQpEtEZ0qIeDTAyk2kjHI/lOMCrwwDmR9L7mfMTMbim4pOEFNvGVKKTenlONAfzUTk/UUrZJQZD71r5PPrm0AryJ1SJeY1X3xtif+NLPZeuC3Q/xz6gG1xUpM47RYfla3uQhikUzwrWCT2KAbMe5oDDvFQbBpgWa0ee+VTz8pW0DmnZF8ACeEUOK9xGAhK4gnjdZKGjREaERooc1SoR1gUfXolRw2nl0h4QRx8+YhlvVl63A0NW4Qtf8VWlERf3yi/lY8dosQCDoOQVzpcZFZ2ZJZx435hS9+gZ+4/4MYDnqURRlt2A1kNvbLbYqusQ3m4ROuYASyPHotVsElIo2ZTLVWQlDKouRb3/oPCucYDLImWEVTPLlvuVPVYGxX768qnZF/AwqadiDg3CXWGv2l9F/VjGx5EXowOzZDwgzEK96nBlRFDIX6MIYwzg3edS7c/XpqtgD5SJlOUasmWcZ7CUFUY1cYxhNmax67kmN6fXQa+yGwiQ8gHUxuO1e/+1YNNnXOYWpGYPsQUG1TPLqYgLYKf/Ue9U5qKrBYUe9jkGSV3ENzMNj+UtrECXGXDDO7BpNdTRicE70PQyT2a60GbMmI1EsFwsXPCyBW8QEycM4hNvHwZ1EaXC3+zkKfkyM3C1iw1tSahTAXJFoddCF4JBfEOUoNGpdi3bnW2gCvdYwLLW9iHCLVM5OB3kBgN0Y15QLG0JLIsPfes7i4yHve/z6u/sEPWd29Jx0Sul2vnXQTdexZqxKQCu9Ijz/mgZq67WmD+q4s+c53LyfL804oSRVaWjqX1J7t0Wy7JZeOtKRpstqcAKETt5FaS0ly8NjuZgSX6WDvkpgMZkcLYIp61wTGoCS6owQhlBICJo+Uzd6S3lz5gDcfD+DwYYH9cHQWWLaK+iFoH4Ko+iDeiUgUyEyPFixcvIBZWBA/7akxGZp89+qZ+xxVVHfi7LczBFrOwcydxewwnkG1O+ttuRZE/b1rGUEIm0evx83WfwTsscZg9zlNNFhkkUhv/Dkcdyb0zxYVg1QegdIqGTFaJaEkOFASsag2SfU+KKVSFlVijRVCQE1QJF0amDQp0OYgqG5aE2/5ugdNC6ziDqjEG1CS574SMEGxJnqZaEzBE01EVWlt/kobQCpeHVBokGudk4OUusc7ZkVJWbrqtYxuuiECov3BiM9/8Qvx33mvMXvtTM7qfqwztu3O26KhnskHnZuhiwEJg+GwwQVaIFMVf9YmR3aUf3O9f0ur0xWViXSY6VIDf1IfZLEVGDA6e4lQwuzYVMUUiHexCqgMRyWMRMM+gjrU5MhUCUEoNoT7T24Wf8AfQwWwrMgJxUkf0T4mGCQExYloadTPZHa4ZPUOkK2OKI/0xBirNQ7Q0k+3tWOdnl3mNj90aLWq8z6A8ydC0yZI67SvPuzKosXWiqEgvhjToPfN9lVVQjmLp3dFytGUIktBvvlZdLpMkEGqHNrqx2bOVB0eiFQeKqgvcQQ++IF+IjgJWWaligCL683U9mDGmPpWrxdiMiutShyJDbEmMpGIkUayUW22skRykW2xmBpbo7ncnk4YqVfPVgjMWuRX77167xvyU/p5YpWiKDnn3HNYXFzCWJuEQ03vX7/20nYXkjpdSNLhqSFQFCVHjh+Pz0Pw2s1/aDCdELRrE6cajVCrAJKWnqdhDrYrgm6tWY0Gq1fV7MgyrTI+smiR0RsxPGeRch2KY2OsLZAytkmVYSgh9AmsCFoEH3pYYNEJM3ezKQJvhjFgoiW2f+WYD7GFkQmIj7RGB8GphIkWh0tsDxnsW2DrikzjCLCNBUgLf6lm6LpdxdXxY66ctRpTkNpVQFoTv3nAr836Ttz5crZJb7CCJDJ98GXjW4jMqcIQSZHXCZ+vPqSoiEqGug2E9aZSadlKVDE20n1va5nDocMdPUPX8ix9usyTHKUjJa1crOupSRcSrUauEecGAWtbojZthTS3cvG0yeyrsw0jeUhzkBIRYzMyUzljN/RHY4Xx1ib3vvd9eM6vvoDSh3p8G7TrsZcsZRNnIY4jfaj4gyrOx4NkOOjz7ne8jQ9+8J8YjoYEv1ObLHSUeemxl2XUK8zbFsgc3aQCVqVVxZEO/IrE1CYRo0TQVqJVG5Kj5GQLiyycMWB2LKjbnGDEKRVpTNMMRVQjJ7s1xtrMlDyhAZ88FQ4ALo39yP0PC8XpwsrV1kz2FGr8NxE9KRrOIeit0KDqHULB9KYZPkD/zBFKX9TkqmTpELDden0ea+3c6rVuuuHTNMBdZGVoiw1I+9Sn4YTXDM1kLLF+hH27zkycII8mWnC0B68u1npYmbzk5/ggnRLE0IImuqOlhoWwo1ZBBDLTSqRSrQmmOjfW7EpTu4WPbgtR2TYaqZAUlXZSb2su2n3+28q+9gsgVTUQrVWDk7L0lM4l4A4McTM4V3L3u9+H3miR8YmjsQVMQHxoWXRFTkPzQKpK3UgiE4d0g0vOHe56dz74z+/Vjh6iDby2kP+KKIUqzvnIgdjh9m7bemntVk2j+RdaBqDd/zYVQ+T+Y3KC5vRWVsh2D3TtazO0GDcjwJQPiWoQDaWihRomoniMjY4hea4wOVXFQGcyzsYhV3tcVDPFnzTq48A2OCQvmB0f4ycwPHMBY4dABEak0sjXpIkWxVW3BX+1hmbbIsR0G8VP50HFhqZZBZOSNviJQ9dw5kV3o98fMp1u0h8toUu7GW8c30GkBAurp5P1R8ymmzUANX/LtE6fDvu1ph7pdpmStLgBQauBW1ecQp22u+O+Vt2p6vlRs9GOWUf3/dqNZdmZVMW2tHYJ3lM6j/O+VWzEhKMs6/HpT/87t77N7epZfHVoeR80OvRoKsSaXjuEqlOKZiNRxBSt0z77qY/XUxW0ay2vHTOYRtxFCkRhTu7TfZ47wF/9WdLqGXbw/275+9nEe8kIPmd4+m6RBXR8/RR1E9SWEFwMyokc9OgbrpSqYYo18do3eZy5fBJuDk+Am+8A+OR+5dKb4slk9kWniBChfw0aeWHqEClwa1v4k57BOUNMf0mZxpMR02taASNxIqDzpVZ3P7HT4pdqUCv13VSxwbZZU9XfL4CxSNanmGxw/OAP2bXvHG64+hvQ67P/gjtTjNfq5JjodOMxNsP2F/Xk0RtqOi3aXT7aefzS+fjc7yOV2dzOG5Qd0M22p0Azt9aW2YbIzpu1u4u1CsCsAzy78WIKnaxG3WaF10IF6j5YxGAN5DbD2JzgokOS90p/0Nevff1r8uLf/DX6vV6cQkiriqgjklJQbp24HtrVXrycjaEoCjlx4jj94TCZa7R/R6WbjthuCITgPUVRNm48bUvwlgyFObypfUiryI4eIg0BKAObQViQxfP3kIFsHdhSlS3UFQgO1RgBrapBoxZgIshJ0IKpenou3Jzb9sdSATDeUMkGU2+ZGg1jxc+MekNwQaQwbrrF5MaC4UVD8t0rUt7YV2N74DPEpCqgzZuX5hQX7doqt73YtBMTr0id2q4dP8H5rdBeKKY3Uu8KOfC9z3PBHR7E8q7TWD9+EFdG48lG/x/97NQ5yo3jLXeYenoxV5hsxx7m8/vYpnfubmKdqxral4x2fjXt1qOtp2q7FELbfHdaVXJT5tZmq9K1ue+GKrfkjc0O8N4xnpaJ+istAk/8wl6/pyfX1yUE35FZM+/SVrtFdk6smjgeVdxC3uvPJfzoNkn4/D2PBmbTErIZwfuu4lNaVUALBJS5GrT72Lu+D5GNZMHkiO0hdonFC1YpN2Fy3UkM6xDKlJLt4ygnOmKdQLhSlR8YDesMMseGevYsncItwI3ASqb4wuc2KwJMgwYXfDDGFD3RAi03mVw/YfUuQ4anrTC7voc1OUKW5uRme2Xame9L67UVdL5spYMFdDbcfJcgc8bjkvUxvRG+2OLa73yKs251H/LBImtHDzAdr7Vxt5oxGNVdnZ54ZxcCpUPzrV1q2rpn1VY/Pddud/3ROhkVSlf53l6/beux7WW8zpGtWuy3tlpQWy3TNo2Fdn7/lrpRTxw/Ihvr6wwWV2RxNNRDG+uMFhYiEy99XpZZhaxySmx1aSrMJ7G13J+7L25dEIhpXwpzwZ71iyNx5Lk1WWf//tNYWl1lc/0EW1tbiMlaz3d3LRqZK8ba/rPSrE+tl6mJojfJEgCYSTZaZXTBEpsHHeWJdYydIcWM5AhUMdBQzCbqj4vouAi+YEs8+Wpg+EOFC09BItBwl7J+Imovw0BL59VmUoj6QtVZ1cKrm+ZiNnVy3Zbk2W5GF+zW418agbUotvbGnwe0ur4gbasG3SYUmosYnjv7t5feXchHMYNF0EA53eL6b32C/RfeiV37z6WcjilmW/iyMjpK5NAk3wy67dSSyrO8SedNKLom7VB73jkfPdaimdRGHxUCHW1lOwPSHaQo2/r5ajLS2VlzIiTdBvjtUEq3n/M0BozhxqEm7Jh8yNFDBzl+07VceOmdedRP/hR/+zevZ339JDYBrjrXSrQ1HfOQaP2q19LxeGBQGUI1J293zLJthJ+MVMoZi0vLPPSRj2VlZZkrv/NFnW6skY2WW92aqojsqO2rwcSW81L3vKlGfzamRpsM7zJG+/fQO6vP8Y9t4qcb5LZIFUCZ2EsV/1w3MLLmQyjwtiTfCNhCWTxV48EXv6esDQPFHic9DWBUVMegJxS3gPoBwXmTTc34wFiLTWTpokWRfJcGGSCSJ7DENDf9NtyvddNuQ59aG1zm5LXzANdO6rumb1cZLGJExBcTDl75RXqDRUar+8n6I7LcikjW9KmtRjhou3duGY9oQ3+tPWM0miTWdljalNqqLu0Dk84JraO5xNh03flUSAitOQHzC7aaf9eiqlYRMz/aS923NKK0qtIxDRKfrJ1ruoOa2mBEgyfgUK8YY3EO/Zf3/A2/dP7Fcqvb311+7rIhX/7cJ9S5snFYqkaWsa2SmoBVtQyNraSQEpKpjJiQGgOtf4Pg0RCkfSPXZil1KxQYDhe56z3vp6efdT6HDl6nn/m3f0SyrE3UqkxAku6vPeLbKf2HruszVelvI8htc4IfsXjRPvojGF+5BeE4wjj6R6hH1QcNwalqEKOHRPUGRdbxYcZoNdBbUj55KsWDz//pLSkUQTQvsS4EtV5ESqMhrorgMDLT4tg6kxtKFm8xJF9ewq/3yOwATA81WbQIa0ssq5JX2bky+BHvaU7lVuw2VWJNjQ/UVIMK7BJVpL+okg9EZxOK6SbFTZv8f/hH/g/v0+2/of4//bbzbJYWxDX/Y4Sd65IOKaMJPU0f9RhM3ufKK77J37z293n8zz2Xcy66HRdccjvpWdGoEfCJIBfZeRUbTxSVmEsg3ldWWenhaYwwb8xXKhZGxHwqbQOJQxBCA/RWfn0hhAjgGuHGG67lX971Bk4cPYwdLrcMihJpqj1D3o7z0/Z7bGgY0qRBVeW/yYElVm+1D7cFm9ecxJhNcDMlOK11JqpBRb2on+BlgmiBsfrjWGQ34wFwqcJhYbhLKccqk5mTnp3gwiZqikii90ooBUr8dF03r9qSMx+1yuicPax/o4eaPpg+mCyVTmZbcbjDqGnHBa5z5szt9qEVJCB1JRGpOFJ35lUFYTM1g0WRMECDS16BYS4hVv4P201akGAbWmMuN1J3Osi0s9d0js8ozc9oB1/Mg30d/89WKo7OQVpdw1PZYSTYjbuS7rsbGFDb4pmAzUd86+tf5MYbXsRt73xvzjz3FowWliiLoh6/VdZkCf+qbt5INU4VsRGpgUrfaqOiV2GV9dnCJFQw1sSpWvB1glTshALeFRy+8Tq+/bXPMdncwA6X5inGc+QzM7fhaZmAyraU8Bg9btN67qGhR7a8h8VLVlm71jE+fBxrpuBTtlIo0ZBylWLcskU0R32Gd4YSgauAs07FA6DlDXjkpE52DX1/bCYhd0dB1lA/kOA9oUTdFPxJtr6/qfnjVmXpVqfJ2jdWkOyohqIXeQGJhmtqBd7O/qA7jrV8iahGbVuN6qaGTiuITbf3uOkkCLWU1KDOq9pMxVhICTSybVgvO13E1X2h3ZFaO2xCu/xx1e2/4Nx9UzuHa5t3js6n13UmVZ2k2saprhJNSlvUNn+o6RwNu2IZttKaK22xdkBArSc2qortL+qxI4f55L++u/XQTEW+0iYmXnZ+UXd2eG0P4pmnKnSLFemehhVzEYV8RDZcigB0jbKa+jxrO1TKDlfNj7oAVEwEFE0Otk8IfRbPOZP8rD5HPnSSMF7TzEzREL0josopBFRdmjcPVFlErWeApRwLnAV32RS+eqrxAOb/TKYqQ5xgimS54jWUGnyplFMx2ZjNa04yPXY2q7dd5Yb374VwA0gfTC+emsG0XFTnkB3m0eCYdT9bO8kf/M//IY959MOZTqcxjoyawildUEi7Ax3tThAGgwFf+vJXeMaznkvWyxLhRncQGiUtimy7L7fRSnUb3kBLejo3xks+YF04TJrJVysWvcMe6HrW6bZxyhym0nnUofvebcBiTXpXOnnf7YlAB6utxncB2+sjMlQNURgkJksK0OjAXHHx4+/SWJcJO/Xacyr8eZ++bQP/ljaCphqq3Zg7czxpHczSOWs78vMKzxHmTqRkcy8VCJiD7cX+/9Znkg1g63tbiJ4APwVfgE8pUoTEbFKvEjJBeoRg8cHgy7mT5rCcQgdAagHqZzdTxKi6MhfJemANwRl0JoQZxs6YHl1n4wdTXbpkQfr79lDeNMLYQWwFklFIU3Nup290bKWrjeQ8F118C25zm0tvlt9qPC0Ek3Utx+ZXWdt6sOVxv32i3z1w2oLgOQ/quZFA8pnRxoQK1Y45rXTIR+zMFJIfwUlo/W4doUF1w7cJLroD70/a4/i5l6UVfV6NEoN3cdFvK1asmv4CBBdvz7TBKrxGts8s2qbK3WDFUAFGpvXuFstUWtMGMfGikCqoNeyMxcj2/E9phYHUB5IxyeQmgn9xPfeQwR5W77if4hhsXnMca9ahmCpaknQAquqDSnDgCwk6RaRANT5ZYbdgN05hU9BP7lfu8lXBXqiYDZUZirV90AHqMtRKFAXNEJ1Cscnmt8fsveuApUtO4+iNS0g2hKKfmIEpK93TwQK2Bft1pgDKbDYDYDqdYa1hjkWw/YpWWsEUsfz33tPv99nc3IDgRTDJJPxHwQ7zLkR1kkYdMbctZLrFsW+sgFtiO5mre6WVU9nedHMX8bxitcWJpTvnm1/iOsdB1DnwYsexTIeXI93IpU7PLAK+GHPRLW7By//3HxKCF+893gcWFxd5x7veI3//jneQZTm+GCcTuOYbhc4unHv2q/JE0mClP4r993S94VoksZl0KLrp0AohmYVmmNES0svr6kukMfZQ5EfTfWkwioj+Z6jkYIb4MGJ49rmyeqtljn1zxuzoUc1kovgi5r/VUVCxBTDoRFXXg4ZNMBPEnKog4MsUfrf7jNiRkpWRy+xLxVhLEIP4CHjYUtTNsHadte9uorPduvuOe+Top5YRGYIdgO/V5omxjAo7MHhle9uXCB4A1ph63hxfZK2dcDqpWdsINo1rjncO3BR6+XYq2ZwxVPeO0u4W1flaIBqCd/rIxod2h9Hn/PeX7uNGt59BOyQP1bNy3YZCdj5XOmBqS4nUCmRrJzs29qRdQ51OhSACwbN71y4e97jHbStfv/Pd7/H3by8Rm7HvhX9J/7RbiHpX5ex0T9xWlaI+5rHEH95DZ4c48qfPBedk9bfeRL5wOkadGpsGjooGrWWGiAbCdII7/AOKq77E+Ntfwh85il3aRduuvCvfNvFcl3Y7US2stG5NFtvZbAHVFVZufx52CU589QTqjqXLPU7G0BJ8EDQgwQcN6hQtUJ1J8CUz8XAIGJ2KU4DWIbC4qBy9Biar3uRTJzo4oaJHRNSjYRUNC2gphKkx+bpsHTjK+JozWb3tMr3V0/EnrwMzQOwAQg6+Ugcaqlav6i2l6r3bYLnuxHhPRpKZxf4/9FReXFzcJkSq9ptBkBSATKt9rx1rFNlmO9YymKzg8rqvrDXvEZszVVkq6Xt2ZLIto5TEL6hEM1InBCVrM1TmcQiZBz8Tah3/NszASutPwvpke+x6w4OtGhSR6FYUb62YKOR9LZtVJIaveNeptsZbG/XtKbd9BHLaedFhtB7SdIqtZvARGnqIMUBxAuwL0WKs/tYPk/7eM5Cy4RaqtukQzUAnN9C3sLJ2DVsf+ktOvvf1SNZHsmxuWW03AUn2Ks34rwL/TB8xPUy+j133OIPJEVi/4pjabIMwnUYrMPUJPQ2qXlU16ucFpmBmGC0R57G7PPkNylfP0lMvHLSNA+RDhVnYLKzL+npclGsJYSqEfZiwS0IJbgL9sZZbR+T4f2xxzmNXWLzkTI59bpE8GyJ+iEofZErtqFKtAu1w7udorirb+G2qZFnGNddcwxVXXIE1Jtlha+eWa2/k4DzD0ZBvfOvbSG8hpn6lV7xKPS/LEp2MI4e7Whgmg16ffi+P9ltVxdGpz4WgHnVBGuVjrP7EWowxWDGUsxmUs8SvGGDzrGP2IQLWZgQNuOkMyqJZ2ZJDv0ee59HTIMxVAK0jyqQ2qZxWP08blL7XJ+/FLLrgfX0YSLs81wYvt9bG52U2kVjSJhDA5vSHI7yYSMgR1Bobf3aq2prqOUOn65RbKMU0DhalsmdIv0fo8JTSRRDADCg2TsRJEwpba8jCaVAW+GA6o2RTbeKgddGmYtDe+az8zMvpXXhHDr/yVxFva6+IDhwgLXPq2hVYoqDN5IjpI3aAL3NGF54lS5cuc+RzYy2O3IQ1W6gfI2EaMQ+iClBUvaClVy0CzIz3TsQ6ev1APlSuWglx87/7VDsAXqrwUomqQBQ/U7J+wG9OsPlxhRHYQtQHQhB0ZrQci5GTevI7G3Lmf1th1133c/QLu8EehXKQpgF5/Ot9HbHVoNC6w+RHtqHuwXvIMt77vvfygue/IH6/jluEtixD5rrofEQ2GKG+TG2kodzYAEoWd++TC259CWecto+FhRGbW2NuuPEmrrv+ejZPHAVyzReXErrdMjZVoZ/36A1iOW3yftxgxZTprMAHKCeb7Nm/j1vf6m4sr6zw7e9+j+uvvwFjY61rbIZ3nmLrCJBzzgUXcN65Z7F7dVVCCNx46IheffXVnDhyEOiRLy3HWXgn507FiKHc2AICe0/bKxecfy6n7T8NMZabDh/m6quv5ehNBxSEfGE1+fptT1y2xhJUKTaPk/WH3OZOd+SCC84TVLn++uv5/pVXycbayUjH8V68D2SZnWtPqm8aMwuMxnBNrKm7dwmV23AdqNwEPqugJobrxNYzo297KsGgwWLqVCapmY2mUg6XRTTstYZQlkxu8OT3eAp7n3GcI3/5YuzSajLqMC3wtcITTF3+i5i6/8cOwQ5x5TJ77noB2Qhd+/oxgjtMxpZKiBwA1QKJI0AC3glMBZ2o+gJjS4yfYScKQ7h0Ilx+WKILz7tPpTGgAL8b3xz+ULnxDM/yDOv8yaC9KxDZUsKZqN+rWvaML4QwlWy0JZtXH2PzqtPYfddlGew/Q93R66NPgOmjpofIrJHazlGE6xag7rv1R5LfbLpde0urGgkotbhE2aEkrmOcghNjLa4sVLdOcI973Ucue8bTeeADH8C5555Lnuf1V89mM2644QY+/olP8LrXvUG+8qUvqAxXyGyG9w5rDOV4jZ/6r0+WP33Fn7C5tSnWWgTIez2e8MQn8sUvfonf+/3f578/4+fYt28/xhp9+R+/Un7z138Ts7ikqFJurNFfXJSnXfaLPOUpT+LOd7wTu3bvqh+Hc05uuukmPvf5z/O6179RP/7RDyP9JUyWR56+MfiywI83ecADHyK//MvP4ifue2/2799fYygANx08yL9/8lPy2te+Vj/z6U9hhssxdisEibr+mNFXTsb0BwOe87zn8/Sn/xyXXnqpZCne2znPjTfewDvf/Q/y0t/5bcbjLXqpQmqcisDYqj2rtAVxGiheYpK8ESSPeJBJ5iEhBEFN7PK8FzVghhmS9cApFhV1kcNlRFFjkDDD+I144/uAZDlmeXdMcp+V8edYiz9Skt/vWfQ+8R4pr/qGmsFCg/zXw5/KA9xUsdJIfXH1UTOUfNc57Lrv2UwPwsYVh7F2Q5luQpgpoaxcgDTqnClUwxaqY4vOUF+UwceIo3Ii9NBTtAKY+2OPKv2lMAu9iQ3uRqM2WOUAobwweDtS08vFF9bohGL9kBz/8jnc4um72XOnc7nxQ9/FZifQcgA2B5+hwYK4baw+kbnAye0ge3PBa+Uia+tUGOlmuddhECGFRoBHbE5ZOpZHQ/mjV/0Rl/3CZdh0e8VEHlcHTOZ5zoUXXsiFF17Iz//sz/L6179BfuPFv8PWZKI2hVyggdHCkDPOPGPb07YwWuD1r3s9l132jPpA6du+OBfL+yzLZXLyEHe+8114wxvewJ3vfKf6a8uyrKsjk1nOPvtsnviEJ/DEJzxB/uqv/pLnveA3dVYWZHmGKwqWRgN51V/8Gc/4hWfU38P7QDErUjlvOP2MM3jKk5/EU578JPmzP/szfv03X6zRt0Pqze8mm3LOWWfqO/7+HdznPvepAdSiKEEVm1nOPfdcXvSC53O/+96HP33Vn1EUBb1eLz53VcPRGsOlCzG+JD5AnlNe+2XWX/uLYDOMyWJ4a2gZhKqmEWJAS8BaXBnIAqJB1ftAvpxRfvNjHH71ZXFS4CbQGzE479YyeuIfYPbdXilmWJsh6lE15Pd4PMV3vyQMl5XacLVhYkRjVann/moyxPTBDnB+mV23uxWLFw848IE1mR0/rD07Rt0U8bMI/qmPAgfVoIQSdAy65ZWJDX5GUMcEWD6lqcAd5Ew5inKkr/SCl9xORXUz+DAR40qMK9SXKn4iYbaJsUc58fWjhJ/azb77nC43fvx01XAE7ALi4xQEyQCXpgHz48D2ME0b4HbOPGs6neK9Z3zihCR0SbsnhzR9bW8Bm0WJaigm7F1Z5QMfeB/3vMfdcc5RzIrYt9LSt6cH45zDO4+1ll/+5WdxxzvcgUc/5ic5ub5Bb7RETL+NvXpRFFhjUZRiVvCiF72Qhz3sYfH7W9PMpn3M25msr/HghzyU9/zDu1lZWWE2nWFsZEw2rXusiZzzeB8zAp/5zGdxu9vfUR796MewubXFUr/HB/7xfdz/fvdN0VqRjqui2MygGg8U60OStgrPfe5zOe300+WpP/1UJO+rMZbgnJx22ml86EMfktvc9jbMZjOsjTFvJiHjcZoSx333uMc9eONfv6GOM2sx6Fv1mEIZbSTVx5wvY6FYP0rxg/9ILL3ADrqI9LYVyUeQ9QgxnzW+3FWs0XQLv3YolujqEbPB1k0/xB06wO7f/YQEXdbIxwFmSHbhXZDBQr35u/kVCVlMtF9JvH9MLP+Rvbr/gReIFnD8s0fV6FHVYoyEIu5171S1VLwLsS0NBRq2VMOmSNgsg5+SmRJvlfwG5fIVPcUPgBYQWADlrLTZ6KhX5wz2OhEOEfxepNiFnyrlWGy2ztYNx/ToN6aycscRy7c4XzauvJ4s31LcWpynmhzRaJmk1SHwIyjBbSu7GBcaX66LL76Ehz/84fRHi4ToAlsh28l3KjJajbF8+Stf4/DRo4ixiC9421vfyD3vcXemkyl5LyezNgZaGkOeZa0bNNKWs6Qqm81m3Ps+9+Zv//Zt8l8f+1jdxjqMqjeVAL1eLg972MNS0o+JXvpe4wgy8RMuueRC/uHd72JlZYViNouViEZrrLyXd56H2TR+vGLYHThwAO8Drpjy6tf9Ffe/332ZTmf0ejnGJFtw25T/VQnvna8P0Cc/6Ul8/etf549e/nLpL+5SN1njz171Jm5z29swmUzo5XnqhSPQWQGI1Z+iKFhIfgBaO+20+rkYrxYNVR2oSwR+DybrIfkA6S8mCXLoakXq6HRUSxeHhyG5oCe8QD2U3kfgsT+s6Q0yXNTiwPdk9sOvk1/wQHwRxwbGofnibonVQhmZi5UFWE0qig5WUvf+fcgWcGEowwtvwerddnPy2wXrPzhIZteg2FJCoaiLqk/vvOILJagEXQvKIRGOIeEI0ttAZ4E+jl5PT/EKII0DKyBwer1i+35mwqQfPEHNYU84JuoGos7jC0FnSpio+ENy+DMndPe9zpD99zuf9e99D7JjqAwQ6YMpIl9aTCO3TJVAQxORDlMvecphxOBKx+Me91ge97jH/kgqT/vt//a4J+o/vu/9BII+61nPlIc//OHMZjPyXgyVCKqa9yLC/s1vflNvuOEGOf3007nTne4ENt6eeZYrkjMZT3nkIx/Brz7nV3nlK17ZoualgtcnSbCxeBcDNPI8J2u9PAsLi6gqr3zFn7C6ukpRFHXAhaqS93KOHT/ON7/xDabTKbe+9a05//zzcc7R6/V485vfzDOeEUv9RzziUfz8z/8cs2lBlg6ILMuYjCe8+tV/xr/927/hvOdRj3wkz/nVXyXP81juG0vwnt9+yUt4z3v/iR9c9X25173vzROe8FPV71uz5IwxmF6Pq6++miu++10GgwF3u8fdWVxYrA+mdBDWrl7tFySoYJJBrlS0/aJEyylaTnd6/eJ3sUMk79W4kPp0cfvaQqE26BCbx3WTZMyEArLE9/CJvFRlntWxYd2lUqkfa9Wf6YEZodmI4Paw9363JFuCQ586rloeQWQSJ2BhGkN/QxElwD6UQdWL6roQjqP+hHi7wWBcsGaU1aOBy/cqLJ7qFcDLgCdEIHBrUcmHgZkLYrISG6bqwwSVCeK8qlfxhWo5xvaPcvJbNzK5ai/77rOLA+8/h+LI1VgzjOWUTFKqSsQConddGglKiFFqLW+7ig6qggaCiAjT6axNVWmLaeob2YfA4sIIHyKPaWV1lRc8/9dqFlosXYP2+z2uvfZafvGXfomPffSjeO/J8iEPftD9+fNXv5qLL7mYoigQMfWt+vzn/SpvefObOX78qFRAmwZFTVOz+BBn4l/96lf52799O9/+zrdZXFjk2uuu5W53uzuPfOQjKIoSY+LG1YQ7vOMd7+Q3fuNFev311wOwsrqLn/uZn5E//pM/5h3v+Hue8YxnYIxV1SAveuHza4wkaJywFcWMJzzpSfzLP3+gIXZ+8pN8+Stf5V3veieFDxgj6rxncWmJn37yT/F7v/d78rSn/jTGxAhzay1Vcqr3Xn7jN36D173u9bq5uQEgl1xyCa961at4xCMewWw6qw1UdX7IrqpaRom8lNEhKUzBDM9k4X5PAttDNGBSdrlYGyNAjGH8rc8Qjh6MZBzVpmtsEkzjP0JJ2FqnCp1VnbFwx/vr8Ny74bYi2i9xhCvlsYPodBMzWNqmF2j3/tHyq4+aEd4N6O07n70PPpvNKz1r/3Ej+eCk6nic0P8CDaVGHUkIGsUAM9ATqB4PylrIyoKNXsBa5dq8tfnfrac2BgDw1UXloqGy4T02FCaDUnXNGHtMNQwluJn6UgkzFT9Wwphy7SY5/JkL9ILLdsuee57Pgfd9F9tfB78JfiO2AsGkKiA0zJAgTfve6gG8BhE11KuSVqaMtPwGK6ZsItTEVRjrxnve857c4qJbMJ3OMCJ4H2WpG5ubPPaxj9Wvf/3rmKyPzXNULB/+8L/yqEc/is9+5rOsxJGcigjT2Yyzzj6bBzzgfvLe974XW5XXIdSuQd57RqMhb3vr27jsv19GURSdzubP//w1AmhZlmRZJiEEhsMBH//4x/Wnf/qpsftNI8W19S199Z+/ms994QtcfvnliM3R4DnvvPO5690ijmFMdNIdDYe85i9e19789QJ797vfJR/84M/zyEc+QqeJYg1wv/vdD2st97rnPet+K/jo7DscDeSP//hPeMUrXqGYnKw3AtDvf//7PP7xj+ezn/2s3OFOd2I6mWDESMVRaHgYid3nY9SQGAPeYVdvz+LT/z7axUhjtq6kG34PlK/6aWb//i5ksBhrABfqQwBAC5CF/fRvf19MfykeIiLkZ9+exQc8Hz/pR+NSBAkBuwjuOx+HshBGRpMlcYv11wR+aLr9pTeSUK6y/ydux9LZGVe94aiWawfo2zVCOUHDDNFC0TJagAUdKxwQwgaq31HvLjdiNoORLcxmoLfoYdv8/xSfAgD0rlPMhUqmYdzzzpY6A91E9aSqLySUqm4GdgrlFln/uB79/CE57ZErnPbw0zj8qQskrB9WYzfBbsTIbcmAoh4FKjuJVOIMz/uQNnKSjIU6Uaflzdcm1kbAKne9WBIC97jnPWqE3WYZzjmWlxZ5y1vfyte//nV6wyVcLNsVLekPF7nyyivlbX/7dp7//OexubklWZZpWToG/T73uOe9eO9731sfNDGUU0RVtdfLuebaa/nlX3k2RVHSH61oVIUKwTvuete71CBZtWlCgD/4wz8EPL3BIs6VGpOMBZMv6Fe+/CVBMvLegHK2yR3ucAeWlhbZ2hpXNzaV+c9j/9tjMdZEswyTkfdyZtMJW1tbiQikCdSDM844Uy65+BLOOfdcSuc1hKiUMNbKiRMn+Yu/eC3G2MhX8HFD9YeLTCabvOpVf8bf/M1b4vez1ElAvu1z6oOKS717SDwcX+LGvtl7LW9I9QENQ7xrpxVm8XJILQBiCVsOOe2BDJ/9YHIh5SuCCVBuQSjKlMHuoDeA9aPMvvAeZLikXbly3PhRJh5Lf7F91C6iLJLtupDTH3EBswOBo58/QJYdRqcb4MeILxV10f3XB4UwlcAJCOtCOCyqR8tMppRakvUDvaMBzr/ZN//NdQD8aEF03dAdE8KSiDgvwU7JZEzQAnGRMOQnBjvGyIZMD13HkX8/jfOespvT7nMxBz7wA0y+Dm4BzFbLKKTFCuw8LU3GbvAB3yrze70eg37eEpY0f0w9SvPkmSXLegB6/rnnpp5UER/qjff5z322poC2XetCGjV+9nOf5/nPfx7OK0jyvAfOO/f8utWoxm6I0er2//C/flg3NzbIBwuUZYGYSJtdWlxg3779lDEYVCR48rzH4SOH+M7l30XE4nzZRFdrdOS1vaFq0BpoP//8+POd98muWpiOp/zyrzybX/mVZ++EiVCUjs2tKYiKesV7rwuLi1xw4YUMBgMtyzJlEiqD0VC/e/l3uPHgQVQyvMbfDwTnYwjqt771TZ1OC0EksSW1ZhrWDyCI1MGD0op08drVhVVT3qDoLKDTMUqoJwuisY9X3zJeKT3Zlq8PnejrpzW+JBJQO8AOkfW3vRB37Aa1i7tSOEyjKGzejsCfmiHkI8piRU+7z+1lcFGf69++xuzIjeR2A3WboBMVmSlaavAuRBPQqP7ToLMQdBqsFJTqmBWe5eVAPlb45KnuCDQ/DDhi2NpncBsG+gZnLFamCodBB4LfVDVTCaXFzYzYLctsXW12lMOfPsRp/2VVTn/4mRz+zEXit44hxbqqHcQTVLKYSC8m1XZtXWxsIX1A4uEamYBLy4v8ywc+wBte/3r6gyFBQ2X21IhK67Ah5atf/6a2y1LnAmS1d56ePLkWAbjKb6rFCVdVtjbX42YNAeeQkFZ5r5d1FrsPAQmBiph0/fUHmoVVlSch0MsNxlpmRSHOR199MYH19Q1m06koRiVJX9sThhBq2qQA5HmmsTrytameMYa1kxuUztW5CaZtbiAt2ZKCESvJzZfSebGpwwpBCR62xuM44hOrc5ojVVWZTmeMp1OszSLu4cOcnssQMJiqBZBI4jG9nuSj+JuYLm9LccCQqCFpqwZTiqkmy6BajhFSVHnazEFNrDRQzEJPivEBNl7/PGbf+DBmcU8cJbQvHjFRskxK/LV9yJbi7b96Hmc++hbMDsGhT9+EscegmKiEqRImkaQQgo9psUEJfob6dTQcF3FrKFO8K8kWHYOjIdJ/fzx/bo4DYIfbv+ULEEqBPEZgexziNyXIWA0FGBepwUWGmxhkA+mtyfTAQY586iw9+3HLsvceF3PDv15J3jsBfhGxYxJyWsd1qXYR2qABH8CFAF7wzmGM5Xvf/z4f+tcPAXk1GN7J0yuu2f4iAOvr67gQZ+oxOdzhArJ7z16VJMCRkCJ40yEgIrpv3z5ETLrpJVpYgZxcaxKGAzHTXnzAuYAH9SF0jEY1GUuOxzPW1zbYt+80YmsDs1nBaGGJpcVFTp44iRiTUoCbX8cai2qNkDKZTFPloUiiVnsfn5/l5RHW2lrjn1rvuOlN80RlFvqbQzY3N5lOZvSHw0ScUibTKcvLu+j3+0xnEaxMLgCR6GMMu3btljzrMZ1OMcZo6T0+iY0aVlOUyMd8D8VnPdzaVYSvvY2AiQStWmxoJPigmvcIB7+HZP2mMHVRgR6t9tJFkWXCMBUWk8pvUFDvCP0+7qoPsfY3zyBMNrELu2I72En9TVRikehfaXqoXUDyJcpihTMeeUdZvmWfa99+Qic3XE0vW1MtNyFE+a94FzS4QAhONYgSZqiuiYSTOLNB0AJTBLRQrupV4N/NXv7/GCuA/cpwUymGStYL2I2AIeCcF8kmgjupylgxpeJEvOtjZqpmIpQbarLDcuhjhzn9JxY581Gnc+jT5xEmhxC7CGYDzAwxlYtq6AqFovhMnPc10cQFpXTQ6/Wx1tJb2q3e+zr5VVGdP8WMCNPZJtdcdz3BpwNA4o06nZY89KH/Rd74169XMRkm6wZGqioP+y8Pp0zkF9V4404dfPeKK5pbzje8Aec9rqTBKFryU5tlTCZjbrrpIBddckuCj1Te6axg797TuN/9H8Dfvu1v6OU9inKWkoIjN9/NNgUsdhQPtGuvu45Z4fEpYdM5x/LyMu9+1zt5x9/9LSurq3hXJmzC1OKf6WyqvV6fXi9Hg3Jy7SSXX3456xvr7BsO8S6m+mxujuW8Cy7kzne+s372M5+SfLiqZVEgGLK8x3Q25gEPeCC9/oD1jU2yLMOVHufajkypNi/jmLxaqdNDVzD9p/85vwm6zgPZEMmGNYYTfKxK4iHgkbxPeeCLzL70l2QLy9J/4MuQsKLg0SD4iYc9t8Osno76Q61cAdNUFakNFcmj2Ycdgl0g6IDe/os443GXsHVj4KaPX09mblCKNcSPVcIMDWUM/AlONYRSNSDKOuix4PW4sTqFImCyUIcjbqO4nLoHgHSmABwJnJ1HFHM89TLQqQ9h3WJFCQdE9Sw0DFSLEcFmGqZIuSmmf1wnBw7IwY/u5+ynLHP6A27LgX+8hry/CX4LDdEoVdWlF1paBg+V7j/+9aJ4H00hNQT13uNDiDfzXIBkh1loDUjO5z73edbXNwiq0Z0WYW1tjUc88lH8/M8/g7e85U2AFWwvlola8Nj/9jge9Zj/ysmTJ+NGD4oPytbGFp//3Oca0lAA5wNBRZz36oNvAv9aIw1jLF49X/jCF/iJBzyY0jmsjVz6ra0t/Y3f/C355Cc/Iddfd60mcEqCxuSYF7zghXzuC1+Rz3/2E2pszn987escPHiI4WhEWTpCUGazglve8hI+8pEP7wSpShwrrsp0VjKbbDWOXyIcuP46OePMsxlPJpEc5T0alN/53ZfyX3/yUUw3T0r0dyjx5YTb3u72PP0Zl3EiVSw+BPEhYiQaaoViZNgHkJB8DzxAppgc019udFvRwbWZ76SNX7VjwUuU21cFXw/cke8z++LfMAN0uJfBT/wOulEkA5oSGZ7N4DF/xOZbnoww1FqTnLT+ldmn2F4i/YywvQVmxV4551F3Y+XCnB+84ZgWR66hl60R/KaiU43cfx8dgDWEgJ+ioUDDIQ16GNUT4ouS0DdY9RGYyJUf4x/z4/vW+2PpYvvKyXXFzIIEUwpMPW4qIpuifqzqZxGti5wA8Zuqs3WsvZGD/3Y94+sdZz/xTAan35LAMpItgx0lqXBlHmo7h0CI4ZL4tPFCqN6ea6VSry1zks6KvmoHC3ztq1/hK1/5Mv3BiNL5mFoLbI0nvPyP/1T+x2+/VG5zm1tz7nnncNvbXcqLX/w/eM1fvp7JbEbpAqXzFGXJYDDia1/9Ml/58ldSCe5wzlGWPk4RQjxgtuvOK8dfw/ve/37W19YIgUg3DsrWeMwZZ57De9//AX7q8U+Qc889W84652we9KAH89a3vp3f+4M/5m/e8hZudatbE3zJgRuu54tf+BwLC4sUZdRWbGxucelt78C73/N+LrroIto9xMLCIk984hP5+n98k/e97/0sLi6KyXo6XNqNqvLRj35UEcNsVuJczOM7ubbOXe56D/mHf3i/PPhBD5bzzj+Piy+5mMsuu4y/f+d7GS0uU7qy4lSkv74DpvpShTIp+0KLxlu9I7hIj/Zpdwcf354zT1FX0e3T15dEjX42VLuwn+knXkV5+Fs4GRBKj6hF12f0z34oo7v9tIbx8WjsWQV81tmVOWr7SLaA5MuUbpHRBbfn9J88n40rHIc/cTV5fhNabCJuDOUUDYVqKFW9B/U+vkMLCFNUx6I6LU3mkKkiY41BoIs/NgDwxzgFeLfCE+L78hsU2wtku4PxRRHobUYUyR1VY4+ImlKV3RKKHG9FfSbiNlX6J5kdvpabPrRPLvzF/Zz1mFvLD950NWawibolFZvmqSZmqqsmV/jkHxdv/YgGu7TIgnYzaGgp2+OaD61JgkaCy6TgFX/yct71D+9nS1MwPVC6AhCe+/xf55ee9WyK2YTBYESvP2JrayP9rKhRdz6wlFle8co/0el0AiDOhwrRV4kHlYSgHZeDSvAUQiAbLPCN//ga//T+9/Kkp/4chw7dRC/vgSDr6+ucdda5vO5Nb+XY0aN472X/vr2Izbj+wA0sreziHe98jzzlSY/niiu+y5+/5s950EMfjvOK4rFGOHbsOPe57wP4p3/5N779za9x4MYb6ff63PEOd+B2t78j6xsb3Oku9+Kv3/RWfuHpP8tkMkaM5e/+7u383NN/iazfq9skRFhfX+cud7snb3vHexhvrcug32dhcUU3t7ZkOp1E2y8f6pGmD3M2JT5pASrUzlXirxYLfyfTz/R2SoWMGLFrPDdI5lKoB2PQjZMUH3sJ/cf+EzKrQ2U0rAUG9/otZt/7KLq1ieSDtPFNGvvliB0g2RLaW8G7s+XMJ94Ns8tw3ZuOUK5fSy6bquWmqh+r6EwJpYovvQavoFNVThLCDGUd1SkmTHGUZLlnSwIrhxQWOdUrANm5R0kzy15PyYaBjUmYOFuUTjaM4ahquIYQrtZQHlTv10MoZ+oLh5sFyk3V6Zr2+of1yCcO6InvFOx79OksXXwbnFuGfFE0W4yIr+0lCaatrcQrdD34UJf7vkXllHqE09782xnBPgTy0Qof/ei/8epXv1L37d2Hc/HmDj5OANZOnqAsPVlvyGRWcvzEscTQi7f8rCjZt38/f/XaV+uHP/Qv9BdXIwEwaMyjDxoz7n1oIrTbAplWb2KyAb/92y/hisu/xdLyKtNpLJ4UYTwZc+LECfJen8FwpMdPrnPs2HH6vR5ra+vsO+0MPvivH+HCW1zEZz/zKf3zP3ulnnbaflzpKoCT9fUN+oMh93/Qw3jq054hj/upp8g551+sNx06qtNpoceOHuHBD30E733fP8pokGNMxnXXXcub3/iX7N+3X2ZFKZUJioiwubXJ+sYmJutTBjhy9CjT6VRHowVUtap8OqPAehlFmUake1dUXpXOpKA6bEQ6QZzdBEhfFQ1RtBPfTnLG4NUs7NHi2/+KufYDZP0BlJ7gBF/M8PZMBg98ieAnIrYnYqyIWJQshnxmC0h/Be9WZM/d78FpD9nHyS/MOPqlH6rNjqLFGoQxEf2fqXgXolGBn4Tg11B/o8A1qv6g4LcE3cJqmW7/wAF7szn//Ce0AKkNuLx6xaxiioCdOPFFKSFMUDcRQqHqyhDKMh4AU1U3Vsp1lbCGX7+Gg++7SfsLcO4TLwV7C+ivItmySLYI2SBqrysXYWPS4nIUZfxbFrHUTuZZaXYvO0BIc4eAxGCKfLTKS1/6Ul79p3/Mrl27sFlOUZTxINBYjk8m07iRVCicZ1YUGJuze88e3vSG1/LbL3kJtjeKjriRAKSlK3HeUZZOitLpdFY2/ISul734EDB5j8OHj/K0n34iP7zqCvbs3UvpHEVZaPBBQ1B1pVNXuhSzFZjOZgyHQ/I85z3v+QeOHD5M1hvwh3/wv3jbm1/Pvn378Bpp0gCuLDl27BhHjhzhxPHjbG5uUqaDbGFpSY1Bv/vd76hPB4/tjXjVn76S97377zj9jDMpSk8xK7UsnVbdeVEUzGaFli6wsLTCd7/zDdY31nFeKcsSn6K5K2mwakCdhzKghUdLXyP5jYNSg8hrJQzbwQxRYxQFeI/4yApUl+zHE4tP7JCtj/8updskOIuWDg1CWJtgz3sS2a0fTphtgu3F/EqTgx1BtoKaJbLFizn3qbdHCrjh/TcoxQ8Rt6G4sUqYpN6/UPVlCBpmGnX+UxPCFuo3QTdQ3RK1EwrvGPc8ZivE/v/dp3wFMK/En1PlD5Xe0UA+9YwLT6Fu5svCa5iFoJPg/ZaoP0EIGxrKmfrS48ugfqJhuqG2d4TjX7uOE58Zs+u+S+z7iTvhyr3IcBXyZcQOkwDD1rmCg+ECo4UlBsOF9PYiC4sLDAfDWgMg7Rjn2h/eND7xSCfVJ+sv8JKX/Ba/+As/yw3XXcPevbtZWl7B2ByvghLLw6zXY3FxidXVXRy+6Qae9+z/rs9/3nOVbBC14qkNGQx67Nm9xMLCcv0YV3ctM+j156w9m6olhEA+WtKrrrpaH/uTj+I97/xbRsMBu3btJusNUAwexFhLrz9gtLDEyuouThw/zPOf+yxe9IJfY2NcqJoMtX2e/exn6wuf98usnTzB3n37WFxaQmyOmJws75H3+wyHIxYXl1leXuXQwev51V++jOc991eZOk3PuaFUwy/+0mX62le/Qvv9vu7avZss7xM0jjDF5owWl9m7dzd/97Y3ynOf82yWl3exuLTMwuISC0vLLC4t0usPms4nWxC1BjVDwY5EM1A76oC9OxZuHRKAIHYBMnB2SLAjYvzkMK4Bk3wmB4u4677J9Et/gl/ICXYYS/58iM5yRg96OWZ1v6p3is0j6p8tI/1linK/nP6T92L59kNu+vCGrn//SnrZccVtqPgJ+KniS49zTtXP0LBhgq4b1aOq4QZVvU4IB43hREm5QUaBbMYKgN6P2FP/fyACxTYAuL9y7SRw9kgogycrHC5X8jAWdFMDJ4CDYgSC7MMWI/WzDD9V3LpKOVRrb5Jr33cjt7z0Qs55yoWsfes2+JMbmGxL1I0VO0uxSor0RvL1r3+dfhbBqH4vQxFGoyHf/s63FdtvR810bATaa6ftnKtJ4paNVvU973k3H/nYv8sjHvZQHvTgh3CLi27J0vIKvTxH1bN28iRXXnkFH//Yx/jXD39Y104cIxuu1Ko9Sa4x119/QD74gX/i5MmT1SEku3etcuWVVyRPeZ3L4IrtiveebLjIkePr+qxn/aLc+a9fzyMf9Ri5813uzplnncXCwojCB7bGm1x11ZV89N8+zD//0z9x7NhRssGyVmYbIoLpD3jjm96o//zBf5XHPPpR/MT9H8i559+C5eVlerllq5hx9MgRufLK7+vH//3jfPQjH2F97QTZYLkO2tRkT1aGwItf/Bu8613v5KlP+xnueKe7yOLyKgujARsbG3z7W9/g/e97Dx/64L+wd//p/Nu/foBerxeBwOBZXlrSH1z5/cipJ8cc/gIhP4TEPCxCPkCOfBkR27EaFqQbEiJdyy+98bPI8gVomFEGhd5AdO178dKo7kD1mNFu/FdfT+/su6K9/fjg0GBFg+J2LdK79QOZfemfYBjn/dJbwbklli+9C2c88RZsXgkHPvhDst5B1ekYKSPtN93+Tim8qp+ielzVTZRwQIUrhXBMnB4svVsnkxkzX2JtIDsZ4Gz9cW5+/m8pvP/vvl96+/4GNoWL1gxrvZw+PVzPZia7OJDd3mD2Y8wtxGRniOT7sfk5YgcD8gWR3qIl223M4mniylvIOY+/N+f9wirX//1RrnztP5Fn30U3D6mUh6FYR/0UQoHbPCEUWw2bLgrmFZth+8PGQky3pwXXMd7tg1cbP7/MWoqygNmGAAwXl1leXqGXZRRlyfr6mk4qd9t8gbyXJ6ZbnQmMiiEU06htqJ8qG9Eq6TU69S6u0soe0TqX3k034kTO5uzevYfhcIR3pW5srrO5vpbWeJ+83+9OQSrZrrWUswJ8HBsurexicWlZrDFMJ2PW19cpZpP4GLIhed4jBF9HF7WgOxVjcJNNgUCvP2R5ZVnyvM/W1hbrJ48BaDZcJfiSUGy1itDKyLSH6Y9QDYgbR7KXVOIvYumdDbYt2hSbsmOMOOUETDRWUV8mMH8I+YBuEkv6fF8gNpmvmjx+noL0lyFbhHwRevuRwT683JJbv/TJsnqXZb7/isMc/fLnQm6uVt06jJkeR90a+InHTyfqpj6o29LgrxPvxqr++6j/FIETPnAYmR5n5ksyV5INHcMtx4EDJR3/4h9ZgZ8yFcB8gIvEEcZdJC2gwHjN4a2aheyYqv4giJ4UJcr8xIuqLEsokNLmItbCBjrpaTa6gYMfvUZW7nQbTn/MXta+flcOf+YYWX+ChimSuVrmmY+WVIajRBTSjoVszRuIRHFkm6Colb7Rqgqqpe6Tdt7ke1RVmZaOyeHjjd40y8gXdscQDO8by6oabIw/yPYGMQOBtve+1BHh7cejOxyzmg6vbLiMkSjTPXZ8HcLx+AnWVB+rx6GdCJL0ULwP2DzD9FdQhY1xwcbGoah6MwbJMvLRakz7DTG+K0Zfpyyc1vOnIZANF6NjsnMcPXoy/mpZRjZcAZQQHGIs2Wi1a4pSnSIhSny1t9h6bRqwr2ONLu08ou4RIFWP30+27iKQjVoHhjY+k9VXS5b6/GTsKVkEmLNeBJzzEZqtIP1lSnemnPOUB7F6j2Vu+sdNPfqV78ax3+ZJpFxXwgRwihaqvvQhhJmqn6h3E0W3RMM6XtfE6gQJU2Z2SlZ4Bqslh9Y8nObhwI+F/df+Y3/MVUVaZpfA8ZmwZw1ktxJ64vs5mfNOQpiazPYUyaNgXYYoVkQyxPRi+IIVyXPxLjA+sCj77r2LxUv2cPTzW7jpETGiiFYJq1FCpnVCbMs/es4RX2gWltT7U9q5T62TrZ38InWgiDGCsRkmyzCZrdlzncXNHFLNXEJR+nydu+9FpJ58SbPOkxq1icyqwkyttZgsRzKLMbZRytENrhAR0bkU3IqAEBV8FpPF/1e/T9DoZhxtMKXOxGjn6yWTvEjFFInPSZ4nu7LQ5Dy2kkO647wmYrwduNGVb9NF/itZYKUDaeX6SWdCkHIPTCu3r5oItfP8ok96svbOwGZINkCyEZotIf09OL9PVu70IC563h2YXuO46q+vwJSXI7MjyOwE4jfAbwXCFFwR1Jdj1E9Qt26CP4iGNVG9VoJe7cTP8GadLGzRW/AcGjs418Pndyr99f9vB0D69/kCY2EfEJaEsCH4PAu5ZFa89WpWjGFJlZ6iu4CBCD0RGSDGiLGCgumLTI8ZRHfJngcu0Fvew7EvHlJjN5FQRteIyBZJAY7thF2dx4d2eLCy868j0pGfdtBPbVHXdkouki5RkfkyVaQVU0STxUezMOv/GGlFmaPzJ5WyQ9pX1MNVxlct9kOz0TqbmHZ8YPPx6nO0PUJN/26J44X5Q7R93Nabu+HTI1KlibQOKmkdzq3nvXXQNCdB80qqdg/b9veSWsFnkhGJdIlgNcEngckmi+GldhDj6rJFyHeh2V7M7nvIxS+6H/mq5YevO6Tja79BFm6EyVEo18BvaaT+Fop3IQS3ifotgtsEvQnV4xr8dcaGG0MwU3KzQX5ySjZT1vfMb/4faxXw/1EFcH6sFY/PhOWR4GeGzFo0mCCiFlOqyoYKm4KehsgiormIDMVmRoxNdVsP21dZv1IYnrlb9j14mXBsWU586wC2P03dUqhslucZtVWpF1/2agF2uEGmmTET5aHGthZMdbMY6RwG8RtmzYKrhUFJqmJsh3hkjMG0GYjpfdqacVubISkDQOotS72IjbEiqQKIwh3bbDKTphhGMMbWD1aj7ZiIyaIBaPX7WNvk3RuDqW7Aeq+aVlJv+vm2CcuQuTLdWIPYrFOyizEYmzXPcf0+I215raTnpnFoqH6PrFZtVs7OYpvYt3ZCU/X5xthOnp+xUb0nnefepmmAaZl7pCh400OyEZLHzU9/F04vkYuf/Qj23meJG961qTd9/Gvk5lqYHlcp1xS3qYSp4gqnrnSqbirqr0T9tai/RoJ+R9VfKyLXu1KOIDLGbY5ZHRRcMwqwS+HaHyvw9590ABQCM2H5uKALwsQbsAYJGkRLFd0yPozBXKiwG+iJmAURY0SQKEczSe1XyOZVI3bfaZfsusde1r6lMj50GMlD5I5qgRDf7lQCdUUueO/ElzOxvX7cMKr4MtpBu+kYk8fRoptMCdMxAcXmPUBwky3CdIKatFFFcLMpwUQnXD/eQLI8hUSY+PnBY7JomOmLGWE2I3iPyXuxv55tITYaTAQN+MmY4BySZSnivKkK/q/23jTGtuvK7/utvfe5Q82v6o18j3x85HucREmU2GrNojW0u9vtJHCShgOkgQBJbMuB4xhIgHzKB3/OB8c2HCQO7A9xYCRwgiRIDANOYLe63XC31CI1URz1yEe+ueaqO51z9l4rH/a5U/FJIimJU2oTxaq699W555571rDX+v//K2lCR8PmmCE/Vg0R77N0XWNsqa7Q0QFa11knT1zz2n00ZmKMeEfK03eyJmE5QssRSqMuZORjhyKfg3OoJtKwn88/FPPbI+dIVYWWw+b8i+a8EmlwmGsAjSNIsbkOarjQyo+N+uioj5IfEyekqhIdDUUBH1qYKbEcoFWJOocPrYbAVOfrIZ5YjfJ7AUJoIS5Ql+WkyJtU8aGY4fRnRyDOZ+eV4cJQLCHFCdzCulTleTn3b/0mD/zeAxz8cc3Vf/QjnL0Iwy2TuAuxZ6ZDkzS0pqVdmcVD1J4zS6+b6JtYuiqwlbzsIHGfKpV0OhXXfYJT+jNgv/JhcQD3ONlrwC2DS3BiJOiykLyjlYRayKEk4c17dfIoyLqItMTJihgu33h5b2lquEIoD5LUd1c4+ZUV2XjsDFt/PCBWmzgSYlUDAbMmI7BZzr5oqjl96jQPX36c2zeuifjA4uKyPPjgw7KzdVse/9hTsrW9KUVoyRe+/HV5/JOflXa7LXdv30DrkTzx5NPymS99TXr9gfR6h2L1kCef/iJal/T3tnn6i7/Jzs4maqDliCc//Xm6nS57W3cAk6c+/Vl5+gu/IecuXJQbb/yEFEuuPPIxer1DkkKns8AzX/ttLj54mRs33mgAME1kT4mlpVW++NXf4eTZ83L75ptoPeKhK0/IYFRy/oHLhKLFYH+Lkxun+OLX/g3OnLsod25fJ5V9HnzoCr/2pd/m0pXH2dq8Tdk7kIcvPyGjYV/qUV8+9dln5KnP/YYsrZyQ2zdfJ3jH5Uc+xs7OFs45tCo5efocn/vKn+fk2XNs3r2JmmYn5Txal1x6+HH5xGe+IisnTsnW9hZmxvLSinzuy78lZ88/KNubN4ijHo88/hS/9uXf4sL9D8ndu7ep+ns8/MiTPPn0M7TbbXZ2tjATufTQIzzz9d8mpsTu1m0WFhd5+nPf4PLjT4Mpezt3OXPmvJxYXZf9/V2xWMsjjzwhn//K1yWpk529XUGTPPnxXxNiJUVRyKnTF9jf28X7otkGTFP/3G1YgLAIrTVc+wRVPCfrn/4aj/8Xn6K6k/jx33mJ1Pshrt4yKbegPjBsaMQRaJnQujJNtVnsY/ElUd021T0x3RKjr+ghZRzgO4n2TqS3nuBf86tM998HJOBRsNCScW0xSxyFYWJoNe1QYjJAXD+66kDUnnfos5j9wCxdU0tbmqo+sVTTgaF9s2rfinDDNv/0VW78HwcsfazFo9/8guCuIO01KDbyni10wbcNF2yMFMw3ac2lh5/gi8/8DkW7S0pRuu2Ch648iZjyqc99DRc61LHm1q2bnLv/Sla4iZWcPfcAlx77NC/86Dme+bP/Lr7oonUlH3/6y/L4xz8jG6fvkz/3u/+RhCZSiQ986av/pjz56S+KpVp80eGN69c5d/4i12++kRVpUuJjn/4inXYbHQ74/Be/zrCMvPjyiznCNhwHcR4dDfjsF74qaJSXn38WX7QxNR5/6gt0uoty3/2XZHVlVazq8+Dlj0ko2rJx5hwPXrqMGWzeuc3qxll2tjcZ9A6wesRjH/8MS2unMN/i+o03OXff/dy9c8PUjOCcfOxTn8/ZlHNYrLnvwiWKwvP61ZcaodY8llucYHXNxz7+NLtbtzl56qw99sQn0cG+nD1/iY3T57j64nPEWONbXW7dvMGZ0/fJ9s4mo6oUqyu58thTcrC3KXfv3hJ8kG53gWe+9uf54Q+/x97uLs45hr1DynIgJ9bW5O71V9GUOH32Ph668iQ26HPq9Fk+/tQX+cFz3+GLX/4NFpdXSINDHr7yOF/7rb/ImbP3cd8DD4vWdc66mpRfvM9An9BBigWkWEGKEyROy8LFz/D433jaWg5+8g9uWXnz+xbSHXP1ton1VGyo6FDFyiQaVVUHZmnb1K4LekOFmyLuZhJ2orNdTPv4uqaVam61FL77K+/5v18OYOZNfcugmxVO2weJVh0ZaqQ4rEmhxHQomm6YpKuQXkftjmncNauHliq1NILUR+IBlPu0W69z4//6CVt/MGLjGye4+Be/JnV9UaSznp1AsZS9uG/ntNN51ISi02VhcYnN29e49OgnsKoGU8pySFRhOOjl/aoruHnrJrfefIm9u2+CwuqJdW5ef5XXXnyWQf+QdqeNLJ7gzs3XWds4wyc/+zWuvvBtilZLdNjjzH0X5M6Nn9DuLtjS6QdJCNubt7l27WVuvHE1I+VaXapqJGoqWJKV5QX5wQ/+lNuvvUJd15Opx2aG6yzw7Hf+FZ3lNR574lNYLKG1QNLIqL9Hv7dHShFwUsXI+slTLHS7DEaVuO4q/f19bly7yvU3fpKht62lPEmn4bxv3n6T62+8wubd61k2OxSMBj1JdZVT/VYXFSfn7zsnpzZOEqvRWCt/UnCpY8nNm2/w4x/9KasrKwKJ0eCA1dVVHrj0KObaSNHhcPuO3HjzZW5ef40UawhtxCmPPf4EK6sn0NGAxU6L27fe5Orzf2LbO1tI0SaZk+2dTXa2rjM4PASEetSnjjXgOHnmAjt727zx8rNsbd2m22mBKTfeeIVeb4cnP/V5BoODCY8E7zPjz7UkF/0Woan4E9aR5Y/z8F//Mq3zBa/+T/u29b0fEcKm2XAb6n0j9c3SMLNZY1RSTGapb9i2YHdU2RKzLUn1LiKHaBxQSUnoJMKuwqJyz9lW90TafigcwM852dUsdRy6iusp/UJxpVJbLZ4Bqj0R66NxJBpL07qyVJrEUkkDI/ZNUs8k7uCqF+2Vf3CVw1dq7v+9+znz9a9SleegcwKKdfBL4BfBtXG+RUzJLjzwkJ09/yBlXXH50mXQSDTHmbMXuPTIk5nXXg5AoHBGu93OkaJoM+gPePihK9z/yNOIRUa9PcR7WkWL0WjA6toJtm/foLu4Aoo89sSnkFDYiRMnuPLok6ZlacFBEdoEl1sH4gOLC4u4YgHwKI6nPvXrnLpwP8E7DDcpLOI9SLDnvv0te/iJp21haRWqEouJiw9e5sKFi+BCHqvpHK/95BW2t+5w/uLlDD5ywuLiIp2ltTx2zdQcHmIJmiiKgoXFZbxIxkyY2anTZ+3+yx9jYWkNYk230+XOnVts3nqDEMJEVTnz18EXXTt/4RKf/NTn5LDfBwLtzgK9wz2uX79G0VlquP9qzhXmRcyZGqFlrc4CL7/wIw72tnAh0Osdsra2wcVHPsnJM+czFl+wIgRanWWjtdAU+EKWN3WO/V6fc+fOc/aBJ1hcWKS/vwu+xfrGGb773T/mwoOXWV4+MS3YNqq+Y24/YSUHkNYG2nlSHvqrX2X5Uwtc+9/7XPunz9EK16Dcz3v+OIA4NNLILNVqWqthCUuHaNzHdF+wfbG6H9WNSNWIMtW4dqJ1kHL0776nUf+9qAH8lHZggwmgEvZrYW1fYBmic7SGDjPxSgu8mRAx6YKZCC3B1jBxIk7EOSc4yTDUWuregP5rK3LqcydY/7VT9F5tS//2lhStKLkqlmTSV0+R9fVTvPTCD3j2T/5AuguL7BzsMygzlPjKIx/nO9/+Vxzs7+Ikj+RSTWzv7hLaHfYPDmm1Olx+9Em+8+0/5PDwAGeZUvvC88/x8vPfRRHb3rpLbVmf/w+/9c+5+uqLLC502bx7K/fZBbZ2tjNtOE8nYmtnBxXhxpuvceniFdZOnubGm6+hTfEtqwhFzp07z5Of/gLff/Zfc/fWm0irYPPOTZ781JfY29/npRe+j4kjViPuf+hRGZYl3/vOHzYKiA7fKtjb26MsR3n6JsbOzjYxpVyFx9javJuBO2Ysr25w8fKj7G5vcri/TV0OOXXuQdZOnuPmzTeyrFnTsjMQS8oDDz3G3bs3ef6Hz0HRQc04c98Dcubs/Xbz+muMRiPEe7wTdrc3qVVBPN4HLlx6BAw2t+6SCPQHAz7xqS/Q6/XY3rqVuwBmVHXN7sEB4gPOOQb9PgfDEb3BQFJULj/2lP3g+3/M1s4OrrtMq92V25tb3L7+GgeHhxz0D3Ghg0jW888V/xWktS6ysC51uiwP/6Xf4vxfOMXuH5S8+g+/T+DHSLlj1DtIfUAT/ZVUWS76pdI09cT0h1i8apquOU0/jvhdTHtEN8BXkU675rYkWB+n/+/Lkvf42I0TeEZg03Gx76gqR7kYiK1AxxXEYj0EWTWRDXHh44a/ID5cEec/6VyrQ2gXFIteQteJX8LCsriFE6R0RTa+8Fl56K+dodpJvPA3/4jB639E4KbpcAfSQfOBDalHhzjyYMuqHFIUOUWsRwNINYSiiWxZ3UZjnamiDdovjoZZfKJoEYoWYBKr0satiSi1+gAANTlJREFUL40RcS7LiI+G+HYbU0VjxLfaWQizLica/gCprvFFHgGmscbKUR6C2e7gxNkEpNh0HKhrKFr4Vu5MaEpYVYIIvp0F7zQmrBo2aOCFSZsy1TUSwkT4M9UVLhSTEWqpGuXuRNNyjKNhdlKtNj74/PpVCaHI0GqZjF3IzMWqasQ5wLczj17NsNGwGcnVmZ5LWebKfTNPMJYlxCpf21YH5xxVVebXK1r4pmuijY5AaK5no/iUr6FBrMrJZ1S0FxDx1EkJoaAus/MpuouIayGuhbkOFMtIsYEsrMuoepiH/r0/y4N/+RK9Hyrf+69fND38Y1x51yi3IO4b1QHoIJFGiVirahya1qVZOsDSP7eUroHeUeX7qA6oBiUFI1qLkc2DBGfikb3/rxz4835mADOPXQOegP0aNhRSV9BCqEqh470izokUJm5BMC9OloD78u0vTsT8FNVqmAXxC7X03qgl9tc58eVF1h4/x973EtXBjjivjei8IpYIIUxS6lDkYh1mhBDwrXbucZtNQCO+cRATjb6iRWh3Jmg7kNxSanrRPoRm8Lzl8V3kybcuFJO+vw8FkzlaQv77Sc88WOh0JbcIZVYcRMDwoYVvd/EhNOCXPPrctzvmi1bDws1bAN/u4osOUxokk9bl5CYoirlBKa5oM8EbCtkQW61cBDQzF1qETld8lk6fIvwaREC+Pt3cSmWKLwjtDr7VbtqJTK6lODfBS/uiyH8bCsb6ij4EfKvbtDwzxNc7n6/zLIIxhAnAJ7Q6hPZCvs7NY74IiOX2pi9a0ER+fBeKZQgb+O46Zf2APPBv/xaX/vJDjK4qP/pbr1i186z5tEk2/gOzhuwjVqnFOqG1msaRaeqZpR2HPm9mWxLZszS6g9ca9TWdbkXYVXprCX7wvhT+PgAO4IgTWD8ACpDKmkGLaoTaQx9hLwc+uWBiXrIOs2/Qfhkf0IiChvaI/ZdGIqNVTn1tmfVHz7Pz3Yp6uC8u2ET9dYJgwTLhZIIkU4wxaL4Z+DjriEUm0Np5lKFMnRGzc4Zlhnk4LzwgRyGwNgu0mdHknqJ/GjRvw3LLBKV5WH5+bBYz20CMbQadN4dgmkAVxyg9m8PgNjTkGbEupsM4xOZwy+OEwWUmg01gPFPU4FiDf3J4NzPIeQY9OKvcJEeQXFM48RyeU2bEOycTlcfn62bRf65p9bVyn98vZDnv4hSuu86ovI/zf/435Mo3H6G+o/zwb121wfXvUNhtbLSDxEOTeGh5wMfI0DqSqmQa1UyvYelFQV+zpK+L2Y5Y2lVkD+8ilBU7lPROjY1ff0q0t4+qA5h9XCZO4GAorPfBFmGkGTRUi3lnRjJVZNU5HhXoNJ4iiOEymaeBkVkU0SShU7P/kkkRTnDy64ssPXSenT+tSNVhLlZrRsfKWCx+xrinoFY3pzclR/4/d+/d87ub4RXcE8I6a9f3OIxM/oNZ1RtsThBjBjJrE6D/7OtN4LA28/O9yfRHEL0TzsBcdHc2c+4yi7efhRPLhK0gzWjxed7FVHNhnkQmyBFY8uzP7si1mHm8wfDPXvcJrn/MJmy+mxtr+HeRsATFChTr+O4Go+oC53/zG/LIX38C3Yfn//Z1Dl/9NoXcRIfbUB+a1AdIGkDKEl9oHbN6iSYs/QRLb4DtY+mWWH0oTnsquk+ZaopQM1ytZ9J+3s/o/6t2AG/HCQDXBM4KB1FY7YMFqBy0TVXM1BOduDbYA4K5HL6laGhoDrFGjTvrPYkrcO2KnR8bRXtNTnx5keXLF9h5bkgc7OOLRjWUWZKQzBigE47cxjb3vLzVWGYMN99s865i7vlZLPysgc/i5WUeWjtHfJkN3vPGPzn3+ddxdsSomQNFOzkSWWfPaZwFuPnzniEUTYg74mazlXnCFfPnP+FBwJFze4s3mpm/N3uuzOD43ZHzmnnM+dxmHQ/xcEXTEu4gYQkr1jLKr7PBsDrPhd/5DS7/Z49L2oMf/93rtvfisxRyw2ywTYb5HiKxn6dZWWmmtaJ1baTKVKNqelOwXVUrnaXrSdOBGgNq26VFye4gwrJmUNxbUv+fP2HrI+QAjjy3CVw2esEYjpSNE5E4TFRSU/ihJdvHyUjMdsGNgGBQYuowQt4W2ji1zybh+2w+V9HqrMnJry+xeuV+dr9fSxwe4tuCRWlQgjTeYyZ4zu6Qj7DT5u/vmcjOLDGGOaPmSDRnnlloE48hRwkqs2pFM+SbiVGMiQAz6cY067CpQ3HzEmPOySyzcWbQ5XyYdvc6n8ZJygwJd2rAc4ZrM/yF6WWaNezZs5q7LvM8KGHOAeGOnA8yF+VlVr1XHOazhh8TeO8iVqwgYQ1pr1PWl7j4u78pl795RdKO8vzfump7L3yXVrhh1t/E6j2T+gBi30SHGKWKVokUo1kaqqUBqn3M7opZT1RHoK+Zun207tNye+xWFSwoPK9vo21+RKb6V+cY/PuQdfyUN3MLeMKghrMDQ1dB+0LlgLZzpGXDPCIdsBUzC3nEmHSakS8i0jC9LSEpii9K2f2RUbRW5fTXllh9/AH2fgDl3h6ulefGC5oZrnNmb4Lci8I3z1Sb3RMfjc5jwzxKlHlLBjBOpIV5aipH09wZyqoLzc0+RjcGkZkbXsZ97VmMu4SGxeQlQ159cywv469xxJxGTj8DkfWN4QeZI86MjyNumk3NxP65dJ4pmaihN87kMjMMwdk0a1aqbZLau5lr5CaPyYwTmDggVyBurN+/CGERC2tIsY601qn1Cg/93m/x0H98UepNeP6/eZP9F5+lFW6ZDXaQeteo9rA4MtLIREvEarWUkllMig1RG5qlSky3wQ4N7anKa/i4j6NP0eszGE/4vWf0/ykt8w9/BvBOnIBNagI7pdDbh+UFgS6EoTOVlvNES6IIQXIi30Jo51GS1tyZCqZCLhLgipFsfa/GpSVZ/9ISSx8/z+HLntHmgfiOwyzk7cAk30XmxfmPpsZHotFM1JU5xpzMyFXNG/bcXleOyljIWynK40qkCKY1xL6QKrFUCakUSxWWyqwwFMvxz2JpJJZKsViKxRGWKvL36b+zOP6qMhgollidf89f4+ea38fP1c3PzRexgtCa0R2Yv49nqb7zycrMY9nvyuy/Neav51xW1PD7J45OZlh9bjy4o5WhvWEx7/nDGtJZx/w6qXhMLv+lPysXfu8co9eTPP+3X7eDV58l+Ftmwx2k3jGLh0bdI0N9y4TFiMYyK9dq39S2Ie1gtm/Y64503VRvmtjrkA6IYcAeJWykI3t/fo7RvycOILyHxv6z9jszsm7fAp42qIywq2gSRlLTtc1U0Q/eDlTTgcFZ59gRc32hWrFkF8AVE4ZsLZLHQSq+E7n6P5dW9z7h7v/3T/HEf/V5Xv17a2z/8R9RLBTY0COxlceOpSIXu1Ilk4kSzWxNyRXEmVr/+C3pTBlrRtZjIlJnR9oJzJX/pbn57UiXYKaAYLhs/LJwQfypX8eIjaH4yeuKExvXK5wIainrYDufZ+eM6yVuXDTLz5klBMN5j8nMjD5xM/poNi0ziORpvGi2QRdMB/uUL/9hHthBY8XOZmTOZ5yZTUCoNqeD0AgdzBZZJwIsMltPma8RyEyGMM6AbFLp74DvYH4JcSu4ziqRExLWnuLxv/olTn51lcMfJfnx33nBRre+T8tvoqM9o97H4qER+walkcraUj1EY20WD0XTHqYHzuymme1gDMBeNkt3VNMeddykJUPau5HembHx/zx5L97rmkB4D41ffs4bn3nD3zV4WmnVgraFuJ+IRYWvfbQ08skdmpNgygbizuFMxKRER2a1ieSBa2JiQooIkdail+v/TCz2npSH/sPTPPZffoyr//2C3Ppnv2++A1IXULWAnmEBkR4khzVOQMYtRNNmEMl4/JSBuUYQxOZagmMhEmu6ejI7+262UCgzLTOYiJDm/bVN21dphDvzDMWn/6a4dIBJqxHosFwGaPlxQXTiBMbZsjjJ+Z6f2k/eu1vefzQZveZBmCZexkV/fCFIaN6j5OljmGBigigiAco9dv7uv0Pau5V5F/NSnT87tk0LsEeyLrtn5X92OIjJ2JE0ugIuYK5ocP4dCF3MLWF+GemuUVanWbz4aR79G59j+ZMddv6o4sW//5LVe89RuG3T4W4W9YiHRuobaWCk0sxiNGJl1AlNozzN13qmemia9jHtqdk+yXqoDPDtmuAj23VqoL7vpupv70cq/m6OYb/E15cpT+EJB33HmZFncKJDmxYWVj12Dt9aFufOi4Qr5vyaiP844i/ig3e+VYjveik6zvyCk9aq0D2FtFddObrAxlOf4OG/fJ+EU55b/8sdu/qP/wVOX8fpPjbaMUkHQtyH2IdUYePhcuOJxGbNROJGZKz5XeaGWzEBsoyHj9rECcgkYTh68eyIAMiMGTViQE7EdacGMNYnG+OJ5+6bI/gDcbMHNY7oA2VA1Hi23oxAyQRrMRVVG6sdNXyB/NJxNJFem3lDEwmwqVccz/Izm+8WuBmltKnq71ynYqokNKNHmOsi5sLMfr+L+EXMLebpPe11quqsbDz9Oa588+N07vfc+qc9Xv3HPzRXvYBn32y4hdT7ZulQiX0jjpRUJtM6msaRat1DUxJNNxG9alF7ht3C4paa9ajjDUz2CLGP87vsDmtYTPBq+iloPztiQ/JeYwH8B8iJ3MOpNN2BvhknOkocJlBMQsuZGI5SYU+cHQIdg7aZlUAQw5sqguaOtUWxGM23Rwxu9OTwxTYrl1Y49bUVWbj/ohy8JFL3B4SWyPiyGDZTsJptbdl8MXCuNDC/v59q3s0ga0REnMx0w6YhMnfX5nsQzEBnELBUIlqB1YbloRNitZBKyI83EmkVk8cmv1egZUP+aZ5PJaSsUixWQ6qRlP9GNNcNmHxNawXTY9SQKpybkT6eNhLnbNqm9U+bq6WIE5lp+U2uo5v28cXNqPmMi4ONgAe+ndP9sICEpTxDMqwjC+uS/ElR96jc/xe+ysPffJSw4Lj2j/d4/Z9827y+gE/bWLmNVHtGPDDSQIlDs1jVpvUow3vjAEsHaNoXs59oSi9h+oZIfFNE7hi2haYdUnUIccBBVUFb4TV9FzDf96wd+H4VAd+uE2i6A5dg5Y5DF4Sy9jjpZKq6IeZxhjPshMEamIjZomAtMUPEXM5XE6JRSElcN1Hv7srOd4VieVnWn1ni5CcuMLjeld7NAa5VI0xlsXJq6uaaMzIHjHNH0tyjAqCzoJkZgcu3dhLnmgqTXrvMAQ5NnJOmzSUz0ngi4mzWQLLn8zPtMZnKX7lp1Xz2i7m/n9HRk6nxuZnW4LT3LnOgqqa9MdEKnQEJTcuhjTDbjKrnW4t8c5X95vzmdPvGEX9M412EsJqr/N0Naj0nxclP8+g3v8T9v3uG8k7kpb93w27/q+co/E9Mym2otpFqD+pD0KGRSpVYqWkdzdLISAlLA0lp11T3Qd/E7E3B9kjsp2Q9au0TXY9Qlvh2pJSm57/5buC+H/kuwDvsDtwSeg8Ya/tCWs8h2LloopV3YWiWShOJYtQi0m9K+1nYT6wAdRn8Y9IwRcSFSrTcZ/u7CRuuyInPLnLmmXNgG7L3QoVR44uQyyTON/Y/G3ncbLNr8pmZvEWBdwqfnRXhZKagNts6nAEG5Z29zL2GTA4+DSJH64sz4zKEI2eJ3OPusnmYwFsbVONNiMzLGc99cjPpj00bKvPgqdkOwDjld8xG/nmxzrcafubth4bB1xj/mMNfrIBfxbVPQHudujwvJ3/tczzxn3+G1acX2P1OxUv/7avWe/lPKMIbUO5BtWvSbPcsNSO8Y1mN036zuItaTyzdNvRlSem6oa+LcVOM/eTqLSTuklKPjuuxGEds+RreTLCp79L4PxJAIPkl/U1z9z0BPTXWNqHqKpVFnFQqsTKhdNjQzHrg+i4HZclQeRbBwkSyGhWxKKQIEnH+UPZfHNF7vWDpkUU5/Y11li5clMOfFJR7h/iFApECMzffihqDT2bd9Tgi2kxUnAntMmMUk9A97gLMof2c5KqdzB+Dedz+zJGasvv4grmJEc5Z/GQ+wU+53HKv+DN9fZnD6c3Kac8j/t56nWYlfKf9+yYlmVyr6ZdvcEhN2p9xDFmqq/nKxt8Bv9C091aRYg23uCFRT4hrPy4P/sWv8tA3rxCWHW/8bwe88j8+b2n3+xa4BcMtpN4z4gES+5iO8vw+LRWtSk2xMkt9VO+KpB2zdNXMXsD0thg3k9iOaupTyg4+9mi1Snalwfm/kKZQ03dc3JMPWw3gPYIOX8tOoF/CYnLgjRiaIJnw4gqgjUgLZEWgi9AWWBUsjLUBxUzEEljM3zGk1ZfB9UN2vmO0V5fk9FeWOPnFi6TRGfZf7YlJkw1YMUWhuXtBZJs34GaNwfEWjLvMw2KZgwE7met7w1shsRxBJU4Qd06agsd8ij4LLpL515xa9BFI8NH6xlyDUu45mXf+dY4CpFzTihiDkWYdnIxBQZPzcBOl3tCIdQRw7Vzdnwh3jPf6WbePYoNaL8ryxz7Plf/085z9c+sMXou89N+9yc3f/5EV+iKuupULvXEf6kMj9sGGJlqqaGWWKjVNZVP97YnFbTPrYXoD1TcFhmLsqukhiQpJfVqdkuJA6S+PST68C+OX98Pg/AfU4H9KTeCawQ4M15X1AwVRajSX+yyaycghAwHfoGz7ZrQwE1OSYB5JzUCZCT1Y0BoXBhKHB+x8Z0i905H1Ty7J2W+ckM7Z++lda8topxTflXzzSWhSUTeZSDwmpRiz+HiOkmQauL7MynxPdcYn/bimbD5T+0OO1Bje4kymBJypMc0P0jA5ors/MWg35T4cGaZxdJjBJC1/i5HPavHP7t2Z5z24aS1C7skzaFiY44jvm6g/k+6P9/l5QOcq0t0g6ln8ysfk/t/9Mlf+6qN0zxXc+qcHvPz3X7b+a89RcA1GWybVjokeGDqA1FfRoUkq1VJdWxbyHJqlXVM9cJp2MNsE7ZnpdSG9ImZ7Uao9TPqEVCJpyG5V0V9M8FJ6G63ud2ofHzko8DspdvyU7sCO0XsANvqGOqiDEUhErcx0JM4FNfXiGArWsUwaMifWFcUhOq3PWTJSLaSIc5VIOOTg5UMOfmjSXl1k4ysrrH3+AcTO0H9DJVYRaefik1CA9zO4ece06HY07Z+FAM9MpRHm0YHzmYDM8w/vkUnwFocyc+XcPaC5zDMSx6n9ERrA/Gsxd95zWwpmgDrOvQUuPaszMG/07kjBT6bwYxcaw89IvulwjjySW/wK0lrFLayJupMSuSInf/3zcuWv/Ronv7FGdUN59R/c4c3/+1mT6kW83YXhpkm1Dekw9/bjMKf9qVJS0tzmSyWWes7SpljaM7MdM3YxHWL2pqZ0VZ320dCnGpaor+j0SwbrEV56N9X+970W8EHbArzdmoBMaMQrC4INhBKhQCgKvKaO4VoNkOaUwWIe9coSmJNmFPG4r4+lhk4cMU3i2yPKvR02/6Qn1VaX1SsLnPnaOicee5Bqb1UOb9YCimu3EPENDr9oqtNupmvQDKUcF7qYQf3P7OttZvDIZBDmXAo+O4zkrZBYkXnOAUdputNaQjPlVGbIPJPpG29l7s1U+N09jitzEV+O7OXd/Bj2I2n+5P1MOg3NJJ6mqi+uPWP4i+BXEL+KhFVcdw0NJ6RKF1l48Nd56D/4HA/83v2w4Ln5fx7wyv/wAoev/sBaxRtIuQnltslExGOI6BBLIyXViRQNi0ktVWapMtM+qluY9gw9AN0RST0sXrfobmJaE6zCWiWhrtkpqpk+/wcRp/OhdgA/4yJcA3aM/jKcLBVrGbGVtwMiQzP2RW3fxCVTS04YZgkRKRGLQECbWs3YTjQiFjGNiKtw/sD2X9xh77uKuK6c+OwS53/jLMv3PUD/VldGOyp4wXc6QGe6NRiDV1yzh3XTiDfFCcykxw2wQMTNzyc8MsrKZiL8tLEgMoUNz2ceb+0wGPOzttwsvGDKZpR7teRmI/oR6K1zb81y3EyPYjJCjGmRjzFRKcyk+0209x0IeRgnYSUP5CxWke6aWDghdX1OWqc/waXf/QKP/JXHWPp4h71nR1z9+29w+19+Hymft4Lb2HDfqLeh3kfrvkkcAKWajlRirY18V6kW+5j+Kao/EtIbWNrEdFeMNzH7MZpeT6rXCbZHHUeIDjgcjih9Ddfiz4D42gfduPyHyGP9lGPvGAcXYKMH6pvtgCl1rAM+ItYBdeZchVlwWBRDwZYwczKpimtTIIygihAxrfB+SD3clZ3nDjl8SWivL8jpLy+x8cUHaK1eYHSnS7lnUAiEABS5deiLZrZcYJ6pN46EfmwsNmP4R1FFcwzC+SGjmIjLEhhzW4d5INJb5mY2jkcmgh4cMWo3l767OWDOuJg3z1KUOa2Ee+Hzm+OM23iTARwBk7Hht5siXwcpFrMyr1uFsIrrrGHFSanTWcKJJzj/O1/i4f/kE5z+0gqjW5Gf/KMtXv8nz1Pf+YEFdx2pdo1yF+I+1D0s9o00AK0Myyk/qY5qcWSWKlT3nKV/jelrpnEXsz7YwLBbjvhaMnZBDqh1SIg13aqif76GF2Yr/R864/9VOQB5bx0AwAPCQYK1Hqg02wET9eac2QomnYZus2hiLiPsbVEkW2EujyWEBKiJJDGNkOpcIKTGF4cyuLHJ3T/qM7zmZfFCl1NfWeHk5x/EL51lcLeQcqcWnBfXboNvibhCjFYWopBpxMvU2kmxTLJSjW+SAHdPDv84QsvsdMOGAP2W+sIRavEshEDmnNAMdc+5+RrAvfQIjqr6zBb4ZkBDNFFe5ijFbtLDxxWIazftvHGqn6v6+KWc6rdO4DqrqKxSprO0Tj4pZ7/xOR75K09x32+fIPYdb/yv27z8D19k74ffI3DVXLqLjXZN6izZTexBGoIOTazSPKyzjpYJPdFM+1jqm+qOmL6kKR0iWqI6xGwoaluJuEnSEXU5pLCS0E5suThj/PZhMfb3ImK/F1jmn0YlmXIHTi0GKl8Q+wXtzkmc3/DGCfPuYTF3HueWkbBkzi+IC6fF+wed+A6+8CLeiW87fOHNtQXXFl8sYWHR4Zcwv0KMJ8WvPMDJX78g9/32SVYea1HdUW79Pze5/f++Qv/664hsEfwhUveNOMxQWy1Bh5iWGWZLzDWI8VbExl9Zjz8nKhm8JPOYemYJh80c9HGCPccqnPskpiCeWZHDCfnA5j24zWGSM7Gh4QBMqXnT+gZzgzjHgKks4TjFTUwRfMWkxWeuDW4B8V2k1RHCAikto/W6tc8+yLk/c0XO/uZ5igcKyjvY1r/c48a/fJPhrWuEcBunB1AeZKNPfYhDszQCq1VSiWkdsbrWVEdT3TTTbdE0xHTTSAeobpvoC5L0ENNRMh0EsSpiPYblHr5dU2iVRT062uz59cMc+d8LB/CrvCDy09slTzjO7YaJ1LhnCWw5SLGkBZcwf58ZK4hvOyceHy4i/ted+K644MU5hys8UgR8y+FDfsp1xUIX8wtCawWTRUnVSVob98vZLz3IuW9ssHA5MNyGrT/c4u7vv8bhq1dJ/RvmwgFeKqiHEAeYjsBKxMpMn9WajE1IMw5AbaxtkG1UG2DjTNZpY1M3a4ZzTLt7dnRY+NiBTCqBTU1w0h2QqcTn0fHjR+jNMx7AZkZvM+MApvv8/KU0GYCMs4AOSAdxbcxnpR71i6K6goRTLD1wkZNfuszpr59m8YKnuglv/otdu/2tGwxvXMO723jpYfXAqPeR2IM4BB0ZqcRSrZkrUUOqStU0MoulqL5qqjcMG6JpS0z7KnYgZq8L6TBmpl+faBXBDYnDAcVCzc6ohpUEP/55U3x+0XvaPmoO4F5v6hd9s/Kze6aXPWcOA9VioLIOLWnjW51AOGfOr+eugF8huJaYXMT5L+H8qohrS7NxF1cUIqHABSeh5fBtEd8WXFfwXSF0kdaymFshVhuE1fs49fR5OfP1syw90cJq2PtRn7vfusne91+10d3rEHcJoY/YcErC0RGmFVgFFhGNkPJwGVyUsUOYGL7NjD43nR983NhtI8s7tdlZmLDZ3HW3MRpxcpAZsNE05s8VCZux5DJXDxjLb3GPUduNchHSyXt9CgitXN13XWJso2lJio3zrD/5CKe/8gCrT68RVqH3emLzD/a58wdvMrr9hnl/lyCHpnUPiX1Iw5zqxxFYbWKVWqoxrRStk2lSUx1qJvMMRfUFwW5o0qEJe3m2mh6K6dVUc4ivRyQ3xGtNmSrafkirH7mzHH8Gq++X6QDeUycgH4Dj2C/5tZqS+2UHI8eZ2hHXHFo7ioUOtbYJtJD2QjC3bMGdNnhE8CfFuYdxcjkLi/guznedKwKEID4EQuHEdRzSkjx4tCv4LrQWxFimjicIi/ex+vgFOftnzrD29BKdZahvKXe/u82tP7rB4ctvUh/cxOuOeT/ASQ1aZ+UdHSGZkWdQCzQZgUbyz9ZkB2m6XcAaiq5lSYK33p+WlVFsQs01E5vlBM9xD2eKjDbl2860Ht24bZGr9xMHMK5r+Dy+y/lcDHUtRNqoFHmvX3QRV0jSNimt4LvnWLx4gfXPnOfk58+w8nCHKmLbPyzZ+f277P3wDsOdGwS5jXdDox5g9QGiI0NLSEOzOEKo8iQUrWuLFarx0Cxui2qFaVRSJaaHZvaKqd0C7ZO0h6OUpAcJvU6KQ7xVVIwIsca3EjuHCToJrqX8IfzK0v4PbQbwixr/O9ETeLuv5SbTh+g7LiShXneMpCD1C9q+wIUuzndJft05ud+JnDAXHkN4yotbNHGLJr4r4gtxuShA8A5XOKMl4lqIbwm+nZ1AM0teZUFSXBEp7mPx4gU2PnOW059ZpXMpUAPV6xW7z22x+52b9K7esPrgNmb7OF/m1qNVSKqQNMryX5YaUZIsbGIWj2wXbKZ+MKt2PMXsjAeL5n/gJvNIskZQ4yImuJ4pyWlGGYhmXNsULyAec74pOrqMgxCPigNpIVKAFBDamCtQDSRbAFbEd9ZZOH8f609dYP2z51h6fJHQheEN2P7OHne+vWm9V27C6BY+7OPsEKsGZmnYzOEbAqVlynJlaIVYNEsxqtalaTRT3RKN1yFFU1OwykwPBF5R1TuYDQT6aDVKuH1SvUkYjSjbFS3KHPWdQasZ3vnj2X0/xw7gFz+G/Qrfx3xNgEpg5NhYDNT9Au14ilYXb22ktehFTmGsmvcPCO4JcW7DkJOCXzUnLRHpOPEdnG+J+AIXPL4QcS0RKQTXEpo2lrm2ELpCaxnVVVE2CEunWbp0mrWnT3LyM2vWPe9IERlcT7b/vW32v3eDw6u3Kbc3sbqHc0O8GzCBLKQKS2NlohprxEly1yKBRayR/xLUJvv/MUXXbDLWbAL9wcCNcUGO+ekHxhhFaGOR0Ukxz016+BPcgxTZ6J1vdPdbmAViHVDtglsmrKyzcum8rD55joUnT7L62DKdVai2Yf/FgW1954Dd799itPkGzrbxRR9npVnVx9LAJA4xy/t70crMas3aBrVlME9Ss1iapoGpRiHdMtUbmNZmOgL6WDow5Ceiuo2lUTIdkmSEap802sfHCt+KLFRVHtpZGLw63vO/W3LPOzH899wJyAfgGO8k+r8TIsWME3haYCic2w3EE4448lS0afuCtFAQtF14axvFoolsGO6UOPewiDwEdMiSuW2cXzNx58T5JecKLxKC+ODNFdKw1ASC4Dsiodug2JZEZYGUllA5SWf9FEsPn2L5qQ3WP7nM0nknXmBwW9n/SY+DF3bpvbrN4MZd6v1dtNpH0iHelzhfN/I/laDJLGlWKqISs4hoo5eCznQMxlsEnV7ESSFxnmyUZVR1kvYz7tkz/t4U/ZyD4CVX8j1KC9UWGj2qXSSsELqr1jm1zvKVM6w8cVLWHl9l9cE21obeJuz/eMDus9scPH/bhnc2sbRH8Ac462NxZGalOR1BGpimskn3a82dlGRoVFI0I0VNcYSl2jTtO0l3Lbfx7hq2KapDtbSJpV0x+mJ2Jxo9okac1kidqLSiYEToR1qtZmLvoh4p+PFLdADyS8iGPzIO4Jfh8X6WtNjM98ueCyNHueKpfEFHCmIZCJ0WSYpCbFEdJzBZMfH3i3AFWBZxniySt4STR0X8KfE+CD6IKxzOO5wTkyAiARHvmoxAGl06ISxgYcnM2pLSEvgNirUNlh7cYO2xdZYeX2bhwZZ0VsFqKLcS/WsDeq/u07u6w+DmDtX2NvWgh9b93EWQ2pxXxEVEVJwk0GSmTdGw+copvzXFQLtHh2BmhNlYl08EfJDGAZiJb2aveIwgagVYB5E2rrtEsbJC+8way5dOsvbwGt2Hlmjd17HWMtgIGVxPHL50yN6P9th7ddeGd+9AuZmNXgZgjbJwKo00MqRujL4yS2WO9Jo0Kx5Z/tmSqmmlGntYKlHbFUu3wPqGboqxDWmgibsicQ9zw6RpE9U+4hVXJ0ZVwnVq2lJOx3W/LeO3X7KtvC/tw4+KA3g7mQDTLcGB59SKp+oHUruZSiwtfOjklqF0TcIpcZwHWUJomTjv8F2DR038RRG3KM4tivOFSMg9Luea8eXBG0FwLXBBcqsrC1TicgFRQltUO6TUBVZx3XW6ZzZYfnBNFh9eZuXyIt3zbcJqrqfVfSjvVoxuDunf6DF884DRZt/K/T7xYEAaDcWqCq1HWCzNtEJIWXDUIUJqtEltZtZJrhUokuv9ipj5RvUznze+Ay7gigLX6RKWFmivLdPZWKR7bpHuhWUWz3dpnVkgrEMRIA5gcEfpXe1z+MrADq/u039zi3p/C+IOLvRxbohLI8jduWYf3xREqbMwhyYl1WapVtFoZkmxZKDJ1EpISVMaGHrHpbSP2J6Z7YKNxHRTsM1kOhJhL0XrI1Q43SPVQ1yVKNs1bpTwrTTV71vVI/Ldv+x9v7zfRv9B6wL8qoop/FzA0LnKUa146n5Be6VF8oGiLIjtViAsW0grmHrEdQy/4AldgzPm5byIPyPmHjQvq+DaIq4rzgXJs66DuJw1mIyRb0V2CNIWaRwDro2FtuBamHRMtYtqV0yWCd1V2utrdM6vsvjgsizdv8DS+Q7t0wG/iDmXSwBxCGUvUh9G4l5FuVNS7ZXUBxV1L6L9ilgnUpXQmCApppoFfsdXxAt4h3hHaHvCUodisSAst2id6BBOFLTXWoSlgrDSor0IrXb+tOoRVLvK4FbJwetDBq8f0r/Vs/LuHvFwD4s9RPp4PzRHiaURVg9BSySVBqVl0dWUU58UDauMlBJ54GZtGmsyci+aWYI0RG1L0JGp7mLpJ4LeRak1g6eimG2K0y2iJvFUddQaZzXJDQmDirJd0/ajadQ/mY6M6j56T9oH0YA/SDgA+ZA4gJniYKM4XDWgoW4ZiCGQXIGTJYqwHFSCZTxvB5EFE7/hYMO82xDxjxicBlkU59ZEpCWSh92La0byuGAiRRbtd84JLbFmko+4Qiw7gxmxi6xjD0E0tcC6mFsRdQv4zgKtE0vWWuvSObkoC+e7dE+38ettK1Y97aUgoQO0pq13B6Sp2O9EvVzHTQM3rQuKB+8mzGBUG83PAdQHiWo/Uu/VVFs1o62K0Z0eo60e9f6Q2D9ER4cGTXSXAWLRsMypEM1ZCbERNNUaI5pYZahiFjP2IVfwVVTVSNow9Go01WZai2kl6KFht0jaM7html4DG4IaqgnRUky2EumA6BKOCjdI1CES3JDKKnwV2a/LHPVbdo+U/2el/W+3WCcfdEchH0BH8qtyAvLWbcGME4hruTioXc9C5ak6HVzqYt4XwXuUwiQtGK1zJrLhnFs03AVETorZkjm3AbIg4hYRv5JBRd7jPZLrBz5rXE1Gc2URAVdgrtkuEHIxMVOLRVwhebJNCwioFagVmBaYdUC64kLHXGjj2i1ct0NY6OAX2zmCLxYSFgO+7ZDC4QqHawUTMRmXBVUtZ9V1wqpGEm+o6DBSDyvqXkk6rIj9AalspgRpZVCCDRGGOFfhqHME15hhuKlCNZpYbMiXlVmqjdzaNDSBJcU0kTMAM03WFNuHZjo0TaWgB5Z0oGjlTEvUKkN7wBbEnqneFeE6agnRRLIojirW1sfHIS5EaquotGY5Rkot6YxqikK5vpLgx3aPvf7bKfa9HeP+wGcLH0QH8B47gWfIeIGmVTgGDsVhILUC3ZZHk0ODJ9AKJhcUfwaRjvNuCWMRkY4hCzgWxdxpxT0iPiyJuJY4KTDnxblCsrZVJv+Iy6p9LnjzwSFFHu/hQqMxIAJhSqAht91MguDGbMMCE4eZR6QwM48SwILk0YkOw2fcPZLnqLrZGX4yARPl7kBqBp1EhBqRDEASl3DORMjQWpGcUjSEKUgVQjQ0YqY56lPl8dnWdCpUteHeZ4+jCmLRNNWYqmkyRDOm2ey2mN4ys0Mzu4PFQzMrMa0wq5yTUixVSbUSZS+n+1Q4i3WdEhKVaBXeKnwRKakoYk1xmDKib1VhqZlC9XOj/r0ee6eI1w+sEwgfQOP/RVOnd1hp/RbwjGYngHLHG+cODFaAkTEKykLbkyojeqKPh160CyFpmuhyRidWW/KloS0R61uSgChm4gTn8yQdyQQgcTYe262achpMQJw3Uy+Ib5JzaaC1DbbeBcnjunJ+34zvycdrmHfehclkYJzLYiUZuGNTvL5OIYCNMEcDFEKTYZJwDXZAAKKBad5+a0S1bmasG2gSsdqwaKYJEzNQy7TqaKiakSCZYjndMLRxDJpUNTZsKDNTQS2KsGdqOyKpD7KL6sCMOkv3SqlRKxEqUUugPVEZ1GjEUiKliDilpkKpSL1Ea7Gm1U/c6uhMkY93WeUXPkLrV9kFsPcYJ/BuawLM4wUAhsLFvmM03hq0AmkY6NZ5a+D9As53gmjHNLRxFDhrIa5j6k6Ik0uIrGXGi1sQ51oG6yLuNEIAZ7kvJwFc14lvZw0wPx7ZO1YHkYloB01G0Iw9penHG9MJwEYermHjmV5jpZ8GpjtlAUzFQbLxi2WFpAYxaFkzUZCM/THNxXCLeRJQLow3/QMl7+EVkWSqSSdtOlIy1XEvMvcnMzCnBMNUe4LtY5owImYVYj0xbpjTHdT6CluCDvJkUxuIxhpxKSaLOI1oHKFhgEsRN1IGXnGtRKgirlDCnnKn0Ezk6dqRCv+7afHJO7hfP/AZgHwIXt/eg9e7x+S6px3suwmCMA4DuuDQyrPQcaQqoIWnFTwaPb7VKpK1TGTBnFs355ZR18IRnPhg2P2CexQn7THjRoS2IidEwpI4CSDdXEh0zaC7LKRnDpcn/TnJ9L0xIWdMt/UzAz9zVDdcJvTO4PQzY7BB/zk3wwJuGARqebJy4ynEjQmCmgFIE0dg5OmgOZLbuFOmaqZkNpNpUrUSUwWSYSVmlTM9NE2HucTIjhl3G6zzSIRDSD1VOxQYAENR2RNjWLuqJkmFS4my0pz1xEiRakaxxgXFFYrzeajsLW/gLff0f6rhv92U/93YyjstGH5otwAfhXWPD+e7Ck8DLaO1K7ACWjlSxxhWnm4LJCmRRNIAFbWK4bx404Ek70wsksSrowUMEOuJyqgJyT6ntRJMlKyAKQGcNycmIiriXUb3GYi48SBMMYe5RuLEGrE/NyPSkRgz8iyHdxnT/TFrgP8mNp5YOJ5zmBUAnFoDFrLU4AYmGgNq+QANTdDEGsJS5hQoZqRkhqIpmVktYgkjCvTFtMJSjxz1o5nsibAHaaS4oRkjjFLUDiH1xfxIxHq11SWaEoXVlF4JKE6UoSXSYiRUkdZBwlfGdfJnRmH5+5zxv51Ib+/AqI8+977Cej+qGcB71Sa81787kg3UMukYZOy9oIuONAx0QiD5gNcOzndDCm2ctTAJ5ihwvgXSNSPgZNmJW1WzDBUU13JYy7w7J7hTCEuIWwPp5PBtY/E+L0K70dbO18OyNthk2mCm9Zsh1gwXkbFmwOTNi4iZynjgeWPU41liOTu3MddY819MccUgqJmUljnMqSEnZFUC0z5obSaVww6b6D4U466SBmLWU9FDko0Q9sV0DycVyZKMq/givVrTCE9NqkaETqQ/yhEeZ7jDXEPxLc3RftzK687cH29rn/+L3FvyMxzArzKL/cg4gPeSQvyLOgJyUa7hFIwZhikJekqoemHiCNq+IFqbVuFQKQoX2pj3SEriqc2CM9Mlc34NkQ5GYU48agXOnxGRsyKcdM49jMnyBIaWjbkjIsuCFI1NW57W1fQVMcFJk6dLMzLbXGPijbyQCIJHzYmIaQb45ixfxDdKysnM6vE84vFItXG+YEYy2AcOxaQSbGSmajmV2M96eoxQejgrxfRQYBPVgZIG4HoCQ5HUF7NhJWJUlWV4bkpEXxJSZFAliljjW4oLhts0vM+fvffGtXGUP6UzFf173Sf2C9xP8g46AfY2njt2AHyw4MNv53xmVDCfzkVCKuFinZ1AueLRJMSRn0CLtfAULpCkQOtAWwxxinpP0jYhLPok3Vw89IWJdIB1B+vm3IYzeQKxRVOpsu07j9miIScQOpg4XNblEKQQIZA7/BEl4WBG3cOa9oMieDFyXzEjgFMzBlnEtGhYA5VBvxm0GmA8a9EstwKkNtgSbBeoMSvVSIJVoJsIPcxKg6EzqVW1j7M9MRsh2pdkh+KlqmGA2XB6lUdG1YkUhzWDdgJntOpI6Cr+jnG90/jDVvPZjyP+d/Xn3BPvlsTzQUK4HjuA9+jiys/fEhzpGABc7LuJM4hdTxp6NDoWOw7T8TTQfK6FFFRFm0BR1LFjPrRwErITkK4Zy044i7Hc/F0EhxNrJ7MVJ26hKeFXOYuXjikdnAkqhlhqyvvLIMuCBBMqQcrcsDcRc8FEA2qWG/siGD53GWyISB8jCYzABmqiTrRWs0qMiFgfk6qpJwRQE2xP4Q0R7RMBJwGLAbEo2Ijok7iqrpKrcDERXUU7lPT6DRfJG76bKEaJYke5Pi7ijdfY4Od6+O+0iv9O7xl5h/fiLxL9P5RswLf7GvYh8q7voFvwTPM9Z7W5fbjqiWWm1Vma+Zv1/C0NPV0fiD7gYxtftNHoi9QqzFuBaNssrAAL5ggiTpu6XYFKByF4cTEhQ0QU1bYTWcIRcgFQVM2cE9YMW20UOEscI1SSiJmZuIwUMpmWCfAISUxKnNSYVWrsiekBooqTGiUrkiSZ7q1dCrn64PZj0lu0QoXWjtoKNHpEDJcSzitlSvg64YIyqBJhKeJ72chdMIoFpbOlvDoG68yu0wb/hLdZyf9V7fXfSeX/uAj4Hp2HvQ/nc0QC/RmZcwIpCelMfl7r/N0VhtZCLB2x41muHZUrKKQgBU+ioPC+SBLMs2JJuoRcncecw1Gg0saZw0KFaUVwimrLCcuoa+MsIlInEw+y6MwWcE5AKxEfm3JgwEzEacSkSmDenCMXAjU5q8SkRi2CDQQbNZmIIqokS/nnpBK1ERoQE2NYFWGfyiKWBI05+5kAn4ZGsRAZaET2Lbfsuoovmn39LaMobN74v/XzKvX2Htwv8g6OK3yISELyETkHex/PU46kb/KWrOBylX9/tWVzzkFrQaOgSWADtHRoFFZVKKUgFZ4FwNpTY7J27j6YCu3CobHJNEywojG2Ksv90mZsnCQCnlAAtUuKKxJa12gscYVOMhVXKKNKcS1FQjZSDkAXBecNN5im67PL94z9dhPJ9wwXbM7paczHD12ltaXTAt5saj9O738pkf6XkZK/0/v0Q8cOlI/I+dkH5HzuIU02zgzGN/eMU6hrIZ3Lz6V6+rdaC3HJsVo6UhRYy1uJWDZOoCto7ei03MRwxecoy2D66qOgiDcWkhCDJxIQZ4RBpGwnQky0ltLEWP2BsVMYrmW4HZtE5tlzm0TrdvNcee9r4ttGcWP6udTn878rbhyJ8HPG/tM+S/sFA4IcO4B7r2Mg0Lu/sd6h81z66TfHrIGNoybALpmSwB409cC5NRhAt4nyloTBYFpohEz1tST0VOgArs4Reugymnjy2rv55x0/2/S0OaMfn2OqZeIEZh3B2BnM/l7XQjHzb3kdXl2c+f1bv0x1XfsoGebxOl7H63gdr+N1vI7X8Tpex+t4Ha/jdbyO1/E6XsfreB2v43W8jtfxOl7H63gdr+N1vI7X8Tpex+t4Ha/jdbyO1/E6XsfreB2v43W8jtfxOl7H63gdr+N1vI7X8Tpex+t4Ha/jdbz+/7v+P2Qzbrw/TOY+AAAAAElFTkSuQmCC" alt="ForcedFR"></div><div class="service-name">ForcedFR</div><span class="service-status-dot online" aria-label="État de ForcedFR"></span></div>
<div class="service-card" id="service-qbittorrent"><div class="service-icon"><img src="data:image/png;base64,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" alt="qBittorrent"></div><div class="service-name">qBittorrent</div><span class="service-status-dot online" aria-label="État de qBittorrent"></span><span class="service-meta value" id="qb" aria-hidden="true"></span></div>
<div class="service-card" id="service-discord"><div class="service-icon"><img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAQAAAAEACAYAAABccqhmAACECUlEQVR42uz9ebxlZ1Xnj7/X8+y9zzl3vjXPVZmTCmQgIcwkDBIQEFSiKCpNi7SgLXY70A7dKX6t3e1X225bwQYbFFFRSpnnQEgIIZCkMlYqVIZKVWoeb93xnLP3fp71/eN59j6nIrbo9/d9fZvUXnqom3vPsM+9Z61nrc/6rM+CxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmusscYaa6yxxhprrLHGGmussf9TTaX5HTw9rPlDfk87oOj/3imHf/7Uxz/1saJ///vfjbP/Q6/RWBMAGjuLTs8mEDQBoLF/1OFvvHHwO9+1a7scO7ZS4DouWtghu8euUriFVauu02PHbpHqX4BVq65TgOq/q+99x/ttj3e4Ifxz7NgtsurW67T679q2//2r3cotsovrvoMzbx884dD3tnPD8H317weF75RVNNYEgLPI+W8E2XUDMr1nhwGY6bZlflMmqx9O5eSKk2ZqWSKsXs3K/hO+nD/t+5tWeYCZmbZsrYIFu5iePlfXrp1XuA6AdYd3yKGZthye7sXvA1znYRsA29jGthhotm69QS/dtb3+e9+w9SFl2zZl2zap7g+wfdd2uWHrDWc67LvRbQzut43BI7ZFp9/2lM/Srqf890fBS5MpNAHg7LAbzY3BRcyurQ8ZgK03HPeXbjuuK3lIbtm7JTn8pQ2tmXtWjPVOpZ0Tx9rtLLOJeoQ0xXtnjCmNYKwziUKB5iLOe09hy0xUyzJX640RUZXCFIaiSJPE9fuQqojJVaGPEVVrMs37qql1Pu931XkROmD7LYUebqkvtKHTiZffBZ/kLpeW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" alt="Discord"></div><div class="service-name">Discord</div><span class="service-status-dot online" aria-label="État de Discord"></span><span class="service-meta value" id="discord" aria-hidden="true"></span></div>
<div class="service-card" id="service-radarr"><div class="service-icon"><img src="data:image/png;base64,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" alt="Radarr"></div><div class="service-name">Radarr</div><span class="service-status-dot online" aria-label="État de Radarr"></span><span class="service-meta value" id="radarr" aria-hidden="true"></span></div>
<div class="service-card" id="service-sonarr"><div class="service-icon"><img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAQAAAAEACAYAAABccqhmAACWvUlEQVR42uz9d5hd13HmC/9qrb33CR3QyDkDTGDOFCkmUxRFisqkrEgFK1iWLNujsS3ZFknLWXKQrWQlS1YmbeVABYpJzEGMYCZAAETubnQ4Ye+91qr7x9qnG/K933zfzDczd2Zw6nnO090Hpxsn7KpV9dZbb0Hf+ta3vvWtb33rW9/61re+9a1vfetb3/rWt771rW9961vf+ta3vvWtb33rW9/61re+9a1vfetb3/rWt771rW9961vf+ta3vvWtb33rW9/61re+9a1vfetb3/rWt771rW9961vf+ta3vvWtb33rW9/61re+9a1vfetb3/rWt771rW9961vf+ta3vvWtb33rW9/61re+9a1vfetb3/rWt771rW9961vf+ta3vvWtb33rW9/61re+9a1vfetb3/rWt771rW9961vf+ta3vvWtb33rW9/61re+9a1vfetb3/rWt771rW9961vf+ta3vvWtb33rW9/61re+9a1vfetb3/rWt771rW9961vf+ta3vvWtb33rW9/61re+9a1v/2ua9N+CQ+Iz1v/C567/X64L7b+F/+da0n8L/nc0rRz0KoGr4LJrBS6DvTfOOu6ic5VrCNGVq7tVpfpWuOxaYe8jv3oALNqkcC0cdZRy9dV95+9nAH37X8rpr0TYjLD3RmHRPuXay/1/xWes/9XXxjXXGB5ZKHBu4GoUpB8Q+gGgb//TPpsrVc658UZz06JzlWvl/+7sb397umLr0iGb+MW2Nm+dpANLfaMxL9TSRSRmYRA7xyTpcIGHWi0IUubTU11xrsS7lg+yT1R2S5Lul1p6wJiwv7Cye87ixeNv7j5/4uqrCb/y/112jeWyy+ARtB8Q+gGgb/+DnP6yzdfKtf/hdB98ze8tGLbrD5NS1w+kA0c4k66WOXOWuqHBBdqsLykHBuaHwUHrhwbxjQyv8de98wQBUQUU4wKUAYvFByV0Csidmk7uJG/loehMqLEH1Cb7tGafUFveZ+rpLzv5o4/wn9/Y+tWkRA3XIlxGQPrBoB8A+vbfnNpfthm59uBT/u0npavzt6+v1eYelya155eN7GyGhlbncwaH3bx5FAN1fCZ08ilCa8q7qUmvkxPK9BR6YA/amobWlDA1Ac6BoiQ1JGtgkgTJMrTZQAaG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" alt="Sonarr"></div><div class="service-name">Sonarr</div><span class="service-status-dot online" aria-label="État de Sonarr"></span><span class="service-meta value" id="sonarr" aria-hidden="true"></span></div>
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
