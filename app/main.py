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

</style></head><body><button class="mobile-nav-toggle" id="mobileNavToggle" aria-label="Afficher la navigation" aria-expanded="false">☰ <span>Menu</span></button><div class="nav-backdrop" id="navBackdrop"></div><div class="app-shell"><aside class="sidebar"><div class="sidebar-brand"><img class="brand-logo" src="data:image/png;base64,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" alt="Logo ForcedFR"><div><h1>ForcedFR</h1><small>Media intelligence</small></div></div><div><div class="nav-label">Espace de travail</div><nav class="tabs"><button class="tab active" data-tab="dashboard"><span class="nav-icon">⌂</span>Vue d’ensemble</button><button class="tab" data-tab="scan"><span class="nav-icon">▦</span>Bibliothèque</button><button class="tab" data-tab="history"><span class="nav-icon">◷</span>Historique</button><button class="tab" data-tab="pending"><span class="nav-icon">⚠</span>Fichiers à traiter</button><button class="tab" data-tab="settings"><span class="nav-icon">⚙</span>Paramètres</button></nav></div><div class="sidebar-bottom"><strong>Surveillance active</strong><p>ForcedFR surveille les téléchargements et vérifie les pistes de sous-titres.</p></div></aside><main class="main-content">
<header class="app-header"><div><h1>Vue d’ensemble</h1><p class="sub">Suivez votre bibliothèque et l’activité de ForcedFR.</p></div><div class="header-actions"><button class="theme-toggle" id="themeToggle" type="button" title="Changer le thème"><span class="theme-icon" id="themeIcon">☼</span><span id="themeLabel">Mode clair</span></button><div class="version">v2.8.7</div></div></header>
<div class="services service-status-row">
<div class="service-card" id="service-forcedfr"><div class="service-icon"><img src="data:image/png;base64,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" alt="ForcedFR"></div><div class="service-name">ForcedFR</div><span class="service-status-dot online" aria-label="État de ForcedFR"></span></div>
<div class="service-card" id="service-qbittorrent"><div class="service-icon"><img src="data:image/png;base64,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" alt="qBittorrent"></div><div class="service-name">qBittorrent</div><span class="service-status-dot online" aria-label="État de qBittorrent"></span><span class="service-meta value" id="qb" aria-hidden="true"></span></div>
<div class="service-card" id="service-discord"><div class="service-icon"><img src="data:image/png;base64,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3CPSQbJ5Je8IeRDJjapVsBhT5EHClDo/izdpTky3rcFVQUPQTL0YVfGpsGHioMOgzqGlM+LjqDTng/HqEeeD77b9XHsuHJubC/untb2ny/yDoXH09nd2Prj3F7P3VcGrbAO7DcI/lnKeo5K+g2WxHXbeIGxZLrDcrJsese1sLhs1hVnZnGkMLvNZt1vYfcdNc8++bKCabubMhYyuMzibYaqCKnicCPlggyCWTuUseY6hSdspHZ33M2GajvFNNVmGGKtiMueN61qfU6nrZZMrFbrOM+jj8+ArOh6qxIwoqopOyDEGnFQ436DpwYcmhRpKl8XRiioErZiuhpZMdNe+6svl9r/8kNsGYft25ZbrbjEr2KY39IbZ/OMp54dT0nceKyVwi4FtbD1wt0w1RlJe8AmKJRv08jUdXb1pRgEOPPaYbN26lf1PNeXYUCHTh/b1rjsy3VZOh2x2zABcyl6//Orl4bG7H5P9T43LmlNO0U1b43W2cUThIYWz5JZblktsBK7fS4GgvS581/57nj3X8B9+QOVakJ0gW0Cve9Z0vX86kR/NZb6T8rYL1yZr4+oFSdxb4md3btNI37z6e1z3Brjh6h/FDdHv8vdqv/v7O/Wtft+J0P8nKelElCb6PS3xu3/F/+U36f9SJX27ohZv9P/GorJ4DxZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlURZlUf6vlf8PVyoIE5M2QGgAAAAASUVORK5CYII=" alt="Discord"></div><div class="service-name">Discord</div><span class="service-status-dot online" aria-label="État de Discord"></span><span class="service-meta value" id="discord" aria-hidden="true"></span></div>
<div class="service-card" id="service-radarr"><div class="service-icon"><img src="data:image/png;base64,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" alt="Radarr"></div><div class="service-name">Radarr</div><span class="service-status-dot online" aria-label="État de Radarr"></span><span class="service-meta value" id="radarr" aria-hidden="true"></span></div>
<div class="service-card" id="service-sonarr"><div class="service-icon"><img src="data:image/png;base64,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" alt="Sonarr"></div><div class="service-name">Sonarr</div><span class="service-status-dot online" aria-label="État de Sonarr"></span><span class="service-meta value" id="sonarr" aria-hidden="true"></span></div>
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
