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
<title>ForcedFR v2.8.6</title>
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

</style></head><body><button class="mobile-nav-toggle" id="mobileNavToggle" aria-label="Afficher la navigation" aria-expanded="false">☰ <span>Menu</span></button><div class="nav-backdrop" id="navBackdrop"></div><div class="app-shell"><aside class="sidebar"><div class="sidebar-brand"><img class="brand-logo" src="data:image/png;base64,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" alt="Logo ForcedFR"><div><h1>ForcedFR</h1><small>Media intelligence</small></div></div><div><div class="nav-label">Espace de travail</div><nav class="tabs"><button class="tab active" data-tab="dashboard"><span class="nav-icon">⌂</span>Vue d’ensemble</button><button class="tab" data-tab="scan"><span class="nav-icon">▦</span>Bibliothèque</button><button class="tab" data-tab="history"><span class="nav-icon">◷</span>Historique</button><button class="tab" data-tab="pending"><span class="nav-icon">⚠</span>Fichiers à traiter</button><button class="tab" data-tab="settings"><span class="nav-icon">⚙</span>Paramètres</button></nav></div><div class="sidebar-bottom"><strong>Surveillance active</strong><p>ForcedFR surveille les téléchargements et vérifie les pistes de sous-titres.</p></div></aside><main class="main-content">
<header class="app-header"><div><h1>Vue d’ensemble</h1><p class="sub">Suivez votre bibliothèque et l’activité de ForcedFR.</p></div><div class="header-actions"><button class="theme-toggle" id="themeToggle" type="button" title="Changer le thème"><span class="theme-icon" id="themeIcon">☼</span><span id="themeLabel">Mode clair</span></button><div class="version">v2.8.6</div></div></header>
<div class="services">
<div class="service-card"><div class="service-icon"><img src="data:image/png;base64,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" alt="ForcedFR"></div><div><div class="service-name">ForcedFR</div><div class="service-meta"><span class="status-dot online"></span> Service actif</div></div></div>
<div class="service-card"><div class="service-icon"><img src="https://cdn.simpleicons.org/qbittorrent" alt="qBittorrent"></div><div><div class="service-name">qBittorrent</div><div class="service-meta value" id="qb">…</div></div></div>
<div class="service-card"><div class="service-icon"><img src="https://cdn.simpleicons.org/discord" alt="Discord"></div><div><div class="service-name">Discord</div><div class="service-meta value" id="discord">…</div></div></div>
<div class="service-card"><div class="service-icon"><img src="https://cdn.simpleicons.org/radarr" alt="Radarr"></div><div><div class="service-name">Radarr</div><div class="service-meta value" id="radarr">…</div></div></div>
<div class="service-card"><div class="service-icon"><img src="https://cdn.simpleicons.org/sonarr" alt="Sonarr"></div><div><div class="service-name">Sonarr</div><div class="service-meta value" id="sonarr">…</div></div></div></div>



<section id="p-dashboard" class="panel active">
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
</script></main></div></body></html>"""


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
