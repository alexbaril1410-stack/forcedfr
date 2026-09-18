import asyncio
import json
import logging
import os
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
    version="2.5.7",
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

    if response.status_code not in (200, 204):
        raise RuntimeError(
            f"Authentification qBittorrent échouée : "
            f"HTTP {response.status_code} "
            f"{response.text}"
        )

    log.info(
        "Connexion qBittorrent réussie : %s",
        QB_HOST,
    )


# ============================================================
# REQUÊTES qBITTORRENT
# ============================================================

def qb_request(
    method: str,
    endpoint: str,
    *,
    params: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
) -> Any:

    response = qb_session.request(
        method,
        f"{QB_HOST}{endpoint}",
        params=params,
        data=data,
        timeout=15,
    )

    # Session expirée
    if response.status_code == 403:

        log.info(
            "Session qBittorrent expirée, "
            "nouvelle authentification."
        )

        qb_login()

        response = qb_session.request(
            method,
            f"{QB_HOST}{endpoint}",
            params=params,
            data=data,
            timeout=15,
        )

    response.raise_for_status()

    if not response.text:
        return None

    content_type = response.headers.get(
        "content-type",
        "",
    )

    if "application/json" in content_type:
        return response.json()

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


async def _send_discord_bot_message(
    *,
    embeds: list[dict[str, Any]],
    torrent_hash: str,
    release: dict[str, Any],
) -> None:
    if discord_bot is None or not discord_bot.is_ready():
        raise RuntimeError("Bot Discord non prêt.")

    if not DISCORD_CHANNEL_ID:
        raise RuntimeError("DISCORD_CHANNEL_ID non configuré.")

    channel = discord_bot.get_channel(int(DISCORD_CHANNEL_ID))
    if channel is None:
        channel = await discord_bot.fetch_channel(int(DISCORD_CHANNEL_ID))

    embed_objects = [discord.Embed.from_dict(embed) for embed in embeds]
    view = ForcedFRView(torrent_hash, release)
    await channel.send(embeds=embed_objects, view=view)


def send_discord_message(
    *,
    embeds: list[dict[str, Any]],
    torrent_hash: str | None = None,
    release: dict[str, Any] | None = None,
) -> None:
    """Envoie la notification via le bot pour permettre les boutons interactifs."""
    if discord_bot is not None and MAIN_EVENT_LOOP is not None and torrent_hash and release:
        try:
            future = asyncio.run_coroutine_threadsafe(
                _send_discord_bot_message(
                    embeds=embeds,
                    torrent_hash=torrent_hash,
                    release=release,
                ),
                MAIN_EVENT_LOOP,
            )
            future.result(timeout=15)
            log.info("Notification Discord envoyée via le bot.")
            return
        except Exception:
            log.exception("Impossible d'envoyer la notification via le bot Discord.")
            return

    log.warning("Bot Discord indisponible : notification interactive non envoyée.")


class ForcedFRView(discord.ui.View if discord else object):
    def __init__(self, torrent_hash: str, release: dict[str, Any]) -> None:
        if discord is None:
            return
        super().__init__(timeout=None)

        tracker_url = release.get("tracker_url")
        arr_item_url = release.get("arr_item_url")
        source = release.get("source")

        if tracker_url:
            self.add_item(discord.ui.Button(
                label="🌐 Voir le torrent",
                style=discord.ButtonStyle.link,
                url=tracker_url,
            ))

        self.add_item(discord.ui.Button(
            label="🖥️ Ouvrir qBittorrent",
            style=discord.ButtonStyle.link,
            url=build_qbittorrent_url(torrent_hash),
        ))

        if arr_item_url:
            label = "🎬 Ouvrir Radarr" if source == "Radarr" else "📺 Ouvrir Sonarr"
            self.add_item(discord.ui.Button(
                label=label,
                style=discord.ButtonStyle.link,
                url=arr_item_url,
            ))

        self.add_item(discord.ui.Button(
            label="▶️ Continuer le téléchargement",
            style=discord.ButtonStyle.success,
            custom_id=f"forcedfr:resume:{torrent_hash}",
        ))

        self.add_item(discord.ui.Button(
            label="⏸️ Laisser en pause",
            style=discord.ButtonStyle.secondary,
            custom_id=f"forcedfr:pause:{torrent_hash}",
        ))


def build_disabled_decision_view(message: Any) -> Any:
    """
    Reconstruit les boutons après une décision.

    Les liens restent actifs. Seuls les boutons interactifs
    Continuer / Laisser en pause sont désactivés.
    """
    if discord is None:
        return None

    view = discord.ui.View(timeout=None)

    for row in getattr(message, "components", []):
        for component in getattr(row, "children", []):
            custom_id = getattr(component, "custom_id", None)
            url = getattr(component, "url", None)

            disabled = bool(
                custom_id
                and str(custom_id).startswith("forcedfr:")
            )

            view.add_item(
                discord.ui.Button(
                    label=getattr(component, "label", None),
                    style=getattr(
                        component,
                        "style",
                        discord.ButtonStyle.secondary,
                    ),
                    custom_id=custom_id,
                    url=url,
                    emoji=getattr(component, "emoji", None),
                    disabled=disabled,
                )
            )

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

            # Désactive les boutons de décision tout en conservant
            # les liens vers torrent / qBittorrent / Radarr-Sonarr.
            if interaction.message is not None:
                await interaction.message.edit(
                    view=build_disabled_decision_view(
                        interaction.message
                    )
                )

            await interaction.followup.send(
                response_message,
                ephemeral=True,
            )

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


def build_release_fields(
    torrent: dict[str, Any],
    release: dict[str, Any],
) -> list[dict[str, Any]]:

    fields = [
        {
            "name": "Torrent",
            "value": (
                f"`{torrent.get('name', 'Nom inconnu')}`"
            )[:1024],
            "inline": False,
        },
        {
            "name": "Progression",
            "value": (
                f"{float(torrent.get('progress', 0)) * 100:.2f}%"
            ),
            "inline": True,
        },
    ]

    if release.get("source"):

        fields.append(
            {
                "name": "Source",
                "value": release["source"],
                "inline": True,
            }
        )

    if release.get("indexer"):

        fields.append(
            {
                "name": "Indexeur",
                "value": str(
                    release["indexer"]
                )[:1024],
                "inline": True,
            }
        )

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


def notify_profile_decision(torrent: dict[str, Any], title: str, description: str, action_text: str, release: dict[str, Any] | None = None) -> None:
    torrent_hash = str(torrent.get("hash", ""))
    if release is None:
        try:
            release = wait_for_release_context(torrent_hash)
        except Exception:
            release = None
    if release is None:
        release = {"source": None, "title": torrent.get("name"), "indexer": None, "tracker_url": None, "arr_item_url": None, "item_id": None}
    else:
        update_torrent_release_context(torrent_hash, release)
    fields = build_release_fields(torrent, release)
    fields.append({"name": "Action ForcedFR", "value": action_text, "inline": False})
    send_discord_message(embeds=[{"title": title, "description": description, "color": 5763719, "fields": fields, "footer": {"text": "ForcedFR • Notification"}}], torrent_hash=torrent_hash, release=release)


def notify_analysis_error(torrent: dict[str, Any], error: str, *, action_text: str, attempt: int | None = None, release: dict[str, Any] | None = None) -> None:
    torrent_hash=str(torrent.get("hash",""))
    if release is None:
        try: release=wait_for_release_context(torrent_hash)
        except Exception: release=None
    if release is None:
        release={"source":None,"title":torrent.get("name"),"indexer":None,"tracker_url":None,"arr_item_url":None,"item_id":None}
    else: update_torrent_release_context(torrent_hash,release)
    fields=build_release_fields(torrent,release)
    text=str(error).strip() or "Erreur inconnue"
    fields += [{"name":"Erreur d'analyse","value":f"```{text[:1000]}```","inline":False},{"name":"Action ForcedFR","value":action_text,"inline":False}]
    if attempt is not None: fields.append({"name":"Tentative","value":str(attempt),"inline":True})
    send_discord_message(embeds=[{"title":"⚠️ Erreur pendant l'analyse ForcedFR","description":"ForcedFR n'a pas pu déterminer le résultat de manière fiable.","color":16776960,"fields":fields,"footer":{"text":"ForcedFR • Erreur d'analyse"}}],torrent_hash=torrent_hash,release=release)


def apply_analysis_error_policy(torrent: dict[str, Any], error: str, error_count: int) -> bool:
    profile,release=_error_profile_for_torrent(torrent)
    policy=str(profile.get("error_action") or "notify_continue")
    retries=max(1,min(20,int(profile.get("error_retries") or 5)))
    delay=max(1.0,min(3600.0,float(profile.get("error_retry_delay") or 30)))
    if policy in {"retry_pause","retry_continue"} and error_count <= retries:
        notify_analysis_error(torrent,error,action_text=f"🔄 Nouvelle tentative dans {delay:g} seconde(s) ({error_count}/{retries}).",attempt=error_count,release=release)
        time.sleep(delay); return True
    if policy=="retry_pause":
        stop_torrent(str(torrent.get("hash",""))); text="⏸️ Après les tentatives prévues, le téléchargement a été mis en pause."
    elif policy=="retry_continue":
        start_torrent(str(torrent.get("hash",""))); text="▶️ Après les tentatives prévues, le téléchargement continue."
    elif policy=="pause_decision":
        stop_torrent(str(torrent.get("hash",""))); text="⏸️ Le téléchargement est en pause. Utilise les boutons Discord pour décider de continuer ou de le laisser en pause."
    else:
        text="▶️ Le téléchargement continue. Une vérification manuelle est recommandée."
    notify_analysis_error(torrent,error,action_text=text,attempt=error_count,release=release); return False


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
                    "après %ss. Le téléchargement continue.",
                    torrent_hash,
                    ANALYSIS_TIMEOUT,
                )

                record_analysis_history(torrent_hash, str(torrent.get("name", "")), "error", f"Timeout après {ANALYSIS_TIMEOUT} secondes.")
                notify_analysis_error(
                    torrent,
                    f"Timeout après {ANALYSIS_TIMEOUT} secondes.",
                )

                # IMPORTANT : ne pas mettre le torrent en pause.
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
                    found_action = str(profile.get("found_action") or "validate")
                    name = str(torrent.get("name", ""))
                    if found_action == "pause_notify":
                        stop_torrent(torrent_hash)
                        action_text = "⏸️ Torrent mis en pause. Forced FR détecté."
                        record_torrent_action(torrent_hash, "auto_pause", source="forcedfr", details="Pause après détection d'une piste FR Forced.")
                        notify_profile_decision(torrent, "🇫🇷 Forced FR détecté", f"Une piste française Forced a été détectée dans **{name}**.", action_text, release)
                    elif found_action == "validate_notify":
                        action_text = "🔔 Torrent validé et notification envoyée. Le téléchargement poursuit son cours."
                        notify_profile_decision(torrent, "🇫🇷 Forced FR détecté", f"Une piste française Forced a été détectée dans **{name}**.", action_text, release)
                    else:
                        action_text = "✓️ Torrent validé. Le téléchargement poursuit son cours."
                    record_analysis_history(torrent_hash, name, "forced_found", action_text)
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
                missing_action = str(profile.get("torrent_missing_action") or "pause_notify")
                name = str(torrent.get("name", ""))
                if missing_action == "continue_notify":
                    start_torrent(torrent_hash)
                    action_text = "▶️ Téléchargement poursuivi. Aucune piste FR Forced détectée."
                    record_torrent_action(torrent_hash, "resume", source="forcedfr", details="Téléchargement poursuivi après analyse sans piste FR Forced.")
                elif missing_action == "pause_decision":
                    stop_torrent(torrent_hash)
                    action_text = "⏸️ Torrent mis en pause. Une décision est demandée dans Discord."
                    record_torrent_action(torrent_hash, "auto_pause", source="forcedfr", details="Torrent mis en pause en attente d'une décision Discord.")
                else:
                    stop_torrent(torrent_hash)
                    action_text = "⏸️ Torrent mis en pause. Aucune piste FR Forced détectée."
                    record_torrent_action(torrent_hash, "auto_pause", source="forcedfr", details="Torrent mis en pause après analyse sans piste FR Forced.")
                notify_profile_decision(torrent, "🚨 Aucun Forced FR détecté", f"Aucune piste française Forced n'a été trouvée dans **{name}**.", action_text, release)
                record_analysis_history(torrent_hash, name, "no_forced", action_text)
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


# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
async def startup() -> None:

    global MAIN_EVENT_LOOP, discord_bot
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
    global discord_bot
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
        "version": "2.5.7",
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
        }
        for key, value in defaults.items():
            conn.execute(
                "INSERT OR IGNORE INTO forcedfr_settings(key,value,updated_at) VALUES (?,?,?)",
                (key, value, time.time()),
            )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS forcedfr_profiles (
                name TEXT PRIMARY KEY,
                media_type TEXT NOT NULL,
                forced_required INTEGER NOT NULL DEFAULT 1,
                missing_action TEXT NOT NULL DEFAULT 'review',
                error_action TEXT NOT NULL DEFAULT 'notify_continue',
                enabled INTEGER NOT NULL DEFAULT 1,
                updated_at REAL NOT NULL
            )
        """)
        for name, media_type in (("strict", "Film"), ("strict_series", "Série")):
            conn.execute(
                "INSERT OR IGNORE INTO forcedfr_profiles(name,media_type,forced_required,missing_action,error_action,enabled,updated_at) VALUES (?,?,?,?,?,?,?)",
                (name, media_type, 1, "review", "notify_continue", 1, time.time()),
            )
        cols = {row[1] for row in conn.execute("PRAGMA table_info(forcedfr_profiles)").fetchall()}
        if "error_retries" not in cols:
            conn.execute("ALTER TABLE forcedfr_profiles ADD COLUMN error_retries INTEGER NOT NULL DEFAULT 5")
        if "error_retry_delay" not in cols:
            conn.execute("ALTER TABLE forcedfr_profiles ADD COLUMN error_retry_delay REAL NOT NULL DEFAULT 30")
        if "found_action" not in cols:
            conn.execute("ALTER TABLE forcedfr_profiles ADD COLUMN found_action TEXT NOT NULL DEFAULT 'validate'")
        if "torrent_missing_action" not in cols:
            conn.execute("ALTER TABLE forcedfr_profiles ADD COLUMN torrent_missing_action TEXT NOT NULL DEFAULT 'pause_notify'")
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
}

def load_runtime_configuration() -> None:
    global DISCORD_BOT_TOKEN, DISCORD_CHANNEL_ID, DISCORD_WEBHOOK_URL
    global QB_HOST, QB_PASSWORD, QB_USERNAME, RADARR_API_KEY, RADARR_URL, SONARR_API_KEY, SONARR_URL
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

def _scan_items(scope: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if scope in ("films", "all"):
        items.extend(_radarr_scan_items())
    if scope in ("series", "all"):
        items.extend(_sonarr_scan_items())
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
    return (_profile_for_media(media_type) or {"media_type": media_type, "found_action": "validate", "torrent_missing_action": "pause_notify"}, release)


def _error_profile_for_torrent(torrent: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    release=None
    try: release=wait_for_release_context(str(torrent.get("hash","")))
    except Exception: pass
    media_type="Série" if release and release.get("source")=="Sonarr" else "Film"
    return (_profile_for_media(media_type) or {"media_type":media_type,"error_action":"notify_continue","error_retries":5,"error_retry_delay":30},release)


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
        items = _scan_items(scope)
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
<title>ForcedFR v2.5.6</title>
<style>
:root{color-scheme:dark;--bg:#080c12;--surface:#101722;--surface2:#151e2b;--surface3:#1b2635;--border:#263345;--text:#f3f6fa;--muted:#8d9aac;--accent:#5b8cff;--accent2:#7b68ee;--green:#35c98a;--yellow:#f0b85a;--red:#ef6b73;--shadow:0 14px 40px rgba(0,0,0,.22);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
*{box-sizing:border-box}html{background:var(--bg)}body{margin:0;background:radial-gradient(circle at 50% -10%,#1a2638 0,#080c12 42%);color:var(--text);min-height:100vh}main{max-width:1440px;margin:auto;padding:30px 28px 55px}h1,h2,h3,p{margin-top:0}h1{font-size:1.72rem;letter-spacing:-.035em;margin-bottom:3px}h2{font-size:1.12rem;letter-spacing:-.015em;margin-bottom:5px}.sub,.small{color:var(--muted)}.sub{font-size:.88rem;line-height:1.45}.small{font-size:.78rem}
.app-header{display:flex;align-items:center;justify-content:space-between;gap:20px;margin-bottom:20px}.brand{display:flex;align-items:center;gap:13px}.brand-logo{width:48px;height:48px;flex:0 0 48px;filter:drop-shadow(0 8px 18px rgba(91,140,255,.2))}.brand .sub{margin:0}.version{border:1px solid var(--border);background:rgba(16,23,34,.8);color:var(--muted);padding:7px 10px;border-radius:9px;font-size:.76rem;font-weight:800}
.services{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px;margin-bottom:23px}.service-card{display:flex;align-items:center;gap:11px;min-width:0;padding:12px 14px;background:rgba(16,23,34,.9);border:1px solid var(--border);border-radius:13px;box-shadow:var(--shadow)}.service-icon{display:grid;place-items:center;width:34px;height:34px;flex:0 0 34px;border-radius:9px;background:#fff;overflow:hidden}.service-icon img{width:25px;height:25px;object-fit:contain}.service-name{font-size:.82rem;font-weight:850}.service-meta{margin-top:3px;font-size:.73rem}.status-dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:5px;background:var(--muted)}.status-dot.online{background:var(--green);box-shadow:0 0 0 3px rgba(53,201,138,.1)}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:11px}.card,.tablewrap,.settings-card{background:rgba(16,23,34,.93);border:1px solid var(--border);border-radius:15px;box-shadow:var(--shadow)}.card{padding:17px}.label{color:var(--muted);font-size:.7rem;text-transform:uppercase;letter-spacing:.06em;font-weight:850}.value{font-weight:750;margin-top:6px}.ok{color:var(--green)}.warn{color:var(--yellow)}.bad{color:var(--red)}
.tabs{display:flex;gap:5px;flex-wrap:wrap;margin:25px 0 21px;padding:5px;background:rgba(13,19,28,.9);border:1px solid var(--border);border-radius:13px;box-shadow:var(--shadow)}.tab,.subtab,button{font:inherit;border:1px solid transparent;border-radius:9px;padding:9px 13px;font-size:.82rem;font-weight:760;cursor:pointer;background:transparent;color:#b9c4d1;transition:.16s}.tab:hover,.subtab:hover,button:hover{background:var(--surface3);color:#fff}.tab.active,.subtab.active{background:#202d40;border-color:#304158;color:#fff;box-shadow:0 3px 12px rgba(0,0,0,.18)}button:active{transform:translateY(1px)}button.primary{background:var(--accent)!important;border-color:#719bff!important;color:#fff!important}.panel{display:none}.panel.active{display:block}.toolbar,.filters,.subtabs{display:flex;gap:8px;flex-wrap:wrap}.toolbar{padding:13px;background:var(--surface);border:1px solid var(--border);border-radius:12px}.toolbar button{background:var(--surface3);border-color:var(--border)}.progress{height:8px;background:#202b39;border-radius:999px;overflow:hidden;margin-top:13px}.progress div{height:100%;width:0;background:linear-gradient(90deg,var(--accent),var(--accent2));transition:.3s}.toolbar+.small,.progress+.small{display:block;margin-top:9px}
select,input{font:inherit;background:#0d131c;color:var(--text);border:1px solid var(--border);border-radius:9px;padding:9px 11px;outline:none}select:focus,input:focus{border-color:#527fd6;box-shadow:0 0 0 3px rgba(91,140,255,.1)}input{min-width:220px;flex:1}.filters{margin:12px 0}.filters select{min-width:180px}
table{width:100%;border-collapse:collapse;min-width:650px}th,td{padding:12px 14px;text-align:left;border-bottom:1px solid var(--border);vertical-align:middle}th{color:var(--muted);font-size:.68rem;text-transform:uppercase;letter-spacing:.06em;font-weight:850;background:#0d141e}tbody tr:hover{background:rgba(255,255,255,.018)}tbody tr:last-child td{border-bottom:0}.tablewrap{padding:0;overflow:auto;margin-top:13px}.badge{font-weight:760}.yes{color:var(--green)}.no{color:var(--red)}.err{color:var(--yellow)}a.btn,.review-btn{display:inline-flex;align-items:center;justify-content:center;background:var(--surface3);color:#dce5ee;text-decoration:none;padding:7px 10px;border:1px solid var(--border);border-radius:8px;font-size:.75rem;font-weight:720;margin:2px 4px 2px 0;white-space:nowrap}.review-btn:hover,a.btn:hover{background:#26364a;border-color:#40536c;color:#fff}.empty{color:var(--muted);text-align:center;padding:28px}
.section-head{display:flex;align-items:flex-end;justify-content:space-between;gap:16px;margin:25px 0 13px}.section-head h2{margin:0}.section-head .sub{margin:5px 0 0}.activity{padding:0;overflow:hidden}.activity-row{display:grid;grid-template-columns:105px 1fr auto;gap:15px;align-items:center;padding:14px 17px;border-bottom:1px solid var(--border)}.activity-row:last-child{border-bottom:0}.activity-date{color:var(--muted);font-size:.74rem;white-space:nowrap}.activity-main{min-width:0}.activity-action{font-weight:780;font-size:.81rem}.activity-details{color:#9eabb9;font-size:.76rem;margin-top:3px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.activity-dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--green);margin-right:8px;box-shadow:0 0 0 3px rgba(53,201,138,.08)}
.subtabs{margin:18px 0 14px;padding:4px;width:max-content;background:var(--surface);border:1px solid var(--border);border-radius:11px}.subtab{padding:8px 12px}
.media-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:14px}.media-card{background:rgba(16,23,34,.92);border:1px solid var(--border);border-radius:14px;overflow:hidden;box-shadow:var(--shadow);transition:.16s}.media-card:hover{border-color:#3a4d67;transform:translateY(-1px)}.poster{width:100%;aspect-ratio:2/3;object-fit:cover;background:#0c121a;display:block}.poster-placeholder{width:100%;aspect-ratio:2/3;display:grid;place-items:center;background:linear-gradient(145deg,#172233,#0c121a);color:#718095;font-size:2rem}.media-info{padding:12px}.media-title{font-weight:850;line-height:1.25;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.media-meta{color:var(--muted);font-size:.74rem;margin-top:5px}.media-actions{display:flex;flex-wrap:wrap;gap:5px;margin-top:10px}.media-actions a.btn{margin:0}.series-card{cursor:pointer}.series-summary{display:flex;gap:7px;flex-wrap:wrap;margin-top:9px}.mini-pill{padding:4px 7px;border-radius:999px;background:#1c2735;font-size:.68rem;font-weight:800;color:#aeb9c6}.mini-pill.y{color:var(--green)}.mini-pill.n{color:var(--red)}.mini-pill.w{color:var(--yellow)}.series-detail{display:none}.series-detail.active{display:block}.series-detail-head{display:grid;grid-template-columns:180px 1fr;gap:24px;align-items:start;margin:20px 0}.series-detail-poster{width:180px;aspect-ratio:2/3;object-fit:cover;border-radius:14px;border:1px solid var(--border);box-shadow:var(--shadow)}.series-detail-title{font-size:1.6rem;font-weight:900}.series-detail-meta{color:var(--muted);margin-top:7px}.series-detail-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:16px}.episode-list{display:grid;gap:8px}.episode-card{display:grid;grid-template-columns:90px 1fr auto;gap:15px;align-items:center;padding:14px 16px;background:var(--surface);border:1px solid var(--border);border-radius:12px}.episode-number{font-weight:850}.episode-title{font-weight:750}.episode-status{font-size:.78rem;margin-top:4px}.episode-actions{display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end}.episode-ok{color:var(--green);font-weight:800}.episode-no{color:var(--red);font-weight:800}.episode-err{color:var(--yellow);font-weight:800}.config-group{padding:13px 0;border-top:1px solid var(--border)}.config-group:first-of-type{border-top:0}.config-group h4{margin:0 0 10px;font-size:.82rem}.config-field{display:grid;grid-template-columns:125px 1fr;align-items:center;gap:12px;margin:8px 0}.config-field>span{color:var(--muted);font-size:.76rem}.config-field input{width:100%;min-width:0}.config-note{margin-top:12px;line-height:1.5}.test-btn{margin-top:8px;background:var(--surface3);border-color:var(--border)}.test-result{font-size:.76rem;margin-top:7px;min-height:1em}.test-result.ok{color:var(--green)}.test-result.bad{color:var(--red)}
.settings-grid{display:grid;grid-template-columns:minmax(290px,.8fr) minmax(0,1.5fr);gap:18px;align-items:start}.settings-card{padding:19px}.settings-card h3{margin:0 0 6px}.settings-card .desc{color:var(--muted);font-size:.84rem;line-height:1.5;margin:0 0 16px}.setting-item{display:flex;align-items:center;justify-content:space-between;gap:15px;padding:14px 0;border-top:1px solid var(--border)}.setting-item:first-of-type{border-top:0}.setting-copy strong{display:block;font-size:.85rem}.setting-copy span{display:block;color:var(--muted);font-size:.76rem;margin-top:4px}.switch{position:relative;width:44px;height:24px;flex:0 0 auto}.switch input{display:none}.switch span{position:absolute;inset:0;background:#2b3542;border-radius:999px;cursor:pointer;transition:.2s}.switch span:before{content:"";position:absolute;width:18px;height:18px;left:3px;top:3px;background:#fff;border-radius:50%;transition:.2s}.switch input:checked+span{background:var(--green)}.switch input:checked+span:before{transform:translateX(20px)}.savebar{display:flex;justify-content:flex-end;margin-top:17px}.profile-list{display:grid;gap:12px}.profile-card{background:rgba(21,29,39,.76);border:1px solid var(--border);border-radius:13px;padding:17px}.profile-top{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px}.profile-name{font-size:.98rem;font-weight:850}.profile-type{color:var(--muted);font-size:.76rem;margin-top:3px}.profile-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}.profile-field{display:flex;flex-direction:column;gap:7px}.profile-field label{color:var(--muted);font-size:.68rem;text-transform:uppercase;font-weight:800;letter-spacing:.05em}.profile-field select{width:100%;min-width:0}.profile-footer{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-top:15px;padding-top:14px;border-top:1px solid var(--border)}.status-pill{display:inline-flex;align-items:center;gap:6px;border-radius:999px;padding:5px 9px;background:#202a36;font-size:.72rem;font-weight:800}.status-pill.on{color:var(--green)}.status-pill.off{color:var(--muted)}.settings-title{margin-top:0;margin-bottom:12px}.settings-title h2{margin-bottom:4px}.settings-title p{margin:0}.tag-list{display:flex;flex-wrap:wrap;gap:7px;margin-top:9px}.tag-chip{border:1px solid var(--border);background:#202a36;color:#e8edf2;border-radius:999px;padding:6px 10px;cursor:pointer;font-size:.75rem}.tag-chip:hover{border-color:var(--accent)}.retry-fields{grid-template-columns:1fr 1fr;gap:8px;margin-top:9px}.retry-fields label{font-size:.68rem;color:var(--muted);text-transform:uppercase;font-weight:800}.retry-fields input{margin-top:5px}
@media(max-width:1000px){.services{grid-template-columns:repeat(3,1fr)}.settings-grid{grid-template-columns:1fr}.profile-grid{grid-template-columns:repeat(3,1fr)}}@media(max-width:720px){main{padding:22px 15px 40px}.services{grid-template-columns:1fr 1fr}.app-header{align-items:flex-start}.tabs{overflow:auto;flex-wrap:nowrap}.tab{white-space:nowrap}.activity-row{grid-template-columns:1fr;gap:5px}.profile-grid{grid-template-columns:1fr 1fr}.profile-footer{align-items:flex-start;flex-direction:column}.savebar{justify-content:stretch}.savebar button{width:100%}.media-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}@media(max-width:480px){.services{grid-template-columns:1fr}.profile-grid{grid-template-columns:1fr}.version{display:none}.media-grid{grid-template-columns:1fr 1fr}}
</style></head><body><main>
<header class="app-header"><div><div class="brand"><svg class="brand-logo" viewBox="0 0 64 64" aria-label="Forced FR"><defs><linearGradient id="ffg" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#5b8cff"/><stop offset="1" stop-color="#7b68ee"/></linearGradient></defs><rect x="4" y="4" width="56" height="56" rx="17" fill="url(#ffg)"/><path d="M18 18h27v8H27v6h16v8H27v8h-9V18z" fill="white"/><circle cx="46" cy="47" r="6" fill="#35c98a" stroke="#fff" stroke-width="3"/></svg><div><h1>ForcedFR</h1><p class="sub">Surveillance des téléchargements et contrôle des bibliothèques.</p></div></div></div><div class="version">v2.5.6</div></header><p class="sub">Surveillance qBittorrent et contrôle des bibliothèques Radarr / Sonarr.</p>
<div class="services">
<div class="service-card"><div class="service-icon">✓</div><div><div class="service-name">ForcedFR</div><div class="service-meta"><span class="status-dot online"></span> Service actif</div></div></div>
<div class="service-card"><div class="service-icon"><img src="https://cdn.simpleicons.org/qbittorrent" alt="qBittorrent"></div><div><div class="service-name">qBittorrent</div><div class="service-meta value" id="qb">…</div></div></div>
<div class="service-card"><div class="service-icon"><img src="https://cdn.simpleicons.org/discord" alt="Discord"></div><div><div class="service-name">Discord</div><div class="service-meta value" id="discord">…</div></div></div>
<div class="service-card"><div class="service-icon"><img src="https://cdn.simpleicons.org/radarr" alt="Radarr"></div><div><div class="service-name">Radarr</div><div class="service-meta value" id="radarr">…</div></div></div>
<div class="service-card"><div class="service-icon"><img src="https://cdn.simpleicons.org/sonarr" alt="Sonarr"></div><div><div class="service-name">Sonarr</div><div class="service-meta value" id="sonarr">…</div></div></div></div>

<div class="tabs"><button class="tab active" data-tab="dashboard">📊 Tableau de bord</button><button class="tab" data-tab="scan">🔍 Scan de bibliothèque</button><button class="tab" data-tab="history">📜 Historique</button><button class="tab" data-tab="errors">⚠ Erreurs</button><button class="tab" data-tab="settings">⚙ Paramètres</button></div>

<section id="p-dashboard" class="panel active">
<div class="grid" id="dashCards"></div>
<div class="section-head"><div><h2>Activité récente</h2><p class="sub">Les dernières actions enregistrées par ForcedFR.</p></div></div><div class="card activity"><div id="recent"></div></div>
</section>

<section id="p-scan" class="panel">
<div class="card"><div class="toolbar"><button onclick="startScan('films','incremental')">⚡ Films incrémental</button><button onclick="startScan('films','full')">🎬 Films complet</button><button onclick="startScan('series','incremental')">⚡ Séries incrémental</button><button onclick="startScan('series','full')">📺 Séries complet</button><button onclick="startScan('all','incremental')">⚡ Toute la bibliothèque</button></div><div class="small" id="scanLabel">Aucun scan en cours.</div><div class="progress"><div id="bar"></div></div><div class="small" id="stats"></div></div>
<div class="subtabs"><button class="subtab active" data-kind="films">🎬 Films</button><button class="subtab" data-kind="series">📺 Séries</button></div>
<div id="k-films"><div class="section-head"><div><h2>Films</h2><p class="sub">Les films déjà analysés sont affichés immédiatement. Un scan incrémental est lancé à l'ouverture.</p></div></div><div class="filters"><select id="ff"><option value="all">Tous</option><option value="yes">Avec Forced FR</option><option value="no">Sans Forced FR</option><option value="error">Erreurs</option><option value="pending">🔴 À traiter</option><option value="validated">🟢 Absence normale</option><option value="waiting">🟠 En attente</option></select><input id="fs" placeholder="Rechercher un film…"></div><div id="films" class="media-grid"></div></div>
<div id="k-series" style="display:none"><div class="section-head"><div><h2>Séries</h2><p class="sub">Une carte par série. Clique sur une série pour afficher ses épisodes.</p></div></div><div class="filters"><select id="sf"><option value="all">Toutes</option><option value="yes">Avec Forced FR</option><option value="no">Sans Forced FR</option><option value="error">Erreurs</option><option value="pending">🔴 À traiter</option><option value="validated">🟢 Absence normale</option><option value="waiting">🟠 En attente</option></select><input id="ss" placeholder="Rechercher une série…"></div><div id="seriesGrid" class="media-grid"><div id="series" style="display:contents"></div></div><div id="seriesDetail" class="series-detail"></div></div>
</section>

<section id="p-errors" class="panel"><h2>Erreurs à traiter</h2><p class="sub">Erreurs persistantes du scan de bibliothèque. Une réussite lors d'un prochain scan les clôture automatiquement.</p><div class="toolbar"><button onclick="startScan('films','incremental')">↻ Relancer Films</button><button onclick="startScan('series','incremental')">↻ Relancer Séries</button><button onclick="startScan('all','incremental')">↻ Relancer toute la bibliothèque</button></div><div class="tablewrap" style="margin-top:14px"><table><thead><tr><th>Dernière détection</th><th>Média</th><th>Erreur</th><th>Action</th></tr></thead><tbody id="errors"></tbody></table></div></section>

<section id="p-settings" class="panel">
<div class="section-head"><div><h2>Paramètres</h2><p class="sub">Toutes les connexions de ForcedFR sont configurables ici. Les paramètres sont conservés dans SQLite et survivent aux redémarrages. Les secrets sont masqués.</p></div></div>
<div class="settings-grid">
<div class="settings-card"><h3>🔌 Connexions</h3><p class="desc">Configure qBittorrent, Radarr, Sonarr et Discord sans modifier Docker.</p>
<div class="config-group"><h4>qBittorrent</h4><label class="config-field"><span>Adresse</span><input id="cfgQBHost"></label><label class="config-field"><span>Utilisateur</span><input id="cfgQBUsername"></label><label class="config-field"><span>Mot de passe</span><input id="cfgQBPassword" type="password" placeholder="Laisser vide pour conserver"></label><button class="test-btn" onclick="testConnection('qbittorrent')">Tester la connexion</button><div id="test-qbittorrent" class="test-result"></div></div>
<div class="config-group"><h4>Radarr</h4><label class="config-field"><span>Adresse</span><input id="cfgRadarrUrl"></label><label class="config-field"><span>Clé API</span><input id="cfgRadarrKey" type="password" placeholder="Laisser vide pour conserver"></label><button class="test-btn" onclick="testConnection('radarr')">Tester la connexion</button><div id="test-radarr" class="test-result"></div></div>
<div class="config-group"><h4>Sonarr</h4><label class="config-field"><span>Adresse</span><input id="cfgSonarrUrl"></label><label class="config-field"><span>Clé API</span><input id="cfgSonarrKey" type="password" placeholder="Laisser vide pour conserver"></label><button class="test-btn" onclick="testConnection('sonarr')">Tester la connexion</button><div id="test-sonarr" class="test-result"></div></div>
<div class="config-group"><h4>Discord</h4><label class="config-field"><span>Token du bot</span><input id="cfgDiscordToken" type="password" placeholder="Laisser vide pour conserver"></label><label class="config-field"><span>ID du salon</span><input id="cfgDiscordChannel"></label><label class="config-field"><span>Webhook URL</span><input id="cfgDiscordWebhook" type="password" placeholder="Laisser vide pour conserver"></label><button class="test-btn" onclick="testConnection('discord')">Tester le bot Discord</button><div id="test-discord" class="test-result"></div></div>
<div class="config-group"><h4>Environnement</h4><label class="config-field"><span>Fuseau horaire</span><input id="cfgTZ" placeholder="Europe/Paris"></label></div>
<div class="savebar"><button class="primary" onclick="saveConnectionSettings()">💾 Enregistrer les connexions</button></div><div class="small config-note">Les paramètres sont enregistrés dans SQLite. Les connexions qBittorrent / Radarr / Sonarr sont actualisées immédiatement. Un redémarrage est nécessaire après changement du token du bot Discord. Pour tester une connexion, enregistre d’abord les paramètres.</div>
</div>
<div><div class="settings-card"><h3>🏷️ Étiquettes qBittorrent ignorées</h3><p class="desc">Les torrents portant l’une de ces étiquettes ne seront pas analysés par ForcedFR.</p><label class="config-field"><span>Étiquettes ignorées</span><input id="cfgQBIgnoredTags" placeholder="cross-seed, autre-etiquette"></label><div class="small" style="margin-top:10px">Étiquettes actuellement disponibles dans qBittorrent :</div><div id="qbTagsAvailable" class="tag-list"></div><div class="config-note">Clique sur une étiquette pour l’ajouter. Tu peux aussi en saisir plusieurs manuellement.</div><div class="savebar"><button class="primary" onclick="saveSettings()">💾 Enregistrer les étiquettes</button></div></div><div class="settings-card" style="margin-top:18px"><h3>🔔 Notifications</h3><p class="desc">Choisis les événements qui doivent générer une notification.</p><div class="setting-item"><div class="setting-copy"><strong>Forced FR absent</strong><span>Notifier lorsqu'un torrent est mis en pause.</span></div><label class="switch"><input type="checkbox" id="setNoForced"><span></span></label></div><div class="setting-item"><div class="setting-copy"><strong>Erreur d'analyse</strong><span>Notifier lorsqu'une analyse FFprobe échoue.</span></div><label class="switch"><input type="checkbox" id="setErrors"><span></span></label></div><div class="savebar"><button class="primary" onclick="saveSettings()">💾 Enregistrer les notifications</button></div></div><div class="settings-title" style="margin-top:22px"><h2>Profils d'analyse</h2><p class="sub">Règles séparées pour les Films et les Séries.</p></div><div id="profiles" class="profile-list"></div></div>
</div></section>

<section id="p-history" class="panel"><h2>Historique des analyses</h2><p class="sub">Historique persistant des analyses et décisions prises sur les téléchargements.</p><div class="filters"><select id="historyFilter"><option value="all">Toutes les analyses</option><option value="forced_found">Forced FR détecté</option><option value="no_forced">Pas de Forced FR</option><option value="error">Erreurs</option></select><input id="historySearch" placeholder="Rechercher un torrent…"></div>
<div class="tablewrap"><table><thead><tr><th>Date</th><th>Torrent</th><th>Résultat</th><th>Décision</th><th>Action</th></tr></thead><tbody id="history"></tbody></table></div></section>

<script>
const $=x=>document.getElementById(x);let data=[],prev=false,loaded=false,timer;
async function api(u,o={}){const r=await fetch(u,o),d=await r.json();if(!r.ok)throw Error(d.detail||'Erreur');return d}
function esc(v){return String(v??'').replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#039;'}[c]))}
function st(id,s,e=''){const m={connected:['● Connecté','ok'],connecting:['● Connexion…','warn'],disabled:['● Désactivé','warn'],error:['● Indisponible','bad'],not_configured:['● Non configuré','warn'],unknown:['● Inconnu','warn']}[s]||['● '+s,'warn'];$(id).textContent=m[0]+(e?' '+e:'');$(id).className='value '+m[1]}
document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>{document.querySelectorAll('.tab').forEach(x=>x.classList.toggle('active',x===b));document.querySelectorAll('.panel').forEach(x=>x.classList.remove('active'));$('p-'+b.dataset.tab).classList.add('active');if(b.dataset.tab==='history')loadHistory();if(b.dataset.tab==='dashboard')loadDashboard();if(b.dataset.tab==='errors')loadErrors();if(b.dataset.tab==='settings')loadSettings();if(b.dataset.tab==='scan'){loadCached('all').then(()=>startScan(document.querySelector('.subtab.active')?.dataset.kind||'films','incremental'))}});
document.querySelectorAll('.subtab').forEach(b=>b.onclick=async()=>{document.querySelectorAll('.subtab').forEach(x=>x.classList.toggle('active',x===b));['films','series'].forEach(k=>$('k-'+k).style.display=k===b.dataset.kind?'block':'none');await loadCached(b.dataset.kind);startScan(b.dataset.kind,'incremental')});
async function startScan(s,m='incremental'){try{await api('/scan/'+s+'?mode='+encodeURIComponent(m),{method:'POST'});loaded=false;refresh()}catch(e){alert(e.message)}}
function rows(kind){const f=$(kind==='films'?'ff':'sf').value,q=$(kind==='films'?'fs':'ss').value.toLowerCase();return data.filter(i=>(kind==='films'?i.type==='Film':i.type==='Série')).filter(i=>{const rs=i.review_status||'pending';if(f==='all')return true;if(f==='error')return i.status==='error';if(f==='yes')return i.status!=='error'&&i.forced_french;if(f==='no')return i.status!=='error'&&!i.forced_french;if(f==='pending')return i.status!=='error'&&!i.forced_french&&rs==='pending';if(f==='validated')return i.status!=='error'&&!i.forced_french&&rs==='validated';if(f==='waiting')return i.status!=='error'&&!i.forced_french&&rs==='waiting_replacement';return true}).filter(i=>!q||(i.title+' '+(i.season||'')+' '+(i.episode||'')).toLowerCase().includes(q))}
function badge(i){return i.status==='error'?'<span class="badge err">⚠ Erreur</span>':i.forced_french?'<span class="badge yes">✅ Forced FR</span>':'<span class="badge no">❌ Sans Forced FR</span>'}
function reviewBadge(i){if(i.forced_french||i.status==='error')return '';const m={pending:'<span class="badge no">🔴 À traiter</span>',validated:'<span class="badge yes">🟢 Absence normale</span>',waiting_replacement:'<span class="badge err">🟠 En attente</span>'};return '<div class="small" style="margin-top:6px">'+(m[i.review_status||'pending']||m.pending)+'</div>'}
function mediaActions(i){let a=[];if(i.arr_url)a.push('<a class="btn" target="_blank" href="'+esc(i.arr_url)+'">Ouvrir '+esc(i.arr_source)+'</a>');if(i.status!=='error'&&!i.forced_french&&i.media_key){const key=encodeURIComponent(i.media_key);if((i.review_status||'pending')==='pending'){a.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="validated">✓ C’est normal</button>');a.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="waiting_replacement">⏳ Attendre</button>')}else{a.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="pending">↩ À traiter</button>')}}return a.join(' ')}
async function setReviewByKey(encodedKey,status){const key=decodeURIComponent(encodedKey);const i=data.find(x=>x.media_key===key);if(!i)return alert('Média introuvable dans les résultats actuels.');try{await api('/library/review',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({media_key:key,review_status:status})});i.review_status=status;i.reviewed_at=status==='pending'?null:Date.now()/1000;render('films');render('series')}catch(e){alert(e.message)}}
document.addEventListener('click',e=>{const b=e.target.closest('.review-btn');if(b){setReviewByKey(b.dataset.reviewKey,b.dataset.reviewStatus);return}const card=e.target.closest('.series-card');if(card&&!e.target.closest('a,button'))openSeries(card.dataset.seriesKey)});
function poster(i){return i.poster_url?'<img class="poster" loading="lazy" src="'+esc(i.poster_url)+'" alt="">':'<div class="poster-placeholder">'+(i.type==='Film'?'🎬':'📺')+'</div>'}
function filmCard(i){return '<article class="media-card"><a target="_blank" href="'+esc(i.arr_url||'#')+'" style="display:block">'+poster(i)+'</a><div class="media-info"><div class="media-title" title="'+esc(i.title)+'">'+esc(i.title)+'</div><div class="media-meta">'+badge(i)+'</div>'+reviewBadge(i)+(i.error?'<div class="small" style="margin-top:6px">'+esc(i.error)+'</div>':'')+'<div class="media-actions">'+mediaActions(i)+'</div></div></article>'}
function seriesCard(title,items){const first=items[0]||{};const yes=items.filter(i=>i.status!=='error'&&i.forced_french).length,no=items.filter(i=>i.status!=='error'&&!i.forced_french).length,err=items.filter(i=>i.status==='error').length;const needs=items.some(i=>i.status==='error'||(!i.forced_french&&(i.review_status||'pending')==='pending'));const q=($('ss')?.value||'').toLowerCase();if(q&&!title.toLowerCase().includes(q))return '';const sonarr=first.arr_url?'<a class="btn" target="_blank" href="'+esc(first.arr_url)+'">Ouvrir Sonarr</a>':'';const actionInfo=needs?'<span class="series-attention">⚠ '+items.filter(i=>i.status==='error'||(!i.forced_french&&(i.review_status||'pending')==='pending')).length+' à traiter</span>':'';return '<article class="media-card series-card" data-series-key="'+esc(title)+'"><div>'+poster(first)+'</div><div class="media-info"><div class="media-title" title="'+esc(title)+'">'+esc(title)+'</div><div class="media-meta">'+items.length+' épisode(s)</div><div class="series-summary"><span class="mini-pill y">'+yes+' avec FR</span><span class="mini-pill n">'+no+' sans FR</span>'+(err?'<span class="mini-pill w">'+err+' erreur(s)</span>':'')+'</div><div class="media-actions">'+sonarr+action+'</div></div></article>'}
function openSeries(title){const items=data.filter(i=>i.type==='Série'&&i.title===title).slice().sort((a,b)=>(a.season_number||0)-(b.season_number||0)||(a.episode_number||0)-(b.episode_number||0));if(!items.length)return;const first=items[0],panel=$('seriesDetail');const episodes=items.map(i=>{const status=i.status==='error'?'<span class="episode-err">⚠ Erreur</span>':i.forced_french?'<span class="episode-ok">✅ Forced FR</span>':'<span class="episode-no">❌ Sans Forced FR</span>';const actions=[];if(i.arr_url)actions.push('<a class="btn" target="_blank" href="'+esc(i.arr_url)+'">Ouvrir Sonarr</a>');if(i.status!=='error'&&!i.forced_french&&i.media_key){const key=encodeURIComponent(i.media_key),rs=i.review_status||'pending';if(rs==='pending'){actions.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="validated">✓ C’est normal</button>');actions.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="waiting_replacement">⏳ Attendre</button>')}else actions.push('<button class="review-btn" data-review-key="'+key+'" data-review-status="pending">↩ À traiter</button>')}return '<div class="episode-card"><div class="episode-number">'+esc(i.season||'—')+' · '+esc(i.episode||'—')+'</div><div><div class="episode-title">'+esc(i.episode_title||i.episode_name||'Épisode')+'</div><div class="episode-status">'+status+(i.error?' — '+esc(i.error):'')+'</div></div><div class="episode-actions">'+actions.join('')+'</div></div>'}).join('');panel.innerHTML='<button class="btn" onclick="closeSeries()">← Retour aux séries</button><div class="series-detail-head"><div>'+poster(first).replace('class="poster"','class="series-detail-poster"')+'</div><div><div class="series-detail-title">'+esc(title)+'</div><div class="series-detail-meta">'+items.length+' épisode(s) · '+items.filter(i=>i.forced_french).length+' avec Forced FR · '+items.filter(i=>i.status!=='error'&&!i.forced_french).length+' sans Forced FR'+(items.filter(i=>i.status==='error').length?' · '+items.filter(i=>i.status==='error').length+' erreur(s)':'')+'</div><div class="series-detail-actions">'+(first.arr_url?'<a class="btn" target="_blank" href="'+esc(first.arr_url)+'">Ouvrir Sonarr</a>':'')+'</div></div></div><div class="episode-list">'+episodes+'</div>';$('seriesGrid').style.display='none';panel.classList.add('active')}
function closeSeries(){$('seriesDetail').classList.remove('active');$('seriesGrid').style.display='grid'}
function render(kind){const r=rows(kind),target=$(kind);if(kind==='films'){target.innerHTML=r.length?r.map(filmCard).join(''):'<div class="empty">Aucun film correspondant.</div>';return}const groups={};r.forEach(i=>(groups[i.title]??=[]).push(i));const html=Object.entries(groups).sort((a,b)=>a[0].localeCompare(b[0],'fr')).map(([title,items])=>seriesCard(title,items)).join('');target.innerHTML=html||'<div class="empty">Aucune série correspondante.</div>'}
async function loadCached(scope='all'){try{const d=await api('/library/cached?scope='+scope);if(scope==='all')data=d.results||[];else{const other=data.filter(i=>i.type!==(scope==='films'?'Film':'Série'));data=other.concat(d.results||[])}render('films');render('series')}catch(e){console.error(e)}}
async function loadResults(){data=(await api('/scan/results')).results||[];render('films');render('series');loaded=true}
['ff','fs'].forEach(id=>$(id).oninput=()=>render('films'));['sf','ss'].forEach(id=>$(id).oninput=()=>render('series'));
function historyResult(i){const m={forced_found:'<span class="badge yes">✅ Forced FR détecté</span>',no_forced:'<span class="badge no">❌ Pas de Forced FR</span>',error:'<span class="badge err">⚠ Erreur d’analyse</span>'};return m[i.result]||'<span class="badge">'+esc(i.result||'—')+'</span>'}
function historyDecision(i){const m={auto_pause:'<span class="badge no">⏸ Pause automatique</span>',pause:'<span class="badge no">⏸ Maintenu en pause</span>',resume:'<span class="badge yes">▶ Téléchargement repris</span>'};if(m[i.last_action])return m[i.last_action];if(i.result==='no_forced')return '<span class="badge no">⏸ Pause automatique</span>';return '—'}
async function loadHistory(){const d=await api('/history');window.historyData=d.results||[];renderHistory()}
function renderHistory(){const q=($('historySearch')?.value||'').toLowerCase(),f=$('historyFilter')?.value||'all';const r=(window.historyData||[]).filter(i=>!q||(i.torrent_name||'').toLowerCase().includes(q)).filter(i=>f==='all'||i.result===f);$('history').innerHTML=r.length?r.map(i=>{const actions=[];if(i.tracker_url)actions.push('<a class="btn" target="_blank" href="'+esc(i.tracker_url)+'">Voir le torrent</a>');if(i.qb_url)actions.push('<a class="btn" target="_blank" href="'+esc(i.qb_url)+'">qBittorrent</a>');if(i.arr_url)actions.push('<a class="btn" target="_blank" href="'+esc(i.arr_url)+'">'+esc(i.arr_source==='Sonarr'?'Sonarr':'Radarr')+'</a>');return '<tr><td>'+new Date(i.timestamp*1000).toLocaleString('fr-FR',{dateStyle:'short',timeStyle:'short'})+'</td><td title="'+esc(i.torrent_name)+'">'+esc(i.torrent_name)+'</td><td>'+historyResult(i)+'</td><td>'+historyDecision(i)+'</td><td>'+actions.join(' ')+'</td></tr>'}).join(''):'<tr><td colspan="5" class="empty">Aucune analyse correspondante.</td></tr>'}

async function loadDashboard(){const d=await api('/dashboard/stats');const l=d.library,t=d.torrents;const cards=[['Bibliothèque',l.total,'médias'],['Avec Forced FR',l.forced,'validés'],['Sans Forced FR',l.no_forced,'médias'],['🔴 À traiter',l.pending,'médias'],['🟠 En attente',l.waiting,'médias'],['🟢 Absence normale',l.validated,'médias'],['⚠ Erreurs',l.errors,'à traiter'],['Torrents analysés',t.total,'analyses']];$('dashCards').innerHTML=cards.map(c=>'<div class="card"><div class="label">'+c[0]+'</div><div class="value" style="font-size:1.5rem">'+c[1]+'</div><div class="small">'+c[2]+'</div></div>').join('');$('recent').innerHTML=d.recent.length?d.recent.map(i=>{const name=i.torrent_name||'Torrent';const texts={analysis:i.result==='forced_found'?'Analyse terminée : Forced FR détecté':i.result==='no_forced'?'Analyse terminée : aucun Forced FR':'Analyse terminée',auto_pause:'Téléchargement mis en pause automatiquement',pause:'Téléchargement maintenu en pause',resume:'Téléchargement repris'};const label=texts[i.action]||'Action effectuée';return '<div class="activity-row"><div class="activity-date">'+new Date(i.created_at*1000).toLocaleString('fr-FR',{dateStyle:'short',timeStyle:'short'})+'</div><div class="activity-main"><div class="activity-action"><span class="activity-dot"></span>'+esc(label)+'</div><div class="activity-details">'+esc(name)+(i.details?' — '+esc(i.details):'')+'</div></div><div>'+ (i.result==='forced_found'?'<span class="badge yes">OK</span>':i.result==='no_forced'?'<span class="badge no">À traiter</span>':'<span class="badge err">Info</span>') +'</div></div>'}).join(''):'<div class="activity-empty">Aucune activité enregistrée pour le moment.</div>'}
async function loadErrors(){const d=await api('/errors');$('errors').innerHTML=d.results.length?d.results.map(i=>'<tr><td>'+new Date(i.last_seen*1000).toLocaleString('fr-FR',{dateStyle:'short',timeStyle:'short'})+'</td><td><strong>'+esc(i.title||'—')+'</strong><div class="small">'+esc(i.media_type)+' · '+esc(i.path||'')+'</div></td><td>'+esc(i.error)+'</td><td><button class="review-btn" onclick="resolveError('+i.id+')">✓ Marquer traité</button></td></tr>').join(''):'<tr><td colspan="4" class="empty">Aucune erreur à traiter.</td></tr>'}
async function resolveError(id){try{await api('/errors/resolve',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})});loadErrors();loadDashboard()}catch(e){alert(e.message)}}
async function loadSettings(){const [s,p,c]=await Promise.all([api('/settings'),api('/profiles'),api('/configuration')]);$('setNoForced').checked=s.notify_no_forced;$('setErrors').checked=s.notify_errors;$('cfgQBHost').value=c.values.QB_HOST||'';$('cfgQBUsername').value=c.values.QB_USERNAME||'';$('cfgQBPassword').value='';$('cfgRadarrUrl').value=c.values.RADARR_URL||'';$('cfgRadarrKey').value='';$('cfgSonarrUrl').value=c.values.SONARR_URL||'';$('cfgSonarrKey').value='';$('cfgDiscordToken').value='';$('cfgDiscordChannel').value=c.values.DISCORD_CHANNEL_ID||'';$('cfgDiscordWebhook').value='';$('cfgTZ').value=c.values.TZ||'';$('cfgQBIgnoredTags').value=s.qb_ignored_tags||'';loadQbTags();$('profiles').innerHTML=p.results.map(i=>'<div class="profile-card"><div class="profile-top"><div><div class="profile-name">'+esc(i.media_type==='Film'?'🎬 Films':'📺 Séries')+'</div><div class="profile-type">Règles appliquées aux analyses de ce type de média</div></div><span class="status-pill '+(i.enabled?'on':'off')+'">'+(i.enabled?'● Actif':'○ Inactif')+'</span></div><div class="profile-grid"><div class="profile-field"><label>Si Forced FR trouvé</label><select id="found-'+esc(i.name)+'"><option value="validate" '+(i.found_action==='validate'?'selected':'')+'>✓ Valider</option><option value="pause_notify" '+(i.found_action==='pause_notify'?'selected':'')+'>⏸ Mettre en pause + notifier Discord</option><option value="validate_notify" '+(i.found_action==='validate_notify'?'selected':'')+'>🔔 Valider + notifier Discord</option></select></div><div class="profile-field"><label>Si Forced FR absent</label><select id="tm-'+esc(i.name)+'"><option value="pause_notify" '+(i.torrent_missing_action==='pause_notify'?'selected':'')+'>⏸ Mettre en pause + notifier Discord</option><option value="continue_notify" '+(i.torrent_missing_action==='continue_notify'?'selected':'')+'>▶ Continuer + notifier Discord</option><option value="pause_decision" '+(i.torrent_missing_action==='pause_decision'?'selected':'')+'>❓ Mettre en pause et demander une décision</option></select></div><div class="profile-field"><label>Après scan bibliothèque</label><select id="miss-'+esc(i.name)+'"><option value="review" '+(i.missing_action==='review'?'selected':'')+'>À traiter</option><option value="validated" '+(i.missing_action==='validated'?'selected':'')+'>Absence normale</option><option value="waiting_replacement" '+(i.missing_action==='waiting_replacement'?'selected':'')+'>Attendre une meilleure release</option></select></div><div class="profile-field"><label>Si erreur d’analyse</label><select id="err-'+esc(i.name)+'" onchange="toggleRetryFields('+JSON.stringify(i.name)+')"><option value="notify_continue" '+(i.error_action==='notify_continue'?'selected':'')+'>▶ Continuer + notifier</option><option value="pause_decision" '+(i.error_action==='pause_decision'?'selected':'')+'>⏸ Pause + demander une décision</option><option value="retry_pause" '+(i.error_action==='retry_pause'?'selected':'')+'>🔄 Réessayer puis mettre en pause</option><option value="retry_continue" '+(i.error_action==='retry_continue'?'selected':'')+'>🔄 Réessayer puis continuer</option></select><div class="retry-fields" id="retry-'+esc(i.name)+'" style="display:'+(i.error_action==='retry_pause'||i.error_action==='retry_continue'?'grid':'none')+'"><label>Nombre de tentatives<input type="number" min="1" max="20" id="retries-'+esc(i.name)+'" value="'+(i.error_retries||5)+'"></label><label>Délai entre tentatives (s)<input type="number" min="1" max="3600" id="delay-'+esc(i.name)+'" value="'+(i.error_retry_delay||30)+'"></label></div></div></div><div class="profile-footer"><label class="setting-item" style="padding:0;border:0;justify-content:flex-start"><input type="checkbox" id="ena-'+esc(i.name)+'" '+(i.enabled?'checked':'')+' style="min-width:0;flex:none"><span>Profil actif</span></label><button class="primary" onclick="saveProfile('+JSON.stringify(i.name)+','+JSON.stringify(i.media_type)+','+(i.forced_required?'true':'false')+')">💾 Enregistrer</button></div></div>').join('')}
function toggleRetryFields(name){const v=$('err-'+name).value;const box=$('retry-'+name);if(box)box.style.display=(v==='retry_pause'||v==='retry_continue')?'grid':'none'}
async function saveProfile(name,mediaType,forcedRequired){try{await api('/profiles',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,media_type:mediaType,forced_required:forcedRequired,missing_action:$('miss-'+name).value,found_action:$('found-'+name).value,torrent_missing_action:$('tm-'+name).value,error_action:$('err-'+name).value,error_retries:parseInt($('retries-'+name)?.value||5,10),error_retry_delay:parseFloat($('delay-'+name)?.value||30),enabled:$('ena-'+name).checked})});alert('Profil enregistré.')}catch(e){alert(e.message)}}
async function testConnection(kind){const el=$('test-'+kind);el.className='test-result warn';el.textContent='Test en cours…';try{const d=await api('/test/'+kind,{method:'POST'});el.className='test-result '+(d.ok?'ok':'bad');el.textContent=(d.ok?'✓ ':'✕ ')+d.message}catch(e){el.className='test-result bad';el.textContent='✕ '+e.message}}
async function loadQbTags(){try{const d=await api('/qbittorrent/tags');const box=$('qbTagsAvailable');if(!box)return;box.innerHTML=d.results.length?d.results.map(t=>'<button type="button" class="tag-chip" onclick="addIgnoredTag('+JSON.stringify(t)+')">'+esc(t)+'</button>').join(''):'<span class="small">Aucune étiquette trouvée dans qBittorrent.</span>'}catch(e){const box=$('qbTagsAvailable');if(box)box.innerHTML='<span class="small warn">Impossible de récupérer les étiquettes qBittorrent.</span>'}}
function addIgnoredTag(tag){const input=$('cfgQBIgnoredTags');const current=input.value.split(',').map(x=>x.trim()).filter(Boolean);if(!current.some(x=>x.toLowerCase()===tag.toLowerCase()))current.push(tag);input.value=current.join(', ')}

async function saveConnectionSettings(){try{const payload={QB_HOST:$('cfgQBHost').value,QB_USERNAME:$('cfgQBUsername').value,QB_PASSWORD:$('cfgQBPassword').value,RADARR_URL:$('cfgRadarrUrl').value,RADARR_API_KEY:$('cfgRadarrKey').value,SONARR_URL:$('cfgSonarrUrl').value,SONARR_API_KEY:$('cfgSonarrKey').value,DISCORD_BOT_TOKEN:$('cfgDiscordToken').value,DISCORD_CHANNEL_ID:$('cfgDiscordChannel').value,DISCORD_WEBHOOK_URL:$('cfgDiscordWebhook').value,TZ:$('cfgTZ').value};await api('/configuration',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});await api('/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({qb_ignored_tags:$('cfgQBIgnoredTags').value})});alert('Connexions enregistrées.');refresh()}catch(e){alert(e.message)}}
async function saveSettings(){try{await api('/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({notify_no_forced:$('setNoForced').checked,notify_errors:$('setErrors').checked,qb_ignored_tags:$('cfgQBIgnoredTags').value})});alert('Paramètres enregistrés.')}catch(e){alert(e.message)}}
$('historyFilter').onchange=renderHistory;$('historySearch').oninput=renderHistory;
async function refresh(){try{const [s,x]=await Promise.all([api('/status'),api('/scan/status')]);st('qb',s.qbittorrent.status,s.qbittorrent.torrents!=null?'('+s.qbittorrent.torrents+')':'');st('discord',s.discord.status);st('radarr',s.radarr.status,s.radarr.version?'v'+s.radarr.version:'');st('sonarr',s.sonarr.status,s.sonarr.version?'v'+s.sonarr.version:'');const p=x.total_files?Math.round(x.processed_files/x.total_files*100):0;$('bar').style.width=p+'%';$('scanLabel').textContent=x.running?'Scan en cours : '+p+'%'+(x.current_file?' — '+x.current_file:''):(x.finished_at?'Dernier scan terminé.':'Aucun scan en cours.');$('stats').textContent='Analysés : '+x.processed_files+'/'+x.total_files+' • Avec FR Forced : '+x.files_with_forced_fr+' • Sans FR Forced : '+x.files_without_forced_fr+' • Cache : '+(x.cache_hits||0)+' • FFprobe : '+(x.reanalyzed||0)+' • Erreurs : '+x.errors;if(!loaded||(prev&&!x.running))await loadResults();prev=x.running;clearTimeout(timer);timer=setTimeout(refresh,x.running?5000:15000)}catch(e){console.error(e);clearTimeout(timer);timer=setTimeout(refresh,15000)}}loadDashboard();refresh();
</script></main></body></html>"""


@app.get("/status")
def status() -> dict[str, Any]:
    qb_status, qb_count = _qbittorrent_status()
    radarr_status = _arr_connection_status(RADARR_URL, RADARR_API_KEY)
    sonarr_status = _arr_connection_status(SONARR_URL, SONARR_API_KEY)

    return {
        "status": "ok",
        "version": "2.5.7",
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
        qb_login()
        torrents = get_torrents()
        return {"ok": True, "message": f"qBittorrent connecté — {len(torrents)} torrent(s)."}
    except Exception as exc:
        return {"ok": False, "message": f"qBittorrent : {exc}"}


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


@app.get("/configuration")
def configuration() -> dict[str, Any]:
    secret_keys = {"DISCORD_BOT_TOKEN", "DISCORD_WEBHOOK_URL", "QB_PASSWORD", "RADARR_API_KEY", "SONARR_API_KEY"}
    values: dict[str, Any] = {}
    configured: dict[str, bool] = {}
    for key, fallback in CONFIG_FALLBACKS.items():
        value = _setting("config_" + key, fallback)
        configured[key] = bool(value)
        if key in secret_keys:
            values[key] = "" if not value else "••••••••"
        else:
            values[key] = value
    return {"values": values, "configured": configured}

@app.post("/configuration")
def update_configuration(payload: dict[str, Any]) -> dict[str, Any]:
    global DISCORD_BOT_TOKEN, DISCORD_CHANNEL_ID, DISCORD_WEBHOOK_URL, QB_HOST, QB_PASSWORD, QB_USERNAME
    global RADARR_API_KEY, RADARR_URL, SONARR_API_KEY, SONARR_URL
    secret_keys = {"DISCORD_BOT_TOKEN", "DISCORD_WEBHOOK_URL", "QB_PASSWORD", "RADARR_API_KEY", "SONARR_API_KEY"}
    for key in CONFIG_FALLBACKS:
        if key not in payload: continue
        value = str(payload.get(key) or "").strip()
        if key in secret_keys and not value: continue
        set_setting("config_" + key, value)
    load_runtime_configuration()
    return configuration()

@app.get("/qbittorrent/tags")
def qbittorrent_tags() -> dict[str, Any]:
    return {"results": get_qbittorrent_tags()}


@app.get("/settings")
def settings() -> dict[str, Any]:
    return {
        "notify_no_forced": _setting_bool("notify_no_forced", True),
        "notify_errors": _setting_bool("notify_errors", True),
        "qb_ignored_tags": _setting("qb_ignored_tags", ""),
    }


@app.post("/settings")
def update_settings(payload: dict[str, Any]) -> dict[str, Any]:
    if "notify_no_forced" in payload:
        set_setting("notify_no_forced", "1" if bool(payload["notify_no_forced"]) else "0")
    if "notify_errors" in payload:
        set_setting("notify_errors", "1" if bool(payload["notify_errors"]) else "0")
    if "qb_ignored_tags" in payload:
        tags = str(payload.get("qb_ignored_tags") or "")
        set_setting("qb_ignored_tags", tags)
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
            INSERT INTO forcedfr_profiles(name,media_type,forced_required,missing_action,error_action,enabled,updated_at,error_retries,error_retry_delay,found_action,torrent_missing_action)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(name) DO UPDATE SET media_type=excluded.media_type,forced_required=excluded.forced_required,missing_action=excluded.missing_action,error_action=excluded.error_action,enabled=excluded.enabled,updated_at=excluded.updated_at,error_retries=excluded.error_retries,error_retry_delay=excluded.error_retry_delay,found_action=excluded.found_action,torrent_missing_action=excluded.torrent_missing_action
        """, (name,media_type,int(bool(payload.get("forced_required",True))),str(payload.get("missing_action") or "review"),str(payload.get("error_action") or "notify_continue"),int(bool(payload.get("enabled",True))),time.time(),max(1,min(20,int(payload.get("error_retries") or 5))),max(1.0,min(3600.0,float(payload.get("error_retry_delay") or 30))),str(payload.get("found_action") or "validate"),str(payload.get("torrent_missing_action") or "pause_notify")))
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
