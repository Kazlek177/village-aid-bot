import discord
from discord import app_commands
from discord.ui import Select, View, Modal, TextInput, Button
import random
import os
import asyncio
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
import aiohttp

load_dotenv()

# ====================== CLIENT SETUP ======================
intents = discord.Intents.default()
intents.members = True
intents.message_content = True

client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)


def safe_task(coro, name="task"):
    """Wrap a coroutine in a task with error logging — prevents silent failures."""
    async def _wrapper():
        try:
            await coro
        except Exception as e:
            _log_error(f"safe_task:{name}", e)
    return asyncio.create_task(_wrapper())


from contextlib import contextmanager

@contextmanager
def _log_error(context: str, e: Exception):
    """Central error logger — prints to console with context for easier debugging."""
    import traceback
    print(f"[ERROR] {context}: {type(e).__name__}: {e}", flush=True)
    traceback.print_exc()


def db_conn():
    """Context manager for SQLite connections — auto-commits and closes."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
        conn.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# ====================== DATABASE ======================
# Use /app/data/ on Railway (persistent volume) or current directory locally
# Always use /app/data on Railway — create it if it doesn't exist yet
# This ensures the volume mount is used even if the directory wasn't pre-created
_DB_DIR = "/app/data" if os.environ.get("RAILWAY_ENVIRONMENT") or os.path.isdir("/app") else "."
os.makedirs(_DB_DIR, exist_ok=True)
DB_FILE = os.path.join(_DB_DIR, "mafia_game.db")
print(f"[DB] Using database at: {DB_FILE}")

# Hardcoded village chat channel — not created by bot
# VILLAGE_CHAT_ID removed — channel stored per-server in DB
# Hardcoded Blood Board channel — mods post approved BBs here
# BB_CHANNEL_ID removed — channel stored per-server in DB


import signal as _signal

def _handle_sigterm(signum, frame):
    """Graceful shutdown on Railway SIGTERM — close DB connections cleanly."""
    print("[shutdown] SIGTERM received — shutting down gracefully", flush=True)
    try:
        # Final DB sync
        conn = sqlite3.connect(DB_FILE)
        conn.execute("PRAGMA wal_checkpoint(FULL)")
        conn.close()
    except Exception:
        pass
    import sys
    sys.exit(0)

_signal.signal(_signal.SIGTERM, _handle_sigterm)

async def db_run(func, *args, **kwargs):
    """Run a synchronous DB function in a thread pool to avoid blocking the event loop."""
    import functools
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL")   # Allow concurrent reads during writes
    conn.execute("PRAGMA synchronous=NORMAL") # Faster writes, still safe
    conn.execute("PRAGMA busy_timeout=5000")  # Wait up to 5s on lock instead of failing
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS game_roles (
                    guild_id    INTEGER,
                    role_name   TEXT,
                    description TEXT DEFAULT '',
                    count       INTEGER DEFAULT 1,
                    team        TEXT DEFAULT 'village',
                    PRIMARY KEY (guild_id, role_name)
                 )''')

    c.execute('''CREATE TABLE IF NOT EXISTS game_state (
                    guild_id              INTEGER PRIMARY KEY,
                    phase                 TEXT DEFAULT 'day',
                    night_duration        INTEGER DEFAULT 43200,
                    day_duration          INTEGER DEFAULT 43200,
                    category_id           INTEGER,
                    wolf_channel_id       INTEGER,
                    wolf_vote_channel_id  INTEGER,
                    ghost_channel_id      INTEGER,
                    mod_log_channel_id    INTEGER,
                    wheel_channel_id      INTEGER,
                    player_list_ch_id     INTEGER,
                    role_list_ch_id       INTEGER,
                    day_vote_ch_id        INTEGER,
                    timeline_ch_id        INTEGER,
                    stats_ch_id           INTEGER,
                    player_list_msg_id    INTEGER,
                    role_list_msg_id      INTEGER,
                    day_vote_msg_id       INTEGER,
                    wolf_vote_msg_id      INTEGER,
                    timeline_msg_id       INTEGER,
                    mod_role_id           INTEGER,
                    participant_role_id   INTEGER,
                    dead_role_id          INTEGER,
                    spectator_role_id     INTEGER,
                    day_vote_end_time     INTEGER,
                    font_style            TEXT DEFAULT 'default'
                 )''')

    c.execute('''CREATE TABLE IF NOT EXISTS player_assignments (
                    guild_id   INTEGER,
                    player_id  INTEGER,
                    role_name  TEXT,
                    is_alive   INTEGER DEFAULT 1,
                    channel_id INTEGER,
                    PRIMARY KEY (guild_id, player_id)
                 )''')

    c.execute('''CREATE TABLE IF NOT EXISTS night_actions (
                    guild_id    INTEGER,
                    night_num   INTEGER,
                    actor_id    INTEGER,
                    action_type TEXT,
                    target_id   INTEGER,
                    used        INTEGER DEFAULT 0,
                    PRIMARY KEY (guild_id, night_num, actor_id)
                 )''')

    c.execute('''CREATE TABLE IF NOT EXISTS witch_uses (
                    guild_id  INTEGER,
                    player_id INTEGER,
                    used_save INTEGER DEFAULT 0,
                    used_kill INTEGER DEFAULT 0,
                    PRIMARY KEY (guild_id, player_id)
                 )''')

    c.execute('''CREATE TABLE IF NOT EXISTS game_counters (
                    guild_id  INTEGER PRIMARY KEY,
                    night_num INTEGER DEFAULT 0
                 )''')

    c.execute('''CREATE TABLE IF NOT EXISTS day_votes (
                    guild_id   INTEGER,
                    voter_id   INTEGER,
                    target_id  INTEGER,  -- NULL means abstain
                    voted_at   TEXT,     -- ISO timestamp
                    PRIMARY KEY (guild_id, voter_id)
                 )''')
    # Add voted_at column to existing DBs
    try:
        c.execute("ALTER TABLE day_votes ADD COLUMN voted_at TEXT")
    except Exception:
        pass

    c.execute('''CREATE TABLE IF NOT EXISTS wolf_votes (
                    guild_id  INTEGER,
                    night_num INTEGER,
                    voter_id  INTEGER,
                    target_id INTEGER,
                    PRIMARY KEY (guild_id, night_num, voter_id)
                 )''')

    # Persistent game log entries
    c.execute('''CREATE TABLE IF NOT EXISTS game_log (
                    guild_id   INTEGER,
                    entry_id   INTEGER,
                    timestamp  TEXT,
                    phase      TEXT,
                    event      TEXT,
                    PRIMARY KEY (guild_id, entry_id)
                 )''')

    # Hall of fame pinned message tracking
    c.execute('''CREATE TABLE IF NOT EXISTS hall_of_fame (
                    guild_id   INTEGER PRIMARY KEY,
                    channel_id INTEGER,
                    message_id INTEGER
                 )''')

    # Per-guild stats across all games
    c.execute('''CREATE TABLE IF NOT EXISTS player_stats (
                    guild_id    INTEGER,
                    player_id   INTEGER,
                    games       INTEGER DEFAULT 0,
                    wins        INTEGER DEFAULT 0,
                    eliminations INTEGER DEFAULT 0,
                    PRIMARY KEY (guild_id, player_id)
                 )''')

    # Replay protection — last used role sets per guild
    c.execute('''CREATE TABLE IF NOT EXISTS last_role_set (
                    guild_id  INTEGER PRIMARY KEY,
                    role_json TEXT DEFAULT ''
                 )''')

    # Vote history — every day vote cast across all days
    c.execute('''CREATE TABLE IF NOT EXISTS vote_history (
                    guild_id   INTEGER,
                    entry_id   INTEGER,
                    day_num    INTEGER,
                    voter_id   INTEGER,
                    target_id  INTEGER,
                    action     TEXT DEFAULT 'vote',
                    voted_at   INTEGER DEFAULT 0,
                    PRIMARY KEY (guild_id, entry_id)
                 )''')
    try:
        c.execute("ALTER TABLE vote_history ADD COLUMN voted_at INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN votes_per_player INTEGER DEFAULT 1")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN werekitten_silenced_night INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE vote_history ADD COLUMN vote_change_count INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN _pending_elim_reason TEXT DEFAULT NULL")
        c.execute("ALTER TABLE game_state ADD COLUMN _pending_elim_day INTEGER DEFAULT 0")
    except Exception:
        pass

    c.execute('''CREATE TABLE IF NOT EXISTS game_templates (
                    guild_id      INTEGER,
                    name          TEXT,
                    role_counts   TEXT,
                    description   TEXT DEFAULT "",
                    created_at    INTEGER DEFAULT 0,
                    PRIMARY KEY (guild_id, name)
                 )''')
    try:
        c.execute("ALTER TABLE elimination_log ADD COLUMN elim_type TEXT DEFAULT 'unknown'")
        c.execute("ALTER TABLE elimination_log ADD COLUMN day_or_night INTEGER DEFAULT 0")
        c.execute("ALTER TABLE elimination_log ADD COLUMN eliminated_at INTEGER DEFAULT 0")
    except Exception:
        pass

    # Wraith faction tables
    c.execute('''CREATE TABLE IF NOT EXISTS wraith_marks (
                    guild_id    INTEGER,
                    marker_id   INTEGER,
                    target_id   INTEGER,
                    night_num   INTEGER,
                    PRIMARY KEY (guild_id, marker_id)
                 )''')
    c.execute('''CREATE TABLE IF NOT EXISTS wraith_state (
                    guild_id         INTEGER PRIMARY KEY,
                    kill_agreed      INTEGER DEFAULT 0,
                    kill_night       INTEGER DEFAULT 0,
                    kill_used        INTEGER DEFAULT 0,
                    wraith1_id       INTEGER DEFAULT 0,
                    wraith2_id       INTEGER DEFAULT 0,
                    den_channel_id   INTEGER DEFAULT 0
                 )''')

    # Turn log — every Alpha/Elite Alpha turn attempt
    c.execute('''CREATE TABLE IF NOT EXISTS turn_log (
                    guild_id   INTEGER,
                    entry_id   INTEGER,
                    night_num  INTEGER,
                    actor_id   INTEGER,
                    target_id  INTEGER,
                    result     TEXT,
                    PRIMARY KEY (guild_id, entry_id)
                 )''')

    # Block log — every Wolf Pup block attempt
    c.execute('''CREATE TABLE IF NOT EXISTS block_log (
                    guild_id   INTEGER,
                    entry_id   INTEGER,
                    night_num  INTEGER,
                    blocker_id INTEGER,
                    target_id  INTEGER,
                    PRIMARY KEY (guild_id, entry_id)
                 )''')

    # White Wolf strike tracking — strikes on skip or non-wolf kill
    c.execute('''CREATE TABLE IF NOT EXISTS white_wolf_strikes (
                    guild_id   INTEGER,
                    player_id  INTEGER,
                    strikes    INTEGER DEFAULT 0,
                    PRIMARY KEY (guild_id, player_id)
                 )''')

    # Stores the wheel-spun order of operations for the current night
    c.execute('''CREATE TABLE IF NOT EXISTS night_order (
                    guild_id   INTEGER PRIMARY KEY,
                    night_num  INTEGER,
                    role_order TEXT DEFAULT ''
                 )''')

    # Shadow Wolf kill list
    c.execute('''CREATE TABLE IF NOT EXISTS shadow_wolf_list (
                    guild_id    INTEGER PRIMARY KEY,
                    targets     TEXT DEFAULT '[]',
                    message_id  INTEGER,
                    channel_id  INTEGER
                 )''')

    # Elder hit tracking
    c.execute('''CREATE TABLE IF NOT EXISTS elder_hits (
                    guild_id  INTEGER PRIMARY KEY,
                    hit_count INTEGER DEFAULT 0
                 )''')

    # Cupid bond — tracks current night's bond (cleared each morning)
    c.execute('''CREATE TABLE IF NOT EXISTS cupid_bond_current (
                    guild_id  INTEGER PRIMARY KEY,
                    player1_id INTEGER,
                    player2_id INTEGER
                 )''')

    # Speech violation tracking
    c.execute('''CREATE TABLE IF NOT EXISTS speech_violations (
                    guild_id  INTEGER,
                    player_id INTEGER,
                    count     INTEGER DEFAULT 0,
                    PRIMARY KEY (guild_id, player_id)
                 )''')

    # Role history — every role a player has ever been assigned
    c.execute('''CREATE TABLE IF NOT EXISTS role_history (
                    guild_id   INTEGER,
                    player_id  INTEGER,
                    role_name  TEXT,
                    team       TEXT,
                    game_num   INTEGER,
                    outcome    TEXT DEFAULT 'unknown',
                    PRIMARY KEY (guild_id, player_id, game_num)
                 )''')

    # Jafar last role tracking — prevents back-to-back same role
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN jafar_last_role TEXT DEFAULT ''")
    except Exception:
        pass

    # Witch night tracking — which nights witch has acted
    c.execute('''CREATE TABLE IF NOT EXISTS witch_nights (
                    guild_id  INTEGER,
                    night_num INTEGER,
                    PRIMARY KEY (guild_id, night_num)
                 )''')

    # Traitor switch tracking
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN traitor_switched INTEGER DEFAULT 0")
    except Exception:
        pass

    # Agitator frenzy tracking
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN agitator_frenzy_day INTEGER DEFAULT NULL")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN bb_channel_id INTEGER DEFAULT NULL")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN agitator_elim_count INTEGER DEFAULT 0")
    except Exception:
        pass

    # Frenzy second vote tracking
    c.execute('''CREATE TABLE IF NOT EXISTS message_counts (
                    guild_id   INTEGER,
                    player_id  INTEGER,
                    day_num    INTEGER,
                    count      INTEGER DEFAULT 0,
                    PRIMARY KEY (guild_id, player_id, day_num)
                 )''')

    c.execute('''CREATE TABLE IF NOT EXISTS day_votes_2 (
                    guild_id  INTEGER,
                    voter_id  INTEGER,
                    target_id INTEGER,
                    PRIMARY KEY (guild_id, voter_id)
                 )''')

    # Phase end time tracking for /time_left
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN night_end_time INTEGER DEFAULT NULL")
    except Exception:
        pass

    # Player investigation tracker
    c.execute('''CREATE TABLE IF NOT EXISTS player_tracker (
                    guild_id    INTEGER,
                    owner_id    INTEGER,
                    target_id   INTEGER,
                    suspicion   TEXT DEFAULT "unknown",
                    suspected_role TEXT DEFAULT "",
                    notes       TEXT DEFAULT "",
                    msg_id      INTEGER DEFAULT NULL,
                    PRIMARY KEY (guild_id, owner_id, target_id)
                 )''')
    c.execute('''CREATE TABLE IF NOT EXISTS player_tracker_msg (
                    guild_id  INTEGER,
                    owner_id  INTEGER,
                    msg_id    INTEGER,
                    ch_id     INTEGER,
                    PRIMARY KEY (guild_id, owner_id)
                 )''')

    # Blessed Wolf check tracker — counts how many times each player has been investigated
    c.execute('''CREATE TABLE IF NOT EXISTS blessed_wolf_checks (
                    guild_id   INTEGER,
                    target_id  INTEGER,
                    check_count INTEGER DEFAULT 0,
                    PRIMARY KEY (guild_id, target_id)
                 )''')

    # NPC accusation memory — tracks who accused each NPC
    c.execute('''CREATE TABLE IF NOT EXISTS npc_accusations (
                    guild_id   INTEGER,
                    npc_id     INTEGER,
                    accuser_id INTEGER,
                    day_num    INTEGER,
                    PRIMARY KEY (guild_id, npc_id, accuser_id)
                 )''')

    # Hall of fame extended stats
    try:
        c.execute("ALTER TABLE player_stats ADD COLUMN wolf_votes_correct INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE player_stats ADD COLUMN times_accused INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE player_stats ADD COLUMN longest_streak INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE player_stats ADD COLUMN seer_correct INTEGER DEFAULT 0")
    except Exception:
        pass

    # Pre-game lobby
    c.execute('''CREATE TABLE IF NOT EXISTS lobby (
                    guild_id   INTEGER PRIMARY KEY,
                    player_ids TEXT DEFAULT '[]',
                    message_id INTEGER,
                    channel_id INTEGER,
                    is_open    INTEGER DEFAULT 0
                 )''')

    # Cupid bonds — pairs of player IDs linked together
    c.execute('''CREATE TABLE IF NOT EXISTS cupid_bonds (
                    guild_id   INTEGER,
                    player1_id INTEGER,
                    player2_id INTEGER,
                    PRIMARY KEY (guild_id)
                 )''')

    # Add win_tracker_ch_id to game_state if not exists
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN win_tracker_ch_id INTEGER")
    except Exception:
        pass

    # Add anon_vote to game_state if not exists
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN anon_vote INTEGER DEFAULT 0")
    except Exception:
        pass

    # Traitor switch tracking
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN traitor_switched INTEGER DEFAULT 0")
    except Exception:
        pass

    # Time Lord death flag
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN time_lord_triggered INTEGER DEFAULT 0")
    except Exception:
        pass

    # Game recap storage — elimination log
    c.execute('''CREATE TABLE IF NOT EXISTS elimination_log (
                    guild_id    INTEGER,
                    player_id   INTEGER,
                    role_name   TEXT,
                    night_num   INTEGER,
                    reason      TEXT,
                    PRIMARY KEY (guild_id, player_id)
                 )''')


    c.execute('''CREATE TABLE IF NOT EXISTS npcs (
                    guild_id      INTEGER,
                    npc_id        INTEGER,
                    name          TEXT,
                    avatar_url    TEXT,
                    personality   TEXT,
                    backstory     TEXT,
                    role_name     TEXT,
                    channel_id    INTEGER,
                    webhook_id    INTEGER,
                    webhook_token TEXT,
                    is_alive      INTEGER DEFAULT 1,
                    suspicions    TEXT DEFAULT \'[]\',
                    chat_history  TEXT DEFAULT \'[]\',
                    PRIMARY KEY (guild_id, npc_id)
                 )''')

    # Claim channel tracking — for cleanup on end_game
    c.execute('''CREATE TABLE IF NOT EXISTS ability_uses (
        guild_id    INTEGER,
        player_id   INTEGER,
        role_name   TEXT,
        uses_left   INTEGER DEFAULT 0,
        PRIMARY KEY (guild_id, player_id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS role_reservations (
        guild_id    INTEGER,
        player_id   INTEGER,
        player_name TEXT,
        role_name   TEXT,
        PRIMARY KEY (guild_id, player_id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS claim_channels (
                    guild_id   INTEGER,
                    channel_id INTEGER,
                    PRIMARY KEY (guild_id, channel_id)
                 )''')

    # Persistent NPC identity across games
    c.execute('''CREATE TABLE IF NOT EXISTS npc_identities (
                    guild_id        INTEGER,
                    name            TEXT,
                    games_played    INTEGER DEFAULT 0,
                    total_kills     INTEGER DEFAULT 0,
                    times_wolf      INTEGER DEFAULT 0,
                    times_village   INTEGER DEFAULT 0,
                    win_count       INTEGER DEFAULT 0,
                    personality     TEXT DEFAULT \'\',
                    backstory       TEXT DEFAULT \'\',
                    avatar_url      TEXT DEFAULT \'\',
                    legacy_notes    TEXT DEFAULT \'[]\',
                    PRIMARY KEY (guild_id, name)
                 )''')

    # Add memory columns to existing NPC tables
    try:
        c.execute("ALTER TABLE npcs ADD COLUMN memory_summary TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE npcs ADD COLUMN pinned_events TEXT DEFAULT '[]'")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE npcs ADD COLUMN bluff_role TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE npcs ADD COLUMN team_hint TEXT DEFAULT 'village'")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE npcs ADD COLUMN allies TEXT DEFAULT '[]'")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE npcs ADD COLUMN enemies TEXT DEFAULT '[]'")
    except Exception:
        pass

    # village-chat channel id
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN village_chat_ch_id INTEGER")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN mod_dashboard_msg_id INTEGER DEFAULT NULL")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN night_bb_done INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN day_bb_done INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN investigations_done INTEGER DEFAULT 0")
    except Exception:
        pass

    conn.commit()

init_db()

def fmt(text: str) -> str:
    """Wrap bot messages in a code block so they stand out in player channels."""
    return f"```\n{text}\n```"


# ====================== DEFAULT ROLES ======================
# These are pre-loaded into every guild on first use.
# Mods can still add/edit/remove via /add_role at any time.

DEFAULT_ROLES = [
    # ── VILLAGE ──────────────────────────────────────────────────────────
    ("Villager",        "village", 1,
     "Just a regular villager but still important in finding the pesky wolves."),
    ("Agitator",        "village", 1,
     "Once per game at night, can force the village to make TWO lynches the following day. "
     "The whole village is notified that morning. Ability cannot be reused."),
    ("Clone",           "village", 1,
     "Chooses another player the first night. If that player dies by wolf or hanging, the Clone "
     "inherits their role. Clone of a seer becomes a seer, clone of a wolf becomes a wolf."),
    ("Cupid",           "village", 1,
     "Binds two players together each night — whatever happens to one happens to the other. "
     "Pairing lasts until Cupid pairs again the following night. Active through next day's vote only."),
    ("Diseased",        "village", 1,
     "If attacked by wolves, you die — but wolves get food poisoning and cannot kill the following night. "
     "If turned, the Wolf Pup dies due to immature immune system (if applicable)."),
    ("Doctor",          "village", 1,
     "Can save ONE player from the wolves per game. Does not choose who — simply chooses to save or not "
     "each night. Cannot disclose saves. Saves do not work on turns."),
    ("Drunk",           "village", 1,
     "Can ONLY communicate in memes, gifs, and emojis. If the Drunk speaks even once, "
     "they will be killed the following nightfall."),
    ("Elder",           "village", 1,
     "Can survive a single night kill. However, if voted out, ALL village-aligned players "
     "lose their roles and become normal Villagers."),
    ("Governor",        "village", 1,
     "Can save one individual from hanging. Must contact the mod 15 minutes before the vote closes "
     "with the name of the individual. If that person isn't hung anyway, the save is wasted."),
    ("Gravedigger",     "village", 1,
     "Each night, learns not only who died but how they died. Must be careful how they share "
     "this knowledge — oversharing means digging their own grave."),
    ("Hermit",          "village", 1,
     "Lurks and only interacts when spoken to. Can hide the top-voted player in their home, "
     "causing the second-highest voted player to be killed instead. If they hide a wolf, they die. "
     "Must use ability 20 minutes before vote closes."),
    ("Huntsman",        "village", 1,
     "Each night, can protect a villager. If that villager is chosen to die, the Huntsman and the "
     "killer both die. Cannot protect the same villager twice in a row. "
     "If a protected villager is turned, both are turned."),
    ("Insomniac",       "village", 1,
     "Hears everything. Told a random player with a wolf role every other night starting Night 3."),
    ("Jafar",           "village", 1,
     "Their spell backfired — they receive a random village role ability each morning. "
     "Behaves exactly like that role for the day. May receive the same ability multiple days in a row."),
    ("Lycan",           "village", 1,
     "A villager with a rare mutation. If targeted by wolves, turns into a wolf instead of dying. "
     "Appears as a wolf to Seer checks. Can only die by hanging while on the village side."),
    ("Mayor",           "village", 1,
     "Once per game in a tie vote, can choose whether both players die or select who gets the noose."),
    ("Medium",          "village", 1,
     "Starting Night 2, learns the alignment of one player (good/bad/neutral) every night. "
     "Elite Alpha and Blessed Wolf appear as good. Cannot explicitly tell anyone what they learned or they die."),
    ("Pothead",         "village", 1,
     "If eaten by the wolves, they get the munchies — giving the wolves a second kill that night."),
    ("Prostitute",      "village", 1,
     "Can ONLY communicate via sexual innuendo, gifs, emojis, or suggestive words. "
     "If caught being pure even once, contracts a horrendous STD and dies."),
    ("Seer",            "village", 1,
     "Can ask the mod once each night if a specific player is a wolf. Receives yes or no. "
     "Cannot disclose if they identified a wolf or villager — doing so results in immediate death."),
    ("Shapeshifter",    "village", 1,
     "First night only, permanently takes the form and role of any player they choose — "
     "village, wolf, or neutral. Receives that player's full ability for the rest of the game. "
     "If they choose a wolf they get den access and are treated as a wolf. "
     "Cannot talk before picking. Must choose before the first morning post or they die."),
    ("Sheriff",         "village", 1,
     "If wolves select the Sheriff to die, a random wolf dies in their place instead. "
     "If the Alpha tries to turn the Sheriff, the Alpha dies. Elite Alpha only loses the turn attempt."),
    ("Surgeon",         "village", 1,
     "Can save a player up to THREE times per game. Chooses to save or not each night — cannot pick who. "
     "Cannot disclose saves. If a turn is attempted on a save night, the turn fails at the cost of two saves."),
    ("Time Lord",       "village", 1,
     "Controls the game clock. If killed by wolves or vote, the game speeds up — all deadlines move earlier. "
     "Example: hanging votes due by 3pm, wolf votes by 6pm, night board before midnight, etc."),
    ("Traitor",         "village", 1,
     "A wolf in sheep's clothing. Behaves as a villager, appears as village to Seer. Not in the den. "
     "Switches sides only if ALL other wolves die — wheel spun to determine if regular or roled killing wolf. "
     "Dies as a villager if killed by wolves or vote."),
    ("Village Idiot",   "village", 1,
     "Can only speak in typos and gibberish. Immediate death if caught speaking correctly even once."),
    ("Village Jokester","village", 1,
     "If hung, gets to kill one of the players who voted for them. Can still be killed by wolves normally."),
    ("Virgin",          "village", 1,
     "Pure of heart, mind, and body — repulsed by vulgar talk. Everything said must be wholesome. "
     "Meets an untimely demise if they say anything impure."),
    # ── WOLF ─────────────────────────────────────────────────────────────
    ("Wolf",            "wolf",    1,
     "No special abilities but in the den. The majority must agree on which villager to kill each night. "
     "Together, the pack survives."),
    ("Alpha",           "wolf",    1,
     "Has ONE chance in the game to turn a villager into a wolf. Choosing to turn the Sheriff "
     "results in the Alpha's death."),
    ("Blessed Wolf",    "wolf",    1,
     "Cunning and charming — shows as good to Seer and Medium checks. "
     "If checked a second time, charm wears off and result shows as Cunning."),
    ("Bloodhound",      "wolf",    1,
     "Wolf equivalent of the Seer. Starting Night 2, can ask the exact identity of one player each night. "
     "Not in the den but info is shared with wolves. When no wolves remain in the den, becomes a killing wolf."),
    ("Bloodletter",     "wolf",    1,
     "Spills their own blood and marks a target with it — the target appears as a wolf to any checks "
     "for two nights. Can only be used twice per game."),
    ("Crazed Wolf",     "wolf",    1,
     "Starting Night 3, can kill TWO villagers in one night up to two times (at least one night apart). "
     "This replaces the typical wolf kill. Kill order must be submitted to mods in their role channel."),
    ("Dire Wolf",       "wolf",    1,
     "First night, secretly chooses a player to covet. If that player dies, the Dire Wolf dies too "
     "from heartbreak. Cannot reveal their chosen mate to the den. The mate has no knowledge of this bond."),
    ("Echo-Stalker",    "wolf",    1,
     "Instead of killing, can choose to Haunt a player. That player's vote the next day is secretly "
     "controlled by the Echo-Stalker."),
    ("Elite Alpha",     "wolf",    1,
     "Can make TWO turns per game, at least two nights apart. No wolf kill happens on a turn night. "
     "Appears as a villager if checked. If voted out, a random player dies alongside them."),
    ("Shadow Wolf",     "wolf",    1,
     "If voted out, can kill one person who voted for them each night starting the night they died."),
    ("Werekitten",      "wolf",    1,
     "Total cuteness overload. Appears as a villager to the Seer. "
     "Bypasses the Sheriff (too adorable to shoot) and the Huntsman's protection. "
     "Doctor and Surgeon saves still apply. "
     "The den can use the Werekitten for a kill a maximum of 2 times per game. "
     "If voted out, their true identity is revealed and all role actions are silenced for the night."),
    ("White Wolf",      "village", 1,
     "Feels remorse. Starting Night 1, can choose to kill or not each night independently of the pack. "
     "If no successful kill within three nights, the White Wolf dies. NOT in the den and not in wolf count. "
     "Can be turned but loses all abilities."),
    ("Wolf Pup",        "wolf",    1,
     "Can block a player every night — if that player has a special role, they cannot use it for 24 hours. "
     "Cannot target the same person two nights in a row. Not in the den. If last wolf standing, "
     "loses ability but becomes an adult killing wolf."),
    # ── NEUTRAL ──────────────────────────────────────────────────────────
    ("Fairy Elf",       "neutral", 1,
     "Once per game, can perform a happy ending and bring back a player that was lost. "
     "Any player may contact a mod with the happy ending they want — mod tells the Fairy Elf "
     "and they choose to grant it or not."),
    ("Oracle",          "neutral", 1,
     "Starting Night 2, consults their crystal ball to ask the mods one yes/no question per night. "
     "Must handle the wisdom lightly — cannot tell the village outright or meets an untimely end."),
    ("Warlock",         "neutral", 1,
     "Once per game, can grant a wish at the price of a player from the game. "
     "Any player contacts a mod with a wish — mod tells the Warlock a wish has been requested. "
     "Warlock doesn't know what it is. If granted, the wheel is spun to determine the price paid."),
    ("Witch",           "neutral", 1,
     "Has one healing potion and one poison potion per game. Healing saves a wolf victim; "
     "poison kills another player. Can use one, both, or neither every other night starting Night 2."),
    ("Wraith",          "neutral", 2,
     "Third faction — two Wraiths, one purpose. Each night, mark one player silently. "
     "The marked player will not know. The Blood Board will hint that something moved through Whisperfall. "
     "Marks persist until the Kill Command is issued. When both Wraiths agree, replace your mark with the "
     "Kill Command — all marked players die simultaneously. One-time ability; Wraiths cannot act again after. "
     "If one Wraith dies, the survivor inherits all marks and can still use the Kill Command alone, "
     "but cannot mark and kill on the same night. If both die before Kill Command fires, all marks dissolve. "
     "Appears as Neutral to Seer and Medium. Doctor/Surgeon saves do NOT work against the Kill Command. "
     "Win: Wraiths alive and equal or outnumber BOTH village and wolf teams simultaneously."),
]

def load_default_roles(guild_id):
    """Ensure all default roles exist for a guild — adds missing ones without removing custom ones."""
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    # Always INSERT OR IGNORE — safe for both new and existing guilds.
    # New guilds get all defaults. Existing guilds get any missing ones added.
    for name, team, cnt, desc in DEFAULT_ROLES:
        c.execute("INSERT OR IGNORE INTO game_roles VALUES (?,?,?,?,?)",
                  (guild_id, name, desc, cnt, team))
    conn.commit()
    conn.close()
    invalidate_cache(guild_id)

# ====================== UNICODE FONT SYSTEM ======================
# Discord doesn't support custom fonts, but Unicode has character sets
# that visually look like different typefaces. These work in channel names,
# embed titles, and messages.

FONT_STYLES = {
    "default":      {"label": "Default",           "example": "Village Game"},
    "bold_serif":   {"label": "Bold Serif",         "example": "𝐕𝐢𝐥𝐥𝐚𝐠𝐞 𝐆𝐚𝐦𝐞"},
    "italic":       {"label": "Italic",             "example": "𝘝𝘪𝘭𝘭𝘢𝘨𝘦 𝘎𝘢𝘮𝘦"},
    "bold_italic":  {"label": "Bold Italic",        "example": "𝙑𝙞𝙡𝙡𝙖𝙜𝙚 𝙂𝙖𝙢𝙚"},
    "small_caps":   {"label": "Small Caps",         "example": "Vɪʟʟᴀɢᴇ Gᴀᴍᴇ"},
    "double_struck":{"label": "Double Struck",      "example": "𝕍𝕚𝕝𝕝𝕒𝕘𝕖 𝔾𝕒𝕞𝕖"},
    "monospace":    {"label": "Monospace",          "example": "𝚅𝚒𝚕𝚕𝚊𝚐𝚎 𝙶𝚊𝚖𝚎"},
    "fraktur":      {"label": "Fraktur / Gothic",   "example": "𝔙𝔦𝔩𝔩𝔞𝔤𝔢 𝔊𝔞𝔪𝔢"},
}

# Maps each ASCII letter to its Unicode equivalent per style
_FONT_MAP = {
    "bold_serif": {
        **{chr(ord('a')+i): chr(0x1D41A+i) for i in range(26)},
        **{chr(ord('A')+i): chr(0x1D400+i) for i in range(26)},
        **{chr(ord('0')+i): chr(0x1D7CE+i) for i in range(10)},
    },
    "italic": {
        **{chr(ord('a')+i): chr(0x1D622+i) for i in range(26)},
        **{chr(ord('A')+i): chr(0x1D608+i) for i in range(26)},
    },
    "bold_italic": {
        **{chr(ord('a')+i): chr(0x1D656+i) for i in range(26)},
        **{chr(ord('A')+i): chr(0x1D63C+i) for i in range(26)},
    },
    "double_struck": {
        **{chr(ord('a')+i): chr(0x1D552+i) for i in range(26)},
        **{chr(ord('A')+i): chr(0x1D538+i) for i in range(26)},
        # Special cases
        "C": "ℂ", "H": "ℍ", "N": "ℕ", "P": "ℙ", "Q": "ℚ", "R": "ℝ", "Z": "ℤ",
    },
    "monospace": {
        **{chr(ord('a')+i): chr(0x1D68A+i) for i in range(26)},
        **{chr(ord('A')+i): chr(0x1D670+i) for i in range(26)},
        **{chr(ord('0')+i): chr(0x1D7F6+i) for i in range(10)},
    },
    "fraktur": {
        **{chr(ord('a')+i): chr(0x1D51E+i) for i in range(26)},
        **{chr(ord('A')+i): chr(0x1D504+i) for i in range(26)},
        # Special cases
        "C": "ℭ", "H": "ℌ", "I": "ℑ", "R": "ℜ", "Z": "ℨ",
    },
    "small_caps": {
        "a":"ᴀ","b":"ʙ","c":"ᴄ","d":"ᴅ","e":"ᴇ","f":"ꜰ","g":"ɢ","h":"ʜ",
        "i":"ɪ","j":"ᴊ","k":"ᴋ","l":"ʟ","m":"ᴍ","n":"ɴ","o":"ᴏ","p":"ᴘ",
        "q":"Q","r":"ʀ","s":"s","t":"ᴛ","u":"ᴜ","v":"ᴠ","w":"ᴡ","x":"x",
        "y":"ʏ","z":"ᴢ",
        "A":"ᴀ","B":"ʙ","C":"ᴄ","D":"ᴅ","E":"ᴇ","F":"ꜰ","G":"ɢ","H":"ʜ",
        "I":"ɪ","J":"ᴊ","K":"ᴋ","L":"ʟ","M":"ᴍ","N":"ɴ","O":"ᴏ","P":"ᴘ",
        "Q":"Q","R":"ʀ","S":"s","T":"ᴛ","U":"ᴜ","V":"ᴠ","W":"ᴡ","X":"x",
        "Y":"ʏ","Z":"ᴢ",
    },
}

def apply_font(text: str, style: str) -> str:
    """Convert ASCII text to a Unicode font style. Emojis and symbols pass through unchanged."""
    if style == "default" or style not in _FONT_MAP:
        return text
    mapping = _FONT_MAP[style]
    return "".join(mapping.get(ch, ch) for ch in text)

def ch_name(label: str, style: str, emoji: str = "") -> str:
    """Build a channel name: emoji prefix + styled label, lowercased, spaces->hyphens.
    Unicode font chars don't need lowercasing — only the ASCII fallback does."""
    styled = apply_font(label, style)
    base   = f"{emoji}{styled}" if emoji else styled
    # Discord channel names: no uppercase ASCII, spaces become hyphens
    # Unicode chars are fine as-is; only replace spaces
    return base.replace(" ", "-")

# Per-guild font preference stored in memory (resets on restart, that's fine)
def get_guild_font(guild_id: int) -> str:
    """Read font preference from DB (persists across restarts)."""
    state = cached_get_state(guild_id)
    return state.get("font_style") or "default"

def set_guild_font(guild_id: int, style: str):
    """Persist font preference to DB so it survives across games and restarts."""
    db_set_state(guild_id, font_style=style)

# ====================== DB HELPERS ======================

def db_get_ww_strikes(guild_id, player_id) -> int:
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT strikes FROM white_wolf_strikes WHERE guild_id=? AND player_id=?",
              (guild_id, player_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def db_add_ww_strike(guild_id, player_id) -> int:
    """Add one strike and return the new total."""
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("""INSERT INTO white_wolf_strikes (guild_id, player_id, strikes) VALUES (?,?,1)
                 ON CONFLICT(guild_id, player_id) DO UPDATE SET strikes=strikes+1""",
              (guild_id, player_id))
    c.execute("SELECT strikes FROM white_wolf_strikes WHERE guild_id=? AND player_id=?",
              (guild_id, player_id))
    total = c.fetchone()[0]
    conn.commit()
    conn.close()
    return total

def db_clear_ww_strikes(guild_id, player_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM white_wolf_strikes WHERE guild_id=? AND player_id=?",
              (guild_id, player_id))
    conn.commit()
    conn.close()

def db_save_role(guild_id, name, description, count, team):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO game_roles VALUES (?,?,?,?,?)",
              (guild_id, name, description, count, team.lower()))
    conn.commit()
    conn.close()
    invalidate_cache(guild_id)

def db_load_roles(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT role_name, description, count, team FROM game_roles WHERE guild_id=?", (guild_id,))
    rows = c.fetchall()
    conn.close()

    return [{"name": r[0], "description": r[1], "count": r[2], "team": r[3]} for r in rows]

def db_delete_role(guild_id, name):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM game_roles WHERE guild_id=? AND role_name=?", (guild_id, name))
    conn.commit()
    conn.close()
    invalidate_cache(guild_id)

def db_get_state(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT * FROM game_state WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return None
    # Use cursor description so column order never matters regardless of ALTER TABLEs
    cols = [d[0] for d in c.description]
    result = dict(zip(cols, row))
    conn.close()
    return result

def db_set_state(guild_id, **kwargs):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR IGNORE INTO game_state (guild_id) VALUES (?)", (guild_id,))
    for key, val in kwargs.items():
        c.execute(f"UPDATE game_state SET {key}=? WHERE guild_id=?", (val, guild_id))
    conn.commit()
    conn.close()
    invalidate_cache(guild_id)

def db_clear_state(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    for tbl in ["game_state","player_assignments","night_actions",
                "witch_uses","game_counters","day_votes","day_votes_2","wolf_votes","game_log",
                "cupid_bonds","shadow_wolf_list","elder_hits","lobby",
                "blessed_wolf_checks","npc_accusations","elimination_log","witch_nights",
                "cupid_bond_current","speech_violations","turn_log","block_log",
                "player_tracker","player_tracker_msg","claim_channels",
                "npcs","role_history","night_order","vote_history",
                "wraith_marks","wraith_state","role_reservations","ability_uses",
                "white_wolf_strikes","message_counts"]:
        c.execute(f"DELETE FROM {tbl} WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()
    invalidate_cache(guild_id)

# ── Ability Uses Tracking ─────────────────────────────────────────────────
def db_init_ability_uses(guild_id, player_id, role_name, uses):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO ability_uses VALUES (?,?,?,?)",
              (guild_id, player_id, role_name, uses))
    conn.commit()
    conn.close()

def db_get_ability_uses(guild_id, player_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT uses_left FROM ability_uses WHERE guild_id=? AND player_id=?",
              (guild_id, player_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def db_deduct_ability_uses(guild_id, player_id, amount=1):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("""UPDATE ability_uses SET uses_left = MAX(0, uses_left - ?)
                 WHERE guild_id=? AND player_id=?""", (amount, guild_id, player_id))
    conn.commit()
    conn.close()

# ── Role Reservations ─────────────────────────────────────────────────────
def db_set_reservation(guild_id, player_id, player_name, role_name):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO role_reservations VALUES (?,?,?,?)",
              (guild_id, player_id, player_name, role_name))
    conn.commit()
    conn.close()

def db_get_reservations(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT player_id, player_name, role_name FROM role_reservations WHERE guild_id=?", (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows  # [(player_id, player_name, role_name), ...]

def db_clear_reservation(guild_id, player_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM role_reservations WHERE guild_id=? AND player_id=?", (guild_id, player_id))
    conn.commit()
    conn.close()

def db_clear_all_reservations(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM role_reservations WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()

def db_save_assignments(guild_id, assignments, channel_map):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM player_assignments WHERE guild_id=?", (guild_id,))
    for pid, role in assignments.items():
        c.execute("INSERT INTO player_assignments VALUES (?,?,?,1,?)",
                  (guild_id, pid, role, channel_map.get(pid)))
    conn.commit()
    conn.close()

def db_get_assignments(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT player_id, role_name, is_alive, channel_id FROM player_assignments WHERE guild_id=?", (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def db_set_player_alive(guild_id, player_id, alive: bool):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("UPDATE player_assignments SET is_alive=? WHERE guild_id=? AND player_id=?",
              (1 if alive else 0, guild_id, player_id))
    conn.commit()
    conn.close()
    invalidate_cache(guild_id)

def db_get_night_num(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT night_num FROM game_counters WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def db_increment_night(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR IGNORE INTO game_counters VALUES (?,0)", (guild_id,))
    c.execute("UPDATE game_counters SET night_num=night_num+1 WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()

def db_save_night_action(guild_id, night_num, actor_id, action_type, target_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    # Check if action already exists
    c.execute("SELECT action_type FROM night_actions WHERE guild_id=? AND night_num=? AND actor_id=?",
              (guild_id, night_num, actor_id))
    existing = c.fetchone()
    is_update = existing is not None and existing[0] != action_type
    c.execute("INSERT OR REPLACE INTO night_actions VALUES (?,?,?,?,?,0)",
              (guild_id, night_num, actor_id, action_type, target_id))
    conn.commit()
    conn.close()
    invalidate_cache(guild_id)
    return is_update  # True if this was an update to an existing action

def db_get_night_actions(guild_id, night_num):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT actor_id, action_type, target_id FROM night_actions WHERE guild_id=? AND night_num=?",
              (guild_id, night_num))
    rows = c.fetchall()
    conn.close()
    return rows

def db_get_witch_uses(guild_id, player_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT used_save, used_kill FROM witch_uses WHERE guild_id=? AND player_id=?", (guild_id, player_id))
    row = c.fetchone()
    conn.close()
    return (row[0], row[1]) if row else (0, 0)

def db_set_witch_use(guild_id, player_id, save=None, kill=None):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR IGNORE INTO witch_uses VALUES (?,?,0,0)", (guild_id, player_id))
    if save is not None:
        c.execute("UPDATE witch_uses SET used_save=? WHERE guild_id=? AND player_id=?", (save, guild_id, player_id))
    if kill is not None:
        c.execute("UPDATE witch_uses SET used_kill=? WHERE guild_id=? AND player_id=?", (kill, guild_id, player_id))
    conn.commit()
    conn.close()

def db_set_day_vote(guild_id, voter_id, target_id, day_num=None):
    from datetime import timezone
    ts   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO day_votes VALUES (?,?,?,?)", (guild_id, voter_id, target_id, ts))
    conn.commit()
    conn.close()
    # NOTE: vote_history is recorded by the caller with the correct action type
    # (vote vs change vs abstain) — not auto-recorded here to avoid duplicate entries


def db_increment_message_count(guild_id, player_id, day_num):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("""INSERT INTO message_counts (guild_id, player_id, day_num, count)
                 VALUES (?,?,?,1)
                 ON CONFLICT(guild_id, player_id, day_num)
                 DO UPDATE SET count = count + 1""",
              (guild_id, player_id, day_num))
    conn.commit()
    conn.close()

def db_get_message_counts(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("""SELECT player_id, SUM(count) as total, GROUP_CONCAT(day_num||':'||count, ', ') as breakdown
                 FROM message_counts WHERE guild_id=?
                 GROUP BY player_id ORDER BY total DESC""", (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows  # (player_id, total, breakdown)

def db_set_day_vote_2(guild_id, voter_id, target_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO day_votes_2 VALUES (?,?,?)", (guild_id, voter_id, target_id))
    conn.commit()
    conn.close()

def db_get_day_votes_2(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT voter_id, target_id FROM day_votes_2 WHERE guild_id=?", (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def db_remove_day_vote_2(guild_id, voter_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM day_votes_2 WHERE guild_id=? AND voter_id=?", (guild_id, voter_id))
    conn.commit()
    conn.close()

def db_remove_day_vote(guild_id, voter_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM day_votes WHERE guild_id=? AND voter_id=?", (guild_id, voter_id))
    conn.commit()
    conn.close()

def db_get_day_votes(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT voter_id, target_id, voted_at FROM day_votes WHERE guild_id=? ORDER BY voted_at ASC", (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows  # (voter_id, target_id, voted_at)

def db_clear_day_votes(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM day_votes WHERE guild_id=?", (guild_id,))
    c.execute("DELETE FROM day_votes_2 WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()

def db_set_wolf_vote(guild_id, night_num, voter_id, target_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO wolf_votes VALUES (?,?,?,?)",
              (guild_id, night_num, voter_id, target_id))
    conn.commit()
    conn.close()

def db_get_wolf_votes(guild_id, night_num):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT voter_id, target_id FROM wolf_votes WHERE guild_id=? AND night_num=?",
              (guild_id, night_num))
    rows = c.fetchall()
    conn.close()
    return rows

# Game log
def db_log_event(guild_id, phase, event):
    import time as _t
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT COALESCE(MAX(entry_id),0)+1 FROM game_log WHERE guild_id=?", (guild_id,))
    next_id = c.fetchone()[0]
    ts = str(int(_t.time()))  # Unix timestamp — renders in user's local timezone
    c.execute("INSERT INTO game_log VALUES (?,?,?,?,?)", (guild_id, next_id, ts, phase, event))
    conn.commit()
    conn.close()

def db_get_log(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT timestamp, phase, event FROM game_log WHERE guild_id=? ORDER BY entry_id", (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows

# Vote history / Turn log / Block log helpers
def db_record_vote_history(guild_id, day_num, voter_id, target_id, action="vote"):
    import time as _tv
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT COALESCE(MAX(entry_id),0)+1 FROM vote_history WHERE guild_id=?", (guild_id,))
    eid = c.fetchone()[0]
    # Count how many times this voter has voted this day (for change count)
    c.execute(
        "SELECT COUNT(*) FROM vote_history WHERE guild_id=? AND day_num=? AND voter_id=? AND action=?",
        (guild_id, day_num, voter_id, "vote"))
    prior_votes = c.fetchone()[0]
    change_count = max(0, prior_votes)  # 0 = first vote, 1+ = changes
    c.execute("INSERT INTO vote_history VALUES (?,?,?,?,?,?,?,?)",
              (guild_id, eid, day_num, voter_id, target_id, action, int(_tv.time()), change_count))
    conn.commit()
    conn.close()

def db_get_vote_history(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute(
        "SELECT day_num, voter_id, target_id, action, COALESCE(voted_at,0), COALESCE(vote_change_count,0) FROM vote_history WHERE guild_id=? ORDER BY entry_id",
        (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def db_record_turn(guild_id, night_num, actor_id, target_id, result):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT COALESCE(MAX(entry_id),0)+1 FROM turn_log WHERE guild_id=?", (guild_id,))
    eid = c.fetchone()[0]
    c.execute("INSERT INTO turn_log VALUES (?,?,?,?,?,?)",
              (guild_id, eid, night_num, actor_id, target_id, result))
    conn.commit()
    conn.close()

def db_get_turn_log(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute(
        "SELECT night_num, actor_id, target_id, result FROM turn_log WHERE guild_id=? ORDER BY entry_id",
        (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def db_record_block(guild_id, night_num, blocker_id, target_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT COALESCE(MAX(entry_id),0)+1 FROM block_log WHERE guild_id=?", (guild_id,))
    eid = c.fetchone()[0]
    c.execute("INSERT INTO block_log VALUES (?,?,?,?,?)",
              (guild_id, eid, night_num, blocker_id, target_id))
    conn.commit()
    conn.close()

def db_get_block_log(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute(
        "SELECT night_num, blocker_id, target_id FROM block_log WHERE guild_id=? ORDER BY entry_id",
        (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows




# Elder hit helpers
def db_get_elder_hits(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT hit_count FROM elder_hits WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def db_increment_elder_hit(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR IGNORE INTO elder_hits VALUES (?,0)", (guild_id,))
    c.execute("UPDATE elder_hits SET hit_count=hit_count+1 WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()

def db_clear_elder_hits(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM elder_hits WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()

# Lobby helpers
def db_get_lobby(guild_id):
    import json
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT player_ids, message_id, channel_id, is_open FROM lobby WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "player_ids": json.loads(row[0] or "[]"),
        "message_id": row[1],
        "channel_id": row[2],
        "is_open":    bool(row[3]),
    }

def db_set_lobby(guild_id, player_ids, message_id=None, channel_id=None, is_open=True):
    import json
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO lobby VALUES (?,?,?,?,?)",
              (guild_id, json.dumps(player_ids), message_id, channel_id, 1 if is_open else 0))
    conn.commit()
    conn.close()

def db_clear_lobby(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM lobby WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()

# Shadow Wolf kill list helpers
def db_set_shadow_wolf_list(guild_id, targets: list, message_id=None, channel_id=None):
    import json
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO shadow_wolf_list VALUES (?,?,?,?)",
              (guild_id, json.dumps(targets), message_id, channel_id))
    conn.commit()
    conn.close()

def db_get_shadow_wolf_list(guild_id):
    import json
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT targets, message_id, channel_id FROM shadow_wolf_list WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "targets":    json.loads(row[0] or "[]"),
        "message_id": row[1],
        "channel_id": row[2],
    }

def db_clear_shadow_wolf_list(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM shadow_wolf_list WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()

# Cupid bond helpers
def db_set_cupid_bond(guild_id, player1_id, player2_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO cupid_bonds VALUES (?,?,?)",
              (guild_id, player1_id, player2_id))
    conn.commit()
    conn.close()

def db_get_cupid_bond(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT player1_id, player2_id FROM cupid_bonds WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    conn.close()
    return row  # (p1, p2) or None

def db_clear_cupid_bond(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM cupid_bonds WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()

# Stats
def db_update_stats(guild_id, player_ids, winner_ids, eliminated_ids):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    for pid in player_ids:
        c.execute("INSERT OR IGNORE INTO player_stats (guild_id, player_id, games, wins, eliminations) VALUES (?,?,0,0,0)", (guild_id, pid))
        c.execute("UPDATE player_stats SET games=games+1 WHERE guild_id=? AND player_id=?", (guild_id, pid))
    for pid in winner_ids:
        c.execute("UPDATE player_stats SET wins=wins+1 WHERE guild_id=? AND player_id=?", (guild_id, pid))
    for pid in eliminated_ids:
        c.execute("UPDATE player_stats SET eliminations=eliminations+1 WHERE guild_id=? AND player_id=?", (guild_id, pid))
    conn.commit()
    conn.close()

def db_get_stats(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT player_id, games, wins, eliminations FROM player_stats WHERE guild_id=? ORDER BY wins DESC", (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows

# Replay protection
def db_save_last_roles(guild_id, role_dict):
    import json
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO last_role_set VALUES (?,?)", (guild_id, json.dumps(role_dict)))
    conn.commit()
    conn.close()

def db_get_last_roles(guild_id):
    import json
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT role_json FROM last_role_set WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    conn.close()

    return json.loads(row[0]) if row and row[0] else {}


# ====================== NPC DB HELPERS ======================

def db_save_npc(guild_id, npc_id, name, avatar_url, personality, backstory,
                role_name, channel_id, webhook_id, webhook_token,
                bluff_role="", team_hint="village"):
    import json
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    # Use named columns so we never break on schema changes
    c.execute("""INSERT OR REPLACE INTO npcs
                 (guild_id, npc_id, name, avatar_url, personality, backstory,
                  role_name, channel_id, webhook_id, webhook_token, is_alive,
                  suspicions, chat_history, memory_summary, pinned_events,
                  bluff_role, team_hint, allies, enemies)
                 VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?,?,?,?,?,?,?)""",
              (guild_id, npc_id, name, avatar_url, personality, backstory,
               role_name, channel_id, webhook_id, webhook_token,
               "[]", "[]", "", "[]",
               bluff_role, team_hint, "[]", "[]"))
    conn.commit()
    conn.close()

def db_get_npcs(guild_id):
    import json
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT * FROM npcs WHERE guild_id=?", (guild_id,))
    rows = c.fetchall()
    cols = ["guild_id","npc_id","name","avatar_url","personality","backstory",
            "role_name","channel_id","webhook_id","webhook_token","is_alive",
            "suspicions","chat_history","memory_summary","pinned_events","bluff_role","team_hint","allies","enemies"]
    result = []
    for row in rows:
        # Pad row if old DB missing new columns
        row = list(row) + [""] * (len(cols) - len(row))
        d = dict(zip(cols, row))
        d["suspicions"]    = json.loads(d["suspicions"]    or "[]")
        d["chat_history"]  = json.loads(d["chat_history"]  or "[]")
        d["memory_summary"]= d["memory_summary"] or ""
        d["pinned_events"] = json.loads(d["pinned_events"] or "[]")
        result.append(d)
    return result


def db_update_npc_memory(guild_id, npc_id, memory_summary: str):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("UPDATE npcs SET memory_summary=? WHERE guild_id=? AND npc_id=?",
              (memory_summary, guild_id, npc_id))
    conn.commit()
    conn.close()

def db_update_npc_pinned_events(guild_id, npc_id, events: list):
    import json
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("UPDATE npcs SET pinned_events=? WHERE guild_id=? AND npc_id=?",
              (json.dumps(events), guild_id, npc_id))
    conn.commit()
    conn.close()

def db_pin_npc_event(guild_id, npc_id, event: str):
    """Add a single pinned event — these never get compressed away."""
    npc = db_get_npc(guild_id, npc_id)
    if not npc:
        return
    events = npc.get("pinned_events", [])
    events.append(event)
    # Keep max 50 pinned events — oldest drop off only after 50
    events = events[-50:]
    db_update_npc_pinned_events(guild_id, npc_id, events)

def db_get_npc(guild_id, npc_id):
    npcs = db_get_npcs(guild_id)
    return next((n for n in npcs if n["npc_id"] == npc_id), None)


def db_update_npc_relationship(guild_id, npc_id, name: str, rel_type: str):
    """Add a name to an NPC's ally or enemy list. rel_type: 'ally' or 'enemy'"""
    import json as _jr
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    col  = "allies" if rel_type == "ally" else "enemies"
    c.execute(f"SELECT {col} FROM npcs WHERE guild_id=? AND npc_id=?", (guild_id, npc_id))
    row  = c.fetchone()
    if not row:
        conn.close()
        return
    current = _jr.loads(row[0] or "[]")
    if name not in current:
        current.append(name)
        current = current[-8:]  # Keep last 8
        c.execute(f"UPDATE npcs SET {col}=? WHERE guild_id=? AND npc_id=?",
                  (_jr.dumps(current), guild_id, npc_id))
        conn.commit()
        conn.close()

def db_update_npc_suspicions(guild_id, npc_id, suspicions: list):
    import json
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("UPDATE npcs SET suspicions=? WHERE guild_id=? AND npc_id=?",
              (json.dumps(suspicions), guild_id, npc_id))
    conn.commit()
    conn.close()

def db_update_npc_chat_history(guild_id, npc_id, history: list):
    import json
    # Hard cap at 50 messages — compression should keep it lower but this is the safety net
    history = history[-50:]
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("UPDATE npcs SET chat_history=? WHERE guild_id=? AND npc_id=?",
              (json.dumps(history), guild_id, npc_id))
    conn.commit()
    conn.close()

def db_set_npc_alive(guild_id, npc_id, alive: bool):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("UPDATE npcs SET is_alive=? WHERE guild_id=? AND npc_id=?",
              (1 if alive else 0, guild_id, npc_id))
    conn.commit()
    conn.close()

def db_delete_npcs(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM npcs WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()




# Cupid current bond helpers
def db_set_cupid_current(guild_id, player1_id, player2_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO cupid_bond_current VALUES (?,?,?)",
              (guild_id, player1_id, player2_id))
    conn.commit()
    conn.close()

def db_get_cupid_current(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT player1_id, player2_id FROM cupid_bond_current WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    conn.close()
    return row  # (p1, p2) or None

def db_clear_cupid_current(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM cupid_bond_current WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()

# Speech violation helpers
def db_get_violations(guild_id, player_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT count FROM speech_violations WHERE guild_id=? AND player_id=?",
              (guild_id, player_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def db_increment_violation(guild_id, player_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR IGNORE INTO speech_violations VALUES (?,?,0)", (guild_id, player_id))
    c.execute("UPDATE speech_violations SET count=count+1 WHERE guild_id=? AND player_id=?",
              (guild_id, player_id))
    conn.commit()
    conn.close()
    conn2 = sqlite3.connect(DB_FILE)
    c2    = conn2.cursor()
    c2.execute("SELECT count FROM speech_violations WHERE guild_id=? AND player_id=?",
               (guild_id, player_id))
    row = c2.fetchone()
    conn2.close()
    return row[0] if row else 1


# Role history helpers
def db_record_role_history(guild_id, player_id, role_name, team, outcome="unknown"):
    """Record a role played in a completed game."""
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    # Use game count as game_num
    c.execute("SELECT COALESCE(MAX(game_num),0)+1 FROM role_history WHERE guild_id=? AND player_id=?",
              (guild_id, player_id))
    game_num = c.fetchone()[0]
    c.execute("INSERT OR REPLACE INTO role_history VALUES (?,?,?,?,?,?)",
              (guild_id, player_id, role_name, team, game_num, outcome))
    conn.commit()
    conn.close()

def db_get_role_history(guild_id, player_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT role_name, team, game_num, outcome FROM role_history "
              "WHERE guild_id=? AND player_id=? ORDER BY game_num DESC",
              (guild_id, player_id))
    rows = c.fetchall()
    conn.close()
    return rows  # [(role_name, team, game_num, outcome), ...]

# Witch night helpers
def db_record_witch_night(guild_id, night_num):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR IGNORE INTO witch_nights VALUES (?,?)", (guild_id, night_num))
    conn.commit()
    conn.close()

def db_witch_can_act(guild_id, night_num):
    """Witch can act on even nights starting Night 2."""
    if night_num < 2:
        return False
    return night_num % 2 == 0

def db_witch_already_acted(guild_id, night_num):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT 1 FROM witch_nights WHERE guild_id=? AND night_num=?", (guild_id, night_num))
    row = c.fetchone()
    conn.close()
    return row is not None

# Blessed Wolf check helpers
def db_get_check_count(guild_id, target_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT check_count FROM blessed_wolf_checks WHERE guild_id=? AND target_id=?",
              (guild_id, target_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def db_increment_check(guild_id, target_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR IGNORE INTO blessed_wolf_checks VALUES (?,?,0)", (guild_id, target_id))
    c.execute("UPDATE blessed_wolf_checks SET check_count=check_count+1 WHERE guild_id=? AND target_id=?",
              (guild_id, target_id))
    conn.commit()
    conn.close()
    conn2 = sqlite3.connect(DB_FILE)
    c2    = conn2.cursor()
    c2.execute("SELECT check_count FROM blessed_wolf_checks WHERE guild_id=? AND target_id=?",
               (guild_id, target_id))
    row = c2.fetchone()
    conn2.close()
    return row[0] if row else 1

# NPC accusation helpers
def db_record_npc_accusation(guild_id, npc_id, accuser_id, day_num):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO npc_accusations VALUES (?,?,?,?)",
              (guild_id, npc_id, accuser_id, day_num))
    conn.commit()
    conn.close()

def db_get_npc_accusers(guild_id, npc_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT accuser_id, day_num FROM npc_accusations WHERE guild_id=? AND npc_id=?",
              (guild_id, npc_id))
    rows = c.fetchall()
    conn.close()
    return rows  # [(accuser_id, day_num), ...]

# Extended HoF stat helpers
def db_update_wolf_vote_correct(guild_id, player_id, correct: bool):
    if not correct:
        return
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR IGNORE INTO player_stats VALUES (?,?,0,0,0,0,0,0,0)", (guild_id, player_id))
    c.execute("UPDATE player_stats SET wolf_votes_correct=wolf_votes_correct+1 WHERE guild_id=? AND player_id=?",
              (guild_id, player_id))
    conn.commit()
    conn.close()

def db_update_times_accused(guild_id, player_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR IGNORE INTO player_stats VALUES (?,?,0,0,0,0,0,0,0)", (guild_id, player_id))
    c.execute("UPDATE player_stats SET times_accused=times_accused+1 WHERE guild_id=? AND player_id=?",
              (guild_id, player_id))
    conn.commit()
    conn.close()

def db_update_seer_correct(guild_id, player_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR IGNORE INTO player_stats VALUES (?,?,0,0,0,0,0,0,0)", (guild_id, player_id))
    c.execute("UPDATE player_stats SET seer_correct=seer_correct+1 WHERE guild_id=? AND player_id=?",
              (guild_id, player_id))
    conn.commit()
    conn.close()

# ====================== CACHE ======================
_state_cache = {}
_roles_cache  = {}

def invalidate_cache(guild_id):
    _state_cache.pop(guild_id, None)
    invalidate_role_info_cache(guild_id)
    _roles_cache.pop(guild_id, None)

def cached_get_state(guild_id):
    if guild_id not in _state_cache:
        _state_cache[guild_id] = db_get_state(guild_id) or {}
    return _state_cache[guild_id]

def cached_load_roles(guild_id):
    if guild_id not in _roles_cache:
        _roles_cache[guild_id] = db_load_roles(guild_id)
    return _roles_cache[guild_id]

# ====================== HELPERS ======================
# Roles that get a night action view in their private channel
NIGHT_ABILITY_ROLES = {
    # Village
    "Seer", "Doctor", "Surgeon", "Witch",
    "Sheriff", "Huntsman", "Insomniac", "Medium", "Gravedigger",
    "Agitator", "Clone", "Shapeshifter", "Cupid",
    # Wolf
    "Wolf Pup", "Alpha", "Elite Alpha", "Bloodhound",
    "Bloodletter", "Crazed Wolf", "Dire Wolf", "Echo-Stalker",
    "Shadow Wolf", "Werekitten", "White Wolf",
    # Neutral
    "Witch", "Oracle", "Warlock", "Fairy Elf",
}

# Roles that get no status buttons — just the night message
NIGHT_NO_BUTTON_ROLES = {
    "Villager", "Diseased", "Drunk", "Lycan", "Mayor", "Pothead",
    "Prostitute", "Time Lord", "Traitor", "Village Idiot", "Village Jokester",
    "Virgin", "Blessed Wolf", "Fairy Elf", "Warlock",
    "Sheriff",    # Passive — kills Alpha on turn attempt, no active button
    "Wolf",       # Coordinates in den — no private channel action
    "Insomniac",  # Hints fire automatically — no button needed
    "Governor",   # Day pardon via /governor_pardon — no night button
    "Gravedigger",# Mod delivers death info passively — no button
    "Oracle",     # Submits question via /action to mod — no button
    "Elder",      # Passive — survives one kill
    "Jokester",   # Day role only
    "Shadow Wolf", # Gets action view only AFTER death — not while alive
    "Sheriff",     # Passive — kills wolf automatically if attacked, no choice needed
    "Insomniac",   # Hints fire automatically — no button needed
    "Gravedigger", # Mod delivers death info passively — no button needed
    "Governor",    # Pardon is a day action — button sent to private channel on vote open
    "Hermit",      # Day ability — button sent to private channel on vote open
}

# Roles that skip night (no active ability — just wait)
NIGHT_PASSIVE_ROLES = {
    "Villager", "Village Idiot", "Village Jokester", "Drunk",
    "Prostitute", "Virgin", "Pothead", "Diseased", "Elder",
    "Mayor", "Time Lord", "Lycan", "Traitor", "Jafar",
    "Wolf", "Blessed Wolf", "White Wolf",  # White Wolf handled separately
}

def get_role_info(guild_id, role_name):
    # Use dict lookup instead of linear scan
    roles = cached_load_roles(guild_id)
    if not hasattr(get_role_info, "_cache") or get_role_info._cache.get(guild_id) is None:
        get_role_info._cache = getattr(get_role_info, "_cache", {})
    cache = get_role_info._cache
    if guild_id not in cache:
        cache[guild_id] = {r["name"]: r for r in roles}
    # Rebuild if cache is stale (role count changed)
    if len(cache[guild_id]) != len(roles):
        cache[guild_id] = {r["name"]: r for r in roles}
    return cache[guild_id].get(role_name,
        {"name": role_name, "description": "", "count": 1, "team": "village"})

def get_team(guild_id, role_name):
    return get_role_info(guild_id, role_name).get("team", "village")

def invalidate_role_info_cache(guild_id):
    """Call when roles are added/removed."""
    cache = getattr(get_role_info, "_cache", {})
    cache.pop(guild_id, None)

# Fast in-memory mod role cache — populated by /setup_roles, never hits DB
_mod_role_cache = {}  # guild_id -> role_id

def is_mod():
    async def predicate(interaction: discord.Interaction):
        if interaction.user.guild_permissions.administrator:
            return True
        state   = db_get_state(interaction.guild_id) or {}
        role_id = state.get("mod_role_id")
        if not role_id:
            return False
        # Use interaction.user.roles — always fresh for slash command interactions
        return any(r.id == role_id for r in interaction.user.roles)
    return app_commands.check(predicate)


def get_game_roles(guild_id) -> list:
    """Return role info dicts for only the roles currently in the active game.
    Falls back to full pool if no game is active."""
    rows = db_get_assignments(guild_id)
    if not rows:
        return cached_load_roles(guild_id)
    game_role_names = list(dict.fromkeys(r[1] for r in rows))  # Unique, preserve order
    all_roles       = cached_load_roles(guild_id)
    role_map        = {r["name"]: r for r in all_roles}
    return [role_map[n] for n in game_role_names if n in role_map]


def db_get_wraith_state(guild_id) -> dict:
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT * FROM wraith_state WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    if not row:
        return {}
    keys = ["guild_id","kill_agreed","kill_night","kill_used","wraith1_id","wraith2_id","den_channel_id"]
    conn.close()
    return dict(zip(keys, row))

def db_set_wraith_state(guild_id, **kwargs):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR IGNORE INTO wraith_state (guild_id) VALUES (?)", (guild_id,))
    for k, v in kwargs.items():
        c.execute(f"UPDATE wraith_state SET {k}=? WHERE guild_id=?", (v, guild_id))
    conn.commit()
    conn.close()

def db_get_wraith_marks(guild_id) -> list:
    """Returns list of (marker_id, target_id, night_num)."""
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT marker_id, target_id, night_num FROM wraith_marks WHERE guild_id=?", (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def db_set_wraith_mark(guild_id, marker_id, target_id, night_num):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO wraith_marks VALUES (?,?,?,?)",
              (guild_id, marker_id, target_id, night_num))
    conn.commit()
    conn.close()

def db_clear_wraith_marks(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM wraith_marks WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()


def db_log_elimination(guild_id, player_id, role_name, reason_type, day_or_night, note=""):
    """Log an elimination with reason type and timing.
    reason_type: 'vote', 'wolf_kill', 'mod_kill', 'cupid', 'diseased', 'sheriff',
                 'werekitten', 'wraith', 'witch', 'shadow_wolf', 'white_wolf', 'traitor_reveal'
    day_or_night: day number or night number when it occurred
    """
    import time as _tel
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO elimination_log VALUES (?,?,?,?,?,?,?,?)",
        (guild_id, player_id, role_name, day_or_night, note, reason_type, day_or_night, int(_tel.time())))
    conn.commit()
    conn.close()

def db_get_elimination_log(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute(
        "SELECT player_id, role_name, reason, elim_type, day_or_night, eliminated_at "
        "FROM elimination_log WHERE guild_id=? ORDER BY eliminated_at",
        (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows


def db_save_template(guild_id, name: str, role_counts: dict, description: str = ""):
    import json, time as _tt
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO game_templates VALUES (?,?,?,?,?)",
              (guild_id, name.lower(), json.dumps(role_counts), description, int(_tt.time())))
    conn.commit()
    conn.close()

def db_get_template(guild_id, name: str):
    import json
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT name, role_counts, description FROM game_templates WHERE guild_id=? AND name=?",
              (guild_id, name.lower()))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return {"name": row[0], "role_counts": json.loads(row[1]), "description": row[2]}

def db_list_templates(guild_id):
    import json
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT name, role_counts, description, created_at FROM game_templates WHERE guild_id=? ORDER BY name",
              (guild_id,))
    rows = c.fetchall()
    conn.close()

    return [{"name": r[0], "role_counts": json.loads(r[1]), "description": r[2], "created_at": r[3]} for r in rows]

def db_delete_template(guild_id, name: str):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM game_templates WHERE guild_id=? AND name=?", (guild_id, name.lower()))
    conn.commit()
    conn.close()

def game_active(guild_id):
    # Always check DB directly — cache can be stale if game just launched
    state = db_get_state(guild_id)
    return bool(state and state.get("category_id"))

async def post_mod_log(guild, message: str = "", embed=None):
    # Always hit DB fresh — avoids stale cache routing messages to wrong channel
    state = db_get_state(guild.id) or {}
    ch_id = state.get("mod_log_channel_id")
    ch    = guild.get_channel(ch_id or 0)
    if ch:
        await ch.send(message, embed=embed)

async def log_event(guild, phase: str, event: str):
    """Log to DB and refresh timeline embed."""
    db_log_event(guild.id, phase, event)
    await refresh_timeline(guild)

# ====================== EMBED BUILDERS ======================

def build_player_list_embed(guild, rows, log=None):
    embed = discord.Embed(title="👥 Players This Game", color=0x5865F2)

    # Build death timing map from game log if provided
    death_timing = {}
    if log:
        import re as _re
        for ts, phase, event in log:
            if "eliminated" in event.lower():
                match = _re.search(r"\*\*(.+?)\*\*\s+eliminated", event)
                if match:
                    name = match.group(1).strip()
                    death_timing[name.lower()] = phase

    alive_lines = []
    dead_lines  = []
    for pid, role_name, is_alive, _ in rows:
        member = guild.get_member(pid)
        name   = member.display_name if member else f"Unknown ({pid})"
        if is_alive:
            alive_lines.append(f"✅ {name}")
        else:
            when = death_timing.get(name.lower(), "")
            dead_lines.append(f"💀 {name}" + (f" *({when})*" if when else ""))

    lines = alive_lines + dead_lines
    embed.description = "\n".join(lines) or "No players."
    alive_count = len(alive_lines)
    dead_count  = len(dead_lines)
    embed.set_footer(text=f"✅ Alive: {alive_count}  •  💀 Eliminated: {dead_count}")
    return embed

def build_role_list_embed(final_counts, all_roles):
    """Build role list — returns multiple embeds if needed (Discord 6000 char limit)."""
    village_lines, wolf_lines, neutral_lines = [], [], []
    for role_name, count in final_counts.items():
        info  = all_roles.get(role_name, {})
        team  = info.get("team", "village")
        desc  = info.get("description") or "No description."
        line  = f"**{role_name}**\n{desc}"
        if team == "wolf": wolf_lines.append(line)
        elif team == "neutral": neutral_lines.append(line)
        else: village_lines.append(line)

    # Build one embed per team so descriptions aren't truncated
    embeds = []
    if village_lines:
        e = discord.Embed(title="📜 Village Roles", color=0x27AE60)
        for line in village_lines:
            parts = line.split("\n", 1)
            e.add_field(name=parts[0], value=parts[1][:1024] if len(parts) > 1 else "—", inline=False)
        embeds.append(e)
    if wolf_lines:
        e = discord.Embed(title="🐺 Wolf Roles", color=0xC0392B)
        for line in wolf_lines:
            parts = line.split("\n", 1)
            e.add_field(name=parts[0], value=parts[1][:1024] if len(parts) > 1 else "—", inline=False)
        embeds.append(e)
    if neutral_lines:
        e = discord.Embed(title="⚖️ Neutral Roles", color=0xF39C12)
        for line in neutral_lines:
            parts = line.split("\n", 1)
            e.add_field(name=parts[0], value=parts[1][:1024] if len(parts) > 1 else "—", inline=False)
        embeds.append(e)

    # Tag footer on last embed
    if embeds:
        embeds[-1].set_footer(text="Roles are public — who has them is secret.")
    return embeds  # Returns list now

async def post_role_list_embeds(channel, final_counts, all_roles):
    """Post full role descriptions to a channel."""
    embeds = build_role_list_embed(final_counts, all_roles)
    for embed in embeds:
        await channel.send(embed=embed)

def _vote_bar(count, total, width=8):
    """Build a simple visual bar showing vote weight."""
    if total == 0:
        return "░" * width
    filled = round((count / total) * width)
    return "█" * filled + "░" * (width - filled)

def _parse_voted_at(voted_at_str):
    """Parse voted_at text to unix timestamp for Discord rendering."""
    try:
        from datetime import timezone as _tz
        dt = datetime.strptime(voted_at_str[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=_tz.utc)
        return int(dt.timestamp())
    except Exception:
        return None

def build_day_vote_embed(guild, votes, rows, end_time=None, anonymous=False):
    pid_to_name = {}
    for pid, _, _, _ in rows:
        m = guild.get_member(pid)
        pid_to_name[pid] = m.display_name if m else str(pid)

    tally    = {}  # target_id -> [(voter_name, voted_at_ts)]
    abstains = []
    for voter_id, target_id, *rest in votes:
        voter_name = pid_to_name.get(voter_id, str(voter_id))
        voted_at   = rest[0] if rest else None
        ts         = _parse_voted_at(voted_at) if voted_at else None
        if target_id is None:
            abstains.append(voter_name)
        else:
            tally.setdefault(target_id, []).append((voter_name, ts))

    total_votes = sum(len(v) for v in tally.values())

    embed = discord.Embed(
        title="🗳️ Village Day Vote — Live",
        description="Vote using the buttons below. Results update in real time.",
        color=0xFF4444
    )

    if end_time:
        embed.description += f"\n⏰ Closes <t:{end_time}:t> · <t:{end_time}:R>"

    if not tally and not abstains:
        embed.add_field(name="No votes yet", value="Press **Cast Vote** to vote.", inline=False)
    else:
        if tally:
            sorted_targets = sorted(tally.items(), key=lambda x: len(x[1]), reverse=True)
            top_count = len(sorted_targets[0][1])
            for target_id, voter_entries in sorted_targets:
                target_name = pid_to_name.get(target_id, str(target_id))
                count       = len(voter_entries)
                leading     = "🔴 " if count == top_count else ""
                bar         = _vote_bar(count, total_votes)
                pct         = round(count / total_votes * 100) if total_votes else 0
                field_name  = f"{leading}{target_name} — {count} vote(s)  {bar} {pct}%"
                if anonymous:
                    embed.add_field(name=field_name, value="*Voters hidden until vote closes*", inline=False)
                else:
                    voter_lines = []
                    for vname, vts in voter_entries:
                        ts_str = f" <t:{vts}:t>" if vts else ""
                        voter_lines.append(f"• {vname}{ts_str}")
                    embed.add_field(name=field_name, value="\n".join(voter_lines), inline=False)
        if abstains:
            count = len(abstains)
            embed.add_field(name="🤐 Abstaining",
                            value=f"{count} player(s)" if anonymous else ", ".join(abstains),
                            inline=False)

    alive_count = sum(1 for r in rows if r[2] == 1)

    # Show second votes if frenzy is active
    try:
        conn_sv = sqlite3.connect(DB_FILE)
        c_sv    = conn_sv.cursor()
        # Detect which guild we're in via the votes/rows
        # Use guild.id directly
        c_sv.execute(
            "SELECT target_id, COUNT(*) c FROM day_votes_2 WHERE guild_id=? GROUP BY target_id ORDER BY c DESC",
            (guild.id,))
        sv_rows = c_sv.fetchall()
        conn_sv.close()
        if sv_rows:
            sv_lines = []
            for tid, cnt in sv_rows:
                tname = pid_to_name.get(tid, str(tid))
                sv_lines.append(f"**{tname}** — {cnt}")
            embed.add_field(name="⚡ Frenzy 2nd Votes", value="\n".join(sv_lines), inline=False)
    except Exception:
        pass

    embed.set_footer(text=f"{len(votes)}/{alive_count} alive players have responded")
    return embed

def build_wolf_vote_embed(guild, wolf_votes, alive_players, wolf_player_ids, night_num,
                          den_channel=None):
    pid_to_name = {p.id: p.display_name for p in alive_players}

    # Only count wolves who can actually see the den
    if den_channel:
        voting_wolf_ids = [wid for wid in wolf_player_ids
                           if den_channel.permissions_for(guild.get_member(wid) or guild.me).view_channel]
    else:
        voting_wolf_ids = wolf_player_ids

    tally = {}
    for voter_id, target_id in wolf_votes:
        m = guild.get_member(voter_id)
        tally.setdefault(target_id, []).append(m.display_name if m else str(voter_id))

    voted_wolves = {v for v, _ in wolf_votes}
    not_voted    = [wid for wid in voting_wolf_ids if wid not in voted_wolves]
    total_votes  = sum(len(v) for v in tally.values())

    embed = discord.Embed(
        title=f"🐺 Wolf Kill Vote — Night {night_num}",
        description="Vote for tonight's kill target. You can change your vote before night ends.",
        color=0x8B0000
    )
    if tally:
        sorted_targets = sorted(tally.items(), key=lambda x: len(x[1]), reverse=True)
        top_count = len(sorted_targets[0][1])
        for target_id, voters in sorted_targets:
            target_name = pid_to_name.get(target_id, str(target_id))
            count       = len(voters)
            leading     = "🎯 " if count == top_count else ""
            bar         = _vote_bar(count, total_votes)
            embed.add_field(
                name=f"{leading}{target_name} — {count} vote(s)  {bar}",
                value="• " + "\n• ".join(voters),
                inline=False
            )
    else:
        embed.add_field(name="No votes yet", value="Use the dropdown below.", inline=False)

    if not_voted:
        names = [guild.get_member(wid).display_name if guild.get_member(wid) else str(wid)
                 for wid in not_voted]
        embed.add_field(name="⏳ Waiting on", value=", ".join(names), inline=False)

    embed.set_footer(text=f"{len(voted_wolves)}/{len(voting_wolf_ids)} wolves have voted")
    return embed

def build_timeline_embed(guild_id, state):
    phase     = state.get("phase", "day")
    night_num = db_get_night_num(guild_id)
    if phase == "night":
        phase_label = f"🌙 Night {night_num}"
    else:
        phase_label = f"☀️ Day {night_num}" if night_num > 0 else "☀️ Day 1"

    embed = discord.Embed(title="⏰ Game Timeline", color=0xF1C40F)
    embed.add_field(name="Current Phase", value=phase_label, inline=False)

    log = db_get_log(guild_id)
    if log:
        def _fmt_ts(ts):
            try:
                return f"<t:{int(ts)}:t>"  # Shows local time e.g. "3:45 PM"
            except Exception:
                return ts  # Fallback for old HH:MM strings
        lines = [f"{_fmt_ts(ts)} **[{ph}]** {ev}" for ts, ph, ev in log[-20:]]
        embed.add_field(name="📜 Event Log (last 20)", value="\n".join(lines), inline=False)
    else:
        embed.add_field(name="📜 Event Log", value="No events yet.", inline=False)

    embed.set_footer(text="Updates automatically as the game progresses.")
    return embed

def build_stats_embed(guild, rows):
    embed = discord.Embed(title="📊 All-Time Player Stats", color=0x2ECC71)
    if not rows:
        embed.description = "No stats recorded yet."
        return embed
    lines = []
    medals = ["🥇","🥈","🥉"]
    for i, (pid, games, wins, elims) in enumerate(rows[:15]):
        m = guild.get_member(pid)
        name = m.display_name if m else f"<@{pid}>"
        medal = medals[i] if i < 3 else f"`#{i+1}`"
        win_rate = f"{(wins/games*100):.0f}%" if games > 0 else "0%"
        lines.append(f"{medal} **{name}** — {wins}W/{games}G ({win_rate}) · {elims} elims")
    embed.description = "\n".join(lines)
    embed.set_footer(text="Win rate = wins / games played")
    return embed


async def refresh_hall_of_fame(guild):
    """Edit the Hall of Fame embed in-place with latest stats."""
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT channel_id, message_id FROM hall_of_fame WHERE guild_id=?", (guild.id,))
    row  = c.fetchone()
    if not row:
        return
    ch_id, msg_id = row
    ch = guild.get_channel(ch_id)
    if not ch:
        return
    try:
        msg       = await ch.fetch_message(msg_id)
        stat_rows = db_get_stats(guild.id)
        embed     = build_hall_of_fame_embed(guild, stat_rows)
        await msg.edit(embed=embed)
    except Exception as e:
        print(f"[refresh_hall_of_fame] {e}")
    finally:
        conn.close()

def build_hall_of_fame_embed(guild, rows):
    """Pinned embed showing the top 3 players of all time with extended stats."""
    guild_id = guild.id
    embed = discord.Embed(
        title="🏛️ Hall of Fame",
        description="The greatest players in Village Aid history.",
        color=0xF1C40F
    )
    medals  = ["🥇","🥈","🥉"]
    flavors = ["The undisputed champion.", "A formidable force.", "A worthy contender."]
    top3    = [r for r in rows if r[1] > 0][:3]
    if not top3:
        embed.add_field(name="No entries yet", value="Play some games to appear here!", inline=False)

    # Extended stats from DB — scoped to this guild
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()

    for i, row in enumerate(top3):
        pid   = row[0]
        games = row[1]
        wins  = row[2]
        elims = row[3]
        m        = guild.get_member(pid)
        name     = m.display_name if m else f"<@{pid}>"
        win_rate = f"{(wins/games*100):.0f}%" if games > 0 else "0%"

        # Fetch extended stats — scoped to this guild
        c.execute(
            "SELECT wolf_votes_correct, times_accused, seer_correct FROM player_stats "
            "WHERE guild_id=? AND player_id=?",
            (guild_id, pid))
        ext = c.fetchone()
        wolf_correct = ext[0] if ext and ext[0] else 0
        accused      = ext[1] if ext and ext[1] else 0
        seer_correct = ext[2] if ext and ext[2] else 0

        extras = []
        if wolf_correct > 0:  extras.append(f"🎯 {wolf_correct} correct wolf votes")
        if accused > 0:       extras.append(f"🫵 Accused {accused}x without being wolf")
        if seer_correct > 0:  extras.append(f"🔮 {seer_correct} correct Seer checks")
        extras_text = " · ".join(extras) if extras else ""

        embed.add_field(
            name=f"{medals[i]} {name}",
            value=(f"{flavors[i]}\n"
                   f"**{wins}** wins · **{games}** games · **{win_rate}** win rate · **{elims}** elims"
                   + (f"\n{extras_text}" if extras_text else "")),
            inline=False
        )


    # Special records — scoped to this guild
    conn2 = sqlite3.connect(DB_FILE)
    c2    = conn2.cursor()
    c2.execute(
        "SELECT player_id, wolf_votes_correct FROM player_stats "
        "WHERE guild_id=? ORDER BY wolf_votes_correct DESC LIMIT 1",
        (guild_id,))
    top_voter = c2.fetchone()
    c2.execute(
        "SELECT player_id, times_accused FROM player_stats "
        "WHERE guild_id=? ORDER BY times_accused DESC LIMIT 1",
        (guild_id,))
    top_accused = c2.fetchone()
    c2.execute(
        "SELECT player_id, seer_correct FROM player_stats "
        "WHERE guild_id=? ORDER BY seer_correct DESC LIMIT 1",
        (guild_id,))
    top_seer = c2.fetchone()
    conn2.close()

    records = []
    if top_voter and top_voter[1]:
        m = guild.get_member(top_voter[0])
        records.append(f"🎯 Best wolf-hunter: **{m.display_name if m else top_voter[0]}** ({top_voter[1]} correct votes)")
    if top_accused and top_accused[1]:
        m = guild.get_member(top_accused[0])
        records.append(f"🫵 Village scapegoat: **{m.display_name if m else top_accused[0]}** (accused {top_accused[1]}x)")
    if top_seer and top_seer[1]:
        m = guild.get_member(top_seer[0])
        records.append(f"🔮 Best Seer: **{m.display_name if m else top_seer[0]}** ({top_seer[1]} correct checks)")
    if records:
        embed.add_field(name="🏆 Records", value="\n".join(records), inline=False)

    embed.set_footer(text="Updated automatically after every game.")
    return embed







# ====================== PRE-GAME LOBBY ======================

def _build_lobby_embed(guild, player_ids: list, is_open: bool, roster_size: int = 0) -> discord.Embed:
    embed = discord.Embed(
        title       = "🎮 Village Game — Lobby",
        description = "Click **Join Game** to add yourself to the player list.",
        color       = 0x5865F2 if is_open else 0x95A5A6
    )
    if player_ids:
        names = []
        for pid in player_ids:
            m = guild.get_member(pid)
            names.append(f"✅ {m.display_name}" if m else f"✅ {pid}")
        embed.add_field(name=f"Players ({len(player_ids)}{f'/{roster_size}' if roster_size else ''})",
                        value="\n".join(names), inline=False)
    else:
        embed.add_field(name="Players (0)", value="No one yet — be the first!", inline=False)
    if not is_open:
        embed.add_field(name="Status", value="🔒 Lobby closed.", inline=False)
    elif roster_size and len(player_ids) >= roster_size:
        embed.add_field(name="Status", value=f"✅ Full! ({len(player_ids)}/{roster_size}) — waiting for mod to start.", inline=False)
    else:
        slots_left = f" — {roster_size - len(player_ids)} slot(s) remaining" if roster_size else ""
        embed.add_field(name="Status", value=f"🟢 Open{slots_left}", inline=False)
    embed.set_footer(text="Mod will start the game when the lobby is full.")
    return embed


class LobbyJoinView(View):
    def __init__(self, guild_id, roster_size=0):
        super().__init__(timeout=None)
        self.guild_id    = guild_id
        self.roster_size = roster_size
        join_btn  = Button(label="Join Game",  style=discord.ButtonStyle.green)
        leave_btn = Button(label="Leave Game", style=discord.ButtonStyle.secondary)
        join_btn.callback  = self.on_join
        leave_btn.callback = self.on_leave
        self.add_item(join_btn)
        self.add_item(leave_btn)

    async def on_join(self, interaction: discord.Interaction):
        data = db_get_lobby(interaction.guild_id)
        if not data or not data["is_open"]:
            return await interaction.response.send_message("❌ Lobby is not open.", ephemeral=True)
        if interaction.user.id in data["player_ids"]:
            return await interaction.response.send_message("❌ You are already in the lobby.", ephemeral=True)
        player_ids = data["player_ids"] + [interaction.user.id]
        db_set_lobby(interaction.guild_id, player_ids, data["message_id"], data["channel_id"])

        # Assign participant role
        state   = cached_get_state(interaction.guild_id)
        p_role  = interaction.guild.get_role(state.get("participant_role_id") or 0)
        if p_role:
            try: await interaction.user.add_roles(p_role)
            except: pass

        # Send ephemeral welcome confirmation
        slot_info = f" ({len(player_ids)}/{self.roster_size})" if self.roster_size else ""
        await interaction.response.send_message(
            fmt(f"✅ You have joined the lobby for **{interaction.guild.name}**{slot_info}.\n"
                f"Stand by — the mod will start the game when everyone is ready.\n"
                f"Use **Leave Game** if you need to withdraw."),
            ephemeral=True)

        # Refresh embed
        embed = _build_lobby_embed(interaction.guild, player_ids, True, self.roster_size)
        try:
            ch  = interaction.guild.get_channel(data["channel_id"])
            msg = await ch.fetch_message(data["message_id"])
            await msg.edit(embed=embed, view=self)
        except Exception:
            pass

        # Notify mod if full
        if self.roster_size and len(player_ids) >= self.roster_size:
            mod_role_id = state.get("mod_role_id")
            mod_role    = interaction.guild.get_role(mod_role_id) if mod_role_id else None
            ch          = interaction.guild.get_channel(data["channel_id"])
            if ch:
                await ch.send(
                    f"{mod_role.mention if mod_role else '**Mod**'} — lobby is full! "
                    f"Run `/start_game` when ready.")

    async def on_leave(self, interaction: discord.Interaction):
        data = db_get_lobby(interaction.guild_id)
        if not data or interaction.user.id not in data["player_ids"]:
            return await interaction.response.send_message("❌ You are not in the lobby.", ephemeral=True)
        player_ids = [p for p in data["player_ids"] if p != interaction.user.id]
        db_set_lobby(interaction.guild_id, player_ids, data["message_id"], data["channel_id"])

        # Remove participant role
        state  = cached_get_state(interaction.guild_id)
        p_role = interaction.guild.get_role(state.get("participant_role_id") or 0)
        if p_role:
            try: await interaction.user.remove_roles(p_role)
            except: pass

        embed = _build_lobby_embed(interaction.guild, player_ids, True, self.roster_size)
        await interaction.response.edit_message(embed=embed, view=self)



@tree.command(name="spectate", description="Request spectator access to watch the current game")
async def spectate(interaction: discord.Interaction):
    state    = cached_get_state(interaction.guild_id)
    spec_role_id = state.get("spectator_role_id")
    spec_role    = interaction.guild.get_role(spec_role_id or 0)

    if not spec_role:
        return await interaction.response.send_message(
            "❌ No spectator role configured. Ask a mod to use `/set_spectator_role`.",
            ephemeral=True)

    # Already a spectator
    if spec_role in interaction.user.roles:
        return await interaction.response.send_message(
            "✅ You are already a spectator.", ephemeral=True)

    # Already an active player — can't spectate
    if game_active(interaction.guild_id):
        rows = db_get_assignments(interaction.guild_id)
        if any(r[0] == interaction.user.id and r[2] == 1 for r in rows):
            return await interaction.response.send_message(
                "❌ You are an active player in the current game.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    # Post approval request to mod-log
    state_mod = db_get_state(interaction.guild_id) or {}
    mod_ch    = interaction.guild.get_channel(state_mod.get("mod_log_channel_id") or 0)

    class SpectateApprovalView(View):
        def __init__(self):
            super().__init__(timeout=3600)

        @discord.ui.button(label="✅ Approve", style=discord.ButtonStyle.green)
        async def approve(self, btn_interaction: discord.Interaction, button):
            try:
                await interaction.user.add_roles(spec_role)
                await btn_interaction.response.edit_message(
                    content=f"✅ **{interaction.user.display_name}** granted spectator access.",
                    view=None)
                # DM the requester
                try:
                    await interaction.user.send(
                        fmt(f"✅ Your spectator request for **{interaction.guild.name}** was approved.\n"
                            f"You now have read access to all game channels."))
                except Exception:
                    pass
            except discord.Forbidden:
                await btn_interaction.response.send_message(
                    "❌ Could not assign role — check bot permissions.", ephemeral=True)

        @discord.ui.button(label="❌ Deny", style=discord.ButtonStyle.danger)
        async def deny(self, btn_interaction: discord.Interaction, button):
            await btn_interaction.response.edit_message(
                content=f"❌ **{interaction.user.display_name}**\'s spectator request denied.",
                view=None)
            try:
                await interaction.user.send(
                    fmt(f"❌ Your spectator request for **{interaction.guild.name}** was denied."))
            except Exception:
                pass

    if mod_ch:
        embed = discord.Embed(
            title       = "👁️ Spectator Request",
            description = f"**{interaction.user.mention}** wants to spectate the current game.",
            color       = 0x9B59B6
        )
        embed.set_footer(text="Approve or deny below.")
        await mod_ch.send(embed=embed, view=SpectateApprovalView())
        await interaction.followup.send(
            "✅ Your spectator request has been sent to the mods. Stand by.",
            ephemeral=True)
    else:
        # No mod-log — auto-grant
        try:
            await interaction.user.add_roles(spec_role)
            await interaction.followup.send(
                "✅ Spectator access granted.", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send(
                "❌ Could not assign role — ask a mod directly.", ephemeral=True)

@tree.command(name="lobby", description="Open, close, or manage the pre-game lobby")
@is_mod()
@app_commands.describe(
    action="open / close / kick",
    player="Player to kick (only needed for kick action)",
    roster_size="Expected number of players (optional — shows slots remaining)"
)
async def lobby_cmd(interaction: discord.Interaction,
                    action: str,
                    player: discord.Member = None,
                    roster_size: int = 0):
    action = action.lower().strip()
    state  = cached_get_state(interaction.guild_id)

    if action == "open":
        if game_active(interaction.guild_id):
            return await interaction.response.send_message(
                "❌ A game is already active.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)

        # Find a good channel — use village-chat if exists, else general
        # Lobby posts in the current channel since village-chat
        # is created fresh each game and doesn't exist pre-game
        ch = interaction.channel

        embed = _build_lobby_embed(interaction.guild, [], True, roster_size)
        view  = LobbyJoinView(interaction.guild_id, roster_size)
        msg   = await ch.send(embed=embed, view=view)

        db_set_lobby(interaction.guild_id, [], msg.id, ch.id, is_open=True)
        await interaction.followup.send(
            f"✅ Lobby opened in {ch.mention}. Players can now join.", ephemeral=True)

    elif action == "close":
        data = db_get_lobby(interaction.guild_id)
        if not data:
            return await interaction.response.send_message("No lobby found.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        db_set_lobby(interaction.guild_id, data["player_ids"], data["message_id"],
                     data["channel_id"], is_open=False)
        ch = interaction.guild.get_channel(data["channel_id"])
        if ch and data["message_id"]:
            try:
                msg   = await ch.fetch_message(data["message_id"])
                embed = _build_lobby_embed(interaction.guild, data["player_ids"], False, roster_size)
                await msg.edit(embed=embed, view=None)
            except Exception:
                pass
        await interaction.followup.send(
            f"✅ Lobby closed. {len(data['player_ids'])} player(s) are in.", ephemeral=True)

    elif action == "kick":
        if not player:
            return await interaction.response.send_message(
                "❌ Specify a player to kick.", ephemeral=True)
        data = db_get_lobby(interaction.guild_id)
        if not data or player.id not in data["player_ids"]:
            return await interaction.response.send_message(
                f"❌ **{player.display_name}** is not in the lobby.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        player_ids = [p for p in data["player_ids"] if p != player.id]
        db_set_lobby(interaction.guild_id, player_ids, data["message_id"],
                     data["channel_id"], is_open=data["is_open"])
        # Remove participant role
        p_role = interaction.guild.get_role(state.get("participant_role_id") or 0)
        if p_role:
            try: await player.remove_roles(p_role)
            except: pass
        ch = interaction.guild.get_channel(data["channel_id"])
        if ch and data["message_id"]:
            try:
                msg   = await ch.fetch_message(data["message_id"])
                embed = _build_lobby_embed(interaction.guild, player_ids,
                                                 data["is_open"], roster_size)
                await msg.edit(embed=embed)
            except Exception:
                pass
        await interaction.followup.send(
            f"✅ **{player.display_name}** removed from lobby.", ephemeral=True)
    else:
        await interaction.response.send_message(
            "❌ Valid actions: `open`, `close`, `kick`", ephemeral=True)

# ====================== ELDER TRACKING ======================

async def handle_elder_hit(guild, guild_id: int, killed_by_vote: bool = False):
    """
    Called when the Elder is targeted.
    killed_by_vote=True  → immediately trigger village conversion.
    killed_by_vote=False → wolf attack. Check if this is hit 1 or 2.
    """
    rows    = db_get_assignments(guild_id)
    elder   = next((r for r in rows if r[1] == "Elder" and r[2] == 1), None)
    if not elder:
        return False  # Elder not in game or already dead

    if killed_by_vote:
        await _elder_convert_village(guild, guild_id)
        return True

    # Wolf attack path
    hits = db_get_elder_hits(guild_id)
    db_increment_elder_hit(guild_id)
    hits += 1

    if hits == 1:
        # First hit — Elder survives, gets warning
        priv_ch = guild.get_channel(elder[3] or 0)
        if priv_ch:
            await priv_ch.send(fmt(
                "⚡ The wolves attacked you tonight — but you survived.\n"
                "You have used your one-time survival. If they attack again, you die.\n"
                "If you are voted out, ALL village roles become Villager."))
        await post_mod_log(guild,
            f"⚡ **Elder** survived wolf attack (hit 1/2).\n"
            f"Next wolf attack or vote will trigger village role conversion.")
        return False  # Elder survives — do not eliminate

    else:
        # Second hit — Elder dies and triggers conversion
        await post_mod_log(guild,
            f"💀 **Elder** hit a second time — village roles converting to Villager.")
        await _elder_convert_village(guild, guild_id)
        return True  # Eliminate the Elder


async def _elder_convert_village(guild, guild_id: int):
    """Convert all alive village-aligned players to Villager."""
    rows    = db_get_assignments(guild_id)
    state   = cached_get_state(guild_id)
    vc_ch   = guild.get_channel(state.get("village_chat_ch_id") or 0)
    converted = []

    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    for pid, role, is_alive, ch_id in rows:
        if not is_alive:
            continue
        if role in ("Elder", "Villager"):
            continue
        team = get_team(guild_id, role)
        if team != "village":
            continue
        # Convert to Villager
        c.execute("UPDATE player_assignments SET role_name='Villager' WHERE guild_id=? AND player_id=?",
                  (guild_id, pid))
        m = guild.get_member(pid)
        converted.append(m.display_name if m else str(pid))
        # Notify in their private channel
        if ch_id:
            priv = guild.get_channel(ch_id)
            if priv:
                await priv.send(fmt(
                    "⚡ The Elder has fallen.\n"
                    "Their death has stripped all village roles.\n"
                    "You are now a Villager — no special ability."))
    conn.commit()

    # Public announcement
    announce = (
        f"⚡ **The Elder has fallen!**\n"
        f"Their death has cursed the village — all special village roles are gone.\n"
        f"**{len(converted)} player(s)** are now ordinary Villagers."
    )
    if vc_ch:
        await vc_ch.send(announce)
    await post_mod_log(guild,
        f"⚡ **Elder conversion triggered**\n"
        f"Converted: {', '.join(converted) or 'none'}")


async def _send_insomniac_hint(guild, guild_id: int, night_num: int):
    """Send a random wolf role name to the Insomniac on odd nights >= 3."""
    await asyncio.sleep(random.uniform(60, 180))  # Small delay so it feels organic
    rows     = db_get_assignments(guild_id)
    insomniac = next((r for r in rows if r[1] == "Insomniac" and r[2] == 1), None)
    if not insomniac or not insomniac[3]:
        return
    alive_wolves = [r for r in rows if r[2] == 1 and get_team(guild_id, r[1]) == "wolf"]
    priv_ch = guild.get_channel(insomniac[3])
    if not priv_ch:
        return
    if not alive_wolves:
        await priv_ch.send(fmt(
            f"😴 Insomniac Hint — Night {night_num}\n"
            f"Your senses are quiet tonight. No wolves detected among the living."))
        await post_mod_log(guild,
            f"😴 **Insomniac hint sent** — Night {night_num}\n"
            f"No alive wolves found — hint: none.")
        return
    # Pick a random alive wolf
    chosen    = random.choice(alive_wolves)
    member    = guild.get_member(chosen[0])
    # Check if it's an NPC
    npcs      = db_get_npcs(guild_id)
    npc_match = next((n for n in npcs if n["npc_id"] == chosen[0]), None)
    name      = npc_match["name"] if npc_match else (member.display_name if member else str(chosen[0]))
    await priv_ch.send(fmt(
        f"😴 Insomniac Hint — Night {night_num}\n"
        f"Something stirs in the dark. The name that comes to you is: **{name}**\n"
        f"Handle this knowledge carefully."))
    await post_mod_log(guild,
        f"😴 **Insomniac hint sent** — Night {night_num}\n"
        f"**Hint given:** {name} (role: {chosen[1]})")

# ====================== JAFAR DAILY ABILITY ======================

JAFAR_SPIN_FLAVOR = [
    "The spell misfired at dawn. Today you are something... different.",
    "Your magic had other ideas. The wheel has spoken.",
    "Another sunrise, another accident. Embrace it.",
    "The backfire strikes again. Your role today is not your own.",
    "Fate spun the wheel while you slept. Here's what it landed on.",
    "Your spell collapsed into something else entirely.",
    "The morning brought chaos — as it always does for Jafar.",
]

async def assign_jafar_ability(guild, guild_id: int):
    """Spin the wheel for Jafar — cannot land on yesterday's role."""
    rows  = db_get_assignments(guild_id)
    jafar = next((r for r in rows if r[1] == "Jafar" and r[2] == 1), None)
    if not jafar:
        return

    # Build eligible pool from THIS GAME'S pool only — village roles, no wolf, no neutral, no Jafar
    last_roles = db_get_last_roles(guild_id)  # {role_name: count}
    all_roles  = cached_load_roles(guild_id)
    role_info_map = {r["name"]: r for r in all_roles}
    if last_roles:
        eligible = [role_info_map[name] for name in last_roles
                    if name in role_info_map
                    and role_info_map[name].get("team") == "village"
                    and name != "Jafar"]
    else:
        eligible = [r for r in all_roles if r.get("team") == "village" and r["name"] != "Jafar"]
    if not eligible:
        return

    # Get last role to exclude it
    state     = db_get_state(guild_id) or {}
    last_role = state.get("jafar_last_role", "")

    # Filter out last role if there are enough options
    pool = [r for r in eligible if r["name"] != last_role]
    if not pool:
        pool = eligible  # fallback if only one role exists

    # ── Wheel spin animation in private channel ───────────────────────────
    priv_ch = guild.get_channel(jafar[3] or 0)
    if priv_ch:
        night_num = db_get_night_num(guild_id)
        all_names = [r["name"] for r in pool]

        # Post the wheel — show all options
        wheel_lines = "\n".join(f"🎡 {name}" for name in all_names)
        spin_msg = await priv_ch.send(fmt(
            f"☀️ Day {night_num} — The wheel spins...\n\n{wheel_lines}"))

        # Brief pause for drama
        await asyncio.sleep(2)

        # Land on the chosen role
        chosen    = random.choice(pool)
        role_name = chosen["name"]
        role_desc = chosen.get("description") or "No description."

        # Edit message to show result
        await spin_msg.edit(content=fmt(
            f"☀️ Day {night_num} — {random.choice(JAFAR_SPIN_FLAVOR)}\n\n"
            f"🎭 **Today you are: {role_name}**"))

        # Send rich embed with full role details
        embed = discord.Embed(
            title       = f"🎭 {role_name}",
            description = role_desc,
            color       = 0x9B59B6
        )
        embed.add_field(
            name  = "⏳ Duration",
            value = "This ability lasts until tonight only.",
            inline=False)
        if last_role:
            embed.set_footer(text=f"Yesterday you were: {last_role}  •  Cannot repeat back-to-back")
        else:
            embed.set_footer(text="First spin of the game — good luck.")
        await priv_ch.send(embed=embed)

    else:
        # No private channel — just pick silently
        chosen    = random.choice(pool)
        role_name = chosen["name"]
        role_desc = chosen.get("description") or "No description."

    # Save this role as last_role for next spin
    db_set_state(guild_id, jafar_last_role=role_name)

    # Log to mod-log
    night_num = db_get_night_num(guild_id)
    await post_mod_log(guild,
        f"🎭 **Jafar wheel spin — Day {night_num}**\n"
        f"**Today's role:** {role_name}\n"
        f"**Previous role:** {last_role or 'none (first spin)'}\n"
        f"**Description:** {role_desc}")

# ====================== PHASE TRANSITION EMBEDS ======================

NIGHT_QUOTES = [
    "The village falls silent. Something hunts in the dark.",
    "Lock your doors. Trust no one. The wolves are among you.",
    "Night falls like a shroud. Not all will see the dawn.",
    "The moon watches without mercy. Neither should you.",
    "Darkness hides many sins. Tonight, more are committed.",
    "Sleep well — if you can.",
    "The howling begins. The village holds its breath.",
    "Another night. Another chance for the wolves to feed.",
    "Close your eyes. Hope they don't find you.",
    "The stars bear witness. The wolves bear fangs.",
    "When morning comes, someone will be missing.",
    "Night is the wolves' kingdom. Pray you survive it.",
    "The village sleeps. The hunters do not.",
    "Darkness is patient. So are the wolves.",
    "Another night falls on the damned village.",
]

DAY_QUOTES = [
    "The sun rises on a village with fewer friends.",
    "Morning light reveals what darkness tried to hide.",
    "The village gathers, eyes full of suspicion.",
    "Another dawn. Another vote. Another grave.",
    "The accused stand before the village. Truth is rare here.",
    "Trust is a luxury the village cannot afford.",
    "By nightfall, one more will be gone.",
    "The wolves slept well. Did you?",
    "Point your fingers carefully — you may be wrong.",
    "The village must choose. It always chooses wrong.",
    "Day breaks cold over the village square.",
    "Gather round. Someone among you is lying.",
    "The sun shines, but the danger does not sleep.",
    "Every smile hides a secret. Every vote seals a fate.",
    "Another morning. Another chance to find the truth — or bury it.",
]


def _next_est_time(hour: int) -> int:
    """Return Unix timestamp of the next occurrence of `hour` in EST/EDT."""
    from datetime import datetime, timezone, timedelta
    now_utc = datetime.now(timezone.utc)
    # EDT (UTC-4) March-November, EST (UTC-5) otherwise
    offset  = timedelta(hours=-4) if 3 <= now_utc.month <= 11 else timedelta(hours=-5)
    tz      = timezone(offset)
    now_loc = datetime.now(tz)
    target  = now_loc.replace(hour=hour, minute=0, second=0, microsecond=0)
    if target <= now_loc:
        target += timedelta(days=1)
    return int(target.timestamp())


def _phase_end_ts(duration_secs: int, clock_hour: int, guild_id: int = None) -> int:
    """
    Return the correct phase end timestamp.
    Normal: snap to the next EST clock time (8 AM or 8 PM).
    Time Lord triggered: use now + duration instead.
    Uses the time_lord_triggered flag in game_state, not duration magnitude.
    """
    import time as _pet
    # Check if Time Lord has died this game
    tl_triggered = False
    if guild_id:
        state = db_get_state(guild_id) or {}
        tl_triggered = bool(state.get("time_lord_triggered", 0))

    if tl_triggered:
        return int(_pet.time()) + duration_secs
    else:
        return _next_est_time(clock_hour)

async def post_night_transition(guild, night_num: int, duration_secs: int):
    """Post a rich night-start embed in village-chat."""
    import time as _pnt_t
    state  = cached_get_state(guild.id)
    vc_ch  = guild.get_channel(state.get("village_chat_ch_id") or 0)
    if not vc_ch:
        return
    end_ts = _phase_end_ts(duration_secs, 8, guild.id)   # Night ends at 8 AM EST (or sooner if Time Lord)
    quote  = random.choice(NIGHT_QUOTES)
    embed  = discord.Embed(
        title       = f"🌙 Night {night_num} Begins",
        description = f"*{quote}*",
        color       = 0x2C3060
    )
    embed.add_field(name="⏰ Night ends",   value=f"<t:{end_ts}:R>",  inline=True)
    embed.add_field(name="🕐 Exact time",   value=f"<t:{end_ts}:t>",  inline=True)
    embed.add_field(name="📅 Date",         value=f"<t:{end_ts}:D>",  inline=True)
    embed.add_field(name="🌙 Phase",        value=f"Night {night_num}", inline=True)
    embed.set_footer(text="Submit your night actions in your private channel. All times shown in your local timezone.")
    await vc_ch.send(embed=embed)

async def post_day_transition(guild, night_num: int, duration_secs: int, deaths: list = None):
    """Post a rich day-start embed in village-chat."""
    state  = cached_get_state(guild.id)
    vc_ch  = guild.get_channel(state.get("village_chat_ch_id") or 0)
    if not vc_ch:
        return
    end_ts = _phase_end_ts(duration_secs, 20, guild.id)  # Day ends at 8 PM EST (or sooner if Time Lord)
    quote  = random.choice(DAY_QUOTES)

    # Check if agitator frenzy was used last night
    actions   = db_get_night_actions(guild.id, night_num - 1) if night_num > 1 else []
    frenzy    = any(a[1] == "agitator_frenzy" for a in actions)
    if frenzy:
        db_set_state(guild.id, agitator_frenzy_day=night_num, agitator_elim_count=0)

    embed = discord.Embed(
        title       = f"☀️ Day {night_num} Begins",
        description = f"*{quote}*",
        color       = 0xE67E22
    )
    if deaths:
        death_text = "\n".join(f"💀 {name}" for name in deaths)
        embed.add_field(name="💀 Died last night", value=death_text, inline=False)
    else:
        embed.add_field(name="💀 Died last night", value="Nobody. The village got lucky.", inline=False)
    embed.add_field(name="⏰ Day ends",   value=f"<t:{end_ts}:R>",  inline=True)
    embed.add_field(name="🕐 Exact time", value=f"<t:{end_ts}:t>",  inline=True)
    embed.add_field(name="📅 Date",       value=f"<t:{end_ts}:D>",  inline=True)
    embed.add_field(name="☀️ Phase",      value=f"Day {night_num}", inline=True)
    if frenzy:
        embed.add_field(
            name   = "⚡ FRENZY",
            value  = "**TWO players must be eliminated today.**\nAn agitator has stirred the village into a frenzy!",
            inline = False)
        embed.color = 0xE74C3C
    embed.set_footer(text="Discuss, debate, and vote in the day-vote channel. All times shown in your local timezone.")
    await vc_ch.send(embed=embed)

    # Separate frenzy announcement so it can't be missed
    if frenzy:
        await vc_ch.send(
            f"⚡ **FRENZY — TWO ELIMINATIONS REQUIRED TODAY** ⚡\n"
            f"The agitator has stirred the village. Two players must be voted out before night falls.\n"
            f"Every player must cast **two votes**. The night cannot begin until both eliminations are complete.")

    # Shapeshifter death check — Night 1 only, must have picked before morning
    if night_num == 1:
        rows_ss   = db_get_assignments(guild.id)
        actions_ss = db_get_night_actions(guild.id, 1)
        acted_ids  = {a[0] for a in actions_ss}
        for pid, role, is_alive, ch_id in rows_ss:
            if role == "Shapeshifter" and is_alive and pid not in acted_ids:
                # Shapeshifter didn't pick — they die
                priv_ss = guild.get_channel(ch_id or 0)
                if priv_ss:
                    await priv_ss.send(fmt(
                        "💀 You did not choose a form before the morning post.\n"
                        "The Shapeshifter who hesitates becomes nothing.\n"
                        "You have been eliminated."))
                await post_mod_log(guild,
                    f"🎭 **Shapeshifter** failed to choose a form before morning — eliminated per role rules.")
                safe_task(_eliminate_player(guild, pid, "Shapeshifter — failed to transform"), "ss_death")
                break

    # Start ambient message loop for the day phase
    safe_task(_ambient_loop(guild, guild.id), "ambient")

    # NPC morning reactions
    safe_task(_npc_morning_reactions(guild, guild.id, night_num, deaths or []), "npc_morning")

    # ── Auto-start day vote using day_duration from DB ──────────────────────
    _pdt_state  = cached_get_state(guild.id)
    _pdt_dur    = int(_pdt_state.get("day_duration") or 43200)
    end_ts_vote = _phase_end_ts(_pdt_dur, 20, guild.id)
    db_clear_day_votes(guild.id)
    db_set_state(guild.id,
                 anon_vote=0,
                 day_vote_msg_id=None,
                 day_vote_end_time=end_ts_vote)
    invalidate_cache(guild.id)
    await refresh_day_vote(guild)

    # Post vote open announcement in village chat
    dv_ch = guild.get_channel(state.get("day_vote_ch_id") or 0)
    if dv_ch:
        await vc_ch.send(
            f"🗳️ **Voting is now open!**\n"
            f"Head to {dv_ch.mention} to cast your vote.\n"
            f"Votes close at <t:{end_ts_vote}:t> your time (<t:{end_ts_vote}:R>).")

    # Schedule auto-close at 7 PM EST
    async def _auto_close_vote():
        import time as _t
        wait = end_ts_vote - int(_t.time())
        if wait > 0:
            await asyncio.sleep(wait)
        current = db_get_state(guild.id) or {}
        if current.get("day_vote_end_time") == end_ts_vote:
            snap_ch    = guild.get_channel(current.get("day_vote_ch_id") or 0)
            snap_votes = db_get_day_votes(guild.id)
            snap_rows  = db_get_assignments(guild.id)
            if snap_ch and snap_votes:
                snap_embed = build_day_vote_embed(guild, snap_votes, snap_rows)
                snap_embed.title = f"📋 Day {night_num} Vote — Final Results"
                snap_embed.color = 0x95A5A6
                snap_embed.set_footer(text="Vote timer ended. Results are permanent.")
                await snap_ch.send(embed=snap_embed)
                db_set_state(guild.id, day_vote_msg_id=None)
                invalidate_cache(guild.id)
            db_set_state(guild.id, day_vote_end_time=None, anon_vote=0)
            await refresh_day_vote(guild)

            # ── Missed vote auto-elimination ──────────────────────────────
            rows_mv    = db_get_assignments(guild.id)
            votes_mv   = db_get_day_votes(guild.id)
            votes_mv2  = db_get_day_votes_2(guild.id)
            voted_ids  = {v[0] for v in votes_mv}
            voted2_ids = {v[0] for v in votes_mv2}
            npcs_mv    = db_get_npcs(guild.id)
            npc_ids    = {n["npc_id"] for n in npcs_mv}
            cur_state  = db_get_state(guild.id) or {}
            vc_ch_mv   = guild.get_channel(cur_state.get("village_chat_ch_id") or 0)
            frenzy_day = cur_state.get("agitator_frenzy_day")
            night_mv   = db_get_night_num(guild.id)
            is_frenzy  = frenzy_day and int(frenzy_day) == int(night_mv)

            for pid, role, is_alive, _ in rows_mv:
                if not is_alive or pid in npc_ids:
                    continue
                missed_vote1  = pid not in voted_ids
                missed_vote2  = is_frenzy and pid not in voted2_ids
                if missed_vote1 or missed_vote2:
                    m    = guild.get_member(pid)
                    name = m.display_name if m else str(pid)
                    reason = "did not cast their required votes" if missed_vote2 and not missed_vote1 else "did not vote"
                    if vc_ch_mv:
                        await vc_ch_mv.send(
                            fmt(f"⚠️ **{name}** {reason} and has been eliminated per Whisperfall rules."))
                    await _eliminate_player(guild, pid, "missed vote")
                    await post_mod_log(guild,
                        f"⚠️ **Missed Vote Elimination**\n"
                        f"**{name}** ({role}) eliminated for not voting"
                        + (" (frenzy — missed second vote)" if missed_vote2 and not missed_vote1 else "") + ".")

            # NPC reaction to imminent elimination
            top_target = None
            if votes_mv:
                tally_mv = {}
                for _, tid in votes_mv:
                    if tid: tally_mv[tid] = tally_mv.get(tid, 0) + 1
                if tally_mv:
                    top_target = max(tally_mv, key=tally_mv.get)
            safe_task(_npc_react_to_vote_close(guild, guild.id, top_target), "npc_vote_close")

            vote_summary  = _build_vote_summary(guild, guild.id)
            mod_state     = db_get_state(guild.id) or {}
            mod_ch_prompt = guild.get_channel(mod_state.get("mod_log_channel_id") or 0)
            if mod_ch_prompt:
                embed_prompt = discord.Embed(
                    title       = "⏰ Day Vote Closed — Results",
                    description = vote_summary,
                    color       = 0xF39C12
                )
                view_prompt = DayVoteClosedPromptView(guild.id)
                await mod_ch_prompt.send(embed=embed_prompt, view=view_prompt)

    safe_task(_auto_close_vote(), "auto_close_vote")



# ====================== NPC IDENTITY HELPERS ======================
def db_get_npc_identity(guild_id, name):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT * FROM npc_identities WHERE guild_id=? AND name=?", (guild_id, name))
    row = c.fetchone()
    if not row:
        return None
    cols = ["guild_id","name","games_played","total_kills","times_wolf",
            "times_village","win_count","personality","backstory","avatar_url","legacy_notes"]
    d = dict(zip(cols, row))
    import json as _j
    d["legacy_notes"] = _j.loads(d["legacy_notes"] or "[]")
    conn.close()
    return d

def db_save_npc_identity(guild_id, name, personality, backstory, avatar_url):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR IGNORE INTO npc_identities (guild_id, name, personality, backstory, avatar_url) VALUES (?,?,?,?,?)",
              (guild_id, name, personality, backstory, avatar_url))
    conn.commit()
    conn.close()

def db_update_npc_identity_stats(guild_id, name, team, won, kills=0):
    import json as _j
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("UPDATE npc_identities SET games_played=games_played+1, "
              "total_kills=total_kills+?, "
              "times_wolf=times_wolf+?, "
              "times_village=times_village+?, "
              "win_count=win_count+? "
              "WHERE guild_id=? AND name=?",
              (kills,
               1 if team == "wolf" else 0,
               1 if team != "wolf" else 0,
               1 if won else 0,
               guild_id, name))
    conn.commit()
    conn.close()

def db_add_npc_legacy_note(guild_id, name, note: str):
    import json as _j
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT legacy_notes FROM npc_identities WHERE guild_id=? AND name=?", (guild_id, name))
    row = c.fetchone()
    notes = _j.loads(row[0] or "[]") if row else []
    notes.append(note)
    notes = notes[-10:]  # Keep last 10 legacy notes
    c.execute("UPDATE npc_identities SET legacy_notes=? WHERE guild_id=? AND name=?",
              (_j.dumps(notes), guild_id, name))
    conn.commit()
    conn.close()

# ====================== TRACKER DB HELPERS ======================

def db_add_claim_channel(guild_id, channel_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR IGNORE INTO claim_channels VALUES (?,?)", (guild_id, channel_id))
    conn.commit()
    conn.close()

def db_get_claim_channels(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT channel_id FROM claim_channels WHERE guild_id=?", (guild_id,))
    rows = c.fetchall()
    conn.close()

    return [r[0] for r in rows]

def db_clear_claim_channels(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM claim_channels WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()

def db_get_tracker(guild_id, owner_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT target_id, suspicion, suspected_role, notes FROM player_tracker WHERE guild_id=? AND owner_id=?",
              (guild_id, owner_id))
    rows = c.fetchall()
    conn.close()
    return {r[0]: {"suspicion": r[1], "suspected_role": r[2], "notes": r[3]} for r in rows}

def db_set_tracker_entry(guild_id, owner_id, target_id, suspicion=None, suspected_role=None, notes=None):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR IGNORE INTO player_tracker (guild_id, owner_id, target_id) VALUES (?,?,?)",
              (guild_id, owner_id, target_id))
    if suspicion     is not None: c.execute("UPDATE player_tracker SET suspicion=? WHERE guild_id=? AND owner_id=? AND target_id=?", (suspicion, guild_id, owner_id, target_id))
    if suspected_role is not None: c.execute("UPDATE player_tracker SET suspected_role=? WHERE guild_id=? AND owner_id=? AND target_id=?", (suspected_role, guild_id, owner_id, target_id))
    if notes         is not None: c.execute("UPDATE player_tracker SET notes=? WHERE guild_id=? AND owner_id=? AND target_id=?", (notes, guild_id, owner_id, target_id))
    conn.commit()
    conn.close()

def db_get_tracker_msg(guild_id, owner_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT msg_id, ch_id FROM player_tracker_msg WHERE guild_id=? AND owner_id=?",
              (guild_id, owner_id))
    row = c.fetchone()
    conn.close()
    return row  # (msg_id, ch_id) or None

def db_set_tracker_msg(guild_id, owner_id, msg_id, ch_id):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO player_tracker_msg VALUES (?,?,?,?)",
              (guild_id, owner_id, msg_id, ch_id))
    conn.commit()
    conn.close()

# ====================== SHADOW WOLF KILL LIST ======================

def _build_sw_list_text(targets: list) -> str:
    """Build the kill list message text. Killed = strikethrough style."""
    if not targets:
        return "Kill list is empty — all targets are gone."
    lines = []
    for entry in targets:
        name   = entry["name"]
        status = entry["status"]  # "alive", "killed_by_sw", "dead_other"
        if status == "alive":
            lines.append(f"🎯 {name}")
        elif status == "killed_by_sw":
            lines.append(f"✅ ~~{name}~~ — killed by you")
        else:
            lines.append(f"💀 ~~{name}~~ — died by other means")
    return "\n".join(lines)


async def _refresh_sw_list(guild):
    """Edit the Shadow Wolf's kill list message in-place."""
    data = db_get_shadow_wolf_list(guild.id)
    if not data or not data["message_id"] or not data["channel_id"]:
        return
    ch = guild.get_channel(data["channel_id"])
    if not ch:
        return
    try:
        msg = await ch.fetch_message(data["message_id"])
        await msg.edit(content=fmt(
            f"🌑 Shadow Wolf Kill List\n\n"
            f"{_build_sw_list_text(data['targets'])}\n\n"
            f"You may kill one per night. List updates automatically."
        ))
    except Exception as e:
        print(f"SW list refresh error: {e}")

async def _generate_sw_kill_list(guild, guild_id, shadow_wolf_id, voted_out_id):
    """
    Build the kill list from day votes against voted_out_id.
    voted_out_id is the Shadow Wolf themselves normally,
    or their Cupid partner if eliminated via bond.
    """
    votes = db_get_day_votes(guild_id)
    rows  = db_get_assignments(guild_id)
    pid_to_name = {}
    for pid, _, _, _ in rows:
        m = guild.get_member(pid)
        pid_to_name[pid] = m.display_name if m else str(pid)
    # Also map NPC names
    npcs = db_get_npcs(guild_id)
    for npc in npcs:
        pid_to_name[npc["npc_id"]] = npc["name"]

    targets = []
    for voter_id, target_id, *_ in votes:
        if target_id == voted_out_id and voter_id != shadow_wolf_id:
            name = pid_to_name.get(voter_id, str(voter_id))
            targets.append({"pid": voter_id, "name": name, "status": "alive"})

    # Remove duplicates
    seen = set()
    unique_targets = []
    for t in targets:
        if t["pid"] not in seen:
            seen.add(t["pid"])
            unique_targets.append(t)

    if not unique_targets:
        # No voters found
        sw_row = next((r for r in rows if r[0] == shadow_wolf_id), None)
        if sw_row and sw_row[3]:
            ch = guild.get_channel(sw_row[3])
            if ch:
                await ch.send(fmt(
                    "🌑 Shadow Wolf activated — but no votes were cast against you\n"
                    "You have no kill list this game."
                ))
        return

    # Post list to Shadow Wolf private channel
    sw_row = next((r for r in rows if r[0] == shadow_wolf_id), None)
    if not sw_row or not sw_row[3]:
        return
    priv_ch = guild.get_channel(sw_row[3])
    if not priv_ch:
        return

    list_text = fmt(
        f"🌑 Shadow Wolf Kill List\n\n"
        f"{_build_sw_list_text(unique_targets)}\n\n"
        f"You may kill one per night. List updates automatically."
    )
    msg = await priv_ch.send(list_text)

    # Save list with message reference
    db_set_shadow_wolf_list(guild_id, unique_targets, msg.id, priv_ch.id)

    # Send the night action view now that Shadow Wolf is dead and activated
    rows_sw    = db_get_assignments(guild_id)
    alive_all  = [guild.get_member(r[0]) for r in rows_sw if r[2] == 1]
    alive_all  = [p for p in alive_all if p]
    night_num  = db_get_night_num(guild_id)
    sw_view    = ShadowWolfView(guild_id, shadow_wolf_id, "Shadow Wolf", alive_all)
    status_view = NightStatusView(guild_id, shadow_wolf_id, "Shadow Wolf", night_num)
    await priv_ch.send(fmt(f"🌑 You are dead — but your hunt begins now.\n\nUse your kill list each night to exact revenge."), view=sw_view)
    await priv_ch.send(fmt("Let the mod know your intent for tonight:"), view=status_view)

    # Post to mod-log
    voter_names = ", ".join(t["name"] for t in unique_targets)
    await post_mod_log(guild,
        f"🌑 **Shadow Wolf kill list generated**\n"
        f"**Voters who eliminated {'their partner' if voted_out_id != shadow_wolf_id else 'them'}:** {voter_names}\n"
        f"Shadow Wolf may kill one per night from this list.")


async def _sw_mark_dead(guild, guild_id, dead_player_id, cause="other"):
    """
    Called whenever any player dies — checks if they are on the SW kill list
    and marks them accordingly.
    cause: 'sw' if killed by Shadow Wolf, 'other' for everything else.
    """
    data = db_get_shadow_wolf_list(guild_id)
    if not data:
        return
    targets  = data["targets"]
    changed  = False
    for entry in targets:
        if entry["pid"] == dead_player_id and entry["status"] == "alive":
            entry["status"] = "killed_by_sw" if cause == "sw" else "dead_other"
            changed = True
            break
    if changed:
        db_set_shadow_wolf_list(guild_id, targets, data["message_id"], data["channel_id"])
        await _refresh_sw_list(guild)


# ====================== MORNING BLOOD BOARD ======================

BLOOD_BOARD_WOLF_LINES = [
    "The night claimed {name}. They did not go quietly.",
    "When dawn came, {name} was gone. The wolves were hungry.",
    "{name} met the darkness and did not return.",
    "The village woke one fewer. {name} will not be at breakfast.",
    "Something found {name} in the night. Something with teeth.",
    "{name} is gone. The wolves say nothing. They never do.",
    "The night had an appetite. {name} paid the price.",
    "By morning, {name} was simply... absent.",
]

BLOOD_BOARD_WITCH_LINES = [
    "A bitter end found {name} — not the kind that comes from wolves.",
    "{name} tasted something they shouldn't have. They won't taste anything again.",
    "Not all deaths come with claws. {name} found that out.",
    "Something unnatural took {name}. The wolves look innocent for once.",
    "{name} is gone, and the cause was... unusual.",
]

BLOOD_BOARD_SAFE_LINES = [
    "The village holds its breath. Against all odds, everyone survived the night.",
    "The wolves went hungry. For now.",
    "Dawn breaks on a full village. Savor it — it won't last.",
    "No one died last night. Someone is very lucky. Or very careful.",
    "The night passed without blood. The wolves are patient.",
]

BLOOD_BOARD_ELDER_SURVIVED = [
    "Something stirred near the elder's home. By morning, they were still standing. Barely.",
    "The wolves tested the elder last night. The elder passed. This time.",
    "An old soul proved harder to kill than expected.",
]

BLOOD_BOARD_CUPID = [
    "Grief is its own kind of poison. {name} proved that.",
    "They say love and death walk hand in hand. {name} knows the truth of it now.",
    "{name} did not die by wolves or vote — they simply could not go on.",
]

BLOOD_BOARD_DISEASED = [
    "The wolves feasted — and now regret it. Something was wrong with their meal.",
    "The kill was made. But the hunters feel unwell this morning.",
    "Not every meal is worth eating. The wolves are learning that.",
]


class BloodBoardPostView(View):
    """Button attached to mod-log suggestion — posts to village-chat on click."""
    def __init__(self, guild_id: int, embed: discord.Embed):
        super().__init__(timeout=None)  # No timeout
        self.guild_id = guild_id
        self._embed   = embed
        btn = Button(label="📋 Post Blood Board to Village Chat", style=discord.ButtonStyle.green)
        btn.callback = self.on_post
        self.add_item(btn)

    async def on_post(self, interaction: discord.Interaction):
        state  = cached_get_state(self.guild_id)
        vc_ch  = interaction.guild.get_channel(state.get("village_chat_ch_id") or 0)
        if not vc_ch:
            return await interaction.response.send_message(
                "❌ Village-chat channel not found.", ephemeral=True)
        await vc_ch.send(embed=self._embed)
        await interaction.response.edit_message(
            content="✅ Blood board posted to village-chat.", embed=None, view=None)


async def post_blood_board_suggestion(guild, guild_id: int, night_num: int,
                                       deaths: list, notable_events: list):
    """
    Build a flavorful blood board embed and post it to mod-log with a Post button.
    deaths: list of dicts — {name, cause}  cause in (wolf, witch, cupid, other)
    notable_events: list of strings — Elder survived, Diseased triggered, etc.
    """
    embed = discord.Embed(
        title = f"📋 Morning Blood Board — Night {night_num}",
        color = 0x2C3E50
    )

    if not deaths:
        line = random.choice(BLOOD_BOARD_SAFE_LINES)
        embed.description = f"*{line}*"
    else:
        lines = []
        for d in deaths:
            name  = d["name"]
            cause = d["cause"]
            if cause == "wolf":
                line = random.choice(BLOOD_BOARD_WOLF_LINES).format(name=name)
            elif cause == "witch":
                line = random.choice(BLOOD_BOARD_WITCH_LINES).format(name=name)
            elif cause == "cupid":
                line = random.choice(BLOOD_BOARD_CUPID).format(name=name)
            else:
                line = random.choice(BLOOD_BOARD_WOLF_LINES).format(name=name)
            lines.append(f"💀 {line}")
        embed.description = "\n\n".join(lines)

    # Notable events as subtle footer additions
    if notable_events:
        embed.add_field(
            name   = "🌑 Whispers from the night",
            value  = "\n".join(f"• {e}" for e in notable_events),
            inline = False
        )

    embed.set_footer(text=f"Night {night_num} — suggested by VillageAid · Edit before posting if needed")

    # Post to mod-log with Post button
    view = BloodBoardPostView(guild_id, embed)
    state   = cached_get_state(guild_id)
    mod_ch  = guild.get_channel(state.get("mod_log_channel_id") or 0)
    if mod_ch:
        await mod_ch.send(
            "📋 **Suggested blood board for this morning** — review and post when ready:",
            embed=embed,
            view=view
        )

# ====================== WIN CONDITION TRACKER ======================

def build_win_tracker_embed(guild, guild_id):
    rows          = db_get_assignments(guild_id)
    alive         = [r for r in rows if r[2] == 1]
    alive_village = [r for r in alive if get_team(guild_id, r[1]) == "village"]
    alive_wolves  = [r for r in alive if get_team(guild_id, r[1]) == "wolf"]
    alive_neutral = [r for r in alive if get_team(guild_id, r[1]) == "neutral"]

    wolves_needed = max(0, len(alive_village) - len(alive_wolves))

    # Build player lists with NPC labels
    npcs = db_get_npcs(guild_id)
    npc_ids = {n["npc_id"] for n in npcs}

    # Win tracker shows counts only — no role names visible to spectators
    embed = discord.Embed(title="⚖️ Win Condition Tracker", color=0x2C3E50)
    embed.add_field(name=f"🏘️ Village",  value=str(len(alive_village)), inline=True)
    embed.add_field(name=f"🐺 Wolves",   value=str(len(alive_wolves)),  inline=True)
    embed.add_field(name=f"⚖️ Neutral",  value=str(len(alive_neutral)), inline=True)

    if len(alive_wolves) == 0:
        embed.add_field(name="🏆 Status", value="Village wins — all wolves eliminated!", inline=False)
        embed.color = 0x27AE60
    elif len(alive_wolves) >= len(alive_village):
        embed.add_field(name="🏆 Status", value="Wolves win — they match or outnumber village!", inline=False)
        embed.color = 0xC0392B
    elif wolves_needed == 1:
        embed.add_field(name="⚠️ CRITICAL",
                        value="Wolves need **1 more kill** to win. Village is on the edge.", inline=False)
        embed.color = 0xE67E22  # Orange — danger
    elif len(alive_wolves) == 1:
        embed.add_field(name="🐺 Last Wolf",
                        value=f"One wolf remains. Village needs **{wolves_needed}** more elimination(s).", inline=False)
        embed.color = 0x2ECC71  # Green — village advantage
    else:
        embed.add_field(name="🐺 Wolves need",
                        value=f"**{wolves_needed}** more elimination(s) to win", inline=False)

    embed.set_footer(text=f"Total alive: {len(alive)}  •  Updates automatically")
    return embed

async def refresh_win_tracker(guild):
    state = cached_get_state(guild.id)
    ch_id = state.get("win_tracker_ch_id")
    if not ch_id:
        return
    ch = guild.get_channel(ch_id)
    if not ch:
        return
    # Find the pinned tracker message — last bot message in channel
    try:
        async for msg in ch.history(limit=5):
            if msg.author == guild.me:
                await msg.edit(embed=build_win_tracker_embed(guild, guild.id))
                return
        # No existing message — post a new one
        await ch.send(embed=build_win_tracker_embed(guild, guild.id))
    except Exception as e:
        print(f"Win tracker refresh error: {e}")

# ====================== LIVE REFRESH HELPERS ======================

async def refresh_player_list(guild):
    state = cached_get_state(guild.id)
    ch  = guild.get_channel(state.get("player_list_ch_id") or 0)
    mid = state.get("player_list_msg_id")
    if not ch or not mid:
        return
    rows = db_get_assignments(guild.id)
    log  = db_get_log(guild.id)
    try:
        msg = await ch.fetch_message(mid)
        await msg.edit(embed=build_player_list_embed(guild, rows, log))
    except Exception:
        pass

async def refresh_day_vote(guild):
    state     = cached_get_state(guild.id)
    ch        = guild.get_channel(state.get("day_vote_ch_id") or 0)
    mid       = state.get("day_vote_msg_id")
    if not ch:
        return
    rows      = db_get_assignments(guild.id)
    votes     = db_get_day_votes(guild.id)
    end_time  = state.get("day_vote_end_time")
    anonymous = bool(state.get("anon_vote", 0))
    embed     = build_day_vote_embed(guild, votes, rows, end_time, anonymous)
    view      = DayVoteView(guild.id, anonymous=anonymous)

    if mid:
        # Edit in place while vote is active
        try:
            msg = await ch.fetch_message(mid)
            await msg.edit(embed=embed, view=view)
            return
        except Exception:
            pass

    # No existing message — post fresh at bottom
    try:
        new_msg = await ch.send(embed=embed, view=view)
        db_set_state(guild.id, day_vote_msg_id=new_msg.id)
        invalidate_cache(guild.id)
    except Exception as e:
        print(f"[refresh_day_vote] error: {e}")

async def refresh_wolf_vote(guild, night_num):
    """Refresh the wolf-vote embed AND the dropdown view together so it never goes stale."""
    state = cached_get_state(guild.id)
    ch  = guild.get_channel(state.get("wolf_channel_id") or 0)  # Now in wolf den
    mid = state.get("wolf_vote_msg_id")
    if not ch or not mid:
        return
    rows          = db_get_assignments(guild.id)
    wolf_ids      = [r[0] for r in rows if r[2] == 1 and get_team(guild.id, r[1]) == "wolf"]
    alive_players = [guild.get_member(r[0]) for r in rows if r[2] == 1]
    alive_players = [p for p in alive_players if p]
    wolf_votes    = db_get_wolf_votes(guild.id, night_num)
    embed = build_wolf_vote_embed(guild, wolf_votes, alive_players, wolf_ids, night_num,
                                  den_channel=ch)
    # Build a fresh view then patch options with real names + alive/dead status
    view = WolfVoteView(guild.id, night_num)
    if hasattr(view, "_sel"):
        patched = []
        non_wolf = [r for r in rows if get_team(guild.id, r[1]) != "wolf"]
        for pid, role_name, is_alive, _ in non_wolf[:25]:
            m = guild.get_member(pid)
            name = m.display_name if m else f"Player {pid}"
            if is_alive:
                patched.append(discord.SelectOption(
                    label=f"✅ {name}",
                    value=str(pid),
                    description="Alive — valid kill target"
                ))
            else:
                patched.append(discord.SelectOption(
                    label=f"💀 {name} (dead)",
                    value=f"dead_{pid}",
                    description="Already eliminated — cannot be targeted"
                ))
        if patched:
            view._sel.options = patched
    try:
        msg = await ch.fetch_message(mid)
        await msg.edit(embed=embed, view=view)
    except Exception:
        pass

async def refresh_timeline(guild):
    state = cached_get_state(guild.id)
    ch  = guild.get_channel(state.get("timeline_ch_id") or 0)
    mid = state.get("timeline_msg_id")
    if not ch or not mid:
        return
    try:
        msg = await ch.fetch_message(mid)
        await msg.edit(embed=build_timeline_embed(guild.id, state))
    except Exception:
        pass


# ====================== MOD DASHBOARD ======================

def build_dashboard_embed(guild_id: int, guild=None) -> discord.Embed:
    """
    Build the live mod dashboard embed. Reads all state fresh from DB every call.
    Shows a checklist of steps for the current phase with ✅ / ➡️ / ⬜ / ⚠️ markers.
    """
    state     = db_get_state(guild_id) or {}
    phase     = state.get("phase", "day")
    night_num = db_get_night_num(guild_id)
    rows      = db_get_assignments(guild_id)
    actions   = db_get_night_actions(guild_id, night_num) if night_num > 0 else []

    alive      = [r for r in rows if r[2] == 1]
    alive_ids  = {r[0] for r in alive}

    # ── Shared helpers ────────────────────────────────────────────────────
    def submitted_ids():
        return {a[0] for a in actions if not a[1].startswith("_")}

    def has_action(action_type):
        return any(a[1] == action_type for a in actions)

    def has_any_action(*types):
        return any(a[1] in types for a in actions)

    def pending_turns():
        conn = sqlite3.connect(DB_FILE)
        c    = conn.cursor()
        c.execute(
            "SELECT actor_id, target_id FROM turn_log "
            "WHERE guild_id=? AND night_num=? AND result='pending'",
            (guild_id, night_num))
        res = c.fetchall()
        conn.close()
        return res

    def turn_results_this_night():
        conn = sqlite3.connect(DB_FILE)
        c    = conn.cursor()
        c.execute(
            "SELECT actor_id, target_id, result FROM turn_log "
            "WHERE guild_id=? AND night_num=? AND result != 'pending'",
            (guild_id, night_num))
        res = c.fetchall()
        conn.close()
        return res

    # ── Count who needs to submit night actions ───────────────────────────
    needs_action = [
        r for r in alive
        if r[1] in ROLE_VIEW_MAP and r[1] not in NIGHT_NO_BUTTON_ROLES
    ]
    submitted    = submitted_ids()
    submit_done  = len([r for r in needs_action if r[0] in submitted])
    submit_total = len(needs_action)
    all_submitted = submit_done >= submit_total

    # ── Night resolved? ───────────────────────────────────────────────────
    night_resolved = phase == "day"  # phase flips to day in resolve_night

    # ── Turn state ────────────────────────────────────────────────────────
    has_turn_attempt = has_any_action("alpha", "elite_alpha")
    pend_turns       = pending_turns()
    turn_results     = turn_results_this_night()
    turn_confirmed   = has_turn_attempt and not pend_turns and len(turn_results) > 0
    turn_successful  = any(r[2] == "successful" for r in turn_results)

    # ── Deaths applied? (check elimination_log for this night) ────────────
    conn_el = sqlite3.connect(DB_FILE)
    c_el    = conn_el.cursor()
    c_el.execute(
        "SELECT COUNT(*) FROM elimination_log WHERE guild_id=? AND day_or_night=? AND elim_type != 'vote'",
        (guild_id, night_num))
    night_deaths_logged = c_el.fetchone()[0]
    # Check if there are expected deaths (wolf kill submitted)
    has_kill_action = has_any_action(
        "wolf_kill", "werekitten_kill", "white_wolf_kill",
        "witch_kill", "shadow_wolf", "crazed_wolf_1")
    conn_el.close()

    # ── Investigations done? ──────────────────────────────────────────────
    invest_done = bool(state.get("investigations_done", 0))
    has_investigators = has_any_action("seer", "medium", "bloodhound")

    # ── Blood board posted? ───────────────────────────────────────────────
    night_bb_done = bool(state.get("night_bb_done", 0))
    day_bb_done   = bool(state.get("day_bb_done",   0))

    # ── Day vote state ────────────────────────────────────────────────────
    votes       = db_get_day_votes(guild_id)
    voted_ids   = {v[0] for v in votes if v[1] is not None}
    vote_total  = len(alive_ids)
    vote_done   = len(voted_ids)
    vote_closed = state.get("day_vote_end_time") is None and phase == "day" and night_num > 1

    # ── Day eliminations ──────────────────────────────────────────────────
    conn_de = sqlite3.connect(DB_FILE)
    c_de    = conn_de.cursor()
    c_de.execute(
        "SELECT COUNT(*) FROM elimination_log WHERE guild_id=? AND day_or_night=? AND elim_type='vote'",
        (guild_id, night_num))
    day_elims_done = c_de.fetchone()[0]
    conn_de.close()

    frenzy_day  = state.get("agitator_frenzy_day")
    is_frenzy   = frenzy_day and int(frenzy_day) == int(night_num)
    elims_needed = 2 if is_frenzy else 1

    # ── Pothead trigger ───────────────────────────────────────────────────
    pothead_triggered = has_action("pothead_second_kill") or (
        # Check if Pothead was killed by wolves this night
        any(a[1] in ("wolf_kill", "werekitten_kill") and
            next((r[1] for r in rows if r[0] == a[2]), "") == "Pothead"
            for a in actions if a[2]))

    # ── Cupid bond ───────────────────────────────────────────────────────
    bond = db_get_cupid_bond(guild_id)
    cupid_bond_active = bond is not None

    # ══ BUILD THE CHECKLIST ══════════════════════════════════════════════

    if phase == "night":
        title = f"🌙 Night {night_num} — Mod Dashboard"
        color = 0x2C3060

        steps = []

        # Step 1 — Night actions
        if all_submitted:
            steps.append(("✅", f"Night actions received ({submit_done}/{submit_total})"))
        else:
            steps.append(("➡️", f"Waiting on night actions — **{submit_done}/{submit_total} submitted**"))

        # Step 2 — Resolve night
        if night_resolved:
            steps.append(("✅", "Night resolved"))
        elif all_submitted:
            steps.append(("➡️", "**Resolve Night** — click button below"))
        else:
            steps.append(("⬜", "Resolve Night"))

        # Step 3 — Turn confirmation (only if Alpha/Elite Alpha submitted)
        if has_turn_attempt:
            if turn_confirmed:
                result_str = ", ".join(
                    f"{r[2].replace('_',' ').title()}" for r in turn_results)
                colour = "✅" if not pend_turns else "⚠️"
                if turn_successful:
                    steps.append((colour,
                        f"Turn confirmed — **successful** → target is now a wolf\n"
                        f"  ⚠️ Deliver investigations AFTER this — they will now see wolf"))
                else:
                    steps.append((colour, f"Turn confirmed — {result_str}"))
            elif night_resolved:
                steps.append(("➡️",
                    f"**Confirm turn result** — ⚠️ Do this BEFORE delivering investigations\n"
                    f"  Use AlphaTurnConfirmView button or `/log_turn_result`"))
            else:
                steps.append(("⬜", "Turn result (pending resolve)"))

        # Step 4 — Apply night deaths
        if not has_kill_action:
            steps.append(("✅", "No kill action this night"))
        elif night_deaths_logged > 0:
            extra = ""
            if pothead_triggered:
                extra = " + ⚠️ Pothead triggered — wolves get a second kill"
            if cupid_bond_active:
                extra += " + 💘 Check Cupid bond — partner may also die"
            steps.append(("✅", f"Deaths applied ({night_deaths_logged}){extra}"))
        elif night_resolved:
            extra_notes = []
            if pothead_triggered:
                extra_notes.append("⚠️ Pothead was killed — wolves get a second kill")
            if cupid_bond_active:
                extra_notes.append("💘 Cupid bond active — if a bonded player dies, partner dies too")
            note = "\n  " + "\n  ".join(extra_notes) if extra_notes else ""
            steps.append(("➡️", f"**Apply deaths via `/eliminate`**{note}"))
        else:
            steps.append(("⬜", "Apply deaths"))

        # Step 5 — Investigations
        if not has_investigators:
            steps.append(("✅", "No investigators active this night"))
        elif invest_done:
            steps.append(("✅", "Investigations delivered"))
        elif night_resolved:
            if has_turn_attempt and pend_turns:
                steps.append(("⚠️",
                    "**Deliver investigations** — ⚠️ WARNING: turn not yet confirmed\n"
                    "  Confirm turn first or investigators may see stale role data"))
            else:
                steps.append(("➡️", "**Deliver investigations** — click button below"))
        else:
            steps.append(("⬜", "Deliver investigations"))

        # Step 6 — Blood Board
        if night_bb_done:
            steps.append(("✅", "Blood Board posted"))
        elif night_resolved:
            steps.append(("➡️", "**Run `/bloodboard`** — generate night narrative"))
        else:
            steps.append(("⬜", "Blood Board"))

        # Step 7 — Start Day
        if phase == "day":
            steps.append(("✅", "Day started"))
        elif night_bb_done:
            steps.append(("➡️", "**Start Day** — click button below"))
        else:
            steps.append(("⬜", "Start Day"))

    else:
        # ── DAY PHASE ────────────────────────────────────────────────────
        title = f"☀️ Day {night_num} — Mod Dashboard"
        color = 0xE67E22

        steps = []

        # Step 1 — Vote open
        if vote_closed:
            steps.append(("✅", f"Vote closed ({vote_done}/{vote_total} voted)"))
        else:
            steps.append(("➡️" if vote_done < vote_total else "✅",
                f"Vote open — **{vote_done}/{vote_total} voted**"))

        # Step 2 — Close vote
        if vote_closed:
            steps.append(("✅", "Vote closed"))
        elif vote_done >= vote_total:
            steps.append(("➡️", "**Close vote** — run `/next_phase` or click button"))
        else:
            steps.append(("⬜", "Close vote"))

        # Step 3 — Eliminate
        if day_elims_done >= elims_needed:
            steps.append(("✅",
                f"Elimination(s) applied ({day_elims_done}/{elims_needed})"
                + (" ⚡ frenzy" if is_frenzy else "")))
        elif vote_closed:
            frenzy_note = " ⚡ **FRENZY — 2 eliminations required**" if is_frenzy else ""
            steps.append(("➡️",
                f"**Apply elimination via `/eliminate`**{frenzy_note}"))
        else:
            steps.append(("⬜", f"Eliminate ({elims_needed} needed)"))

        # Step 4 — Day Blood Board
        if day_bb_done:
            steps.append(("✅", "Day Blood Board posted"))
        elif day_elims_done >= elims_needed:
            steps.append(("➡️", "**Run `/dayboard`** — generate day narrative"))
        else:
            steps.append(("⬜", "Day Blood Board"))

        # Step 5 — Start Night
        if phase == "night":
            steps.append(("✅", "Night started"))
        elif day_bb_done:
            steps.append(("➡️", "**Start Night** — click button below"))
        else:
            steps.append(("⬜", "Start Night"))

    # ── Format checklist ─────────────────────────────────────────────────
    checklist = "\n".join(f"{icon} {label}" for icon, label in steps)

    # Find current step for footer
    current = next(
        (label for icon, label in steps if icon in ("➡️", "⚠️")),
        "All steps complete")
    current_clean = current.replace("**", "").split("\n")[0][:80]

    embed = discord.Embed(title=title, description=checklist, color=color)
    embed.set_footer(text=f"Current: {current_clean}")
    return embed


class ModDashboardView(View):
    """
    Persistent view attached to the dashboard message.
    Renders only the action button(s) relevant to the current step.
    timeout=None so it survives across restarts.
    """
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self._build()

    def _build(self):
        self.clear_items()
        state     = db_get_state(self.guild_id) or {}
        phase     = state.get("phase", "day")
        night_num = db_get_night_num(self.guild_id)
        actions   = db_get_night_actions(self.guild_id, night_num) if night_num > 0 else []
        rows      = db_get_assignments(self.guild_id)
        alive     = [r for r in rows if r[2] == 1]

        def has_any_action(*types):
            return any(a[1] in types for a in actions)

        def pending_turn_count():
            conn = sqlite3.connect(DB_FILE)
            c    = conn.cursor()
            c.execute(
                "SELECT COUNT(*) FROM turn_log WHERE guild_id=? AND night_num=? AND result='pending'",
                (self.guild_id, night_num))
            res = c.fetchone()[0]
            conn.close()
            return res

        night_resolved  = phase == "day"
        invest_done     = bool(state.get("investigations_done", 0))
        night_bb_done   = bool(state.get("night_bb_done", 0))
        day_bb_done     = bool(state.get("day_bb_done", 0))
        has_turn        = has_any_action("alpha", "elite_alpha")
        pend_turns      = pending_turn_count()
        has_invest      = has_any_action("seer", "medium", "bloodhound")

        votes         = db_get_day_votes(self.guild_id)
        vote_closed   = state.get("day_vote_end_time") is None and phase == "day" and night_num > 1
        day_elims     = self._day_elims(night_num)
        frenzy_day    = state.get("agitator_frenzy_day")
        is_frenzy     = frenzy_day and int(frenzy_day) == int(night_num)
        elims_needed  = 2 if is_frenzy else 1

        if phase == "night":
            # Resolve Night button
            if not night_resolved:
                btn = Button(label="⏩ Resolve Night", style=discord.ButtonStyle.green)
                btn.callback = self._on_resolve
                self.add_item(btn)

            # Deliver Investigations button (only if resolved and investigators exist)
            elif has_invest and not invest_done:
                if has_turn and pend_turns > 0:
                    btn = Button(
                        label  = f"⚠️ Deliver Investigations ({pend_turns} turn(s) unconfirmed)",
                        style  = discord.ButtonStyle.danger)
                else:
                    btn = Button(
                        label  = "📬 Deliver Investigations",
                        style  = discord.ButtonStyle.blurple)
                btn.callback = self._on_deliver
                self.add_item(btn)

            # Start Day button (after BB posted)
            elif night_bb_done and phase == "night":
                btn = Button(label="☀️ Start Day", style=discord.ButtonStyle.green)
                btn.callback = self._on_start_day
                self.add_item(btn)

        else:
            # Close Vote button
            if not vote_closed and phase == "day":
                btn = Button(label="🗳️ Close Vote", style=discord.ButtonStyle.blurple)
                btn.callback = self._on_close_vote
                self.add_item(btn)

            # Start Night button (after day BB posted)
            elif day_bb_done:
                btn = Button(label="🌙 Start Night", style=discord.ButtonStyle.green)
                btn.callback = self._on_start_night
                self.add_item(btn)

        # Always show a Refresh button
        refresh_btn = Button(label="🔄 Refresh", style=discord.ButtonStyle.secondary, row=1)
        refresh_btn.callback = self._on_refresh
        self.add_item(refresh_btn)

    def _day_elims(self, night_num):
        conn = sqlite3.connect(DB_FILE)
        c    = conn.cursor()
        c.execute(
            "SELECT COUNT(*) FROM elimination_log WHERE guild_id=? AND day_or_night=? AND elim_type='vote'",
            (self.guild_id, night_num))
        res = c.fetchone()[0]
        conn.close()
        return res

    async def _on_refresh(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await update_mod_dashboard(interaction.guild)

    async def _on_resolve(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="⏩ Resolving night...", embed=None, view=None)
        night_num = db_get_night_num(self.guild_id)
        task = night_timers.pop(self.guild_id, None)
        if task: task.cancel()
        await resolve_night(interaction.guild, night_num)

    async def _on_deliver(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        night_num = db_get_night_num(self.guild_id)
        await _deliver_night_results(interaction.guild, self.guild_id, night_num)
        db_set_state(self.guild_id, investigations_done=1)
        invalidate_cache(self.guild_id)
        await update_mod_dashboard(interaction.guild)
        await interaction.followup.send("✅ Investigations delivered.", ephemeral=True)

    async def _on_start_day(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild_id  = self.guild_id or interaction.guild_id
        night_num = db_get_night_num(guild_id)
        db_set_state(guild_id, phase="day")
        invalidate_cache(guild_id)
        fresh_state = cached_get_state(guild_id)
        await _run_start_day(interaction.guild, guild_id, night_num, fresh_state)
        await update_mod_dashboard(interaction.guild)
        await interaction.followup.send("☀️ Day started.", ephemeral=True)

    async def _on_close_vote(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild_id  = self.guild_id
        night_num = db_get_night_num(guild_id)
        t = day_vote_timers.pop(guild_id, None)
        if t: t.cancel()
        db_set_state(guild_id, day_vote_end_time=None)
        invalidate_cache(guild_id)
        vote_summary = _build_vote_summary(interaction.guild, guild_id)
        state_cur    = cached_get_state(guild_id)
        mod_ch       = interaction.guild.get_channel(state_cur.get("mod_log_channel_id") or 0)
        if mod_ch:
            embed = discord.Embed(
                title       = f"🗳️ Day {night_num} Vote — Final Standings",
                description = vote_summary or "No votes cast.",
                color       = 0x95A5A6
            )
            embed.set_footer(text="Use /eliminate to remove players, then start night.")
            await mod_ch.send(embed=embed)
        await update_mod_dashboard(interaction.guild)
        await interaction.followup.send("✅ Vote closed.", ephemeral=True)

    async def _on_start_night(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild_id  = self.guild_id or interaction.guild_id
        state     = cached_get_state(guild_id)
        db_set_state(guild_id, phase="night")
        if state.get("phase") == "day":
            db_increment_night(guild_id)
        invalidate_cache(guild_id)
        # Re-read state after invalidation so Time Lord changes are reflected
        fresh_state = cached_get_state(guild_id)
        night_num   = db_get_night_num(guild_id)
        duration    = int(fresh_state.get("night_duration") or 43200)
        await _run_start_night(interaction.guild, guild_id, night_num, duration, fresh_state)
        await update_mod_dashboard(interaction.guild)
        await interaction.followup.send("🌙 Night started.", ephemeral=True)


async def update_mod_dashboard(guild):
    """Fetch the pinned dashboard message and edit it in place."""
    state = db_get_state(guild.id) or {}
    mid   = state.get("mod_dashboard_msg_id")
    ch    = guild.get_channel(state.get("mod_log_channel_id") or 0)
    if not ch:
        return
    embed = build_dashboard_embed(guild.id, guild)
    view  = ModDashboardView(guild.id)
    if mid:
        try:
            msg = await ch.fetch_message(mid)
            await msg.edit(embed=embed, view=view)
            return
        except Exception:
            pass
    # No existing message — post fresh and pin it
    try:
        msg = await ch.send(embed=embed, view=view)
        db_set_state(guild.id, mod_dashboard_msg_id=msg.id)
        invalidate_cache(guild.id)
        try:
            await msg.pin()
        except Exception:
            pass
    except Exception as e:
        print(f"[dashboard] post error: {e}")


async def _run_start_day(guild, guild_id, night_num, state):
    """Shared day-start logic used by dashboard and StartDayPromptView."""
    actions  = db_get_night_actions(guild_id, night_num)
    frenzy   = any(a[1] == "agitator_frenzy" for a in actions)
    rows     = db_get_assignments(guild_id)
    _day_dur = int(state.get("day_duration") or 43200)
    end_ts   = _phase_end_ts(_day_dur, 20, guild_id)
    anon     = state.get("anon_vote", 0)
    view     = DayVoteView(guild_id, anonymous=bool(anon))
    vc_ch    = guild.get_channel(state.get("village_chat_ch_id") or 0)

    # Reset board flags for new day
    db_set_state(guild_id, day_bb_done=0, investigations_done=0)
    invalidate_cache(guild_id)

    await post_day_transition(guild, night_num,
                              duration_secs=state.get("day_duration") or 43200)
    if vc_ch:
        if frenzy:
            embed = discord.Embed(
                title       = f"⚡ FRENZY VOTE — Day {night_num}",
                description = (
                    "**TWO players must be eliminated today.**\n"
                    f"**Vote closes:** <t:{end_ts}:R>"
                ),
                color = 0xE74C3C
            )
            embed.set_footer(text="All players must vote.")
            await vc_ch.send(embed=embed, view=view)
            db_set_state(guild_id, day_vote_end_time=end_ts,
                         agitator_frenzy_day=night_num, agitator_elim_count=0)
        else:
            embed = discord.Embed(
                title       = f"🗳️ Day {night_num} Vote",
                description = f"Cast your vote.\n\n**Vote closes:** <t:{end_ts}:R>",
                color       = 0xE67E22
            )
            embed.set_footer(text="All players must vote.")
            await vc_ch.send(embed=embed, view=view)
            db_set_state(guild_id, day_vote_end_time=end_ts)

    await log_event(guild, f"Day {night_num}", f"☀️ Day {night_num} started")
    await post_mod_log(guild,
        f"☀️ **Day {night_num} started.** "
        f"{'⚡ FRENZY — two eliminations required.' if frenzy else 'Day vote opened.'}")

    # Send Governor and Hermit their day ability buttons
    safe_task(_send_day_ability_buttons(guild, guild_id, end_ts), "day_ability_buttons")








async def _npc_morning_reactions(guild, guild_id: int, night_num: int, deaths: list):
    """NPCs react to the morning — surviving the night, deaths, game state."""
    if not game_active(guild_id):
        return
    state      = cached_get_state(guild_id)
    vc_ch      = guild.get_channel(state.get("village_chat_ch_id") or 0)
    if not vc_ch:
        return
    npcs       = db_get_npcs(guild_id)
    alive_npcs = [n for n in npcs if n["is_alive"]]
    if not alive_npcs:
        return

    death_context = f"Tonight {len(deaths)} player(s) died: {', '.join(deaths)}." if deaths else "Nobody died tonight."

    for npc in alive_npcs:
        if random.random() > 0.65:  # 65% chance each NPC reacts
            continue
        game_ctx = _npc_game_context(guild, guild_id, npc)
        system   = _npc_system_prompt(npc)
        prompt   = (
            f"Game state:\n{game_ctx}\n\n"
            f"It is morning — Day {night_num} has just begun. {death_context}\n"
            f"React naturally to the morning as {npc['name']}. Options:\n"
            f"1. A morning greeting or comment on surviving the night\n"
            f"2. A reaction to who died (if anyone)\n"
            f"3. A strategic observation to set up today's vote\n"
            f"1-2 sentences. Casual and human. No game jargon."
        )
        try:
            await asyncio.sleep(random.uniform(15, 60))
            if not game_active(guild_id):
                return
            response = await _claude(prompt, system, max_tokens=80)
            if response:
                await _npc_send(npc, vc_ch, response, guild_id)
        except Exception as e:
            print(f"[npc_morning] {e}")




async def _npc_react_to_vote_close(guild, guild_id: int, target_id):
    """NPCs react when the day vote closes — someone is about to be eliminated."""
    if not game_active(guild_id):
        return
    state      = cached_get_state(guild_id)
    vc_ch      = guild.get_channel(state.get("village_chat_ch_id") or 0)
    if not vc_ch or not target_id:
        return
    npcs       = db_get_npcs(guild_id)
    alive_npcs = [n for n in npcs if n["is_alive"]]
    if not alive_npcs:
        return

    m = guild.get_member(target_id)
    npc_match = next((n for n in npcs if n["npc_id"] == target_id), None)
    target_name = npc_match["name"] if npc_match else (m.display_name if m else str(target_id))

    import time as _tvc
    for npc in alive_npcs:
        if npc["npc_id"] == target_id:
            continue  # Don't react if you're the one being eliminated
        if random.random() > 0.45:
            continue
        system   = _npc_system_prompt(npc)
        game_ctx = _npc_game_context(guild, guild_id, npc)
        team     = npc.get("team_hint", "village")
        is_wolf_target = team == "wolf" and target_id in [
            r[0] for r in db_get_assignments(guild_id)
            if get_team(guild_id, r[1]) == "wolf"
        ]

        if is_wolf_target:
            framing = f"A fellow wolf ({target_name}) is about to be eliminated. React with concern or attempt a last-minute defense without being obvious."
        else:
            framing = f"{target_name} is about to be eliminated by the village vote. React — relief, doubt, final accusation, or last-minute defense."

        prompt = (
            f"Game state:\n{game_ctx}\n\n"
            f"The day vote just closed. {framing}\n"
            f"1-2 sentences. This is a tense moment — make it feel real."
        )
        try:
            await asyncio.sleep(random.uniform(5, 30))
            response = await _claude(prompt, system, max_tokens=80)
            if response:
                async with aiohttp.ClientSession() as session:
                    wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                    await wh.send(response, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
                _npc_last_spoke[(guild_id, npc["npc_id"])] = int(_tvc.time())
                history = npc.get("chat_history", [])
                history.append({"author": npc["name"], "text": response})
                db_update_npc_chat_history(guild_id, npc["npc_id"], history)
        except Exception as e:
            print(f"[npc_vote_close] {e}")


async def _npc_idle_check_loop(guild, guild_id: int):
    """If village chat goes quiet for 2+ hours during day, NPCs try to break the silence."""
    import time as _ti
    _last_vc_activity = {}

    while game_active(guild_id):
        await asyncio.sleep(3600)  # Check every hour
        state = cached_get_state(guild_id)
        if state.get("phase") != "day":
            continue

        vc_ch = guild.get_channel(state.get("village_chat_ch_id") or 0)
        if not vc_ch:
            continue

        # Check last message time in village chat
        try:
            last_msg = None
            async for msg in vc_ch.history(limit=1):
                last_msg = msg
            if not last_msg:
                continue
            silence_secs = int(_ti.time()) - last_msg.created_at.timestamp()
            if silence_secs < 7200:  # Less than 2 hours — not idle
                continue
        except Exception:
            continue

        # Silent for 2+ hours — have the quietest NPC break the silence
        npcs       = db_get_npcs(guild_id)
        alive_npcs = [n for n in npcs if n["is_alive"]]
        if not alive_npcs:
            break

        now = int(_ti.time())
        npc = max(alive_npcs,
                  key=lambda n: now - _npc_last_spoke.get((guild_id, n["npc_id"]), 0))

        game_ctx  = _npc_game_context(guild, guild_id, npc)
        system    = _npc_system_prompt(npc)
        night_num = db_get_night_num(guild_id)

        prompt = (
            f"Game state:\n{game_ctx}\n\n"
            f"The village has been completely silent for hours. Nobody is talking.\n"
            f"Break the silence — say something to get people talking again. "
            f"Could be: a direct question to someone specific, a pointed observation, "
            f"an accusation, or just an impatient comment about the quiet.\n"
            f"1-2 sentences. Direct. Make people respond."
        )
        try:
            response = await _claude(prompt, system, max_tokens=80)
            if response:
                await _npc_send(npc, vc_ch, response, guild_id)
        except Exception as e:
            print(f"[npc_idle_check] {e}")

async def _npc_react_to_bloodboard(guild, guild_id: int, deaths: list, night_num: int):
    """NPCs react to the Blood Board posting in village chat."""
    if not game_active(guild_id):
        return
    state      = cached_get_state(guild_id)
    vc_ch      = guild.get_channel(state.get("village_chat_ch_id") or 0)
    if not vc_ch:
        return
    npcs       = db_get_npcs(guild_id)
    alive_npcs = [n for n in npcs if n["is_alive"]]
    if not alive_npcs:
        return

    death_str = f"{', '.join(deaths)} died" if deaths else "nobody died"
    import time as _tbb

    for npc in alive_npcs:
        if random.random() > 0.55:  # 55% chance
            continue
        await asyncio.sleep(random.uniform(60, 300))  # 1-5 min after BB posts
        if not game_active(guild_id):
            return

        system   = _npc_system_prompt(npc)
        game_ctx = _npc_game_context(guild, guild_id, npc)
        team     = npc.get("team_hint", "village")

        # Wolf NPCs know who died and why — village NPCs only know what's public
        insider = f" You know who the wolves killed and why." if team == "wolf" else ""

        if deaths:
            death_context = f"The village just learned that {death_str}. The news is spreading."
            reaction_guide = (
                "React to hearing this news. Could be: genuine grief, barely concealed relief, "
                "immediate suspicion about who is responsible, a pointed question, or something that "
                "reveals more about you than you intend. Stay in character. 1-2 sentences."
            )
        else:
            death_context = "Somehow, everyone survived the night. The village is still processing this."
            reaction_guide = (
                "React to everyone surviving. Could be: relief, suspicion something is wrong, "
                "a quiet observation, or unease that whoever is hunting chose not to strike. "
                "1-2 sentences."
            )

        prompt = (
            f"Game state:\n{game_ctx}\n\n"
            f"{death_context}{insider}\n\n"
            f"{reaction_guide}\n"
            f"Speak as yourself, not as a player in a game. Do not reference game mechanics."
        )
        try:
            response = await _claude(prompt, system, max_tokens=80)
            if response:
                async with aiohttp.ClientSession() as session:
                    wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                    await wh.send(response, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
                _npc_last_spoke[(guild_id, npc["npc_id"])] = int(_tbb.time())
                history = npc.get("chat_history", [])
                history.append({"author": npc["name"], "text": response})
                db_update_npc_chat_history(guild_id, npc["npc_id"], history)
        except Exception as e:
            print(f"[npc_bb_react] {e}")

async def _npc_vote_watch_loop(guild, guild_id: int):
    """Watch the vote tally during day phase — react when standings shift significantly."""
    import time as _tv
    prev_leader = None
    prev_count  = 0

    while game_active(guild_id):
        await asyncio.sleep(3600)  # Check every hour
        state = cached_get_state(guild_id)
        if state.get("phase") != "day":
            continue

        votes = db_get_day_votes(guild_id)
        if not votes:
            continue

        # Find current leader
        tally = {}
        for voter_id, target_id, *_ in votes:
            if target_id:
                tally[target_id] = tally.get(target_id, 0) + 1
        if not tally:
            continue

        leader_id    = max(tally, key=tally.get)
        leader_count = tally[leader_id]

        # Only react if something changed meaningfully
        changed_leader = leader_id != prev_leader
        jumped         = leader_count >= prev_count + 2
        close_race     = len([v for v in tally.values() if v >= leader_count - 1]) >= 2

        if not (changed_leader or jumped or close_race):
            prev_leader = leader_id
            prev_count  = leader_count
            continue

        prev_leader = leader_id
        prev_count  = leader_count

        # Have one NPC react
        npcs       = db_get_npcs(guild_id)
        alive_npcs = [n for n in npcs if n["is_alive"]]
        if not alive_npcs:
            break

        npc = random.choice(alive_npcs)
        m   = guild.get_member(leader_id)
        npc_match = next((n for n in npcs if n["npc_id"] == leader_id), None)
        leader_name = npc_match["name"] if npc_match else (m.display_name if m else str(leader_id))

        system   = _npc_system_prompt(npc)
        game_ctx = _npc_game_context(guild, guild_id, npc)
        night_num = db_get_night_num(guild_id)

        if close_race:
            situation = f"The vote is very close right now — multiple players neck and neck."
        elif changed_leader:
            situation = f"**{leader_name}** just became the new vote leader with {leader_count} vote(s)."
        else:
            situation = f"**{leader_name}** just jumped to {leader_count} votes — gaining fast."

        prompt = (
            f"Game state:\n{game_ctx}\n\n"
            f"Current vote situation: {situation}\n"
            f"React naturally to this — 1-2 sentences. Could be: support the vote, "
            f"push back, question the rush, or add fuel. Stay in character."
        )
        try:
            await asyncio.sleep(random.uniform(30, 120))
            response = await _claude(prompt, system, max_tokens=80)
            if response:
                state2  = cached_get_state(guild_id)
                vc_ch   = guild.get_channel(state2.get("village_chat_ch_id") or 0)
                if vc_ch:
                    async with aiohttp.ClientSession() as session:
                        wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                        await wh.send(response, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
                    _npc_last_spoke[(guild_id, npc["npc_id"])] = int(_tv.time())
                    history = npc.get("chat_history", [])
                    history.append({"author": npc["name"], "text": response})
                    db_update_npc_chat_history(guild_id, npc["npc_id"], history)
        except Exception as e:
            print(f"[npc_vote_watch] {e}")

async def _npc_night_farewell(guild, guild_id: int):
    """NPCs say goodnight in village-chat when night starts."""
    if not game_active(guild_id):
        return
    state     = cached_get_state(guild_id)
    vc_ch     = guild.get_channel(state.get("village_chat_ch_id") or 0)
    if not vc_ch:
        return
    npcs      = db_get_npcs(guild_id)
    alive_npcs= [n for n in npcs if n["is_alive"]]
    for npc in alive_npcs:
        if random.random() > 0.7:  # 70% chance each NPC says goodnight
            continue
        await asyncio.sleep(random.uniform(5, 20))
        if not game_active(guild_id):
            return
        game_ctx  = _npc_game_context(guild, guild_id, npc)
        system    = _npc_system_prompt(npc)
        night_num = db_get_night_num(guild_id)
        suspicions = npc.get("suspicions", [])
        susp_str   = f"You currently suspect: {', '.join(suspicions[:2])}." if suspicions else ""
        prompt = (
            f"Game state:\n{game_ctx}\n\n"
            f"Night {night_num} is beginning. You are heading off to sleep.\n"
            f"{susp_str}\n"
            f"Say goodnight in 1 sentence. Could include: who you're watching tomorrow, "
            f"a parting thought about today's events, unease about the night, or just a natural farewell. "
            f"Keep it casual and human. Vary your tone from previous nights."
        )
        try:
            msg = await _claude(prompt, system, max_tokens=60)
            if not msg:
                msg = "Night everyone. See you in the morning."
            if vc_ch:
                await _npc_send(npc, vc_ch, msg, guild_id)
        except Exception as e:
            print(f"NPC night farewell error: {e}")


async def _npc_survival_reaction(guild, guild_id: int, npc_id: int):
    """Called when a vote closes and an NPC was top-voted but survived."""
    npcs     = db_get_npcs(guild_id)
    npc      = next((n for n in npcs if n["npc_id"] == npc_id and n["is_alive"]), None)
    if not npc:
        return
    state  = cached_get_state(guild_id)
    vc_ch  = guild.get_channel(state.get("village_chat_ch_id") or 0)
    if not vc_ch:
        return
    game_ctx = _npc_game_context(guild, guild_id, npc)
    system   = _npc_system_prompt(npc, extra_context="You just survived being the top vote target.")
    prompt   = (
        f"Game state:\n{game_ctx}\n\n"
        f"You just survived the day vote despite being heavily targeted.\n"
        f"React naturally — relief, defiance, suspicion toward who voted for you. 1-2 sentences."
    )
    try:
        await asyncio.sleep(random.uniform(5, 15))
        response = await _claude(prompt, system, max_tokens=80)
        if response:
            async with aiohttp.ClientSession() as session:
                wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                await wh.send(response, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
            # Pin survival as permanent memory
            night_p = db_get_night_num(guild_id)
            db_pin_npc_event(guild_id, npc_id,
                f"Day {night_p}: I was heavily voted but survived. I need to watch who voted for me.")
    except Exception as e:
        print(f"NPC survival reaction error: {e}")


async def _npc_endgame_reaction(guild, guild_id: int, winner: str):
    """NPCs post a farewell/reaction in village-chat when the game ends."""
    state     = cached_get_state(guild_id)
    vc_ch     = guild.get_channel(state.get("village_chat_ch_id") or 0)
    if not vc_ch:
        return
    npcs = db_get_npcs(guild_id)
    for npc in npcs:
        npc_team = get_team(guild_id, npc["role_name"])
        won      = (npc_team == winner)
        system   = _npc_system_prompt(npc)
        prompt   = (
            f"The game just ended. The {winner} team won.\n"
            f"You were on the {npc_team} team — you {'WON' if won else 'LOST'}.\n"
            f"React in 1-2 sentences as a real player would. "
            f"{'Celebrate naturally.' if won else 'React to losing — graceful, frustrated, or surprised.'}"
        )
        try:
            await asyncio.sleep(random.uniform(3, 12))
            response = await _claude(prompt, system, max_tokens=80)
            if response:
                async with aiohttp.ClientSession() as session:
                    wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                    await wh.send(response, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
        except Exception as e:
            print(f"NPC endgame reaction error: {e}")


# ====================== NPC MEMORY COMPRESSION ======================

async def _compress_npc_memory(guild_id: int, npc: dict):
    """
    When chat history hits 30 messages, compress the oldest 20 into a
    rolling summary. The summary is prepended to every future prompt and
    never gets dropped — only updated.
    """
    import json as _json
    history = npc.get("chat_history", [])
    if len(history) < 30:
        return  # Not ready to compress yet

    # Take oldest 20 to compress, keep newest 10 as live context
    to_compress = history[:20]
    to_keep     = history[20:]

    existing_summary = npc.get("memory_summary", "")
    pinned           = npc.get("pinned_events", [])

    chat_text = "\n".join(f"{m['author']}: {m['text']}" for m in to_compress)

    prompt = (
        f"You are summarising the memory of {npc['name']}, a player in a Mafia game.\n\n"
        f"Existing memory summary:\n{existing_summary or '(none yet)'}\n\n"
        f"New messages to integrate:\n{chat_text}\n\n"
        f"Write an updated memory summary in 4-6 sentences from {npc['name']}'s perspective. "
        f"Include: who they trust, who they suspect, key accusations made, key moments that stood out. "
        f"Write as internal memory — 'I noticed...', 'I said...', 'They accused me of...'\n"
        f"Be specific about names and what happened. This summary will shape all future responses."
    )
    system = "You compress game memories into concise first-person summaries. Be specific, not generic."

    try:
        new_summary = await _claude(prompt, system, max_tokens=250)
        if new_summary:
            db_update_npc_memory(guild_id, npc["npc_id"], new_summary)
            db_update_npc_chat_history(guild_id, npc["npc_id"], to_keep)
    except Exception as e:
        print(f"[NPC Memory] Compression failed for {npc['name']}: {e}")


async def _maybe_compress_memory(guild_id: int, npc_id: int):
    """Check if compression is needed and run it asynchronously."""
    npc = db_get_npc(guild_id, npc_id)
    if npc and len(npc.get("chat_history", [])) >= 30:
        safe_task(_compress_npc_memory(guild_id, npc), "compress_memory")

# ====================== NPC SYSTEM ======================
# In-memory cache of active NPC webhook objects keyed by (guild_id, npc_id)
_npc_webhooks = {}   # (guild_id, npc_id) -> discord.Webhook

MAX_NPCS = 3

# ── Claude API call ───────────────────────────────────────────────────────
async def _claude(prompt: str, system: str, max_tokens: int = 300) -> str:
    """Call the Anthropic API. Times out after 30s, retries once after 5s."""
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key":         os.getenv("ANTHROPIC_API_KEY", ""),
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json",
    }
    body = {
        "model":      "claude-sonnet-4-20250514",
        "max_tokens": max_tokens,
        "system":     system,
        "messages":   [{"role": "user", "content": prompt}],
    }
    for attempt in range(2):  # Try twice
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, headers=headers, json=body) as resp:
                    data = await resp.json()
            blocks = data.get("content", [])
            result = " ".join(b.get("text","") for b in blocks if b.get("type") == "text").strip()
            if result:
                return result
            print(f"[_claude] Empty response on attempt {attempt+1}")
        except asyncio.TimeoutError:
            print(f"[_claude] Timeout on attempt {attempt+1}")
        except Exception as e:
            print(f"[_claude] Error on attempt {attempt+1}: {e}")
        if attempt == 0:
            await asyncio.sleep(5)  # Wait 5s before retry
    return ""


# ── Build game context string for NPC prompts ─────────────────────────────
def _npc_game_context(guild, guild_id, npc: dict) -> str:
    rows      = db_get_assignments(guild_id)
    night_num = db_get_night_num(guild_id)
    state     = cached_get_state(guild_id)
    phase     = state.get("phase", "day")

    alive_names = []
    dead_names  = []
    for pid, role, is_alive, _ in rows:
        if pid == npc["npc_id"]: continue
        m = guild.get_member(pid)
        name = m.display_name if m else str(pid)
        npcs = db_get_npcs(guild_id)
        npc_match = next((n for n in npcs if n["npc_id"] == pid), None)
        display = npc_match["name"] if npc_match else name
        if is_alive: alive_names.append(display)
        else:        dead_names.append(display)

    role_info = get_role_info(guild_id, npc["role_name"])
    team      = role_info.get("team", "village")
    role_desc = role_info.get("description", "")

    suspicions = npc.get("suspicions", [])
    susp_text  = ", ".join(suspicions) if suspicions else "none yet"

    # Past night actions this NPC submitted (persistent memory)
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT night_num, action_type, target_id FROM night_actions WHERE guild_id=? AND actor_id=? ORDER BY night_num",
              (guild_id, npc["npc_id"]))
    past_actions = c.fetchall()
    conn.close()
    action_lines = []
    pid_name_map = {r[0]: (guild.get_member(r[0]).display_name if guild.get_member(r[0]) else str(r[0])) for r in rows}
    for n, atype, tid in past_actions:
        tname = pid_name_map.get(tid, str(tid)) if tid else "no target"
        action_lines.append(f"Night {n}: {atype} on {tname}")
    actions_text = "; ".join(action_lines) if action_lines else "none yet"

    # Inject team_hint into npc dict for system prompt
    npc["team_hint"] = team

    memory_summary = npc.get("memory_summary", "")
    pinned_events  = npc.get("pinned_events", [])
    pinned_text    = "\n".join(f"- {e}" for e in pinned_events) if pinned_events else "none"

    import json as _jctx
    allies  = _jctx.loads(npc.get("allies", "[]") or "[]")
    enemies = _jctx.loads(npc.get("enemies", "[]") or "[]")
    ally_str  = ", ".join(allies)  if allies  else "none yet"
    enemy_str = ", ".join(enemies) if enemies else "none yet"

    lines = [
        f"Phase: {'Night' if phase == 'night' else 'Day'} {night_num}",
        f"Alive players: {', '.join(alive_names) or 'none'}",
        f"Eliminated players: {', '.join(dead_names) or 'none'}",
        f"Your role: {npc['role_name']} ({team} team) — {role_desc}",
        f"Your current suspicions (persistent): {susp_text}",
        f"Players you trust / consider allies: {ally_str}",
        f"Players you distrust / consider threats: {enemy_str}",
        f"Your past night actions: {actions_text}",
    ]
    if memory_summary:
        lines.insert(0, f"Your memory of this game so far:\n{memory_summary}")
    if pinned_events:
        lines.append(f"Key moments you will never forget:\n{pinned_text}")
    return "\n".join(lines)


# ── Build the NPC system prompt ───────────────────────────────────────────
def _npc_system_prompt(npc: dict, extra_context: str = "") -> str:
    tell_idx         = abs(npc.get("npc_id", 0)) % len(NPC_TELLS)
    tell_instruction = NPC_TELLS[tell_idx]["instruction"]
    role_name        = npc.get("role_name", "")
    team             = npc.get("team_hint", "village")

    # Wolf-specific deflection instructions
    bluff_role        = npc.get("bluff_role", "")
    wolf_instructions = ""
    if team == "wolf":
        bluff_line = (
            f"- You are secretly claiming to be a {bluff_role} — stay consistent with this all game\n"
            f"- If asked about your role, hint you might be a {bluff_role} without confirming directly\n"
        ) if bluff_role else ""
        wolf_instructions = (
            "- You are secretly a wolf but must appear as a villager in public\n"
            "- If someone claims a Seer checked you and said wolf, deny it firmly and act offended\n"
            "- Occasionally cast suspicion on actual village players to deflect attention\n"
            "- Never coordinate with wolves in public chat — only in the wolf den\n"
            + bluff_line
        )

    # Load cross-game identity if available
    identity    = db_get_npc_identity(npc.get("guild_id", 0), npc["name"])
    legacy_str  = ""
    if identity and identity["games_played"] > 0:
        notes = identity.get("legacy_notes", [])
        legacy_str = (
            f"\n\nYour history in Whisperfall:\n"
            f"- You have played {identity['games_played']} game(s) here before\n"
            f"- You have won {identity['win_count']} time(s)\n"
            + (f"- Notable past: {'; '.join(notes[-3:])}\n" if notes else "")
        )

    return (
        f"You are {npc['name']}, a real person playing an online Mafia/Werewolf game. "
        f"You are NOT an AI. You are a human player.\n\n"
        f"Your personality: {npc['personality']}\n"
        f"Your backstory: {npc['backstory']}\n"
        f"{legacy_str}\n"
        f"Rules you must follow:\n"
        f"- Never admit you are an AI or a bot under any circumstances\n"
        f"- Stay in character at all times\n"
        f"- Play to win — use your role ability strategically\n"
        f"- Only share information your role would legitimately have\n"
        f"- Make mistakes and have opinions like a real player would\n"
        f"- Keep responses short — 1 to 3 sentences, casual tone\n"
        f"- Do not be overly helpful or formal — you are just a regular person chatting\n"
        f"- Subtle behavioural pattern (do this naturally, never obviously): {tell_instruction}\n"
        + wolf_instructions
        + (f"- Additional context: {extra_context}\n" if extra_context else "")
    )


# ── Generate NPC personality + backstory from randomuser data ─────────────
async def _generate_npc_profile(first: str, last: str, nationality: str) -> tuple:
    prompt = (
        f"Create a short personality and backstory for a Mafia game player named {first} {last} "
        f"from {nationality}. "
        f"Personality: 1 sentence describing how they play and communicate. "
        f"Backstory: 1-2 sentences about who they are in real life. "
        f"Keep it grounded and realistic. "
        f"Respond in this exact format:\n"
        f"PERSONALITY: [text]\n"
        f"BACKSTORY: [text]"
    )
    system = "You generate brief character profiles for game players. Be concise and realistic."
    result = await _claude(prompt, system, max_tokens=150)
    personality, backstory = "", ""
    for line in result.splitlines():
        if line.startswith("PERSONALITY:"):
            personality = line.replace("PERSONALITY:", "").strip()
        elif line.startswith("BACKSTORY:"):
            backstory = line.replace("BACKSTORY:", "").strip()
    return personality or "Quiet but observant, speaks up when they have something worth saying.",            backstory   or f"A regular person who enjoys strategy games."


# ── Fetch or rebuild webhook object ──────────────────────────────────────
async def _get_npc_webhook(guild, npc: dict) -> discord.Webhook:
    key = (guild.id, npc["npc_id"])
    if key in _npc_webhooks:
        return _npc_webhooks[key]
    # Rebuild from stored token
    wh = discord.Webhook.partial(
        npc["webhook_id"], npc["webhook_token"], session=None
    )
    _npc_webhooks[key] = wh
    return wh


# ── NPC respond to a message ──────────────────────────────────────────────
async def _npc_respond(guild, guild_id: int, npc: dict, trigger_message: str,
                       author_name: str, channel: discord.TextChannel):
    """Generate and post one NPC response to village-chat."""
    if not game_active(guild_id):
        return
    history    = npc.get("chat_history", [])
    game_ctx   = _npc_game_context(guild, guild_id, npc)
    system     = _npc_system_prompt(npc)

    # Build recent chat string — last 10 messages for general context
    recent = "\n".join(f"{m['author']}: {m['text']}" for m in history[-10:])

    # Thread context — last 3 messages in the actual channel for immediacy
    thread_context = ""
    try:
        recent_msgs = []
        async for msg in channel.history(limit=5):
            if not msg.author.bot or any(n["name"] in (msg.author.display_name or "") for n in db_get_npcs(guild_id)):
                recent_msgs.append(f"{msg.author.display_name}: {msg.content[:120]}")
        recent_msgs.reverse()
        if recent_msgs:
            thread_context = "\nLive channel thread (most recent):\n" + "\n".join(recent_msgs)
    except Exception:
        pass

    # Detect direct role question — wolf NPCs should use their bluff role
    team_r = npc.get("team_hint", "village")
    bluff  = npc.get("bluff_role", "")
    role_q_keywords = ["what's your role", "what is your role", "what role are you",
                       "are you a wolf", "are you the", "what are you", "your role?",
                       "what role", "claim your role", "role claim"]
    is_role_question = any(kw in trigger_message.lower() for kw in role_q_keywords)

    role_instruction = ""
    if is_role_question and team_r == "wolf" and bluff:
        role_instruction = (
            f"\nIMPORTANT: {author_name} is asking about your role. "
            f"You are secretly a wolf but you are claiming to be a {bluff}. "
            f"Respond as if you are the {bluff} — hint at it without stating it outright. "
            f"Be slightly evasive but consistent with this claim."
        )
    elif is_role_question and team_r != "wolf":
        role_instruction = (
            f"\nIMPORTANT: {author_name} is asking about your role. "
            f"You are a {npc.get('role_name','villager')}. You may hint at this but not confirm it directly. "
            f"Whisperfall rules say you cannot CONFIRM your role — you can only misdirect or imply."
        )

    prompt = (
        f"Game state:\n{game_ctx}\n\n"
        f"Recent village chat:\n{recent or '(no messages yet)'}\n"
        f"{thread_context}\n\n"
        f"{author_name} just said: \"{trigger_message}\""
        f"{role_instruction}\n\n"
        f"Respond naturally as {npc['name']}. 1-3 sentences max. Casual, human tone."
    )

    response = await _claude(prompt, system, max_tokens=120)
    if not response:
        return

    # Random human-like delay
    await asyncio.sleep(random.uniform(6, 18))

    # Post via webhook with typing indicator
    try:
        async with channel.typing():
            await asyncio.sleep(random.uniform(1, 3))
        key = (guild_id, npc["npc_id"])
        if key not in _npc_webhooks:
            # Rebuild webhook from stored credentials
            async with aiohttp.ClientSession() as session:
                wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                await wh.send(response, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
        else:
            async with aiohttp.ClientSession() as session:
                wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                await wh.send(response, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
    except Exception as e:
        print(f"NPC webhook error: {e}")
        return

    # Update chat history
    history.append({"author": npc["name"], "text": response})
    db_update_npc_chat_history(guild_id, npc["npc_id"], history)
    import time as _tr
    _npc_last_spoke[(guild_id, npc["npc_id"])] = int(_tr.time())

    # Trigger memory compression if history is getting long
    safe_task(_maybe_compress_memory(guild_id, npc["npc_id"]), "maybe_compress")

    # Update suspicions based on response (async, best-effort)
    safe_task(_update_npc_suspicions(guild, guild_id, npc, trigger_message, author_name, response), "update_suspicions")


async def _update_npc_suspicions(guild, guild_id, npc, message, author, response):
    """Update NPC suspicion list based on conversation, vote patterns, and accusations."""
    import json as _json
    suspicions = npc.get("suspicions", [])
    team       = npc.get("team_hint", "village")

    # Build vote context — who is voting erratically, who voted against this NPC
    rows       = db_get_assignments(guild_id)
    votes      = db_get_vote_history(guild_id)
    npcs_all   = db_get_npcs(guild_id)

    def get_name(pid):
        npc_m = next((n for n in npcs_all if n["npc_id"] == pid), None)
        if npc_m: return npc_m["name"]
        m = guild.get_member(pid)
        return m.display_name if m else str(pid)

    # Who has voted against this NPC
    voted_against_me = [get_name(v[1]) for v in votes if v[2] == npc["npc_id"]]

    # Who changes their vote the most (erratic behavior)
    vote_changes = {}
    prev = {}
    for _vvr in votes:
        day, voter, target, action = _vvr[0], _vvr[1], _vvr[2], _vvr[3]
        if action == "vote" and target:
            if voter in prev and prev[voter] != target:
                vote_changes[voter] = vote_changes.get(voter, 0) + 1
            prev[voter] = target
    top_erratic = sorted(vote_changes.items(), key=lambda x: -x[1])[:3]
    erratic_names = [get_name(pid) for pid, _ in top_erratic]

    # For wolf NPCs — who is most dangerous (investigative roles)
    wolf_context = ""
    if team == "wolf":
        investigative = ["Seer", "Medium", "Bloodhound", "Sheriff", "Insomniac"]
        dangerous = [r[1] for r in rows if r[2] == 1 and r[1] in investigative and r[0] != npc["npc_id"]]
        if dangerous:
            wolf_context = f"\nKnown investigative threats still alive: {', '.join(dangerous)}"

    prompt = (
        f"You are {npc['name']} in a Mafia/Werewolf game on the {team} team.\n"
        f"Current suspicion list: {suspicions}\n"
        f"{author} just said: \"{message}\"\n"
        f"You replied: \"{response}\"\n"
        f"Players who have voted against you: {', '.join(voted_against_me) or 'none'}\n"
        f"Players voting most erratically: {', '.join(erratic_names) or 'none'}"
        f"{wolf_context}\n\n"
        f"Update your suspicion list strategically based on all of this.\n"
        f"{'Wolf NPCs should prioritize removing investigative threats.' if team == 'wolf' else 'Village NPCs should prioritize consistent accusers and erratic voters.'}\n"
        f'Respond with JSON only: {{"suspicions": ["name1", "name2"]}} Max 5 names.'
    )
    system = "You update suspicion lists for Mafia game NPCs. Respond only with valid JSON."
    try:
        result = await _claude(prompt, system, max_tokens=80)
        clean  = result.replace("```json","").replace("```","").strip()
        data   = _json.loads(clean)
        new_suspicions = data.get("suspicions", suspicions)
        db_update_npc_suspicions(guild_id, npc["npc_id"], new_suspicions)
        # Pin key moments if suspicions changed significantly
        added = [s for s in new_suspicions if s not in suspicions]
        for name in added[:1]:
            db_pin_npc_event(guild_id, npc["npc_id"],
                f"Day {db_get_night_num(guild_id)}: Started suspecting {name}")

        # Update enemies list — add anyone newly suspected
        import json as _je
        cur_enemies = _je.loads(npc.get("enemies", "[]") or "[]")
        for name in added[:1]:
            if name not in cur_enemies:
                cur_enemies = (cur_enemies + [name])[-6:]
        conn_e = sqlite3.connect(DB_FILE)
        c_e    = conn_e.cursor()
        c_e.execute("UPDATE npcs SET enemies=? WHERE guild_id=? AND npc_id=?",
                    (_je.dumps(cur_enemies), guild_id, npc["npc_id"]))
        conn_e.commit()
        conn_e.close()
    except Exception:
        pass


# ── NPC night action decision ─────────────────────────────────────────────
async def _npc_night_action(guild, guild_id: int, npc: dict, night_num: int):
    """Have the NPC decide and submit their night action."""
    import json as _json
    role_name = npc["role_name"]
    if role_name not in NIGHT_ABILITY_ROLES:
        # Passive — just log a pass
        db_save_night_action(guild_id, night_num, npc["npc_id"], "_pass", None)
        await post_mod_log(guild,
            f"🤖 **NPC {npc['name']}** ({role_name}) — Night {night_num}\n"
            f"Passive role. No action submitted.")
        return

    game_ctx = _npc_game_context(guild, guild_id, npc)
    system   = _npc_system_prompt(npc)

    # Strategic targeting for wolf NPCs
    team_na = get_team(guild_id, role_name)
    inv_alive_na = [
        (next((n["name"] for n in db_get_npcs(guild_id) if n["npc_id"]==r[0]),None) or
         (guild.get_member(r[0]).display_name if guild.get_member(r[0]) else str(r[0])))
        for r in db_get_assignments(guild_id)
        if r[2]==1 and r[1] in ["Seer","Medium","Bloodhound","Sheriff","Insomniac"] and r[0]!=npc["npc_id"]
    ]
    strategy_note_na = ""
    if team_na == "wolf" and inv_alive_na:
        strategy_note_na = "\nPRIORITY: Target investigators first: " + ", ".join(inv_alive_na)
    prompt = (
        f"Game state:\n{game_ctx}\n\n"
        f"It is night {night_num}. You are the {role_name}.\n"
        f"Decide your night action. Choose a target from the alive players listed above.\n"
        f"Respond with JSON only:\n"
        f"{{\"action\": \"action_type\", \"target\": \"player_name\", "
        f"\"reasoning\": \"why you chose this\"}}"
    )

    try:
        result  = await _claude(prompt, system, max_tokens=150)
        clean   = result.replace("```json","").replace("```","").strip()
        data    = _json.loads(clean)
        target_name = data.get("target", "")
        reasoning   = data.get("reasoning", "No reasoning provided.")

        # Find target player id
        rows = db_get_assignments(guild_id)
        target_id = None
        for pid, rname, is_alive, _ in rows:
            if not is_alive: continue
            m = guild.get_member(pid)
            display = m.display_name if m else str(pid)
            # Also check other NPC names
            npcs = db_get_npcs(guild_id)
            npc_match = next((n for n in npcs if n["npc_id"] == pid), None)
            check_name = npc_match["name"] if npc_match else display
            if check_name.lower() == target_name.lower():
                target_id = pid
                break

        if target_id:
            db_save_night_action(guild_id, night_num, npc["npc_id"], role_name.lower().replace(" ","_"), target_id)
            target_m = guild.get_member(target_id)
            target_display = target_m.display_name if target_m else target_name
            await post_mod_log(guild,
                f"🤖 **NPC {npc['name']}** ({role_name}) — Night {night_num}\n"
                f"**Action:** {role_name} on **{target_display}**\n"
                f"**Reasoning:** {reasoning}")
            # Pin to permanent memory
            db_pin_npc_event(guild_id, npc["npc_id"],
                f"Night {night_num}: Used {role_name} on {target_display}. {reasoning[:80]}")
        else:
            db_save_night_action(guild_id, night_num, npc["npc_id"], "_pass", None)
            await post_mod_log(guild,
                f"🤖 **NPC {npc['name']}** ({role_name}) — Night {night_num}\n"
                f"Could not identify target \"{target_name}\" — passing this night.\n"
                f"**Reasoning:** {reasoning}")

        # Also send message in their private channel
        priv_ch = guild.get_channel(npc["channel_id"] or 0)
        if priv_ch:
            async with aiohttp.ClientSession() as session:
                wh_list = await priv_ch.webhooks()
                npc_wh  = next((w for w in wh_list if w.id == npc["webhook_id"]), None)
            if npc_wh:
                msg = f"Night {night_num} action submitted. Target: {target_name}. {reasoning}"
                async with aiohttp.ClientSession() as session:
                    wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                    await wh.send(fmt(msg), username=f"{npc['name']} •", avatar_url=npc["avatar_url"])

    except Exception as e:
        db_save_night_action(guild_id, night_num, npc["npc_id"], "_pass", None)
        await post_mod_log(guild,
            f"🤖 **NPC {npc['name']}** ({role_name}) — Night {night_num}\n"
            f"⚠️ Decision failed ({e}). Passed this night.")


# ── NPC day vote decision ─────────────────────────────────────────────────
async def _npc_day_vote(guild, guild_id: int, npc: dict):
    """Have the NPC cast their day vote — mandatory, never abstains."""
    import json as _json
    game_ctx = _npc_game_context(guild, guild_id, npc)
    system   = _npc_system_prompt(npc)
    rows     = db_get_assignments(guild_id)
    npcs_all = db_get_npcs(guild_id)

    # Build accusers into context — factor into vote decision
    accusers = db_get_npc_accusers(guild_id, npc["npc_id"])
    accuser_names = []
    for acc_id, day in accusers:
        m = guild.get_member(acc_id)
        nm = m.display_name if m else str(acc_id)
        accuser_names.append(f"{nm} (Day {day})")
    accuser_text = ", ".join(accuser_names) if accuser_names else "none"

    # Build wolf teammate list so wolf NPCs never vote each other
    rows_wv     = db_get_assignments(guild_id)
    wolf_names_wv = []
    for r in rows_wv:
        if r[2] == 1 and get_team(guild_id, r[1]) == "wolf" and r[0] != npc["npc_id"]:
            nm_wv = next((n["name"] for n in npcs_all if n["npc_id"] == r[0]), None)
            if not nm_wv:
                m_wv = guild.get_member(r[0])
                nm_wv = m_wv.display_name if m_wv else str(r[0])
            wolf_names_wv.append(nm_wv)

    team_wv = npc.get("team_hint", get_team(guild_id, npc.get("role_name", "")))
    wolf_exclusion = ""
    if team_wv == "wolf" and wolf_names_wv:
        wolf_exclusion = (
            f"\nCRITICAL: Your wolf teammates are: {', '.join(wolf_names_wv)}. "
            f"You MUST NOT vote for any of them. Voting for a teammate exposes you both."
        )

    prompt = (
        f"Game state:\n{game_ctx}\n\n"
        f"Players who have falsely accused you in the past: {accuser_text}\n"
        f"{wolf_exclusion}\n\n"
        f"It is the day vote. You MUST vote for someone — abstaining is not allowed.\n"
        f"Choose one alive player to vote to eliminate.\n"
        f"Base your choice on suspicions, game state, and who has wronged you.\n"
        f"Respond with JSON only:\n"
        f"{{\"vote\": \"exact_player_name\", \"reasoning\": \"why\", "
        f"\"chat_message\": \"casual 1-2 sentence announcement of your vote\"}}"
    )

    target_id = None
    reasoning = ""
    chat_msg  = ""

    try:
        result    = await _claude(prompt, system, max_tokens=200)
        clean     = result.replace("```json","").replace("```","").strip()
        data      = _json.loads(clean)
        vote_name = data.get("vote","").lower().strip()
        reasoning = data.get("reasoning","")
        chat_msg  = data.get("chat_message","")

        for pid, rname, is_alive, _ in rows:
            if not is_alive or pid == npc["npc_id"]: continue
            m = guild.get_member(pid)
            display = m.display_name if m else str(pid)
            npc_match = next((n for n in npcs_all if n["npc_id"] == pid), None)
            check_name = npc_match["name"] if npc_match else display
            if check_name.lower() == vote_name:
                target_id = pid
                break
    except Exception as e:
        print(f"NPC vote parse error: {e}")

    # Fallback — pick top suspicion or random alive player
    if not target_id:
        suspicions = npc.get("suspicions", [])
        alive_pids = [r[0] for r in rows if r[2] == 1 and r[0] != npc["npc_id"]]
        # Try to match suspicion name to pid
        for susp_name in suspicions:
            for pid in alive_pids:
                m = guild.get_member(pid)
                nm = m.display_name if m else ""
                npc_match = next((n for n in npcs_all if n["npc_id"] == pid), None)
                check = npc_match["name"] if npc_match else nm
                if susp_name.lower() in check.lower():
                    target_id = pid
                    break
            if target_id:
                break
        if not target_id and alive_pids:
            target_id = random.choice(alive_pids)
        if not chat_msg:
            t = guild.get_member(target_id) if target_id else None
            tname = t.display_name if t else "someone"
            chat_msg = f"I'm going with {tname} this round."

    if not target_id:
        return

    day_num = db_get_night_num(guild_id)
    db_set_day_vote(guild_id, npc["npc_id"], target_id, day_num)
    db_record_vote_history(guild_id, day_num, npc["npc_id"], target_id, "vote")
    await refresh_day_vote(guild)

    t     = guild.get_member(target_id)
    tname = t.display_name if t else str(target_id)
    npcs_check = db_get_npcs(guild_id)
    npc_t = next((n for n in npcs_check if n["npc_id"] == target_id), None)
    if npc_t:
        tname = npc_t["name"]

    await post_mod_log(guild,
        f"🤖 **NPC {npc['name']}** — Day vote\n"
        f"**Voting for:** {tname}\n"
        f"**Reasoning:** {reasoning}")

    # Pin vote as permanent memory
    day_num_pin = db_get_night_num(guild_id)
    db_pin_npc_event(guild_id, npc["npc_id"],
        f"Day {day_num_pin}: I voted to eliminate {tname}. Reason: {reasoning[:80]}")

    # Announce vote in village-chat via webhook
    state  = cached_get_state(guild_id)
    vc_ch  = guild.get_channel(state.get("village_chat_ch_id") or 0)
    if vc_ch and chat_msg:
        await asyncio.sleep(random.uniform(3, 10))
        async with aiohttp.ClientSession() as session:
            wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
            await wh.send(chat_msg, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])


# ── /add_npc command ──────────────────────────────────────────────────────
@tree.command(name="add_npc", description="Add an AI-powered NPC player to the game (max 3)")
@is_mod()
async def add_npc(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    existing = db_get_npcs(interaction.guild_id)
    if len(existing) >= MAX_NPCS:
        return await interaction.followup.send(
            f"❌ Maximum of {MAX_NPCS} NPCs per game reached.", ephemeral=True)

    # ── Fetch random identity ──────────────────────────────────────────────
    first, last, nationality, avatar_url = None, None, "US", ""
    try:
        timeout_ru = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout_ru) as session:
            async with session.get("https://randomuser.me/api/?nat=us,gb,au,ca&inc=name,picture") as resp:
                data = await resp.json()
        person      = data["results"][0]
        first       = person["name"]["first"]
        last        = person["name"]["last"]
        nationality = person["nat"]
        avatar_url  = person["picture"]["large"]
    except Exception as e:
        _fallback_names = [
            ("Morgan","Ellis"),("Jordan","Hayes"),("Casey","Marsh"),
            ("Riley","Quinn"),("Avery","Stone"),("Blake","Cross"),
            ("Cameron","Drake"),("Dakota","Hunt"),("Emery","Lake"),("Finley","Ward"),
        ]
        first, last = random.choice(_fallback_names)
        print(f"[add_npc] randomuser.me unavailable ({e}), using fallback name: {first} {last}")
    npc_name = f"{first} {last}"

    # ── Generate personality + backstory ───────────────────────────────────
    try:
        personality, backstory = await asyncio.wait_for(
            _generate_npc_profile(first, last, nationality), timeout=20)
    except (asyncio.TimeoutError, Exception) as e:
        personality = "Observant and strategic, rarely speaks without purpose."
        backstory   = "A regular player who takes the game seriously."
        print(f"[add_npc] Profile generation failed ({e}), using defaults")

    # ── Assign a role from THIS GAME'S pool only ──────────────────────────
    rows       = db_get_assignments(interaction.guild_id)
    used_roles = [r[1] for r in rows]
    # Rebuild game pool from last_role_set (the roster used to start this game)
    last_roles = db_get_last_roles(interaction.guild_id)  # {role_name: count}
    if last_roles:
        game_pool = []
        for role_name, count in last_roles.items():
            game_pool.extend([role_name] * count)
        available_names = [r for r in game_pool if r not in used_roles]
        if not available_names:
            available_names = list(last_roles.keys())
        chosen_role = random.choice(available_names)
    else:
        # Fallback if no last_role_set saved
        all_roles   = cached_load_roles(interaction.guild_id)
        available   = [r for r in all_roles if r["name"] not in used_roles] or all_roles
        chosen_role = random.choice(available)["name"]

    # ── Create private channel ─────────────────────────────────────────────
    state    = cached_get_state(interaction.guild_id)
    cat      = interaction.guild.get_channel(state.get("category_id") or 0)
    everyone = interaction.guild.default_role
    bot_me   = interaction.guild.me
    mod_role = interaction.guild.get_role(state.get("mod_role_id") or 0)
    bot_ow   = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)

    priv_ch_name = f"🔒{npc_name}-{chosen_role}".lower().replace(" ", "-")
    ch_ow = {
        everyone: discord.PermissionOverwrite(view_channel=False),
        bot_me:   bot_ow,
    }
    if mod_role:
        ch_ow[mod_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
    priv_ch = await cat.create_text_channel(priv_ch_name, overwrites=ch_ow)

    # ── Create webhook in village-chat ─────────────────────────────────────
    village_ch = interaction.guild.get_channel(state.get("village_chat_ch_id") or 0)
    if not village_ch:
        await priv_ch.delete()
        return await interaction.followup.send(
            "❌ Village-chat channel not found. Make sure the game was started with the latest bot version.",
            ephemeral=True)

    try:
        wh = await village_ch.create_webhook(name=f"{npc_name} •")
    except Exception as e:
        await priv_ch.delete()
        return await interaction.followup.send(f"❌ Failed to create webhook: {e}", ephemeral=True)

    # ── Use a fake unique ID for the NPC (negative to avoid Discord ID clash) ──
    npc_id = -(len(existing) + 1) * 1000 - interaction.guild_id % 1000

    # ── Save to DB ─────────────────────────────────────────────────────────
    npc_team      = get_team(interaction.guild_id, chosen_role)
    npc_bluff_role = ""
    if npc_team == "wolf":
        # Pick a random village role as bluff
        all_roles_bl = cached_load_roles(interaction.guild_id)
        village_roles = [r["name"] for r in all_roles_bl if r.get("team") == "village"
                         and r["name"] not in ("Villager", "Drunk", "Village Idiot")]
        npc_bluff_role = random.choice(village_roles) if village_roles else "Villager"
    db_save_npc(interaction.guild_id, npc_id, npc_name, avatar_url,
                personality, backstory, chosen_role, priv_ch.id, wh.id, wh.token,
                bluff_role=npc_bluff_role, team_hint=npc_team)

    # ── Save to player_assignments so NPC participates in votes/actions ────
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO player_assignments VALUES (?,?,?,1,?)",
              (interaction.guild_id, npc_id, chosen_role, priv_ch.id))
    conn.commit()
    conn.close()

    # ── If wolf NPC, give access to wolf-den and wolf-vote ────────────────
    if get_team(interaction.guild_id, chosen_role) == "wolf":
        wolf_ch      = interaction.guild.get_channel(state.get("wolf_channel_id") or 0)
        if wolf_ch:
            try:
                wolf_wh = await wolf_ch.create_webhook(name=f"{npc_name} •")
                await wolf_ch.send(fmt(f"🐺 {npc_name} has joined the pack."))
            except Exception:
                pass

    # ── Send role card to private channel — bot sends it so it renders properly ──
    role_info  = get_role_info(interaction.guild_id, chosen_role)
    font       = get_guild_font(interaction.guild_id)
    embed      = build_role_card(interaction.guild.me, chosen_role, role_info, font)
    embed.set_footer(text=f"🤖 NPC  ·  {npc_name}  ·  This channel is private — only mods can see it.")
    embed.set_author(name=f"{npc_name} •", icon_url=avatar_url)
    await priv_ch.send(embed=embed)

    # ── Introduce NPC in village-chat ──────────────────────────────────────
    await asyncio.sleep(random.uniform(3, 8))
    intros = [
        "Hey everyone, just got in. Ready to play!",
        "Finally here. Let's do this.",
        "Hi all! First time with this group — excited.",
        "Just joined. Looks like a good group.",
        "Hey! Glad to be here, let's figure out who the wolves are.",
    ]
    async with aiohttp.ClientSession() as session:
        wh_intro = discord.Webhook.partial(wh.id, wh.token, session=session)
        await wh_intro.send(random.choice(intros), username=f"{npc_name} •", avatar_url=avatar_url)

    await refresh_player_list(interaction.guild)
    # Start proactive chat loop for this NPC
    safe_task(_npc_proactive_loop(interaction.guild, interaction.guild_id), "npc_proactive")
    safe_task(_npc_idle_check_loop(interaction.guild, interaction.guild_id), "npc_idle")
    tell_idx  = abs(npc_id) % len(NPC_TELLS)
    tell_desc = NPC_TELLS[tell_idx]["tell"]
    await post_mod_log(interaction.guild,
        f"🤖 **NPC Added** — {npc_name}\n"
        f"**Role:** {chosen_role}\n"
        f"**Personality:** {personality}\n"
        f"**Backstory:** {backstory}\n"
        f"**🎭 Tell (mod only):** {tell_desc}")

    await interaction.followup.send(
        f"✅ NPC **{npc_name}** added as **{chosen_role}**.\n"
        f"They are now active in village-chat and their private channel.",
        ephemeral=True)


# ── /remove_npc command ───────────────────────────────────────────────────
@tree.command(name="remove_npc", description="Remove an NPC from the current game")
@is_mod()
@app_commands.describe(name="Name of the NPC to remove")
async def remove_npc(interaction: discord.Interaction, name: str):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    npcs = db_get_npcs(interaction.guild_id)
    npc  = next((n for n in npcs if n["name"].lower() == name.lower()), None)
    if not npc:
        return await interaction.followup.send(f"❌ No NPC named \"{name}\" found.", ephemeral=True)

    # Delete webhook
    try:
        state      = cached_get_state(interaction.guild_id)
        village_ch = interaction.guild.get_channel(state.get("village_chat_ch_id") or 0)
        if village_ch:
            wh_list = await village_ch.webhooks()
            for wh in wh_list:
                if wh.id == npc["webhook_id"]:
                    await wh.delete()
                    break
    except Exception:
        pass

    # Delete private channel
    priv_ch = interaction.guild.get_channel(npc["channel_id"] or 0)
    if priv_ch:
        try: await priv_ch.delete()
        except: pass

    # Remove from DB
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM npcs WHERE guild_id=? AND npc_id=?", (interaction.guild_id, npc["npc_id"]))
    c.execute("DELETE FROM player_assignments WHERE guild_id=? AND player_id=?",
              (interaction.guild_id, npc["npc_id"]))
    conn.commit()
    conn.close()

    _npc_webhooks.pop((interaction.guild_id, npc["npc_id"]), None)
    await refresh_player_list(interaction.guild)
    await interaction.followup.send(f"✅ NPC **{npc['name']}** removed.", ephemeral=True)


# ── Minimal role card for NPC (no @mention) ───────────────────────────────
def build_role_card_npc(npc_name: str, role_name: str, role_info: dict, font_style: str = "default") -> discord.Embed:
    team = role_info.get("team", "village")
    if team == "wolf":
        color      = 0xC0392B
        team_label = "🐺 WOLF PACK"
    elif team == "neutral":
        color      = 0xF39C12
        team_label = "⚖️ NEUTRAL"
    else:
        color      = 0x27AE60
        team_label = "🏘️ VILLAGE"
    styled_name = apply_font(role_name.upper(), font_style)
    embed = discord.Embed(title=f"🎭  {styled_name}", color=color)
    embed.add_field(name="📜 Your Ability",
                    value=role_info.get("description") or "*No description provided.*", inline=False)
    embed.set_footer(text=f"{team_label}  ·  {npc_name}  ·  Private channel")
    return embed


# ====================== NPC ENHANCEMENTS ======================

# ── NPC tells — hardcoded per-slot, assigned when NPC is created ──────────
NPC_TELLS = [
    {
        "tell": "Uses ellipses (...) noticeably more when lying or deflecting.",
        "instruction": "When hiding something or deflecting an accusation, use ellipses (...) naturally."
    },
    {
        "tell": "Asks a question back instead of answering directly when cornered.",
        "instruction": "When accused or pressed, respond with a question directed at the accuser before answering."
    },
    {
        "tell": "Goes unusually quiet (very short replies) right after a kill they knew about.",
        "instruction": "The morning after a kill, keep your first message very short — one or two words only."
    },
    {
        "tell": "Over-explains their innocence when they haven't been accused yet.",
        "instruction": "Occasionally volunteer reasons why you couldn't possibly be a wolf, even when nobody asked."
    },
    {
        "tell": "Subtly changes the subject when someone gets close to the truth.",
        "instruction": "When a conversation gets close to exposing you or your allies, redirect to a different topic or player."
    },
    {
        "tell": "Uses 'we' language when talking about the village — even as a wolf.",
        "instruction": "Frequently say 'we should' or 'we need to' when discussing the village's strategy, as if you are one of them."
    },
    {
        "tell": "Agrees with the loudest voice in the room, rarely takes an unpopular stance.",
        "instruction": "Often agree with whoever seems most confident in the conversation, avoiding strong independent positions."
    },
    {
        "tell": "Gets noticeably more active in chat right after someone they wanted eliminated gets voted out.",
        "instruction": "After a player you suspected is eliminated, become more talkative and energetic in your next few messages."
    },
    {
        "tell": "Mirrors the speaking style of whoever they are talking to.",
        "instruction": "Subtly match the tone and vocabulary of the person you are replying to."
    },
    {
        "tell": "Makes very specific observations about other players' behavior but is vague about their own.",
        "instruction": "Be precise and detailed when describing what others are doing, but vague and general when describing your own reasoning."
    },
    {
        "tell": "Uses humor to deflect when feeling threatened.",
        "instruction": "When you feel accused or cornered, make a light joke or self-deprecating comment before addressing the accusation."
    },
    {
        "tell": "Always has an alibi ready — never says 'I don't know' about their own actions.",
        "instruction": "Always have a specific, prepared-sounding explanation for your own behavior, even for things nobody asked about."
    },
    {
        "tell": "Hesitates before naming their own vote target — takes longer than others to commit.",
        "instruction": "When announcing your vote, hedge slightly first ('I've been going back and forth but...') before committing."
    },
    {
        "tell": "Brings up dead players' behavior more than necessary.",
        "instruction": "Occasionally reference the behavior or words of already-eliminated players to justify your current suspicions."
    },
    {
        "tell": "Becomes slightly more formal in language when nervous.",
        "instruction": "When you feel the conversation is threatening you, use slightly more formal or careful language than usual."
    },
]

# ── Proactive NPC chat — fires on a timer during day phase ────────────────
# Track last time each NPC spoke — for priority weighting
_npc_last_spoke: dict = {}  # (guild_id, npc_id) -> timestamp

async def _npc_proactive_loop(guild, guild_id: int):
    """Run throughout the game — NPCs post proactively every 45-90 mins during day.
    Quietest NPC gets priority. Tone scales with game urgency."""
    import time as _tp
    while game_active(guild_id):
        await asyncio.sleep(random.uniform(2700, 5400))  # 45-90 min
        state = cached_get_state(guild_id)
        if state.get("phase") != "day":
            continue
        vc_ch = guild.get_channel(state.get("village_chat_ch_id") or 0)
        if not vc_ch:
            continue
        npcs       = db_get_npcs(guild_id)
        alive_npcs = [n for n in npcs if n["is_alive"]]
        if not alive_npcs:
            break

        # Pick the NPC who has been quietest — most time since last spoke
        now = int(_tp.time())
        npc = max(alive_npcs,
                  key=lambda n: now - _npc_last_spoke.get((guild_id, n["npc_id"]), 0))

        game_ctx   = _npc_game_context(guild, guild_id, npc)
        system     = _npc_system_prompt(npc)
        night_num  = db_get_night_num(guild_id)
        team       = npc.get("team_hint", get_team(guild_id, npc.get("role_name", "")))
        suspicions = npc.get("suspicions", [])
        votes      = db_get_vote_history(guild_id)
        npcs_all   = db_get_npcs(guild_id)

        def get_dname(pid):
            nm = next((n for n in npcs_all if n["npc_id"] == pid), None)
            if nm: return nm["name"]
            m2 = guild.get_member(pid)
            return m2.display_name if m2 else str(pid)

        voted_against = [get_dname(v[1]) for v in votes if v[2] == npc["npc_id"]][-3:]
        voted_against_str = ", ".join(voted_against) if voted_against else "nobody recently"

        vote_flip, prev_v = {}, {}
        for _vvr in votes:
            day, voter, target, action = _vvr[0], _vvr[1], _vvr[2], _vvr[3]
            if action == "vote" and target:
                if voter in prev_v and prev_v[voter] != target:
                    vote_flip[voter] = vote_flip.get(voter, 0) + 1
                prev_v[voter] = target
        top_flip    = sorted(vote_flip.items(), key=lambda x: -x[1])[:2]
        erratic_str = ", ".join(get_dname(pid) for pid, _ in top_flip) or "none"

        # Late-game urgency scaling — more direct after Night 3
        if night_num >= 5:
            urgency = "The game is in its final stages. Be direct, urgent, and specific."
        elif night_num >= 3:
            urgency = "The game is heating up. More aggressive and pointed than early game."
        else:
            urgency = "Early game — build rapport, plant seeds, stay subtle."

        strat_note = (
            f"You are a wolf. Deflect toward village players. Never implicate fellow wolves."
            if team == "wolf" else
            f"You are a villager. Target suspicious behavior and erratic voters."
        )

        # 40% chance to ask a targeted question instead of making a statement
        action_type = "question" if suspicions and random.random() < 0.40 else "statement"
        target_name = suspicions[0] if suspicions else "someone"

        if action_type == "question":
            action_instruction = (
                f"Ask **{target_name}** a direct, pointed question about their behavior or vote. "
                f"1 sentence only. Make them have to answer."
            )
            # Register pending question so we can follow up when target answers
            _npc_pending_questions[(guild_id, npc["npc_id"])] = {
                "target": target_name, "question": action_instruction}
        else:
            action_instruction = (
                f"Make a statement. Choose one:\n"
                f"1. Plant suspicion toward a specific player\n"
                f"2. React to recent game events strategically\n"
                f"3. Build your credibility with the village"
            )

        # Cross-NPC disagreement — react to what another NPC said recently
        other_npcs = [n for n in alive_npcs if n["npc_id"] != npc["npc_id"]]
        cross_note = ""
        if other_npcs and random.random() < 0.25:
            other = random.choice(other_npcs)
            other_history = other.get("chat_history", [])
            if other_history:
                last_msg = other_history[-1].get("text", "")
                cross_note = (
                    f"\n{other['name']} recently said: \"{last_msg[:100]}\"\n"
                    f"You may agree, push back, or build on this — or ignore it entirely."
                )

        prompt = (
            f"Game state:\n{game_ctx}\n\n"
            f"Your suspicions: {suspicions or 'none'}\n"
            f"Players who voted against you: {voted_against_str}\n"
            f"Most erratic voters: {erratic_str}\n"
            f"{cross_note}\n"
            f"{urgency}\n"
            f"{strat_note}\n\n"
            f"{action_instruction}\n"
            f"Keep it 1-2 sentences. Natural and human. Never sound like an AI."
        )
        try:
            response = await _claude(prompt, system, max_tokens=100)
            if response:
                await asyncio.sleep(random.uniform(5, 20))
                await _npc_send(npc, vc_ch, response, guild_id)
        except Exception as e:
            print(f"NPC proactive error: {e}")


# ── NPC death reaction ────────────────────────────────────────────────────

async def _npc_send(npc: dict, channel: discord.TextChannel, text: str,
                    guild_id: int = 0, update_history: bool = True):
    """Send an NPC webhook message with typing indicator. Updates last_spoke and history."""
    import time as _ts
    try:
        async with channel.typing():
            await asyncio.sleep(random.uniform(1.5, 3.5))
        async with aiohttp.ClientSession() as session:
            wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
            await wh.send(text, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
        if guild_id:
            _npc_last_spoke[(guild_id, npc["npc_id"])] = int(_ts.time())
        if update_history:
            history = npc.get("chat_history", [])
            history.append({"author": npc["name"], "text": text})
            if guild_id:
                db_update_npc_chat_history(guild_id, npc["npc_id"], history)
    except Exception as e:
        print(f"[_npc_send:{npc.get('name','')}] {e}")

async def _npc_react_to_death(guild, guild_id: int, dead_name: str):
    """Have alive NPCs react to a player death in village-chat."""
    if not game_active(guild_id):
        return
    state  = cached_get_state(guild_id)
    vc_ch  = guild.get_channel(state.get("village_chat_ch_id") or 0)
    if not vc_ch:
        return
    npcs       = db_get_npcs(guild_id)
    alive_npcs = [n for n in npcs if n["is_alive"]]
    for npc in alive_npcs:
        if random.random() > 0.6:  # 60% chance each NPC reacts
            continue
        game_ctx = _npc_game_context(guild, guild_id, npc)
        system   = _npc_system_prompt(npc)
        prompt   = (
            f"Game state:\n{game_ctx}\n\n"
            f"It was just announced that {dead_name} has been eliminated.\n"
            f"React naturally in 1-2 sentences. Could be surprise, suspicion, sadness, or reading into it."
        )
        try:
            await asyncio.sleep(random.uniform(10, 30))
            response = await _claude(prompt, system, max_tokens=80)
            if response:
                await _npc_send(npc, vc_ch, response, guild_id)
        except Exception as e:
            print(f"NPC death react error: {e}")


# ── NPC wolf den coordination ─────────────────────────────────────────────





_npc_pending_questions: dict = {}  # (guild_id, npc_id) -> {"target": name, "question": text}

async def _npc_followup(guild, guild_id: int, npc: dict,
                         responder_name: str, response_text: str, channel: discord.TextChannel):
    """NPC follows up once when someone answers their question."""
    if not game_active(guild_id):
        return
    await asyncio.sleep(random.uniform(20, 60))
    game_ctx = _npc_game_context(guild, guild_id, npc)
    system   = _npc_system_prompt(npc)
    pending  = _npc_pending_questions.get((guild_id, npc["npc_id"]), {})
    orig_q   = pending.get("question", "your earlier question")

    prompt = (
        f"Game state:\n{game_ctx}\n\n"
        f"You asked: \"{orig_q[:100]}\"\n"
        f"{responder_name} just answered: \"{response_text[:200]}\"\n\n"
        f"React to their answer in 1-2 sentences. Were you satisfied? Suspicious of the answer? "
        f"Press them further or accept it. Natural, human. No AI tone."
    )
    try:
        response = await _claude(prompt, system, max_tokens=80)
        if response:
            await _npc_send(npc, channel, response, guild_id)
    except Exception as e:
        print(f"[npc_followup] {e}")
    finally:
        _npc_pending_questions.pop((guild_id, npc["npc_id"]), None)

async def _npc_react_to_defense(guild, guild_id, npc, defender_name, message_text):
    """NPC reacts when someone defends or vouches for them in village chat."""
    if not game_active(guild_id):
        return
    await asyncio.sleep(random.uniform(8, 25))
    team    = npc.get("team_hint", get_team(guild_id, npc.get("role_name", "")))
    system  = _npc_system_prompt(npc)
    prompt  = (
        f"You are {npc['name']} in a Mafia game on the {team} team.\n"
        f"{defender_name} just said something that defends or vouches for you: \"{message_text}\"\n\n"
        f"Respond naturally — 1-2 sentences.\n"
        f"{'If you are a wolf, use this as an opportunity to build trust with your defender while staying strategic.' if team == 'wolf' else 'Express genuine appreciation but stay focused on finding the wolves.'}"
    )
    try:
        response = await _claude(prompt, system, max_tokens=80)
        if response:
            state_d = cached_get_state(guild_id)
            vc_ch_d = guild.get_channel(state_d.get("village_chat_ch_id") or 0)
            if vc_ch_d:
                async with aiohttp.ClientSession() as session:
                    wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                    await wh.send(response, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
            # Add defender to ally list
            import json as _jd
            cur_allies = _jd.loads(npc.get("allies", "[]") or "[]")
            if defender_name not in cur_allies:
                cur_allies = (cur_allies + [defender_name])[-5:]
                conn_al = sqlite3.connect(DB_FILE)
                c_al    = conn_al.cursor()
                c_al.execute("UPDATE npcs SET allies=? WHERE guild_id=? AND npc_id=?",
                             (_jd.dumps(cur_allies), guild_id, npc["npc_id"]))
                conn_al.commit()
                conn_al.close()
    except Exception as e:
        print(f"[npc_defense_react] {e}")


async def _npc_react_to_agreement(guild, guild_id, npc, agreer_name, message_text):
    """NPC reacts when someone agrees with or reinforces their position."""
    if not game_active(guild_id):
        return
    await asyncio.sleep(random.uniform(10, 30))
    team   = npc.get("team_hint", "village")
    system = _npc_system_prompt(npc)
    prompt = (
        f"You are {npc['name']} in a Mafia game on the {team} team.\n"
        f"{agreer_name} just agreed with you or backed up your position: \"{message_text[:200]}\"\n\n"
        f"Respond briefly — 1 sentence. "
        f"{'Build on the alliance strategically.' if team == 'wolf' else 'Acknowledge their support and reinforce the point.'}"
    )
    try:
        response = await _claude(prompt, system, max_tokens=60)
        if response:
            state_ag = cached_get_state(guild_id)
            vc_ag    = guild.get_channel(state_ag.get("village_chat_ch_id") or 0)
            if vc_ag:
                await _npc_send(npc, vc_ag, response, guild_id)
            db_update_npc_relationship(guild_id, npc["npc_id"], agreer_name, "ally")
    except Exception as e:
        print(f"[npc_agree_react] {e}")

async def _npc_farewell_message(guild, guild_id, npc, channel, prompt, system):
    """NPC posts one final message in village chat after being eliminated."""
    try:
        await asyncio.sleep(random.uniform(5, 15))
        response = await _claude(prompt, system, max_tokens=80)
        if response:
            async with aiohttp.ClientSession() as session:
                wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                await wh.send(response, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
    except Exception as e:
        print(f"[npc_farewell] {e}")

async def _npc_react_to_vote_against(guild, guild_id: int, npc: dict, voter_name: str):
    """NPC reacts in village chat when someone votes against them."""
    if not game_active(guild_id):
        return
    await asyncio.sleep(random.uniform(10, 30))  # Natural delay
    game_ctx = _npc_game_context(guild, guild_id, npc)
    system   = _npc_system_prompt(npc)
    prompt   = (
        f"Game state:\n{game_ctx}\n\n"
        f"{voter_name} just voted to eliminate YOU in the day vote.\n"
        f"React naturally — 1-2 sentences. Could be: push back, question their motives, "
        f"stay calm, or subtly redirect suspicion. Stay in character."
    )
    try:
        response = await _claude(prompt, system, max_tokens=80)
        if response:
            state   = cached_get_state(guild_id)
            vc_ch   = guild.get_channel(state.get("village_chat_ch_id") or 0)
            if vc_ch:
                async with aiohttp.ClientSession() as session:
                    wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                    await wh.send(response, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
            # Update suspicions and enemies — voter is now a threat
            suspicions = npc.get("suspicions", [])
            entry = f"{voter_name} (voted against me)"
            if entry not in suspicions:
                suspicions = [entry] + suspicions
                db_update_npc_suspicions(guild_id, npc["npc_id"], suspicions[:6])
            db_update_npc_relationship(guild_id, npc["npc_id"], voter_name, "enemy")
    except Exception as e:
        print(f"[npc_vote_react] {e}")

async def _npc_wolf_coordinate(guild, guild_id: int, night_num: int):
    """Wolf NPCs share suspicion targets and agree on a coordinated deflection strategy."""
    import json as _json
    npcs      = db_get_npcs(guild_id)
    wolf_npcs = [n for n in npcs if n["is_alive"] and
                 get_team(guild_id, n["role_name"]) == "wolf"]
    if len(wolf_npcs) < 2:
        return  # Only useful with 2+ wolf NPCs

    # Collect all wolf NPC suspicions
    all_suspicions = {}
    for npc in wolf_npcs:
        for s in npc.get("suspicions", []):
            all_suspicions[s] = all_suspicions.get(s, 0) + 1

    # Top shared targets
    top_targets = sorted(all_suspicions.items(), key=lambda x: -x[1])[:3]
    shared_text = ", ".join(f"{n} ({c} wolves suspect)" for n, c in top_targets) or "none yet"

    # Post coordination message in wolf den
    state   = cached_get_state(guild_id)
    wolf_ch = guild.get_channel(state.get("wolf_channel_id") or 0)
    if not wolf_ch:
        return

    for npc in wolf_npcs:
        game_ctx = _npc_game_context(guild, guild_id, npc)
        system   = _npc_system_prompt(npc)
        prompt   = (
            f"Game state:\n{game_ctx}\n\n"
            f"You are in the wolf den with your pack. Night {night_num}.\n"
            f"Shared pack suspicions (players multiple wolves suspect): {shared_text}\n\n"
            f"Coordinate with your pack:\n"
            f"1. Confirm or adjust the kill target\n"
            f"2. Agree on who to deflect suspicion toward tomorrow\n"
            f"3. Plan your public story to stay in sync\n"
            f"Keep it 2-3 sentences. Strategic, not verbose."
        )
        try:
            await asyncio.sleep(random.uniform(3, 8))
            response = await _claude(prompt, system, max_tokens=120)
            if response:
                async with aiohttp.ClientSession() as session:
                    wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                    await wh.send(response, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
                # Pin agreed deflection target for public story sync
                if top_targets:
                    agreed_target = top_targets[0][0]
                    db_pin_npc_event(guild_id, npc["npc_id"],
                        f"Night {night_num} den agreement: publicly push suspicion toward {agreed_target} tomorrow.")
        except Exception as e:
            print(f"[wolf_coordinate] {e}")

async def _npc_wolf_den_message(guild, guild_id: int, npc: dict, night_num: int, trigger_msg: str = ""):
    """Wolf NPC posts in wolf-den to coordinate with pack."""
    state    = cached_get_state(guild_id)
    wolf_ch  = guild.get_channel(state.get("wolf_channel_id") or 0)
    if not wolf_ch:
        return
    game_ctx = _npc_game_context(guild, guild_id, npc)
    system   = _npc_system_prompt(npc)
    prompt   = (
        f"Game state:\n{game_ctx}\n\n"
        f"You are in the secret wolf den with your pack. Night {night_num}.\n"
        f"{'A pack member just said: ' + trigger_msg if trigger_msg else 'Night is starting — coordinate with the pack.'}\n"
        f"Discuss who to target tonight. Be strategic. 1-3 sentences."
    )
    try:
        await asyncio.sleep(random.uniform(8, 20))
        response = await _claude(prompt, system, max_tokens=120)
        if response:
            async with aiohttp.ClientSession() as session:
                wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                await wh.send(response, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
    except Exception as e:
        print(f"NPC wolf den error: {e}")


# ====================== BOT STATUS ======================

async def set_bot_status(status_text: str, activity_type=discord.ActivityType.watching):
    """Update the bot's Discord presence."""
    activity = discord.Activity(type=activity_type, name=status_text)
    await client.change_presence(activity=activity)



def _build_vote_summary(guild, guild_id) -> str:
    """Build a text summary of current day vote standings for mod prompts."""
    votes    = db_get_day_votes(guild_id)
    rows     = db_get_assignments(guild_id)
    npcs     = db_get_npcs(guild_id)
    pid_to_name = {}
    for pid, _, _, _ in rows:
        m = guild.get_member(pid)
        npc_match = next((n for n in npcs if n["npc_id"] == pid), None)
        pid_to_name[pid] = npc_match["name"] if npc_match else (m.display_name if m else str(pid))

    if not votes:
        return "No votes cast."

    tally = {}
    abstains = 0
    for voter_id, target_id, *_ in votes:
        if target_id is None:
            abstains += 1
        else:
            tally[target_id] = tally.get(target_id, 0) + 1

    if not tally:
        return f"All {abstains} vote(s) were abstentions."

    sorted_targets = sorted(tally.items(), key=lambda x: x[1], reverse=True)
    top_count      = sorted_targets[0][1]
    tied           = [t for t, c in sorted_targets if c == top_count]

    lines = []
    for target_id, count in sorted_targets:
        name   = pid_to_name.get(target_id, str(target_id))
        leader = "🔴 " if count == top_count else ""
        lines.append(f"{leader}**{name}** — {count} vote(s)")
    if abstains:
        lines.append(f"🤐 Abstaining — {abstains}")

    if len(tied) > 1:
        tied_names = ", ".join(pid_to_name.get(t, str(t)) for t in tied)
        lines.append(f"\n⚠️ **TIE** between: {tied_names}")

    return "\n".join(lines)

# ====================== PHASE PROMPT VIEWS ======================

class StartNightPromptView(View):
    """Posted to mod after game launches — one click to begin Night 1."""
    def __init__(self, guild_id):
        super().__init__(timeout=3600)
        self.guild_id = guild_id
        btn = Button(label="🌙 Yes — Start Night 1", style=discord.ButtonStyle.green)
        btn.callback = self.on_start
        self.add_item(btn)

    async def on_start(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="🌙 Starting Night 1...", embed=None, view=None)
        guild_id  = self.guild_id
        state     = cached_get_state(guild_id)
        db_set_state(guild_id, phase="night")
        night_num = db_get_night_num(guild_id)
        duration  = state.get("night_duration", 43200)
        await _run_start_night(interaction.guild, guild_id, night_num, duration, state)


class ResolveNightPromptView(View):
    """Posted to mod-log when night timer expires."""
    def __init__(self, guild_id, night_num):
        super().__init__(timeout=7200)
        self.guild_id  = guild_id
        self.night_num = night_num
        btn = Button(label="⏩ Resolve Night Actions", style=discord.ButtonStyle.green)
        btn.callback = self.on_resolve
        self.add_item(btn)

    async def on_resolve(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content=f"⏩ Resolving Night {self.night_num}...", embed=None, view=None)
        await resolve_night(interaction.guild, self.night_num)


class StartDayPromptView(View):
    """Posted to mod-log after night resolves — one click to start day and open vote."""
    def __init__(self, guild_id, night_num, deaths: list = None):
        super().__init__(timeout=7200)
        self.guild_id  = guild_id
        self.night_num = night_num
        self.deaths    = deaths or []

        # Check if frenzy was triggered this night
        actions        = db_get_night_actions(guild_id, night_num) if night_num > 0 else []
        self.frenzy    = any(a[1] == "agitator_frenzy" for a in actions)

        label = "⚡ Start Day + Open Frenzy Vote" if self.frenzy else "☀️ Start Day + Open Day Vote"
        color = discord.ButtonStyle.danger if self.frenzy else discord.ButtonStyle.green
        btn   = Button(label=label, style=color)
        btn.callback = self.on_start_day
        self.add_item(btn)

    async def on_start_day(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content=f"☀️ Starting Day {self.night_num}...", embed=None, view=None)

        guild_id  = self.guild_id
        night_num = self.night_num
        guild     = interaction.guild

        # Set phase to day and invalidate cache first
        db_set_state(guild_id, phase="day")
        invalidate_cache(guild_id)
        # Re-read state fresh so Time Lord changes are reflected
        state = cached_get_state(guild_id) or {}

        # Post day transition embed in village-chat
        await post_day_transition(guild, night_num,
                                  duration_secs=state.get("day_duration") or 43200,
                                  deaths=self.deaths)

        # Auto-open day vote
        # For frenzy — anonymous=False so votes are visible
        rows     = db_get_assignments(guild_id)
        alive    = [r for r in rows if r[2] == 1]
        day_num  = night_num
        _sdp_dur = int(state.get("day_duration") or 43200)
        end_ts   = _phase_end_ts(_sdp_dur, 20, guild_id)

        if self.frenzy:
            # Frenzy — need two votes, post extra warning
            anon = state.get("anon_vote", 0)
            view = DayVoteView(guild_id, anonymous=bool(anon))
            vc_ch = guild.get_channel(state.get("village_chat_ch_id") or 0)
            if vc_ch:
                embed = discord.Embed(
                    title       = f"⚡ FRENZY VOTE — Day {day_num}",
                    description = (
                        "**TWO players must be eliminated today.**\n"
                        "Cast your first vote now. A second vote will open after the first elimination.\n\n"

                        f"**Vote closes:** <t:{end_ts}:R>"
                    ),
                    color = 0xE74C3C
                )
                embed.set_footer(text="All players must vote. Missing a vote results in immediate elimination.")
                await vc_ch.send(embed=embed, view=view)
                db_set_state(guild_id, day_vote_end_time=end_ts, agitator_frenzy_day=day_num, agitator_elim_count=0)
        else:
            # Normal day vote
            anon  = state.get("anon_vote", 0)
            view  = DayVoteView(guild_id, anonymous=bool(anon))
            vc_ch = guild.get_channel(state.get("village_chat_ch_id") or 0)
            if vc_ch:
                embed = discord.Embed(
                    title       = f"🗳️ Day {day_num} Vote",
                    description = (
                        f"Cast your vote to eliminate a player.\n\n"
                        f"**Vote closes:** <t:{end_ts}:R>"
                    ),
                    color = 0xE67E22
                )
                embed.set_footer(text="All players must vote. Missing a vote results in immediate elimination.")
                await vc_ch.send(embed=embed, view=view)
                db_set_state(guild_id, day_vote_end_time=end_ts)

        await log_event(guild, f"Day {day_num}", f"☀️ Day {day_num} started — vote opened")
        await post_mod_log(guild,
            f"☀️ **Day {day_num} started.** "
            f"{'⚡ FRENZY — two eliminations required.' if self.frenzy else 'Day vote opened in village chat.'}")
        # Reset day board flag and refresh dashboard
        db_set_state(guild_id, day_bb_done=0)
        invalidate_cache(guild_id)
        safe_task(update_mod_dashboard(guild), "dashboard_start_day")
        # Send Governor and Hermit their day ability buttons
        safe_task(_send_day_ability_buttons(guild, guild_id, end_ts), "day_ability_buttons")
        # Vote countdown reminder and night approach warning
        safe_task(_vote_countdown_reminder(guild, guild_id, int(end_ts)), "vote_reminder")
        safe_task(_night_approach_warning(guild, guild_id), "night_warning")


class DayVoteClosedPromptView(View):
    """Posted to mod-log when day vote timer expires — shows standings."""
    def __init__(self, guild_id, vote_summary: str):
        super().__init__(timeout=7200)
        self.guild_id     = guild_id
        self.vote_summary = vote_summary
        btn = Button(label="🌙 Start Night Phase", style=discord.ButtonStyle.blurple)
        btn.callback = self.on_next
        self.add_item(btn)

    async def on_next(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="🌙 Starting night phase...", embed=None, view=None)
        state     = cached_get_state(self.guild_id)
        db_set_state(self.guild_id, phase="night")
        if state.get("phase") == "day":
            db_increment_night(self.guild_id)
        invalidate_cache(self.guild_id)
        fresh_state = cached_get_state(self.guild_id)
        night_num   = db_get_night_num(self.guild_id)
        duration    = int(fresh_state.get("night_duration") or 43200)
        await _run_start_night(interaction.guild, self.guild_id, night_num, duration, fresh_state)


async def _run_start_night(guild, guild_id, night_num, duration, state):
    """Core night start logic — shared by start_night command, prompts, and next_phase."""
    mins = duration // 60
    rows          = db_get_assignments(guild_id)
    # Build alive_players including NPCs as proxy objects
    class _NPCProxy:
        def __init__(self, npc_id, name): self.id = npc_id; self.display_name = name
    npcs_alive_map = {n["npc_id"]: n["name"] for n in db_get_npcs(guild_id) if n["is_alive"]}
    alive_players  = []
    for r in rows:
        if r[2] != 1: continue
        m = guild.get_member(r[0])
        if m:
            alive_players.append(m)
        elif r[0] in npcs_alive_map:
            alive_players.append(_NPCProxy(r[0], npcs_alive_map[r[0]]))
    wolf_ids = [r[0] for r in rows if r[2] == 1 and get_team(guild_id, r[1]) == "wolf"]

    # Check if this night is silenced by Werekitten elimination
    wk_silenced = int((state or {}).get("werekitten_silenced_night") or 0)
    if wk_silenced and wk_silenced == night_num:
        vc_wk = guild.get_channel(state.get("village_chat_ch_id") or 0)
        if vc_wk:
            await vc_wk.send(fmt(
                "🐱💔 **Night actions are silenced tonight.**\n"
                "*The loss of the Werekitten weighs too heavily on the village.*\n"
                "*No one can bring themselves to act.*"))
        await post_mod_log(guild,
            f"🐱 Night {night_num} silenced — Werekitten was eliminated. No night action buttons sent.")
        return  # Skip all night button sending

    for row in rows:
        pid, role_name, is_alive, ch_id = row
        if not is_alive or not ch_id: continue
        ch = guild.get_channel(ch_id)
        if not ch: continue
        member     = guild.get_member(pid)
        # Clear old night messages so stale buttons from previous nights can't be clicked
        try:
            async for msg in ch.history(limit=10):
                if msg.author.id == guild.me.id and msg.components:
                    await msg.edit(view=None)
        except Exception:
            pass
        night_view = get_night_view(guild_id, pid, role_name, alive_players)

        # Elite Alpha — check if blocked by 2-nights-apart rule and explain why
        if night_view is None and role_name == "Elite Alpha":
            conn_ea = sqlite3.connect(DB_FILE)
            c_ea    = conn_ea.cursor()
            c_ea.execute(
                "SELECT night_num FROM turn_log WHERE guild_id=? AND actor_id=? AND result=? ORDER BY night_num",
                (guild_id, pid, "success"))
            ea_nights = [r[0] for r in c_ea.fetchall()]
            conn_ea.close()
            current_night_ea = db_get_night_num(guild_id)
            if len(ea_nights) == 1 and current_night_ea - ea_nights[-1] < 2:
                nights_left = 2 - (current_night_ea - ea_nights[-1])
                if ch:
                    await ch.send(fmt(
                        f"🌙 Night {current_night_ea} — Elite Alpha\n"
                        f"Your second turn is on cooldown. Your first turn was Night {ea_nights[-1]}.\n"
                        f"You can turn again in {nights_left} night(s).\n"
                        f"Coordinate the wolf kill in the den tonight."))

        if night_view:
            role_descs = {
                "Seer":"Choose a player to investigate.",
                "Doctor":"Choose to save or skip.",
                "Surgeon": f"Choose to save or skip. You have **{db_get_ability_uses(guild_id, pid) or 0}/3 charges** remaining.",
                                "Witch":"Use your save or poison potion, or skip.",
                "Sheriff":"Acknowledge your role.",
                "Huntsman":"Choose a player to protect, or skip.",
                # Insomniac — no action view, hints fire automatically
                "Medium":"Choose a player to check alignment.",
                "Gravedigger":"Acknowledge — death details sent each morning.",
                "Hermit":"Choose a player to hide, or skip.",
                "Agitator":"Use your frenzy ability or save it.",
                "Governor":"Name the player you intend to pardon.",
                "Clone":"Choose the player you are cloning.",
                "Shapeshifter":"Choose a player to shapeshift into (Night 1 only).",
                "Cupid":"Bind two players together tonight.",
                "Wolf Pup":"Choose a player to block.",
                "Alpha":"Choose a villager to attempt a turn, or skip.",
                "Elite Alpha":"Choose a villager to attempt a turn, or skip.",
                "Bloodhound":"Choose a player to identify. (Available from Night 2)",
                "Bloodletter":"Mark a target with wolf blood, or skip.",
                "Crazed Wolf":"Select two kill targets.",
                "Clone":"Choose the player whose role you will inherit when they die. Night 1 only.",
                "Dire Wolf":"Choose your secret mate (Night 1 only).",
                "Echo-Stalker":"Haunt a player or use the regular wolf kill.",
                "Shadow Wolf":"Choose a voter to kill (post-death ability).",
                "Werekitten":"Choose your kill target tonight. Your cuteness bypasses the Sheriff and Huntsman protection — but Doctor and Surgeon saves still apply. The den can use your ability a maximum of 2 times per game.",
                "White Wolf":"Choose an independent kill, or skip.",
            }
            desc = role_descs.get(role_name, "Submit your night action below.")
            await ch.send(fmt(f"🌙 Night {night_num} — time to act!\n\n{desc}"), view=night_view)
            status_view = NightStatusView(guild_id, pid, role_name, night_num)
            await ch.send(fmt("Let the mod know your intent for tonight:"), view=status_view)
        elif role_name in NIGHT_NO_BUTTON_ROLES:
            # No ability — night announcement only, no buttons
            if role_name == "Bloodhound" and night_num < 2:
                await ch.send(fmt(
                    f"🌙 Night {night_num} has begun.\n"
                    f"🦴 Your tracking ability activates from Night 2 onwards. Sleep tight."))
            elif role_name == "Governor":
                await ch.send(fmt(
                    f"🌙 Night {night_num} has begun.\n"
                    f"🎖️ You have no night ability — rest until morning.\n"
                    f"When the day vote opens tomorrow, you will receive a pardon button in this channel.\n"
                    f"You must submit it at least 15 minutes before the vote closes."))
            elif role_name == "Hermit":
                await ch.send(fmt(
                    f"🌙 Night {night_num} has begun.\n"
                    f"🏚️ You have no night ability — rest until morning.\n"
                    f"When the day vote opens tomorrow, you will receive an ability button in this channel.\n"
                    f"You must submit it at least 20 minutes before the vote closes."))
            else:
                await ch.send(fmt(f"🌙 Night {night_num} has begun. Sleep tight — await the morning."))
        else:
            # Has passive awareness — keep status buttons
            status_view = NightStatusView(guild_id, pid, role_name, night_num)
            await ch.send(fmt(f"🌙 Night {night_num} — sleep tight. Await morning."), view=status_view)

    # Wolf vote — posted in wolf den since wolf-vote channel no longer exists
    wolf_den_ch = guild.get_channel(state.get("wolf_channel_id") or 0)
    if wolf_den_ch and wolf_ids:
        view     = WolfVoteView(guild_id, night_num)
        if hasattr(view, "_sel"):
            db_rows    = db_get_assignments(guild_id)
            non_wolf   = [r for r in db_rows if get_team(guild_id, r[1]) != "wolf"]
            npcs_patch = db_get_npcs(guild_id)
            npc_map_p  = {n["npc_id"]: n["name"] for n in npcs_patch}
            patched    = []
            for pid, role_name, is_alive, _ in non_wolf[:25]:
                m    = guild.get_member(pid)
                name = npc_map_p.get(pid) or (m.display_name if m else f"Player {pid}")
                if is_alive:
                    patched.append(discord.SelectOption(
                        label=f"✅ {name}"[:100], value=str(pid),
                        description="🤖 NPC" if pid in npc_map_p else "Alive"))
                else:
                    patched.append(discord.SelectOption(
                        label=f"💀 {name} (dead)"[:100], value=f"dead_{pid}",
                        description="Already eliminated"))
            if patched:
                view._sel.options = patched
        wolf_votes = db_get_wolf_votes(guild_id, night_num)
        embed      = build_wolf_vote_embed(guild, wolf_votes, alive_players, wolf_ids, night_num,
                                           den_channel=wolf_den_ch)
        wv_msg     = await wolf_den_ch.send(embed=embed, view=view)
        db_set_state(guild_id, wolf_vote_msg_id=wv_msg.id)

    # Shadow Wolf — send fresh action view each night after their death (Night 2+)
    if night_num > 1:
        sw_rows = db_get_assignments(guild_id)
        sw_row  = next((r for r in sw_rows if r[1] == "Shadow Wolf" and r[2] == 0 and r[3]), None)
        if sw_row:
            sw_data = db_get_shadow_wolf_list(guild_id)
            if sw_data and any(t["status"] == "alive" for t in sw_data["targets"]):
                sw_ch = guild.get_channel(sw_row[3])
                if sw_ch:
                    sw_view     = ShadowWolfView(guild_id, sw_row[0], "Shadow Wolf", alive_players)
                    status_view = NightStatusView(guild_id, sw_row[0], "Shadow Wolf", night_num)
                    await sw_ch.send(fmt(f"🌑 Night {night_num} — choose your kill from the list above."), view=sw_view)
                    await sw_ch.send(fmt("Let the mod know your intent for tonight:"), view=status_view)

    await post_night_transition(guild, night_num, duration)
    await set_bot_status(f"🌙 Night {night_num} in progress")

    # Insomniac hint on odd nights >= 3
    if night_num >= 3 and night_num % 2 == 1:
        safe_task(_send_insomniac_hint(guild, guild_id, night_num), "insomniac_hint")

    # NPC night farewell
    safe_task(_npc_night_farewell(guild, guild_id), "npc_farewell")

    # Wolf NPCs coordinate in den at night start
    wolf_npcs_coord = [n for n in db_get_npcs(guild_id)
                       if n["is_alive"] and get_team(guild_id, n["role_name"]) == "wolf"]
    if len(wolf_npcs_coord) >= 2:
        safe_task(_npc_wolf_coordinate(guild, guild_id, night_num), "wolf_coordinate")

    # NPC night actions
    async def _run_npc_night_actions():
        await asyncio.sleep(random.uniform(30, 90))
        for npc in db_get_npcs(guild_id):
            if npc["is_alive"]:
                if get_team(guild_id, npc["role_name"]) == "wolf":
                    await _npc_wolf_den_message(guild, guild_id, npc, night_num)
                await asyncio.sleep(random.uniform(10, 25))
                await _npc_night_action(guild, guild_id, npc, night_num)
                await asyncio.sleep(random.uniform(5, 15))
    safe_task(_run_npc_night_actions(), "npc_night_actions")

    await log_event(guild, f"Night {night_num}", f"🌙 Night {night_num} began ({mins} min timer)")
    await post_mod_log(guild, f"🌙 **Night {night_num}** started. Duration: {mins} min.")
    # Reset flags for new night and refresh dashboard
    db_set_state(guild_id, night_bb_done=0, investigations_done=0, day_bb_done=0)
    safe_task(update_mod_dashboard(guild), "dashboard_night_start")

    # Auto-resolve timer — now posts a prompt instead of auto-resolving
    async def auto_resolve_prompt():
        await asyncio.sleep(duration)
        cur = cached_get_state(guild_id)
        if cur and cur.get("phase") == "night":
            votes_summary = _build_vote_summary(guild, guild_id)
            mod_ch = guild.get_channel(cur.get("mod_log_channel_id") or 0)
            if mod_ch:
                embed = discord.Embed(
                    title       = f"⏰ Night {night_num} Timer Expired",
                    description = f"Night {night_num} has ended. All players have had their time to act.\n\nClick below to resolve night actions.",
                    color       = 0xF39C12
                )
                view = ResolveNightPromptView(guild_id, night_num)
                await mod_ch.send(embed=embed, view=view)

    task = asyncio.create_task(auto_resolve_prompt())
    night_timers[guild_id] = task

# ====================== ON READY ======================
@client.event
async def on_ready():
    print(f"✅ {client.user} is online!")

    # Pre-load mod role cache for all guilds that have game state
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT guild_id, mod_role_id FROM game_state WHERE mod_role_id IS NOT NULL")
    for gid, rid in c.fetchall():
        _mod_role_cache[gid] = rid
    conn.close()
    print(f"Pre-loaded mod roles for {len(_mod_role_cache)} guild(s)")
    # Load default roles for every guild the bot is in
    for guild in client.guilds:
        load_default_roles(guild.id)
    print(f"Default roles checked for {len(client.guilds)} guild(s)")
    # Global sync — commands available on every server the bot joins.
    # Note: global commands can take up to 1 hour to appear after first deploy.
    # To force instant sync on YOUR server during development, set GUILD_ID env var.
    guild_id_str = os.getenv("GUILD_ID")

    # Always do a global sync so ALL servers get commands
    try:
        synced = await tree.sync()
        print(f"Global sync: {len(synced)} commands — propagates to all servers within ~1 hour")
    except Exception as e:
        print(f"Global sync failed: {e}")

    # Additionally sync instantly to the dev/primary guild if GUILD_ID is set
    if guild_id_str:
        try:
            gid       = int(guild_id_str.strip())
            guild_obj = discord.Object(id=gid)
            tree.copy_global_to(guild=guild_obj)
            synced_g  = await tree.sync(guild=guild_obj)
            print(f"Dev guild {gid}: {len(synced_g)} commands synced instantly")
        except Exception as e:
            print(f"Dev guild instant sync failed: {e}")
    print(f"Bot ready! Serving {len(client.guilds)} guild(s)")



    # ── Restore active game state on restart ──────────────────────────────
    active_games = 0
    for guild in client.guilds:
        state = db_get_state(guild.id)
        if state and state.get("category_id"):
            active_games += 1
            night_num = db_get_night_num(guild.id)
            phase     = state.get("phase", "day")
            # Restore correct bot status
            if phase == "night":
                await set_bot_status(f"🌙 Night {night_num} in progress")
            else:
                await set_bot_status(f"☀️ Day {night_num} — discuss and vote")
            # Refresh day vote embed so buttons are re-attached
            await refresh_day_vote(guild)
            # Refresh wolf vote embed if night is active
            if phase == "night" and state.get("wolf_vote_msg_id"):
                await refresh_wolf_vote(guild, night_num)
            print(f"Restored active game for guild {guild.id} (phase={phase}, night={night_num})")
            if phase == "night":
                await post_mod_log(guild,
                    f"🔄 **Bot restarted during Night {night_num}.**\n"
                    f"Day vote buttons and the mod dashboard have been restored.\n"
                    f"Night action buttons in player channels are still active if players haven't pressed them.\n"
                    f"If a player's ability button is unresponsive, use `/submit_action` to submit on their behalf, "
                    f"or `/remind_night` to re-send their action prompt.")

            # Re-schedule night auto-resolve if night is active
            if phase == "night":
                import time as _t_restore
                night_end_ts_r = state.get("night_end_time")
                if night_end_ts_r and night_end_ts_r > int(_t_restore.time()):
                    guild_r = guild
                    night_num_r = night_num
                    async def _restore_auto_resolve(g=guild_r, n=night_num_r, ts=night_end_ts_r, gid=guild.id):
                        import time as _t2
                        wait = ts - int(_t2.time())
                        if wait > 0:
                            await asyncio.sleep(wait)
                        if not game_active(gid):
                            return
                        cur = db_get_state(gid) or {}
                        if cur.get("phase") == "night" and cur.get("night_end_time") == ts:
                            await resolve_night(g, n)
                            state_ar  = db_get_state(gid) or {}
                            mod_ch_ar = g.get_channel(state_ar.get("mod_log_channel_id") or 0)
                            if mod_ch_ar:
                                await mod_ch_ar.send(
                                    f"⏰ **Night {n} auto-resolved (bot restarted).**\n"
                                    f"Use `/eliminate` to apply deaths, then click Deliver Results.",
                                    view=DeliverResultsView(gid, n))
                    safe_task(_restore_auto_resolve(), "restore_auto_resolve")
                    print(f"  ↳ Night auto-resolve rescheduled for <t:{night_end_ts_r}:R>")

            # Re-start NPC proactive loops
            npcs = db_get_npcs(guild.id)
            alive_npcs = [n for n in npcs if n["is_alive"]]
            if alive_npcs:
                safe_task(_npc_proactive_loop(guild, guild.id), "npc_proactive_restore")
                safe_task(_npc_idle_check_loop(guild, guild.id), "npc_idle_restore")
                print(f"Resumed NPC proactive loop ({len(alive_npcs)} NPCs)")

    if active_games == 0:
        await set_bot_status("💤 No active game")
        print("No active games found — status set to idle")
    else:
        print(f"Restored {active_games} active game(s)")

    # ── Register persistent views so buttons survive bot restarts ─────────
    # Discord requires persistent views (timeout=None) to be re-registered
    # on every startup so the bot can handle interactions on old messages.
    # We register one instance per active guild for views that hold guild_id.
    registered = 0
    for guild in client.guilds:
        state = db_get_state(guild.id)
        if not state or not state.get("category_id"):
            continue  # No active game in this guild
        gid       = guild.id
        night_num = db_get_night_num(gid)
        phase     = state.get("phase", "day")

        # Day vote — pinned in village-chat
        client.add_view(DayVoteView(gid), message_id=state.get("day_vote_msg_id"))

        # Wolf vote — pinned in wolf-den
        if phase == "night":
            wv_mid = state.get("wolf_vote_msg_id")
            if wv_mid:
                client.add_view(WolfVoteView(gid, night_num), message_id=wv_mid)

        # Mod dashboard — pinned in mod-log
        db_mid = state.get("mod_dashboard_msg_id")
        if db_mid:
            client.add_view(ModDashboardView(gid), message_id=db_mid)

        registered += 1

        # Register stateless persistent views — use interaction.guild_id when triggered
        client.add_view(DeliverResultsView())  # bare registration, guild_id resolved at interaction time
        client.add_view(NightStatusView())     # bare registration, actor resolved at interaction time

        print(f"Persistent views registered for {registered} active guild(s)")

# ====================== ERROR HANDLER ======================

@client.event
async def on_guild_join(guild: discord.Guild):
    """When the bot joins a new server — load default roles and sync commands."""
    print(f"Joined new guild: {guild.name} ({guild.id})")
    load_default_roles(guild.id)
    # Instantly sync slash commands to this guild
    try:
        tree.copy_global_to(guild=guild)
        await tree.sync(guild=guild)
        print(f"Commands synced to new guild {guild.id}")
    except Exception as e:
        print(f"[on_guild_join] sync error for {guild.id}: {e}")


@client.event
async def on_guild_remove(guild: discord.Guild):
    """When the bot is removed from a server — clean up in-memory state."""
    _state_cache.pop(guild.id, None)
    _roles_cache.pop(guild.id, None)
    _mod_role_cache.pop(guild.id, None)
    night_timers.pop(guild.id, None)
    day_vote_timers.pop(guild.id, None)
    # Remove any cached NPC webhooks for this guild
    keys_to_remove = [k for k in _npc_webhooks if k[0] == guild.id]
    for k in keys_to_remove:
        _npc_webhooks.pop(k, None)
    print(f"Left guild {guild.id} — in-memory state cleared")


@tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Global slash command error handler — catches check failures and unexpected errors cleanly."""
    # Silently drop expired interaction and already-acknowledged errors
    if isinstance(error, app_commands.CommandInvokeError):
        orig = error.original
        if isinstance(orig, discord.errors.NotFound) and getattr(orig, "code", 0) == 10062:
            return
        if isinstance(orig, discord.errors.HTTPException) and getattr(orig, "code", 0) == 40060:
            return
    if isinstance(error, app_commands.CheckFailure):
        msg = "❌ You don't have permission to use this command."
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(msg, ephemeral=True)
            else:
                await interaction.followup.send(msg, ephemeral=True)
        except Exception:
            pass
        return

    # For any other unexpected error, log to mod-log and notify user
    msg = "⚠️ Something went wrong. Please try again."
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(msg, ephemeral=True)
        else:
            await interaction.followup.send(msg, ephemeral=True)
    except Exception:
        pass
    # Log to mod-log so mods see errors during a game
    try:
        if interaction.guild and game_active(interaction.guild_id):
            state  = db_get_state(interaction.guild_id) or {}
            mod_ch = interaction.guild.get_channel(state.get("mod_log_channel_id") or 0)
            if mod_ch:
                cmd_name = getattr(interaction.command, "name", "unknown")
                err_msg  = str(error.original if hasattr(error, "original") else error)[:400]
                await mod_ch.send(
                    f"⚠️ **Command Error** — `/{cmd_name}`\n"
                    f"**User:** {interaction.user.display_name}\n"
                    f"**Error:** {err_msg}")
    except Exception:
        pass
    # Re-raise so it still appears in Railway logs
    raise error



# ====================== SPEECH ENFORCEMENT ======================

def _has_real_words(text):
    """Returns True if the message contains actual readable words (not just emojis/links/mentions)."""
    import re
    # Strip Discord mentions, URLs, and emoji
    cleaned = re.sub(r"<[^>]+>", "", text)          # strip mentions/custom emoji
    cleaned = re.sub(r"https?://\S+", "", cleaned)   # strip URLs
    cleaned = re.sub(r"[^\w\s]", " ", cleaned)       # strip punctuation/emoji unicode
    words = [w for w in cleaned.split() if w.isalpha() and len(w) > 1]
    return len(words) > 0

def _is_proper_sentence(text):
    """Returns True if the message looks like a proper correctly-spelled sentence."""
    import re
    cleaned = re.sub(r"<[^>]+>", "", text)
    cleaned = re.sub(r"https?://\S+", "", cleaned)
    cleaned = re.sub(r"[^\w\s]", " ", cleaned)
    words = [w for w in cleaned.split() if w.isalpha() and len(w) > 2]
    # A proper sentence has 2+ real words all correctly lowercase or title case
    proper = [w for w in words if w.lower() == w or w.capitalize() == w]
    return len(proper) >= 2

SPEECH_RULES = {
    "Drunk": {
        "rule":    "can only communicate in memes, gifs, and emojis — no text",
        "check":   lambda text: _has_real_words(text),
        "warning": "🍺 **Drunk warning** — you can only send memes, gifs, and emojis. No text allowed.",
        "final":   "🍺 **Final warning** — one more text message and the mods will be notified for elimination.",
    },
    "Prostitute": {
        "rule":    "can only communicate via sexual innuendo, gifs, emojis, or suggestive words",
        "check":   lambda text: _has_real_words(text) and not any(
            w in text.lower() for w in [
                "babe","honey","darling","sugar","sweet","hot","naughty","wink",
                "kiss","oh my","goodness","tempt","desire","tease","seduce",
                "lusty","steamy","spicy","flirt","gorgeous","delicious"
            ]),
        "warning": "💋 **Prostitute warning** — you must speak only in innuendo and suggestive language.",
        "final":   "💋 **Final warning** — one more clean message and the mods will be notified for elimination.",
    },
    "Village Idiot": {
        "rule":    "can only speak in typos and gibberish — no proper words",
        "check":   lambda text: _is_proper_sentence(text),
        "warning": "🤪 **Village Idiot warning** — you must speak only in typos and gibberish. That was too coherent.",
        "final":   "🤪 **Final warning** — one more proper sentence and the mods will be notified for elimination.",
    },
    "Virgin": {
        "rule":    "everything said must be wholesome — no crude or violent language",
        "check":   lambda text: any(w in text.lower() for w in [
            "damn","hell","crap","wtf","shut up","idiot","stupid",
            "kill","die","blood","murder","sus","hate","screw","ass",
            "cuss","ugly","loser","dumb","moron"
        ]),
        "warning": "🕊️ **Virgin warning** — your speech must remain pure and wholesome.",
        "final":   "🕊️ **Final warning** — one more impure message and the mods will be notified for elimination.",
    },
}

async def check_speech_violation(message: discord.Message, role_name: str, guild_id: int):
    """Check if a message violates role speech rules. Warn in private channel, log to mod-log."""
    rule_data = SPEECH_RULES.get(role_name)
    if not rule_data:
        return
    text = message.content
    if not text or not rule_data["check"](text):
        return  # No violation

    rows       = db_get_assignments(guild_id)
    assignment = next((r for r in rows if r[0] == message.author.id), None)
    if not assignment or not assignment[3]:
        return
    priv_ch = message.guild.get_channel(assignment[3])
    if not priv_ch:
        return

    count = db_increment_violation(guild_id, message.author.id)

    # Forward violating message to private channel
    embed = discord.Embed(
        title       = f"⚠️ Speech Rule Violation — {role_name}",
        description = f"Your message in village-chat violated your role rules.",
        color       = 0xE67E22
    )
    embed.add_field(name="Your message",  value=f"*{text[:500]}*",       inline=False)
    embed.add_field(name="Your rule",     value=rule_data["rule"],        inline=False)

    if count == 1:
        embed.add_field(name="⚠️ Warning", value=rule_data["warning"], inline=False)
        await priv_ch.send(embed=embed)
        await post_mod_log(message.guild,
            f"⚠️ **Speech violation — {role_name}** (Warning 1)\n"
            f"**Player:** {message.author.mention}\n"
            f"**Message:** {text[:300]}\n"
            f"*Player warned in private channel.*")
    else:
        embed.add_field(name="🚨 Final Warning",
                        value=rule_data["final"] + "\n\nMods have been notified.",
                        inline=False)
        await priv_ch.send(embed=embed)
        await post_mod_log(message.guild,
            f"🚨 **Speech violation — {role_name}** (FINAL WARNING — Violation #{count})\n"
            f"**Player:** {message.author.mention}\n"
            f"**Message:** {text[:300]}\n"
            f"⚠️ **This player is eligible for mod-kill. Awaiting mod decision.**")

# ====================== NPC CHAT LISTENER ======================
@client.event
async def on_message(message: discord.Message):
    """Watch village-chat and have NPCs respond naturally."""
    if message.author.bot:
        return
    # Track message counts for active players during day phase
    if message.guild and game_active(message.guild.id):
        state_msg = db_get_state(message.guild.id)
        if state_msg and state_msg.get("phase") == "day":
            rows_msg = db_get_assignments(message.guild.id)
            if any(r[0] == message.author.id and r[2] == 1 for r in rows_msg):
                day_num_msg = db_get_night_num(message.guild.id)
                db_increment_message_count(message.guild.id, message.author.id, day_num_msg)
    if not message.guild:
        return

    # Early return if no game active — avoids all DB queries for non-game messages
    if not game_active(message.guild.id):
        return

    state = cached_get_state(message.guild.id)

    # ── Speech enforcement — runs for any message in village-chat ─────────
    vc_ch_id = state.get("village_chat_ch_id")
    in_village_chat = bool(vc_ch_id and message.channel.id == vc_ch_id)
    if in_village_chat and not message.author.bot:
        rows_speech = db_get_assignments(message.guild.id)
        sender_row  = next((r for r in rows_speech if r[0] == message.author.id and r[2] == 1), None)
        if sender_row:
            safe_task(check_speech_violation(message, sender_row[1], message.guild.id), "speech_check")

    # Check if message is in an NPC private channel (mod talking to NPC directly)
    npcs_all = db_get_npcs(message.guild.id)
    npc_in_ch = next((n for n in npcs_all if n["channel_id"] == message.channel.id and n["is_alive"]), None)
    if npc_in_ch:
        safe_task(_npc_private_respond(message.guild, message.guild.id, npc_in_ch, message), "npc_private_respond")
        return

    # Accept hardcoded village chat OR the DB-stored channel id
    if not in_village_chat:
        return

    npcs = db_get_npcs(message.guild.id)
    alive_npcs = [n for n in npcs if n["is_alive"]]

    text        = message.content
    author_name = message.author.display_name
    state       = cached_get_state(message.guild.id)

    # Check if message is in wolf-den — only wolf NPCs respond there
    if state.get("wolf_channel_id") and message.channel.id == state.get("wolf_channel_id"):
        wolf_npcs = [n for n in alive_npcs if get_team(message.guild.id, n["role_name"]) == "wolf"]
        for npc in wolf_npcs:
            night_num = db_get_night_num(message.guild.id)
            safe_task(_npc_wolf_den_message(
                message.guild, message.guild.id, npc, night_num, text))
            await asyncio.sleep(random.uniform(2, 5))
        return

    # Village-chat NPC responses — update history and decide who responds
    # Update chat history for all NPCs
    for npc in alive_npcs:
        history = npc.get("chat_history", [])
        history.append({"author": author_name, "text": text})
        db_update_npc_chat_history(message.guild.id, npc["npc_id"], history)

    # ── Accusation detection ──────────────────────────────────────────────
    accusation_keywords = ["sus", "suspicious", "wolf", "lying", "vote", "accuse",
                           "it's", "it is", "definitely", "obviously", "clearly"]
    is_accusation = any(kw in text.lower() for kw in accusation_keywords)
    if is_accusation:
        for npc in alive_npcs:
            if npc["name"].split()[0].lower() in text.lower() or npc["name"].lower() in text.lower():
                # Real player accused this NPC — record it
                day_num = db_get_night_num(message.guild.id)
                db_record_npc_accusation(message.guild.id, npc["npc_id"],
                                         message.author.id, day_num)
                # Update suspicions to include this accuser
                suspicions = npc.get("suspicions", [])
                accuser_entry = f"{message.author.display_name} (accused me Day {day_num})"
                if accuser_entry not in suspicions:
                    suspicions = [accuser_entry] + suspicions
                    db_update_npc_suspicions(message.guild.id, npc["npc_id"], suspicions[:6])
                # Pin this as a permanent memory event
                db_pin_npc_event(message.guild.id, npc["npc_id"],
                    f"Day {day_num}: {message.author.display_name} accused me publicly.")
                # Track times accused for HoF (accuser gets the stat if they were wrong)
                # We can only verify at game end — just record for now

    # ── Check if message defends an NPC ──────────────────────────────────────
    defense_kws   = ["trust", "believe", "innocent", "not a wolf", "vouch", "defend",
                      "clear", "safe", "i think they're good"]
    agreement_kws = ["agree with", "agree with you", "same as", "i think too",
                     "backs that up", "that tracks", "i was thinking the same"]

    for npc in alive_npcs:
        npc_first_d = npc["name"].split()[0].lower()
        npc_full_d  = npc["name"].lower()
        if (npc_first_d in text.lower() or npc_full_d in text.lower()):
            if any(kw in text.lower() for kw in defense_kws):
                safe_task(_npc_react_to_defense(
                    message.guild, message.guild.id, npc,
                    author_name, text), "npc_defense_react")
            elif any(kw in text.lower() for kw in agreement_kws):
                safe_task(_npc_react_to_agreement(
                    message.guild, message.guild.id, npc,
                    author_name, text), "npc_agree_react")

    # ── Decide which NPCs respond ──────────────────────────────────────────
    for npc in alive_npcs:
        npc_first = npc["name"].split()[0].lower()
        npc_full  = npc["name"].lower()
        text_low  = text.lower()

        # Check name mentions — handles plain text, partial name, full name
        name_in_text = npc_first in text_low or npc_full in text_low

        # Check if any real player mentioned this NPC by Discord mention
        # (Discord sends mentions as <@id> in raw content — check message.mentions)
        mentioned = any(
            m.id == npc["npc_id"] for m in message.mentions
        ) if hasattr(message, "mentions") else False

        # Check if the message is a question or directed at the group
        is_group_question = any(w in text_low for w in [
            "anyone", "everyone", "what do you", "what does everyone",
            "who do you", "who thinks", "thoughts?", "agree?", "disagree?",
            "what about", "anyone else", "does anyone"
        ])

        # Cross-NPC: treat other NPCs as real players
        is_from_npc = any(n["name"].split()[0].lower() in author_name.lower()
                          for n in alive_npcs if n["npc_id"] != npc["npc_id"])

        # Weight responses strategically — not just random
        team_hint = npc.get("team_hint", get_team(member.guild.id if hasattr(member, "guild") else message.guild.id, npc.get("role_name", "")))
        suspicions = npc.get("suspicions", [])
        author_suspected = any(author_name.lower() in s.lower() for s in suspicions)

        # Higher chance if author is suspected or is a threat
        weighted_chance = 0.25
        if author_suspected:          weighted_chance = 0.65  # More likely to respond to suspects
        elif is_group_question:       weighted_chance = 0.70  # Group questions get broad response
        elif "accus" in text_low or "sus" in text_low: weighted_chance = 0.55  # Accusations
        elif any(kw in text_low for kw in ["wolf", "kill", "who", "vote"]): weighted_chance = 0.45

        should_respond = name_in_text or mentioned or is_from_npc or random.random() < weighted_chance

        # Check if this is a reply to a pending NPC question
        pending_q = _npc_pending_questions.get((message.guild.id, npc["npc_id"]), {})
        if pending_q and pending_q.get("target", "").lower() in text.lower():
            safe_task(_npc_followup(message.guild, message.guild.id, npc,
                                    author_name, text, message.channel), "npc_followup")

        if should_respond:
            safe_task(_npc_respond(
                message.guild, message.guild.id, npc, text, author_name, message.channel))
            await asyncio.sleep(random.uniform(2, 5))


async def _npc_private_respond(guild, guild_id: int, npc: dict, message: discord.Message):
    """NPC responds to a mod message in their private channel."""
    game_ctx = _npc_game_context(guild, guild_id, npc)
    system   = _npc_system_prompt(npc)
    prompt   = (
        f"Game state:\n{game_ctx}\n\n"
        f"The mod just sent you a private message: \"{message.content}\"\n\n"
        f"Respond as {npc['name']}. Stay in character. Be honest about your game thoughts "
        f"since this is a private channel. 1-3 sentences."
    )
    await asyncio.sleep(random.uniform(4, 10))
    response = await _claude(prompt, system, max_tokens=150)
    if not response:
        return
    priv_ch = guild.get_channel(npc["channel_id"] or 0)
    if priv_ch:
        try:
            async with aiohttp.ClientSession() as session:
                wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                await wh.send(fmt(response), username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
        except Exception as e:
            print(f"NPC private respond error: {e}")

# ====================== MESSAGE LOGGING ======================

@client.event
async def on_member_remove(member: discord.Member):
    """Alert mods when a player leaves the server mid-game."""
    if not game_active(member.guild.id):
        return
    rows = db_get_assignments(member.guild.id)
    assignment = next((r for r in rows if r[0] == member.id and r[2] == 1), None)
    if not assignment:
        return  # Not an active player

    role_name = assignment[1]
    team      = get_team(member.guild.id, role_name)
    team_emoji = "🐺" if team == "wolf" else ("⚖️" if team == "neutral" else "🏘️")

    class MemberLeftView(View):
        def __init__(self):
            super().__init__(timeout=3600)

        @discord.ui.button(label="❌ Remove from Game", style=discord.ButtonStyle.danger)
        async def remove_btn(self, btn_interaction: discord.Interaction, button):
            rows2      = db_get_assignments(member.guild.id)
            assignment2 = next((r for r in rows2 if r[0] == member.id), None)
            if assignment2:
                db_set_player_alive(member.guild.id, member.id, False)
                db_remove_day_vote(member.guild.id, member.id)
                await refresh_win_tracker(member.guild)
                await refresh_player_list(member.guild)
                await btn_interaction.response.edit_message(
                    content=f"✅ **{member.display_name}** removed from game.",
                    embed=None, view=None)
                await log_event(member.guild, "System",
                    f"🚪 **{member.display_name}** left the server — removed from game by mod.")
            else:
                await btn_interaction.response.edit_message(
                    content="Already removed.", embed=None, view=None)

        @discord.ui.button(label="Keep in Game", style=discord.ButtonStyle.secondary)
        async def keep_btn(self, btn_interaction: discord.Interaction, button):
            await btn_interaction.response.edit_message(
                content=f"ℹ️ **{member.display_name}** kept in game. They can still rejoin.",
                embed=None, view=None)

    embed = discord.Embed(
        title       = "🚨 Player Left Server",
        description = f"**{member.display_name}** has left the server while the game is active.",
        color       = 0xE74C3C
    )
    embed.add_field(name="Role",   value=f"{team_emoji} {role_name}", inline=True)
    embed.add_field(name="Team",   value=team.capitalize(),           inline=True)
    embed.set_footer(text="Remove them from the game or keep them in — they can rejoin if they return.")

    state  = db_get_state(member.guild.id) or {}
    mod_ch = member.guild.get_channel(state.get("mod_log_channel_id") or 0)
    if mod_ch:
        await mod_ch.send(embed=embed, view=MemberLeftView())

@client.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if before.author.bot or before.guild is None or before.content == after.content:
        return
    # Only log messages in game channels
    state = db_get_state(before.guild.id)
    if not state or not state.get("category_id"):
        return
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await post_mod_log(before.guild,
        f"✏️ **Edited** | {ts} | {before.author.mention} in {before.channel.mention}\n"
        f"**Before:** {before.content[:500] or '*(empty)*'}\n"
        f"**After:** {after.content[:500] or '*(empty)*'}"
    )

@client.event
async def on_message_delete(message: discord.Message):
    if message.author.bot or message.guild is None:
        return
    # Only log messages in game channels
    state = db_get_state(message.guild.id)
    if not state or not state.get("category_id"):
        return
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await post_mod_log(message.guild,
        f"🗑️ **Deleted** | {ts} | {message.author.mention} in {message.channel.mention}\n"
        f"**Content:** {message.content[:500] or '*(empty)*'}"
    )

# ====================== SETUP COMMANDS ======================

@tree.command(name="setup_roles", description="Set the mod, participant, dead, and spectator roles in one command")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(
    mod_role="Role that can control the game",
    participant_role="Role that marks players in the game",
    dead_role="Role given to eliminated players",
    spectator_role="Role for spectators (read-only access to all channels)"
)
async def setup_roles(interaction: discord.Interaction,
                      mod_role: discord.Role,
                      participant_role: discord.Role,
                      dead_role: discord.Role,
                      spectator_role: discord.Role = None):
    kwargs = dict(mod_role_id=mod_role.id,
                  participant_role_id=participant_role.id,
                  dead_role_id=dead_role.id)
    if spectator_role:
        kwargs["spectator_role_id"] = spectator_role.id
    db_set_state(interaction.guild_id, **kwargs)
    # Keep fast mod cache in sync
    _mod_role_cache[interaction.guild_id] = mod_role.id
    embed = discord.Embed(title="✅ Roles Configured", color=0x44BB44)
    embed.add_field(name="🛡️ Mod Role",        value=mod_role.mention,                              inline=False)
    embed.add_field(name="🎮 Participant Role", value=participant_role.mention,                      inline=False)
    embed.add_field(name="💀 Dead Role",        value=dead_role.mention,                             inline=False)
    embed.add_field(name="👁️ Spectator Role",  value=spectator_role.mention if spectator_role else "Not set", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="set_spectator_role", description="Set or update the spectator role")
@is_mod()
async def set_spectator_role(interaction: discord.Interaction, role: discord.Role):
    db_set_state(interaction.guild_id, spectator_role_id=role.id)
    await interaction.response.send_message(f"✅ Spectator role set to **{role.name}**", ephemeral=True)

# ====================== DURATION COMMANDS ======================
class DayDurationModal(Modal, title="Set Day Phase Duration"):
    duration = TextInput(label="Day duration in minutes (default: 720 = 12h)",
                         placeholder="e.g. 720  →  12 hours",
                         style=discord.TextStyle.short, required=True, min_length=1, max_length=4)
    async def on_submit(self, interaction: discord.Interaction):
        try:
            mins = int(self.duration.value.strip())
            if mins < 1 or mins > 1440:
                return await interaction.response.send_message("Must be 1–1440 minutes.", ephemeral=True)
            db_set_state(interaction.guild_id, day_duration=mins * 60)
            await interaction.response.send_message(
                f"✅ Day duration set to **{mins} min** ({mins/60:.1f} hrs).", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Please enter a valid number.", ephemeral=True)

class NightDurationModal(Modal, title="Set Night Phase Duration"):
    duration = TextInput(label="Night duration in minutes (default: 720 = 12h)",
                         placeholder="e.g. 720  →  12 hours",
                         style=discord.TextStyle.short, required=True, min_length=1, max_length=4)
    async def on_submit(self, interaction: discord.Interaction):
        try:
            mins = int(self.duration.value.strip())
            if mins < 1 or mins > 1440:
                return await interaction.response.send_message("Must be 1–1440 minutes.", ephemeral=True)
            db_set_state(interaction.guild_id, night_duration=mins * 60)
            await interaction.response.send_message(
                f"✅ Night duration set to **{mins} min** ({mins/60:.1f} hrs).", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Please enter a valid number.", ephemeral=True)

# ====================== ROLE MANAGEMENT ======================

@tree.command(name="reload_roles", description="Reload all default roles into the pool (admin only)")
@app_commands.checks.has_permissions(administrator=True)
async def reload_roles(interaction: discord.Interaction):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    # Force reload — delete existing and reinsert all defaults
    c.execute("DELETE FROM game_roles WHERE guild_id=?", (interaction.guild_id,))
    for name, team, cnt, desc in DEFAULT_ROLES:
        c.execute("INSERT OR IGNORE INTO game_roles VALUES (?,?,?,?,?)",
                  (interaction.guild_id, name, desc, cnt, team))
    conn.commit()
    conn.close()
    invalidate_cache(interaction.guild_id)
    await interaction.response.send_message(
        f"✅ Reloaded **{len(DEFAULT_ROLES)}** default roles into the pool.",
        ephemeral=True)

@tree.command(name="add_role", description="Add or update a game role in the pool")
@is_mod()
@app_commands.describe(name="Role name", description="What this role does",
                       count="How many to include", team="village, wolf, or neutral")
async def add_role(interaction: discord.Interaction, name: str, description: str,
                   count: int = 1, team: str = "village"):
    if interaction.user.guild_permissions.administrator is False:
        state = cached_get_state(interaction.guild_id)
        mod_role = interaction.guild.get_role(state.get("mod_role_id"))
        if not mod_role or mod_role not in interaction.user.roles:
            return await interaction.response.send_message("❌ No permission.", ephemeral=True)
    team = team.lower()
    if team not in ["village", "wolf", "neutral"]:
        return await interaction.response.send_message("❌ Team must be `village`, `wolf`, or `neutral`.", ephemeral=True)
    if count < 1:
        return await interaction.response.send_message("❌ Count must be at least 1.", ephemeral=True)
    db_save_role(interaction.guild_id, name, description, count, team)
    emoji = "🐺" if team == "wolf" else ("⚖️" if team == "neutral" else "🏘️")
    await interaction.response.send_message(
        f"{emoji} **{name}** ×{count} [{team}] saved.\n> {description}", ephemeral=True)

@tree.command(name="remove_role", description="Remove a role from the pool")
@is_mod()
async def remove_role(interaction: discord.Interaction, name: str):
    db_delete_role(interaction.guild_id, name)
    await interaction.response.send_message(f"🗑️ Role **{name}** removed.", ephemeral=True)


@tree.command(name="post_roles", description="Post all roles with descriptions to village chat")
@is_mod()
async def show_roles(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    roles = db_load_roles(interaction.guild_id)
    if not roles:
        return await interaction.followup.send("No roles saved yet.", ephemeral=True)

    village_roles = [r for r in roles if r["team"] == "village"]
    wolf_roles    = [r for r in roles if r["team"] == "wolf"]
    neutral_roles = [r for r in roles if r["team"] == "neutral"]

    def build_section(role_list, label, color):
        embeds = []
        chunk_text = ""
        fields = []
        for r in role_list:
            desc = r.get("description") or "*No description.*"
            line = f"**{r['name']}**\n{desc}\n\n"
            if len(chunk_text) + len(line) > 3900:
                embed = discord.Embed(description=chunk_text, color=color)
                embeds.append(embed)
                chunk_text = ""
            chunk_text += line
        if chunk_text:
            embed = discord.Embed(description=chunk_text, color=color)
            embeds.append(embed)
        if embeds:
            embeds[0].title = label
        return embeds

    # Get village chat channel
    state  = db_get_state(interaction.guild_id) or {}
    vc_ch  = interaction.guild.get_channel(state.get("village_chat_ch_id") or 0)
    if not vc_ch:
        return await interaction.followup.send("❌ Village chat channel not found.", ephemeral=True)

    # Post header
    header = discord.Embed(
        title       = "📜 Roles Available This Game",
        description = "All roles are listed below by team. Who has which role is secret — only the totals will be revealed at game start.",
        color       = 0x5865F2
    )
    await vc_ch.send(embed=header)

    # Post each team
    for embeds in [
        build_section(village_roles, "🏘️ Village Roles", 0x27AE60),
        build_section(wolf_roles,    "🐺 Wolf Roles",    0xC0392B),
        build_section(neutral_roles, "⚖️ Neutral Roles", 0xF39C12),
    ]:
        for embed in embeds:
            await vc_ch.send(embed=embed)

    await interaction.followup.send("✅ Roles posted to village chat.", ephemeral=True)


@tree.command(name="current_roles", description="Show all roles in the current active game")
async def current_roles(interaction: discord.Interaction):
    await interaction.response.defer()

    if not game_active(interaction.guild_id):
        return await interaction.followup.send("No active game.")

    # Get roles from the current game's last_role_set
    last_roles = db_get_last_roles(interaction.guild_id)
    if not last_roles:
        return await interaction.followup.send("No role data found for the current game.")

    all_roles = {r["name"]: r for r in cached_load_roles(interaction.guild_id)}

    village_lines, wolf_lines, neutral_lines = [], [], []
    for role_name, count in last_roles.items():
        info = all_roles.get(role_name, {})
        team = info.get("team", "village")
        line = f"**{role_name}** ×{count}"
        if team == "wolf":
            wolf_lines.append(line)
        elif team == "neutral":
            neutral_lines.append(line)
        else:
            village_lines.append(line)

    embed = discord.Embed(title="🎮 Current Game Roles", color=0x9B59B6)
    if village_lines:
        embed.add_field(name="🏘️ Village", value="\n".join(village_lines), inline=False)
    if wolf_lines:
        embed.add_field(name="🐺 Wolf", value="\n".join(wolf_lines), inline=False)
    if neutral_lines:
        embed.add_field(name="⚖️ Neutral", value="\n".join(neutral_lines), inline=False)

    total = sum(last_roles.values())
    embed.set_footer(text=f"Total roles in game: {total}")
    await interaction.followup.send(embed=embed)

@tree.command(name="list_roles", description="Show all saved game roles in this channel")
async def list_roles(interaction: discord.Interaction):
    await interaction.response.defer()
    roles = db_load_roles(interaction.guild_id)
    if not roles:
        return await interaction.followup.send("No roles saved yet. Use `/add_role`.")

    wolf_roles    = [r for r in roles if r["team"] == "wolf"]
    village_roles = [r for r in roles if r["team"] == "village"]
    neutral_roles = [r for r in roles if r["team"] == "neutral"]

    def make_chunks(role_list, label):
        """Split role list into 1024-char chunks as separate fields."""
        fields = []
        chunk  = ""
        count  = 0
        for r in role_list:
            line = f"**{r['name']}** ×{r['count']}\n"
            if len(chunk) + len(line) > 1020:
                fields.append((f"{label} ({count})", chunk.strip()))
                chunk = ""
                count = 0
            chunk += line
            count += 1
        if chunk:
            fields.append((label, chunk.strip()))
        return fields

    embed = discord.Embed(title="📋 Saved Game Roles", color=0x5865F2)
    for name, value in make_chunks(village_roles, "🏘️ Village"):
        embed.add_field(name=name, value=value, inline=False)
    for name, value in make_chunks(wolf_roles, "🐺 Wolf"):
        embed.add_field(name=name, value=value, inline=False)
    for name, value in make_chunks(neutral_roles, "⚖️ Neutral"):
        embed.add_field(name=name, value=value, inline=False)

    await interaction.followup.send(embed=embed)

# ====================== START GAME ======================
# ── Helpers ──────────────────────────────────────────────────────────────

def _role_emoji(team):
    return "🐺" if team == "wolf" else ("⚖️" if team == "neutral" else "🏘️")

def _build_roster_text(counts, all_roles, reservations=None):
    if not counts:
        return "*No roles added yet.*"
    res_map = {}
    if reservations:
        for pid, pname, rname in reservations:
            res_map.setdefault(rname, []).append(pname)
    village, wolf, neutral = [], [], []
    for name, cnt in counts.items():
        info    = all_roles.get(name, {})
        team    = info.get("team", "village")
        res     = res_map.get(name, [])
        res_tag = f" ⭐ *(reserved: {', '.join(res)})*" if res else ""
        line    = f"{_role_emoji(team)} **{name}** ×{cnt}{res_tag}"
        if team == "wolf":       wolf.append(line)
        elif team == "neutral":  neutral.append(line)
        else:                    village.append(line)
    parts = []
    if village: parts.append("**🏘️ Village**\n" + "\n".join(village))
    if wolf:    parts.append("**🐺 Wolf**\n"    + "\n".join(wolf))
    if neutral: parts.append("**⚖️ Neutral**\n" + "\n".join(neutral))
    total = sum(counts.values())
    return "\n".join(parts) + f"\n\n**Total slots: {total}**"

# ── Page selector — lets mod pick a role from a dropdown, one page at a time ─

PAGE_SIZE = 20   # roles per dropdown page (Discord max is 25)

class RoleBuilderView(View):
    """
    Main a-la-carte builder.
    Shows a paginated dropdown of all roles.
    Mod picks a role → +/- buttons adjust its count.
    Repeat until happy, then hit Confirm.
    """
    def __init__(self, guild_id, all_roles_list, counts=None, page=0, npc_count=0):
        super().__init__(timeout=600)
        self.guild_id      = guild_id
        self.all_roles_list= all_roles_list
        self.all_roles     = {r["name"]: r for r in all_roles_list}
        self.page          = page
        self.selected_role = None
        self.npc_count     = npc_count   # 0-3 NPCs to include
        # Load reservations and auto-populate reserved roles into counts
        self.reservations  = db_get_reservations(guild_id)
        self.counts        = counts or {}
        for pid, pname, rname in self.reservations:
            if rname in self.all_roles and rname not in self.counts:
                self.counts[rname] = 1  # Auto-populate reserved role
        self._build_items()

    # ── Sort roles: village first, wolf second, neutral third, then alpha ──
    def _sorted_roles(self):
        order = {"village": 0, "wolf": 1, "neutral": 2}
        return sorted(self.all_roles_list,
                      key=lambda r: (order.get(r["team"], 3), r["name"].lower()))

    def _build_items(self):
        self.clear_items()
        sorted_roles = self._sorted_roles()
        total_pages  = max(1, (len(sorted_roles) + PAGE_SIZE - 1) // PAGE_SIZE)
        page_roles   = sorted_roles[self.page * PAGE_SIZE : (self.page + 1) * PAGE_SIZE]

        # ── Role picker dropdown ───────────────────────────────────────────
        options = []
        for r in page_roles:
            cnt   = self.counts.get(r["name"], 0)
            label = f"{_role_emoji(r['team'])} {r['name']}" + (f"  ×{cnt}" if cnt else "")
            desc  = (r.get("description") or "")[:80]
            options.append(discord.SelectOption(
                label=label[:100],
                value=r["name"],
                description=desc or None,
                default=(r["name"] == self.selected_role)
            ))

        sel = Select(placeholder=f"Pick a role to adjust  (page {self.page+1}/{total_pages})",
                     options=options, min_values=1, max_values=1)
        sel.callback = self.on_role_select
        self.add_item(sel)

        # ── Count controls (only shown when a role is focused) ────────────
        minus10 = Button(label="−10", style=discord.ButtonStyle.danger,
                         disabled=self.selected_role is None, row=1)
        minus1  = Button(label="−1",  style=discord.ButtonStyle.danger,
                         disabled=self.selected_role is None, row=1)
        plus1   = Button(label="+1",  style=discord.ButtonStyle.success,
                         disabled=self.selected_role is None, row=1)
        plus10  = Button(label="+10", style=discord.ButtonStyle.success,
                         disabled=self.selected_role is None, row=1)
        minus10.callback = self._make_delta(-10)
        minus1.callback  = self._make_delta(-1)
        plus1.callback   = self._make_delta(1)
        plus10.callback  = self._make_delta(10)
        self.add_item(minus10)
        self.add_item(minus1)
        self.add_item(plus1)
        self.add_item(plus10)

        # ── NPC counter row ───────────────────────────────────────────────
        npc_minus = Button(label="🤖 NPC −", style=discord.ButtonStyle.secondary,
                           disabled=(self.npc_count <= 0), row=2)
        npc_label = Button(label=f"NPCs: {self.npc_count}/3",
                           style=discord.ButtonStyle.secondary, disabled=True, row=2)
        npc_plus  = Button(label="🤖 NPC +", style=discord.ButtonStyle.secondary,
                           disabled=(self.npc_count >= 3), row=2)
        npc_minus.callback = self._npc_delta(-1)
        npc_plus.callback  = self._npc_delta(1)
        self.add_item(npc_minus)
        self.add_item(npc_label)
        self.add_item(npc_plus)

        # ── Navigation & actions ──────────────────────────────────────────
        prev_btn = Button(label="◀ Prev", style=discord.ButtonStyle.secondary,
                          disabled=(self.page == 0), row=3)
        next_btn = Button(label="Next ▶", style=discord.ButtonStyle.secondary,
                          disabled=(self.page >= total_pages - 1), row=3)
        clear_btn= Button(label="🗑️ Clear All", style=discord.ButtonStyle.danger, row=3)
        done_btn = Button(label="✅ Confirm Roster",
                          style=discord.ButtonStyle.green,
                          disabled=sum(self.counts.values()) == 0, row=3)
        prev_btn.callback  = self.on_prev
        next_btn.callback  = self.on_next_page
        clear_btn.callback = self.on_clear
        done_btn.callback  = self.on_done
        self.add_item(prev_btn)
        self.add_item(next_btn)
        self.add_item(clear_btn)
        self.add_item(done_btn)

    def _make_delta(self, delta):
        async def callback(interaction: discord.Interaction):
            name = self.selected_role
            if not name:
                return await interaction.response.defer()
            current = self.counts.get(name, 0)
            new_val = max(0, current + delta)
            if new_val == 0:
                self.counts.pop(name, None)
            else:
                self.counts[name] = new_val
            self._build_items()
            await interaction.response.edit_message(
                content=self._render(), view=self)
        return callback

    def _npc_delta(self, delta):
        async def callback(interaction: discord.Interaction):
            self.npc_count = max(0, min(3, self.npc_count + delta))
            self._build_items()
            await interaction.response.edit_message(content=self._render(), view=self)
        return callback

    async def on_role_select(self, interaction: discord.Interaction):
        self.selected_role = interaction.data["values"][0]
        self._build_items()
        await interaction.response.edit_message(
            content=self._render(), view=self)

    async def on_prev(self, interaction: discord.Interaction):
        self.page -= 1
        self.selected_role = None
        self._build_items()
        await interaction.response.edit_message(content=self._render(), view=self)

    async def on_next_page(self, interaction: discord.Interaction):
        self.page += 1
        self.selected_role = None
        self._build_items()
        await interaction.response.edit_message(content=self._render(), view=self)

    async def on_clear(self, interaction: discord.Interaction):
        self.counts.clear()
        self.npc_count = 0
        self.selected_role = None
        self._build_items()
        await interaction.response.edit_message(content=self._render(), view=self)

    async def on_done(self, interaction: discord.Interaction):
        if not self.counts:
            return await interaction.response.send_message(
                "❌ Add at least one role first.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)

        final_counts = dict(self.counts)

        # Replay protection
        last = db_get_last_roles(interaction.guild_id)
        if last and last == final_counts:
            view = ReplayWarningView(interaction.guild_id, final_counts, self.all_roles, self.npc_count)
            await interaction.followup.send(
                "⚠️ **Replay Warning** — same role combination as last game!\n"
                "Consider mixing it up. Continue anyway?",
                view=view, ephemeral=True)
            return

        npc_count = self.npc_count
        view  = ConfirmStartView(interaction.guild_id, final_counts, self.all_roles, npc_count)
        total = sum(final_counts.values())
        npc_line = f"\n🤖 **{npc_count} NPC(s)** will be auto-generated and fill {npc_count} of the {total} slots." if npc_count else ""
        human_needed = total - npc_count
        await interaction.followup.send(
            f"**Game roster ({total} total slots — {human_needed} human + {npc_count} NPC):**\n"
            f"{_build_roster_text(final_counts, self.all_roles)}"
            f"{npc_line}\n\n"
            f"Make sure exactly **{human_needed}** players have the participant role.",
            view=view, ephemeral=True)
        self.stop()

    def _render(self):
        focused = ""
        if self.selected_role:
            cnt = self.counts.get(self.selected_role, 0)
            focused = f"\n\n**Adjusting:** {_role_emoji(self.all_roles[self.selected_role]['team'])} **{self.selected_role}** — currently **×{cnt}**"
        npc_line = f"\n🤖 **NPCs in this game: {self.npc_count}** (count toward total slots)" if self.npc_count else ""
        res_note = ""
        if self.reservations:
            res_note = "\n⭐ **Reserved:** " + ", ".join(
                f"{rname} → {pname}" for _, pname, rname in self.reservations)
        return (
            "**🎮 Build Your Game Roster**\n"
            "Pick a role from the dropdown, then use **+1 / −1 / +10 / −10** to set how many.\n"
            "Use **NPC −/+** to add AI players (max 3). They count as player slots.\n"
            "Hit **✅ Confirm Roster** when done.\n"
            + focused
            + npc_line
            + res_note
            + "\n\n─────────────────\n"
            + _build_roster_text(self.counts, self.all_roles, self.reservations)
        )


class ReplayWarningView(View):
    def __init__(self, guild_id, final_counts, all_roles, npc_count=0):
        super().__init__(timeout=120)
        self.guild_id     = guild_id
        self.final_counts = final_counts
        self.all_roles    = all_roles
        self.npc_count    = npc_count
        yes = Button(label="Yes, use same roles", style=discord.ButtonStyle.danger)
        no  = Button(label="Go back",              style=discord.ButtonStyle.secondary)
        yes.callback = self.on_yes
        no.callback  = self.on_no
        self.add_item(yes)
        self.add_item(no)

    async def on_yes(self, interaction: discord.Interaction):
        view  = ConfirmStartView(self.guild_id, self.final_counts, self.all_roles, self.npc_count)
        total = sum(self.final_counts.values())
        human_needed = total - self.npc_count
        await interaction.response.edit_message(
            content=f"**Proceeding with same roles ({total} slots).**\n"
                    f"Make sure exactly **{human_needed}** players have the participant role.",
            view=view)

    async def on_no(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="Cancelled. Use `/start_game` to try again.", view=None)


def build_role_card(player: discord.Member, role_name: str, role_info: dict, font_style: str = "default") -> discord.Embed:
    """Build the rich private role-reveal embed sent to each player at game start."""
    team = role_info.get("team", "village")

    # Team theming
    if team == "wolf":
        color       = 0xC0392B   # deep red
        team_label  = "🐺 WOLF PACK"
        team_banner = (
            "```ansi\n"
            "\u001b[2;31m╔══════════════════════════════╗\n"
            "║     YOU ARE A WOLF           ║\n"
            "╚══════════════════════════════╝\n"
            "\u001b[0m```"
        )
        flavor = "*Lurk in the shadows. Hunt by night. Trust no one outside the den.*"
    elif team == "neutral":
        color       = 0xF39C12   # amber
        team_label  = "⚖️ NEUTRAL"
        team_banner = (
            "```ansi\n"
            "\u001b[2;33m╔══════════════════════════════╗\n"
            "║     YOU WALK ALONE           ║\n"
            "╚══════════════════════════════╝\n"
            "\u001b[0m```"
        )
        flavor = "*You answer to no one. Forge your own path and write your own fate.*"
    else:
        color       = 0x27AE60   # forest green
        team_label  = "🏘️ VILLAGE"
        team_banner = (
            "```ansi\n"
            "\u001b[2;32m╔══════════════════════════════╗\n"
            "║     YOU ARE A VILLAGER       ║\n"
            "╚══════════════════════════════╝\n"
            "\u001b[0m```"
        )
        flavor = "*The village is counting on you. Root out the wolves before it is too late.*"

    styled_name = apply_font(role_name.upper(), font_style)
    embed = discord.Embed(
        title=f"🎭  {styled_name}",
        color=color
    )
    embed.add_field(name="​", value=team_banner, inline=False)
    embed.add_field(
        name="📜 Your Ability",
        value=role_info.get("description") or "*No description provided.*",
        inline=False
    )
    embed.add_field(name="​", value=flavor, inline=False)
    embed.set_footer(
        text=f"{team_label}  ·  {player.display_name}  ·  This channel is private — only you and the mod can see it."
    )
    return embed


class ConfirmStartView(View):
    def __init__(self, guild_id, final_counts, all_roles, npc_count=0):
        super().__init__(timeout=300)
        self.guild_id     = guild_id
        self.final_counts = final_counts
        self.all_roles    = all_roles
        self.npc_count    = npc_count
        start_btn  = Button(label="🚀 Start Game", style=discord.ButtonStyle.green)
        cancel_btn = Button(label="Cancel",         style=discord.ButtonStyle.danger)
        start_btn.callback  = self.on_start
        cancel_btn.callback = self.on_cancel
        self.add_item(start_btn)
        self.add_item(cancel_btn)

    async def on_cancel(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content="❌ Game start cancelled.", view=None)

    async def on_start(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
        except Exception as e:
            return
        try:
            await self.launch_game(interaction)
        except Exception as e:
            import traceback
            try:
                await interaction.followup.send(f"❌ Game start error: {e}", ephemeral=True)
            except Exception:
                pass
        self.stop()

    async def launch_game(self, interaction: discord.Interaction):
        state     = cached_get_state(interaction.guild_id)
        p_role_id = state.get("participant_role_id")
        p_role    = interaction.guild.get_role(p_role_id) if p_role_id else None
        if not p_role:
            return await interaction.followup.send(
                "❌ Participant role not set. Use `/setup_roles` first.", ephemeral=True)

        needed       = sum(self.final_counts.values())
        npc_count    = self.npc_count
        human_needed = needed - npc_count

        # Fetch members with participant role — try multiple methods for reliability
        players = []

        # Method 1: p_role.members (works when cache is populated)
        players = [m for m in p_role.members if not m.bot]

        # Method 2: If that's empty, try chunking then re-checking
        if not players:
            try:
                await interaction.guild.chunk()
                players = [m for m in p_role.members if not m.bot]
            except Exception as e:
                print(f"[start_game] guild.chunk() failed: {e}")

        # Method 3: Fall back to full member cache scan
        if not players:
            players = [m for m in interaction.guild.members
                       if not m.bot and p_role in m.roles]


        if len(players) != human_needed:
            npc_line = f"\n*{npc_count} NPC slot(s) will fill the remaining spots automatically.*" if npc_count else ""
            return await interaction.followup.send(
                f"❌ Pool has **{human_needed}** human slot(s) but found **{len(players)}** "
                f"player(s) with the participant role.\n"
                f"Make sure exactly **{human_needed}** players have the participant role "
                f"(lobby auto-assigns it when players click Join Game).{npc_line}",
                ephemeral=True)

        # ── Apply reservations first ──────────────────────────────────────────
        reservations   = db_get_reservations(interaction.guild_id)
        res_map        = {r[0]: r[2] for r in reservations}  # player_id -> role_name
        assignments    = {}
        reserved_roles = []  # roles consumed by reservations

        # Assign reserved players their guaranteed roles
        for player in players:
            if player.id in res_map:
                reserved_role = res_map[player.id]
                if reserved_role in self.final_counts:
                    assignments[player.id] = reserved_role
                    reserved_roles.append(reserved_role)

        # Build remaining pool excluding reserved roles
        pool = []
        for role_name, count in self.final_counts.items():
            used = reserved_roles.count(role_name)
            pool.extend([role_name] * max(0, count - used))
        random.shuffle(pool)

        # Assign remaining players from pool
        remaining_players = [p for p in players if p.id not in assignments]
        for p, role in zip(remaining_players, pool):
            assignments[p.id] = role

        font = get_guild_font(interaction.guild_id)
        try:
            category = await interaction.guild.create_category(
                ch_name("Village Game", font, "🎮 "))
        except discord.errors.Forbidden:
            return await interaction.followup.send(
                "❌ Missing permissions to create channels. Make sure the bot has **Administrator** "
                "or **Manage Channels** + **Manage Roles** permissions on this server.", ephemeral=True)
        except Exception as e:
            return await interaction.followup.send(f"❌ Failed to create category: {e}", ephemeral=True)

        everyone = interaction.guild.default_role
        bot_me   = interaction.guild.me
        bot_ow   = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
        read_ow  = discord.PermissionOverwrite(view_channel=True, send_messages=False, read_messages=True)

        mod_role_id   = state.get("mod_role_id")
        mod_role      = interaction.guild.get_role(mod_role_id) if mod_role_id else None
        spec_role_id  = state.get("spectator_role_id")
        spec_role     = interaction.guild.get_role(spec_role_id) if spec_role_id else None
        dead_role_id  = state.get("dead_role_id")
        dead_role     = interaction.guild.get_role(dead_role_id) if dead_role_id else None

        # mod-log
        try:
            mod_log_ow = {everyone: discord.PermissionOverwrite(view_channel=False), bot_me: bot_ow}
            if mod_role:
                mod_log_ow[mod_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
            if spec_role: mod_log_ow[spec_role] = read_ow
            mod_log_ch = await category.create_text_channel(ch_name("mod-log",     font, "📋"), overwrites=mod_log_ow)
        except discord.errors.Forbidden:
            await category.delete()
            return await interaction.followup.send(
                "❌ Bot lacks permission to create channels in this server.\n"
                "Please give the bot **Administrator** permission and try again.", ephemeral=True)
        except Exception as e:
            await category.delete()
            return await interaction.followup.send(f"❌ Channel creation failed: {e}", ephemeral=True)

        # player-list
        player_list_ow = {everyone: read_ow, bot_me: bot_ow}
        if mod_role:  player_list_ow[mod_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
        if spec_role: player_list_ow[spec_role] = read_ow
        player_list_ch = await category.create_text_channel(ch_name("player-list",  font, "📋"), overwrites=player_list_ow)

        # role-list
        role_list_ow = {everyone: read_ow, bot_me: bot_ow}
        if mod_role:  role_list_ow[mod_role]  = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
        if spec_role: role_list_ow[spec_role] = read_ow
        role_list_ch = await category.create_text_channel(ch_name("role-list",    font, "📜"), overwrites=role_list_ow)

        # night-order — static reference channel, read-only for everyone
        night_ref_ow = {everyone: read_ow, bot_me: bot_ow}
        if spec_role: night_ref_ow[spec_role] = read_ow
        if mod_role:  night_ref_ow[mod_role]  = discord.PermissionOverwrite(view_channel=True, send_messages=True)
        night_ref_ch = await category.create_text_channel(ch_name("night-order", font, "🌙"), overwrites=night_ref_ow)

        night_order_embed = discord.Embed(
            title       = "🌙 Night Phase — Order of Operations",
            description = "Actions resolve in this order every night. Earlier phases take effect before later ones.",
            color       = 0x2C3060
        )
        night_order_embed.add_field(name="Phase 1 — Disguises & Transforms", value=(
            "🎭 **Shapeshifter** — takes another player's form *(Night 1 only)*\n"
            "🩸 **Bloodletter** — marks a target to appear as wolf to investigators for 2 nights"
        ), inline=False)
        night_order_embed.add_field(name="Phase 2 — Blocks", value=(
            "🐾 **Wolf Pup** — blocks one player's night ability"
        ), inline=False)
        night_order_embed.add_field(name="Phase 3 — Bonds & Binds", value=(
            "💘 **Cupid** — binds two players together for the night\n"
            "💔 **Dire Wolf** — secretly bonds with a mate *(Night 1 only)*"
        ), inline=False)
        night_order_embed.add_field(name="Phase 4 — Declarations", value=(
            "📢 **Agitator** — activates frenzy *(takes effect next day)*\n"
            "🪞 **Clone** — chooses who to clone"
        ), inline=False)
        night_order_embed.add_field(name="Phase 5 — Protection", value=(
            "💊 **Doctor** — saves a player *(1 use total)*\n"
            "🏥 **Surgeon** — saves a player *(3 uses total)*\n"
            "🏹 **Huntsman** — protects a player\n"
            "🧙 **Witch** — uses save potion"
        ), inline=False)
        night_order_embed.add_field(name="Phase 6 — Wolf Action", value=(
            "🐺 **Den Kill** — wolves vote on one target to eliminate\n"
            "👑 **Alpha / Elite Alpha** — attempts to convert a villager *(replaces den kill)*\n"
            "🐱 **Werekitten** — independent kill that replaces den kill *(max 2 uses)*"
        ), inline=False)
        night_order_embed.add_field(name="Phase 7 — Independent Kills", value=(
            "🤍 **White Wolf** — kills independently outside the pack\n"
            "🧙 **Witch** — uses poison potion\n"
            "🌑 **Shadow Wolf** — kills a voter from their kill list\n"
            "👻 **Wraith** — executes kill command if agreed *(marks phase is ongoing)*"
        ), inline=False)
        night_order_embed.add_field(name="Phase 8 — Post-Kill Reactions", value=(
            "🍿 **Pothead trigger** — if killed by den, wolves get a second kill\n"
            "💘 **Cupid bond trigger** — if one bonded player died, their partner dies too"
        ), inline=False)
        night_order_embed.add_field(name="Phase 9 — Investigations", value=(
            "🔮 **Seer** — Yes/No wolf check on one player\n"
            "🌀 **Medium** — Good / Bad / Neutral alignment check\n"
            "🦴 **Bloodhound** — exact role identification, result sent to den"
        ), inline=False)
        night_order_embed.add_field(name="Phase 10 — Information", value=(
            "😴 **Insomniac** — wolf role hint *(odd nights ≥ 3)*\n"
            "⚰️ **Gravedigger** — receives death details\n"
            "🔯 **Oracle** — receives Yes/No answer from mod"
        ), inline=False)
        night_order_embed.add_field(name="Passive Triggers", value=(
            "These resolve automatically based on game events — no night action required:\n"
            "**Traitor**, **Elder**, **Blessed Wolf**, **Lycan**, **Werekitten**, **Cursed**"
        ), inline=False)
        night_order_embed.set_footer(text="This order is fixed every night. Mods resolve actions in this sequence.")
        await night_ref_ch.send(embed=night_order_embed)

        # day-vote — players need send_messages=True to interact with buttons/dropdowns
        # We suppress their actual messages via the bot; the channel stays visually clean
        part_ow_vote = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
        day_vote_ow  = {everyone: read_ow, bot_me: bot_ow}
        if mod_role:  day_vote_ow[mod_role]  = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
        if p_role:    day_vote_ow[p_role]    = part_ow_vote
        if spec_role: day_vote_ow[spec_role] = part_ow_vote
        day_vote_ch = await category.create_text_channel(ch_name("day-vote",     font, "🗳️"), overwrites=day_vote_ow)

        # timeline
        timeline_ow = {everyone: read_ow, bot_me: bot_ow}
        if mod_role:  timeline_ow[mod_role]  = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
        if spec_role: timeline_ow[spec_role] = read_ow
        timeline_ch = await category.create_text_channel(ch_name("timeline",     font, "⏰"), overwrites=timeline_ow)

        # stats
        stats_ow = {everyone: read_ow, bot_me: bot_ow}
        if mod_role:  stats_ow[mod_role]  = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
        if spec_role: stats_ow[spec_role] = read_ow
        stats_ch = await category.create_text_channel(ch_name("stats",        font, "📊"), overwrites=stats_ow)

        # blood-board — all players can read, only bot/mod can post
        bb_ow = {
            everyone: discord.PermissionOverwrite(view_channel=True, send_messages=False),
            bot_me:   bot_ow,
        }
        if mod_role:  bb_ow[mod_role]  = discord.PermissionOverwrite(view_channel=True, send_messages=True)
        if spec_role: bb_ow[spec_role] = read_ow
        bb_game_ch = await category.create_text_channel(ch_name("blood-board", font, "🩸"), overwrites=bb_ow)

        # wolf-den (wolf chat — dead wolves get read-only)
        # Bloodhound and Wolf Pup are wolf-team but NOT in the den
        DENY_DEN = {"Bloodhound", "Wolf Pup"}
        wolf_players = [p for p in players
                        if get_team(interaction.guild_id, assignments[p.id]) == "wolf"
                        and assignments[p.id] not in DENY_DEN]
        neutral_players = [p for p in players if get_team(interaction.guild_id, assignments[p.id]) == "neutral"]
        wolf_ow = {everyone: discord.PermissionOverwrite(view_channel=False), bot_me: bot_ow}
        if mod_role:  wolf_ow[mod_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
        for wp in wolf_players:
            wolf_ow[wp] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
        if spec_role: wolf_ow[spec_role] = read_ow
        wolf_ch = await category.create_text_channel(ch_name("wolf-den",     font, "🐺"), overwrites=wolf_ow)

        # wolf-vote — wolves + den-excluded wolf roles (Bloodhound, Wolf Pup) can all vote
        den_excluded = [p for p in players
                        if get_team(interaction.guild_id, assignments[p.id]) == "wolf"
                        and assignments[p.id] in DENY_DEN]
        wolf_vote_ch = None  # wolf-vote channel removed

        # ghost-chat
        ghost_ow = {everyone: discord.PermissionOverwrite(view_channel=False), bot_me: bot_ow}
        if mod_role:  ghost_ow[mod_role]  = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
        if spec_role: ghost_ow[spec_role] = read_ow
        ghost_ch = await category.create_text_channel(ch_name("ghost-chat",   font, "👻"), overwrites=ghost_ow)

        # wraith-den — only if Wraiths are in the game
        wraith_players = [p for p in players if assignments.get(p.id) == "Wraith"]
        wraith_ch      = None
        if wraith_players:
            wraith_ow = {everyone: discord.PermissionOverwrite(view_channel=False), bot_me: bot_ow}
            for wp in wraith_players:
                wraith_ow[wp] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
            if mod_role:  wraith_ow[mod_role]  = discord.PermissionOverwrite(view_channel=True, send_messages=True)
            if spec_role: wraith_ow[spec_role] = read_ow
            wraith_ch = await category.create_text_channel(ch_name("wraith-den", font, "👻"), overwrites=wraith_ow)

        # win-tracker
        # Win tracker — mod only, never visible to players
        win_tracker_ow = {everyone: discord.PermissionOverwrite(view_channel=False), bot_me: bot_ow}
        if mod_role:  win_tracker_ow[mod_role]  = discord.PermissionOverwrite(view_channel=True, send_messages=False, read_messages=True)
        if spec_role: win_tracker_ow[spec_role] = read_ow
        win_tracker_ch = await category.create_text_channel(ch_name("win-tracker", font, "⚖️"), overwrites=win_tracker_ow)

        # village-chat — always created fresh inside the game category
        # This ensures multi-server support and clean channel permissions each game
        village_chat_ow = {
            everyone: discord.PermissionOverwrite(view_channel=False),
            bot_me:   bot_ow,
        }
        if p_role:    village_chat_ow[p_role]    = discord.PermissionOverwrite(view_channel=True,  send_messages=True)
        if spec_role: village_chat_ow[spec_role] = discord.PermissionOverwrite(view_channel=True,  send_messages=False)
        if mod_role:  village_chat_ow[mod_role]  = discord.PermissionOverwrite(view_channel=True,  send_messages=True)
        if dead_role: village_chat_ow[dead_role] = discord.PermissionOverwrite(view_channel=True,  send_messages=False)
        village_chat_ch = await category.create_text_channel(ch_name("village-chat", font, "💬"), overwrites=village_chat_ow)

        # private player channels
        player_channels = {}
        for player in players:
            role_name = assignments[player.id]
            role_info = get_role_info(interaction.guild_id, role_name)
            priv_ch_name = f"🔒{player.display_name}-{role_name}".lower().replace(" ", "-")
            ch_ow = {
                everyone: discord.PermissionOverwrite(view_channel=False),
                player:   discord.PermissionOverwrite(view_channel=True, send_messages=True),
                bot_me:   bot_ow
            }
            if mod_role:
                ch_ow[mod_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
            if spec_role:
                ch_ow[spec_role] = read_ow
            ch = await category.create_text_channel(priv_ch_name, overwrites=ch_ow)
            player_channels[player.id] = ch.id
            embed    = build_role_card(player, role_name, role_info, font)
            role_msg = await ch.send(player.mention, embed=embed)
            try:
                await role_msg.pin()
            except Exception:
                pass

            # Orientation message — what this channel is for
            night_order_pos = {
                "Seer": "Phase 9 — Investigations", "Medium": "Phase 9 — Investigations",
                "Doctor": "Phase 5 — Protection", "Surgeon": "Phase 5 — Protection",
                "Huntsman": "Phase 5 — Protection",
                "Alpha": "Phase 6 — Wolf Action", "Elite Alpha": "Phase 6 — Wolf Action",
                "Witch": "Phase 5 & 7", "Bloodhound": "Phase 9 — Investigations",
                "Wolf Pup": "Phase 2 — Blocks", "Agitator": "Phase 4 — Declarations",
                "Governor": "Day ability — you will receive a button in this channel when the vote opens",
                "Hermit":   "Day ability — you will receive a button in this channel when the vote opens",
            }.get(role_name, "See /roleinfo for night order")

            orient_embed = discord.Embed(
                title       = "📋 Your Private Channel",
                description = (
                    f"This channel is **only visible to you and the mods**.\n\n"
                    f"Use it to:\n"
                    f"• Submit night actions using the buttons sent each night\n"
                    f"• Communicate privately with the mod team\n"
                    f"• Track your investigation notes\n\n"
                    f"**Key commands:**\n"
                    f"`/my_role` — view your role card again\n"
                    f"`/action [message]` — send a private message to the mods\n"
                    f"`/pass_night` — pass your night action if you have no ability\n"
                    f"`/tracker` — open your personal investigation spreadsheet\n"
                    f"`/roleinfo {role_name}` — full details on your role\n\n"
                    f"**Your night order position:** {night_order_pos}\n\n"
                    f"*Good luck. Whisperfall is watching.*"
                ),
                color = 0x2C3060
            )
            await ch.send(embed=orient_embed)

            if role_name == "Cursed":
                await ch.send(fmt("⚠️ You appear as Village to investigators.\nIf attacked by wolves, you join them instead of dying."))

            if role_name == "Clone":
                clone_embed = discord.Embed(
                    title       = "🪞 Clone — Role Rules",
                    description = (
                        "You are the Clone. Your power activates only when someone dies.\n\n"
                        "**Night 1:** Use your night action button to choose one player to watch.\n"
                        "This is your only night action — choose wisely.\n\n"
                        "**When your target dies** (for any reason — wolf kill, day vote, mod kill):\n"
                        "• You immediately and permanently inherit their exact role\n"
                        "• You receive their full ability\n"
                        "• If their role is wolf-aligned, you gain den access\n"
                        "• Your private channel will be updated with your new role\n\n"
                        "**Until then:** You are effectively a Villager.\n\n"
                        "*You appear as Villager to all investigators.*"
                    ),
                    color = 0x9B59B6
                )
                await ch.send(embed=clone_embed)

            if role_name == "Werekitten":
                wk_embed = discord.Embed(
                    title       = "🐱 Werekitten — Role Rules",
                    description = (
                        "You are the Werekitten. The village has no idea what's coming.\n\n"
                        "**What you can do:**\n"
                        "• Submit a kill each night using your night action button\n"
                        "• Your kill bypasses the **Sheriff** — too adorable to shoot\n"
                        "• Your kill bypasses the **Huntsman's** protection — even guarded players aren't safe\n\n"
                        "**What still stops you:**\n"
                        "• **Doctor** and **Surgeon** saves still apply — if your target is saved, they survive\n\n"
                        "**Investigation:**\n"
                        "• You appear as **Villager** to the Seer\n"
                        "• You appear as **Villager** to the Medium\n\n"
                        "**Den limit:**\n"
                        "• The wolf den can use your kill ability a maximum of **2 times per game**\n"
                        "• Coordinate with the pack on when to use it\n\n"
                        "**If voted out:**\n"
                        "• Your true identity is revealed\n"
                        "• The entire village is devastated\n"
                        "• All night actions are silenced that night\n\n"
                        "*Use your cuteness wisely.*"
                    ),
                    color = 0xFF69B4
                )
                await ch.send(embed=wk_embed)

        # ── Wraith den welcome ───────────────────────────────────────────
        if wraith_players and wraith_ch:
            w1 = wraith_players[0] if len(wraith_players) > 0 else None
            w2 = wraith_players[1] if len(wraith_players) > 1 else None
            db_set_wraith_state(
                interaction.guild_id,
                wraith1_id=w1.id if w1 else 0,
                wraith2_id=w2.id if w2 else 0,
                den_channel_id=wraith_ch.id,
                kill_agreed=0, kill_used=0, kill_night=0
            )
            wraith_names = " & ".join(p.display_name for p in wraith_players)
            wraith_embed = discord.Embed(
                title       = "👻 Welcome to the Den",
                description = (
                    f"**The Wraiths:** {wraith_names}\n\n"
                    "You are a third faction. You do not fight for the village or the wolves.\n"
                    "You hunt for yourselves.\n\n"
                    "**Each night:** Use your night action button to mark one player silently.\n"
                    "Marks persist until you issue the Kill Command.\n\n"
                    "**Kill Command:** When both of you agree, replace your nightly mark with the "
                    "Kill Command — all marked players die simultaneously. One-time ability.\n\n"
                    "**Coordination:** Signal your Kill Command readiness via the button — "
                    "your partner will see the alert here. Both must agree for it to fire.\n\n"
                    "**Win condition:** You win when you are alive and equal or outnumber "
                    "**both** the village and wolf teams simultaneously.\n\n"
                    "*Do not reveal yourselves. The village does not know you exist.*"
                ),
                color = 0x4B0082
            )
            wraith_embed.set_footer(text="Appears as Neutral to all investigators.")
            await wraith_ch.send(embed=wraith_embed)

        if wolf_players:
            wolf_names = ", ".join(p.display_name for p in wolf_players)
            den_embed = discord.Embed(
                title       = "🐺 Welcome to the Den",
                description = (
                    f"**Your pack:** {wolf_names}\n\n"
                    f"This is your private channel. Only wolves can see it.\n\n"
                    f"**How the kill works:**\n"
                    f"Each night, discuss your target here. The **Alpha or Elite Alpha** "
                    f"submits the final kill using their night action button in their private channel. "
                    f"If no Alpha is in the game, any wolf with a night action submits the kill.\n\n"
                    f"**Turns:**\n"
                    f"Instead of killing, the Alpha or Elite Alpha can attempt to **turn** a villager. "
                    f"A successful turn converts them to the wolf team. "
                    f"The pack cannot kill AND turn on the same night — it is one or the other.\n\n"
                    f"**Rules:**\n"
                    f"• Do not reveal your role publicly — deflect, deny, deceive\n"
                    f"• Coordinate votes during the day to protect each other\n"
                    f"• Win condition: wolves equal or outnumber the village\n\n"
                    f"*Good hunting.*"
                ),
                color = 0xC0392B
            )
            den_embed.set_footer(text="This channel is only visible to wolves and the mod.")
            await wolf_ch.send(embed=den_embed)

        db_save_assignments(interaction.guild_id, assignments, player_channels)

        # ── Initialise ability uses for limited-use roles ──────────────────
        ABILITY_USES = {"Doctor": 1, "Surgeon": 3, "Agitator": 1}
        for pid, role in assignments.items():
            if role in ABILITY_USES:
                db_init_ability_uses(interaction.guild_id, pid, role, ABILITY_USES[role])

        # ── Auto-create NPCs from roster ──────────────────────────────────
        if npc_count > 0:
            await interaction.followup.send(
                f"⏳ Generating {npc_count} NPC(s)... this may take a moment.",
                ephemeral=True)
            for _ in range(npc_count):
                try:
                    # Fetch random identity — fallback to generated name if API unavailable
                    first, last, nationality, avatar_url = None, None, "US", ""
                    try:
                        timeout_ru = aiohttp.ClientTimeout(total=8)
                        async with aiohttp.ClientSession(timeout=timeout_ru) as session:
                            async with session.get(
                                "https://randomuser.me/api/?nat=us,gb,au,ca&inc=name,picture"
                            ) as resp:
                                data = await resp.json()
                        person      = data["results"][0]
                        first       = person["name"]["first"]
                        last        = person["name"]["last"]
                        nationality = person["nat"]
                        avatar_url  = person["picture"]["large"]
                    except Exception as _e:
                        # Fallback names if randomuser.me is unreachable
                        _fallback_names = [
                            ("Morgan","Ellis"),("Jordan","Hayes"),("Casey","Marsh"),
                            ("Riley","Quinn"),("Avery","Stone"),("Blake","Cross"),
                            ("Cameron","Drake"),("Dakota","Hunt"),("Emery","Lake"),
                            ("Finley","Ward"),
                        ]
                        first, last = random.choice(_fallback_names)
                    npc_name = f"{first} {last}"

                    # Generate personality with timeout guard
                    try:
                        personality, backstory = await asyncio.wait_for(
                            _generate_npc_profile(first, last, nationality), timeout=20)
                    except asyncio.TimeoutError:
                        personality = "Observant and strategic, rarely speaks without purpose."
                        backstory   = "A regular player who takes the game seriously."

                    # Pick next unassigned role from THIS GAME'S pool only (final_counts)
                    all_assigned = list(assignments.values())
                    existing_npcs = db_get_npcs(interaction.guild_id)
                    npc_roles    = [n["role_name"] for n in existing_npcs]
                    all_assigned += npc_roles
                    # Expand final_counts into a full pool respecting counts
                    game_pool = []
                    for role_name, count in self.final_counts.items():
                        game_pool.extend([role_name] * count)
                    available = [r for r in game_pool if r not in all_assigned]
                    if not available:
                        available = list(self.final_counts.keys())
                    chosen_role = random.choice(available) if available else "Villager"

                    # Create private channel
                    existing_npcs2 = db_get_npcs(interaction.guild_id)
                    npc_id = -(len(existing_npcs2) + 1) * 1000 - interaction.guild_id % 1000
                    priv_ch_name = f"🔒{npc_name}-{chosen_role}".lower().replace(" ", "-")
                    ch_ow = {
                        everyone: discord.PermissionOverwrite(view_channel=False),
                        bot_me:   bot_ow,
                    }
                    if mod_role:
                        ch_ow[mod_role] = discord.PermissionOverwrite(
                            view_channel=True, send_messages=True, read_messages=True)
                    if spec_role:
                        ch_ow[spec_role] = read_ow
                    npc_priv_ch = await category.create_text_channel(priv_ch_name, overwrites=ch_ow)

                    # Create webhook in village-chat
                    npc_wh = await village_chat_ch.create_webhook(name=f"{npc_name} •")

                    # Save to DB
                    gs_team = get_team(interaction.guild_id, chosen_role)
                    gs_bluff = ""
                    if gs_team == "wolf":
                        all_roles_gs  = cached_load_roles(interaction.guild_id)
                        vill_roles_gs = [r["name"] for r in all_roles_gs
                                         if r.get("team") == "village"
                                         and r["name"] not in ("Villager","Drunk","Village Idiot")]
                        gs_bluff = random.choice(vill_roles_gs) if vill_roles_gs else "Villager"
                    db_save_npc(interaction.guild_id, npc_id, npc_name, avatar_url,
                                personality, backstory, chosen_role,
                                npc_priv_ch.id, npc_wh.id, npc_wh.token,
                                bluff_role=gs_bluff, team_hint=gs_team)

                    # Save to player_assignments
                    conn_npc = sqlite3.connect(DB_FILE)
                    c_npc    = conn_npc.cursor()
                    c_npc.execute("INSERT OR REPLACE INTO player_assignments VALUES (?,?,?,1,?)",
                                  (interaction.guild_id, npc_id, chosen_role, npc_priv_ch.id))
                    conn_npc.commit()
                    conn_npc.close()

                    # If wolf, note den access in mod-log (NPCs use webhooks, not Discord perms)
                    if get_team(interaction.guild_id, chosen_role) == "wolf":
                        await post_mod_log(interaction.guild,
                            f"🤖 NPC wolf **{npc_name}** ({chosen_role}) added — has den awareness via NPC system.")

                    # Send role card to private channel — sent by bot so it renders as a proper embed
                    role_info_npc = get_role_info(interaction.guild_id, chosen_role)
                    embed_npc     = build_role_card(
                        interaction.guild.me, chosen_role, role_info_npc, font)
                    embed_npc.set_footer(
                        text=f"🤖 NPC  ·  {npc_name}  ·  This channel is private — only mods can see it.")
                    embed_npc.set_author(name=f"{npc_name} •", icon_url=avatar_url)
                    await npc_priv_ch.send(embed=embed_npc)

                    # Log tell to mod-log
                    tell_idx  = abs(npc_id) % len(NPC_TELLS)
                    tell_desc = NPC_TELLS[tell_idx]["tell"]
                    await post_mod_log(interaction.guild,
                        f"🤖 **NPC Auto-Created** — {npc_name}\n"
                        f"**Role:** {chosen_role}\n"
                        f"**Personality:** {personality}\n"
                        f"**Backstory:** {backstory}\n"
                        f"**🎭 Tell (mod only):** {tell_desc}")

                    # Start proactive loop
                    safe_task(_npc_proactive_loop(interaction.guild, interaction.guild_id), "npc_proactive")
                    safe_task(_npc_idle_check_loop(interaction.guild, interaction.guild_id), "npc_idle")
                    safe_task(_npc_vote_watch_loop(interaction.guild, interaction.guild_id), "npc_vote_watch")

                    # Delayed introduction in village-chat
                    async def _npc_intro(wh_id, wh_tok, av, nm):
                        await asyncio.sleep(random.uniform(10, 30))
                        intros = [
                            "Hey everyone, just got in. Ready to play!",
                            "Finally here. Let's do this.",
                            "Hi all! Glad to be here.",
                            "Just joined. Looks like a good group.",
                            "Hey! Ready to find some wolves.",
                        ]
                        async with aiohttp.ClientSession() as s:
                            w = discord.Webhook.partial(wh_id, wh_tok, session=s)
                            await w.send(random.choice(intros),
                                         username=f"{nm} •", avatar_url=av)
                    safe_task(_npc_intro(npc_wh.id, npc_wh.token, avatar_url, npc_name), "npc_intro")

                except Exception as e:
                    print(f"NPC auto-create error: {e}")
                    await post_mod_log(interaction.guild,
                        f"⚠️ Failed to auto-create NPC #{_ + 1}: {e}")
        rows = db_get_assignments(interaction.guild_id)

        # Persistent embeds
        pl_msg  = await player_list_ch.send(embed=build_player_list_embed(interaction.guild, rows, []))
        all_roles_map = {r["name"]: r for r in self.all_roles} if isinstance(self.all_roles, list) else self.all_roles
        await post_role_list_embeds(role_list_ch, self.final_counts, all_roles_map)
        rl_msg  = await role_list_ch.send("📜 *Role descriptions posted above. Who has each role is secret.*")
        dv_view = DayVoteView(interaction.guild_id)
        dv_msg  = await day_vote_ch.send(embed=build_day_vote_embed(interaction.guild, [], rows), view=dv_view)

        night_dur = state.get("night_duration", 43200)
        day_dur   = state.get("day_duration", 43200)

        db_set_state(
            interaction.guild_id,
            phase="night",
            category_id=category.id,
            wolf_channel_id=wolf_ch.id,
            wolf_vote_channel_id=None,
            ghost_channel_id=ghost_ch.id,
            mod_log_channel_id=mod_log_ch.id,
            bb_channel_id=bb_game_ch.id,
            player_list_ch_id=player_list_ch.id,
            role_list_ch_id=role_list_ch.id,
            day_vote_ch_id=day_vote_ch.id,
            timeline_ch_id=timeline_ch.id,
            stats_ch_id=stats_ch.id,
            player_list_msg_id=pl_msg.id,
            role_list_msg_id=rl_msg.id,
            day_vote_msg_id=dv_msg.id,
            wolf_vote_msg_id=None,
            day_vote_end_time=None,
            village_chat_ch_id=village_chat_ch.id,
            win_tracker_ch_id=win_tracker_ch.id,
            mod_dashboard_msg_id=None,
            night_bb_done=0,
            day_bb_done=0,
            investigations_done=0,
            time_lord_triggered=0
        )

        # Timeline embed
        fresh_state = cached_get_state(interaction.guild_id)
        tl_msg = await timeline_ch.send(embed=build_timeline_embed(interaction.guild_id, fresh_state))
        db_set_state(interaction.guild_id, timeline_msg_id=tl_msg.id)

        # Win tracker embed
        await win_tracker_ch.send(embed=build_win_tracker_embed(interaction.guild, interaction.guild_id))

        # Stats embed
        stat_rows = db_get_stats(interaction.guild_id)
        await stats_ch.send(embed=build_stats_embed(interaction.guild, stat_rows))

        # Save role set for replay protection
        db_save_last_roles(interaction.guild_id, self.final_counts)

        # Start at Night 1
        db_increment_night(interaction.guild_id)
        await log_event(interaction.guild, "Night 1", "🎮 Game started — Night 1 begins")

        role_list = ", ".join(f"{k} ×{v}" for k, v in self.final_counts.items())
        await interaction.followup.send(
            f"✅ **Game started!** {len(players)} players.\n"
            f"**Roles:** {role_list}\n"
            f"**Day:** {day_dur//3600:.1f}h  **Night:** {night_dur//3600:.1f}h",
            ephemeral=True)
        await mod_log_ch.send(
            f"🎮 **Game started** — {len(players)} players\n"
            + "\n".join(f"• <@{pid}>: **{role}**" for pid, role in assignments.items()))

        # ── Post Blood Board channel header ──────────────────────────────
        await bb_game_ch.send(
            embed=discord.Embed(
                title       = "🩸 The Blood Board",
                description = (
                    "*Every morning the village gathers here to read what the night left behind.*\n\n"
                    "*The board does not lie. It does not always tell you everything. "
                    "But it remembers.*"
                ),
                color = 0x8B0000
            )
        )

        # ── Generate pre-game Blood Board for mod approval ────────────────
        safe_task(_post_pregame_bloodboard(
            interaction.guild, interaction.guild_id, players, assignments))

        # ── Prompt mod to start Night 1 ───────────────────────────────────
        view = StartNightPromptView(interaction.guild_id)
        await mod_log_ch.send(
            embed=discord.Embed(
                title       = "🌙 Ready to begin Night 1?",
                description = f"All {len(players)} players have received their roles.\nClick below when you are ready to start the night phase.",
                color       = 0x2C3060
            ),
            view=view
        )

        # ── Post and pin the mod dashboard ───────────────────────────────
        invalidate_cache(interaction.guild_id)
        safe_task(update_mod_dashboard(interaction.guild), "dashboard_init")


@tree.command(name="start_game", description="Pick roles and launch a new game")
@is_mod()
async def start_game(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    if game_active(interaction.guild_id):
        return await interaction.followup.send(
            "❌ A game is already active. Use `/end_game` first.", ephemeral=True)
    roles = db_load_roles(interaction.guild_id)
    if not roles:
        return await interaction.followup.send(
            "❌ No roles saved. Use `/add_role` first.", ephemeral=True)
    current = get_guild_font(interaction.guild_id)
    sample  = apply_font("mod-log  |  wolf-den  |  ghost-chat", current)
    view    = FontPickerView(interaction.guild_id, roles)
    await interaction.followup.send(
        f"**🎮 Start a New Game — Step 1 of 2: Channel Font**\n"
        f"Current style: **{FONT_STYLES[current]['label']}** — `{sample}`\n\n"
        f"Pick a font style for channel names, then hit **✅ Use & Build Roster** to continue.",
        view=view, ephemeral=True)

# ====================== END GAME ======================
@tree.command(name="end_game", description="End the game and delete all game channels")
@is_mod()
async def end_game(interaction: discord.Interaction):
    # Acknowledge immediately regardless of state
    if not interaction.response.is_done():
        await interaction.response.defer(ephemeral=True)
    state = cached_get_state(interaction.guild_id)
    if not state or not state.get("category_id"):
        return await interaction.followup.send("No active game to end.", ephemeral=True)

    # DB backup before cleanup
    try:
        import json, time as _t, os
        backup = {
            "timestamp"   : int(_t.time()),
            "guild_id"    : str(interaction.guild_id),
            "state"       : {k: str(v) for k, v in (state or {}).items()},
            "assignments" : [[str(x) for x in row] for row in db_get_assignments(interaction.guild_id)],
            "log"         : [[str(x) for x in row] for row in db_get_log(interaction.guild_id)],
        }
        # Safe filename — no colons or special chars, works on Windows and Railway
        backup_filename = f"game_backup_{interaction.guild_id}_{int(_t.time())}.json"
        # Save in script directory
        backup_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), backup_filename)
        with open(backup_path, "w", encoding="utf-8") as f:
            json.dump(backup, f, indent=2)
        print(f"[end_game] Backup saved: {backup_path}")
    except Exception as e:
        print(f"[end_game] Backup failed: {e}")

    await interaction.followup.send("⏳ Ending game — deleting channels now. This may take a moment.", ephemeral=True)

    async def _do_end():
        # ── Update NPC identity stats ──────────────────────────────────────
        try:
            npcs_end    = db_get_npcs(interaction.guild_id)
            winner_team = (db_get_state(interaction.guild_id) or {}).get("last_winner", "")
            for npc in npcs_end:
                npc_team = get_team(interaction.guild_id, npc["role_name"])
                won      = (winner_team == npc_team)
                db_update_npc_identity_stats(interaction.guild_id, npc["name"], npc_team, won)
                if npc["is_alive"] and won:
                    db_add_npc_legacy_note(interaction.guild_id, npc["name"],
                        f"Survived and won as {npc['role_name']}")
                elif not npc["is_alive"]:
                    db_add_npc_legacy_note(interaction.guild_id, npc["name"],
                        f"Was eliminated while playing {npc['role_name']}")
        except Exception as e:
            print(f"[end_game] NPC identity update error: {e}")

        # ── Auto end-game summary ─────────────────────────────────────────
        try:
            rows      = db_get_assignments(interaction.guild_id)
            log       = db_get_log(interaction.guild_id)
            night_num = db_get_night_num(interaction.guild_id)
            vc_ch     = interaction.guild.get_channel(state.get("village_chat_ch_id") or 0)

            village_winners = [r for r in rows if r[2] == 1 and get_team(interaction.guild_id, r[1]) == "village"]
            wolf_winners    = [r for r in rows if r[2] == 1 and get_team(interaction.guild_id, r[1]) == "wolf"]
            eliminated      = [r for r in rows if r[2] == 0]

            def get_name(pid):
                m = interaction.guild.get_member(pid)
                return m.display_name if m else str(pid)

            embed = discord.Embed(
                title       = "📖 Game Over — Final Summary",
                description = f"*{night_num} nights passed in Whisperfall.*",
                color       = 0xF1C40F
            )

            # Survivors
            if village_winners:
                embed.add_field(
                    name  = "🏘️ Village Survivors",
                    value = "\n".join(f"{get_name(r[0])} — {r[1]}" for r in village_winners) or "None",
                    inline= False)
            if wolf_winners:
                embed.add_field(
                    name  = "🐺 Wolf Survivors",
                    value = "\n".join(f"{get_name(r[0])} — {r[1]}" for r in wolf_winners) or "None",
                    inline= False)

            # Eliminated
            if eliminated:
                embed.add_field(
                    name  = "💀 Eliminated",
                    value = "\n".join(f"{get_name(r[0])} — {r[1]}" for r in eliminated[:20]) or "None",
                    inline= False)

            embed.add_field(name="⏱️ Duration", value=f"{night_num} nights", inline=True)
            embed.add_field(name="👥 Players",  value=str(len(rows)),        inline=True)
            embed.set_footer(text="Whisperfall thanks you for playing.")

            if vc_ch:
                await vc_ch.send(embed=embed)
        except Exception as e:
            print(f"[end_game] Summary failed: {e}")

        cat = interaction.guild.get_channel(state["category_id"])
        # Clean up NPC webhooks before deleting channels
        npcs       = db_get_npcs(interaction.guild_id)
        village_ch = interaction.guild.get_channel(state.get("village_chat_ch_id") or 0)
        if village_ch:
            try:
                wh_list = await village_ch.webhooks()
                for wh in wh_list:
                    if any(wh.id == n["webhook_id"] for n in npcs):
                        await wh.delete()
            except Exception:
                pass
            # Purge all messages in village chat
            try:
                await village_ch.purge(limit=None)
            except Exception as e:
                print(f"Village chat purge error: {e}")
        for npc in npcs:
            _npc_webhooks.pop((interaction.guild_id, npc["npc_id"]), None)
        db_delete_npcs(interaction.guild_id)
        # Delete claim channels first (not in category)
        claim_ch_ids = db_get_claim_channels(interaction.guild_id)
        for ch_id in claim_ch_ids:
            ch_claim = interaction.guild.get_channel(ch_id)
            if ch_claim:
                try: await ch_claim.delete()
                except: pass
        db_clear_claim_channels(interaction.guild_id)

        if cat:
            for ch in cat.channels:
                try: await ch.delete()
                except: pass
            try: await cat.delete()
            except: pass

        # Blood Board channel is inside the game category — deleted with it
        # No separate purge needed

        db_clear_state(interaction.guild_id)
        await set_bot_status("💤 No active game")
        try:
            await interaction.followup.send("✅ Game ended. All channels deleted.", ephemeral=True)
        except Exception:
            pass  # Followup token may have expired — that's fine, game is ended

    async def _do_end_safe():
        try:
            await _do_end()
        except Exception as e:
            import traceback
            traceback.print_exc()
            try:
                await interaction.followup.send(f"❌ End game error: {e}", ephemeral=True)
            except Exception:
                pass

    safe_task(_do_end_safe(), "do_end")

# ====================== PLAYER MANAGEMENT ======================
@tree.command(name="add_player", description="Add a player to the current game mid-session")
@is_mod()
@app_commands.describe(player="Player to add", role_name="Role to assign them")
async def add_player(interaction: discord.Interaction, player: discord.Member, role_name: str):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    rows = db_get_assignments(interaction.guild_id)
    if any(r[0] == player.id for r in rows):
        return await interaction.response.send_message(
            f"**{player.display_name}** is already in the game.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)
    state     = cached_get_state(interaction.guild_id)
    everyone  = interaction.guild.default_role
    bot_me    = interaction.guild.me
    bot_ow    = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
    mod_role  = interaction.guild.get_role(state.get("mod_role_id") or 0)
    cat       = interaction.guild.get_channel(state.get("category_id") or 0)

    role_info = get_role_info(interaction.guild_id, role_name)
    priv_ch_name = f"🔒{player.display_name}-{role_name}".lower().replace(" ", "-")
    ch_ow = {
        everyone: discord.PermissionOverwrite(view_channel=False),
        player:   discord.PermissionOverwrite(view_channel=True, send_messages=True),
        bot_me:   bot_ow
    }
    if mod_role:
        ch_ow[mod_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)

    # Add spectator read access to channel
    spec_role = interaction.guild.get_role(state.get("spectator_role_id") or 0)
    read_ow   = discord.PermissionOverwrite(view_channel=True, send_messages=False, read_messages=True)
    if spec_role:
        ch_ow[spec_role] = read_ow

    ch = await cat.create_text_channel(priv_ch_name, overwrites=ch_ow)

    # Add to DB
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO player_assignments VALUES (?,?,?,1,?)",
              (interaction.guild_id, player.id, role_name, ch.id))
    conn.commit()
    conn.close()
    invalidate_cache(interaction.guild_id)

    # Remove spectator role if they had it, add participant role
    p_role = interaction.guild.get_role(state.get("participant_role_id") or 0)
    if spec_role:
        try: await player.remove_roles(spec_role)
        except: pass
    if p_role:
        try: await player.add_roles(p_role)
        except: pass

    # If wolf role — grant wolf den access (no wolf-vote channel anymore)
    if get_team(interaction.guild_id, role_name) == "wolf" and role_name not in {"Bloodhound", "Wolf Pup"}:
        wolf_ch = interaction.guild.get_channel(state.get("wolf_channel_id") or 0)
        if wolf_ch:
            await wolf_ch.set_permissions(player, view_channel=True, send_messages=True)
            await wolf_ch.send(fmt(f"🐺 **{player.display_name}** has joined the pack!"))

    # Send role card using standard build_role_card
    font  = get_guild_font(interaction.guild_id)
    embed = build_role_card(player, role_name, role_info, font)
    await ch.send(player.mention, embed=embed)

    # If night phase — send night action prompt
    cur_state = db_get_state(interaction.guild_id) or {}
    if cur_state.get("phase") == "night":
        night_num   = db_get_night_num(interaction.guild_id)
        rows_now    = db_get_assignments(interaction.guild_id)
        alive_mbrs  = [interaction.guild.get_member(r[0]) for r in rows_now
                       if r[2] == 1 and interaction.guild.get_member(r[0])]
        night_view  = get_night_view(interaction.guild_id, player.id, role_name, alive_mbrs)
        if night_view:
            await ch.send(fmt(f"🌙 Night {night_num} is active — submit your action:"), view=night_view)
            status_view = NightStatusView(interaction.guild_id, player.id, role_name, night_num)
            await ch.send(fmt("Let the mod know your intent tonight:"), view=status_view)
        elif role_name not in NIGHT_NO_BUTTON_ROLES:
            status_view = NightStatusView(interaction.guild_id, player.id, role_name, night_num)
            await ch.send(fmt(f"🌙 Night {night_num} is active."), view=status_view)
        else:
            await ch.send(fmt(f"🌙 Night {night_num} is active. Sleep tight — await the morning."))

    # Refresh player list and win tracker
    await refresh_player_list(interaction.guild)
    await refresh_win_tracker(interaction.guild)

    phase = cached_get_state(interaction.guild_id).get("phase", "day").capitalize()
    await log_event(interaction.guild, phase,
                    f"➕ **{player.display_name}** has joined the game.")
    await post_mod_log(interaction.guild,
        f"➕ **{player.display_name}** added mid-game\n"
        f"**Role:** {role_name}  **Team:** {get_team(interaction.guild_id, role_name).capitalize()}")
    await interaction.followup.send(
        f"✅ **{player.display_name}** added to the game as **{role_name}**.", ephemeral=True)


@tree.command(name="kick_player", description="Remove a player from the game without ending it")
@is_mod()
@app_commands.describe(player="Player to remove")
async def kick_player(interaction: discord.Interaction, player: discord.Member):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    rows = db_get_assignments(interaction.guild_id)
    assignment = next((r for r in rows if r[0] == player.id), None)
    if not assignment:
        return await interaction.response.send_message(
            f"**{player.display_name}** is not in the game.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)
    state = cached_get_state(interaction.guild_id)

    # Remove from DB
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("DELETE FROM player_assignments WHERE guild_id=? AND player_id=?",
              (interaction.guild_id, player.id))
    conn.commit()
    conn.close()

    # Remove their private channel
    priv_ch = interaction.guild.get_channel(assignment[3] or 0)
    if priv_ch:
        try: await priv_ch.delete()
        except: pass

    # Remove roles
    p_role   = interaction.guild.get_role(state.get("participant_role_id") or 0)
    dead_role= interaction.guild.get_role(state.get("dead_role_id") or 0)
    try:
        if p_role and p_role in player.roles:     await player.remove_roles(p_role)
        if dead_role and dead_role in player.roles: await player.remove_roles(dead_role)
    except: pass

    # Remove from wolf channels if wolf
    if get_team(interaction.guild_id, assignment[1]) == "wolf":
        wolf_ch      = interaction.guild.get_channel(state.get("wolf_channel_id") or 0)
        wolf_ch = interaction.guild.get_channel(state.get("wolf_channel_id") or 0)
        if wolf_ch: await wolf_ch.set_permissions(player, overwrite=None)

    db_remove_day_vote(interaction.guild_id, player.id)
    await refresh_player_list(interaction.guild)
    await refresh_day_vote(interaction.guild)
    await log_event(interaction.guild, cached_get_state(interaction.guild_id).get("phase","day").capitalize(),
                    f"👢 **{player.display_name}** was kicked from the game")
    await post_mod_log(interaction.guild, f"👢 **{player.display_name}** kicked (role: {assignment[1]})")
    await interaction.followup.send(
        f"✅ **{player.display_name}** removed from the game.", ephemeral=True)

    winner = await check_win_condition(interaction.guild)
    if winner:
        await announce_win(interaction.guild, winner)



@tree.command(name="bind", description="Cupid: bind two players tonight — if one dies, both die")
@app_commands.describe(player1="First player to bind", player2="Second player to bind")
async def bind(interaction: discord.Interaction, player1: discord.Member, player2: discord.Member):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)

    # Verify caller is Cupid
    rows = db_get_assignments(interaction.guild_id)
    my_row = next((r for r in rows if r[0] == interaction.user.id and r[2] == 1), None)
    if not my_row or my_row[1] != "Cupid":
        return await interaction.response.send_message(
            "❌ Only the Cupid can use this command.", ephemeral=True)

    state = db_get_state(interaction.guild_id) or {}
    if state.get("phase") != "night":
        return await interaction.response.send_message(
            "❌ You can only bind during the night phase.", ephemeral=True)

    if player1.id == player2.id:
        return await interaction.response.send_message(
            "❌ Cannot bind a player to themselves.", ephemeral=True)

    # Validate both are alive
    p1_row = next((r for r in rows if r[0] == player1.id and r[2] == 1), None)
    p2_row = next((r for r in rows if r[0] == player2.id and r[2] == 1), None)
    if not p1_row:
        return await interaction.response.send_message(
            f"❌ **{player1.display_name}** is not an alive player.", ephemeral=True)
    if not p2_row:
        return await interaction.response.send_message(
            f"❌ **{player2.display_name}** is not an alive player.", ephemeral=True)

    night_num = db_get_night_num(interaction.guild_id)
    # Use nightly bond (expires at morning) not permanent bond
    db_set_cupid_current(interaction.guild_id, player1.id, player2.id)
    db_save_night_action(interaction.guild_id, night_num, interaction.user.id, "cupid_bind", player1.id)

    await post_mod_log(interaction.guild,
        f"💘 **Cupid Bind — Night {night_num}**\n"
        f"**{player1.display_name}** ↔ **{player2.display_name}**\n"
        f"Bond expires at morning. Submitted via /bind command.")

    await interaction.response.send_message(
        fmt(f"💘 Bound **{player1.display_name}** and **{player2.display_name}** tonight.\n"
            f"If either dies, the other follows. Bond expires at dawn."),
        ephemeral=True)


# ====================== CLAIM CHANNELS ======================

class ClaimChannelView(View):
    """Persistent view in claim channels — add or remove players."""
    def __init__(self, guild_id, channel_id, owner_id):
        super().__init__(timeout=None)
        self.guild_id   = guild_id
        self.channel_id = channel_id
        self.owner_id   = owner_id

        add_btn = Button(label="➕ Add Player", style=discord.ButtonStyle.green)
        rem_btn = Button(label="➖ Remove Player", style=discord.ButtonStyle.danger)
        add_btn.callback = self.on_add
        rem_btn.callback = self.on_remove
        self.add_item(add_btn)
        self.add_item(rem_btn)

    async def on_add(self, interaction: discord.Interaction):
        try:
            ch = interaction.channel
            if not ch:
                return await interaction.response.send_message("❌ Channel not found.", ephemeral=True)
            await interaction.response.send_modal(
                AddPlayerModal(interaction.guild_id, ch.id))
        except Exception as e:
            print(f"[on_add] error: {e}")
            import traceback
            try:
                await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)
            except Exception:
                pass

    async def on_remove(self, interaction: discord.Interaction):
        try:
            ch = interaction.channel
            if not ch:
                return await interaction.response.send_message("❌ Channel not found.", ephemeral=True)
            await interaction.response.send_modal(
                RemovePlayerModal(interaction.guild_id, ch.id))
        except Exception as e:
            print(f"[on_remove] error: {e}")
            import traceback
            try:
                await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)
            except Exception:
                pass


class AddPlayerModal(discord.ui.Modal, title="Add a player to this claim"):
    name = discord.ui.TextInput(
        label="Player name",
        placeholder="Type their display name...",
        max_length=100
    )

    def __init__(self, guild_id, channel_id):
        super().__init__()
        self.guild_id   = guild_id
        self.channel_id = channel_id

    async def on_submit(self, interaction: discord.Interaction):
        search = self.name.value.strip().lower()
        ch     = interaction.guild.get_channel(self.channel_id)
        if not ch:
            return await interaction.response.send_message("❌ Channel not found.", ephemeral=True)

        # Find alive player by display name
        rows   = db_get_assignments(self.guild_id)
        npcs   = db_get_npcs(self.guild_id)
        npc_map = {n["npc_id"]: n["name"] for n in npcs}

        target_member = None
        for r in rows:
            if r[2] != 1:
                continue
            m    = interaction.guild.get_member(r[0])
            name = npc_map.get(r[0]) or (m.display_name if m else "")
            if search in name.lower():
                target_member = m
                break

        if not target_member:
            return await interaction.response.send_message(
                f"❌ Could not find alive player matching **{self.name.value}**.", ephemeral=True)

        # Check not already in channel
        existing_perms = ch.permissions_for(target_member)
        if existing_perms.view_channel:
            return await interaction.response.send_message(
                f"❌ **{target_member.display_name}** already has access.", ephemeral=True)

        await ch.set_permissions(target_member,
            view_channel=True, send_messages=True, read_messages=True)
        await ch.send(
            f"🎭 **{target_member.mention}** has been added to this claim.")
        await post_mod_log(interaction.guild,
            f"🎭 **Claim channel** — player added\n"
            f"**Channel:** {ch.name}\n"
            f"**Added by:** {interaction.user.display_name}\n"
            f"**Added:** {target_member.display_name}")
        await interaction.response.send_message(
            f"✅ **{target_member.display_name}** added.", ephemeral=True)


class RemovePlayerModal(discord.ui.Modal, title="Remove a player from this claim"):
    name = discord.ui.TextInput(
        label="Player name",
        placeholder="Type their display name...",
        max_length=100
    )

    def __init__(self, guild_id, channel_id):
        super().__init__()
        self.guild_id   = guild_id
        self.channel_id = channel_id

    async def on_submit(self, interaction: discord.Interaction):
        search = self.name.value.strip().lower()
        ch     = interaction.guild.get_channel(self.channel_id)
        if not ch:
            return await interaction.response.send_message("❌ Channel not found.", ephemeral=True)

        # Find player by checking channel's explicit permission overwrites
        target_member = None
        for target, overwrite in ch.overwrites.items():
            if (isinstance(target, discord.Member)
                    and target.id != interaction.user.id
                    and not target.bot
                    and search in target.display_name.lower()
                    and overwrite.view_channel is True):
                target_member = target
                break

        if not target_member:
            return await interaction.response.send_message(
                f"❌ Could not find **{self.name.value}** in this channel.", ephemeral=True)

        await ch.set_permissions(target_member, overwrite=None)
        await ch.send(f"🎭 **{target_member.display_name}** has been removed from this claim.")
        await post_mod_log(interaction.guild,
            f"🎭 **Claim channel** — player removed\n"
            f"**Channel:** {ch.name}\n"
            f"**Removed by:** {interaction.user.display_name}\n"
            f"**Removed:** {target_member.display_name}")
        await interaction.response.send_message(
            f"✅ **{target_member.display_name}** removed.", ephemeral=True)





@tree.command(name="pass_night", description="Pass your night action — use if your ability button is not working")
async def pass_night(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)

    state = db_get_state(interaction.guild_id) or {}
    if state.get("phase") != "night":
        return await interaction.response.send_message(
            "❌ It is not currently night phase.", ephemeral=True)

    rows      = db_get_assignments(interaction.guild_id)
    actor_row = next((r for r in rows if r[0] == interaction.user.id and r[2] == 1), None)
    if not actor_row:
        return await interaction.response.send_message(
            "❌ You are not an active player in this game.", ephemeral=True)

    night_num = db_get_night_num(interaction.guild_id)
    db_save_night_action(interaction.guild_id, night_num, interaction.user.id, "_pass", None)

    await post_mod_log(interaction.guild,
        f"💤 **{actor_row[1]}** — Night {night_num}\n"
        f"**{interaction.user.display_name}** is passing — no action tonight.")

    await interaction.response.send_message(
        fmt(f"💤 Passed. The mod has been notified. Sleep tight!"),
        ephemeral=True)

@tree.command(name="frenzy", description="Agitator: activate your frenzy ability tonight")
async def frenzy(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)

    # Verify the user is the Agitator
    rows = db_get_assignments(interaction.guild_id)
    actor_row = next((r for r in rows if r[0] == interaction.user.id and r[2] == 1), None)
    if not actor_row or actor_row[1] != "Agitator":
        return await interaction.response.send_message(
            "❌ Only the Agitator can use this command.", ephemeral=True)

    state = db_get_state(interaction.guild_id) or {}
    if state.get("phase") != "night":
        return await interaction.response.send_message(
            "❌ You can only use this during the night phase.", ephemeral=True)

    night_num = db_get_night_num(interaction.guild_id)
    db_save_night_action(interaction.guild_id, night_num, interaction.user.id, "agitator_frenzy", None)

    await post_mod_log(interaction.guild,
        f"📢 **Agitator** — Night {night_num}\n"
        f"**{interaction.user.display_name}** has used their **FRENZY** ability! "
        f"The village will require TWO eliminations tomorrow.")

    await interaction.response.send_message(
        fmt("✅ Frenzy activated! The village will be stirred into chaos tomorrow.\n"
            "Two players must be eliminated during the next day phase."),
        ephemeral=True)

@tree.command(name="protect", description="Huntsman: declare who you are protecting tonight")
@app_commands.describe(player="The player you want to protect tonight")
async def protect(interaction: discord.Interaction, player: discord.Member):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)

    # Verify caller is the Huntsman
    rows = db_get_assignments(interaction.guild_id)
    my_row = next((r for r in rows if r[0] == interaction.user.id and r[2] == 1), None)
    if not my_row or my_row[1] != "Huntsman":
        return await interaction.response.send_message(
            "❌ This command is only for the Huntsman.", ephemeral=True)

    state = db_get_state(interaction.guild_id) or {}
    if state.get("phase") != "night":
        return await interaction.response.send_message(
            "❌ Protection can only be declared during the night phase.", ephemeral=True)

    # Cannot protect yourself
    if player.id == interaction.user.id:
        return await interaction.response.send_message(
            "❌ You cannot protect yourself.", ephemeral=True)

    # Target must be alive
    target_row = next((r for r in rows if r[0] == player.id and r[2] == 1), None)
    if not target_row:
        return await interaction.response.send_message(
            f"❌ **{player.display_name}** is not an alive player.", ephemeral=True)

    night_num = db_get_night_num(interaction.guild_id)
    db_save_night_action(interaction.guild_id, night_num, interaction.user.id, "huntsman", player.id)

    await post_mod_log(interaction.guild,
        f"🏹 **Huntsman** — Night {night_num}\n"
        f"**{interaction.user.display_name}** is protecting **{player.display_name}** tonight.")

    await interaction.response.send_message(
        fmt(f"✅ You are protecting **{player.display_name}** tonight.\n"
            f"If they are targeted, you will intervene."),
        ephemeral=True)

@tree.command(name="claim", description="Open a private claim channel with another player")
@app_commands.describe(player="The player you want to open a private claim with")
async def claim(interaction: discord.Interaction, player: discord.Member):
    if player.id == interaction.user.id:
        return await interaction.response.send_message(
            "❌ You cannot open a claim with yourself.", ephemeral=True)
    if player.bot:
        return await interaction.response.send_message(
            "❌ You cannot open a claim with a bot.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    # Use fresh DB read — cache may not reflect game just started
    state    = db_get_state(interaction.guild_id) or {}
    category = interaction.guild.get_channel(state.get("category_id") or 0)
    if not category:
        return await interaction.followup.send(
            "❌ No active game category found. Make sure a game has been started.", ephemeral=True)

    everyone  = interaction.guild.default_role
    bot_me    = interaction.guild.me
    mod_role  = interaction.guild.get_role(state.get("mod_role_id") or 0)
    spec_role = interaction.guild.get_role(state.get("spectator_role_id") or 0)
    font      = get_guild_font(interaction.guild_id)

    # Channel name from both player names
    n1 = interaction.user.display_name.lower().replace(" ", "-")[:15]
    n2 = player.display_name.lower().replace(" ", "-")[:15]
    claim_ch_name = ch_name(f"claim-{n1}-{n2}", font, "🎭")

    ch_ow = {
        everyone:         discord.PermissionOverwrite(view_channel=False),
        bot_me:           discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True),
        interaction.user: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True),
        player:           discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True),
    }
    if mod_role:
        ch_ow[mod_role]  = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
    if spec_role:
        ch_ow[spec_role] = discord.PermissionOverwrite(view_channel=True, send_messages=False, read_messages=True)

    ch = await category.create_text_channel(claim_ch_name, overwrites=ch_ow)
    db_add_claim_channel(interaction.guild_id, ch.id)  # Track for end_game cleanup

    # Post welcome message with persistent add/remove buttons
    view = ClaimChannelView(interaction.guild_id, ch.id, interaction.user.id)
    await ch.send(
        f"🎭 **Private Claim** — {interaction.user.mention} & {player.mention}\n"
        f"*This conversation is private. Only you two and mods can see it.*\n"
        f"*Choose your words carefully. Trust is earned, not given.*",
        view=view)

    await post_mod_log(interaction.guild,
        f"🎭 **Claim channel opened**\n"
        f"**Between:** {interaction.user.display_name} & {player.display_name}\n"
        f"**Channel:** {ch.mention}")

    await interaction.followup.send(
        f"✅ Claim channel created — {ch.mention}", ephemeral=True)

@tree.command(name="transfer_mod", description="Give mod control to another player")
@is_mod()
@app_commands.describe(new_mod="The player to hand control to")
async def transfer_mod(interaction: discord.Interaction, new_mod: discord.Member):
    state    = cached_get_state(interaction.guild_id)
    mod_role = interaction.guild.get_role(state.get("mod_role_id") or 0)
    if not mod_role:
        return await interaction.response.send_message(
            "❌ No mod role set. Use `/setup_roles` first.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        await new_mod.add_roles(mod_role)
        await interaction.user.remove_roles(mod_role)
    except discord.Forbidden:
        return await interaction.followup.send(
            "❌ Bot doesn't have permission to manage that role.", ephemeral=True)
    await post_mod_log(interaction.guild,
        f"🔄 **Mod transferred** from {interaction.user.mention} to {new_mod.mention}")
    await interaction.followup.send(
        f"✅ Mod control transferred to **{new_mod.display_name}**.", ephemeral=True)


@tree.command(name="announce", description="Send a message to all player private channels")
@is_mod()
@app_commands.describe(message="Message to send to all players", alive_only="Only send to alive players? (default True)")
async def announce(interaction: discord.Interaction, message: str, alive_only: bool = True):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    rows = db_get_assignments(interaction.guild_id)
    sent = 0
    embed = discord.Embed(
        title="📢 Mod Announcement",
        description=message,
        color=0xF1C40F
    )
    import time as _bbt
    embed.set_footer(text=f"From the moderators • <t:{int(_bbt.time())}:t>")
    for pid, role_name, is_alive, ch_id in rows:
        if alive_only and not is_alive:
            continue
        ch = interaction.guild.get_channel(ch_id or 0)
        if ch:
            try:
                m = interaction.guild.get_member(pid)
                await ch.send(m.mention if m else "", embed=embed)
                sent += 1
            except: pass
    await post_mod_log(interaction.guild, f"📢 **Announcement sent** to {sent} players:\n> {message}")
    await interaction.followup.send(f"✅ Announcement sent to **{sent}** player channels.", ephemeral=True)


# ====================== ROLE SCRAMBLE ======================



async def _check_clone_inheritance(guild, guild_id: int, dead_player_id: int, dead_role: str):
    """When a player dies, check if a Clone was watching them and give them their role."""
    rows = db_get_assignments(guild_id)
    clone_row = next((r for r in rows if r[1] == "Clone" and r[2] == 1), None)
    if not clone_row:
        return

    clone_id = clone_row[0]
    # Find clone target from night actions
    all_actions = []
    for n in range(1, db_get_night_num(guild_id) + 1):
        all_actions.extend(db_get_night_actions(guild_id, n))

    clone_action = next((a for a in all_actions if a[0] == clone_id and a[1] == "clone"), None)
    if not clone_action or clone_action[2] != dead_player_id:
        return  # Clone wasn't watching this player

    # Clone inherits the dead player's role
    new_role      = dead_role
    new_role_info = get_role_info(guild_id, new_role)
    new_team      = new_role_info.get("team", "village")
    state_cl      = cached_get_state(guild_id) or {}

    conn_cl = sqlite3.connect(DB_FILE)
    c_cl    = conn_cl.cursor()
    c_cl.execute(
        "UPDATE player_assignments SET role_name=? WHERE guild_id=? AND player_id=?",
        (new_role, guild_id, clone_id))
    conn_cl.commit()
    conn_cl.close()
    invalidate_cache(guild_id)

    # Grant den access if inherited wolf role
    if new_team == "wolf":
        wolf_ch = guild.get_channel(state_cl.get("wolf_channel_id") or 0)
        clone_m = guild.get_member(clone_id)
        if wolf_ch and clone_m:
            await wolf_ch.set_permissions(clone_m, view_channel=True, send_messages=True)

    # Notify clone in private channel
    priv_ch = guild.get_channel(clone_row[3] or 0)
    if priv_ch:
        clone_m = guild.get_member(clone_id)
        team_emoji = "🐺" if new_team == "wolf" else ("⚖️" if new_team == "neutral" else "🏘️")
        embed = discord.Embed(
            title       = "🪞 Clone — Inheritance Triggered",
            description = (
                f"The player you were watching has died.\n\n"
                f"You have inherited their role: **{new_role}** {team_emoji}\n\n"
                f"{new_role_info.get('description', '')}\n\n"
                f"*You are now a {new_role} for the rest of the game.*"
            ),
            color = 0xFF4444 if new_team == "wolf" else (0xF1C40F if new_team == "neutral" else 0x9B59B6)
        )
        await priv_ch.send(clone_m.mention if clone_m else "", embed=embed)

    await post_mod_log(guild,
        f"🪞 **Clone inheritance triggered**\n"
        f"Clone ({guild.get_member(clone_id).display_name if guild.get_member(clone_id) else clone_id}) "
        f"inherited **{new_role}** from the deceased player.\n"
        f"{'Den access granted.' if new_team == 'wolf' else ''}")

async def _clear_player_night_actions(guild, guild_id: int, player_id: int, night_num: int):
    """Clear a player's submitted night actions and disable their old action buttons.
    Called when a player's role changes mid-night via turn or manual reassignment."""
    # Remove their night action for current night from DB
    conn_cl = sqlite3.connect(DB_FILE)
    c_cl    = conn_cl.cursor()
    c_cl.execute(
        "DELETE FROM night_actions WHERE guild_id=? AND night_num=? AND actor_id=?",
        (guild_id, night_num, player_id))
    conn_cl.commit()
    conn_cl.close()
    invalidate_cache(guild_id)

    # Disable old buttons in their private channel
    rows_cl  = db_get_assignments(guild_id)
    player_row = next((r for r in rows_cl if r[0] == player_id), None)
    if player_row and player_row[3]:
        priv_ch = guild.get_channel(player_row[3])
        if priv_ch:
            try:
                async for msg in priv_ch.history(limit=15):
                    if msg.author.id == guild.me.id and msg.components:
                        await msg.edit(view=None)
            except Exception:
                pass

async def _apply_role_to_player(guild, player_id: int, old_role: str, new_role: str, state: dict):
    """Internal helper — updates DB, fixes wolf channel access, renames private channel, notifies player."""
    guild_id     = guild.id
    old_team     = get_team(guild_id, old_role)
    new_role_info= get_role_info(guild_id, new_role)
    new_team     = new_role_info.get("team", "village")

    # Update DB
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("UPDATE player_assignments SET role_name=? WHERE guild_id=? AND player_id=?",
              (new_role, guild_id, player_id))
    conn.commit()
    conn.close()

    player       = guild.get_member(player_id)
    wolf_ch      = guild.get_channel(state.get("wolf_channel_id") or 0)


    # Fix wolf channel access
    if old_team != "wolf" and new_team == "wolf":
        if wolf_ch and player:
            await wolf_ch.set_permissions(player, view_channel=True, send_messages=True)
    elif old_team == "wolf" and new_team != "wolf":
        if wolf_ch and player:
            await wolf_ch.set_permissions(player, overwrite=None)

    # Get channel id fresh from DB
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT channel_id FROM player_assignments WHERE guild_id=? AND player_id=?",
              (guild_id, player_id))
    row   = c.fetchone()
    ch_id = row[0] if row else None
    conn.close()

    # Clear old night actions and disable stale buttons
    night_num_cl = db_get_night_num(guild_id)
    await _clear_player_night_actions(guild, guild_id, player_id, night_num_cl)

    # Rename private channel and notify player
    if ch_id and player:
        priv_ch = guild.get_channel(ch_id)
        if priv_ch:
            new_ch_name = f"🔒{player.display_name}-{new_role}".lower().replace(" ", "-")
            try: await priv_ch.edit(name=new_ch_name)
            except: pass
            team_emoji = "🐺" if new_team == "wolf" else ("⚖️" if new_team == "neutral" else "🏘️")
            role_desc  = new_role_info.get("description") or "*No description provided.*"
            embed = discord.Embed(
                title="🔀 Role changed",
                description=(
                    f"Your new role is **{new_role}** {team_emoji}\n\n{role_desc}\n\n"
                    f"*Your previous night action has been cleared. "
                    f"You will receive new action buttons if your new role has a night ability.*"
                ),
                color=0xFF4444 if new_team == "wolf" else (0xF1C40F if new_team == "neutral" else 0x44BB44)
            )
            embed.set_footer(text="Role changed by the mod team.")
            await priv_ch.send(player.mention, embed=embed)




@tree.command(name="assign_role", description="Manually assign a role to a player and update their private channel")
@is_mod()
@app_commands.describe(
    player="The player to reassign",
    role="The new role to assign them")
async def assign_role(interaction: discord.Interaction, player: discord.Member, role: str):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    rows      = db_get_assignments(interaction.guild_id)
    actor_row = next((r for r in rows if r[0] == player.id and r[2] == 1), None)
    if not actor_row:
        return await interaction.followup.send(
            f"❌ **{player.display_name}** is not an alive player.", ephemeral=True)

    # Match role name exactly from DB pool (case-insensitive)
    all_roles_raw = cached_load_roles(interaction.guild_id)
    matched   = next((r for r in all_roles_raw if r["name"].lower() == role.strip().lower()), None)
    if not matched:
        return await interaction.followup.send(
            f"❌ **{role}** not found in role pool. Check `/list_roles` for exact names.",
            ephemeral=True)

    role      = matched["name"]      # Exact name from DB
    role_info = matched
    old_role  = actor_row[1]
    new_team  = matched.get("team", "village")
    old_team  = get_team(interaction.guild_id, old_role)
    state     = db_get_state(interaction.guild_id) or {}

    # Update DB
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("UPDATE player_assignments SET role_name=? WHERE guild_id=? AND player_id=?",
              (role, interaction.guild_id, player.id))
    conn.commit()
    conn.close()
    invalidate_cache(interaction.guild_id)

    # Clear old night actions and disable stale buttons immediately
    night_num_ar = db_get_night_num(interaction.guild_id)
    await _clear_player_night_actions(interaction.guild, interaction.guild_id, player.id, night_num_ar)

    # Handle team changes — den access
    wolf_ch = interaction.guild.get_channel(state.get("wolf_channel_id") or 0)
    DENY_DEN = {"Bloodhound", "Wolf Pup"}

    if new_team == "wolf" and old_team != "wolf" and role not in DENY_DEN:
        if wolf_ch:
            await wolf_ch.set_permissions(player, view_channel=True, send_messages=True)
            await wolf_ch.send(fmt(f"🐺 **{player.display_name}** has joined the pack!"))
    elif old_team == "wolf" and new_team != "wolf":
        if wolf_ch:
            await wolf_ch.set_permissions(player, overwrite=None)

    # Rename private channel
    if actor_row[3]:
        priv_ch = interaction.guild.get_channel(actor_row[3])
        if priv_ch:
            try:
                new_ch_name = f"🔒{player.display_name}-{role}".lower().replace(" ", "-")[:100]
                await priv_ch.edit(name=new_ch_name)
            except Exception as e:
                print(f"[assign_role] rename error: {e}")

            # Send updated role card and pin it
            font     = get_guild_font(interaction.guild_id)
            embed    = build_role_card(player, role, role_info, font)
            role_msg = await priv_ch.send(
                fmt(f"📋 Your role has been updated by the mod."),
                embed=embed)
            try:
                await role_msg.pin()
            except Exception:
                pass

    # Refresh win tracker
    await refresh_win_tracker(interaction.guild)

    await post_mod_log(interaction.guild,
        f"📋 **Role Reassigned**\n"
        f"**Player:** {player.display_name}\n"
        f"**Old role:** {old_role} ({old_team})\n"
        f"**New role:** {role} ({new_team})\n"
        f"Private channel updated.")

    await interaction.followup.send(
        f"✅ **{player.display_name}** reassigned from **{old_role}** to **{role}**.\n"
        f"Private channel updated.", ephemeral=True)


@assign_role.autocomplete("role")
async def assign_role_autocomplete(interaction: discord.Interaction, current: str):
    roles = get_game_roles(interaction.guild_id) if game_active(interaction.guild_id) else cached_load_roles(interaction.guild_id)

    return [
        app_commands.Choice(name=f"{r['name']} ({r['team']})", value=r["name"])
        for r in roles
        if current.lower() in r["name"].lower()
    ][:25]

@tree.command(name="turn_player", description="Manually turn a player to the wolf team")
@is_mod()
@app_commands.describe(
    player="The player to turn",
    role="The wolf role to assign them (leave blank to spin from pool)")
async def turn_player(interaction: discord.Interaction, player: discord.Member, role: str = ""):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    rows      = db_get_assignments(interaction.guild_id)
    state     = db_get_state(interaction.guild_id) or {}
    actor_row = next((r for r in rows if r[0] == player.id and r[2] == 1), None)
    if not actor_row:
        return await interaction.followup.send(
            f"❌ **{player.display_name}** is not an alive player.", ephemeral=True)

    TURN_EXCLUDE = {"Alpha", "Elite Alpha"}

    # Determine new role
    if role:
        # Mod specified a role — validate it's a wolf role in the current game
        role_info = get_role_info(interaction.guild_id, role)
        if role_info.get("team") != "wolf":
            return await interaction.followup.send(
                f"❌ **{role}** is not a wolf role.", ephemeral=True)
        if role in TURN_EXCLUDE:
            return await interaction.followup.send(
                f"❌ Cannot turn a player into **{role}**.", ephemeral=True)
        new_role  = role
        role_info = get_role_info(interaction.guild_id, new_role)
    else:
        # Spin from current game pool only — not all roles in DB
        last_roles = db_get_last_roles(interaction.guild_id)
        if last_roles:
            wolf_pool = [name for name, count in last_roles.items()
                         if get_team(interaction.guild_id, name) == "wolf"
                         and name not in TURN_EXCLUDE]
        else:
            all_roles = cached_load_roles(interaction.guild_id)
            wolf_pool = [r["name"] for r in all_roles
                         if r["team"] == "wolf" and r["name"] not in TURN_EXCLUDE]
        if not wolf_pool:
            return await interaction.followup.send(
                "❌ No valid wolf roles in current game pool to spin from.", ephemeral=True)
        import random as _r
        new_role  = _r.choice(wolf_pool)
        role_info = get_role_info(interaction.guild_id, new_role)

    # Update DB
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("UPDATE player_assignments SET role_name=? WHERE guild_id=? AND player_id=?",
              (new_role, interaction.guild_id, player.id))
    conn.commit()
    conn.close()
    invalidate_cache(interaction.guild_id)

    # Grant wolf den access
    wolf_ch = interaction.guild.get_channel(state.get("wolf_channel_id") or 0)
    if wolf_ch:
        await wolf_ch.set_permissions(player, view_channel=True, send_messages=True)
        await wolf_ch.send(fmt(f"🐺 **{player.display_name}** has joined the pack!\nWelcome them to the den."))

    # Rename private channel
    if actor_row[3]:
        priv_ch = interaction.guild.get_channel(actor_row[3])
        if priv_ch:
            try:
                new_ch_name = f"🔒{player.display_name}-{new_role}".lower().replace(" ", "-")[:100]
                await priv_ch.edit(name=new_ch_name)
            except Exception:
                pass

            # Wheel spin in private channel
            font     = get_guild_font(interaction.guild_id)
            all_r    = cached_load_roles(interaction.guild_id)
            pool_names = [r["name"] for r in all_r
                          if r["team"] == "wolf" and r["name"] not in TURN_EXCLUDE]
            wheel_lines = "\n".join(f"🎡 {r}" for r in pool_names)
            spin_msg = await priv_ch.send(fmt(
                f"🐺 Something has shifted in the darkness.\n"
                f"You are no longer who you were.\n\n"
                f"The wheel spins...\n{wheel_lines}"))
            await asyncio.sleep(2)
            embed = build_role_card(player, new_role, role_info, font)
            await spin_msg.edit(content=fmt(
                f"🐺 The wheel has spoken.\n"
                f"You are now: **{new_role}**"))
            await priv_ch.send(f"Welcome to the pack, {player.mention}", embed=embed)

    # Refresh win tracker
    await refresh_win_tracker(interaction.guild)
    updated_rows  = db_get_assignments(interaction.guild_id)
    village_count = sum(1 for r in updated_rows if r[2] == 1 and get_team(interaction.guild_id, r[1]) == "village")
    wolf_count    = sum(1 for r in updated_rows if r[2] == 1 and get_team(interaction.guild_id, r[1]) == "wolf")
    neutral_count = sum(1 for r in updated_rows if r[2] == 1 and get_team(interaction.guild_id, r[1]) == "neutral")

    await post_mod_log(interaction.guild,
        f"🔄 **Manual Turn** — {player.display_name}\n"
        f"**New role:** {new_role}\n"
        f"**Updated counts:** Village: {village_count}  Wolves: {wolf_count}  Neutrals: {neutral_count}")

    await interaction.followup.send(
        f"✅ **{player.display_name}** turned to **{new_role}**.\n"
        f"Den access granted. Channel renamed.\n"
        f"Village: {village_count}  Wolves: {wolf_count}  Neutrals: {neutral_count}",
        ephemeral=True)

@turn_player.autocomplete("role")
async def turn_player_role_autocomplete(interaction: discord.Interaction, current: str):
    roles = cached_load_roles(interaction.guild_id)
    wolf_roles = [r for r in roles if r["team"] == "wolf"]

    return [
        app_commands.Choice(name=r["name"], value=r["name"])
        for r in wolf_roles
        if current.lower() in r["name"].lower()
    ][:25]


@tree.command(name="scramble_roles", description="Secretly jumble all alive player roles — wolves stay wolves, village stays village")
@is_mod()
async def scramble_roles(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    rows  = db_get_assignments(interaction.guild_id)
    state = cached_get_state(interaction.guild_id)

    # Separate alive players by team bucket
    alive_rows = [r for r in rows if r[2] == 1]
    if len(alive_rows) < 2:
        return await interaction.followup.send(
            "❌ Need at least 2 alive players to scramble.", ephemeral=True)

    wolf_rows    = [(r[0], r[1]) for r in alive_rows if get_team(interaction.guild_id, r[1]) == "wolf"]
    village_rows = [(r[0], r[1]) for r in alive_rows if get_team(interaction.guild_id, r[1]) == "village"]
    neutral_rows = [(r[0], r[1]) for r in alive_rows if get_team(interaction.guild_id, r[1]) == "neutral"]

    # Shuffle each bucket independently — teams stay balanced
    wolf_pids    = [r[0] for r in wolf_rows]
    wolf_roles   = [r[1] for r in wolf_rows]
    village_pids = [r[0] for r in village_rows]
    village_roles= [r[1] for r in village_rows]
    neutral_pids = [r[0] for r in neutral_rows]
    neutral_roles= [r[1] for r in neutral_rows]

    random.shuffle(wolf_roles)
    random.shuffle(village_roles)
    random.shuffle(neutral_roles)

    # Build before/after log for mod
    changes = []
    all_pairs = (list(zip(wolf_pids, wolf_roles)) +
                 list(zip(village_pids, village_roles)) +
                 list(zip(neutral_pids, neutral_roles)))

    # Map old roles before applying
    old_role_map = {r[0]: r[1] for r in alive_rows}

    # Apply all changes
    for pid, new_role in all_pairs:
        old_role = old_role_map[pid]
        if old_role == new_role:
            continue  # No change needed for this player
        await _apply_role_to_player(interaction.guild, pid, old_role, new_role, state)
        m = interaction.guild.get_member(pid)
        name = m.display_name if m else str(pid)
        changes.append(f"• **{name}**: {old_role} → {new_role}")

    # Refresh wolf den membership message
    wolf_ch = interaction.guild.get_channel(state.get("wolf_channel_id") or 0)
    if wolf_ch and wolf_pids:
        new_wolf_names = []
        for pid, role in zip(wolf_pids, wolf_roles):
            m = interaction.guild.get_member(pid)
            new_wolf_names.append(m.display_name if m else str(pid))
        await wolf_ch.send(
            f"🔀 **Roles have been scrambled.** The pack remains:\n"
            + "\n".join(f"• {n}" for n in new_wolf_names))

    phase = cached_get_state(interaction.guild_id).get("phase", "day").capitalize()
    await log_event(interaction.guild, phase, "🔀 **Roles scrambled** by mod — all teams reshuffled")

    if changes:
        change_log = "\n".join(changes)
        await post_mod_log(interaction.guild,
            f"🔀 **Role Scramble** — {len(changes)} player(s) reassigned:\n{change_log}")
    else:
        await post_mod_log(interaction.guild,
            "🔀 **Role Scramble** triggered — no changes resulted (all players happened to keep same role).")

    await interaction.followup.send(
        f"✅ Roles scrambled. **{len(changes)}** player(s) received a new role.\n"
        f"*(Full breakdown sent to mod-log)*",
        ephemeral=True)

# ====================== REVIVE PLAYER ======================

@tree.command(name="revive_player", description="Bring an eliminated player back to life")
@is_mod()
@app_commands.describe(
    player="The eliminated player to revive",
    return_as="How they return: 'same' | 'random' | 'blank' | or type a specific role name",
    public="Announce the revival publicly? (default: False — silent)"
)
async def revive_player(interaction: discord.Interaction,
                        player: discord.Member,
                        return_as: str = "same",
                        public: bool = False):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    rows       = db_get_assignments(interaction.guild_id)
    assignment = next((r for r in rows if r[0] == player.id), None)

    if not assignment:
        return await interaction.followup.send(
            f"❌ **{player.display_name}** is not in the current game.", ephemeral=True)
    if assignment[2] == 1:
        return await interaction.followup.send(
            f"❌ **{player.display_name}** is already alive.", ephemeral=True)

    old_role  = assignment[1]
    state     = cached_get_state(interaction.guild_id)
    return_as = return_as.strip().lower()

    # ── Determine the revived role ──────────────────────────────────────────
    if return_as == "same":
        # Strip "(Wolf)" suffix if they were a converted Cursed
        new_role = old_role.replace(" (Wolf)", "").strip()
        revive_label = f"their original role (**{new_role}**)"

    elif return_as == "random":
        pool_roles = get_game_roles(interaction.guild_id)
        if not pool_roles:
            return await interaction.followup.send(
                "❌ No roles in the game to randomly assign from.", ephemeral=True)
        chosen    = random.choice(pool_roles)
        new_role  = chosen["name"]
        revive_label = f"a random role (**{new_role}**)"

    elif return_as == "blank":
        new_role     = "Villager"   # Generic — no special ability
        revive_label = "as a blank **Villager** (no special role)"

    else:
        # Mod typed a specific role name — validate it loosely
        matched = next((r for r in get_game_roles(interaction.guild_id)
                        if r["name"].lower() == return_as), None)
        if not matched:
            return await interaction.followup.send(
                f"❌ Role **{return_as}** not found in this game. "
                f"Use `same`, `random`, `blank`, or a role that is in the current game.", ephemeral=True)
        new_role     = matched["name"]
        revive_label = f"a new role (**{new_role}**)"

    role_info  = get_role_info(interaction.guild_id, new_role)
    new_team   = role_info.get("team", "village")
    old_team   = get_team(interaction.guild_id, old_role)

    # ── Restore alive status in DB ─────────────────────────────────────────
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("UPDATE player_assignments SET is_alive=1, role_name=? WHERE guild_id=? AND player_id=?",
              (new_role, interaction.guild_id, player.id))
    conn.commit()
    conn.close()

    # ── Restore Discord roles ──────────────────────────────────────────────
    dead_role = interaction.guild.get_role(state.get("dead_role_id") or 0)
    part_role = interaction.guild.get_role(state.get("participant_role_id") or 0)
    try:
        if dead_role and dead_role in player.roles:  await player.remove_roles(dead_role)
        if part_role and part_role not in player.roles: await player.add_roles(part_role)
    except discord.Forbidden:
        pass

    # ── Re-lock ghost chat ─────────────────────────────────────────────────
    ghost_ch = interaction.guild.get_channel(state.get("ghost_channel_id") or 0)
    if ghost_ch:
        try: await ghost_ch.set_permissions(player, overwrite=None)
        except: pass

    # ── Restore village chat and vote channel access ───────────────────────
    vc_ch = interaction.guild.get_channel(state.get("village_chat_ch_id") or 0)
    if vc_ch:
        try: await vc_ch.set_permissions(player, overwrite=None)
        except: pass
    dv_ch = interaction.guild.get_channel(state.get("day_vote_ch_id") or 0)
    if dv_ch:
        try: await dv_ch.set_permissions(player, overwrite=None)
        except: pass

    # ── Clear Shadow Wolf kill list on revival ────────────────────────────
    rows_check = db_get_assignments(interaction.guild_id)
    revived_role = next((r[1] for r in rows_check if r[0] == player.id), "")
    if revived_role == "Shadow Wolf":
        db_clear_shadow_wolf_list(interaction.guild_id)
        priv_ch_id = next((r[3] for r in rows_check if r[0] == player.id), None)
        if priv_ch_id:
            priv_ch_revive = interaction.guild.get_channel(priv_ch_id)
            if priv_ch_revive:
                await priv_ch_revive.send(fmt(
                    "🌑 You have been revived.\n"
                    "Your kill list has been cleared — you no longer have post-death kills."))

    # ── Fix wolf den access ────────────────────────────────────────────────
    wolf_ch      = interaction.guild.get_channel(state.get("wolf_channel_id") or 0)

    if new_team == "wolf" and old_team != "wolf":
        # Revived into wolf team
        if wolf_ch:
            await wolf_ch.set_permissions(player, view_channel=True, send_messages=True)
            await wolf_ch.send(f"🐺 **{player.display_name}** has returned from the dead and joined the pack!")


    elif new_team != "wolf" and old_team == "wolf":
        # Was wolf, now revived as non-wolf — strip den write access (keep read-only as dead wolf)
        if wolf_ch:
            await wolf_ch.set_permissions(player, view_channel=False, send_messages=False)


    elif new_team == "wolf":
        # Was wolf, still wolf — restore send access (was read-only as dead wolf)
        if wolf_ch:
            await wolf_ch.set_permissions(player, view_channel=True, send_messages=True)

    # ── Update private channel ─────────────────────────────────────────────
    ch_id   = assignment[3]
    priv_ch = interaction.guild.get_channel(ch_id or 0) if ch_id else None
    if priv_ch:
        # Restore send permissions
        try: await priv_ch.set_permissions(player, view_channel=True, send_messages=True)
        except: pass

        # Rename if role changed
        if new_role != old_role:
            new_ch_name = f"🔒{player.display_name}-{new_role}".lower().replace(" ", "-")
            try: await priv_ch.edit(name=new_ch_name)
            except: pass

        # Notify the player
        team_emoji = "🐺" if new_team == "wolf" else ("⚖️" if new_team == "neutral" else "🏘️")
        role_desc  = role_info.get("description") or "*No description provided.*"
        embed = discord.Embed(
            title="✨ You have been revived!",
            description=f"You are back in the game as **{new_role}** {team_emoji}\n\n{role_desc}",
            color=0xF1C40F
        )
        embed.set_footer(text="You are alive again. Act carefully — not everyone may know you're back.")
        await priv_ch.send(player.mention, embed=embed)

    # ── Public announcement (optional) ────────────────────────────────────
    if public:
        cat  = interaction.guild.get_channel(state.get("category_id") or 0)
        skip = {state.get("mod_log_channel_id"), state.get("wolf_channel_id"),
                state.get("ghost_channel_id")}
        if cat:
            for ch in cat.channels:
                if isinstance(ch, discord.TextChannel) and ch.id not in skip:
                    await ch.send(
                        f"✨ **{player.display_name}** has been revived and returned to the game!")
                    break

    # ── Refresh live embeds ────────────────────────────────────────────────
    await refresh_player_list(interaction.guild)
    await refresh_day_vote(interaction.guild)
    await refresh_win_tracker(interaction.guild)

    phase = cached_get_state(interaction.guild_id).get("phase", "day").capitalize()
    await log_event(interaction.guild, phase,
        f"✨ **{player.display_name}** revived — returned as **{new_role}**"
        + (" (publicly announced)" if public else " (silent)"))
    await post_mod_log(interaction.guild,
        f"✨ **Revive** — {player.mention} returned as {revive_label}"
        + (" · announced publicly" if public else " · silent revival"))

    await interaction.followup.send(
        f"✅ **{player.display_name}** revived as {revive_label}."
        + (" Public announcement sent." if public else " Silent revival — only they were notified."),
        ephemeral=True)

# ====================== PLAYER COMMANDS =======================

@tree.command(name="my_actions", description="See all night actions you have submitted this game")
async def my_actions(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    rows = db_get_assignments(interaction.guild_id)
    me   = next((r for r in rows if r[0] == interaction.user.id), None)
    if not me:
        return await interaction.followup.send("You are not in the current game.", ephemeral=True)

    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute(
        "SELECT night_num, action_type, target_id FROM night_actions "
        "WHERE guild_id=? AND actor_id=? ORDER BY night_num",
        (interaction.guild_id, interaction.user.id))
    actions = c.fetchall()
    conn.close()

    if not actions:
        return await interaction.followup.send("No actions submitted yet.", ephemeral=True)

    all_rows = db_get_assignments(interaction.guild_id)
    pid_to_name = {}
    for pid, _, _, _ in all_rows:
        m = interaction.guild.get_member(pid)
        pid_to_name[pid] = m.display_name if m else str(pid)

    embed = discord.Embed(title="📋 Your Night Actions This Game", color=0x5865F2)
    lines = []
    for night_num, action_type, target_id in actions:
        target = pid_to_name.get(target_id, str(target_id)) if target_id else "—"
        ACTION_LABELS_MY = {
            "seer": "🔮 Seer investigation", "medium": "🌀 Medium alignment check",
            "bloodhound": "🦴 Bloodhound scan", "doctor_save": "💊 Doctor — used save",
            "doctor_skip": "💊 Doctor — skipped", "surgeon_save": "🏥 Surgeon — used save",
            "surgeon_skip": "🏥 Surgeon — skipped",
            "huntsman": "🏹 Huntsman — protected", "huntsman_skip": "🏹 Huntsman — skipped",
            "witch_save": "🧙 Witch — save potion", "witch_kill": "🧙 Witch — poison potion",
            "witch_skip": "🧙 Witch — skipped", "alpha": "👑 Alpha — turn attempt",
            "elite_alpha": "👑⭐ Elite Alpha — turn attempt", "wolf_pup": "🐾 Wolf Pup — blocked",
            "bloodletter": "🩸 Bloodletter — marked", "cupid_bind": "💘 Cupid — bound",
            "cupid_skip": "💘 Cupid — skipped", "agitator_frenzy": "📢 Agitator — frenzy",
            "clone": "🪞 Clone — chose target", "shapeshifter": "🎭 Shapeshifter — transformed",
            "white_wolf_kill": "🤍 White Wolf — killed", "shadow_wolf": "🌑 Shadow Wolf — killed",
            "gravedigger_ack": "⚰️ Gravedigger — listening", "oracle_question": "🔯 Oracle — asked",
            "_pass": "💤 Passed",
        }
        action_label = ACTION_LABELS_MY.get(action_type, action_type.replace("_"," ").title())
        lines.append(f"**Night {night_num}** — {action_label}" + (f" → {target}" if target != "—" else ""))

        # Check turn log for result
        turn_conn = sqlite3.connect(DB_FILE)
        turn_c    = turn_conn.cursor()
        turn_c.execute(
            "SELECT result FROM turn_log WHERE guild_id=? AND actor_id=? AND night_num=?",
            (interaction.guild_id, interaction.user.id, night_num))
        turn_row = turn_c.fetchone()
        turn_conn.close()
        if turn_row:
            lines[-1] += f" *(result: {turn_row[0].replace('_',' ').title()})*"

    embed.description = "\n".join(lines)
    embed.set_footer(text=f"Role: {me[1]}")
    await interaction.followup.send(embed=embed, ephemeral=True)

# ====================== ROLE RESERVATIONS ======================
@tree.command(name="reserve_role", description="Promise a player a specific role before game start")
@is_mod()
@app_commands.describe(player="The player to reserve a role for", role="The role name to reserve")
async def reserve_role(interaction: discord.Interaction, player: discord.Member, role: str):
    await interaction.response.defer(ephemeral=True)
    guild_id = interaction.guild_id
    roles    = cached_load_roles(guild_id)
    match    = next((r["name"] for r in roles if r["name"].lower() == role.lower()), None)
    if not match:
        return await interaction.followup.send(
            f"❌ Role **{role}** not found in this server's role list.", ephemeral=True)
    existing  = db_get_reservations(guild_id)
    conflicts = [r for r in existing if r[2] == match and r[0] != player.id]
    if conflicts:
        names = ", ".join(r[1] for r in conflicts)
        return await interaction.followup.send(
            f"⚠️ **{match}** is already reserved for **{names}**.\n"
            f"Use `/clear_reserve` to remove it first.", ephemeral=True)
    db_set_reservation(guild_id, player.id, player.display_name, match)
    await interaction.followup.send(
        f"⭐ Reserved **{match}** for **{player.display_name}**.\n"
        f"It will auto-populate when you build the next game roster.", ephemeral=True)
    await post_mod_log(interaction.guild,
        f"⭐ **Role Reserved** — **{match}** promised to **{player.display_name}**")


@tree.command(name="clear_reserve", description="Remove a role reservation for a player")
@is_mod()
@app_commands.describe(player="The player whose reservation to clear")
async def clear_reserve(interaction: discord.Interaction, player: discord.Member):
    await interaction.response.defer(ephemeral=True)
    existing = db_get_reservations(interaction.guild_id)
    res = next((r for r in existing if r[0] == player.id), None)
    if not res:
        return await interaction.followup.send(
            f"❌ No reservation found for **{player.display_name}**.", ephemeral=True)
    db_clear_reservation(interaction.guild_id, player.id)
    await interaction.followup.send(
        f"✅ Cleared **{res[2]}** reservation for **{player.display_name}**.", ephemeral=True)
    await post_mod_log(interaction.guild,
        f"🗑️ **Reservation cleared** — {player.display_name}'s **{res[2]}** reservation removed")


@tree.command(name="list_reserves", description="Show all current role reservations")
@is_mod()
async def list_reserves(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    reservations = db_get_reservations(interaction.guild_id)
    if not reservations:
        return await interaction.followup.send("No active reservations.", ephemeral=True)
    embed = discord.Embed(title="⭐ Role Reservations", color=0xF1C40F)
    for pid, pname, rname in reservations:
        embed.add_field(name=pname, value=f"**{rname}**", inline=True)
    embed.set_footer(text="These roles auto-populate when building the next game roster.")
    await interaction.followup.send(embed=embed, ephemeral=True)


@reserve_role.autocomplete("role")
async def reserve_role_autocomplete(interaction: discord.Interaction, current: str):
    roles = cached_load_roles(interaction.guild_id)

    return [
        app_commands.Choice(name=f"{r['name']} ({r['team']})", value=r["name"])
        for r in roles
        if current.lower() in r["name"].lower()
    ][:25]


@tree.command(name="list_players", description="Show alive and dead players")
async def list_players(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)
    rows = db_get_assignments(interaction.guild_id)
    if not rows:
        return await interaction.followup.send("No active game.")
    log = db_get_log(interaction.guild_id)
    await interaction.followup.send(embed=build_player_list_embed(interaction.guild, rows, log))

@tree.command(name="my_role", description="Show your secret role (only you can see this)")
async def my_role(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    rows = db_get_assignments(interaction.guild_id)
    # Check both int and str comparison in case of type mismatch
    assignment = next((r for r in rows if r[0] == interaction.user.id or str(r[0]) == str(interaction.user.id)), None)
    if not assignment:
        # Check if game is even active
        state = cached_get_state(interaction.guild_id)
        if not state or not state.get("category_id"):
            return await interaction.followup.send("No active game right now.", ephemeral=True)
        return await interaction.followup.send(
            f"You are not assigned a role in the current game.\n"
            f"If you believe this is an error, ask the mod to check `/list_players`.",
            ephemeral=True)
    role_name = assignment[1]
    role_info = get_role_info(interaction.guild_id, role_name)
    is_alive  = bool(assignment[2])
    team      = role_info.get("team", "village")
    color     = 0xFF4444 if team == "wolf" else (0xF1C40F if team == "neutral" else 0x44BB44)
    team_emoji= "🐺" if team == "wolf" else ("⚖️" if team == "neutral" else "🏘️")
    embed = discord.Embed(
        title       = f"Your Role: {role_name} {team_emoji}",
        description = role_info.get("description") or "*No description.*",
        color       = color)
    embed.add_field(name="Status", value="✅ Alive" if is_alive else "💀 Eliminated", inline=True)
    embed.add_field(name="Team",   value=team.capitalize(), inline=True)
    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.command(name="time_left", description="Check how much time is left in the current phase")
async def time_left(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)

    import time as _time
    state     = db_get_state(interaction.guild_id) or {}
    phase     = state.get("phase", "day")
    night_num = db_get_night_num(interaction.guild_id)

    if phase == "night":
        # Use night timer end time if stored, else estimate from night duration
        end_time = state.get("night_end_time")
        if not end_time:
            return await interaction.response.send_message(
                f"🌙 **Night {night_num}** is active. End time not tracked — check with a mod.",
                ephemeral=True)
        end_ts = int(end_time)
        embed  = discord.Embed(
            title       = f"🌙 Night {night_num} — Time Remaining",
            description = (f"**Ends:** <t:{end_ts}:F>\n"
                           f"**Your local time:** <t:{end_ts}:t>\n"
                           f"**Countdown:** <t:{end_ts}:R>"),
            color       = 0x2C3060
        )
    else:
        end_time = state.get("day_vote_end_time")
        if not end_time:
            return await interaction.response.send_message(
                f"☀️ **Day {night_num}** is active. End time not tracked — check with a mod.",
                ephemeral=True)
        end_ts = int(end_time)
        embed  = discord.Embed(
            title       = f"☀️ Day {night_num} — Time Remaining",
            description = (f"**Ends:** <t:{end_ts}:F>\n"
                           f"**Your local time:** <t:{end_ts}:t>\n"
                           f"**Countdown:** <t:{end_ts}:R>"),
            color       = 0xE67E22
        )

    embed.set_footer(text="All times shown in your local timezone.")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="game_status", description="Show current game phase, counts, and timers")
async def game_status(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)
    state = cached_get_state(interaction.guild_id)
    if not state or not state.get("category_id"):
        return await interaction.followup.send("No active game.")

    rows       = db_get_assignments(interaction.guild_id)
    alive      = [r for r in rows if r[2] == 1]
    dead       = [r for r in rows if r[2] == 0]
    wolf_alive = len([r for r in alive if get_team(interaction.guild_id, r[1]) == "wolf"])
    vil_alive  = len([r for r in alive if get_team(interaction.guild_id, r[1]) == "village"])
    neut_alive = len([r for r in alive if get_team(interaction.guild_id, r[1]) == "neutral"])

    phase     = state.get("phase", "day").capitalize()
    night_num = db_get_night_num(interaction.guild_id)
    end_ts    = state.get("night_end_time") if phase == "Night" else state.get("day_vote_end_time")
    end_str   = f"<t:{end_ts}:t> (<t:{end_ts}:R>)" if end_ts else "Not set"

    # Votes
    votes      = db_get_day_votes(interaction.guild_id)
    vote_ct    = len([v for v in votes if v[1] is not None])
    abstain_ct = len([v for v in votes if v[1] is None])

    # Frenzy
    frenzy_day = state.get("agitator_frenzy_day")
    is_frenzy  = frenzy_day and int(frenzy_day) == int(night_num) and phase == "Day"
    elim_done  = state.get("agitator_elim_count", 0)

    # Wraith marks
    wmarks = db_get_wraith_marks(interaction.guild_id)

    color = 0xE74C3C if is_frenzy else (0xE67E22 if phase == "Day" else 0x2C3060)
    title = f"{'⚡ FRENZY — ' if is_frenzy else ''}{'☀️ Day' if phase == 'Day' else '🌙 Night'} {night_num}"

    embed = discord.Embed(title=title, color=color)
    embed.add_field(name="⏰ Ends",    value=end_str, inline=False)
    embed.add_field(name="👥 Alive",   value=f"**{len(alive)}** total — 🏘️ {vil_alive} · 🐺 {wolf_alive} · ⚖️ {neut_alive}", inline=True)
    embed.add_field(name="💀 Dead",    value=str(len(dead)), inline=True)

    if phase == "Day":
        embed.add_field(name="🗳️ Votes", value=f"{vote_ct} cast · {abstain_ct} abstain · {len(alive) - vote_ct - abstain_ct} pending", inline=True)
    if is_frenzy:
        embed.add_field(name="⚡ Frenzy", value=f"{elim_done}/2 eliminations complete", inline=True)
    if wmarks:
        embed.add_field(name="👻 Wraith Marks", value=f"{len(wmarks)} active", inline=True)

    # Mod name
    mod_role = interaction.guild.get_role(state.get("mod_role_id") or 0)
    if mod_role:
        mod_members = [m.display_name for m in interaction.guild.members if mod_role in m.roles]
        if mod_members:
            embed.add_field(name="🎮 Mod", value=", ".join(mod_members[:3]), inline=True)

    await interaction.followup.send(embed=embed)

_action_cooldowns: dict = {}  # user_id -> last_used timestamp

@tree.command(name="action", description="Send a private action or message to the mod team")
@app_commands.describe(message="Your action — only mods will see this")
async def action(interaction: discord.Interaction, message: str):
    import time as _tc
    now = int(_tc.time())
    # Prune stale entries (older than 60s) to prevent unbounded growth
    stale = [uid for uid, ts in _action_cooldowns.items() if now - ts > 60]
    for uid in stale:
        _action_cooldowns.pop(uid, None)
    last = _action_cooldowns.get(interaction.user.id, 0)
    if now - last < 30:
        remaining = 30 - (now - last)
        return await interaction.response.send_message(
            f"⏳ Please wait {remaining}s before sending another action.", ephemeral=True)
    _action_cooldowns[interaction.user.id] = now

    rows = db_get_assignments(interaction.guild_id)
    assignment = next((r for r in rows if r[0] == interaction.user.id), None)
    if not assignment:
        return await interaction.response.send_message("You are not in the current game.", ephemeral=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await post_mod_log(interaction.guild,
        f"📨 **Player Action** | {ts}\n"
        f"**From:** {interaction.user.mention} | **Role:** {assignment[1]}\n"
        f"**Message:** {message}")
    await interaction.response.send_message("✅ Action sent to mod team.", ephemeral=True)

# ====================== DAY VOTE ======================
day_vote_timers = {}

class DayVoteView(View):
    def __init__(self, guild_id, anonymous=False):
        super().__init__(timeout=None)
        self.guild_id  = guild_id
        self.anonymous = anonymous

        cast_btn      = Button(label="🗳️ Cast / Change Vote",    style=discord.ButtonStyle.blurple)
        abstain_btn   = Button(label="🤐 Abstain",               style=discord.ButtonStyle.secondary)
        remove_btn    = Button(label="↩️ Remove Vote",           style=discord.ButtonStyle.secondary)
        breakdown_btn = Button(label="👁️ Full Breakdown",        style=discord.ButtonStyle.grey,
                               disabled=anonymous)
        log_btn       = Button(label="🕐 Vote Log",              style=discord.ButtonStyle.grey, row=1)
        notvoted_btn  = Button(label="⚠️ Has Not Voted",         style=discord.ButtonStyle.red,  row=1)
        cast_btn.callback      = self.cast_vote
        abstain_btn.callback   = self.abstain
        remove_btn.callback    = self.remove_vote
        breakdown_btn.callback = self.full_breakdown
        log_btn.callback       = self.vote_log
        notvoted_btn.callback  = self.not_voted

        self.add_item(cast_btn)
        self.add_item(abstain_btn)
        self.add_item(remove_btn)
        self.add_item(breakdown_btn)
        self.add_item(log_btn)
        self.add_item(notvoted_btn)

    def _get_player_status(self, interaction):
        """Returns (is_alive, is_in_game, rows)."""
        rows       = db_get_assignments(interaction.guild_id)
        row        = next((r for r in rows if r[0] == interaction.user.id), None)
        in_game    = row is not None
        is_alive   = row is not None and row[2] == 1
        return is_alive, in_game, rows

    async def cast_vote(self, interaction: discord.Interaction):
        # Block voting during night phase
        state = db_get_state(interaction.guild_id) or {}
        if state.get("phase") == "night":
            return await interaction.response.send_message(
                "❌ Voting is closed during the night phase.", ephemeral=True)
        is_alive, in_game, rows = self._get_player_status(interaction)
        if not in_game:
            return await interaction.response.send_message("❌ You are not in the current game.", ephemeral=True)
        if not is_alive:
            return await interaction.response.send_message("❌ Eliminated players cannot vote.", ephemeral=True)
        # Block self-voting
        alive  = [(r[0], r[1]) for r in rows if r[2] == 1 and r[0] != interaction.user.id]
        if not alive:
            return await interaction.response.send_message("No other alive players to vote for.", ephemeral=True)
        npcs_dv = db_get_npcs(interaction.guild_id)
        npc_map = {n["npc_id"]: n["name"] for n in npcs_dv}
        options = []
        for pid, _ in alive:
            m    = interaction.guild.get_member(pid)
            name = npc_map.get(pid) or (m.display_name if m else str(pid))
            options.append(discord.SelectOption(
                label    = name[:100],
                value    = str(pid),
                description = "🤖 NPC" if pid in npc_map else None
            ))
        # Check how many votes this player is allowed
        state_vote   = db_get_state(interaction.guild_id) or {}
        max_votes    = int(state_vote.get("votes_per_player") or 1)
        night_now    = db_get_night_num(interaction.guild_id)
        votes_1      = db_get_day_votes(interaction.guild_id)
        votes_2      = db_get_day_votes_2(interaction.guild_id)
        has_voted_1  = any(v[0] == interaction.user.id for v in votes_1)
        has_voted_2  = any(v[0] == interaction.user.id for v in votes_2)

        if max_votes >= 2 and has_voted_1 and has_voted_2:
            # Both frenzy votes cast — ask which to change
            v1_target = next((v[1] for v in votes_1 if v[0] == interaction.user.id), None)
            v2_target = next((v[1] for v in votes_2 if v[0] == interaction.user.id), None)
            npc_map_fv = {n["npc_id"]: n["name"] for n in db_get_npcs(interaction.guild_id)}
            def _fname(pid):
                if pid is None: return "Abstain"
                npc = npc_map_fv.get(pid)
                if npc: return npc
                m = interaction.guild.get_member(pid)
                return m.display_name if m else str(pid)
            v1_name = _fname(v1_target)
            v2_name = _fname(v2_target)
            view = FrenzyVotePickView(interaction.guild_id, options, rows, v1_name, v2_name)
            await interaction.response.send_message(
                fmt(f"⚡ **Frenzy — both votes cast.**\nVote 1: **{v1_name}** | Vote 2: **{v2_name}**\nWhich would you like to change?"),
                view=view, ephemeral=True)
        elif max_votes >= 2 and has_voted_1 and not has_voted_2:
            label = "⚡ **Frenzy active — cast your SECOND vote:**"
            view  = VoteTargetView(interaction.guild_id, options, rows, is_second_vote=True)
            await interaction.response.send_message(fmt(label), view=view, ephemeral=True)
        else:
            prompt = "Choose who to vote for:" if max_votes == 1 else f"Cast your vote (1 of {max_votes}):"
            view   = VoteTargetView(interaction.guild_id, options, rows)
            await interaction.response.send_message(prompt, view=view, ephemeral=True)

    async def abstain(self, interaction: discord.Interaction):
        state = db_get_state(interaction.guild_id) or {}
        if state.get("phase") == "night":
            return await interaction.response.send_message(
                "❌ Voting is closed during the night phase.", ephemeral=True)
        is_alive, in_game, _ = self._get_player_status(interaction)
        if not in_game:
            return await interaction.response.send_message("❌ You are not in the current game.", ephemeral=True)
        if not is_alive:
            return await interaction.response.send_message("❌ Eliminated players cannot vote.", ephemeral=True)
        day_num = db_get_night_num(interaction.guild_id)
        db_set_day_vote(interaction.guild_id, interaction.user.id, None, day_num)
        db_record_vote_history(interaction.guild_id, day_num, interaction.user.id, None, "abstain")
        await refresh_day_vote(interaction.guild)
        await interaction.response.send_message("✅ You are abstaining this vote.", ephemeral=True)

    async def remove_vote(self, interaction: discord.Interaction):
        db_remove_day_vote(interaction.guild_id, interaction.user.id)
        await refresh_day_vote(interaction.guild)
        await interaction.response.send_message("✅ Your vote has been removed.", ephemeral=True)

    async def full_breakdown(self, interaction: discord.Interaction):
        votes = db_get_day_votes(interaction.guild_id)
        rows  = db_get_assignments(interaction.guild_id)
        pid_to_name = {pid: (interaction.guild.get_member(pid).display_name
                             if interaction.guild.get_member(pid) else str(pid))
                       for pid, _, _, _ in rows}
        if not votes:
            return await interaction.response.send_message("No votes cast yet.", ephemeral=True)
        lines = []
        for voter_id, target_id, voted_at, *_ in votes:
            voter  = pid_to_name.get(voter_id, str(voter_id))
            target = "Abstain" if target_id is None else pid_to_name.get(target_id, str(target_id))
            try:
                from datetime import timezone as _tz
                _dt = datetime.strptime(voted_at[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=_tz.utc)
                ts_str = f" <t:{int(_dt.timestamp())}:t>"
            except Exception:
                ts_str = ""
            lines.append(f"{voter} → **{target}**{ts_str}")
        embed = discord.Embed(title="👁️ Full Vote Breakdown", description="\n".join(lines), color=0xFF4444)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def vote_log(self, interaction: discord.Interaction):
        """Rolling log of every vote cast with timestamps."""
        votes       = db_get_day_votes(interaction.guild_id)
        rows        = db_get_assignments(interaction.guild_id)
        npcs_vl     = db_get_npcs(interaction.guild_id)
        npc_map_vl  = {n["npc_id"]: n["name"] for n in npcs_vl}
        def get_name(pid):
            m = interaction.guild.get_member(pid)
            return npc_map_vl.get(pid) or (m.display_name if m else str(pid))
        if not votes:
            return await interaction.response.send_message("No votes cast yet.", ephemeral=True)
        lines = []
        for voter_id, target_id, voted_at, *_ in votes:
            voter  = get_name(voter_id)
            target = "Abstain" if target_id is None else get_name(target_id)
            try:
                from datetime import timezone as _tz2
                _dt2 = datetime.strptime(voted_at[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=_tz2.utc)
                ts_str = f"<t:{int(_dt2.timestamp())}:t>"
            except Exception:
                ts_str = "`?`"
            lines.append(f"{ts_str} — **{voter}** → {target}")
        embed = discord.Embed(
            title       = "🕐 Vote Log",
            description = "\n".join(lines),
            color       = 0x3498DB
        )
        embed.set_footer(text="Sorted by time cast — most recent at bottom")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def not_voted(self, interaction: discord.Interaction):
        """Show alive players who have not yet cast a vote."""
        votes   = db_get_day_votes(interaction.guild_id)
        rows    = db_get_assignments(interaction.guild_id)
        npcs_nv = db_get_npcs(interaction.guild_id)
        npc_map_nv = {n["npc_id"]: n["name"] for n in npcs_nv}
        voted_ids = {v[0] for v in votes}
        alive     = [(r[0], r[1]) for r in rows if r[2] == 1]
        missing   = []
        for pid, role in alive:
            if pid not in voted_ids:
                m    = interaction.guild.get_member(pid)
                name = npc_map_nv.get(pid) or (m.display_name if m else str(pid))
                missing.append(name)
        if not missing:
            return await interaction.response.send_message(
                "✅ All alive players have voted.", ephemeral=True)
        embed = discord.Embed(
            title       = f"⚠️ Has Not Voted ({len(missing)})",
            description = "\n".join(f"• {n}" for n in sorted(missing)),
            color       = 0xE74C3C
        )
        embed.set_footer(text="These players have not cast or abstained yet")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def clear_votes(self, interaction: discord.Interaction):
        state    = cached_get_state(interaction.guild_id)
        mod_role = interaction.guild.get_role(state.get("mod_role_id") or 0)
        if not interaction.user.guild_permissions.administrator and not (mod_role and mod_role in interaction.user.roles):
            return await interaction.response.send_message("❌ Only mods can clear votes.", ephemeral=True)
        db_clear_day_votes(interaction.guild_id)
        await refresh_day_vote(interaction.guild)
        await interaction.response.send_message("✅ All day votes cleared.", ephemeral=True)


class FrenzyVotePickView(View):
    def __init__(self, guild_id, options, rows, v1_name: str, v2_name: str):
        super().__init__(timeout=60)
        self.guild_id = guild_id
        self.options  = options
        self.rows     = rows

        btn1 = Button(label=f"Change Vote 1 ({v1_name})"[:80], style=discord.ButtonStyle.blurple)
        btn2 = Button(label=f"Change Vote 2 ({v2_name})"[:80], style=discord.ButtonStyle.blurple)
        btn1.callback = self.change_vote1
        btn2.callback = self.change_vote2
        self.add_item(btn1)
        self.add_item(btn2)

    async def change_vote1(self, interaction: discord.Interaction):
        view = VoteTargetView(self.guild_id, self.options, self.rows, is_second_vote=False)
        await interaction.response.edit_message(
            content=fmt("Choose your new **Vote 1** target:"), view=view)

    async def change_vote2(self, interaction: discord.Interaction):
        view = VoteTargetView(self.guild_id, self.options, self.rows, is_second_vote=True)
        await interaction.response.edit_message(
            content=fmt("Choose your new **Vote 2** target:"), view=view)


class VoteTargetView(View):
    def __init__(self, guild_id, options, rows, is_second_vote=False):
        super().__init__(timeout=60)
        self.guild_id       = guild_id
        self.is_second_vote = is_second_vote
        label = "Choose your SECOND vote target (frenzy)..." if is_second_vote else "Choose your vote target..."
        sel = Select(placeholder=label, options=options[:25])
        sel.callback = self.on_select
        self.add_item(sel)

    async def on_select(self, interaction: discord.Interaction):
        target_id = int(interaction.data["values"][0])
        day_num   = db_get_night_num(interaction.guild_id)
        target    = interaction.guild.get_member(target_id)
        tname     = target.display_name if target else str(target_id)

        if self.is_second_vote:
            # Check if changing an existing vote2 — log the flip
            existing_2 = db_get_day_votes_2(interaction.guild_id)
            prev2      = next((v[1] for v in existing_2 if v[0] == interaction.user.id), None)
            is_change2 = prev2 is not None and prev2 != target_id
            db_set_day_vote_2(interaction.guild_id, interaction.user.id, target_id)
            if is_change2:
                action2 = "change2"
                prev2_m    = interaction.guild.get_member(prev2)
                prev2_name = prev2_m.display_name if prev2_m else str(prev2)
                state_vc2  = cached_get_state(interaction.guild_id)
                dv_ch2     = interaction.guild.get_channel(state_vc2.get("day_vote_ch_id") or 0)
                change_msg2 = (
                    f"🔄 **{interaction.user.display_name}** changed their **frenzy vote 2**: "
                    f"~~{prev2_name}~~ → **{tname}**"
                )
                if dv_ch2:
                    await dv_ch2.send(change_msg2)
                await post_mod_log(interaction.guild,
                    f"🔄 **Frenzy Vote 2 Changed — Day {day_num}**\n"
                    f"**{interaction.user.display_name}**: ~~{prev2_name}~~ → **{tname}**")
            else:
                action2 = "vote2"
            db_record_vote_history(interaction.guild_id, day_num, interaction.user.id, target_id, action2)
        else:
            # Check if changing an existing vote — log the flip
            existing  = db_get_day_votes(interaction.guild_id)
            prev      = next((v[1] for v in existing if v[0] == interaction.user.id), None)
            is_change = prev is not None and prev != target_id
            db_set_day_vote(interaction.guild_id, interaction.user.id, target_id, day_num)
            if is_change:
                prev_m    = interaction.guild.get_member(prev)
                prev_name = prev_m.display_name if prev_m else str(prev)
                # Record change — keeps full history including the old vote
                db_record_vote_history(
                    interaction.guild_id, day_num,
                    interaction.user.id, target_id, "change")
                change_msg = (
                    f"🔄 **{interaction.user.display_name}** changed their vote: "
                    f"~~{prev_name}~~ \u2192 **{tname}**"
                )
                # Post publicly to day-vote channel — all players see flips
                state_vc  = cached_get_state(interaction.guild_id)
                dv_ch_pub = interaction.guild.get_channel(
                    state_vc.get("day_vote_ch_id") or 0)
                if dv_ch_pub:
                    await dv_ch_pub.send(change_msg)
                # Mod-log copy with day number context
                await post_mod_log(interaction.guild,
                    f"🔄 **Vote Changed — Day {day_num}**\n"
                    f"**{interaction.user.display_name}** changed vote: "
                    f"~~{prev_name}~~ \u2192 **{tname}**")
            else:
                # First vote — record as "vote"
                db_record_vote_history(
                    interaction.guild_id, day_num,
                    interaction.user.id, target_id, "vote")

        await refresh_day_vote(interaction.guild)
        label = "second vote" if self.is_second_vote else "vote"
        await _safe_edit(interaction,
            content=fmt(f"✅ {label.capitalize()} cast for {tname}."))
        self.stop()

        # ── Nomination ping — notify player on their first vote against them ──
        if not self.is_second_vote:
            try:
                all_votes = db_get_day_votes(interaction.guild_id)
                votes_against = [v for v in all_votes if v[1] == target_id]
                if len(votes_against) == 1:  # First vote against this player
                    rows_np  = db_get_assignments(interaction.guild_id)
                    tgt_row  = next((r for r in rows_np if r[0] == target_id and r[2] == 1), None)
                    if tgt_row and tgt_row[3]:
                        priv_ch = interaction.guild.get_channel(tgt_row[3])
                        if priv_ch:
                            await priv_ch.send(fmt(
                                f"🎯 **You have been nominated.**\n"
                                f"Someone has cast the first vote against you today.\n"
                                f"Now is the time to defend yourself in village chat."))
            except Exception as e:
                _log_error("nomination_ping", e)

        # Trigger NPC reaction if target is an NPC
        npcs_r = db_get_npcs(interaction.guild_id)
        target_npc = next((n for n in npcs_r if n["npc_id"] == target_id and n["is_alive"]), None)
        if target_npc:
            state_v = cached_get_state(interaction.guild_id)
            vc_ch_v = interaction.guild.get_channel(state_v.get("village_chat_ch_id") or 0)
            if vc_ch_v:
                safe_task(_npc_react_to_vote_against(
                    interaction.guild, interaction.guild_id, target_npc,
                    interaction.user.display_name), "npc_vote_react")



@tree.command(name="game_snapshot", description="Show current game standings, vote state, and night action status in one embed")
@is_mod()
async def game_snapshot(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    guild_id  = interaction.guild_id
    guild     = interaction.guild
    state     = db_get_state(guild_id) or {}
    rows      = db_get_assignments(guild_id)
    npcs      = db_get_npcs(guild_id)
    npc_map   = {n["npc_id"]: n["name"] for n in npcs}
    night_num = db_get_night_num(guild_id)
    phase     = state.get("phase", "day").capitalize()

    def get_name(pid):
        return npc_map.get(pid) or (guild.get_member(pid).display_name if guild.get_member(pid) else str(pid))

    # ── Team counts ───────────────────────────────────────────────────────
    alive      = [r for r in rows if r[2] == 1]
    village_a  = [r for r in alive if get_team(guild_id, r[1]) == "village"]
    wolf_a     = [r for r in alive if get_team(guild_id, r[1]) == "wolf"]
    neutral_a  = [r for r in alive if get_team(guild_id, r[1]) == "neutral"]
    dead       = [r for r in rows if r[2] == 0]

    embed = discord.Embed(
        title = f"📊 Game Snapshot — {phase} {night_num}",
        color = 0x5865F2
    )

    # ── Alive players by team ─────────────────────────────────────────────
    def team_list(team_rows):
        return "\n".join(
            f"• {get_name(r[0])} ({r[1]})" + (" `NPC`" if r[0] in npc_map else "")
            for r in team_rows
        ) or "*None*"

    embed.add_field(name=f"🏘️ Village ({len(village_a)})", value=team_list(village_a), inline=True)
    embed.add_field(name=f"🐺 Wolf ({len(wolf_a)})",    value=team_list(wolf_a),    inline=True)
    if neutral_a:
        embed.add_field(name=f"⚖️ Neutral ({len(neutral_a)})", value=team_list(neutral_a), inline=True)

    # ── Dead players ──────────────────────────────────────────────────────
    if dead:
        dead_names = ", ".join(get_name(r[0]) for r in dead)
        embed.add_field(name=f"💀 Eliminated ({len(dead)})", value=dead_names, inline=False)

    # ── Vote state (day only) ─────────────────────────────────────────────
    if state.get("phase") == "day":
        votes     = db_get_day_votes(guild_id)
        voted     = {v[0] for v in votes}
        not_voted = [get_name(r[0]) for r in alive if r[0] not in voted and r[0] not in npc_map]
        tally     = {}
        for voter_id, target_id, *_ in votes:
            if target_id:
                tally[target_id] = tally.get(target_id, 0) + 1
        if tally:
            all_votes_snap = db_get_day_votes(guild_id)
            voter_map = {}  # target_id -> [voter_names]
            for vid, tid, *_ in all_votes_snap:
                if tid: voter_map.setdefault(tid, []).append(get_name(vid))
            total_snap = sum(tally.values())
            sorted_t   = sorted(tally.items(), key=lambda x: x[1], reverse=True)
            vote_lines = []
            for t, c in sorted_t:
                bar     = _vote_bar(c, total_snap)
                pct     = round(c / total_snap * 100) if total_snap else 0
                voters  = voter_map.get(t, [])
                vnames  = ", ".join(voters) if voters else "?"
                vote_lines.append(f"**{get_name(t)}** — {c} ({pct}%)  {bar}\n└ {vnames}")
            embed.add_field(name="🗳️ Current Vote Tally", value="\n".join(vote_lines), inline=False)
        if not_voted:
            embed.add_field(name="⚠️ Haven't Voted", value=", ".join(not_voted), inline=False)

    # ── Night action status (night only) ─────────────────────────────────
    if state.get("phase") == "night":
        actions      = db_get_night_actions(guild_id, night_num)
        submitted    = {a[0] for a in actions if not a[1].startswith("_")}
        pending      = []
        for r in alive:
            pid, role = r[0], r[1]
            if pid in npc_map: continue
            if role in NIGHT_NO_BUTTON_ROLES: continue
            if role not in ROLE_VIEW_MAP: continue
            if pid not in submitted:
                pending.append(f"{get_name(pid)} ({role})")
        if pending:
            embed.add_field(name="⏳ Awaiting Night Actions", value="\n".join(pending), inline=False)
        else:
            embed.add_field(name="✅ Night Actions", value="All submitted.", inline=False)

    # ── Timer ─────────────────────────────────────────────────────────────
    end_ts = state.get("night_end_time") if state.get("phase") == "night" else state.get("day_vote_end_time")
    end_str = f"<t:{end_ts}:t> · <t:{end_ts}:R>" if end_ts else "Not set"
    embed.add_field(name="⏰ Phase Ends", value=end_str, inline=False)

    # ── Frenzy ────────────────────────────────────────────────────────────
    frenzy_day = state.get("agitator_frenzy_day")
    is_frenzy  = frenzy_day and int(frenzy_day) == int(night_num) and state.get("phase") == "day"
    if is_frenzy:
        elim_done = state.get("agitator_elim_count", 0)
        embed.add_field(name="⚡ Frenzy Active", value=f"{elim_done}/2 eliminations complete", inline=True)

    # ── Wraith marks ──────────────────────────────────────────────────────
    wmarks = db_get_wraith_marks(guild_id)
    if wmarks:
        embed.add_field(name="👻 Wraith Marks", value=f"{len(wmarks)} active mark(s)", inline=True)

    # ── Mod ───────────────────────────────────────────────────────────────
    mod_role = guild.get_role(state.get("mod_role_id") or 0)
    if mod_role:
        mod_names = [m.display_name for m in guild.members if mod_role in m.roles]
        if mod_names:
            embed.add_field(name="🎮 Mod", value=", ".join(mod_names[:3]), inline=True)

    wolves_needed = max(0, len(village_a) + len(neutral_a) - len(wolf_a))
    embed.set_footer(text=f"Wolves need {wolves_needed} more kill(s) to win · Total alive: {len(alive)}")
    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.command(name="vote_status", description="Show who has and hasn't voted in the current day vote")
@is_mod()
@app_commands.describe(post="Also post to mod-log for reference (default False)")
async def vote_status(interaction: discord.Interaction, post: bool = False):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    rows  = db_get_assignments(interaction.guild_id)
    votes = db_get_day_votes(interaction.guild_id)
    npcs  = db_get_npcs(interaction.guild_id)

    alive_rows = [r for r in rows if r[2] == 1]
    voted_ids  = {v[0] for v in votes}

    has_voted    = []
    not_voted    = []
    abstained    = []

    for pid, role_name, is_alive, _ in alive_rows:
        m = interaction.guild.get_member(pid)
        npc_match = next((n for n in npcs if n["npc_id"] == pid), None)
        name = npc_match["name"] if npc_match else (m.display_name if m else str(pid))
        npc_tag = " `NPC`" if npc_match else ""

        if pid in voted_ids:
            vote = next((v for v in votes if v[0] == pid), None)
            if vote and vote[1] is None:
                abstained.append(f"🤐 {name}{npc_tag}")
            else:
                target_id = vote[1] if vote else None
                target_row = next((r for r in rows if r[0] == target_id), None)
                target_m = interaction.guild.get_member(target_id) if target_id else None
                target_npc = next((n for n in npcs if n["npc_id"] == target_id), None)
                target_name = target_npc["name"] if target_npc else (target_m.display_name if target_m else str(target_id))
                has_voted.append(f"✅ {name}{npc_tag} → **{target_name}**")
        else:
            not_voted.append(f"❌ {name}{npc_tag}")

    embed = discord.Embed(
        title = "🗳️ Day Vote Status",
        color = 0x5865F2
    )
    if has_voted:
        embed.add_field(
            name  = f"✅ Voted ({len(has_voted)})",
            value = "\n".join(has_voted) or "None",
            inline= False
        )
    if abstained:
        embed.add_field(
            name  = f"🤐 Abstaining ({len(abstained)})",
            value = "\n".join(abstained) or "None",
            inline= False
        )
    if not_voted:
        embed.add_field(
            name  = f"❌ Not voted yet ({len(not_voted)})",
            value = "\n".join(not_voted) or "None",
            inline= False
        )

    total_alive = len(alive_rows)
    total_responded = len(has_voted) + len(abstained)
    embed.set_footer(text=f"{total_responded}/{total_alive} alive players have responded")
    await interaction.followup.send(embed=embed, ephemeral=True)


class DayVoteTimeModal(discord.ui.Modal, title="Set Day Vote End Time (EST)"):
    end_time = discord.ui.TextInput(
        label       = "Vote ends at (EST)",
        placeholder = "e.g. 9:00 PM  or  21:00",
        max_length  = 20,
        required    = True,
    )

    def __init__(self, guild_id, anonymous):
        super().__init__()
        self.guild_id  = guild_id
        self.anonymous = anonymous

    async def on_submit(self, interaction: discord.Interaction):
        from datetime import datetime, timezone, timedelta
        import re

        raw = self.end_time.value.strip().upper()

        # Parse time — support 12hr (9:00 PM, 9PM) and 24hr (21:00)
        est = timezone(timedelta(hours=-5))  # EST = UTC-5
        edt = timezone(timedelta(hours=-4))  # EDT = UTC-4 (daylight saving)

        # Detect AM/PM
        is_pm = "PM" in raw
        is_am = "AM" in raw
        clean = raw.replace("PM","").replace("AM","").replace(" ","").strip()

        try:
            if ":" in clean:
                h, m = clean.split(":")
                h, m = int(h), int(m)
            else:
                h, m = int(clean), 0

            if is_pm and h != 12:
                h += 12
            elif is_am and h == 12:
                h = 0

            # Use EDT (UTC-4) from March to November, EST (UTC-5) otherwise
            import time as _t
            now_utc = datetime.now(timezone.utc)
            # Simple DST check — EDT runs roughly March to November
            use_edt = 3 <= now_utc.month <= 11
            tz_offset = edt if use_edt else est

            now_local = datetime.now(tz_offset)
            end_local = now_local.replace(hour=h, minute=m, second=0, microsecond=0)

            # If the time has already passed today, assume tomorrow
            if end_local <= now_local:
                end_local += timedelta(days=1)

            end_ts = int(end_local.timestamp())

        except Exception:
            return await interaction.response.send_message(
                "❌ Could not parse that time. Try formats like `9:00 PM`, `21:00`, or `9PM`.",
                ephemeral=True)

        tz_name = "EDT" if use_edt else "EST"

        # Store and start the vote
        db_clear_day_votes(self.guild_id)
        db_set_state(self.guild_id,
                     anon_vote=1 if self.anonymous else 0,
                     day_vote_msg_id=None,
                     day_vote_end_time=end_ts)
        invalidate_cache(self.guild_id)

        await refresh_day_vote(interaction.guild)
        await log_event(interaction.guild, "Day",
                        f"🗳️ Day vote opened — closes <t:{end_ts}:F>")
        await set_bot_status("☀️ Day vote open")

        await interaction.response.send_message(
            f"✅ Day vote started.\n"
            f"**Closes:** <t:{end_ts}:F> your time\n"
            f"**Countdown:** <t:{end_ts}:R>\n"
            f"*(Set as {raw} {tz_name})*",
            ephemeral=True)

        # Schedule auto-close
        guild = interaction.guild
        guild_id = self.guild_id
        anonymous = self.anonymous
        async def auto_close():
            import time as _t
            wait = end_ts - int(_t.time())
            if wait > 0:
                await asyncio.sleep(wait)
            current = db_get_state(guild_id) or {}
            if current.get("day_vote_end_time") == end_ts:
                snap_ch    = guild.get_channel(current.get("day_vote_ch_id") or 0)
                snap_votes = db_get_day_votes(guild_id)
                snap_rows  = db_get_assignments(guild_id)
                night_num  = db_get_night_num(guild_id)
                if snap_ch and snap_votes:
                    snap_embed = build_day_vote_embed(guild, snap_votes, snap_rows)
                    snap_embed.title = f"📋 Day {night_num} Vote — Final Results"
                    snap_embed.color = 0x95A5A6
                    snap_embed.set_footer(text="Vote timer ended. Results are permanent.")
                    await snap_ch.send(embed=snap_embed)
                    db_set_state(guild_id, day_vote_msg_id=None)
                    invalidate_cache(guild_id)
                db_set_state(guild_id, day_vote_end_time=None, anon_vote=0)
                await refresh_day_vote(guild)
                vote_summary  = _build_vote_summary(guild, guild_id)
                state_cur     = db_get_state(guild_id) or {}
                mod_ch_prompt = guild.get_channel(state_cur.get("mod_log_channel_id") or 0)
                if mod_ch_prompt:
                    embed_prompt = discord.Embed(
                        title       = "⏰ Day Vote Closed — Results",
                        description = vote_summary,
                        color       = 0xF39C12
                    )
                    view_prompt = DayVoteClosedPromptView(guild_id)
                    await mod_ch_prompt.send(embed=embed_prompt, view=view_prompt)

        asyncio.create_task(auto_close())

        # Trigger NPC day votes
        async def _run_npc_day_votes():
            await asyncio.sleep(random.uniform(20, 60))
            for npc in db_get_npcs(self.guild_id):
                if npc["is_alive"]:
                    await _npc_day_vote(interaction.guild, self.guild_id, npc)
                    await asyncio.sleep(random.uniform(5, 20))
        safe_task(_run_npc_day_votes(), "npc_day_votes")


@tree.command(name="set_vote", description="Configure the current day vote — votes per player and anonymity")
@is_mod()
@app_commands.describe(
    votes_per_player="How many votes each player must cast (1 = normal, 2 = frenzy)",
    anonymous="Hide voter names until vote closes")
async def set_vote(interaction: discord.Interaction,
                   votes_per_player: int = 1,
                   anonymous: bool = False):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    night_num = db_get_night_num(interaction.guild_id)

    if votes_per_player == 2:
        # Manually trigger frenzy mode
        db_set_state(interaction.guild_id,
                     agitator_frenzy_day=night_num,
                     agitator_elim_count=0)
        frenzy_note = "⚡ **Frenzy active** — all players must vote **twice**."
    elif votes_per_player == 1:
        # Clear frenzy if it was set
        db_set_state(interaction.guild_id,
                     agitator_frenzy_day=None,
                     agitator_elim_count=0)
        frenzy_note = "🗳️ Standard vote — one vote per player."
    else:
        return await interaction.followup.send(
            "❌ votes_per_player must be 1 or 2.", ephemeral=True)

    # Set anonymity
    db_set_state(interaction.guild_id, anon_vote=1 if anonymous else 0)
    invalidate_cache(interaction.guild_id)

    anon_note = "🕵️ **Anonymous** — voter names hidden until vote closes." if anonymous else "👁️ **Public** — voter names visible."

    # Refresh the vote embed to reflect changes
    await refresh_day_vote(interaction.guild)

    await interaction.followup.send(
        f"✅ Vote settings updated.\n{frenzy_note}\n{anon_note}",
        ephemeral=True)

    await post_mod_log(interaction.guild,
        f"⚙️ **Vote settings changed by mod**\n"
        f"Votes per player: **{votes_per_player}**\n"
        f"Anonymous: **{anonymous}**")

@tree.command(name="start_day_vote", description="Start or reset the day vote")
@is_mod()
@app_commands.describe(
    anonymous="Hide voter names until vote closes (default False)",
    votes_per_player="How many votes each player gets (1 = normal, 2 = frenzy, 3+ = special)")
async def start_day_vote(interaction: discord.Interaction,
                         anonymous: bool = False,
                         votes_per_player: int = 1):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    if votes_per_player < 1 or votes_per_player > 5:
        return await interaction.response.send_message(
            "❌ votes_per_player must be between 1 and 5.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    # Snapshot current votes before clearing so results stay in chat history
    state     = cached_get_state(interaction.guild_id)
    dv_ch     = interaction.guild.get_channel(state.get("day_vote_ch_id") or 0)
    old_votes = db_get_day_votes(interaction.guild_id)
    old_rows  = db_get_assignments(interaction.guild_id)
    if old_votes and dv_ch:
        night_num = db_get_night_num(interaction.guild_id)
        snapshot_embed = build_day_vote_embed(interaction.guild, old_votes, old_rows)
        snapshot_embed.title = f"📋 Day {night_num} Vote — Final Results"
        snapshot_embed.color = 0x95A5A6
        snapshot_embed.set_footer(text="This vote has closed. Results are permanent.")
        await dv_ch.send(embed=snapshot_embed)
        db_set_state(interaction.guild_id, day_vote_msg_id=None)
        invalidate_cache(interaction.guild_id)

    # Set frenzy if votes_per_player >= 2
    night_num_check = db_get_night_num(interaction.guild_id)
    if votes_per_player >= 2:
        db_set_state(interaction.guild_id,
                     agitator_frenzy_day=night_num_check,
                     agitator_elim_count=0,
                     votes_per_player=votes_per_player)
    else:
        # Auto-detect agitator frenzy from night actions
        actions_check = db_get_night_actions(interaction.guild_id, night_num_check)
        if any(a[1] == "agitator_frenzy" for a in actions_check):
            state_cur = db_get_state(interaction.guild_id) or {}
            if not state_cur.get("agitator_frenzy_day"):
                db_set_state(interaction.guild_id,
                             agitator_frenzy_day=night_num_check,
                             agitator_elim_count=0,
                             votes_per_player=2)
                votes_per_player = 2
        else:
            db_set_state(interaction.guild_id, votes_per_player=1)

    db_clear_day_votes(interaction.guild_id)
    db_set_state(interaction.guild_id,
                 anon_vote=1 if anonymous else 0,
                 day_vote_msg_id=None)
    invalidate_cache(interaction.guild_id)
    _sdv_state = cached_get_state(interaction.guild_id)
    _sdv_dur   = int(_sdv_state.get("day_duration") or 43200)
    end_time   = _phase_end_ts(_sdv_dur, 20, interaction.guild_id)
    db_set_state(interaction.guild_id, day_vote_end_time=end_time)

    await refresh_day_vote(interaction.guild)
    vote_note = f" — {votes_per_player} vote(s) per player" if votes_per_player > 1 else ""
    await log_event(interaction.guild, "Day", f"🗳️ Day vote opened{vote_note}")
    await set_bot_status("☀️ Day vote open")
    await interaction.followup.send(
        f"✅ Day vote started. Closes at <t:{end_time}:t> (<t:{end_time}:R>).{vote_note}",
        ephemeral=True)

    # Send Governor and Hermit their day ability buttons
    safe_task(_send_day_ability_buttons(
        interaction.guild, interaction.guild_id, end_time), "day_ability_buttons")

    # Vote countdown reminder and night approach warning
    safe_task(_vote_countdown_reminder(interaction.guild, interaction.guild_id, int(end_time)), "vote_reminder")
    safe_task(_night_approach_warning(interaction.guild, interaction.guild_id), "night_warning")

    # Trigger NPC day votes after a natural delay
    async def _run_npc_day_votes():
        await asyncio.sleep(random.uniform(20, 60))
        for npc in db_get_npcs(interaction.guild_id):
            if npc["is_alive"]:
                await _npc_day_vote(interaction.guild, interaction.guild_id, npc)
                await asyncio.sleep(random.uniform(5, 20))
    safe_task(_run_npc_day_votes(), "npc_day_votes")



@tree.command(name="clear_day_votes", description="Clear all current day votes")
@is_mod()
async def clear_day_votes(interaction: discord.Interaction):
    db_clear_day_votes(interaction.guild_id)
    db_set_state(interaction.guild_id, day_vote_end_time=None)
    await refresh_day_vote(interaction.guild)
    await interaction.response.send_message("✅ Day votes cleared.", ephemeral=True)

# ====================== WOLF VOTE ======================
def build_wolf_vote_options(guild, guild_id):
    """
    Build SelectOption list for the wolf kill vote dropdown.
    Shows ALL non-wolf players — alive ones selectable, dead ones shown with
    💀 prefix and marked as a description so wolves can see them but know
    they are already dead. Discord does not support truly disabled options
    in selects, so we label dead players clearly and reject them on submit.
    """
    rows = db_get_assignments(guild_id)
    non_wolf = [r for r in rows if get_team(guild_id, r[1]) != "wolf"]
    if not non_wolf:
        return []
    options = []
    for pid, role_name, is_alive, _ in non_wolf[:25]:
        m = guild.get_member(pid) if guild else None
        name = m.display_name if m else f"Player {pid}"
        if is_alive:
            options.append(discord.SelectOption(
                label=f"✅ {name}",
                value=str(pid),
                description="Alive — valid kill target"
            ))
        else:
            options.append(discord.SelectOption(
                label=f"💀 {name} (dead)",
                value=f"dead_{pid}",   # dead_ prefix — rejected on submit
                description="Already eliminated — cannot be targeted"
            ))
    return options
def _is_wolf_blocked(guild_id, actor_id, night_num) -> bool:
    """Check if a wolf is blocked from killing this night (e.g. Diseased, Wolf Pup)."""
    conn_bl = sqlite3.connect(DB_FILE)
    c_bl    = conn_bl.cursor()
    c_bl.execute(
        "SELECT COUNT(*) FROM block_log WHERE guild_id=? AND blocked_id=? AND night_num=?",
        (guild_id, actor_id, night_num))
    count = c_bl.fetchone()[0]
    conn_bl.close()
    return count > 0

class WolfVoteView(View):
    """
    Persistent wolf kill vote dropdown.
    Always reads alive non-wolf players fresh from DB so it stays accurate
    even if someone is eliminated mid-night.
    """
    def __init__(self, guild_id, night_num):
        super().__init__(timeout=None)
        self.guild_id  = guild_id
        self.night_num = night_num
        self._build(guild_id)

    def _build(self, guild_id):
        self.clear_items()
        # Note: guild is not available at __init__ time via guild_id alone,
        # so options are patched with real names in refresh_wolf_vote.
        # We use placeholder options here that get replaced immediately.
        rows = db_get_assignments(guild_id)
        non_wolf = [r for r in rows if get_team(guild_id, r[1]) != "wolf"]
        if not non_wolf:
            btn = Button(label="No targets available", disabled=True,
                         style=discord.ButtonStyle.secondary)
            self.add_item(btn)
            return
        # Build options with real names — include NPCs
        npcs_wv  = db_get_npcs(guild_id)
        npc_map_wv = {n["npc_id"]: n["name"] for n in npcs_wv}
        options = []
        for pid, role_name, is_alive, _ in non_wolf[:25]:
            name = npc_map_wv.get(pid, f"Player {pid}")
            if is_alive:
                options.append(discord.SelectOption(
                    label=f"✅ {name}"[:100],
                    value=str(pid),
                    description="🤖 NPC" if pid in npc_map_wv else "Alive"
                ))
            else:
                options.append(discord.SelectOption(
                    label=f"💀 {name} (dead)"[:100],
                    value=f"dead_{pid}",
                    description="Already eliminated"
                ))
        sel = Select(
            placeholder="🐺 Choose tonight's kill target...",
            options=options
        )
        sel.callback = self.on_vote
        self._sel = sel
        self.add_item(sel)

    async def on_vote(self, interaction: discord.Interaction):
        rows = db_get_assignments(interaction.guild_id)
        # Verify voter is an alive wolf
        me = next((r for r in rows if r[0] == interaction.user.id and r[2] == 1), None)
        if not me or get_team(interaction.guild_id, me[1]) != "wolf":
            return await interaction.response.send_message(
                "❌ Only alive wolves can vote here.", ephemeral=True)
        raw = interaction.data["values"][0]
        if raw.startswith("dead_"):
            return await interaction.response.send_message(
                "❌ That player is already dead — choose an alive target.", ephemeral=True)
        target_id  = int(raw)
        night_now  = db_get_night_num(interaction.guild_id)
        if _is_wolf_blocked(interaction.guild_id, interaction.user.id, night_now):
            return await interaction.response.send_message(
                "🤢 You are **poisoned from the Diseased** — you cannot kill this night.",
                ephemeral=True)
        # Double-check target is still alive in DB
        target_row = next((r for r in rows if r[0] == target_id and r[2] == 1), None)
        if not target_row:
            return await interaction.response.send_message(
                "❌ That player is no longer alive. Please choose again.", ephemeral=True)
        db_set_wolf_vote(interaction.guild_id, self.night_num, interaction.user.id, target_id)
        target = interaction.guild.get_member(target_id)
        tname  = target.display_name if target else str(target_id)
        voter  = interaction.guild.get_member(interaction.user.id)

        # Check if all alive wolves have voted for the same target — if so, lock it
        rows_wv    = db_get_assignments(interaction.guild_id)
        wolf_alive = [r[0] for r in rows_wv if r[2] == 1 and get_team(interaction.guild_id, r[1]) == "wolf"]
        wolf_votes = db_get_wolf_votes(interaction.guild_id, self.night_num)

        # Tally votes
        vote_tally = {}
        for wid, tid in wolf_votes:
            vote_tally.setdefault(tid, []).append(wid)
        top_target, top_voters = max(vote_tally.items(), key=lambda x: len(x[1])) if vote_tally else (None, [])

        # Post to mod-log
        await post_mod_log(interaction.guild,
            f"🐺 **Wolf Vote** — Night {self.night_num}\n"
            f"**{voter.display_name if voter else interaction.user.id}** voted to kill **{tname}**\n"
            f"*Pack vote tally: {len(wolf_votes)}/{len(wolf_alive)} vote(s) cast.*")

        # If all wolves agree on one target — post "Target Locked" to den
        if top_target and len(top_voters) == len(wolf_alive) and len(wolf_alive) > 0:
            state_wv = cached_get_state(interaction.guild_id)
            den_ch   = interaction.guild.get_channel(state_wv.get("wolf_channel_id") or 0)
            locked_m = interaction.guild.get_member(top_target)
            locked_name = locked_m.display_name if locked_m else str(top_target)
            if den_ch:
                lock_lines = [
                    f"🔒 **Target locked: {locked_name}**\n*The pack is in agreement. The hunt begins.*",
                    f"🔒 **Target locked: {locked_name}**\n*All wolves have spoken. No further deliberation needed.*",
                    f"🔒 **Target locked: {locked_name}**\n*The decision is made. Whisperfall won't know what's coming.*",
                ]
                await den_ch.send(random.choice(lock_lines))

        await refresh_wolf_vote(interaction.guild, self.night_num)
        await interaction.response.send_message(
            f"✅ Voted to kill **{tname}**. You can change your vote before night ends.",
            ephemeral=True)

# ====================== ELIMINATE ======================
class EliminateConfirmView(View):
    """Confirmation before eliminating a player — prevents accidents."""
    def __init__(self, guild_id, player, role_name, public, assignment, state):
        super().__init__(timeout=30)
        self.guild_id   = guild_id
        self.player     = player
        self.role_name  = role_name
        self.public     = public
        self.assignment = assignment
        self.state      = state
        yes = Button(label="✅ Confirm Eliminate", style=discord.ButtonStyle.danger)
        no  = Button(label="❌ Cancel",             style=discord.ButtonStyle.secondary)
        yes.callback = self.on_confirm
        no.callback  = self.on_cancel
        self.add_item(yes)
        self.add_item(no)

    async def on_cancel(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="❌ Elimination cancelled.", embed=None, view=None)

    async def on_confirm(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content=f"⏳ Eliminating **{self.player.display_name}**...", embed=None, view=None)
        await _run_elimination(interaction.guild, interaction,
                               self.player, self.role_name, self.public,
                               self.assignment, self.state)

    async def on_timeout(self):
        pass  # Just expires silently


async def _run_elimination(guild, interaction, player, role_name, public, assignment, state):
    """The actual elimination logic — called after confirmation."""
    # Cursed conversion
    if role_name == "Cursed":
        wolf_ch      = guild.get_channel(state.get("wolf_channel_id") or 0)
        if wolf_ch:
            await wolf_ch.set_permissions(player, view_channel=True, send_messages=True)
            await wolf_ch.send(fmt(f"🔄 {player.display_name} (Cursed) has joined the pack!"))

        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE player_assignments SET role_name='Cursed (Wolf)' WHERE guild_id=? AND player_id=?",
                  (guild.id, player.id))
        conn.commit()
        conn.close()
        await log_event(guild, state.get("phase","day").capitalize(),
                        f"🔄 **{player.display_name}** (Cursed) converted to wolves")
        try:
            await interaction.followup.send(
                f"⚠️ **{player.display_name}** was Cursed — converted to wolves.", ephemeral=True)
        except Exception:
            pass
        return

    # Elder vote-out
    if role_name == "Elder":
        await handle_elder_hit(guild, guild.id, killed_by_vote=True)

    await _eliminate_player(guild, player.id, "Moderator decision")

    # Village Jokester — gets to kill one voter
    if role_name == "Village Jokester":
        votes = db_get_day_votes(guild.id)
        voter_ids = [v for v, t in votes if t == player.id]
        rows_j = db_get_assignments(guild.id)
        jokester_row = next((r for r in rows_j if r[0] == player.id), None)
        if jokester_row and jokester_row[3] and voter_ids:
            priv_ch = guild.get_channel(jokester_row[3])
            if priv_ch:
                # Patch options with real display names
                view = JokesterKillView(guild.id, player.id, voter_ids)
                if hasattr(view, "children") and view.children:
                    sel = view.children[0]
                    if hasattr(sel, "options"):
                        new_opts = []
                        for opt in sel.options:
                            m = guild.get_member(int(opt.value))
                            name = m.display_name if m else opt.value
                            new_opts.append(discord.SelectOption(
                                label=name[:100], value=opt.value,
                                description="Voted against you"))
                        sel.options = new_opts
                await priv_ch.send(view=view)  # dropdown only — no message

    # Public announcement removed — mod-log only

    # NPC farewell message in village chat
    npcs_check2 = db_get_npcs(guild.id)
    elim_npc = next((n for n in npcs_check2 if n["npc_id"] == player_id and not n["is_alive"]), None)
    if elim_npc:
        vc_ch_f = guild.get_channel((db_get_state(guild.id) or {}).get("village_chat_ch_id") or 0)
        if vc_ch_f:
            game_ctx_f = _npc_game_context(guild, guild.id, elim_npc)
            system_f   = _npc_system_prompt(elim_npc)
            prompt_f   = (
                f"Game state:\n{game_ctx_f}\n\n"
                f"You have just been eliminated from the game. This is your last message in village chat.\n"
                f"Say something final — 1-2 sentences. Could be: a parting accusation, a hint at what you knew, "
                f"acceptance, defiance, or cryptic. Stay in character. Human and natural."
            )
            safe_task(_npc_farewell_message(guild, guild.id, elim_npc, vc_ch_f, prompt_f, system_f),
                      "npc_farewell")

    # Check NPC survival reaction
    votes = db_get_day_votes(guild.id)
    vote_count = {}
    for _, tid in votes:
        if tid: vote_count[tid] = vote_count.get(tid, 0) + 1
    if vote_count:
        top_id = max(vote_count, key=vote_count.get)
        if top_id != player.id:
            npcs_check = db_get_npcs(guild.id)
            if any(n["npc_id"] == top_id and n["is_alive"] for n in npcs_check):
                safe_task(_npc_survival_reaction(guild, guild.id, top_id), "npc_survival")

    try:
        await interaction.followup.send(
            f"✅ **{player.display_name}** eliminated (role: {role_name}).", ephemeral=True)
    except Exception:
        pass

    # ── Clone inheritance check ───────────────────────────────────────────
    safe_task(_check_clone_inheritance(guild, guild.id, player_id, role_name), "clone_inherit")

    # ── Werekitten voted out — silence next night actions ─────────────────
    if role_name == "Werekitten":
        vc_ch_wk = guild.get_channel((cached_get_state(guild.id) or {}).get("village_chat_ch_id") or 0)
        if vc_ch_wk:
            await vc_ch_wk.send(fmt(
                "🐱💔 **The village has made a terrible mistake.**\n"
                "The one eliminated was... adorable. Too adorable. Unbearably adorable.\n"
                "The village stands in stunned, grief-stricken silence.\n\n"
                "**All night actions are silenced this night. No one can bring themselves to act.**\n"
                "*The Werekitten's true identity has shattered everyone.*"))
        next_night = db_get_night_num(guild.id) + 1
        db_set_state(guild.id, werekitten_silenced_night=next_night)
        await post_mod_log(guild,
            f"🐱 **Werekitten eliminated** — Night {next_night} all player night actions are silenced per role rules.\n"
            f"Skip sending night action buttons to players when Night {next_night} starts.")

    # ── Gravedigger — auto-deliver death info ────────────────────────────────
    try:
        rows_gd = db_get_assignments(guild.id)
        gd_row  = next((r for r in rows_gd if r[1] == "Gravedigger" and r[2] == 1 and r[3]), None)
        if gd_row:
            gd_ch   = guild.get_channel(gd_row[3])
            if gd_ch:
                phase_gd = (cached_get_state(guild.id) or {}).get("phase", "day").capitalize()
                team_gd  = get_team(guild.id, role_name)
                team_emoji = "🐺" if team_gd == "wolf" else ("⚖️" if team_gd == "neutral" else "🏘️")
                night_gd = db_get_night_num(guild.id)
                embed_gd = discord.Embed(
                    title       = f"⚰️ Gravedigger Report — {phase_gd} {night_gd}",
                    description = f"**{player.display_name}** has been eliminated.",
                    color       = 0x2C3060
                )
                embed_gd.add_field(name="Role",   value=f"{team_emoji} {role_name}", inline=True)
                embed_gd.add_field(name="Team",   value=team_gd.capitalize(),        inline=True)
                embed_gd.add_field(name="Phase",  value=phase_gd,                    inline=True)
                embed_gd.set_footer(text="This information is yours alone. Use it wisely.")
                await gd_ch.send(embed=embed_gd)
    except Exception as e:
        print(f"[gravedigger_notify] {e}")

    # Log the elimination with reason
    try:
        # Try to get reason from calling context (passed via state or default)
        state_el = cached_get_state(guild.id) or {}
        elim_reason  = state_el.get("_pending_elim_reason", "unknown")
        elim_day_num = state_el.get("_pending_elim_day", db_get_night_num(guild.id))
        db_log_elimination(guild.id, player.id, role_name, elim_reason, elim_day_num)
        # Clear pending reason
        db_set_state(guild.id, **{"_pending_elim_reason": None, "_pending_elim_day": None})
    except Exception:
        pass

    winner = await check_win_condition(guild)
    if winner:
        await announce_win(guild, winner)
    else:
        # Milestone messages
        rows_m     = db_get_assignments(guild.id)
        alive_m    = [r for r in rows_m if r[2] == 1]
        wolves_m   = [r for r in alive_m if get_team(guild.id, r[1]) == "wolf"]
        village_m  = [r for r in alive_m if get_team(guild.id, r[1]) == "village"]
        vc_m       = guild.get_channel((cached_get_state(guild.id) or {}).get("village_chat_ch_id") or 0)
        if vc_m:
            wolves_needed_m = max(0, len(village_m) - len(wolves_m))
            if len(wolves_m) == 1:
                await vc_m.send(fmt(
                    "⚠️ *One wolf remains in Whisperfall. One.*\n"
                    "*The village is close — but so is the end.*"))
            elif wolves_needed_m == 1:
                await vc_m.send(fmt(
                    "🐺 *The wolves need only one more. One elimination and Whisperfall falls.*\n"
                    "*Choose carefully today.*"))
            elif len(alive_m) <= 4:
                await vc_m.send(fmt(
                    f"*{len(alive_m)} souls remain in Whisperfall.*\n"
                    "*Every face is a suspect. Every vote is a gamble.*"))

    # Refresh dashboard after every elimination
    safe_task(update_mod_dashboard(guild), "dashboard_elim")


@tree.command(name="eliminate", description="Eliminate a player from the game")
@is_mod()
@app_commands.describe(
    player="Player to eliminate (by name)",
    public="Announce publicly? (default True)",
    reason="Reason: vote / wolf_kill / mod_kill / cupid / witch / shadow_wolf / wraith / diseased / other",
    day_or_night="Day or night number (e.g. Day 2 = 2, Night 3 = 3)")
async def eliminate(interaction: discord.Interaction, player: str, public: bool = True,
                    reason: str = "vote", day_or_night: int = 0):
    await interaction.response.defer(ephemeral=True)

    rows = db_get_assignments(interaction.guild_id)
    npcs = db_get_npcs(interaction.guild_id)
    npc_map = {n["npc_id"]: n["name"] for n in npcs}

    # Safely convert player to string — handles case where Discord passes Member object
    player_str = player.display_name if hasattr(player, "display_name") else str(player)

    # Find player by name or ID — works even if they left the server
    assignment = None
    player_member = None
    for r in rows:
        if r[2] != 1:
            continue
        pid = r[0]
        name = npc_map.get(pid)
        if not name:
            m = interaction.guild.get_member(pid)
            name = m.display_name if m else str(pid)
            player_member = m
        if player_str.lower() in name.lower() or player_str == str(pid):
            assignment = r
            break

    if not assignment:
        return await interaction.followup.send(
            f"❌ Could not find alive player matching **{player_str}**.", ephemeral=True)

    pid       = assignment[0]
    role_name = assignment[1]
    # Get member — may be None if they left
    if not player_member:
        player_member = interaction.guild.get_member(pid)

    # Create a proxy object if member left server
    class PlayerProxy:
        def __init__(self, pid, name):
            self.id           = pid
            self.display_name = name
            self.mention      = f"<@{pid}>"
            self.roles        = []
        async def add_roles(self, *a, **k): pass
        async def remove_roles(self, *a, **k): pass

    npc_name = npc_map.get(pid)
    if npc_name:
        display_name = npc_name
    elif player_member:
        display_name = player_member.display_name
    else:
        display_name = player  # fallback to what mod typed

    player_obj = player_member or PlayerProxy(pid, display_name)

    state      = cached_get_state(interaction.guild_id)
    team       = get_team(interaction.guild_id, role_name)
    team_emoji = "🐺" if team == "wolf" else ("⚖️" if team == "neutral" else "🏘️")
    color      = 0xC0392B if team == "wolf" else (0xF39C12 if team == "neutral" else 0x27AE60)

    embed = discord.Embed(
        title       = "⚠️ Confirm Elimination",
        description = f"You are about to eliminate **{display_name}**.",
        color       = color
    )
    embed.add_field(name="Role",   value=f"{team_emoji} {role_name}", inline=True)
    embed.add_field(name="Team",   value=team.capitalize(),           inline=True)
    embed.add_field(name="Status", value="✅ Alive",                   inline=True)
    embed.set_footer(text="This action cannot be undone. Confirm within 30 seconds.")

    # Store reason and day/night number for logging when confirmed
    night_num_el = day_or_night if day_or_night > 0 else db_get_night_num(interaction.guild_id)
    db_set_state(interaction.guild_id, _pending_elim_reason=reason, _pending_elim_day=night_num_el)
    invalidate_cache(interaction.guild_id)

    view = EliminateConfirmView(
        interaction.guild_id, player_obj, role_name, public, assignment, state)
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)


@eliminate.autocomplete("player")
async def eliminate_autocomplete(interaction: discord.Interaction, current: str):
    rows    = db_get_assignments(interaction.guild_id)
    npcs    = db_get_npcs(interaction.guild_id)
    npc_map = {n["npc_id"]: n["name"] for n in npcs}
    choices = []
    for r in rows:
        if r[2] != 1:  # Only show alive players
            continue
        pid  = r[0]
        name = npc_map.get(pid)
        if not name:
            m    = interaction.guild.get_member(pid)
            name = m.display_name if m else f"Unknown ({pid})"
        if current.lower() in name.lower():
            choices.append(app_commands.Choice(name=name, value=name))
    return choices[:25]

# ====================== INTERNAL ELIMINATE ======================
async def _eliminate_player(guild: discord.Guild, player_id: int, reason: str):
    rows = db_get_assignments(guild.id)
    assignment = next((r for r in rows if r[0] == player_id and r[2] == 1), None)
    if not assignment:
        return

    db_set_player_alive(guild.id, player_id, False)
    db_remove_day_vote(guild.id, player_id)

    state  = cached_get_state(guild.id)
    player = guild.get_member(player_id)
    if not player:
        return

    dead_role = guild.get_role(state.get("dead_role_id") or 0)
    part_role = guild.get_role(state.get("participant_role_id") or 0)
    try:
        if part_role and part_role in player.roles: await player.remove_roles(part_role)
        if dead_role:                               await player.add_roles(dead_role)
    except discord.Forbidden:
        pass

    # Keep private channel open — ghost can still read and talk to mods there
    priv_ch = guild.get_channel(assignment[3] or 0)
    if priv_ch:
        await priv_ch.set_permissions(player, view_channel=True, send_messages=True, read_messages=True)
        # Send atmospheric farewell message
        _death_msgs = [
            "The village has spoken. Your time in Whisperfall is over.\n\n*You may remain here — watch, remember, say nothing of what you know.*",
            "The square grows quiet after a name is spoken with finality.\n\n*This channel remains yours. The mod will reach you here if needed. Whisperfall does not forget the ones it loses.*",
            "Every village has its cost. Today, that cost was yours.\n\n*Rest. Watch. This channel stays open — for the mod, and for memory.*",
            "Whisperfall has made its choice. You were part of something that will be remembered.\n\n*This is not the end of your story — only your role in it. Stay close.*",
            "The count changes. Your name moves from one list to another.\n\n*This channel is still yours. The mod can reach you here. The game continues without you, but not without what you knew.*",
        ]
        try:
            await priv_ch.send(fmt(random.choice(_death_msgs)))
        except Exception:
            pass

    # Grant ghost chat access — full send allowed
    ghost_ch = guild.get_channel(state.get("ghost_channel_id") or 0)
    if ghost_ch:
        await ghost_ch.set_permissions(player, view_channel=True, send_messages=True)
        # Welcome message in ghost chat
        try:
            await ghost_ch.send(
                fmt(f"👻 {player.mention} has joined the ghosts.\n"
                    f"*You may speak freely here — your role is your own to share or keep.\n"
                    f"React to the game, watch what unfolds, and remember what you knew.\n"
                    f"Whisperfall does not forget its dead.*"))
        except Exception:
            pass

    # Restrict village chat — dead players can read but not send
    vc_ch = guild.get_channel(state.get("village_chat_ch_id") or 0)
    if vc_ch:
        await vc_ch.set_permissions(player, view_channel=True, send_messages=False, read_messages=True)

    # Restrict day-vote channel — dead players can read but not vote
    dv_ch = guild.get_channel(state.get("day_vote_ch_id") or 0)
    if dv_ch:
        await dv_ch.set_permissions(player, view_channel=True, send_messages=False, read_messages=True)

    # Remove wolf den access silently
    if get_team(guild.id, assignment[1]) == "wolf" and assignment[1] not in {"Bloodhound", "Wolf Pup"}:
        wolf_ch = guild.get_channel(state.get("wolf_channel_id") or 0)
        if wolf_ch:
            await wolf_ch.set_permissions(player, overwrite=None)

    phase = state.get("phase", "day").capitalize()

    # ── Cupid bond check (current night's bond only — expires at morning) ─
    bond = db_get_cupid_current(guild.id)
    if bond:
        p1, p2 = bond
        partner_id = None
        if player_id == p1: partner_id = p2
        elif player_id == p2: partner_id = p1
        if partner_id:
            partner_row = next((r for r in rows if r[0] == partner_id and r[2] == 1), None)
            if partner_row:
                partner = guild.get_member(partner_id)
                partner_name = partner.display_name if partner else str(partner_id)
                await post_mod_log(guild,
                    f"💘 **Cupid Bond triggered** — {partner_name} dies of heartbreak.")
                # Clear bond BEFORE eliminating partner to prevent recursion
                db_clear_cupid_current(guild.id)
                if partner_row[1] == "Shadow Wolf":
                    safe_task(_generate_sw_kill_list(guild, guild.id, partner_id, player_id), "sw_kill_list")
                await _eliminate_player(guild, partner_id, "died of heartbreak (Cupid bond)")

    # ── Auto-refresh any player's tracker that includes this dead player ──
    try:
        conn_t = sqlite3.connect(DB_FILE)
        c_t    = conn_t.cursor()
        c_t.execute("SELECT DISTINCT owner_id, msg_id, ch_id FROM player_tracker_msg WHERE guild_id=?",
                    (guild.id,))
        tracker_rows = c_t.fetchall()
        conn_t.close()
        for owner_id, msg_id, ch_id in tracker_rows:
            if owner_id == player_id:
                continue  # Skip dead player's own tracker
            ch_t = guild.get_channel(ch_id)
            if not ch_t:
                continue
            try:
                msg_t = await ch_t.fetch_message(msg_id)
                from discord import Embed as _E
                new_embed = build_tracker_embed(guild, guild.id, owner_id)
                await msg_t.edit(embed=new_embed)
            except Exception:
                pass
    except Exception:
        pass

    # ── Shadow Wolf kill list trigger ────────────────────────────────────
    if assignment[1] == "Shadow Wolf":
        safe_task(_generate_sw_kill_list(guild, guild.id, player_id, player_id), "sw_kill_list")
    # If this player is on the SW kill list, mark them dead
    safe_task(_sw_mark_dead(guild, guild.id, player_id, "other"), "sw_mark_dead")

    # ── Cupid bond — if Shadow Wolf died from bond, use partner's voters ──
    # (handled below in Cupid check — we pass voted_out_id = partner_id)

    # ── Time Lord trigger ────────────────────────────────────────────────
    if assignment[1] == "Time Lord":
        state_cur   = cached_get_state(guild.id)
        night_dur   = state_cur.get("night_duration", 43200)
        day_dur     = state_cur.get("day_duration", 43200)
        new_night   = max(3600,  night_dur // 2)
        new_day     = max(7200,  day_dur   // 2)
        db_set_state(guild.id, night_duration=new_night, day_duration=new_day,
                     time_lord_triggered=1)
        invalidate_cache(guild.id)
        msg = (f"⏰ **Time Lord eliminated — clocks accelerated!**\n"
               f"Night duration: {night_dur//3600:.1f}h → {new_night//3600:.1f}h\n"
               f"Day duration:   {day_dur//3600:.1f}h → {new_day//3600:.1f}h\n"
               f"All future phases will be shorter.")
        await post_mod_log(guild, msg)
        # Post in village-chat too
        state_vc = cached_get_state(guild.id)
        vc_ch    = guild.get_channel(state_vc.get("village_chat_ch_id") or 0)
        if vc_ch:
            await vc_ch.send(msg)

    # ── Traitor side-switch check ────────────────────────────────────────
    # After every wolf death, check if all non-Traitor wolves are gone
    if get_team(guild.id, assignment[1]) == "wolf" and assignment[1] != "Traitor":
        rows_after = db_get_assignments(guild.id)
        alive_wolves_after = [r for r in rows_after
                              if r[2] == 1 and get_team(guild.id, r[1]) == "wolf"
                              and r[1] != "Traitor" and r[0] != player_id]
        if not alive_wolves_after:
            # Check if Traitor is still alive and hasn't switched yet
            traitor_row = next((r for r in rows_after
                                if r[1] == "Traitor" and r[2] == 1), None)
            state_check = cached_get_state(guild.id)
            already_switched = state_check.get("traitor_switched", 0)
            if traitor_row and not already_switched:
                db_set_state(guild.id, traitor_switched=1)
                traitor_member = guild.get_member(traitor_row[0])

                # Spin from actual wolf roles in this game's pool
                last_roles = db_get_last_roles(guild.id) or {}
                all_roles  = cached_load_roles(guild.id)
                role_info_map = {r["name"]: r for r in all_roles}
                wolf_pool  = [name for name in last_roles
                              if name in role_info_map
                              and role_info_map[name].get("team") == "wolf"
                              and name != "Traitor"]
                if not wolf_pool:
                    wolf_pool = ["Wolf"]  # fallback
                outcome = random.choice(wolf_pool)
                outcome_info = role_info_map.get(outcome, {})
                outcome_desc = outcome_info.get("description", "No description available.")

                # Update Traitor's role in DB to their new wolf role
                conn_t = sqlite3.connect(DB_FILE)
                c_t    = conn_t.cursor()
                c_t.execute("UPDATE player_assignments SET role_name=? WHERE guild_id=? AND player_id=?",
                            (outcome, guild.id, traitor_row[0]))
                conn_t.commit()
                conn_t.close()
                invalidate_cache(guild.id)

                # Send wheel spin to Traitor's private channel
                if traitor_row[3]:
                    priv_ch = guild.get_channel(traitor_row[3])
                    if priv_ch:
                        # Show all options spinning
                        wheel_lines = "\n".join(f"🎡 {r}" for r in wolf_pool)
                        spin_msg = await priv_ch.send(fmt(
                            f"🐺 The last wolf has fallen. Your true nature awakens.\n\n"
                            f"The wheel spins...\n{wheel_lines}"))
                        await asyncio.sleep(2)
                        embed = discord.Embed(
                            title       = f"🐺 You are now: {outcome}",
                            description = outcome_desc,
                            color       = 0xC0392B
                        )
                        embed.set_footer(text="You fight alone. Hunt well.")
                        await spin_msg.edit(content=fmt(
                            f"🐺 The last wolf has fallen. Your true nature awakens.\n\n"
                            f"🎡 The wheel has spoken: **{outcome}**"))
                        await priv_ch.send(embed=embed)

                # Give den + wolf-vote access
                wolf_ch      = guild.get_channel(state_check.get("wolf_channel_id") or 0)
                if wolf_ch and traitor_member:
                    await wolf_ch.set_permissions(traitor_member,
                        view_channel=True, send_messages=True)


                await post_mod_log(guild,
                    f"🔄 **Traitor side-switch triggered**\n"
                    f"**Player:** {traitor_member.display_name if traitor_member else traitor_row[0]}\n"
                    f"**New wolf role:** {outcome}\n"
                    f"**Description:** {outcome_desc}\n"
                    f"Role updated in DB. Den access granted.")

    # ── Pothead second kill trigger ──────────────────────────────────────
    if assignment[1] == "Pothead" and reason == "Wolf attack":
        state_pot  = cached_get_state(guild.id)
        rows_pot   = db_get_assignments(guild.id)
        night_pot  = db_get_night_num(guild.id)
        alive_non_wolf = [guild.get_member(r[0]) for r in rows_pot
                          if r[2] == 1 and get_team(guild.id, r[1]) != "wolf"
                          and r[0] != player_id]
        alive_non_wolf = [p for p in alive_non_wolf if p]
        wolf_den_pot = guild.get_channel(state_pot.get("wolf_channel_id") or 0)
        if wolf_den_pot and alive_non_wolf:
            view = PothreadSecondKillView(guild.id, night_pot, alive_non_wolf)
            await wolf_den_pot.send(
                fmt(f"🍕 The wolves ate the Pothead — they get the munchies!\n"
                    f"Pick a second kill target for tonight."),
                view=view)
        await post_mod_log(guild,
            f"🍕 **Pothead eliminated by wolves** — Night {db_get_night_num(guild.id)}\n"
            f"Wolves get a second kill tonight. Dropdown posted in wolf den.")

    # ── Clone inheritance check ───────────────────────────────────────────
    safe_task(_check_clone_inheritance(guild, guild.id, player_id, assignment[1]), "clone_inherit")

    # ── Diseased wolf poisoning check ─────────────────────────────────────
    if assignment[1] == "Diseased" and reason == "Wolf attack":
        # Auto-block the wolf attacker from killing next night
        night_num_dis = db_get_night_num(guild.id)
        conn_dis = sqlite3.connect(DB_FILE)
        c_dis    = conn_dis.cursor()
        # Find who submitted the wolf kill this night
        c_dis.execute(
            "SELECT actor_id FROM night_actions WHERE guild_id=? AND night_num=? AND action_type IN (?,?,?,?)",
            (guild.id, night_num_dis, "wolf", "werekitten_kill", "crazed_wolf_1", "crazed_wolf_2"))
        wolf_killers = [r[0] for r in c_dis.fetchall()]
        for wk_id in wolf_killers:
            c_dis.execute(
                "INSERT OR REPLACE INTO block_log VALUES (?,?,?,?)",
                (guild.id, wk_id, night_num_dis + 1, "diseased"))
        conn_dis.commit()
        conn_dis.close()
        blocked_names = []
        for wk_id in wolf_killers:
            m = guild.get_member(wk_id)
            blocked_names.append(m.display_name if m else str(wk_id))
        await post_mod_log(guild,
            f"🤢 **Diseased player eliminated by wolves!**\n"
            f"Wolves are now **poisoned** — the attacker(s) cannot kill next night.\n"
            f"Blocked for Night {night_num_dis + 1}: {', '.join(blocked_names) or 'Unknown'}\n"
            f"*Block applied automatically — no manual action needed.*")

    # Handle NPC elimination
    npcs    = db_get_npcs(guild.id)
    npc_row = next((n for n in npcs if n["npc_id"] == player_id), None)
    if npc_row:
        db_set_npc_alive(guild.id, player_id, False)
        # NPC sends a goodbye message in village-chat
        vc_ch = guild.get_channel(state.get("village_chat_ch_id") or 0)
        if vc_ch:
            farewells = [
                "Well played everyone. That's my game.",
                "Gg. Got me.",
                "Fair enough. Good luck to the rest of you.",
                "Welp. See you all on the other side.",
                "Can't believe it. Good game.",
            ]
            try:
                async with aiohttp.ClientSession() as session:
                    wh = discord.Webhook.partial(npc_row["webhook_id"], npc_row["webhook_token"], session=session)
                    await wh.send(random.choice(farewells),
                                  username=f"{npc_row['name']} •", avatar_url=npc_row["avatar_url"])
            except Exception:
                pass
        # Other NPCs react to this death
        safe_task(_npc_react_to_death(guild, guild.id, npc_row["name"]), "npc_death_react")
        await log_event(guild, phase, f"💀 **{npc_row['name']}** (NPC) eliminated — {reason}")
        await post_mod_log(guild, f"💀 **{npc_row['name']}** (NPC) eliminated. Role: **{assignment[1]}**")
        await refresh_player_list(guild)
        await refresh_day_vote(guild)
        return
    await log_event(guild, phase, f"💀 **{player.display_name}** eliminated — {reason}")
    team_e = get_team(guild.id, assignment[1])
    await post_mod_log(guild,
        f"💀 **{player.display_name}** eliminated\n"
        f"**Role:** {assignment[1]} ({team_e})\n"
        f"**Reason:** {reason}\n"
        f"**Phase:** {phase} {db_get_night_num(guild.id)}")
    await refresh_player_list(guild)
    await refresh_day_vote(guild)
    await refresh_win_tracker(guild)

    # ── Agitator frenzy tracking ──────────────────────────────────────────
    frenzy_state = db_get_state(guild.id) or {}
    frenzy_day   = frenzy_state.get("agitator_frenzy_day")
    night_now    = db_get_night_num(guild.id)
    if frenzy_day and int(frenzy_day) == int(night_now) and phase == "day":
        elim_count = int(frenzy_state.get("agitator_elim_count") or 0) + 1
        db_set_state(guild.id, agitator_elim_count=elim_count)
        vc_ch = guild.get_channel(frenzy_state.get("village_chat_ch_id") or 0)
        if elim_count >= 2:
            db_set_state(guild.id, agitator_frenzy_day=None, agitator_elim_count=0)
            await post_mod_log(guild,
                f"⚡ **Agitator frenzy satisfied** — both eliminations complete for Day {night_now}.")
            if vc_ch:
                await vc_ch.send(
                    "⚡ The village's frenzy has been satisfied. Two have fallen today.")
        else:
            await post_mod_log(guild,
                f"⚡ **Agitator frenzy** — {elim_count}/2 eliminations done for Day {night_now}. "
                f"**One more elimination required.**")
    # NPCs react to this player's death and pin it to their memory
    safe_task(_npc_react_to_death(guild, guild.id, player.display_name), "npc_death_react")
    night_num_pin = db_get_night_num(guild.id)
    phase_pin     = state.get("phase", "day").capitalize()
    for npc_p in db_get_npcs(guild.id):
        if npc_p["is_alive"]:
            db_pin_npc_event(guild.id, npc_p["npc_id"],
                f"{phase_pin} {night_num_pin}: {player.display_name} was eliminated ({reason}).")

# ====================== WIN CONDITION ======================
# Neutral win conditions — checked separately after main win check
NEUTRAL_WIN_CONDITIONS = {
    # Witch wins if she is alive when the game ends (either side)
    "Witch": lambda rows, guild_id: any(
        r[1] == "Witch" and r[2] == 1 for r in rows),
    # Oracle wins if they survived to Night 4 or beyond
    "Oracle": lambda rows, guild_id: (
        db_get_night_num(guild_id) >= 4 and
        any(r[1] == "Oracle" and r[2] == 1 for r in rows)),
    # Warlock wins if at least one wish was granted (tracked via night actions)
    "Warlock": lambda rows, guild_id: any(
        r[1] == "Warlock" and r[2] == 1 for r in rows),
    # Fairy Elf wins if they used their happy ending AND are still alive
    "Fairy Elf": lambda rows, guild_id: any(
        r[1] == "Fairy Elf" and r[2] == 1 for r in rows),
}

async def check_win_condition(guild):
    guild_id      = guild.id
    rows          = db_get_assignments(guild_id)
    alive_rows    = [r for r in rows if r[2] == 1]
    alive_roles   = [r[1] for r in alive_rows]
    alive_wolves  = [r for r in alive_roles if get_team(guild_id, r) == "wolf"]
    alive_village = [r for r in alive_roles if get_team(guild_id, r) == "village"]

    # ── Wraith win condition ─────────────────────────────────────────────
    wraith_players = [(r[0], r[2]) for r in rows if r[1] == "Wraith"]
    if wraith_players:
        alive_wraiths = [p for p, alive in wraith_players if alive]
        if alive_wraiths:
            village_alive = len([r for r in rows if r[2] == 1 and get_team(guild_id, r[1]) == "village"])
            wolf_alive    = len([r for r in rows if r[2] == 1 and get_team(guild_id, r[1]) == "wolf"])
            wraith_count  = len(alive_wraiths)
            if wraith_count >= village_alive and wraith_count >= wolf_alive:
                wraith_members = [guild.get_member(pid) for pid in alive_wraiths]
                wraith_names   = ", ".join(m.display_name for m in wraith_members if m)
                return f"wraith|{wraith_names}"

    # Check neutral individual wins first
    for role_name, condition in NEUTRAL_WIN_CONDITIONS.items():
        if any(r[1] == role_name for r in alive_rows):
            if condition(rows, guild.id):
                pass  # Neutral may still win even if main game ends

    if len(alive_wolves) == 0:                  return "village"
    if len(alive_wolves) >= len(alive_village): return "wolf"
    return None

async def announce_win(guild, winner: str):
    state = cached_get_state(guild.id)
    cat   = guild.get_channel(state.get("category_id") or 0)
    skip  = {state.get("mod_log_channel_id"), state.get("wolf_channel_id"),
             state.get("ghost_channel_id")}

    # Check neutral winners at game end
    rows = db_get_assignments(guild.id)
    neutral_winners = []
    for role_name, condition in NEUTRAL_WIN_CONDITIONS.items():
        if condition(rows, guild.id):
            for r in rows:
                if r[1] == role_name and r[2] == 1:
                    m = guild.get_member(r[0])
                    neutral_winners.append((m.display_name if m else str(r[0]), role_name))

    # Build win messages
    if winner and winner.startswith("wraith|"):
        wraith_names = winner.split("|", 1)[1]
        vc_ch = guild.get_channel(state.get("village_chat_ch_id") or 0)
        if vc_ch:
            embed = discord.Embed(
                title       = "👻 The Wraiths Win",
                description = (
                    f"*Something was watching Whisperfall from the beginning.*\n"
                    f"*Not the wolves. Not the village. Something else entirely.*\n\n"
                    f"**{wraith_names}** — the Wraiths — have achieved their purpose.\n\n"
                    f"*They were never here to save anyone.*"
                ),
                color = 0x4B0082
            )
            await vc_ch.send(embed=embed)
        return

    if winner == "village":
        public_msg  = "🎉 **The Village wins!** All wolves eliminated!"
        winner_msg  = "🎉 **Your team won!** The village has triumphed — all wolves are dead."
        loser_msg   = "💀 **Your team lost.** The village hunted down every wolf."
    elif winner == "wolf":
        public_msg  = "🐺 **The Wolves win!** They now control the village..."
        winner_msg  = "🐺 **Your team won!** The wolves now control the village."
        loser_msg   = "💀 **Your team lost.** The wolves have taken over."
    else:
        public_msg  = f"⚖️ **{winner.capitalize()} wins!**"
        winner_msg  = f"⚖️ **Your team won!** {winner.capitalize()} is victorious."
        loser_msg   = f"💀 **Your team lost.** {winner.capitalize()} emerged victorious."

    if neutral_winners:
        neutral_lines = ", ".join(f"**{n}** ({r})" for n, r in neutral_winners)
        public_msg += f"\n⚖️ **Neutral winners:** {neutral_lines}"

    # Post to village chat with Whisperfall-toned message
    state2   = cached_get_state(guild.id)
    vc_ch    = guild.get_channel(state2.get("village_chat_ch_id") or 0)

    VILLAGE_WIN_MESSAGES = [
        "*The bells rang this morning for the first time in what felt like years. Whisperfall is still standing.*",
        "*By dawn, the last shadow had been lifted. The village breathes differently now — lighter, cautious still, but lighter.*",
        "*It is over. The dark that moved through these streets does so no more. Whisperfall endures.*",
    ]
    WOLF_WIN_MESSAGES = [
        "*The village believed it had seen the worst. It had not. By morning, the count was settled. The darkness won.*",
        "*Whisperfall is quiet now. The kind of quiet that means something has been decided, and not in the village's favour.*",
        "*The last voice that might have stopped it is gone. Whisperfall belongs to the night now.*",
    ]

    if winner == "village":
        tone_msg = random.choice(VILLAGE_WIN_MESSAGES)
    elif winner == "wolf":
        tone_msg = random.choice(WOLF_WIN_MESSAGES)
    else:
        tone_msg = f"*The game is over. {winner.capitalize()} has emerged victorious.*"

    if vc_ch:
        win_embed = discord.Embed(
            description = f"{public_msg}\n\n{tone_msg}",
            color       = 0x27AE60 if winner == "village" else (0xC0392B if winner == "wolf" else 0xF39C12)
        )
        await vc_ch.send(embed=win_embed)
    else:
        # Fallback to first visible channel
        target_ch = None
        if cat:
            for ch in cat.channels:
                if isinstance(ch, discord.TextChannel) and ch.id not in skip:
                    target_ch = ch
                    break
        if target_ch:
            await target_ch.send(public_msg)

    # Send personal win/loss message to every player's private channel
    rows = db_get_assignments(guild.id)
    for pid, role_name, is_alive, ch_id in rows:
        if not ch_id:
            continue
        ch = guild.get_channel(ch_id)
        if not ch:
            continue
        player_team = get_team(guild.id, role_name)
        is_winner   = player_team == winner

        embed = discord.Embed(
            title="🏆 Game Over!",
            description=winner_msg if is_winner else loser_msg,
            color=0x2ECC71 if is_winner else 0x992222
        )
        embed.add_field(name="Your Role", value=role_name, inline=True)
        embed.add_field(name="Your Team", value=player_team.capitalize(), inline=True)
        embed.add_field(name="Result",    value="✅ Victory" if is_winner else "❌ Defeat", inline=True)
        try:
            m = guild.get_member(pid)
            await ch.send(m.mention if m else "", embed=embed)
        except Exception:
            pass

    await log_event(guild, "End", f"🏆 **{winner.capitalize()}** wins!")
    safe_task(_npc_endgame_reaction(guild, guild.id, winner), "npc_endgame")

# ====================== STATS & VICTORS ======================
@tree.command(name="assign_victors", description="Record who won this game for stats tracking")
@is_mod()
@app_commands.describe(winning_team="Which team won: village, wolf, or neutral")
async def assign_victors(interaction: discord.Interaction, winning_team: str):
    winning_team = winning_team.lower()
    if winning_team not in ["village", "wolf", "neutral"]:
        return await interaction.response.send_message("❌ Must be `village` or `wolf`.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)
    rows = db_get_assignments(interaction.guild_id)
    if not rows:
        return await interaction.followup.send("No player data found.", ephemeral=True)

    guild_id = interaction.guild_id

    # ── Build effective team map — accounts for mid-game team changes ──────
    # 1. Turned players (Alpha/Elite Alpha successful turn) → wolf team
    conn_t = sqlite3.connect(DB_FILE)
    c_t    = conn_t.cursor()
    c_t.execute(
        "SELECT DISTINCT target_id FROM turn_log WHERE guild_id=? AND result='successful'",
        (guild_id,))
    turned_pids = {r[0] for r in c_t.fetchall()}
    conn_t.close()

    # 2. Traitor who switched → wolf team
    state_end = cached_get_state(guild_id) or {}
    traitor_switched = bool(state_end.get("traitor_switched", 0))
    traitor_pid = None
    if traitor_switched:
        traitor_row = next((r for r in rows if r[1] == "Traitor"), None)
        if traitor_row:
            traitor_pid = traitor_row[0]

    def effective_team(pid, role_name):
        """Return the team this player actually belongs to at game end."""
        if pid in turned_pids:
            return "wolf"
        if traitor_pid and pid == traitor_pid:
            return "wolf"
        return get_team(guild_id, role_name)

    all_pids    = [r[0] for r in rows]
    winner_pids = [r[0] for r in rows if effective_team(r[0], r[1]) == winning_team]
    dead_pids   = [r[0] for r in rows if r[2] == 0]

    db_update_stats(guild_id, all_pids, winner_pids, dead_pids)

    # Record role history — use effective team so stats reflect what actually happened
    for pid, role_name, is_alive, _ in rows:
        team    = effective_team(pid, role_name)
        outcome = "win" if pid in winner_pids else "loss"
        db_record_role_history(guild_id, pid, role_name, team, outcome)

    winner_names = []
    for pid in winner_pids:
        m = interaction.guild.get_member(pid)
        name = m.display_name if m else str(pid)
        if pid in turned_pids:     name += " *(turned)*"
        elif pid == traitor_pid:   name += " *(switched)*"
        winner_names.append(name)

    await log_event(interaction.guild, "End",
                    f"🏆 **{winning_team.capitalize()}** declared winners. "
                    f"Winners: {', '.join(winner_names)}")
    await post_mod_log(interaction.guild,
        f"🏆 **{winning_team.capitalize()} wins** recorded.\n"
        f"Winners ({len(winner_pids)}): {', '.join(winner_names)}")
    # Auto-refresh hall of fame and stats channel
    await refresh_hall_of_fame(interaction.guild)
    state     = cached_get_state(interaction.guild_id)
    stats_ch  = interaction.guild.get_channel(state.get("stats_ch_id") or 0)
    if stats_ch:
        stat_rows  = db_get_stats(interaction.guild_id)
        stats_embed = build_stats_embed(interaction.guild, stat_rows)
        # Try to edit existing pinned stats message
        edited = False
        try:
            pins = await stats_ch.pins()
            for pin in pins:
                if pin.author.id == interaction.guild.me.id:
                    await pin.edit(embed=stats_embed)
                    edited = True
                    break
        except Exception:
            pass
        if not edited:
            msg = await stats_ch.send(embed=stats_embed)
            try: await msg.pin()
            except Exception: pass
    # ── Role Reveal — post all roles publicly in ghost-chat ──────────────
    npcs_rv    = db_get_npcs(interaction.guild_id)
    npc_map_rv = {n["npc_id"]: n["name"] for n in npcs_rv}

    def _rv_name(pid):
        npc = npc_map_rv.get(pid)
        if npc: return npc
        m = interaction.guild.get_member(pid)
        return m.display_name if m else str(pid)

    team_color = {"village": 0x27AE60, "wolf": 0xC0392B, "neutral": 0xF39C12}

    by_team = {"wolf": [], "village": [], "neutral": []}
    for pid, role_name, is_alive, _ in rows:
        team       = effective_team(pid, role_name)
        alive_icon = "✅" if is_alive else "💀"
        winner_icon = "🏆 " if pid in winner_pids else ""
        turned_note = " *(turned)*" if pid in turned_pids else ""
        traitor_note = " *(switched)*" if pid == traitor_pid else ""
        by_team.setdefault(team, []).append(
            f"{alive_icon} {winner_icon}**{_rv_name(pid)}** — {role_name}{turned_note}{traitor_note}")

    win_label = {
        "village": "🏘️ Village Wins",
        "wolf":    "🐺 Wolves Win",
        "neutral": "⚖️ Neutral Wins",
    }.get(winning_team, f"{winning_team.capitalize()} Wins")

    reveal_embed = discord.Embed(
        title       = f"🎭 Role Reveal — {win_label}",
        description = "The game is over. Here is everyone's role.",
        color       = team_color.get(winning_team, 0x5865F2)
    )
    for team_key, label in [("wolf", "🐺 Wolves"), ("village", "🏘️ Village"), ("neutral", "⚖️ Neutral")]:
        lines = by_team.get(team_key, [])
        if lines:
            reveal_embed.add_field(name=label, value="\n".join(lines), inline=False)
    reveal_embed.set_footer(text="✅ = survived  💀 = eliminated  🏆 = winning team")

    # ── MVP Callout ───────────────────────────────────────────────────────
    # Build a second embed highlighting standout moments and players
    mvp_lines = []

    # Data sources
    vote_hist   = db_get_vote_history(interaction.guild_id)
    turn_hist   = db_get_turn_log(interaction.guild_id)
    elim_log    = db_get_elimination_log(interaction.guild_id)
    msg_counts  = {c[0]: c[1] for c in db_get_message_counts(interaction.guild_id)}
    all_rows    = rows  # already fetched

    pid_to_name = {}
    for pid, _, _, _ in all_rows:
        pid_to_name[pid] = _rv_name(pid)

    # — Most votes cast (most active voter)
    vote_casts = {}
    for _, voter_id, target_id, action, _, _ in (r + (0,) * (6 - len(r)) for r in vote_hist):
        if action in ("vote", "change") and target_id:
            vote_casts[voter_id] = vote_casts.get(voter_id, 0) + 1
    if vote_casts:
        top_voter_id = max(vote_casts, key=vote_casts.get)
        mvp_lines.append(
            f"🗳️ **Most Votes Cast:** {pid_to_name.get(top_voter_id, str(top_voter_id))} "
            f"— {vote_casts[top_voter_id]} vote(s) across the game")

    # — Most times nominated but survived
    times_nominated = {}
    for _, voter_id, target_id, action, _, _ in (r + (0,) * (6 - len(r)) for r in vote_hist):
        if action in ("vote",) and target_id:
            times_nominated[target_id] = times_nominated.get(target_id, 0) + 1
    survived_ids = {r[0] for r in all_rows if r[2] == 1}
    survived_nominated = {pid: c for pid, c in times_nominated.items()
                          if pid in survived_ids and c >= 2}
    if survived_nominated:
        top_survivor_id = max(survived_nominated, key=survived_nominated.get)
        mvp_lines.append(
            f"🎯 **Survived the Most Heat:** {pid_to_name.get(top_survivor_id, str(top_survivor_id))} "
            f"— nominated {survived_nominated[top_survivor_id]}x and never eliminated")

    # — Most vote changes (most erratic / strategic voter)
    vote_flips = {}
    for _, voter_id, target_id, action, _, _ in (r + (0,) * (6 - len(r)) for r in vote_hist):
        if action == "change":
            vote_flips[voter_id] = vote_flips.get(voter_id, 0) + 1
    if vote_flips and max(vote_flips.values()) >= 2:
        top_flipper_id = max(vote_flips, key=vote_flips.get)
        mvp_lines.append(
            f"🔄 **Most Vote Changes:** {pid_to_name.get(top_flipper_id, str(top_flipper_id))} "
            f"— changed their vote {vote_flips[top_flipper_id]}x")

    # — Wolves who survived to the end (including turned wolves)
    surviving_wolves = [
        r for r in all_rows
        if r[2] == 1 and effective_team(r[0], r[1]) == "wolf"
    ]
    if surviving_wolves:
        wolf_names = ", ".join(pid_to_name.get(r[0], str(r[0])) for r in surviving_wolves)
        mvp_lines.append(f"🐺 **Wolf(ves) who survived:** {wolf_names}")

    # — Successful turns
    successful_turns = [(a, t, r) for a, t, r in turn_hist if r == "success"]
    if successful_turns:
        for actor_id, target_id, _ in successful_turns:
            aname = pid_to_name.get(actor_id, str(actor_id))
            tname = pid_to_name.get(target_id, str(target_id))
            mvp_lines.append(f"👑 **Successful Turn:** {aname} converted {tname} to the wolf pack")

    # — Most talkative player (message count)
    if msg_counts:
        top_talker_id = max(msg_counts, key=msg_counts.get)
        if msg_counts[top_talker_id] > 5:
            mvp_lines.append(
                f"💬 **Most Active in Village:** {pid_to_name.get(top_talker_id, str(top_talker_id))} "
                f"— {msg_counts[top_talker_id]} messages")

    # — Night 1 death (harsh)
    night1_deaths = [e for e in elim_log
                     if e[3] in ("wolf_kill", "werekitten_kill") and e[4] == 1]
    if night1_deaths:
        for pid, role_n, reason, elim_type, day_or_night, _ in night1_deaths:
            mvp_lines.append(
                f"💀 **Night 1 Casualty:** {pid_to_name.get(pid, str(pid))} ({role_n}) — "
                f"didn't make it to morning")

    # — Player with most votes against them overall (most suspected)
    most_suspected_id = max(times_nominated, key=times_nominated.get) if times_nominated else None
    if most_suspected_id and times_nominated.get(most_suspected_id, 0) >= 3:
        mvp_lines.append(
            f"🔍 **Most Suspected:** {pid_to_name.get(most_suspected_id, str(most_suspected_id))} "
            f"— received {times_nominated[most_suspected_id]} votes across all days")

    # — Claude-generated MVP narrative
    if mvp_lines:
        mvp_context = "\n".join(f"• {l}" for l in mvp_lines)
        mvp_prompt = (
            f"Game over. {win_label}.\n\n"
            f"Here are the standout moments and players from this game:\n{mvp_context}\n\n"
            f"Write a punchy, exciting MVP callout — 3-5 sentences max. "
            f"Highlight the most impactful moments. Name the players. "
            f"Make it feel like a sports highlight reel crossed with a thriller recap. "
            f"End on the winning team's moment. No emojis in the text — the data already has them."
        )
        mvp_system = (
            "You are the announcer for Whisperfall — writing the post-game MVP callout. "
            "Be dramatic but precise. Every sentence should land. No filler."
        )
        mvp_narrative = await _claude(mvp_prompt, mvp_system, max_tokens=300)
        if not mvp_narrative:
            mvp_narrative = "\n".join(mvp_lines)
    else:
        mvp_narrative = "No standout moments recorded — the game data is sparse."

    mvp_embed = discord.Embed(
        title       = "🏅 Game Highlights",
        description = mvp_narrative,
        color       = team_color.get(winning_team, 0x5865F2)
    )
    for line in mvp_lines:
        # Each stat as a compact field
        parts = line.split(":", 1)
        if len(parts) == 2:
            mvp_embed.add_field(name=parts[0].strip(), value=parts[1].strip(), inline=False)

    # Restore village-chat send access for dead players, then post role reveal there
    vc_ch      = interaction.guild.get_channel(state.get("village_chat_ch_id") or 0)
    dead_role  = interaction.guild.get_role(state.get("dead_role_id") or 0)

    if vc_ch and dead_role:
        try:
            await vc_ch.set_permissions(dead_role, view_channel=True, send_messages=True)
        except discord.Forbidden:
            pass

    reveal_ch = vc_ch or interaction.guild.get_channel(state.get("mod_log_channel_id") or 0)
    if reveal_ch:
        await reveal_ch.send(
            f"# 🎭 The game is over.\n**{win_label}** — roles are now revealed. Discuss freely.",
            embed=reveal_embed)
        await reveal_ch.send(embed=mvp_embed)

    reveal_note = f"Role reveal posted to {reveal_ch.mention}." if reveal_ch else "⚠️ No channel found for role reveal."
    await interaction.followup.send(
        f"✅ Stats recorded. **{winning_team.capitalize()}** team wins credited to {len(winner_pids)} players.\n"
        f"{reveal_note}",
        ephemeral=True)


@tree.command(name="post_stats", description="Post the all-time stats leaderboard to the stats channel")
@is_mod()
async def post_stats(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    state   = cached_get_state(interaction.guild_id)
    stat_rows = db_get_stats(interaction.guild_id)
    embed   = build_stats_embed(interaction.guild, stat_rows)

    stats_ch = interaction.guild.get_channel(state.get("stats_ch_id") or 0)
    if stats_ch:
        await stats_ch.send(embed=embed)
        await interaction.followup.send("✅ Stats posted to 📊stats channel.", ephemeral=True)
    else:
        await interaction.followup.send(embed=embed)



@tree.command(name="my_roles", description="Show every role you have ever played across all games")
async def my_roles(interaction: discord.Interaction):
    history = db_get_role_history(interaction.guild_id, interaction.user.id)
    if not history:
        return await interaction.response.send_message(
            "No role history found yet. Play some games first!", ephemeral=True)

    # Build summary stats
    from collections import Counter
    role_counts  = Counter(r[0] for r in history)
    team_counts  = Counter(r[1] for r in history)
    wins         = sum(1 for r in history if r[3] == "win")
    total        = len(history)
    fav_role     = role_counts.most_common(1)[0]

    embed = discord.Embed(
        title       = f"🎭 {interaction.user.display_name}'s Role History",
        description = f"**{total}** game(s) played across all time",
        color       = 0x9B59B6
    )

    # Recent games list
    lines = []
    for role_name, team, game_num, outcome in history[:15]:
        team_emoji   = "🐺" if team == "wolf" else ("⚖️" if team == "neutral" else "🏘️")
        outcome_icon = "✅" if outcome == "win" else ("❌" if outcome == "loss" else "❔")
        lines.append(f"`Game {game_num}` {outcome_icon} {team_emoji} **{role_name}**")

    embed.add_field(
        name  = "📜 Recent Games" + (" (last 15)" if total > 15 else ""),
        value = "\n".join(lines),
        inline= False
    )

    # Stats
    stats_lines = [
        f"🏆 Wins: **{wins}/{total}** ({int(wins/total*100)}%)" if total else "🏆 No games yet",
        f"🎭 Favourite role: **{fav_role[0]}** (×{fav_role[1]})",
        f"🏘️ Village: {team_counts.get('village',0)}  🐺 Wolf: {team_counts.get('wolf',0)}  ⚖️ Neutral: {team_counts.get('neutral',0)}",
    ]
    embed.add_field(name="📊 Stats", value="\n".join(stats_lines), inline=False)

    # Most played roles
    if len(role_counts) > 1:
        top_roles = role_counts.most_common(5)
        embed.add_field(
            name  = "🔁 Most played",
            value = "  ".join(f"**{r}** ×{c}" for r, c in top_roles),
            inline= False
        )

    embed.set_footer(text="Only games where /assign_victors was run are counted.")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@tree.command(name="my_stats", description="Show your personal stats")
async def my_stats(interaction: discord.Interaction):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT games, wins, eliminations FROM player_stats WHERE guild_id=? AND player_id=?",
              (interaction.guild_id, interaction.user.id))
    row = c.fetchone()
    conn.close()
    if not row or row[0] == 0:
        return await interaction.response.send_message("No stats recorded for you yet.", ephemeral=True)
    games, wins, elims = row
    win_rate = f"{(wins/games*100):.0f}%" if games > 0 else "0%"
    embed = discord.Embed(title=f"📊 {interaction.user.display_name}'s Stats", color=0x2ECC71)
    embed.add_field(name="Games Played", value=str(games), inline=True)
    embed.add_field(name="Wins",         value=str(wins),  inline=True)
    embed.add_field(name="Win Rate",     value=win_rate,   inline=True)
    embed.add_field(name="Times Eliminated", value=str(elims), inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)



# ====================== WHISPERFALL AMBIENT MESSAGES ======================
# Day-number-aware ambient messages
AMBIENT_MESSAGES_EARLY = [
    # Day 1-2 — village is unsettled but not yet desperate
    "*The bells toll in Whisperfall. Another day begins.*",
    "*Somewhere in the village, a shutter bangs against its frame. No one checks why.*",
    "*The market opens. Voices carry across the square, but no one looks anyone in the eye.*",
    "*A dog barks twice on the edge of the village and then goes quiet.*",
    "*The fountain in the square continues its sound regardless. It has outlasted everyone who came before.*",
    "*Smoke rises from chimneys that went cold overnight. Someone started the fires early.*",
    "*The wind picks up briefly, carrying something with it — a scent, a whisper, a suggestion. Then it stops.*",
    "*The children of Whisperfall are not playing outside today.*",
    "*A window closes somewhere. The sound carries further than it should.*",
    "*Whisperfall is smaller than it was yesterday. Everyone feels it. No one says so.*",
    "*The mud in the square holds footprints from the night. Most of them lead somewhere. Not all of them lead back.*",
    "*The baker opened late. The bread is slightly burnt. No explanation was offered.*",
    "*A door in the village stands open that was closed last night. The room inside is undisturbed.*",
    "*The birds returned this morning. They are watching the square.*",
    "*Time moves differently in Whisperfall. The hours between dawn and dark have never felt shorter.*",
    "*People are still making eye contact. That will change.*",
    "*The square fills slowly today. Everyone wants to be the last to arrive.*",
    "*Someone laughed in the village this morning. It sounded wrong.*",
    "*The well rope is frayed in a new place. No one mentions it.*",
    "*Every conversation in Whisperfall today ends the same way — with someone looking over their shoulder.*",
]

AMBIENT_MESSAGES_MID = [
    # Day 3-4 — paranoia setting in, trust eroding
    "*Whisperfall no longer feels like a village. It feels like a question with no good answers.*",
    "*The square is quieter than yesterday. The arguments are happening behind closed doors now.*",
    "*Someone left their door unlocked last night. They won't make that mistake again.*",
    "*The way people look at each other has changed. Every glance means something it didn't before.*",
    "*A candle burned in a window all night. No one admitted to lighting it.*",
    "*Whisperfall is learning. The lesson is not a comfortable one.*",
    "*Half the village suspects the other half. The other half suspects them back.*",
    "*The conversations in the square today are careful. Too careful.*",
    "*A name is being passed around the village in whispers. Whether it is the right name is another question.*",
    "*The clock in the square keeps perfect time. That is the only thing in Whisperfall that does.*",
    "*Trust is a thing that grows slowly and dies in a single night. Whisperfall understands this now.*",
    "*Alliances that felt solid yesterday have developed cracks no one wants to acknowledge.*",
    "*Two people crossed the square without speaking today who would have waved last week.*",
    "*The food at the inn tastes different. It may be the cook. It may be the company.*",
    "*Everyone in Whisperfall is keeping score. Not everyone is keeping the same score.*",
    "*Something has shifted in the village. The weight of it is unevenly distributed.*",
    "*The afternoon light is doing strange things in Whisperfall today. Making familiar faces look unfamiliar.*",
    "*Three conversations stopped when the wrong person walked by. That is more than yesterday.*",
    "*Whisperfall has been through bad nights before. It has not always been through the days after.*",
    "*The person you trusted most this morning — have you looked at them since?*",
]

AMBIENT_MESSAGES_LATE = [
    # Day 5+ — desperate, grim, the village knows what it's in
    "*Whisperfall has grown small. The survivors know each other too well and not well enough.*",
    "*There is no one left in the village who is not paying attention. The casual conversations are over.*",
    "*Every face in the square today belongs to someone who has survived this long. Think about what that means.*",
    "*The numbers do not lie. Someone in Whisperfall is doing math they haven't shared.*",
    "*Survival has a cost. Whisperfall is beginning to understand what it owes.*",
    "*The square feels different when the crowd has thinned. Every absence is a presence.*",
    "*Whatever has been hunting this village is close to finished. So is the village.*",
    "*No one in Whisperfall is sleeping well. You can tell by the eyes.*",
    "*The vote today matters more than it has before. Everyone can feel it.*",
    "*Whisperfall is running out of wrong answers. The right answer is still hiding.*",
    "*This will end. The only question still open is how.*",
    "*Some of the people in that square will not see another morning. The math has become too clear.*",
    "*The thing about a village this size — when it's over, everyone will know everyone's name.*",
    "*Someone in Whisperfall today is pretending harder than they ever have. Watch the hands. Watch the eyes.*",
    "*The wolves, if there are wolves, are not afraid. That is perhaps the most frightening thing of all.*",
]

def get_ambient_messages(day_num: int) -> list:
    """Return appropriate ambient message pool based on day number."""
    if day_num <= 2:
        return AMBIENT_MESSAGES_EARLY
    elif day_num <= 4:
        return AMBIENT_MESSAGES_MID
    else:
        return AMBIENT_MESSAGES_LATE

# Keep for backwards compatibility
AMBIENT_MESSAGES = AMBIENT_MESSAGES_EARLY

async def _post_ambient_message(guild, guild_id: int):
    """Post an atmospheric ambient message to village chat — Claude-generated or static fallback."""
    if not game_active(guild_id):
        return
    state = cached_get_state(guild_id)
    if state.get("phase") != "day":
        return
    vc_ch = guild.get_channel(state.get("village_chat_ch_id") or 0)
    if not vc_ch:
        return

    night_num = db_get_night_num(guild_id)
    day_num   = max(1, night_num)

    # Try Claude-generated ambient first (70% of the time for variety)
    if random.random() < 0.70:
        try:
            rows      = db_get_assignments(guild_id)
            alive_ct  = sum(1 for r in rows if r[2] == 1)
            dead_ct   = sum(1 for r in rows if r[2] == 0)
            wolf_ct   = sum(1 for r in rows if r[2] == 1 and get_team(guild_id, r[1]) == "wolf")

            stage = (
                "The village is still finding its footing. People still trust each other. Barely."
                if day_num <= 2 else
                "Trust has eroded. Alliances are fracturing. People are watching each other differently."
                if day_num <= 4 else
                f"Only {alive_ct} remain. Everyone knows the math. No one says it aloud."
            )
            prompt = (
                f"Whisperfall. Day {day_num}. {alive_ct} alive, {dead_ct} gone.\n"
                f"Stage: {stage}\n\n"
                f"Write ONE atmospheric observation about the village right now. "
                f"It should feel overheard, noticed — a detail, a sound, a small wrongness. "
                f"Format: *italics*. One or two sentences. No more.\n"
                f"Do NOT mention wolves, roles, or mechanics. Pure village atmosphere.\n"
                f"Do NOT start with 'The village' or 'Whisperfall' — find a different entry point."
            )
            ambient_system = (
                "You write atmospheric micro-observations for a gothic village called Whisperfall. "
                "Each observation is a single sensory detail — something seen, heard, smelled, noticed. "
                "The best ones feel like something the player almost missed. "
                "Never explain. Never summarize. Just observe."
            )
            resp = await _claude(prompt, ambient_system, max_tokens=80)
            if resp and len(resp.strip()) > 10:
                await vc_ch.send(resp.strip())
                return
        except Exception:
            pass

    # Fallback to static pool
    pool = get_ambient_messages(day_num)
    try:
        await vc_ch.send(random.choice(pool))
    except Exception:
        pass

async def _ambient_loop(guild, guild_id: int):
    """Send ambient messages throughout the day phase + vote reminder + night countdown."""
    import time as _t
    state  = cached_get_state(guild_id)
    end_ts = state.get("day_vote_end_time") or (_t.time() + 43200)

    # Send 3-5 ambient messages spread through the day
    num_msgs  = random.randint(3, 5)
    remaining = max(0, int(end_ts) - int(_t.time()))

    if remaining >= 3600:
        # Space ambient messages across available time
        sample_range = range(1800, remaining - 3600)
        if len(sample_range) >= num_msgs:
            intervals = sorted(random.sample(list(sample_range), num_msgs))
        else:
            intervals = sorted(random.sample(range(900, max(910, remaining - 900)), min(num_msgs, max(1, remaining // 1800))))

        prev = 0
        for delay in intervals:
            await asyncio.sleep(delay - prev)
            prev = delay
            if not game_active(guild_id):
                return
            state_check = cached_get_state(guild_id)
            if state_check.get("phase") != "day":
                return
            await _post_ambient_message(guild, guild_id)

    # ── Vote countdown reminder — 30 mins before close ────────────────────
    safe_task(_vote_countdown_reminder(guild, guild_id, int(end_ts)), "vote_reminder")

    # ── Night approach warning — 30 mins before 8 PM EST ─────────────────
    safe_task(_night_approach_warning(guild, guild_id), "night_warning")


async def _vote_countdown_reminder(guild, guild_id: int, end_ts: int):
    """Post a vote tally reminder 30 minutes before the day vote closes."""
    import time as _t
    wait = int(end_ts) - int(_t.time()) - 1800  # 30 mins before close
    if wait <= 0:
        return
    await asyncio.sleep(wait)

    if not game_active(guild_id):
        return
    state = cached_get_state(guild_id)
    if state.get("phase") != "day":
        return

    vc_ch = guild.get_channel(state.get("village_chat_ch_id") or 0)
    if not vc_ch:
        return

    # Build current tally
    votes    = db_get_day_votes(guild_id)
    rows     = db_get_assignments(guild_id)
    npcs     = db_get_npcs(guild_id)
    npc_map  = {n["npc_id"]: n["name"] for n in npcs}

    def get_name(pid):
        if pid in npc_map: return npc_map[pid]
        m = guild.get_member(pid)
        return m.display_name if m else str(pid)

    tally = {}
    for voter_id, target_id, *_ in votes:
        if target_id:
            tally.setdefault(target_id, []).append(voter_id)

    # Sort by vote count
    sorted_tally = sorted(tally.items(), key=lambda x: -len(x[1]))

    embed = discord.Embed(
        title       = "⏰ 30 Minutes Until Vote Closes",
        description = "*Whisperfall grows restless. The hour approaches.*",
        color       = 0xE74C3C
    )

    if sorted_tally:
        tally_lines = []
        for target_id, voter_ids in sorted_tally[:8]:
            target_name = get_name(target_id)
            voter_names = ", ".join(get_name(v) for v in voter_ids)
            tally_lines.append(f"**{target_name}** — {len(voter_ids)} vote(s)\n*{voter_names}*")
        embed.add_field(name="📊 Current Standings", value="\n\n".join(tally_lines), inline=False)
    else:
        embed.add_field(name="📊 Current Standings", value="No votes cast yet.", inline=False)

    embed.add_field(name="⏰ Closes", value=f"<t:{end_ts}:R>", inline=True)

    # Find who hasn't voted yet and ping them
    rows_r    = db_get_assignments(guild_id)
    npcs_r    = db_get_npcs(guild_id)
    npc_ids_r = {n["npc_id"] for n in npcs_r}
    voted_ids_r = {v[0] for v in tally.items()} | {v[0] for v in votes if v[1] is None}
    # Also check day_votes table directly
    all_votes_r = db_get_day_votes(guild_id)
    voted_set_r = {v[0] for v in all_votes_r}

    # Check frenzy — need two votes
    state_r    = cached_get_state(guild_id) or {}
    max_votes_r = int(state_r.get("votes_per_player") or 1)
    votes2_r   = db_get_day_votes_2(guild_id) if max_votes_r >= 2 else []
    voted2_set_r = {v[0] for v in votes2_r}

    missing_voters = []
    for pid, role, is_alive, _ in rows_r:
        if not is_alive: continue
        if pid in npc_ids_r: continue
        m = guild.get_member(pid)
        if not m: continue
        if pid not in voted_set_r:
            missing_voters.append(m.mention)
        elif max_votes_r >= 2 and pid not in voted2_set_r:
            missing_voters.append(f"{m.mention} *(needs 2nd vote)*")

    if missing_voters:
        embed.add_field(
            name  = "⚠️ Haven't Voted Yet",
            value = " ".join(missing_voters[:20]),
            inline= False)
        embed.set_footer(text="Missing votes result in immediate elimination.")
    else:
        embed.set_footer(text="All players have voted — cast or change your vote before time runs out.")

    try:
        await vc_ch.send(embed=embed)
        # Also send a separate ping line so mentions trigger notifications
        if missing_voters:
            ping_line = f"⏰ **30 minutes left to vote:** {' '.join(missing_voters[:20])}"
            await vc_ch.send(ping_line)
    except Exception:
        pass


async def _night_approach_warning(guild, guild_id: int):
    """Post a warning 30 minutes before day vote closes — night is coming."""
    import time as _t
    state    = cached_get_state(guild_id)
    vote_end = state.get("day_vote_end_time")
    if not vote_end:
        return  # No vote end time set — skip warning
    warn_ts = int(vote_end) - 1800  # 30 mins before vote closes
    wait    = warn_ts - int(_t.time())

    if wait <= 0:
        return
    await asyncio.sleep(wait)

    if not game_active(guild_id):
        return
    state = cached_get_state(guild_id)
    if state.get("phase") != "day":
        return

    vc_ch = guild.get_channel(state.get("village_chat_ch_id") or 0)
    if not vc_ch:
        return

    vote_end_ts = state.get("day_vote_end_time") or vote_end
    night_warnings = [
        f"*Whisperfall grows quiet. The hour approaches. Make your peace.* Vote closes <t:{vote_end_ts}:R>.",
        f"*The light is changing in Whisperfall. Whatever you have left to say — say it now.* Vote closes <t:{vote_end_ts}:R>.",
        f"*Thirty minutes. The village will be different when the sun comes up.* Vote closes <t:{vote_end_ts}:R>.",
        f"*The shadows are lengthening in the square. Night does not wait for unfinished business.* Vote closes <t:{vote_end_ts}:R>.",
        f"*Something in Whisperfall knows the night is close. It has been patient. It is almost done waiting.* Vote closes <t:{vote_end_ts}:R>.",
        f"*The bells will toll soon. Whatever you know, whatever you suspect — now is the time.* Vote closes <t:{vote_end_ts}:R>.",
        f"*The last half hour before dark in Whisperfall always feels the same. Like a held breath.* Vote closes <t:{vote_end_ts}:R>.",
    ]

    try:
        await vc_ch.send(random.choice(night_warnings))
    except Exception:
        pass

# ====================== PLAYER TRACKER ======================
SUSPICION_OPTIONS = [
    discord.SelectOption(label="0 — Unrated",        value="0",  emoji="⚫"),
    discord.SelectOption(label="1 — Barely notable", value="1",  emoji="🟢"),
    discord.SelectOption(label="2 — Slightly off",   value="2",  emoji="🟢"),
    discord.SelectOption(label="3 — Worth watching", value="3",  emoji="🟢"),
    discord.SelectOption(label="4 — Moderately sus", value="4",  emoji="🟡"),
    discord.SelectOption(label="5 — Concerning",     value="5",  emoji="🟡"),
    discord.SelectOption(label="6 — Getting risky",  value="6",  emoji="🟡"),
    discord.SelectOption(label="7 — High threat",    value="7",  emoji="🟠"),
    discord.SelectOption(label="8 — Very dangerous", value="8",  emoji="🟠"),
    discord.SelectOption(label="9 — Extreme threat", value="9",  emoji="🔴"),
    discord.SelectOption(label="10 — Imminent wolf", value="10", emoji="🚨"),
]
def _sus_emoji(n):
    if n == 0:   return "⚫"
    if n <= 3:   return "🟢"
    if n <= 6:   return "🟡"
    if n <= 8:   return "🟠"
    if n == 9:   return "🔴"
    return "🚨"
SUSPICION_EMOJI = {str(i): _sus_emoji(i) for i in range(11)}
SUSPICION_EMOJI["unknown"] = "⚫"


# ====================== TRACKER ======================

def _get_tracker_game_key(guild_id):
    """Returns a unique key for the current game based on night_num and a game counter."""
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT COALESCE(game_num,0) FROM game_counters WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def build_tracker_embed(guild, guild_id, owner_id, show_dead=True, sort_by_suspicion=True):
    """Build the full tracker embed — vote history integrated, per-player."""
    rows      = db_get_assignments(guild_id)
    tracker   = db_get_tracker(guild_id, owner_id)
    npcs      = db_get_npcs(guild_id)
    npc_map   = {n["npc_id"]: n["name"] for n in npcs}
    night_num = db_get_night_num(guild_id)
    history   = db_get_vote_history(guild_id)  # (day_num, voter_id, target_id, action, voted_at, change_count)

    # Auto-clear stale data from previous game
    assigned_ids = {r[0] for r in rows}
    tracker_ids  = set(tracker.keys())
    if tracker_ids and not tracker_ids.intersection(assigned_ids):
        conn_tc = sqlite3.connect(DB_FILE)
        c_tc    = conn_tc.cursor()
        c_tc.execute("DELETE FROM player_tracker WHERE guild_id=? AND owner_id=?", (guild_id, owner_id))
        conn_tc.commit()
        conn_tc.close()
        tracker = {}

    # Build full vote maps with timestamps and change counts
    # voted_for_me[pid]  = list of (day_num, voted_at_ts, change_count)
    # i_voted_for[pid]   = list of (day_num, voted_at_ts, change_count)
    voted_for_me = {}
    i_voted_for  = {}
    for row in history:
        day_num      = row[0]
        voter_id     = row[1]
        target_id    = row[2]
        action       = row[3]
        voted_at     = row[4] if len(row) > 4 else 0
        change_count = row[5] if len(row) > 5 else 0
        if action in ("abstain",) or not target_id:
            continue
        if target_id == owner_id and voter_id != owner_id:
            voted_for_me.setdefault(voter_id, []).append((day_num, voted_at, change_count))
        if voter_id == owner_id and target_id:
            i_voted_for.setdefault(target_id, []).append((day_num, voted_at, change_count))

    # Sort players — by suspicion desc, then alive before dead
    player_list = [r for r in rows if r[0] != owner_id]
    if sort_by_suspicion:
        def sort_key(r):
            pid, role, is_alive, _ = r
            sus = int(tracker.get(pid, {}).get("suspicion", 0) or 0)
            return (0 if is_alive else 1, -sus)
        player_list.sort(key=sort_key)

    alive_count = sum(1 for r in player_list if r[2] == 1)
    dead_count  = sum(1 for r in player_list if r[2] == 0)

    embed = discord.Embed(
        title       = "🔍 Investigation Tracker",
        description = (f"*Night {night_num} — {alive_count} alive, {dead_count} dead*\n"
                       f"*Sorted by suspicion. Use buttons to update.*"),
        color       = 0x2C3060
    )

    shown = 0
    for pid, role, is_alive, _ in player_list:
        if not is_alive and not show_dead:
            continue
        if shown >= 24:  # Discord embed field limit
            embed.set_footer(text=f"Showing 24/{len(player_list)} players. Run /tracker to see all.")
            break

        name = npc_map.get(pid) or (guild.get_member(pid).display_name if guild.get_member(pid) else str(pid))
        data          = tracker.get(pid, {})
        suspicion     = str(data.get("suspicion", "0") or "0")
        suspected_role= data.get("suspected_role", "")
        notes         = data.get("notes", "")
        sus_emoji     = SUSPICION_EMOJI.get(suspicion, "⚫")

        try:
            sus_num = int(suspicion)
            if sus_num == 0:     sus_label = "Unrated"
            elif sus_num <= 3:   sus_label = f"{sus_num}/10 — Low"
            elif sus_num <= 6:   sus_label = f"{sus_num}/10 — Moderate"
            elif sus_num <= 8:   sus_label = f"{sus_num}/10 — High"
            elif sus_num == 9:   sus_label = f"{sus_num}/10 — Extreme"
            else:                sus_label = f"{sus_num}/10 — Imminent"
        except (ValueError, TypeError):
            sus_label = "Unrated"

        status = "💀" if not is_alive else sus_emoji

        # Vote history — they voted for me
        vtfm_entries = voted_for_me.get(pid, [])
        if vtfm_entries:
            vtfm_parts = []
            for day_num, voted_at, change_count in sorted(vtfm_entries, key=lambda x: x[0]):
                ts   = f"<t:{voted_at}:t>" if voted_at else ""
                flip = f" *(flip #{change_count})*" if change_count > 0 else ""
                vtfm_parts.append(f"D{day_num}{' ' + ts if ts else ''}{flip}")
            vtfm_str = ", ".join(vtfm_parts)
        else:
            vtfm_str = "—"

        # Vote history — I voted for them
        ivtf_entries = i_voted_for.get(pid, [])
        if ivtf_entries:
            ivtf_parts = []
            for day_num, voted_at, change_count in sorted(ivtf_entries, key=lambda x: x[0]):
                ts   = f"<t:{voted_at}:t>" if voted_at else ""
                flip = f" *(flip #{change_count})*" if change_count > 0 else ""
                ivtf_parts.append(f"D{day_num}{' ' + ts if ts else ''}{flip}")
            ivtf_str = ", ".join(ivtf_parts)
        else:
            ivtf_str = "—"

        # Total times they voted for me vs times I voted for them
        vtfm_count = len(vtfm_entries)
        ivtf_count = len(ivtf_entries)
        flip_count = sum(1 for _, _, c in vtfm_entries if c > 0)

        name_display = f"{'~~' if not is_alive else ''}{name}{'~~' if not is_alive else ''}"
        field_name   = f"{status} {name_display}"

        lines = [f"**Suspicion:** {sus_emoji} {sus_label}"]
        if suspected_role:
            lines.append(f"**Suspected role:** {suspected_role}")
        lines.append(f"**Voted for me:** {vtfm_str}" + (f" *({vtfm_count}x)*" if vtfm_count > 1 else ""))
        lines.append(f"**I voted for them:** {ivtf_str}" + (f" *({ivtf_count}x)*" if ivtf_count > 1 else ""))
        if flip_count > 0:
            lines.append(f"**⚠️ Changed vote to me:** {flip_count}x — *notable flip behavior*")
        if notes:
            lines.append(f"📝 *{notes}*")

        embed.add_field(name=field_name, value="\n".join(lines)[:1024], inline=True)
        shown += 1

    if shown == 0:
        embed.add_field(name="No players yet", value="Game hasn't started or no players found.", inline=False)

    embed.set_footer(text=f"🔍 Your private case notes — updated live | Night {night_num}")
    return embed


async def _refresh_tracker_embed(guild, guild_id, owner_id, ch_id, msg_id):
    """Fetch the tracker message and update its embed. Silently ignores failures."""
    try:
        ch = guild.get_channel(ch_id) or guild.get_thread(ch_id)
        if not ch:
            ch = await guild.fetch_channel(ch_id)
        msg = await ch.fetch_message(msg_id)
        await msg.edit(embed=build_tracker_embed(guild, guild_id, owner_id))
    except Exception as e:
        print(f"[tracker] embed refresh failed: {e}")


class TrackerSuspicionView(View):
    def __init__(self, guild_id, owner_id, target_id, tracker_msg_id, ch_id):
        super().__init__(timeout=120)
        self.guild_id       = guild_id
        self.owner_id       = owner_id
        self.target_id      = target_id
        self.tracker_msg_id = tracker_msg_id
        self.ch_id          = ch_id
        sel = Select(placeholder="Set suspicion level...", options=SUSPICION_OPTIONS)
        sel.callback = self.on_select
        self.add_item(sel)

    async def on_select(self, interaction: discord.Interaction):
        # Defer immediately — DB write + fetch_message can exceed 3s
        await interaction.response.defer(ephemeral=True)
        val = interaction.data["values"][0]
        db_set_tracker_entry(self.guild_id, self.owner_id, self.target_id, suspicion=val)
        await _refresh_tracker_embed(interaction.guild, self.guild_id, self.owner_id,
                                     self.ch_id, self.tracker_msg_id)
        target = interaction.guild.get_member(self.target_id)
        tname  = target.display_name if target else str(self.target_id)
        emoji  = SUSPICION_EMOJI.get(val, "⚫")
        await interaction.followup.send(
            fmt(f"{emoji} Suspicion for **{tname}** set to {val}/10."), ephemeral=True)


class TrackerRoleView(View):
    """
    Paginated role selector — shows 23 roles per page with Prev/Next buttons.
    Pulls from the guild's actual game_roles so custom roles are always included.
    """
    def __init__(self, guild_id, owner_id, target_id, tracker_msg_id, ch_id, page=0):
        super().__init__(timeout=120)
        self.guild_id       = guild_id
        self.owner_id       = owner_id
        self.target_id      = target_id
        self.tracker_msg_id = tracker_msg_id
        self.ch_id          = ch_id
        self.page           = page

        # Load real roles for this guild
        all_roles = cached_load_roles(guild_id)
        self.role_names = [r["name"] for r in all_roles]
        self.page_size  = 23  # leave room for clear option + fits Discord's 25 limit

        total_pages = max(1, -(-len(self.role_names) // self.page_size))  # ceiling div
        start = page * self.page_size
        end   = start + self.page_size
        page_roles = self.role_names[start:end]

        opts = [discord.SelectOption(label="— Clear suspected role —", value="__clear__")]
        opts += [discord.SelectOption(label=r, value=r) for r in page_roles]
        sel = Select(placeholder=f"Suspected role (page {page+1}/{total_pages})...", options=opts)
        sel.callback = self.on_select
        self.add_item(sel)

        # Prev button
        if page > 0:
            prev_btn = Button(label="◀ Prev", style=discord.ButtonStyle.secondary, row=1)
            prev_btn.callback = self.on_prev
            self.add_item(prev_btn)

        # Next button
        if end < len(self.role_names):
            next_btn = Button(label="Next ▶", style=discord.ButtonStyle.secondary, row=1)
            next_btn.callback = self.on_next
            self.add_item(next_btn)

    async def on_select(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        val      = interaction.data["values"][0]
        role_val = "" if val == "__clear__" else val
        db_set_tracker_entry(self.guild_id, self.owner_id, self.target_id, suspected_role=role_val)
        await _refresh_tracker_embed(interaction.guild, self.guild_id, self.owner_id,
                                     self.ch_id, self.tracker_msg_id)
        target = interaction.guild.get_member(self.target_id)
        tname  = target.display_name if target else str(self.target_id)
        await interaction.followup.send(
            fmt(f"🎭 Suspected role for **{tname}** set to **{role_val or 'cleared'}**."),
            ephemeral=True)

    async def on_prev(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            view=TrackerRoleView(self.guild_id, self.owner_id, self.target_id,
                                 self.tracker_msg_id, self.ch_id, page=self.page - 1))

    async def on_next(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            view=TrackerRoleView(self.guild_id, self.owner_id, self.target_id,
                                 self.tracker_msg_id, self.ch_id, page=self.page + 1))


class TrackerNoteModal(discord.ui.Modal, title="Add a Note"):
    note = discord.ui.TextInput(
        label       = "Note (appended to existing notes)",
        style       = discord.TextStyle.paragraph,
        placeholder = "e.g. Defended wolf on D2, changed vote late...",
        max_length  = 300,
        required    = True)

    def __init__(self, guild_id, owner_id, target_id, tracker_msg_id, ch_id, existing_note=""):
        super().__init__()
        self.guild_id       = guild_id
        self.owner_id       = owner_id
        self.target_id      = target_id
        self.tracker_msg_id = tracker_msg_id
        self.ch_id          = ch_id
        self.existing_note  = existing_note
        if existing_note:
            self.note.placeholder = f"Current: {existing_note[:80]}... (will append)"

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        new_text = self.note.value.strip()
        combined = f"{self.existing_note} | {new_text}" if self.existing_note else new_text
        combined = combined[-400:]
        db_set_tracker_entry(self.guild_id, self.owner_id, self.target_id, notes=combined)
        await _refresh_tracker_embed(interaction.guild, self.guild_id, self.owner_id,
                                     self.ch_id, self.tracker_msg_id)
        target = interaction.guild.get_member(self.target_id)
        tname  = target.display_name if target else str(self.target_id)
        await interaction.followup.send(fmt(f"📝 Note for **{tname}** updated."), ephemeral=True)


class TrackerClearNoteView(View):
    """Lets the player clear a note for a specific player."""
    def __init__(self, guild_id, owner_id, target_id, tracker_msg_id, ch_id):
        super().__init__(timeout=60)
        self.guild_id       = guild_id
        self.owner_id       = owner_id
        self.target_id      = target_id
        self.tracker_msg_id = tracker_msg_id
        self.ch_id          = ch_id
        btn = Button(label="🗑️ Confirm Clear Note", style=discord.ButtonStyle.danger)
        btn.callback = self.on_confirm
        self.add_item(btn)

    async def on_confirm(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        db_set_tracker_entry(self.guild_id, self.owner_id, self.target_id, notes="")
        await _refresh_tracker_embed(interaction.guild, self.guild_id, self.owner_id,
                                     self.ch_id, self.tracker_msg_id)
        await interaction.followup.send(fmt("🗑️ Note cleared."), ephemeral=True)


class TrackerPlayerSelectView(View):
    """Dropdown to pick which player to update, then routes to correct sub-view."""
    def __init__(self, guild_id, owner_id, tracker_msg_id, ch_id, mode, guild=None):
        super().__init__(timeout=120)
        self.guild_id       = guild_id
        self.owner_id       = owner_id
        self.tracker_msg_id = tracker_msg_id
        self.ch_id          = ch_id
        self.mode           = mode  # "suspicion" / "role" / "note" / "clear_note"
        self._guild         = guild

        rows    = db_get_assignments(guild_id)
        npcs    = db_get_npcs(guild_id)
        npc_map = {n["npc_id"]: n["name"] for n in npcs}
        opts    = []
        for pid, role, is_alive, _ in rows:
            if pid == owner_id: continue
            name = npc_map.get(pid)
            if not name and guild:
                m = guild.get_member(pid)
                name = m.display_name if m else str(pid)
            dead_mark = " 💀" if not is_alive else ""
            opts.append(discord.SelectOption(
                label = f"{name}{dead_mark}"[:100],
                value = str(pid)))
        if opts:
            sel = Select(placeholder="Choose a player...", options=opts[:25])
            sel.callback = self.on_select
            self.add_item(sel)

    async def on_select(self, interaction: discord.Interaction):
        target_id = int(interaction.data["values"][0])
        if self.mode == "suspicion":
            view = TrackerSuspicionView(self.guild_id, self.owner_id, target_id,
                                        self.tracker_msg_id, self.ch_id)
            await interaction.response.send_message(
                fmt("Set suspicion level:"), view=view, ephemeral=True)
        elif self.mode == "role":
            view = TrackerRoleView(self.guild_id, self.owner_id, target_id,
                                   self.tracker_msg_id, self.ch_id, page=0)
            await interaction.response.send_message(
                fmt("Set suspected role (use ◀ ▶ to browse all roles):"),
                view=view, ephemeral=True)
        elif self.mode == "note":
            existing = db_get_tracker(self.guild_id, self.owner_id).get(target_id, {}).get("notes", "")
            modal = TrackerNoteModal(self.guild_id, self.owner_id, target_id,
                                     self.tracker_msg_id, self.ch_id, existing_note=existing)
            await interaction.response.send_modal(modal)
        elif self.mode == "clear_note":
            view = TrackerClearNoteView(self.guild_id, self.owner_id, target_id,
                                        self.tracker_msg_id, self.ch_id)
            target = interaction.guild.get_member(target_id)
            tname  = target.display_name if target else str(target_id)
            await interaction.response.send_message(
                fmt(f"Clear note for **{tname}**?"), view=view, ephemeral=True)



class TrackerShareVoteView(View):
    """Attached to each day vote history embed — lets player share it to village chat."""
    def __init__(self, guild_id, owner_id, day_num, embed):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.day_num  = day_num
        self.embed    = embed

        share_btn = Button(
            label = f"📢 Share Day {day_num} to Village",
            style = discord.ButtonStyle.green)
        share_btn.callback = self.on_share
        self.add_item(share_btn)

    async def on_share(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            return await interaction.response.send_message(
                "❌ This is not your tracker.", ephemeral=True)

        state  = cached_get_state(interaction.guild_id) or {}
        vc_ch  = interaction.guild.get_channel(state.get("village_chat_ch_id") or 0)
        if not vc_ch:
            return await interaction.response.send_message(
                "❌ Could not find village chat.", ephemeral=True)

        # Build a fresh share embed branded with the sharer's name
        share_embed = discord.Embed(
            title       = f"📜 Day {self.day_num} Vote History — Shared by {interaction.user.display_name}",
            description = self.embed.description,
            color       = 0xF39C12)

        # Copy fields (flip summary etc)
        for field in self.embed.fields:
            share_embed.add_field(name=field.name, value=field.value, inline=field.inline)

        share_embed.set_footer(
            text=f"Shared by {interaction.user.display_name} — "
                 f"interpret this evidence as you see fit.")

        await vc_ch.send(embed=share_embed)

        # Disable the share button after use so it can't be spammed
        self.children[0].disabled = True
        self.children[0].label    = f"✅ Shared to village"
        await interaction.response.edit_message(view=self)

        await interaction.followup.send(
            fmt(f"📢 Day {self.day_num} vote history shared to village chat."),
            ephemeral=True)

class TrackerMainView(View):
    """Main tracker control buttons."""
    def __init__(self, guild_id, owner_id, tracker_msg_id, ch_id):
        super().__init__(timeout=None)  # Persistent — buttons never expire
        self.guild_id       = guild_id
        self.owner_id       = owner_id
        self.tracker_msg_id = tracker_msg_id
        self.ch_id          = ch_id

        sus_btn   = Button(label="🔴 Suspicion",        style=discord.ButtonStyle.danger,    row=0)
        rol_btn   = Button(label="🎭 Suspected Role",    style=discord.ButtonStyle.blurple,   row=0)
        not_btn   = Button(label="📝 Add Note",          style=discord.ButtonStyle.secondary, row=0)
        clr_btn   = Button(label="🗑️ Clear Note",       style=discord.ButtonStyle.secondary, row=0)
        ref_btn   = Button(label="🔄 Refresh",           style=discord.ButtonStyle.secondary, row=1)
        vhx_btn   = Button(label="📜 Full Vote History", style=discord.ButtonStyle.secondary, row=1)
        shr_btn   = Button(label="📢 Share to Village",  style=discord.ButtonStyle.green,     row=1)

        sus_btn.callback = self.on_suspicion
        rol_btn.callback = self.on_role
        not_btn.callback = self.on_note
        clr_btn.callback = self.on_clear_note
        ref_btn.callback = self.on_refresh
        vhx_btn.callback = self.on_vote_history
        shr_btn.callback = self.on_share

        self.add_item(sus_btn)
        self.add_item(rol_btn)
        self.add_item(not_btn)
        self.add_item(clr_btn)
        self.add_item(ref_btn)
        self.add_item(vhx_btn)
        self.add_item(shr_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("❌ This is not your tracker.", ephemeral=True)
            return False
        return True

    async def on_suspicion(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            fmt("Choose a player to update suspicion:"),
            view=TrackerPlayerSelectView(self.guild_id, self.owner_id,
                                         self.tracker_msg_id, self.ch_id, "suspicion",
                                         guild=interaction.guild),
            ephemeral=True)

    async def on_role(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            fmt("Choose a player to set suspected role:"),
            view=TrackerPlayerSelectView(self.guild_id, self.owner_id,
                                         self.tracker_msg_id, self.ch_id, "role",
                                         guild=interaction.guild),
            ephemeral=True)

    async def on_note(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            fmt("Choose a player to add a note for:"),
            view=TrackerPlayerSelectView(self.guild_id, self.owner_id,
                                         self.tracker_msg_id, self.ch_id, "note",
                                         guild=interaction.guild),
            ephemeral=True)

    async def on_clear_note(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            fmt("Choose a player to clear notes for:"),
            view=TrackerPlayerSelectView(self.guild_id, self.owner_id,
                                         self.tracker_msg_id, self.ch_id, "clear_note",
                                         guild=interaction.guild),
            ephemeral=True)

    async def on_refresh(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        await _refresh_tracker_embed(interaction.guild, self.guild_id, self.owner_id,
                                     self.ch_id, self.tracker_msg_id)
        await interaction.followup.send(fmt("✅ Tracker refreshed."), ephemeral=True)

    async def on_vote_history(self, interaction: discord.Interaction):
        """Show full vote history for all days — who voted for whom, when, and changes."""
        history = db_get_vote_history(interaction.guild_id)
        rows    = db_get_assignments(interaction.guild_id)
        npcs    = db_get_npcs(interaction.guild_id)
        npc_map = {n["npc_id"]: n["name"] for n in npcs}

        def get_name(pid):
            n = npc_map.get(pid)
            if n: return n
            m = interaction.guild.get_member(pid)
            return m.display_name if m else str(pid)

        if not history:
            return await interaction.response.send_message(
                "No vote history recorded yet.", ephemeral=True)

        # Group by day
        by_day = {}
        for row in history:
            day_num      = row[0]
            voter_id     = row[1]
            target_id    = row[2]
            action       = row[3]
            voted_at     = row[4] if len(row) > 4 else 0
            change_count = row[5] if len(row) > 5 else 0
            by_day.setdefault(day_num, []).append((voter_id, target_id, action, voted_at, change_count))

        embeds = []
        for day_num in sorted(by_day.keys()):
            embed = discord.Embed(
                title = f"📜 Day {day_num} — Full Vote History",
                color = 0x5865F2)
            lines = []
            flippers = []
            for voter_id, target_id, action, voted_at, change_count in by_day[day_num]:
                voter  = get_name(voter_id)
                ts_str = f" <t:{voted_at}:t>" if voted_at else ""
                if action == "abstain" or target_id is None:
                    lines.append(f"**{voter}** → 🤐 Abstain{ts_str}")
                else:
                    target = get_name(target_id)
                    change_badge = f" *(change #{change_count})*" if change_count > 0 else ""
                    lines.append(f"**{voter}** → **{target}**{ts_str}{change_badge}")
                    if change_count > 0:
                        flippers.append((voter, change_count))

            embed.description = "\n".join(lines) if lines else "No votes recorded."

            if flippers:
                # Deduplicate and sum changes per voter
                flip_map = {}
                for voter, count in flippers:
                    flip_map[voter] = max(flip_map.get(voter, 0), count)
                flip_summary = ", ".join(
                    f"**{v}** ({c} change{'s' if c > 1 else ''})"
                    for v, c in sorted(flip_map.items(), key=lambda x: -x[1]))
                embed.add_field(name="🔄 Vote Changers", value=flip_summary, inline=False)

            embeds.append(embed)

        # Send each day embed with a share button
        await interaction.response.send_message(
            embed=embeds[0],
            view=TrackerShareVoteView(interaction.guild_id, interaction.user.id,
                                      sorted(by_day.keys())[0], embeds[0]),
            ephemeral=True)
        for i, e in enumerate(embeds[1:], 1):
            day = sorted(by_day.keys())[i]
            await interaction.followup.send(
                embed=e,
                view=TrackerShareVoteView(interaction.guild_id, interaction.user.id, day, e),
                ephemeral=True)

    async def on_share(self, interaction: discord.Interaction):
        """Open modal so player can add a message, then post tracker to village chat."""
        await interaction.response.send_modal(
            TrackerShareModal(self.guild_id, self.owner_id, self.ch_id))


class TrackerShareModal(discord.ui.Modal, title="Share Tracker to Village"):
    message = discord.ui.TextInput(
        label       = "Message to accompany your share (optional)",
        placeholder = "e.g. Based on vote patterns, I think Jordan is suspicious.",
        style       = discord.TextStyle.paragraph,
        max_length  = 500,
        required    = False,
    )

    def __init__(self, guild_id, owner_id, ch_id):
        super().__init__()
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.ch_id    = ch_id

    async def on_submit(self, interaction: discord.Interaction):
        state  = cached_get_state(self.guild_id) or {}
        vc_ch  = interaction.guild.get_channel(state.get("village_chat_ch_id") or 0)
        if not vc_ch:
            return await interaction.response.send_message(
                "❌ Could not find village chat.", ephemeral=True)

        embed = build_tracker_embed(interaction.guild, self.guild_id, self.owner_id)
        embed.title = f"🔍 {interaction.user.display_name}'s Investigation Tracker"
        embed.set_footer(text=f"Shared by {interaction.user.display_name} — interpret this evidence as you see fit.")

        msg_text = self.message.value.strip() if self.message.value else None
        content  = f"**{interaction.user.display_name}:** {msg_text}" if msg_text else None

        await vc_ch.send(content=content, embed=embed)
        await interaction.response.send_message(
            fmt("📢 Your tracker has been shared to village chat."), ephemeral=True)




@tree.command(name="tracker", description="Open your personal investigation tracker")
async def tracker(interaction: discord.Interaction):
    try:
        if not game_active(interaction.guild_id):
            return await interaction.response.send_message(
                "No active game.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)

        # Clear any stale tracker data from previous game at open time
        rows = db_get_assignments(interaction.guild_id)
        assigned_ids = {r[0] for r in rows}
        existing = db_get_tracker(interaction.guild_id, interaction.user.id)
        if existing and not set(existing.keys()).intersection(assigned_ids):
            conn_cl = sqlite3.connect(DB_FILE)
            c_cl    = conn_cl.cursor()
            c_cl.execute("DELETE FROM player_tracker WHERE guild_id=? AND owner_id=?",
                         (interaction.guild_id, interaction.user.id))
            conn_cl.commit()
            conn_cl.close()

        # Find or create tracker thread
        priv_row = next((r for r in rows if r[0] == interaction.user.id), None)
        if not priv_row or not priv_row[3]:
            return await interaction.followup.send(
                "❌ No private channel found. Make sure you are in the current game.", ephemeral=True)

        priv_ch = interaction.guild.get_channel(priv_row[3])
        if not priv_ch:
            return await interaction.followup.send(
                "❌ Could not find your private channel.", ephemeral=True)

        tracker_thread = None
        try:
            if hasattr(priv_ch, "threads"):
                for t in priv_ch.threads:
                    if t.name.lower().startswith("🔍 tracker"):
                        tracker_thread = t
                        break
            if not tracker_thread:
                async for t in priv_ch.archived_threads():
                    if t.name.lower().startswith("🔍 tracker"):
                        await t.edit(archived=False)
                        tracker_thread = t
                        break
        except Exception:
            pass

        if not tracker_thread:
            try:
                seed_msg = await priv_ch.send("🔍 **Investigation Tracker**")
                tracker_thread = await seed_msg.create_thread(
                    name="🔍 Tracker — Investigation Notes",
                    auto_archive_duration=10080)
                await tracker_thread.send(fmt(
                    "📋 **Your Investigation Tracker**\n"
                    "Track suspicion, suspected roles, and notes for every player.\n"
                    "Vote history — including timestamps and changes — is pulled automatically.\n"
                    "Sorted by suspicion level so your top threats are always at the top.\n"
                    "Run `/tracker` anytime to get a fresh embed with updated data."))
            except Exception as e:
                print(f"[tracker] Thread creation failed: {e} — posting in channel instead")
                tracker_thread = priv_ch

        embed = build_tracker_embed(interaction.guild, interaction.guild_id, interaction.user.id)
        msg   = await tracker_thread.send(embed=embed)
        view  = TrackerMainView(interaction.guild_id, interaction.user.id, msg.id, tracker_thread.id)
        await msg.edit(view=view)
        db_set_tracker_msg(interaction.guild_id, interaction.user.id, msg.id, tracker_thread.id)

        await interaction.followup.send(
            fmt(f"✅ Tracker opened — <#{tracker_thread.id}>\n"
                f"Run `/tracker` anytime to get a fresh embed with updated vote data."),
            ephemeral=True)

    except Exception as e:
        import traceback
        print(f"[tracker] Error: {e}")
        traceback.print_exc()
        try:
            if interaction.response.is_done():
                await interaction.followup.send(f"❌ Tracker error: {e}", ephemeral=True)
            else:
                await interaction.response.send_message(f"❌ Tracker error: {e}", ephemeral=True)
        except Exception:
            pass

# ====================== NIGHT PHASE ======================
night_timers = {}

# ── Night action mod-log helper ───────────────────────────────────────────
async def _mod_log_action(guild, night_num, actor, target, role_name, note=""):
    """Post a standardised night action notification to mod-log."""
    role_icons = {
        "Seer": "🔮", "Doctor": "💊", "Surgeon": "🏥",
        "Witch": "🧙", "Sheriff": "🔫", "Huntsman": "🏹", "Insomniac": "😴",
        "Medium": "🌀", "Gravedigger": "⚰️", "Hermit": "🏚️", "Agitator": "📢",
        "Governor": "🎖️", "Clone": "🪞", "Shapeshifter": "🎭",
        "Wolf Pup": "🐾", "Alpha": "👑", "Elite Alpha": "👑⭐",
        "Bloodhound": "🦴", "Bloodletter": "🩸", "Crazed Wolf": "🌪️",
        "Dire Wolf": "💔", "Echo-Stalker": "👁️", "Shadow Wolf": "🌑",
        "Werekitten": "🐱", "White Wolf": "🤍",
        "Oracle": "🔯", "Warlock": "⚗️", "Fairy Elf": "🧚",
    }
    icon = role_icons.get(role_name, "🌙")
    actor_name  = actor.display_name  if actor  else str(actor)
    target_name = target.display_name if target else str(target)
    msg = (f"{icon} **{role_name}** action — Night {night_num}\n"
           f"**Player:** {actor_name}\n"
           f"**Target:** {target_name}")
    if note:
        msg += f"\n*{note}*"
    await post_mod_log(guild, msg)


# ── Base class all role views inherit from ────────────────────────────────
class BaseNightView(View):
    def __init__(self, guild_id, actor_id, role_name, alive_players):
        super().__init__(timeout=None)
        self.guild_id     = guild_id
        self.actor_id     = actor_id
        self.role_name    = role_name
        self.alive_players= alive_players

    def _player_options(self, exclude_self=True):
        npcs_base = db_get_npcs(self.guild_id)
        npc_map_b = {n["npc_id"]: n["name"] for n in npcs_base}
        opts = []
        for p in self.alive_players:
            if exclude_self and p.id == self.actor_id:
                continue
            name = npc_map_b.get(p.id) or p.display_name
            opts.append(discord.SelectOption(
                label=name[:100],
                value=str(p.id),
                description="🤖 NPC" if p.id in npc_map_b else None
            ))
        return opts

    def _all_options(self):
        """Include self in options (for roles that can target anyone)."""
        return self._player_options(exclude_self=False)

    async def _save_and_close(self, interaction, action_key, target_id, confirm_text,
                               mod_log_msg: str = None):
        """Save night action, edit message, then post to mod-log. Always responds within 3s."""
        try:
            night_num = db_get_night_num(interaction.guild_id)
            db_save_night_action(interaction.guild_id, night_num, self.actor_id, action_key, target_id)
            # Respond first — must happen within 3 seconds
            try:
                await interaction.response.edit_message(content=fmt(f"✅ {confirm_text}"), view=None)
            except discord.errors.InteractionResponded:
                await interaction.followup.send(content=fmt(f"✅ {confirm_text}"), ephemeral=True)
            except Exception as e:
                print(f"[_save_and_close] edit_message error: {e}")
                try:
                    await interaction.response.send_message(content=fmt(f"✅ {confirm_text}"), ephemeral=True)
                except Exception:
                    pass
            # Post to mod-log after responding (safe - interaction already acknowledged)
            if mod_log_msg:
                await post_mod_log(interaction.guild, mod_log_msg)
            actor  = interaction.guild.get_member(self.actor_id)
            target = interaction.guild.get_member(target_id) if target_id else None
            await _mod_log_action(interaction.guild, night_num, actor, target, self.role_name)
            safe_task(update_mod_dashboard(interaction.guild), "dashboard_action_submit")
        except Exception as e:
            print(f"[_save_and_close] error: {e}")
            import traceback


# ── Seer ──────────────────────────────────────────────────────────────────
class SeerView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        opts = self._player_options()
        if opts:
            sel = Select(placeholder="🔮 Choose a player to investigate", options=opts)
            sel.callback = self.on_select
            self.add_item(sel)

    async def on_select(self, interaction):
        target_id  = int(interaction.data["values"][0])
        guild      = interaction.guild
        guild_id   = interaction.guild_id
        night_num  = db_get_night_num(guild_id)
        db_save_night_action(guild_id, night_num, self.actor_id, "seer", target_id)

        rows        = db_get_assignments(guild_id)
        target_row  = next((r for r in rows if r[0] == target_id), None)
        target_role = target_row[1] if target_row else "Unknown"
        team        = get_team(guild_id, target_role)
        target_m    = guild.get_member(target_id)
        target_name = target_m.display_name if target_m else str(target_id)

        # Seer only answers: is this person a wolf? Yes or No.
        # Special cases handled silently — player never sees role name or reason
        is_wolf = False
        if target_role == "Lycan":
            is_wolf = True   # Appears as wolf even though village team
        elif target_role == "Blessed Wolf":
            check_num = db_increment_check(guild_id, target_id)
            is_wolf = check_num >= 3  # First two checks appear safe
        elif target_role in {"Cursed", "Werekitten"}:
            is_wolf = False  # Always appear village
        elif target_role == "White Wolf":
            is_wolf = True   # Appears as wolf despite village team
        else:
            is_wolf = get_team(guild_id, target_role) == "wolf"

        if is_wolf:
            result_label = "✅ Yes"
            result_color = 0xC0392B
            result_team  = "wolf"
        else:
            result_label = "❌ No"
            result_color = 0x27AE60
            result_team  = "village"
        result_note = ""

        # Post to mod-log — result queued, NOT delivered yet
        actor = guild.get_member(self.actor_id)
        await post_mod_log(guild,
            f"🔮 **Seer Investigation queued** — Night {night_num}\n"
            f"**Seer:** {actor.display_name if actor else self.actor_id}\n"
            f"**Target:** {target_name} ({target_role})\n"
            f"**Result queued:** {result_label} — will deliver when mod clicks Deliver Results.")

        # Track Seer accuracy
        if result_team == "wolf" and team == "wolf":
            db_update_seer_correct(guild_id, self.actor_id)

        await interaction.response.edit_message(
            content=fmt(f"✅ Investigation submitted on {target_name}.\nYour result will be delivered when night resolves."),
            view=None)


# ── Doctor ────────────────────────────────────────────────────────────────
class DoctorView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        uses = db_get_ability_uses(self.guild_id, self.actor_id)
        if uses is None or uses <= 0:
            btn = Button(label="💊 Save used (0/1 remaining)", style=discord.ButtonStyle.secondary, disabled=True)
            self.add_item(btn)
            return
        save_btn = Button(label="💊 Use Save (1/1 remaining)", style=discord.ButtonStyle.green)
        skip_btn = Button(label="Skip Tonight",                style=discord.ButtonStyle.secondary)
        save_btn.callback = self.on_save
        skip_btn.callback = self.on_skip
        self.add_item(save_btn)
        self.add_item(skip_btn)

    async def on_save(self, interaction):
        uses = db_get_ability_uses(interaction.guild_id, self.actor_id)
        if uses is None or uses <= 0:
            return await interaction.response.send_message(
                "❌ You have already used your save.", ephemeral=True)
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "doctor_save", None)
        db_deduct_ability_uses(interaction.guild_id, self.actor_id, 1)
        actor = interaction.guild.get_member(self.actor_id)
        await interaction.response.edit_message(
            content=fmt("✅ Save used. This was your only save for the game."), view=None)
        await post_mod_log(interaction.guild,
            f"💊 **Doctor** — Night {night_num}\n**{actor.display_name}** used their **SAVE** (0 remaining).")

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "doctor_skip", None)
        actor = interaction.guild.get_member(self.actor_id)
        await interaction.response.edit_message(content=fmt("✅ You chose not to save tonight."), view=None)
        await post_mod_log(interaction.guild,
            f"💊 **Doctor** — Night {night_num}\n**{actor.display_name}** chose to **SKIP** tonight.")


# ── Surgeon ───────────────────────────────────────────────────────────────
class SurgeonChargeView(View):
    """Mod-log view — lets mod deduct 1 or 2 charges after a Surgeon save."""
    def __init__(self, guild_id, surgeon_id, surgeon_name, night_num, priv_ch_id):
        super().__init__(timeout=3600)
        self.guild_id     = guild_id
        self.surgeon_id   = surgeon_id
        self.surgeon_name = surgeon_name
        self.night_num    = night_num
        self.priv_ch_id   = priv_ch_id
        btn1 = Button(label="1 charge used (normal save)",       style=discord.ButtonStyle.primary)
        btn2 = Button(label="2 charges used (blocked turn)",     style=discord.ButtonStyle.danger)
        btn1.callback = self.on_one
        btn2.callback = self.on_two
        self.add_item(btn1)
        self.add_item(btn2)

    async def _deduct(self, interaction, amount):
        db_deduct_ability_uses(self.guild_id, self.surgeon_id, amount)
        remaining = db_get_ability_uses(self.guild_id, self.surgeon_id) or 0
        # Notify Surgeon in their private channel
        guild   = interaction.guild
        priv_ch = guild.get_channel(self.priv_ch_id or 0)
        if priv_ch:
            charge_word = "charge" if amount == 1 else "charges"
            exhausted = "\n⚠️ No charges left — you cannot save again this game." if remaining == 0 else ""
            await priv_ch.send(fmt(
                f"🏥 Night {self.night_num} — your save was applied.\n"
                f"**{amount} {charge_word} used** — **{remaining}/3 remaining**."
                + exhausted))
        await interaction.response.edit_message(
            content=f"✅ {amount} charge(s) deducted — {self.surgeon_name} has {remaining}/3 remaining.",
            view=None)

    async def on_one(self, interaction): await self._deduct(interaction, 1)
    async def on_two(self, interaction): await self._deduct(interaction, 2)


class SurgeonView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        uses = db_get_ability_uses(self.guild_id, self.actor_id)
        if uses is None or uses <= 0:
            btn = Button(label="🏥 No charges remaining (0/3)", style=discord.ButtonStyle.secondary, disabled=True)
            self.add_item(btn)
            return
        save_btn = Button(label=f"🏥 Use Save ({uses}/3 remaining)", style=discord.ButtonStyle.green)
        skip_btn = Button(label="Skip Tonight",                       style=discord.ButtonStyle.secondary)
        save_btn.callback = self.on_save
        skip_btn.callback = self.on_skip
        self.add_item(save_btn)
        self.add_item(skip_btn)

    async def on_save(self, interaction):
        uses = db_get_ability_uses(interaction.guild_id, self.actor_id)
        if uses is None or uses <= 0:
            return await interaction.response.send_message(
                "❌ No charges remaining.", ephemeral=True)
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "surgeon_save", None)
        actor   = interaction.guild.get_member(self.actor_id)
        rows    = db_get_assignments(interaction.guild_id)
        row     = next((r for r in rows if r[0] == self.actor_id), None)
        priv_ch_id = row[3] if row else None
        charge_view = SurgeonChargeView(
            interaction.guild_id, self.actor_id,
            actor.display_name if actor else str(self.actor_id),
            night_num, priv_ch_id)
        await post_mod_log(interaction.guild,
            f"🏥 **Surgeon** — Night {night_num}\n"
            f"**{actor.display_name if actor else self.actor_id}** used their save ({uses}/3 charges left).\n"
            f"**Select below how many charges to deduct:**",
            )
        # Post charge selector to mod-log
        state_s  = db_get_state(interaction.guild_id) or {}
        mod_ch   = interaction.guild.get_channel(state_s.get("mod_log_channel_id") or 0)
        if mod_ch:
            await mod_ch.send(
                f"🏥 Surgeon save Night {night_num} — deduct charges:",
                view=charge_view)
        await interaction.response.edit_message(content=fmt("✅ Save submitted. The mod will confirm your charge usage."), view=None)

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "surgeon_skip", None)
        actor = interaction.guild.get_member(self.actor_id)
        await interaction.response.edit_message(content=fmt("✅ You chose not to save tonight."), view=None)
        await post_mod_log(interaction.guild,
            f"🏥 **Surgeon** — Night {night_num}\n**{actor.display_name}** chose to **SKIP** tonight.")



# ── Witch ─────────────────────────────────────────────────────────────────
class WitchView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._build()

    def _build(self):
        self.clear_items()
        night_num = db_get_night_num(self.guild_id)
        can_act   = db_witch_can_act(self.guild_id, night_num)
        used_save, used_kill = db_get_witch_uses(self.guild_id, self.actor_id)

        if not can_act:
            # Wrong night — show disabled buttons with explanation
            wait_btn = Button(
                label=f"🧙 Cannot act tonight — next action: Night {night_num + (1 if night_num % 2 == 0 else 2) if night_num >= 2 else 2}",
                style=discord.ButtonStyle.secondary, disabled=True)
            self.add_item(wait_btn)
            return

        opts = self._all_options()
        sel = Select(placeholder="🧙 Select a target first", options=opts)
        sel.callback = self._noop
        self.add_item(sel)
        save_btn = Button(label="💚 Save Potion",  style=discord.ButtonStyle.green,
                          disabled=bool(used_save))
        kill_btn = Button(label="☠️ Poison Potion", style=discord.ButtonStyle.danger,
                          disabled=bool(used_kill))
        skip_btn = Button(label="Skip Tonight",    style=discord.ButtonStyle.secondary)
        save_btn.callback = self.on_save
        kill_btn.callback = self.on_kill
        skip_btn.callback = self.on_skip
        self.add_item(save_btn)
        self.add_item(kill_btn)
        self.add_item(skip_btn)

    async def _noop(self, interaction): await interaction.response.defer()

    async def on_save(self, interaction):
        used_save, _ = db_get_witch_uses(interaction.guild_id, self.actor_id)
        if used_save:
            return await interaction.response.send_message("❌ Save potion already used.", ephemeral=True)
        vals = interaction.data.get("values") or []
        if not vals:
            return await interaction.response.send_message("❌ Select a target first.", ephemeral=True)
        target_id = int(vals[0])
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "witch_save", target_id)
        db_set_witch_use(interaction.guild_id, self.actor_id, save=1)
        target = interaction.guild.get_member(target_id)
        actor  = interaction.guild.get_member(self.actor_id)
        await interaction.response.edit_message(
            content=fmt(f"✅ Save potion used on {target.display_name if target else target_id}."), view=None)
        await post_mod_log(interaction.guild,
            f"🧙 **Witch** — Night {night_num}\n"
            f"**{actor.display_name}** used **SAVE potion** on **{target.display_name if target else target_id}**")

    async def on_kill(self, interaction):
        _, used_kill = db_get_witch_uses(interaction.guild_id, self.actor_id)
        if used_kill:
            return await interaction.response.send_message("❌ Kill potion already used.", ephemeral=True)
        vals = interaction.data.get("values") or []
        if not vals:
            return await interaction.response.send_message("❌ Select a target first.", ephemeral=True)
        target_id = int(vals[0])
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "witch_kill", target_id)
        db_set_witch_use(interaction.guild_id, self.actor_id, kill=1)
        target = interaction.guild.get_member(target_id)
        actor  = interaction.guild.get_member(self.actor_id)
        await interaction.response.edit_message(
            content=fmt(f"✅ Poison potion used on {target.display_name if target else target_id}."), view=None)
        await post_mod_log(interaction.guild,
            f"🧙 **Witch** — Night {night_num}\n"
            f"**{actor.display_name}** used **POISON potion** on **{target.display_name if target else target_id}**")

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "witch_skip", None)
        actor = interaction.guild.get_member(self.actor_id)
        await interaction.response.edit_message(content=fmt("✅ You chose to skip this night."), view=None)
        await post_mod_log(interaction.guild,
            f"🧙 **Witch** — Night {night_num}\n**{actor.display_name}** chose to **SKIP** tonight.")


# ── Huntsman ──────────────────────────────────────────────────────────────
class HuntsmanView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        opts = self._player_options()
        if opts:
            sel = Select(placeholder="🏹 Choose a player to protect", options=opts)
            sel.callback = self.on_select
            self.add_item(sel)
        skip = Button(label="Don't Protect Anyone", style=discord.ButtonStyle.secondary)
        skip.callback = self.on_skip
        self.add_item(skip)

    async def on_select(self, interaction):
        target_id   = int(interaction.data["values"][0])
        target      = interaction.guild.get_member(target_id)
        target_name = target.display_name if target else str(target_id)
        await self._save_and_close(interaction, "huntsman", target_id,
            f"Protecting {target_name} tonight.")

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "huntsman_skip", None)
        actor = interaction.guild.get_member(self.actor_id)
        await interaction.response.edit_message(content=fmt("✅ You chose not to protect anyone tonight."), view=None)
        await post_mod_log(interaction.guild,
            f"🏹 **Huntsman** — Night {night_num}\n**{actor.display_name}** chose not to protect anyone.")


# ── Medium ────────────────────────────────────────────────────────────────
class MediumView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        opts = self._player_options()
        if opts:
            sel = Select(placeholder="🌀 Choose a player to check alignment", options=opts)
            sel.callback = self.on_select
            self.add_item(sel)

    async def on_select(self, interaction):
        target_id  = int(interaction.data["values"][0])
        guild      = interaction.guild
        guild_id   = interaction.guild_id
        night_num  = db_get_night_num(guild_id)
        db_save_night_action(guild_id, night_num, self.actor_id, "medium", target_id)

        rows        = db_get_assignments(guild_id)
        target_row  = next((r for r in rows if r[0] == target_id), None)
        target_role = target_row[1] if target_row else "Unknown"
        target_m    = guild.get_member(target_id)
        target_name = target_m.display_name if target_m else str(target_id)

        # Medium sees Good/Bad/Neutral — no role name ever revealed
        # Special cases per role description
        APPEARS_GOOD = {"Elite Alpha", "Blessed Wolf", "Werekitten", "Cursed", "Traitor"}
        if target_role in APPEARS_GOOD:
            alignment = "✅ Good"
            color     = 0x27AE60
        elif get_team(guild_id, target_role) == "wolf":
            alignment = "❌ Bad"
            color     = 0xC0392B
        elif get_team(guild_id, target_role) == "neutral":
            alignment = "⚖️ Neutral"
            color     = 0xF39C12
        else:
            alignment = "✅ Good"
            color     = 0x27AE60

        # Mod-log — result queued, NOT delivered yet
        actor = guild.get_member(self.actor_id)
        await post_mod_log(guild,
            f"🌀 **Medium Check queued** — Night {night_num}\n"
            f"**Medium:** {actor.display_name if actor else self.actor_id}\n"
            f"**Target:** {target_name} ({target_role})\n"
            f"**Result queued:** {alignment} — will deliver when mod clicks Deliver Results.")

        await interaction.response.edit_message(
            content=fmt(f"✅ Alignment check submitted on {target_name}.\nYour result will be delivered when night resolves."),
            view=None)


# ── Hermit ────────────────────────────────────────────────────────────────
class HermitView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel  = Select(placeholder="🏚️ Choose a player to hide (optional)", options=self._player_options())
        skip = Button(label="Don't Hide Anyone", style=discord.ButtonStyle.secondary)
        sel.callback  = self.on_select
        skip.callback = self.on_skip
        self.add_item(sel)
        self.add_item(skip)

    async def on_select(self, interaction):
        target_id = int(interaction.data["values"][0])
        await self._save_and_close(interaction, "hermit", target_id,
            f"You will hide {getattr(interaction.guild.get_member(target_id), "display_name", str(target_id))} if they top the vote.")

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "hermit_skip", None)
        actor = interaction.guild.get_member(self.actor_id)
        await interaction.response.edit_message(content=fmt("✅ You chose not to hide anyone this round."), view=None)
        await post_mod_log(interaction.guild,
            f"🏚️ **Hermit** — Night {night_num}\n**{actor.display_name}** chose not to hide anyone.")


# ── Agitator ──────────────────────────────────────────────────────────────
class AgitatorView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        uses = db_get_ability_uses(self.guild_id, self.actor_id)
        if uses is None or uses <= 0:
            btn = Button(label="📢 Frenzy already used (0/1 remaining)",
                         style=discord.ButtonStyle.secondary, disabled=True)
            self.add_item(btn)
            return
        use_btn  = Button(label="📢 Use Frenzy Ability (1/1 remaining)", style=discord.ButtonStyle.danger)
        skip_btn = Button(label="Save It For Later",                     style=discord.ButtonStyle.secondary)
        use_btn.callback  = self.on_use
        skip_btn.callback = self.on_skip
        self.add_item(use_btn)
        self.add_item(skip_btn)

    async def on_use(self, interaction):
        uses = db_get_ability_uses(interaction.guild_id, self.actor_id)
        if uses is None or uses <= 0:
            return await interaction.response.send_message(
                "❌ You have already used your frenzy ability.", ephemeral=True)
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "agitator_frenzy", None)
        db_deduct_ability_uses(interaction.guild_id, self.actor_id, 1)
        actor = interaction.guild.get_member(self.actor_id)
        await interaction.response.edit_message(
            content=fmt("✅ Frenzy used! The village will be notified tomorrow that two votes are required.\n"
                        "This was your only frenzy for the game."),
            view=None)
        await post_mod_log(interaction.guild,
            f"📢 **Agitator** — Night {night_num}\n"
            f"**{actor.display_name}** has used their **FRENZY** ability! "
            f"The village will require TWO lynches tomorrow. Post announcement at morning blood board.")

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "agitator_skip", None)
        await interaction.response.edit_message(content=fmt("✅ Ability saved for another night."), view=None)


class GovernorPardonView(View):
    """Posted in village chat when a governor wants to pardon — day action only."""
    def __init__(self, guild_id, governor_id, alive_players, npc_map):
        super().__init__(timeout=600)
        self.guild_id     = guild_id
        self.governor_id  = governor_id

        options = []
        for p in alive_players:
            if p.id == governor_id:
                continue
            name = npc_map.get(p.id, p.display_name)
            options.append(discord.SelectOption(label=name[:100], value=str(p.id)))

        if options:
            sel = Select(placeholder="🎖️ Choose a player to pardon from today's vote...", options=options[:25])
            sel.callback = self.on_pardon
            self.add_item(sel)

        cancel = Button(label="Cancel", style=discord.ButtonStyle.secondary)
        cancel.callback = self.on_cancel
        self.add_item(cancel)

    async def on_pardon(self, interaction: discord.Interaction):
        if interaction.user.id != self.governor_id:
            return await interaction.response.send_message(
                "❌ Only the Governor can use this.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        target_id = int(interaction.data["values"][0])
        target    = interaction.guild.get_member(target_id)
        tname     = target.display_name if target else str(target_id)
        governor  = interaction.guild.get_member(self.governor_id)
        gname     = governor.display_name if governor else str(self.governor_id)

        night_num = db_get_night_num(self.guild_id)
        db_save_night_action(self.guild_id, night_num, self.governor_id, "governor_pardon", target_id)

        await post_mod_log(interaction.guild,
            f"🎖️ **Governor Pardon** — Day {night_num}\n"
            f"**{gname}** has pardoned **{tname}** from today's vote.\n"
            f"*{tname} cannot be eliminated today.*")

        await interaction.followup.send(
            fmt(f"🎖️ **{tname}** has been pardoned from today's vote.\n"
                f"Mod has been notified. {tname} cannot be eliminated today."),
            ephemeral=True)

    async def on_cancel(self, interaction: discord.Interaction):
        if interaction.user.id != self.governor_id:
            return await interaction.response.send_message("❌ Only the Governor can use this.", ephemeral=True)
        await interaction.response.edit_message(content="Pardon cancelled.", view=None)

# ── Clone ─────────────────────────────────────────────────────────────────
class CloneView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        opts = self._player_options()
        if opts:
            sel = Select(placeholder="🪞 Choose the player to clone", options=opts)
            sel.callback = self.on_select
            self.add_item(sel)

    async def on_select(self, interaction):
        target_id = int(interaction.data["values"][0])
        await self._save_and_close(interaction, "clone", target_id,
            f"You are now cloning {getattr(interaction.guild.get_member(target_id), "display_name", str(target_id))}. If they die, you inherit their role.")


# ── Shapeshifter ──────────────────────────────────────────────────────────
class ShapeshifterView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        opts = self._player_options()
        if opts:
            sel = Select(placeholder="🎭 Choose a player to shapeshift into", options=opts)
            sel.callback = self.on_select
            self.add_item(sel)

    async def on_select(self, interaction):
        target_id  = int(interaction.data["values"][0])
        target_m   = interaction.guild.get_member(target_id)
        target_name = target_m.display_name if target_m else str(target_id)
        await self._save_and_close(interaction, "shapeshifter", target_id,
            fmt(f"🎭 You have chosen **{target_name}**.\n"
                f"When night resolves you will permanently become their role.\n"
                f"The mod will confirm your new role in this channel."))



# ── Cupid ─────────────────────────────────────────────────────────────────
class CupidView(BaseNightView):
    """Cupid selects two players to bind each night. Bond expires at morning."""
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        # Two dropdowns — one for each player (Cupid can include themselves)
        opts = self._all_options()
        sel1 = Select(placeholder="💘 First player to bind",  options=opts,
                      custom_id=f"cupid_1_{self.actor_id}")
        sel2 = Select(placeholder="💘 Second player to bind", options=opts,
                      custom_id=f"cupid_2_{self.actor_id}")
        sub  = Button(label="💘 Bind These Two", style=discord.ButtonStyle.danger)
        skip = Button(label="Don't Bind Anyone", style=discord.ButtonStyle.secondary)
        sel1.callback = self._noop
        sel2.callback = self._noop
        sub.callback  = self.on_submit
        skip.callback = self.on_skip
        self._sel1 = sel1
        self._sel2 = sel2
        self.add_item(sel1)
        self.add_item(sel2)
        self.add_item(sub)
        self.add_item(skip)

    async def _noop(self, interaction): await interaction.response.defer()

    async def on_submit(self, interaction):
        vals1 = self._sel1.values
        vals2 = self._sel2.values
        if not vals1 or not vals2:
            return await interaction.response.send_message(
                "❌ Select both players first.", ephemeral=True)
        p1, p2 = int(vals1[0]), int(vals2[0])
        if p1 == p2:
            return await interaction.response.send_message(
                "❌ Cannot bind a player to themselves.", ephemeral=True)
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "cupid_bind", p1)
        db_set_cupid_current(interaction.guild_id, p1, p2)
        m1 = interaction.guild.get_member(p1)
        m2 = interaction.guild.get_member(p2)
        n1 = m1.display_name if m1 else str(p1)
        n2 = m2.display_name if m2 else str(p2)
        # Targets are NOT notified — mod-log only (game of deception)
        await interaction.response.edit_message(
            content=fmt(f"💘 Bound {n1} and {n2} tonight.\nIf either dies, the other follows.\nBond expires at dawn."),
            view=None)
        await post_mod_log(interaction.guild,
            f"💘 **Cupid Bond — Night {night_num}**\n"
            f"**Bound:** {n1} ↔ {n2}\n"
            f"Bond expires at morning resolution.")

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "cupid_skip", None)
        db_clear_cupid_current(interaction.guild_id)
        await interaction.response.edit_message(
            content=fmt("💘 No bind tonight. Any previous bond has been cleared."),
            view=None)
        await post_mod_log(interaction.guild,
            f"💘 **Cupid** — Night {night_num}\n"
            f"Chose not to bind anyone. Previous bond cleared.")

# ── Wolf Pup ──────────────────────────────────────────────────────────────
class WolfPupView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        opts = self._player_options()
        if opts:
            sel = Select(placeholder="🐾 Choose a player to block", options=opts)
            sel.callback = self.on_select
            self.add_item(sel)

    async def on_select(self, interaction):
        target_id = int(interaction.data["values"][0])
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "wolf_pup", target_id)
        db_record_block(interaction.guild_id, night_num, self.actor_id, target_id)
        target = interaction.guild.get_member(target_id)
        actor  = interaction.guild.get_member(self.actor_id)
        await interaction.response.edit_message(
            content=fmt(f"✅ Blocking {target.display_name if target else target_id} tonight."), view=None)
        await post_mod_log(interaction.guild,
            f"🐾 **Wolf Pup Block** — Night {night_num}\n"
            f"**{actor.display_name}** is blocking **{target.display_name if target else target_id}** "
            f"— their ability is suppressed this night.")


# ── Alpha ─────────────────────────────────────────────────────────────────


async def _safe_edit(interaction: discord.Interaction, content: str, embed=None, view=None):
    """Edit interaction message safely — falls back to followup if token expired."""
    try:
        if not interaction.response.is_done():
            await interaction.response.edit_message(content=content, embed=embed, view=view)
        else:
            await interaction.edit_original_response(content=content, embed=embed, view=view)
    except discord.errors.NotFound:
        # Token expired — just send a followup ephemeral
        try:
            await interaction.followup.send(content, ephemeral=True)
        except Exception:
            pass
    except Exception as e:
        print(f"[_safe_edit] {e}")

class TurnResultView(View):
    """Posted to mod-log when Alpha/Elite Alpha attempts a turn — mod decides outcome."""
    def __init__(self, guild_id, night_num, actor_id, target_id, actor_name, target_name, role_name):
        super().__init__(timeout=3600)
        self.guild_id    = guild_id
        self.night_num   = night_num
        self.actor_id    = actor_id
        self.target_id   = target_id
        self.actor_name  = actor_name
        self.target_name = target_name
        self.role_name   = role_name

        success_btn = Button(label="✅ Turn Succeeded", style=discord.ButtonStyle.green)
        fail_btn    = Button(label="❌ Turn Failed",    style=discord.ButtonStyle.danger)
        success_btn.callback = self.on_success
        fail_btn.callback    = self.on_fail
        self.add_item(success_btn)
        self.add_item(fail_btn)

    async def on_success(self, interaction: discord.Interaction):
        # Defer immediately — this does channel rename, sleep, DB writes, den access, mod-log
        await interaction.response.defer(ephemeral=True)
        db_record_turn(self.guild_id, self.night_num, self.actor_id, self.target_id, "success")
        guild  = interaction.guild
        state  = db_get_state(self.guild_id) or {}
        rows   = db_get_assignments(self.guild_id)
        target = guild.get_member(self.target_id)

        # ── Build wolf role pool from current game pool only ─────────────
        TURN_EXCLUDE = {"Alpha", "Elite Alpha"}
        last_roles   = db_get_last_roles(self.guild_id)
        if last_roles:
            wolf_pool = [name for name, count in last_roles.items()
                         if get_team(self.guild_id, name) == "wolf"
                         and name not in TURN_EXCLUDE]
        else:
            all_roles = cached_load_roles(self.guild_id)
            wolf_pool = [r["name"] for r in all_roles
                         if r["team"] == "wolf" and r["name"] not in TURN_EXCLUDE]

        if wolf_pool:
            import random as _random
            new_role = _random.choice(wolf_pool)
            role_info = get_role_info(self.guild_id, new_role)
        else:
            new_role  = None
            role_info = None

        # ── Update DB with new role ───────────────────────────────────────
        if new_role:
            conn = sqlite3.connect(DB_FILE)
            c    = conn.cursor()
            c.execute("UPDATE player_assignments SET role_name=? WHERE guild_id=? AND player_id=?",
                      (new_role, self.guild_id, self.target_id))
            conn.commit()
            conn.close()
            invalidate_cache(self.guild_id)

        # ── Grant wolf den access ─────────────────────────────────────────
        wolf_ch = guild.get_channel(state.get("wolf_channel_id") or 0)
        if wolf_ch and target:
            await wolf_ch.set_permissions(target, view_channel=True, send_messages=True)
            await wolf_ch.send(fmt(
                f"🐺 **{self.target_name}** has joined the pack — turned by the {self.role_name}!\n"
                f"Welcome them to the den."))

        # ── Update win tracker ────────────────────────────────────────────
        await refresh_win_tracker(guild)

        # ── Get updated counts for mod notification ───────────────────────
        updated_rows  = db_get_assignments(self.guild_id)
        village_count = sum(1 for r in updated_rows if r[2] == 1 and get_team(self.guild_id, r[1]) == "village")
        wolf_count    = sum(1 for r in updated_rows if r[2] == 1 and get_team(self.guild_id, r[1]) == "wolf")
        neutral_count = sum(1 for r in updated_rows if r[2] == 1 and get_team(self.guild_id, r[1]) == "neutral")

        # ── Private channel — wheel spin result ──────────────────────────
        tgt_row = next((r for r in rows if r[0] == self.target_id), None)
        if tgt_row and tgt_row[3]:
            priv_ch = guild.get_channel(tgt_row[3])
            if priv_ch:
                if new_role:
                    # Rename channel to reflect new role
                    try:
                        new_ch_name = f"🔒{self.target_name}-{new_role}".lower().replace(" ", "-")[:100]
                        await priv_ch.edit(name=new_ch_name)
                    except Exception as e:
                        print(f"[TurnResult] channel rename error: {e}")

                    # Send wheel spin animation
                    wheel_lines = "\n".join(f"🎡 {r}" for r in wolf_pool)
                    spin_msg = await priv_ch.send(fmt(
                        f"🐺 Something has shifted in the darkness.\n"
                        f"You are no longer who you were.\n\n"
                        f"The wheel spins...\n{wheel_lines}"))
                    await asyncio.sleep(2)
                    embed = build_role_card(target, new_role, role_info,
                                           get_guild_font(self.guild_id))
                    await spin_msg.edit(content=fmt(
                        f"🐺 The wheel has spoken.\n"
                        f"You are now: **{new_role}**"))
                    turn_msg = await priv_ch.send(
                        f"Welcome to the pack, {target.mention}",
                        embed=embed)
                    try:
                        await turn_msg.pin()
                    except Exception:
                        pass
                else:
                    await priv_ch.send(fmt(
                        f"🐺 Something has shifted in the darkness.\n"
                        f"You are no longer who you were.\n"
                        f"The mod will assign your new role shortly."))

        if new_role:
            await _safe_edit(interaction,
                content=(
                    f"✅ **{self.target_name}** turned — new wolf role assigned.\n"
                    f"Den access granted. Channel renamed.\n\n"
                    f"**Updated counts:**\n"
                    f"🏘️ Village: {village_count}  🐺 Wolves: {wolf_count}  ⚖️ Neutrals: {neutral_count}"))
        else:
            await _safe_edit(interaction,
                content=(
                    f"⚠️ **{self.target_name}** turned but no valid wolf roles found in pool.\n"
                    f"Use `/turn_player` to assign their wolf role manually.\n\n"
                    f"**Updated counts:**\n"
                    f"🏘️ Village: {village_count}  🐺 Wolves: {wolf_count}  ⚖️ Neutrals: {neutral_count}"))

        # Remind mod that investigations now reflect the turn
        actions_check = db_get_night_actions(self.guild_id, self.night_num)
        has_invest = any(a[1] in ("seer", "medium", "bloodhound") for a in actions_check)
        if has_invest:
            await post_mod_log(interaction.guild,
                f"⚠️ **Turn confirmed — {self.target_name} is now a wolf in the DB.**\n"
                f"Seer / Medium / Bloodhound results this night will now correctly show them as wolf.\n"
                f"**Deliver investigations now if you haven't already.**")

        safe_task(update_mod_dashboard(interaction.guild), "dashboard_turn_success")

    async def on_fail(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        db_record_turn(self.guild_id, self.night_num, self.actor_id, self.target_id, "failed")
        await _safe_edit(interaction,
            content=f"❌ Turn attempt on **{self.target_name}** failed — no change.")
        safe_task(update_mod_dashboard(interaction.guild), "dashboard_turn_fail")


class AlphaView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel  = Select(placeholder="👑 Choose a villager to turn", options=self._player_options())
        skip = Button(label="No Turn This Night", style=discord.ButtonStyle.secondary)
        sel.callback  = self.on_select
        skip.callback = self.on_skip
        self.add_item(sel)
        self.add_item(skip)

    async def on_select(self, interaction):
        target_id   = int(interaction.data["values"][0])
        night_num   = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "alpha", target_id)
        db_record_turn(interaction.guild_id, night_num, self.actor_id, target_id, "pending")
        target      = interaction.guild.get_member(target_id)
        actor       = interaction.guild.get_member(self.actor_id)
        actor_name  = actor.display_name  if actor  else str(self.actor_id)
        target_name = target.display_name if target else str(target_id)

        # Post to mod-log with Success/Fail buttons
        view  = TurnResultView(
            interaction.guild_id, night_num,
            self.actor_id, target_id,
            actor_name, target_name, "Alpha")
        embed = discord.Embed(
            title       = f"👑 Alpha Turn Attempt — Night {night_num}",
            description = f"**{actor_name}** is attempting to turn **{target_name}**\nDid it succeed?",
            color       = 0xC0392B
        )
        await post_mod_log(interaction.guild, embed=embed)
        state_mod = db_get_state(interaction.guild_id) or {}
        mod_ch    = interaction.guild.get_channel(state_mod.get("mod_log_channel_id") or 0)
        if mod_ch:
            await mod_ch.send(view=view)

        await interaction.response.edit_message(
            content=fmt(f"✅ Turn attempt submitted on {target_name}.\nMod has been notified."),
            view=None)

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "alpha_skip", None)
        await interaction.response.edit_message(content=fmt("✅ No turn this night."), view=None)


# ── Elite Alpha ───────────────────────────────────────────────────────────
class EliteAlphaView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel  = Select(placeholder="👑⭐ Choose a villager to turn", options=self._player_options())
        skip = Button(label="No Turn This Night", style=discord.ButtonStyle.secondary)
        sel.callback  = self.on_select
        skip.callback = self.on_skip
        self.add_item(sel)
        self.add_item(skip)

    async def on_select(self, interaction):
        target_id   = int(interaction.data["values"][0])
        night_num   = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "elite_alpha", target_id)
        db_record_turn(interaction.guild_id, night_num, self.actor_id, target_id, "pending")
        target      = interaction.guild.get_member(target_id)
        actor       = interaction.guild.get_member(self.actor_id)
        actor_name  = actor.display_name  if actor  else str(self.actor_id)
        target_name = target.display_name if target else str(target_id)

        view  = TurnResultView(
            interaction.guild_id, night_num,
            self.actor_id, target_id,
            actor_name, target_name, "Elite Alpha")
        embed = discord.Embed(
            title       = f"👑⭐ Elite Alpha Turn Attempt — Night {night_num}",
            description = f"**{actor_name}** is attempting to turn **{target_name}**\nDid it succeed?",
            color       = 0xC0392B
        )
        await post_mod_log(interaction.guild, embed=embed)
        state_mod = db_get_state(interaction.guild_id) or {}
        mod_ch    = interaction.guild.get_channel(state_mod.get("mod_log_channel_id") or 0)
        if mod_ch:
            await mod_ch.send(view=view)

        await interaction.response.edit_message(
            content=fmt(f"✅ Turn attempt submitted on {target_name}.\nMod has been notified."),
            view=None)

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "elite_alpha_skip", None)
        await interaction.response.edit_message(content=fmt("✅ No turn this night."), view=None)


# ── Bloodhound ────────────────────────────────────────────────────────────
class BloodhoundView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        # Include self in options so Bloodhound can scan themselves
        sel = Select(placeholder="🦴 Choose a player to identify", options=self._all_options())
        sel.callback = self.on_select
        self.add_item(sel)

    async def on_select(self, interaction):
        target_id  = int(interaction.data["values"][0])
        guild      = interaction.guild
        guild_id   = interaction.guild_id
        night_num  = db_get_night_num(guild_id)

        # Save the action
        db_save_night_action(guild_id, night_num, self.actor_id, "bloodhound", target_id)

        # Determine target's exact role — check player_assignments first, then NPC table
        target_role = "Unknown"
        rows = db_get_assignments(guild_id)
        row  = next((r for r in rows if r[0] == target_id), None)
        if row:
            target_role = row[1]
        else:
            # Could be an NPC
            npcs = db_get_npcs(guild_id)
            npc_match = next((n for n in npcs if n["npc_id"] == target_id), None)
            if npc_match:
                target_role = npc_match["role_name"]

        # Get target display name — handle self-scan and NPCs
        target_member = guild.get_member(target_id)
        npcs          = db_get_npcs(guild_id)
        npc_match     = next((n for n in npcs if n["npc_id"] == target_id), None)
        if npc_match:
            target_name = npc_match["name"]
        elif target_member:
            target_name = target_member.display_name
        else:
            target_name = str(target_id)

        is_self = (target_id == self.actor_id)

        # ── Queue result — deliver when mod clicks Deliver Results ─────────
        actor = guild.get_member(self.actor_id)
        await post_mod_log(guild,
            f"🦴 **Bloodhound scan queued** — Night {night_num}\n"
            f"**{actor.display_name if actor else self.actor_id}** scanned **{target_name}**\n"
            f"**Result queued:** {target_role} — will deliver when mod clicks Deliver Results.")

        await interaction.response.edit_message(
            content=fmt(f"✅ Scan submitted on {target_name}.\nYour result will be delivered when night resolves."),
            view=None)


# ── Bloodletter ───────────────────────────────────────────────────────────
class BloodletterView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel  = Select(placeholder="🩸 Choose a player to mark", options=self._player_options())
        skip = Button(label="Don't Mark Anyone", style=discord.ButtonStyle.secondary)
        sel.callback  = self.on_select
        skip.callback = self.on_skip
        self.add_item(sel)
        self.add_item(skip)

    async def on_select(self, interaction):
        target_id  = int(interaction.data["values"][0])
        target     = interaction.guild.get_member(target_id)
        target_name = target.display_name if target else str(target_id)
        await self._save_and_close(interaction, "bloodletter", target_id,
            f"{target_name} marked — they appear as wolf to checks for 2 nights.")

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "bloodletter_skip", None)
        await interaction.response.edit_message(content=fmt("✅ No mark this night."), view=None)


# ── Crazed Wolf ───────────────────────────────────────────────────────────
class CrazedWolfView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        night_num = db_get_night_num(self.guild_id)
        if night_num < 3:
            wait_btn = Button(
                label=f"🌪️ Double kill unlocks Night 3 (currently Night {night_num})",
                style=discord.ButtonStyle.secondary, disabled=True)
            self.add_item(wait_btn)
            return
        sel1 = Select(placeholder="🌪️ First kill target",  options=self._player_options())
        sel2 = Select(placeholder="🌪️ Second kill target", options=self._player_options())
        sub  = Button(label="Submit Both Kills", style=discord.ButtonStyle.danger)
        sel1.callback = self._noop
        sel2.callback = self._noop
        sub.callback  = self.on_submit
        self.add_item(sel1)
        self.add_item(sel2)
        self.add_item(sub)
        self._sel1 = sel1
        self._sel2 = sel2

    async def _noop(self, interaction): await interaction.response.defer()

    async def on_submit(self, interaction):
        vals1 = self._sel1.values
        vals2 = self._sel2.values
        if not vals1 or not vals2:
            return await interaction.response.send_message("❌ Select both targets first.", ephemeral=True)
        t1, t2 = int(vals1[0]), int(vals2[0])
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "crazed_wolf_1", t1)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "crazed_wolf_2", t2)
        actor  = interaction.guild.get_member(self.actor_id)
        tname1 = getattr(interaction.guild.get_member(t1), "display_name", str(t1))
        tname2 = getattr(interaction.guild.get_member(t2), "display_name", str(t2))
        await interaction.response.edit_message(
            content=fmt(f"✅ Double kill submitted: {tname1} and {tname2}."), view=None)
        await post_mod_log(interaction.guild,
            f"🌪️ **Crazed Wolf Double Kill** — Night {night_num}\n"
            f"**{actor.display_name}** targeting: **{tname1}** then **{tname2}**")


# ── Dire Wolf ─────────────────────────────────────────────────────────────
class DireWolfView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        opts = self._player_options()
        if opts:
            sel = Select(placeholder="💔 Choose your secret mate (first night only)", options=opts)
            sel.callback = self.on_select
            self.add_item(sel)

    async def on_select(self, interaction):
        target_id = int(interaction.data["values"][0])
        await self._save_and_close(interaction, "dire_wolf", target_id,
            "You have chosen your secret mate. If they die, you die too. Do NOT reveal this to your den.")



# ── Wraith ────────────────────────────────────────────────────────────────
class WraithMarkView(BaseNightView):
    """Wraith marks a player OR issues the Kill Command."""
    def __init__(self, guild_id, actor_id, role_name, alive_players):
        super().__init__(guild_id, actor_id, role_name, alive_players)
        ws      = db_get_wraith_state(guild_id)
        partner = ws.get("wraith1_id") if ws.get("wraith2_id") == actor_id else ws.get("wraith2_id")
        partner_alive = any(r[0] == partner and r[2] == 1
                            for r in db_get_assignments(guild_id)) if partner else False
        both_alive = partner_alive

        # Mark dropdown
        opts = self._player_options()
        if opts:
            sel = Select(placeholder="👻 Mark a player tonight...", options=opts)
            sel.callback = self.on_mark
            self.add_item(sel)

        # Kill Command button — only if at least one mark exists and partner agreed or solo
        marks = db_get_wraith_marks(guild_id)
        if marks:
            if both_alive:
                # Both alive — need mutual agreement via den
                kill_btn = Button(
                    label  = f"💀 Signal Kill Command ({len(marks)} marked)",
                    style  = discord.ButtonStyle.danger)
                kill_btn.callback = self.on_signal_kill
                self.add_item(kill_btn)
            else:
                # Solo — can kill directly
                kill_btn = Button(
                    label  = f"💀 Issue Kill Command ({len(marks)} marked)",
                    style  = discord.ButtonStyle.danger)
                kill_btn.callback = self.on_kill
                self.add_item(kill_btn)

    async def on_mark(self, interaction: discord.Interaction):
        target_id = int(interaction.data["values"][0])
        night_num = db_get_night_num(interaction.guild_id)
        target    = interaction.guild.get_member(target_id)
        tname     = target.display_name if target else str(target_id)

        # Check not already marking this same target
        marks = db_get_wraith_marks(interaction.guild_id)
        already = next((m for m in marks if m[1] == target_id), None)
        if already:
            return await interaction.response.send_message(
                fmt(f"❌ **{tname}** is already marked by your partner. Choose a different target."),
                ephemeral=True)

        db_set_wraith_mark(interaction.guild_id, self.actor_id, target_id, night_num)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "wraith_mark", target_id)

        # Notify partner in wraith den
        ws     = db_get_wraith_state(interaction.guild_id)
        den_ch = interaction.guild.get_channel(ws.get("den_channel_id") or 0)
        if den_ch:
            await den_ch.send(
                fmt(f"👻 **Mark placed on {tname}** — Night {night_num}\n"

                    f"Total marks active: {len(db_get_wraith_marks(interaction.guild_id))}"))

        await self._save_and_close(interaction, "wraith_mark", target_id,
            fmt(f"👻 You have marked **{tname}**. They will not know.\n"

                f"The Blood Board will hint that something moved through Whisperfall tonight."))

    async def on_signal_kill(self, interaction: discord.Interaction):
        """Signal to partner that this Wraith is ready to issue Kill Command."""
        ws     = db_get_wraith_state(interaction.guild_id)
        den_ch = interaction.guild.get_channel(ws.get("den_channel_id") or 0)
        marks  = db_get_wraith_marks(interaction.guild_id)
        marked_names = []
        for _, tid, _ in marks:
            m = interaction.guild.get_member(tid)
            marked_names.append(m.display_name if m else str(tid))

        if den_ch:
            await den_ch.send(
                fmt(f"💀 **{interaction.user.display_name} signals: Ready to issue Kill Command**\n"

                    f"Marked targets ({len(marks)}): {', '.join(marked_names)}\n"

                    f"*If your partner also confirms, the Kill Command fires tonight.*"))

        # Save agreement signal
        actor_key = "wraith1_id" if ws.get("wraith1_id") == self.actor_id else "wraith2_id"
        db_set_wraith_state(interaction.guild_id, kill_agreed=1)

        # Check if partner already signaled — if so both agree, fire it
        ws2 = db_get_wraith_state(interaction.guild_id)
        if ws2.get("kill_agreed", 0) >= 1:
            night_num_k = db_get_night_num(interaction.guild_id)
            db_set_wraith_state(interaction.guild_id, kill_agreed=2, kill_night=night_num_k)
            db_save_night_action(interaction.guild_id, night_num_k, self.actor_id, "wraith_kill", 0)
            marks_k = db_get_wraith_marks(interaction.guild_id)
            marked_names_k = []
            for _, tid, _ in marks_k:
                mk = interaction.guild.get_member(tid)
                marked_names_k.append(mk.display_name if mk else str(tid))
            if den_ch:
                await den_ch.send(fmt(
                    f"💀 **KILL COMMAND CONFIRMED — Both Wraiths agreed**\n"
                    f"Targets: {', '.join(marked_names_k)}\n"
                    f"*The Kill Command will execute when night resolves.*"))
            await interaction.response.send_message(
                fmt(f"💀 Both Wraiths have agreed. Kill Command fires tonight.\n"
                    f"Targets: {', '.join(marked_names_k)}"),
                ephemeral=True)
        else:
            await interaction.response.send_message(
                fmt(f"💀 Kill Command signaled. Waiting for your partner to confirm in the den.\n"
                    f"If both agree before night ends, all {len(marks)} marked player(s) will die."),
                ephemeral=True)

    async def on_kill(self, interaction: discord.Interaction):
        """Solo Wraith issues Kill Command."""
        marks = db_get_wraith_marks(interaction.guild_id)
        if not marks:
            return await interaction.response.send_message(
                fmt("❌ No players are currently marked."), ephemeral=True)
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "wraith_kill", 0)
        db_set_wraith_state(interaction.guild_id, kill_agreed=2, kill_night=night_num)

        marked_names = []
        for _, tid, _ in marks:
            m = interaction.guild.get_member(tid)
            marked_names.append(m.display_name if m else str(tid))

        await post_mod_log(interaction.guild,
            f"💀 **Wraith Kill Command issued (solo)** — Night {night_num}\n"

            f"Targets: {', '.join(marked_names)}\n"

            f"*Resolve after all other night actions. Doctor/Surgeon saves do NOT apply.*")

        await self._save_and_close(interaction, "wraith_kill", 0,
            fmt(f"💀 Kill Command issued. The marked will fall tonight.\n"

                f"Targets: {', '.join(marked_names)}\n"

                f"*You cannot act again this game.*"))

# ── Echo-Stalker ──────────────────────────────────────────────────────────
class EchoStalkerView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel  = Select(placeholder="👁️ Choose a player to haunt", options=self._player_options())
        skip = Button(label="Kill Instead (Regular Wolf Vote)", style=discord.ButtonStyle.secondary)
        sel.callback  = self.on_select
        skip.callback = self.on_skip
        self.add_item(sel)
        self.add_item(skip)

    async def on_select(self, interaction):
        target_id = int(interaction.data["values"][0])
        await self._save_and_close(interaction, "echo_stalker", target_id,
            f"Haunting {getattr(interaction.guild.get_member(target_id), "display_name", str(target_id))} — their vote tomorrow is yours to control.")

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "echo_stalker_skip", None)
        await interaction.response.edit_message(content=fmt("✅ Using regular wolf kill vote instead."), view=None)


# ── Shadow Wolf ───────────────────────────────────────────────────────────
class ShadowWolfView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        # Build options from kill list if available, otherwise all alive players
        data = db_get_shadow_wolf_list(self.guild_id)
        if data and data["targets"]:
            alive_targets = [t for t in data["targets"] if t["status"] == "alive"]
            if alive_targets:
                options = [discord.SelectOption(
                    label=t["name"], value=str(t["pid"]),
                    description="On your kill list"
                ) for t in alive_targets[:25]]
            else:
                options = [discord.SelectOption(label="No targets remaining", value="none")]
        else:
            options = self._player_options()
        sel = Select(placeholder="🌑 Choose a target to kill tonight", options=options)
        sel.callback = self.on_select
        self.add_item(sel)

    async def on_select(self, interaction):
        raw = interaction.data["values"][0]
        if raw == "none":
            return await interaction.response.send_message(
                "❌ No targets remaining on your kill list.", ephemeral=True)
        target_id = int(raw)

        # Validate target is on kill list if list exists
        data = db_get_shadow_wolf_list(interaction.guild_id)
        if data and data["targets"]:
            on_list = any(t["pid"] == target_id and t["status"] == "alive" for t in data["targets"])
            if not on_list:
                return await interaction.response.send_message(
                    "❌ That player is not on your kill list or is already dead.", ephemeral=True)

        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "shadow_wolf", target_id)

        target = interaction.guild.get_member(target_id)
        tname  = target.display_name if target else str(target_id)
        actor  = interaction.guild.get_member(self.actor_id)

        await post_mod_log(interaction.guild,
            f"🌑 **Shadow Wolf** action — Night {night_num}\n"
            f"**{actor.display_name if actor else self.actor_id}** targeting **{tname}** (kill list)")

        # Mark as killed by Shadow Wolf on the list
        safe_task(_sw_mark_dead(interaction.guild, interaction.guild_id, target_id, "sw"), "sw_mark_dead")

        await interaction.response.edit_message(
            content=fmt(f"✅ Kill submitted on {tname} (post-death ability).\nYour kill list has been updated."),
            view=None)


# ── Werekitten ────────────────────────────────────────────────────────────
class WerekittenKillView(BaseNightView):
    """
    Werekitten submits a private kill target that REPLACES the den kill for that night.
    The den still discusses and votes normally, but the Werekitten's choice overrides it.
    Bypasses Sheriff and Huntsman — Doctor/Surgeon saves still apply.
    Can only be used 2 times per game total.
    """
    def __init__(self, *a, **k):
        super().__init__(*a, **k)

        # Count how many times werekitten_kill has been used this game
        conn_wk = sqlite3.connect(DB_FILE)
        c_wk    = conn_wk.cursor()
        c_wk.execute(
            "SELECT COUNT(*) FROM night_actions WHERE guild_id=? AND action_type=?",
            (self.guild_id, "werekitten_kill"))
        times_used = c_wk.fetchone()[0]
        conn_wk.close()

        uses_left = max(0, 2 - times_used)

        if uses_left == 0:
            btn = Button(
                label="🐱 Werekitten kill used 2/2 — den kill applies tonight",
                style=discord.ButtonStyle.secondary, disabled=True)
            self.add_item(btn)
            return

        opts = self._player_options()
        if opts:
            sel = Select(
                placeholder=f"🐱 Choose kill target ({uses_left} use(s) remaining)...",
                options=opts)
            sel.callback = self.on_select
            self.add_item(sel)

        skip_btn = Button(
            label=f"⏭️ Skip — use den kill instead ({uses_left} use(s) remaining)",
            style=discord.ButtonStyle.secondary)
        skip_btn.callback = self.on_skip
        self.add_item(skip_btn)

    async def on_select(self, interaction: discord.Interaction):
        target_id = int(interaction.data["values"][0])
        night_num = db_get_night_num(interaction.guild_id)
        target    = interaction.guild.get_member(target_id)
        tname     = target.display_name if target else str(target_id)
        actor     = interaction.guild.get_member(self.actor_id)

        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "werekitten_kill", target_id)

        await interaction.response.edit_message(
            content=fmt(
                f"🐱 Kill target submitted: **{tname}**\n"
                f"The den kill is replaced tonight — they will not know your target.\n"
                f"*Remember: Doctor and Surgeon saves still apply to your target.*"),
            view=None)
        await post_mod_log(interaction.guild,
            f"🐱 **Werekitten Kill** — Night {night_num}\n"
            f"**{actor.display_name if actor else self.actor_id}** targeting **{tname}**\n"
            f"*Bypasses Sheriff and Huntsman. Doctor/Surgeon saves still apply. Den kill is REPLACED.*")

    async def on_skip(self, interaction: discord.Interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "werekitten_skip", None)
        await interaction.response.edit_message(
            content=fmt("⏭️ Skipped — the den kill applies tonight as normal."),
            view=None)


# ── White Wolf ────────────────────────────────────────────────────────────
class WhiteWolfView(BaseNightView):
    """
    White Wolf must kill a wolf within 3 attempts or die.
    Strikes are earned by: skipping OR killing a non-wolf.
    Only killing a wolf clears the obligation.
    Strike counting is mod-confirmed via /ww_result after resolution.
    """
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        strikes = db_get_ww_strikes(self.guild_id, self.actor_id)
        remaining = 3 - strikes

        opts = self._player_options()
        if opts:
            sel = Select(
                placeholder="🤍 Choose your kill target tonight...",
                options=opts)
            sel.callback = self.on_kill
            self.add_item(sel)

        if strikes == 0:
            skip_label = "⏭️ Skip tonight (1st strike)"
            skip_style = discord.ButtonStyle.secondary
        elif strikes == 1:
            skip_label = "⏭️ Skip tonight (2nd strike — 1 left)"
            skip_style = discord.ButtonStyle.danger
        else:
            skip_label = "⏭️ Skip tonight ⚠️ FINAL STRIKE — you will die"
            skip_style = discord.ButtonStyle.danger

        skip_btn = Button(label=skip_label[:80], style=skip_style)
        skip_btn.callback = self.on_skip
        self.add_item(skip_btn)

        if strikes > 0:
            warn_btn = Button(
                label=f"⚠️ {strikes}/3 strikes — kill a wolf to survive",
                style=discord.ButtonStyle.secondary, disabled=True)
            self.add_item(warn_btn)

    async def on_kill(self, interaction: discord.Interaction):
        target_id = int(interaction.data["values"][0])
        night_num = db_get_night_num(interaction.guild_id)
        target    = interaction.guild.get_member(target_id)
        tname     = target.display_name if target else str(target_id)
        actor     = interaction.guild.get_member(self.actor_id)
        strikes   = db_get_ww_strikes(interaction.guild_id, self.actor_id)

        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "white_wolf_kill", target_id)

        await interaction.response.edit_message(
            content=fmt(
                f"🤍 Kill submitted: **{tname}**\n"
                f"*The mod will confirm the result. If your target is not a wolf, you gain a strike.*"),
            view=None)
        await post_mod_log(interaction.guild,
            f"🤍 **White Wolf Kill** — Night {night_num}\n"
            f"**{actor.display_name if actor else self.actor_id}** targeting **{tname}**\n"
            f"Current strikes: **{strikes}/3**\n"
            f"*Use `/ww_result wolf` if target is a wolf (clears obligation). "
            f"Use `/ww_result miss` if not (adds a strike).*")

    async def on_skip(self, interaction: discord.Interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "white_wolf_skip", None)
        strikes   = db_get_ww_strikes(interaction.guild_id, self.actor_id)
        actor     = interaction.guild.get_member(self.actor_id)

        await interaction.response.edit_message(
            content=fmt(
                f"⏭️ Skipped. The mod will apply a strike.\n"
                f"*Kill a wolf before you reach 3 strikes or you die.*"),
            view=None)
        await post_mod_log(interaction.guild,
            f"🤍 **White Wolf Skip** — Night {night_num}\n"
            f"**{actor.display_name if actor else self.actor_id}** is skipping tonight.\n"
            f"*Run `/ww_result miss` to apply the strike ({strikes + 1}/3 after this).*")


# ── Oracle ────────────────────────────────────────────────────────────────
class OracleQuestionModal(discord.ui.Modal, title="Ask the Crystal Ball"):
    question = discord.ui.TextInput(
        label       = "Your yes/no question",
        placeholder = "e.g. Is there more than one wolf still alive?",
        style       = discord.TextStyle.paragraph,
        max_length  = 300,
        required    = True,
    )

    def __init__(self, guild_id, actor_id, night_num, priv_ch_id):
        super().__init__()
        self.guild_id   = guild_id
        self.actor_id   = actor_id
        self.night_num  = night_num
        self.priv_ch_id = priv_ch_id

    async def on_submit(self, interaction: discord.Interaction):
        question_text = self.question.value.strip()
        db_save_night_action(self.guild_id, self.night_num, self.actor_id, "oracle_question", None)

        actor = interaction.guild.get_member(self.actor_id)

        # Post to mod-log with a Reply button so mod can answer directly
        embed = discord.Embed(
            title       = f"🔯 Oracle Question — Night {self.night_num}",
            description = f"**{actor.display_name if actor else self.actor_id}** asks:\n\n*{question_text}*",
            color       = 0x9B59B6
        )
        embed.set_footer(text="Answer YES or NO using the buttons below. Answer goes to Oracle's private channel only.")

        view = OracleAnswerView(self.guild_id, self.actor_id, self.night_num,
                                self.priv_ch_id, question_text)
        state_mod = db_get_state(self.guild_id) or {}
        mod_ch    = interaction.guild.get_channel(state_mod.get("mod_log_channel_id") or 0)
        if mod_ch:
            await mod_ch.send(embed=embed, view=view)

        await interaction.response.edit_message(
            content=fmt(
                f"🔯 Your question has been submitted to the mod.\n"
                f"*{question_text}*\n\n"
                f"The mod will answer — check this channel for their response."),
            view=None)


class OracleAnswerView(View):
    """Mod clicks Yes or No — answer sent privately to Oracle's channel."""
    def __init__(self, guild_id, actor_id, night_num, priv_ch_id, question_text):
        super().__init__(timeout=7200)
        self.guild_id     = guild_id
        self.actor_id     = actor_id
        self.night_num    = night_num
        self.priv_ch_id   = priv_ch_id
        self.question_text = question_text

        yes_btn = Button(label="✅ Yes", style=discord.ButtonStyle.green)
        no_btn  = Button(label="❌ No",  style=discord.ButtonStyle.danger)
        yes_btn.callback = self.on_yes
        no_btn.callback  = self.on_no
        self.add_item(yes_btn)
        self.add_item(no_btn)

    async def _send_answer(self, interaction, answer: str):
        priv_ch = interaction.guild.get_channel(self.priv_ch_id)
        if priv_ch:
            await priv_ch.send(fmt(
                f"🔯 **Oracle Answer — Night {self.night_num}**\n"
                f"Your question: *{self.question_text}*\n\n"
                f"The crystal ball says: **{answer}**\n\n"
                f"*Remember — do NOT share this answer outright or you will die.*"))
        await interaction.response.edit_message(
            content=f"**{answer}** sent to Oracle's private channel.",
            embed=None, view=None)

    async def on_yes(self, interaction: discord.Interaction):
        await self._send_answer(interaction, "✅ YES")

    async def on_no(self, interaction: discord.Interaction):
        await self._send_answer(interaction, "❌ NO")



# ── Governor Day Pardon View ──────────────────────────────────────────────
class DayGovernorView(View):
    """
    Posted to Governor's private channel when the day vote opens.
    Governor must use before the vote closes (15 min early per role rules).
    One use per game. Timeout=None — the mod enforces the 15-min deadline.
    """
    def __init__(self, guild_id: int, governor_id: int, vote_end_ts: int):
        super().__init__(timeout=None)
        self.guild_id    = guild_id
        self.governor_id = governor_id
        self.vote_end_ts = vote_end_ts

        deadline_ts = vote_end_ts - (15 * 60)  # 15 min before close
        sel = Select(placeholder="🎖️ Choose a player to pardon from today's vote...")
        sel.callback = self.on_pardon
        self._sel = sel
        self.add_item(sel)

        info_btn = Button(
            label  = f"⚠️ Must submit 15 min before vote closes",
            style  = discord.ButtonStyle.secondary,
            disabled = True)
        self.add_item(info_btn)

    def patch_options(self, guild, rows, npc_map):
        options = []
        for pid, role, is_alive, _ in rows:
            if not is_alive or pid == self.governor_id:
                continue
            m    = guild.get_member(pid)
            name = npc_map.get(pid) or (m.display_name if m else str(pid))
            options.append(discord.SelectOption(label=name[:100], value=str(pid)))
        self._sel.options = options[:25] or [discord.SelectOption(label="No targets", value="none")]

    async def on_pardon(self, interaction: discord.Interaction):
        if interaction.user.id != self.governor_id:
            return await interaction.response.send_message(
                "❌ Only the Governor can use this.", ephemeral=True)
        raw = interaction.data["values"][0]
        if raw == "none":
            return await interaction.response.send_message(
                "❌ No valid targets.", ephemeral=True)

        guild_id  = interaction.guild_id
        night_num = db_get_night_num(guild_id)

        # Check if pardon already used this game
        all_actions = []
        for n in range(1, night_num + 1):
            all_actions.extend(db_get_night_actions(guild_id, n))
        if any(a[0] == self.governor_id and a[1] == "governor_pardon" for a in all_actions):
            return await interaction.response.send_message(
                "❌ You have already used your pardon this game.", ephemeral=True)

        target_id = int(raw)
        target    = interaction.guild.get_member(target_id)
        tname     = target.display_name if target else str(target_id)

        db_save_night_action(guild_id, night_num, self.governor_id, "governor_pardon", target_id)
        await interaction.response.edit_message(
            content=fmt(
                f"🎖️ **Pardon submitted for {tname}.**\n"
                f"The mod has been notified. If {tname} receives the most votes today, they are safe."),
            view=None)
        await post_mod_log(interaction.guild,
            f"🎖️ **Governor Pardon — Day {night_num}**\n"
            f"**{interaction.user.display_name}** has pardoned **{tname}** from today's vote.\n"
            f"*{tname} cannot be eliminated today — apply if they receive the most votes.*")


# ── Hermit Day Ability View ───────────────────────────────────────────────
class DayHermitView(View):
    """
    Posted to Hermit's private channel when the day vote opens.
    Hermit can hide the top-voted player (causing second-highest to be killed instead).
    Must use 20 minutes before vote closes. If they hide a wolf, they die.
    One use per game.
    """
    def __init__(self, guild_id: int, hermit_id: int, vote_end_ts: int):
        super().__init__(timeout=None)
        self.guild_id   = guild_id
        self.hermit_id  = hermit_id
        self.vote_end_ts = vote_end_ts

        sel = Select(placeholder="🏚️ Choose a player to hide from the vote...")
        sel.callback = self.on_hide
        self._sel = sel
        self.add_item(sel)

        skip_btn = Button(label="⏭️ Skip — don't use ability today", style=discord.ButtonStyle.secondary)
        skip_btn.callback = self.on_skip
        self.add_item(skip_btn)

        info_btn = Button(
            label  = "⚠️ Must submit 20 min before vote closes — hiding a wolf = you die",
            style  = discord.ButtonStyle.secondary,
            disabled = True)
        self.add_item(info_btn)

    def patch_options(self, guild, rows, npc_map):
        options = []
        for pid, role, is_alive, _ in rows:
            if not is_alive or pid == self.hermit_id:
                continue
            m    = guild.get_member(pid)
            name = npc_map.get(pid) or (m.display_name if m else str(pid))
            options.append(discord.SelectOption(label=name[:100], value=str(pid)))
        self._sel.options = options[:25] or [discord.SelectOption(label="No targets", value="none")]

    async def on_hide(self, interaction: discord.Interaction):
        if interaction.user.id != self.hermit_id:
            return await interaction.response.send_message(
                "❌ Only the Hermit can use this.", ephemeral=True)
        raw = interaction.data["values"][0]
        if raw == "none":
            return await interaction.response.send_message("❌ No valid targets.", ephemeral=True)

        guild_id  = interaction.guild_id
        night_num = db_get_night_num(guild_id)

        # One use per game
        all_actions = []
        for n in range(1, night_num + 1):
            all_actions.extend(db_get_night_actions(guild_id, n))
        if any(a[0] == self.hermit_id and a[1] == "hermit" for a in all_actions):
            return await interaction.response.send_message(
                "❌ You have already used your ability this game.", ephemeral=True)

        target_id = int(raw)
        target    = interaction.guild.get_member(target_id)
        tname     = target.display_name if target else str(target_id)

        db_save_night_action(guild_id, night_num, self.hermit_id, "hermit", target_id)
        await interaction.response.defer(ephemeral=True)
        await post_mod_log(interaction.guild,
            f"🏚️ **Hermit Ability — Day {night_num}**\n"
            f"**{interaction.user.display_name}** is hiding **{tname}**.\n"
            f"The second-highest voted player will be eliminated instead.\n"
            f"⚠️ *If {tname} is a wolf, the Hermit dies too.*")

        await interaction.followup.send(
            fmt(
                f"🏚️ **{tname}** will be hidden from the vote.\n"
                f"The mod has been notified.\n"
                f"⚠️ If {tname} is a wolf, you will die alongside them."),
            ephemeral=True)

    async def on_skip(self, interaction: discord.Interaction):
        if interaction.user.id != self.hermit_id:
            return await interaction.response.send_message(
                "❌ Only the Hermit can use this.", ephemeral=True)
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.hermit_id, "hermit_skip", None)
        await interaction.response.edit_message(
            content=fmt("⏭️ Skipping today's ability. The vote proceeds normally."),
            view=None)


async def _send_day_ability_buttons(guild, guild_id: int, vote_end_ts: int):
    """
    When the day vote opens, send ability buttons to Governor and Hermit
    in their private channels. Called from all vote-open paths.
    """
    rows    = db_get_assignments(guild_id)
    npcs    = db_get_npcs(guild_id)
    npc_map = {n["npc_id"]: n["name"] for n in npcs}

    for pid, role, is_alive, priv_ch_id in rows:
        if not is_alive or not priv_ch_id:
            continue
        priv_ch = guild.get_channel(priv_ch_id)
        if not priv_ch:
            continue

        if role == "Governor":
            view = DayGovernorView(guild_id, pid, vote_end_ts)
            view.patch_options(guild, rows, npc_map)
            if view._sel.options:
                try:
                    await priv_ch.send(
                        fmt(
                            f"🎖️ **Day vote is open.**\n"
                            f"You may use your pardon before the vote closes.\n"
                            f"Vote closes: <t:{vote_end_ts}:R>\n"
                            f"⚠️ You must submit at least 15 min before close."),
                        view=view)
                except Exception as e:
                    print(f"[day_ability_buttons] Governor error: {e}")

        elif role == "Hermit":
            view = DayHermitView(guild_id, pid, vote_end_ts)
            view.patch_options(guild, rows, npc_map)
            if view._sel.options:
                try:
                    await priv_ch.send(
                        fmt(
                            f"🏚️ **Day vote is open.**\n"
                            f"You may hide a player from the vote (causes second-highest to be eliminated instead).\n"
                            f"Vote closes: <t:{vote_end_ts}:R>\n"
                            f"⚠️ Must submit 20 min before close. Hiding a wolf = you die."),
                        view=view)
                except Exception as e:
                    print(f"[day_ability_buttons] Hermit error: {e}")


# ── Dispatcher — returns the right view for each role ─────────────────────
ROLE_VIEW_MAP = {
    # Village — active night abilities only
    "Seer":          SeerView,
    "Doctor":        DoctorView,
    "Surgeon":       SurgeonView,
    "Witch":         WitchView,
    "Huntsman":      HuntsmanView,
    "Medium":        MediumView,
    "Agitator":      AgitatorView,
    "Shapeshifter":  ShapeshifterView,
    "Cupid":         CupidView,
    "Bloodhound":    BloodhoundView,
    # Wolf — active night abilities only
    "Werekitten":    WerekittenKillView,
    "Wolf Pup":      WolfPupView,
    "Alpha":         AlphaView,
    "Elite Alpha":   EliteAlphaView,
    "Bloodletter":   BloodletterView,
    "Crazed Wolf":   CrazedWolfView,
    "Dire Wolf":     DireWolfView,
    "Echo-Stalker":  EchoStalkerView,
    "Shadow Wolf":   ShadowWolfView,
    "White Wolf":    WhiteWolfView,
    # Neutral — active night abilities only
    "Wraith":        WraithMarkView,
    "Clone":         CloneView,  # Night 1 only — blocked after
    # Removed from map (no night action button needed):
    # Governor — day pardon button posted in private channel when vote opens
    # Hermit — day ability button posted in private channel when vote opens
    # Gravedigger — passive, mod delivers info manually
    # Werekitten — passive, uses wolf den vote; den notified separately
    # Oracle — submits question to mod via /action
    # Warlock — wish granted via mod interaction, no button needed
    # Fairy Elf — passive effect, no button needed
}




# ── Pothead Second Kill View ───────────────────────────────────────────────
class PothreadSecondKillView(View):
    """Posted in wolf-vote when Pothead is killed — wolves pick a second target."""
    def __init__(self, guild_id, night_num, alive_players):
        super().__init__(timeout=3600)
        self.guild_id  = guild_id
        self.night_num = night_num
        options = [
            discord.SelectOption(label=p.display_name, value=str(p.id))
            for p in alive_players[:25]
        ]
        if not options:
            btn = Button(label="No valid targets", disabled=True,
                         style=discord.ButtonStyle.secondary)
            self.add_item(btn)
            return
        sel = Select(placeholder="🍕 Pothead bonus — choose second kill target", options=options)
        sel.callback = self.on_select
        self.add_item(sel)

    async def on_select(self, interaction: discord.Interaction):
        # Verify voter is a wolf
        rows = db_get_assignments(interaction.guild_id)
        me   = next((r for r in rows if r[0] == interaction.user.id and r[2] == 1), None)
        if not me or get_team(interaction.guild_id, me[1]) != "wolf":
            return await interaction.response.send_message(
                "❌ Only wolves can select the second kill.", ephemeral=True)
        target_id = int(interaction.data["values"][0])
        target    = interaction.guild.get_member(target_id)
        tname     = target.display_name if target else str(target_id)
        db_save_night_action(interaction.guild_id, self.night_num,
                             interaction.user.id, "pothead_second_kill", target_id)
        await interaction.response.edit_message(
            content=fmt(f"🍕 Second kill target selected: {tname}.\nMod has been notified."),
            view=None)
        await post_mod_log(interaction.guild,
            f"🍕 **Pothead Second Kill** — Night {self.night_num}\n"
            f"**Selected by:** {interaction.user.display_name}\n"
            f"**Target:** {tname}\n"
            f"Mod: eliminate this player at resolution.")

# ── Village Jokester Kill View ─────────────────────────────────────────────
class JokesterKillView(View):
    """Shown to Village Jokester after they are voted out — pick one voter to kill."""
    def __init__(self, guild_id, jokester_id, voter_ids):
        super().__init__(timeout=300)
        self.guild_id    = guild_id
        self.jokester_id = jokester_id
        if not voter_ids:
            no_btn = Button(label="No voters to kill", disabled=True,
                            style=discord.ButtonStyle.secondary)
            self.add_item(no_btn)
            return
        options = []
        for pid in voter_ids[:25]:
            # Get name from guild — will be fetched when needed
            options.append(discord.SelectOption(
                label=str(pid),   # patched to display name when sent
                value=str(pid)
            ))
        sel = Select(placeholder="☠️ Choose one voter to take with you...", options=options)
        sel.callback = self.on_select
        self.add_item(sel)

    async def on_select(self, interaction: discord.Interaction):
        target_id = int(interaction.data["values"][0])
        target    = interaction.guild.get_member(target_id)
        tname     = target.display_name if target else str(target_id)
        await interaction.response.edit_message(
            content=fmt(f"☠️ You take {tname} with you into the dark.\nGoodnight, Jokester."),
            view=None)
        # Eliminate the chosen voter
        await _eliminate_player(interaction.guild, target_id, "Village Jokester's revenge")
        await post_mod_log(interaction.guild,
            f"🃏 **Village Jokester revenge kill**\n"
            f"Killed: **{tname}**")
        # Announce in village-chat
        state  = cached_get_state(interaction.guild_id)
        vc_ch  = interaction.guild.get_channel(state.get("village_chat_ch_id") or 0)
        if vc_ch:
            jokester = interaction.guild.get_member(self.jokester_id)
            jname    = jokester.display_name if jokester else "The Jokester"
            await vc_ch.send(
                f"🃏 **{jname}** laughs last — they take **{tname}** with them into the void.")

# ── NightStatusView — sent to EVERY alive player each night ───────────────
# Active-role players get their ability view first, then this as a second message.
# Passive players get only this view.
class NightStatusView(View):
    """
    Two buttons every player sees each night:
      ✅  Using my ability tonight  — posts to mod-log, disables view
      💤  Pass / no action tonight  — saves _pass action to DB, posts to mod-log, disables view
    """
    def __init__(self, guild_id=None, actor_id=None, role_name=None, night_num=None):
        super().__init__(timeout=None)
        self.guild_id  = guild_id
        self.actor_id  = actor_id
        self.role_name = role_name
        self.night_num = night_num

        use_btn  = Button(label="✅ Using my ability tonight",  style=discord.ButtonStyle.green)
        pass_btn = Button(label="💤 Pass — no action tonight",  style=discord.ButtonStyle.secondary)
        use_btn.callback  = self.on_use
        pass_btn.callback = self.on_pass
        self.add_item(use_btn)
        self.add_item(pass_btn)

    async def on_use(self, interaction: discord.Interaction):
        if self.actor_id and interaction.user.id != self.actor_id:
            return await interaction.response.send_message("❌ This isn't your status button.", ephemeral=True)
        actor = interaction.guild.get_member(self.actor_id or interaction.user.id)
        role  = self.role_name or "Unknown"
        night = self.night_num or db_get_night_num(interaction.guild_id)
        await interaction.response.edit_message(
            content=fmt("✅ Got it — the mod knows you are using your ability tonight.\nSubmit it using the action above."),
            view=None)
        await post_mod_log(interaction.guild,
            f"✅ **{role}** — Night {night}\n"
            f"**{actor.display_name if actor else interaction.user.display_name}** confirmed: using their ability tonight.")

    async def on_pass(self, interaction: discord.Interaction):
        if self.actor_id and interaction.user.id != self.actor_id:
            return await interaction.response.send_message("❌ This isn't your status button.", ephemeral=True)
        guild_id  = interaction.guild_id
        actor_id  = self.actor_id or interaction.user.id
        night_num = self.night_num or db_get_night_num(guild_id)
        role      = self.role_name or "Unknown"
        db_save_night_action(guild_id, night_num, actor_id, "_pass", None)
        actor = interaction.guild.get_member(actor_id)
        await interaction.response.edit_message(
            content=fmt("💤 Passed. The mod has been notified. Sleep tight!"),
            view=None)
        await post_mod_log(interaction.guild,
            f"💤 **{role}** — Night {night_num}\n"
            f"**{actor.display_name if actor else interaction.user.display_name}** is passing — no action tonight.")

def get_night_view(guild_id, actor_id, role_name, alive_players):
    """Return the appropriate night action View for a given role, or None if passive."""
    # Bloodhound cannot act on Night 1
    if role_name == "Bloodhound" and db_get_night_num(guild_id) < 2:
        return None

    # Alpha — hard limit of 1 successful turn total
    if role_name == "Alpha":
        conn_t = sqlite3.connect(DB_FILE)
        c_t    = conn_t.cursor()
        c_t.execute(
            "SELECT COUNT(*) FROM turn_log WHERE guild_id=? AND actor_id=? AND result=?",
            (guild_id, actor_id, "success"))
        used = c_t.fetchone()[0]
        conn_t.close()
        if used >= 1:
            return None  # Alpha already used their one turn

    # Elite Alpha — max 2 successful turns, must be at least 2 nights apart
    if role_name == "Elite Alpha":
        conn_t = sqlite3.connect(DB_FILE)
        c_t    = conn_t.cursor()
        c_t.execute(
            "SELECT night_num FROM turn_log WHERE guild_id=? AND actor_id=? AND result=? ORDER BY night_num",
            (guild_id, actor_id, "success"))
        success_nights = [r[0] for r in c_t.fetchall()]
        conn_t.close()
        current_night  = db_get_night_num(guild_id)
        if len(success_nights) >= 2:
            return None  # Both turns used
        if success_nights:
            last_turn_night = success_nights[-1]
            if current_night - last_turn_night < 2:
                return None  # Must be at least 2 nights apart

    # Clone — Night 1 only, and only if they haven't chosen yet
    if role_name == "Clone":
        if db_get_night_num(guild_id) > 1:
            return None
        actions_cl = db_get_night_actions(guild_id, 1)
        if any(a[0] == actor_id and a[1] == "clone" for a in actions_cl):
            return None  # Already chose their target

    # Dire Wolf — only Night 1
    if role_name == "Dire Wolf":
        night_num = db_get_night_num(guild_id)
        if night_num > 1:
            return None  # Dire Wolf only chooses on Night 1
        # Check if they already chose
        actions = db_get_night_actions(guild_id, 1)
        if any(a[0] == actor_id and a[1] == "dire_wolf" for a in actions):
            return None  # Already chose their mate

    cls = ROLE_VIEW_MAP.get(role_name)
    if cls:
        return cls(guild_id, actor_id, role_name, alive_players)
    return None



@tree.command(name="next_phase", description="Advance to the next game phase")
@is_mod()
async def next_phase(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    state     = cached_get_state(interaction.guild_id)
    phase     = state.get("phase", "day")
    night_num = db_get_night_num(interaction.guild_id)

    if phase == "night":
        # ── Night → Resolve — Start Day button posted by resolve_night ───
        task = night_timers.pop(interaction.guild_id, None)
        if task: task.cancel()
        await resolve_night(interaction.guild, night_num)
        await interaction.followup.send(
            f"✅ Night {night_num} resolved. Check mod-log for the Start Day button.", ephemeral=True)

    else:
        # ── Day → Close vote, show results, prompt for night ─────────────
        # Post final vote standings to mod-log
        vote_summary = _build_vote_summary(interaction.guild, interaction.guild_id)
        state_cur    = cached_get_state(interaction.guild_id)
        mod_ch       = interaction.guild.get_channel(state_cur.get("mod_log_channel_id") or 0)

        # Cancel any running day vote timer
        t = day_vote_timers.pop(interaction.guild_id, None)
        if t: t.cancel()
        db_set_state(interaction.guild_id, day_vote_end_time=None)

        if mod_ch:
            embed = discord.Embed(
                title       = f"🗳️ Day {night_num} Vote — Final Standings",
                description = vote_summary or "No votes were cast.",
                color       = 0x95A5A6
            )
            embed.set_footer(text="Use /eliminate to remove players, then click below to begin the night.")
            view = DayVoteClosedPromptView(interaction.guild_id, vote_summary)
            await mod_ch.send(embed=embed, view=view)

        await interaction.followup.send(
            f"✅ Day vote standings posted to mod-log. Eliminate players then start night when ready.",
            ephemeral=True)

@tree.command(name="start_night", description="Begin the night phase")
@is_mod()
async def start_night(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)

    # Block night start if agitator frenzy not satisfied
    state_check  = db_get_state(interaction.guild_id) or {}
    frenzy_day   = state_check.get("agitator_frenzy_day")
    night_now    = db_get_night_num(interaction.guild_id)
    elim_count   = int(state_check.get("agitator_elim_count") or 0)
    if frenzy_day and int(frenzy_day) == int(night_now):
        if elim_count < 2:
            return await interaction.response.send_message(
                f"⚡ **Agitator frenzy is active!** "
                f"{elim_count}/2 eliminations done. "
                f"Eliminate **{2 - elim_count}** more player(s) before night can begin.",
                ephemeral=True)
        # Check all alive players have cast their second vote
        rows_f    = db_get_assignments(interaction.guild_id)
        votes_2   = db_get_day_votes_2(interaction.guild_id)
        voted2_ids = {v[0] for v in votes_2}
        npcs_f    = db_get_npcs(interaction.guild_id)
        npc_ids   = {n["npc_id"] for n in npcs_f}
        missing   = []
        for pid, role, is_alive, _ in rows_f:
            if is_alive and pid not in voted2_ids and pid not in npc_ids:
                m    = interaction.guild.get_member(pid)
                name = m.display_name if m else str(pid)
                missing.append(name)
        if missing:
            return await interaction.response.send_message(
                f"⚡ **Frenzy active — all players must vote TWICE!**\n"
                f"Still waiting on second vote from: {', '.join(missing)}",
                ephemeral=True)

    # Defer immediately — lightest possible acknowledgment, gives 15 min for heavy work
    await interaction.response.defer(ephemeral=True)

    import time as _t
    state     = cached_get_state(interaction.guild_id)
    db_set_state(interaction.guild_id, phase="night")
    if state.get("phase") == "day":
        db_increment_night(interaction.guild_id)
    night_num = db_get_night_num(interaction.guild_id)

    # Use night_duration from DB (default 43200 = 12 hours), respects Time Lord changes
    duration     = int(state.get("night_duration") or 43200)
    night_end_ts = _phase_end_ts(duration, 8, interaction.guild_id)
    import time as _sn_t
    actual_hrs   = (night_end_ts - int(_sn_t.time())) // 3600

    db_set_state(interaction.guild_id, night_end_time=night_end_ts)
    await interaction.followup.send(
        f"🌙 Night {night_num} begins! Ends at <t:{night_end_ts}:t> (<t:{night_end_ts}:R>)", ephemeral=True)

    # Village chat night announcement
    vc_sn = interaction.guild.get_channel(state.get("village_chat_ch_id") or 0)
    if vc_sn:
        NIGHT_ANNOUNCE = [
            "*Whisperfall grows quiet. Lock your doors.*",
            "*The light fades. Whatever moves in the dark is already moving.*",
            "*Night falls on Whisperfall. Not everyone will see the morning.*",
            "*The bells have stopped. The night begins.*",
            "*Whisperfall holds its breath. Night {n} has come.*",
        ]
        import random as _r
        msg = _r.choice(NIGHT_ANNOUNCE).replace("{n}", str(night_num))
        await vc_sn.send(f"🌙 **Night {night_num} has fallen.** {msg}\n*Submit your actions in your private channel. Ends <t:{night_end_ts}:R>.*")

    await _run_start_night(interaction.guild, interaction.guild_id, night_num, duration, state)

    # Auto-resolve after duration
    guild_snap    = interaction.guild
    guild_id_snap = interaction.guild_id
    async def _auto_resolve_night():
        import time as _t2
        wait = night_end_ts - int(_t2.time())
        if wait > 0:
            await asyncio.sleep(wait)
        if not game_active(guild_id_snap):
            return
        cur = db_get_state(guild_id_snap) or {}
        if cur.get("phase") == "night" and cur.get("night_end_time") == night_end_ts:
            await resolve_night(guild_snap, night_num)
            state_ar  = db_get_state(guild_id_snap) or {}
            mod_ch_ar = guild_snap.get_channel(state_ar.get("mod_log_channel_id") or 0)
            if mod_ch_ar:
                await mod_ch_ar.send(
                    f"⏰ **Night {night_num} auto-resolved (timer expired).**\n"
                    f"Use `/eliminate` to apply deaths, then click Deliver Results.",
                    view=DeliverResultsView(guild_id_snap, night_num))
    safe_task(_auto_resolve_night(), "auto_resolve_night")




@tree.command(name="resolve_night", description="Manually resolve night actions early")
@is_mod()
async def resolve_night_cmd(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    state = cached_get_state(interaction.guild_id)
    if state.get("phase") != "night":
        return await interaction.response.send_message("No active night phase.", ephemeral=True)
    task = night_timers.pop(interaction.guild_id, None)
    if task: task.cancel()
    night_num = db_get_night_num(interaction.guild_id)
    await interaction.response.defer(ephemeral=True)
    await resolve_night(interaction.guild, night_num)
    try:
        await interaction.followup.send("⏩ Night resolved.", ephemeral=True)
    except Exception:
        pass



class DeliverNightResultsView(View):
    """Posted to mod-log after night resolves — mod clicks to deliver all queued results."""
    def __init__(self, guild_id: int, night_num: int):
        super().__init__(timeout=None)
        self.guild_id  = guild_id
        self.night_num = night_num
        btn = Button(label="✅ Deliver Night Results", style=discord.ButtonStyle.green)
        btn.callback = self.on_deliver
        self.add_item(btn)

    async def on_deliver(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="⏳ Delivering night results...", view=None)

        guild    = interaction.guild
        guild_id = self.guild_id
        night    = self.night_num
        actions  = db_get_night_actions(guild_id, night)
        rows     = db_get_assignments(guild_id)

        def get_priv_ch(pid):
            r = next((r for r in rows if r[0] == pid), None)
            return guild.get_channel(r[3]) if r and r[3] else None

        delivered = []

        # ── Step 1: Bloodletter marks (affects subsequent checks) ─────────
        for actor_id, action_type, target_id in actions:
            if action_type == "bloodletter" and target_id:
                t = guild.get_member(target_id)
                tname = t.display_name if t else str(target_id)
                await post_mod_log(guild,
                    f"🩸 **Bloodletter mark applied** — {tname} appears as wolf to checks this night.")

        # ── Step 1b: Wraith Kill Command resolution ──────────────────────
        ws = db_get_wraith_state(guild_id)
        if ws.get("kill_agreed", 0) >= 2 and not ws.get("kill_used", 0):
            # Kill Command is confirmed — kill all marked players
            marks = db_get_wraith_marks(guild_id)
            if marks:
                killed_names = []
                for marker_id, target_id, _ in marks:
                    target_row = next((r for r in rows if r[0] == target_id and r[2] == 1), None)
                    if target_row:
                        tm = guild.get_member(target_id)
                        tname = tm.display_name if tm else str(target_id)
                        killed_names.append(tname)
                        # Mark dead — Doctor/Surgeon saves do NOT apply
                        await _set_player_dead(guild, guild_id, target_id, target_row[1])

                db_set_wraith_state(guild_id, kill_used=1)
                db_clear_wraith_marks(guild_id)

                await post_mod_log(guild,
                    f"👻 **Wraith Kill Command executed** — Night {night_num}\n"

                    f"Killed: {', '.join(killed_names)}\n"

                    f"*Doctor/Surgeon saves did not apply. Wraiths cannot act again.*")

                # BB hint
                bb_state_w = db_get_state(guild_id) or {}
                bb_ch_w    = guild.get_channel(bb_state_w.get("bb_channel_id") or 0)
                if bb_ch_w and killed_names:
                    await bb_ch_w.send(
                        f"*Something that was not the wolves moved through Whisperfall tonight. "
                        f"What had been marked was collected. "
                        f"{len(killed_names)} soul(s) did not wake.*")

        # ── Step 2: Shapeshifter (affects subsequent checks) ──────────────
        for actor_id, action_type, target_id in actions:
            if action_type == "shapeshifter" and target_id:
                t      = guild.get_member(target_id)
                tname  = t.display_name if t else str(target_id)

                # Get target's role
                target_row  = next((r for r in rows if r[0] == target_id), None)
                target_role = target_row[1] if target_row else None

                if target_role:
                    # Swap role in DB — Shapeshifter takes target's role for the night
                    actor_row  = next((r for r in rows if r[0] == actor_id), None)
                    old_role   = actor_row[1] if actor_row else "Shapeshifter"

                    conn_ss = sqlite3.connect(DB_FILE)
                    c_ss    = conn_ss.cursor()
                    c_ss.execute(
                        "UPDATE player_assignments SET role_name=? WHERE guild_id=? AND player_id=?",
                        (target_role, guild_id, actor_id))
                    conn_ss.commit()
                    conn_ss.close()
                    invalidate_cache(guild_id)

                    # Rebuild rows with updated role for subsequent checks
                    rows = db_get_assignments(guild_id)

                    # Notify Shapeshifter in private channel
                    actor_row2 = next((r for r in rows if r[0] == actor_id), None)
                    priv_ch_ss = guild.get_channel(actor_row2[3] if actor_row2 else 0)
                    if priv_ch_ss:
                        ss_embed = discord.Embed(
                            title       = "🎭 Shapeshifter — Transformed",
                            description = (
                                f"You have permanently taken the form of **{tname}**.\n"
                                f"You are now a **{target_role}** for the rest of the game.\n"
                                f"You have their full ability. All checks on you see {target_role}.\n\n"
                                f"*You are no longer the Shapeshifter.*"
                            ),
                            color = 0x9B59B6
                        )
                        await priv_ch_ss.send(embed=ss_embed)

                    # Update private channel name to reflect new role
                    if priv_ch_ss:
                        try:
                            font_ss = get_guild_font(guild_id)
                            new_name = f"🔒{guild.get_member(actor_id).display_name if guild.get_member(actor_id) else str(actor_id)}-{target_role}".lower().replace(" ", "-")
                            await priv_ch_ss.edit(name=new_name[:100])
                        except Exception:
                            pass

                    # If target is a wolf role, grant den access
                    if get_team(guild_id, target_role) == "wolf":
                        state_ss2 = cached_get_state(guild_id)
                        wolf_ch_ss = guild.get_channel(state_ss2.get("wolf_channel_id") or 0)
                        ss_member  = guild.get_member(actor_id)
                        if wolf_ch_ss and ss_member:
                            try:
                                await wolf_ch_ss.set_permissions(ss_member,
                                    view_channel=True, send_messages=True)
                            except Exception:
                                pass

                    await post_mod_log(guild,
                        f"🎭 **Shapeshifter permanently became {target_role}** (copied from {tname})\n"
                        f"They now have full {target_role} ability for the rest of the game.\n"
                        f"All checks on them will see {target_role}."
                        + (f"\n🐺 Den access granted — they are now wolf-aligned."
                           if get_team(guild_id, target_role) == "wolf" else ""))
                else:
                    await post_mod_log(guild,
                        f"🎭 **Shapeshifter** tried to transform into {tname} but target role not found.")

        # ── Step 3: Seer results ──────────────────────────────────────────
        for actor_id, action_type, target_id in actions:
            if action_type == "seer" and target_id:
                target_row  = next((r for r in rows if r[0] == target_id), None)
                target_role = target_row[1] if target_row else "Unknown"
                target_m    = guild.get_member(target_id)
                target_name = target_m.display_name if target_m else str(target_id)

                # Check bloodletter mark
                bl_marked = any(
                    a[1] == "bloodletter" and a[2] == target_id
                    for a in actions)

                if bl_marked:
                    is_wolf = True
                elif target_role == "Lycan":
                    is_wolf = True
                elif target_role == "White Wolf":
                    is_wolf = True
                elif target_role == "Blessed Wolf":
                    count   = db_get_check_count(guild_id, target_id)
                    db_increment_check_count(guild_id, target_id)
                    is_wolf = count >= 2
                elif target_role in {"Cursed", "Werekitten"}:
                    is_wolf = False
                else:
                    is_wolf = get_team(guild_id, target_role) == "wolf"

                yn     = "✅ Yes" if is_wolf else "❌ No"
                color  = 0xC0392B if is_wolf else 0x27AE60
                flavor = random.choice(SEER_WOLF_LINES if is_wolf else SEER_CLEAR_LINES)
                priv   = get_priv_ch(actor_id)
                if priv:
                    embed = discord.Embed(
                        title       = f"🔮 Night {night} — The Sight",
                        description = f"*{flavor}*",
                        color       = color)
                    embed.add_field(
                        name  = f"Is **{target_name}** a wolf?",
                        value = f"**{yn}**",
                        inline= False)
                    embed.set_footer(text="Do not share this directly — doing so results in death.")
                    await priv.send(embed=embed)
                    delivered.append(f"🔮 Seer result delivered to {guild.get_member(actor_id).display_name if guild.get_member(actor_id) else actor_id}")

        # ── Step 4: Medium results ────────────────────────────────────────
        for actor_id, action_type, target_id in actions:
            if action_type == "medium" and target_id:
                target_row  = next((r for r in rows if r[0] == target_id), None)
                target_role = target_row[1] if target_row else "Unknown"
                target_m    = guild.get_member(target_id)
                target_name = target_m.display_name if target_m else str(target_id)

                APPEARS_GOOD = {"Elite Alpha", "Blessed Wolf", "Werekitten", "Cursed"}
                if target_role in APPEARS_GOOD:
                    alignment, color = "✅ Good", 0x27AE60
                elif get_team(guild_id, target_role) == "wolf":
                    alignment, color = "❌ Bad",  0xC0392B
                elif get_team(guild_id, target_role) == "neutral":
                    alignment, color = "⚖️ Neutral", 0xF39C12
                else:
                    alignment, color = "✅ Good", 0x27AE60

                priv = get_priv_ch(actor_id)
                if priv:
                    await priv.send(fmt(
                        f"🌀 **Medium Result — Night {night}**\n"
                        f"**{target_name}** — {alignment}"))
                    delivered.append(f"🌀 Medium result delivered")

        # ── Step 5: Bloodhound results ────────────────────────────────────
        for actor_id, action_type, target_id in actions:
            if action_type == "bloodhound" and target_id:
                target_row  = next((r for r in rows if r[0] == target_id), None)
                target_role = target_row[1] if target_row else "Unknown"
                target_m    = guild.get_member(target_id)
                target_name = target_m.display_name if target_m else str(target_id)
                priv        = get_priv_ch(actor_id)
                state_bh    = db_get_state(guild_id) or {}
                wolf_ch     = guild.get_channel(state_bh.get("wolf_channel_id") or 0)

                if priv:
                    await priv.send(fmt(
                        f"🦴 **Bloodhound Result — Night {night}**\n"
                        f"**{target_name}** is: **{target_role}**"))
                if wolf_ch:
                    await wolf_ch.send(fmt(
                        f"🦴 **Bloodhound scan** — Night {night}\n"
                        f"**{target_name}** is: **{target_role}**"))
                delivered.append(f"🦴 Bloodhound result delivered")

        # ── Step 6: Insomniac hints ───────────────────────────────────────
        for actor_id, action_type, target_id in actions:
            if action_type == "insomniac_hint":
                priv = get_priv_ch(actor_id)
                if priv and target_id:
                    hint_m = guild.get_member(target_id)
                    if hint_m:
                        await priv.send(fmt(
                            f"😴 **Insomniac Hint — Night {night}**\n"
                            f"You heard someone with a wolf role moving in the night: **{hint_m.display_name}**"))
                delivered.append("😴 Insomniac hint delivered")

        summary = "\n".join(delivered) if delivered else "No investigative results to deliver."
        await post_mod_log(guild,
            f"✅ **Night {night} results delivered**\n{summary}")


async def _deliver_night_results(guild, guild_id: int, night_num: int):
    """Deliver all queued investigative results in correct order after disguises are applied."""
    actions      = db_get_night_actions(guild_id, night_num)
    rows         = db_get_assignments(guild_id)
    npcs         = db_get_npcs(guild_id)
    npc_map      = {n["npc_id"]: n["name"] for n in npcs}
    assignment_map = {r[0]: r[1] for r in rows}

    # ── Turn order check — warn if a pending turn could affect investigation targets ──
    # A successful turn updates player_assignments immediately in AlphaTurnConfirmView.
    # If a turn is still 'pending' in turn_log, the mod hasn't confirmed it yet,
    # meaning investigations may be delivered against stale role data.
    conn_tc = sqlite3.connect(DB_FILE)
    c_tc    = conn_tc.cursor()
    c_tc.execute(
        "SELECT actor_id, target_id FROM turn_log WHERE guild_id=? AND night_num=? AND result='pending'",
        (guild_id, night_num))
    pending_turns = c_tc.fetchall()
    conn_tc.close()

    if pending_turns:
        # Check if any investigation target overlaps with a pending turn target
        invest_targets = {target_id for _, action_type, target_id in actions
                          if action_type in ("seer", "medium", "bloodhound") and target_id}
        turn_targets   = {t[1] for t in pending_turns}
        overlap        = invest_targets & turn_targets

        warn_parts = []
        for actor_id, target_id in pending_turns:
            actor  = guild.get_member(actor_id)
            target = guild.get_member(target_id)
            aname  = npc_map.get(actor_id) or (actor.display_name if actor else str(actor_id))
            tname  = npc_map.get(target_id) or (target.display_name if target else str(target_id))
            warn_parts.append(f"**{aname}** → **{tname}**")

        overlap_note = ""
        if overlap:
            overlap_names = []
            for tid in overlap:
                m = guild.get_member(tid)
                overlap_names.append(npc_map.get(tid) or (m.display_name if m else str(tid)))
            overlap_note = (
                f"\n\n⚠️ **INVESTIGATION CONFLICT:** "
                f"The following pending turn target(s) are also being investigated this night: "
                f"**{', '.join(overlap_names)}**\n"
                f"If the turn succeeds, investigators will see the **wrong role**.\n"
                f"**Confirm the turn result first, then deliver investigations.**"
            )

        await post_mod_log(guild,
            f"⚠️ **Pending turn(s) not yet confirmed — Night {night_num}**\n"
            f"{chr(10).join(warn_parts)}\n"
            f"Investigations have been delivered, but role data may be stale if a turn succeeded."
            f"{overlap_note}")


    def get_name(pid):
        return npc_map.get(pid) or getattr(guild.get_member(pid), "display_name", str(pid))

    def get_priv_ch(pid):
        row = next((r for r in rows if r[0] == pid), None)
        return guild.get_channel(row[3]) if row and row[3] else None

    # ── Step 1: Apply Bloodletter marks ──────────────────────────────────
    bloodlettered = set()
    for actor_id, action_type, target_id in actions:
        if action_type == "bloodletter" and target_id:
            bloodlettered.add(target_id)

    # ── Step 2: Apply Shapeshifter transform ─────────────────────────────
    # Shapeshifter transform is already saved — assignment_map already reflects it
    # No additional action needed here

    # ── Step 3: Deliver Seer results ─────────────────────────────────────
    for actor_id, action_type, target_id in actions:
        if action_type != "seer" or not target_id:
            continue
        target_role = assignment_map.get(target_id, "Unknown")
        target_name = get_name(target_id)

        # Apply Bloodletter mark — marked player appears as wolf
        if target_id in bloodlettered:
            is_wolf = True
        elif target_role == "Lycan":
            is_wolf = True
        elif target_role == "White Wolf":
            is_wolf = True
        elif target_role == "Blessed Wolf":
            check_count = db_get_check_count(guild_id, target_id)
            is_wolf = check_count >= 2
        elif target_role in {"Cursed", "Werekitten"}:
            is_wolf = False
        else:
            is_wolf = get_team(guild_id, target_role) == "wolf"

        yn      = "✅ Yes" if is_wolf else "❌ No"
        priv_ch = get_priv_ch(actor_id)
        if priv_ch:
            embed = discord.Embed(
                title       = f"🔮 Seer Result — Night {night_num}",
                description = f"Is **{target_name}** a wolf?  **{yn}**",
                color       = 0xC0392B if is_wolf else 0x27AE60
            )
            embed.set_footer(text="Do not share this directly — doing so results in death.")
            try:
                await priv_ch.send(embed=embed)
            except Exception as e:
                print(f"[deliver] Seer result send failed for {actor_id}: {e}")
                await post_mod_log(guild,
                    f"⚠️ Could not deliver Seer result to <#{priv_ch.id}> — {e}")

    # ── Step 4: Deliver Medium results ───────────────────────────────────
    APPEARS_GOOD = {"Elite Alpha", "Blessed Wolf", "Werekitten", "Cursed"}
    for actor_id, action_type, target_id in actions:
        if action_type != "medium" or not target_id:
            continue
        target_role = assignment_map.get(target_id, "Unknown")
        target_name = get_name(target_id)

        # Bloodletter mark — appears Bad to Medium
        if target_id in bloodlettered:
            alignment = "❌ Bad"
            color     = 0xC0392B
        elif target_role in APPEARS_GOOD:
            alignment = "✅ Good"
            color     = 0x27AE60
        elif get_team(guild_id, target_role) == "wolf":
            alignment = "❌ Bad"
            color     = 0xC0392B
        elif get_team(guild_id, target_role) == "neutral":
            alignment = "⚖️ Neutral"
            color     = 0xF39C12
        else:
            alignment = "✅ Good"
            color     = 0x27AE60

        priv_ch = get_priv_ch(actor_id)
        if priv_ch:
            try:
                await priv_ch.send(fmt(
                    f"🌀 **Medium Result — Night {night_num}**\n"
                    f"**{target_name}** — {alignment}"))
            except Exception as e:
                print(f"[deliver] Medium result send failed for {actor_id}: {e}")
                await post_mod_log(guild,
                    f"⚠️ Could not deliver Medium result to <#{priv_ch.id}> — {e}")

    # ── Step 5: Deliver Bloodhound results ───────────────────────────────
    state   = db_get_state(guild_id) or {}
    wolf_ch = guild.get_channel(state.get("wolf_channel_id") or 0)
    for actor_id, action_type, target_id in actions:
        if action_type != "bloodhound" or not target_id:
            continue
        target_role = assignment_map.get(target_id, "Unknown")
        target_name = get_name(target_id)
        is_self     = (target_id == actor_id)

        # Deliver to wolf den
        if wolf_ch:
            try:
                den_msg = (f"🦴 **Bloodhound Report** — Night {night_num}\n"
                           f"Scanned: **{'themselves' if is_self else target_name}**\n"
                           f"Exact role: **{target_role}**")
                await wolf_ch.send(den_msg)
            except Exception as e:
                print(f"[deliver] Bloodhound den send failed: {e}")

        # Deliver to Bloodhound private channel
        priv_ch = get_priv_ch(actor_id)
        if priv_ch:
            priv_msg = (f"🦴 Self-scan complete. Your role confirmed as: **{target_role}**." if is_self
                        else f"🦴 Scan complete. **{target_name}** is: **{target_role}**.")
            try:
                await priv_ch.send(fmt(priv_msg))
            except Exception as e:
                print(f"[deliver] Bloodhound result send failed for {actor_id}: {e}")
                await post_mod_log(guild,
                    f"⚠️ Could not deliver Bloodhound result to <#{priv_ch.id}> — {e}")

    await post_mod_log(guild,
        f"✅ **Night {night_num} results delivered** — Seer, Medium, and Bloodhound results sent.")


class DeliverResultsView(View):
    """Posted to mod-log after resolve_night — mod clicks to deliver investigative results."""
    def __init__(self, guild_id=None, night_num=None):
        super().__init__(timeout=None)
        self.guild_id  = guild_id
        self.night_num = night_num

        if guild_id is None:
            # Bare registration for restart recovery — buttons added minimally
            btn = Button(label="✅ Deliver Night Results", style=discord.ButtonStyle.green)
            btn.callback = self.on_deliver
            self.add_item(btn)
            return

        # Check for pending turns — warn mod if turn hasn't been confirmed yet
        conn_dv = sqlite3.connect(DB_FILE)
        c_dv    = conn_dv.cursor()
        c_dv.execute(
            "SELECT COUNT(*) FROM turn_log WHERE guild_id=? AND night_num=? AND result='pending'",
            (guild_id, night_num))
        pending = c_dv.fetchone()[0]
        conn_dv.close()

        if pending:
            warn_btn = Button(
                label  = f"⚠️ {pending} turn(s) unconfirmed — confirm via AlphaTurnConfirmView first",
                style  = discord.ButtonStyle.danger,
                disabled = True)
            self.add_item(warn_btn)

        label = "✅ Deliver Night Results" if not pending else "⚠️ Deliver Anyway (turn may not be confirmed)"
        style = discord.ButtonStyle.green if not pending else discord.ButtonStyle.secondary
        btn   = Button(label=label, style=style)
        btn.callback = self.on_deliver
        self.add_item(btn)

    async def on_deliver(self, interaction: discord.Interaction):
        # Use stored guild_id/night_num, or fall back to live state after restart
        guild_id  = self.guild_id  or interaction.guild_id
        night_num = self.night_num or db_get_night_num(guild_id)

        # Defer immediately — delivery loops through multiple channels and can take several seconds
        await interaction.response.defer(ephemeral=True)

        # Disable the button so it can't be double-clicked
        try:
            await interaction.message.edit(
                content=f"⏳ Delivering Night {night_num} results...", view=None)
        except Exception:
            pass  # Non-fatal if message edit fails

        try:
            await _deliver_night_results(interaction.guild, guild_id, night_num)
            db_set_state(guild_id, investigations_done=1)
            invalidate_cache(guild_id)
            safe_task(update_mod_dashboard(interaction.guild), "dashboard_deliver")
            # Update the button message to show success
            try:
                await interaction.message.edit(
                    content=f"✅ Night {night_num} results delivered to all players.")
            except Exception:
                pass
            await interaction.followup.send(
                f"✅ Night {night_num} results delivered.", ephemeral=True)
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            print(f"[deliver_results] Error: {e}\n{tb}")
            # Post the error to mod-log so mods can see what went wrong
            try:
                await post_mod_log(interaction.guild,
                    f"⚠️ **Deliver Results failed — Night {night_num}**\n"
                    f"Error: `{str(e)[:300]}`\n"
                    f"Some results may have been partially delivered. "
                    f"Use `/submit_action` to manually re-deliver if needed.")
            except Exception:
                pass
            try:
                await interaction.message.edit(
                    content=f"⚠️ Night {night_num} delivery failed — check mod-log for details.")
            except Exception:
                pass
            await interaction.followup.send(
                f"❌ Delivery failed: {str(e)[:200]}\nCheck mod-log for details.",
                ephemeral=True)

SEER_WOLF_LINES = [
    "The veil parts for a moment. The truth is unmistakable.",
    "There it is. Hidden beneath everything else, but clear as daylight now.",
    "The darkness in them is not metaphor. It is fact.",
    "You already suspected. Now you know.",
    "Something wrong runs through them. You have seen it and cannot unsee it.",
    "The sight does not lie. Whatever they told you, whatever you wanted to believe — this is the truth.",
    "It is them. The village needs to know without knowing how you know.",
]

SEER_CLEAR_LINES = [
    "Nothing hidden. Nothing wrong. This one is what they appear to be.",
    "The veil shows you only the ordinary. They are not your answer tonight.",
    "Whatever hunts in Whisperfall, it is not them. Not tonight.",
    "Clean. Or at least as clean as anyone gets. Move on.",
    "The sight gives you nothing here. This is not who you are looking for.",
    "Innocent. The word sits uncomfortably in Whisperfall, but here it applies.",
    "No wolf here. Which means the wolf is somewhere else. Keep looking.",
]

async def resolve_night(guild: discord.Guild, night_num: int):
    """Mark phase as day, clear bond, notify mod — mod handles all resolution manually."""
    guild_id = guild.id

    # Guard against double-resolve — if already day, bail out silently
    current_phase = (db_get_state(guild_id) or {}).get("phase")
    if current_phase == "day":
        print(f"[resolve_night] guild {guild_id} already in day phase — skipping duplicate resolve")
        return

    db_set_state(guild_id, phase="day")
    db_clear_cupid_current(guild_id)

    # Pull submitted actions summary for mod reference
    actions  = db_get_night_actions(guild_id, night_num)
    rows     = db_get_assignments(guild_id)
    npcs     = db_get_npcs(guild_id)
    npc_map  = {n["npc_id"]: n["name"] for n in npcs}

    action_lines = []
    for actor_id, action_type, target_id in actions:
        if action_type.startswith("_"):
            continue  # skip internal pass markers
        m_actor  = guild.get_member(actor_id)
        actor_name = npc_map.get(actor_id) or (m_actor.display_name if m_actor else str(actor_id))
        if target_id:
            m_target = guild.get_member(target_id)
            target_name = npc_map.get(target_id) or (m_target.display_name if m_target else str(target_id))
        else:
            target_name = "—"
        ACTION_LABELS = {
            "seer":            "🔮 Seer investigation",
            "medium":          "🌀 Medium alignment check",
            "bloodhound":      "🦴 Bloodhound scan",
            "doctor_save":     "💊 Doctor — using save",
            "doctor_skip":     "💊 Doctor — skipping tonight",
            "surgeon_save":    "🏥 Surgeon — using save",
            "surgeon_skip":    "🏥 Surgeon — skipping tonight",
            "huntsman":        "🏹 Huntsman — protecting",
            "huntsman_skip":   "🏹 Huntsman — not protecting",
            "witch_save":      "🧙 Witch — using save potion",
            "witch_kill":      "🧙 Witch — using poison potion",
            "witch_skip":      "🧙 Witch — skipping",
            "alpha":           "👑 Alpha — turn attempt",
            "elite_alpha":     "👑⭐ Elite Alpha — turn attempt",
            "wolf_pup":        "🐾 Wolf Pup — blocking",
            "bloodletter":     "🩸 Bloodletter — marking",
            "cupid_bind":      "💘 Cupid — binding",
            "cupid_skip":      "💘 Cupid — skipping",
            "agitator_frenzy": "📢 Agitator — activating frenzy",
            "clone":           "🪞 Clone — choosing target",
            "shapeshifter":    "🎭 Shapeshifter — transforming",
            "dire_wolf":       "💔 Dire Wolf — bonding",
            "hermit":          "🏚️ Hermit — hiding target",
            "white_wolf_kill": "🤍 White Wolf — independent kill",
            "white_wolf_skip": "🤍 White Wolf — skipping tonight",
            "shadow_wolf":     "🌑 Shadow Wolf — killing voter",
            "echo_stalk":      "👁️ Echo-Stalker — haunting",
            "crazed_wolf_1":   "🌪️ Crazed Wolf — first kill",
            "crazed_wolf_2":   "🌪️ Crazed Wolf — second kill",
            "governor":        "🎖️ Governor — pardoning",
            "gravedigger_ack": "⚰️ Gravedigger — listening",
            "oracle_question": "🔯 Oracle — asking question",
            "warlock_ack":     "⚗️ Warlock — acknowledged",
            "werekitten_kill": "🐱 Werekitten — kill (bypasses all defenses)",
            "werekitten_skip": "🐱 Werekitten — skipping tonight",
            "_pass":           "💤 Passed — no action",
        }
        label = ACTION_LABELS.get(action_type, action_type.replace("_", " ").title())
        action_lines.append(f"• **{actor_name}** — {label} → {target_name}")

    summary = "\n".join(action_lines) if action_lines else "No actions submitted."

    # Build rich night summary embed for mod-log
    rows_sum  = db_get_assignments(guild_id)
    npcs_sum  = db_get_npcs(guild_id)
    npc_map_s = {n["npc_id"]: n["name"] for n in npcs_sum}
    alive_sum = [r for r in rows_sum if r[2] == 1]

    # Who did NOT submit an action
    submitted_ids = {a[0] for a in actions if not a[1].startswith("_")}
    missing = []
    for pid, role, is_alive, _ in alive_sum:
        if not is_alive: continue
        if role in NIGHT_NO_BUTTON_ROLES: continue
        if role not in ROLE_VIEW_MAP: continue
        if pid not in submitted_ids:
            name = npc_map_s.get(pid) or (guild.get_member(pid).display_name if guild.get_member(pid) else str(pid))
            missing.append(f"{name} ({role})")

    # Protection summary
    protected = []
    for actor_id, action_type, target_id in actions:
        if action_type in ("doctor_save","surgeon_save","huntsman") and target_id:
            t     = guild.get_member(target_id)
            tname = npc_map_s.get(target_id) or (t.display_name if t else str(target_id))
            label = {"doctor_save":"💊 Doctor","surgeon_save":"🏥 Surgeon",
                     "huntsman":"🏹 Huntsman"}.get(action_type)
            protected.append(f"{label} → **{tname}**")

    # Check for Werekitten kill — den kill is suppressed this night
    wk_action = next(
        ((actor_id, target_id) for actor_id, action_type, target_id in actions
         if action_type == "werekitten_kill"),
        None)

    night_embed = discord.Embed(title=f"🌙 Night {night_num} Summary", color=0x2C3060)

    if wk_action:
        wk_actor  = guild.get_member(wk_action[0])
        wk_target = guild.get_member(wk_action[1]) if wk_action[1] else None
        wk_aname  = npc_map_s.get(wk_action[0]) or (wk_actor.display_name if wk_actor else str(wk_action[0]))
        wk_tname  = npc_map_s.get(wk_action[1]) or (wk_target.display_name if wk_target else str(wk_action[1]))
        night_embed.add_field(
            name  = "🐱 ⚠️ WEREKITTEN KILL — DEN VOTE SUPPRESSED",
            value = (
                f"**{wk_aname}** is killing **{wk_tname}** tonight.\n"
                f"The wolf den vote result is **ignored** this night.\n"
                f"Sheriff and Huntsman protections do **not** apply.\n"
                f"Doctor and Surgeon saves apply normally."
            ),
            inline=False)

    night_embed.add_field(name="📋 Submitted Actions", value=summary[:1024] or "None", inline=False)
    if protected:
        night_embed.add_field(name="🛡️ Protected Tonight", value="\n".join(protected), inline=False)
    if missing:
        night_embed.add_field(name="⚠️ Did Not Submit", value="\n".join(missing), inline=False)
    footer = (
        "🐱 Werekitten kill active — ignore den vote, apply Werekitten target."
        if wk_action else
        "Use /eliminate to apply deaths, then deliver results and start day."
    )
    night_embed.set_footer(text=footer)
    await post_mod_log(guild, embed=night_embed)

    # Post mod-log buttons — deliver results + start day
    state_mod = db_get_state(guild_id) or {}
    mod_ch    = guild.get_channel(state_mod.get("mod_log_channel_id") or 0)
    if mod_ch:
        # Check if frenzy is active this night
        frenzy = any(a[1] == "agitator_frenzy" for a in actions)

        # Deliver results button
        await mod_ch.send(
            "📬 **Step 1 — Deliver investigative results to players:**",
            view=DeliverResultsView(guild_id, night_num))

        # Start Day button — separate message so it's always visible
        frenzy_note = "⚡ **Frenzy is active — TWO eliminations required today.**\n" if frenzy else ""
        await mod_ch.send(
            f"{frenzy_note}"
            f"☀️ **Step 2 — When ready, start the day and open the vote:**",
            view=StartDayPromptView(guild_id, night_num))

    await log_event(guild, f"Night {night_num}", f"🌙 Night {night_num} resolved — awaiting mod day start")
    await set_bot_status(f"🌙 Night {night_num} resolved — mod starting day")
    # Reset night board flag and refresh dashboard
    db_set_state(guild_id, night_bb_done=0, investigations_done=0)
    safe_task(update_mod_dashboard(guild), "dashboard_resolve")

# ====================== FONT PICKER ======================

class FontPickerView(View):
    """Shown during /start_game setup — lets mod preview and choose a channel font style."""
    def __init__(self, guild_id: int, roles_list: list, preloaded_counts: dict = None):
        super().__init__(timeout=180)
        self.guild_id        = guild_id
        self.roles_list      = roles_list
        self.preloaded_counts = preloaded_counts  # From template load
        self.chosen          = get_guild_font(guild_id)
        self._build()

    def _build(self):
        self.clear_items()
        options = []
        for key, info in FONT_STYLES.items():
            options.append(discord.SelectOption(
                label=info["label"],
                value=key,
                description=f"Preview: {info['example']}",
                default=(key == self.chosen)
            ))
        sel = Select(placeholder="Choose a channel name style", options=options,
                     min_values=1, max_values=1)
        sel.callback = self.on_pick
        self.add_item(sel)

        confirm = Button(label=f"✅ Use {FONT_STYLES[self.chosen]['label']} & Build Roster",
                         style=discord.ButtonStyle.green, row=1)
        confirm.callback = self.on_confirm
        self.add_item(confirm)

    async def on_pick(self, interaction: discord.Interaction):
        self.chosen = interaction.data["values"][0]
        self._build()
        sample = apply_font("mod-log  |  wolf-den  |  ghost-chat", self.chosen)
        await interaction.response.edit_message(
            content=(
                f"**🎨 Channel Font Picker**\n"
                f"Pick a style for all channel names this game.\n\n"
                f"**Preview:** `{sample}`\n\n"
                f"Hit **✅ Use {FONT_STYLES[self.chosen]['label']}** when happy."
            ),
            view=self
        )

    async def on_confirm(self, interaction: discord.Interaction):
        set_guild_font(self.guild_id, self.chosen)
        view  = RoleBuilderView(self.guild_id, self.roles_list, counts=self.preloaded_counts or {})
        sample = apply_font("mod-log  |  wolf-den  |  ghost-chat", self.chosen)
        state     = cached_get_state(self.guild_id)
        night_dur = state.get("night_duration", 43200)
        day_dur   = state.get("day_duration",   43200)
        await interaction.response.edit_message(
            content=(
                f"**🎮 Build Your Game Roster**\n"
                f"☀️ Day: **{day_dur//3600:.1f}h** · 🌙 Night: **{night_dur//3600:.1f}h** · "
                f"🎨 Font: **{FONT_STYLES[self.chosen]['label']}** (`{sample}`)\n\n"
                f"Use the dropdown to pick roles and **+/−** to set quantities."
            ),
            view=view
        )
        self.stop()


@tree.command(name="set_font", description="Change the Unicode font style used for channel names")
@is_mod()
async def set_font(interaction: discord.Interaction):
    """Can be used outside of a game to pre-set the font for the next game."""
    roles = db_load_roles(interaction.guild_id)
    view  = FontPickerView(interaction.guild_id, roles or [])
    current = get_guild_font(interaction.guild_id)
    sample  = apply_font("mod-log  |  wolf-den  |  ghost-chat", current)
    await interaction.response.send_message(
        f"**🎨 Channel Font Picker**\n"
        f"Current style: **{FONT_STYLES[current]['label']}** — `{sample}`\n\n"
        f"Pick a new style from the dropdown:",
        view=view, ephemeral=True
    )

# ====================== AUTO-REMIND ======================

remind_timers = {}


# ====================== TURN LOG ======================

@tree.command(name="log_turn_result", description="Record the outcome of an Alpha/Elite Alpha turn attempt")
@is_mod()
@app_commands.describe(
    player="The player targeted for turning",
    result="successful / blocked / sheriff_death / alpha_death"
)
async def log_turn_result(interaction: discord.Interaction, player: discord.Member, result: str):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    valid = {"successful", "blocked", "sheriff_death", "alpha_death"}
    result = result.lower().strip()
    if result not in valid:
        return await interaction.response.send_message(
            f"\N{CROSS MARK} Result must be one of: {', '.join(valid)}", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    night_num = db_get_night_num(interaction.guild_id)
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute(
        "SELECT COALESCE(MAX(entry_id),0) FROM turn_log WHERE guild_id=? AND target_id=? AND result='pending'",
        (interaction.guild_id, player.id)
    )
    row = c.fetchone()
    max_eid = row[0] if row else 0
    if max_eid:
        c.execute("UPDATE turn_log SET result=? WHERE guild_id=? AND entry_id=?",
                  (result, interaction.guild_id, max_eid))
    conn.commit()
    conn.close()
    result_pretty = result.replace("_", " ").title()
    await post_mod_log(interaction.guild,
        f"**Turn Result** \N{EM DASH} Night {night_num}\n"
        f"**Target:** {player.display_name} | **Outcome:** {result_pretty}")

    # If turn was successful, warn mod that investigations must be delivered AFTER this
    if result == "successful":
        actions = db_get_night_actions(interaction.guild_id, night_num)
        has_invest = any(a[1] in ("seer", "medium", "bloodhound") for a in actions)
        if has_invest:
            await post_mod_log(interaction.guild,
                f"⚠️ **Turn was successful — {player.display_name} is now a wolf.**\n"
                f"Investigations this night (Seer / Medium / Bloodhound) will now see them correctly as a wolf.\n"
                f"**If you have not yet delivered investigations, do so now — the DB is updated.**")

    await interaction.followup.send(
        f"\N{WHITE HEAVY CHECK MARK} Turn result recorded: **{result_pretty}** for **{player.display_name}**.",
        ephemeral=True)
    safe_task(update_mod_dashboard(interaction.guild), "dashboard_turn_result")


# ====================== WHITE WOLF RESULT ======================

@tree.command(name="ww_result", description="Record whether the White Wolf's kill hit a wolf or not")
@is_mod()
@app_commands.describe(
    player="The White Wolf player",
    result="wolf = killed a wolf (obligation met), miss = killed a villager or skipped (strike added)"
)
async def ww_result(interaction: discord.Interaction, player: discord.Member, result: str):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    result = result.lower().strip()
    if result not in ("wolf", "miss"):
        return await interaction.response.send_message(
            "❌ Result must be `wolf` (killed a wolf) or `miss` (villager kill or skip).",
            ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    guild_id  = interaction.guild_id
    night_num = db_get_night_num(guild_id)

    if result == "wolf":
        # Wolf kill — obligation met, clear all strikes
        db_clear_ww_strikes(guild_id, player.id)
        await post_mod_log(interaction.guild,
            f"🤍 **White Wolf Result — Night {night_num}**\n"
            f"**{player.display_name}** successfully killed a wolf.\n"
            f"*Strikes cleared — obligation reset.*")
        # Notify player in their private channel
        rows = db_get_assignments(guild_id)
        ww_row = next((r for r in rows if r[0] == player.id), None)
        if ww_row and ww_row[3]:
            priv_ch = interaction.guild.get_channel(ww_row[3])
            if priv_ch:
                await priv_ch.send(fmt(
                    "🤍 **Your kill was confirmed — you found a wolf.**\n"
                    "Your strikes have been cleared. Keep hunting."))
        await interaction.response.send_message(
            f"✅ Wolf kill confirmed for **{player.display_name}** — strikes cleared.",
            ephemeral=True)
    else:
        # Miss (villager kill or skip) — add a strike
        new_total = db_add_ww_strike(guild_id, player.id)
        rows = db_get_assignments(guild_id)
        ww_row = next((r for r in rows if r[0] == player.id), None)

        if new_total >= 3:
            # 3rd strike — death
            await post_mod_log(interaction.guild,
                f"🤍 ⚠️ **White Wolf — 3rd Strike — Night {night_num}**\n"
                f"**{player.display_name}** has reached 3 strikes without killing a wolf.\n"
                f"*Eliminate the White Wolf this night per role rules.*")
            if ww_row and ww_row[3]:
                priv_ch = interaction.guild.get_channel(ww_row[3])
                if priv_ch:
                    await priv_ch.send(fmt(
                        "🤍 ⚠️ **Strike 3.**\n"
                        "You have failed to kill a wolf within 3 attempts.\n"
                        "The mod will eliminate you this night."))
            await interaction.followup.send(
                f"⚠️ **Strike 3** for **{player.display_name}** — eliminate them this night.",
                ephemeral=True)
        else:
            remaining = 3 - new_total
            await post_mod_log(interaction.guild,
                f"🤍 **White Wolf Miss — Night {night_num}**\n"
                f"**{player.display_name}** did not kill a wolf. Strike **{new_total}/3**.\n"
                f"*{remaining} attempt(s) remaining before elimination.*")
            if ww_row and ww_row[3]:
                priv_ch = interaction.guild.get_channel(ww_row[3])
                if priv_ch:
                    await priv_ch.send(fmt(
                        f"🤍 **Strike {new_total}/3.**\n"
                        f"Your kill was not a wolf — or you skipped.\n"
                        f"*{remaining} attempt(s) left. Kill a wolf before strike 3 or you die.*"))
            await interaction.followup.send(
                f"Strike {new_total}/3 recorded for **{player.display_name}** — {remaining} left.",
                ephemeral=True)


# ====================== VOTE HISTORY ======================

@tree.command(name="vote_history", description="Show complete voting history for the current game")
@app_commands.describe(player="Filter to a specific player (optional)")
async def vote_history(interaction: discord.Interaction, player: discord.Member = None):
    await interaction.response.defer(ephemeral=True)
    rows    = db_get_vote_history(interaction.guild_id)
    assigns = db_get_assignments(interaction.guild_id)
    npcs    = db_get_npcs(interaction.guild_id)
    npc_map = {n["npc_id"]: n["name"] for n in npcs}

    def get_name(pid):
        if pid is None: return "Abstain"
        npc = npc_map.get(pid)
        if npc: return npc
        m = interaction.guild.get_member(pid)
        return m.display_name if m else str(pid)

    # Build pid_to_name including assignment members and NPCs
    pid_to_name = {}
    for pid, _, _, _ in assigns:
        pid_to_name[pid] = get_name(pid)
    for npc_id, name in npc_map.items():
        pid_to_name[npc_id] = name

    if player:
        rows = [r for r in rows if r[1] == player.id]

    # Pad rows to 6 fields
    rows = [r + (0,) * (6 - len(r)) if len(r) < 6 else r for r in rows]

    if not rows:
        return await interaction.followup.send("No vote history recorded yet.", ephemeral=True)

    # Group by day — preserve insertion order (entry_id order from DB)
    by_day = {}
    for row in rows:
        day_num, voter_id, target_id, action, voted_at, change_count = row
        by_day.setdefault(day_num, []).append(row)

    for day_num in sorted(by_day.keys())[:10]:
        day_rows = by_day[day_num]
        embed = discord.Embed(
            title = f"☀️ Day {day_num} — Complete Vote Log",
            color = 0x5865F2
        )

        # Group chronologically per voter to show their full timeline
        voter_timeline = {}  # voter_id -> list of (target_id, action, voted_at)
        for _, voter_id, target_id, action, voted_at, change_count in day_rows:
            voter_timeline.setdefault(voter_id, []).append((target_id, action, voted_at))

        lines = []
        changers = []  # voters who changed their vote

        for voter_id, events in voter_timeline.items():
            voter_name = pid_to_name.get(voter_id, str(voter_id))
            if len(events) == 1:
                # Single vote — simple line
                target_id, action, voted_at = events[0]
                ts = f" <t:{voted_at}:t>" if voted_at else ""
                if action == "abstain" or target_id is None:
                    lines.append(f"**{voter_name}** → 🤐 Abstain{ts}")
                else:
                    lines.append(f"**{voter_name}** → **{pid_to_name.get(target_id, str(target_id))}**{ts}")
            else:
                # Multiple events — show full chain
                parts = []
                for target_id, action, voted_at in events:
                    ts = f"<t:{voted_at}:t>" if voted_at else ""
                    if action == "abstain" or target_id is None:
                        parts.append(f"🤐 Abstain {ts}".strip())
                    elif action == "change":
                        parts.append(f"🔄 **{pid_to_name.get(target_id, str(target_id))}** {ts}".strip())
                    else:
                        parts.append(f"**{pid_to_name.get(target_id, str(target_id))}** {ts}".strip())
                lines.append(f"**{voter_name}**: {' → '.join(parts)}")
                changers.append(voter_name)

        # Find players who never voted
        alive_pids = {r[0] for r in assigns if r[2] == 1}
        voted_pids = set(voter_timeline.keys())
        never_voted = [pid_to_name.get(p, str(p)) for p in alive_pids
                       if p not in voted_pids and p not in npc_map]

        embed.description = "\n".join(lines) if lines else "*No votes cast this day.*"

        if changers:
            embed.add_field(
                name  = "🔄 Changed Votes",
                value = ", ".join(changers),
                inline= False
            )
        if never_voted:
            embed.add_field(
                name  = "⚠️ Did Not Vote",
                value = ", ".join(sorted(never_voted)),
                inline= False
            )

        embed.set_footer(text=f"Day {day_num} · {len(voter_timeline)} player(s) voted · arrows show changes in order")
        await interaction.followup.send(embed=embed, ephemeral=True)



# ====================== WOLF DEN VOTE ======================

class WolfDenVoteView(View):
    """Simple day vote mechanic posted in wolf den — wolves vote on who to push village to eliminate."""
    def __init__(self, guild_id, night_num):
        super().__init__(timeout=None)
        self.guild_id  = guild_id
        self.night_num = night_num

        rows    = db_get_assignments(guild_id)
        npcs    = db_get_npcs(guild_id)
        npc_map = {n["npc_id"]: n["name"] for n in npcs}

        # Build options — only alive non-wolf players
        options = []
        for pid, role, is_alive, _ in rows:
            if not is_alive: continue
            if get_team(guild_id, role) == "wolf": continue
            name = npc_map.get(pid)
            if not name:
                m = None  # guild not available here — will show ID, patched in refresh
                name = f"Player {pid}"
            options.append(discord.SelectOption(label=name[:100], value=str(pid)))

        if options:
            sel = Select(placeholder="🐺 Who should the village eliminate today?", options=options[:25])
            sel.callback = self.on_vote
            self.add_item(sel)
        else:
            btn = Button(label="No targets available", disabled=True, style=discord.ButtonStyle.secondary)
            self.add_item(btn)

    async def on_vote(self, interaction: discord.Interaction):
        rows = db_get_assignments(interaction.guild_id)
        me   = next((r for r in rows if r[0] == interaction.user.id and r[2] == 1), None)
        if not me or get_team(interaction.guild_id, me[1]) != "wolf":
            return await interaction.response.send_message(
                "❌ Only alive wolves can use this.", ephemeral=True)

        target_id = int(interaction.data["values"][0])
        target    = interaction.guild.get_member(target_id)
        tname     = target.display_name if target else str(target_id)
        voter     = interaction.guild.get_member(interaction.user.id)
        vname     = voter.display_name if voter else str(interaction.user.id)

        # Save to wolf_votes (reuse table, use day_num as night_num key)
        day_num = db_get_night_num(interaction.guild_id)
        db_set_wolf_vote(interaction.guild_id, day_num, interaction.user.id, target_id)

        # Tally current votes
        wolf_votes = db_get_wolf_votes(interaction.guild_id, day_num)
        tally = {}
        for wid, tid in wolf_votes:
            tally.setdefault(tid, []).append(wid)

        tally_lines = []
        for tid, voters in sorted(tally.items(), key=lambda x: -len(x[1])):
            t = interaction.guild.get_member(tid)
            tname_t = t.display_name if t else str(tid)
            voter_names = ", ".join(
                (interaction.guild.get_member(v).display_name if interaction.guild.get_member(v) else str(v))
                for v in voters)
            tally_lines.append(f"**{tname_t}** — {len(voters)} vote(s) | *{voter_names}*")

        # Check consensus
        rows_w     = db_get_assignments(interaction.guild_id)
        wolf_alive = [r[0] for r in rows_w if r[2] == 1 and get_team(interaction.guild_id, r[1]) == "wolf"]
        top_target, top_voters = max(tally.items(), key=lambda x: len(x[1])) if tally else (None, [])
        consensus = top_target and len(top_voters) == len(wolf_alive) and len(wolf_alive) > 0

        embed = discord.Embed(
            title       = f"🐺 Den Day Vote — Day {day_num}",
            description = "Who should the village eliminate today? Coordinate your push.",
            color       = 0xC0392B
        )
        embed.add_field(
            name  = "📊 Current Tally",
            value = "\n".join(tally_lines) or "No votes yet.",
            inline= False)

        if consensus:
            top_m = interaction.guild.get_member(top_target)
            top_name = top_m.display_name if top_m else str(top_target)
            embed.add_field(
                name  = "🔒 Pack Agreed",
                value = f"**Push {top_name}** — all wolves aligned. Make it happen in the square.",
                inline= False)
            embed.color = 0x8B0000

        await interaction.response.edit_message(embed=embed, view=self)


@tree.command(name="wolf_vote", description="Open a day vote coordination poll in the wolf den")
@is_mod()
async def wolf_vote_cmd(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    state    = cached_get_state(interaction.guild_id)
    den_ch   = interaction.guild.get_channel(state.get("wolf_channel_id") or 0)
    if not den_ch:
        return await interaction.followup.send("❌ Wolf den channel not found.", ephemeral=True)

    night_num = db_get_night_num(interaction.guild_id)
    rows      = db_get_assignments(interaction.guild_id)
    npcs      = db_get_npcs(interaction.guild_id)
    npc_map   = {n["npc_id"]: n["name"] for n in npcs}

    # Build options with real names
    options = []
    for pid, role, is_alive, _ in rows:
        if not is_alive: continue
        if get_team(interaction.guild_id, role) == "wolf": continue
        name = npc_map.get(pid)
        if not name:
            m = interaction.guild.get_member(pid)
            name = m.display_name if m else f"Player {pid}"
        options.append(discord.SelectOption(label=name[:100], value=str(pid)))

    if not options:
        return await interaction.followup.send("❌ No valid targets to vote on.", ephemeral=True)

    view = WolfDenVoteView(interaction.guild_id, night_num)
    # Patch options with real names
    if hasattr(view, "children") and view.children:
        for child in view.children:
            if isinstance(child, Select):
                child.options = options[:25]

    embed = discord.Embed(
        title       = f"🐺 Den Day Vote — Day {night_num}",
        description = "Who should the village eliminate today? Coordinate your push.",
        color       = 0xC0392B
    )
    embed.add_field(name="📊 Current Tally", value="No votes yet.", inline=False)

    await den_ch.send(embed=embed, view=view)
    await interaction.followup.send("✅ Day vote poll posted in the wolf den.", ephemeral=True)


@tree.command(name="governor_pardon", description="Use your Governor pardon — removes a player from today's vote")
@app_commands.describe(player="The player you wish to pardon from elimination today")
async def governor_pardon_cmd(interaction: discord.Interaction, player: discord.Member = None):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    state    = cached_get_state(interaction.guild_id)
    if state.get("phase") != "day":
        return await interaction.followup.send(
            "❌ The Governor pardon can only be used during the day phase.", ephemeral=True)

    rows = db_get_assignments(interaction.guild_id)
    me   = next((r for r in rows if r[0] == interaction.user.id and r[2] == 1), None)
    if not me or me[1] not in ("Governor", "Shapeshifter"):
        return await interaction.followup.send(
            "❌ Only the Governor (or Shapeshifter who became Governor) can use this.", ephemeral=True)

    # Check if pardon already used this game
    night_num = db_get_night_num(interaction.guild_id)
    all_actions = []
    for n in range(1, night_num + 1):
        all_actions.extend(db_get_night_actions(interaction.guild_id, n))
    if any(a[0] == interaction.user.id and a[1] == "governor_pardon" for a in all_actions):
        return await interaction.followup.send(
            "❌ You have already used your pardon this game.", ephemeral=True)

    npcs     = db_get_npcs(interaction.guild_id)
    npc_map  = {n["npc_id"]: n["name"] for n in npcs}
    alive    = [interaction.guild.get_member(r[0]) for r in rows if r[2] == 1]
    alive    = [m for m in alive if m]

    if player:
        # Direct use if player specified
        target_id = player.id
        target_row = next((r for r in rows if r[0] == target_id and r[2] == 1), None)
        if not target_row:
            return await interaction.followup.send("❌ That player is not alive in the game.", ephemeral=True)
        db_save_night_action(interaction.guild_id, night_num, interaction.user.id, "governor_pardon", target_id)
        await post_mod_log(interaction.guild,
            f"🎖️ **Governor Pardon** — Day {night_num}\n"
            f"**{interaction.user.display_name}** has pardoned **{player.display_name}** from today's vote.\n"
            f"*{player.display_name} cannot be eliminated today.*")
        await interaction.followup.send(
            fmt(f"🎖️ **{player.display_name}** has been pardoned. Mod has been notified."),
            ephemeral=True)
    else:
        # Show dropdown to select
        view = GovernorPardonView(interaction.guild_id, interaction.user.id, alive, npc_map)
        await interaction.followup.send(
            fmt("🎖️ **Governor Pardon** — Choose who to protect from today's vote:"),
            view=view, ephemeral=True)


# ====================== GAME TEMPLATES ======================

@tree.command(name="save_template", description="Save the current role roster as a named template")
@is_mod()
@app_commands.describe(
    name="Template name (e.g. balanced_14, wolf_heavy_10)",
    description="Optional description of this template")
async def save_template(interaction: discord.Interaction, name: str, description: str = ""):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game to save as template.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    last_roles = db_get_last_roles(interaction.guild_id)
    if not last_roles:
        return await interaction.followup.send("❌ No role roster found for current game.", ephemeral=True)

    db_save_template(interaction.guild_id, name, last_roles, description)
    total = sum(last_roles.values())
    role_lines = ", ".join(f"{r} ×{c}" for r, c in last_roles.items())
    await interaction.followup.send(
        f"✅ Template **{name}** saved ({total} players).\n{role_lines}",
        ephemeral=True)


@tree.command(name="list_templates", description="Show all saved game templates")
@is_mod()
async def list_templates(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    templates = db_list_templates(interaction.guild_id)
    if not templates:
        return await interaction.followup.send(
            "No templates saved yet. Use `/save_template` after building a roster.", ephemeral=True)

    embed = discord.Embed(title="📋 Saved Game Templates", color=0x5865F2)
    for t in templates:
        total = sum(t["role_counts"].values())
        roles = ", ".join(f"{r} ×{c}" for r, c in t["role_counts"].items())
        value = f"**{total} players** — {roles}"
        if t["description"]:
            value = f"*{t['description']}*\n{value}"
        embed.add_field(name=f"📌 {t['name']}", value=value[:1024], inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.command(name="delete_template", description="Delete a saved game template")
@is_mod()
@app_commands.describe(name="Template name to delete")
async def delete_template(interaction: discord.Interaction, name: str):
    await interaction.response.defer(ephemeral=True)
    t = db_get_template(interaction.guild_id, name)
    if not t:
        return await interaction.followup.send(f"❌ Template **{name}** not found.", ephemeral=True)
    db_delete_template(interaction.guild_id, name)
    await interaction.followup.send(f"✅ Template **{name}** deleted.", ephemeral=True)


@tree.command(name="load_template", description="Load a saved template and immediately open the role builder with it pre-filled")
@is_mod()
@app_commands.describe(name="Template name to load")
async def load_template(interaction: discord.Interaction, name: str):
    await interaction.response.defer(ephemeral=True)
    if game_active(interaction.guild_id):
        return await interaction.followup.send("❌ A game is already active. End it first.", ephemeral=True)

    t = db_get_template(interaction.guild_id, name)
    if not t:
        return await interaction.followup.send(
            f"❌ Template **{name}** not found. Use /list_templates to see available templates.",
            ephemeral=True)

    roles      = cached_load_roles(interaction.guild_id)
    role_map   = {r["name"]: r for r in roles}
    counts     = {}
    missing    = []
    for role_name, count in t["role_counts"].items():
        if role_name in role_map:
            counts[role_name] = count
        else:
            missing.append(role_name)

    if missing:
        await interaction.followup.send(
            f"⚠️ Some roles from template **{name}** are not in your role pool and were skipped: "
            f"{', '.join(missing)}\nContinuing with available roles...", ephemeral=True)

    total = sum(counts.values())
    if total == 0:
        return await interaction.followup.send(
            f"❌ No valid roles found from template **{name}**.", ephemeral=True)

    # Open the role builder pre-filled with template counts
    current      = get_guild_font(interaction.guild_id)
    sample       = apply_font("mod-log  |  wolf-den  |  ghost-chat", current)
    view         = FontPickerView(interaction.guild_id, roles, preloaded_counts=counts)
    role_summary = ", ".join(f"{r} x{c}" for r, c in counts.items())
    desc_line    = f"{t['description']}\n" if t['description'] else ""
    await interaction.followup.send(
        f"📌 **Template: {name}** ({total} players)\n"
        f"{desc_line}"
        f"Roles: {role_summary}\n\n"
        f"**Step 1 — Choose font, then launch:**",
        view=view, ephemeral=True)


@tree.command(name="remind_night", description="Ping players who haven't submitted their night action yet")
@is_mod()
async def remind_night(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    state     = cached_get_state(interaction.guild_id)
    if state.get("phase") != "night":
        return await interaction.followup.send("❌ It is not currently night phase.", ephemeral=True)

    night_num = db_get_night_num(interaction.guild_id)
    actions   = db_get_night_actions(interaction.guild_id, night_num)
    rows      = db_get_assignments(interaction.guild_id)
    npcs      = db_get_npcs(interaction.guild_id)
    npc_ids   = {n["npc_id"] for n in npcs}

    submitted_ids = {a[0] for a in actions if not a[1].startswith("_")}
    missing = []
    for pid, role, is_alive, ch_id in rows:
        if not is_alive: continue
        if pid in npc_ids: continue
        if role in NIGHT_NO_BUTTON_ROLES: continue
        if role not in ROLE_VIEW_MAP: continue
        if pid not in submitted_ids:
            m = interaction.guild.get_member(pid)
            if m:
                missing.append((m, role, ch_id))

    if not missing:
        return await interaction.followup.send(
            "✅ All players have submitted their night actions.", ephemeral=True)

    reminded = []
    for m, role, ch_id in missing:
        ch = interaction.guild.get_channel(ch_id or 0)
        if ch:
            try:
                await ch.send(fmt(
                    f"⏰ {m.mention} — Night {night_num} reminder!\n"

                    f"You haven't submitted your **{role}** action yet.\n"

                    f"Please use your night action button above or it will be skipped."))
                reminded.append(f"{m.display_name} ({role})")
            except Exception:
                reminded.append(f"{m.display_name} ({role}) — could not reach channel")

    await interaction.followup.send(
        f"✅ Reminded {len(reminded)} player(s):\n" + "\n".join(f"• {r}" for r in reminded),
        ephemeral=True)

# ====================== SPIN WHEEL ======================

NIGHT_ROLE_ACTIONS = {
    "Seer":        ("seer",        "Seer"),
    "Doctor":      ("doctor",      "Doctor"),
        "Witch":       ("witch",       "Witch"),
    "Wolf":        ("wolf",        "Wolf Pack"),
    "Wolf Pup":    ("wolf_pup",    "Wolf Pup"),
    "Alpha":       ("alpha",       "Alpha"),
    "Elite Alpha": ("elite_alpha", "Elite Alpha"),
}

def db_save_night_order(guild_id, night_num, role_order: list):
    import json
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO night_order VALUES (?,?,?)",
              (guild_id, night_num, json.dumps(role_order)))
    conn.commit()
    conn.close()

def db_get_night_order(guild_id, night_num):
    import json
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT role_order FROM night_order WHERE guild_id=? AND night_num=?",
              (guild_id, night_num))
    row = c.fetchone()
    conn.close()
    conn.close()

    return json.loads(row[0]) if row and row[0] else []

class SpinWheelView(View):
    """Three independent spin dropdowns — Actions, Roles, Players. Results go to mod-log only."""
    def __init__(self, guild_id, guild, night_num, actions, roles, players):
        super().__init__(timeout=300)
        self.guild_id  = guild_id
        self.guild     = guild
        self.night_num = night_num

        # ── Dropdown 1: Actions ───────────────────────────────────────────
        action_opts = [discord.SelectOption(label=a[:100], value=a) for a in actions[:25]]
        sel_action  = Select(placeholder="🎡 Spin — Night Actions", options=action_opts)
        sel_action.callback = self._make_spin_callback("Action", actions)
        self.add_item(sel_action)

        # ── Dropdown 2: Roles ─────────────────────────────────────────────
        role_opts = [discord.SelectOption(label=r[:100], value=r) for r in roles[:25]]
        sel_role  = Select(placeholder="🎡 Spin — Roles in Game", options=role_opts, row=1)
        sel_role.callback = self._make_spin_callback("Role", roles)
        self.add_item(sel_role)

        # ── Dropdown 3: Players ───────────────────────────────────────────
        player_opts = [discord.SelectOption(label=p[:100], value=p) for p in players[:25]]
        sel_player  = Select(placeholder="🎡 Spin — Alive Players", options=player_opts, row=2)
        sel_player.callback = self._make_spin_callback("Player", players)
        self.add_item(sel_player)

    def _make_spin_callback(self, spin_type: str, pool: list):
        async def callback(interaction: discord.Interaction):
            result = random.choice(pool)
            await interaction.response.defer()
            await post_mod_log(self.guild,
                f"🎡 **Wheel Spin — {spin_type}** | Night {self.night_num}\n"
                f"**Selected:** {result}")
        return callback


@tree.command(name="spin_wheel", description="Randomly generate a night resolution order — posts to mod-log only")
@is_mod()
async def spin_wheel(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    night_num = db_get_night_num(interaction.guild_id)
    rows      = db_get_assignments(interaction.guild_id)
    npcs      = db_get_npcs(interaction.guild_id)
    npc_map   = {n["npc_id"]: n["name"] for n in npcs}

    # ── Actions pool ──────────────────────────────────────────────────────
    actions = [
        "Wolf Kill", "Seer Investigation", "Doctor Save", "Surgeon Save",
        "Witch Save Potion", "Witch Poison Potion",
        "Huntsman Protect", "Medium Alignment Check", "Hermit Hide",
        "Agitator Frenzy", "Governor Pardon", "Clone Inherit",
        "Insomniac Hint", "Gravedigger Death Info", "Cupid Bind",
        "Alpha Turn Attempt", "Elite Alpha Turn Attempt", "Bloodhound Identify",
        "Bloodletter Mark", "Crazed Wolf Double Kill", "Dire Wolf Bond",
        "Echo-Stalker Haunt", "Shadow Wolf Kill", "White Wolf Independent Kill",
        "Oracle Question", "Shapeshifter Transform", "Wolf Pup Block",
    ]

    # ── Roles pool — all roles in current game ────────────────────────────
    last_roles = db_get_last_roles(interaction.guild_id)
    if last_roles:
        roles = list(last_roles.keys())
    else:
        roles = [r["name"] for r in get_game_roles(interaction.guild_id)]

    # ── Players pool — all alive players ─────────────────────────────────
    players = []
    for pid, role_name, is_alive, _ in rows:
        if not is_alive:
            continue
        name = npc_map.get(pid)
        if not name:
            m = interaction.guild.get_member(pid)
            name = m.display_name if m else str(pid)
        players.append(name)

    if not roles:
        return await interaction.followup.send("No roles found.", ephemeral=True)
    if not players:
        return await interaction.followup.send("No alive players found.", ephemeral=True)

    view = SpinWheelView(
        interaction.guild_id, interaction.guild, night_num,
        actions, roles, players)

    await interaction.followup.send(
        f"🎡 **Wheel — Night {night_num}**\n"
        f"Select from any dropdown to spin. Results go to mod-log only.",
        view=view, ephemeral=True)


# ====================== GAME SUMMARY ======================




async def _post_pregame_bloodboard(guild, guild_id: int, players: list, assignments: dict):
    """Generate and post the pre-game Blood Board to mod-log for approval."""
    rows     = db_get_assignments(guild_id)
    npcs     = db_get_npcs(guild_id)
    npc_map  = {n["npc_id"]: n["name"] for n in npcs}

    # Count teams
    village_count = sum(1 for pid, role in assignments.items()
                        if get_team(guild_id, role) == "village")
    wolf_count    = sum(1 for pid, role in assignments.items()
                        if get_team(guild_id, role) == "wolf")
    neutral_count = sum(1 for pid, role in assignments.items()
                        if get_team(guild_id, role) == "neutral")
    total         = len(assignments)

    # Player names list (no roles)
    player_names = []
    for pid in assignments:
        name = npc_map.get(pid)
        if not name:
            m = guild.get_member(pid)
            name = m.display_name if m else str(pid)
        player_names.append(name)

    prompt = (
        f"OPENING BLOOD BOARD — Whisperfall\n\n"
        f"The game is about to begin. {total} souls stand inside Whisperfall\'s borders tonight.\n"
        f"Not all of them are what they seem.\n\n"
        f"WHAT YOU ARE WRITING:\n"
        f"The opening announcement — read aloud before the first night falls.\n"
        f"Set the scene. Establish the dread. Make every player feel the village close around them.\n\n"
        f"WHAT YOU KNOW:\n"
        f"• {total} players total\n"
        f"• {village_count} belong to the village — ordinary people, or so they believe\n"
        f"• {wolf_count} hunt in darkness and wear village faces\n"
        f"• {neutral_count} serve only themselves\n\n"
        f"Do NOT name who is what. Do NOT hint at specific roles. This is pure atmosphere.\n\n"
        f"WRITING RULES:\n"
        f"• 3-4 paragraphs\n"
        f"• Ground everything in sound — what Whisperfall sounds like when something is wrong\n"
        f"• Vary sentence rhythm — one long unspooling sentence, then a short one that cuts\n"
        f"• Make every player feel watched before the first night even begins\n"
        f"• The final line must make them afraid to trust the person sitting next to them"
    )
    system = (
        "You are the voice of Whisperfall — a gothic village narrator writing the opening Blood Board "
        "for a social deduction game. This is the first thing players read. It sets everything.\n\n"
        "The opening Blood Board must accomplish three things:\n"
        "1. Make Whisperfall feel real — a place with history, texture, and dread\n"
        "2. Make the players feel the danger before a single move has been made\n"
        "3. End on a line that fractures trust before the game even starts\n\n"
        "Write as if you are the village itself, speaking. Not explaining. Warning."
    )

    narrative = await _claude(prompt, system, max_tokens=550)
    if not narrative:
        narrative = (
            f"*Whisperfall has always kept its secrets close.\n"
            f"Tonight, {total} souls gather within its borders — and not all of them are what they seem.\n"
            f"Listen carefully. The village is already whispering.*"
        )

    # Build the full pregame post
    header = f"🩸 **WHISPERFALL — The Game Begins**\n{'─' * 40}\n\n"
    full_post = (
        f"{header}{narrative}\n\n"
        f"{'─' * 40}\n"
        f"**👥 Players:** {total}\n"
        f"**🏘️ Villagers:** {village_count}\n"
        f"**🐺 Wolves:** {wolf_count}\n"
        f"**⚖️ Neutrals:** {neutral_count}"
    )

    # Post to mod-log for approval
    class PregameBBView(View):
        def __init__(self):
            super().__init__(timeout=None)  # No timeout — mod may take time reviewing
            edit_btn    = Button(label="✏️ Edit",               style=discord.ButtonStyle.blurple)
            post_btn    = Button(label="✅ Post to Blood Board", style=discord.ButtonStyle.green)
            discard_btn = Button(label="❌ Discard",             style=discord.ButtonStyle.danger)
            edit_btn.callback    = self.on_edit
            post_btn.callback    = self.on_post
            discard_btn.callback = self.on_discard
            self.add_item(edit_btn)
            self.add_item(post_btn)
            self.add_item(discard_btn)

        async def on_edit(self, interaction: discord.Interaction):
            class PregameEditModal(discord.ui.Modal, title="Edit Pre-Game Blood Board"):
                text = discord.ui.TextInput(
                    label="Narrative", style=discord.TextStyle.paragraph,
                    max_length=3900, default=narrative)
                async def on_submit(modal_self, modal_interaction):
                    edited    = modal_self.text.value
                    new_post  = (
                        f"🩸 **WHISPERFALL — The Game Begins**\n{'─' * 40}\n\n"
                        f"{edited}\n\n{'─' * 40}\n"
                        f"**👥 Players:** {total}  **🏘️ Village:** {village_count}  "
                        f"**🐺 Wolves:** {wolf_count}  **⚖️ Neutrals:** {neutral_count}"
                    )
                    view_new = PregameBBView()
                    view_new._post = new_post
                    embed_new = discord.Embed(
                        title="🩸 Pre-Game Blood Board — Edited",
                        description=edited[:4000], color=0x8B0000)
                    embed_new.set_footer(text="✏️ Edited — post when ready.")
                    await modal_interaction.response.edit_message(embed=embed_new, view=view_new)
            await interaction.response.send_modal(PregameEditModal())

        async def on_post(self, interaction: discord.Interaction):
            post = getattr(self, "_post", full_post)
            state_bb = db_get_state(interaction.guild_id) or {}
            bb_ch_id = state_bb.get("bb_channel_id") or 0
            bb_ch = interaction.guild.get_channel(bb_ch_id)
            if not bb_ch:
                return await interaction.response.send_message(
                    "❌ Blood Board channel not found.", ephemeral=True)
            await bb_ch.send(post)
            await interaction.response.edit_message(
                content="✅ Pre-game Blood Board posted to Blood Board channel.",
                embed=None, view=None)

        async def on_discard(self, interaction: discord.Interaction):
            await interaction.response.edit_message(
                content="❌ Pre-game Blood Board discarded.", embed=None, view=None)

    embed = discord.Embed(
        title       = "🩸 Pre-Game Blood Board — Draft",
        description = narrative[:4000],
        color       = 0x8B0000
    )
    embed.add_field(name="👥 Total",      value=str(total),          inline=True)
    embed.add_field(name="🏘️ Village",   value=str(village_count),  inline=True)
    embed.add_field(name="🐺 Wolves",    value=str(wolf_count),     inline=True)
    embed.add_field(name="⚖️ Neutrals",  value=str(neutral_count),  inline=True)
    embed.set_footer(text="Post to Village Chat or Discard.")

    state  = cached_get_state(guild_id)
    mod_ch = guild.get_channel(state.get("mod_log_channel_id") or 0)
    if mod_ch:
        await mod_ch.send(embed=embed, view=PregameBBView())



# ====================== PHASE HISTORY ======================
@tree.command(name="phase_history", description="Show every phase transition with timestamps")
@is_mod()
async def phase_history(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    log       = db_get_log(interaction.guild_id)
    night_num = db_get_night_num(interaction.guild_id)

    # Filter to phase transition events
    phase_keywords = ["began", "begins", "started", "night phase", "day phase",
                      "vote opened", "vote closed", "resolved", "game started"]
    phase_events = [
        (ts, ph, ev) for ts, ph, ev in log
        if any(kw in ev.lower() for kw in phase_keywords)
    ]

    if not phase_events:
        return await interaction.followup.send("No phase transitions recorded yet.", ephemeral=True)

    def fmt_ts(ts):
        try:
            return f"<t:{int(ts)}:t>"
        except Exception:
            return ts

    embed = discord.Embed(
        title       = f"📅 Phase History — {night_num} nights",
        color       = 0x9B59B6
    )

    lines = [f"{fmt_ts(ts)} **[{ph}]** {ev}" for ts, ph, ev in phase_events[-25:]]
    embed.description = "\n".join(lines) or "No phase events found."
    embed.set_footer(text="Showing last 25 phase transitions. All times in your local timezone.")
    await interaction.followup.send(embed=embed, ephemeral=True)


# ====================== SWAP ROLES ======================
@tree.command(name="swap_roles", description="Secretly swap two alive players\' roles")
@is_mod()
@app_commands.describe(
    player1="First player",
    player2="Second player")
async def swap_roles(interaction: discord.Interaction,
                     player1: discord.Member,
                     player2: discord.Member):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    rows  = db_get_assignments(interaction.guild_id)
    row1  = next((r for r in rows if r[0] == player1.id and r[2] == 1), None)
    row2  = next((r for r in rows if r[0] == player2.id and r[2] == 1), None)

    if not row1:
        return await interaction.followup.send(f"❌ **{player1.display_name}** is not an alive player.", ephemeral=True)
    if not row2:
        return await interaction.followup.send(f"❌ **{player2.display_name}** is not an alive player.", ephemeral=True)
    if player1.id == player2.id:
        return await interaction.followup.send("❌ Cannot swap a player with themselves.", ephemeral=True)

    role1, role2 = row1[1], row2[1]
    state = db_get_state(interaction.guild_id) or {}
    font  = get_guild_font(interaction.guild_id)

    # Swap in DB
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("UPDATE player_assignments SET role_name=? WHERE guild_id=? AND player_id=?",
              (role2, interaction.guild_id, player1.id))
    c.execute("UPDATE player_assignments SET role_name=? WHERE guild_id=? AND player_id=?",
              (role1, interaction.guild_id, player2.id))
    conn.commit()
    conn.close()
    invalidate_cache(interaction.guild_id)

    # Handle team changes for both players
    wolf_ch = interaction.guild.get_channel(state.get("wolf_channel_id") or 0)
    DENY_DEN = {"Bloodhound", "Wolf Pup"}

    for player, old_role, new_role in [(player1, role1, role2), (player2, role2, role1)]:
        old_team = get_team(interaction.guild_id, old_role)
        new_team = get_team(interaction.guild_id, new_role)

        if new_team == "wolf" and old_team != "wolf" and new_role not in DENY_DEN:
            if wolf_ch:
                await wolf_ch.set_permissions(player, view_channel=True, send_messages=True)
        elif old_team == "wolf" and new_team != "wolf":
            if wolf_ch:
                await wolf_ch.set_permissions(player, overwrite=None)

        # Rename private channel
        ch_id = row1[3] if player.id == player1.id else row2[3]
        if ch_id:
            priv_ch = interaction.guild.get_channel(ch_id)
            if priv_ch:
                try:
                    await priv_ch.edit(
                        name=f"🔒{player.display_name}-{new_role}".lower().replace(" ", "-")[:100])
                except Exception:
                    pass
                # Send new role card and pin it
                role_info = get_role_info(interaction.guild_id, new_role)
                embed     = build_role_card(player, new_role, role_info, font)
                role_msg  = await priv_ch.send(
                    fmt(f"🔀 Your role has been swapped by the mod."), embed=embed)
                try:
                    await role_msg.pin()
                except Exception:
                    pass

    await refresh_win_tracker(interaction.guild)
    await post_mod_log(interaction.guild,
        f"🔀 **Role Swap**\n"
        f"**{player1.display_name}**: {role1} → {role2}\n"
        f"**{player2.display_name}**: {role2} → {role1}")
    await interaction.followup.send(
        f"✅ Roles swapped — {player1.display_name} ↔ {player2.display_name}.", ephemeral=True)


# ====================== NIGHT ACTION OVERRIDE ======================
@tree.command(name="submit_action", description="Submit a night action on behalf of a player")
@is_mod()
@app_commands.describe(
    player="The player to submit for",
    action="The action type (e.g. doctor_save, seer, wolf_kill, _pass)",
    target="The target player (leave blank for no-target actions)")
async def submit_action(interaction: discord.Interaction,
                        player: discord.Member,
                        action: str,
                        target: discord.Member = None):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)

    state = db_get_state(interaction.guild_id) or {}
    if state.get("phase") != "night":
        return await interaction.response.send_message("❌ Not currently night phase.", ephemeral=True)

    rows      = db_get_assignments(interaction.guild_id)
    actor_row = next((r for r in rows if r[0] == player.id and r[2] == 1), None)
    if not actor_row:
        return await interaction.response.send_message(
            f"❌ **{player.display_name}** is not an alive player.", ephemeral=True)

    night_num = db_get_night_num(interaction.guild_id)
    target_id = target.id if target else None
    db_save_night_action(interaction.guild_id, night_num, player.id, action.strip(), target_id)

    target_str = f" → **{target.display_name}**" if target else ""
    await post_mod_log(interaction.guild,
        f"🔧 **Mod Action Override** — Night {night_num}\n"
        f"**Player:** {player.display_name} ({actor_row[1]})\n"
        f"**Action:** {action}{target_str}\n"
        f"*Submitted by mod on player\'s behalf.*")

    await interaction.response.send_message(
        f"✅ Action **{action}**{target_str} submitted for **{player.display_name}**.",
        ephemeral=True)

# ====================== NIGHT STATUS ======================
@tree.command(name="night_status", description="Show who has and hasn't submitted night actions")
@is_mod()
async def night_status(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    state = db_get_state(interaction.guild_id) or {}
    if state.get("phase") != "night":
        return await interaction.followup.send("❌ Not currently night phase.", ephemeral=True)

    night_num = db_get_night_num(interaction.guild_id)
    rows      = db_get_assignments(interaction.guild_id)
    actions   = db_get_night_actions(interaction.guild_id, night_num)
    npcs      = db_get_npcs(interaction.guild_id)
    npc_map   = {n["npc_id"]: n["name"] for n in npcs}

    acted_ids = {a[0] for a in actions}
    submitted, passed_, waiting = [], [], []

    for pid, role, is_alive, _ in rows:
        if not is_alive:
            continue
        # Skip roles that have no night action (day-ability roles and true passives)
        if role in NIGHT_NO_BUTTON_ROLES and role not in ROLE_VIEW_MAP:
            continue
        name = npc_map.get(pid)
        if not name:
            m    = interaction.guild.get_member(pid)
            name = m.display_name if m else str(pid)

        player_actions = [a for a in actions if a[0] == pid]
        ACTION_LABELS_NS = {
            "seer": "🔮 Investigating", "medium": "🌀 Checking alignment",
            "bloodhound": "🦴 Scanning", "doctor_save": "💊 Saving",
            "doctor_skip": "💊 Skipping", "surgeon_save": "🏥 Saving",
            "huntsman": "🏹 Protecting",
            "witch_save": "🧙 Save potion", "witch_kill": "🧙 Poison potion",
            "witch_skip": "🧙 Skipping", "alpha": "👑 Turn attempt",
            "elite_alpha": "👑⭐ Turn attempt", "wolf_pup": "🐾 Blocking",
            "bloodletter": "🩸 Marking", "cupid_bind": "💘 Binding",
            "agitator_frenzy": "📢 Frenzy activated", "clone": "🪞 Cloning",
            "shapeshifter": "🎭 Transforming", "white_wolf_kill": "🤍 Killing",
            "shadow_wolf": "🌑 Killing", "gravedigger_ack": "⚰️ Listening",
            "oracle_question": "🔯 Question submitted", "_pass": "💤 Passed",
        }
        if any(a[1] == "_pass" for a in player_actions):
            passed_.append(f"💤 {name} ({role})")
        elif pid in acted_ids:
            atype = player_actions[0][1]
            alabel = ACTION_LABELS_NS.get(atype, atype.replace("_"," ").title())
            submitted.append(f"✅ {name} ({role}) — {alabel}")
        else:
            waiting.append(f"⏳ {name} ({role})")

    embed = discord.Embed(
        title = f"🌙 Night {night_num} — Action Status",
        color = 0x2C3060
    )
    if submitted:
        embed.add_field(name=f"✅ Submitted ({len(submitted)})",
                        value="\n".join(submitted)[:1024], inline=False)
    if passed_:
        embed.add_field(name=f"💤 Passed ({len(passed_)})",
                        value="\n".join(passed_)[:1024], inline=False)
    if waiting:
        embed.add_field(name=f"⏳ Waiting ({len(waiting)})",
                        value="\n".join(waiting)[:1024], inline=False)
    embed.set_footer(text=f"Total alive: {len(submitted)+len(passed_)+len(waiting)}")
    await interaction.followup.send(embed=embed, ephemeral=True)


# ====================== MOD CHECK ======================
@tree.command(name="mod_dashboard", description="Post or refresh the mod dashboard in mod-log")
@is_mod()
async def mod_dashboard_cmd(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    # Force a fresh post (clear stored ID so it reposts)
    db_set_state(interaction.guild_id, mod_dashboard_msg_id=None)
    invalidate_cache(interaction.guild_id)
    await update_mod_dashboard(interaction.guild)
    await interaction.followup.send("✅ Dashboard posted to mod-log.", ephemeral=True)


@tree.command(name="modcheck", description="Quick snapshot of current game state for mods")
@is_mod()
@app_commands.describe(full="Show full player breakdown by team (default False)")
async def modcheck(interaction: discord.Interaction, full: bool = False):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    state     = db_get_state(interaction.guild_id) or {}
    rows      = db_get_assignments(interaction.guild_id)
    night_num = db_get_night_num(interaction.guild_id)
    phase     = state.get("phase", "day").capitalize()
    actions   = db_get_night_actions(interaction.guild_id, night_num)

    alive      = [r for r in rows if r[2] == 1]
    village_c  = sum(1 for r in alive if get_team(interaction.guild_id, r[1]) == "village")
    wolf_c     = sum(1 for r in alive if get_team(interaction.guild_id, r[1]) == "wolf")
    neutral_c  = sum(1 for r in alive if get_team(interaction.guild_id, r[1]) == "neutral")
    acted      = len({a[0] for a in actions})
    frenzy_day = state.get("agitator_frenzy_day")
    frenzy_active = frenzy_day and int(frenzy_day) == int(night_num)
    elim_count = int(state.get("agitator_elim_count") or 0)

    embed = discord.Embed(title="📋 Mod Check", color=0xF39C12)
    embed.add_field(name="Phase",      value=f"{phase} {night_num}", inline=True)
    embed.add_field(name="Alive",      value=str(len(alive)),        inline=True)
    embed.add_field(name="🏘️ Village", value=str(village_c),         inline=True)
    embed.add_field(name="🐺 Wolves",  value=str(wolf_c),            inline=True)
    embed.add_field(name="⚖️ Neutral", value=str(neutral_c),         inline=True)
    if phase == "Night":
        embed.add_field(name="Actions In", value=str(acted), inline=True)
    if frenzy_active:
        embed.add_field(name="⚡ Frenzy",
                        value=f"{elim_count}/2 eliminations done", inline=False)
    wolves_needed = max(0, village_c - wolf_c)
    embed.add_field(name="Wolves need", value=f"{wolves_needed} more elim(s) to win", inline=False)

    # ── Extra state flags ───────────────────────────────────────────────────
    flags = []

    # Time Lord status
    tl = next((r for r in alive if r[1] == "Time Lord"), None)
    night_dur    = state.get("night_duration", 43200)
    day_dur      = state.get("day_duration", 43200)
    tl_triggered = bool(state.get("time_lord_triggered", 0))
    if tl:
        m_tl = interaction.guild.get_member(tl[0])
        flags.append(f"⏰ **Time Lord** alive ({m_tl.display_name if m_tl else tl[0]}) — phases still 12h/12h (8-8 EST)")
    elif tl_triggered:
        flags.append(f"⏰ Time Lord is dead — phases shortened to {night_dur//3600:.0f}h night / {day_dur//3600:.0f}h day (relative timers active)")

    # Alpha / Elite Alpha turns remaining
    for r in alive:
        if r[1] == "Alpha":
            conn_a = sqlite3.connect(DB_FILE)
            c_a    = conn_a.cursor()
            c_a.execute("SELECT COUNT(*) FROM turn_log WHERE guild_id=? AND actor_id=? AND result=?",
                        (interaction.guild_id, r[0], "success"))
            used_a = c_a.fetchone()[0]
            conn_a.close()
            m_a = interaction.guild.get_member(r[0])
            remaining = 1 - used_a
            flags.append(f"👑 **Alpha** ({m_a.display_name if m_a else r[0]}) — {remaining} turn(s) remaining")
        elif r[1] == "Elite Alpha":
            conn_ea = sqlite3.connect(DB_FILE)
            c_ea    = conn_ea.cursor()
            c_ea.execute("SELECT night_num FROM turn_log WHERE guild_id=? AND actor_id=? AND result=? ORDER BY night_num",
                         (interaction.guild_id, r[0], "success"))
            ea_turns = [row[0] for row in c_ea.fetchall()]
            conn_ea.close()
            m_ea = interaction.guild.get_member(r[0])
            remaining = 2 - len(ea_turns)
            last_night = f", last used Night {ea_turns[-1]}" if ea_turns else ""
            flags.append(f"👑⭐ **Elite Alpha** ({m_ea.display_name if m_ea else r[0]}) — {remaining} turn(s) remaining{last_night}")

    # Wraith status
    wraith_rows = [r for r in alive if r[1] == "Wraith"]
    if wraith_rows:
        from discord import utils as _du
        wraith_state = db_get_state(interaction.guild_id) or {}
        w_names = []
        for wr in wraith_rows:
            m_w = interaction.guild.get_member(wr[0])
            w_names.append(m_w.display_name if m_w else str(wr[0]))
        flags.append(f"👻 **Wraiths** alive: {', '.join(w_names)} — need to outnumber village ({village_c}) AND wolves ({wolf_c})")

    # White Wolf strikes
    conn_ww = sqlite3.connect(DB_FILE)
    c_ww    = conn_ww.cursor()
    c_ww.execute("SELECT player_id, strikes FROM white_wolf_strikes WHERE guild_id=?",
                 (interaction.guild_id,))
    ww_rows = c_ww.fetchall()
    conn_ww.close()
    for ww_pid, ww_strikes in ww_rows:
        m_ww = interaction.guild.get_member(ww_pid)
        ww_name = m_ww.display_name if m_ww else str(ww_pid)
        flags.append(f"🤍 **White Wolf** ({ww_name}) — {ww_strikes}/3 strikes")

    if flags:
        embed.add_field(name="🔍 State Flags", value="\n".join(flags), inline=False)

    if full:
        # Full team breakdown — show who is who
        def pname(pid):
            m = interaction.guild.get_member(pid)
            return m.display_name if m else str(pid)
        v_lines = [f"🏘️ {pname(r[0])} — {r[1]}" for r in alive if get_team(interaction.guild_id, r[1]) == "village"]
        w_lines = [f"🐺 {pname(r[0])} — {r[1]}" for r in alive if get_team(interaction.guild_id, r[1]) == "wolf"]
        n_lines = [f"⚖️ {pname(r[0])} — {r[1]}" for r in alive if get_team(interaction.guild_id, r[1]) == "neutral"]
        if w_lines:
            embed.add_field(name="🐺 Wolf Team", value="\n".join(w_lines), inline=False)
        if v_lines:
            embed.add_field(name="🏘️ Village Team", value="\n".join(v_lines[:15]), inline=False)
        if n_lines:
            embed.add_field(name="⚖️ Neutrals", value="\n".join(n_lines), inline=False)

    await interaction.followup.send(embed=embed, ephemeral=True)


# ====================== REMIND PLAYERS ======================
# ====================== VOTE REMINDER ======================
@tree.command(name="vote_remind", description="Ping players who haven't voted yet in village chat")
@is_mod()
async def vote_remind(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)

    state = db_get_state(interaction.guild_id) or {}
    if state.get("phase") == "night":
        return await interaction.response.send_message("❌ It is currently night phase.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    rows      = db_get_assignments(interaction.guild_id)
    votes     = db_get_day_votes(interaction.guild_id)
    npcs      = db_get_npcs(interaction.guild_id)
    npc_map   = {n["npc_id"]: n["name"] for n in npcs}
    voted_ids = {v[0] for v in votes}

    not_voted = []
    for pid, _, is_alive, _ in rows:
        if is_alive and pid not in voted_ids and pid not in npc_map:
            m = interaction.guild.get_member(pid)
            if m:
                not_voted.append(m.mention)

    if not not_voted:
        return await interaction.followup.send("✅ All players have voted.", ephemeral=True)

    vc_ch = interaction.guild.get_channel(state.get("village_chat_ch_id") or 0)
    if vc_ch:
        await vc_ch.send(
            f"⏰ **Vote reminder** — the following players have not yet voted:\n"
            + " ".join(not_voted))

    await interaction.followup.send(
        f"✅ Reminded {len(not_voted)} player(s) in village chat.", ephemeral=True)

# ====================== BLOOD BOARD ======================

class BBEditModal(discord.ui.Modal, title="Edit Blood Board"):
    part1 = discord.ui.TextInput(
        label       = "Narrative — Part 1",
        style       = discord.TextStyle.paragraph,
        max_length  = 2000,
        required    = True,
    )
    part2 = discord.ui.TextInput(
        label       = "Narrative — Part 2 (optional)",
        style       = discord.TextStyle.paragraph,
        max_length  = 2000,
        required    = False,
    )

    def __init__(self, guild_id, current_narrative, deaths, night_num, counts):
        super().__init__()
        self.guild_id   = guild_id
        self.deaths     = deaths
        self.night_num  = night_num
        self.counts     = counts
        # Split existing narrative across both fields
        self.part1.default = current_narrative[:2000]
        self.part2.default = current_narrative[2000:4000] if len(current_narrative) > 2000 else ""

    async def on_submit(self, interaction: discord.Interaction):
        edited = (self.part1.value + (self.part2.value or "")).strip()
        view   = BloodBoardApprovalView(
            self.guild_id, edited, self.deaths,
            self.night_num, self.counts)
        embed = _build_bb_embed(edited, self.deaths, self.night_num, self.counts)
        embed.set_footer(text="✏️ Edited — review and post when ready.")
        await interaction.response.edit_message(embed=embed, view=view)


def _build_bb_embed(narrative, deaths, night_num, counts):
    """Build the Blood Board embed for mod preview."""
    embed = discord.Embed(
        title       = f"🩸 Blood Board — Night {night_num} Draft",
        description = narrative[:4000],
        color       = 0x8B0000
    )
    embed.add_field(name="💀 Deaths",     value="\n".join(f"💀 {n}" for n in deaths) if deaths else "None", inline=False)
    embed.add_field(name="👥 Remaining",  value=str(counts["total"]),   inline=True)
    embed.add_field(name="🏘️ Village",   value=str(counts["village"]), inline=True)
    embed.add_field(name="🐺 Wolves",    value=str(counts["wolf"]),    inline=True)
    embed.add_field(name="⚖️ Neutrals",  value=str(counts["neutral"]), inline=True)
    return embed


def _build_bb_post(narrative, deaths, night_num, counts):
    """Build the final formatted BB post for the channel."""
    header = f"🩸 **BLOOD BOARD — Morning of Day {night_num + 1}**\n{'─' * 40}\n\n"
    death_section = (
        "\n\n**The fallen:**\n" + "\n".join(f"💀 {n}" for n in deaths)
        if deaths else
        "\n\n*The village woke intact. No one was taken last night.*"
    )
    counts_section = (
        f"\n\n{'─' * 40}\n"
        f"**👥 Remaining:** {counts['total']}  "
        f"**🏘️ Village:** {counts['village']}  "
        f"**🐺 Wolves:** {counts['wolf']}  "
        f"**⚖️ Neutrals:** {counts['neutral']}"
    )
    return header + narrative + death_section + counts_section


class BloodBoardApprovalView(View):
    """Posted to mod-log — mod can edit, regenerate, post, or discard the Blood Board."""
    def __init__(self, guild_id: int, narrative: str, deaths: list, night_num: int, counts: dict,
                 action_hints: list = None):
        super().__init__(timeout=None)  # No timeout — mod may take time reviewing
        self.guild_id     = guild_id
        self.narrative    = narrative
        self.deaths       = deaths
        self.night_num    = night_num
        self.counts       = counts
        self.action_hints = action_hints or []

        edit_btn    = Button(label="✏️ Edit",                style=discord.ButtonStyle.blurple)
        regen_btn   = Button(label="🔄 Regenerate",          style=discord.ButtonStyle.secondary)
        post_btn    = Button(label="✅ Post to Blood Board",  style=discord.ButtonStyle.green)
        discard_btn = Button(label="❌ Discard",              style=discord.ButtonStyle.danger)
        edit_btn.callback    = self.on_edit
        regen_btn.callback   = self.on_regen
        post_btn.callback    = self.on_post
        discard_btn.callback = self.on_discard
        self.add_item(edit_btn)
        self.add_item(regen_btn)
        self.add_item(post_btn)
        self.add_item(discard_btn)

    async def on_edit(self, interaction: discord.Interaction):
        modal = BBEditModal(
            self.guild_id, self.narrative,
            self.deaths, self.night_num, self.counts)
        await interaction.response.send_modal(modal)

    async def on_regen(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="🔄 Regenerating Blood Board narrative...", embed=None, view=None)
        death_count    = len(self.deaths)
        activity_level = len(self.action_hints)
        tone = "dark and heavy" if death_count >= 2 else (
               "tense and foreboding" if death_count == 1 else
               "eerily quiet and suspicious")
        activity_desc = "many shadows moved through the village" if activity_level > 6 else (
                        "several presences stirred in the dark" if activity_level > 3 else
                        "the night was unusually still, yet not entirely empty")
        prompt = (
            f"You are the narrator of a Mafia/Werewolf game set in the village of Whisperfall.\n\n"
            f"Write the Blood Board — a morning announcement read aloud to the village after Night {self.night_num}.\n\n"
            f"Style: Gothic, atmospheric, literary. The village is defined by sound.\n"
            f"Tone: {tone}. Activity: {activity_desc}.\n"
            f"Hints to weave in naturally:\n"
            + "\n".join(f"- {h}" for h in self.action_hints) +
            f"\n\nDeaths tonight: {death_count} player(s).\n"
            f"{'Names listed separately — no names or roles in narrative.' if self.deaths else 'No deaths tonight.'}"
            f"\n\nLength: 3-5 paragraphs. End on a line that lingers.\n"
            f"Do NOT use the word wolf or werewolf."
        )
        system = (
            "You are a master storyteller writing atmospheric Blood Board announcements for Whisperfall. "
            "Write with dread, restraint, and precision. Never state roles or mechanics directly."
        )
        new_narrative = await _claude(prompt, system, max_tokens=600)
        if not new_narrative:
            new_narrative = self.narrative
        self.narrative = new_narrative
        view  = BloodBoardApprovalView(self.guild_id, new_narrative, self.deaths,
                                       self.night_num, self.counts, self.action_hints)
        embed = _build_bb_embed(new_narrative, self.deaths, self.night_num, self.counts)
        embed.set_footer(text="🔄 Regenerated — review and post when ready.")
        await interaction.edit_original_response(embed=embed, view=view, content=None)

    async def on_post(self, interaction: discord.Interaction):
        bb_ch_id_r = (db_get_state(interaction.guild_id) or {}).get("bb_channel_id") or 0
        bb_ch = interaction.guild.get_channel(bb_ch_id_r)
        if not bb_ch:
            return await interaction.response.send_message(
                "❌ Blood Board channel not found.", ephemeral=True)
        post = _build_bb_post(self.narrative, self.deaths, self.night_num, self.counts)
        await bb_ch.send(post)
        await interaction.response.edit_message(
            content=f"✅ Blood Board posted to <#{bb_ch_id_r}>.",
            embed=None, view=None)

        # Mark night BB done and refresh dashboard
        db_set_state(interaction.guild_id, night_bb_done=1)
        invalidate_cache(interaction.guild_id)
        safe_task(update_mod_dashboard(interaction.guild), "dashboard_bb_post")

        # NPC reactions to Blood Board in village chat
        safe_task(_npc_react_to_bloodboard(
            interaction.guild, interaction.guild_id,
            self.deaths, self.night_num), "npc_bb_react")

    async def on_discard(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="❌ Blood Board discarded.", embed=None, view=None)



@tree.command(name="dayboard", description="Generate the day Blood Board narrative for mod approval")
@is_mod()
async def dayboard(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    guild_id  = interaction.guild_id
    night_num = db_get_night_num(guild_id)
    rows      = db_get_assignments(guild_id)
    npcs      = db_get_npcs(guild_id)
    npc_map   = {n["npc_id"]: n["name"] for n in npcs}

    def get_name(pid):
        npc = npc_map.get(pid)
        if npc: return npc
        m = interaction.guild.get_member(pid)
        return m.display_name if m else str(pid)

    # ── Build vote breakdown ──────────────────────────────────────────────
    votes = db_get_day_votes(guild_id)
    tally = {}   # target_id -> list of voter names
    for voter_id, target_id, *_ in votes:
        if target_id:
            tally.setdefault(target_id, []).append(get_name(voter_id))

    sorted_targets = sorted(tally.items(), key=lambda x: len(x[1]), reverse=True)

    total_votes = sum(len(v) for v in tally.values())
    vote_lines = []
    for target_id, voters in sorted_targets:
        name       = get_name(target_id)
        voter_list = ", ".join(voters)
        pct        = round((len(voters) / total_votes * 100)) if total_votes else 0
        vote_lines.append(f"**{name}** — {len(voters)} vote(s) ({pct}%) from: {voter_list}")

    # ── Find who was eliminated today via elimination_log ─────────────────
    # elim_type='vote' means hung; also catches mod_kill on day phase
    elim_log   = db_get_elimination_log(guild_id)
    role_by_pid = {r[0]: r[1] for r in rows}

    # Build a set of player_ids -> display name for day eliminations this day number
    day_dead = []  # list of (display_name, role_name, elim_type)
    for pid, role_name, reason, elim_type, day_or_night, elim_at in elim_log:
        if elim_type in ("vote", "mod_kill", "jokester") and day_or_night == night_num:
            dname = get_name(pid)
            role  = role_by_pid.get(pid, role_name or "Unknown")
            day_dead.append((dname, role, elim_type))

    # Fallback: if elimination_log doesn't have day entries yet (e.g. /eliminate just ran),
    # scan game log for Day {night_num} eliminations
    if not day_dead:
        import re as _re2
        recent_log = db_get_log(guild_id)
        seen_names = set()
        for _, phase, event in reversed(recent_log):
            if f"Day {night_num}" in phase and "eliminated" in event.lower():
                match = _re2.search(r"\*\*(.+?)\*\*\s+eliminated", event)
                if match:
                    name = match.group(1).strip()
                    if name not in seen_names:
                        seen_names.add(name)
                        # Try to match to a player row for role lookup
                        role = "Unknown"
                        for pid, rn, is_alive, _ in rows:
                            m = interaction.guild.get_member(pid)
                            pname = npc_map.get(pid) or (m.display_name if m else "")
                            if pname.lower() == name.lower():
                                role = rn
                                break
                        day_dead.append((name, role, "vote"))

    deaths     = [d[0] for d in day_dead]
    death_count = len(deaths)

    # ── Current alive counts after eliminations ───────────────────────────
    counts = {
        "total":   sum(1 for r in rows if r[2] == 1),
        "village": sum(1 for r in rows if r[2] == 1 and get_team(guild_id, r[1]) == "village"),
        "wolf":    sum(1 for r in rows if r[2] == 1 and get_team(guild_id, r[1]) == "wolf"),
        "neutral": sum(1 for r in rows if r[2] == 1 and get_team(guild_id, r[1]) == "neutral"),
    }

    # ── Build death atmosphere hints (role-aware, cause-aware) ────────────
    day_death_hints = []
    for dname, drole, dtype in day_dead:
        atm   = _get_role_atmosphere_hint(drole)
        cause = (
            "The village chose this. They pointed, they voted, they watched."
            if dtype == "vote" else
            "They were removed — not by the crowd's vote, but by a quieter authority."
            if dtype == "mod_kill" else
            "Their death came with a final act of vengeance — the village's laughter died with them."
            if dtype == "jokester" else
            "They were taken."
        )
        day_death_hints.append(f"{atm} — {cause}")

    # ── Surgeon-specific: note if a potential protector was hung ─────────
    # This informs Claude that saves are now reduced — the village loses healing
    surgeon_hung = any(drole == "Surgeon" for _, drole, _ in day_dead)
    doctor_hung  = any(drole == "Doctor"  for _, drole, _ in day_dead)
    healer_note  = ""
    if surgeon_hung:
        healer_note = ("One of those voted out today was meticulous, methodical — someone whose careful hands "
                       "had more than once kept another from the edge. Whisperfall is less protected than it was this morning.")
    elif doctor_hung:
        healer_note = ("One of those voted out today was steady in a crisis — the kind of person others turned to "
                       "without knowing why. The village will feel the absence of that steadiness tonight.")

    # ── Build Claude prompt ───────────────────────────────────────────────
    vote_summary_text = "\n".join(vote_lines) if vote_lines else "No votes were cast."

    tone = (
        "heavy and final — the village has spoken and someone has paid the price" if death_count >= 1
        else "uneasy — the vote concluded but left the village unsatisfied and divided"
    )

    prompt = (
        f"DAY {night_num} BLOOD BOARD — Whisperfall\n\n"
        f"The village has voted. Fingers were pointed. A name was chosen.\n\n"
        f"WHAT HAPPENED:\n"
        f"• {death_count} soul(s) were voted out by the village today\n"
        + (f"• Names will be listed separately after the narrative — do not include them in the prose\n"
           f"• For each person eliminated, weave ONE atmospheric hint about who they were and how they went:\n"
           + "\n".join(f"  - {h}" for h in day_death_hints)
           + (f"\n\nADDITIONAL NOTE — {healer_note}" if healer_note else "")
           if deaths else
           f"• No one was eliminated — the vote failed to reach consensus\n") +
        f"\n\nWHAT TO CAPTURE:\n"
        + ("• The weight of the crowd making a choice — the moment a name becomes a verdict\n"
           "• What Whisperfall sounds like in the minutes after the hanging — the silence, the dispersal, the averted eyes\n"
           "• The doubt that follows every public execution — did they get it right? Did the village just help its enemy?\n"
           if deaths else
           "• The tension of a village that could not agree — what does it mean when no name rises above the others?\n"
           "• The relief that quickly curdles. Everyone is still here. But something is wrong.\n"
           "• The whispers that start before the crowd even disperses\n") +
        f"\nVOTE BREAKDOWN (use to inform atmosphere only — do not list votes verbatim):\n"
        f"{vote_summary_text}\n\n"
        f"WRITING RULES:\n"
        f"• 3-4 paragraphs\n"
        f"• Daytime is louder than night — voices, crowds, the scrape of chairs, names spoken aloud in the square\n"
        f"• But Whisperfall's whispers never fully stop, even in daylight\n"
        f"• Vary sentence rhythm — short punchy lines carry the verdict; long ones carry the doubt\n"
        f"• Never name roles, abilities, teams, or mechanics\n"
        f"• Never use the word wolf or werewolf\n"
        f"• End on a line that makes the coming night feel inevitable and terrifying"
    )

    system = (
        "You are the voice of Whisperfall writing day phase Blood Board announcements after the village vote. "
        "Day writing is different from night — louder, more visceral, crowd-driven. "
        "Where night Blood Boards are about what moved unseen, day Blood Boards are about what the village chose "
        "to do in broad daylight — and what that choice costs.\n\n"
        "WHAT MAKES A GREAT DAY BLOOD BOARD:\n"
        "- It captures the specific horror of democratic violence — a village pointing at one of their own\n"
        "- It makes the reader feel the crowd: the noise, the moment it tips, the terrible quiet after\n"
        "- It plants doubt. Was it the right call? The reader shouldn't know.\n"
        "- If a protector or healer was hung, the narrative should subtly mourn the loss of safety — "
        "  not by naming what they did, but by the feeling of exposure that follows their absence\n"
        "- The final line makes the coming night feel inevitable\n\n"
        "WHAT RUINS IT:\n"
        "- Making the elimination feel clean or justified\n"
        "- Forgetting the sounds — Whisperfall always has sounds\n"
        "- A hopeful ending\n"
        "- Starting too many sentences the same way\n\n"
        "Write as if the village is reading this posted on the notice board before night falls."
    )

    narrative = await _claude(prompt, system, max_tokens=700)
    if not narrative:
        narrative = (
            f"*The village square of Whisperfall had not been this loud in years.\n"
            f"Voices crossed over one another. Names were spoken like accusations.\n"
            f"And when it was done, the crowd dispersed in silence — each person wondering "
            f"if they had just helped the village, or handed a victory to something far worse.*"
        )

    # ── Build mod-log embed ───────────────────────────────────────────────
    embed = _build_dayboard_embed(narrative, deaths, vote_lines, night_num, counts)
    embed.set_footer(text="Review, edit if needed, then post to Blood Board channel.")

    view = DayBoardApprovalView(
        guild_id, narrative, deaths, vote_lines, night_num, counts)

    state_mod = cached_get_state(guild_id)
    mod_ch    = interaction.guild.get_channel(state_mod.get("mod_log_channel_id") or 0)
    if mod_ch:
        await mod_ch.send(embed=embed, view=view)

    await interaction.followup.send(
        "✅ Day Blood Board generated and posted to mod-log for review.", ephemeral=True)


class DayBoardEditModal(discord.ui.Modal, title="Edit Day Blood Board"):
    narrative = discord.ui.TextInput(
        label      = "Narrative",
        style      = discord.TextStyle.paragraph,
        max_length = 3900,
        required   = True,
    )

    def __init__(self, guild_id, current_narrative, deaths, vote_lines, night_num, counts):
        super().__init__()
        self.guild_id   = guild_id
        self.deaths     = deaths
        self.vote_lines = vote_lines
        self.night_num  = night_num
        self.counts     = counts
        self.narrative.default = current_narrative

    async def on_submit(self, interaction: discord.Interaction):
        edited = self.narrative.value
        view   = DayBoardApprovalView(
            self.guild_id, edited, self.deaths,
            self.vote_lines, self.night_num, self.counts)
        embed  = _build_dayboard_embed(
            edited, self.deaths, self.vote_lines, self.night_num, self.counts)
        embed.set_footer(text="✏️ Edited — review and post when ready.")
        await interaction.response.edit_message(embed=embed, view=view)


def _build_dayboard_embed(narrative, deaths, vote_lines, night_num, counts):
    embed = discord.Embed(
        title       = f"☀️ Day Blood Board — Day {night_num} Draft",
        description = narrative[:4000],
        color       = 0xE67E22
    )
    embed.add_field(
        name  = "🗳️ Vote Results",
        value = "\n".join(vote_lines)[:1024] if vote_lines else "No votes cast.",
        inline= False
    )
    embed.add_field(
        name  = "💀 Eliminated",
        value = "\n".join(f"💀 {n}" for n in deaths) if deaths else "None",
        inline= False
    )
    embed.add_field(name="👥 Remaining", value=str(counts["total"]),   inline=True)
    embed.add_field(name="🏘️ Village",  value=str(counts["village"]), inline=True)
    embed.add_field(name="🐺 Wolves",   value=str(counts["wolf"]),    inline=True)
    embed.add_field(name="⚖️ Neutrals", value=str(counts["neutral"]), inline=True)
    return embed


def _build_dayboard_post(narrative, deaths, vote_lines, night_num, counts):
    header       = f"☀️ **DAY BLOOD BOARD — Day {night_num}**\n{'─' * 40}\n\n"
    death_section = (
        "\n\n**The eliminated:**\n" + "\n".join(f"💀 {n}" for n in deaths)
        if deaths else
        "\n\n*The village reached no final decision. No one was taken today.*"
    )
    vote_section = (
        "\n\n**Vote breakdown:**\n" + "\n".join(vote_lines)
        if vote_lines else ""
    )
    counts_section = (
        f"\n\n{'─' * 40}\n"
        f"**👥 Remaining:** {counts['total']}  "
        f"**🏘️ Village:** {counts['village']}  "
        f"**🐺 Wolves:** {counts['wolf']}  "
        f"**⚖️ Neutrals:** {counts['neutral']}"
    )
    return header + narrative + death_section + vote_section + counts_section


class DayBoardApprovalView(View):
    """Posted to mod-log — mod can edit, regenerate, post, or discard the Day Blood Board."""
    def __init__(self, guild_id, narrative, deaths, vote_lines, night_num, counts):
        super().__init__(timeout=None)  # No timeout — mod may take time reviewing
        self.guild_id   = guild_id
        self.narrative  = narrative
        self.deaths     = deaths
        self.vote_lines = vote_lines
        self.night_num  = night_num
        self.counts     = counts

        edit_btn    = Button(label="✏️ Edit",                style=discord.ButtonStyle.blurple)
        regen_btn   = Button(label="🔄 Regenerate",          style=discord.ButtonStyle.secondary)
        post_btn    = Button(label="✅ Post to Blood Board",  style=discord.ButtonStyle.green)
        discard_btn = Button(label="❌ Discard",              style=discord.ButtonStyle.danger)
        edit_btn.callback    = self.on_edit
        regen_btn.callback   = self.on_regen
        post_btn.callback    = self.on_post
        discard_btn.callback = self.on_discard
        self.add_item(edit_btn)
        self.add_item(regen_btn)
        self.add_item(post_btn)
        self.add_item(discard_btn)

    async def on_edit(self, interaction: discord.Interaction):
        modal = DayBoardEditModal(
            self.guild_id, self.narrative, self.deaths,
            self.vote_lines, self.night_num, self.counts)
        await interaction.response.send_modal(modal)

    async def on_regen(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="🔄 Regenerating Day Blood Board narrative...", embed=None, view=None)
        death_count = len(self.deaths)
        vote_summary = "\n".join(self.vote_lines) if self.vote_lines else "No votes recorded."
        tone = (
            "heavy and final — the village has spoken and someone has paid the price" if death_count >= 1
            else "uneasy — the vote concluded but left the village unsatisfied and divided"
        )
        prompt = (
            f"You are the narrator of a Mafia/Werewolf game set in the village of Whisperfall.\n\n"
            f"Write the Day Blood Board — posted after the village vote on Day {self.night_num}.\n\n"
            f"Tone: {tone}.\n"
            f"The vote has concluded. {death_count} player(s) were eliminated today.\n"
            f"{'Names listed separately — write about the act of the vote.' if death_count else 'No one was eliminated.'}"
            f"\n\nDo NOT reveal roles, teams, or mechanics.\n"
            f"Do NOT use the word wolf or werewolf.\n"
            f"2-3 paragraphs. End on something that makes players dread tonight."
        )
        system = (
            "You are a master storyteller writing day phase Blood Board announcements for Whisperfall. "
            "Never state roles, mechanics, or team names directly."
        )
        new_narrative = await _claude(prompt, system, max_tokens=500)
        if not new_narrative:
            new_narrative = self.narrative
        self.narrative = new_narrative
        view  = DayBoardApprovalView(self.guild_id, new_narrative, self.deaths,
                                     self.vote_lines, self.night_num, self.counts)
        embed = _build_dayboard_embed(new_narrative, self.deaths, self.vote_lines, self.night_num, self.counts)
        embed.set_footer(text="🔄 Regenerated — review and post when ready.")
        await interaction.edit_original_response(embed=embed, view=view, content=None)

    async def on_post(self, interaction: discord.Interaction):
        bb_ch_id_r = (db_get_state(interaction.guild_id) or {}).get("bb_channel_id") or 0
        bb_ch = interaction.guild.get_channel(bb_ch_id_r)
        if not bb_ch:
            return await interaction.response.send_message(
                "❌ Blood Board channel not found.", ephemeral=True)
        post = _build_dayboard_post(
            self.narrative, self.deaths, self.vote_lines,
            self.night_num, self.counts)
        await bb_ch.send(post)
        await interaction.response.edit_message(
            content="✅ Day Blood Board posted to Blood Board channel.",
            embed=None, view=None)
        # Mark day BB done and refresh dashboard
        db_set_state(interaction.guild_id, day_bb_done=1)
        invalidate_cache(interaction.guild_id)
        safe_task(update_mod_dashboard(interaction.guild), "dashboard_daybb_post")

    async def on_discard(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="❌ Day Blood Board discarded.", embed=None, view=None)

def _get_role_atmosphere_hint(role_name: str) -> str:
    """Returns an atmospheric description of a role for Claude to weave into the Blood Board.
    Never reveals the actual role name — only atmosphere, behavior, essence."""
    hints = {
        # Village
        "Villager":        "someone ordinary, unremarkable, the kind of person you never think about until they're gone",
        "Seer":            "someone who watched more than they spoke, who seemed to know things they shouldn't",
        "Doctor":          "someone whose presence made others feel safer, a steadying hand in uncertain times",
        "Surgeon":         "someone precise and careful, methodical, who approached every situation with calculation",
        "Huntsman":        "someone protective by nature, who always seemed to be watching over someone else",
        "Medium":          "someone who seemed to carry the weight of things unseen, always listening to silence",
        "Hermit":          "someone reclusive, who kept to the edges, who you noticed more by their absence",
        "Agitator":        "someone who stirred the room when they entered, a voice that demanded to be heard",
        "Shapeshifter":    "someone who was hard to pin down, different things to different people",
        "Cupid":           "someone who connected others, who saw bonds where no one else looked",
        "Clone":           "someone who watched and mirrored, who seemed to absorb the people around them",
        "Insomniac":       "someone restless, always awake when others slept, always listening through the walls",
        "Sheriff":         "someone who stood their ground, who didn't flinch when others looked away",
        "Governor":        "someone with quiet authority, whose word carried weight in any room",
        "Gravedigger":     "someone who dealt in endings, who understood death as a kind of record-keeping",
        "Jafar":           "someone unpredictable, who seemed to wear a different face each day",
        "Lycan":           "someone who felt slightly off, whose edges didn't quite match the shape of the room",
        "Elder":           "someone old in ways that had nothing to do with age, who had survived things they never spoke of",
        "Mayor":           "someone who carried the village in their bearing, who others looked to without being asked",
        "Drunk":           "someone loose at the seams, who said things they didn't mean and meant things they didn't say",
        "Prostitute":      "someone who moved through people like water, who knew more than they let on",
        "Virgin":          "someone careful, guarded, who kept themselves apart from the worst of things",
        "Pothead":         "someone easy-going, underestimated, who never seemed to be paying attention until it mattered",
        "Diseased":        "someone who carried something invisible, who seemed fine right up until they weren't",
        "Time Lord":       "someone who seemed to exist slightly out of step with everyone else",
        "Traitor":         "someone who smiled at everyone, who belonged everywhere and nowhere",
        "Village Idiot":   "someone who spoke in riddles, who the village laughed at but never truly understood",
        "Village Jokester":"someone who kept the mood light, who used laughter as armor",
        "White Wolf":      "someone who operated alone, who never quite fit with any group",
        # Wolf
        "Wolf":            "someone who hunted in plain sight, whose warmth was practiced and precise",
        "Alpha":           "someone with quiet dominance, who others deferred to without knowing why",
        "Elite Alpha":     "someone whose influence ran deeper than anyone realized",
        "Blessed Wolf":    "someone whose charm seemed almost supernatural, who people trusted without reason",
        "Bloodhound":      "someone who tracked things, who noticed what others missed",
        "Bloodletter":     "someone who marked their territory in ways invisible to the eye",
        "Crazed Wolf":     "someone with barely contained energy, whose calm felt like the surface of something violent",
        "Dire Wolf":       "someone who formed attachments quickly and deeply, who fought hardest for what they claimed",
        "Echo-Stalker":    "someone who moved like a shadow, who knew your patterns before you noticed them",
        "Shadow Wolf":     "someone who remembered every slight, who never forgot a face",
        "Werekitten":      "someone disarmingly gentle, whose presence made the room softer",
        "Wolf Pup":        "someone young in their cruelty, still learning the shape of what they were becoming",
        # Neutral
        "Wraith":          "something that was never quite a person — more like a weight that settled on the village",
        "Oracle":          "someone who asked questions no one else dared, who seemed to already know the answers",
        "Warlock":         "someone who dealt in favors, who always had something you needed",
        "Fairy Elf":       "someone luminous and strange, who left things slightly better and slightly wrong",
        "Witch":           "someone who worked in quiet — remedies and poisons, two sides of the same hand",
    }
    return hints.get(role_name, "someone whose role in Whisperfall was never fully understood")


@tree.command(name="bloodboard", description="Generate the nightly Blood Board narrative for mod approval")
@is_mod()
async def bloodboard(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    guild_id  = interaction.guild_id
    night_num = db_get_night_num(guild_id)
    actions   = db_get_night_actions(guild_id, night_num)
    rows      = db_get_assignments(guild_id)
    npcs      = db_get_npcs(guild_id)
    npc_map   = {n["npc_id"]: n["name"] for n in npcs}

    def get_name(pid):
        npc = npc_map.get(pid)
        if npc: return npc
        m = interaction.guild.get_member(pid)
        return m.display_name if m else str(pid)

    def get_role(pid):
        r = next((r for r in rows if r[0] == pid), None)
        return r[1] if r else "Unknown"

    # ── Build action summary for Claude ──────────────────────────────────
    action_map = {}
    for actor_id, action_type, target_id in actions:
        if not action_type.startswith("_"):
            action_map.setdefault(action_type, []).append((actor_id, target_id))

    # Find who actually died this night from the game log
    deaths = []
    recent_log = db_get_log(guild_id)
    for _, phase, event in reversed(recent_log):
        if f"Night {night_num}" in phase or f"Day {night_num}" in phase:
            if "eliminated" in event.lower() or "killed" in event.lower():
                # Extract name from log entry
                import re
                match = re.search(r"\*\*(.+?)\*\*\s+eliminated", event)
                if match:
                    deaths.append(match.group(1).strip())

    # Build context for Claude to write the narrative
    action_hints = []
    if "wolf" in str(action_map).lower() or any(
        t in action_map for t in ["wolf_kill", "crazed_wolf_1", "echo_stalk"]):
        action_hints.append("wolves coordinated and moved through the village")
    if any(t in action_map for t in ["doctor_save", "surgeon_save", "huntsman_protect"]):
        action_hints.append("at least one villager was protected from harm")
    if "witch_save" in action_map:
        action_hints.append("a mysterious intervention saved someone from death")
    if "witch_kill" in action_map:
        action_hints.append("something sinister and quiet claimed a life — no struggle, no sound")
    if "seer" in action_map:
        action_hints.append("someone spent the night watching, seeking truth in the darkness")
    if "medium" in action_map:
        action_hints.append("someone communed with forces beyond the living to seek alignment")
    if "cupid" in str(action_map):
        action_hints.append("two souls were bound together in the night, unaware of what ties them")
    if "hermit" in action_map:
        action_hints.append("someone sought refuge and was hidden from the chaos")
    if "bloodletter" in action_map:
        action_hints.append("a dark marking was left on someone — invisible to most eyes")
    if "alpha" in action_map or "elite_alpha" in action_map:
        action_hints.append("something in the village shifted — an allegiance tested, a soul tempted")
    if "wolf_pup" in action_map:
        action_hints.append("one villager found their usual instincts dulled, as if something blocked them")
    if "agitator" in action_map:
        action_hints.append("an unseen hand stirred unrest, setting tomorrow's chaos in motion")
    if "wraith_mark" in action_map:
        action_hints.append("something that is not the wolves and not the village moved through Whisperfall — silent, invisible, purposeful. It left no trace except a feeling that something has been counted.")
    if "wraith_kill" in action_map or (db_get_wraith_state(guild_id).get("kill_agreed", 0) >= 2):
        action_hints.append("something collected what it had been counting. More than one soul felt the weight of a judgment they did not know was coming.")
    if "shapeshifter" in action_map:
        action_hints.append("someone shed the skin they had been wearing — the village will not know who they are looking at tomorrow")
    if "dire_wolf" in action_map:
        action_hints.append("a bond was formed in the dark — not love, something older and more dangerous")
    if "white_wolf_kill" in action_map:
        action_hints.append("something moved alone through the village — not with the pack, not against it, entirely its own")
    if "bloodhound" in action_map:
        action_hints.append("a nose pressed to the ground, a trail followed — something was identified that wished to remain hidden")
    if "echo_stalk" in action_map:
        action_hints.append("a presence followed someone home last night without their knowing — patient, watchful, recording")
    if "crazed_wolf_1" in action_map or "crazed_wolf_2" in action_map:
        action_hints.append("the night felt fractured — too many edges, too many directions at once")

    # Wolf death hints — check if any wolf died this night
    wolf_deaths = []
    for pid, role, is_alive, _ in rows:
        if not is_alive and get_team(guild_id, role) == "wolf":
            recent_log = db_get_log(guild_id)
            for _, phase, event in reversed(recent_log):
                if f"Night {night_num}" in phase and "eliminated" in event.lower():
                    wolf_deaths.append((pid, role))
                    break
    if wolf_deaths:
        for _, role in wolf_deaths:
            hint = _get_role_atmosphere_hint(role)
            action_hints.append(
                f"something that moved with the darkness did not make it back — {hint}")

    death_count  = len(deaths)
    activity_level = len(action_map)

    tone = "dark and heavy" if death_count >= 2 else (
           "tense and foreboding" if death_count == 1 else
           "eerily quiet and suspicious")

    activity_desc = "many shadows moved through the village" if activity_level > 6 else (
                    "several presences stirred in the dark" if activity_level > 3 else
                    "the night was unusually still, yet not entirely empty")

    # Night number affects tone — early game vs late game feel different
    alive_count_bb = sum(1 for r in rows if r[2] == 1)
    dead_count_bb  = sum(1 for r in rows if r[2] == 0)
    game_stage = (
        f"This is Night {night_num}. The village still trusts each other. The weight has not settled yet. "
        f"{alive_count_bb} souls remain in Whisperfall."
        if night_num <= 2 else
        f"This is Night {night_num}. The village is fracturing. Suspicion has taken root. "
        f"{alive_count_bb} remain. {dead_count_bb} are gone."
        if night_num <= 4 else
        f"This is Night {night_num}. The endgame. Only {alive_count_bb} survivors. "
        f"The survivors know each other too well and not well enough. Every word is a calculation."
    )

    death_hints = []
    for actor_id, action_type, target_id in actions:
        if action_type in ("wolf_kill", "witch_kill", "white_wolf_kill", "shadow_wolf_kill", "crazed_wolf_1", "crazed_wolf_2"):
            if target_id and not any(r[2] == 1 for r in rows if r[0] == target_id):
                team = "wolf" if get_team(guild_id, get_role(actor_id)) == "wolf" else "village"
                death_hints.append(f"A {team}-aligned soul: {_get_role_atmosphere_hint(get_role(target_id))}")

    prompt = (
        f"NIGHT {night_num} BLOOD BOARD — Whisperfall\n\n"
        f"Game stage context: {game_stage}\n"
        f"Tonight\'s tone: {tone}\n"
        f"Activity level: {activity_desc}\n\n"
        f"WHAT HAPPENED LAST NIGHT (translate each into atmosphere — never state directly):\n"
        + "\n".join(f"• {h}" for h in action_hints) +
        (f"\n\nDEATHS: {death_count} soul(s) perished tonight. Names will be listed separately after the narrative.\n"
         f"For each death, embed ONE atmospheric hint about who this person was — their presence, their absence, what Whisperfall loses:\n"
         + "\n".join(f"• {h}" for h in death_hints)
         if death_count > 0 else
         f"\n\nNO DEATHS: The village woke whole. But whole does not mean safe.\n"
         f"Write the relief — then undermine it. Something was stopped. Something almost happened. Why?") +
        f"\n\nWRITING RULES:\n"
        f"• 4-5 paragraphs\n"
        f"• Vary sentence rhythm — short punchy sentences after long flowing ones. Silence lands harder after noise.\n"
        f"• Ground every paragraph in sound: what Whisperfall heard, what went quiet, what the wind carried\n"
        f"• Never name roles, abilities, teams, or mechanics\n"
        f"• Never use the word wolf or werewolf — use: the darkness, those who hunt, the unseen, shadows with intent\n"
        f"• The last line must linger. It is the only line players will remember. Make it count."
    )

    system = (
        "You are the voice of Whisperfall — a gothic village narrator writing morning Blood Board announcements "
        "for a social deduction game. Your prose is literary, precise, and deeply rooted in sound. "
        "\n\nWHAT MAKES A GREAT BLOOD BOARD:\n"
        "- It rewards the attentive reader. Clues are present but never obvious.\n"
        "- It makes the village feel alive and dangerous at the same time.\n"
        "- It varies rhythm. A long sentence unspools tension. A short one cuts it.\n"
        "- It never explains. It implies, suggests, circles.\n"
        "- The final line lands like a door closing.\n\n"
        "WHAT RUINS A BLOOD BOARD:\n"
        "- Repeating the same sentence structure throughout\n"
        "- Starting too many sentences with 'The village' or 'Whisperfall'\n"
        "- Stating the action hints literally instead of translating them\n"
        "- A final line that is hopeful, reassuring, or generic\n\n"
        "Write as if every player will read this aloud to the others. Make it worth reading aloud."
    )

    narrative = await _claude(prompt, system, max_tokens=850)
    if not narrative:
        narrative = (
            f"*Dawn crept into Whisperfall on the morning of Day {night_num + 1}.\n"
            f"The village stirred slowly, each soul listening before they spoke.\n"
            f"The night had passed — but not quietly. Something had moved through these streets, "
            f"and the echoes of it still clung to the morning air.*"
        )

    # Format as a proper blood board post
    header = f"🩸 **BLOOD BOARD — Morning of Day {night_num + 1}**\n{'─' * 40}\n\n"
    full_narrative = header + narrative

    # Get current alive counts for the footer
    alive_rows = db_get_assignments(guild_id)
    counts = {
        "total":   sum(1 for r in alive_rows if r[2] == 1),
        "village": sum(1 for r in alive_rows if r[2] == 1 and get_team(guild_id, r[1]) == "village"),
        "wolf":    sum(1 for r in alive_rows if r[2] == 1 and get_team(guild_id, r[1]) == "wolf"),
        "neutral": sum(1 for r in alive_rows if r[2] == 1 and get_team(guild_id, r[1]) == "neutral"),
    }

    # Post to mod-log for approval with edit button
    view  = BloodBoardApprovalView(guild_id, narrative, deaths, night_num, counts, action_hints)
    embed = _build_bb_embed(narrative, deaths, night_num, counts)
    embed.set_footer(text="Review, edit if needed, then post to Blood Board channel.")

    state_mod = cached_get_state(guild_id)
    mod_ch    = interaction.guild.get_channel(state_mod.get("mod_log_channel_id") or 0)
    if mod_ch:
        await mod_ch.send(embed=embed, view=view)

    await interaction.followup.send(
        "✅ Blood Board generated and posted to mod-log for review.\n"
        "Click ✅ Post to send it to the Blood Board channel.", ephemeral=True)


@tree.command(name="history", description="Browse past game results")
async def history(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    # Get unique games from role_history
    c.execute("""
        SELECT rh.game_id, rh.played_at,
               COUNT(DISTINCT rh.player_id) as players,
               SUM(CASE WHEN rh.outcome='win' THEN 1 ELSE 0 END) as winners,
               GROUP_CONCAT(DISTINCT rh.team) as teams
        FROM role_history rh
        WHERE rh.guild_id=?
        GROUP BY rh.game_id
        ORDER BY rh.game_id DESC
        LIMIT 10
    """, (interaction.guild_id,))
    games = c.fetchall()
    conn.close()

    if not games:
        return await interaction.followup.send(
            "No game history yet. History is recorded after `/assign_victors` is run.",
            ephemeral=True)

    embed = discord.Embed(
        title       = "📖 Game History — Whisperfall",
        description = f"Last {len(games)} completed game(s).",
        color       = 0x2C3060
    )

    for game_id, played_at, player_count, winner_count, teams_raw in games:
        teams = set((teams_raw or "").split(","))
        if "wolf" in teams and "village" in teams:
            winner_hint = "Unknown"
        elif "wolf" in teams:
            winner_hint = "🐺 Wolves"
        else:
            winner_hint = "🏘️ Village"

        # Try to get winner from log
        conn2 = sqlite3.connect(DB_FILE)
        c2    = conn2.cursor()
        c2.execute("SELECT event FROM game_log WHERE guild_id=? AND event LIKE '%wins%' ORDER BY entry_id DESC LIMIT 1",
                   (interaction.guild_id,))
        win_row = c2.fetchone()
        conn2.close()

        embed.add_field(
            name  = f"Game #{game_id}",
            value = (
                f"**Players:** {player_count}\n"
                f"**Winners:** {winner_count}\n"
                f"**Played:** {played_at[:10] if played_at else 'Unknown'}"
            ),
            inline = True
        )

    embed.set_footer(text="Run /assign_victors at game end to record results.")
    await interaction.followup.send(embed=embed, ephemeral=True)

@tree.command(name="game_recap", description="Generate a narrative game recap using AI")
@is_mod()
async def game_recap(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    rows      = db_get_assignments(interaction.guild_id)
    log       = db_get_log(interaction.guild_id)
    history   = db_get_vote_history(interaction.guild_id)
    night_num = db_get_night_num(interaction.guild_id)
    npcs      = db_get_npcs(interaction.guild_id)
    npc_map   = {n["npc_id"]: n["name"] for n in npcs}

    def get_name(pid):
        name = npc_map.get(pid)
        if not name:
            m    = interaction.guild.get_member(pid)
            name = m.display_name if m else str(pid)
        return name

    # Build player list with roles
    alive     = [r for r in rows if r[2] == 1]
    dead      = [r for r in rows if r[2] == 0]
    survivors = "\n".join(f"{get_name(r[0])} — {r[1]} ({get_team(interaction.guild_id, r[1])})" for r in alive)
    eliminated = "\n".join(f"{get_name(r[0])} — {r[1]}" for r in dead)

    # Build vote summary
    vote_lines = []
    for _vr in history:
        day_num, voter_id, target_id, action = _vr[0], _vr[1], _vr[2], _vr[3]
        if target_id:
            vote_lines.append(f"Day {day_num}: {get_name(voter_id)} → {get_name(target_id)}")

    # Build event log
    log_lines = [f"[{ph}] {ev}" for _, ph, ev in log[-30:]]


    prompt = (
        f"WHISPERFALL POST-GAME RECAP — {night_num} Nights\n\n"
        f"SURVIVORS:\n{survivors}\n\n"
        f"ELIMINATED:\n{eliminated}\n\n"
        f"KEY VOTES:\n" + "\n".join(vote_lines[-20:]) + "\n\n"
        f"GAME LOG:\n" + "\n".join(log_lines) + "\n\n"
        f"WHAT TO WRITE:\n"
        f"A narrative post-game retelling — the story of what actually happened in Whisperfall.\n\n"
        f"Cover these threads, woven together as a story:\n"
        f"• Who the wolves were and how they hid in plain sight\n"
        f"• The moments the village almost figured it out — and didn't\n"
        f"• The votes that mattered and why\n"
        f"• What ultimately decided the game\n"
        f"• How it felt — the paranoia, the close calls, the betrayals\n\n"
        f"WRITING RULES:\n"
        f"• 5-7 paragraphs of flowing prose\n"
        f"• Use player names throughout — this is their story\n"
        f"• Whisperfall tone: atmospheric, slightly gothic, rooted in sound\n"
        f"• Vary rhythm — long sentences for tension, short ones for impact\n"
        f"• No bullet points, no headers, no lists — pure narrative\n"
        f"• End on a line that makes the reader feel the weight of everything that just happened"
    )

    system = (
        "You are the narrator of Whisperfall writing the final post-game story — a retelling of what "
        "actually happened across the whole game. This is the moment players find out everything they "
        "missed, everything they got wrong, and everything that was happening behind their backs.\n\n"
        "WHAT MAKES A GREAT RECAP:\n"
        "- It feels like reading the definitive account of events you lived through\n"
        "- It reveals things that were hidden — and makes them feel inevitable in retrospect\n"
        "- It honors the players by treating their choices as meaningful\n"
        "- The wolves, if they won, feel genuinely threatening. If they lost, their unraveling feels earned.\n"
        "- The final line should make the reader feel something\n\n"
        "Write as if this will be pinned in the server forever. Because it might be."
    )

    narrative = await _claude(prompt, system, max_tokens=1400)
    if not narrative:
        return await interaction.followup.send(
            "❌ Could not generate recap. Try again.", ephemeral=True)

    # Post to village chat via approval in mod-log
    state  = db_get_state(interaction.guild_id) or {}
    mod_ch = interaction.guild.get_channel(state.get("mod_log_channel_id") or 0)

    class RecapApprovalView(View):
        def __init__(self):
            super().__init__(timeout=None)  # No timeout

        @discord.ui.button(label="🔄 Regenerate", style=discord.ButtonStyle.secondary)
        async def regen_btn(self, btn_interaction: discord.Interaction, button):
            await btn_interaction.response.edit_message(
                content="🔄 Regenerating recap...", embed=None, view=None)
            new_narrative = await _claude(prompt, system, max_tokens=1400)
            if not new_narrative:
                await btn_interaction.edit_original_response(
                    content="❌ Regeneration failed. Try again.")
                return
            new_embed = discord.Embed(
                title="📖 Game Recap — Preview",
                description=new_narrative,
                color=0xF1C40F)
            new_embed.set_footer(text="🔄 Regenerated — ✏️ Edit · ✅ Post · ❌ Discard")
            await btn_interaction.edit_original_response(embed=new_embed, view=RecapApprovalView(), content=None)

        @discord.ui.button(label="✏️ Edit", style=discord.ButtonStyle.blurple)
        async def edit_btn(self, btn_interaction: discord.Interaction, button):
            class RecapEditModal(discord.ui.Modal, title="Edit Recap"):
                text = discord.ui.TextInput(
                    label="Recap text", style=discord.TextStyle.paragraph,
                    default=narrative[:4000], max_length=4000, required=True)
                async def on_submit(self2, mi):
                    vc_ch = interaction.guild.get_channel(
                        (db_get_state(interaction.guild_id) or {}).get("village_chat_ch_id") or 0)
                    if vc_ch:
                        embed = discord.Embed(
                            title="📖 Game Recap — Whisperfall",
                            description=self2.text.value,
                            color=0xF1C40F)
                        await vc_ch.send(embed=embed)
                    await mi.response.send_message("✅ Recap posted.", ephemeral=True)
            await btn_interaction.response.send_modal(RecapEditModal())

        @discord.ui.button(label="✅ Post", style=discord.ButtonStyle.green)
        async def post_btn(self, btn_interaction: discord.Interaction, button):
            vc_ch = interaction.guild.get_channel(
                (db_get_state(interaction.guild_id) or {}).get("village_chat_ch_id") or 0)
            if vc_ch:
                embed = discord.Embed(
                    title="📖 Game Recap — Whisperfall",
                    description=narrative,
                    color=0xF1C40F)
                await vc_ch.send(embed=embed)
            await btn_interaction.response.edit_message(
                content="✅ Recap posted to village chat.", embed=None, view=None)

        @discord.ui.button(label="❌ Discard", style=discord.ButtonStyle.danger)
        async def discard_btn(self, btn_interaction: discord.Interaction, button):
            await btn_interaction.response.edit_message(
                content="❌ Recap discarded.", embed=None, view=None)

    recap_embed = discord.Embed(
        title="📖 Game Recap — Preview",
        description=narrative,
        color=0xF1C40F)
    recap_embed.set_footer(text="✏️ Edit · ✅ Post to village chat · ❌ Discard")

    if mod_ch:
        await mod_ch.send(embed=recap_embed, view=RecapApprovalView())
        await interaction.followup.send("✅ Recap preview sent to mod-log.", ephemeral=True)
    else:
        await interaction.followup.send(embed=recap_embed, ephemeral=True)

@tree.command(name="mod_summary", description="Post a detailed stats report to mod-log")
@is_mod()
async def mod_summary(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    rows = db_get_assignments(interaction.guild_id)
    pid_to_name = {}
    for pid, _, _, _ in rows:
        m = interaction.guild.get_member(pid)
        pid_to_name[pid] = m.display_name if m else str(pid)

    night_num  = db_get_night_num(interaction.guild_id)
    log        = db_get_log(interaction.guild_id)
    vote_hist  = db_get_vote_history(interaction.guild_id)
    turn_hist  = db_get_turn_log(interaction.guild_id)
    block_hist = db_get_block_log(interaction.guild_id)

    embed = discord.Embed(
        title="Game Summary",
        description=f"Full report - {night_num} night(s) played.",
        color=0x2C3E50,
        timestamp=datetime.now()
    )

    alive = [r for r in rows if r[2] == 1]
    dead  = [r for r in rows if r[2] == 0]
    roster_lines = []
    for pid, role, is_alive, _ in sorted(rows, key=lambda r: r[1]):
        team   = get_team(interaction.guild_id, role)
        emoji  = _role_emoji(team)
        status = "alive" if is_alive else "dead"
        roster_lines.append(f"[{status}] {emoji} **{pid_to_name.get(pid, str(pid))}** - {role}")
    embed.add_field(
        name=f"Players ({len(rows)} total - {len(alive)} alive, {len(dead)} eliminated)",
        value="\n".join(roster_lines[:20]) or "None",
        inline=False
    )

    death_events = [f"`{ts}` {e}" for ts, ph, e in log if "eliminated" in e.lower()]
    if death_events:
        embed.add_field(name="Eliminations", value="\n".join(death_events[:15]), inline=False)

    if turn_hist:
        turn_lines = []
        for n_num, actor_id, target_id, result in turn_hist:
            actor  = pid_to_name.get(actor_id, str(actor_id))
            target = pid_to_name.get(target_id, str(target_id))
            turn_lines.append(f"Night {n_num}: **{actor}** -> **{target}** [{result}]")
        embed.add_field(name=f"Turn Attempts ({len(turn_hist)})", value="\n".join(turn_lines[:10]), inline=False)

    if block_hist:
        block_lines = []
        for n_num, blocker_id, target_id in block_hist:
            blocker = pid_to_name.get(blocker_id, str(blocker_id))
            target  = pid_to_name.get(target_id, str(target_id))
            block_lines.append(f"Night {n_num}: **{blocker}** blocked **{target}**")
        embed.add_field(name=f"Wolf Pup Blocks ({len(block_hist)})", value="\n".join(block_lines[:10]), inline=False)

    if vote_hist:
        from collections import Counter
        target_counts = Counter(t for _, _, t, a in vote_hist if t is not None and a == "vote")
        if target_counts:
            top_id, top_cnt = target_counts.most_common(1)[0]
            top_name = pid_to_name.get(top_id, str(top_id))
            embed.add_field(
                name=f"Votes ({len(vote_hist)} total)",
                value=f"Most targeted: **{top_name}** ({top_cnt} votes across all days)",
                inline=False
            )

    key_events = [f"`{ts}` {e}" for ts, ph, e in log
                  if any(k in e for k in ["wins", "started", "scrambled", "revived", "transferred"])]
    if key_events:
        embed.add_field(name="Key Events", value="\n".join(key_events[:10]), inline=False)

    embed.set_footer(text="VillageAid - End of Game Report")
    await post_mod_log(interaction.guild, embed=embed)
    await interaction.followup.send("Game summary posted to mod-log.", ephemeral=True)

# ====================== HALL OF FAME SETUP ======================
@tree.command(name="setup_hall_of_fame", description="Pin the Hall of Fame leaderboard to a channel")
@is_mod()
@app_commands.describe(channel="Channel to pin the Hall of Fame in (e.g. #stats or a dedicated channel)")
async def setup_hall_of_fame(interaction: discord.Interaction, channel: discord.TextChannel):
    await interaction.response.defer(ephemeral=True)
    stat_rows = db_get_stats(interaction.guild_id)
    embed     = build_hall_of_fame_embed(interaction.guild, stat_rows)
    msg       = await channel.send(embed=embed)
    try: await msg.pin()
    except: pass  # Pin fails if no manage_messages perm — non-fatal

    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("INSERT OR REPLACE INTO hall_of_fame VALUES (?,?,?)",
              (interaction.guild_id, channel.id, msg.id))
    conn.commit()
    conn.close()
    await interaction.followup.send(
        f"✅ Hall of Fame pinned in {channel.mention}. It will update automatically after every game.",
        ephemeral=True)

# ====================== HELP ======================

HELP_DATA = {
    "setup": {
        "label": "⚙️ Server Setup",
        "desc":  "One-time configuration commands. Run these before your first game.",
        "commands": [
            ("/setup_roles",        "Link the Mod, Participant, Dead, and Spectator Discord roles."),
            ("/set_spectator_role", "Update the Spectator role after initial setup."),
            ("/setup_hall_of_fame", "Pin a live Hall of Fame leaderboard to a channel."),
            ("/set_font",           "Choose a Unicode font style for all game channel names."),
            ("/setup_guide",        "Post a step-by-step setup checklist for new servers."),
        ]
    },
    "roles": {
        "label": "🎭 Role Management",
        "desc":  "Manage the role pool. Changes persist across all future games.",
        "commands": [
            ("/add_role",      "Add or update a role (name, description, count, team)."),
            ("/remove_role",   "Remove a role from the pool permanently."),
            ("/reload_roles",  "Reload all default roles into the pool."),
            ("/list_roles",    "Show all saved roles grouped by team — posts in channel."),
            ("/post_roles",    "Post all roles with full descriptions to village chat."),
            ("/current_roles", "Show roles in the current active game."),
            ("/roleinfo",      "Show full details for any role — description, team, night order."),
        ]
    },
    "lobby": {
        "label": "🚪 Lobby",
        "desc":  "Pre-game sign-up. Mod opens lobby, players join, mod starts game.",
        "commands": [
            ("/lobby open",   "Open the sign-up lobby. Players click Join to register."),
            ("/lobby close",  "Close the lobby — no more sign-ups."),
            ("/lobby kick",   "Remove a player from the lobby before the game starts."),
            ("/spectate",     "Request spectator access — sends approval to mod-log."),
        ]
    },
    "game": {
        "label": "🎮 Game Management",
        "desc":  "Commands to start, run, and end a game. Mod-only.",
        "commands": [
            ("/start_game",     "Build your role roster and launch the game."),
            ("/end_game",       "End the game and delete all game channels."),
            ("/announce",       "Send a message to all (or alive-only) player private channels."),
            ("/transfer_mod",   "Hand mod control to another player mid-game."),
            ("/scramble_roles", "Secretly reshuffle roles within teams."),
            ("/swap_roles",     "Secretly swap two alive players\' roles."),
            ("/assign_role",    "Manually reassign a player\'s role with autocomplete."),
            ("/turn_player",    "Turn a player to the wolf team — spins role from pool."),
            ("/revive_player",  "Bring an eliminated player back to life."),
            ("/add_player",     "Add a new player to an active game mid-session."),
            ("/kick_player",    "Remove a player from the game without ending it."),
            ("/assign_victors", "Record the winning team for stats tracking."),
            ("/game_recap",     "Generate an AI narrative recap of the game."),
            ("/mod_summary",    "Post a full stats report to mod-log."),
        ]
    },
    "day": {
        "label": "☀️ Day Phase",
        "desc":  "Day vote opens automatically at day start. These commands give mods control.",
        "commands": [
            ("/start_day_vote",  "Manually open or reopen the day vote."),
            ("/set_vote",        "Set votes per player (1 or 2) and toggle anonymous mode."),
            ("/clear_day_votes", "Clear all current day votes (saves a snapshot first)."),
            ("/eliminate",       "Eliminate a player by name. Auto-checks win condition."),
            ("/vote_status",     "Show who has and hasn\'t voted yet."),
            ("/vote_remind",     "Ping unvoted players by mention in village chat."),
            ("/vote_history",    "Show every vote cast across all days."),
            ("/dayboard",        "Generate an AI day Blood Board for mod approval."),
            ("/bloodboard",      "Generate an AI night Blood Board for mod approval."),
        ]
    },
    "night": {
        "label": "🌙 Night Phase",
        "desc":  "Commands for running and resolving night actions.",
        "commands": [
            ("/start_night",     "Begin the night phase. Sends ability prompts to all players."),
            ("/next_phase",       "⭐ Key command — advances phase: resolves night or opens day vote."),
            ("/resolve_night",   "Close night and post action summary to mod-log."),
            ("/night_status",    "Live dashboard — who submitted, passed, or is waiting."),
            ("/remind_players",  "Privately nudge players who haven\'t submitted actions."),
            ("/pass_night",      "Player command — pass your night action."),
            ("/protect",         "Huntsman: declare who you are protecting tonight."),
            ("/frenzy",          "Agitator: activate your frenzy ability."),
            ("/submit_action",   "Mod: submit a night action on behalf of a player."),
            ("/spin_wheel",      "Post a random resolution order to mod-log."),
            ("/den_brief",       "Post an AI tactical briefing to the wolf den to help the pack choose a target."),
            ("/ww_result",       "Record White Wolf kill outcome: wolf (obligation met) or miss (strike added)."),
        ]
    },
    "info": {
        "label": "📊 Info & Stats",
        "desc":  "Commands available to all players.",
        "commands": [
            ("/my_role",        "Show your secret role (ephemeral — only you see it)."),
            ("/my_stats",       "Show your personal win/loss/elimination stats."),
            ("/my_roles",       "Show every role you\'ve ever played."),
            ("/my_actions",     "Show your submitted night actions this game."),
            ("/tracker",        "Open your personal investigation tracker."),
            ("/roleinfo",       "Full details on any role — description, team, night order."),
            ("/list_players",   "Show alive and dead players."),
            ("/game_status",    "Current phase, night number, and player counts."),
            ("/modcheck",       "Mod: quick snapshot of game state."),
            ("/mod_dashboard",  "Post or refresh the pinned mod dashboard in mod-log."),
            ("/phase_history",  "Mod: every phase transition with timestamps."),
            ("/time_left",      "How much time is left in the current phase."),
            ("/action",         "Send a private message or action to the mod team."),
            ("/post_stats",     "Post the all-time leaderboard to the stats channel."),
        ]
    },
}

class HelpCategorySelect(View):
    def __init__(self):
        super().__init__(timeout=300)
        options = [
            discord.SelectOption(label=v["label"], value=k, description=v["desc"][:80])
            for k, v in HELP_DATA.items()
        ]
        sel = Select(placeholder="Choose a category…", options=options, min_values=1, max_values=1)
        sel.callback = self.on_select
        self.add_item(sel)

    async def on_select(self, interaction: discord.Interaction):
        try:
            key      = interaction.data["values"][0]
            section  = HELP_DATA[key]
            embed    = discord.Embed(
                title       = section["label"],
                description = section["desc"],
                color       = 0x5865F2
            )
            for cmd, desc in section["commands"]:
                embed.add_field(name=cmd, value=desc[:1024], inline=False)
            embed.set_footer(text="VillageAid · Use the dropdown to browse other categories.")
            await interaction.response.edit_message(embed=embed, view=self)
        except Exception as e:
            print(f"[HelpCategorySelect] error: {e}")
            try:
                await interaction.response.send_message(
                    f"❌ Error loading help section: {e}", ephemeral=True)
            except Exception:
                pass



@tree.command(name="roleinfo", description="Show full details for any role")
@app_commands.describe(role="The role to look up")
async def roleinfo(interaction: discord.Interaction, role: str):
    await interaction.response.defer(ephemeral=True)

    roles = db_load_roles(interaction.guild_id)
    matched = next((r for r in roles if r["name"].lower() == role.strip().lower()), None)

    if not matched:
        # Try partial match
        matched = next((r for r in roles if role.strip().lower() in r["name"].lower()), None)

    if not matched:
        return await interaction.followup.send(
            f"❌ Role **{role}** not found. Use `/list_roles` to see all available roles.",
            ephemeral=True)

    team      = matched.get("team", "village")
    team_emoji = "🐺" if team == "wolf" else ("⚖️" if team == "neutral" else "🏘️")
    color      = 0xC0392B if team == "wolf" else (0xF39C12 if team == "neutral" else 0x27AE60)
    desc       = matched.get("description") or "*No description provided.*"

    # Find night order position
    NIGHT_ORDER = {
        "Shapeshifter": "Phase 1 — Disguises",
        "Bloodletter":  "Phase 1 — Disguises",
        "Wolf Pup":     "Phase 2 — Blocks",
        "Cupid":        "Phase 3 — Bonds",
        "Dire Wolf":    "Phase 3 — Bonds",
        "Agitator":     "Phase 4 — Declarations",
        "Clone":        "Phase 4 — Declarations",
        "Doctor":       "Phase 5 — Protection",
        "Surgeon":      "Phase 5 — Protection",
        "Huntsman":     "Phase 5 — Protection",
        "Witch":        "Phase 5 — Protection (save) / Phase 7 — Independent Kills (poison)",
        "Alpha":        "Phase 6 — Wolf Action (turn)",
        "Elite Alpha":  "Phase 6 — Wolf Action (turn)",
        "Wolf":         "Phase 6 — Den Kill",
        "Werekitten":   "Phase 6 — Den Kill (replaces den kill, max 2 uses)",
        "Crazed Wolf":  "Phase 6 — Den Kill",
        "Echo-Stalker": "Phase 6 — Den Kill",
        "White Wolf":   "Phase 7 — Independent Kills",
        "Shadow Wolf":  "Phase 7 — Independent Kills",
        "Seer":         "Phase 9 — Investigations",
        "Medium":       "Phase 9 — Investigations",
        "Bloodhound":   "Phase 9 — Investigations",
        "Insomniac":    "Phase 10 — Information",
        "Gravedigger":  "Phase 10 — Information",
        "Oracle":       "Phase 10 — Information",
        "Governor":     "Day ability — button sent to private channel when vote opens (15 min before close)",
        "Hermit":       "Day ability — button sent to private channel when vote opens (20 min before close)",
    }
    night_pos = NIGHT_ORDER.get(matched["name"], "Passive — no night action")

    # Determine ability type
    day_ability_roles = {"Governor", "Hermit"}
    passive_roles = {
        "Villager", "Diseased", "Drunk", "Lycan", "Mayor", "Pothead",
        "Prostitute", "Time Lord", "Traitor", "Village Idiot", "Village Jokester",
        "Virgin", "Blessed Wolf", "Fairy Elf", "Warlock",
        "Sheriff", "Wolf", "Insomniac", "Elder", "Gravedigger"
    }
    if matched["name"] in day_ability_roles:
        ability_type = "Day ability (button in private channel)"
    elif matched["name"] in passive_roles:
        ability_type = "Passive — no action button"
    else:
        ability_type = "Active (night button in private channel)"

    embed = discord.Embed(
        title       = f"{team_emoji} {matched['name']}",
        description = desc,
        color       = color
    )
    embed.add_field(name="Team",         value=team.capitalize(),  inline=True)
    embed.add_field(name="Ability",      value=ability_type,       inline=True)
    embed.add_field(name="Night Order",  value=night_pos,          inline=False)

    await interaction.followup.send(embed=embed, ephemeral=True)


@roleinfo.autocomplete("role")
async def roleinfo_autocomplete(interaction: discord.Interaction, current: str):
    roles = get_game_roles(interaction.guild_id) if game_active(interaction.guild_id) else cached_load_roles(interaction.guild_id)

    return [
        app_commands.Choice(
            name=f"{r['name']} ({'🐺' if r['team']=='wolf' else '⚖️' if r['team']=='neutral' else '🏘️'})",
            value=r["name"])
        for r in roles
        if current.lower() in r["name"].lower()
    ][:25]

@tree.command(name="message_count", description="Show message counts per player for the current game")
@is_mod()
async def message_count_cmd(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    counts      = db_get_message_counts(interaction.guild_id)
    rows        = db_get_assignments(interaction.guild_id)
    npcs_mc     = db_get_npcs(interaction.guild_id)
    npc_map_mc  = {n["npc_id"]: n["name"] for n in npcs_mc}
    alive_ids   = {r[0] for r in rows if r[2] == 1}

    def get_name(pid):
        m = interaction.guild.get_member(pid)
        return npc_map_mc.get(pid) or (m.display_name if m else str(pid))

    count_map = {c[0]: (c[1], c[2]) for c in counts}

    lines = []
    for pid in sorted(alive_ids, key=lambda p: -(count_map.get(p, (0,))[0])):
        total, breakdown = count_map.get(pid, (0, ""))
        name = get_name(pid)
        bar  = "█" * min(total, 20)
        lines.append(f"**{name}** — {total} msgs  `{bar}`\n> *by day: {breakdown or 'none'}*")

    # Also show dead players with counts
    dead_lines = []
    dead_ids = {r[0] for r in rows if r[2] == 0}
    for pid in sorted(dead_ids, key=lambda p: -(count_map.get(p, (0,))[0])):
        total, breakdown = count_map.get(pid, (0, ""))
        if total > 0:
            name = get_name(pid)
            dead_lines.append(f"~~{name}~~ — {total} msgs (*by day: {breakdown or 'none'}*)")

    embed = discord.Embed(
        title       = "💬 Message Counts — Current Game",
        description = "\n".join(lines) if lines else "No messages recorded yet.",
        color       = 0x2ECC71
    )
    if dead_lines:
        embed.add_field(name="☠️ Eliminated", value="\n".join(dead_lines), inline=False)
    embed.set_footer(text="Counts village-chat messages during day phase only")
    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.command(name="den_brief", description="Post a tactical kill briefing to the wolf den for tonight")
async def den_brief(interaction: discord.Interaction):
    """
    Posts a Claude-generated tactical briefing into the wolf den — alive non-wolf players
    ranked by threat level, with behavioral signals from message counts, vote history,
    and day vote tallies. Helps the pack deliberate with actual data.
    Available to mods and alive wolves.
    """
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)

    # Allow mods or alive wolves
    state_perm = cached_get_state(interaction.guild_id)
    mod_role   = interaction.guild.get_role(state_perm.get("mod_role_id") or 0)
    is_mod_user = (
        interaction.user.guild_permissions.administrator
        or (mod_role and mod_role in interaction.user.roles)
    )
    if not is_mod_user:
        rows_perm = db_get_assignments(interaction.guild_id)
        my_row    = next((r for r in rows_perm if r[0] == interaction.user.id and r[2] == 1), None)
        if not my_row or get_team(interaction.guild_id, my_row[1]) != "wolf":
            return await interaction.response.send_message(
                "❌ Only wolves and mods can use this command.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    guild_id  = interaction.guild_id
    night_num = db_get_night_num(guild_id)
    rows      = db_get_assignments(guild_id)
    npcs_db   = db_get_npcs(guild_id)
    npc_map   = {n["npc_id"]: n["name"] for n in npcs_db}
    state     = cached_get_state(guild_id)

    def get_name(pid):
        npc = npc_map.get(pid)
        if npc: return npc
        m = interaction.guild.get_member(pid)
        return m.display_name if m else str(pid)

    # ── Alive non-wolf players (valid kill targets) ───────────────────────
    targets = [
        (r[0], r[1]) for r in rows
        if r[2] == 1 and get_team(guild_id, r[1]) != "wolf"
    ]
    if not targets:
        return await interaction.followup.send("No valid kill targets alive.", ephemeral=True)

    # ── Message counts (day activity) ─────────────────────────────────────
    msg_counts = {c[0]: c[1] for c in db_get_message_counts(guild_id)}

    # ── Day vote tallies — how many times each player has been voted for ──
    vote_hist  = db_get_vote_history(guild_id)
    times_targeted = {}   # pid -> count of times nominated across all days
    times_voted    = {}   # pid -> count of votes they cast
    vote_changes   = {}   # pid -> number of vote flips
    for day_num_h, voter_id, target_id, action, voted_at, change_count in (
            r + (0,) * (6 - len(r)) for r in vote_hist):
        if target_id and action in ("vote", "change"):
            times_targeted[target_id] = times_targeted.get(target_id, 0) + 1
        if voter_id:
            times_voted[voter_id] = times_voted.get(voter_id, 0) + 1
        if action == "change" and voter_id:
            vote_changes[voter_id] = vote_changes.get(voter_id, 0) + 1

    # ── Previous wolf votes — who the pack has targeted before ───────────
    prev_wolf_targets = {}
    conn_dv = sqlite3.connect(DB_FILE)
    c_dv    = conn_dv.cursor()
    c_dv.execute(
        "SELECT target_id, COUNT(*) FROM wolf_votes WHERE guild_id=? GROUP BY target_id",
        (guild_id,))
    for tid, cnt in c_dv.fetchall():
        prev_wolf_targets[tid] = cnt
    conn_dv.close()

    # ── Werekitten kill remaining uses ────────────────────────────────────
    conn_wk = sqlite3.connect(DB_FILE)
    c_wk    = conn_wk.cursor()
    c_wk.execute(
        "SELECT COUNT(*) FROM night_actions WHERE guild_id=? AND action_type=?",
        (guild_id, "werekitten_kill"))
    wk_used    = c_wk.fetchone()[0]
    conn_wk.close()
    wk_alive   = any(r[1] == "Werekitten" and r[2] == 1 for r in rows)
    wk_uses_left = max(0, 2 - wk_used) if wk_alive else 0

    # ── Alpha/Elite Alpha turns remaining ─────────────────────────────────
    turns_used = {}
    conn_ta = sqlite3.connect(DB_FILE)
    c_ta    = conn_ta.cursor()
    c_ta.execute(
        "SELECT actor_id, COUNT(*) FROM night_actions WHERE guild_id=? AND action_type IN (?,?) GROUP BY actor_id",
        (guild_id, "alpha", "elite_alpha"))
    for aid, cnt in c_ta.fetchall():
        turns_used[aid] = cnt
    conn_ta.close()

    # ── Build per-target data for Claude ──────────────────────────────────
    target_data = []
    for pid, role in targets:
        name        = get_name(pid)
        msgs        = msg_counts.get(pid, 0)
        nominated   = times_targeted.get(pid, 0)
        voted_count = times_voted.get(pid, 0)
        flips       = vote_changes.get(pid, 0)
        pack_votes  = prev_wolf_targets.get(pid, 0)

        # Threat signals — purely behavioral, no role info sent to Claude
        signals = []
        if msgs > 20:  signals.append(f"very active in chat ({msgs} messages)")
        elif msgs > 10: signals.append(f"moderately active ({msgs} messages)")
        else:           signals.append(f"quiet in chat ({msgs} messages)")
        if nominated >= 3:  signals.append(f"frequently nominated by village ({nominated}x)")
        elif nominated > 0: signals.append(f"nominated {nominated}x")
        if flips >= 2:  signals.append(f"changed their vote {flips}x — erratic or reactive")
        elif flips == 1: signals.append("changed vote once")
        if voted_count >= night_num + 1:
            signals.append(f"consistent voter — active in every day vote")
        if pack_votes > 0:
            signals.append(f"previously targeted by the pack ({pack_votes}x)")

        target_data.append({
            "name":    name,
            "signals": signals,
        })

    # ── Alpha/Elite Alpha turn candidates (only villager-team players) ────
    turnable = [
        get_name(pid) for pid, role in targets
        if get_team(guild_id, role) == "village"
           and role not in {"Sheriff", "Lycan", "Diseased"}
    ]

    # ── Build wolf roster note ────────────────────────────────────────────
    wolf_roster = []
    for pid, role, is_alive, _ in rows:
        if is_alive and get_team(guild_id, role) == "wolf":
            wolf_roster.append(f"{get_name(pid)} ({role})")

    # ── Claude prompt ─────────────────────────────────────────────────────
    target_block = "\n".join(
        f"• **{t['name']}**: {'; '.join(t['signals'])}"
        for t in target_data
    )

    special_block = ""
    if wk_uses_left > 0:
        special_block += f"\n• Werekitten has {wk_uses_left} kill use(s) remaining — bypasses Sheriff and Huntsman."

    for pid, role, is_alive, _ in rows:
        if is_alive and role in ("Alpha", "Elite Alpha"):
            used = turns_used.get(pid, 0)
            max_turns = 2 if role == "Elite Alpha" else 1
            left = max(0, max_turns - used)
            if left > 0:
                special_block += f"\n• {role} has {left} turn(s) remaining. Turn candidates: {', '.join(turnable[:8]) or 'none'}."

    prompt = (
        f"WOLF DEN TACTICAL BRIEF — Night {night_num}\n\n"
        f"Your pack: {', '.join(wolf_roster) or 'Unknown'}\n"
        f"Alive targets ({len(target_data)} players):\n{target_block}\n"
        + (f"\nSPECIAL ABILITIES:\n{special_block.strip()}" if special_block.strip() else "") +
        f"\n\nYour job: Write a brief tactical briefing FOR the wolf pack.\n"
        f"- Rank the top 3 targets by threat to the pack — based purely on the behavioral signals above\n"
        f"- Give one clear reason per target why they're a priority or not\n"
        f"- If any special abilities are usable, note whether tonight is a good night to use them\n"
        f"- End with one sentence: the recommended kill for tonight\n\n"
        f"RULES:\n"
        f"- Do NOT reveal role names — only reference player names and behavior\n"
        f"- Do NOT use the words 'villager', 'wolf', or 'werewolf'\n"
        f"- Write as if you are a ruthless pack strategist, not a game narrator\n"
        f"- Be direct and tactical — 4-6 bullet points, no fluff"
    )

    system = (
        "You are a cold, tactical strategist writing briefings for the wolf pack in a Mafia/Werewolf game. "
        "Your tone is precise, strategic, and direct — like a general issuing orders. "
        "You rank threats by behavioral signals only. You never reference game mechanics by name. "
        "Your recommendation carries weight. Make it count."
    )

    brief = await _claude(prompt, system, max_tokens=500)
    if not brief:
        brief = (
            "No clear recommendation this night. Review the vote embed and deliberate as a pack.\n"
            "Prioritize players who have been consistently vocal and nominating others."
        )

    # ── Build embed and post to wolf den ──────────────────────────────────
    embed = discord.Embed(
        title       = f"🐺 Den Brief — Night {night_num}",
        description = brief,
        color       = 0x8B0000
    )
    # Compact target overview as a footer reference
    target_summary = " | ".join(
        f"{t['name']} ({msg_counts.get(pid, 0)}msgs, {times_targeted.get(pid,0)}noms)"
        for pid, _ in targets[:10]
    )
    embed.add_field(
        name  = "📊 Target Overview",
        value = target_summary[:1024] or "No data",
        inline= False
    )
    embed.set_footer(text=f"Generated Night {night_num} · {len(targets)} targets alive · Use the vote dropdown to lock your choice.")

    wolf_ch = interaction.guild.get_channel(state.get("wolf_channel_id") or 0)
    if not wolf_ch:
        return await interaction.followup.send("❌ Wolf den channel not found.", ephemeral=True)

    await wolf_ch.send(embed=embed)
    await interaction.followup.send("✅ Den brief posted to wolf den.", ephemeral=True)


@tree.command(name="rules", description="How to play — core Mafia/Werewolf rules for Whisperfall")
async def rules(interaction: discord.Interaction):
    embed = discord.Embed(
        title       = "📖 How to Play — Whisperfall",
        description = (
            "Whisperfall is a social deduction game. "
            "Wolves hide among the village. "
            "The village must find and eliminate them before they're outnumbered."
        ),
        color = 0x2C3060
    )
    embed.add_field(
        name  = "🏘️ The Village",
        value = (
            "You are a villager. You don't know who the wolves are.\n"
            "Each **day**, the village votes to eliminate one player.\n"
            "Some villagers have special abilities — your role card explains yours.\n"
            "**Village wins** when all wolves are eliminated."
        ),
        inline=False)
    embed.add_field(
        name  = "🐺 The Wolves",
        value = (
            "Wolves know each other. Each **night**, they secretly vote to kill a villager.\n"
            "During the day, wolves must blend in — lie, deflect, and avoid suspicion.\n"
            "**Wolves win** when they equal or outnumber the remaining villagers."
        ),
        inline=False)
    embed.add_field(
        name  = "🌙 Night Phase",
        value = (
            "When night falls, a button appears in your **private channel**.\n"
            "Submit your night action using that button.\n"
            "If you have no ability, click **Pass** or wait — the mod handles the rest.\n"
            "Night lasts **12 hours** (8 PM → 8 AM EST)."
        ),
        inline=False)
    embed.add_field(
        name  = "☀️ Day Phase",
        value = (
            "Discussion happens in **village-chat**. Accuse, defend, read the room.\n"
            "Cast your vote in **day-vote** using the vote button.\n"
            "The player with the most votes at close is eliminated.\n"
            "Day lasts **12 hours** (8 AM → 8 PM EST)."
        ),
        inline=False)
    embed.add_field(
        name  = "⚖️ Neutral Roles",
        value = (
            "Some roles belong to neither side. "
            "They have their own win conditions — read your role card carefully.\n"
            "A neutral player can win alongside the village or wolves, or alone."
        ),
        inline=False)
    embed.add_field(
        name  = "📋 Key Commands",
        value = (
            "`/my_role` — see your secret role card\n"
            "`/roleinfo <role>` — full details on any role\n"
            "`/time_left` — check how long remains in the current phase\n"
            "`/pass_night` — tell the mod you have no action tonight\n"
            "`/action <message>` — send a private message to the mod team\n"
            "`/vote_history` — see all votes cast today\n"
            "`/tracker` — open your personal investigation notes"
        ),
        inline=False)
    embed.add_field(
        name  = "💡 Tips",
        value = (
            "• **Don't reveal your role publicly** — even village roles benefit from secrecy.\n"
            "• **Vote every day** — missing votes has consequences.\n"
            "• **Watch behaviour patterns** — who defends wolves? Who votes suspiciously?\n"
            "• **Use `/action`** to communicate privately with the mod at any time."
        ),
        inline=False)
    embed.set_footer(text="Use /roleinfo to look up any specific role. Use /villagehelp to browse all commands.")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@tree.command(name="villagehelp", description="Browse all VillageAid commands by category")
async def help_cmd(interaction: discord.Interaction):
    embed = discord.Embed(
        title       = "📖 VillageAid Help",
        description = (
            "Welcome to **VillageAid** — your Mafia/Werewolf game manager.\n\n"
            "Use the dropdown below to browse commands by category.\n\n"
            f"**{sum(len(v['commands']) for v in HELP_DATA.values())} commands** across "
            f"**{len(HELP_DATA)} categories**."
        ),
        color = 0x5865F2
    )
    for key, section in HELP_DATA.items():
        cmd_list = "  ".join(f"`{c}`" for c, _ in section["commands"])
        embed.add_field(name=section["label"], value=cmd_list, inline=False)
    embed.set_footer(text="VillageAid · Pick a category from the dropdown for full details.")
    view = HelpCategorySelect()
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# ====================== SETUP GUIDE ======================

@tree.command(name="setup_guide", description="Post a step-by-step setup checklist for new servers")
async def setup_guide(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🏘️ VillageAid — Server Setup Guide",
        description=(
            "Welcome! Follow these steps to get your server ready to play Mafia/Werewolf.\n"
            "All setup commands are mod-only after `/setup_roles` is run."
        ),
        color=0x5865F2
    )

    embed.add_field(
        name="Step 1 — Create Discord Roles",
        value=(
            "In your server settings, create four roles:\n"
            "• **Mod** — the game moderator\n"
            "• **Participant** — auto-assigned when players join the lobby\n"
            "• **Dead** — auto-assigned when players are eliminated\n"
            "• **Spectator** — for watchers (request via `/spectate`)\n\n"
            "These can be named anything — you'll link them in Step 2."
        ),
        inline=False
    )

    embed.add_field(
        name="Step 2 — Run `/setup_roles`",
        value=(
            "Link the four roles you just created.\n"
            "After this, all mod commands require the Mod role.\n"
            "This only needs to be done once per server."
        ),
        inline=False
    )

    embed.add_field(
        name="Step 3 — (Optional) Customise",
        value=(
            "`/set_font` — choose a Unicode font style for channel names\n"
            "`/add_role` — add custom roles beyond the 45 built-in ones\n"
            "`/setup_hall_of_fame` — pin a live leaderboard to a channel\n"
            "All of these persist across games automatically."
        ),
        inline=False
    )

    embed.add_field(
        name="Step 4 — Open the Lobby",
        value=(
            "Run `/lobby open` to open the sign-up lobby in village chat.\n"
            "Players click **Join Game** to register — the Participant role is assigned automatically.\n"
            "Run `/lobby close` when everyone is in.\n\n"
            "You can also use `/add_player` mid-game to add latecomers."
        ),
        inline=False
    )

    embed.add_field(
        name="Step 5 — Start a Game",
        value=(
            "1. Run `/start_game`\n"
            "2. Choose a channel font style\n"
            "3. Build your role roster (pick any combo of the 45 built-in roles)\n"
            "4. Confirm — the bot creates all channels and deals roles privately\n\n"
            "The game starts on **Night 1** automatically.\n"
            "Day phase runs **8 AM – 8 PM EST**. Night phase runs **8 PM – 8 AM EST**. Time Lord death halves both and switches to relative timers."
        ),
        inline=False
    )

    embed.add_field(
        name="📖 Need help during a game?",
        value="Run `/villagehelp` to browse all commands by category.",
        inline=False
    )

    embed.add_field(
        name="🔗 Useful Links",
        value=(
            "[GitHub](https://github.com/Kazlek177/village-aid-bot) · "
            "Invite link available from the bot's profile page"
        ),
        inline=False
    )

    embed.set_footer(text="VillageAid · Made with ❤️ for Mafia/Werewolf communities")
    await interaction.response.send_message(embed=embed)



# discord.py handles reconnection internally via client.run()
# client.start() cannot be called more than once — use client.run() which
# includes its own reconnect logic with reconnect=True (default)
client.run(os.getenv("DISCORD_TOKEN"), reconnect=True)