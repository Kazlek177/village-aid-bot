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

# ====================== DATABASE ======================
# Use /app/data/ on Railway (persistent volume) or current directory locally
# Always use /app/data on Railway — create it if it doesn't exist yet
# This ensures the volume mount is used even if the directory wasn't pre-created
_DB_DIR = "/app/data" if os.environ.get("RAILWAY_ENVIRONMENT") or os.path.isdir("/app") else "."
os.makedirs(_DB_DIR, exist_ok=True)
DB_FILE = os.path.join(_DB_DIR, "mafia_game.db")
print(f"[DB] Using database at: {DB_FILE}")

# Hardcoded village chat channel — not created by bot
VILLAGE_CHAT_ID = 1482889389519409202

def init_db():
    conn = sqlite3.connect(DB_FILE)
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
                    night_duration        INTEGER DEFAULT 36000,
                    day_duration          INTEGER DEFAULT 50400,
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
                    guild_id  INTEGER,
                    voter_id  INTEGER,
                    target_id INTEGER,  -- NULL means abstain
                    PRIMARY KEY (guild_id, voter_id)
                 )''')

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
                    PRIMARY KEY (guild_id, entry_id)
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

    # Add memory columns to existing NPC tables
    try:
        c.execute("ALTER TABLE npcs ADD COLUMN memory_summary TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE npcs ADD COLUMN pinned_events TEXT DEFAULT '[]'")
    except Exception:
        pass

    # village-chat channel id
    try:
        c.execute("ALTER TABLE game_state ADD COLUMN village_chat_ch_id INTEGER")
    except Exception:
        pass

    conn.commit()
    conn.close()

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
     "First night only, takes the form and role of any player they choose. "
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
     "Appears as villager to Seer. Can kill the Sheriff successfully (too cute to shoot). "
     "If voted out, all role actions are silenced for the night."),
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
]

def load_default_roles(guild_id):
    """Insert default roles for a guild only if they have no roles saved yet."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM game_roles WHERE guild_id=?", (guild_id,))
    count = c.fetchone()[0]
    if count == 0:
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

def db_save_role(guild_id, name, description, count, team):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO game_roles VALUES (?,?,?,?,?)",
              (guild_id, name, description, count, team.lower()))
    conn.commit()
    conn.close()
    invalidate_cache(guild_id)

def db_load_roles(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT role_name, description, count, team FROM game_roles WHERE guild_id=?", (guild_id,))
    rows = c.fetchall()
    conn.close()
    return [{"name": r[0], "description": r[1], "count": r[2], "team": r[3]} for r in rows]

def db_delete_role(guild_id, name):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM game_roles WHERE guild_id=? AND role_name=?", (guild_id, name))
    conn.commit()
    conn.close()
    invalidate_cache(guild_id)

def db_get_state(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT * FROM game_state WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return None
    # Use cursor description so column order never matters regardless of ALTER TABLEs
    cols = [d[0] for d in c.description]
    conn.close()
    return dict(zip(cols, row))

def db_set_state(guild_id, **kwargs):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO game_state (guild_id) VALUES (?)", (guild_id,))
    for key, val in kwargs.items():
        c.execute(f"UPDATE game_state SET {key}=? WHERE guild_id=?", (val, guild_id))
    conn.commit()
    conn.close()
    invalidate_cache(guild_id)

def db_clear_state(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    for tbl in ["game_state","player_assignments","night_actions",
                "witch_uses","game_counters","day_votes","wolf_votes","game_log",
                "cupid_bonds","shadow_wolf_list","elder_hits","lobby",
                "blessed_wolf_checks","npc_accusations","elimination_log","witch_nights","cupid_bond_current","speech_violations"]:
        c.execute(f"DELETE FROM {tbl} WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()
    invalidate_cache(guild_id)

def db_save_assignments(guild_id, assignments, channel_map):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM player_assignments WHERE guild_id=?", (guild_id,))
    for pid, role in assignments.items():
        c.execute("INSERT INTO player_assignments VALUES (?,?,?,1,?)",
                  (guild_id, pid, role, channel_map.get(pid)))
    conn.commit()
    conn.close()

def db_get_assignments(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT player_id, role_name, is_alive, channel_id FROM player_assignments WHERE guild_id=?", (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def db_set_player_alive(guild_id, player_id, alive: bool):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE player_assignments SET is_alive=? WHERE guild_id=? AND player_id=?",
              (1 if alive else 0, guild_id, player_id))
    conn.commit()
    conn.close()

def db_get_night_num(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT night_num FROM game_counters WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def db_increment_night(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO game_counters VALUES (?,0)", (guild_id,))
    c.execute("UPDATE game_counters SET night_num=night_num+1 WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()

def db_save_night_action(guild_id, night_num, actor_id, action_type, target_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO night_actions VALUES (?,?,?,?,?,0)",
              (guild_id, night_num, actor_id, action_type, target_id))
    conn.commit()
    conn.close()

def db_get_night_actions(guild_id, night_num):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT actor_id, action_type, target_id FROM night_actions WHERE guild_id=? AND night_num=?",
              (guild_id, night_num))
    rows = c.fetchall()
    conn.close()
    return rows

def db_get_witch_uses(guild_id, player_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT used_save, used_kill FROM witch_uses WHERE guild_id=? AND player_id=?", (guild_id, player_id))
    row = c.fetchone()
    conn.close()
    return (row[0], row[1]) if row else (0, 0)

def db_set_witch_use(guild_id, player_id, save=None, kill=None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO witch_uses VALUES (?,?,0,0)", (guild_id, player_id))
    if save is not None:
        c.execute("UPDATE witch_uses SET used_save=? WHERE guild_id=? AND player_id=?", (save, guild_id, player_id))
    if kill is not None:
        c.execute("UPDATE witch_uses SET used_kill=? WHERE guild_id=? AND player_id=?", (kill, guild_id, player_id))
    conn.commit()
    conn.close()

def db_set_day_vote(guild_id, voter_id, target_id, day_num=None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO day_votes VALUES (?,?,?)", (guild_id, voter_id, target_id))
    conn.commit()
    conn.close()
    # Record in persistent vote history
    if day_num is not None:
        db_record_vote_history(guild_id, day_num, voter_id, target_id, "vote")

def db_remove_day_vote(guild_id, voter_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM day_votes WHERE guild_id=? AND voter_id=?", (guild_id, voter_id))
    conn.commit()
    conn.close()

def db_get_day_votes(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT voter_id, target_id FROM day_votes WHERE guild_id=?", (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def db_clear_day_votes(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM day_votes WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()

def db_set_wolf_vote(guild_id, night_num, voter_id, target_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO wolf_votes VALUES (?,?,?,?)",
              (guild_id, night_num, voter_id, target_id))
    conn.commit()
    conn.close()

def db_get_wolf_votes(guild_id, night_num):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT voter_id, target_id FROM wolf_votes WHERE guild_id=? AND night_num=?",
              (guild_id, night_num))
    rows = c.fetchall()
    conn.close()
    return rows

# Game log
def db_log_event(guild_id, phase, event):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT COALESCE(MAX(entry_id),0)+1 FROM game_log WHERE guild_id=?", (guild_id,))
    next_id = c.fetchone()[0]
    ts = datetime.now().strftime("%H:%M")
    c.execute("INSERT INTO game_log VALUES (?,?,?,?,?)", (guild_id, next_id, ts, phase, event))
    conn.commit()
    conn.close()

def db_get_log(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT timestamp, phase, event FROM game_log WHERE guild_id=? ORDER BY entry_id", (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows

# Vote history / Turn log / Block log helpers
def db_record_vote_history(guild_id, day_num, voter_id, target_id, action="vote"):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT COALESCE(MAX(entry_id),0)+1 FROM vote_history WHERE guild_id=?", (guild_id,))
    eid = c.fetchone()[0]
    c.execute("INSERT INTO vote_history VALUES (?,?,?,?,?,?)",
              (guild_id, eid, day_num, voter_id, target_id, action))
    conn.commit()
    conn.close()

def db_get_vote_history(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "SELECT day_num, voter_id, target_id, action FROM vote_history WHERE guild_id=? ORDER BY entry_id",
        (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def db_record_turn(guild_id, night_num, actor_id, target_id, result):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT COALESCE(MAX(entry_id),0)+1 FROM turn_log WHERE guild_id=?", (guild_id,))
    eid = c.fetchone()[0]
    c.execute("INSERT INTO turn_log VALUES (?,?,?,?,?,?)",
              (guild_id, eid, night_num, actor_id, target_id, result))
    conn.commit()
    conn.close()

def db_get_turn_log(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "SELECT night_num, actor_id, target_id, result FROM turn_log WHERE guild_id=? ORDER BY entry_id",
        (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def db_record_block(guild_id, night_num, blocker_id, target_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT COALESCE(MAX(entry_id),0)+1 FROM block_log WHERE guild_id=?", (guild_id,))
    eid = c.fetchone()[0]
    c.execute("INSERT INTO block_log VALUES (?,?,?,?,?)",
              (guild_id, eid, night_num, blocker_id, target_id))
    conn.commit()
    conn.close()

def db_get_block_log(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
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
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO cupid_bonds VALUES (?,?,?)",
              (guild_id, player1_id, player2_id))
    conn.commit()
    conn.close()

def db_get_cupid_bond(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT player1_id, player2_id FROM cupid_bonds WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    conn.close()
    return row  # (p1, p2) or None

def db_clear_cupid_bond(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM cupid_bonds WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()

# Stats
def db_update_stats(guild_id, player_ids, winner_ids, eliminated_ids):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    for pid in player_ids:
        c.execute("INSERT OR IGNORE INTO player_stats VALUES (?,?,0,0,0)", (guild_id, pid))
        c.execute("UPDATE player_stats SET games=games+1 WHERE guild_id=? AND player_id=?", (guild_id, pid))
    for pid in winner_ids:
        c.execute("UPDATE player_stats SET wins=wins+1 WHERE guild_id=? AND player_id=?", (guild_id, pid))
    for pid in eliminated_ids:
        c.execute("UPDATE player_stats SET eliminations=eliminations+1 WHERE guild_id=? AND player_id=?", (guild_id, pid))
    conn.commit()
    conn.close()

def db_get_stats(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT player_id, games, wins, eliminations FROM player_stats WHERE guild_id=? ORDER BY wins DESC", (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows

# Replay protection
def db_save_last_roles(guild_id, role_dict):
    import json
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO last_role_set VALUES (?,?)", (guild_id, json.dumps(role_dict)))
    conn.commit()
    conn.close()

def db_get_last_roles(guild_id):
    import json
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT role_json FROM last_role_set WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    conn.close()
    return json.loads(row[0]) if row and row[0] else {}


# ====================== NPC DB HELPERS ======================

def db_save_npc(guild_id, npc_id, name, avatar_url, personality, backstory,
                role_name, channel_id, webhook_id, webhook_token):
    import json
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO npcs VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?,?,?)",
              (guild_id, npc_id, name, avatar_url, personality, backstory,
               role_name, channel_id, webhook_id, webhook_token, "[]", "[]", "", "[]"))
    conn.commit()
    conn.close()

def db_get_npcs(guild_id):
    import json
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT * FROM npcs WHERE guild_id=?", (guild_id,))
    rows = c.fetchall()
    conn.close()
    cols = ["guild_id","npc_id","name","avatar_url","personality","backstory",
            "role_name","channel_id","webhook_id","webhook_token","is_alive",
            "suspicions","chat_history","memory_summary","pinned_events"]
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

def db_update_npc_suspicions(guild_id, npc_id, suspicions: list):
    import json
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE npcs SET suspicions=? WHERE guild_id=? AND npc_id=?",
              (json.dumps(suspicions), guild_id, npc_id))
    conn.commit()
    conn.close()

def db_update_npc_chat_history(guild_id, npc_id, history: list):
    import json
    # No hard truncation — compression handles memory management
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE npcs SET chat_history=? WHERE guild_id=? AND npc_id=?",
              (json.dumps(history), guild_id, npc_id))
    conn.commit()
    conn.close()

def db_set_npc_alive(guild_id, npc_id, alive: bool):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE npcs SET is_alive=? WHERE guild_id=? AND npc_id=?",
              (1 if alive else 0, guild_id, npc_id))
    conn.commit()
    conn.close()

def db_delete_npcs(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
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
    "Seer", "Doctor", "Surgeon", "Bodyguard", "Witch",
    "Sheriff", "Huntsman", "Insomniac", "Medium", "Gravedigger",
    "Hermit", "Agitator", "Governor", "Clone", "Shapeshifter", "Cupid",
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
    "Virgin", "Blessed Wolf", "Werekitten", "Fairy Elf", "Warlock",
}

# Roles that skip night (no active ability — just wait)
NIGHT_PASSIVE_ROLES = {
    "Villager", "Village Idiot", "Village Jokester", "Drunk",
    "Prostitute", "Virgin", "Pothead", "Diseased", "Elder",
    "Mayor", "Time Lord", "Lycan", "Traitor", "Jafar",
    "Wolf", "Blessed Wolf", "White Wolf",  # White Wolf handled separately
}

def get_role_info(guild_id, role_name):
    for r in cached_load_roles(guild_id):
        if r["name"] == role_name:
            return r
    return {"name": role_name, "description": "", "count": 1, "team": "village"}

def get_team(guild_id, role_name):
    return get_role_info(guild_id, role_name).get("team", "village")

# Fast in-memory mod role cache — populated by /setup_roles, never hits DB
_mod_role_cache = {}  # guild_id -> role_id

def is_mod():
    async def predicate(interaction: discord.Interaction):
        if interaction.user.guild_permissions.administrator:
            return True
        # Always hit DB fresh
        state   = db_get_state(interaction.guild_id) or {}
        role_id = state.get("mod_role_id")
        if not role_id:
            print(f"[is_mod] FAIL — no mod_role_id in DB for guild {interaction.guild_id}")
            return False
        _mod_role_cache[interaction.guild_id] = role_id
        # Fetch member fresh from Discord so roles are never stale
        try:
            member = await interaction.guild.fetch_member(interaction.user.id)
            print(f"[is_mod] fetched member {member} roles={[r.id for r in member.roles]} looking for {role_id}")
        except Exception as e:
            print(f"[is_mod] fetch_member failed: {e} — falling back to interaction.user")
            member = interaction.user
        if any(r.id == role_id for r in member.roles):
            return True
        print(f"[is_mod] FAIL — role {role_id} not in member roles {[r.id for r in member.roles]}")
        return False
    return app_commands.check(predicate)

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

def build_player_list_embed(guild, rows):
    embed = discord.Embed(title="👥 Players This Game", color=0x5865F2)
    lines = []
    for pid, role_name, is_alive, _ in rows:
        member = guild.get_member(pid)
        name   = member.display_name if member else f"Unknown ({pid})"
        status = "✅" if is_alive else "💀"
        lines.append(f"{status} {name}")
    embed.description = "\n".join(lines) or "No players."
    embed.set_footer(text="✅ Alive  •  💀 Eliminated")
    return embed

def build_role_list_embed(final_counts, all_roles):
    embed = discord.Embed(title="📜 Roles In This Game", color=0x9B59B6)
    village_lines, wolf_lines, neutral_lines = [], [], []
    for role_name, count in final_counts.items():
        info  = all_roles.get(role_name, {})
        team  = info.get("team", "village")
        desc  = (info.get("description") or "No description")[:80]
        line  = f"**{role_name}** ×{count}\n> {desc}"
        if team == "wolf": wolf_lines.append(line)
        elif team == "neutral": neutral_lines.append(line)
        else: village_lines.append(line)
    if village_lines:
        embed.add_field(name="🏘️ Village Roles",  value="\n".join(village_lines),  inline=False)
    if wolf_lines:
        embed.add_field(name="🐺 Wolf Roles",     value="\n".join(wolf_lines),     inline=False)
    if neutral_lines:
        embed.add_field(name="⚖️ Neutral Roles",  value="\n".join(neutral_lines),  inline=False)
    embed.set_footer(text="Roles are public — who has them is secret.")
    return embed

def build_day_vote_embed(guild, votes, rows, end_time=None, anonymous=False):
    pid_to_name = {}
    for pid, _, _, _ in rows:
        m = guild.get_member(pid)
        pid_to_name[pid] = m.display_name if m else str(pid)

    tally    = {}  # target_id -> [voter_names]  (None = abstain)
    abstains = []
    for voter_id, target_id in votes:
        voter_name = pid_to_name.get(voter_id, str(voter_id))
        if target_id is None:
            abstains.append(voter_name)
        else:
            tally.setdefault(target_id, []).append(voter_name)

    embed = discord.Embed(
        title="🗳️ Village Day Vote — Live",
        description="Vote using the buttons below. Results update in real time.",
        color=0xFF4444
    )

    if end_time:
        embed.description += f"\n⏰ Vote closes <t:{end_time}:R>"

    if not tally and not abstains:
        embed.add_field(name="No votes yet", value="Press **Cast Vote** to vote.", inline=False)
    else:
        if tally:
            sorted_targets = sorted(tally.items(), key=lambda x: len(x[1]), reverse=True)
            top_count = len(sorted_targets[0][1])
            for target_id, voters in sorted_targets:
                target_name = pid_to_name.get(target_id, str(target_id))
                leading = "🔴 " if len(voters) == top_count else ""
                if anonymous:
                    embed.add_field(
                        name=f"{leading}{target_name} — {len(voters)} vote(s)",
                        value="*Voters hidden until vote closes*",
                        inline=False
                    )
                else:
                    embed.add_field(
                        name=f"{leading}{target_name} — {len(voters)} vote(s)",
                        value="Voted by: " + ", ".join(voters),
                        inline=False
                    )
        if abstains:
            count = len(abstains)
            embed.add_field(name="🤐 Abstaining",
                            value=f"{count} player(s)" if anonymous else ", ".join(abstains),
                            inline=False)

    alive_count = sum(1 for r in rows if r[2] == 1)
    embed.set_footer(text=f"{len(votes)}/{alive_count} alive players have responded")
    return embed

def build_wolf_vote_embed(guild, wolf_votes, alive_players, wolf_player_ids, night_num):
    pid_to_name = {p.id: p.display_name for p in alive_players}
    tally = {}
    for voter_id, target_id in wolf_votes:
        m = guild.get_member(voter_id)
        tally.setdefault(target_id, []).append(m.display_name if m else str(voter_id))

    voted_wolves = {v for v, _ in wolf_votes}
    not_voted    = [guild.get_member(wid) for wid in wolf_player_ids if wid not in voted_wolves]

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
            leading = "🎯 " if len(voters) == top_count else ""
            embed.add_field(
                name=f"{leading}{target_name} — {len(voters)} vote(s)",
                value="Voted by: " + ", ".join(voters),
                inline=False
            )
    else:
        embed.add_field(name="No votes yet", value="Use the dropdown below.", inline=False)

    if not_voted:
        embed.add_field(name="⏳ Waiting on", value=", ".join(m.display_name for m in not_voted if m), inline=False)

    embed.set_footer(text=f"{len(voted_wolves)}/{len(wolf_player_ids)} wolves have voted")
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
        lines = [f"`{ts}` **[{ph}]** {ev}" for ts, ph, ev in log[-20:]]
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

def build_hall_of_fame_embed(guild, rows):
    """Pinned embed showing the top 3 players of all time with extended stats."""
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

    # Extended stats from DB
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

        # Fetch extended stats
        c.execute("SELECT wolf_votes_correct, times_accused, seer_correct FROM player_stats WHERE player_id=?", (pid,))
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

    conn.close()

    # Special records section
    c2 = sqlite3.connect(DB_FILE).cursor()
    c2.execute("SELECT player_id, wolf_votes_correct FROM player_stats ORDER BY wolf_votes_correct DESC LIMIT 1")
    top_voter = c2.fetchone()
    c2.execute("SELECT player_id, times_accused FROM player_stats ORDER BY times_accused DESC LIMIT 1")
    top_accused = c2.fetchone()
    c2.execute("SELECT player_id, seer_correct FROM player_stats ORDER BY seer_correct DESC LIMIT 1")
    top_seer = c2.fetchone()

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

async def _build_lobby_embed(guild, player_ids: list, is_open: bool, roster_size: int = 0) -> discord.Embed:
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

        # Refresh embed
        embed = await _build_lobby_embed(interaction.guild, player_ids, True, self.roster_size)
        await interaction.response.edit_message(embed=embed, view=self)

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

        embed = await _build_lobby_embed(interaction.guild, player_ids, True, self.roster_size)
        await interaction.response.edit_message(embed=embed, view=self)


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
        vc_ch_id = state.get("village_chat_ch_id")
        ch = interaction.guild.get_channel(vc_ch_id) if vc_ch_id else None
        if not ch:
            # Post in current channel
            ch = interaction.channel

        embed = await _build_lobby_embed(interaction.guild, [], True, roster_size)
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
                embed = await _build_lobby_embed(interaction.guild, data["player_ids"], False, roster_size)
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
                embed = await _build_lobby_embed(interaction.guild, player_ids,
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
    vc_ch   = guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
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
    conn.close()

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

async def post_night_transition(guild, night_num: int, duration_secs: int):
    """Post a rich night-start embed in village-chat."""
    state  = cached_get_state(guild.id)
    vc_ch  = guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
    if not vc_ch:
        return
    import time
    end_ts  = int(time.time()) + duration_secs
    quote   = random.choice(NIGHT_QUOTES)
    embed   = discord.Embed(
        title       = f"🌙 Night {night_num} Begins",
        description = f"*{quote}*",
        color       = 0x2C3060
    )
    embed.add_field(name="⏰ Night ends", value=f"<t:{end_ts}:R>", inline=True)
    embed.add_field(name="🌙 Phase",      value=f"Night {night_num}", inline=True)
    embed.set_footer(text="Submit your night actions in your private channel.")
    await vc_ch.send(embed=embed)

async def post_day_transition(guild, night_num: int, duration_secs: int, deaths: list = None):
    """Post a rich day-start embed in village-chat."""
    state  = cached_get_state(guild.id)
    vc_ch  = guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
    if not vc_ch:
        return
    import time
    end_ts = int(time.time()) + duration_secs
    quote  = random.choice(DAY_QUOTES)
    embed  = discord.Embed(
        title       = f"☀️ Day {night_num} Begins",
        description = f"*{quote}*",
        color       = 0xE67E22
    )
    if deaths:
        death_text = "\n".join(f"💀 {name}" for name in deaths)
        embed.add_field(name="💀 Died last night", value=death_text, inline=False)
    else:
        embed.add_field(name="💀 Died last night", value="Nobody. The village got lucky.", inline=False)
    embed.add_field(name="⏰ Day ends", value=f"<t:{end_ts}:R>", inline=True)
    embed.add_field(name="☀️ Phase",   value=f"Day {night_num}", inline=True)
    embed.set_footer(text="Discuss, debate, and vote in the day-vote channel.")
    await vc_ch.send(embed=embed)

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
    for voter_id, target_id in votes:
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
        super().__init__(timeout=3600)  # 1 hour to post
        self.guild_id = guild_id
        self._embed   = embed
        btn = Button(label="📋 Post Blood Board to Village Chat", style=discord.ButtonStyle.green)
        btn.callback = self.on_post
        self.add_item(btn)

    async def on_post(self, interaction: discord.Interaction):
        state  = cached_get_state(self.guild_id)
        vc_ch  = interaction.guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
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

    wolves_needed = max(0, len(alive_village) - len(alive_wolves) + 1)

    # Build player lists with NPC labels
    npcs = db_get_npcs(guild_id)
    npc_ids = {n["npc_id"] for n in npcs}

    def player_line(pid, role):
        m        = guild.get_member(pid)
        name     = m.display_name if m else str(pid)
        npc_tag  = " `NPC`" if pid in npc_ids else ""
        return f"{name}{npc_tag} — {role}"

    village_lines = [player_line(r[0], r[1]) for r in alive_village]
    wolf_lines    = [player_line(r[0], r[1]) for r in alive_wolves]
    neutral_lines = [player_line(r[0], r[1]) for r in alive_neutral]

    embed = discord.Embed(title="⚖️ Win Condition Tracker", color=0x2C3E50)
    embed.add_field(name=f"🏘️ Village ({len(alive_village)})",
                    value="\n".join(village_lines) or "None", inline=True)
    embed.add_field(name=f"🐺 Wolves ({len(alive_wolves)})",
                    value="\n".join(wolf_lines)    or "None", inline=True)
    embed.add_field(name=f"⚖️ Neutral ({len(alive_neutral)})",
                    value="\n".join(neutral_lines) or "None", inline=True)

    if len(alive_wolves) == 0:
        embed.add_field(name="🏆 Status", value="Village wins — all wolves eliminated!", inline=False)
        embed.color = 0x27AE60
    elif len(alive_wolves) >= len(alive_village):
        embed.add_field(name="🏆 Status", value="Wolves win — they match or outnumber village!", inline=False)
        embed.color = 0xC0392B
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
    try:
        msg = await ch.fetch_message(mid)
        await msg.edit(embed=build_player_list_embed(guild, rows))
    except Exception:
        pass

async def refresh_day_vote(guild):
    state     = cached_get_state(guild.id)
    ch        = guild.get_channel(state.get("day_vote_ch_id") or 0)
    mid       = state.get("day_vote_msg_id")
    if not ch or not mid:
        return
    rows      = db_get_assignments(guild.id)
    votes     = db_get_day_votes(guild.id)
    end_time  = state.get("day_vote_end_time")
    anonymous = bool(state.get("anon_vote", 0))
    embed     = build_day_vote_embed(guild, votes, rows, end_time, anonymous)
    # Disable full breakdown button in anonymous mode
    view      = DayVoteView(guild.id, anonymous=anonymous)
    try:
        msg = await ch.fetch_message(mid)
        await msg.edit(embed=embed, view=view)
    except Exception:
        pass

async def refresh_wolf_vote(guild, night_num):
    """Refresh the wolf-vote embed AND the dropdown view together so it never goes stale."""
    state = cached_get_state(guild.id)
    ch  = guild.get_channel(state.get("wolf_vote_channel_id") or 0)
    mid = state.get("wolf_vote_msg_id")
    if not ch or not mid:
        return
    rows          = db_get_assignments(guild.id)
    wolf_ids      = [r[0] for r in rows if r[2] == 1 and get_team(guild.id, r[1]) == "wolf"]
    alive_players = [guild.get_member(r[0]) for r in rows if r[2] == 1]
    alive_players = [p for p in alive_players if p]
    wolf_votes    = db_get_wolf_votes(guild.id, night_num)
    embed = build_wolf_vote_embed(guild, wolf_votes, alive_players, wolf_ids, night_num)
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



async def _npc_night_farewell(guild, guild_id: int):
    """NPCs say goodnight in village-chat when night starts."""
    state     = cached_get_state(guild_id)
    vc_ch     = guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
    if not vc_ch:
        return
    npcs      = db_get_npcs(guild_id)
    alive_npcs= [n for n in npcs if n["is_alive"]]
    farewells = [
        "Alright, heading off for the night. See you all in the morning.",
        "Night everyone. Hopefully we all make it to morning.",
        "Going to sleep. Stay safe out there.",
        "Night. Try not to get eaten.",
        "Off to bed. Eyes open tomorrow.",
        "Goodnight. Let's see what the morning brings.",
        "Signing off. Stay alive.",
    ]
    for npc in alive_npcs:
        if random.random() < 0.7:  # 70% chance each NPC says goodnight
            await asyncio.sleep(random.uniform(5, 20))
            msg = random.choice(farewells)
            try:
                async with aiohttp.ClientSession() as session:
                    wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                    await wh.send(msg, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
            except Exception as e:
                print(f"NPC night farewell error: {e}")


async def _npc_survival_reaction(guild, guild_id: int, npc_id: int):
    """Called when a vote closes and an NPC was top-voted but survived."""
    npcs     = db_get_npcs(guild_id)
    npc      = next((n for n in npcs if n["npc_id"] == npc_id and n["is_alive"]), None)
    if not npc:
        return
    state  = cached_get_state(guild_id)
    vc_ch  = guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
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
    vc_ch     = guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
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
            print(f"[NPC Memory] Compressed memory for {npc['name']} "
                  f"({len(to_compress)} messages → summary)")
    except Exception as e:
        print(f"[NPC Memory] Compression failed for {npc['name']}: {e}")


async def _maybe_compress_memory(guild_id: int, npc_id: int):
    """Check if compression is needed and run it asynchronously."""
    npc = db_get_npc(guild_id, npc_id)
    if npc and len(npc.get("chat_history", [])) >= 30:
        asyncio.create_task(_compress_npc_memory(guild_id, npc))

# ====================== NPC SYSTEM ======================
# In-memory cache of active NPC webhook objects keyed by (guild_id, npc_id)
_npc_webhooks = {}   # (guild_id, npc_id) -> discord.Webhook

MAX_NPCS = 3

# ── Claude API call ───────────────────────────────────────────────────────
async def _claude(prompt: str, system: str, max_tokens: int = 300) -> str:
    """Call the Anthropic API and return the text response."""
    import json as _json
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
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=body) as resp:
            data = await resp.json()
    blocks = data.get("content", [])
    return " ".join(b.get("text","") for b in blocks if b.get("type") == "text").strip()


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

    lines = [
        f"Phase: {'Night' if phase == 'night' else 'Day'} {night_num}",
        f"Alive players: {', '.join(alive_names) or 'none'}",
        f"Eliminated players: {', '.join(dead_names) or 'none'}",
        f"Your role: {npc['role_name']} ({team} team) — {role_desc}",
        f"Your current suspicions (persistent): {susp_text}",
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
    wolf_instructions = ""
    if team == "wolf":
        wolf_instructions = (
            "- You are secretly a wolf but must appear as a villager in public\n"
            "- If someone claims a Seer checked you and said wolf, deny it firmly and act offended\n"
            "- Occasionally cast suspicion on actual village players to deflect attention\n"
            "- Never coordinate with wolves in public chat — only in the wolf den\n"
        )

    return (
        f"You are {npc['name']}, a real person playing an online Mafia/Werewolf game. "
        f"You are NOT an AI. You are a human player.\n\n"
        f"Your personality: {npc['personality']}\n"
        f"Your backstory: {npc['backstory']}\n\n"
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
    history    = npc.get("chat_history", [])
    game_ctx   = _npc_game_context(guild, guild_id, npc)
    system     = _npc_system_prompt(npc)

    # Build recent chat string (last 10 messages)
    recent = "\n".join(f"{m['author']}: {m['text']}" for m in history[-10:])

    prompt = (
        f"Game state:\n{game_ctx}\n\n"
        f"Recent village chat:\n{recent or '(no messages yet)'}\n\n"
        f"{author_name} just said: \"{trigger_message}\"\n\n"
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

    # Trigger memory compression if history is getting long
    asyncio.create_task(_maybe_compress_memory(guild_id, npc["npc_id"]))

    # Update suspicions based on response (async, best-effort)
    asyncio.create_task(_update_npc_suspicions(guild, guild_id, npc, trigger_message, author_name, response))


async def _update_npc_suspicions(guild, guild_id, npc, message, author, response):
    """Ask Claude if this NPC should update their suspicion list."""
    suspicions = npc.get("suspicions", [])
    prompt = (
        f"You are {npc['name']} in a Mafia game. Current suspicions: {suspicions}\n"
        f"{author} said: \"{message}\"\n"
        f"You replied: \"{response}\"\n\n"
        f"Should you add or remove anyone from your suspicion list based on this exchange? "
        f"Respond with JSON only: {{\"suspicions\": [\"name1\", \"name2\"]}} "
        f"Return the full updated list, max 4 people."
    )
    system = "You update suspicion lists for Mafia game players. Respond only with JSON."
    try:
        result = await _claude(prompt, system, max_tokens=60)
        import json as _json
        # Strip markdown fences if present
        clean = result.replace("```json","").replace("```","").strip()
        data  = _json.loads(clean)
        new_suspicions = data.get("suspicions", suspicions)
        db_update_npc_suspicions(guild_id, npc["npc_id"], new_suspicions)
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

    prompt = (
        f"Game state:\n{game_ctx}\n\n"
        f"Players who have falsely accused you in the past: {accuser_text}\n\n"
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
    vc_ch  = guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
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
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://randomuser.me/api/") as resp:
                data = await resp.json()
        person      = data["results"][0]
        first       = person["name"]["first"]
        last        = person["name"]["last"]
        nationality = person["nat"]
        avatar_url  = person["picture"]["large"]
        npc_name    = f"{first} {last}"
    except Exception as e:
        return await interaction.followup.send(f"❌ Failed to fetch NPC identity: {e}", ephemeral=True)

    # ── Generate personality + backstory ───────────────────────────────────
    try:
        personality, backstory = await _generate_npc_profile(first, last, nationality)
    except Exception as e:
        return await interaction.followup.send(f"❌ Failed to generate NPC profile: {e}", ephemeral=True)

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
    village_ch = interaction.guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
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
    db_save_npc(interaction.guild_id, npc_id, npc_name, avatar_url,
                personality, backstory, chosen_role, priv_ch.id, wh.id, wh.token)

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
        wolf_vote_ch = interaction.guild.get_channel(state.get("wolf_vote_channel_id") or 0)
        if wolf_ch:
            # Create a webhook for the wolf NPC in the den
            try:
                wolf_wh = await wolf_ch.create_webhook(name=f"{npc_name} •")
                # Store wolf_wh alongside main webhook (reuse same id/token fields — den uses same webhook)
                # We post to wolf-den using the village-chat webhook but a different channel
                await wolf_ch.send(fmt(f"🐺 {npc_name} has joined the pack."))
            except Exception:
                pass
        if wolf_vote_ch:
            try:
                await wolf_vote_ch.send(fmt(f"🐺 {npc_name} can vote here."))
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
    asyncio.create_task(_npc_proactive_loop(interaction.guild, interaction.guild_id))
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
        village_ch = interaction.guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
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
        "tell": "Uses ellipses (...) noticeably more when they are lying or deflecting.",
        "instruction": "When you are hiding something or deflecting an accusation, use ellipses (...) naturally in your message."
    },
    {
        "tell": "Asks a question back instead of answering directly when they feel cornered.",
        "instruction": "When accused or pressed, respond with a question directed back at the accuser before answering."
    },
    {
        "tell": "Goes unusually quiet (short one-word replies) right after a wolf kill they knew about.",
        "instruction": "The morning after a kill, keep your first response very short — one or two words only."
    },
]

# ── Proactive NPC chat — fires on a timer during day phase ────────────────
async def _npc_proactive_loop(guild, guild_id: int):
    """Run throughout the game — NPCs initiate conversation every 2-3 hours during day."""
    while game_active(guild_id):
        await asyncio.sleep(random.uniform(7200, 10800))  # 2-3 hours
        state = cached_get_state(guild_id)
        if state.get("phase") != "day":
            continue
        vc_ch = guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
        if not vc_ch:
            continue
        npcs       = db_get_npcs(guild_id)
        alive_npcs = [n for n in npcs if n["is_alive"]]
        if not alive_npcs:
            break
        # Pick one random NPC to speak
        npc      = random.choice(alive_npcs)
        game_ctx = _npc_game_context(guild, guild_id, npc)
        system   = _npc_system_prompt(npc)
        prompt   = (
            f"Game state:\n{game_ctx}\n\n"
            f"You feel like saying something in the village chat. Choose one:\n"
            f"1. A morning check-in or casual comment\n"
            f"2. Accuse someone from your suspicion list\n"
            f"3. React to recent game events\n"
            f"Keep it 1-2 sentences. Natural and human."
        )
        try:
            response = await _claude(prompt, system, max_tokens=100)
            if response:
                await asyncio.sleep(random.uniform(5, 20))
                async with aiohttp.ClientSession() as session:
                    wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                    await wh.send(response, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
                # Update chat history
                history = npc.get("chat_history", [])
                history.append({"author": npc["name"], "text": response})
                db_update_npc_chat_history(guild_id, npc["npc_id"], history)
        except Exception as e:
            print(f"NPC proactive error: {e}")


# ── NPC death reaction ────────────────────────────────────────────────────
async def _npc_react_to_death(guild, guild_id: int, dead_name: str):
    """Have alive NPCs react to a player death in village-chat."""
    state  = cached_get_state(guild_id)
    vc_ch  = guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
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
                async with aiohttp.ClientSession() as session:
                    wh = discord.Webhook.partial(npc["webhook_id"], npc["webhook_token"], session=session)
                    await wh.send(response, username=f"{npc['name']} •", avatar_url=npc["avatar_url"])
                history = npc.get("chat_history", [])
                history.append({"author": npc["name"], "text": response})
                db_update_npc_chat_history(guild_id, npc["npc_id"], history)
        except Exception as e:
            print(f"NPC death react error: {e}")


# ── NPC wolf den coordination ─────────────────────────────────────────────
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
    for voter_id, target_id in votes:
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
        duration  = state.get("night_duration", 36000)
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
        night_num = db_get_night_num(self.guild_id)
        duration  = state.get("night_duration", 36000)
        await _run_start_night(interaction.guild, self.guild_id, night_num, duration, state)


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

    for row in rows:
        pid, role_name, is_alive, ch_id = row
        if not is_alive or not ch_id: continue
        ch = guild.get_channel(ch_id)
        if not ch: continue
        member     = guild.get_member(pid)
        night_view = get_night_view(guild_id, pid, role_name, alive_players)
        if night_view:
            role_descs = {
                "Seer":"Choose a player to investigate.",
                "Doctor":"Choose to save or skip.",
                "Surgeon":"Choose to save or skip. You have 3 saves total.",
                "Bodyguard":"Choose a player to guard.",
                "Witch":"Use your save or poison potion, or skip.",
                "Sheriff":"Acknowledge your role.",
                "Huntsman":"Choose a player to protect, or skip.",
                "Insomniac":"Acknowledge — you will receive hints every other night from Night 3.",
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
                "Bloodhound":"Choose a player to identify.",
                "Bloodletter":"Mark a target with wolf blood, or skip.",
                "Crazed Wolf":"Select two kill targets.",
                "Dire Wolf":"Choose your secret mate (Night 1 only).",
                "Echo-Stalker":"Haunt a player or use the regular wolf kill.",
                "Shadow Wolf":"Choose a voter to kill (post-death ability).",
                "Werekitten":"Acknowledge your role.",
                "White Wolf":"Choose an independent kill, or skip.",
                "Oracle":"Send your yes/no question to the mod.",
                "Warlock":"Acknowledge wish status.",
                "Fairy Elf":"Acknowledge happy ending status.",
            }
            desc = role_descs.get(role_name, "Submit your night action below.")
            await ch.send(fmt(f"🌙 Night {night_num} — time to act!\n\n{desc}"), view=night_view)
            status_view = NightStatusView(guild_id, pid, role_name, night_num)
            await ch.send(fmt("Let the mod know your intent for tonight:"), view=status_view)
        elif role_name in NIGHT_NO_BUTTON_ROLES:
            # No ability — night announcement only, no buttons
            await ch.send(fmt(f"🌙 Night {night_num} has begun. Sleep tight — await the morning."))
        else:
            # Has passive awareness — keep status buttons
            status_view = NightStatusView(guild_id, pid, role_name, night_num)
            await ch.send(fmt(f"🌙 Night {night_num} — sleep tight. Await morning."), view=status_view)

    # Wolf vote
    wolf_vote_ch = guild.get_channel(state.get("wolf_vote_channel_id") or 0)
    if wolf_vote_ch:
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
        embed      = build_wolf_vote_embed(guild, wolf_votes, alive_players, wolf_ids, night_num)
        wv_msg     = await wolf_vote_ch.send(embed=embed, view=view)
        db_set_state(guild_id, wolf_vote_msg_id=wv_msg.id)

    await post_night_transition(guild, night_num, duration)
    await set_bot_status(f"🌙 Night {night_num} in progress")

    # Insomniac hint on odd nights >= 3
    if night_num >= 3 and night_num % 2 == 1:
        asyncio.create_task(_send_insomniac_hint(guild, guild_id, night_num))

    # NPC night farewell
    asyncio.create_task(_npc_night_farewell(guild, guild_id))

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
    asyncio.create_task(_run_npc_night_actions())

    await log_event(guild, f"Night {night_num}", f"🌙 Night {night_num} began ({mins} min timer)")
    await post_mod_log(guild, f"🌙 **Night {night_num}** started. Duration: {mins} min.")

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
    c = conn.cursor()
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
    if guild_id_str:
        try:
            gid       = int(guild_id_str.strip())
            guild_obj = discord.Object(id=gid)
            await tree.copy_global_to(guild=guild_obj)
            await tree.sync(guild=guild_obj)
            print(f"Dev mode: commands synced instantly to guild {gid}")
        except Exception as e:
            print(f"Dev guild sync failed: {e} — falling back to global sync")
            await tree.sync()
    else:
        await tree.sync()
        print(f"Global sync triggered — commands will appear on all servers within ~1 hour")
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
            # Re-start NPC proactive loops
            npcs = db_get_npcs(guild.id)
            alive_npcs = [n for n in npcs if n["is_alive"]]
            if alive_npcs:
                asyncio.create_task(_npc_proactive_loop(guild, guild.id))
                print(f"Resumed NPC proactive loop ({len(alive_npcs)} NPCs)")

    if active_games == 0:
        await set_bot_status("💤 No active game")
        print("No active games found — status set to idle")
    else:
        print(f"Restored {active_games} active game(s)")

# ====================== ERROR HANDLER ======================

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

    # For any other unexpected error, log it and notify the user cleanly
    msg = "⚠️ Something went wrong. Please try again."
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(msg, ephemeral=True)
        else:
            await interaction.followup.send(msg, ephemeral=True)
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
    if not message.guild:
        return

    state      = cached_get_state(message.guild.id)

    # ── Speech enforcement — runs for any message in village-chat ─────────
    vc_ch_id = state.get("village_chat_ch_id")
    in_village_chat = (message.channel.id == VILLAGE_CHAT_ID or
                       (vc_ch_id and message.channel.id == vc_ch_id))
    if in_village_chat and not message.author.bot:
        rows_speech = db_get_assignments(message.guild.id)
        sender_row  = next((r for r in rows_speech if r[0] == message.author.id and r[2] == 1), None)
        if sender_row:
            asyncio.create_task(
                check_speech_violation(message, sender_row[1], message.guild.id))

    # Check if message is in an NPC private channel (mod talking to NPC directly)
    npcs_all = db_get_npcs(message.guild.id)
    npc_in_ch = next((n for n in npcs_all if n["channel_id"] == message.channel.id and n["is_alive"]), None)
    if npc_in_ch:
        asyncio.create_task(_npc_private_respond(message.guild, message.guild.id, npc_in_ch, message))
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
            asyncio.create_task(_npc_wolf_den_message(
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

        # Always respond if directly addressed, mentioned, or group question
        # 25% random chance otherwise
        should_respond = name_in_text or mentioned or is_from_npc or is_group_question or random.random() < 0.25

        if should_respond:
            asyncio.create_task(_npc_respond(
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
    duration = TextInput(label="Day duration in minutes (default: 840)",
                         placeholder="e.g. 840  →  14 hours",
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
    duration = TextInput(label="Night duration in minutes (default: 600)",
                         placeholder="e.g. 600  →  10 hours",
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

@tree.command(name="set_day_duration", description="Set day phase duration (default 840 min = 14 hrs)")
@is_mod()
async def set_day_duration(interaction: discord.Interaction):
    await interaction.response.send_modal(DayDurationModal())

@tree.command(name="set_night_duration", description="Set night phase duration (default 600 min = 10 hrs)")
@is_mod()
async def set_night_duration(interaction: discord.Interaction):
    await interaction.response.send_modal(NightDurationModal())

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

@tree.command(name="list_roles", description="Show all saved game roles")
async def list_roles(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    roles = db_load_roles(interaction.guild_id)
    if not roles:
        return await interaction.followup.send("No roles saved yet. Use `/add_role`.", ephemeral=True)

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

    await interaction.followup.send(embed=embed, ephemeral=True)

# ====================== START GAME ======================
# ── Helpers ──────────────────────────────────────────────────────────────

def _role_emoji(team):
    return "🐺" if team == "wolf" else ("⚖️" if team == "neutral" else "🏘️")

def _build_roster_text(counts, all_roles):
    if not counts:
        return "*No roles added yet.*"
    village, wolf, neutral = [], [], []
    for name, cnt in counts.items():
        info  = all_roles.get(name, {})
        team  = info.get("team", "village")
        line  = f"{_role_emoji(team)} **{name}** ×{cnt}"
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
        self.counts        = counts or {}
        self.page          = page
        self.selected_role = None
        self.npc_count     = npc_count   # 0-3 NPCs to include
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
            view = ReplayWarningView(interaction.guild_id, final_counts, self.all_roles)
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
        return (
            "**🎮 Build Your Game Roster**\n"
            "Pick a role from the dropdown, then use **+1 / −1 / +10 / −10** to set how many.\n"
            "Use **NPC −/+** to add AI players (max 3). They count as player slots.\n"
            "Hit **✅ Confirm Roster** when done.\n"
            + focused
            + npc_line
            + "\n\n─────────────────\n"
            + _build_roster_text(self.counts, self.all_roles)
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
        await interaction.response.defer(ephemeral=True)
        await self.launch_game(interaction)
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
        # Use p_role.members directly — avoids missing players from incomplete member cache
        players = [m for m in p_role.members if not m.bot]
        if len(players) != human_needed:
            return await interaction.followup.send(
                f"❌ Pool has **{human_needed}** human slots but found **{len(players)}** players with the participant role."
                + (f" ({npc_count} NPC slot(s) will be filled automatically.)" if npc_count else ""),
                ephemeral=True)

        pool = []
        for role_name, count in self.final_counts.items():
            pool.extend([role_name] * count)
        random.shuffle(pool)
        assignments = {p.id: role for p, role in zip(players, pool)}

        font = get_guild_font(interaction.guild_id)
        try:
            category = await interaction.guild.create_category(
                ch_name("Village Game", font, "🎮 "))
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

        # mod-log
        mod_log_ow = {everyone: discord.PermissionOverwrite(view_channel=False), bot_me: bot_ow}
        if mod_role:
            mod_log_ow[mod_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
        if spec_role: mod_log_ow[spec_role] = read_ow
        mod_log_ch = await category.create_text_channel(ch_name("mod-log",     font, "📋"), overwrites=mod_log_ow)

        # player-list
        player_list_ow = {everyone: read_ow, bot_me: bot_ow}
        if spec_role: player_list_ow[spec_role] = read_ow
        player_list_ch = await category.create_text_channel(ch_name("player-list",  font, "📋"), overwrites=player_list_ow)

        # role-list
        role_list_ow = {everyone: read_ow, bot_me: bot_ow}
        if spec_role: role_list_ow[spec_role] = read_ow
        role_list_ch = await category.create_text_channel(ch_name("role-list",    font, "📜"), overwrites=role_list_ow)

        # day-vote — players need send_messages=True to interact with buttons/dropdowns
        # We suppress their actual messages via the bot; the channel stays visually clean
        part_ow_vote = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)
        day_vote_ow  = {everyone: read_ow, bot_me: bot_ow}
        if p_role: day_vote_ow[p_role] = part_ow_vote
        if spec_role: day_vote_ow[spec_role]  = part_ow_vote
        day_vote_ch = await category.create_text_channel(ch_name("day-vote",     font, "🗳️"), overwrites=day_vote_ow)

        # timeline
        timeline_ow = {everyone: read_ow, bot_me: bot_ow}
        if spec_role: timeline_ow[spec_role] = read_ow
        timeline_ch = await category.create_text_channel(ch_name("timeline",     font, "⏰"), overwrites=timeline_ow)

        # stats
        stats_ow = {everyone: read_ow, bot_me: bot_ow}
        if spec_role: stats_ow[spec_role] = read_ow
        stats_ch = await category.create_text_channel(ch_name("stats",        font, "📊"), overwrites=stats_ow)

        # wheel-spins
        wheel_ch = await category.create_text_channel(ch_name("wheel-spins",  font, "🎡"), overwrites={
            everyone: read_ow, bot_me: bot_ow})

        # wolf-den (wolf chat — dead wolves get read-only)
        # Bloodhound and Wolf Pup are wolf-team but NOT in the den
        DENY_DEN = {"Bloodhound", "Wolf Pup"}
        wolf_players = [p for p in players
                        if get_team(interaction.guild_id, assignments[p.id]) == "wolf"
                        and assignments[p.id] not in DENY_DEN]
        neutral_players = [p for p in players if get_team(interaction.guild_id, assignments[p.id]) == "neutral"]
        wolf_ow = {everyone: discord.PermissionOverwrite(view_channel=False), bot_me: bot_ow}
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
        if spec_role: ghost_ow[spec_role] = read_ow
        ghost_ch = await category.create_text_channel(ch_name("ghost-chat",   font, "👻"), overwrites=ghost_ow)

        # win-tracker
        # Win tracker — mod only, never visible to players
        win_tracker_ow = {everyone: discord.PermissionOverwrite(view_channel=False), bot_me: bot_ow}
        if mod_role:  win_tracker_ow[mod_role]  = discord.PermissionOverwrite(view_channel=True, send_messages=False, read_messages=True)
        if spec_role: win_tracker_ow[spec_role] = read_ow
        win_tracker_ch = await category.create_text_channel(ch_name("win-tracker", font, "⚖️"), overwrites=win_tracker_ow)

        # village-chat — use hardcoded external channel, not bot-created
        village_chat_ch = interaction.guild.get_channel(VILLAGE_CHAT_ID)
        if not village_chat_ch:
            # Fallback — create one if the hardcoded channel isn't found
            village_chat_ow = {
                everyone: discord.PermissionOverwrite(view_channel=False),
                bot_me:   bot_ow,
            }
            if p_role:    village_chat_ow[p_role]    = discord.PermissionOverwrite(view_channel=True, send_messages=True)
            if spec_role: village_chat_ow[spec_role] = discord.PermissionOverwrite(view_channel=True, send_messages=False)
            if mod_role:  village_chat_ow[mod_role]  = discord.PermissionOverwrite(view_channel=True, send_messages=True)
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
            embed = build_role_card(player, role_name, role_info, font)
            await ch.send(player.mention, embed=embed)
            if role_name == "Cursed":
                await ch.send(fmt("⚠️ You appear as Village to investigators.\nIf attacked by wolves, you join them instead of dying."))

        if wolf_players:
            wolf_names = ", ".join(p.display_name for p in wolf_players)
            await wolf_ch.send(f"🐺 **The Den** — pack members: {wolf_names}\n"
                               f"Use 🗳️wolf-vote each night to agree on a kill target.")

        db_save_assignments(interaction.guild_id, assignments, player_channels)

        # ── Auto-create NPCs from roster ──────────────────────────────────
        if npc_count > 0:
            await interaction.followup.send(
                f"⏳ Generating {npc_count} NPC(s)... this may take a moment.",
                ephemeral=True)
            for _ in range(npc_count):
                try:
                    # Fetch random identity
                    async with aiohttp.ClientSession() as session:
                        async with session.get("https://randomuser.me/api/") as resp:
                            data = await resp.json()
                    person      = data["results"][0]
                    first       = person["name"]["first"]
                    last        = person["name"]["last"]
                    nationality = person["nat"]
                    avatar_url  = person["picture"]["large"]
                    npc_name    = f"{first} {last}"

                    # Generate personality
                    personality, backstory = await _generate_npc_profile(first, last, nationality)

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
                    db_save_npc(interaction.guild_id, npc_id, npc_name, avatar_url,
                                personality, backstory, chosen_role,
                                npc_priv_ch.id, npc_wh.id, npc_wh.token)

                    # Save to player_assignments
                    conn_npc = sqlite3.connect(DB_FILE)
                    c_npc    = conn_npc.cursor()
                    c_npc.execute("INSERT OR REPLACE INTO player_assignments VALUES (?,?,?,1,?)",
                                  (interaction.guild_id, npc_id, chosen_role, npc_priv_ch.id))
                    conn_npc.commit()
                    conn_npc.close()

                    # If wolf, give den access
                    if get_team(interaction.guild_id, chosen_role) == "wolf":
                        await wolf_ch.set_permissions(bot_me,
                            view_channel=True, send_messages=True, read_messages=True)

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
                    asyncio.create_task(_npc_proactive_loop(interaction.guild, interaction.guild_id))

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
                    asyncio.create_task(_npc_intro(npc_wh.id, npc_wh.token, avatar_url, npc_name))

                except Exception as e:
                    print(f"NPC auto-create error: {e}")
                    await post_mod_log(interaction.guild,
                        f"⚠️ Failed to auto-create NPC #{_ + 1}: {e}")
        rows = db_get_assignments(interaction.guild_id)

        # Persistent embeds
        pl_msg  = await player_list_ch.send(embed=build_player_list_embed(interaction.guild, rows))
        rl_msg  = await role_list_ch.send(embed=build_role_list_embed(self.final_counts, self.all_roles))
        dv_view = DayVoteView(interaction.guild_id)
        dv_msg  = await day_vote_ch.send(embed=build_day_vote_embed(interaction.guild, [], rows), view=dv_view)

        night_dur = state.get("night_duration", 36000)
        day_dur   = state.get("day_duration", 50400)

        db_set_state(
            interaction.guild_id,
            phase="night",
            category_id=category.id,
            wolf_channel_id=wolf_ch.id,
            wolf_vote_channel_id=None,
            ghost_channel_id=ghost_ch.id,
            mod_log_channel_id=mod_log_ch.id,
            wheel_channel_id=wheel_ch.id,
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
            win_tracker_ch_id=win_tracker_ch.id
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
    await interaction.followup.send("⏳ Ending game — deleting channels now. This may take a moment.", ephemeral=True)

    async def _do_end():
        cat = interaction.guild.get_channel(state["category_id"])
        # Clean up NPC webhooks before deleting channels
        npcs       = db_get_npcs(interaction.guild_id)
        village_ch = interaction.guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
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
        if cat:
            for ch in cat.channels:
                try: await ch.delete()
                except: pass
            try: await cat.delete()
            except: pass
        db_clear_state(interaction.guild_id)
        await set_bot_status("💤 No active game")
        try:
            await interaction.followup.send("✅ Game ended. All channels deleted.", ephemeral=True)
        except Exception:
            pass  # Followup token may have expired — that's fine, game is ended

    asyncio.create_task(_do_end())

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

    ch = await cat.create_text_channel(priv_ch_name, overwrites=ch_ow)

    # Add to DB
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO player_assignments VALUES (?,?,?,1,?)",
              (interaction.guild_id, player.id, role_name, ch.id))
    conn.commit()
    conn.close()

    # Add participant role
    p_role = interaction.guild.get_role(state.get("participant_role_id") or 0)
    if p_role:
        try: await player.add_roles(p_role)
        except: pass

    # If wolf, add to wolf-den
    if get_team(interaction.guild_id, role_name) == "wolf" and role_name not in {"Bloodhound", "Wolf Pup"}:
        wolf_ch      = interaction.guild.get_channel(state.get("wolf_channel_id") or 0)
        wolf_vote_ch = interaction.guild.get_channel(state.get("wolf_vote_channel_id") or 0)
        if wolf_ch:
            await wolf_ch.set_permissions(player, view_channel=True, send_messages=True)
            await wolf_ch.send(f"🐺 **{player.display_name}** has joined the pack!")
        if wolf_vote_ch:
            await wolf_vote_ch.set_permissions(player, view_channel=True, send_messages=True, read_messages=True, view_audit_log=False)

    # Welcome message
    team_emoji = "🐺" if role_info["team"] == "wolf" else "🏘️"
    embed = discord.Embed(
        title=f"Your Role: {role_name} {team_emoji}",
        description=role_info.get("description") or "*No description provided.*",
        color=0xFF4444 if role_info["team"] == "wolf" else 0x44BB44
    )
    await ch.send(player.mention, embed=embed)

    await refresh_player_list(interaction.guild)
    await log_event(interaction.guild, cached_get_state(interaction.guild_id).get("phase","day").capitalize(),
                    f"➕ **{player.display_name}** added to game as **{role_name}**")
    await post_mod_log(interaction.guild, f"➕ **{player.display_name}** added as **{role_name}**")
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
    c = conn.cursor()
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
        wolf_vote_ch = interaction.guild.get_channel(state.get("wolf_vote_channel_id") or 0)
        if wolf_ch:      await wolf_ch.set_permissions(player, overwrite=None)
        if wolf_vote_ch: await wolf_vote_ch.set_permissions(player, overwrite=None)

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



@tree.command(name="bind", description="Cupid — bind two players together. If one dies, both die.")
@is_mod()
@app_commands.describe(player1="First player to bind", player2="Second player to bind")
async def bind(interaction: discord.Interaction, player1: discord.Member, player2: discord.Member):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    if player1.id == player2.id:
        return await interaction.response.send_message("❌ Cannot bind a player to themselves.", ephemeral=True)

    db_set_cupid_bond(interaction.guild_id, player1.id, player2.id)

    rows = db_get_assignments(interaction.guild_id)
    state = cached_get_state(interaction.guild_id)

    # Targets are NOT notified — mod-log only
    await post_mod_log(interaction.guild,
        f"💘 **Cupid Bond** — {player1.display_name} ↔ {player2.display_name}\n"
        f"If either dies, the other will be automatically eliminated.")
    await interaction.response.send_message(
        f"✅ **{player1.display_name}** and **{player2.display_name}** are now bound.", ephemeral=True)


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
        # Only channel participants can add
        ch = interaction.guild.get_channel(self.channel_id)
        if not ch:
            return await interaction.response.send_message("❌ Channel not found.", ephemeral=True)
        # Check if user has access
        perms = ch.permissions_for(interaction.user)
        if not perms.view_channel:
            return await interaction.response.send_message("❌ You are not in this claim.", ephemeral=True)

        await interaction.response.send_modal(AddPlayerModal(self.guild_id, self.channel_id))

    async def on_remove(self, interaction: discord.Interaction):
        ch = interaction.guild.get_channel(self.channel_id)
        if not ch:
            return await interaction.response.send_message("❌ Channel not found.", ephemeral=True)
        perms = ch.permissions_for(interaction.user)
        if not perms.view_channel:
            return await interaction.response.send_message("❌ You are not in this claim.", ephemeral=True)

        await interaction.response.send_modal(RemovePlayerModal(self.guild_id, self.channel_id))


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

        # Cannot remove yourself or mods
        target_member = None
        for m in interaction.guild.members:
            if search in m.display_name.lower() and m.id != interaction.user.id:
                ch_perms = ch.permissions_for(m)
                if ch_perms.view_channel and not m.bot:
                    target_member = m
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


@tree.command(name="claim", description="Open a private claim channel with another player")
@app_commands.describe(player="The player you want to open a private claim with")
async def claim(interaction: discord.Interaction, player: discord.Member):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    if player.id == interaction.user.id:
        return await interaction.response.send_message(
            "❌ You cannot open a claim with yourself.", ephemeral=True)
    if player.bot:
        return await interaction.response.send_message(
            "❌ You cannot open a claim with a bot.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    state    = cached_get_state(interaction.guild_id)
    category = interaction.guild.get_channel(state.get("category_id") or 0)
    if not category:
        return await interaction.followup.send(
            "❌ Game category not found.", ephemeral=True)

    everyone  = interaction.guild.default_role
    bot_me    = interaction.guild.me
    mod_role  = interaction.guild.get_role(state.get("mod_role_id") or 0)
    spec_role = interaction.guild.get_role(state.get("spectator_role_id") or 0)
    font      = get_guild_font(interaction.guild_id)

    # Channel name from both player names
    n1 = interaction.user.display_name.lower().replace(" ", "-")[:15]
    n2 = player.display_name.lower().replace(" ", "-")[:15]
    ch_name = ch_name(f"claim-{n1}-{n2}", font, "🎭")

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

    ch = await category.create_text_channel(ch_name, overwrites=ch_ow)

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
    embed.set_footer(text=f"From the moderators • {datetime.now().strftime('%H:%M')}")
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

async def _apply_role_to_player(guild, player_id: int, old_role: str, new_role: str, state: dict):
    """Internal helper — updates DB, fixes wolf channel access, renames private channel, notifies player."""
    guild_id     = guild.id
    old_team     = get_team(guild_id, old_role)
    new_role_info= get_role_info(guild_id, new_role)
    new_team     = new_role_info.get("team", "village")

    # Update DB
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE player_assignments SET role_name=? WHERE guild_id=? AND player_id=?",
              (new_role, guild_id, player_id))
    conn.commit()
    conn.close()

    player       = guild.get_member(player_id)
    wolf_ch      = guild.get_channel(state.get("wolf_channel_id") or 0)
    wolf_vote_ch = guild.get_channel(state.get("wolf_vote_channel_id") or 0)

    # Fix wolf channel access
    if old_team != "wolf" and new_team == "wolf":
        if wolf_ch and player:
            await wolf_ch.set_permissions(player, view_channel=True, send_messages=True)
        if wolf_vote_ch and player:
            await wolf_vote_ch.set_permissions(player, view_channel=True, send_messages=True, read_messages=True, view_audit_log=False)
    elif old_team == "wolf" and new_team != "wolf":
        if wolf_ch and player:
            await wolf_ch.set_permissions(player, overwrite=None)
        if wolf_vote_ch and player:
            await wolf_vote_ch.set_permissions(player, overwrite=None)

    # Get channel id fresh from DB
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT channel_id FROM player_assignments WHERE guild_id=? AND player_id=?",
              (guild_id, player_id))
    row = c.fetchone()
    conn.close()
    ch_id = row[0] if row else None

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
                title="🔀 The roles have been scrambled!",
                description=f"Your new role is **{new_role}** {team_emoji}\n\n{role_desc}",
                color=0xFF4444 if new_team == "wolf" else (0xF1C40F if new_team == "neutral" else 0x44BB44)
            )
            embed.set_footer(text="All roles have been secretly reshuffled by the mod team.")
            await priv_ch.send(player.mention, embed=embed)


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

@tree.command(name="turn_player", description="Spin the wheel to assign a wolf role to a turned player")
@is_mod()
@app_commands.describe(player="The player who has been turned")
async def turn_player(interaction: discord.Interaction, player: discord.Member):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    rows = db_get_assignments(interaction.guild_id)
    assignment = next((r for r in rows if r[0] == player.id and r[2] == 1), None)
    if not assignment:
        return await interaction.followup.send(
            f"**{player.display_name}** is not an alive player.", ephemeral=True)

    # Spin from full wolf role pool
    all_roles  = cached_load_roles(interaction.guild_id)
    wolf_roles = [r for r in all_roles if r.get("team") == "wolf"]
    if not wolf_roles:
        return await interaction.followup.send(
            "❌ No wolf roles found in the role pool.", ephemeral=True)

    chosen     = random.choice(wolf_roles)
    new_role   = chosen["name"]
    role_info  = chosen
    old_role   = assignment[1]
    state      = cached_get_state(interaction.guild_id)

    # Update DB
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("UPDATE player_assignments SET role_name=? WHERE guild_id=? AND player_id=?",
              (new_role, interaction.guild_id, player.id))
    conn.commit()
    conn.close()

    # Give wolf-den access
    wolf_ch      = interaction.guild.get_channel(state.get("wolf_channel_id") or 0)
    wolf_vote_ch = interaction.guild.get_channel(state.get("wolf_vote_channel_id") or 0)
    if wolf_ch:
        await wolf_ch.set_permissions(player, view_channel=True, send_messages=True)
        await wolf_ch.send(fmt(f"🐺 {player.display_name} has joined the pack as **{new_role}**!"))
    if wolf_vote_ch:
        await wolf_vote_ch.set_permissions(player,
            view_channel=True, send_messages=True, read_messages=True)

    # Rename private channel
    if assignment[3]:
        priv_ch = interaction.guild.get_channel(assignment[3])
        if priv_ch:
            new_ch_name = f"🔒{player.display_name}-{new_role}".lower().replace(" ", "-")
            try: await priv_ch.edit(name=new_ch_name)
            except: pass
            # Send new role card
            font  = get_guild_font(interaction.guild_id)
            embed = build_role_card(player, new_role, role_info, font)
            await priv_ch.send(
                fmt(f"🔄 The wheel has spoken.\nYou have been turned — you are now a wolf.\nYour new role: **{new_role}**"),
                embed=embed)

    await refresh_player_list(interaction.guild)
    await post_mod_log(interaction.guild,
        f"🎡 **Turn Wheel Result**\n"
        f"**Player:** {player.display_name}\n"
        f"**Old role:** {old_role}\n"
        f"**New wolf role:** {new_role}\n"
        f"Den access granted. Private channel updated.")
    await interaction.followup.send(
        f"✅ **{player.display_name}** turned — wheel landed on **{new_role}**.\n"
        f"Den access granted.", ephemeral=True)

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
        pool_roles = cached_load_roles(interaction.guild_id)
        if not pool_roles:
            return await interaction.followup.send(
                "❌ No roles in the pool to randomly assign from.", ephemeral=True)
        chosen    = random.choice(pool_roles)
        new_role  = chosen["name"]
        revive_label = f"a random role (**{new_role}**)"

    elif return_as == "blank":
        new_role     = "Villager"   # Generic — no special ability
        revive_label = "as a blank **Villager** (no special role)"

    else:
        # Mod typed a specific role name — validate it loosely
        matched = next((r for r in cached_load_roles(interaction.guild_id)
                        if r["name"].lower() == return_as), None)
        if not matched:
            return await interaction.followup.send(
                f"❌ Role **{return_as}** not found in the role pool. "
                f"Check `/list_roles` or use `same`, `random`, or `blank`.", ephemeral=True)
        new_role     = matched["name"]
        revive_label = f"a new role (**{new_role}**)"

    role_info  = get_role_info(interaction.guild_id, new_role)
    new_team   = role_info.get("team", "village")
    old_team   = get_team(interaction.guild_id, old_role)

    # ── Restore alive status in DB ─────────────────────────────────────────
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
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
    wolf_vote_ch = interaction.guild.get_channel(state.get("wolf_vote_channel_id") or 0)

    if new_team == "wolf" and old_team != "wolf":
        # Revived into wolf team
        if wolf_ch:
            await wolf_ch.set_permissions(player, view_channel=True, send_messages=True)
            await wolf_ch.send(f"🐺 **{player.display_name}** has returned from the dead and joined the pack!")
        if wolf_vote_ch:
            await wolf_vote_ch.set_permissions(player, view_channel=True, send_messages=True, read_messages=True, view_audit_log=False)
    elif new_team != "wolf" and old_team == "wolf":
        # Was wolf, now revived as non-wolf — strip den write access (keep read-only as dead wolf)
        if wolf_ch:
            await wolf_ch.set_permissions(player, view_channel=False, send_messages=False)
        if wolf_vote_ch:
            await wolf_vote_ch.set_permissions(player, view_channel=False)
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
                state.get("ghost_channel_id"), state.get("wolf_vote_channel_id")}
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
        action_label = action_type.replace("_", " ").title()
        lines.append(f"**Night {night_num}** — {action_label} → {target}")

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

@tree.command(name="list_players", description="Show alive and dead players")
async def list_players(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)
    rows = db_get_assignments(interaction.guild_id)
    if not rows:
        return await interaction.followup.send("No active game.")
    await interaction.followup.send(embed=build_player_list_embed(interaction.guild, rows))

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

@tree.command(name="game_status", description="Show current game phase and player counts")
async def game_status(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)
    state = cached_get_state(interaction.guild_id)
    if not state or not state.get("category_id"):
        return await interaction.followup.send("No active game.")
    rows        = db_get_assignments(interaction.guild_id)
    alive_count = sum(1 for r in rows if r[2] == 1)
    dead_count  = sum(1 for r in rows if r[2] == 0)
    phase       = state.get("phase", "day").capitalize()
    embed = discord.Embed(title="🎮 Game Status", color=0x5865F2)
    embed.add_field(name="Phase",   value=f"{'🌙' if phase == 'Night' else '☀️'} {phase}", inline=True)
    embed.add_field(name="Phase #", value=str(db_get_night_num(interaction.guild_id)),      inline=True)
    embed.add_field(name="Players", value=f"✅ {alive_count} alive  💀 {dead_count} dead",  inline=False)
    await interaction.followup.send(embed=embed)

@tree.command(name="action", description="Send a private action or message to the mod team")
@app_commands.describe(message="Your action — only mods will see this")
async def action(interaction: discord.Interaction, message: str):
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
        clear_btn     = Button(label="🗑️ Clear All (Mod)",       style=discord.ButtonStyle.danger)

        cast_btn.callback      = self.cast_vote
        abstain_btn.callback   = self.abstain
        remove_btn.callback    = self.remove_vote
        breakdown_btn.callback = self.full_breakdown
        clear_btn.callback     = self.clear_votes

        self.add_item(cast_btn)
        self.add_item(abstain_btn)
        self.add_item(remove_btn)
        self.add_item(breakdown_btn)
        self.add_item(clear_btn)

    def _get_player_status(self, interaction):
        """Returns (is_alive, is_in_game, rows)."""
        rows       = db_get_assignments(interaction.guild_id)
        row        = next((r for r in rows if r[0] == interaction.user.id), None)
        in_game    = row is not None
        is_alive   = row is not None and row[2] == 1
        return is_alive, in_game, rows

    async def cast_vote(self, interaction: discord.Interaction):
        is_alive, in_game, rows = self._get_player_status(interaction)
        if not in_game:
            return await interaction.response.send_message("❌ You are not in the current game.", ephemeral=True)
        if not is_alive:
            return await interaction.response.send_message("❌ Eliminated players cannot vote.", ephemeral=True)
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
        view = VoteTargetView(self.guild_id, options, rows)
        await interaction.response.send_message("Choose who to vote for:", view=view, ephemeral=True)

    async def abstain(self, interaction: discord.Interaction):
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
        for voter_id, target_id in votes:
            voter  = pid_to_name.get(voter_id, str(voter_id))
            target = "Abstain" if target_id is None else pid_to_name.get(target_id, str(target_id))
            lines.append(f"{voter} → {target}")
        embed = discord.Embed(title="👁️ Full Vote Breakdown", description="\n".join(lines), color=0xFF4444)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def clear_votes(self, interaction: discord.Interaction):
        state    = cached_get_state(interaction.guild_id)
        mod_role = interaction.guild.get_role(state.get("mod_role_id") or 0)
        if not interaction.user.guild_permissions.administrator and not (mod_role and mod_role in interaction.user.roles):
            return await interaction.response.send_message("❌ Only mods can clear votes.", ephemeral=True)
        db_clear_day_votes(interaction.guild_id)
        await refresh_day_vote(interaction.guild)
        await interaction.response.send_message("✅ All day votes cleared.", ephemeral=True)


class VoteTargetView(View):
    def __init__(self, guild_id, options, rows):
        super().__init__(timeout=60)
        self.guild_id = guild_id
        sel = Select(placeholder="Choose your vote target...", options=options[:25])
        sel.callback = self.on_select
        self.add_item(sel)

    async def on_select(self, interaction: discord.Interaction):
        target_id = int(interaction.data["values"][0])
        day_num   = db_get_night_num(interaction.guild_id)
        db_set_day_vote(interaction.guild_id, interaction.user.id, target_id, day_num)
        await refresh_day_vote(interaction.guild)
        target = interaction.guild.get_member(target_id)
        await interaction.response.edit_message(
            content=fmt(f"✅ Voted for {target.display_name if target else target_id}."), view=None)
        self.stop()



@tree.command(name="vote_status", description="Show who has and hasn't voted in the current day vote")
@is_mod()
async def vote_status(interaction: discord.Interaction):
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

@tree.command(name="start_day_vote", description="Start the day vote with an optional timer")
@is_mod()
@app_commands.describe(
    duration_minutes="Auto-close vote after this many minutes (optional)",
    anonymous="Hide voter names until vote closes (default False)")
async def start_day_vote(interaction: discord.Interaction, duration_minutes: int = 0, anonymous: bool = False):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
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

    db_clear_day_votes(interaction.guild_id)
    db_set_state(interaction.guild_id, anon_vote=1 if anonymous else 0)
    end_time = None
    if duration_minutes > 0:
        import time
        end_time = int(time.time()) + duration_minutes * 60
        db_set_state(interaction.guild_id, day_vote_end_time=end_time)
    else:
        db_set_state(interaction.guild_id, day_vote_end_time=None)

    await refresh_day_vote(interaction.guild)
    await log_event(interaction.guild, "Day", f"🗳️ Day vote opened" +
                    (f" ({duration_minutes} min timer)" if duration_minutes else ""))
    await set_bot_status(f"☀️ Day vote open")
    await interaction.followup.send(
        f"✅ Day vote started." + (f" Auto-closes in {duration_minutes} min." if duration_minutes else ""),
        ephemeral=True)

    # Trigger NPC day votes after a natural delay
    async def _run_npc_day_votes():
        await asyncio.sleep(random.uniform(20, 60))
        for npc in db_get_npcs(interaction.guild_id):
            if npc["is_alive"]:
                await _npc_day_vote(interaction.guild, interaction.guild_id, npc)
                await asyncio.sleep(random.uniform(5, 20))
    asyncio.create_task(_run_npc_day_votes())

    if duration_minutes > 0:
        async def auto_close():
            await asyncio.sleep(duration_minutes * 60)
            current = cached_get_state(interaction.guild_id)
            if current and current.get("day_vote_end_time") == end_time:
                # Post permanent snapshot before clearing
                snap_state = cached_get_state(interaction.guild_id)
                snap_ch    = interaction.guild.get_channel(snap_state.get("day_vote_ch_id") or 0)
                snap_votes = db_get_day_votes(interaction.guild_id)
                snap_rows  = db_get_assignments(interaction.guild_id)
                if snap_ch and snap_votes:
                    night_num = db_get_night_num(interaction.guild_id)
                    snap_embed = build_day_vote_embed(interaction.guild, snap_votes, snap_rows)
                    snap_embed.title = f"📋 Day {night_num} Vote — Final Results"
                    snap_embed.color = 0x95A5A6
                    snap_embed.set_footer(text="Vote timer ended. Results are permanent.")
                    await snap_ch.send(embed=snap_embed)
                # If anonymous — post full reveal now
                cur_state = cached_get_state(interaction.guild_id)
                if cur_state.get("anon_vote"):
                    reveal_votes = db_get_day_votes(interaction.guild_id)
                    reveal_rows  = db_get_assignments(interaction.guild_id)
                    reveal_embed = build_day_vote_embed(interaction.guild, reveal_votes, reveal_rows, anonymous=False)
                    reveal_embed.title = "🔓 Anonymous Vote — Full Reveal"
                    reveal_embed.color = 0xF39C12
                    if snap_ch:
                        await snap_ch.send("🔓 **Anonymous vote closed — revealing all votes now:**", embed=reveal_embed)
                    await post_mod_log(interaction.guild, embed=reveal_embed)
                db_set_state(interaction.guild_id, day_vote_end_time=None, anon_vote=0)
                await refresh_day_vote(interaction.guild)
                # Post prompt to mod-log with standings and next-phase button
                vote_summary  = _build_vote_summary(interaction.guild, interaction.guild_id)
                state_cur     = cached_get_state(interaction.guild_id)
                mod_ch_prompt = interaction.guild.get_channel(state_cur.get("mod_log_channel_id") or 0)
                if mod_ch_prompt:
                    embed_prompt = discord.Embed(
                        title       = "⏰ Day Vote Closed — Results",
                        description = vote_summary,
                        color       = 0xF39C12
                    )
                    embed_prompt.set_footer(text="Eliminate players manually with /eliminate, then start the next night.")
                    view_prompt = DayVoteClosedPromptView(interaction.guild_id, vote_summary)
                    await mod_ch_prompt.send(embed=embed_prompt, view=view_prompt)
                await log_event(interaction.guild, "Day", "⏰ Day vote timer ended")
        t = asyncio.create_task(auto_close())
        day_vote_timers[interaction.guild_id] = t

        # Warning messages at 15 and 5 minutes before close
        # + mod-log reminder at 30 minutes showing who hasn't voted
        async def vote_warnings():
            vc_ch  = interaction.guild.get_channel(
                cached_get_state(interaction.guild_id).get("village_chat_ch_id") or VILLAGE_CHAT_ID)

            # 30-minute mod-log reminder — who hasn't voted yet
            if duration_minutes > 30:
                await asyncio.sleep((duration_minutes - 30) * 60)
                cur = cached_get_state(interaction.guild_id)
                if cur and cur.get("day_vote_end_time") == end_time:
                    rows_r  = db_get_assignments(interaction.guild_id)
                    votes_r = db_get_day_votes(interaction.guild_id)
                    npcs_r  = db_get_npcs(interaction.guild_id)
                    voted_r = {v[0] for v in votes_r}
                    not_yet = []
                    for pid, _, is_alive, _ in rows_r:
                        if is_alive and pid not in voted_r:
                            m = interaction.guild.get_member(pid)
                            npc_m = next((n for n in npcs_r if n["npc_id"] == pid), None)
                            name  = npc_m["name"] if npc_m else (m.display_name if m else str(pid))
                            not_yet.append(name)
                    if not_yet:
                        await post_mod_log(interaction.guild,
                            f"⏰ **Day vote — 30 min remaining**\n"
                            f"**{len(not_yet)} player(s) haven't voted yet:**\n"
                            + "\n".join(f"• {n}" for n in not_yet))

            if duration_minutes > 15:
                await asyncio.sleep((duration_minutes - 15) * 60)
                cur = cached_get_state(interaction.guild_id)
                if cur and cur.get("day_vote_end_time") == end_time and vc_ch:
                    await vc_ch.send("⏰ **15 minutes remaining** on the day vote!")
            if duration_minutes > 5:
                remaining = min(duration_minutes - 5, 10) * 60
                await asyncio.sleep(remaining)
                cur = cached_get_state(interaction.guild_id)
                if cur and cur.get("day_vote_end_time") == end_time and vc_ch:
                    await vc_ch.send("⚠️ **5 minutes remaining** on the day vote — last chance to vote!")
        asyncio.create_task(vote_warnings())


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
        target_id = int(raw)
        # Double-check target is still alive in DB
        target_row = next((r for r in rows if r[0] == target_id and r[2] == 1), None)
        if not target_row:
            return await interaction.response.send_message(
                "❌ That player is no longer alive. Please choose again.", ephemeral=True)
        db_set_wolf_vote(interaction.guild_id, self.night_num, interaction.user.id, target_id)
        target = interaction.guild.get_member(target_id)
        tname  = target.display_name if target else str(target_id)
        # Notify mod-log
        voter  = interaction.guild.get_member(interaction.user.id)
        wolf_votes = db_get_wolf_votes(interaction.guild_id, self.night_num)
        await post_mod_log(interaction.guild,
            f"🐺 **Wolf Vote** — Night {self.night_num}\n"
            f"**{voter.display_name if voter else interaction.user.id}** voted to kill **{tname}**\n"
            f"*Pack vote tally: {len(wolf_votes)} vote(s) cast so far.*")
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
        wolf_vote_ch = guild.get_channel(state.get("wolf_vote_channel_id") or 0)
        if wolf_ch:
            await wolf_ch.set_permissions(player, view_channel=True, send_messages=True)
            await wolf_ch.send(fmt(f"🔄 {player.display_name} (Cursed) has joined the pack!"))
        if wolf_vote_ch:
            await wolf_vote_ch.set_permissions(player,
                view_channel=True, send_messages=True, read_messages=True)
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
                asyncio.create_task(
                    _npc_survival_reaction(guild, guild.id, top_id))

    try:
        await interaction.followup.send(
            f"✅ **{player.display_name}** eliminated (role: {role_name}).", ephemeral=True)
    except Exception:
        pass

    winner = await check_win_condition(guild)
    if winner:
        await announce_win(guild, winner)


@tree.command(name="eliminate", description="Eliminate a player from the game")
@is_mod()
@app_commands.describe(player="Player to eliminate", public="Announce publicly? (default True)")
async def eliminate(interaction: discord.Interaction, player: discord.Member, public: bool = True):
    await interaction.response.defer(ephemeral=True)

    rows = db_get_assignments(interaction.guild_id)
    assignment = next((r for r in rows if r[0] == player.id and r[2] == 1), None)
    if not assignment:
        return await interaction.followup.send(
            f"**{player.display_name}** is not an alive player.", ephemeral=True)
    role_name  = assignment[1]
    state      = cached_get_state(interaction.guild_id)
    team       = get_team(interaction.guild_id, role_name)
    team_emoji = "🐺" if team == "wolf" else ("⚖️" if team == "neutral" else "🏘️")
    color      = 0xC0392B if team == "wolf" else (0xF39C12 if team == "neutral" else 0x27AE60)

    embed = discord.Embed(
        title       = "⚠️ Confirm Elimination",
        description = f"You are about to eliminate **{player.display_name}**.",
        color       = color
    )
    embed.add_field(name="Role",   value=f"{team_emoji} {role_name}", inline=True)
    embed.add_field(name="Team",   value=team.capitalize(),           inline=True)
    embed.add_field(name="Status", value="✅ Alive",                   inline=True)
    embed.set_footer(text="This action cannot be undone. Confirm within 30 seconds.")

    view = EliminateConfirmView(
        interaction.guild_id, player, role_name, public, assignment, state)
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)

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

    # Lock private channel — no message sent to player
    priv_ch = guild.get_channel(assignment[3] or 0)
    if priv_ch:
        await priv_ch.set_permissions(player, view_channel=True, send_messages=False)

    # Grant ghost chat access — no announcement
    ghost_ch = guild.get_channel(state.get("ghost_channel_id") or 0)
    if ghost_ch:
        await ghost_ch.set_permissions(player, view_channel=True, send_messages=True)

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
                # Notify partner in their private channel
                # Partner is NOT notified why they die — mod-log only
                # If the partner is a Shadow Wolf, their kill list uses the voters
                # who voted out the original player (player_id), not themselves
                if partner_row[1] == "Shadow Wolf":
                    asyncio.create_task(
                        _generate_sw_kill_list(guild, guild.id, partner_id, player_id))
                await _eliminate_player(guild, partner_id, "died of heartbreak (Cupid bond)")

    # ── Shadow Wolf kill list trigger ────────────────────────────────────
    if assignment[1] == "Shadow Wolf":
        asyncio.create_task(_generate_sw_kill_list(guild, guild.id, player_id, player_id))
    # If this player is on the SW kill list, mark them dead
    asyncio.create_task(_sw_mark_dead(guild, guild.id, player_id, "other"))

    # ── Cupid bond — if Shadow Wolf died from bond, use partner's voters ──
    # (handled below in Cupid check — we pass voted_out_id = partner_id)

    # ── Time Lord trigger ────────────────────────────────────────────────
    if assignment[1] == "Time Lord":
        state_cur   = cached_get_state(guild.id)
        night_dur   = state_cur.get("night_duration", 36000)
        day_dur     = state_cur.get("day_duration", 50400)
        new_night   = max(3600,  night_dur // 2)
        new_day     = max(7200,  day_dur   // 2)
        db_set_state(guild.id, night_duration=new_night, day_duration=new_day)
        msg = (f"⏰ **Time Lord eliminated — clocks accelerated!**\n"
               f"Night duration: {night_dur//3600:.1f}h → {new_night//3600:.1f}h\n"
               f"Day duration:   {day_dur//3600:.1f}h → {new_day//3600:.1f}h\n"
               f"All future phases will be shorter.")
        await post_mod_log(guild, msg)
        # Post in village-chat too
        state_vc = cached_get_state(guild.id)
        vc_ch    = guild.get_channel(state_vc.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
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
                _invalidate_cache(guild.id)

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
                wolf_vote_ch = guild.get_channel(state_check.get("wolf_vote_channel_id") or 0)
                if wolf_ch and traitor_member:
                    await wolf_ch.set_permissions(traitor_member,
                        view_channel=True, send_messages=True)
                if wolf_vote_ch and traitor_member:
                    await wolf_vote_ch.set_permissions(traitor_member,
                        view_channel=True, send_messages=True, read_messages=True, view_audit_log=False)

                await post_mod_log(guild,
                    f"🔄 **Traitor side-switch triggered**\n"
                    f"**Player:** {traitor_member.display_name if traitor_member else traitor_row[0]}\n"
                    f"**New wolf role:** {outcome}\n"
                    f"**Description:** {outcome_desc}\n"
                    f"Role updated in DB. Den access granted.")

    # ── Pothead second kill trigger ──────────────────────────────────────
    if assignment[1] == "Pothead" and reason == "Wolf attack":
        state_pot  = cached_get_state(guild.id)
        wolf_vote_ch = guild.get_channel(state_pot.get("wolf_vote_channel_id") or 0)
        rows_pot   = db_get_assignments(guild.id)
        night_pot  = db_get_night_num(guild.id)
        alive_non_wolf = [guild.get_member(r[0]) for r in rows_pot
                          if r[2] == 1 and get_team(guild.id, r[1]) != "wolf"
                          and r[0] != player_id]
        alive_non_wolf = [p for p in alive_non_wolf if p]
        if wolf_vote_ch and alive_non_wolf:
            view = PothreadSecondKillView(guild.id, night_pot, alive_non_wolf)
            await wolf_vote_ch.send(
                fmt(f"🍕 The wolves ate the Pothead — they get the munchies!\n"
                    f"Pick a second kill target for tonight."),
                view=view)
        await post_mod_log(guild,
            f"🍕 **Pothead eliminated by wolves** — Night {db_get_night_num(guild.id)}\n"
            f"Wolves get a second kill tonight. Dropdown posted in wolf-vote.")

    # ── Diseased wolf poisoning check ─────────────────────────────────────
    if assignment[1] == "Diseased" and reason == "Wolf attack":
        await post_mod_log(guild,
            f"🤢 **Diseased player eliminated by wolves!**\n"
            f"Wolves are now **poisoned** — they cannot make a kill next night.\n"
            f"⚠️ Reminder: block the wolf vote next night manually.")

    # Handle NPC elimination
    npcs    = db_get_npcs(guild.id)
    npc_row = next((n for n in npcs if n["npc_id"] == player_id), None)
    if npc_row:
        db_set_npc_alive(guild.id, player_id, False)
        # NPC sends a goodbye message in village-chat
        vc_ch = guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
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
        asyncio.create_task(_npc_react_to_death(guild, guild.id, npc_row["name"]))
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
    # NPCs react to this player's death and pin it to their memory
    asyncio.create_task(_npc_react_to_death(guild, guild.id, player.display_name))
    night_num_pin = db_get_night_num(guild.id)
    phase_pin     = state.get("phase", "day").capitalize()
    for npc_p in db_get_npcs(guild.id):
        if npc_p["is_alive"]:
            db_pin_npc_event(guild.id, npc_p["npc_id"],
                f"{phase_pin} {night_num_pin}: {player.display_name} was eliminated ({reason}).")

# ====================== WIN CONDITION ======================
async def check_win_condition(guild):
    rows = db_get_assignments(guild.id)
    alive_roles   = [r[1] for r in rows if r[2] == 1]
    # Neutral players are excluded from the win ratio calculation
    alive_wolves  = [r for r in alive_roles if get_team(guild.id, r) == "wolf"]
    alive_village = [r for r in alive_roles if get_team(guild.id, r) == "village"]
    if len(alive_wolves) == 0:                  return "village"
    if len(alive_wolves) >= len(alive_village): return "wolf"
    return None

async def announce_win(guild, winner: str):
    state = cached_get_state(guild.id)
    cat   = guild.get_channel(state.get("category_id") or 0)
    skip  = {state.get("mod_log_channel_id"), state.get("wolf_channel_id"),
             state.get("ghost_channel_id"), state.get("wolf_vote_channel_id")}

    # Build win messages
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

    # Post public announcement in first visible channel
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
    asyncio.create_task(_npc_endgame_reaction(guild, guild.id, winner))

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

    all_pids      = [r[0] for r in rows]
    winner_pids   = [r[0] for r in rows if get_team(interaction.guild_id, r[1]) == winning_team]
    dead_pids     = [r[0] for r in rows if r[2] == 0]

    db_update_stats(interaction.guild_id, all_pids, winner_pids, dead_pids)

    # Record role history for every player
    for pid, role_name, is_alive, _ in rows:
        team    = get_team(interaction.guild_id, role_name)
        outcome = "win" if pid in winner_pids else "loss"
        db_record_role_history(interaction.guild_id, pid, role_name, team, outcome)

    winner_names = []
    for pid in winner_pids:
        m = interaction.guild.get_member(pid)
        winner_names.append(m.display_name if m else str(pid))

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
        stat_rows = db_get_stats(interaction.guild_id)
        await stats_ch.send(embed=build_stats_embed(interaction.guild, stat_rows))
    await interaction.followup.send(
        f"✅ Stats recorded. **{winning_team.capitalize()}** team wins credited to {len(winner_pids)} players.",
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
    c = conn.cursor()
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

# ====================== NIGHT PHASE ======================
night_timers = {}

# ── Night action mod-log helper ───────────────────────────────────────────
async def _mod_log_action(guild, night_num, actor, target, role_name, note=""):
    """Post a standardised night action notification to mod-log."""
    role_icons = {
        "Seer": "🔮", "Doctor": "💊", "Surgeon": "🏥", "Bodyguard": "🛡️",
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

    async def _save_and_close(self, interaction, action_key, target_id, confirm_text):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, action_key, target_id)
        await interaction.response.edit_message(content=fmt(f"✅ {confirm_text}"), view=None)
        actor  = interaction.guild.get_member(self.actor_id)
        target = interaction.guild.get_member(target_id) if target_id else None
        await _mod_log_action(interaction.guild, night_num, actor, target, self.role_name)


# ── Seer ──────────────────────────────────────────────────────────────────
class SeerView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel = Select(placeholder="🔮 Choose a player to investigate", options=self._player_options())
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

        # Send result to Seer private channel
        actor_row = next((r for r in rows if r[0] == self.actor_id), None)
        if actor_row and actor_row[3]:
            priv_ch = guild.get_channel(actor_row[3])
            if priv_ch:
                embed = discord.Embed(
                    title       = f"🔮 Seer Result — Night {night_num}",
                    description = f"Is **{target_name}** a wolf?  **{result_label}**",
                    color       = result_color
                )
                embed.set_footer(text="Do not share this directly — doing so results in death.")
                await priv_ch.send(embed=embed)

        # Post to mod-log
        actor = guild.get_member(self.actor_id)
        await post_mod_log(guild,
            f"🔮 **Seer Investigation** — Night {night_num}\n"
            f"**Seer:** {actor.display_name if actor else self.actor_id}\n"
            f"**Target:** {target_name} ({target_role})\n"
            f"**Result sent:** {result_label}\n"
            + (f"*{result_note}*" if result_note else ""))

        # Track Seer accuracy
        if result_team == "wolf" and team == "wolf":
            db_update_seer_correct(guild_id, self.actor_id)

        await interaction.response.edit_message(
            content=fmt(f"✅ Investigation submitted on {target_name}.\nResult has been sent to your channel."),
            view=None)


# ── Doctor ────────────────────────────────────────────────────────────────
class DoctorView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        save_btn = Button(label="💊 Use Save", style=discord.ButtonStyle.green)
        skip_btn = Button(label="Skip Tonight", style=discord.ButtonStyle.secondary)
        save_btn.callback = self.on_save
        skip_btn.callback = self.on_skip
        self.add_item(save_btn)
        self.add_item(skip_btn)

    async def on_save(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "doctor_save", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"💊 **Doctor** — Night {night_num}\n**{actor.display_name}** chose to **SAVE** tonight.")
        await interaction.response.edit_message(content=fmt("✅ You chose to save tonight."), view=None)

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "doctor_skip", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"💊 **Doctor** — Night {night_num}\n**{actor.display_name}** chose to **SKIP** tonight.")
        await interaction.response.edit_message(content=fmt("✅ You chose not to save tonight."), view=None)


# ── Surgeon ───────────────────────────────────────────────────────────────
class SurgeonView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        save_btn = Button(label="🏥 Use Save", style=discord.ButtonStyle.green)
        skip_btn = Button(label="Skip Tonight", style=discord.ButtonStyle.secondary)
        save_btn.callback = self.on_save
        skip_btn.callback = self.on_skip
        self.add_item(save_btn)
        self.add_item(skip_btn)

    async def on_save(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "surgeon_save", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"🏥 **Surgeon** — Night {night_num}\n**{actor.display_name}** chose to **SAVE** tonight.")
        await interaction.response.edit_message(content=fmt("✅ Save submitted."), view=None)

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "surgeon_skip", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"🏥 **Surgeon** — Night {night_num}\n**{actor.display_name}** chose to **SKIP** tonight.")
        await interaction.response.edit_message(content="✅ You chose not to save tonight.", view=None)


# ── Bodyguard ─────────────────────────────────────────────────────────────
class BodyguardView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel = Select(placeholder="🛡️ Choose a player to guard", options=self._player_options())
        sel.callback = self.on_select
        self.add_item(sel)

    async def on_select(self, interaction):
        target_id = int(interaction.data["values"][0])
        await self._save_and_close(interaction, "bodyguard", target_id,
            f"Guarding {interaction.guild.get_member(target_id).display_name} tonight.")


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
        await post_mod_log(interaction.guild,
            f"🧙 **Witch** — Night {night_num}\n"
            f"**{actor.display_name}** used **SAVE potion** on **{target.display_name if target else target_id}**")
        db_record_witch_night(interaction.guild_id, db_get_night_num(interaction.guild_id))
        await interaction.response.edit_message(
            content=fmt(f"✅ Save potion used on {target.display_name if target else target_id}."), view=None)

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
        await post_mod_log(interaction.guild,
            f"🧙 **Witch** — Night {night_num}\n"
            f"**{actor.display_name}** used **POISON potion** on **{target.display_name if target else target_id}**")
        db_record_witch_night(interaction.guild_id, db_get_night_num(interaction.guild_id))
        await interaction.response.edit_message(
            content=fmt(f"✅ Poison potion used on {target.display_name if target else target_id}."), view=None)

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "witch_skip", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"🧙 **Witch** — Night {night_num}\n**{actor.display_name}** chose to **SKIP** tonight.")
        await interaction.response.edit_message(content=fmt("✅ You chose to skip this night."), view=None)


# ── Sheriff ───────────────────────────────────────────────────────────────
class SheriffView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        note_btn = Button(label="🔫 Acknowledge Sheriff Role", style=discord.ButtonStyle.blurple)
        note_btn.callback = self.on_ack
        self.add_item(note_btn)

    async def on_ack(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "sheriff_ack", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"🔫 **Sheriff** — Night {night_num}\n**{actor.display_name}** has acknowledged their role. "
            f"They will kill any wolf that targets them.")
        await interaction.response.edit_message(
            content=fmt("✅ Acknowledged. If wolves target you tonight, one will die in your place."), view=None)


# ── Huntsman ──────────────────────────────────────────────────────────────
class HuntsmanView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel  = Select(placeholder="🏹 Choose a player to protect", options=self._player_options())
        skip = Button(label="Don't Protect Anyone", style=discord.ButtonStyle.secondary)
        sel.callback  = self.on_select
        skip.callback = self.on_skip
        self.add_item(sel)
        self.add_item(skip)

    async def on_select(self, interaction):
        target_id = int(interaction.data["values"][0])
        await self._save_and_close(interaction, "huntsman", target_id,
            f"Protecting {interaction.guild.get_member(target_id).display_name} tonight.")

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "huntsman_skip", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"🏹 **Huntsman** — Night {night_num}\n**{actor.display_name}** chose not to protect anyone.")
        await interaction.response.edit_message(content=fmt("✅ You chose not to protect anyone tonight."), view=None)


# ── Insomniac ─────────────────────────────────────────────────────────────
class InsomniacView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        ack = Button(label="😴 Acknowledge (Listening Tonight)", style=discord.ButtonStyle.blurple)
        ack.callback = self.on_ack
        self.add_item(ack)

    async def on_ack(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "insomniac_ack", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"😴 **Insomniac** — Night {night_num}\n**{actor.display_name}** is listening. "
            f"Mod: remember to send them a wolf role hint every other night starting Night 3.")
        await interaction.response.edit_message(
            content=fmt("✅ You are listening tonight. The mod will send you info every other night starting Night 3."),
            view=None)


# ── Medium ────────────────────────────────────────────────────────────────
class MediumView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel = Select(placeholder="🌀 Choose a player to check alignment", options=self._player_options())
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
        APPEARS_GOOD = {"Elite Alpha", "Blessed Wolf", "Werekitten", "Cursed"}
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

        # Send result to Medium's private channel only
        actor_row = next((r for r in rows if r[0] == self.actor_id), None)
        if actor_row and actor_row[3]:
            priv_ch = guild.get_channel(actor_row[3])
            if priv_ch:
                await priv_ch.send(
                    fmt(f"🌀 **Medium Result — Night {night_num}**\n"
                        f"**{target_name}** — {alignment}"))

        # Mod-log with full detail
        actor = guild.get_member(self.actor_id)
        await post_mod_log(guild,
            f"🌀 **Medium Check** — Night {night_num}\n"
            f"**Medium:** {actor.display_name if actor else self.actor_id}\n"
            f"**Target:** {target_name} ({target_role})\n"
            f"**Result sent:** {alignment}")

        await interaction.response.edit_message(
            content=fmt(f"✅ Alignment check submitted on {target_name}.\nResult sent to your channel."),
            view=None)


# ── Gravedigger ───────────────────────────────────────────────────────────
class GravediggerView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        ack = Button(label="⚰️ Acknowledge (Digging Tonight)", style=discord.ButtonStyle.blurple)
        ack.callback = self.on_ack
        self.add_item(ack)

    async def on_ack(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "gravedigger_ack", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"⚰️ **Gravedigger** — Night {night_num}\n**{actor.display_name}** is ready. "
            f"Mod: send them death details in the morning.")
        await interaction.response.edit_message(
            content=fmt("✅ Acknowledged. The mod will send you death details each morning."), view=None)


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
            f"You will hide {interaction.guild.get_member(target_id).display_name} if they top the vote.")

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "hermit_skip", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"🏚️ **Hermit** — Night {night_num}\n**{actor.display_name}** chose not to hide anyone.")
        await interaction.response.edit_message(content=fmt("✅ You chose not to hide anyone this round."), view=None)


# ── Agitator ──────────────────────────────────────────────────────────────
class AgitatorView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        use_btn  = Button(label="📢 Use Frenzy Ability", style=discord.ButtonStyle.danger)
        skip_btn = Button(label="Save It For Later",     style=discord.ButtonStyle.secondary)
        use_btn.callback  = self.on_use
        skip_btn.callback = self.on_skip
        self.add_item(use_btn)
        self.add_item(skip_btn)

    async def on_use(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "agitator_frenzy", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"📢 **Agitator** — Night {night_num}\n"
            f"**{actor.display_name}** has used their **FRENZY** ability! "
            f"The village will require TWO lynches tomorrow. Post announcement at morning blood board.")
        await interaction.response.edit_message(
            content=fmt("✅ Frenzy ability used! The village will be notified tomorrow morning that two votes are required."),
            view=None)

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "agitator_skip", None)
        await interaction.response.edit_message(content=fmt("✅ Ability saved for another night."), view=None)


# ── Governor ──────────────────────────────────────────────────────────────
class GovernorView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel  = Select(placeholder="🎖️ Name the player you wish to pardon", options=self._player_options())
        skip = Button(label="Don't Use Pardon",  style=discord.ButtonStyle.secondary)
        sel.callback  = self.on_select
        skip.callback = self.on_skip
        self.add_item(sel)
        self.add_item(skip)

    async def on_select(self, interaction):
        target_id = int(interaction.data["values"][0])
        target = interaction.guild.get_member(target_id)
        actor  = interaction.guild.get_member(self.actor_id)
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "governor_pardon", target_id)
        await post_mod_log(interaction.guild,
            f"🎖️ **Governor** — Night {night_num}\n"
            f"**{actor.display_name}** intends to pardon **{target.display_name if target else target_id}** "
            f"from tomorrow's vote. Mod: they must contact you 15 min before hanging to confirm.")
        await interaction.response.edit_message(
            content=fmt(f"✅ Pardon intent recorded for {target.display_name if target else target_id}.\nRemember — contact the mod 15 minutes before the hanging to confirm."), view=None)

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "governor_skip", None)
        await interaction.response.edit_message(content=fmt("✅ No pardon this round."), view=None)


# ── Clone ─────────────────────────────────────────────────────────────────
class CloneView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel = Select(placeholder="🪞 Choose the player to clone", options=self._player_options())
        sel.callback = self.on_select
        self.add_item(sel)

    async def on_select(self, interaction):
        target_id = int(interaction.data["values"][0])
        await self._save_and_close(interaction, "clone", target_id,
            f"You are now cloning {interaction.guild.get_member(target_id).display_name}. If they die, you inherit their role.")


# ── Shapeshifter ──────────────────────────────────────────────────────────
class ShapeshifterView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel = Select(placeholder="🎭 Choose a player to shapeshift into", options=self._player_options())
        sel.callback = self.on_select
        self.add_item(sel)

    async def on_select(self, interaction):
        target_id = int(interaction.data["values"][0])
        await self._save_and_close(interaction, "shapeshifter", target_id,
            f"You are taking the form of {interaction.guild.get_member(target_id).display_name}. Mod will reassign your role.")



# ── Cupid ─────────────────────────────────────────────────────────────────
class CupidView(BaseNightView):
    """Cupid selects two players to bind each night. Bond expires at morning."""
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        # Two dropdowns — one for each player
        opts = self._player_options()
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
        await post_mod_log(interaction.guild,
            f"💘 **Cupid Bond — Night {night_num}**\n"
            f"**Bound:** {n1} ↔ {n2}\n"
            f"Bond expires at morning resolution.")
        await interaction.response.edit_message(
            content=fmt(f"💘 Bound {n1} and {n2} tonight.\nIf either dies, the other follows.\nBond expires at dawn."),
            view=None)

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "cupid_skip", None)
        db_clear_cupid_current(interaction.guild_id)
        await post_mod_log(interaction.guild,
            f"💘 **Cupid** — Night {night_num}\n"
            f"Chose not to bind anyone. Previous bond cleared.")
        await interaction.response.edit_message(
            content=fmt("💘 No bind tonight. Any previous bond has been cleared."),
            view=None)

# ── Wolf Pup ──────────────────────────────────────────────────────────────
class WolfPupView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel = Select(placeholder="🐾 Choose a player to block", options=self._player_options())
        sel.callback = self.on_select
        self.add_item(sel)

    async def on_select(self, interaction):
        target_id = int(interaction.data["values"][0])
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "wolf_pup", target_id)
        db_record_block(interaction.guild_id, night_num, self.actor_id, target_id)
        target = interaction.guild.get_member(target_id)
        actor  = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"🐾 **Wolf Pup Block** — Night {night_num}\n"
            f"**{actor.display_name}** is blocking **{target.display_name if target else target_id}** "
            f"— their ability is suppressed this night.")
        await interaction.response.edit_message(
            content=fmt(f"✅ Blocking {target.display_name if target else target_id} tonight."), view=None)


# ── Alpha ─────────────────────────────────────────────────────────────────
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
        target_id = int(interaction.data["values"][0])
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "alpha", target_id)
        db_record_turn(interaction.guild_id, night_num, self.actor_id, target_id, "pending")
        target = interaction.guild.get_member(target_id)
        actor  = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"👑 **Alpha Turn Attempt** — Night {night_num}\n"
            f"**{actor.display_name}** is attempting to turn **{target.display_name if target else target_id}**\n"
            f"*Use `/log_turn_result` to record the outcome.*")
        await interaction.response.edit_message(
            content=fmt(f"✅ Turn attempt submitted on {target.display_name if target else target_id}."), view=None)

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
        target_id = int(interaction.data["values"][0])
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "elite_alpha", target_id)
        db_record_turn(interaction.guild_id, night_num, self.actor_id, target_id, "pending")
        target = interaction.guild.get_member(target_id)
        actor  = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"👑⭐ **Elite Alpha Turn Attempt** — Night {night_num}\n"
            f"**{actor.display_name}** is attempting to turn **{target.display_name if target else target_id}**\n"
            f"*Use `/log_turn_result` to record the outcome.*")
        await interaction.response.edit_message(
            content=fmt(f"✅ Turn attempt submitted on {target.display_name if target else target_id}."), view=None)

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

        # ── Post result to wolf-den immediately ───────────────────────────
        state   = cached_get_state(guild_id)
        wolf_ch = guild.get_channel(state.get("wolf_channel_id") or 0)
        if wolf_ch:
            if is_self:
                den_msg = (f"🦴 **Bloodhound Report** — Night {night_num}\n"
                           f"Scanned: **themselves**\n"
                           f"Exact role: **{target_role}**")
            else:
                den_msg = (f"🦴 **Bloodhound Report** — Night {night_num}\n"
                           f"Scanned: **{target_name}**\n"
                           f"Exact role: **{target_role}**")
            await wolf_ch.send(den_msg)

        # ── Notify Bloodhound in their private channel immediately ────────
        actor_row = next((r for r in rows if r[0] == self.actor_id), None)
        if actor_row and actor_row[3]:
            priv_ch = guild.get_channel(actor_row[3])
            if priv_ch:
                if is_self:
                    priv_msg = f"🦴 Self-scan complete. Your role confirmed as: **{target_role}**. This has been reported to the den."
                else:
                    priv_msg = f"🦴 Scan complete. **{target_name}** is: **{target_role}**. This has been reported to the den."
                await priv_ch.send(fmt(priv_msg))

        # ── Post to mod-log ───────────────────────────────────────────────
        actor = guild.get_member(self.actor_id)
        await post_mod_log(guild,
            f"🦴 **Bloodhound** — Night {night_num}\n"
            f"**{actor.display_name if actor else self.actor_id}** scanned **{target_name}**\n"
            f"**Result:** {target_role}\n"
            f"*Auto-reported to wolf-den and Bloodhound private channel.*")

        # ── Confirm to player ─────────────────────────────────────────────
        await interaction.response.edit_message(
            content=fmt(f"✅ Scan submitted on {target_name}.\nResult sent to your channel and the den."),
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
        target_id = int(interaction.data["values"][0])
        await self._save_and_close(interaction, "bloodletter", target_id,
            f"{interaction.guild.get_member(target_id).display_name} marked — they appear as wolf to checks for 2 nights.")

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
        await post_mod_log(interaction.guild,
            f"🌪️ **Crazed Wolf Double Kill** — Night {night_num}\n"
            f"**{actor.display_name}** targeting: **{tname1}** then **{tname2}**")
        await interaction.response.edit_message(
            content=fmt(f"✅ Double kill submitted: {tname1} and {tname2}."), view=None)


# ── Dire Wolf ─────────────────────────────────────────────────────────────
class DireWolfView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel = Select(placeholder="💔 Choose your secret mate (first night only)", options=self._player_options())
        sel.callback = self.on_select
        self.add_item(sel)

    async def on_select(self, interaction):
        target_id = int(interaction.data["values"][0])
        await self._save_and_close(interaction, "dire_wolf", target_id,
            "You have chosen your secret mate. If they die, you die too. Do NOT reveal this to your den.")


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
            f"Haunting {interaction.guild.get_member(target_id).display_name} — their vote tomorrow is yours to control.")

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
        asyncio.create_task(_sw_mark_dead(interaction.guild, interaction.guild_id, target_id, "sw"))

        await interaction.response.edit_message(
            content=fmt(f"✅ Kill submitted on {tname} (post-death ability).\nYour kill list has been updated."),
            view=None)


# ── Werekitten ────────────────────────────────────────────────────────────
class WerekittenView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        ack = Button(label="🐱 Acknowledge Werekitten Role", style=discord.ButtonStyle.blurple)
        ack.callback = self.on_ack
        self.add_item(ack)

    async def on_ack(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "werekitten_ack", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"🐱 **Werekitten** — Night {night_num}\n**{actor.display_name}** acknowledged. "
            f"Remind: appears as villager to Seer, bypasses Sheriff, if voted out all actions silenced.")
        await interaction.response.edit_message(
            content=fmt("✅ Acknowledged. Vote with the pack in wolf-vote."), view=None)


# ── White Wolf ────────────────────────────────────────────────────────────
class WhiteWolfView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        sel  = Select(placeholder="🤍 Choose an independent kill target", options=self._player_options())
        skip = Button(label="Skip Kill This Night", style=discord.ButtonStyle.secondary)
        sel.callback  = self.on_select
        skip.callback = self.on_skip
        self.add_item(sel)
        self.add_item(skip)

    async def on_select(self, interaction):
        target_id = int(interaction.data["values"][0])
        await self._save_and_close(interaction, "white_wolf", target_id,
            f"Kill submitted on {interaction.guild.get_member(target_id).display_name}.")

    async def on_skip(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "white_wolf_skip", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"🤍 **White Wolf** — Night {night_num}\n**{actor.display_name}** chose NOT to kill. "
            f"Track consecutive skips — 3 in a row = White Wolf dies.")
        await interaction.response.edit_message(content=fmt("✅ No kill this night.\n⚠️ 3 consecutive skips = death!"), view=None)


# ── Oracle ────────────────────────────────────────────────────────────────
class OracleView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        ack = Button(label="🔯 Submit Question to Mod", style=discord.ButtonStyle.blurple)
        ack.callback = self.on_ack
        self.add_item(ack)

    async def on_ack(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "oracle_ack", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"🔯 **Oracle** — Night {night_num}\n**{actor.display_name}** is consulting the crystal ball. "
            f"Await their yes/no question via DM or `/action`.")
        await interaction.response.edit_message(
            content=fmt("✅ Send your yes/no question to the mod via /action or DM.\nRemember — do NOT share the answer outright."),
            view=None)


# ── Warlock ───────────────────────────────────────────────────────────────
class WarlockView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        ack = Button(label="⚗️ Acknowledge Wish Status", style=discord.ButtonStyle.blurple)
        ack.callback = self.on_ack
        self.add_item(ack)

    async def on_ack(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "warlock_ack", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"⚗️ **Warlock** — Night {night_num}\n**{actor.display_name}** acknowledged wish status.")
        await interaction.response.edit_message(
            content=fmt("✅ If a wish has been requested, the mod will notify you. You choose whether to grant it."),
            view=None)


# ── Fairy Elf ─────────────────────────────────────────────────────────────
class FairyElfView(BaseNightView):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        ack = Button(label="🧚 Acknowledge Happy Ending Status", style=discord.ButtonStyle.blurple)
        ack.callback = self.on_ack
        self.add_item(ack)

    async def on_ack(self, interaction):
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "fairy_elf_ack", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"🧚 **Fairy Elf** — Night {night_num}\n**{actor.display_name}** acknowledged their status.")
        await interaction.response.edit_message(
            content=fmt("✅ If a happy ending has been requested, the mod will notify you. You choose whether to grant it."),
            view=None)


# ── Dispatcher — returns the right view for each role ─────────────────────
ROLE_VIEW_MAP = {
    "Seer":          SeerView,
    "Doctor":        DoctorView,
    "Surgeon":       SurgeonView,
    "Bodyguard":     BodyguardView,
    "Witch":         WitchView,
    "Sheriff":       SheriffView,
    "Huntsman":      HuntsmanView,
    "Insomniac":     InsomniacView,
    "Medium":        MediumView,
    "Gravedigger":   GravediggerView,
    "Hermit":        HermitView,
    "Agitator":      AgitatorView,
    "Governor":      GovernorView,
    "Clone":         CloneView,
    "Shapeshifter":  ShapeshifterView,
    "Cupid":         CupidView,
    "Wolf Pup":      WolfPupView,
    "Alpha":         AlphaView,
    "Elite Alpha":   EliteAlphaView,
    "Bloodhound":    BloodhoundView,
    "Bloodletter":   BloodletterView,
    "Crazed Wolf":   CrazedWolfView,
    "Dire Wolf":     DireWolfView,
    "Echo-Stalker":  EchoStalkerView,
    "Shadow Wolf":   ShadowWolfView,
    "Werekitten":    WerekittenView,
    "White Wolf":    WhiteWolfView,
    "Oracle":        OracleView,
    "Warlock":       WarlockView,
    "Fairy Elf":     FairyElfView,
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
        await post_mod_log(interaction.guild,
            f"🍕 **Pothead Second Kill** — Night {self.night_num}\n"
            f"**Selected by:** {interaction.user.display_name}\n"
            f"**Target:** {tname}\n"
            f"Mod: eliminate this player at resolution.")
        await interaction.response.edit_message(
            content=fmt(f"🍕 Second kill target selected: {tname}.\nMod has been notified."),
            view=None)

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
        vc_ch  = interaction.guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
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
    def __init__(self, guild_id, actor_id, role_name, night_num):
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
        if interaction.user.id != self.actor_id:
            return await interaction.response.send_message("❌ This isn't your status button.", ephemeral=True)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"✅ **{self.role_name}** — Night {self.night_num}\n"
            f"**{actor.display_name if actor else self.actor_id}** confirmed: using their ability tonight.")
        await interaction.response.edit_message(
            content=fmt("✅ Got it — the mod knows you are using your ability tonight.\nSubmit it using the action above."),
            view=None)

    async def on_pass(self, interaction: discord.Interaction):
        if interaction.user.id != self.actor_id:
            return await interaction.response.send_message("❌ This isn't your status button.", ephemeral=True)
        db_save_night_action(self.guild_id, self.night_num, self.actor_id, "_pass", None)
        actor = interaction.guild.get_member(self.actor_id)
        await post_mod_log(interaction.guild,
            f"💤 **{self.role_name}** — Night {self.night_num}\n"
            f"**{actor.display_name if actor else self.actor_id}** is passing — no action tonight.")
        await interaction.response.edit_message(
            content=fmt("💤 Passed. The mod has been notified. Sleep tight!"),
            view=None)

def get_night_view(guild_id, actor_id, role_name, alive_players):
    """Return the appropriate night action View for a given role, or None if passive."""
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
        # ── Night → Resolve and show day summary ─────────────────────────
        task = night_timers.pop(interaction.guild_id, None)
        if task: task.cancel()
        await resolve_night(interaction.guild, night_num)
        # Post standings prompt to mod-log
        vote_summary = _build_vote_summary(interaction.guild, interaction.guild_id)
        state_new    = cached_get_state(interaction.guild_id)
        mod_ch       = interaction.guild.get_channel(state_new.get("mod_log_channel_id") or 0)
        if mod_ch:
            embed = discord.Embed(
                title       = f"☀️ Night {night_num} Resolved — Day {night_num} Begins",
                description = "Night actions have been processed. Use `/eliminate` to remove players then start the day vote.",
                color       = 0xE67E22
            )
            embed.add_field(name="Current vote standings", value=vote_summary or "No votes yet.", inline=False)
            await mod_ch.send(embed=embed)
        await interaction.followup.send(
            f"✅ Night {night_num} resolved. Day phase active.", ephemeral=True)

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

    # Defer immediately — lightest possible acknowledgment, gives 15 min for heavy work
    await interaction.response.defer(ephemeral=True)

    state = cached_get_state(interaction.guild_id)
    db_set_state(interaction.guild_id, phase="night")
    if state.get("phase") == "day":
        db_increment_night(interaction.guild_id)
    night_num = db_get_night_num(interaction.guild_id)
    duration  = state.get("night_duration", 36000)
    mins      = duration // 60

    await interaction.followup.send(f"🌙 Night {night_num} begins! ({mins} min)", ephemeral=True)
    await _run_start_night(interaction.guild, interaction.guild_id, night_num, duration, state)




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


async def resolve_night(guild: discord.Guild, night_num: int):
    """Mark phase as day, clear bond, notify mod — mod handles all resolution manually."""
    guild_id = guild.id

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
        label = action_type.replace("_", " ").title()
        action_lines.append(f"• **{actor_name}** — {label} → {target_name}")

    summary = "\n".join(action_lines) if action_lines else "No actions submitted."

    await post_mod_log(guild,
        f"☀️ **Night {night_num} has ended — ready to resolve**\n\n"
        f"**Submitted actions:**\n{summary}\n\n"
        f"Use `/eliminate` to apply deaths, then `/next_phase` or `/start_day_vote` when ready.")

    await log_event(guild, f"Day {night_num}", f"☀️ Day {night_num} begins — Night {night_num} resolved")
    await set_bot_status(f"☀️ Day {night_num} — discuss and vote")
    await post_day_transition(guild, night_num, state=cached_get_state(guild_id))

# ====================== FONT PICKER ======================

class FontPickerView(View):
    """Shown during /start_game setup — lets mod preview and choose a channel font style."""
    def __init__(self, guild_id: int, roles_list: list):
        super().__init__(timeout=180)
        self.guild_id   = guild_id
        self.roles_list = roles_list
        self.chosen     = get_guild_font(guild_id)
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
        view  = RoleBuilderView(self.guild_id, self.roles_list)
        sample = apply_font("mod-log  |  wolf-den  |  ghost-chat", self.chosen)
        state     = cached_get_state(self.guild_id)
        night_dur = state.get("night_duration", 36000)
        day_dur   = state.get("day_duration",   50400)
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

@tree.command(name="remind_night", description="Ping players who have not submitted a night action yet")
@is_mod()
@app_commands.describe(auto_minutes="Also auto-remind after this many minutes (0 = manual only)")
async def remind_night(interaction: discord.Interaction, auto_minutes: int = 0):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    state = cached_get_state(interaction.guild_id)
    if state.get("phase") != "night":
        return await interaction.response.send_message("No active night phase.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    async def send_reminders(guild_id, guild):
        rows      = db_get_assignments(guild_id)
        night_num = db_get_night_num(guild_id)
        actions   = db_get_night_actions(guild_id, night_num)
        submitted = {a[0] for a in actions}
        reminded  = 0
        for pid, role_name, is_alive, ch_id in rows:
            if not is_alive or role_name not in ROLE_VIEW_MAP:
                continue
            if pid in submitted:
                continue
            ch = guild.get_channel(ch_id or 0)
            if ch:
                m = guild.get_member(pid)
                try:
                    await ch.send(
                        f"\N{ALARM CLOCK} {m.mention if m else ''} **Reminder** \N{EM DASH} you haven't submitted "
                        f"your **{role_name}** action for Night {night_num} yet!")
                    reminded += 1
                except Exception:
                    pass
        return reminded

    count = await send_reminders(interaction.guild_id, interaction.guild)
    await interaction.followup.send(
        f"\N{WHITE HEAVY CHECK MARK} Reminded **{count}** player(s) who hadn't submitted yet."
        + (f" Auto-reminder set for {auto_minutes} min." if auto_minutes else ""),
        ephemeral=True)

    if auto_minutes > 0:
        async def auto_remind():
            await asyncio.sleep(auto_minutes * 60)
            cur = cached_get_state(interaction.guild_id)
            if cur and cur.get("phase") == "night":
                cnt = await send_reminders(interaction.guild_id, interaction.guild)
                await post_mod_log(interaction.guild,
                    f"\N{ALARM CLOCK} **Auto-Remind** fired \N{EM DASH} pinged **{cnt}** player(s) who hadn't acted.")
        t = asyncio.create_task(auto_remind())
        remind_timers[interaction.guild_id] = t


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
    night_num = db_get_night_num(interaction.guild_id)
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
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
    await interaction.response.send_message(
        f"\N{WHITE HEAVY CHECK MARK} Turn result recorded: **{result_pretty}** for **{player.display_name}**.",
        ephemeral=True)


# ====================== VOTE HISTORY ======================

@tree.command(name="vote_history", description="Show complete voting history for the current game")
@is_mod()
@app_commands.describe(player="Filter to a specific player (optional)")
async def vote_history(interaction: discord.Interaction, player: discord.Member = None):
    await interaction.response.defer(ephemeral=True)
    rows    = db_get_vote_history(interaction.guild_id)
    assigns = db_get_assignments(interaction.guild_id)
    pid_to_name = {}
    for pid, _, _, _ in assigns:
        m = interaction.guild.get_member(pid)
        pid_to_name[pid] = m.display_name if m else str(pid)

    if player:
        rows = [r for r in rows if r[1] == player.id]
    if not rows:
        return await interaction.followup.send("No vote history recorded yet.", ephemeral=True)

    by_day = {}
    for day_num, voter_id, target_id, action in rows:
        by_day.setdefault(day_num, []).append((voter_id, target_id, action))

    for day_num in sorted(by_day.keys())[:10]:
        embed = discord.Embed(title=f"Day {day_num} Vote History", color=0x5865F2)
        lines = []
        for voter_id, target_id, action in by_day[day_num]:
            voter = pid_to_name.get(voter_id, str(voter_id))
            if action == "abstain" or target_id is None:
                lines.append(f"**{voter}** -> Abstain")
            else:
                target = pid_to_name.get(target_id, str(target_id))
                lines.append(f"**{voter}** -> **{target}**")
        embed.description = "\n".join(lines) or "No votes this day."
        await interaction.followup.send(embed=embed, ephemeral=True)


# ====================== SPIN WHEEL ======================

NIGHT_ROLE_ACTIONS = {
    "Seer":        ("seer",        "Seer"),
    "Doctor":      ("doctor",      "Doctor"),
    "Bodyguard":   ("bodyguard",   "Bodyguard"),
    "Witch":       ("witch",       "Witch"),
    "Wolf":        ("wolf",        "Wolf Pack"),
    "Wolf Pup":    ("wolf_pup",    "Wolf Pup"),
    "Alpha":       ("alpha",       "Alpha"),
    "Elite Alpha": ("elite_alpha", "Elite Alpha"),
}

def db_save_night_order(guild_id, night_num, role_order: list):
    import json
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO night_order VALUES (?,?,?)",
              (guild_id, night_num, json.dumps(role_order)))
    conn.commit()
    conn.close()

def db_get_night_order(guild_id, night_num):
    import json
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT role_order FROM night_order WHERE guild_id=? AND night_num=?",
              (guild_id, night_num))
    row = c.fetchone()
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


@tree.command(name="spin_wheel", description="Spin the wheel — actions, roles, or players")
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
        "Bodyguard Guard", "Witch Save Potion", "Witch Poison Potion",
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
        roles = [r["name"] for r in cached_load_roles(interaction.guild_id)]

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


@tree.command(name="game_recap", description="Post a full end-of-game recap embed to village-chat")
@is_mod()
async def game_recap(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    rows     = db_get_assignments(interaction.guild_id)
    if not rows:
        return await interaction.followup.send("No player data found.", ephemeral=True)

    state     = cached_get_state(interaction.guild_id)
    night_num = db_get_night_num(interaction.guild_id)
    log       = db_get_log(interaction.guild_id)
    vote_hist = db_get_vote_history(interaction.guild_id)
    npcs      = db_get_npcs(interaction.guild_id)

    # Build name map including NPCs
    pid_to_name = {}
    for pid, _, _, _ in rows:
        m = interaction.guild.get_member(pid)
        npc_match = next((n for n in npcs if n["npc_id"] == pid), None)
        pid_to_name[pid] = npc_match["name"] if npc_match else (m.display_name if m else str(pid))

    # Find who each player voted for most
    vote_tally = {}  # voter_id -> {target_id: count}
    for _, voter_id, target_id, action in vote_hist:
        if action == "vote" and target_id:
            vote_tally.setdefault(voter_id, {})
            vote_tally[voter_id][target_id] = vote_tally[voter_id].get(target_id, 0) + 1

    # Find elimination night from log
    elim_night = {}
    for ts, phase, event in log:
        if "eliminated" in event.lower():
            for pid, name in pid_to_name.items():
                if name.lower() in event.lower() and pid not in elim_night:
                    elim_night[pid] = phase

    # Determine winner from log
    winner_line = next((e for _, _, e in log if "wins!" in e.lower()), None)

    # Sort: alive first, then by elimination order
    alive_rows = [r for r in rows if r[2] == 1]
    dead_rows  = [r for r in rows if r[2] == 0]

    village_lines, wolf_lines, neutral_lines = [], [], []

    for pid, role_name, is_alive, _ in alive_rows + dead_rows:
        team       = get_team(interaction.guild_id, role_name)
        npc_match  = next((n for n in npcs if n["npc_id"] == pid), None)
        name       = pid_to_name.get(pid, str(pid))
        is_npc     = npc_match is not None
        npc_tag    = " `NPC`" if is_npc else ""

        if is_alive:
            status = "✅ Survived"
            phase  = ""
        else:
            phase_str = elim_night.get(pid, "")
            status = f"💀 Eliminated ({phase_str})" if phase_str else "💀 Eliminated"

        # Most voted target
        targets = vote_tally.get(pid, {})
        if targets:
            top_target = max(targets, key=targets.get)
            top_name   = pid_to_name.get(top_target, str(top_target))
            vote_note  = f" · Voted {top_name} most ({targets[top_target]}x)"
        else:
            vote_note = ""

        line = f"**{name}**{npc_tag} — {role_name} · {status}{vote_note}"

        if team == "wolf":    wolf_lines.append(line)
        elif team == "neutral": neutral_lines.append(line)
        else:                 village_lines.append(line)

    embed = discord.Embed(
        title       = f"📖 Game Recap — Night {night_num}",
        description = f"*{winner_line}*" if winner_line else "*Game ended*",
        color       = 0x2C3E50,
        timestamp   = datetime.now()
    )
    if village_lines:
        embed.add_field(name="🏘️ Village",  value="\n".join(village_lines),  inline=False)
    if wolf_lines:
        embed.add_field(name="🐺 Wolves",   value="\n".join(wolf_lines),     inline=False)
    if neutral_lines:
        embed.add_field(name="⚖️ Neutral",  value="\n".join(neutral_lines),  inline=False)

    embed.set_footer(text=f"VillageAid Game Recap  ·  {len(rows)} players")

    # Post to village-chat
    vc_ch = interaction.guild.get_channel(state.get("village_chat_ch_id") or VILLAGE_CHAT_ID)
    if vc_ch:
        await vc_ch.send(embed=embed)
        await interaction.followup.send("✅ Recap posted to village-chat.", ephemeral=True)
    else:
        await interaction.followup.send(embed=embed)

@tree.command(name="game_summary", description="Post a full end-of-game report to mod-log")
@is_mod()
async def game_summary(interaction: discord.Interaction):
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
    c = conn.cursor()
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
            ("/setup_roles",          "Set the Mod, Participant, Dead, and Spectator Discord roles."),
            ("/set_spectator_role",   "Update the Spectator role after initial setup."),
            ("/set_day_duration",     "Set how long the day phase lasts (default 14 hrs)."),
            ("/set_night_duration",   "Set how long the night phase lasts (default 10 hrs)."),
            ("/setup_hall_of_fame",   "Pin a live Hall of Fame leaderboard to a channel."),
            ("/set_font",             "Choose a Unicode font style for all game channel names. Persists across games."),
        ]
    },
    "roles": {
        "label": "🎭 Role Management",
        "desc":  "Manage the pool of roles available for selection. Changes persist across all future games.",
        "commands": [
            ("/add_role",    "Add or update a role in the pool (name, description, count, team)."),
            ("/remove_role", "Remove a role from the pool permanently."),
            ("/list_roles",  "Show all saved roles grouped by team."),
        ]
    },
    "game": {
        "label": "🎮 Game Management",
        "desc":  "Commands to start, run, and end a game. Mod-only.",
        "commands": [
            ("/start_game",     "Step 1: Choose font. Step 2: Build your role roster a-la-carte. Launch the game."),
            ("/end_game",       "End the active game and delete all game channels and data."),
            ("/announce",       "Send a message to all (or alive-only) player private channels."),
            ("/transfer_mod",   "Hand mod control to another player mid-game."),
            ("/scramble_roles", "Secretly reshuffle roles within teams — wolves stay wolves, village stays village."),
            ("/revive_player",  "Bring an eliminated player back. Choose: same role, random, blank, or a specific role."),
            ("/add_player",     "Add a new player to an active game mid-session."),
            ("/kick_player",    "Remove a player from the game without ending it."),
            ("/assign_victors", "Record the winning team for stats tracking after a game ends."),
        ]
    },
    "day": {
        "label": "☀️ Day Phase",
        "desc":  "Commands for managing the daily vote.",
        "commands": [
            ("/start_day_vote", "Open the day vote. Optionally set a timer in minutes."),
            ("/clear_day_votes","Clear all current day votes (posts a snapshot first)."),
            ("/eliminate",      "Eliminate a player. Optionally post publicly. Auto-checks win condition."),
            ("/vote_history",   "Show every vote cast across all days. Filter by player optionally."),
        ]
    },
    "night": {
        "label": "🌙 Night Phase",
        "desc":  "Commands for running and resolving night actions.",
        "commands": [
            ("/start_night",      "Begin the night phase. Sends ability prompts to all relevant players."),
            ("/spin_wheel",       "Spin the wheel to randomly set the resolution order. Posted to mod-log only."),
            ("/resolve_night",    "Manually resolve all night actions early (or let the timer do it)."),
            ("/remind_night",     "Ping players who haven't submitted their night action yet. Optional auto-timer."),
            ("/log_turn_result",  "Record the outcome of an Alpha/Elite Alpha turn: successful, blocked, etc."),
        ]
    },
    "info": {
        "label": "📊 Info & Stats",
        "desc":  "Commands available to all players during a game.",
        "commands": [
            ("/my_role",        "Show your secret role (only visible to you)."),
            ("/my_stats",       "Show your personal win/loss/elimination stats."),
            ("/list_players",   "Show alive and dead players."),
            ("/game_status",    "Show current game phase, night number, and player counts."),
            ("/action",         "Send a private action or message to the mod team."),
            ("/post_stats",     "Post the all-time leaderboard to the stats channel."),
            ("/game_summary",   "Post a full end-of-game report to mod-log (deaths, votes, turns, blocks)."),
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
            "• **Participant** — assigned to players joining the game\n"
            "• **Dead** — assigned to eliminated players\n"
            "• **Spectator** — watchers who can see public channels\n\n"
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
        name="Step 3 — (Optional) Adjust Timers",
        value=(
            "`/set_day_duration` — default 14 hours\n"
            "`/set_night_duration` — default 10 hours\n"
            "These persist and apply to every future game."
        ),
        inline=False
    )

    embed.add_field(
        name="Step 4 — (Optional) Customise",
        value=(
            "`/set_font` — choose a Unicode font style for channel names\n"
            "`/add_role` — add custom roles beyond the 45 built-in ones\n"
            "`/setup_hall_of_fame` — pin a live leaderboard to a channel\n"
            "All of these persist across games automatically."
        ),
        inline=False
    )

    embed.add_field(
        name="Step 5 — Start a Game",
        value=(
            "1. Assign the **Participant** role to all players joining\n"
            "2. Run `/start_game`\n"
            "3. Choose a channel font style\n"
            "4. Build your role roster (pick any combo of the 45 built-in roles)\n"
            "5. Confirm — the bot creates all channels and deals roles privately\n\n"
            "The game starts on **Night 1** automatically."
        ),
        inline=False
    )

    embed.add_field(
        name="📖 Need help during a game?",
        value="Run `/villagehelp` to browse all 35 commands by category.",
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



@tree.command(name="modcheck", description="Debug — check your mod status")
async def modcheck(interaction: discord.Interaction):
    state   = db_get_state(interaction.guild_id) or {}
    role_id = state.get("mod_role_id")
    try:
        member = await interaction.guild.fetch_member(interaction.user.id)
        member_roles = [r.id for r in member.roles]
    except Exception as e:
        member_roles = f"fetch failed: {e}"
    is_admin = interaction.user.guild_permissions.administrator
    has_role = isinstance(member_roles, list) and role_id in member_roles
    await interaction.response.send_message(
        f"**Mod Check**\n"
        f"DB mod_role_id: `{role_id}`\n"
        f"Your role IDs: `{member_roles}`\n"
        f"Is admin: `{is_admin}`\n"
        f"Has mod role: `{has_role}`\n"
        f"Result: `{'✅ PASS' if is_admin or has_role else '❌ FAIL'}`",
        ephemeral=True)

client.run(os.getenv("DISCORD_TOKEN"))