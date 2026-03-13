import discord
from discord import app_commands
from discord.ui import Select, View, Modal, TextInput, Button
import random
import os
import asyncio
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

# ====================== CLIENT SETUP ======================
intents = discord.Intents.default()
intents.members = True
intents.message_content = True  # Required for message logging

client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# ====================== DATABASE ======================
DB_FILE = "mafia_game.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Permanent role definitions (per guild)
    c.execute('''CREATE TABLE IF NOT EXISTS game_roles (
                    guild_id    INTEGER,
                    role_name   TEXT,
                    description TEXT DEFAULT '',
                    count       INTEGER DEFAULT 1,
                    team        TEXT DEFAULT 'village',
                    PRIMARY KEY (guild_id, role_name)
                 )''')

    # Persisted game state (survives restarts)
    c.execute('''CREATE TABLE IF NOT EXISTS game_state (
                    guild_id            INTEGER PRIMARY KEY,
                    phase               TEXT DEFAULT 'day',
                    night_duration      INTEGER DEFAULT 36000,
                    category_id         INTEGER,
                    wolf_channel_id     INTEGER,
                    ghost_channel_id    INTEGER,
                    mod_log_channel_id  INTEGER,
                    wheel_channel_id    INTEGER,
                    mod_role_id         INTEGER,
                    participant_role_id INTEGER,
                    dead_role_id        INTEGER,
                    active_vote_msg_id  INTEGER,
                    active_vote_ch_id   INTEGER
                 )''')

    # Player assignments
    c.execute('''CREATE TABLE IF NOT EXISTS player_assignments (
                    guild_id   INTEGER,
                    player_id  INTEGER,
                    role_name  TEXT,
                    is_alive   INTEGER DEFAULT 1,
                    channel_id INTEGER,
                    PRIMARY KEY (guild_id, player_id)
                 )''')

    # Night actions submitted per phase
    c.execute('''CREATE TABLE IF NOT EXISTS night_actions (
                    guild_id    INTEGER,
                    night_num   INTEGER,
                    actor_id    INTEGER,
                    action_type TEXT,
                    target_id   INTEGER,
                    used        INTEGER DEFAULT 0,
                    PRIMARY KEY (guild_id, night_num, actor_id)
                 )''')

    # Witch one-use tracking
    c.execute('''CREATE TABLE IF NOT EXISTS witch_uses (
                    guild_id  INTEGER,
                    player_id INTEGER,
                    used_save INTEGER DEFAULT 0,
                    used_kill INTEGER DEFAULT 0,
                    PRIMARY KEY (guild_id, player_id)
                 )''')

    # General game counters (night number, etc.)
    c.execute('''CREATE TABLE IF NOT EXISTS game_counters (
                    guild_id  INTEGER PRIMARY KEY,
                    night_num INTEGER DEFAULT 0
                 )''')

    conn.commit()
    conn.close()

init_db()

# ---- Role DB helpers ----
def db_save_role(guild_id, name, description, count, team):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO game_roles VALUES (?,?,?,?,?)",
              (guild_id, name, description, count, team.lower()))
    conn.commit()
    conn.close()

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

# ---- Game state DB helpers ----
def db_get_state(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT * FROM game_state WHERE guild_id=?", (guild_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    cols = ["guild_id","phase","night_duration","category_id","wolf_channel_id",
            "ghost_channel_id","mod_log_channel_id","wheel_channel_id",
            "mod_role_id","participant_role_id","dead_role_id",
            "active_vote_msg_id","active_vote_ch_id"]
    return dict(zip(cols, row))

def db_set_state(guild_id, **kwargs):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO game_state (guild_id) VALUES (?)", (guild_id,))
    for key, val in kwargs.items():
        c.execute(f"UPDATE game_state SET {key}=? WHERE guild_id=?", (val, guild_id))
    conn.commit()
    conn.close()

def db_clear_state(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM game_state WHERE guild_id=?", (guild_id,))
    c.execute("DELETE FROM player_assignments WHERE guild_id=?", (guild_id,))
    c.execute("DELETE FROM night_actions WHERE guild_id=?", (guild_id,))
    c.execute("DELETE FROM witch_uses WHERE guild_id=?", (guild_id,))
    c.execute("DELETE FROM game_counters WHERE guild_id=?", (guild_id,))
    conn.commit()
    conn.close()

# ---- Player DB helpers ----
def db_save_assignments(guild_id, assignments: dict, channel_map: dict):
    """assignments: {player_id: role_name}, channel_map: {player_id: channel_id}"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM player_assignments WHERE guild_id=?", (guild_id,))
    for pid, role in assignments.items():
        ch_id = channel_map.get(pid)
        c.execute("INSERT INTO player_assignments VALUES (?,?,?,1,?)", (guild_id, pid, role, ch_id))
    conn.commit()
    conn.close()

def db_get_assignments(guild_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT player_id, role_name, is_alive, channel_id FROM player_assignments WHERE guild_id=?", (guild_id,))
    rows = c.fetchall()
    conn.close()
    return rows  # list of (player_id, role_name, is_alive, channel_id)

def db_set_player_alive(guild_id, player_id, alive: bool):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE player_assignments SET is_alive=? WHERE guild_id=? AND player_id=?",
              (1 if alive else 0, guild_id, player_id))
    conn.commit()
    conn.close()

# ---- Night action helpers ----
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
    return rows  # list of (actor_id, action_type, target_id)

# ---- Witch helpers ----
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

# ====================== IN-MEMORY CACHE ======================
# Lightweight cache to avoid DB calls on every event
_state_cache = {}  # guild_id -> state dict (mirrors DB)

def get_state(guild_id):
    if guild_id not in _state_cache:
        _state_cache[guild_id] = db_get_state(guild_id) or {}
    return _state_cache[guild_id]

def invalidate_cache(guild_id):
    _state_cache.pop(guild_id, None)

# ====================== HELPERS ======================
ROLE_ABILITIES = {"Seer", "Doctor", "Bodyguard", "Witch", "Cursed"}
NIGHT_ABILITY_ROLES = {"Seer", "Doctor", "Bodyguard", "Witch"}

def get_role_info(guild_id, role_name):
    roles = db_load_roles(guild_id)
    for r in roles:
        if r["name"] == role_name:
            return r
    return {"name": role_name, "description": "", "count": 1, "team": "village"}

def get_team(guild_id, role_name):
    return get_role_info(guild_id, role_name).get("team", "village")

def is_mod():
    async def predicate(interaction: discord.Interaction):
        state = db_get_state(interaction.guild_id)
        if state:
            mod_role = interaction.guild.get_role(state.get("mod_role_id"))
            if mod_role and mod_role in interaction.user.roles:
                return True
        return interaction.user.guild_permissions.administrator
    return app_commands.check(predicate)

def game_active(guild_id):
    state = db_get_state(guild_id)
    return state is not None and state.get("category_id") is not None

async def post_mod_log(guild, message: str):
    state = db_get_state(guild.id)
    if not state:
        return
    ch_id = state.get("mod_log_channel_id")
    if not ch_id:
        return
    ch = guild.get_channel(ch_id)
    if ch:
        await ch.send(message)

async def check_win_condition(guild):
    """Returns winning team string or None. Announces and ends game if won."""
    rows = db_get_assignments(guild.id)
    alive_roles = [r[1] for r in rows if r[2] == 1]
    alive_wolves = [r for r in alive_roles if get_team(guild.id, r) == "wolf"]
    alive_village = [r for r in alive_roles if get_team(guild.id, r) != "wolf"]

    if len(alive_wolves) == 0:
        return "village"
    if len(alive_wolves) >= len(alive_village):
        return "wolf"
    return None

async def announce_win(guild, winner: str):
    state = db_get_state(guild.id)
    if not state:
        return
    cat = guild.get_channel(state.get("category_id"))
    # Try to find any accessible channel in the category to announce
    target_ch = None
    if cat:
        for ch in cat.channels:
            if isinstance(ch, discord.TextChannel):
                target_ch = ch
                break
    if target_ch:
        if winner == "village":
            await target_ch.send("🎉 **The Village wins!** All wolves have been eliminated!")
        else:
            await target_ch.send("🐺 **The Wolves win!** They now control the village...")

# ====================== ON READY ======================
@client.event
async def on_ready():
    print(f"✅ {client.user} is online!")
    guild_id_str = os.getenv("GUILD_ID")
    if guild_id_str:
        try:
            gid = int(guild_id_str.strip())
            guild_obj = discord.Object(id=gid)
            await tree.copy_global_to(guild=guild_obj)
            await tree.sync(guild=guild_obj)
            print(f"Commands synced to guild {gid}")
        except Exception as e:
            print(f"Guild sync failed: {e}")
            await tree.sync()
    else:
        await tree.sync()
    print("Bot ready!")

# ====================== MESSAGE LOGGING ======================
@client.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if before.author.bot or before.guild is None:
        return
    if before.content == after.content:
        return
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log = (
        f"✏️ **Message Edited** | {ts}\n"
        f"**User:** {before.author.mention} (`{before.author}`)\n"
        f"**Channel:** {before.channel.mention}\n"
        f"**Before:** {before.content or '*(empty)*'}\n"
        f"**After:** {after.content or '*(empty)*'}\n"
        f"[Jump to message]({after.jump_url})"
    )
    await post_mod_log(before.guild, log)

@client.event
async def on_message_delete(message: discord.Message):
    if message.author.bot or message.guild is None:
        return
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log = (
        f"🗑️ **Message Deleted** | {ts}\n"
        f"**User:** {message.author.mention} (`{message.author}`)\n"
        f"**Channel:** {message.channel.mention}\n"
        f"**Content:** {message.content or '*(empty or attachment)*'}"
    )
    await post_mod_log(message.guild, log)

# ====================== SETUP COMMANDS ======================
@tree.command(name="set_mod_role", description="Set the moderator role")
@is_mod()
async def set_mod_role(interaction: discord.Interaction, role: discord.Role):
    db_set_state(interaction.guild_id, mod_role_id=role.id)
    invalidate_cache(interaction.guild_id)
    await interaction.response.send_message(f"✅ Mod role set to **{role.name}**", ephemeral=True)

@tree.command(name="set_participant_role", description="Set the role that marks players in the game")
@is_mod()
async def set_participant_role(interaction: discord.Interaction, role: discord.Role):
    db_set_state(interaction.guild_id, participant_role_id=role.id)
    invalidate_cache(interaction.guild_id)
    await interaction.response.send_message(f"✅ Participant role set to **{role.name}**", ephemeral=True)

@tree.command(name="set_dead_role", description="Set the role given to eliminated players")
@is_mod()
async def set_dead_role(interaction: discord.Interaction, role: discord.Role):
    db_set_state(interaction.guild_id, dead_role_id=role.id)
    invalidate_cache(interaction.guild_id)
    await interaction.response.send_message(f"✅ Dead role set to **{role.name}**", ephemeral=True)

# ====================== ROLE MANAGEMENT ======================
@tree.command(name="add_role", description="Add or update a game role in the pool")
@is_mod()
@app_commands.describe(
    name="Role name (e.g. Seer, Villager, Werewolf)",
    description="What this role does — shown to the player",
    count="How many of this role to include",
    team="Which team: village or wolf"
)
async def add_role(interaction: discord.Interaction, name: str, description: str, count: int = 1, team: str = "village"):
    team = team.lower()
    if team not in ["village", "wolf"]:
        return await interaction.response.send_message("❌ Team must be `village` or `wolf`.", ephemeral=True)
    if count < 1:
        return await interaction.response.send_message("❌ Count must be at least 1.", ephemeral=True)
    db_save_role(interaction.guild_id, name, description, count, team)
    emoji = "🐺" if team == "wolf" else "🏘️"
    await interaction.response.send_message(
        f"{emoji} Role **{name}** ×{count} `[{team}]` saved.\n> {description}", ephemeral=True
    )

@tree.command(name="remove_role", description="Remove a role from the pool")
@is_mod()
async def remove_role(interaction: discord.Interaction, name: str):
    db_delete_role(interaction.guild_id, name)
    await interaction.response.send_message(f"🗑️ Role **{name}** removed.", ephemeral=True)

@tree.command(name="list_roles", description="Show all saved game roles")
async def list_roles(interaction: discord.Interaction):
    roles = db_load_roles(interaction.guild_id)
    if not roles:
        return await interaction.response.send_message("No roles saved yet. Use `/add_role` to add some.", ephemeral=True)

    wolf_roles = [r for r in roles if r["team"] == "wolf"]
    village_roles = [r for r in roles if r["team"] != "wolf"]

    embed = discord.Embed(title="📋 Game Roles", color=0x5865F2)
    if village_roles:
        embed.add_field(
            name="🏘️ Village",
            value="\n".join(f"**{r['name']}** ×{r['count']}\n> {r['description'] or '*No description*'}" for r in village_roles),
            inline=False
        )
    if wolf_roles:
        embed.add_field(
            name="🐺 Wolf",
            value="\n".join(f"**{r['name']}** ×{r['count']}\n> {r['description'] or '*No description*'}" for r in wolf_roles),
            inline=False
        )
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ====================== SET NIGHT DURATION ======================
class NightDurationModal(Modal, title="Set Night Phase Duration"):
    duration = TextInput(
        label="Night duration (minutes, default 600)",
        placeholder="e.g. 600",
        style=discord.TextStyle.short,
        required=True,
        min_length=1,
        max_length=4
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            mins = int(self.duration.value.strip())
            if mins < 1 or mins > 1440:
                return await interaction.response.send_message("Duration must be 1–1440 minutes.", ephemeral=True)
            db_set_state(interaction.guild_id, night_duration=mins * 60)
            invalidate_cache(interaction.guild_id)
            hrs = mins / 60
            await interaction.response.send_message(
                f"✅ Night duration set to **{mins} minutes** ({hrs:.1f} hours). Takes effect on next `/start_night`.",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Please enter a valid number.", ephemeral=True)


@tree.command(name="set_night_duration", description="Change the night phase duration (default: 600 min = 10 hours)")
@is_mod()
async def set_night_duration(interaction: discord.Interaction):
    await interaction.response.send_modal(NightDurationModal())


# ====================== START GAME ======================
class RolePoolView(View):
    """
    Step 1 of /start_game — mod picks which roles from the full saved pool
    to include this game, and sets a count for each chosen role.
    The pool is shown as a multi-select; counts are entered via a modal.
    """
    def __init__(self, guild_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.chosen_roles = {}  # role_name -> count for this game

        roles = db_load_roles(guild_id)
        if not roles:
            self.add_item(Button(label="No roles saved — use /add_role first", disabled=True, style=discord.ButtonStyle.secondary))
            return

        self.all_roles = {r["name"]: r for r in roles}

        # Role picker dropdown (pick which roles to include)
        self.role_select = Select(
            placeholder="1️⃣  Pick roles to include this game",
            min_values=1,
            max_values=min(len(roles), 25),
            options=[
                discord.SelectOption(
                    label=r["name"],
                    description=f"{r['team']} · max ×{r['count']} · {(r['description'] or 'No description')[:50]}"
                )
                for r in roles
            ]
        )
        self.role_select.callback = self.on_role_pick
        self.add_item(self.role_select)

        self.next_btn = Button(label="2️⃣  Set Counts & Continue →", style=discord.ButtonStyle.blurple, disabled=True)
        self.next_btn.callback = self.on_next
        self.add_item(self.next_btn)

    def _summary(self):
        if not self.chosen_roles:
            return "No roles selected yet."
        lines = []
        for name, cnt in self.chosen_roles.items():
            info = self.all_roles[name]
            emoji = "🐺" if info["team"] == "wolf" else "🏘️"
            lines.append(f"{emoji} **{name}** ×{cnt}")
        total = sum(self.chosen_roles.values())
        return "\n".join(lines) + f"\n\n**Total slots:** {total}"

    async def on_role_pick(self, interaction: discord.Interaction):
        picked = interaction.data["values"]
        # Seed chosen_roles with max count for each newly picked role
        for name in picked:
            if name not in self.chosen_roles:
                self.chosen_roles[name] = self.all_roles[name]["count"]
        # Remove roles that were deselected
        self.chosen_roles = {k: v for k, v in self.chosen_roles.items() if k in picked}

        self.next_btn.disabled = len(self.chosen_roles) == 0
        await interaction.response.edit_message(
            content=f"**Selected roles (counts default to max — adjust in next step):**\n{self._summary()}",
            view=self
        )

    async def on_next(self, interaction: discord.Interaction):
        # Open a modal to let mod tweak counts for each selected role
        modal = RoleCountModal(self.guild_id, self.chosen_roles, self.all_roles)
        await interaction.response.send_modal(modal)
        self.stop()


class RoleCountModal(Modal, title="Set counts for each role"):
    """Dynamically built modal — one TextInput per chosen role (max 5 due to Discord limits)."""

    def __init__(self, guild_id, chosen_roles: dict, all_roles: dict):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.all_roles = all_roles
        self.role_names = list(chosen_roles.keys())[:5]  # Discord allows max 5 inputs per modal

        self.inputs = []
        for name in self.role_names:
            max_count = all_roles[name]["count"]
            field = TextInput(
                label=f"{name} (max {max_count})",
                placeholder=f"How many {name}s? (1–{max_count})",
                default=str(chosen_roles[name]),
                min_length=1,
                max_length=2,
                required=True
            )
            self.inputs.append(field)
            self.add_item(field)

    async def on_submit(self, interaction: discord.Interaction):
        final_counts = {}
        errors = []
        for i, name in enumerate(self.role_names):
            raw = self.inputs[i].value.strip()
            try:
                cnt = int(raw)
                max_cnt = self.all_roles[name]["count"]
                if cnt < 1:
                    errors.append(f"**{name}**: must be at least 1")
                elif cnt > max_cnt:
                    errors.append(f"**{name}**: max is {max_cnt}")
                else:
                    final_counts[name] = cnt
            except ValueError:
                errors.append(f"**{name}**: '{raw}' is not a number")

        if errors:
            return await interaction.response.send_message(
                "❌ Fix these issues:\n" + "\n".join(errors), ephemeral=True
            )

        # If they had more than 5 roles, we only got counts for the first 5.
        # Show a warning and continue with what we have.
        total = sum(final_counts.values())
        await interaction.response.defer(ephemeral=True)

        summary = "\n".join(
            f"{'🐺' if self.all_roles[n]['team'] == 'wolf' else '🏘️'} **{n}** ×{c}"
            for n, c in final_counts.items()
        )
        view = ConfirmStartView(self.guild_id, final_counts, self.all_roles)
        await interaction.followup.send(
            f"**Game pool ({total} total slots):**\n{summary}\n\n"
            f"Make sure exactly **{total}** players have the participant role, then hit **Start Game**.",
            view=view,
            ephemeral=True
        )


class ConfirmStartView(View):
    """Final confirmation before launching the game."""
    def __init__(self, guild_id, final_counts: dict, all_roles: dict):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.final_counts = final_counts  # role_name -> count
        self.all_roles = all_roles

        start_btn = Button(label="🚀 Start Game", style=discord.ButtonStyle.green)
        start_btn.callback = self.on_start
        self.add_item(start_btn)

        cancel_btn = Button(label="Cancel", style=discord.ButtonStyle.danger)
        cancel_btn.callback = self.on_cancel
        self.add_item(cancel_btn)

    async def on_cancel(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content="❌ Game start cancelled.", view=None)
        self.stop()

    async def on_start(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        await self.launch_game(interaction)
        self.stop()

    async def launch_game(self, interaction: discord.Interaction):
        state = db_get_state(interaction.guild_id) or {}
        p_role_id = state.get("participant_role_id")
        p_role = interaction.guild.get_role(p_role_id) if p_role_id else None

        if not p_role:
            return await interaction.followup.send("❌ Participant role not set. Use `/set_participant_role`.", ephemeral=True)

        needed = sum(self.final_counts.values())
        players = [m for m in interaction.guild.members if p_role in m.roles and not m.bot]
        if len(players) != needed:
            return await interaction.followup.send(
                f"❌ Pool has **{needed}** slots but found **{len(players)}** players with the participant role. "
                f"Adjust role counts or participant role membership so they match.",
                ephemeral=True
            )

        # Build and shuffle pool
        pool = []
        for role_name, count in self.final_counts.items():
            pool.extend([role_name] * count)
        random.shuffle(pool)
        assignments = {p.id: role for p, role in zip(players, pool)}

        try:
            category = await interaction.guild.create_category("🎮 Village Game")
        except Exception as e:
            return await interaction.followup.send(f"❌ Failed to create category: {e}", ephemeral=True)

        everyone = interaction.guild.default_role
        bot_me = interaction.guild.me
        bot_ow = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_messages=True)

        # Mod log
        mod_role_id = state.get("mod_role_id")
        mod_role = interaction.guild.get_role(mod_role_id) if mod_role_id else None
        mod_log_ow = {everyone: discord.PermissionOverwrite(view_channel=False), bot_me: bot_ow}
        if mod_role:
            mod_log_ow[mod_role] = discord.PermissionOverwrite(view_channel=True, send_messages=False, read_messages=True)
        mod_log_ch = await category.create_text_channel("📋mod-log", overwrites=mod_log_ow)

        # Wheel spins
        wheel_ch = await category.create_text_channel("🎡wheel-spins", overwrites={
            everyone: discord.PermissionOverwrite(view_channel=True, send_messages=False),
            bot_me: bot_ow
        })

        # Wolf chat
        wolf_players = [p for p in players if get_team(interaction.guild_id, assignments[p.id]) == "wolf"]
        wolf_ow = {everyone: discord.PermissionOverwrite(view_channel=False), bot_me: bot_ow}
        for wp in wolf_players:
            wolf_ow[wp] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
        wolf_ch = await category.create_text_channel("🐺wolf-chat", overwrites=wolf_ow)

        # Ghost chat (locked until someone dies)
        ghost_ch = await category.create_text_channel("👻ghost-chat", overwrites={
            everyone: discord.PermissionOverwrite(view_channel=False), bot_me: bot_ow
        })

        # Private player channels
        player_channels = {}
        for player in players:
            role_name = assignments[player.id]
            role_info = get_role_info(interaction.guild_id, role_name)
            ch_ow = {
                everyone: discord.PermissionOverwrite(view_channel=False),
                player: discord.PermissionOverwrite(view_channel=True, send_messages=True),
                bot_me: bot_ow
            }
            ch = await category.create_text_channel(f"🔒{player.name}", overwrites=ch_ow)
            player_channels[player.id] = ch.id

            team_emoji = "🐺" if role_info["team"] == "wolf" else "🏘️"
            embed = discord.Embed(
                title=f"Your Role: {role_name} {team_emoji}",
                description=role_info.get("description") or "*No description provided.*",
                color=0xFF4444 if role_info["team"] == "wolf" else 0x44BB44
            )
            embed.set_footer(text="This channel is private — only you and the mod can see it.")
            await ch.send(player.mention, embed=embed)

            if role_name == "Cursed":
                await ch.send("⚠️ You appear as **Village** to investigators, but if a wolf attacks you, you **join the wolves** instead of dying.")

        # Wolf chat welcome
        if wolf_players:
            wolf_names = ", ".join(p.display_name for p in wolf_players)
            await wolf_ch.send(f"🐺 **Wolf Pack** — your allies: {wolf_names}\nCoordinate your nightly kill here.")

        # Persist everything
        db_save_assignments(interaction.guild_id, assignments, player_channels)
        db_set_state(
            interaction.guild_id,
            phase="day",
            category_id=category.id,
            wolf_channel_id=wolf_ch.id,
            ghost_channel_id=ghost_ch.id,
            mod_log_channel_id=mod_log_ch.id,
            wheel_channel_id=wheel_ch.id
        )
        invalidate_cache(interaction.guild_id)

        night_dur = state.get("night_duration", 36000)
        night_hrs = night_dur / 3600

        await interaction.followup.send(
            f"✅ **Game started!** {len(players)} players assigned.\n"
            f"**Roles:** {', '.join(f'{k} ×{v}' for k, v in self.final_counts.items())}\n"
            f"**Night duration:** {night_hrs:.1f} hours\n"
            f"Channels created under **{category.name}**.",
            ephemeral=True
        )
        await mod_log_ch.send(
            f"🎮 **Game started** — {len(players)} players | Night: {night_hrs:.1f}h\n"
            + "\n".join(f"• <@{pid}>: **{role}**" for pid, role in assignments.items())
        )


@tree.command(name="start_game", description="Pick roles from your saved pool and launch a new game")
@is_mod()
async def start_game(interaction: discord.Interaction):
    if game_active(interaction.guild_id):
        return await interaction.response.send_message("❌ A game is already active. Use `/end_game` first.", ephemeral=True)
    roles = db_load_roles(interaction.guild_id)
    if not roles:
        return await interaction.response.send_message("❌ No roles saved. Use `/add_role` to build your pool first.", ephemeral=True)
    state = db_get_state(interaction.guild_id)
    night_dur = state.get("night_duration", 36000) if state else 36000
    night_hrs = night_dur / 3600
    view = RolePoolView(interaction.guild_id)
    await interaction.response.send_message(
        f"**Start a new game**\nNight duration: **{night_hrs:.1f} hours** (change with `/set_night_duration`)\n\n"
        f"**Step 1:** Select which roles to include this game:",
        view=view,
        ephemeral=True
    )

# ====================== END GAME ======================
@tree.command(name="end_game", description="End the current game and clean up all channels")
@is_mod()
async def end_game(interaction: discord.Interaction):
    state = db_get_state(interaction.guild_id)
    if not state or not state.get("category_id"):
        return await interaction.response.send_message("No active game to end.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)
    cat = interaction.guild.get_channel(state["category_id"])
    if cat:
        for ch in cat.channels:
            try:
                await ch.delete()
            except Exception:
                pass
        try:
            await cat.delete()
        except Exception:
            pass

    db_clear_state(interaction.guild_id)
    invalidate_cache(interaction.guild_id)
    await interaction.followup.send("✅ Game ended. All game channels deleted.", ephemeral=True)

# ====================== PLAYER STATUS ======================
@tree.command(name="list_players", description="Show alive and dead players")
async def list_players(interaction: discord.Interaction):
    rows = db_get_assignments(interaction.guild_id)
    if not rows:
        return await interaction.response.send_message("No active game.", ephemeral=True)

    alive = [f"<@{r[0]}>" for r in rows if r[2] == 1]
    dead  = [f"<@{r[0]}>" for r in rows if r[2] == 0]

    embed = discord.Embed(title="👥 Player Status", color=0x5865F2)
    embed.add_field(name=f"✅ Alive ({len(alive)})", value=" ".join(alive) or "None", inline=False)
    embed.add_field(name=f"💀 Dead ({len(dead)})",  value=" ".join(dead) or "None", inline=False)
    await interaction.response.send_message(embed=embed)

@tree.command(name="my_role", description="Show your secret role (private)")
async def my_role(interaction: discord.Interaction):
    rows = db_get_assignments(interaction.guild_id)
    assignment = next((r for r in rows if r[0] == interaction.user.id), None)
    if not assignment:
        return await interaction.response.send_message("You are not in the current game.", ephemeral=True)
    role_name = assignment[1]
    role_info = get_role_info(interaction.guild_id, role_name)
    team_emoji = "🐺" if role_info["team"] == "wolf" else "🏘️"
    embed = discord.Embed(title=f"Your Role: {role_name} {team_emoji}", description=role_info.get("description") or "*No description.*",
                          color=0xFF4444 if role_info["team"] == "wolf" else 0x44BB44)
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ====================== ELIMINATE ======================
@tree.command(name="eliminate", description="Eliminate a player from the game")
@is_mod()
@app_commands.describe(player="The player to eliminate", public="Announce publicly? (default: True)")
async def eliminate(interaction: discord.Interaction, player: discord.Member, public: bool = True):
    rows = db_get_assignments(interaction.guild_id)
    assignment = next((r for r in rows if r[0] == player.id and r[2] == 1), None)
    if not assignment:
        return await interaction.response.send_message(f"**{player.display_name}** is not an alive player.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)
    role_name = assignment[1]
    state = db_get_state(interaction.guild_id)

    # Check Cursed conversion
    if role_name == "Cursed":
        wolf_ch_id = state.get("wolf_channel_id")
        wolf_ch = interaction.guild.get_channel(wolf_ch_id) if wolf_ch_id else None
        if wolf_ch:
            await wolf_ch.set_permissions(player, view_channel=True, send_messages=True)
            await wolf_ch.send(f"🔄 **{player.display_name}** has been revealed as the **Cursed** — they now join the wolves!")
        await interaction.followup.send(
            f"⚠️ **{player.display_name}** was Cursed — they have been converted to the wolf team instead of eliminated.",
            ephemeral=True
        )
        # Update their role in DB to a wolf-side role marker
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE player_assignments SET role_name='Cursed (Wolf)' WHERE guild_id=? AND player_id=?",
                  (interaction.guild_id, player.id))
        conn.commit()
        conn.close()
        return

    # Mark dead
    db_set_player_alive(interaction.guild_id, player.id, False)

    # Role management
    dead_role_id = state.get("dead_role_id")
    participant_role_id = state.get("participant_role_id")
    dead_role = interaction.guild.get_role(dead_role_id) if dead_role_id else None
    part_role = interaction.guild.get_role(participant_role_id) if participant_role_id else None
    try:
        if part_role and part_role in player.roles:
            await player.remove_roles(part_role)
        if dead_role:
            await player.add_roles(dead_role)
    except discord.Forbidden:
        pass

    # Lock private channel, grant ghost access
    priv_ch_id = assignment[3]
    priv_ch = interaction.guild.get_channel(priv_ch_id) if priv_ch_id else None
    if priv_ch:
        await priv_ch.set_permissions(player, view_channel=True, send_messages=False)
        await priv_ch.send(f"💀 You have been **eliminated**. Your role was **{role_name}**.")

    ghost_ch_id = state.get("ghost_channel_id")
    ghost_ch = interaction.guild.get_channel(ghost_ch_id) if ghost_ch_id else None
    if ghost_ch:
        await ghost_ch.set_permissions(player, view_channel=True, send_messages=True)
        await ghost_ch.send(f"👻 **{player.display_name}** has joined the ghost chat. Role was: **{role_name}**.")

    # Mod log
    await post_mod_log(interaction.guild, f"💀 **{player.display_name}** eliminated. Role: **{role_name}**")

    if public:
        cat = interaction.guild.get_channel(state.get("category_id"))
        if cat:
            for ch in cat.channels:
                if isinstance(ch, discord.TextChannel) and ch.id not in [
                    state.get("mod_log_channel_id"), priv_ch_id, state.get("wolf_channel_id"), ghost_ch_id
                ]:
                    await ch.send(f"💀 **{player.display_name}** has been eliminated!")
                    break

    await interaction.followup.send(f"✅ **{player.display_name}** eliminated (role: {role_name}).", ephemeral=True)

    # Win check
    winner = await check_win_condition(interaction.guild)
    if winner:
        await announce_win(interaction.guild, winner)

# ====================== ACTION COMMAND (all players) ======================
@tree.command(name="action", description="Submit an action or note to the mod team")
@app_commands.describe(message="Your action or message — only mods will see this")
async def action(interaction: discord.Interaction, message: str):
    rows = db_get_assignments(interaction.guild_id)
    assignment = next((r for r in rows if r[0] == interaction.user.id), None)
    if not assignment:
        return await interaction.response.send_message("You are not in the current game.", ephemeral=True)

    role_name = assignment[1]
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log = (
        f"📨 **Player Action** | {ts}\n"
        f"**From:** {interaction.user.mention} (`{interaction.user}`)\n"
        f"**Role:** {role_name}\n"
        f"**Message:** {message}"
    )
    await post_mod_log(interaction.guild, log)
    await interaction.response.send_message("✅ Your action has been sent to the mod team.", ephemeral=True)

# ====================== NIGHT PHASE ======================
night_timers = {}  # guild_id -> asyncio.Task

class NightAbilityView(View):
    """Sent in a player's private channel during night phase."""
    def __init__(self, guild_id, actor_id, role_name, alive_players):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.actor_id = actor_id
        self.role_name = role_name

        if role_name in NIGHT_ABILITY_ROLES:
            options = [discord.SelectOption(label=p.display_name, value=str(p.id)) for p in alive_players if p.id != actor_id]
            if options:
                label_map = {
                    "Seer":       "🔮 Investigate a player",
                    "Doctor":     "💊 Protect a player",
                    "Bodyguard":  "🛡️ Guard a player",
                    "Witch":      "🧙 Target a player (Save or Kill)"
                }
                placeholder = label_map.get(role_name, "Choose a target")
                self.target_select = Select(placeholder=placeholder, options=options)
                self.target_select.callback = self.on_target
                self.add_item(self.target_select)

                if role_name == "Witch":
                    save_btn = Button(label="Use Save Potion", style=discord.ButtonStyle.green, custom_id="witch_save")
                    kill_btn = Button(label="Use Kill Potion", style=discord.ButtonStyle.danger, custom_id="witch_kill")
                    save_btn.callback = self.witch_save
                    kill_btn.callback = self.witch_kill
                    self.add_item(save_btn)
                    self.add_item(kill_btn)

    async def on_target(self, interaction: discord.Interaction):
        target_id = int(interaction.data["values"][0])
        night_num = db_get_night_num(interaction.guild_id)
        action_type = self.role_name.lower()
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, action_type, target_id)
        target = interaction.guild.get_member(target_id)
        await interaction.response.edit_message(
            content=f"✅ Action submitted — targeting **{target.display_name if target else target_id}**.\nWait for night to resolve.",
            view=None
        )

    async def witch_save(self, interaction: discord.Interaction):
        used_save, _ = db_get_witch_uses(interaction.guild_id, self.actor_id)
        if used_save:
            return await interaction.response.send_message("❌ You have already used your save potion.", ephemeral=True)
        target_vals = interaction.data.get("values") or []
        if not target_vals:
            return await interaction.response.send_message("Select a target first using the dropdown.", ephemeral=True)
        target_id = int(target_vals[0])
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "witch_save", target_id)
        db_set_witch_use(interaction.guild_id, self.actor_id, save=1)
        target = interaction.guild.get_member(target_id)
        await interaction.response.edit_message(
            content=f"✅ Save potion used on **{target.display_name if target else target_id}**.", view=None
        )

    async def witch_kill(self, interaction: discord.Interaction):
        _, used_kill = db_get_witch_uses(interaction.guild_id, self.actor_id)
        if used_kill:
            return await interaction.response.send_message("❌ You have already used your kill potion.", ephemeral=True)
        target_vals = interaction.data.get("values") or []
        if not target_vals:
            return await interaction.response.send_message("Select a target first using the dropdown.", ephemeral=True)
        target_id = int(target_vals[0])
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, self.actor_id, "witch_kill", target_id)
        db_set_witch_use(interaction.guild_id, self.actor_id, kill=1)
        target = interaction.guild.get_member(target_id)
        await interaction.response.edit_message(
            content=f"✅ Kill potion used on **{target.display_name if target else target_id}**.", view=None
        )


class WolfKillView(View):
    """Posted in wolf-chat for wolves to vote on a kill target."""
    def __init__(self, guild_id, alive_non_wolf):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        options = [discord.SelectOption(label=p.display_name, value=str(p.id)) for p in alive_non_wolf]
        if options:
            self.select = Select(placeholder="Choose tonight's kill target", options=options)
            self.select.callback = self.on_select
            self.add_item(self.select)

    async def on_select(self, interaction: discord.Interaction):
        target_id = int(interaction.data["values"][0])
        night_num = db_get_night_num(interaction.guild_id)
        db_save_night_action(interaction.guild_id, night_num, interaction.user.id, "wolf_kill", target_id)
        target = interaction.guild.get_member(target_id)
        await interaction.response.send_message(
            f"🐺 **{interaction.user.display_name}** voted to kill **{target.display_name if target else target_id}**.",
        )


@tree.command(name="start_night", description="Begin the night phase")
@is_mod()
async def start_night(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)

    state = db_get_state(interaction.guild_id)
    if state.get("phase") == "night":
        return await interaction.response.send_message("Night is already active.", ephemeral=True)

    db_set_state(interaction.guild_id, phase="night")
    db_increment_night(interaction.guild_id)
    night_num = db_get_night_num(interaction.guild_id)
    duration = state.get("night_duration", 600)
    mins = duration // 60

    await interaction.response.send_message(f"🌙 Night phase {night_num} begins! ({mins} minutes)", ephemeral=True)

    rows = db_get_assignments(interaction.guild_id)
    alive_players = [interaction.guild.get_member(r[0]) for r in rows if r[2] == 1]
    alive_players = [p for p in alive_players if p]

    alive_non_wolf = [interaction.guild.get_member(r[0]) for r in rows
                      if r[2] == 1 and get_team(interaction.guild_id, r[1]) != "wolf"]
    alive_non_wolf = [p for p in alive_non_wolf if p]

    # Send ability prompts to private channels
    for row in rows:
        pid, role_name, is_alive, ch_id = row
        if not is_alive or not ch_id:
            continue
        ch = interaction.guild.get_channel(ch_id)
        if not ch:
            continue
        if role_name in NIGHT_ABILITY_ROLES:
            view = NightAbilityView(interaction.guild_id, pid, role_name, alive_players)
            await ch.send(f"🌙 **Night {night_num}** — submit your action below:", view=view)
        else:
            await ch.send(f"🌙 **Night {night_num}** — sleep tight. Await morning.")

    # Send wolf kill prompt to wolf-chat
    wolf_ch_id = state.get("wolf_channel_id")
    wolf_ch = interaction.guild.get_channel(wolf_ch_id) if wolf_ch_id else None
    if wolf_ch and alive_non_wolf:
        view = WolfKillView(interaction.guild_id, alive_non_wolf)
        await wolf_ch.send(f"🌙 **Night {night_num}** — choose your kill target:", view=view)

    # Announce in mod log
    await post_mod_log(interaction.guild, f"🌙 **Night {night_num}** started. Duration: {mins} minutes.")

    # Schedule auto-resolve
    async def auto_resolve():
        await asyncio.sleep(duration)
        current_state = db_get_state(interaction.guild_id)
        if current_state and current_state.get("phase") == "night":
            await resolve_night(interaction.guild, night_num)

    task = asyncio.create_task(auto_resolve())
    night_timers[interaction.guild_id] = task


@tree.command(name="resolve_night", description="Manually resolve night actions early")
@is_mod()
async def resolve_night_cmd(interaction: discord.Interaction):
    if not game_active(interaction.guild_id):
        return await interaction.response.send_message("No active game.", ephemeral=True)
    state = db_get_state(interaction.guild_id)
    if state.get("phase") != "night":
        return await interaction.response.send_message("No active night phase.", ephemeral=True)

    # Cancel timer
    task = night_timers.pop(interaction.guild_id, None)
    if task:
        task.cancel()

    night_num = db_get_night_num(interaction.guild_id)
    await interaction.response.send_message("⏩ Resolving night actions now...", ephemeral=True)
    await resolve_night(interaction.guild, night_num)


async def resolve_night(guild: discord.Guild, night_num: int):
    """Core night resolution logic. Order: Bodyguard → Doctor → Wolf kill → Witch → Seer."""
    guild_id = guild.id
    actions = db_get_night_actions(guild_id, night_num)
    state = db_get_state(guild_id)
    rows = db_get_assignments(guild_id)
    assignment_map = {r[0]: r[1] for r in rows}  # pid -> role_name

    # Index actions by type
    def get_action(atype):
        return next((a for a in actions if a[1] == atype), None)

    def get_actions(atype):
        return [a for a in actions if a[1] == atype]

    protected_ids = set()
    results = []

    # 1. Bodyguard
    bg_action = get_action("bodyguard")
    if bg_action:
        protected_ids.add(bg_action[2])
        target = guild.get_member(bg_action[2])
        results.append(f"🛡️ **Bodyguard** protected **{target.display_name if target else '?'}**.")

    # 2. Doctor
    doc_action = get_action("doctor")
    if doc_action:
        protected_ids.add(doc_action[2])
        target = guild.get_member(doc_action[2])
        results.append(f"💊 **Doctor** protected **{target.display_name if target else '?'}**.")

    # 3. Wolf kill (most-voted target wins)
    wolf_kills = get_actions("wolf_kill")
    killed_id = None
    if wolf_kills:
        vote_count = {}
        for a in wolf_kills:
            vote_count[a[2]] = vote_count.get(a[2], 0) + 1
        killed_id = max(vote_count, key=vote_count.get)

    # 4. Witch
    witch_save = get_action("witch_save")
    witch_kill = get_action("witch_kill")
    if witch_save:
        protected_ids.add(witch_save[2])
        t = guild.get_member(witch_save[2])
        results.append(f"🧙 **Witch** used save potion on **{t.display_name if t else '?'}**.")
    if witch_kill:
        wk_target = guild.get_member(witch_kill[2])
        if witch_kill[2] not in protected_ids:
            results.append(f"🧙 **Witch** poisoned **{wk_target.display_name if wk_target else '?'}** — they will be eliminated.")
            # Eliminate witch target
            await _eliminate_player(guild, witch_kill[2], "Witch's poison")
        else:
            results.append(f"🧙 **Witch** tried to poison someone but they were protected.")

    # 5. Seer
    seer_action = get_action("seer")
    seer_result = None
    if seer_action:
        target_id = seer_action[2]
        target_role = assignment_map.get(target_id, "Unknown")
        team = get_team(guild_id, target_role)
        target = guild.get_member(target_id)
        # Cursed appears as village
        if target_role == "Cursed":
            team = "village"
        seer_result = (seer_action[0], target, team, target_role)

    # Apply wolf kill
    if killed_id:
        kill_target = guild.get_member(killed_id)
        if killed_id in protected_ids:
            results.append(f"🐺 Wolves targeted **{kill_target.display_name if kill_target else '?'}**, but they were **protected**!")
        else:
            results.append(f"🐺 Wolves killed **{kill_target.display_name if kill_target else '?'}**.")
            await _eliminate_player(guild, killed_id, "Wolf attack")

    # Send Seer result privately
    if seer_result:
        seer_id, target, team, target_role = seer_result
        seer_ch_id = next((r[3] for r in rows if r[0] == seer_id), None)
        seer_ch = guild.get_channel(seer_ch_id) if seer_ch_id else None
        if seer_ch:
            emoji = "🐺" if team == "wolf" else "🏘️"
            await seer_ch.send(
                f"🔮 **Seer Result** — **{target.display_name if target else '?'}** is on the **{team.upper()}** team {emoji}"
            )
        results.append(f"🔮 **Seer** investigated a player (result sent privately).")

    # Post resolution to mod log
    db_set_state(guild_id, phase="day")
    resolution_text = f"☀️ **Night {night_num} Resolution:**\n" + ("\n".join(results) if results else "No actions taken.")
    await post_mod_log(guild, resolution_text)

    # Announce publicly (without spoilers) that day has begun
    cat = guild.get_channel(state.get("category_id"))
    if cat:
        for ch in cat.channels:
            if isinstance(ch, discord.TextChannel) and ch.id not in [
                state.get("mod_log_channel_id"),
                state.get("wolf_channel_id"),
                state.get("ghost_channel_id")
            ]:
                await ch.send(f"☀️ **Day {night_num} begins.** Check the results — who survived the night?")
                break

    # Win check
    winner = await check_win_condition(guild)
    if winner:
        await announce_win(guild, winner)


async def _eliminate_player(guild: discord.Guild, player_id: int, reason: str):
    """Internal: mark player dead, update roles/channels."""
    rows = db_get_assignments(guild.id)
    assignment = next((r for r in rows if r[0] == player_id and r[2] == 1), None)
    if not assignment:
        return

    db_set_player_alive(guild.id, player_id, False)
    state = db_get_state(guild.id)
    player = guild.get_member(player_id)
    if not player:
        return

    dead_role_id = state.get("dead_role_id")
    participant_role_id = state.get("participant_role_id")
    dead_role = guild.get_role(dead_role_id) if dead_role_id else None
    part_role = guild.get_role(participant_role_id) if participant_role_id else None
    try:
        if part_role and part_role in player.roles:
            await player.remove_roles(part_role)
        if dead_role:
            await player.add_roles(dead_role)
    except discord.Forbidden:
        pass

    priv_ch_id = assignment[3]
    priv_ch = guild.get_channel(priv_ch_id) if priv_ch_id else None
    if priv_ch:
        await priv_ch.set_permissions(player, view_channel=True, send_messages=False)
        await priv_ch.send(f"💀 You were eliminated tonight ({reason}). Your role was **{assignment[1]}**.")

    ghost_ch_id = state.get("ghost_channel_id")
    ghost_ch = guild.get_channel(ghost_ch_id) if ghost_ch_id else None
    if ghost_ch:
        await ghost_ch.set_permissions(player, view_channel=True, send_messages=True)
        await ghost_ch.send(f"👻 **{player.display_name}** joins the ghost chat. Eliminated by: {reason}.")


# ====================== VOTE SYSTEM ======================
@tree.command(name="start_vote", description="Start a vote among alive players")
@is_mod()
@app_commands.describe(
    duration_minutes="How long the vote runs (minutes)",
    votes_per_player="Votes each player gets (default 1)"
)
async def start_vote(interaction: discord.Interaction, duration_minutes: int, votes_per_player: int = 1):
    rows = db_get_assignments(interaction.guild_id)
    alive = [interaction.guild.get_member(r[0]) for r in rows if r[2] == 1]
    alive = [p for p in alive if p]

    if not alive:
        return await interaction.response.send_message("No alive players!", ephemeral=True)
    if duration_minutes < 1 or duration_minutes > 1440:
        return await interaction.response.send_message("Duration must be 1–1440 minutes.", ephemeral=True)
    if votes_per_player < 1 or votes_per_player > 5:
        return await interaction.response.send_message("Votes per player must be 1–5.", ephemeral=True)

    end_time = datetime.now() + timedelta(minutes=duration_minutes)
    human_time = end_time.strftime("%H:%M")

    embed = discord.Embed(
        title="🗳️ Village Vote",
        description=f"Ends in **{duration_minutes} min** (at {human_time})\nEach player has **{votes_per_player}** vote(s)",
        color=0xFF4444
    )
    for i, p in enumerate(alive, 1):
        embed.add_field(name=f"{i}. {p.display_name}", value="", inline=False)

    msg = await interaction.channel.send(embed=embed)
    for i in range(1, min(len(alive) + 1, 21)):
        await msg.add_reaction(f"{i}\u20e3")

    db_set_state(interaction.guild_id, active_vote_msg_id=msg.id, active_vote_ch_id=interaction.channel.id)
    await interaction.response.send_message(f"✅ Vote started. Ends at **{human_time}**. Use `/end_vote` to stop early.", ephemeral=True)

    await asyncio.sleep(duration_minutes * 60)
    state = db_get_state(interaction.guild_id)
    if state and state.get("active_vote_msg_id") == msg.id:
        await tally_vote(msg, alive, interaction.channel)
        db_set_state(interaction.guild_id, active_vote_msg_id=None, active_vote_ch_id=None)


@tree.command(name="end_vote", description="End the current vote early and tally results")
@is_mod()
async def end_vote(interaction: discord.Interaction):
    state = db_get_state(interaction.guild_id)
    if not state or not state.get("active_vote_msg_id"):
        return await interaction.response.send_message("No active vote.", ephemeral=True)
    try:
        ch = interaction.guild.get_channel(state["active_vote_ch_id"])
        msg = await ch.fetch_message(state["active_vote_msg_id"])
        rows = db_get_assignments(interaction.guild_id)
        alive = [interaction.guild.get_member(r[0]) for r in rows if r[2] == 1]
        alive = [p for p in alive if p]
        await tally_vote(msg, alive, interaction.channel)
        db_set_state(interaction.guild_id, active_vote_msg_id=None, active_vote_ch_id=None)
        await interaction.response.send_message("✅ Vote ended early.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Failed to end vote: {e}", ephemeral=True)


async def tally_vote(msg: discord.Message, alive_players: list, channel: discord.TextChannel):
    votes = {}
    for i, p in enumerate(alive_players, 1):
        if i > 20:
            break
        r = discord.utils.get(msg.reactions, emoji=f"{i}\u20e3")
        count = (r.count - 1) if r else 0
        votes[p.display_name] = count

    if not votes or max(votes.values()) == 0:
        return await channel.send("🗳️ **Vote ended** — no votes cast.")

    max_votes = max(votes.values())
    winners = [name for name, v in votes.items() if v == max_votes]
    result = ", ".join(f"**{w}**" for w in winners)
    await channel.send(f"🗳️ **Vote ended!**\nMost voted: {result} ({max_votes} vote{'s' if max_votes != 1 else ''})")

# ====================== GAME INFO ======================
@tree.command(name="game_status", description="Show current game phase and stats")
async def game_status(interaction: discord.Interaction):
    state = db_get_state(interaction.guild_id)
    if not state or not state.get("category_id"):
        return await interaction.response.send_message("No active game.", ephemeral=True)

    rows = db_get_assignments(interaction.guild_id)
    alive_count = sum(1 for r in rows if r[2] == 1)
    dead_count  = sum(1 for r in rows if r[2] == 0)
    phase = state.get("phase", "day").capitalize()
    night_num = db_get_night_num(interaction.guild_id)

    embed = discord.Embed(title="🎮 Game Status", color=0x5865F2)
    embed.add_field(name="Phase", value=f"{'🌙' if phase == 'Night' else '☀️'} {phase}", inline=True)
    embed.add_field(name="Night #", value=str(night_num), inline=True)
    embed.add_field(name="Players", value=f"✅ {alive_count} alive · 💀 {dead_count} dead", inline=False)
    await interaction.response.send_message(embed=embed)

# ====================== RUN ======================
client.run(os.getenv("DISCORD_TOKEN"))