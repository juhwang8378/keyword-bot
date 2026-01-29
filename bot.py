import logging
import os
import re
import sqlite3
import sys
from typing import Dict, List, Optional, Set, Tuple

import discord
from discord import app_commands

DB_PATH = os.getenv("KEYWORD_BOT_DB", "data/keywords.db")
LOG_PATH = os.getenv("KEYWORD_BOT_LOG_PATH", "bot.log")
LOG_LEVEL = os.getenv("KEYWORD_BOT_LOG_LEVEL", "INFO").upper()
GUILD_IDS_RAW = os.getenv("KEYWORD_BOT_GUILD_IDS", "")

INTENTS = discord.Intents.default()
INTENTS.message_content = True
INTENTS.guilds = True
INTENTS.members = True

bot = discord.Client(intents=INTENTS)
tree = app_commands.CommandTree(bot)

logger = logging.getLogger("keyword_bot")

KOREAN_PARTICLES: Tuple[str, ...] = (
    "께서",
    "에서부터",
    "으로부터",
    "로부터",
    "에게서",
    "한테서",
    "으로써",
    "로써",
    "으로서",
    "로서",
    "이라고는",
    "라고는",
    "이라고",
    "라고",
    "이나마",
    "나마",
    "이라도",
    "라도",
    "이든지",
    "든지",
    "이든",
    "든",
    "이랑",
    "랑",
    "이나",
    "나",
    "이며",
    "하며",
    "하고",
    "이자",
    "자",
    "에게",
    "한테",
    "에서",
    "으로",
    "로",
    "까지",
    "부터",
    "밖에",
    "뿐",
    "조차",
    "마저",
    "마다",
    "만큼",
    "쯤",
    "씩",
    "만치",
    "같이",
    "처럼",
    "대로",
    "보다",
    "커녕",
    "도",
    "만",
    "의",
    "과",
    "와",
    "을",
    "를",
    "은",
    "는",
    "이",
    "가",
    "에",
    "께",
)

_KOREAN_PARTICLE_PATTERN = "|".join(
    sorted((re.escape(p) for p in KOREAN_PARTICLES), key=len, reverse=True)
)


def setup_logging() -> None:
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    try:
        handlers.append(logging.FileHandler(LOG_PATH))
    except OSError:
        pass

    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
    )


def _preview_message(text: str, limit: int = 200) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _compile_keyword_pattern(keyword: str) -> re.Pattern:
    escaped = re.escape(keyword)
    if re.search(r"[가-힣]", keyword):
        # Allow up to 3 stacked particles while keeping strict word boundaries.
        return re.compile(
            rf"(?<!\w){escaped}(?:{_KOREAN_PARTICLE_PATTERN}){{0,3}}(?!\w)",
            re.IGNORECASE,
        )
    return re.compile(rf"\b{escaped}\b", re.IGNORECASE)


def init_db() -> None:
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                keyword TEXT NOT NULL COLLATE NOCASE,
                channel_id TEXT NOT NULL,
                guild_id INTEGER NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_keywords_unique
            ON keywords (user_id, keyword, channel_id, guild_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_keywords_guild
            ON keywords (guild_id)
            """
        )
        conn.commit()
    finally:
        conn.close()


def _connect() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, timeout=10)


def add_keyword(user_id: int, keyword: str, channel_id: str, guild_id: int) -> bool:
    conn = _connect()
    try:
        cur = conn.execute(
            "INSERT OR IGNORE INTO keywords (user_id, keyword, channel_id, guild_id) VALUES (?, ?, ?, ?)",
            (user_id, keyword, channel_id, guild_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def keyword_exists(user_id: int, keyword: str, channel_id: str, guild_id: int) -> bool:
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT 1 FROM keywords WHERE user_id = ? AND keyword = ? AND channel_id = ? AND guild_id = ? LIMIT 1",
            (user_id, keyword, channel_id, guild_id),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def count_keywords(user_id: int, guild_id: int) -> int:
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT COUNT(*) FROM keywords WHERE user_id = ? AND guild_id = ?",
            (user_id, guild_id),
        )
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def list_keywords(user_id: int, guild_id: int) -> List[Tuple[str, str]]:
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT keyword, channel_id FROM keywords WHERE user_id = ? AND guild_id = ? ORDER BY keyword, channel_id",
            (user_id, guild_id),
        )
        return [(row[0], row[1]) for row in cur.fetchall()]
    finally:
        conn.close()


def remove_keyword(user_id: int, guild_id: int, keyword: str) -> int:
    conn = _connect()
    try:
        cur = conn.execute(
            "DELETE FROM keywords WHERE user_id = ? AND guild_id = ? AND keyword = ?",
            (user_id, guild_id, keyword),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def fetch_keywords_for_guild(guild_id: int) -> List[Tuple[int, str, str]]:
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT user_id, keyword, channel_id FROM keywords WHERE guild_id = ?",
            (guild_id,),
        )
        return [(row[0], row[1], row[2]) for row in cur.fetchall()]
    finally:
        conn.close()


@bot.event
async def on_ready() -> None:
    setup_logging()
    init_db()
    guild_ids = [gid.strip() for gid in GUILD_IDS_RAW.split(",") if gid.strip()]
    if guild_ids:
        for guild_id in guild_ids:
            try:
                guild = discord.Object(id=int(guild_id))
            except ValueError:
                logger.warning("Invalid guild id for sync: %s", guild_id)
                continue
            tree.copy_global_to(guild=guild)
            await tree.sync(guild=guild)
            logger.info("Synced commands to guild=%s", guild_id)
    await tree.sync()
    logger.info("Logged in as %s (ID: %s)", bot.user, bot.user.id)


async def channel_autocomplete(
    interaction: discord.Interaction, current: str
) -> List[app_commands.Choice[str]]:
    if interaction.guild is None:
        return []

    query = current.lstrip("#").lower()
    channels: List[discord.TextChannel] = []
    for channel in interaction.guild.text_channels:
        if not channel.permissions_for(interaction.user).view_channel:
            continue
        if query and query not in channel.name.lower():
            continue
        channels.append(channel)

    channels.sort(key=lambda ch: ch.position)
    return [
        app_commands.Choice(name=f"#{channel.name}", value=str(channel.id))
        for channel in channels[:25]
    ]


@tree.command(name="add-keyword-channel", description="특정 채널에 키워드를 추가합니다")
@app_commands.describe(keyword="추적할 키워드", channel_id="키워드를 추적할 채널")
@app_commands.autocomplete(channel_id=channel_autocomplete)
async def add_keyword_channel(interaction: discord.Interaction, keyword: str, channel_id: str) -> None:
    if interaction.guild is None:
        await interaction.response.send_message("이 명령어는 서버에서만 사용할 수 있습니다.", ephemeral=True)
        return

    try:
        channel = interaction.guild.get_channel(int(channel_id))
    except (TypeError, ValueError):
        channel = None

    if channel is None or not isinstance(channel, discord.TextChannel):
        await interaction.response.send_message("텍스트 채널을 선택해 주세요.", ephemeral=True)
        return

    if channel.guild.id != interaction.guild.id:
        await interaction.response.send_message("이 서버의 채널을 선택해 주세요.", ephemeral=True)
        return

    if not channel.permissions_for(interaction.user).view_channel:
        await interaction.response.send_message("해당 채널에 접근 권한이 없습니다.", ephemeral=True)
        return

    keyword = keyword.strip()
    if not keyword:
        await interaction.response.send_message("키워드는 비워둘 수 없습니다.", ephemeral=True)
        return

    if keyword_exists(interaction.user.id, keyword, str(channel.id), interaction.guild.id):
        await interaction.response.send_message(
            f"`{keyword}` 키워드는 이미 {channel.mention}에서 추적 중입니다.",
            ephemeral=True,
        )
        logger.info(
            "Keyword already tracked (channel): user=%s guild=%s channel=%s keyword=%s",
            interaction.user.id,
            interaction.guild.id,
            channel.id,
            keyword,
        )
        return

    if count_keywords(interaction.user.id, interaction.guild.id) >= 10:
        await interaction.response.send_message(
            "키워드 한도에 도달했습니다(서버당 최대 10개). 새로 추가하려면 기존 키워드를 삭제해 주세요.",
            ephemeral=True,
        )
        logger.info(
            "Keyword limit reached: user=%s guild=%s",
            interaction.user.id,
            interaction.guild.id,
        )
        return

    added = add_keyword(interaction.user.id, keyword, str(channel.id), interaction.guild.id)
    if added:
        await interaction.response.send_message(
            f"{channel.mention}에 `{keyword}` 키워드를 추가했습니다.",
            ephemeral=True,
        )
        logger.info(
            "Added keyword channel: user=%s guild=%s channel=%s keyword=%s",
            interaction.user.id,
            interaction.guild.id,
            channel.id,
            keyword,
        )
    else:
        await interaction.response.send_message(
            f"`{keyword}` 키워드는 이미 {channel.mention}에서 추적 중입니다.",
            ephemeral=True,
        )
        logger.info(
            "Keyword already tracked (channel): user=%s guild=%s channel=%s keyword=%s",
            interaction.user.id,
            interaction.guild.id,
            channel.id,
            keyword,
        )


@tree.command(name="add-keyword-server", description="이 서버의 접근 가능한 모든 채널에 키워드를 추가합니다")
@app_commands.describe(keyword="서버 전체에서 추적할 키워드")
async def add_keyword_server(interaction: discord.Interaction, keyword: str) -> None:
    if interaction.guild is None:
        await interaction.response.send_message("이 명령어는 서버에서만 사용할 수 있습니다.", ephemeral=True)
        return

    keyword = keyword.strip()
    if not keyword:
        await interaction.response.send_message("키워드는 비워둘 수 없습니다.", ephemeral=True)
        return

    if keyword_exists(interaction.user.id, keyword, "GLOBAL", interaction.guild.id):
        await interaction.response.send_message(
            f"`{keyword}` 키워드는 이미 서버 전체에서 추적 중입니다.",
            ephemeral=True,
        )
        logger.info(
            "Keyword already tracked (server): user=%s guild=%s keyword=%s",
            interaction.user.id,
            interaction.guild.id,
            keyword,
        )
        return

    if count_keywords(interaction.user.id, interaction.guild.id) >= 10:
        await interaction.response.send_message(
            "키워드 한도에 도달했습니다(서버당 최대 10개). 새로 추가하려면 기존 키워드를 삭제해 주세요.",
            ephemeral=True,
        )
        logger.info(
            "Keyword limit reached: user=%s guild=%s",
            interaction.user.id,
            interaction.guild.id,
        )
        return

    added = add_keyword(interaction.user.id, keyword, "GLOBAL", interaction.guild.id)
    if added:
        await interaction.response.send_message(
            f"이 서버의 접근 가능한 모든 채널에 `{keyword}` 키워드를 추가했습니다.",
            ephemeral=True,
        )
        logger.info(
            "Added keyword server: user=%s guild=%s keyword=%s",
            interaction.user.id,
            interaction.guild.id,
            keyword,
        )
    else:
        await interaction.response.send_message(
            f"`{keyword}` 키워드는 이미 서버 전체에서 추적 중입니다.",
            ephemeral=True,
        )
        logger.info(
            "Keyword already tracked (server): user=%s guild=%s keyword=%s",
            interaction.user.id,
            interaction.guild.id,
            keyword,
        )


@tree.command(name="list-keywords", description="이 서버에서 추적 중인 키워드를 확인합니다")
async def list_keywords_cmd(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message("이 명령어는 서버에서만 사용할 수 있습니다.", ephemeral=True)
        return

    rows = list_keywords(interaction.user.id, interaction.guild.id)
    if not rows:
        await interaction.response.send_message("이 서버에서 추적 중인 키워드가 없습니다.", ephemeral=True)
        return

    lines: List[str] = []
    for keyword, channel_id in rows:
        if channel_id == "GLOBAL":
            location = "전체"
        else:
            channel = interaction.guild.get_channel(int(channel_id))
            if channel is None:
                location = f"#알 수 없는 채널 ({channel_id})"
            else:
                location = f"#{channel.name}"
        lines.append(f"`{keyword}` → {location}")

    message = "내 키워드 목록:\n" + "\n".join(lines)
    await interaction.response.send_message(message, ephemeral=True)
    logger.info(
        "Listed keywords: user=%s guild=%s count=%s",
        interaction.user.id,
        interaction.guild.id,
        len(rows),
    )


@tree.command(name="remove-keyword", description="이 서버에서 키워드를 삭제합니다")
@app_commands.describe(keyword="삭제할 키워드")
async def remove_keyword_cmd(interaction: discord.Interaction, keyword: str) -> None:
    if interaction.guild is None:
        await interaction.response.send_message("이 명령어는 서버에서만 사용할 수 있습니다.", ephemeral=True)
        return

    keyword = keyword.strip()
    if not keyword:
        await interaction.response.send_message("키워드는 비워둘 수 없습니다.", ephemeral=True)
        return

    removed = remove_keyword(interaction.user.id, interaction.guild.id, keyword)
    if removed > 0:
        await interaction.response.send_message(
            f"`{keyword}` 키워드를 이 서버에서 삭제했습니다 ({removed}개).",
            ephemeral=True,
        )
        logger.info(
            "Removed keyword: user=%s guild=%s keyword=%s removed=%s",
            interaction.user.id,
            interaction.guild.id,
            keyword,
            removed,
        )
    else:
        await interaction.response.send_message(
            f"이 서버에서 `{keyword}` 키워드를 찾을 수 없습니다.",
            ephemeral=True,
        )
        logger.info(
            "Remove keyword miss: user=%s guild=%s keyword=%s",
            interaction.user.id,
            interaction.guild.id,
            keyword,
        )


async def _get_member(guild: discord.Guild, user_id: int) -> Optional[discord.Member]:
    member = guild.get_member(user_id)
    if member is not None:
        return member
    try:
        return await guild.fetch_member(user_id)
    except discord.NotFound:
        return None
    except discord.Forbidden:
        return None


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.guild is None:
        return
    if not message.content:
        return
    # Only skip this bot's own messages; include other bot messages for keyword tracking.
    if message.author.bot and bot.user is not None and message.author.id == bot.user.id:
        return

    logger.info(
        "Message seen: guild=%s channel=%s author=%s content=%s",
        message.guild.id,
        message.channel.id,
        message.author.id,
        _preview_message(message.content),
    )
    logger.debug("Processing message from %s: %s", message.author, _preview_message(message.content, 500))

    rows = fetch_keywords_for_guild(message.guild.id)
    if not rows:
        logger.info("No keywords for guild=%s", message.guild.id)
        logger.debug("No keywords registered for guild %s", message.guild.name)
        return

    patterns: Dict[str, re.Pattern] = {}
    matched: Dict[int, Set[str]] = {}

    for user_id, keyword, channel_id in rows:
        if message.author.id == user_id:
            logger.info(
                "Skip self match: user=%s keyword=%s",
                user_id,
                keyword,
            )
            continue
        if channel_id != "GLOBAL" and str(message.channel.id) != channel_id:
            continue

        pattern = patterns.get(keyword)
        if pattern is None:
            pattern = _compile_keyword_pattern(keyword)
            patterns[keyword] = pattern

        if not pattern.search(message.content):
            logger.debug(
                "Keyword '%s' found in DB but not matched in text.",
                keyword,
            )
            continue

        user_matches = matched.setdefault(user_id, set())
        user_matches.add(keyword)

    if not matched:
        logger.info("No keyword matches: guild=%s channel=%s", message.guild.id, message.channel.id)
        return

    member_cache: Dict[int, Optional[discord.Member]] = {}
    user_cache: Dict[int, Optional[discord.User]] = {}

    for user_id, keywords in matched.items():
        member = member_cache.get(user_id)
        if member is None:
            member = await _get_member(message.guild, user_id)
            member_cache[user_id] = member
        if member is None:
            logger.info("Member not found: user=%s guild=%s", user_id, message.guild.id)
            continue

        if not message.channel.permissions_for(member).view_channel:
            logger.info(
                "Permission denied for user=%s channel=%s",
                user_id,
                message.channel.id,
            )
            continue

        user = user_cache.get(user_id)
        if user is None:
            user = bot.get_user(user_id)
            if user is None:
                try:
                    user = await bot.fetch_user(user_id)
                except discord.NotFound:
                    user = None
                except discord.Forbidden:
                    user = None
            user_cache[user_id] = user

        if user is None:
            logger.info("User fetch failed: user=%s", user_id)
            continue

        for keyword in keywords:
            dm_text = (
                "## :mega: 키워드가 감지되었습니다\n"
                f"채널: #{message.channel.name} ({message.jump_url})\n"
                f"유저: {message.author.display_name}\n"
                f"메시지: {message.content}\n"
                "\n"
            )
            try:
                await user.send(dm_text)
                logger.info(
                    "DM sent: user=%s guild=%s channel=%s keyword=%s",
                    user_id,
                    message.guild.id,
                    message.channel.id,
                    keyword,
                )
            except discord.Forbidden:
                logger.info("DM forbidden: user=%s", user_id)
                break

    if hasattr(bot, "process_commands"):
        await bot.process_commands(message)


if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN environment variable is required")
    bot.run(token)
