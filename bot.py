import discord
from discord import app_commands
from discord.ext = commands
import json
import os
from dotenv import load_dotenv
from aiohttp import web
import asyncio
import aiohttp
from datetime import datetime, timedelta
import secrets
import time
from typing import Dict, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

linked_accounts_file = "linked_accounts.json"
CONFIG_FILE = "config.json"
ADMIN_ROLE_NAME = "Admin"
SUPPORTER_ROLE_NAME = "Supporter"
OWNER_ID = 1322627642746339432
ROBLOX_API_URL = "https://inventory.roblox.com/v1/users/{user_id}/items/GamePass/{gamepass_id}"
REDEEM_URL = "/redeem"
DOWNLOAD_URL = "/download"
ZIP_FILE_PATH = "secure_downloads/app.zip"

# Customizable download base URL
DOWNLOAD_BASE_URL = os.getenv("DOWNLOAD_BASE_URL", f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME', 'backend-2-0-9uod.onrender.com')}")

# Rate limiting and caching
roblox_cache: Dict[str, Dict] = {}
cache_expiry = 300
last_request_time = 0
min_request_interval = 1.0

# In-memory storage for pending codes
pending_codes: Dict[str, Dict] = {}

# ------------------- Load Config & Accounts -------------------

try:
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)
except FileNotFoundError:
    config = {"gamepass_roles": []}

try:
    with open(linked_accounts_file, "r") as f:
        temp_accounts = json.load(f)
        if not isinstance(temp_accounts, dict) or ("discord_to_roblox" not in temp_accounts and "roblox_to_discord" not in temp_accounts):
            discord_to_roblox = {}
            roblox_to_discord = {}
            for discord_id, roblox_id in temp_accounts.items():
                discord_to_roblox[discord_id] = roblox_id
                roblox_to_discord[str(roblox_id)] = discord_id
            linked_accounts = {
                "discord_to_roblox": discord_to_roblox,
                "roblox_to_discord": roblox_to_discord,
                "force_linked_users": [],
                "generated_codes": {},
                "linked_devices": {}
            }
        else:
            linked_accounts = temp_accounts
            if "force_linked_users" not in linked_accounts:
                linked_accounts["force_linked_users"] = []
            if "generated_codes" not in linked_accounts:
                linked_accounts["generated_codes"] = {}
            if "linked_devices" not in linked_accounts:
                linked_accounts["linked_devices"] = {}
except FileNotFoundError:
    linked_accounts = {
        "discord_to_roblox": {},
        "roblox_to_discord": {},
        "force_linked_users": [],
        "generated_codes": {},
        "linked_devices": {}
    }

def save_linked_accounts():
    try:
        with open(linked_accounts_file, "w") as f:
            json.dump(linked_accounts, f, indent=2)
        logger.info("Saved linked_accounts.json")
    except Exception as e:
        logger.error(f"Failed to save linked_accounts.json: {e}")

def is_admin(interaction: discord.Interaction) -> bool:
    role = discord.utils.get(interaction.guild.roles, name=ADMIN_ROLE_NAME)
    if role is None:
        return False
    return (role in interaction.user.roles) or (interaction.user.id == OWNER_ID)

def has_supporter_role(member: discord.Member) -> bool:
    role = discord.utils.get(member.guild.roles, name=SUPPORTER_ROLE_NAME)
    if role is None:
        return False
    return role in member.roles

# ------------------- Rate Limited API Calls -------------------

async def rate_limited_request():
    global last_request_time
    current_time = time.time()
    elapsed = current_time - last_request_time
    if elapsed < min_request_interval:
        await asyncio.sleep(min_request_interval - elapsed)
    last_request_time = time.time()

async def get_roblox_user_id(username: str) -> Optional[int]:
    cache_key = f"user_{username}"
    if cache_key in roblox_cache:
        cached_data = roblox_cache[cache_key]
        if time.time() - cached_data["timestamp"] < cache_expiry:
            return cached_data["data"]
    
    await rate_limited_request()
    url = "https://users.roblox.com/v1/usernames/users"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json={"usernames": [username]}, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    user_data = await response.json()
                    if user_data["data"]:
                        user_id = user_data["data"][0]["id"]
                        roblox_cache[cache_key] = {
                            "data": user_id,
                            "timestamp": time.time()
                        }
                        return user_id
                elif response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    await asyncio.sleep(retry_after)
                    return await get_roblox_user_id(username)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        pass
    
    return None

async def has_gamepass(user_id: int, gamepass_id: int) -> bool:
    cache_key = f"gamepass_{user_id}_{gamepass_id}"
    if cache_key in roblox_cache:
        cached_data = roblox_cache[cache_key]
        if time.time() - cached_data["timestamp"] < cache_expiry:
            return cached_data["data"]
    
    await rate_limited_request()
    url = ROBLOX_API_URL.format(user_id=user_id, gamepass_id=gamepass_id)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    gamepasses = await response.json()
                    has_pass = bool(gamepasses.get("data", []))
                    roblox_cache[cache_key] = {
                        "data": has_pass,
                        "timestamp": time.time()
                    }
                    return has_pass
                elif response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    await asyncio.sleep(retry_after)
                    return await has_gamepass(user_id, gamepass_id)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        pass
    
    return False

# ------------------- Discord Bot Commands -------------------

@bot.tree.command(name="link-roblox", description="Link your Roblox account to your Discord account.")
async def link_roblox(interaction: discord.Interaction, username: str):
    embed = discord.Embed(color=discord.Color.blue())
    user_id = await get_roblox_user_id(username)
    discord_id = str(interaction.user.id)

    if not user_id:
        embed.title = "❌ User Not Found"
        embed.description = f"Could not find a Roblox user with the username: `{username}`"
        embed.color = discord.Color.red()s
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return

    roblox_id_str = str(user_id)

    if discord_id in linked_accounts["discord_to_roblox"]:
        embed.title = "❌ Already Linked"
        embed.description = "Your Discord account is already linked to a Roblox account."
        embed.color = discord.Color.red()
    elif roblox_id_str in linked_accounts["roblox_to_discord"]:
        embed.title = "❌ Already Linked"
        embed.description = "This Roblox account is already linked to another Discord user."
        embed.color = discord.Color.red()
    else:
        linked_accounts["discord_to_roblox"][discord_id] = user_id
        linked_accounts["roblox_to_discord"][roblox_id_str] = discord_id
        save_linked_accounts()
        embed.title = "✅ Account Linked"
        embed.description = f"Successfully linked to Roblox account: `{username}`"
        embed.color = discord.Color.green()

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="unlink-roblox", description="Unlink your Roblox account from your Discord account.")
async def unlink_roblox(interaction: discord.Interaction):
    discord_id = str(interaction.user.id)

    if discord_id in linked_accounts.get("force_linked_users", []):
        embed = discord.Embed(title="❌ Cannot Unlink", description="This account was force-linked by an admin and cannot be unlinked.", color=discord.Color.red())
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return

    if discord_id in linked_accounts["discord_to_roblox"]:
        await remove_gamepass_roles(interaction.user)
        roblox_id = str(linked_accounts["discord_to_roblox"][discord_id])
        del linked_accounts["discord_to_roblox"][discord_id]
        del linked_accounts["roblox_to_discord"][roblox_id]
        save_linked_accounts()

        embed = discord.Embed(title="✅ Account Unlinked", color=discord.Color.green())
        await interaction.response.send_message(embed=embed, ephemeral=True)
    else:
        embed = discord.Embed(title="❌ No Account Linked", description="You don't have any Roblox account linked.", color=discord.Color.red())
        await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="claim-roles", description="Claim your roles based on your Roblox gamepasses.")
async def claim_roles(interaction: discord.Interaction):
    embed = discord.Embed(color=discord.Color.blue())
    discord_id = str(interaction.user.id)

    if discord_id not in linked_accounts["discord_to_roblox"]:
        embed.title = "❌ Not Linked"
        embed.description = "You need to link your Roblox account first using `/link-roblox`!"
        embed.color = discord.Color.red()
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return

    roblox_id = linked_accounts["discord_to_roblox"][discord_id]

    added_roles = []

    for mapping in config["gamepass_roles"]:
        gamepass_id = mapping["gamepass_id"]
        role_id = mapping["role_id"]
        description = mapping["description"]
        role = interaction.guild.get_role(role_id)
        if role is None:
            continue
        if role in interaction.user.roles:
            continue
        if await has_gamepass(roblox_id, gamepass_id):
            await interaction.user.add_roles(role)
            added_roles.append(description)

    embed.title = "🎮 Role Claim"
    if added_roles:
        embed.description = "✅ Successfully claimed your roles!"
        embed.color = discord.Color.green()
    else:
        embed.description = "ℹ️ You have no new roles to claim."
        embed.color = discord.Color.blue()

    await interaction.response.send_message(embed=embed, ephemeral=True)


# ------------------- Admin Commands -------------------

@bot.tree.command(name="list-linked", description="(Admin) List all linked accounts.")
@app_commands.checks.has_role(ADMIN_ROLE_NAME)
async def list_linked(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message("❌ You do not have permission.", ephemeral=True)
        return

    description = ""
    for discord_id, roblox_id in linked_accounts["discord_to_roblox"].items():
        description += f"<@{discord_id}> ➜ `{roblox_id}`\n"

    embed = discord.Embed(title="🔗 Linked Accounts", description=description or "None found.", color=discord.Color.blue())
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="force-link", description="(Admin) Force link a user to a Roblox username.")
@app_commands.checks.has_role(ADMIN_ROLE_NAME)
async def force_link(interaction: discord.Interaction, discord_user: discord.User, roblox_username: str):
    if not is_admin(interaction):
        await interaction.response.send_message("❌ You do not have permission.", ephemeral=True)
        return

    user_id = await get_roblox_user_id(roblox_username)
    if not user_id:
        await interaction.response.send_message("❌ Roblox user not found.", ephemeral=True)
        return

    discord_id = str(discord_user.id)
    roblox_id = str(user_id)

    linked_accounts["discord_to_roblox"][discord_id] = user_id
    linked_accounts["roblox_to_discord"][roblox_id] = discord_id
    if discord_id not in linked_accounts["force_linked_users"]:
        linked_accounts["force_linked_users"].append(discord_id)

    save_linked_accounts()
    await interaction.response.send_message(f"✅ Force linked {discord_user.mention} to `{roblox_username}`", ephemeral=True)


@bot.tree.command(name="admin-unlink", description="(Admin) Unlink a user manually.")
@app_commands.checks.has_role(ADMIN_ROLE_NAME)
async def admin_unlink(interaction: discord.Interaction, discord_user: discord.User):
    if not is_admin(interaction):
        await interaction.response.send_message("❌ You do not have permission.", ephemeral=True)
        return

    discord_id = str(discord_user.id)
    if discord_id in linked_accounts["discord_to_roblox"]:
        roblox_id = str(linked_accounts["discord_to_roblox"][discord_id])
        del linked_accounts["discord_to_roblox"][discord_id]
        del linked_accounts["roblox_to_discord"][roblox_id]
        if discord_id in linked_accounts["force_linked_users"]:
            linked_accounts["force_linked_users"].remove(discord_id)
        save_linked_accounts()
        await interaction.response.send_message(f"✅ Unlinked {discord_user.mention}", ephemeral=True)
    else:
        await interaction.response.send_message("❌ User is not linked.", ephemeral=True)


# ------------------- Helper Functions -------------------

async def remove_gamepass_roles(member: discord.Member):
    role_ids = [mapping["role_id"] for mapping in config["gamepass_roles"]]
    roles_to_remove = [role for role in member.roles if role.id in role_ids]
    if roles_to_remove:
        await member.remove_roles(*roles_to_remove)


# ------------------- Events -------------------

@bot.event
async def on_ready():
    await bot.tree.sync()
    print(f"✅ Logged in as {bot.user}")


# ------------------- Minimal Webserver -------------------

async def handle(request):
    return web.Response(text="Bot is running")

async def run_webserver():
    app = web.Application()
    app.router.add_get('/', handle)
    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🌐 Web server running on port {port}")


# ------------------- Run Bot & Webserver -------------------

async def main():
    await run_webserver()
    await bot.start(os.getenv("DISCORD_TOKEN"))


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(main())
