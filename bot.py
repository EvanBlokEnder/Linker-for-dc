import discord
from discord import app_commands
from discord.ext import commands
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

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

linked_accounts_file = "linked_accounts.json"
CONFIG_FILE = "config.json"
ADMIN_ROLE_NAME = "Admin"
SUPPORTER_ROLE_NAME = "Supporter"
OWNER_ID = 1322627642746339432
ROBLOX_API_URL = "https://inventory.roblox.com/v1/users/{user_id}/items/GamePass/{gamepass_id}"
DOWNLOAD_URL = "/download"  # Relative path for Render server
ZIP_FILE_PATH = "secure_downloads/app.zip"  # Secret folder on Render

# Rate limiting and caching
roblox_cache: Dict[str, Dict] = {}
cache_expiry = 300  # 5 minutes cache
last_request_time = 0
min_request_interval = 1.0

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
    with open(linked_accounts_file, "w") as f:
        json.dump(linked_accounts, f, indent=2)

def is_admin(interaction: discord.Interaction) -> bool:
    role = discord.utils.get(interaction.guild.roles, name=ADMIN_ROLE_NAME)
    if role is None:
        return False
    return (role in interaction.user.roles) or (interaction.user.id == OWNER_ID)

def has_supporter_role(member: discord.Member) -> bool:
    role = discord.utils.get(member.guild.roles, name=SUPPORTER_ROLE_NAME)
    return role in member.roles if role else False

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

# ------------------- Code Generation and Verification -------------------

async def generate_code(discord_id: str) -> tuple[str, str]:
    code = secrets.token_urlsafe(16)
    expiry = (datetime.utcnow() + timedelta(minutes=5)).timestamp()
    download_token = secrets.token_urlsafe(16)
    linked_accounts["generated_codes"][code] = {
        "discord_id": discord_id,
        "expiry": expiry,
        "download_token": download_token
    }
    save_linked_accounts()
    return code, download_token

async def verify_code(code: str, discord_id: str) -> Optional[str]:
    if code not in linked_accounts["generated_codes"]:
        return None
    code_data = linked_accounts["generated_codes"][code]
    if code_data["discord_id"] != discord_id or time.time() > code_data["expiry"]:
        del linked_accounts["generated_codes"][code]
        save_linked_accounts()
        return None
    download_token = code_data["download_token"]
    del linked_accounts["generated_codes"][code]
    save_linked_accounts()
    return download_token

async def invalidate_user_codes(discord_id: str):
    codes_to_remove = [code for code, data in linked_accounts["generated_codes"].items() if data["discord_id"] == discord_id]
    for code in codes_to_remove:
        del linked_accounts["generated_codes"][code]
    save_linked_accounts()

# ------------------- New Discord Bot Commands -------------------

@bot.tree.command(name="link-account", description="Link your account to download the application (Supporter role required).")
async def link_account(interaction: discord.Interaction):
    discord_id = str(interaction.user.id)
    embed = discord.Embed(color=discord.Color.blue())

    if not has_supporter_role(interaction.user):
        embed.title = "❌ Permission Denied"
        embed.description = f"You need the '{SUPPORTER_ROLE_NAME}' role to use this command."
        embed.color = discord.Color.red()
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return

    if discord_id in linked_accounts["linked_devices"]:
        embed.title = "❌ Already Linked"
        embed.description = "Your account is already linked to a device. Use `/change-account` to link a new device."
        embed.color = discord.Color.red()
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return

    code, _ = await generate_code(discord_id)
    linked_accounts["linked_devices"][discord_id] = {"linked": True}
    save_linked_accounts()

    try:
        await interaction.user.send(f"Your verification code is: `{code}`\nThis code expires in 5 minutes. Use `/verify-code <code>` to proceed.")
        embed.title = "✅ Code Generated"
        embed.description = "A verification code has been sent to your DMs. Please check and use `/verify-code` to verify."
        embed.color = discord.Color.green()
    except discord.Forbidden:
        embed.title = "❌ DM Error"
        embed.description = "I couldn't send you a DM. Please enable DMs from server members and try again."
        embed.color = discord.Color.red()
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="verify-code", description="Verify your code to receive the download link (Supporter role required).")
async def verify_code(interaction: discord.Interaction, code: str):
    discord_id = str(interaction.user.id)
    embed = discord.Embed(color=discord.Color.blue())

    if not has_supporter_role(interaction.user):
        embed.title = "❌ Permission Denied"
        embed.description = f"You need the '{SUPPORTER_ROLE_NAME}' role to use this command."
        embed.color = discord.Color.red()
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return

    download_token = await verify_code(code, discord_id)
    if not download_token:
        embed.title = "❌ Invalid or Expired Code"
        embed.description = "The code is invalid or has expired. Please use `/link-account` to generate a new code."
        embed.color = discord.Color.red()
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return

    # Get the base URL of the Render server dynamically
    base_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME', 'backend-2-0-9uod.onrender.com')}"
    download_link = f"{base_url}{DOWNLOAD_URL}?token={download_token}"
    embed.title = "✅ Code Verified"
    embed.description = (
        f"Download your file here: {download_link}\n"
        f"This link is valid for 5 minutes."
    )
    embed.color = discord.Color.green()
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="change-account", description="Unlink your current device and link a new one (Supporter role required).")
async def change_account(interaction: discord.Interaction):
    discord_id = str(interaction.user.id)
    embed = discord.Embed(color=discord.Color.blue())

    if not has_supporter_role(interaction.user):
        embed.title = "❌ Permission Denied"
        embed.description = f"You need the '{SUPPORTER_ROLE_NAME}' role to use this command."
        embed.color = discord.Color.red()
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return

    if discord_id not in linked_accounts["linked_devices"]:
        embed.title = "❌ No Device Linked"
        embed.description = "You haven't linked a device yet. Use `/link-account` to link one."
        embed.color = discord.Color.red()
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return

    await invalidate_user_codes(discord_id)
    del linked_accounts["linked_devices"][discord_id]
    code, _ = await generate_code(discord_id)
    linked_accounts["linked_devices"][discord_id] = {"linked": True}
    save_linked_accounts()

    try:
        await interaction.user.send(f"Your new verification code is: `{code}`\nThis code expires in 5 minutes. Use `/verify-code <code>` to proceed.")
        embed.title = "✅ Device Unlinked & New Code Generated"
        embed.description = "Your previous device link has been removed. A new verification code has been sent to your DMs."
        embed.color = discord.Color.green()
    except discord.Forbidden:
        embed.title = "❌ DM Error"
        embed.description = "I couldn't send you a DM. Please enable DMs from server members and try again."
        embed.color = discord.Color.red()
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ------------------- Existing Commands -------------------

@bot.tree.command(name="link-roblox", description="Link your Roblox account to your Discord account.")
async def link_roblox(interaction: discord.Interaction, username: str):
    embed = discord.Embed(color=discord.Color.blue())
    user_id = await get_roblox_user_id(username)
    discord_id = str(interaction.user.id)

    if not user_id:
        embed.title = "❌ User Not Found"
        embed.description = f"Could not find a Roblox user with the username: `{username}`"
        embed.color = discord.Color.red()
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

# ------------------- Render Backend Webserver -------------------

async def handle_redeem(request):
    data = await request.json()
    code = data.get("code")
    discord_id = data.get("discord_id")
    if not code or not discord_id:
        return web.json_response({"error": "Missing code or discord_id"}, status=400)

    download_token = await verify_code(code, discord_id)
    if not download_token:
        return web.json_response({"error": "Invalid or expired code"}, status=401)

    base_url = f"https://{request.host}"
    return web.json_response({
        "download_link": f"{base_url}{DOWNLOAD_URL}?token={download_token}"
    })

async def handle_download(request):
    token = request.query.get("token")
    if not token:
        return web.json_response({"error": "Missing token"}, status=400)

    for code, data in linked_accounts["generated_codes"].items():
        if data.get("download_token") == token and time.time() < data["expiry"]:
            if not os.path.exists(ZIP_FILE_PATH):
                return web.json_response({"error": "File not found"}, status=404)
            return web.FileResponse(ZIP_FILE_PATH, headers={
                "Content-Disposition": "attachment; filename=app.zip"
            })
    
    return web.json_response({"error": "Invalid or expired token"}, status=401)

async def run_webserver():
    app = web.Application()
    app.router.add_get('/', lambda r: web.Response(text="Bot is running"))
    app.router.add_post('/redeem', handle_redeem)
    app.router.add_get('/download', handle_download)
    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🌐 Web server running on port {port}")

# ------------------- Events -------------------

@bot.event
async def on_ready():
    await bot.tree.sync()
    print(f"✅ Logged in as {bot.user}")

# ------------------- Run Bot & Webserver -------------------

async def main():
    await run_webserver()
    await bot.start(os.getenv("DISCORD_TOKEN"))

if __name__ == "__main__":
    load_dotenv()
    asyncio.run(main())
