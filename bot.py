import discord
from discord import app_commands
from discord.ext import commands
import json
import os
from dotenv import load_dotenv
from aiohttp import web
import aiohttp
import asyncio
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

# Configurable download base URL (set in .env or fallback to Render)
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
        logger.warning(f"Admin role '{ADMIN_ROLE_NAME}' not found in guild {interaction.guild.id}")
        return False
    return (role in interaction.user.roles) or (interaction.user.id == OWNER_ID)

def has_supporter_role(member: discord.Member) -> bool:
    role = discord.utils.get(member.guild.roles, name=SUPPORTER_ROLE_NAME)
    if role is None:
        logger.warning(f"Supporter role '{SUPPORTER_ROLE_NAME}' not found in guild {member.guild.id}")
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
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error(f"Error in get_roblox_user_id: {e}")
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
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error(f"Error in has_gamepass: {e}")
    return False

# ------------------- Code Verification -------------------

async def verify_code(code: str, discord_id: str) -> Optional[str]:
    try:
        if code not in pending_codes:
            logger.warning(f"Code {code} not found in pending_codes")
            return None
        code_data = pending_codes[code]
        if code_data["discord_id"] != discord_id or time.time() > code_data["expiry"]:
            logger.warning(f"Code {code} invalid or expired for discord_id {discord_id}")
            del pending_codes[code]
            return None
        download_token = code_data["download_token"]
        
        # Store in linked_accounts.json after verification
        linked_accounts["generated_codes"][code] = {
            "discord_id": discord_id,
            "expiry": code_data["expiry"],
            "download_token": download_token
        }
        linked_accounts["linked_devices"][discord_id] = {"linked": True}
        save_linked_accounts()
        del pending_codes[code]
        logger.info(f"Code {code} verified and stored for discord_id {discord_id}")
        return download_token
    except Exception as e:
        logger.error(f"Error in verify_code: {e}")
        return None

async def invalidate_user_codes(discord_id: str):
    try:
        # Remove from pending_codes
        codes_to_remove = [code for code, data in pending_codes.items() if data["discord_id"] == discord_id]
        for code in codes_to_remove:
            del pending_codes[code]
        # Remove from linked_accounts
        codes_to_remove = [code for code, data in linked_accounts["generated_codes"].items() if data["discord_id"] == discord_id]
        for code in codes_to_remove:
            del linked_accounts["generated_codes"][code]
        if discord_id in linked_accounts["linked_devices"]:
            del linked_accounts["linked_devices"][discord_id]
        save_linked_accounts()
        logger.info(f"Invalidated codes for discord_id {discord_id}")
    except Exception as e:
        logger.error(f"Error in invalidate_user_codes: {e}")

# ------------------- Discord Bot Commands -------------------

@bot.tree.command(name="link-account", description="Link your account to download the application (Supporter role required).")
async def link_account(interaction: discord.Interaction):
    try:
        discord_id = str(interaction.user.id)
        embed = discord.Embed(color=discord.Color.blue())

        # Check if user is in the guild and has Supporter role
        if not has_supporter_role(interaction.user):
            embed.title = "❌ Permission Denied"
            embed.description = f"You need the '{SUPPORTER_ROLE_NAME}' role in this server to use this command."
            embed.color = discord.Color.red()
            await interaction.response.send_message(embed=embed, ephemeral=True)
            logger.info(f"link-account denied for discord_id {discord_id}: no Supporter role")
            return

        if discord_id in linked_accounts["linked_devices"]:
            embed.title = "❌ Already Linked"
            embed.description = "Your account is already linked to a device. Use `/change-account` to link a new device."
            embed.color = discord.Color.red()
            await interaction.response.send_message(embed=embed, ephemeral=True)
            logger.info(f"link-account denied for discord_id {discord_id}: already linked")
            return

        embed.title = "✅ Ready to Generate Code"
        embed.description = "Run the terminal application to generate a verification code. Then use `/verify-code <code>` to receive the download link."
        embed.color = discord.Color.green()
        await interaction.response.send_message(embed=embed, ephemeral=True)
        logger.info(f"link-account called by discord_id {discord_id} in guild {interaction.guild.id}")
    except Exception as e:
        logger.error(f"Error in link_account: {e}")
        embed = discord.Embed(title="❌ Error", description="An error occurred. Please try again later.", color=discord.Color.red())
        await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="verify-code", description="Verify your code to receive the download link (Supporter role required).")
async def verify_code(interaction: discord.Interaction, code: str):
    try:
        discord_id = str(interaction.user.id)
        embed = discord.Embed(color=discord.Color.blue())

        # Check if user is in the guild and has Supporter role
        if not has_supporter_role(interaction.user):
            embed.title = "❌ Permission Denied"
            embed.description = f"You need the '{SUPPORTER_ROLE_NAME}' role in this server to use this command."
            embed.color = discord.Color.red()
            await interaction.response.send_message(embed=embed, ephemeral=True)
            logger.info(f"verify-code denied for discord_id {discord_id}: no Supporter role")
            return

        # Verify code
        download_token = await verify_code(code, discord_id)
        if not download_token:
            embed.title = "❌ Invalid or Expired Code"
            embed.description = "The code is invalid or has expired. Run the terminal app to generate a new code and try again."
            embed.color = discord.Color.red()
            await interaction.response.send_message(embed=embed, ephemeral=True)
            logger.info(f"verify-code failed for discord_id {discord_id}: invalid or expired code {code}")
            return

        # Generate server-sided download link
        download_link = f"{DOWNLOAD_BASE_URL}{DOWNLOAD_URL}?token={download_token}"
        embed.title = "✅ Code Verified"
        embed.description = (
            f"Download your file here: {download_link}\n"
            f"This link is valid for 5 minutes. Paste it into the terminal app to download and run the application."
        )
        embed.color = discord.Color.green()
        await interaction.response.send_message(embed=embed, ephemeral=True)
        logger.info(f"verify-code successful for discord_id {discord_id}, code {code} in guild {interaction.guild.id}")
    except Exception as e:
        logger.error(f"Error in verify_code command: {e}")
        embed = discord.Embed(title="❌ Error", description="An error occurred. Please try again later.", color=discord.Color.red())
        await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="change-account", description="Unlink your current device and link a new one (Supporter role required).")
async def change_account(interaction: discord.Interaction):
    try:
        discord_id = str(interaction.user.id)
        embed = discord.Embed(color=discord.Color.blue())

        # Check if user is in the guild and has Supporter role
        if not has_supporter_role(interaction.user):
            embed.title = "❌ Permission Denied"
            embed.description = f"You need the '{SUPPORTER_ROLE_NAME}' role in this server to use this command."
            embed.color = discord.Color.red()
            await interaction.response.send_message(embed=embed, ephemeral=True)
            logger.info(f"change-account denied for discord_id {discord_id}: no Supporter role")
            return

        if discord_id not in linked_accounts["linked_devices"]:
            embed.title = "❌ No Device Linked"
            embed.description = "You haven't linked a device yet. Use `/link-account` to start the process."
            embed.color = discord.Color.red()
            await interaction.response.send_message(embed=embed, ephemeral=True)
            logger.info(f"change-account denied for discord_id {discord_id}: no device linked")
            return

        await invalidate_user_codes(discord_id)
        embed.title = "✅ Device Unlinked"
        embed.description = "Your previous device link has been removed. Run the terminal app to generate a new verification code and use `/verify-code`."
        embed.color = discord.Color.green()
        await interaction.response.send_message(embed=embed, ephemeral=True)
        logger.info(f"change-account successful for discord_id {discord_id} in guild {interaction.guild.id}")
    except Exception as e:
        logger.error(f"Error in change_account: {e}")
        embed = discord.Embed(title="❌ Error", description="An error occurred. Please try again later.", color=discord.Color.red())
        await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="link-roblox", description="Link your Roblox account to your Discord account.")
async def link_roblox(interaction: discord.Interaction, username: str):
    try:
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
        logger.info(f"link-roblox called by discord_id {discord_id}, username {username}")
    except Exception as e:
        logger.error(f"Error in link_roblox: {e}")
        embed = discord.Embed(title="❌ Error", description="An error occurred. Please try again later.", color=discord.Color.red())
        await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="unlink-roblox", description="Unlink your Roblox account from your Discord account.")
async def unlink_roblox(interaction: discord.Interaction):
    try:
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
            logger.info(f"unlink-roblox successful for discord_id {discord_id}")
        else:
            embed = discord.Embed(title="❌ No Account Linked", description="You don't have any Roblox account linked.", color=discord.Color.red())
            await interaction.response.send_message(embed=embed, ephemeral=True)
    except Exception as e:
        logger.error(f"Error in unlink_roblox: {e}")
        embed = discord.Embed(title="❌ Error", description="An error occurred. Please try again later.", color=discord.Color.red())
        await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="claim-roles", description="Claim your roles based on your Roblox gamepasses.")
async def claim_roles(interaction: discord.Interaction):
    try:
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
        logger.info(f"claim-roles called by discord_id {discord_id}")
    except Exception as e:
        logger.error(f"Error in claim_roles: {e}")
        embed = discord.Embed(title="❌ Error", description="An error occurred. Please try again later.", color=discord.Color.red())
        await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="list-linked", description="(Admin) List all linked accounts.")
@app_commands.checks.has_role(ADMIN_ROLE_NAME)
async def list_linked(interaction: discord.Interaction):
    try:
        if not is_admin(interaction):
            await interaction.response.send_message("❌ You do not have permission.", ephemeral=True)
            return

        description = ""
        for discord_id, roblox_id in linked_accounts["discord_to_roblox"].items():
            description += f"<@{discord_id}> ➜ `{roblox_id}`\n"

        embed = discord.Embed(title="🔗 Linked Accounts", description=description or "None found.", color=discord.Color.blue())
        await interaction.response.send_message(embed=embed, ephemeral=True)
        logger.info(f"list-linked called by discord_id {str(interaction.user.id)}")
    except Exception as e:
        logger.error(f"Error in list_linked: {e}")
        embed = discord.Embed(title="❌ Error", description="An error occurred. Please try again later.", color=discord.Color.red())
        await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="force-link", description="(Admin) Force link a user to a Roblox username.")
@app_commands.checks.has_role(ADMIN_ROLE_NAME)
async def force_link(interaction: discord.Interaction, discord_user: discord.User, roblox_username: str):
    try:
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
        logger.info(f"force-link called by discord_id {str(interaction.user.id)}, linked {discord_id} to {roblox_username}")
    except Exception as e:
        logger.error(f"Error in force_link: {e}")
        embed = discord.Embed(title="❌ Error", description="An error occurred. Please try again later.", color=discord.Color.red())
        await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="admin-unlink", description="(Admin) Unlink a user manually.")
@app_commands.checks.has_role(ADMIN_ROLE_NAME)
async def admin_unlink(interaction: discord.Interaction, discord_user: discord.User):
    try:
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
            logger.info(f"admin-unlink successful for discord_id {discord_id}")
        else:
            await interaction.response.send_message("❌ User is not linked.", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in admin_unlink: {e}")
        embed = discord.Embed(title="❌ Error", description="An error occurred. Please try again later.", color=discord.Color.red())
        await interaction.response.send_message(embed=embed, ephemeral=True)

# ------------------- Helper Functions -------------------

async def remove_gamepass_roles(member: discord.Member):
    try:
        role_ids = [mapping["role_id"] for mapping in config["gamepass_roles"]]
        roles_to_remove = [role for role in member.roles if role.id in role_ids]
        if roles_to_remove:
            await member.remove_roles(*roles_to_remove)
            logger.info(f"Removed gamepass roles from {member.id}")
    except Exception as e:
        logger.error(f"Error in remove_gamepass_roles: {e}")

# ------------------- Render Backend Webserver -------------------

async def handle_redeem(request):
    try:
        data = await request.json()
        code = data.get("code")
        discord_id = data.get("discord_id")
        if not code or not discord_id:
            logger.warning(f"Missing code or discord_id in /redeem: {data}")
            return web.json_response({"error": "Missing code or discord_id"}, status=400)

        download_token = secrets.token_urlsafe(16)
        expiry = time.time() + 300  # 5 minutes
        pending_codes[code] = {
            "discord_id": discord_id,
            "expiry": expiry,
            "download_token": download_token
        }
        logger.info(f"Stored pending code {code} for discord_id {discord_id}")
        return web.json_response({"message": "Code stored successfully"})
    except Exception as e:
        logger.error(f"Error in handle_redeem: {e}")
        return web.json_response({"error": "Server error"}, status=500)

async def handle_download(request):
    try:
        token = request.query.get("token")
        if not token:
            logger.warning("Missing token in /download")
            return web.json_response({"error": "Missing token"}, status=400)

        for code, data in list(linked_accounts["generated_codes"].items()):
            if data.get("download_token") == token and time.time() < data["expiry"]:
                if not os.path.exists(ZIP_FILE_PATH):
                    logger.error(f"Zip file not found at {ZIP_FILE_PATH}")
                    return web.json_response({"error": "File not found"}, status=404)
                logger.info(f"Serving zip file for token {token}")
                return web.FileResponse(ZIP_FILE_PATH, headers={
                    "Content-Disposition": "attachment; filename=app.zip"
                })
        
        logger.warning(f"Invalid or expired token in /download: {token}")
        return web.json_response({"error": "Invalid or expired token"}, status=401)
    except Exception as e:
        logger.error(f"Error in handle_download: {e}")
        return web.json_response({"error": "Server error"}, status=500)

async def run_webserver():
    try:
        app = web.Application()
        app.router.add_get('/', lambda r: web.Response(text="Bot is running"))
        app.router.add_post('/redeem', handle_redeem)
        app.router.add_get('/download', handle_download)
        port = int(os.environ.get("PORT", 8080))
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        logger.info(f"Web server running on port {port}")
    except Exception as e:
        logger.error(f"Error starting webserver: {e}")

# ------------------- Events -------------------

@bot.event
async def on_ready():
    try:
        await bot.tree.sync()
        logger.info(f"Logged in as {bot.user}")
    except Exception as e:
        logger.error(f"Error in on_ready: {e}")

# ------------------- Run Bot & Webserver -------------------

async def main():
    await run_webserver()
    await bot.start(os.getenv("DISCORD_TOKEN"))

if __name__ == "__main__":
    load_dotenv()
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Error in main: {e}")

