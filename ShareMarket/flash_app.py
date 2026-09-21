# https://rishabhjain777.pythonanywhere.com/
# Local : python -m flask --app flash_app run --port 8765 --no-reload
NEO_ENABLED = True
NEO_MOBILE_NUMBER = "+918747005100"
NEO_UCC = "XFQIZ"

FLASK_SECRET_KEY = "CHANGE_THIS_TO_A_LONG_RANDOM_SECRET"

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465
SMTP_USER = "rishabhjain777@gmail.com"
EMAIL_FROM = "trade_analysis@gmail.com"
EMAIL_RECIPIENT = "rishabhjain777@gmail.com"

TELEGRAM_CHAT_ID = "-5542300865"


import asyncio
import csv
import io
import json
import os
import re
import sys
import threading
import smtplib
import secrets
import urllib.parse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path

# PythonAnywhere can expose an ASCII-only stdout/stderr stream.  Keep all
# application logging safe so Unicode in Neo/API responses or log messages
# can never turn a successful login into a UnicodeEncodeError.
def safe_print(*args, **kwargs):
    sep = kwargs.pop("sep", " ")
    end = kwargs.pop("end", "\n")
    file = kwargs.pop("file", None)
    flush = kwargs.pop("flush", False)
    message = sep.join(str(a) for a in args) + end
    if file is None:
        file = sys.stdout
    try:
        # Prefer the stream's native encoding when it is UTF-8 capable.
        encoding = getattr(file, "encoding", None) or "utf-8"
        message.encode(encoding)
        file.write(message)
    except (UnicodeEncodeError, LookupError, AttributeError):
        # Fall back to an ASCII-safe representation. This is only for logs;
        # it does not change API data, credentials, JSON, HTML, or Telegram text.
        try:
            file.write(message.encode("ascii", errors="backslashreplace").decode("ascii"))
        except Exception:
            try:
                file.buffer.write(message.encode("utf-8", errors="replace"))
            except Exception:
                pass
    if flush:
        try:
            file.flush()
        except Exception:
            pass

# Use the safe logger throughout this application.
print = safe_print

from flask import Flask, request, redirect, make_response, jsonify

import httpx
import pandas as pd
from neo_api_client import NeoAPI
try:
    import importlib.metadata as _importlib_metadata
    KOTAK_NEO_SDK_INFO = _importlib_metadata.version("kotakneoapi")
except Exception:
    KOTAK_NEO_SDK_INFO = "unknown"
# REQUIREMENT: use the current Kotak Neo SDK: pip install --upgrade kotakneoapi
# Do not install the retired legacy package: neo-api-client / kotak-neo-api-v2.

from neo_api_client.websocket.feed import WsToken, SFeedScrip
# Set interval to 30 minutes (1800 seconds)
SNAPSHOT_SECONDS = 1800
WEB_PORT = int(os.getenv("OI_WEB_PORT", "8765"))
# Keep all generated files beside the application unless BASE_DIR is explicitly set.
# This makes the files easy to find on PythonAnywhere and avoids writing to an
# unexpected working directory.
BASE_DIR = Path(os.getenv("BASE_DIR", str(Path(__file__).resolve().parent))).resolve()
try:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    BASE_DIR = Path(__file__).resolve().parent

print(f"Application data directory: {BASE_DIR}")

JSON_FILE = str(BASE_DIR / "nse_futures_oi_live.json")
HTML_FILE = str(BASE_DIR / "nse_futures_oi_live.html")
CSV_FILE = str(BASE_DIR / "nse_futures_oi_refresh_history.csv")
LEGACY_CSV_FILE = str(BASE_DIR / "nse_futures_oi_5sec.csv")
CONTRACT_FILE = str(BASE_DIR / "nse_futures_contracts.json")
# Paper-trade recommendations only. No broker orders are placed.
RECOMMENDED_TRADES_FILE = str(BASE_DIR / "recommended_trades.json")
RECOMMENDED_TRADES_CSV_FILE = str(BASE_DIR / "recommended_trades_history.csv")

# Application authentication is memory-only. The browser gets only an opaque
# session-id cookie; the actual session record lives in this Python process.
APP_USERNAME = os.getenv("APP_USERNAME", "admin")
WEB_SESSION_TTL_SECONDS = int(os.getenv("WEB_SESSION_TTL_SECONDS", "2592000"))
WEB_COOKIE_NAME = "trade_analysis_session"
WEB_SESSIONS = {}
WEB_SESSIONS_LOCK = threading.RLock()

NEO_CLIENT = None
NEO_AUTH_LOCK = threading.RLock()
NEO_REAUTH_REQUIRED = False
# Authenticated Neo trading session.  The TOTP itself is NEVER stored.
# These values are kept only in process memory and are reused by the same
# authenticated NeoAPI client for subsequent orders.
NEO_SESSION = {field: None for field in (
    "view_token", "sid", "ucc", "edit_token", "edit_sid", "edit_rid",
    "data_center", "base_url", "sfeed_websocket_url", "order_feed_url",
    "feed_url", "rt_url",
)}

# Optional notification credentials are supplied through the authenticated UI.
# They are intentionally kept in process memory only and are never loaded from
# .env, written to disk, returned to the browser, or logged.
RUNTIME_TELEGRAM_BOT_TOKEN = None
RUNTIME_SMTP_PASSWORD = None
RUNTIME_NEO_CONSUMER_KEY = None
RUNTIME_NEO_MPIN = None

NOTIFICATION_CREDENTIALS_LOCK = threading.RLock()
FUTURE_EXPIRIES_TO_MONITOR = int(os.getenv("FUTURE_EXPIRIES_TO_MONITOR", "1"))
MAX_SUBSCRIPTIONS = 3000
RECONNECT_DELAY = 5

# Fixed Neo account identifiers can come from PythonAnywhere environment
# variables, with the values configured in this application used as fallback.
# Consumer Key and MPIN remain runtime-only login inputs.
CREDS = {
    "NEO_CONSUMER_KEY": os.getenv("NEO_CONSUMER_KEY"),
    "NEO_MOBILE_NUMBER": os.getenv("NEO_MOBILE_NUMBER", NEO_MOBILE_NUMBER),
    "NEO_UCC": os.getenv("NEO_UCC", NEO_UCC),
    "NEO_MPIN": os.getenv("NEO_MPIN"),
}

MARKET = {}
CM_TOKENS = {} # Maps underlying equity symbol to its instrument token
CM_MARKET = {} # Tracks the live LTP of cash equities
PREV_OI = {}
PREV_LTP = {}
YESTERDAY_OI = {}
LOCK = threading.Lock()

HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NSE F&O Live OI - 30 Min</title>
<style>
body{margin:0;background:#0b1020;color:#e9eef8;font-family:Arial,sans-serif}
header{padding:18px 24px;background:#121a2d;border-bottom:1px solid #293653;position:sticky;top:0;z-index:2}
h1{margin:0 0 5px;font-size:24px}.sub{color:#9aa8c1;font-size:13px}
.wrap{padding:18px 24px}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:15px}
.card{background:#121a2d;border:1px solid #293653;border-radius:10px;padding:14px}.label{color:#9aa8c1;font-size:12px}.value{font-size:22px;font-weight:bold;margin-top:5px}
.toolbar{display:flex;gap:10px;margin-bottom:12px;flex-wrap:wrap;align-items:center}
input,button{background:#121a2d;color:#fff;border:1px solid #394967;border-radius:6px;padding:9px}
button{cursor:pointer}.tablebox{border:none;border-radius:10px}
table{width:100%;border-collapse:collapse;min-width:700px;background:#121a2d;border:1px solid #293653;border-radius:10px;overflow:hidden;margin-bottom:20px}
th{background:#18233b;color:#aebbd2;padding:10px;text-align:right;font-size:12px}
th:first-child,td:first-child{text-align:left}
td{padding:9px 10px;border-top:1px solid #202c46;text-align:right;font-size:13px}
.pos{color:#26a69a;font-weight:bold}.neg{color:#ef5350;font-weight:bold}
h3{color:#aebbd2;margin:10px 0 10px 0;font-size:15px;text-transform:uppercase;letter-spacing:1px;border-left:3px solid #2979ff;padding-left:10px}
.logout{float:right;background:#ef5350;border:none;font-weight:bold}
@media(max-width:800px){.cards{grid-template-columns:repeat(2,1fr)}}
</style>
</head>
<body>
<header>
<button class="logout" onclick="logout()">Logout</button>
<h1>NSE F&O Live Open Interest</h1>
<div class="sub">Kotak Neo live feed · Trade recommendations only · <span id="status">Waiting...</span></div>
</header>
<div class="wrap">
<div class="cards">
<div class="card"><div class="label">Total Contracts</div><div class="value" id="contracts">-</div></div>
<div class="card"><div class="label">Live Data</div><div class="value" id="live">-</div></div>
<div class="card"><div class="label">Last Background Snapshot</div><div class="value" id="time">-</div></div>
<div class="card"><div class="label">Auto-Snapshot Interval</div><div class="value">30 mins</div></div>
</div>
<div class="toolbar">
<input id="search" placeholder="Search symbol..." oninput="render();loadRecommendations()">
<button onclick="load()" style="background:#2979ff;font-weight:bold;border:none;">Refresh Screen</button>
</div>
<div id="tables_container" class="tablebox"></div>
<h3>Recommended Trades</h3><div id="recommendations" class="tablebox"></div>
</div>
<script>
let data=[];
const num=(v,d=0)=>v==null?"-":Number(v).toLocaleString(undefined,{maximumFractionDigits:d});
const signed=(v,d=0)=>v==null?"-":(Number(v)>=0?"+":"")+Number(v).toLocaleString(undefined,{maximumFractionDigits:d});
const cls=v=>v==null?"":Number(v)>0?"pos":Number(v)<0?"neg":"";

function render(){
 let q=document.getElementById("search").value.toUpperCase();
 let filtered=data.filter(x=>(x.trading_symbol||"").toUpperCase().includes(q));
 const signals=["LONG_BUILDUP","SHORT_BUILDUP","SHORT_COVERING","LONG_UNWINDING"];
 let html="";
 signals.forEach(sig=>{
   let items=filtered.filter(x=>x.classification===sig);
   if(items.length===0)return;
   items.sort((x,y)=>{
     const a=x.oi_change_pct_since_refresh==null?0:Math.abs(Number(x.oi_change_pct_since_refresh));
     const b=y.oi_change_pct_since_refresh==null?0:Math.abs(Number(y.oi_change_pct_since_refresh));
     return b-a;
   });
   items=items.slice(0,3);
   html+=`<h3>${sig.replace("_"," ")}</h3>`;
   html+=`<table><thead><tr><th>Trading Symbol</th><th>LTP</th><th>Δ LTP</th><th>OI</th><th>Δ OI</th><th>Δ OI %</th><th>OI % vs Yest</th></tr></thead><tbody>`;
   html+=items.map(x=>`<tr>
     <td><b>${x.trading_symbol||""}</b></td>
     <td>${num(x.ltp,2)}</td><td class="${cls(x.ltp_change_since_refresh)}">${signed(x.ltp_change_since_refresh,2)}</td>
     <td>${num(x.oi)}</td><td class="${cls(x.oi_change_since_refresh)}">${signed(x.oi_change_since_refresh)}</td>
     <td class="${cls(x.oi_change_pct_since_refresh)}">${x.oi_change_pct_since_refresh!=null?signed(x.oi_change_pct_since_refresh,2)+"%":"-"}</td>
     <td class="${cls(x.oi_change_pct_since_yesterday)}">${x.oi_change_pct_since_yesterday!=null?signed(x.oi_change_pct_since_yesterday,2)+"%":"-"}</td>
   </tr>`).join("");
   html+=`</tbody></table>`;
 });
 document.getElementById("tables_container").innerHTML=html||"<div style='color:#9aa8c1;margin-top:20px;'>No matching signals found for your search.</div>";
}



async function loadRecommendations(){
 try{
   const r=await fetch("/api/recommended-trades?t="+Date.now(),{cache:"no-store"});
   if(r.status===401){window.location.href="/login";return;}
   const j=await r.json();
   const items=j.data||[];
   const q=document.getElementById("search").value.toUpperCase();
   const filtered=items.filter(x=>(x.trading_symbol||"").toUpperCase().includes(q));
   if(!filtered.length){
     document.getElementById("recommendations").innerHTML="<div style='color:#9aa8c1;padding:15px;'>No trade recommendations recorded yet.</div>";
     return;
   }
   let html="<table><thead><tr><th>ID</th><th>Time</th><th>Symbol</th><th>Future Contract</th><th>Action</th><th>Qty</th><th>Product</th><th>LTP</th><th>Δ OI %</th><th>Signal</th><th>Status</th></tr></thead><tbody>";
   filtered.slice().reverse().forEach(x=>{
     html+=`<tr><td>${x.id||""}</td><td>${x.timestamp||""}</td><td><b>${x.symbol||""}</b></td><td>${x.trading_symbol||""}</td><td>${x.recommendation||""}</td><td>${x.quantity||""}</td><td>${x.product||""}</td><td>${num(x.ltp,2)}</td><td class="${cls(x.oi_change_pct)}">${x.oi_change_pct!=null?signed(x.oi_change_pct,2)+"%":"-"}</td><td>${x.signal||""}</td><td>${x.status||""}</td></tr>`;
   });
   html+="</tbody></table>";
   document.getElementById("recommendations").innerHTML=html;
 }catch(e){
   document.getElementById("recommendations").innerHTML="<div style='color:#9aa8c1;padding:15px;'>Unable to load recommendations.</div>";
 }
}

async function load(){
 try{
   const r=await fetch("/nse_futures_oi_live.json?t="+Date.now(),{cache:"no-store"});
   if(r.status===401){window.location.href="/login";return;}
   if(!r.ok)throw Error(r.status);
   const j=await r.json(); data=j.data||[];
   document.getElementById("contracts").textContent=j.total_contracts??data.length;
   document.getElementById("live").textContent=j.live_contracts??0;
   document.getElementById("time").textContent=j.timestamp||"-";
   document.getElementById("status").textContent="Connected & Auto-refreshing";
   render();
 }catch(e){document.getElementById("status").textContent="Waiting for monitor data...";}
}

async function logout(){
 try{await fetch("/api/logout",{method:"POST",credentials:"same-origin"});}
 finally{window.location.href="/login";}
}
load();
checkNeoAuth();
setInterval(load,300000);
</script>
</body>
</html>
"""

LOGIN_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Trade Analysis Login</title>
<style>
body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#0b1020;color:#e9eef8;font-family:Arial,sans-serif}
.box{width:min(420px,calc(100% - 32px));background:#121a2d;border:1px solid #293653;border-radius:14px;padding:26px;box-sizing:border-box;box-shadow:0 20px 60px rgba(0,0,0,.35)}
h1{margin:0 0 8px;font-size:24px}.sub{color:#9aa8c1;font-size:13px;margin-bottom:20px}
label{display:block;color:#aebbd2;font-size:13px;margin:12px 0 6px}
input{width:100%;box-sizing:border-box;background:#0b1020;color:#fff;border:1px solid #394967;border-radius:7px;padding:11px;font-size:15px}
input:focus{outline:none;border-color:#2979ff}
button{width:100%;margin-top:18px;background:#2979ff;color:white;border:0;border-radius:7px;padding:11px;font-weight:bold;font-size:15px;cursor:pointer}
button:hover{background:#1e63d8}
.error{display:none;margin-top:14px;color:#ff8a80;font-size:13px}
.note{margin-top:16px;color:#7f8da8;font-size:11px;line-height:1.5}
code{background:#18233b;padding:2px 5px;border-radius:4px;color:#cbd5e1;font-size:11px}
.input-wrap{position:relative}
.toggle-btn{position:absolute;right:10px;top:50%;transform:translateY(-50%);cursor:pointer;color:#9aa8c1;font-size:14px;user-select:none}
</style>
</head>
<body>
<div class="box">
<h1>Trade Analysis</h1>
<div class="sub">Secure Kotak Neo market-data login</div>
<form id="loginForm">

<label>Username</label>
<input id="username" autocomplete="username" required value="%s">

<label>Credentials (Key|MPIN|Telegram_Token|SMTP_Password)</label>
<div class="input-wrap">
  <input id="credentials" type="password" autocomplete="off" required
         placeholder="Neo_Consumer_Key|Neo_MPIN|Telegram_Bot_Token|SMTP_Password"
         style="padding-right:38px;">
  <span class="toggle-btn" id="toggleCreds" title="Show/Hide">👁️</span>
</div>

<label>TOTP</label>
<input id="totp" inputmode="numeric" autocomplete="one-time-code"
       maxlength="6" pattern="[0-9]{6}" required placeholder="6-digit TOTP">

<button type="submit">Login</button>

<div id="error" class="error"></div>

</form>

<div class="note">
Format: <code>Neo_Consumer_Key|Neo_MPIN|Telegram_Bot_Token|SMTP_Password</code><br>
Telegram Bot Token and SMTP Password are optional.<br>
All credentials are kept only in server memory. They are never written to files, cookies, browser storage, or logs.
</div>

</div>

<script>
const credInput = document.getElementById("credentials");
const toggleBtn = document.getElementById("toggleCreds");
toggleBtn.addEventListener("click", () => {
  credInput.type = credInput.type === "password" ? "text" : "password";
});

document.getElementById("loginForm").addEventListener("submit", async e => {
 e.preventDefault();

 const error = document.getElementById("error");
 error.style.display = "none";

 try {
   const r = await fetch("/api/login", {
     method: "POST",
     headers: {"Content-Type": "application/x-www-form-urlencoded"},
     credentials: "same-origin",
     body: new URLSearchParams({
       username: document.getElementById("username").value.trim(),
       credentials: document.getElementById("credentials").value.trim(),
       totp: document.getElementById("totp").value.trim()
     })
   });

   const j = await r.json();

   if (!r.ok || !j.success) {
     error.textContent = j.error || "Login failed.";
     error.style.display = "block";
     return;
   }

   window.location.href = "/";

 } catch (err) {
   error.textContent = "Unable to contact the server.";
   error.style.display = "block";
 }
});
</script>

</body>
</html>
"""

def write_dashboard():
    """Write the current dashboard HTML to the legacy HTML output file."""
    Path(HTML_FILE).write_text(HTML_TEMPLATE, encoding="utf-8")



def validate():
    """Validate fixed, non-secret Neo configuration."""
    if not CREDS.get("NEO_MOBILE_NUMBER"):
        raise RuntimeError("NEO_MOBILE_NUMBER is required.")
    if not CREDS.get("NEO_UCC"):
        raise RuntimeError("NEO_UCC is required.")


def authentication_required(error):
    text = str(error).lower()
    return any(x in text for x in (
        "unauthorized", "authentication", "session expired",
        "invalid token", "token expired", "not authenticated"
    ))


def _capture_neo_session(client):
    """Capture the authenticated Neo session created after TOTP + MPIN.

    The six-digit TOTP is deliberately not stored.  Only the session values
    generated by Neo are retained in process memory.
    """
    global NEO_SESSION

    configuration = getattr(client, "configuration", None)
    if configuration is None:
        raise RuntimeError("Neo client has no configuration/session state.")

    captured = {}
    for field in NEO_SESSION:
        captured[field] = getattr(configuration, field, None)

    if not captured.get("edit_token") or not captured.get("edit_sid"):
        raise RuntimeError("Neo did not create a usable authenticated trading session.")

    with NEO_AUTH_LOCK:
        NEO_SESSION.update(captured)

    return captured


def _neo_session_is_usable(client=None):
    """Return True when the current in-memory Neo trading session is usable."""
    active_client = client
    if active_client is None:
        active_client = NEO_CLIENT

    if active_client is None:
        return False

    configuration = getattr(active_client, "configuration", None)
    if configuration is None:
        return False

    # Prefer the actual client configuration because NeoAPI will use it for
    # place_order/create_websocket.  NEO_SESSION is a memory-only backup/state
    # record, not a second authentication mechanism.
    edit_token = getattr(configuration, "edit_token", None)
    edit_sid = getattr(configuration, "edit_sid", None)
    return bool(edit_token and edit_sid)


def _mark_neo_session_expired(reason=""):
    """Pause trading only when Neo explicitly reports an auth/session failure."""
    global NEO_REAUTH_REQUIRED
    NEO_REAUTH_REQUIRED = True
    suffix = f" Reason: {reason}" if reason else ""
    print(
        f"[{datetime.now():%H:%M:%S}] 🔐 Neo trading session expired/unauthorized."
        f"{suffix} Fresh TOTP required."
    )


def _is_explicit_neo_auth_error(response=None, exception=None):
    """Detect explicit Neo authentication failures without over-triggering TOTP."""
    if exception is not None:
        text = str(exception).lower()
        auth_terms = (
            "unauthorized", "not authenticated", "authentication failed",
            "invalid token", "token expired", "session expired",
            "invalid session", "login required", "authentication error",
        )
        return any(term in text for term in auth_terms)

    if isinstance(response, dict):
        stat = str(response.get("stat", "")).lower()
        st_code = str(response.get("stCode", ""))
        err = str(
            response.get("errMsg")
            or response.get("error")
            or response.get("message")
            or ""
        ).lower()

        # 100008 is the unauthorized response used by the current code.
        if st_code == "100008":
            return True
        if stat in ("unauthorized", "not_authenticated"):
            return True
        auth_terms = (
            "unauthorized", "not authenticated", "authentication failed",
            "invalid token", "token expired", "session expired",
            "invalid session", "login required",
        )
        return any(term in err for term in auth_terms)

    return False


def ensure_authentication_success(response, step):
    """Raise a clear error when a Kotak Neo authentication step fails."""
    if not isinstance(response, dict) or not response.get("data"):
        detail = response
        if isinstance(response, dict):
            detail = (
                response.get("error")
                or response.get("Error")
                or response.get("message")
                or response
            )
        raise RuntimeError(f"{step} failed: {detail}")


def login(client, totp=None, persist=False):
    """Authenticate Neo using only a live TOTP supplied by the caller."""
    global NEO_REAUTH_REQUIRED

    totp = str(totp or "").strip()
    if not re.fullmatch(r"\d{6}", totp):
        raise RuntimeError("A valid 6-digit TOTP is required.")

    with NEO_AUTH_LOCK:
        print("Authenticating with Kotak Neo...")

        login_response = client.totp_login(
            mobile_number=CREDS["NEO_MOBILE_NUMBER"],
            ucc=CREDS["NEO_UCC"],
            totp=totp
        )
        ensure_authentication_success(login_response, "TOTP login")

        validate_response = client.totp_validate(mpin=RUNTIME_NEO_MPIN)
        ensure_authentication_success(validate_response, "MPIN validation")

        _capture_neo_session(client)

        NEO_REAUTH_REQUIRED = False
        print("Neo authentication successful; market-data session saved in memory.")
        return client


def create_authenticated_neo_client(totp):
    """
    Create a completely fresh NeoAPI client and authenticate it with the
    browser-supplied TOTP. Neo session tokens remain memory-only.
    """
    global NEO_CLIENT, NEO_REAUTH_REQUIRED

    totp = str(totp or "").strip()
    if not re.fullmatch(r"\d{6}", totp):
        raise RuntimeError("A valid 6-digit TOTP is required.")

    if not RUNTIME_NEO_CONSUMER_KEY or not RUNTIME_NEO_MPIN:
        raise RuntimeError("Neo consumer key and MPIN must be supplied through the login UI.")

    new_client = NeoAPI(
        environment="prod",
        consumer_key=RUNTIME_NEO_CONSUMER_KEY
    )

    login_response = new_client.totp_login(
        mobile_number=CREDS["NEO_MOBILE_NUMBER"],
        ucc=CREDS["NEO_UCC"],
        totp=totp
    )
    ensure_authentication_success(login_response, "TOTP login")

    validate_response = new_client.totp_validate(
        mpin=RUNTIME_NEO_MPIN
    )
    ensure_authentication_success(validate_response, "MPIN validation")

    _capture_neo_session(new_client)

    # Atomically install the NEW authenticated client.
    with NEO_AUTH_LOCK:
        old_client = NEO_CLIENT
        NEO_CLIENT = new_client
        NEO_REAUTH_REQUIRED = False

    # Best-effort cleanup of the stale in-memory session.
    if old_client is not None and old_client is not new_client:
        try:
            old_client.logout()
        except Exception:
            pass

    print("🔐 Fresh Kotak Neo client authenticated successfully for market data.")
    return new_client


def authenticate_web_user(totp):
    """Authenticate browser TOTP using a completely fresh NeoAPI client."""
    create_authenticated_neo_client(totp)
    return True


def set_runtime_notification_credentials(
    telegram_bot_token="",
    smtp_password="",
    neo_consumer_key="",
    neo_mpin="",
):
    """Store runtime credentials in process memory only."""
    global RUNTIME_TELEGRAM_BOT_TOKEN, RUNTIME_SMTP_PASSWORD
    global RUNTIME_NEO_CONSUMER_KEY, RUNTIME_NEO_MPIN

    telegram_bot_token = str(telegram_bot_token or "").strip()
    smtp_password = str(smtp_password or "").strip()
    neo_consumer_key = str(neo_consumer_key or "").strip()
    neo_mpin = str(neo_mpin or "").strip()

    if telegram_bot_token and not re.fullmatch(r"\d{6,15}:[A-Za-z0-9_-]{20,200}", telegram_bot_token):
        raise ValueError("Invalid Telegram bot token format.")

    if not neo_consumer_key:
        raise ValueError("Neo Consumer Key is required.")

    if not neo_mpin:
        raise ValueError("Neo MPIN is required.")

    with NOTIFICATION_CREDENTIALS_LOCK:
        RUNTIME_TELEGRAM_BOT_TOKEN = telegram_bot_token or None
        RUNTIME_SMTP_PASSWORD = smtp_password or None
        RUNTIME_NEO_CONSUMER_KEY = neo_consumer_key
        RUNTIME_NEO_MPIN = neo_mpin


def clear_runtime_notification_credentials():
    """Remove optional notification credentials from process memory."""
    global RUNTIME_TELEGRAM_BOT_TOKEN, RUNTIME_SMTP_PASSWORD
    with NOTIFICATION_CREDENTIALS_LOCK:
        RUNTIME_TELEGRAM_BOT_TOKEN = None
        RUNTIME_SMTP_PASSWORD = None


SESSION_FIELDS = (
    "view_token", "sid", "ucc", "edit_token", "edit_sid", "edit_rid",
    "data_center", "base_url", "sfeed_websocket_url", "order_feed_url",
    "feed_url", "rt_url",
)


def scrip_master_url(client, segment="nse_fo"):
    r = client.scrip_master(exchange_segment=segment)
    if isinstance(r, str):
        try:
            r = json.loads(r)
        except Exception:
            if r.startswith("http"):
                return r
    if isinstance(r, dict):
        paths = r.get("filesPaths", [])
        if isinstance(paths, str):
            paths = [paths]
        for u in paths:
            if isinstance(u, str) and segment in u.lower() and u.lower().endswith(".csv"):
                return u
        for u in paths:
            if isinstance(u, str) and u.startswith("http"):
                return u
    raise RuntimeError(f"Could not find Scrip Master URL for {segment}. Response: {r}")

def get_master(client, segment="nse_fo"):
    url = scrip_master_url(client, segment)
    print(f"Downloading {segment.upper()} Scrip Master...")
    r = httpx.get(url, timeout=60, follow_redirects=True)
    r.raise_for_status()
    file_name = f"{segment}_scrip_master.csv"
    Path(file_name).write_bytes(r.content)
    return r.content

def col(df, names, required=True):
    cols = list(df.columns)
    for n in names:
        if n.lower() in cols:
            return n.lower()
    for c in cols:
        for n in names:
            if n.lower() in c or c in n.lower():
                return c
    if required:
        raise RuntimeError(f"Column not found: {names}. Available: {cols}")
    return None

def expiry(v):
    if v is None:
        return pd.NaT
    s = str(v).strip()
    for f in ("%d%b%Y", "%d%b%y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y%m%d"):
        try:
            return pd.to_datetime(s, format=f)
        except Exception:
            pass
    return pd.to_datetime(s, errors="coerce", dayfirst=True)

def derive_symbol(s):
    s = str(s).upper().strip()
    for suffix in ("-FUT", "FUT"):
        if s.endswith(suffix):
            s = s[:-len(suffix)]
    s = re.sub(r"\d{1,2}[A-Z]{3}\d{2,4}$", "", s)
    s = re.sub(r"\d{1,2}[A-Z]{3}$", "", s)
    return re.sub(r"\d{6,8}$", "", s)

def discover(df):
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    token = col(df, ["psymbol", "instrument_token", "instrumenttoken", "token"])
    trading = col(df, ["ptrdsymbol", "trading_symbol", "tradingsymbol", "symbol"])
    typ = col(df, ["pinsttype", "instrument_type", "instrumenttype", "inst_type"], False)
    exp = col(df, ["pexpirydate", "expiry_date", "expirydate", "expiry"], False)
    lot = col(df, ["llotsize", "lotsize", "lot_size"], False)
    under = col(df, ["psymbolname", "underlying", "underlyingsymbol", "symbol_name"], False)

    if typ:
        t = df[typ].fillna("").astype(str).str.upper().str.strip()
        x = df[t.isin(["FUTSTK", "FUTIDX", "FUT", "FUTURE", "FUTURES"])].copy()
    else:
        t = df[trading].fillna("").astype(str).str.upper()
        x = df[t.str.contains("FUT", regex=False)].copy()

    if x.empty:
        raise RuntimeError("No futures found in Scrip Master.")

    out = []
    for _, row in x.iterrows():
        tok = str(row.get(token, "")).strip()
        tr = str(row.get(trading, "")).strip()
        if not tok or tok.lower() == "nan" or not tr:
            continue
        u = str(row.get(under, "")).strip() if under else ""
        if not u or u.lower() == "nan":
            u = derive_symbol(tr)
        out.append({
            "symbol": u,
            "trading_symbol": tr,
            "instrument_token": tok,
            "exchange_segment": "nse_fo",
            "instrument_type": str(row.get(typ, "")).strip().upper() if typ else "",
            "expiry": str(row.get(exp, "")).strip() if exp else "",
            "lot_size": str(row.get(lot, "")).strip() if lot else ""
        })
    return pd.DataFrame(out)

def select_active(df):
    df = df.copy()
    df["_expiry"] = df["expiry"].apply(expiry)
    today = pd.Timestamp.now().normalize()
    df = df[df["_expiry"].isna() | (df["_expiry"] >= today)].copy()
    chosen = []
    for symbol, g in df.groupby("symbol"):
        valid = g[g["_expiry"].notna()].sort_values("_expiry")
        invalid = g[g["_expiry"].isna()]
        chosen.append((valid if not valid.empty else invalid).head(FUTURE_EXPIRIES_TO_MONITOR))
    return pd.concat(chosen, ignore_index=True)

def initialize(df):
    MARKET.clear()
    for _, r in df.iterrows():
        key = ("nse_fo", str(r["instrument_token"]))
        MARKET[key] = {
            "symbol": str(r["symbol"]),
            "trading_symbol": str(r["trading_symbol"]),
            "instrument_token": str(r["instrument_token"]),
            "lot_size": str(r["lot_size"]),
            "ltp": None,
            "oi": None,
            "volume": None,
            "last_traded_quantity": None,
            "last_update": None
        }

def initialize_cm(selected_futures, df_cm):
    df_cm.columns = [str(c).strip().lower().replace(" ", "_") for c in df_cm.columns]
    cm_token_col = col(df_cm, ["psymbol", "instrument_token", "instrumenttoken", "token"])
    cm_symbol_col = col(df_cm, ["ptrdsymbol", "trading_symbol", "tradingsymbol", "symbol"])

    eq_tokens = {}
    for _, row in df_cm.iterrows():
        sym = str(row.get(cm_symbol_col, "")).strip()
        tok = str(row.get(cm_token_col, "")).strip()
        if sym.endswith("-EQ"):
            base = sym[:-3]
            eq_tokens[base] = tok
        elif sym:
            if sym not in eq_tokens:
                eq_tokens[sym] = tok

    CM_TOKENS.clear()
    CM_MARKET.clear()

    for _, r in selected_futures.iterrows():
        base_sym = str(r["symbol"])
        if base_sym in eq_tokens:
            tok = eq_tokens[base_sym]
            CM_TOKENS[base_sym] = tok
            CM_MARKET[tok] = {"symbol": base_sym, "ltp": None}

def load_yesterday_oi():
    YESTERDAY_OI.clear()
    previous_date = (datetime.now().date() - pd.Timedelta(days=1)).isoformat()
    for history_file in (LEGACY_CSV_FILE, CSV_FILE):
        if not Path(history_file).exists():
            continue
        try:
            with open(history_file, newline="", encoding="utf-8") as file:
                for row in csv.DictReader(file):
                    if not str(row.get("timestamp", "")).startswith(previous_date):
                        continue
                    token, oi = row.get("instrument_token"), row.get("oi")
                    if token and oi not in (None, "", "None"):
                        YESTERDAY_OI[("nse_fo", str(token))] = float(oi)
        except (OSError, ValueError, csv.Error) as error:
            print(f"Warning: could not load yesterday's OI baseline: {error}")

def process(message):
    if not isinstance(message, SFeedScrip):
        return
    try:
        seg = message.exchange_segment
        tok = str(message.instrument_token)
    except Exception:
        return

    # Process underlying cash equities for Telegram logic
    if seg == "nse_cm":
        if tok in CM_MARKET:
            try:
                v = getattr(message, "last_traded_price", None)
                if v is not None: CM_MARKET[tok]["ltp"] = float(v)
            except Exception: pass
        return

    # Process F&O
    key = (seg, tok)
    if key not in MARKET:
        for k in MARKET:
            if k[1] == tok:
                key = k
                break
        else:
            return

    r = MARKET[key]
    try:
        v = getattr(message, "last_traded_price", None)
        if v is not None: r["ltp"] = float(v)
    except Exception: pass
    try:
        v = getattr(message, "oi", None)
        if v is None: v = getattr(message, "open_interest", None)
        if v is not None: r["oi"] = int(float(v))
    except Exception: pass
    try:
        v = getattr(message, "volume", None)
        if v is not None: r["volume"] = int(float(v))
    except Exception: pass
    try:
        v = getattr(message, "last_traded_quantity", None)
        if v is not None: r["last_traded_quantity"] = int(float(v))
    except Exception: pass
    r["last_update"] = datetime.now().isoformat(timespec="seconds")


def _load_recommended_trades():
    path = Path(RECOMMENDED_TRADES_FILE)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def save_recommended_trade(row):
    """Persist a paper-trade recommendation to JSON and CSV.

    The underlying equity symbol is stored as ``symbol`` (for example HAVELLS),
    while the actual futures contract is retained as ``trading_symbol``
    (for example HAVELLS26OCTFUT).
    """
    # Underlying equity symbol: HAVELLS
    symbol = str(row.get("symbol") or "").strip().upper()

    # Futures contract: HAVELLS26OCTFUT
    trading_symbol = str(row.get("trading_symbol") or "").strip().upper()

    # Safety fallback in case Neo did not populate symbol.
    if not symbol and trading_symbol:
        symbol = derive_symbol(trading_symbol).upper()

    if not symbol:
        print(f"[{datetime.now():%H:%M:%S}] ERROR: Cannot save recommendation - empty symbol.")
        return False

    records = _load_recommended_trades()
    source_timestamp = row.get("timestamp")

    # Prevent duplicate writes for the same underlying symbol + snapshot.
    if records:
        last = records[-1]
        if (
            last.get("symbol") == symbol
            and last.get("source_timestamp") == source_timestamp
        ):
            print(
                f"[{datetime.now():%H:%M:%S}] "
                f"Recommendation already saved: {symbol}"
            )
            return True

    record = {
        "id": len(records) + 1,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "source_timestamp": source_timestamp,

        # HAVELLS - used for Telegram / equity identification
        "symbol": symbol,

        # HAVELLS26OCTFUT - actual futures contract used for OI analysis
        "trading_symbol": trading_symbol,

        "recommendation": "BUY",
        "instrument": "EQUITY",
        "quantity": 1,
        "product": "CNC",
        "ltp": row.get("ltp"),
        "ltp_change": row.get("ltp_change_since_refresh"),
        "oi": row.get("oi"),
        "oi_change": row.get("oi_change_since_refresh"),
        "oi_change_pct": row.get("oi_change_pct_since_refresh"),
        "oi_change_pct_vs_yesterday": row.get("oi_change_pct_since_yesterday"),
        "signal": row.get("classification"),
        "status": "RECOMMENDED",
    }

    records.append(record)

    # Make absolutely sure the destination directory exists.
    json_path = Path(RECOMMENDED_TRADES_FILE).resolve()
    csv_path = Path(RECOMMENDED_TRADES_CSV_FILE).resolve()
    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # -----------------------------
    # Save JSON atomically
    # -----------------------------
    try:
        tmp_path = Path(str(json_path) + ".tmp")
        tmp_path.write_text(
            json.dumps(records, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        os.replace(str(tmp_path), str(json_path))
    except Exception as error:
        print(
            f"[{datetime.now():%H:%M:%S}] ERROR saving recommendation JSON "
            f"to {json_path}: {error}"
        )
        return False

    # -----------------------------
    # Save CSV incrementally
    # -----------------------------
    try:
        fields = list(record.keys())
        exists = csv_path.exists()

        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            if not exists:
                writer.writeheader()
            writer.writerow(record)

    except Exception as error:
        print(
            f"[{datetime.now():%H:%M:%S}] ERROR saving recommendation CSV "
            f"to {csv_path}: {error}"
        )
        return False

    print(
        f"[{datetime.now():%H:%M:%S}] Recommendation saved: "
        f"{symbol} | {trading_symbol} | BUY | CNC | Qty 1"
    )
    print(f"    JSON: {json_path}")
    print(f"    CSV : {csv_path}")

    return True


def get_recommended_trades():
    return _load_recommended_trades()


def snapshot():
    rows = []
    with LOCK:
        for key, r in MARKET.items():
            oi, ltp = r["oi"], r["ltp"]
            po, pl = PREV_OI.get(key), PREV_LTP.get(key)
            d_oi = oi - po if oi is not None and po is not None else None
            d_ltp = ltp - pl if ltp is not None and pl is not None else None

            d_oi_pct = (
                (d_oi / po * 100)
                if d_oi is not None and po and po > 0
                else None
            )

            yesterday_oi = YESTERDAY_OI.get(key)
            oi_pct_yesterday = (
                (oi - yesterday_oi) / yesterday_oi * 100
                if oi is not None and yesterday_oi else None
            )
            if d_ltp is not None and d_oi is not None:
                if d_ltp > 0 and d_oi > 0: sig = "LONG_BUILDUP"
                elif d_ltp < 0 and d_oi > 0: sig = "SHORT_BUILDUP"
                elif d_ltp > 0 and d_oi < 0: sig = "SHORT_COVERING"
                elif d_ltp < 0 and d_oi < 0: sig = "LONG_UNWINDING"
                else: sig = "NEUTRAL"
            else:
                sig = "UNKNOWN"

            rows.append({
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "symbol": r["symbol"],
                "trading_symbol": r["trading_symbol"],
                "instrument_token": r["instrument_token"],
                "lot_size": r["lot_size"],
                "ltp": ltp,
                "ltp_change_since_refresh": d_ltp,
                "oi": oi,
                "oi_change_since_refresh": d_oi,
                "oi_change_pct_since_refresh": d_oi_pct,
                "oi_change_pct_since_yesterday": oi_pct_yesterday,
                "last_traded_quantity": r["last_traded_quantity"],
                "classification": sig
            })
            if oi is not None: PREV_OI[key] = oi
            if ltp is not None: PREV_LTP[key] = ltp
    return rows

def save_outputs(rows):
    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "snapshot_mode": "manual_refresh",
        "total_contracts": len(MARKET),
        "live_contracts": sum(1 for r in rows if r["oi"] is not None or r["ltp"] is not None),
        "data": rows
    }
    tmp = JSON_FILE + ".tmp"
    Path(tmp).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp, JSON_FILE)

    fields = list(rows[0].keys()) if rows else []
    if fields:
        exists = Path(CSV_FILE).exists()
        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            if not exists: w.writeheader()
            w.writerows(rows)

def send_email(rows):
    smtp_host = SMTP_HOST
    smtp_port = SMTP_PORT
    smtp_user = SMTP_USER
    smtp_pass = RUNTIME_SMTP_PASSWORD

    if not smtp_host or not smtp_user or not smtp_pass:
        return

    categories = {
        "LONG_BUILDUP": [],
        "SHORT_BUILDUP": [],
        "SHORT_COVERING": [],
        "LONG_UNWINDING": []
    }

    for r in rows:
        sig = r.get("classification")
        if sig in categories:
            categories[sig].append(r)

    html_parts = []
    html_parts.append("<h2 style='font-family:Arial,sans-serif;'>NSE F&O 5-Min Update</h2>")

    for sig, items in categories.items():
        items.sort(key=lambda x: abs(x.get("oi_change_pct_since_refresh") or 0), reverse=True)
        top3 = items[:3]

        if not top3:
            continue

        html_parts.append(f"<h3 style='font-family:Arial,sans-serif;color:#333;'>{sig.replace('_', ' ')}</h3>")
        html_parts.append("<table border='1' cellspacing='0' cellpadding='6' style='font-family:Arial,sans-serif;border-collapse:collapse;width:100%;max-width:800px;text-align:right;'>")
        html_parts.append("<tr style='background-color:#f4f4f4;'><th style='text-align:left;'>Trading Symbol</th><th>LTP</th><th>Δ LTP</th><th>OI</th><th>Δ OI</th><th>Δ OI %</th><th>OI % vs Yest</th></tr>")

        for x in top3:
            sym = x.get('trading_symbol', '')
            ltp = f"{x['ltp']:.2f}" if x.get('ltp') is not None else "-"
            d_ltp = f"{x['ltp_change_since_refresh']:.2f}" if x.get('ltp_change_since_refresh') is not None else "-"
            oi = f"{x['oi']:,}" if x.get('oi') is not None else "-"
            d_oi = f"{x['oi_change_since_refresh']:,}" if x.get('oi_change_since_refresh') is not None else "-"
            d_oi_pct = f"{x['oi_change_pct_since_refresh']:.2f}%" if x.get('oi_change_pct_since_refresh') is not None else "-"
            yest_pct = f"{x['oi_change_pct_since_yesterday']:.2f}%" if x.get('oi_change_pct_since_yesterday') is not None else "-"

            html_parts.append(f"<tr><td style='text-align:left;'><b>{sym}</b></td><td>{ltp}</td><td>{d_ltp}</td><td>{oi}</td><td>{d_oi}</td><td>{d_oi_pct}</td><td>{yest_pct}</td></tr>")

        html_parts.append("</table><br>")

    if len(html_parts) == 1:
        return

    msg = MIMEMultipart()
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL_RECIPIENT
    msg['Subject'] = f"NSE Live OI Update - {datetime.now():%H:%M %p}"

    msg.attach(MIMEText("".join(html_parts), 'html'))

    try:
        with smtplib.SMTP_SSL(smtp_host, int(smtp_port)) as server:
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
    except Exception as e:
        print(f"Failed to send summary email: {e}")

def send_telegram(rows):
    bot_token = RUNTIME_TELEGRAM_BOT_TOKEN
    chat_id = TELEGRAM_CHAT_ID

    if not bot_token or not chat_id:
        return

    long_buildups = [r for r in rows if r.get("classification") == "LONG_BUILDUP"]
    long_buildups.sort(key=lambda x: abs(x.get("oi_change_pct_since_refresh") or 0), reverse=True)

    if not long_buildups:
        return

    # Get only the very first trade
    top1 = long_buildups[0]

    # Use the underlying equity symbol for Telegram as the primary symbol.
    # Example: HAVELLS instead of HAVELLS26OCTFUT.
    futures_symbol = top1.get('trading_symbol', '')
    base_sym = str(top1.get('symbol', '') or '').strip().upper()
    if not base_sym and futures_symbol:
        base_sym = derive_symbol(futures_symbol).upper()

    ltp = f"{top1['ltp']:.2f}" if top1.get('ltp') is not None else "-"

    raw_dltp = top1.get('ltp_change_since_refresh')
    d_ltp = f"+{raw_dltp:.2f}" if raw_dltp and raw_dltp > 0 else (f"{raw_dltp:.2f}" if raw_dltp is not None else "-")

    raw_doi_pct = top1.get('oi_change_pct_since_refresh')
    d_oi_pct = f"+{raw_doi_pct:.2f}%" if raw_doi_pct and raw_doi_pct > 0 else (f"{raw_doi_pct:.2f}%" if raw_doi_pct is not None else "-")

    # Retrieve matching cash equity price
    cm_price_str = "-"
    if base_sym in CM_TOKENS:
        cm_tok = CM_TOKENS[base_sym]
        cm_info = CM_MARKET.get(cm_tok)
        if cm_info and cm_info.get("ltp") is not None:
            cm_price_str = f"₹{cm_info['ltp']:.2f}"

    msg_lines = [
        "📊 *NSE F&O 5-Min Update*",
        "",
        "*Top LONG BUILDUP*",
        f"• *Symbol*: {base_sym}",
        f"  Future: {futures_symbol}",
        f"  Future LTP: {ltp} ({d_ltp}) | ΔOI: {d_oi_pct}",
        f"  Equity LTP: {cm_price_str}"
    ]

    final_message = "\n".join(msg_lines)
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": final_message,
        "parse_mode": "Markdown"
    }

    try:
        response = httpx.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            print(f"[{datetime.now():%H:%M:%S}] Telegram message sent successfully.")
        else:
            print(f"Telegram API Error: {response.text}")
    except Exception as e:
        print(f"Failed to send Telegram message: {e}")

async def snapshot_loop(client=None):
    await asyncio.sleep(5)

    while True:
        rows = snapshot()
        save_outputs(rows)
        send_email(rows)
        send_telegram(rows)
        # --- TOP LONG BUILDUP RECOMMENDATION ---
        long_buildups = [r for r in rows if r.get("classification") == "LONG_BUILDUP"]
        long_buildups.sort(
            key=lambda x: abs(x.get("oi_change_pct_since_refresh") or 0),
            reverse=True
        )

        if long_buildups:
            save_recommended_trade(long_buildups[0])
        # ------------------------------------------


        live = sum(1 for r in rows if r["oi"] is not None or r["ltp"] is not None)
        print(f"[{datetime.now():%H:%M:%S}] 15-min background snapshot: {live}/{len(MARKET)} live F&O")

        await asyncio.sleep(SNAPSHOT_SECONDS)

async def websocket_loop(client, contracts):
    global NEO_CLIENT, NEO_REAUTH_REQUIRED

    tokens = [
        WsToken("nse_fo", str(r["instrument_token"]))
        for _, r in contracts.iterrows()
    ]

    cm_ws_tokens = [
        WsToken("nse_cm", str(tok))
        for tok in CM_MARKET.keys()
    ]

    all_tokens = tokens + cm_ws_tokens

    while True:
        try:
            with NEO_AUTH_LOCK:
                active_client = NEO_CLIENT
                auth_required = NEO_REAUTH_REQUIRED

            if active_client is None or auth_required:
                await asyncio.sleep(1)
                continue

            async with active_client.create_websocket(
                max_subscriptions=MAX_SUBSCRIPTIONS,
                max_reconnect_attempts=10,
                reconnect_delay=5
            ) as ws:
                print(
                    f"WebSocket connected. Subscribing to "
                    f"{len(tokens)} futures and {len(cm_ws_tokens)} cash equities..."
                )
                await ws.subscribe_scrips(all_tokens)
                print("Subscription successful.")

                async for message in ws:
                    with LOCK:
                        process(message)

        except asyncio.CancelledError:
            raise

        except Exception as e:
            if _is_explicit_neo_auth_error(exception=e):
                _mark_neo_session_expired(str(e))
                print(
                    "🔐 Neo WebSocket authentication expired. "
                    "Enter a fresh TOTP in the browser Trade Analysis page."
                )

            print(
                f"WebSocket error: {e}; "
                f"reconnecting in {RECONNECT_DELAY}s..."
            )
            await asyncio.sleep(RECONNECT_DELAY)





# ============================================================
# FLASK / PYTHONANYWHERE WEB APPLICATION
# ============================================================

app = Flask(__name__)
app.config["SECRET_KEY"] = FLASK_SECRET_KEY

APP_USERNAME = os.getenv("APP_USERNAME", "admin")
WEB_SESSION_TTL_SECONDS = int(os.getenv("WEB_SESSION_TTL_SECONDS", "2592000"))

def _cookie_session_id():
    return request.cookies.get(WEB_COOKIE_NAME)

def _get_authenticated_session():
    sid = _cookie_session_id()
    if not sid:
        return None

    now = datetime.now().timestamp()

    with WEB_SESSIONS_LOCK:
        session = WEB_SESSIONS.get(sid)

        if not session:
            return None

        if now >= session["expires_at"]:
            WEB_SESSIONS.pop(sid, None)
            return None

        session["expires_at"] = now + WEB_SESSION_TTL_SECONDS
        return session

def _create_web_session():
    sid = secrets.token_urlsafe(48)
    now = datetime.now().timestamp()

    with WEB_SESSIONS_LOCK:
        WEB_SESSIONS[sid] = {
            "username": APP_USERNAME,
            "created_at": now,
            "expires_at": now + WEB_SESSION_TTL_SECONDS,
        }

    return sid

def _login_html():
    return LOGIN_HTML.replace("%s", APP_USERNAME)


@app.route("/login")
def login_page():

    if _get_authenticated_session():
        return redirect("/")

    return _login_html()


@app.route("/api/auth/status")
def auth_status():

    return jsonify({
        "authenticated":
            _get_authenticated_session() is not None
    })


@app.route("/api/login", methods=["POST"])
def api_login():

    username = str(
        request.form.get("username", "")
    ).strip()

    totp = str(
        request.form.get("totp", "")
    ).strip()

    # Combined credentials string format:
    # "Neo_Consumer_Key|Neo_MPIN|Telegram_Bot_Token|SMTP_Password"
    credentials = str(
        request.form.get("credentials", "")
    ).strip()

    # Backwards-compatibility for individual fields if supplied directly
    consumer_key = str(
        request.form.get("consumer_key", "")
    ).strip()

    mpin = str(
        request.form.get("mpin", "")
    ).strip()

    telegram_bot_token = str(
        request.form.get("telegram_bot_token", "")
    ).strip()

    smtp_password = str(
        request.form.get("smtp_password", "")
    ).strip()

    # Decode combined credentials string internally
    if credentials:
        # Strip optional enclosing quotes if copied with quotes
        if (credentials.startswith('"') and credentials.endswith('"')) or \
           (credentials.startswith("'") and credentials.endswith("'")):
            credentials = credentials[1:-1].strip()

        parts = credentials.split("|", 3)
        if len(parts) < 2:
            return jsonify({
                "success": False,
                "error": "Invalid format. Expected: Neo_Consumer_Key|Neo_MPIN|Telegram_Bot_Token|SMTP_Password"
            }), 400

        consumer_key = parts[0].strip()
        mpin = parts[1].strip()
        telegram_bot_token = parts[2].strip() if len(parts) > 2 else ""
        smtp_password = parts[3].strip() if len(parts) > 3 else ""

    if username != APP_USERNAME:

        return jsonify({
            "success": False,
            "error": "Invalid username."
        }), 401

    if not re.fullmatch(r"\d{6}", totp):

        return jsonify({
            "success": False,
            "error": "Enter a valid 6-digit TOTP."
        }), 400

    if not consumer_key:

        return jsonify({
            "success": False,
            "error": "Neo Consumer Key is required."
        }), 400

    if not mpin:

        return jsonify({
            "success": False,
            "error": "Neo MPIN is required."
        }), 400

    try:

        set_runtime_notification_credentials(
            telegram_bot_token=telegram_bot_token,
            smtp_password=smtp_password,
            neo_consumer_key=consumer_key,
            neo_mpin=mpin,
        )

        authenticate_web_user(totp)

    except ValueError as error:

        return jsonify({
            "success": False,
            "error": str(error)
        }), 400

    except Exception as error:

        print(
            f"Web login rejected: {error}"
        )

        return jsonify({
            "success": False,
            "error":
                "Kotak Neo authentication failed. "
                "Check Consumer Key, MPIN and TOTP."
        }), 401

    old_sid = _cookie_session_id()

    if old_sid:

        with WEB_SESSIONS_LOCK:
            WEB_SESSIONS.pop(
                old_sid,
                None
            )

    sid = _create_web_session()

    response = make_response(
        jsonify({
            "success": True
        })
    )

    response.set_cookie(
        WEB_COOKIE_NAME,
        sid,
        max_age=WEB_SESSION_TTL_SECONDS,
        httponly=True,
        secure=True,
        samesite="Lax",
        path="/",
    )

    return response


@app.route("/api/neo-auth/status")
def neo_auth_status():

    if not _get_authenticated_session():

        return jsonify({
            "error":
                "Authentication required"
        }), 401

    return jsonify({
        "reauth_required":
            bool(NEO_REAUTH_REQUIRED)
    })


@app.route("/api/neo-reauth", methods=["POST"])
def neo_reauth():

    if not _get_authenticated_session():

        return jsonify({
            "success": False,
            "error":
                "Application authentication required."
        }), 401

    totp = str(
        request.form.get("totp", "")
    ).strip()

    if not re.fullmatch(r"\d{6}", totp):

        return jsonify({
            "success": False,
            "error":
                "Enter a valid 6-digit TOTP."
        }), 400

    try:

        authenticate_web_user(totp)

        return jsonify({
            "success": True,
            "message":
                "Neo trading session refreshed."
        })

    except Exception as error:

        print(
            "Neo browser re-authentication rejected:",
            error
        )

        return jsonify({
            "success": False,
            "error":
                "Invalid or expired TOTP."
        }), 401



@app.route("/api/recommended-trades")
def api_recommended_trades():
    if not _get_authenticated_session():
        return jsonify({"error": "Unauthorized"}), 401
    return jsonify({
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "data": get_recommended_trades(),
    })


@app.route("/api/logout", methods=["POST"])
def api_logout():

    sid = _cookie_session_id()

    if sid:

        with WEB_SESSIONS_LOCK:
            WEB_SESSIONS.pop(
                sid,
                None
            )

    clear_runtime_notification_credentials()

    global NEO_CLIENT, NEO_REAUTH_REQUIRED, NEO_SESSION
    with NEO_AUTH_LOCK:
        old_client = NEO_CLIENT
        NEO_CLIENT = None
        NEO_REAUTH_REQUIRED = False
        for field in NEO_SESSION:
            NEO_SESSION[field] = None

    if old_client is not None:
        try:
            old_client.logout()
        except Exception:
            pass

    response = make_response(
        jsonify({
            "success": True
        })
    )

    response.delete_cookie(
        WEB_COOKIE_NAME,
        path="/"
    )

    return response


@app.route("/")
@app.route("/index.html")
def dashboard():

    if not _get_authenticated_session():

        return redirect("/login")

    return HTML_TEMPLATE


@app.route("/nse_futures_oi_live.json")
def live_json():

    if not _get_authenticated_session():

        return jsonify({
            "error":
                "Authentication required"
        }), 401

    path = Path(JSON_FILE)

    if not path.exists():

        return jsonify({
            "timestamp": None,
            "data": [],
            "total_contracts": len(MARKET),
            "live_contracts": 0,
        })

    try:

        # Return the existing JSON exactly as the monitor writes it.
        return path.read_text(
            encoding="utf-8"
        )

    except Exception as error:

        return jsonify({
            "error": str(error)
        }), 500


@app.route("/health")
def health():

    return jsonify({
        "status": "OK",
        "neo_authenticated":
            NEO_CLIENT is not None and _neo_session_is_usable(NEO_CLIENT),
        "reauth_required":
            bool(NEO_REAUTH_REQUIRED),
        "neo_session_saved":
            bool(NEO_SESSION.get("edit_token") and NEO_SESSION.get("edit_sid")),
        "json_exists":
            Path(JSON_FILE).exists(),
        "timestamp":
            datetime.now().isoformat(
                timespec="seconds"
            ),
    })


# ============================================================
# BACKGROUND NEO MONITOR
# ============================================================

MONITOR_THREAD = None
MONITOR_START_LOCK = threading.Lock()


def monitor_worker():

    global NEO_CLIENT

    try:

        validate()

        write_dashboard()

        Path(JSON_FILE).write_text(
            json.dumps({
                "timestamp": None,
                "data": [],
                "total_contracts": 0,
                "live_contracts": 0
            }, indent=2),
            encoding="utf-8"
        )

        async def runner():

            global NEO_CLIENT

            print(
                "PythonAnywhere Neo monitor started."
            )

            print(
                "Waiting for browser TOTP login..."
            )

            while NEO_CLIENT is None:

                await asyncio.sleep(0.5)

            client = NEO_CLIENT

            # ----------------------------------------
            # NSE F&O Scrip Master
            # ----------------------------------------

            raw_fo = get_master(
                client,
                "nse_fo"
            )

            text_fo = raw_fo.decode(
                "utf-8-sig",
                errors="replace"
            )

            df_fo = pd.read_csv(
                io.StringIO(text_fo),
                dtype=str,
                low_memory=False
            )

            # ----------------------------------------
            # NSE Cash Scrip Master
            # ----------------------------------------

            raw_cm = get_master(
                client,
                "nse_cm"
            )

            text_cm = raw_cm.decode(
                "utf-8-sig",
                errors="replace"
            )

            df_cm = pd.read_csv(
                io.StringIO(text_cm),
                dtype=str,
                low_memory=False
            )

            # ----------------------------------------
            # Discover Futures
            # ----------------------------------------

            futures = discover(df_fo)

            selected = select_active(
                futures
            )

            if len(selected) > MAX_SUBSCRIPTIONS:

                raise RuntimeError(
                    f"{len(selected)} contracts exceed "
                    f"{MAX_SUBSCRIPTIONS}."
                )

            initialize(
                selected
            )

            initialize_cm(
                selected,
                df_cm
            )

            load_yesterday_oi()

            print(
                f"Futures discovered: "
                f"{len(futures)}"
            )

            print(
                f"Active futures selected: "
                f"{len(selected)}"
            )

            print(
                "Starting live OI feed..."
            )

            # ----------------------------------------
            # Snapshot worker
            # ----------------------------------------

            asyncio.create_task(
                snapshot_loop(client)
            )

            # ----------------------------------------
            # Live Neo WebSocket
            # ----------------------------------------

            await websocket_loop(
                client,
                selected
            )

        asyncio.run(
            runner()
        )

    except Exception as error:

        print(
            "FATAL MONITOR ERROR:",
            error
        )


def start_background_monitor():

    global MONITOR_THREAD

    with MONITOR_START_LOCK:

        if (
            MONITOR_THREAD is not None
            and MONITOR_THREAD.is_alive()
        ):
            return

        MONITOR_THREAD = threading.Thread(
            target=monitor_worker,
            name="neo-monitor",
            daemon=True
        )

        MONITOR_THREAD.start()


# Start Neo/OI engine when PythonAnywhere
# loads the WSGI application.
start_background_monitor()


# PythonAnywhere WSGI entry point.
application = app
