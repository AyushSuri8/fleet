#!/usr/bin/env python3
"""Weekly OAuth refresh-token rotation helper (terminal-only).

Exchanges a Google consent `?code=` for a refresh token and PRINTS it.
Saves nothing, touches Cloudflare not at all — you paste the token into
the Worker secret (SHELL_X_REFRESH) yourself.

Run it with no arguments and it walks you through everything.
"""
import argparse
import base64
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

AUTH_BASE = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
SCOPE = "https://www.googleapis.com/auth/cloud-platform"
REDIRECT = "http://localhost"  # must match the OAuth client + consent URL exactly
NODES = ["shell-a", "shell-b", "shell-c", "shell-d"]

REPO = Path(__file__).resolve().parent.parent
DEFAULT_CLIENT = REPO / ".secrets" / "oauth-client.json"

STATUS_URL = "https://fleet-frontdoor.ayushsuri37.workers.dev/fleet/status"
OAUTH_HELP_URL = "https://fleet-frontdoor.ayushsuri37.workers.dev/fleet/oauth-help"


def instructions():
    print(
        """
==========================================================
 WEEKLY REFRESH-TOKEN ROTATION  (one shell account at a time)
==========================================================
Why: the OAuth client is in Testing mode, so refresh tokens
die every 7 days (watchdog raises AUTH-EXPIRED per node).

Steps:
 1. See which nodes are dead (optional):
      curl -s "%s" | python3 -m json.tool
    or per node:  curl -s "%s?node=shell-x"

 2. This script prints the consent URL -> open it in a browser
    and sign in as the GOOGLE ACCOUNT that owns that shell
    (all 4 accounts must stay Test users on the consent screen).

 3. After "Allow" you land on  http://localhost/?code=4/0A...  (the
    page itself errors — that is fine). Copy the WHOLE address.

 4. Paste it here. The script exchanges the code (codes are
    single-use and short-lived) and prints the new refresh token.

 5. Copy the printed token into the Worker secret yourself:
      Cloudflare dashboard -> Workers & Pages -> fleet-frontdoor
        -> Settings -> Variables and Secrets -> SHELL_<A|B|C|D>_REFRESH
        -> Edit -> paste -> Save   (no redeploy; next cron tick picks up)

 6. Verify:  curl -s "%s?node=shell-x"   ->  "mint":"ok"

This script prints the token to the terminal only. It never writes
files, never calls Cloudflare, and never prints the client secret.
==========================================================
""" % (STATUS_URL, OAUTH_HELP_URL, OAUTH_HELP_URL)
    )


def load_client(path):
    p = Path(path).expanduser()
    if not p.exists():
        print(f"ERROR: OAuth client file not found: {p}")
        print("Expected the Desktop-app client JSON you downloaded from Google")
        print('(shape: {"installed":{"client_id":...,"client_secret":...}}).')
        sys.exit(1)
    try:
        installed = json.loads(p.read_text())["installed"]
        return installed["client_id"], installed["client_secret"]
    except (KeyError, json.JSONDecodeError) as e:
        print(f"ERROR: {p} is not a valid 'installed' OAuth client JSON: {e}")
        sys.exit(1)


def consent_url(client_id):
    q = urllib.parse.urlencode(
        {
            "client_id": client_id,
            "redirect_uri": REDIRECT,
            "response_type": "code",
            "scope": SCOPE,
            "access_type": "offline",
            "prompt": "consent",
        }
    )
    return f"{AUTH_BASE}?{q}"


def post_form(fields):
    data = urllib.parse.urlencode(fields).encode()
    req = urllib.request.Request(
        TOKEN_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        try:
            return e.code, json.loads(body)
        except json.JSONDecodeError:
            return e.code, {"error": body[:300]}
    except urllib.error.URLError as e:
        return 0, {"error": f"network: {e}"}


def extract_code(raw):
    """Accepts the full localhost URL, a quoted URL, or a bare code."""
    raw = raw.strip().strip("'\"")
    if not raw:
        return None
    if "code=" in raw:
        try:
            q = urllib.parse.urlparse(raw).query or raw.split("?", 1)[1]
            val = urllib.parse.parse_qs(q).get("code", [None])[0]
            if val:
                return val.strip()
        except ValueError:
            pass
    if raw.startswith("4/") or raw.startswith("1//"):  # Google code shapes
        return raw
    return None

def hint_for(err):
    return {
        "invalid_grant": (
            "code already used, expired (codes live minutes), or the account's\n"
            "     previous refresh token just hit the 7-day Testing expiry -> redo\n"
            "     the consent step and paste a FRESH ?code= URL"
        ),
        "redirect_uri_mismatch": (
            "the OAuth client must allow exactly http://localhost (no trailing /)"
        ),
        "access_denied": (
            "that Google account is not a Test user on the OAuth consent screen\n"
            "     (add it: APIs & Services -> OAuth consent screen -> Test users)"
        ),
        "invalid_client": "client_id/client_secret wrong — check the client JSON",
    }.get(err, "see Google's error above")


def print_token_block(token, node):
    suffix = node.split("-")[1].upper() if node else "X"
    line = "-" * 64
    print()
    print(line)
    print(f"  NEW REFRESH TOKEN  ({node or 'node?'})  — copy the line below")
    print(line)
    print(token)
    print(line)
    print(
        f"\nPut it in the Worker secret SHELL_{suffix}_REFRESH:\n"
        "  Cloudflare dashboard -> Workers & Pages -> fleet-frontdoor\n"
        "    -> Settings -> Variables and Secrets -> SHELL_"
        f"{suffix}_REFRESH -> Edit -> paste -> Save\n"
        "  (no redeploy; next cron tick picks it up)\n\n"
        f"Verify: curl -s \"{OAUTH_HELP_URL}?node={node or 'shell-x'}\"  ->  \"mint\":\"ok\"\n"
        "This script saved nothing — once you close the terminal the token\n"
        "lives only in the secret you just pasted it into."
    )


def do_exchange(client, code, node):
    status, resp = post_form(
        {
            "client_id": client[0],
            "client_secret": client[1],
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": REDIRECT,
        }
    )
    token = resp.get("refresh_token")
    if status == 200 and token:
        print_token_block(token, node)
        return 0
    err = resp.get("error", "?")
    print(f"\nEXCHANGE FAILED (HTTP {status}): {err}")
    if resp.get("error_description"):
        print(f"  detail: {resp['error_description']}")
    print(f"  hint: {hint_for(err)}")
    return 3


def do_check(path):
    """Mint-test an existing refresh token. Prints verdict only, never the token."""
    p = Path(path).expanduser()
    if not p.exists():
        print(f"ERROR: token file not found: {p}")
        return 1
    token = p.read_text().strip()
    if not token:
        print(f"ERROR: {p} is empty")
        return 1
    client = load_client(DEFAULT_CLIENT)
    status, resp = post_form(
        {
            "client_id": client[0],
            "client_secret": client[1],
            "refresh_token": token,
            "grant_type": "refresh_token",
        }
    )
    if status == 200 and resp.get("access_token"):
        email = None
        idt = resp.get("id_token")
        if idt:
            try:
                payload = idt.split(".")[1]
                payload += "=" * (-len(payload) % 4)
                email = json.loads(base64.urlsafe_b64decode(payload)).get("email")
            except Exception:
                pass
        print(f"MINT OK  ({p.name})" + (f"  account: {email}" if email else ""))
        print("Token is alive — no rotation needed for this one.")
        return 0
    err = resp.get("error", "?")
    print(f"MINT FAILED (HTTP {status}): {err}  ({p.name})")
    if err == "invalid_grant":
        print("  7-day Testing expiry hit (or token revoked) -> rotate: run this")
        print("  script with no args and redo the consent for that node.")
    return 4


def main():
    ap = argparse.ArgumentParser(
        description="Exchange a Google consent code for a refresh token; prints it, saves nothing.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Run with no arguments for the full walkthrough.",
    )
    ap.add_argument("url", nargs="?", help="the http://localhost/?code=... callback URL (or bare code)")
    ap.add_argument("--node", choices=NODES, help="which shell this consent is for (labels the output)")
    ap.add_argument("--client", default=str(DEFAULT_CLIENT), help="path to oauth-client.json (installed)")
    ap.add_argument("--url-only", action="store_true", help="print the consent URL and exit")
    ap.add_argument("--check", metavar="TOKEN_FILE", help="mint-test an existing refresh token file, verdict only")
    args = ap.parse_args()

    if args.check:
        sys.exit(do_check(args.check))

    instructions()
    client = load_client(args.client)

    url = consent_url(client[0])
    print("Consent URL (open in a browser, sign in as the account that owns the shell):\n")
    print(url)
    print()
    if args.url_only:
        return

    node = args.node
    if not node and not args.url:
        pick = input("Which node is this for? [shell-a/b/c/d] (Enter = skip label): ").strip().lower()
        if pick in NODES:
            node = pick
        elif pick:
            print(f"NOTE: '{pick}' is not shell-a/b/c/d — continuing without a label.")

    raw = args.url
    if not raw:
        try:
            raw = input("\nPaste the http://localhost/?code=... URL (or just the code): ")
        except EOFError:
            raw = ""
    code = extract_code(raw)
    if not code:
        print("\nERROR: could not find a ?code= in what you pasted.")
        print("Copy the FULL address-bar URL after allowing consent, e.g.")
        print("  http://localhost/?code=4/0Ab_5q...&scope=https://www.googleapis.com/auth/cloud-platform")
        sys.exit(2)

    print("\nExchanging code with Google ...")
    sys.exit(do_exchange(client, code, node))


if __name__ == "__main__":
    main()

