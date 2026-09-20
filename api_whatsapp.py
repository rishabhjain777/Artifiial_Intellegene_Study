"""
WhatsApp Cloud API Client (Official Meta Free Tier)
--------------------------------------------------
Sends WhatsApp messages via Meta's Official WhatsApp Business Cloud API.

Free Tier:
- 1,000 free service conversations per month.
- Official API: Zero risk of phone number bans.
- Server-friendly: Works headless on any cloud server or background worker.

Prerequisites:
1. Meta Developer Account: https://developers.facebook.com
2. A WhatsApp Business App created in the developer portal.
3. WHATSAPP_TOKEN and WHATSAPP_PHONE_NUMBER_ID configured in your `.env` file or passed via arguments.
"""

import os
import sys
import argparse
import json
import requests
from dotenv import load_dotenv

# Load variables from .env file if present
load_dotenv()

# Meta API Base URL
GRAPH_API_VERSION = os.getenv("WHATSAPP_API_VERSION", "v20.0")
GRAPH_API_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"


def clean_phone_number(phone: str) -> str:
    """
    Meta Cloud API expects digits only with country code (no '+' or spaces).
    Example: '+91 87470 05100' -> '918747005100'
    """
    return "".join(ch for ch in phone if ch.isdigit())


class WhatsAppCloudAPI:
    def __init__(self, token: str = None, phone_number_id: str = None):
        """
        Initialize the WhatsApp Cloud API client.
        
        :param token: Meta Graph API Access Token (temporary or permanent system user token)
        :param phone_number_id: Phone Number ID from the Meta Developer Dashboard (not the raw phone number)
        """
        self.token = token or os.getenv("WHATSAPP_TOKEN")
        self.phone_number_id = phone_number_id or os.getenv("WHATSAPP_PHONE_NUMBER_ID")

        if not self.token or not self.phone_number_id:
            raise ValueError(
                "Missing credentials! Please provide WHATSAPP_TOKEN and WHATSAPP_PHONE_NUMBER_ID "
                "either in your .env file or as constructor arguments."
            )

        self.endpoint = f"{GRAPH_API_URL}/{self.phone_number_id}/messages"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def send_template(
        self,
        to_phone: str,
        template_name: str = "hello_world",
        language_code: str = "en_US",
        components: list = None,
    ) -> dict:
        """
        Sends a pre-approved Meta message template.
        Note: The default 'hello_world' template is available immediately on all test numbers.

        :param to_phone: Recipient phone number (e.g. '918747005100')
        :param template_name: Name of the approved template (default: 'hello_world')
        :param language_code: Language code (default: 'en_US')
        :param components: Optional dynamic parameters for body/header/buttons
        """
        recipient = clean_phone_number(to_phone)
        payload = {
            "messaging_product": "whatsapp",
            "to": recipient,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"code": language_code},
            },
        }
        if components:
            payload["template"]["components"] = components

        print(f"[*] Sending template '{template_name}' to {recipient}...")
        response = requests.post(self.endpoint, headers=self.headers, json=payload)
        return self._handle_response(response)

    def send_text(self, to_phone: str, message: str, preview_url: bool = False) -> dict:
        """
        Sends a free-form text message.
        Note: Free-form text can only be sent within an open 24-hour service window 
        (i.e., when the user has messaged your business number in the past 24 hours).
        For initiating conversations, use `send_template`.

        :param to_phone: Recipient phone number (e.g. '918747005100')
        :param message: Text message content
        :param preview_url: Whether to generate a link preview
        """
        recipient = clean_phone_number(to_phone)
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": recipient,
            "type": "text",
            "text": {
                "preview_url": preview_url,
                "body": message,
            },
        }

        print(f"[*] Sending text message to {recipient}...")
        response = requests.post(self.endpoint, headers=self.headers, json=payload)
        return self._handle_response(response)

    def _handle_response(self, response: requests.Response) -> dict:
        try:
            data = response.json()
        except Exception:
            data = {"raw_text": response.text}

        if response.status_code in (200, 201):
            print("[+] Message dispatched successfully!")
            print(f"    Message ID: {data.get('messages', [{}])[0].get('id', 'N/A')}")
            return {"success": True, "data": data}
        else:
            error_info = data.get("error", {})
            error_msg = error_info.get("message", response.text)
            error_code = error_info.get("code")
            error_subcode = error_info.get("error_subcode")
            print(f"[-] API Error ({response.status_code}): {error_msg}")
            if error_code:
                print(f"    Code: {error_code}, Subcode: {error_subcode}")
                self._explain_common_error(error_code, error_subcode)
            return {"success": False, "error": data}

    @staticmethod
    def _explain_common_error(code: int, subcode: int):
        """Helper to explain common Meta API errors."""
        if code == 190:
            print("    -> Tip: Your Access Token has expired. Generate a new token in Meta App Dashboard.")
        elif code == 131030:
            print("    -> Tip: Recipient phone number is not in your allowed test recipients list.")
            print("            Add the recipient number under WhatsApp > API Setup > 'To' field in Meta Dashboard.")
        elif code == 132000:
            print("    -> Tip: The template name does not exist or has not been approved yet.")
        elif code == 131047:
            print("    -> Tip: Outside 24h customer window. You must use `send_template()` to initiate contact.")


def main():
    parser = argparse.ArgumentParser(description="Send WhatsApp messages via Meta Cloud API.")
    parser.add_argument("-p", "--phone", required=True, help="Recipient phone number with country code (e.g. 918747005100)")
    parser.add_argument("-m", "--message", help="Free-form message body (only valid within 24h conversation window)")
    parser.add_argument("-t", "--template", default="hello_world", help="Template name (default: hello_world)")
    parser.add_argument("-l", "--lang", default="en_US", help="Template language code (default: en_US)")
    parser.add_argument("--token", help="Meta Access Token (overrides WHATSAPP_TOKEN env)")
    parser.add_argument("--phone-id", help="Meta Phone Number ID (overrides WHATSAPP_PHONE_NUMBER_ID env)")

    args = parser.parse_args()

    try:
        client = WhatsAppCloudAPI(token=args.token, phone_number_id=args.phone_id)
    except ValueError as e:
        print(f"[-] Configuration Error: {e}")
        print("\nSet them in your .env file:")
        print("  WHATSAPP_TOKEN=your_meta_access_token")
        print("  WHATSAPP_PHONE_NUMBER_ID=your_phone_number_id")
        sys.exit(1)

    if args.message:
        # User specified custom text
        client.send_text(to_phone=args.phone, message=args.message)
    else:
        # Send pre-approved template (hello_world by default)
        client.send_template(to_phone=args.phone, template_name=args.template, language_code=args.lang)


if __name__ == "__main__":
    main()
