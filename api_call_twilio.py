from twilio.rest import Client
from dotenv import load_dotenv
import os
load_dotenv()
ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
FROM_NUMBER = os.getenv("FROM_NUMBER")
TO_NUMBER = os.getenv("TO_NUMBER")

client = Client(ACCOUNT_SID, AUTH_TOKEN)

def make_call():
    call = client.calls.create(
        # twiml="<Response><Say>Hello! This is an automated call from your Python script.</Say></Response>",
        url="https://webhooks.twilio.com/v1/Voice/Template/voice_auto_response",
        from_=FROM_NUMBER,
        to=TO_NUMBER,
    )
    print(f"Call initiated successfully! SID: {call.sid}")


if __name__ == "__main__":
    # Place the call
    make_call()