from twilio.rest import Client

from dotenv import load_dotenv
import os
load_dotenv()
ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
FROM_NUMBER = os.getenv("FROM_NUMBER")
TO_NUMBER = os.getenv("TO_NUMBER")

client = Client(ACCOUNT_SID, AUTH_TOKEN)

def send_sms():
    message = client.messages.create(
        body="sms_feedback_surveys",
        from_=FROM_NUMBER,
        to=TO_NUMBER,
    )

    print(f"Message sent successfully! SID: {message.sid}")


if __name__ == "__main__":
    # Send the SMS
    send_sms()
