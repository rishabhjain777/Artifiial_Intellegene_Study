import requests

API_URL = "http://127.0.0.1:8000/api/v1/call"

payload = {
    "phone_number": "+918747005100"
}

response = requests.post(API_URL, json=payload)
print(response.json())