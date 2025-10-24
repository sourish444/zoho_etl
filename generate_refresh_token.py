import os
import requests

client_id = "1000.SHTYCLXDECLMFPMY9RFMRUPV2KS48E"
client_secret = os.getenv('SECRET')
redirect_uri = "https://www.zoho.in"
auth_code = "1000.6e25ef4a8453108abda21c52c22b12f1.a514856df7a652ec72ecf6999fba8d56"

url = "https://accounts.zoho.in/oauth/v2/token"

params = {
    "grant_type": "authorization_code",
    "client_id": client_id,
    "client_secret": client_secret,
    "redirect_uri": redirect_uri,
    "code": auth_code
}

response = requests.post(url, data=params)

print("🔐 Token Response:")
print(response.status_code)
print(response.json())
