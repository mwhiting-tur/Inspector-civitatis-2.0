import requests

API_KEY = "88a5369ee04943cba850fe2422d54400"
# Probamos con un dominio estándar (google.com)
url = 'https://api.similarweb.com/v1/segment/traffic-and-engagement/describe/?api_key=88a5369ee04943cba850fe2422d54400&userOnlySegments=false' 

response = requests.get(url)
print(f"Status Code: {response.status_code}")
if response.status_code == 200:
    print("✅ Tu API Key funciona para Dominios. El problema es que NO tienes acceso a 'Custom Segments'.")
else:
    print(f"❌ Error {response.status_code}: {response.text}")