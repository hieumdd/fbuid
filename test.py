import requests

payload = {
    'uids': [100012364151531, 100004986047543]
}
r = requests.get('http://localhost:8080', json=payload)

print(r.json())