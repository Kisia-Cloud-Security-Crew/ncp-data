import base64
import hashlib
import hmac
import json
import time

import requests


# 시그니처 생성 함수
def make_signature(method, uri, access_key, secret_key):
    timestamp = str(int(time.time() * 1000))
    secret_key = bytes(secret_key, 'UTF-8')
    message = method + " " + uri + "\n" + timestamp + "\n" + access_key
    message = bytes(message, 'UTF-8')
    signingKey = base64.b64encode(hmac.new(secret_key, message, digestmod=hashlib.sha256).digest())
    return signingKey, timestamp

# API 요청 함수
def make_api_request(api_url, uri, access_key, secret_key, api_key, body):
    signature, timestamp = make_signature("POST", uri, access_key, secret_key)

    headers = {
        'x-ncp-apigw-timestamp': timestamp,
        'x-ncp-apigw-api-key': api_key,
        'x-ncp-iam-access-key': access_key,
        'x-ncp-apigw-signature-v2': signature.decode('UTF-8'),
        'Content-Type': 'application/json'
    }

    response = requests.post(api_url, headers=headers, data=json.dumps(body))
    return response