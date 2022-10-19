import os

import requests


class InergySource:

    @staticmethod
    def authenticate():
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        res = requests.post(url=f"{os.getenv('INERGY_BASE_URL')}/account/login",
                            headers=headers,
                            data={"grant_type": 'password', "username": os.getenv('INERGY_USERNAME'),
                                  "password": os.getenv('INERGY_PASSWORD')}, timeout=15)

        if res.ok:
            return res.json()
        else:
            res.raise_for_status()

    @staticmethod
    def insert_elements(token, data):
        headers = {'Authorization': f'Bearer {token}'}
        res = requests.post(url=f"{os.getenv('INERGY_BASE_URL')}/common/insert_element", headers=headers, json=data,
                            timeout=15)

        if res.ok:
            return res.json()
        else:
            res.raise_for_status()
