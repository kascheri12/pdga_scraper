import requests
from datetime import datetime

class PDGA:
    pdga_api_url = "https://api.pdga.com"
    password = ''
    username = ''
    session_name = ''
    sessid = ''

    def __init__(self,username,password):
        self.username = username
        self.password = password

    def get_sessid(self):
        if self.password:
            json = { 'username': self.username, 'password': self.password}
            headers = { 'Content-Type': "application/json"}
            response = requests.request("POST", self.pdga_api_url + "/services/json/user/login", headers=headers, json=json)

            if response.status_code == 200:
                self.session_name = response.json()['session_name']
                self.sessid = response.json()['sessid']
                print(response.json())
            elif response.status_code == 406:
                raise Exception('Api Key is invalid',response.status_code,response.text)
            else:
                raise Exception('Error calling PDGA api login endpoint',response.status_code,response.text)
        else:
            raise Exception('Api Key not defined')

    def get_player(self,pdga_number,retry=True):
        if not self.sessid:
            self.get_sessid()

        headers = {
            'Cookie': self.session_name + '=' + self.sessid
        }

        try:
            response = requests.request("GET", self.pdga_api_url + "/services/json/players?pdga_number=" + str(pdga_number), headers=headers)
        except requests.exceptions.RequestException as e:  # This is the correct syntax
            raise SystemExit(e)

        if response.status_code == 200:
            return response.json()['players']
        elif response.status_code == 401 and retry:
            print('SESSID has expired')
            self.get_sessid()
            return self.get_player(pdga_number,False)
        else:
            raise Exception('Error calling PDGA api endpoint',response.status_code,response.text)

    def get_players(self,limit=10,offset=0,retry=True):
        if not self.sessid:
            self.get_sessid()

        headers = {
            'Cookie': self.session_name + '=' + self.sessid
        }

        try:
            response = requests.request("GET", self.pdga_api_url + "/services/json/players?limit=" + str(limit) + "&offset=" + str(offset), headers=headers)
        except requests.exceptions.RequestException as e:  # This is the correct syntax
            raise SystemExit(e)

        if response.status_code == 200:
            try:
                return response.json()['players']
            except requests.exceptions.RequestException as e:
                raise SystemExit(e)

        elif response.status_code == 401 and retry:
            print('SESSID has expired')
            self.get_sessid()
            return self.get_players(limit,offset,False)
        else:
            raise Exception('Error calling PDGA api endpoint',response.status_code,response.text)
