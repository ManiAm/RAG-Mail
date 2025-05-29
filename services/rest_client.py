
import os
import json
import logging
import requests
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

load_dotenv()


class REST_API_Client():

    def __init__(self,
                 url,
                 api_ver=None,
                 base=None,
                 user=None):

        self.baseurl = url

        if api_ver:
            self.baseurl += f'/{api_ver}'

        if base:
            self.baseurl += f'/{base}'

        self.user = user

        self.headers = {
            'Content-Type': 'application/json',
            'accept': 'application/json',
        }

        access_token = os.getenv('API_TOKEN', None)
        if access_token:
            self.headers['Authorization'] = f'Bearer {access_token}'


    def request(self, method, url, timeout=10, verify=True, stream=False, decode=True, **kwargs):

        try:
            response = requests.request(method,
                                        url,
                                        headers=self.headers,
                                        timeout=timeout,
                                        verify=verify,
                                        stream=stream,
                                        **kwargs)
        except Exception as E:
            return False, str(E)

        try:
            response.raise_for_status()
        except Exception as E:
            return False, f'Return code={response.status_code}, {E}\n{response.text}'

        if stream:
            return True, response

        if not decode:
            return True, response.content

        try:
            content_decoded = response.content.decode('utf-8')
            if not content_decoded:
                return True, {}

            data_dict = json.loads(content_decoded)
        except Exception as E:
            return False, f'Error while decoding content: {E}'

        return True, data_dict
