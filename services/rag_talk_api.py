
import getpass
import json

from services.rest_client import REST_API_Client


class RAG_TALK_REST_API_Client(REST_API_Client):

    def __init__(self,
                 url,
                 api_ver=None,
                 base=None,
                 user=getpass.getuser()):

        super().__init__(url, api_ver, base, user)


    def create_collection(self, collection_name, embed_model):

        url = f"{self.baseurl}/api/v1/rag/create-collection"

        json = {
            "collection_name": collection_name,
            "embed_model": embed_model
        }

        return self.request("POST", url, json=json)


    def delete_by_filter(self, collection_name, filter_dict):

        url = f"{self.baseurl}/api/v1/rag/del-by-filter"

        json = {
            "collection_name": collection_name,
            "filter": filter_dict
        }

        return self.request("POST", url, json=json)


    def embed_email_thread(self, text_block, collection_name, embed_model, metadata={}, separators=None, chunk_size=None, timeout=30):

        url = f"{self.baseurl}/api/v1/rag/paste"

        payload = {
            "text": text_block,
            "collection_name": collection_name,
            "embed_model": embed_model,
            "metadata": json.dumps(metadata)
        }

        if separators:
            payload["separators"] = separators

        if chunk_size:
            payload["chunk_size"] = chunk_size

        return self.request("POST", url, json=payload, timeout=timeout)
