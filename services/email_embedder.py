
import config
from services.rag_talk_api import RAG_TALK_REST_API_Client


def create_collection(collection_name, embed_model):

    rest_obj = RAG_TALK_REST_API_Client(url=config.rag_talk_url)

    return rest_obj.create_collection(collection_name, embed_model)


def remove_embed_email_thread(collection_name, thread_id):

    rest_obj = RAG_TALK_REST_API_Client(url=config.rag_talk_url)

    return rest_obj.delete_by_filter(collection_name, {"metadata.thread_id": thread_id})


def embed_email_thread(text_block, collection_name, embed_model, metadata={}, separators=None, chunk_size=None):

    rest_obj = RAG_TALK_REST_API_Client(url=config.rag_talk_url)

    return rest_obj.embed_email_thread(text_block, collection_name, embed_model, metadata, separators, chunk_size)
