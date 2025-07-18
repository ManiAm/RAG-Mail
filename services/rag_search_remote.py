
import config
from services.rag_search_api import RAG_SEARCH_REST_API_Client

#################

llm_info_map = {}

def get_llm_info(model_name):

    global llm_info_map

    if model_name in llm_info_map:
        return True, llm_info_map[model_name]

    rest_obj = RAG_SEARCH_REST_API_Client(url=config.rag_search_url)

    status, output = rest_obj.get_llm_info(model_name)
    if not status:
        return False, output

    if output:
        llm_info_map[model_name] = output

    return True, output


def llm_chat(question, llm_model, context="", session_id="default", timeout=5*60):

    rest_obj = RAG_SEARCH_REST_API_Client(url=config.rag_search_url)

    status, output = rest_obj.llm_chat(question, llm_model, context, session_id, timeout)
    if not status:
        return False, output

    answer = output.get("answer", None)

    if not answer:
        return False, "Did not get an answer from LLM"

    return True, answer

#################

def load_model(model_list):

    rest_obj = RAG_SEARCH_REST_API_Client(url=config.rag_search_url)

    return rest_obj.load_model(model_list)


def unload_model(model_name):

    rest_obj = RAG_SEARCH_REST_API_Client(url=config.rag_search_url)

    return rest_obj.unload_model(model_name)


def unload_all_models():

    rest_obj = RAG_SEARCH_REST_API_Client(url=config.rag_search_url)

    return rest_obj.unload_all_models()

#################

tokens_dict_cache = {}

def get_max_tokens(embed_model):

    global tokens_dict_cache

    if embed_model in tokens_dict_cache:
        return True, tokens_dict_cache[embed_model]

    rest_obj = RAG_SEARCH_REST_API_Client(url=config.rag_search_url)

    status, output = rest_obj.get_max_tokens(embed_model)
    if not status:
        return False, output

    if output:
        tokens_dict_cache[embed_model] = output

    return True, output


def split_document(text, chunk_size=1000, separators=None):

    rest_obj = RAG_SEARCH_REST_API_Client(url=config.rag_search_url)

    return rest_obj.split_document(text, chunk_size, separators)

#################

def create_collection(collection_name, embed_model):

    rest_obj = RAG_SEARCH_REST_API_Client(url=config.rag_search_url)

    return rest_obj.create_collection(collection_name, embed_model)

#################

def remove_embed_email_thread(collection_name, thread_id):

    rest_obj = RAG_SEARCH_REST_API_Client(url=config.rag_search_url)

    return rest_obj.delete_by_filter(collection_name, {"metadata.thread_id": thread_id})


def embed_email_thread(text_block, collection_name, embed_model, metadata={}, separators=None, chunk_size=None, timeout=5*60):

    rest_obj = RAG_SEARCH_REST_API_Client(url=config.rag_search_url)

    return rest_obj.embed_email_thread(text_block, collection_name, embed_model, metadata, separators, chunk_size, timeout)

