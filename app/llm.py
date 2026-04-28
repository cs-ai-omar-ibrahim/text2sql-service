from llama_index.llms.azure_openai import AzureOpenAI

from app.config import get_settings


def build_llm() -> AzureOpenAI:
    settings = get_settings()
    return AzureOpenAI(
        deployment_name=settings.azure_openai_deployment,
        model="gpt-4o-mini",
        azure_endpoint=settings.azure_openai_endpoint,
        api_key=settings.azure_openai_api_key,
        api_version=settings.azure_openai_api_version,
        temperature=0.0,
    )
