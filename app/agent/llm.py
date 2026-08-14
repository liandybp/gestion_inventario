from __future__ import annotations

from typing import Type

from langchain_openai import ChatOpenAI
from pydantic import BaseModel

from app.agent import config


def build_llm(**kwargs) -> ChatOpenAI:
    # api_key: llama-swap local no la valida (env vacía); las APIs externas la
    # requieren y se setea en el .env / docker-compose de producción.
    api_key = config.LLM_API_KEY or "sk-local-no-key"
    return ChatOpenAI(
        model=config.LLM_MODEL,
        base_url=config.LLM_BASE_URL,
        api_key=api_key,
        temperature=0,
        timeout=config.LLM_TIMEOUT_S,
        **kwargs,
    )


def _strict_schema(schema: dict) -> dict:
    """Hace un JSON Schema compatible con el modo strict de OpenAI.

    OpenAI exige `additionalProperties: false` y que `required` incluya TODAS
    las propiedades (de lo contrario devuelve 400). Se aplica recursivamente a
    objetos y arrays anidados, incluidos los `$defs` de pydantic.
    """
    props = schema.get("properties")
    if isinstance(props, dict):
        schema["additionalProperties"] = False
        schema["required"] = list(props.keys())
        for value in props.values():
            if not isinstance(value, dict):
                continue
            if value.get("type") == "object":
                _strict_schema(value)
            elif value.get("type") == "array" and isinstance(value.get("items"), dict):
                _strict_schema(value["items"])
    for ref in (schema.get("$defs") or {}).values():
        if isinstance(ref, dict):
            _strict_schema(ref)
    return schema


def build_planner(model: Type[BaseModel]) -> ChatOpenAI:
    """LLM configurado para devolver el modelo Pydantic forzado por JSON Schema.

    Funciona con llama.cpp (dev) y con las APIs OpenAI-compatibles (prod): el
    schema se adapta a los requisitos strict de OpenAI (C1).
    """
    schema = _strict_schema(model.model_json_schema())
    return build_llm(model_kwargs={
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": model.__name__,
                "schema": schema,
                "strict": True,
            },
        }
    })
