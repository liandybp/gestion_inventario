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


def _tighten_schema(schema: dict) -> dict:
    """Endurece el JSON Schema de pydantic para guiar mejor al modelo.

    Añade ``additionalProperties: false`` y ``required`` con todas las
    propiedades, recursivamente (objetos, arrays y ``$defs``). Se usa como
    ``json_schema`` SIN ``strict``: el modo strict de OpenAI rechaza los
    ``anyOf: [.., null]`` de los campos Optional y los dicts libres.
    """
    props = schema.get("properties")
    if isinstance(props, dict):
        schema["additionalProperties"] = False
        schema["required"] = list(props.keys())
        for value in props.values():
            if not isinstance(value, dict):
                continue
            if value.get("type") == "object":
                _tighten_schema(value)
            elif value.get("type") == "array" and isinstance(value.get("items"), dict):
                _tighten_schema(value["items"])
    for ref in (schema.get("$defs") or {}).values():
        if isinstance(ref, dict):
            _tighten_schema(ref)
    return schema


def build_planner(model: Type[BaseModel]) -> ChatOpenAI:
    """LLM configurado para devolver el modelo Pydantic forzado por JSON Schema.

    Funciona con llama.cpp (dev) y con las APIs OpenAI-compatibles (prod). Se
    usa ``json_schema`` SIN ``strict``: PlanModel contiene campos Optional
    (``anyOf: [.., null]``) y dicts libres (``additionalProperties: true``) que
    el modo strict de OpenAI rechaza con 400. La salida se re-valida con pydantic
    y se reintenta en ``_plan_node``, así que el modo no-estricto es seguro.
    """
    schema = _tighten_schema(model.model_json_schema())
    return build_llm(model_kwargs={
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": model.__name__,
                "schema": schema,
            },
        }
    })
