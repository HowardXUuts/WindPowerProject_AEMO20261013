"""Ask a language model for SQL, over any OpenAI-compatible chat endpoint.

DeepSeek, and most hosted models, serve `POST /chat/completions` with the same
request and response shape. That means this needs no SDK: `httpx` is already a
runtime dependency of the pipeline, so the assistant adds nothing to install.
The provider is a URL and a model name, which keeps it a configuration choice
rather than a rewrite.

**It also runs with no credential at all.** Recorded replies live in
`assistant/evals/recordings/`, keyed by a hash of the exact prompt. In `replay`
mode a recording is served instead of a request, so the tests and the CI eval
exercise the whole path — prompt assembly, SQL extraction, the guard, execution,
formatting — deterministically and for free. `--record` writes the recordings
when a key is present.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import httpx

LOG: Final = logging.getLogger("assistant.provider")

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
RECORDINGS: Final = REPO_ROOT / "assistant" / "evals" / "recordings"

TIMEOUT: Final = httpx.Timeout(60.0, connect=15.0)


@dataclass(frozen=True)
class Provider:
    """Where to send a chat completion, and what to call the model."""

    name: str
    base_url: str
    model: str
    env_var: str

    @property
    def api_key(self) -> str | None:
        key = os.environ.get(self.env_var, "").strip()
        return key or None


# Adding one is a row here, not a code change.
PROVIDERS: Final = {
    "deepseek": Provider(
        name="deepseek",
        base_url="https://api.deepseek.com/v1",
        model="deepseek-chat",
        env_var="DEEPSEEK_API_KEY",
    ),
    "openai": Provider(
        name="openai",
        base_url="https://api.openai.com/v1",
        model="gpt-4o-mini",
        env_var="OPENAI_API_KEY",
    ),
    "together": Provider(
        name="together",
        base_url="https://api.together.xyz/v1",
        model="Qwen/Qwen2.5-Coder-32B-Instruct",
        env_var="TOGETHER_API_KEY",
    ),
}

DEFAULT_PROVIDER: Final = "deepseek"


class ProviderError(Exception):
    """The model could not be reached, or returned something unusable."""


def resolve(name: str | None = None, model: str | None = None) -> Provider:
    """Return the configured provider, honouring ASSISTANT_PROVIDER/_MODEL."""
    chosen = name or os.environ.get("ASSISTANT_PROVIDER") or DEFAULT_PROVIDER
    if chosen not in PROVIDERS:
        raise ProviderError(f"unknown provider {chosen!r}; have {', '.join(sorted(PROVIDERS))}")
    provider = PROVIDERS[chosen]
    override = model or os.environ.get("ASSISTANT_MODEL")
    base = os.environ.get("ASSISTANT_BASE_URL")
    if override or base:
        provider = Provider(
            name=provider.name,
            base_url=base or provider.base_url,
            model=override or provider.model,
            env_var=provider.env_var,
        )
    return provider


def fingerprint(provider: Provider, system: str, question: str) -> str:
    """A stable key for one exact prompt, used as the recording's file name.

    The model is part of it: a recording made against one model is not evidence
    about another.
    """
    payload = json.dumps(
        {"model": provider.model, "system": system, "question": question},
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def recording_path(key: str, directory: Path | None = None) -> Path:
    return (directory or RECORDINGS) / f"{key}.json"


def load_recording(key: str, directory: Path | None = None) -> str | None:
    path = recording_path(key, directory)
    if not path.exists():
        return None
    return json.loads(path.read_text())["reply"]


def save_recording(
    key: str, provider: Provider, question: str, reply: str, directory: Path | None = None
) -> Path:
    directory = directory or RECORDINGS
    directory.mkdir(parents=True, exist_ok=True)
    path = recording_path(key, directory)
    path.write_text(
        json.dumps(
            {
                "provider": provider.name,
                "model": provider.model,
                # Kept for a human reading the file; the hash is the real key.
                "question": question,
                "reply": reply,
            },
            indent=2,
        )
        + "\n"
    )
    return path


def call(provider: Provider, system: str, question: str) -> str:
    """Send one chat completion and return the reply text."""
    key = provider.api_key
    if not key:
        raise ProviderError(
            f"{provider.env_var} is not set. Put it in .env (gitignored), or run "
            "with --replay to use the recorded replies."
        )

    body = {
        "model": provider.model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": question},
        ],
        # SQL generation wants the same answer every time, not a creative one.
        "temperature": 0,
        "max_tokens": 1000,
        "stream": False,
    }
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

    try:
        response = httpx.post(
            f"{provider.base_url}/chat/completions", json=body, headers=headers, timeout=TIMEOUT
        )
        response.raise_for_status()
        payload = response.json()
    except httpx.HTTPStatusError as exc:
        raise ProviderError(
            f"{provider.name} returned HTTP {exc.response.status_code}: "
            f"{exc.response.text[:200]}"
        ) from exc
    except httpx.HTTPError as exc:
        raise ProviderError(f"could not reach {provider.name}: {exc}") from exc

    try:
        return payload["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ProviderError(f"unexpected response shape from {provider.name}: {payload}") from exc


def ask(
    system: str,
    question: str,
    provider: Provider | None = None,
    replay: bool = False,
    record: bool = False,
    directory: Path | None = None,
) -> tuple[str, str]:
    """Return ``(reply, source)`` where source is "recording" or the provider name.

    Replay is preferred when asked for, and is also the automatic fallback when
    no credential is configured: a repository anyone can clone and run green is
    worth more than a hard failure that only the key holder can get past.
    """
    chosen = provider or resolve()
    key = fingerprint(chosen, system, question)

    if replay or not chosen.api_key:
        reply = load_recording(key, directory)
        if reply is not None:
            return reply, "recording"
        if replay:
            raise ProviderError(
                f"no recording for this question under {chosen.model} "
                f"(expected {recording_path(key, directory).name}). "
                "Run with --record and a key to create it."
            )
        raise ProviderError(
            f"{chosen.env_var} is not set and there is no recording for this question. "
            "Set the key, or ask one of the questions in assistant/evals/questions.json."
        )

    reply = call(chosen, system, question)
    if record:
        saved = save_recording(key, chosen, question, reply, directory)
        LOG.info("recorded %s", saved.name)
    return reply, chosen.name
