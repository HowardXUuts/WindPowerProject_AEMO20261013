"""Ask a question about Australian wind generation; get an answer and the SQL.

    python assistant/qa.py "which farm earned the most last week?"
    python assistant/qa.py --replay "what is the fleet capacity factor?"
    python assistant/qa.py --sql-only "how many farms are there?"

The query is always shown with the answer. A number without the query that
produced it is a number nobody can check, and this one was written by a
language model.

No credential is needed to try it: without one, recorded replies are served for
the questions in `assistant/evals/questions.json`. Set `DEEPSEEK_API_KEY` in
`.env` to ask anything else. See `--help` for other providers.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Final

from guard import GOLD_DIR, Result, UnsafeQuery, run
from provider import PROVIDERS, Provider, ProviderError, ask, resolve

LOG: Final = logging.getLogger("assistant.qa")

ASSISTANT_DIR: Final = Path(__file__).resolve().parent
SCHEMA: Final = ASSISTANT_DIR / "schema.md"
SYSTEM_PROMPT: Final = ASSISTANT_DIR / "prompts" / "system.md"


def system_prompt() -> str:
    """The instructions and the schema, as one system message."""
    return SYSTEM_PROMPT.read_text().rstrip() + "\n\n" + SCHEMA.read_text()


def render(result: Result) -> str:
    """Format a result as a plain text table."""
    if not result.rows:
        return "(no rows)"

    def cell(value: object) -> str:
        if value is None:
            return "—"
        if isinstance(value, float):
            return f"{value:,.4f}".rstrip("0").rstrip(".")
        if isinstance(value, int):
            return f"{value:,}"
        return str(value)

    table = [list(result.columns)] + [[cell(v) for v in row] for row in result.rows]
    widths = [max(len(row[i]) for row in table) for i in range(len(result.columns))]
    lines = [
        "  ".join(value.ljust(widths[i]) for i, value in enumerate(table[0])),
        "  ".join("-" * w for w in widths),
    ]
    lines += ["  ".join(v.ljust(widths[i]) for i, v in enumerate(row)) for row in table[1:]]
    if result.truncated:
        lines.append(f"... truncated; the query returned more than {len(result.rows)} rows")
    return "\n".join(lines)


def answer(
    question: str,
    provider: Provider | None = None,
    gold_dir: Path = GOLD_DIR,
    replay: bool = False,
    record: bool = False,
    recordings: Path | None = None,
) -> tuple[Result, str]:
    """Question in, executed result out. Returns ``(result, source)``."""
    reply, source = ask(
        system_prompt(), question, provider, replay=replay, record=record, directory=recordings
    )
    return run(reply, gold_dir), source


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"providers: {', '.join(sorted(PROVIDERS))} "
        f"(set ASSISTANT_PROVIDER, or --provider)",
    )
    parser.add_argument("question", help="what you want to know")
    parser.add_argument("--provider", choices=sorted(PROVIDERS), help="which API to ask")
    parser.add_argument("--model", help="override the provider's default model")
    parser.add_argument("--replay", action="store_true", help="use recorded replies only")
    parser.add_argument("--record", action="store_true", help="save the reply as a recording")
    parser.add_argument("--sql-only", action="store_true", help="print the SQL, do not run it")
    parser.add_argument("--gold-dir", type=Path, default=GOLD_DIR)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s %(message)s",
    )

    try:
        chosen = resolve(args.provider, args.model)
        if args.sql_only:
            reply, source = ask(
                system_prompt(), args.question, chosen, replay=args.replay, record=args.record
            )
            print(reply.strip())
            print(f"\n[{source}: {chosen.model}]", file=sys.stderr)
            return 0

        result, source = answer(
            args.question, chosen, args.gold_dir, replay=args.replay, record=args.record
        )
    except ProviderError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except UnsafeQuery as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2

    print(render(result))
    print()
    print(result.sql)
    print(f"\n[{source}: {chosen.model}]", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
