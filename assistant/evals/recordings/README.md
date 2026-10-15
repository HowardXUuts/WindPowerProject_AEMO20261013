# Recorded model replies

Each file is one model reply, named by a hash of the exact prompt **and** the
model that produced it — a recording made against one model is not evidence
about another.

They exist so this repository needs no credential: `eval.py` and `qa.py` serve a
recording instead of making a request, so CI runs with no key and no network,
and anyone can clone the project and get a green build.

This directory is empty until someone with a key runs:

    python assistant/eval.py --record

Until then the eval reports that the model was not scored, rather than claiming
a number it did not measure. It still re-runs every reference query in
`../questions.json` against the committed gold layer on each CI run, which is
what catches the question set going stale when the gold schema changes.
