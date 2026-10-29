# Project instructions

Read [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) before continuing this project.
It records the user's decisions, the current implementation, and deferred work.
Use the actual files as the source of truth if the summary becomes outdated.

## Working preferences

- Do not run Git commands unless the user explicitly asks. Write code and files
  only; do not stage, commit, push, or otherwise operate on Git automatically.
- Keep the benchmark simple and build it iteratively. For now, use one Python
  file, `benchmark/benchmark.py`, and only the `generate-table` and `benchmark`
  commands. Avoid speculative infrastructure, extra modes, and large config
  classes. Discuss substantial architecture changes before introducing them.
- Follow the Google Python style guide. Use type annotations, short function
  docstrings, clear spacing, and comments before nontrivial logic. Generally
  keep functions under 40 lines. Give longer modules a short overview. Use
  small types or enums when they clarify the code without adding machinery.
- This is research code for a small group of trusted users. Do not add
  defensive argument checks for implausible inputs such as negative scale
  factors. Let argparse, Python, and DuckDB report ordinary input errors.
- Leave DuckDB resource defaults unset unless the user explicitly overrides
  them; do not guess thread counts or memory limits.
- Install dependencies only for the user's account, never machine-wide.
  CPU and GPU comparisons must use the same compatible DuckDB engine version.
- Explain measurement choices and limitations plainly. Keep per-query timings
  in the output. Do not hide negative slowdown values or force them to zero.
- Preserve user edits. In particular, GPU CPU fallback is currently enabled;
  do not silently disable it.

## Current priority

The cold-start cache mechanism has been implemented. Next, evaluate N=1 on
the real cluster before returning to prepared statements and the timing/barrier
question. Hot-cache warmups, validation, and additional infrastructure are
deferred until requested.
