# User-local installation on Ubuntu 24.04

Everything below installs under your home directory. No `sudo`, system pip,
database server, or system DuckDB installation is needed. Commands are for you
to run on the cluster; no cluster or Git operations were performed by the agent.

There are two distinct routes: the published pair is convenient for an initial
installation check; the source route is for the concurrency revision targeted by
this benchmark. Do not mix their Python packages and extension binaries.

## Check the allocated GPU node

Run GPU checks inside your cluster's allocation, not necessarily on its login
node. Obtain that allocation using your cluster's usual scheduler commands.

```bash
uname -m
python3 --version
nvidia-smi
cat /proc/sys/kernel/io_uring_disabled
```

Sirius requires an NVIDIA Turing-or-newer GPU (compute capability 7.5+).
Its release notes specify driver 525.60.13+ for CUDA 12 assets or 580.65.06+
for CUDA 13 assets. A working NVIDIA driver and permission to use `io_uring`
are machine prerequisites; a Python environment cannot supply them. The CUDA
version printed by `nvidia-smi` is driver capability, not proof of an installed
toolkit. [Upstream requirements](https://github.com/sirius-db/sirius#requirements)

## Install Python packages privately

From your repository's `benchmark/` directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install 'duckdb==1.5.6'
python -m pip install -e '.[sirius]'
python -c 'import duckdb; print(duckdb.sql("PRAGMA version").fetchall())'
```

The runner uses the Python package; installing the DuckDB CLI is unnecessary.
Re-enter this environment with `source .venv/bin/activate` in later sessions.
TPC-H's `INSTALL tpch`, used by preparation, normally caches the extension in
your home directory and needs outbound network access once.

If Ubuntu's Python lacks `venv`/`ensurepip`, use a private Python from Pixi
instead of requesting a system package installation:

```bash
curl -fsSL https://pixi.sh/install.sh -o /tmp/pixi-install.sh
bash /tmp/pixi-install.sh
export PATH="$HOME/.pixi/bin:$PATH"
pixi init "$HOME/.local/share/db-bench-python"
pixi add --manifest-path "$HOME/.local/share/db-bench-python/pixi.toml" python=3.12
pixi run --manifest-path "$HOME/.local/share/db-bench-python/pixi.toml" \
  python -m venv "$HOME/.venvs/db-bench"
source "$HOME/.venvs/db-bench/bin/activate"
python -m pip install 'duckdb==1.5.6'
python -m pip install -e '.[sirius]'
```

Keep that Pixi directory: the virtual environment depends on its Python.
[Pixi installation](https://pixi.prefix.dev/latest/installation/)

## Download a released Sirius extension

The `stable` release checked on 2026-10-28 advertises DuckDB **1.5.6**.
`stable` is a moving tag: recheck the release page's required DuckDB version
before downloading later. This is an installation route, not evidence that
that snapshot supports this project's two-query admission protocol.
[Sirius release instructions](https://github.com/sirius-db/sirius/releases/tag/stable)

For x86-64 and a CUDA 12-compatible driver, in the Python environment above:

```bash
mkdir -p "$HOME/.local/lib/sirius/stable"
curl -fL --retry 3 \
  https://github.com/sirius-db/sirius/releases/download/stable/sirius-linux_amd64-cuda12.duckdb_extension.gz \
  -o "$HOME/.local/lib/sirius/stable/sirius.duckdb_extension.gz"
gzip -dk "$HOME/.local/lib/sirius/stable/sirius.duckdb_extension.gz"
sha256sum "$HOME/.local/lib/sirius/stable/sirius.duckdb_extension"
```

For `aarch64`, select `linux_arm64`; for CUDA 13, select `cuda13` and check its
driver requirement. Preserve the downloaded binary and its fingerprint when
recording experiments; a future download from the same URL may differ.

Within the GPU allocation, test loading it:

```bash
python - <<'PY'
from pathlib import Path
import duckdb

extension = Path.home() / '.local/lib/sirius/stable/sirius.duckdb_extension'
with duckdb.connect(config={'allow_unsigned_extensions': 'true'}) as con:
    con.execute("LOAD '" + str(extension).replace("'", "''") + "'")
    con.execute('SET gpu_execution = false')
    print(con.execute('PRAGMA version').fetchall())
    print('Sirius loaded successfully')
PY
```

Loading initializes Sirius and can allocate GPU/pinned-host memory. Use the
allocation's memory limits when configuring Sirius. A successful LOAD checks
binary/runtime compatibility, not GPU query correctness or concurrent execution.

The supplied comparison configs currently expect DuckDB `d54cbbe3df`, so this
1.5.6 environment is intentionally rejected by them. Do not simply remove that
check to make the comparison run. Use `configs/cpu-smoke.toml` for a CPU check.

## Build the source revision for concurrency

Use a separate checkout and environment. The following follows the current
upstream layout and chooses its CUDA 12 environment. Install Pixi as above if
needed. Build within an allocation with adequate RAM, disk space, and CPUs;
adjust the example parallelism of four to the allocation.

```bash
mkdir -p "$HOME/.local/src"
cd "$HOME/.local/src"
git clone --no-recurse-submodules https://github.com/sirius-db/sirius.git
cd sirius
git rev-parse HEAD > sirius-source-revision.txt
git submodule update --init --depth=1 --jobs 4 \
  duckdb substrait cucascade sirius-duckdb/duckdb sirius-duckdb/extension-ci-tools
git -C duckdb rev-parse HEAD > duckdb-source-revision.txt
diff duckdb-source-revision.txt <(git -C sirius-duckdb/duckdb rev-parse HEAD)
CMAKE_BUILD_PARALLEL_LEVEL=4 pixi run -e cuda12 make TEST_BUILD_TARGET=
pixi run -e cuda12 duckdb
```

Quit the CLI with `.quit`. The extension is at
`$HOME/.local/src/sirius/sirius-duckdb/build/release/extension/sirius/sirius.duckdb_extension`.
Preserve both recorded revisions and `pixi.lock`; cloning main on another day
can select different code. For a repeat build, check out your recorded Sirius
commit before initializing its submodules.
[Sirius source instructions](https://github.com/sirius-db/sirius#building-and-running-sirius),
[build environments](https://github.com/sirius-db/sirius/blob/main/pixi.toml)

### Build a matching Python client

The built CLI does **not** install a matching Python package. Upstream explicitly
requires Python to use the same DuckDB source revision as its extension. Use the
same resulting wheel for both our CPU and GPU runs.
[Sirius Python requirements](https://github.com/sirius-db/sirius#python-api)

One conservative approach is to find a Python bindings commit whose DuckDB
submodule already matches the engine. These commands stop if no such commit
exists; they do not guess whether unrelated bindings are compatible.

```bash
cd "$HOME/.local/src"
git clone https://github.com/duckdb/duckdb-python.git
cd duckdb-python
python3 - <<'PY'
from pathlib import Path
import subprocess

expected = (Path.home() / '.local/src/sirius/duckdb-source-revision.txt').read_text().strip()
history = subprocess.check_output(
    ['git', 'log', '--all', '--format=%H', '--', 'external/duckdb'], text=True
).splitlines()
for commit in history:
    result = subprocess.run(
        ['git', 'rev-parse', f'{commit}:external/duckdb'],
        text=True, capture_output=True, check=False,
    )
    if result.returncode == 0 and result.stdout.strip() == expected:
        subprocess.run(['git', 'checkout', '--detach', commit], check=True)
        Path('matching-bindings-revision.txt').write_text(commit + '\n')
        break
else:
    raise SystemExit('No matching bindings commit: obtain a compatible source build/wheel from upstream before continuing.')
PY
```

**Continue only if that search succeeded.** Then initialize and build using
Sirius's private compiler environment:

```bash
git submodule update --init --recursive
cd "$HOME/.local/src/sirius"
pixi shell -e cuda12
python -m venv "$HOME/.venvs/sirius-bench"
source "$HOME/.venvs/sirius-bench/bin/activate"
python -m pip install --upgrade pip
CMAKE_BUILD_PARALLEL_LEVEL=4 python -m pip wheel \
  "$HOME/.local/src/duckdb-python" --no-deps \
  --wheel-dir "$HOME/.local/share/sirius-wheels" \
  --config-settings 'cmake.define.BUILD_EXTENSIONS=core_functions;parquet;icu;json;tpch'
python -m pip install --no-index \
  --find-links "$HOME/.local/share/sirius-wheels" duckdb
python -c 'import duckdb; print(duckdb.sql("PRAGMA version").fetchall())'
```

The extension list includes TPC-H so preparation need not download an extension
for an unpublished engine revision. Building Python is based on upstream's
[bindings build configuration](https://github.com/duckdb/duckdb-python/blob/main/cmake/duckdb_loader.cmake);
this cluster build recipe has not been executed locally. A matching source ID
is necessary but not sufficient: compiler/ABI compatibility and actual LOAD
still need checking. If no matching bindings commit is available, a compatible
wheel/build from the Sirius maintainers is the remaining dependency.

Stay in the Pixi shell for native runtime dependencies. Navigate to your
project's `benchmark/`, install `python -m pip install -e '.[sirius]'`, and:

1. Put the built extension's absolute path in `configs/sirius.toml`.
2. Check `PRAGMA version` against `duckdb-source-revision.txt`. Set
   `expected_source_id` in **both** comparison configs to that verified revision
   (at least ten characters), documenting any change from `d54cbbe3df`.
3. Check that the chosen Sirius revision implements
   [two-query admission](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/concurrent-queries.md).
4. Preview the YAML, prepare a small dataset with this same Python client,
   and run a small CPU/GPU correctness check before collecting performance data.

Do not reinstall `requirements-cpu-smoke.txt` into this source-build environment:
it would replace the custom DuckDB wheel with 1.5.6. No successful GPU benchmark
or cluster installation is claimed until these checks run on your allocation.
