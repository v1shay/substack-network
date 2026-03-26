#!/bin/bash
# Setup script for substack-cartographer
# Creates virtual environment and installs dependencies

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Sibling repo by default; override with SUBSTACK_API_REPO if needed
API_REPO="${SUBSTACK_API_REPO:-$SCRIPT_DIR/../substack_api}"
API_PIP_SPEC="${SUBSTACK_API_PIP_SPEC:-substack_api>=1.1.3}"

default_venv_dir() {
    local preferred="$SCRIPT_DIR/.venv"
    if [[ "$preferred" == *:* ]]; then
        local slug=""
        slug="$(basename "$SCRIPT_DIR" | tr -cs 'A-Za-z0-9._-' '-' | sed 's/^-*//; s/-*$//')"
        printf '%s\n' "${HOME}/.venvs/${slug:-substack-cartographer}"
        return 0
    fi
    printf '%s\n' "$preferred"
}

VENV_DIR="${VENV_DIR:-$(default_venv_dir)}"

find_supported_python() {
    local candidate=""
    local candidates=()
    local best_candidate=""
    local best_version=""
    if [ -n "${PYTHON_BIN:-}" ]; then
        candidates+=("$PYTHON_BIN")
    fi
    candidates+=("python3" "python3.14" "python3.13" "python3.12" "python3.11")

    for candidate in "${candidates[@]}"; do
        if ! command -v "$candidate" >/dev/null 2>&1; then
            continue
        fi
        local version=""
        version="$("$candidate" -c "import sys; ok = sys.version_info >= (3, 11); print(f'{sys.version_info[0]}.{sys.version_info[1]}.{sys.version_info[2]}' if ok else ''); raise SystemExit(0 if ok else 1)" 2>/dev/null)" || continue
        if [ -z "$best_version" ] || [ "$(printf '%s\n%s\n' "$best_version" "$version" | sort -V | tail -n 1)" = "$version" ]; then
            best_candidate="$candidate"
            best_version="$version"
        fi
    done
    if [ -n "$best_candidate" ]; then
        echo "$best_candidate"
        return 0
    fi
    return 1
}

echo "🚀 Setting up substack-cartographer..."
echo "📁 Virtual environment path: $VENV_DIR"

# Check if Python 3.11+ is available
PYTHON_CMD="$(find_supported_python)" || true
if [ -z "$PYTHON_CMD" ]; then
    echo "❌ Error: Python 3.11+ is required but no supported interpreter was found."
    echo "   Install Python 3.11 or later, or set PYTHON_BIN to a compatible interpreter."
    exit 1
fi

echo "🐍 Using interpreter: $PYTHON_CMD"

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "📦 Creating virtual environment..."
    "$PYTHON_CMD" -m venv "$VENV_DIR"
else
    echo "✅ Virtual environment already exists"
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip --quiet

# Install API library in editable mode.
# Fallback: keep setup usable if editable-install metadata resolution fails.
install_substack_api() {
    echo "📚 Installing substack-api library..."
    if [ ! -d "$API_REPO" ]; then
        echo "⚠️  API repository not found at $API_REPO"
        echo "   Falling back to PyPI package: $API_PIP_SPEC"
        pip install "$API_PIP_SPEC"
        return 0
    fi

    if pip install -e "$API_REPO"; then
        return 0
    fi

    echo "⚠️  Editable install failed; falling back to source-path injection."
    local site_packages_dir=""
    site_packages_dir="$(
        python - <<'PY'
import site
paths = [p for p in site.getsitepackages() if p.endswith("site-packages")]
print(paths[0] if paths else "")
PY
    )"

    if [ -z "$site_packages_dir" ]; then
        echo "❌ Could not locate site-packages for fallback install."
        return 1
    fi

    mkdir -p "$site_packages_dir"
    printf '%s\n' "$API_REPO" > "$site_packages_dir/substack_api_local_repo.pth"
    echo "✅ Added fallback path file: $site_packages_dir/substack_api_local_repo.pth"
    return 0
}

install_substack_api

# Install requirements (networkx, numpy, scipy, pyvis for centrality and visualization)
if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
    echo "📦 Installing requirements..."
    pip install -r "$SCRIPT_DIR/requirements.txt" --quiet
fi

# Verify installation
echo "🔍 Verifying installation..."
if python -c "from substack_api import Newsletter; import requests; print('✅ API and requests imported successfully')" 2>/dev/null; then
    echo ""
    echo "✅ Setup complete!"
    echo ""
    echo "Because you ran 'source setup.sh', the venv is active in this terminal."
    echo "In other terminals, activate with: source $VENV_DIR/bin/activate"
else
    echo "❌ Warning: Installation verification failed"
    exit 1
fi
