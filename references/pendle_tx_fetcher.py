import sys
import json
import time
import requests
import pandas as pd
from pathlib import Path

BASE_URL = "https://api-v2.pendle.finance/core"
DATA_DIR  = Path("../pendle_data")        # everything lives here
DATA_DIR.mkdir(exist_ok=True)
CSV_PATH = DATA_DIR / "pendle_transactions.csv"
CHECKPOINT_PATH = DATA_DIR / "pendle_checkpoint.json"

HEADERS = {"accept": "application/json"}   # tweak if you need an api-key later

# ──────────────────────────────────────────────────────────────────────────────
def fetch_with_retry(url: str, params: dict | None = None) -> dict:
    """GET url, retrying on HTTP 429."""
    while True:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 429:
            print(f"[429] throttle hit → sleeping 10 s …", file=sys.stderr)
            time.sleep(10)
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"{url} → {r.status_code} : {r.text[:120]}")
        return r.json()

# ──────────────────────────────────────────────────────────────────────────────
def load_checkpoint(chain_id: int) -> str | None:
    """Load resume token for a specific chain_id from the single checkpoint file."""
    if CHECKPOINT_PATH.exists():
        with CHECKPOINT_PATH.open() as fh:
            try:
                data = json.load(fh)
                return data.get(str(chain_id))  # Keys are stored as strings in JSON
            except json.JSONDecodeError:
                print(f"Warning: Checkpoint file {CHECKPOINT_PATH} is corrupted.", file=sys.stderr)
                return None
    return None

def save_checkpoint(chain_id: int, resume_token: str | None) -> None:
    """Save resume token for a specific chain_id to the single checkpoint file."""
    data = {}
    if CHECKPOINT_PATH.exists():
        try:
            with CHECKPOINT_PATH.open() as fh:
                data = json.load(fh)
        except json.JSONDecodeError:
            print(f"Warning: Overwriting corrupted checkpoint file {CHECKPOINT_PATH}.", file=sys.stderr)

    if resume_token:
        data[str(chain_id)] = resume_token
    else:
        # Remove the key if the token is None (signifying completion for that chain)
        data.pop(str(chain_id), None)

    with CHECKPOINT_PATH.open("w") as fh:
        json.dump(data, fh)

# ──────────────────────────────────────────────────────────────────────────────
def load_existing_hashes(csv_path: Path) -> set[str]:
    """Return a set of txHash already present; empty if file doesn't exist."""
    if not csv_path.exists():
        return set()
    # read only the one column; avoids blowing up RAM
    return set(pd.read_csv(csv_path, usecols=["txHash"]).txHash.values)

# ──────────────────────────────────────────────────────────────────────────────
def process_chain(
    chain_id: int,
    csv_path: Path,
    existing_hashes: set[str],
    write_header: bool
) -> None:
    """Process transactions for a single chain, appending to the common CSV."""
    resume_token = load_checkpoint(chain_id)
    # Initial status print is less informative now without per-chain file size, adjust if needed
    print(f"▶ chain {chain_id}", file=sys.stderr)

    # Determine if header needs to be written for this specific chain's *first* write batch
    # This is tricky because multiple chains write to the same file.
    # We rely on the initial `write_header` passed from main for the very first write overall.
    # Subsequent writes by this chain instance won't write headers.
    first_write_for_this_chain = True

    while True:
        params = {"limit": 1000, "action": "SWAP_PT", "origin": "PENDLE_MARKET"}
        if resume_token:
            params["resumeToken"] = resume_token
        url = f"{BASE_URL}/v4/{chain_id}/transactions"
        payload = fetch_with_retry(url, params)

        results      = payload.get("results", [])
        resume_token = payload.get("resumeToken")  # may be None
        if not results:
            print(f"✓ chain {chain_id} done (no more results)", file=sys.stderr)
            break

        df = pd.json_normalize(results)
        # de-dup vs existing + within-batch duplicates
        df = df[~df["txHash"].isin(existing_hashes)]
        if df.empty:
            print("…batch contained only duplicates; skipping write", file=sys.stderr)
        else:
            # Determine header: write if it's the overall first write OR
            # if the file didn't exist initially and this is the first write for *this chain*.
            # Simplified: Rely on the global `write_header` flag controlled by `main`
            # and ensure subsequent appends don't write headers.
            current_write_header = write_header and first_write_for_this_chain and not csv_path.exists()

            df.to_csv(csv_path, mode="a", header=current_write_header, index=False)
            existing_hashes.update(df["txHash"].tolist())
            print(f"  +{len(df):>5} rows added to {csv_path.name}", file=sys.stderr)
            first_write_for_this_chain = False # Header logic only applies once per chain run

        save_checkpoint(chain_id, resume_token)
        # if the API fails to return a resumeToken, we reached the end
        if not resume_token:
            print(f"✓ chain {chain_id} done (no resumeToken)", file=sys.stderr)
            break

# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    chains_url = f"{BASE_URL}/v1/chains"
    chain_ids = fetch_with_retry(chains_url)["chainIds"]
    print(f"Discovered chains: {chain_ids}", file=sys.stderr)

    # Load existing hashes once from the single CSV
    existing_hashes = load_existing_hashes(CSV_PATH)
    print(f"Loaded {len(existing_hashes):,} existing transaction hashes from {CSV_PATH.name}", file=sys.stderr)

    # Determine if header needs to be written (only if file doesn't exist yet)
    write_header = not CSV_PATH.exists()

    for cid in chain_ids:
        try:
            # Pass csv_path, existing_hashes set, and initial header flag
            process_chain(cid, CSV_PATH, existing_hashes, write_header)
            # After the first chain potentially writes the header, subsequent chains shouldn't.
            write_header = False
        except Exception as exc:
            print(f"\n[ERROR] chain {cid}: {exc}\n", file=sys.stderr)

if __name__ == "__main__":
    main()