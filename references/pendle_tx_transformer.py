import pandas as pd
from pathlib import Path
import sys
import json # Added for JSON parsing

# Define file paths
DATA_DIR = Path("../pendle_data")
INPUT_CSV_PATH = DATA_DIR / "pendle_transactions.csv"
OUTPUT_CSV_PATH = DATA_DIR / "curated_pendle_transactions.csv"

# Helper function to extract and normalize nested JSON-like data
def extract_nested_data(df: pd.DataFrame, column_name: str, prefix: str) -> pd.DataFrame:
    """
    Parses a column containing JSON strings (expected to be a list with one dict),
    normalizes the first dictionary, prefixes new columns, and drops the original column.
    """
    # print(f"DEBUG: Starting extraction for column: {column_name}", file=sys.stderr) # DEBUG - Commented out
    if column_name not in df.columns:
        print(f"  Info: Column '{column_name}' not found, skipping extraction.", file=sys.stderr)
        return df

    # debug_counter = {'count': 0} # DEBUG - Commented out
    # debug_limit = 5 # DEBUG - Commented out

    def parse_value(val):
        if pd.isna(val):
            # if debug_counter['count'] < debug_limit: # DEBUG - Commented out
            #     if debug_counter['count'] == 0: print(f"DEBUG ({column_name}): Encountered NaN values...", file=sys.stderr) # DEBUG - Commented out
            return {}
        
        # debug_counter['count'] += 1 # DEBUG - Commented out
        # if debug_counter['count'] <= debug_limit: # DEBUG - Commented out
        #     print(f"DEBUG ({column_name}) Row {debug_counter['count']} - Raw val: {val!r}", file=sys.stderr) # DEBUG - Commented out

        data_to_normalize = None
        if isinstance(val, str):
            # if debug_counter['count'] <= debug_limit: # DEBUG - Commented out
            #     print(f"DEBUG ({column_name}) Row {debug_counter['count']} - Attempting to parse string...", file=sys.stderr) # DEBUG - Commented out
            try:
                # Replace single quotes with double quotes for JSON compatibility
                corrected_val_str = val.replace("'", "\"")
                parsed_json = json.loads(corrected_val_str)
                data_to_normalize = parsed_json
            except (json.JSONDecodeError, TypeError) as e:
                # if debug_counter['count'] <= debug_limit: # DEBUG - Commented out
                #     print(f"DEBUG ({column_name}) Row {debug_counter['count']} - JSONDecodeError/TypeError: {e} (Original val: {val!r})", file=sys.stderr) # DEBUG - Commented out
                return {}
        elif isinstance(val, (list, dict)):
            # if debug_counter['count'] <= debug_limit: # DEBUG - Commented out
            #     print(f"DEBUG ({column_name}) Row {debug_counter['count']} - Value is already list/dict.", file=sys.stderr) # DEBUG - Commented out
            data_to_normalize = val
        else:
            # if debug_counter['count'] <= debug_limit: # DEBUG - Commented out
            #     print(f"DEBUG ({column_name}) Row {debug_counter['count']} - Value is not string, list, or dict. Type: {type(val)}", file=sys.stderr) # DEBUG - Commented out
            return {}

        parsed_result = {}
        if isinstance(data_to_normalize, list):
            if len(data_to_normalize) > 0 and isinstance(data_to_normalize[0], dict):
                parsed_result = data_to_normalize[0]
            else:
                # if debug_counter['count'] <= debug_limit: # DEBUG - Commented out
                #     print(f"DEBUG ({column_name}) Row {debug_counter['count']} - Parsed list is empty or first element not a dict.", file=sys.stderr) # DEBUG - Commented out
                parsed_result = {}
        elif isinstance(data_to_normalize, dict):
            parsed_result = data_to_normalize
        else:
            # if debug_counter['count'] <= debug_limit: # DEBUG - Commented out
            #     print(f"DEBUG ({column_name}) Row {debug_counter['count']} - Parsed data is not list or dict after initial parse. Type: {type(data_to_normalize)}", file=sys.stderr) # DEBUG - Commented out
            parsed_result = {}
        
        # if debug_counter['count'] <= debug_limit: # DEBUG - Commented out
        #     print(f"DEBUG ({column_name}) Row {debug_counter['count']} - Parsed to: {parsed_result}", file=sys.stderr) # DEBUG - Commented out
        return parsed_result

    extracted_data_series = df[column_name].apply(parse_value)
    
    # print(f"DEBUG ({column_name}): First 5 items of extracted_data_series (what json_normalize gets):\n{extracted_data_series.head().tolist()}", file=sys.stderr) # DEBUG - Commented out

    df_normalized = pd.json_normalize(extracted_data_series.tolist())

    df_processed = df.drop(columns=[column_name])

    if df_normalized.empty:
        print(f"  Info: No data extracted or normalized from column '{column_name}'. Original column was dropped.", file=sys.stderr)
        return df_processed
    
    df_normalized = df_normalized.add_prefix(prefix)
    df_out = pd.concat([df_processed, df_normalized], axis=1)
    print(f"  ✓ Extracted and normalized '{column_name}' into {len(df_normalized.columns)} new columns with prefix '{prefix}'.", file=sys.stderr)
    return df_out

def transform_transactions(input_path: Path, output_path: Path) -> None:
    """
    Reads transaction data from input_path, removes specified columns,
    and saves the curated data to output_path.
    """
    print(f"▶ Starting transformation process...", file=sys.stderr)
    if not input_path.exists():
        print(f"Error: Input file not found at {input_path}", file=sys.stderr)
        return

    print(f"  Reading input file: {input_path}", file=sys.stderr)
    try:
        df = pd.read_csv(input_path)
    except Exception as e:
        print(f"Error reading CSV file: {e}", file=sys.stderr)
        return
    print(f"  ✓ Successfully read {len(df)} rows from {input_path}.", file=sys.stderr)

    # Extract nested data from 'inputs' and 'outputs' columns
    print(f"  Processing: Extracting 'inputs' data...", file=sys.stderr)
    df = extract_nested_data(df, 'inputs', 'inputs_')
    print(f"  Processing: Extracting 'outputs' data...", file=sys.stderr)
    df = extract_nested_data(df, 'outputs', 'outputs_')

    if 'inputs_asset' in df.columns:
        df['inputs_asset'] = df['inputs_asset'].astype(str).str.split('-', n=1).str[1]

    if 'outputs_asset' in df.columns:
        df['outputs_asset'] = df['outputs_asset'].astype(str).str.split('-', n=1).str[1]

    columns_to_drop = [
        'id',
        'market.id',
        'market.chainId',
        'market.expiry',
        'market.name',
        'market.symbol',
        'action',
        'origin'
    ]

    # Drop columns, handling cases where some might already be missing
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    df_transformed = df.drop(columns=existing_columns_to_drop)

    print(f"  Writing transformed data to: {output_path}", file=sys.stderr)
    try:
        df_transformed.to_csv(output_path, index=False)
        print(f"✓ Transformation complete. Output: {output_path}", file=sys.stderr)
    except Exception as e:
        print(f"Error writing CSV file: {e}", file=sys.stderr)

if __name__ == "__main__":
    DATA_DIR.mkdir(exist_ok=True) # Ensure data directory exists
    transform_transactions(INPUT_CSV_PATH, OUTPUT_CSV_PATH)
