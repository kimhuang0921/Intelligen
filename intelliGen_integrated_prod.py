import os
import pickle
import io
import subprocess
import pandas as pd
from datetime import datetime
import json
import shutil
import smtplib
import zipfile
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import re
from dataclasses import dataclass
from typing import List, Optional
import getpass
import csv
import argparse
import sys
import logging

BASE_DIR = Path("/work/kimhuang/1_Python/10_IntelliGen")
CREDENTIALS_FILE = BASE_DIR / "gsheet_key.json"
TOKEN_PICKLE = BASE_DIR / "token.pickle"
SUBMIT_SHEET_ID = "1_Qk9E4BhBxC7kPvZGrMlfdnyMMJmE9nqJfeMYyWP3IY"
DOWNLOAD_DIR = BASE_DIR / "downloads"
CONFIG_FILE = BASE_DIR / "repack_config.json"
RAW_DIR = BASE_DIR / "raw"
TESTPROGRAM_DIR = Path("/projects/ga0/patterns/testprogram")
FALLBACK_ROOT = Path("/projects/ga0/patterns/release_pattern/src")
SOURCE_DIR = BASE_DIR / "source"
QUEUE_FILE = BASE_DIR / "IntelliGen_WorkQueue_prod.csv"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly"
]
TARGET_SUFFIXES = [".pat", "_wvt.spec", "_tim.spec", "_spec_decl.spec", "_seq.seq", "_specs.spec"]

def ensure_queue_file():
    os.makedirs(QUEUE_FILE.parent, exist_ok=True)
    if not QUEUE_FILE.exists():
        with open(QUEUE_FILE, "w", newline="") as f:
            csv.writer(f).writerow(["TabName", "Status", "Submitter", "Email", "SubmitTime", "LastUpdate", "Format"])

def submit_task(tab_name: str):
    ensure_queue_file()
    submitter = getpass.getuser()
    email = f"{submitter}@rivosinc.com"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(QUEUE_FILE, "a", newline="") as f:
        csv.writer(f, quoting=csv.QUOTE_ALL).writerow([tab_name, "WAIT", submitter, email, now, "", "prod"])
    print(f"[INFO] Submitted tab '{tab_name}' as '{submitter}' (Format: prod).")

def extract_file_base(filepath: str) -> str:
    filepath = filepath.strip()
    if filepath.lower().endswith('.stil.gz'):
        return Path(filepath).stem[:-len('.stil')]
    elif filepath.lower().endswith('.stil'):
        return Path(filepath).stem
    return Path(filepath).stem

def format_error(step: str, error: Exception) -> str:
    return f"{step}: Error\n  Type: {type(error).__name__}\n  Message: {str(error)}\n"

def format_file(filepath: Path):
    try:
        with open(filepath, 'r') as f:
            lines = f.read().splitlines()
        max_key_len = max((len(line.split('=')[0].strip()) for line in lines if '=' in line and not line.strip().startswith('in ') and not line.strip().endswith('{')), default=0)
        formatted = []
        indent_level = 0
        for line in lines:
            stripped = line.strip()
            indent = '  ' * indent_level
            if stripped.startswith('}'):
                indent_level -= 1
                indent = '  ' * indent_level
            if not stripped:
                formatted.append('')
            elif '=' in stripped and not stripped.startswith('in ') and not stripped.endswith('{'):
                key, value = map(str.strip, stripped.split('=', 1))
                formatted.append(f"{indent}{key:<{max_key_len}} = {value}")
            else:
                formatted.append(f"{indent}{stripped}")
            if stripped.endswith('{'):
                indent_level += 1
        with open(filepath, 'w') as f:
            f.write('\n'.join(formatted) + '\n')
        os.chmod(filepath, 0o777)
    except Exception as e:
        print(f"Failed to format {filepath}: {e}")

def download_google_sheet(timestamp: str) -> Path:
    try:
        creds = None
        if TOKEN_PICKLE.exists():
            with open(TOKEN_PICKLE, "rb") as token:
                creds = pickle.load(token)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
            with open(TOKEN_PICKLE, "wb") as token:
                pickle.dump(creds, token)
        service = build("drive", "v3", credentials=creds)
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        os.chmod(DOWNLOAD_DIR, 0o777)
        output_file = DOWNLOAD_DIR / f"IntelliGenSubmit_{timestamp}.xlsx"
        with io.FileIO(output_file, "wb") as fh:
            request = service.files().export_media(fileId=SUBMIT_SHEET_ID, mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            downloader = MediaIoBaseDownload(fh, request)
            while not downloader.next_chunk()[1]:
                pass
        os.chmod(output_file, 0o777)
        print(f"Step 1: Downloaded: {output_file}")
        return output_file
    except Exception as e:
        print(format_error("Step 1: Download", e))
        raise

def check_patterns(patterns: set) -> tuple:
    paths = []
    warning = ""
    missing_patterns = []
    
    for pattern in patterns:
        pattern_path = Path(pattern)
        pattern_name = extract_file_base(pattern)
        exists = pattern_path.exists()
        paths.append({
            "pattern": pattern,
            "pattern_name": pattern_name,
            "absolute_path": str(pattern_path.absolute()),
            "exists": exists
        })
        if not exists:
            missing_patterns.append(pattern)
    
    if missing_patterns:
        warning = f"Warning: {len(missing_patterns)} patterns not found: {', '.join(missing_patterns[:5])}"
        if len(missing_patterns) > 5:
            warning += f" and {len(missing_patterns) - 5} more"
    
    return paths, warning

def convert_excel_to_csv(xlsx_path: Path, sheet_name: str, raw_dir: Path, debug: bool = False) -> Path:
    output_path = raw_dir / "flowgenInput.csv"
    try:
        logging.info(f"Step 2: Converting {xlsx_path} (sheet: {sheet_name})")
        xls = pd.read_excel(xlsx_path, sheet_name=None)
        corrected_sheet_name = next((s for s in xls if s.lstrip("0") == sheet_name), None)
        if not corrected_sheet_name:
            raise ValueError(f"Sheet '{sheet_name}' not found. Available: {list(xls.keys())}")
        plan_df = xls[corrected_sheet_name]
        preamble_df = xls.get("Preamble Definition")
        if preamble_df is None:
            raise ValueError("Sheet 'Preamble Definition' not found")

        # 標準化欄位名稱
        column_mapping = {
            'block': ['block', 'Block'],
            'pattern name (with full path)': ['pattern name (with full path)', 'Pattern name (with full path)'],
            'preamble set': ['preamble set', 'Preamble Set', 'Preamble Set  '],
            'relay': ['relay', 'Relay'],
            'tml': ['tml', 'TML'],
            'level': ['level', 'Level'],
            'level spec': ['level spec', 'Level Spec'],
            'before payload': ['before payload', 'Before payload', 'Before paylod'],
            'after payload': ['after payload', 'After payload', 'After paylod']
        }

        plan_df.columns = [col.strip().lower() for col in plan_df.columns]
        if debug:
            logging.debug(f"Normalized Excel columns: {plan_df.columns.tolist()}")

        required_columns = ['block', 'pattern name (with full path)', 'preamble set', 'tml', 'level', 'level spec']
        available_columns = set(plan_df.columns)
        missing_cols = []
        for req_col in required_columns:
            found = False
            for mapped_col in column_mapping.get(req_col, [req_col]):
                if mapped_col.strip().lower() in available_columns:
                    found = True
                    break
            if not found:
                missing_cols.append(req_col)
        if missing_cols:
            raise ValueError(f"Missing columns: {missing_cols}")

        preamble_mapping = {str(col).strip().lower().replace("premable", "preamble"): [str(e).strip() for e in preamble_df[col].dropna()] for col in preamble_df.columns}

        columns = ['shmoo x signal', 'shmoo x start', 'shmoo x stop', 'shmoo x step size', 
                   'shmoo y signal', 'shmoo y start', 'shmoo y stop', 'shmoo y step size', 'enable fail logs', 'level spec']

        def clean_block_name(block: str) -> str:
            if not block or pd.isna(block):
                return "DEFAULT_BLOCK"
            return re.sub(r'[^a-zA-Z0-9_]', '_', str(block).strip())

        def get_suite_name(pattern_path: str, level: str) -> str:
            name = extract_file_base(pattern_path)
            level = str(level).strip().replace(" ", "_") if level and not pd.isna(level) else "Default"
            return f"{name}_{level}"

        def generate_ts_path(path: str, key: str) -> str:
            name = extract_file_base(path)
            prefix, postfix = name.split(key, 1)
            ts_name = f"{prefix}int_sa_edt_ts{postfix}" if key in ["int_sa_chain_edt_pl", "int_sa_scan_edt_pl", "ssn_loopback", "ssn_continuity"] else f"{prefix}int_tdf_edt_ts{postfix}"
            return str(Path(path).parent / f"{ts_name}.stil.gz")

        def get_preamble_list(name: str, path: str, set_name: str, preamble_mapping: dict, debug: bool = False) -> list:
            special_keys = ["int_sa_edt_ts", "int_sa_chain_edt_pl", "int_sa_scan_edt_pl", "ssn_loopback", "ssn_continuity", "int_tdf_edt_ts", "int_tdf_scan_edt_pl"]
            matched_key = next((k for k in special_keys if k in name), None)
            preamble_set = set_name.strip().lower().replace("premable", "preamble")
            preambles = preamble_mapping.get(preamble_set, [])
            valid_preambles = [p for p in preambles if p and not pd.isna(p)]
            
            if matched_key:
                ts_path = generate_ts_path(path, matched_key)
                valid_preambles.append(ts_path)
                if debug:
                    logging.debug(f"Appended _ts pattern for {name}: {ts_path}")
            
            if not valid_preambles and debug:
                logging.warning(f"Preamble set '{preamble_set}' not found or empty for pattern {name}")
            
            if debug:
                logging.debug(f"Preamble set '{preamble_set}' for pattern {name}: {valid_preambles}")
            
            return valid_preambles

        output_rows, patterns, relays = [], set(), set()
        invalid_paths = []

        for _, row in plan_df.iterrows():
            block = clean_block_name(row.get('block'))
            block_id = f"BLK_{block}"
            path = str(row.get("pattern name (with full path)", ""))
            if not path.strip():
                continue

            if not (path.lower().endswith('.stil') or path.lower().endswith('.stil.gz')):
                invalid_paths.append(path)
                continue

            pattern_name = extract_file_base(path)
            if "_ts" in pattern_name.lower():
                continue
            level = row.get("level", "")
            levelspec = row.get("level spec", "")
            suite_name = get_suite_name(path, level)
            pattern_id = suite_name
            preamble_set = str(row.get("preamble set", "")).strip().lower()
            preambles = get_preamble_list(pattern_name, path, preamble_set, preamble_mapping, debug)
            relay = str(row.get("relay", "")).strip() if pd.notna(row.get("relay")) else ""
            tml = row.get("tml", "FunctionalTest_wo_profiling")
            before_payload = str(row.get("before payload", row.get("before paylod", ""))).strip()
            after_payload = str(row.get("after payload", row.get("after paylod", ""))).strip()
            if relay:
                relays.add(relay)
            patterns.add(path)
            patterns.update(preambles)
            if before_payload and not pd.isna(before_payload) and before_payload.lower().endswith(('.stil', '.stil.gz')):
                patterns.add(before_payload)
            if after_payload and not pd.isna(after_payload) and after_payload.lower().endswith(('.stil', '.stil.gz')):
                patterns.add(after_payload)

            if debug:
                logging.debug(f"Processing pattern: {path}, Block: {block}, Flow: {pattern_id}, Preambles: {preambles}")


            # Preamble suites (包括 _ts pattern)
            for p in preambles:
                output_rows.append({
                    "BlockIdentifier": block_id,
                    "Identifier": pattern_id,
                    "Flows": path,
                    "Suites": p,
                    "Timing Spec": f"time_{extract_file_base(p)}",
                    "TestMethod": "FunctionalTest_wo_profiling",
                    **{col: "" for col in columns},
                    "Relay": "",
                    "level spec": levelspec
                })
            # Before payload
            if before_payload and not pd.isna(before_payload) and before_payload.lower().endswith(('.stil', '.stil.gz')):
                output_rows.append({
                    "BlockIdentifier": block_id,
                    "Identifier": pattern_id,
                    "Flows": path,
                    "Suites": before_payload,
                    "Timing Spec": f"time_{extract_file_base(before_payload)}",
                    "TestMethod": "FunctionalTest_wo_profiling",
                    **{col: "" for col in columns},
                    "Relay": "",
                    "level spec": levelspec
                })

            # Main suite
            output_rows.append({
                "BlockIdentifier": block_id,
                "Identifier": pattern_id,
                "Flows": path,
                "Suites": path,
                "Timing Spec": f"time_{suite_name}",
                "TestMethod": tml,
                **{col: row.get(col, "") for col in columns},
                "Relay": relay
            })

            # After payload
            if after_payload and not pd.isna(after_payload) and after_payload.lower().endswith(('.stil', '.stil.gz')):
                output_rows.append({
                    "BlockIdentifier": block_id,
                    "Identifier": pattern_id,
                    "Flows": path,
                    "Suites": after_payload,
                    "Timing Spec": f"time_{extract_file_base(after_payload)}",
                    "TestMethod": "FunctionalTest_wo_profiling",
                    **{col: "" for col in columns},
                    "Relay": "",
                    "level spec": levelspec
                })

        if invalid_paths:
            raise ValueError(f"Invalid pattern paths (must end with .stil or .stil.gz): {', '.join(invalid_paths[:5])}{' and more' if len(invalid_paths) > 5 else ''}")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(output_path.parent, 0o777)
        pd.DataFrame(output_rows).to_csv(output_path, index=False)
        os.chmod(output_path, 0o777)

        expected_columns = ['BlockIdentifier', 'Identifier', 'Flows', 'Suites', 'Timing Spec', 'TestMethod', 'Relay'] + columns
        if not all(col in pd.read_csv(output_path).columns for col in expected_columns):
            raise ValueError("Invalid flowgenInput.csv columns")

        if debug:
            logging.debug(f"Generated {len(output_rows)} rows in {output_path}")

        with open(output_path.parent / "relay_combinations.txt", "w") as f:
            f.write("\n".join(relays))
        os.chmod(output_path.parent / "relay_combinations.txt", 0o777)

        paths, warning = check_patterns(patterns)
        with open(output_path.parent / "pattern_paths.txt", "w") as f:
            for p in paths:
                f.write(f"Pattern: {p['pattern']}\nPatternName: {p['pattern_name']}\nAbsolute Path: {p['absolute_path']}\nExists: {p['exists']}\n\n")
        os.chmod(output_path.parent / "pattern_paths.txt", 0o777)

        print(f"Step 2: Converted: {output_path}")
        if warning:
            print(f"Step 2: {warning}")
        return output_path
    except Exception as e:
        print(format_error("Step 2: Conversion", e))
        raise

def rsync_pattern_files(csv_path: Path, raw_dir: Path, debug_dir: Path, timestamp: str) -> Path:
    output_dir = raw_dir / "temp"
    patterns_dir = debug_dir / "Patterns" / "global"
    output_dir.mkdir(parents=True, exist_ok=True)
    patterns_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(output_dir, 0o777)
    os.chmod(patterns_dir, 0o777)
    
    try:
        if not shutil.which("rsync"):
            summary_file = output_dir / f"suite_rsync_summary_{timestamp}.csv"
            pd.DataFrame([]).to_csv(summary_file, index=False)
            os.chmod(summary_file, 0o777)
            print(f"Step 3: rsync not found, created: {summary_file}")
            return output_dir
        
        df = pd.read_csv(csv_path)
        patterns = set(df["Suites"].dropna().apply(extract_file_base))
        results = []
        for pattern in patterns:
            target_files = [f"{pattern}{suffix}" for suffix in TARGET_SUFFIXES]
            found_paths = []
            for root, _, files in os.walk(FALLBACK_ROOT):
                for name in target_files:
                    if name in files:
                        found_paths.append(Path(root) / name)
            found_paths = set(found_paths)
            if not found_paths:
                results.append({"Pattern": pattern, "Copied": 0, "Status": "NOT_FOUND"})
                continue
            for path in found_paths:
                dst = output_dir / path.name
                subprocess.run(["rsync", "-av", str(path), str(dst)], check=True)
                os.chmod(dst, 0o777)
                results.append({"Pattern": pattern, "File": path.name, "CopiedTo": str(dst), "Status": "SUCCESS"})
                if path.suffix == ".pat":
                    pat_dst = patterns_dir / path.name
                    shutil.copy(dst, pat_dst)
                    os.chmod(pat_dst, 0o777)
                    results.append({"Pattern": pattern, "File": path.name, "CopiedTo": str(pat_dst), "Status": "COPIED_TO_PATTERNS"})
        
        summary_file = output_dir / f"suite_rsync_summary_{timestamp}.csv"
        pd.DataFrame(results).to_csv(summary_file, index=False)
        os.chmod(summary_file, 0o777)
        print(f"Step 3: Rsynced: {summary_file}")
        return output_dir
    except Exception as e:
        print(format_error("Step 3: Rsync", e))
        raise

def generate_timing_files(csv_path: Path, temp_dir: Path, debug_dir: Path):
    output_dir = debug_dir / "Timings" / "global"
    output_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(output_dir, 0o777)
    
    try:
        df = pd.read_csv(csv_path)
        patterns = set(df["Suites"].apply(lambda x: Path(str(x)).stem.replace(".stil", "").replace(".gz", "")))
        
        def get_period(path: Path) -> str:
            if not path.exists():
                return "per_40"
            for line in path.read_text(encoding='utf-8').splitlines():
                if match := re.match(r'\s*var\s+\w+\s+(\w+)\s*;', line.strip()):
                    return match.group(1)
            return "per_40"
        
        def get_wft_name(path: Path) -> str:
            for line in path.read_text().splitlines():
                if match := re.match(r'\s*set\s+([^\s;]+);\s*$', line.strip()):
                    name = match.group(1).strip()
                    if name and not name[0].isdigit():
                        return name
            return "wft"
        
        def extract_spec_block(path: Path) -> list:
            if not path.exists():
                return []
            lines = path.read_text(encoding='utf-8').splitlines()
            spec_lines, in_block = [], False
            for line in lines:
                stripped = line.strip()
                if stripped.startswith("spec ") and "{" in stripped:
                    in_block = True
                    continue
                if in_block:
                    if stripped == "}":
                        break
                    if stripped and not stripped.startswith("//"):
                        spec_lines.append(f"\t\t{stripped}")
            return spec_lines
        
        def handle_spec_decl(path_in: Path, path_out: Path, pattern: str):
            lines = path_in.read_text().splitlines()
            header_end = next((i + 1 for i, line in enumerate(lines) if line.strip().endswith("*/")), 0)
            updated = lines[:header_end] + ["import configuration.IO_Group;", "import Timings.global.TimingRatio;"] + lines[header_end:]
            path_out.write_text('\n'.join(updated) + '\n')
            format_file(path_out)
        
        def handle_specs(path_in: Path, path_out: Path, pattern: str):
            lines = path_in.read_text().splitlines()
            header = lines[:next((i + 1 for i, line in enumerate(lines) if line.strip().endswith("*/")), 0)]
            spec_lines = extract_spec_block(path_in)
            content = header + [""] + [
                f"import Timings.global.{pattern}_spec_decl;",
                f"import Timings.global.{pattern}_tim;",
                f"import Timings.global.{pattern}_wvt;",
                "import configuration.IO_Group;",
                "import Timings.global.AllRefClkPins10ns_diff_specs;",
                "",
                f"spec {pattern}_specs {{"
            ] + spec_lines + ["\t\tper_AllRefClkPins10ns = 10.00 ns;", "\t\tRatio = 1.0;"] + ["}"]
            path_out.write_text('\n'.join(content) + '\n')
            format_file(path_out)
        
        def handle_tim(path_in: Path, path_out: Path, pattern: str, period_var: str):
            lines = path_in.read_text().splitlines()
            header = lines[:next((i + 1 for i, line in enumerate(lines) if line.strip().endswith("*/")), 0)]
            wft_name = get_wft_name(path_in)
            pads, blocks = [], []
            capture = False
            for line in lines:
                stripped = line.strip()
                if stripped.startswith("setup digInOut"):
                    pins = [p.strip() for p in stripped.removeprefix("setup digInOut").strip().rstrip("{").split("+")]
                    if any(p.lower().startswith("gpio") for p in pins):
                        capture = False
                        continue
                    pads.extend(pins)
                    blocks.append([line])
                    capture = True
                elif capture:
                    blocks[-1].append(re.sub(rf'\b({period_var})(?!\s*\*)', r'\1*Ratio', line))
                    if "}" in line:
                        capture = False
            content = header + [
                f"import Timings.global.{pattern}_spec_decl;",
                "import configuration.IO_Group;",
                "",
                f"spec {pattern}_tim {{",
                f"    set {wft_name};",
                f"setup digInOut G_ALL_IO - G_ref - {' - '.join(pads)} {{",
                f"    set timing {wft_name} {{",
                f"        period = {period_var}*Ratio;",
                f"        d1 = 0.0 * {period_var}*Ratio;",
                "    }",
                "}"
            ] + ["\n".join(b) for b in blocks] + ["}"]
            path_out.write_text('\n'.join(content) + '\n')
            format_file(path_out)
        
        def handle_wvt(path_in: Path, path_out: Path):
            lines = path_in.read_text().splitlines()
            result, skipping, brace_level = [], False, 0
            for line in lines:
                stripped = line.strip()
                if stripped.startswith("import"):
                    result.append("import configuration.IO_Group;")
                    continue
                if not skipping and ("GPIOS" in stripped.upper()):
                    skipping = True
                    brace_level = stripped.count("{") - stripped.count("}")
                    continue
                if skipping:
                    brace_level += stripped.count("{") - stripped.count("}")
                    if brace_level <= 0:
                        skipping = False
                    continue
                result.append(line)
            path_out.write_text('\n'.join(result) + '\n')
            format_file(path_out)
        
        period_vars = {}
        for file in temp_dir.glob("*.spec"):
            name = file.stem
            pattern = next((p for p in patterns if name.startswith(p)), None)
            if not pattern:
                continue
            if "_spec_decl" in name:
                period_vars[pattern] = get_period(file)
                handle_spec_decl(file, output_dir / file.name, pattern)
        
        for file in temp_dir.glob("*.spec"):
            name = file.stem
            pattern = next((p for p in patterns if name.startswith(p)), None)
            if not pattern:
                continue
            period_var = period_vars.get(pattern, "per_40")
            if "_specs" in name:
                handle_specs(file, output_dir / file.name, pattern)
            elif "_tim" in name:
                handle_tim(file, output_dir / file.name, pattern, period_var)
            elif "_wvt" in name:
                handle_wvt(file, output_dir / file.name)
        
        ratio_file = output_dir / "TimingRatio.spec"
        if not ratio_file.exists():
            ratio_file.write_text("spec TimingRatio {\n    var Double Ratio;\n}\n")
            format_file(ratio_file)
        
        print("Step 4: Timing files generated")
    except Exception as e:
        print(format_error("Step 4: Timing generation", e))
        raise

def generate_sequence_files(csv_path: Path, debug_dir: Path):
    output_dir = debug_dir / "Patterns" / "global"
    output_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(output_dir, 0o777)
    
    try:
        for suite in pd.read_csv(csv_path)["Suites"].dropna().unique():
            name = extract_file_base(suite)
            (output_dir / f"{name}_Pseq.seq").write_text(
                f"""sequence {name}_Pseq {{
    parallel {{
        sequential {{
            patternCall Patterns.global.AllRefClkPins10ns_diff;
        }}
        sequential {{
            patternCall Patterns.global.{name};
        }}
    }}
}}
""")
            format_file(output_dir / f"{name}_Pseq.seq")
        print("Step 5: Sequence files generated")
    except Exception as e:
        print(format_error("Step 5: Sequence generation", e))
        raise

def generate_flow_files(csv_path: Path, debug_dir: Path, timestamp: str, debug: bool = False) -> None:
    @dataclass
    class FlowConfig:
        extensions: List[str] = None
        max_failed_cycles: int = 20000
        ffv_cycles: int = 20000
        suite_templates: dict = None
        pattern_flow_template: str = """flow {flow_name} {{
    in failNonStop = testProgramVariables.getBoolean("SYS.OFFLINE") || testProgramVariables.getBoolean("TP_FAIL_NON_STOP");
    setup {{
{relay_on}
{suites}
{relay_off}
    }}
    execute {{
{relay_on_exec}
{suite_exec}
{relay_off_exec}
    }}
}}
"""
        block_flow_template: str = """flow {flow_name} {{
    setup {{
{pattern_flow_calls}
{shmoo_block}
    }}
    execute {{
{pattern_flow_exec}
{shmoo_exec}
    }}
}}
"""
        main_flow_template: str = """flow MainFlow {{
    setup {{
{setup_block}
    }}
    execute {{
{execute_block}
    }}
}}
"""
        shmoo_template: str = """        shmoo shmoo_{db_id} {{
            target = {target};
            axis[X_axis] = {{
                resourceType = specVariable;
                resourceName = "Levels.DFT_Vtyp.DFT_Vtyp_specValue.{x_signal}";
                range.resolution = {x_steps};
                range.start = {x_start};
                range.stop = {x_stop};
            }};
            axis[Y_axis] = {{
                resourceType = specVariable;
                resourceName = "{y_timing_spec}";
                range.steps = {y_steps};
                range.start = {y_start};
                range.stop = {y_stop};
                {tracking_blocks}
            }};
        }}
"""
        shmoo_x_only_template: str = """        shmoo shmoo_{db_id} {{
            target = {target};
            axis[X_axis] = {{
                resourceType = specVariable;
                resourceName = "Levels.DFT_Vtyp.DFT_Vtyp_specValue.{x_signal}";
                range.resolution = {x_steps};
                range.start = {x_start};
                range.stop = {x_stop};
            }};
        }}
"""
        def __post_init__(self):
            self.extensions = self.extensions or [".stil.gz", ".stil", ".STIL.GZ", ".STIL"]
            self.suite_templates = self.suite_templates or {
                "FunctionalTest_wo_profiling": """    suite {name} calls digital.{testmethod} {{
        timingSpec    = setupRef(Timings.global.{name}_specs);
        levelSpec    = setupRef({level_spec});
        operatingSequence = setupRef(Patterns.global.{name}_Pseq);
        maxFailedCycles = {max_failed_cycles};
        ffvCycles    = {ffv_cycles};
    }}
"""
            }

    class FlowGenerator:
        def __init__(self, csv_path: str, debug_dir: str, timestamp: str):
            self.config = FlowConfig()
            self.output_dir = Path(debug_dir) / "Flows" / f"DEBUG_{timestamp}"
            self.output_dir.mkdir(parents=True, exist_ok=True)
            os.chmod(self.output_dir, 0o777)
            self.flow_info = []

        def extract_basename(self, path: str) -> str:
            if not path or not isinstance(path, str) or pd.isna(path):
                return ""
            base = os.path.basename(path.strip())
            for ext in self.config.extensions:
                if base.lower().endswith(ext.lower()):
                    return base[:-len(ext)]
            return base

        def parse_time(self, value):
            try:
                if pd.isna(value) or not str(value).strip():
                    return 0.0
                value = str(value).lower().strip()
                match = re.match(r"([-]?\d*\.?\d*)\s*(ns|us|ms|s)?", value)
                if not match:
                    return 0.0
                num, unit = match.groups()
                num = float(num)
                return abs(num * {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1.0}.get(unit, 1.0))
            except Exception:
                return 0.0

        def generate_pattern_flow(self, df: pd.DataFrame, pattern_id: str, block_id: str):
            rows = df[df["Identifier"] == pattern_id]
            if rows.empty:
                return None, None
            suites = []
            shmoo_data = None
            relay = ""
            relay_name = ""

            for _, row in rows.iterrows():
                suite = self.extract_basename(str(row["Suites"]))
                if not suite or pd.isna(row["Suites"]) or not row["Suites"]:
                    if debug:
                        logging.debug(f"Skipping invalid suite for {pattern_id}: {row['Suites']}")
                    continue
                testmethod = row.get("TestMethod", "FunctionalTest_wo_profiling")
                level_spec = row.get("level spec", "")
                if row.get("Relay") and str(row["Relay"]).strip() and str(row["Relay"]).lower() != "nan":
                    relay = str(row["Relay"]).strip()
                    relay_name = relay.replace(" ", "_").replace("+", "_")

                if debug:
                    logging.debug(f"Generating suite: {suite}, TestMethod: {testmethod}, LevelSpec: {level_spec}")

                template_key = testmethod if testmethod in self.config.suite_templates else "FunctionalTest_wo_profiling"
                suites.append(self.config.suite_templates[template_key].format(
                    name=suite, testmethod=testmethod, level_spec=level_spec,
                    max_failed_cycles=self.config.max_failed_cycles, ffv_cycles=self.config.ffv_cycles
                ))

                if pd.notna(row.get("shmoo x signal")) and not shmoo_data:
                    try:
                        x_start, x_stop, x_steps = map(float, [row["shmoo x start"], row["shmoo x stop"], row["shmoo x step size"]])
                        if x_start > 2 or x_stop > 2:
                            x_start, x_stop, x_steps = x_start / 1000, x_stop / 1000, x_steps / 1000
                        x_steps = -abs(x_steps) if x_start > x_stop else abs(x_steps)
                        y_start, y_stop = map(self.parse_time, [row["shmoo y start"], row["shmoo y stop"]])
                        y_steps_raw = row.get("shmoo y step size")
                        y_steps = int(abs(y_start - y_stop) / self.parse_time(y_steps_raw)) if pd.notna(y_steps_raw) and y_steps_raw != "0" and self.parse_time(y_steps_raw) else 0
                        y_timing_spec = f"Timings.global.{suite}_specs.per_AllRefClkPins10ns"
                        tracking_blocks = "".join(
                            f"                tracking[pr{i}] = {{resourceType = specVariable;resourceName = \"Timings.global.{s}_specs.per_AllRefClkPins10ns\";range.start = {y_start};range.stop = {y_stop};}};\n"
                            for i, s in enumerate([self.extract_basename(r["Suites"]) for _, r in rows.iterrows() if r["Suites"] != row["Suites"] and self.extract_basename(r["Suites"])], 1)
                        ) if y_steps else ""
                        shmoo_data = {
                            "db_id": pattern_id, "target": pattern_id, "x_signal": row["shmoo x signal"],
                            "x_start": x_start, "x_stop": x_stop, "x_steps": x_steps,
                            "y_timing_spec": y_timing_spec, "y_start": y_start, "y_stop": y_stop, "y_steps": y_steps,
                            "tracking_blocks": tracking_blocks
                        } if y_steps else {
                            "db_id": pattern_id, "target": pattern_id, "x_signal": row["shmoo x signal"],
                            "x_start": x_start, "x_stop": x_stop, "x_steps": x_steps
                        }
                        if debug:
                            logging.debug(f"Collected shmoo data for {pattern_id}")
                    except Exception as e:
                        print(f"Step 6: Shmoo data error for {pattern_id}: {e}")

            if not suites:
                if debug:
                    logging.debug(f"No valid suites for {pattern_id}, skipping flow generation")
                return None, None

            flow_name = pattern_id
            relay_on = f"""    suite relay_on_{relay_name} calls misc.UtilityAction {{
        UtilityPins_to_On = "{relay.replace('_', '+').upper()}";
    }}
""" if relay_name else ""
            relay_off = f"""    suite relay_off_{relay_name} calls misc.UtilityAction {{
        UtilityPins_to_Off = "{relay.replace('_', '+').upper()}";
    }}
""" if relay_name else ""
            relay_on_exec = f"        relay_on_{relay_name}.execute();\n" if relay_name else ""
            relay_off_exec = f"        relay_off_{relay_name}.execute();\n" if relay_name else ""
            suite_exec = "".join(f"        {self.extract_basename(r['Suites'])}.execute();\n" for _, r in rows.iterrows() if self.extract_basename(r['Suites']) and not pd.isna(r['Suites']))

            flow_block = self.config.pattern_flow_template.format(
                flow_name=flow_name, relay_on=relay_on, suites="".join(suites),
                relay_off=relay_off, relay_on_exec=relay_on_exec,
                suite_exec=suite_exec, relay_off_exec=relay_off_exec
            )

            output_path = self.output_dir / f"{flow_name}.flow"
            output_path.write_text(flow_block)
            format_file(output_path)
            if debug:
                logging.debug(f"Generated PatternFlow: {output_path}")

            return {"flow_name": flow_name, "block_id": block_id}, shmoo_data

        def generate_block_flow(self, df: pd.DataFrame, block_id: str):
            pattern_ids = sorted(set(df[df["BlockIdentifier"] == block_id]["Identifier"]))
            if not pattern_ids:
                return

            pattern_flow_calls = []
            pattern_flow_exec = []
            shmoo_blocks = []
            shmoo_execs = []

            for pattern_id in pattern_ids:
                flow_info, shmoo_data = self.generate_pattern_flow(df, pattern_id, block_id)
                if flow_info:
                    pattern_flow_calls.append(f"        flow {flow_info['flow_name']} calls Flows.DEBUG_{timestamp}.{block_id}.{flow_info['flow_name']}{{}}")
                    pattern_flow_exec.append(f"        {flow_info['flow_name']}.execute();")
                    self.flow_info.append(flow_info)
                    if shmoo_data:
                        if shmoo_data.get("y_steps"):
                            shmoo_block = self.config.shmoo_template.format(**shmoo_data)
                            shmoo_exec = f"        shmoo_{shmoo_data['db_id']}.execute();\n"
                        else:
                            shmoo_block = self.config.shmoo_x_only_template.format(**shmoo_data)
                            shmoo_exec = f"        shmoo_{shmoo_data['db_id']}.execute();\n"
                        shmoo_blocks.append(shmoo_block)
                        shmoo_execs.append(shmoo_exec)
                        if debug:
                            logging.debug(f"Added shmoo block for {pattern_id} in block {block_id}")

            flow_name = block_id
            flow_block = self.config.block_flow_template.format(
                flow_name=flow_name,
                pattern_flow_calls="\n".join(pattern_flow_calls),
                shmoo_block="".join(shmoo_blocks),
                pattern_flow_exec="\n".join(pattern_flow_exec),
                shmoo_exec="\n".join(shmoo_execs)
            )

            output_path = self.output_dir / f"{flow_name}.flow"
            output_path.write_text(flow_block)
            format_file(output_path)
            if debug:
                logging.debug(f"Generated BlockFlow: {output_path}")

        def generate_main_flow(self):
            if not self.flow_info:
                return
            setup_block, execute_block = "", ""
            block_ids = sorted(set(info["block_id"] for info in self.flow_info))
            for block_id in block_ids:
                setup_block += f"        flow {block_id} calls Flows.DEBUG_{timestamp}.{block_id}{{}}\n"
                execute_block += f"        {block_id}.execute();\n"

            output_path = self.output_dir / "MainFlow.flow"
            output_path.write_text(self.config.main_flow_template.format(setup_block=setup_block, execute_block=execute_block))
            format_file(output_path)
            if debug:
                logging.debug(f"Generated MainFlow with {len(block_ids)} block flows")

    try:
        logging.info(f"Step 6: Generating flows from {csv_path}")
        df = pd.read_csv(csv_path)
        generator = FlowGenerator(csv_path, debug_dir, timestamp)
        for block_id in sorted(set(df["BlockIdentifier"])):
            generator.generate_block_flow(df, block_id)
        generator.generate_main_flow()
        print("Step 6: Flow files generated")
    except Exception as e:
        print(format_error("Step 6: Flow generation", e))
        raise

def package_outputs(debug_dir: Path, batch_id: str) -> Path:
    zip_file = debug_dir.parent / f"{batch_id}.zip"
    try:
        for subdir in ["Patterns", "Timings", "Flows"]:
            (debug_dir / subdir).mkdir(parents=True, exist_ok=True)
            os.chmod(debug_dir / subdir, 0o777)
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            for file in debug_dir.rglob("*"):
                if file.is_file():
                    zf.write(file, file.relative_to(debug_dir.parent))
        os.chmod(zip_file, 0o777)
        print(f"Step 7: Packaged: {zip_file}")
        return zip_file
    except Exception as e:
        print(format_error("Step 7: Packaging", e))
        raise

def copy_to_testprogram(zip_file: Path, batch_id: str) -> Path:
    TESTPROGRAM_DIR.mkdir(parents=True, exist_ok=True)
    os.chmod(TESTPROGRAM_DIR, 0o777)
    dest_zip = TESTPROGRAM_DIR / f"{batch_id}.zip"
    try:
        shutil.copy(zip_file, dest_zip)
        os.chmod(dest_zip, 0o777)
        print(f"Step 8: Copied: {dest_zip}")
        return dest_zip
    except Exception as e:
        print(format_error("Step 8: Copy", e))
        raise

def send_email(email: str, batch_id: str, zip_file: Path, log_content: str = ""):
    try:
        with open(CONFIG_FILE) as f:
            cfg = json.load(f)["email"]
        msg = MIMEMultipart()
        msg["From"] = cfg["from"]
        msg["To"] = email
        msg["Subject"] = f"IntelliGen Task Completed: {batch_id}"
        msg.attach(MIMEText(f"""
Dear Submitter,

Task ({batch_id}) processed.
Output: {zip_file}
Copied to: {TESTPROGRAM_DIR}/{batch_id}.zip

Log:
{log_content}

Best,
IntelliGen Automation
""", "plain"))
        for att in cfg.get("attachments", []):
            att_path = BASE_DIR / att
            if att_path.exists():
                with open(att_path, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f"attachment; filename={att_path.name}")
                msg.attach(part)
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(cfg["from"], cfg["password"])
            server.send_message(msg)
        print("Step 9: Email sent")
    except Exception as e:
        print(format_error("Step 9: Email", e))
        raise

def copy_source_files(raw_dir: Path, debug_dir: Path, timestamp: str):
    patterns_dir = debug_dir / "Patterns" / "global"
    timings_dir = debug_dir / "Timings" / "global"
    patterns_dir.mkdir(parents=True, exist_ok=True)
    timings_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(patterns_dir, 0o777)
    os.chmod(timings_dir, 0o777)
    
    try:
        results = []
        for file in SOURCE_DIR.rglob("*"):
            if file.is_file() and any(file.name.endswith(suffix) for suffix in TARGET_SUFFIXES):
                dst = patterns_dir / file.name if file.suffix == ".pat" else timings_dir / file.name
                shutil.copy2(file, dst)
                os.chmod(dst, 0o777)
                results.append({"File": file.name, "CopiedTo": str(dst), "Status": "SUCCESS"})
        summary_file = raw_dir / f"source_copy_summary_{timestamp}.csv"
        pd.DataFrame(results).to_csv(summary_file, index=False)
        os.chmod(summary_file, 0o777)
        print(f"Step 5.1: Source copied: {summary_file}")
    except Exception as e:
        print(format_error("Step 5.1: Source copy", e))
        raise

def process_queue(debug: bool = False):
    ensure_queue_file()
    try:
        df = pd.read_csv(
            QUEUE_FILE,
            names=["TabName", "Status", "Submitter", "Email", "SubmitTime", "LastUpdate", "Format"],
            header=0,
            skipinitialspace=True,
            quoting=csv.QUOTE_ALL
        )
        df["Format"] = df["Format"].fillna("")
    except Exception as e:
        logging.error(f"Failed to read queue file: {e}")
        raise
    
    for idx, row in df[(df["Status"] == "WAIT") & (df["Format"] == "prod")].iterrows():
        tab_name, email = row["TabName"], row["Email"]
        log_buffer = io.StringIO()
        sys.stdout = log_buffer
        try:
            df.loc[idx, "Status"] = "RUNNING"
            df.loc[idx, "LastUpdate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df.to_csv(QUEUE_FILE, index=False, quoting=csv.QUOTE_ALL)
            logging.info(f"[INFO] Task '{tab_name}' (Index: {idx}) RUNNING")
            
            timestamp = datetime.now().strftime("%m%d_%H%M%S")
            batch_id = f"{tab_name}_{timestamp}"
            raw_dir = RAW_DIR / batch_id
            debug_dir = raw_dir / f"DEBUG_{timestamp}"
            for d in [raw_dir, debug_dir, debug_dir / "Patterns", debug_dir / "Timings", debug_dir / "Flows"]:
                d.mkdir(parents=True, exist_ok=True)
                os.chmod(d, 0o777)
            
            xlsx_path = download_google_sheet(timestamp)
            csv_path = convert_excel_to_csv(xlsx_path, tab_name, raw_dir, debug)
            temp_dir = rsync_pattern_files(csv_path, raw_dir, debug_dir, timestamp)
            generate_timing_files(csv_path, temp_dir, debug_dir)
            generate_sequence_files(csv_path, debug_dir)
            copy_source_files(raw_dir, debug_dir, timestamp)
            generate_flow_files(csv_path, debug_dir, timestamp, debug)
            zip_file = package_outputs(debug_dir, batch_id)
            dest_zip = copy_to_testprogram(zip_file, batch_id)
            
            sys.stdout = sys.__stdout__
            send_email(email, batch_id, dest_zip, log_buffer.getvalue())
            df.loc[idx, "Status"] = "DONE"
        except Exception as e:
            sys.stdout = sys.__stdout__
            send_email(email, batch_id, zip_file if 'zip_file' in locals() else Path("N/A"), log_buffer.getvalue())
            df.loc[idx, "Status"] = "FAILED"
            print(format_error(f"Workflow for '{tab_name}' (Index: {idx})", e))
        finally:
            df.loc[idx, "LastUpdate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df.to_csv(QUEUE_FILE, index=False, quoting=csv.QUOTE_ALL)
            log_buffer.close()
            print(f"Completed tab '{tab_name}' (Index: {idx})")

def main():
    parser = argparse.ArgumentParser(description="IntelliGen Prod workflow")
    parser.add_argument("--tab", help="Tab name to submit")
    parser.add_argument("--gen", action="store_true", help="Process queue")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()
    
    if not (args.tab or args.gen):
        print("Error: Provide --tab or --gen")
        sys.exit(1)
    
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, 
                       format="%(asctime)s [%(levelname)s] %(message)s")
    
    if args.gen:
        process_queue(debug=args.debug)
    elif args.tab:
        submit_task(args.tab)

if __name__ == "__main__":
    main()

