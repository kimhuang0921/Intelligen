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

class Config:
    BASE_DIR = Path("/work/kimhuang/1_Python/10_IntelliGen")
    CREDENTIALS_FILE = BASE_DIR / "gsheet_key.json"
    TOKEN_PICKLE = BASE_DIR / "token.pickle"
    SUBMIT_SHEET_ID = "1_Qk9E4BhBxC7kPvZGrMlfdnyMMJmE9nqJfeMYyWP3IY"
    DOWNLOAD_DIR = BASE_DIR / "downloads"
    CONFIG_FILE = BASE_DIR / "repack_config.json"
    RAW_DIR = BASE_DIR / "raw"
    TESTPROGRAM_DIR = Path("/projects/ga0/patterns/testprogram")
    FALLBACK_ROOT = Path("/projects/ga0/patterns/release_pattern/src")
    SOURCE_DIR = Path("/work/kimhuang/1_Python/10_IntelliGen/source")
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly"
    ]
    TARGET_SUFFIXES = [".pat", "_wvt.spec", "_tim.spec", "_spec_decl.spec", "_seq.seq", "_specs.spec"]

def extract_file_base(filepath: str) -> str:
    name = Path(filepath.strip()).name
    stem = Path(name).stem
    while "." in stem:
        stem = Path(stem).stem
    return stem.rstrip("_")

def format_error_message(step: str, error: Exception) -> str:
    return f"{step}: Error occurred\n  Type: {type(error).__name__}\n  Message: {str(error)}\n"

def format_file(filepath: Path) -> None:
    try:
        with open(filepath, 'r') as f:
            content = f.read()
        lines = content.split('\n')
        formatted = []
        indent_level = 0
        indent_unit = '  '
        max_key_length = 0
        for line in lines:
            stripped = line.strip()
            if '=' in stripped and not stripped.startswith('in ') and not stripped.endswith('{'):
                key = stripped.split('=')[0].strip()
                max_key_length = max(max_key_length, len(key))
        for line in lines:
            stripped = line.strip()
            if not stripped:
                formatted.append('')
                continue
            if stripped.startswith('}'):
                indent_level -= 1
            indent = indent_unit * indent_level
            if '=' in stripped and not stripped.startswith('in ') and not stripped.endswith('{'):
                key, value = map(str.strip, stripped.split('=', 1))
                formatted.append(f"{indent}{key:<{max_key_length}} = {value}")
            else:
                formatted.append(f"{indent}{stripped}")
            if stripped.endswith('{'):
                indent_level += 1
        with open(filepath, 'w') as f:
            f.write('\n'.join(formatted) + '\n')
        os.chmod(filepath, 0o777)
    except Exception as e:
        print(f"Failed to format {filepath}: {e}")

def download_google_sheet(config: Config, timestamp: str) -> Optional[Path]:
    try:
        creds = None
        if config.TOKEN_PICKLE.exists():
            with open(config.TOKEN_PICKLE, "rb") as token:
                creds = pickle.load(token)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not config.CREDENTIALS_FILE.exists():
                    raise FileNotFoundError(f"Credentials file not found: {config.CREDENTIALS_FILE}")
                flow = InstalledAppFlow.from_client_secrets_file(str(config.CREDENTIALS_FILE), config.SCOPES)
                creds = flow.run_local_server(port=0)
            with open(config.TOKEN_PICKLE, "wb") as token:
                pickle.dump(creds, token)
        service = build("drive", "v3", credentials=creds)
        config.DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        os.chmod(config.DOWNLOAD_DIR, 0o777)
        output_file = config.DOWNLOAD_DIR / f"IntelliGenSubmit_{timestamp}.xlsx"
        request = service.files().export_media(
            fileId=config.SUBMIT_SHEET_ID,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        with io.FileIO(output_file, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        os.chmod(output_file, 0o777)
        print(f"Step 1: Download completed: {output_file}")
        return output_file
    except Exception as e:
        print(format_error_message("Step 2: Download failed", e))
        raise

def convert_excel_to_csv(xlsx_path: Path, sheet_name: str, raw_dir: Path) -> Path:
    output_path = raw_dir / "flowgenInput.csv"
    try:
        xls = pd.read_excel(xlsx_path, sheet_name=None)
        available_sheets = list(xls.keys())
        corrected_sheet_name = sheet_name
        if sheet_name not in xls:
            for possible_sheet in available_sheets:
                if possible_sheet.lstrip("0") == sheet_name:
                    corrected_sheet_name = possible_sheet
                    break
            else:
                raise ValueError(f"Sheet '{sheet_name}' not found. Available sheets: {available_sheets}")
        plan_df = xls[corrected_sheet_name]
        preamble_df = xls.get("Preamble Definition")
        if preamble_df is None:
            raise ValueError("Sheet 'Preamble Definition' not found")
        def build_preamble_mapping(preamble_df: pd.DataFrame) -> dict:
            mapping = {}
            preamble_df.columns = [str(col).strip().lower() for col in preamble_df.columns]
            for col in preamble_df.columns:
                entries = [str(e).strip() for e in preamble_df[col].dropna() if str(e).strip()]
                if entries:
                    mapping[col.strip().lower()] = entries
            return mapping
        def generate_ts_path(original_path: str, pattern_key: str) -> str:
            pattern_name = extract_file_base(original_path)
            prefix = pattern_name[:pattern_name.index(pattern_key)]
            postfix = pattern_name[pattern_name.index(pattern_key) + len(pattern_key):]
            if pattern_key in ["int_sa_chain_edt_pl", "int_sa_scan_edt_pl", "ssn_loopback", "ssn_continuity"]:
                ts_name = f"{prefix}int_sa_edt_ts{postfix}"
            elif pattern_key == "int_tdf_scan_edt_pl":
                ts_name = f"{prefix}int_tdf_edt_ts{postfix}"
            else:
                raise ValueError(f"Unknown pattern_key: {pattern_key}")
            dir_path = os.path.dirname(original_path)
            return os.path.join(dir_path, f"{ts_name}.stil.gz")
        def get_preamble_list(pattern_name: str, full_path: str, preamble_set: str, preamble_mapping: dict) -> list:
            special_patterns = [
                "int_sa_edt_ts", "int_sa_chain_edt_pl", "int_sa_scan_edt_pl",
                "ssn_loopback", "ssn_continuity", "int_tdf_edt_ts", "int_tdf_scan_edt_pl"
            ]
            matched_key = next((key for key in special_patterns if key in pattern_name), None)
            if matched_key:
                ts_path = generate_ts_path(full_path, matched_key)
                return preamble_mapping.get("preamble_set_scan", []) + [ts_path]
            else:
                preamble_list = preamble_mapping.get(preamble_set, [])
                if not preamble_list:
                    print(f"Step 3: Preamble set not found: {preamble_set}")
                return preamble_list
        def check_pattern_paths(patterns: set, base_dir: str = "/projects/ga0/patterns/release_pattern/src") -> tuple:
            path_list = []
            missing_patterns = []
            for pattern in patterns:
                pattern_name = extract_file_base(pattern)
                abs_folder = os.path.join(base_dir, pattern_name)
                exists = os.path.exists(abs_folder)
                path_list.append({
                    "pattern": pattern,
                    "pattern_name": pattern_name,
                    "absolute_path": abs_folder,
                    "exists": exists
                })
                if not exists:
                    missing_patterns.append(pattern_name)
            warning = ""
            if missing_patterns:
                missing_str = ", ".join(f'"{p}"' for p in missing_patterns)
                warning = f"WARNING: Missing {missing_str}, please submit the stil files"
            return path_list, warning
        def build_timing_spec(flow_path: str) -> str:
            pattern_name = extract_file_base(flow_path)
            return f"time_{pattern_name}" if pattern_name else "time_default"
        preamble_mapping = build_preamble_mapping(preamble_df)
        plan_df.columns = [col.strip().lower() for col in plan_df.columns]
        columns = ['shmoo x signal', 'shmoo x start', 'shmoo x stop', 'shmoo x step size',
                   'shmoo y signal', 'shmoo y start', 'shmoo y stop', 'shmoo y step size', 'enable fail logs']
        if 'pattern name (with full path)' not in plan_df.columns.str.lower():
            raise ValueError("Missing required column 'pattern name (with full path)'")
        output_rows = []
        patterns = set()
        db_id = 1
        for idx, row in plan_df.iterrows():
            full_path = row.get("pattern name (with full path)", "")
            if not isinstance(full_path, str) or not full_path.strip():
                print(f"Step 3: Skipping row {idx}: Invalid pattern path")
                continue
            pattern_name = extract_file_base(full_path)
            if "_ts" in pattern_name.lower():
                print(f"Step 3: Skipping row {idx}: Pattern contains _ts")
                continue
            preamble_set = str(row.get("preamble set", "")).strip().lower()
            preamble_list = get_preamble_list(pattern_name, full_path, preamble_set, preamble_mapping)
            group_id = f"DB{db_id}"
            db_id += 1
            patterns.add(full_path)
            patterns.update(preamble_list)
            for preamble in preamble_list:
                output_rows.append({
                    "Identifier": group_id,
                    "Flows": full_path,
                    "Suites": preamble,
                    "Timing Spec": build_timing_spec(preamble),
                    "TestMethod": "FunctionalTest_wo_profiling",
                    **{col: "" for col in columns}
                })
            output_rows.append({
                "Identifier": group_id,
                "Flows": full_path,
                "Suites": full_path,
                "Timing Spec": build_timing_spec(full_path),
                "TestMethod": row.get("testmethod", "FunctionalTest_wo_profiling"),
                **{col: row.get(col, "") for col in columns}
            })
        output_df = pd.DataFrame(output_rows)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(output_path.parent, 0o777)
        output_df.to_csv(output_path, index=False)
        os.chmod(output_path, 0o777)
        path_list, warning = check_pattern_paths(patterns)
        path_output = output_path.parent / "pattern_paths.txt"
        with open(path_output, "w") as f:
            for entry in path_list:
                f.write(f"Pattern: {entry['pattern']}\nPatternName: {entry['pattern_name']}\nAbsolute Path: {entry['absolute_path']}\nExists: {entry['exists']}\n\n")
        os.chmod(path_output, 0o777)
        print(f"Step 3: Conversion completed: {output_path}")
        if warning:
            print(f"Step 3: {warning}")
        return output_path
    except Exception as e:
        print(format_error_message("Step 3: Conversion failed", e))
        raise

def rsync_pattern_files(csv_path: Path, raw_dir: Path, debug_dir: Path, config: Config, timestamp: str) -> Path:
    try:
        if not shutil.which("rsync"):
            output_dir = raw_dir / "temp"
            output_dir.mkdir(parents=True, exist_ok=True)
            summary_file = output_dir / f"suite_rsync_summary_{timestamp}.csv"
            pd.DataFrame([]).to_csv(summary_file, index=False)
            os.chmod(summary_file, 0o777)
            print(f"Step 4: rsync command not found, created empty summary: {summary_file}")
            return output_dir
        output_dir = raw_dir / "temp"
        patterns_dir = debug_dir / "Patterns" / "global"
        output_dir.mkdir(parents=True, exist_ok=True)
        patterns_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(output_dir, 0o777)
        os.chmod(patterns_dir, 0o777)
        df = pd.read_csv(csv_path)
        df["Suites"] = df["Suites"].astype(str).str.strip()
        df = df[df["Suites"] != ""]
        df["PatternBase"] = df["Suites"].apply(extract_file_base)
        unique_patterns = df["PatternBase"].drop_duplicates().tolist()
        results = []
        for pattern_base in unique_patterns:
            target_files = [f"{pattern_base}{suffix}" for suffix in config.TARGET_SUFFIXES]
            found_paths = []
            for root, _, files in os.walk(config.FALLBACK_ROOT):
                for name in target_files:
                    if name in files:
                        found_paths.append(Path(root) / name)
            found_paths = list(set(found_paths))
            if not found_paths:
                print(f"Step 4: Pattern not found: {pattern_base}")
                results.append({"Pattern": pattern_base, "Copied": 0, "Status": "NOT_FOUND_ANY"})
                continue
            for actual_path in found_paths:
                file_name = actual_path.name
                dst_path = output_dir / file_name
                subprocess.run(["rsync", "-av", str(actual_path), str(dst_path)], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                os.chmod(dst_path, 0o777)
                results.append({"Pattern": pattern_base, "File": file_name, "CopiedTo": str(dst_path), "Status": "SUCCESS"})
                if file_name.endswith(".pat"):
                    pat_dst_path = patterns_dir / file_name
                    shutil.copy(dst_path, pat_dst_path)
                    os.chmod(pat_dst_path, 0o777)
                    results.append({"Pattern": pattern_base, "File": file_name, "CopiedTo": str(pat_dst_path), "Status": "COPIED_TO_PATTERNS"})
        summary_file = output_dir / f"suite_rsync_summary_{timestamp}.csv"
        pd.DataFrame(results).to_csv(summary_file, index=False)
        os.chmod(summary_file, 0o777)
        print(f"Step 4: Rsync completed: {summary_file}")
#        print(f"Step 4: .pat files copied to: {patterns_dir}")
        return output_dir
    except Exception as e:
        print(format_error_message("Step 4: Rsync failed", e))
        raise
def generate_timing_files(csv_path: Path, temp_dir: Path, debug_dir: Path) -> None:
    try:
        input_dir = temp_dir
        output_dir = debug_dir / "Timings" / "global"
        output_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(output_dir, 0o777)
        df = pd.read_csv(csv_path)
        if "Suites" not in df.columns:
            raise ValueError("CSV missing required column: Suites")
        df["PatternName"] = df["Suites"].apply(lambda x: Path(str(x)).stem.replace(".stil", "").replace(".gz", ""))
        pattern_set = set(df["PatternName"].tolist())
#        print(f"Pattern set: {pattern_set}")
#        print(f"Files in input_dir: {[f.name for f in input_dir.iterdir() if f.is_file()]}")
        
        # 儲存每個模式對應的 period_var
        period_vars = {}
        
        def read_header(lines):
            header = []
            in_header = False
            for line in lines:
                if line.strip().startswith("/*"):
                    in_header = True
                if in_header:
                    header.append(line)
                if line.strip().endswith("*/"):
                    break
            return header
        def append_ratio(expr_line, period_var):
            return re.sub(rf'\b({period_var})(?!\s*\*)', r'\1*Ratio', expr_line)
        def extract_pads(setup_line):
            pin_expr = setup_line.strip().removeprefix("setup digInOut").strip().rstrip("{").strip()
            pin_list = [p.strip() for p in re.split(r"[+]", pin_expr)]
            return pin_list
        def get_period_from_spec_decl(path_in: Path) -> str:
            try:
#                print(f"Reading _spec_decl.spec file: {path_in}")
                if not path_in.exists():
                    print(f"Error: File {path_in} does not exist")
                    return "per_40"
                lines = path_in.read_text(encoding='utf-8').splitlines()
                for line in lines:
 #                   print(f"Processing line: '{line}'")
                    match = re.match(r'\s*var\s+\w+\s+(\w+)\s*;', line.strip())
                    if match:
                        var_name = match.group(1)
 #                       print(f"Found variable: {var_name}")
                        return var_name
                print(f"Warning: No variable found in {path_in}, using default per_40")
                return "per_40"
            except Exception as e:
                print(f"Error reading {path_in}: {str(e)}")
                return "per_40"
        def extract_wft_name(path_in: Path) -> str:
            try:
                lines = path_in.read_text().splitlines()
                for line in lines:
                    match = re.match(r'\s*set\s+([^\s;]+);\s*$', line.strip())
                    if match:
                        wft_name = match.group(1).strip()
                        if wft_name and not wft_name[0].isdigit():
                            return wft_name
                print(f"Warning: No valid timing name found in {path_in}, using default 'wft'")
                return "wft"
            except Exception as e:
                print(f"Error reading {path_in}: {e}")
                return "wft"
        def handle_spec_decl(path_in, path_out, pattern):
            lines = path_in.read_text().splitlines()
            header_end_idx = 0
            for idx, line in enumerate(lines):
                if line.strip().endswith("*/"):
                    header_end_idx = idx + 1
                    break
            insert_lines = [
                "import configuration.IO_Group;",
                "import Timings.global.TimingRatio;"
            ]
            updated = lines[:header_end_idx] + insert_lines + lines[header_end_idx:]
            path_out.write_text('\n'.join(updated) + '\n')
            format_file(path_out)
            os.chmod(path_out, 0o777)
        def handle_specs(path_in, path_out, pattern):
            lines = path_in.read_text().splitlines()
            header = read_header(lines)
            imports = [
                f"import Timings.global.{pattern}_spec_decl;",
                f"import Timings.global.{pattern}_tim;",
                f"import Timings.global.{pattern}_wvt;",
                "import configuration.IO_Group;",
                "import Timings.global.AllRefClkPins10ns_diff_tim;",
                "",
                f"spec {pattern}_specs {{",
                "\t\tper_AllRefClkPins10ns = 10.00 ns;",
                "\t\tRatio = 1.0;",
                "}"
            ]
            path_out.write_text('\n'.join(header + [""] + imports) + '\n')
            format_file(path_out)
            os.chmod(path_out, 0o777)
        def handle_tim(path_in, path_out, pattern, period_var):
#            print(f"Generating _tim.spec for pattern {pattern} with period_var: {period_var}")
            lines = path_in.read_text().splitlines()
            header = read_header(lines)
            pad_signals, setup_blocks = [], []
            current_block = []
            capture = False
            wft_name = extract_wft_name(path_in)
            for line in lines:
                if line.strip().startswith("setup digInOut"):
                    pins = extract_pads(line)
                    if any(p.lower().startswith("gpio") for p in pins):
                        capture = False
                        continue
                    pad_signals.extend(pins)
                    current_block = [line]
                    capture = True
                elif capture:
                    current_block.append(append_ratio(line, period_var))
                    if "}" in line:
                        setup_blocks.append("\n".join(current_block))
                        setup_blocks.append("}\n")
                        capture = False
            gallio_block = [
                f"setup digInOut G_ALL_IO - {' - '.join(pad_signals)} {{",
                f"    set timing {wft_name} {{",
                append_ratio(f"        period = {period_var};", period_var),
                append_ratio(f"        d1 = 0.0 * {period_var};", period_var),
                "    }",
                "}"
            ]
            body = [f"    set {wft_name};"] + gallio_block + setup_blocks
            content = header + [
                f"import Timings.global.{pattern}_spec_decl;",
                "import configuration.IO_Group;",
                "",
                f"spec {pattern}_tim {{"
            ] + body + ["}"]
            path_out.write_text('\n'.join(content) + '\n')
            format_file(path_out)
            os.chmod(path_out, 0o777)
        def handle_wvt(path_in, path_out):
            lines = path_in.read_text().splitlines()
            result = []
            skipping = False
            brace_level = 0
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
            os.chmod(path_out, 0o777)
        def generate_timing_ratio_file(output_dir: Path):
            ratio_file = output_dir / "TimingRatio.spec"
            if ratio_file.exists():
                return
            content = [
                "spec TimingRatio {",
                "    var Double Ratio;",
                "}"
            ]
            ratio_file.write_text('\n'.join(content) + '\n')
            format_file(ratio_file)
            os.chmod(ratio_file, 0o777)
        
        # 第一遍：處理 _spec_decl.spec 文件，儲存 period_var
        for file in input_dir.iterdir():
            if not file.is_file() or file.suffix != ".spec":
                continue
            name = file.stem
 #           print(f"Processing file: {file.name}")
            matched = None
            for pat in pattern_set:
                if name.startswith(pat):
                    matched = pat
                    break
            if not matched:
                print(f"No pattern match for file: {file.name}")
                continue
            if "_spec_decl" in name:
                period_var = get_period_from_spec_decl(file)
                period_vars[matched] = period_var
                handle_spec_decl(file, output_dir / file.name, matched)
        
        # 第二遍：處理其他 .spec 文件，使用對應的 period_var
        spec_files_found = False
        for file in input_dir.iterdir():
            if not file.is_file() or file.suffix != ".spec":
                continue
            name = file.stem
#            print(f"Processing file: {file.name}")
            matched = None
            for pat in pattern_set:
                if name.startswith(pat):
                    matched = pat
                    break
            if not matched:
                print(f"No pattern match for file: {file.name}")
                continue
            spec_files_found = True
            # 使用對應模式的 period_var，默認為 per_40
            period_var = period_vars.get(matched, "per_40")
            if "_specs" in name:
                handle_specs(file, output_dir / file.name, matched)
            elif "_tim" in name:
                handle_tim(file, output_dir / file.name, matched, period_var)
            elif "_wvt" in name:
                handle_wvt(file, output_dir / file.name)
        
        if not spec_files_found:
            raise FileNotFoundError(f"No matching .spec files found in {input_dir}")
        generate_timing_ratio_file(output_dir)
        print("Step 5: Timing files generated")
    except Exception as e:
        print(format_error_message("Step 5: Timing generation failed", e))
        raise

def generate_sequence_files(csv_path: Path, debug_dir: Path) -> None:
    try:
        output_dir = debug_dir / "Patterns" / "global"
        output_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(output_dir, 0o777)
        df = pd.read_csv(csv_path)
        for suite_path in df['Suites'].dropna().unique():
            pattern_name = extract_file_base(suite_path)
            output_file = output_dir / f"{pattern_name}_Pseq.seq"
            with open(output_file, "w") as f:
                f.write(f"""sequence {pattern_name}_Pseq {{
    parallel {{
        sequential {{
            patternCall Patterns.global.AllRefClkPins10ns_diff;
        }}
        sequential {{
            patternCall Patterns.global.{pattern_name};
        }}
    }}
}}""")
            format_file(output_file)
            os.chmod(output_file, 0o777)
    except Exception as e:
        print(format_error_message("Step 6: Sequence generation failed", e))
        raise

def generate_flow_files(csv_path: Path, debug_dir: Path, timestamp: str) -> None:
    try:
        @dataclass
        class FlowConfig:
            extensions: List[str] = None
            default_testmethod: str = "FunctionalTest_wo_profiling"
            default_timing_prefix: str = "Timings"
            level_spec: str = "Levels.DFT_Vtyp.DFT_Vtyp_specValue"
            max_failed_cycles: int = 2000
            ffv_cycles: int = 2000
            use_integer_steps: bool = False
            suite_template: str = """    suite {name} calls digital.{testmethod} {{
        timingSpec    = setupRef({timingspec});
        levelSpec    = setupRef({level_spec});
        operatingSequence = setupRef(Patterns.global.{name}_Pseq);
        maxFailedCycles = {max_failed_cycles};
        ffvCycles    = {ffv_cycles};
    }}
    """
            flow_header_template: str = """flow {flow_name} {{
    in failNonStop = testProgramVariables.getBoolean("SYS.OFFLINE") || testProgramVariables.getBoolean("TP_FAIL_NON_STOP");
    setup {{
    """
            main_flow_name: str = "MainFlow"
            main_flow_template: str = """flow {main_flow_name} {{
    setup {{
        {setup_block}
        {shmoo_block}
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
                range.resolution = {y_steps};
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
                if self.extensions is None:
                    self.extensions = [".stil.gz", ".stil", ".STIL.GZ", ".STIL"]
        class SuiteGenerator:
            def __init__(self, config: FlowConfig):
                self.config = config
            def extract_basename(self, path: str) -> str:
                if not path or not isinstance(path, str):
                    return ""
                base = os.path.basename(path.strip())
                for ext in self.config.extensions:
                    if base.lower().endswith(ext.lower()):
                        return base[:-len(ext)]
                return base
            def generate_suite_block(self, name: str, testmethod: str, timingspec: str, row: pd.Series = None) -> str:
                return self.config.suite_template.format(
                    name=name,
                    testmethod=testmethod,
                    timingspec=timingspec,
                    level_spec=self.config.level_spec,
                    max_failed_cycles=self.config.max_failed_cycles,
                    ffv_cycles=self.config.ffv_cycles
                )
        class FlowGenerator:
            def __init__(self, config: FlowConfig, input_csv: str, debug_dir: str, timestamp: str):
                self.config = config
                self.suite_generator = SuiteGenerator(config)
                self.flow_info = []
                self.csv_name = f"DEBUG_{timestamp}"
                debug_dir_name = Path(debug_dir).name
                self.output_dir = Path(debug_dir) / "Flows" / debug_dir_name
                self.output_dir.mkdir(parents=True, exist_ok=True)
                os.chmod(self.output_dir, 0o777)
            def parse_time(self, value):
                try:
                    if isinstance(value, (int, float)):
                        return float(value)
                    value = str(value).lower().strip()
                    match = re.match(r"([-]?\d*\.?\d*)\s*(ns|us|ms|s)?", value)
                    if not match:
                        print(f"Step 7: Invalid time format: {value}")
                        return float(value)
                    num, unit = match.groups()
                    num = float(num)
                    if unit == "ns":
                        return num * 1e-9
                    elif unit == "us":
                        return num * 1e-6
                    elif unit == "ms":
                        return num * 1e-3
                    return num
                except (ValueError, TypeError):
                    print(f"Step 7: Invalid time value: {value}")
                    return float(value)
            def generate_tracking_blocks(self, preamble_suites: List[str], y_start: float, y_stop: float) -> str:
                tracking_blocks = ""
                for i, suite in enumerate(preamble_suites, 1):
                    resource_name = f"Timings.{suite}.project_tim_specs.per_AllRefClkPins10ns"
                    tracking_blocks += f"""                tracking[pr{i}] = {{resourceType = specVariable;resourceName = "{resource_name}";range.start = {y_start};range.stop = {y_stop};}};\n"""
                return tracking_blocks.strip()
            def generate_flow_file(self, df: pd.DataFrame, db_id: str) -> None:
                pattern_row = df[df["Identifier"] == db_id]
                if pattern_row.empty:
                    print(f"Step 7: No rows found for {db_id}")
                    return
                if not pattern_row["Flows"].nunique() == 1:
                    print(f"Step 7: Multiple Flows values found for {db_id}: {pattern_row['Flows'].unique()}")
                    return
                main_row = pattern_row.iloc[-1]
                suite_names = []
                for _, row in pattern_row.iterrows():
                    suite_name = self.suite_generator.extract_basename(str(row["Suites"]))
                    suite_names.append(suite_name)
                flow_name = f"RV_{suite_names[-1]}_{db_id}" if suite_names else f"RV_{db_id}"
                flow_block = self.config.flow_header_template.format(flow_name=flow_name)
                for suite_name in suite_names:
                    testmethod = main_row.get("TestMethod", self.config.default_testmethod)
                    timing = f"{self.config.default_timing_prefix}.global.{suite_name}_specs"
                    flow_block += self.suite_generator.generate_suite_block(suite_name, testmethod, timing, main_row)
                flow_block += "\n    }\n"
                flow_block += "\n    execute {\n"
                for name in suite_names:
                    flow_block += f"        {name}.execute();\n"
                flow_block += "    }\n}\n"
                shmoo_data = None
                if pd.notna(main_row.get("shmoo x signal")):
                    try:
                        x_start = float(main_row["shmoo x start"])
                        x_stop = float(main_row["shmoo x stop"])
                        x_steps = float(main_row["shmoo x step size"])
                        is_mv = x_start > 2 or x_stop > 2
                        if is_mv:
                            x_start /= 1000
                            x_stop /= 1000
                            x_steps /= 1000
                        if x_start > x_stop and x_steps > 0:
                            x_steps = -abs(x_steps)
                        elif x_start < x_stop and x_steps < 0:
                            x_steps = abs(x_steps)
                        y_start = self.parse_time(main_row["shmoo y start"])
                        y_stop = self.parse_time(main_row["shmoo y stop"])
                        y_steps_raw = main_row.get("shmoo y step size")
                        y_steps = self.parse_time(y_steps_raw) if pd.notna(y_steps_raw) else 0
                        if y_start > y_stop and y_steps > 0:
                            y_steps = -abs(y_steps)
                        elif y_start < y_stop and y_steps < 0:
                            y_steps = abs(y_steps)
                        if self.config.use_integer_steps:
                            x_steps = int(abs(x_steps)) if x_steps != 0 else 0
                            y_steps = int(abs(y_steps)) if y_steps != 0 else 0
                        is_2d = y_steps != 0 and pd.notna(y_steps_raw) and y_steps_raw != "0"
                        if suite_names:
                            y_suite = suite_names[-1]
                            preamble_suites = suite_names[:-1]
                            y_timing_spec = f"{self.config.default_timing_prefix}.{y_suite}.per_AllRefClkPins10ns"
                            tracking_blocks = self.generate_tracking_blocks(preamble_suites, y_start, y_stop)
                        else:
                            y_timing_spec = ""
                            tracking_blocks = ""
                            is_2d = False
                            print(f"Step 7: No suites found for {db_id}, disabling 2D shmoo")
                        shmoo_data = {
                            "db_id": db_id,
                            "target": flow_name,
                            "x_signal": main_row["shmoo x signal"],
                            "x_start": x_start,
                            "x_stop": x_stop,
                            "x_steps": x_steps,
                            "y_timing_spec": y_timing_spec,
                            "y_start": y_start,
                            "y_stop": y_stop,
                            "y_steps": y_steps,
                            "tracking_blocks": tracking_blocks
                        } if is_2d else {
                            "db_id": db_id,
                            "target": flow_name,
                            "x_signal": main_row["shmoo x signal"],
                            "x_start": x_start,
                            "x_stop": x_stop,
                            "x_steps": x_steps,
                            "y_timing_spec": "",
                            "y_start": 0,
                            "y_stop": 0,
                            "y_steps": 0
                        }
                    except Exception as e:
                        print(f"Step 7: Failed to generate shmoo data for {db_id}: {e}")
                self.flow_info.append({"flow_name": flow_name, "shmoo_data": shmoo_data})
                output_path = self.output_dir / f"{flow_name}.flow"
                output_path.parent.mkdir(parents=True, exist_ok=True)
                os.chmod(output_path.parent, 0o777)
                output_path.write_text(flow_block)
                format_file(output_path)
                os.chmod(output_path, 0o777)
            def generate_main_flow(self) -> None:
                if not self.flow_info:
                    print("Step 7: No flows generated, skipping MainFlow creation")
                    return
                setup_block = ""
                shmoo_block = ""
                execute_block = ""
                for info in sorted(self.flow_info, key=lambda x: x["flow_name"]):
                    flow_name = info["flow_name"]
                    setup_block += f"        flow {flow_name} calls Flows.{self.csv_name}.{flow_name}{{}}\n"
                    execute_block += f"        {flow_name}.execute();\n"
                    if info["shmoo_data"]:
                        is_2d = info["shmoo_data"]["y_steps"] != 0
                        if not is_2d:
                            shmoo_content = self.config.shmoo_x_only_template.format(**info["shmoo_data"])
                        else:
                            shmoo_content = self.config.shmoo_template.format(**info["shmoo_data"])
                        shmoo_block += shmoo_content
                        execute_block += f"        shmoo_{info['shmoo_data']['db_id']}.execute();\n"
                main_flow_content = self.config.main_flow_template.format(
                    main_flow_name=self.config.main_flow_name,
                    setup_block=setup_block,
                    shmoo_block=shmoo_block,
                    execute_block=execute_block
                )
                output_path = self.output_dir / f"{self.config.main_flow_name}.flow"
                output_path.parent.mkdir(parents=True, exist_ok=True)
                os.chmod(output_path.parent, 0o777)
                output_path.write_text(main_flow_content)
                format_file(output_path)
                os.chmod(output_path, 0o777)
        config = FlowConfig()
        df = pd.read_csv(csv_path)
        required_columns = ["Identifier", "Flows", "Suites"]
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"Missing required columns: {set(required_columns) - set(df.columns)}")
        generator = FlowGenerator(config, str(csv_path), str(debug_dir), timestamp)
        identifiers = sorted(set(df["Identifier"]))
        for db_id in identifiers:
            generator.generate_flow_file(df, db_id)
        generator.generate_main_flow()
        print("Step 7: Flow files generated")
    except Exception as e:
        print(format_error_message("Step 7: Flow generation failed", e))
        raise

def package_outputs(debug_dir: Path, batch_id: str) -> Path:
    try:
        debug_dir = Path(debug_dir)
        zip_file = debug_dir.parent / f"{batch_id}.zip"
        for subdir in ["Patterns", "Timings", "Flows"]:
            src = debug_dir / subdir
            if not src.exists():
                src.mkdir(parents=True, exist_ok=True)
                os.chmod(src, 0o777)
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(debug_dir):
                for file in files:
                    file_path = Path(root) / file
                    arcname = str(file_path.relative_to(debug_dir.parent))
                    zf.write(file_path, arcname)
        os.chmod(zip_file, 0o777)
        print(f"Step 8: Packaged outputs: {zip_file}")
        return zip_file
    except Exception as e:
        print(format_error_message("Step 8: Packaging failed", e))
        raise

def copy_to_testprogram(zip_file: Path, batch_id: str, config: Config) -> Path:
    try:
        config.TESTPROGRAM_DIR.mkdir(parents=True, exist_ok=True)
        os.chmod(config.TESTPROGRAM_DIR, 0o777)
        dest_zip = config.TESTPROGRAM_DIR / f"{batch_id}.zip"
        shutil.copy(zip_file, dest_zip)
        os.chmod(dest_zip, 0o777)
        print(f"Step 8.2: Copied zip to: {dest_zip}")
        return dest_zip
    except Exception as e:
        print(format_error_message("Step 8.2: Copy failed", e))
        raise

def send_email(submitter_email: str, batch_id: str, zip_file: Path, config: Config) -> None:
    try:
        with open(config.CONFIG_FILE, "r") as f:
            email_config = json.load(f)["email"]
        msg = MIMEMultipart()
        msg["From"] = email_config["from"]
        msg["To"] = submitter_email
        msg["Subject"] = f"IntelliGen Task Completed: {batch_id}"
        body = f"""
Dear Submitter,

Your IntelliGen task ({batch_id}) has been successfully completed.
The output files are packaged in: {zip_file}
Copied to: {config.TESTPROGRAM_DIR}/{batch_id}.zip

Best regards,
IntelliGen Automation
"""
        msg.attach(MIMEText(body, "plain"))
        for attachment in email_config.get("attachments", []):
            attachment_path = config.BASE_DIR / attachment
            if attachment_path.exists():
                with open(attachment_path, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename={attachment_path.name}"
                )
                msg.attach(part)
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(email_config["from"], email_config["password"])
            server.send_message(msg)
        print("Step 9: Email sent")
    except Exception as e:
        print(format_error_message("Step 9: Email sending failed", e))
        raise
def copy_source_files(raw_dir: Path, debug_dir: Path, config: Config, timestamp: str) -> None:
    try:
        patterns_dir = debug_dir / "Patterns" / "global"
        timings_dir = debug_dir / "Timings" / "global"
        patterns_dir.mkdir(parents=True, exist_ok=True)
        timings_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(patterns_dir, 0o777)
        os.chmod(timings_dir, 0o777)
        results = []
        for root, _, files in os.walk(config.SOURCE_DIR):
            for file in files:
                if any(file.endswith(suffix) for suffix in config.TARGET_SUFFIXES):
                    src_path = Path(root) / file
                    if file.endswith(".pat"):
                        dst_path = patterns_dir / file
                    else:  # .spec files (_wvt.spec, _tim.spec, _spec_decl.spec, _specs.spec)
                        dst_path = timings_dir / file
                    shutil.copy2(src_path, dst_path)  # Use copy2 to preserve metadata
                    os.chmod(dst_path, 0o777)
                    results.append({"File": file, "CopiedTo": str(dst_path), "Status": "SUCCESS"})
        summary_file = raw_dir / f"source_copy_summary_{timestamp}.csv"
        pd.DataFrame(results).to_csv(summary_file, index=False)
        os.chmod(summary_file, 0o777)
        print(f"Step 6.1: Source files copied, summary: {summary_file}")
    except Exception as e:
        print(format_error_message("Step 6.1: Source copy failed", e))
        raise
def main(tab_name: str, email: str):
    config = Config()
    try:
        if not tab_name or not email:
            raise ValueError("Tab name and email are required")
        timestamp = datetime.now().strftime("%m%d%H%M%S")
        batch_id = f"{tab_name}_{timestamp}"
        raw_dir = config.RAW_DIR / batch_id
        debug_dir = raw_dir / f"DEBUG_{timestamp}"
        raw_dir.mkdir(parents=True, exist_ok=True)
        debug_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(raw_dir, 0o777)
        os.chmod(debug_dir, 0o777)
        for subdir in ["Patterns", "Timings", "Flows"]:
            subdir_path = debug_dir / subdir
            subdir_path.mkdir(parents=True, exist_ok=True)
            os.chmod(subdir_path, 0o777)
        xlsx_path = download_google_sheet(config, timestamp)
        os.chmod(xlsx_path, 0o777)
        csv_path = convert_excel_to_csv(xlsx_path, tab_name, raw_dir)
        os.chmod(csv_path, 0o777)
        temp_dir = rsync_pattern_files(csv_path, raw_dir, debug_dir, config, timestamp)
        os.chmod(temp_dir, 0o777)
        generate_timing_files(csv_path, temp_dir, debug_dir)
        generate_sequence_files(csv_path, debug_dir)
        copy_source_files(raw_dir, debug_dir, config, timestamp)
        generate_flow_files(csv_path, debug_dir, timestamp)
        zip_file = package_outputs(debug_dir, batch_id)
        dest_zip = copy_to_testprogram(zip_file, batch_id, config)
        send_email(email, batch_id, dest_zip, config)
        print(f"Workflow completed for tab '{tab_name}'")
    except Exception as e:
        print(format_error_message("Workflow failed", e))
        raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run IntelliGen workflow for a specific tab.")
    parser.add_argument("--tab", required=True, help="Tab name in IntelliGen Submit sheet")
    args = parser.parse_args()
    submitter = getpass.getuser()
    email = f"{submitter}@rivosinc.com"
    main(args.tab, email)
