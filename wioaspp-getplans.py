import requests
import os
import time
import re
import csv
import argparse # For command-line arguments
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote, quote

# --- Configuration (Unchanged) ---
BASE_URL = "https://wioaplans.ed.gov"
BASE_DOWNLOAD_DIR = "wioa_plans"
FILE_BASE_PATH_PDFS = "/sites/default/files/pdfs/state-plan/" # For 2020+
FILE_BASE_PATH_PLANS = "/sites/default/files/state_plans/"    # For 2016, 2018
CSV_FILENAME = "wioa_download_log.csv" # Success/Found Log file
FAILED_URLS_CSV = "wioa_failed_urls.csv" # Individual Failure Log

HEADERS = { # (Unchanged)
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9', 'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1', 'Upgrade-Insecure-Requests': '1', 'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1', 'Referer': BASE_URL
}

DELAY_BETWEEN_ATTEMPTS = 5
DELAY_AFTER_SUCCESS = 1.0
TARGET_PLAN_YEARS = [2024, 2022, 2020, 2018, 2016]
PLAN_YEAR_RANGES = { 2024: "2024-2027", 2022: "2022-2023", 2020: "2020-2023" }
FOLDER_YEAR_CANDIDATES = { 2024: [2024, 2025], 2022: [2022, 2023], 2020: [2020, 2021] }

# --- Helper Functions (sanitize_filename, load logs, log_failed_url, attempt_download - mostly unchanged) ---
# (sanitize_filename, load_download_log, save_download_log, load_failed_urls_set, log_failed_url are unchanged from the previous version)
# ... (Keep these functions as they were in the last version) ...
def sanitize_filename(filename):
    try: decoded_filename = unquote(filename)
    except Exception: decoded_filename = filename
    sanitized = re.sub(r'[\\/*?:"<>|]', "", decoded_filename)
    sanitized = sanitized.replace(" ", "_")
    max_len = 200
    if len(sanitized) > max_len: name, ext = os.path.splitext(sanitized); sanitized = name[:max_len - len(ext)] + ext
    return sanitized

def load_download_log(filename):
    log = {}
    if not os.path.exists(filename): print(f"Success log file '{filename}' not found."); return log
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            if not all(col in reader.fieldnames for col in ['Year', 'State', 'URL']): print(f"Warning: Success log '{filename}' headers invalid."); return log
            for row in reader:
                try:
                    year, state, url = int(row['Year']), row['State'], row['URL']
                    if url: log[(year, state)] = url
                except (ValueError, KeyError, TypeError): pass
    except Exception as e: print(f"Error loading success log '{filename}': {e}")
    print(f"Loaded {len(log)} successful entries from {filename}")
    return log

def save_download_log(filename, log):
    print(f"    --> Updating success log file: {filename}")
    try:
        temp_filename = filename + ".tmp"
        with open(temp_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Year', 'State', 'URL']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            sorted_keys = sorted(log.keys(), key=lambda item: (-item[0], item[1]))
            for key in sorted_keys:
                if log[key]: writer.writerow({'Year': key[0], 'State': key[1], 'URL': log[key]})
        os.replace(temp_filename, filename)
    except Exception as e: print(f"    --> ERROR saving success log '{filename}': {e}")

def load_failed_urls_set(filename):
    failed_urls = set()
    if not os.path.exists(filename):
        print(f"Individual failure log file '{filename}' not found. Starting with empty set.")
        return failed_urls
    try:
        with open(filename, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            if 'FailedURL' not in reader.fieldnames:
                print(f"Warning: Failure log '{filename}' is missing 'FailedURL' header. Cannot load failed URLs.")
                return failed_urls
            for row in reader:
                if 'FailedURL' in row and row['FailedURL']:
                    failed_urls.add(row['FailedURL'].strip())
    except Exception as e:
        print(f"Error reading individual failure log {filename}: {e}. Starting with empty set.")
    print(f"Loaded {len(failed_urls)} previously failed URLs from {filename}")
    return failed_urls

def log_failed_url(filename, year, state, failed_url, failed_urls_set_in_memory):
    if not failed_url: return
    if failed_url not in failed_urls_set_in_memory:
        failed_urls_set_in_memory.add(failed_url)
        print(f"      Logging failed URL: {failed_url}")
        file_exists = os.path.exists(filename)
        try:
            with open(filename, mode='a', newline='', encoding='utf-8') as outfile:
                fieldnames = ['Year', 'State', 'FailedURL', 'Timestamp']
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                if not file_exists or os.path.getsize(filename) == 0:
                    writer.writeheader()
                writer.writerow({
                    'Year': year, 'State': state, 'FailedURL': failed_url,
                    'Timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                 })
        except Exception as e:
            print(f"      ERROR writing failed URL {failed_url} to {filename}: {e}")

def attempt_download(url, target_save_dir):
    """Attempts to download a single URL. Returns final URL on success, None on failure."""
    print(f"    Trying URL: {url}")
    print(f"      Pausing for {DELAY_BETWEEN_ATTEMPTS} seconds...")
    time.sleep(DELAY_BETWEEN_ATTEMPTS)

    success_url = None
    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=60, allow_redirects=True)
        final_url = response.url

        if response.status_code == 200:
            content_disposition = response.headers.get('content-disposition')
            filename = None
            if content_disposition:
                fnames = re.findall('filename="?([^"]+)"?', content_disposition); filename = sanitize_filename(fnames[0]) if fnames else None
            if not filename: fname_path = os.path.basename(unquote(urlparse(final_url).path)); filename = sanitize_filename(fname_path) if fname_path else None
            if not filename: ext_match = re.search(r'\.(docx|pdf)$', final_url, re.IGNORECASE); ext = ext_match.group(1) if ext_match else 'download'; filename = f"download_{int(time.time())}.{ext}"
            filepath = os.path.join(target_save_dir, filename)

            if os.path.exists(filepath):
                print(f"      Skipping download, file already exists: {filepath}")
                success_url = final_url
            else:
                print(f"      Success! Status 200. Downloading to: {filepath}")
                try:
                    os.makedirs(target_save_dir, exist_ok=True)
                    with open(filepath, 'wb') as f:
                        for chunk in response.iter_content(8192): f.write(chunk)
                    print(f"      Download complete: {filepath}")
                    success_url = final_url
                    time.sleep(DELAY_AFTER_SUCCESS)
                except Exception as write_e:
                    print(f"      ERROR writing file to {filepath}: {write_e}"); success_url = None
        elif response.status_code == 404:
            print(f"      Not Found (404)."); success_url = None
        else:
            print(f"      Failed! Status: {response.status_code}"); success_url = None
    except requests.exceptions.Timeout:
        print(f"      Request Timed Out."); success_url = None
    except requests.exceptions.RequestException as e:
        print(f"      Request/Network Error: {e}"); success_url = None
    except Exception as e:
        print(f"      Unexpected Error during download attempt: {e}"); success_url = None
    return success_url

# *** NEW: Function to parse targets for --update mode ***
def parse_update_targets(update_args):
    """Parses arguments for --update mode into sets of states, years, and (state, year) pairs."""
    target_states = set()
    target_years = set()
    target_state_years = set()
    update_all_combinations = False # Flag to indicate if no specific filters were given

    if not update_args: # --update with no args means update all found in the log
        update_all_combinations = True
        print("Update target: All entries in the success log.")
        return target_states, target_years, target_state_years, update_all_combinations

    print("Parsing update targets...")
    for arg in update_args:
        arg = arg.strip()
        if ':' in arg:
            parts = arg.split(':', 1)
            state_name = parts[0].strip()
            year_str = parts[1].strip()
            try:
                year = int(year_str)
                target_state_years.add((state_name, year))
                print(f"  - Added State/Year target: {state_name}:{year}")
            except ValueError:
                print(f"  Warning: Could not parse year in update target '{arg}'. Ignoring.")
        else:
            try:
                year = int(arg)
                target_years.add(year)
                print(f"  - Added Year target: {year}")
            except ValueError:
                # Assume it's a state name if not an integer
                target_states.add(arg)
                print(f"  - Added State target: {arg}")

    if not target_states and not target_years and not target_state_years:
        print("Warning: No valid update targets parsed. Will check all entries by default.")
        update_all_combinations = True

    return target_states, target_years, target_state_years, update_all_combinations

# --- END Helper Functions ---

# --- Main Script Logic ---
if __name__ == "__main__":
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Download or update WIOA state plans.")
    # Use a mutually exclusive group to ensure only one mode runs
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--try-again', '-ta', nargs='+', metavar='STATE_NAME',
                            help='Find/Download mode: Process ONLY these states, retrying ALL URL variants, ignoring failure log.')
    mode_group.add_argument('--update', '-u', nargs='*', metavar='TARGET', # nargs='*' allows --update with no args
                            help='Update mode: Re-check/download ONLY from URLs in the success log. Optional targets: "State", YYYY, "State:YYYY". No targets = update all.')

    args = parser.parse_args()
    # --- End Argument Parsing ---

    print(f"Starting WIOA plan script...")
    print(f"Success Log: {CSV_FILENAME}")
    print(f"Individual Failure Log: {FAILED_URLS_CSV}")

    os.makedirs(BASE_DOWNLOAD_DIR, exist_ok=True)
    download_log = load_download_log(CSV_FILENAME) # Load success log early for all modes
    initial_success_size = len(download_log)

    # =========================
    # === UPDATE MODE LOGIC ===
    # =========================
    if args.update is not None: # Check if --update was used (even with no arguments)
        print("\n--- Running in UPDATE mode ---")
        target_states, target_years, target_state_years, update_all = parse_update_targets(args.update)

        if not download_log:
            print("Success log is empty. Nothing to update. Exiting.")
            exit()

        print(f"\nChecking {len(download_log)} entries from success log ('{CSV_FILENAME}')...")
        update_processed_count = 0
        update_skipped_count = 0
        update_attempted_count = 0
        update_success_count = 0 # Counts successful checks/downloads in this run
        update_failed_check_count = 0 # Counts if logged URL fails now

        # Sort items for consistent processing order
        sorted_log_items = sorted(download_log.items(), key=lambda item: (-item[0][0], item[0][1])) # Year DESC, State ASC

        for (year, state), success_url in sorted_log_items:
            update_processed_count += 1
            process_this_item = False

            # Determine if this item matches the update filters
            if update_all:
                process_this_item = True
            else:
                if state in target_states: process_this_item = True
                elif year in target_years: process_this_item = True
                elif (state, year) in target_state_years: process_this_item = True

            if not process_this_item:
                # print(f"  Skipping {state} - {year} (doesn't match update target criteria)") # Optional: verbose skip log
                update_skipped_count += 1
                continue

            # --- Process the targeted item ---
            print(f"\nProcessing update target: {state} - {year}")
            if not success_url:
                 print("  Skipping: No URL recorded in success log for this entry.")
                 update_skipped_count += 1
                 continue

            update_attempted_count += 1
            year_dir = os.path.join(BASE_DOWNLOAD_DIR, str(year))
            # attempt_download handles directory creation if needed inside
            result_url = attempt_download(success_url, year_dir) # Use the logged URL

            if result_url:
                # The check/download was successful (or file already existed)
                update_success_count += 1
            else:
                # The previously successful URL failed this time
                print(f"  WARNING: Attempt to update/verify failed for logged URL: {success_url}")
                update_failed_check_count += 1
                # NOTE: We do NOT modify the success log here. Update mode is read-only regarding logs.

        # --- Update Mode Summary ---
        print(f"\n--- UPDATE Mode Finished ---")
        print(f"Checked {update_processed_count} entries from the success log.")
        if not update_all:
             print(f"Skipped {update_skipped_count} entries that did not match update targets.")
        print(f"Attempted to update/verify {update_attempted_count} targeted entries.")
        print(f"Successfully updated/verified (or file existed): {update_success_count}")
        if update_failed_check_count > 0:
             print(f"WARNING: {update_failed_check_count} previously successful URLs failed during this check.")
        print(f"Total entries currently in success log '{CSV_FILENAME}': {len(download_log)}")


    # ===========================================
    # === FIND/DOWNLOAD MODE LOGIC (Default or --try-again) ===
    # ===========================================
    else: # Run standard download or --try-again mode
        retry_states = set(args.try_again) if args.try_again else set()
        mode_string = f"TRY-AGAIN mode for states: {', '.join(sorted(list(retry_states)))}" if retry_states else "STANDARD mode"
        print(f"\n--- Running in {mode_string} ---")
        if retry_states:
            print(f"--- ONLY specified states will be processed. ALL URL variants will be attempted, ignoring failure log. ---")
        else:
            print(f"--- Will skip successes and individually logged failed URLs. ---")

        # Load failure log only if needed (not in --update mode)
        failed_urls_set = load_failed_urls_set(FAILED_URLS_CSV)
        initial_failure_size = len(failed_urls_set)

        # Fetch state list (only needed in this mode)
        print("\nFetching state list...")
        all_states_fetched = []
        try:
            session = requests.Session(); session.headers.update(HEADERS)
            response = session.get(BASE_URL, timeout=30); response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            state_select = soup.find('select', {'name': 'states'})
            if state_select:
                for option in state_select.find_all('option'):
                    val, name = option.get('value'), option.text.strip()
                    if val and name != "State or Territory": all_states_fetched.append({'id': val, 'name': name, 'filename_name': name.replace(' ', '_')})
                print(f"OK: Extracted {len(all_states_fetched)} states.")
            else: raise ValueError("State dropdown not found.")
        except Exception as e: print(f"FATAL: State list error: {e}"); exit(1)

        # Filter states if --try-again is used
        states_to_process = []
        if retry_states:
            print(f"\nFiltering state list based on --try-again argument...")
            states_to_process = [s for s in all_states_fetched if s['name'] in retry_states]
            found_set = {s['name'] for s in states_to_process}
            missing_set = retry_states - found_set
            if missing_set: print(f"Warning: The following states from --try-again were not found in website list: {', '.join(sorted(list(missing_set)))}")
            if not states_to_process: print("Error: No states to process after filtering. Exiting."); exit(0)
            print(f"Processing only {len(states_to_process)} state(s).")
        else:
            states_to_process = all_states_fetched

        print("\nStarting download attempts...")
        processed_combinations = 0
        attempted_urls_this_run = 0
        skipped_urls_due_to_failure_log = 0
        skipped_combinations_due_to_success_log = 0
        success_count_this_run = 0
        new_failures_logged_this_run = 0
        states_to_process.sort(key=lambda x: x['name'])

        for state_info in states_to_process:
            state_name, state_fname_part = state_info['name'], state_info['filename_name']
            print(f"\nProcessing State: {state_name}")

            for plan_year in sorted(TARGET_PLAN_YEARS, reverse=True):
                log_key = (plan_year, state_name)
                processed_combinations += 1
                found_url_for_combo = None

                # --- Check Success Log FIRST ---
                if log_key in download_log:
                    print(f"  Skipping {plan_year}: Already in success log -> {download_log[log_key]}")
                    skipped_combinations_due_to_success_log += 1
                    continue

                print(f"  Attempting check for Plan Year: {plan_year}")
                year_dir = os.path.join(BASE_DOWNLOAD_DIR, str(plan_year))
                try: os.makedirs(year_dir, exist_ok=True)
                except OSError as e: print(f"  ERROR creating directory '{year_dir}': {e}"); continue

                url_safe_state = quote(state_name)
                is_retry_target = state_name in retry_states

                # --- Year-Specific URL Generation Logic ---
                urls_to_try = []
                if plan_year in [2016, 2018]:
                    fname_url = f"{url_safe_state}.pdf"; url_path = f"{FILE_BASE_PATH_PLANS}{plan_year}/{fname_url}"; urls_to_try.append(urljoin(BASE_URL, url_path))
                elif plan_year >= 2020:
                    exts = [".docx"] if plan_year == 2024 else [".pdf"]; yr_range = PLAN_YEAR_RANGES.get(plan_year, f"{plan_year}-{plan_year+3}"); base_stem = f"{state_fname_part}_PYs_{yr_range}"; stems = [f"{base_stem}_(mod)"] if plan_year == 2022 else [base_stem]; suffixes = ["", "_0", "_1", "_2"]; folders = FOLDER_YEAR_CANDIDATES.get(plan_year, [str(plan_year)])
                    for folder in folders:
                        for stem in stems:
                            for suffix in suffixes:
                                for ext in exts:
                                    fname = f"{stem}{suffix}{ext}"; url_path_normal = f"{FILE_BASE_PATH_PDFS}{folder}/{fname}"; urls_to_try.append(urljoin(BASE_URL, url_path_normal)); url_path_double = f"{FILE_BASE_PATH_PDFS}{folder}//{fname}"; urls_to_try.append(urljoin(BASE_URL, url_path_double))

                # --- Try each generated URL ---
                for full_url in urls_to_try:
                    if found_url_for_combo: break
                    attempted_urls_this_run += 1
                    skip_this_url = False
                    if not is_retry_target and full_url in failed_urls_set:
                        skipped_urls_due_to_failure_log += 1; skip_this_url = True
                    if skip_this_url: continue

                    current_try_result = attempt_download(full_url, year_dir)
                    if current_try_result:
                        found_url_for_combo = current_try_result; print(f"    --> SUCCESS: File found/exists for {state_name} - {plan_year} via {found_url_for_combo}"); success_count_this_run += 1
                        if log_key not in download_log or download_log[log_key] != found_url_for_combo: download_log[log_key] = found_url_for_combo; save_download_log(CSV_FILENAME, download_log)
                        break
                    else:
                        initial_fail_set_size = len(failed_urls_set); log_failed_url(FAILED_URLS_CSV, plan_year, state_name, full_url, failed_urls_set)
                        if len(failed_urls_set) > initial_fail_set_size: new_failures_logged_this_run += 1

                if not found_url_for_combo:
                    print(f"    --> FAILED: Could not find file for {state_name} - {plan_year} after trying all variants.")

        # --- Find/Download Mode Summary ---
        print(f"\n--- {mode_string} Finished ---")
        if retry_states: print(f"Processed {processed_combinations} combinations for {len(states_to_process)} targeted state(s).")
        else: total_possible_combinations = len(all_states_fetched) * len(TARGET_PLAN_YEARS); print(f"Processed {processed_combinations} out of {total_possible_combinations} total state/year combinations.")
        print(f"Skipped {skipped_combinations_due_to_success_log} combinations already in the success log.")
        print(f"Attempted {attempted_urls_this_run} specific URL downloads this run.")
        print(f"Skipped {skipped_urls_due_to_failure_log} specific URL attempts based on failure log ('{FAILED_URLS_CSV}').")
        print(f"Found/Downloaded/Confirmed {success_count_this_run} files this run.")
        print(f"Logged {new_failures_logged_this_run} new individual URL failures this run.")
        final_dl_log = load_download_log(CSV_FILENAME)
        final_fail_set = load_failed_urls_set(FAILED_URLS_CSV)
        print(f"Total entries in success log '{CSV_FILENAME}': {len(final_dl_log)} (Initial: {initial_success_size})")
        print(f"Total URLs in individual failure log '{FAILED_URLS_CSV}': {len(final_fail_set)} (Initial: {initial_failure_size})")
