import requests
import os
import time
import re
import csv
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote

# --- Configuration ---
BASE_URL = "https://wioaplans.ed.gov"
BASE_DOWNLOAD_DIR = "wioa_plans"
# Ensure base paths end with a forward slash
FILE_BASE_PATH_PDFS = "/sites/default/files/pdfs/state-plan/" # For 2020+
FILE_BASE_PATH_PLANS = "/sites/default/files/state_plans/"    # For 2016, 2018
CSV_FILENAME = "wioa_download_log.csv" # Log file

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36', # Consider updating Chrome version periodically
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1', # Do Not Track
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Referer': BASE_URL
}

DELAY_BETWEEN_ATTEMPTS = 15 # Seconds - Be respectful to the server
DELAY_AFTER_SUCCESS = 1.5 # Optional shorter delay after a successful download

# --- Targets only these specific years ---
TARGET_PLAN_YEARS = [2024, 2022, 2020, 2018, 2016]

# Define year ranges used in filenames for >= 2020 plans
PLAN_YEAR_RANGES = {
    2024: "2024-2027", 2022: "2022-2023", 2020: "2020-2023",
}

# Define potential parent folder years on the server for >= 2020 plans
# Note: User provided [2024], [2022], [2020] - keeping this setting
FOLDER_YEAR_CANDIDATES = {
    2024: [2024], 2022: [2022], 2020: [2020],
}

# --- Helper Functions ---

def sanitize_filename(filename):
    """Removes or replaces characters invalid for filenames, decodes URL encoding."""
    try:
        decoded_filename = unquote(filename)
    except Exception:
        decoded_filename = filename
    sanitized = re.sub(r'[\\/*?:"<>|]', "", decoded_filename)
    sanitized = sanitized.replace(" ", "_")
    max_len = 200 # Conservative limit
    if len(sanitized) > max_len:
        name, ext = os.path.splitext(sanitized)
        sanitized = name[:max_len - len(ext)] + ext
    return sanitized

def load_download_log(filename):
    """Loads previous download results from CSV into a dictionary { (year, state): url }."""
    log = {}
    if not os.path.exists(filename):
        print(f"Log file '{filename}' not found. Starting fresh.")
        return log
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            if not all(col in reader.fieldnames for col in ['Year', 'State', 'URL']):
                 print(f"Warning: CSV file '{filename}' has incorrect headers (Expected 'Year', 'State', 'URL'). Ignoring.")
                 return log
            for row_num, row in enumerate(reader, 1):
                try:
                    year_str = row.get('Year', '').strip()
                    state = row.get('State', '').strip()
                    url = row.get('URL', '').strip()
                    if year_str and state:
                        year = int(year_str)
                        log[(year, state)] = url
                    else:
                         print(f"Warning: Skipping row {row_num} with missing Year or State in CSV: {row}")
                except (ValueError, KeyError, TypeError) as e:
                    print(f"Warning: Skipping invalid row {row_num} in CSV: {row} - {e}")
    except Exception as e:
        print(f"Error loading CSV log '{filename}': {e}")
    print(f"Loaded {len(log)} entries from {filename}")
    return log

# MODIFICATION: Adjusted print statement for clarity when saving incrementally
def save_download_log(filename, log):
    """Saves the download log dictionary to a CSV file."""
    # Reduced verbosity for incremental saves
    print(f"    --> Updating log file: {filename}")
    try:
        # Use a temporary file for atomicity (safer during interruptions)
        temp_filename = filename + ".tmp"
        with open(temp_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Year', 'State', 'URL']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            sorted_keys = sorted(log.keys(), key=lambda item: (item[0], item[1]))
            for key in sorted_keys:
                year, state = key
                url = log[key]
                writer.writerow({'Year': year, 'State': state, 'URL': url})
        # If write successful, replace original file with temp file
        os.replace(temp_filename, filename)
        # print("    --> Log updated successfully.") # Optional: Can uncomment if needed
    except Exception as e:
        print(f"    --> ERROR saving CSV log '{filename}': {e}")
        # Clean up temp file if error occurred
        if os.path.exists(temp_filename):
            try:
                os.remove(temp_filename)
            except OSError:
                pass # Ignore cleanup error

def attempt_download(url, target_save_dir):
    """Tries to download a file into target_save_dir. Returns the final URL on success, None on failure."""
    print(f"    Trying URL: {url}")
    # Reduced pause time based on user providing script with 15s, assuming this is acceptable.
    # Consider adjusting if server issues arise.
    print(f"    Pausing for {DELAY_BETWEEN_ATTEMPTS} seconds...")
    time.sleep(DELAY_BETWEEN_ATTEMPTS)
    success_url = None
    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=60, allow_redirects=True)
        final_url = response.url

        if response.status_code == 200:
            content_disposition = response.headers.get('content-disposition')
            filename = None
            if content_disposition:
                filenames = re.findall('filename="?([^"]+)"?', content_disposition)
                if filenames:
                    filename = sanitize_filename(filenames[0])

            if not filename:
                parsed_url = urlparse(final_url)
                filename_from_path = os.path.basename(parsed_url.path)
                if filename_from_path:
                    filename = sanitize_filename(filename_from_path)

            if not filename:
                filename = f"downloaded_file_{int(time.time())}"
                print(f"      Warning: Could not determine filename reliably, using fallback: {filename}")

            filepath = os.path.join(target_save_dir, filename)

            if os.path.exists(filepath):
                print(f"      Skipping download, file already exists: {filepath}")
                success_url = final_url
            else:
                print(f"      Success! Status {response.status_code}. Downloading to: {filepath}")
                try:
                    os.makedirs(target_save_dir, exist_ok=True)
                    with open(filepath, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    print(f"      Download complete: {filepath}")
                    success_url = final_url
                    # time.sleep(DELAY_AFTER_SUCCESS) # Optional delay after successful write
                except Exception as write_e:
                    print(f"      ERROR: Failed to write file '{filepath}': {write_e}")
                    success_url = None
                    # Clean up partial file on write error
                    if os.path.exists(filepath):
                        try:
                            os.remove(filepath)
                        except OSError:
                            pass

        elif response.status_code == 404:
            print(f"      Not Found (404).")
        else:
            print(f"      Failed! Status: {response.status_code}")

    except requests.exceptions.Timeout:
        print(f"      Network Error: Timeout occurred for {url}")
    except requests.exceptions.RequestException as e:
        print(f"      Network/Request Error for {url}: {e}")
    except Exception as e:
        print(f"      An unexpected error occurred processing {url}: {e}")

    return success_url


# --- Main Script Logic ---
if __name__ == "__main__":
    print(f"Starting targeted download script for {BASE_URL} with CSV logging.")
    print(f"Target plan years: {TARGET_PLAN_YEARS}")
    print(f"Files will be saved in: '{BASE_DOWNLOAD_DIR}/<YEAR>/' structure")
    print(f"Log file: '{CSV_FILENAME}'")
    print(f"*** Log will be updated after each successful download or confirmed failure ***") # Added note
    print(f"*** Delay between attempts set to: {DELAY_BETWEEN_ATTEMPTS} seconds ***")
    print(f"*** Using Chrome-like headers ***")
    print(f"*** Applying year-specific extensions (.docx for 2024, .pdf prior) ***")
    print(f"*** Applying specific 2022 filename logic (using '_(mod)') ***")

    os.makedirs(BASE_DOWNLOAD_DIR, exist_ok=True)

    # 1. Load previous download log
    download_log = load_download_log(CSV_FILENAME)
    initial_log_size = len(download_log)

    # 2. Fetch main page to get state list
    print(f"\nFetching main page to get state list...")
    states = []
    try:
        session = requests.Session()
        session.headers.update(HEADERS)
        response = session.get(BASE_URL, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        state_select = soup.find('select', {'name': 'states'})
        if state_select:
            for option in state_select.find_all('option'):
                value = option.get('value')
                name = option.text.strip()
                if value and name and name != "State or Territory":
                    formatted_name = name.replace(' ', '_')
                    states.append({'id': value, 'name': name, 'filename_name': formatted_name})
            print(f"Successfully extracted {len(states)} states/territories.")
        else:
            raise ValueError("Could not find state selection dropdown on the page.")

    except requests.exceptions.RequestException as e:
         print(f"Fatal Error: Network error fetching state list from {BASE_URL}: {e}")
         exit(1)
    except Exception as e:
        print(f"Fatal Error: Could not fetch or parse state list from {BASE_URL}: {e}")
        exit(1)

    # 3. Iterate and attempt downloads, checking log first
    print("\nStarting download attempts (checking log first)...")
    total_processed = 0
    total_downloaded_this_run = 0
    total_failed_this_run = 0
    total_skipped_from_log = 0

    states.sort(key=lambda x: x['name'])

    for state_info in states:
        state_name = state_info['name']
        state_filename_part = state_info['filename_name']
        print(f"\nProcessing State: {state_name} (Filename part: {state_filename_part})")

        for plan_year in sorted(TARGET_PLAN_YEARS, reverse=True):
            log_key = (plan_year, state_name)
            total_processed += 1

            log_entry = download_log.get(log_key)
            if log_entry is not None:
                if log_entry:
                    print(f"  Skipping {plan_year}: Already successfully downloaded (in log) -> {log_entry}")
                else:
                    print(f"  Skipping {plan_year}: Previously failed (marked in log).")
                total_skipped_from_log += 1
                continue

            print(f"  Checking Plan Year: {plan_year} (Not in log or needs update)")

            year_specific_dir = os.path.join(BASE_DOWNLOAD_DIR, str(plan_year))
            try:
                os.makedirs(year_specific_dir, exist_ok=True)
            except OSError as e:
                print(f"  ERROR: Could not create directory '{year_specific_dir}': {e}")
                # Log failure due to dir error and save log immediately
                download_log[log_key] = ""
                total_failed_this_run += 1
                save_download_log(CSV_FILENAME, download_log) # Save log on directory error failure
                continue

            found_url = None

            # === Year-Specific Logic ===
            if plan_year in [2016, 2018]:
                filename = f"{state_filename_part}.pdf"
                url_path = f"{FILE_BASE_PATH_PLANS}{plan_year}/{filename}"
                full_url = urljoin(BASE_URL, url_path)
                found_url = attempt_download(full_url, year_specific_dir)

            elif plan_year >= 2020:
                if plan_year == 2024: extensions_to_try = [".docx"]
                else: extensions_to_try = [".pdf"]

                year_range = PLAN_YEAR_RANGES.get(plan_year, f"{plan_year}-{plan_year+3}")
                base_stem_part = f"{state_filename_part}_PYs_{year_range}"

                if plan_year == 2022:
                    stems_to_try = [f"{base_stem_part}_(Mod)"]
                    print(f"    -> Applying 2022 rule: Trying only stems with '_(Mod)'")
                else:
                    stems_to_try = [base_stem_part]
                    print(f"    -> Applying standard rule for {plan_year}: Trying stems without '_(Mod)'")

                suffixes_to_try = ["", "_0", "_1", "_2"]
                folder_years_to_try = FOLDER_YEAR_CANDIDATES.get(plan_year, [str(plan_year)])

                for folder_year in folder_years_to_try:
                    if found_url: break
                    for stem in stems_to_try:
                        if found_url: break
                        for suffix in suffixes_to_try:
                            if found_url: break
                            for ext in extensions_to_try:
                                if found_url: break
                                filename = f"{stem}{suffix}{ext}"
                                url_path = f"{FILE_BASE_PATH_PDFS}{folder_year}/{filename}"
                                full_url = urljoin(BASE_URL, url_path)
                                current_try_url = attempt_download(full_url, year_specific_dir)
                                if current_try_url:
                                    found_url = current_try_url

            # === Update Log based on outcome AND SAVE IMMEDIATELY ===
            if found_url:
                print(f"    --> SUCCESS: Found file for {state_name} - {plan_year}")
                download_log[log_key] = found_url
                total_downloaded_this_run += 1
                # ---- START MODIFICATION: Save log after success ----
                save_download_log(CSV_FILENAME, download_log)
                # ---- END MODIFICATION ----
            else:
                print(f"    --> FAILED: Could not find file for {state_name} - {plan_year} after trying all combinations.")
                download_log[log_key] = "" # Mark as failed in memory
                total_failed_this_run += 1
                # ---- START MODIFICATION: Save log after confirmed failure ----
                save_download_log(CSV_FILENAME, download_log)
                # ---- END MODIFICATION ----

    # 4. Save the updated log (REMOVED - Saving is now done incrementally)
    # save_download_log(CSV_FILENAME, download_log) # <-- REMOVED THIS LINE

    # 5. Final Summary
    print(f"\n--- Script Finished ---")
    total_combinations = len(states) * len(TARGET_PLAN_YEARS)
    print(f"Attempted to process {total_combinations} state/year combinations.")
    print(f"Skipped {total_skipped_from_log} combinations found in the previous log.")
    print(f"Found/Downloaded {total_downloaded_this_run} new files this run.")
    print(f"Confirmed {total_failed_this_run} failures this run (marked in log).")
    # Recalculate final log size by reloading (most accurate if save failed silently)
    final_log_check = load_download_log(CSV_FILENAME)
    final_log_size = len(final_log_check)
    new_entries = final_log_size - initial_log_size # Note: This might be slightly off if save failed multiple times
    print(f"Total entries in log file '{CSV_FILENAME}': {final_log_size} (Initial: {initial_log_size})")
    print(f"Check the '{BASE_DOWNLOAD_DIR}' directory (with year subfolders) and the log file.")