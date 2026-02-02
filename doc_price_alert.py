import os
import json
import time
import random
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials
import requests
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Supplier columns (in order as they appear in the sheet)
SUPPLIERS = [
    "AGRITED", "AGRICARE", "CHI", "OROBOR", "FARM SUPPORT", "ZARTECH",
    "FIDAN", "EMINENT", "SAYED", "AMO", "GS", "SUPREME", "BOUNTY HARVEST",
    "CASCADA", "DADDY'S FARMS", "AJILA", "DAMAS", "YAMMFY", "VALENTINE",
    "TUNS", "CALIBER", "CHIKUN", "VERTEX"
]

# State file path
STATE_FILE = ".github/state/alert_state.json"

# Timezone for Nigeria
NIGERIA_TZ = pytz.timezone('Africa/Lagos')

# Predicted monthly prices (expected DOC cost per month)
PREDICTED_MONTHLY_PRICES = {
    1: 1569,   # JAN
    2: 1713,   # FEB
    3: 850,    # MAR
    4: 1177,   # APR
    5: 727,    # MAY
    6: 802,    # JUN
    7: 938,    # JUL
    8: 1414,   # AUG
    9: 1897,   # SEP
    10: 2212,  # OCT
    11: 1926,  # NOV
    12: 843,   # DEC
}


def get_env_variable(name: str) -> str:
    """Get environment variable or raise error if not set."""
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Environment variable {name} is not set")
    return value


def authenticate_google_sheets() -> gspread.Client:
    """Authenticate with Google Sheets using service account."""
    service_account_json = get_env_variable("GOOGLE_SERVICE_ACCOUNT_KEY")

    try:
        service_account_info = json.loads(service_account_json)
    except json.JSONDecodeError:
        # Try base64 decoding if direct JSON fails
        import base64
        service_account_info = json.loads(base64.b64decode(service_account_json))

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets.readonly'
    ]

    credentials = Credentials.from_service_account_info(
        service_account_info,
        scopes=scopes
    )

    return gspread.authorize(credentials)


def get_sheet_data_with_retry(client: gspread.Client, max_retries: int = 5) -> list:
    """Fetch sheet data with retry logic for rate limiting."""
    sheet_id = get_env_variable("GOOGLE_SHEET_ID")
    sheet_name = get_env_variable("SHEET_NAME")

    for attempt in range(max_retries):
        try:
            spreadsheet = client.open_by_key(sheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
            data = worksheet.get_all_values()
            logger.info(f"Successfully fetched {len(data)} rows from sheet")
            return data
        except gspread.exceptions.APIError as e:
            error_code = e.response.status_code if hasattr(e, 'response') else None

            if error_code in (429, 503) or (error_code == 403 and 'rateLimitExceeded' in str(e)):
                delay = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"Rate limited. Retrying in {delay:.2f}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(min(delay, 60))
                continue
            raise
        except Exception as e:
            if attempt < max_retries - 1:
                delay = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"Error fetching data: {e}. Retrying in {delay:.2f}s")
                time.sleep(delay)
                continue
            raise

    raise Exception("Max retries exceeded when fetching sheet data")


def compute_row_hash(row: list) -> str:
    """Compute a hash of the row data to detect changes."""
    # Use first 24 columns (Date + 23 suppliers)
    row_data = '|'.join(str(cell).strip() for cell in row[:24])
    return hashlib.md5(row_data.encode()).hexdigest()


def load_state() -> dict:
    """Load state from file or return default state."""
    default_state = {
        "last_processed_row": 0,
        "last_row_hash": None,
        "last_monthly_alert": None
    }

    if not os.path.exists(STATE_FILE):
        logger.info("No state file found, using default state")
        return default_state

    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            logger.info(f"Loaded state: {state}")
            return state
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Error reading state file: {e}. Using default state")
        return default_state


def save_state(state: dict) -> None:
    """Save state to file."""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)
    logger.info(f"Saved state: {state}")


def is_row_complete(row: list) -> bool:
    """Check if a row has all required values (Date + 23 suppliers)."""
    if len(row) < 24:  # Date + 23 suppliers (VERTEX is 24th column)
        return False

    # Check first 24 columns (Date + 23 suppliers)
    for i in range(24):
        value = row[i].strip() if i < len(row) else ""
        if not value:
            return False

    return True


def parse_price(value: str) -> Optional[float]:
    """Parse price value, handling commas and other formatting."""
    try:
        # Remove commas and whitespace
        cleaned = value.replace(',', '').replace(' ', '').strip()
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def format_price(value: str) -> str:
    """Format price with commas where necessary (e.g., 1650 -> 1,650)."""
    try:
        # Parse the price
        cleaned = value.replace(',', '').replace(' ', '').strip()
        num = float(cleaned)
        # Format with commas, no decimal places
        return f"{num:,.0f}"
    except (ValueError, AttributeError):
        return value  # Return original if parsing fails


def get_latest_complete_row(data: list) -> tuple:
    """Find the latest row with all columns filled. Returns (row_index, row_data)."""
    if len(data) <= 1:  # Only header or empty
        return None, None

    # Start from the end and find the last complete row
    for i in range(len(data) - 1, 0, -1):  # Skip header row (index 0)
        if is_row_complete(data[i]):
            return i, data[i]

    return None, None


def get_latest_row_with_date(data: list) -> tuple:
    """Find the latest row that has a date (may be incomplete). Returns (row_index, row_data)."""
    if len(data) <= 1:  # Only header or empty
        return None, None

    # Start from the end and find the last row with a date
    for i in range(len(data) - 1, 0, -1):  # Skip header row (index 0)
        row = data[i]
        if row and len(row) > 0 and row[0].strip():
            return i, row

    return None, None


def is_row_incomplete_but_started(row: list) -> bool:
    """Check if a row has a date but not all prices filled."""
    if not row or len(row) == 0:
        return False

    # Has date?
    if not row[0].strip():
        return False

    # Check if incomplete (missing some values in first 24 columns)
    return not is_row_complete(row)


def calculate_daily_average(row: list) -> Optional[float]:
    """Calculate average price for a single day."""
    prices = []

    # Skip first column (Date), get prices for all suppliers
    for i in range(1, min(len(row), 24)):
        price = parse_price(row[i])
        if price is not None:
            prices.append(price)

    if not prices:
        return None

    return sum(prices) / len(prices)


def get_month_data(data: list, year: int, month: int) -> list:
    """Get all rows for a specific month."""
    month_rows = []

    for i, row in enumerate(data[1:], start=1):  # Skip header
        if not row or not row[0]:
            continue

        try:
            # Try parsing date in various formats
            date_str = row[0].strip()
            row_date = None

            for fmt in ['%d-%b-%Y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']:
                try:
                    row_date = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    continue

            if row_date and row_date.year == year and row_date.month == month:
                if is_row_complete(row):
                    month_rows.append(row)
        except Exception:
            continue

    return month_rows


def calculate_monthly_averages(month_data: list) -> dict:
    """Calculate average prices per supplier for the month."""
    if not month_data:
        return {}

    supplier_totals = {supplier: [] for supplier in SUPPLIERS}

    for row in month_data:
        for i, supplier in enumerate(SUPPLIERS):
            col_index = i + 1  # +1 because first column is Date
            if col_index < len(row):
                price = parse_price(row[col_index])
                if price is not None:
                    supplier_totals[supplier].append(price)

    averages = {}
    for supplier, prices in supplier_totals.items():
        if prices:
            averages[supplier] = sum(prices) / len(prices)

    return averages


def get_wat_timestamp() -> str:
    """Get current timestamp in WAT AM/PM format (time only)."""
    now = datetime.now(NIGERIA_TZ)
    return now.strftime("%I:%M %p WAT")


def format_daily_card(date_str: str, row: list, daily_avg: float, is_update: bool = False) -> dict:
    """Format daily price alert as Google Chat card."""
    # Build supplier price widgets
    supplier_widgets = []

    for i, supplier in enumerate(SUPPLIERS):
        col_index = i + 1
        price_raw = row[col_index] if col_index < len(row) else "N/A"
        price = format_price(price_raw) if price_raw != "N/A" else "N/A"

        supplier_widgets.append({
            "decoratedText": {
                "topLabel": supplier,
                "text": price
            }
        })

    # Get current WAT timestamp
    wat_timestamp = get_wat_timestamp()

    # Different title for new entry vs update
    title = "DOC Price Update" if is_update else "DOC Price Alert"

    card = {
        "cardsV2": [{
            "cardId": "daily-price-alert",
            "card": {
                "header": {
                    "title": title,
                    "subtitle": f"{date_str} | {wat_timestamp}"
                },
                "sections": [
                    {
                        "header": "Supplier Prices",
                        "collapsible": True,
                        "uncollapsibleWidgetsCount": 4,
                        "widgets": supplier_widgets
                    },
                    {
                        "header": "Summary",
                        "widgets": [
                            {
                                "decoratedText": {
                                    "topLabel": "Daily Average",
                                    "text": f"{daily_avg:,.0f}"
                                }
                            }
                        ]
                    }
                ]
            }
        }]
    }

    return card


def format_monthly_card(month_name: str, year: int, month_num: int, averages: dict, days_count: int) -> dict:
    """Format monthly summary alert as Google Chat card."""
    # Build supplier average widgets
    supplier_widgets = []

    for supplier in SUPPLIERS:
        avg = averages.get(supplier)
        avg_text = f"{avg:,.0f}" if avg else "N/A"

        supplier_widgets.append({
            "decoratedText": {
                "topLabel": supplier,
                "text": avg_text
            }
        })

    # Calculate overall monthly average
    valid_avgs = [v for v in averages.values() if v is not None]
    overall_avg = sum(valid_avgs) / len(valid_avgs) if valid_avgs else 0

    # Get predicted price for this month
    predicted_price = PREDICTED_MONTHLY_PRICES.get(month_num, 0)

    # Calculate difference (actual - predicted)
    difference = overall_avg - predicted_price
    diff_sign = "+" if difference >= 0 else ""
    diff_text = f"{diff_sign}{difference:,.0f}"

    # Visual styling for difference (green if favorable, red if not)
    is_favorable = difference < 0  # Lower than predicted = good
    diff_color = "#2ca02c" if is_favorable else "#e74c3c"
    diff_icon = "STAR" if is_favorable else "BOOKMARK"

    # Get current WAT timestamp
    wat_timestamp = get_wat_timestamp()

    card = {
        "cardsV2": [{
            "cardId": "monthly-summary",
            "card": {
                "header": {
                    "title": "Monthly DOC Price Summary",
                    "subtitle": f"{month_name} {year} | {wat_timestamp}"
                },
                "sections": [
                    {
                        "header": "Average Prices by Supplier",
                        "collapsible": True,
                        "uncollapsibleWidgetsCount": 4,
                        "widgets": supplier_widgets
                    },
                    {
                        "header": "Summary",
                        "widgets": [
                            {
                                "decoratedText": {
                                    "topLabel": "Actual Monthly Average",
                                    "text": f"{overall_avg:,.0f}"
                                }
                            },
                            {
                                "decoratedText": {
                                    "topLabel": "Predicted Monthly Average",
                                    "text": f"{predicted_price:,.0f}"
                                }
                            },
                            {
                                "decoratedText": {
                                    "startIcon": {
                                        "knownIcon": diff_icon
                                    },
                                    "topLabel": "Difference (Actual - Predicted)",
                                    "text": f"<font color=\"{diff_color}\">{diff_text}</font>"
                                }
                            },
                            {
                                "decoratedText": {
                                    "topLabel": "Days Recorded",
                                    "text": str(days_count)
                                }
                            }
                        ]
                    }
                ]
            }
        }]
    }

    return card


def send_webhook_with_retry(payload: dict, max_retries: int = 5) -> bool:
    """Send webhook with retry logic for rate limiting."""
    webhook_url = get_env_variable("SPACE_WEBHOOK_URL")

    for attempt in range(max_retries):
        try:
            response = requests.post(
                webhook_url,
                json=payload,
                timeout=30,
                headers={"Content-Type": "application/json"}
            )

            if response.status_code == 200:
                logger.info("Webhook sent successfully")
                return True

            if response.status_code in (429, 503):
                # Rate limited
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        delay = int(retry_after)
                    except ValueError:
                        delay = (2 ** (attempt + 1)) + random.uniform(0, 1)
                else:
                    delay = (2 ** (attempt + 1)) + random.uniform(0, 1)

                delay = min(delay, 60)
                logger.warning(f"Rate limited (HTTP {response.status_code}). Retrying in {delay:.2f}s")
                time.sleep(delay)
                continue

            if response.status_code >= 500:
                # Server error - retry
                delay = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"Server error (HTTP {response.status_code}). Retrying in {delay:.2f}s")
                time.sleep(delay)
                continue

            # Client error (4xx except 429) - don't retry
            logger.error(f"Webhook failed with HTTP {response.status_code}: {response.text}")
            return False

        except requests.exceptions.Timeout:
            delay = (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"Request timeout. Retrying in {delay:.2f}s")
            time.sleep(delay)
            continue
        except requests.exceptions.RequestException as e:
            delay = (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"Request error: {e}. Retrying in {delay:.2f}s")
            time.sleep(delay)
            continue

    logger.error("Max retries exceeded when sending webhook")
    return False


def check_and_process_daily_data(client: gspread.Client, state: dict, max_retries: int = 3, retry_delay: int = 600) -> bool:
    """
    Check for new daily data with retry logic for incomplete entries.

    Args:
        client: Authenticated gspread client
        state: Current state dict
        max_retries: Max retries for incomplete rows (default 3)
        retry_delay: Seconds to wait between retries (default 600 = 10 minutes)

    Returns:
        True if state was updated, False otherwise
    """
    last_processed_row = state.get("last_processed_row", 0)
    last_row_hash = state.get("last_row_hash")

    for attempt in range(max_retries + 1):
        # Fetch fresh data
        try:
            data = get_sheet_data_with_retry(client)
        except Exception as e:
            logger.error(f"Failed to fetch sheet data: {e}")
            return False

        # Check for complete rows
        latest_complete_index, latest_complete_row = get_latest_complete_row(data)

        # Check for incomplete rows (entry in progress)
        latest_any_index, latest_any_row = get_latest_row_with_date(data)

        # If there's a newer incomplete row, wait and retry
        if latest_any_index is not None:
            is_incomplete_new_row = (
                latest_any_index > last_processed_row and
                is_row_incomplete_but_started(latest_any_row)
            )

            if is_incomplete_new_row:
                if attempt < max_retries:
                    date_str = latest_any_row[0] if latest_any_row else "Unknown"
                    logger.info(f"Incomplete entry detected for {date_str} at row {latest_any_index}. "
                               f"Waiting {retry_delay // 60} minutes before retry {attempt + 1}/{max_retries}")
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.info("Max retries reached for incomplete entry. Will check again next run.")
                    return False

        # Process complete row if found
        if latest_complete_index is not None:
            current_row_hash = compute_row_hash(latest_complete_row)

            # Check if this is a new row or an update to existing row
            is_new_row = latest_complete_index > last_processed_row
            is_data_changed = (latest_complete_index == last_processed_row and current_row_hash != last_row_hash)

            if is_new_row or is_data_changed:
                alert_type = "New" if is_new_row else "Update"
                logger.info(f"{alert_type} data detected at row {latest_complete_index}")

                date_str = latest_complete_row[0] if latest_complete_row else "Unknown"
                daily_avg = calculate_daily_average(latest_complete_row)

                if daily_avg is not None:
                    card = format_daily_card(date_str, latest_complete_row, daily_avg, is_update=is_data_changed)

                    if send_webhook_with_retry(card):
                        state["last_processed_row"] = latest_complete_index
                        state["last_row_hash"] = current_row_hash
                        logger.info(f"Daily alert sent for {date_str} ({alert_type})")
                        return True
                    else:
                        logger.error("Failed to send daily alert")
                        return False
            else:
                logger.info("No new data or updates found")

        break

    return False


def main():
    """Main function to check for price updates and send alerts."""
    logger.info("Starting DOC Price Alert check")

    # Load state
    state = load_state()
    last_monthly_alert = state.get("last_monthly_alert")

    # Get current time in Nigeria timezone
    now = datetime.now(NIGERIA_TZ)
    current_month_key = now.strftime("%Y-%m")

    # Authenticate
    try:
        client = authenticate_google_sheets()
    except Exception as e:
        logger.error(f"Failed to authenticate: {e}")
        return

    state_updated = False

    # Check for new daily data with retry logic for incomplete entries
    if check_and_process_daily_data(client, state):
        state_updated = True

    # Check for monthly alert (first day of new month)
    if now.day == 1 and last_monthly_alert != current_month_key:
        # Calculate previous month
        first_of_current = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_of_previous = first_of_current - timedelta(days=1)
        prev_year = last_of_previous.year
        prev_month = last_of_previous.month
        prev_month_name = last_of_previous.strftime("%B")

        logger.info(f"Calculating monthly summary for {prev_month_name} {prev_year}")

        # Fetch data for monthly calculation
        try:
            data = get_sheet_data_with_retry(client)
            month_data = get_month_data(data, prev_year, prev_month)

            if month_data:
                averages = calculate_monthly_averages(month_data)
                card = format_monthly_card(prev_month_name, prev_year, prev_month, averages, len(month_data))

                if send_webhook_with_retry(card):
                    state["last_monthly_alert"] = current_month_key
                    state_updated = True
                    logger.info(f"Monthly alert sent for {prev_month_name} {prev_year}")
                else:
                    logger.error("Failed to send monthly alert")
            else:
                logger.warning(f"No data found for {prev_month_name} {prev_year}")
        except Exception as e:
            logger.error(f"Failed to fetch data for monthly summary: {e}")

    # Save state if updated
    if state_updated:
        save_state(state)

    logger.info("DOC Price Alert check completed")


if __name__ == "__main__":
    main()
