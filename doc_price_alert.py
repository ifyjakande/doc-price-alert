import os
import json
import time
import random
import logging
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


def load_state() -> dict:
    """Load state from file or return default state."""
    default_state = {
        "last_processed_row": 0,
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


def get_latest_complete_row(data: list) -> tuple:
    """Find the latest row with all columns filled. Returns (row_index, row_data)."""
    if len(data) <= 1:  # Only header or empty
        return None, None

    # Start from the end and find the last complete row
    for i in range(len(data) - 1, 0, -1):  # Skip header row (index 0)
        if is_row_complete(data[i]):
            return i, data[i]

    return None, None


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


def format_daily_card(date_str: str, row: list, daily_avg: float) -> dict:
    """Format daily price alert as Google Chat card."""
    # Build supplier price widgets
    supplier_widgets = []

    for i, supplier in enumerate(SUPPLIERS):
        col_index = i + 1
        price = row[col_index] if col_index < len(row) else "N/A"

        supplier_widgets.append({
            "decoratedText": {
                "topLabel": supplier,
                "text": f"{price}"
            }
        })

    card = {
        "cardsV2": [{
            "cardId": "daily-price-alert",
            "card": {
                "header": {
                    "title": "DOC Price Alert",
                    "subtitle": date_str
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


def format_monthly_card(month_name: str, year: int, averages: dict, days_count: int) -> dict:
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

    card = {
        "cardsV2": [{
            "cardId": "monthly-summary",
            "card": {
                "header": {
                    "title": "Monthly DOC Price Summary",
                    "subtitle": f"{month_name} {year}"
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
                                    "topLabel": "Monthly Average",
                                    "text": f"{overall_avg:,.0f}"
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


def main():
    """Main function to check for price updates and send alerts."""
    logger.info("Starting DOC Price Alert check")

    # Load state
    state = load_state()
    last_processed_row = state.get("last_processed_row", 0)
    last_monthly_alert = state.get("last_monthly_alert")

    # Get current time in Nigeria timezone
    now = datetime.now(NIGERIA_TZ)
    current_month_key = now.strftime("%Y-%m")

    # Authenticate and fetch data
    try:
        client = authenticate_google_sheets()
        data = get_sheet_data_with_retry(client)
    except Exception as e:
        logger.error(f"Failed to fetch sheet data: {e}")
        return

    state_updated = False

    # Check for new daily data
    latest_row_index, latest_row = get_latest_complete_row(data)

    if latest_row_index is not None and latest_row_index > last_processed_row:
        logger.info(f"New complete row found at index {latest_row_index}")

        date_str = latest_row[0] if latest_row else "Unknown"
        daily_avg = calculate_daily_average(latest_row)

        if daily_avg is not None:
            card = format_daily_card(date_str, latest_row, daily_avg)

            if send_webhook_with_retry(card):
                state["last_processed_row"] = latest_row_index
                state_updated = True
                logger.info(f"Daily alert sent for {date_str}")
            else:
                logger.error("Failed to send daily alert")
    else:
        logger.info("No new complete rows found")

    # Check for monthly alert (first day of new month)
    if now.day == 1 and last_monthly_alert != current_month_key:
        # Calculate previous month
        first_of_current = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_of_previous = first_of_current - timedelta(days=1)
        prev_year = last_of_previous.year
        prev_month = last_of_previous.month
        prev_month_name = last_of_previous.strftime("%B")

        logger.info(f"Calculating monthly summary for {prev_month_name} {prev_year}")

        month_data = get_month_data(data, prev_year, prev_month)

        if month_data:
            averages = calculate_monthly_averages(month_data)
            card = format_monthly_card(prev_month_name, prev_year, averages, len(month_data))

            if send_webhook_with_retry(card):
                state["last_monthly_alert"] = current_month_key
                state_updated = True
                logger.info(f"Monthly alert sent for {prev_month_name} {prev_year}")
            else:
                logger.error("Failed to send monthly alert")
        else:
            logger.warning(f"No data found for {prev_month_name} {prev_year}")

    # Save state if updated
    if state_updated:
        save_state(state)

    logger.info("DOC Price Alert check completed")


if __name__ == "__main__":
    main()
