import logging
import requests
import pyodbc
import os
import sys
import time
import json
from datetime import datetime, timezone
from dateutil.parser import parse as parse_date
from dotenv import load_dotenv

load_dotenv()

# ------------- Config -------------
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 18 for SQL Server")
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REDIRECT_URI = os.getenv("ZOHO_REDIRECT_URI")
ZOHO_ORG_ID = os.getenv("ZOHO_ORGANIZATION_ID")

# Optional: status filter for Bills. Valid values:
# Status.All | Status.Open | Status.Paid | Status.PartiallyPaid | Status.Overdue | Status.Void
ZOHO_BILLS_STATUS_FILTER = os.getenv("ZOHO_BILLS_STATUS_FILTER")  # e.g., "Status.All" or leave None/empty

REQUEST_TIMEOUT = 60
PER_PAGE = 200
RATE_LIMIT_SLEEP = 0.2  # seconds between detail calls

# ------------- Logging -------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(asctime)s:%(message)s",
)

# ------------- Helpers -------------
def safe_parse_date(value):
    try:
        if value:
            return parse_date(value)
    except Exception:
        return None
    return None

def today_yyyy_mm_dd():
    # Use system local date; if you prefer IST explicitly, adjust with tzinfo.
    return datetime.now().strftime("%Y-%m-%d")

# ------------- Auth -------------
def get_new_access_token():
    """
    Refresh OAuth token. Returns (access_token, api_domain).
    """
    payload = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "redirect_uri": ZOHO_REDIRECT_URI,
        "grant_type": "refresh_token",
    }
    url = "https://accounts.zoho.in/oauth/v2/token"
    r = requests.post(url, data=payload, timeout=REQUEST_TIMEOUT)
    logging.info(f"Token Response: {r.status_code}")
    try:
        j = r.json()
        logging.info(f"Token JSON: {j}")
    except Exception as e:
        logging.error(f"Failed to parse token JSON: {e} | Body: {r.text}")
        return None, None

    access_token = j.get("access_token")
    api_domain = j.get("api_domain", "https://www.zohoapis.in")  # safe default for India region
    if not access_token:
        logging.error("No access_token in token response.")
        return None, None
    return access_token, api_domain

# ------------- SQL -------------
def connect_to_sql():
    conn_str = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};PWD={SQL_PASSWORD}"
    )
    # format driver in curly braces properly
    conn_str = conn_str.replace("{SQL_DRIVER}", SQL_DRIVER)
    return pyodbc.connect(conn_str)

def create_tables(cursor):
    cursor.execute('''
    IF OBJECT_ID('Bills', 'U') IS NULL
    CREATE TABLE Bills (
        bill_id VARCHAR(100) PRIMARY KEY,
        vendor_name NVARCHAR(255),
        date DATE,
        status VARCHAR(100),
        total DECIMAL(18, 2),
        currency VARCHAR(10),
        place_of_supply NVARCHAR(255),
        billing_address NVARCHAR(MAX),
        shipping_address NVARCHAR(MAX),
        notes NVARCHAR(MAX),
        terms NVARCHAR(MAX),
        billing_state NVARCHAR(255),
        shipping_state NVARCHAR(255),
        created_time DATETIME,
        last_modified_time DATETIME
    )''')

    cursor.execute('''
    IF OBJECT_ID('BillLineItems', 'U') IS NULL
    CREATE TABLE BillLineItems (
        id INT IDENTITY(1,1) PRIMARY KEY,
        bill_id VARCHAR(100),
        item_name NVARCHAR(255),
        description NVARCHAR(MAX),
        rate DECIMAL(18, 2),
        quantity DECIMAL(18, 2),
        amount DECIMAL(18, 2),
        item_total DECIMAL(18, 2),
        tax_name NVARCHAR(255),
        tax_percentage DECIMAL(5,2),
        account_id NVARCHAR(100),
        account_name NVARCHAR(255)
    )''')

    cursor.connection.commit()

def insert_bill(cursor, bill):
    billing_address_str = json.dumps(bill.get('billing_address', {}), ensure_ascii=False)
    shipping_address_str = json.dumps(bill.get('shipping_address', {}), ensure_ascii=False)

    cursor.execute('''
        MERGE Bills AS target
        USING (SELECT ? AS bill_id) AS source
        ON target.bill_id = source.bill_id
        WHEN MATCHED THEN
            UPDATE SET 
                vendor_name = ?, date = ?, status = ?, total = ?, currency = ?,
                place_of_supply = ?, billing_address = ?, shipping_address = ?,
                notes = ?, terms = ?, billing_state = ?, shipping_state = ?,
                created_time = ?, last_modified_time = ?
        WHEN NOT MATCHED THEN
            INSERT (
                bill_id, vendor_name, date, status, total, currency,
                place_of_supply, billing_address, shipping_address,
                notes, terms, billing_state, shipping_state,
                created_time, last_modified_time
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    ''', (
        # source
        bill.get('bill_id'),
        # UPDATE values
        bill.get('vendor_name'),
        safe_parse_date(bill.get('date')),
        bill.get('status'),
        bill.get('total'),
        bill.get('currency_code'),
        bill.get('place_of_supply'),
        billing_address_str,
        shipping_address_str,
        bill.get('notes'),
        bill.get('terms'),
        (bill.get('billing_address') or {}).get('state'),
        (bill.get('shipping_address') or {}).get('state'),
        safe_parse_date(bill.get('created_time')),
        safe_parse_date(bill.get('last_modified_time')),
        # INSERT values (same as above)
        bill.get('bill_id'),
        bill.get('vendor_name'),
        safe_parse_date(bill.get('date')),
        bill.get('status'),
        bill.get('total'),
        bill.get('currency_code'),
        bill.get('place_of_supply'),
        billing_address_str,
        shipping_address_str,
        bill.get('notes'),
        bill.get('terms'),
        (bill.get('billing_address') or {}).get('state'),
        (bill.get('shipping_address') or {}).get('state'),
        safe_parse_date(bill.get('created_time')),
        safe_parse_date(bill.get('last_modified_time'))
    ))
    cursor.connection.commit()

def insert_line_items(cursor, bill_id, line_items):
    cursor.execute('DELETE FROM BillLineItems WHERE bill_id = ?', bill_id)
    if not line_items:
        cursor.connection.commit()
        return

    for item in line_items:
        tax_name = None
        tax_percentage = None
        taxes = item.get('taxes')
        if taxes and isinstance(taxes, list) and len(taxes) > 0:
            tax_name = taxes[0].get('name')
            tax_percentage = taxes[0].get('percentage')

        cursor.execute('''
            INSERT INTO BillLineItems (
                bill_id, item_name, description, rate, quantity,
                amount, item_total, tax_name, tax_percentage,
                account_id, account_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            bill_id,
            item.get('name'),
            item.get('description'),
            item.get('rate'),
            item.get('quantity'),
            item.get('amount'),
            item.get('item_total'),
            tax_name,
            tax_percentage,
            item.get('account_id'),
            item.get('account_name')
        ))

    cursor.connection.commit()

# ------------- Zoho API -------------
def fetch_bill_detail(api_domain, access_token, bill_id, org_id):
    url = f"{api_domain}/books/v3/bills/{bill_id}"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    params = {"organization_id": org_id}
    r = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
    try:
        r.raise_for_status()
        j = r.json()
    except Exception as e:
        logging.error(f"Bill detail request failed for {bill_id}: {e} | Body: {r.text}")
        return None

    if j.get("code", 0) != 0:
        logging.error(f"Bill detail API error for {bill_id}: {j}")
        return None

    return j.get("bill")

def fetch_all_bills(api_domain, access_token, cursor, start_date, end_date, org_id):
    """
    Fetches bills for the given date range using date_start/date_end.
    Optionally applies a valid status filter if ZOHO_BILLS_STATUS_FILTER is set.
    """
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    params = {
        "organization_id": org_id,
        "date_start": start_date,
        "date_end": end_date,
        "per_page": PER_PAGE,
        "page": 1,
    }
    if ZOHO_BILLS_STATUS_FILTER:
        params["filter_by"] = ZOHO_BILLS_STATUS_FILTER  # must be a valid Status.*

    list_url = f"{api_domain}/books/v3/bills"

    total_inserted = 0
    while True:
        r = requests.get(list_url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
        try:
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logging.error(f"HTTP/Parse error on page {params['page']}: {e} | Body: {r.text}")
            raise

        if data.get("code", 0) != 0:
            # Zoho soft error
            logging.error(f"API error: {data}")
            raise RuntimeError(data)

        bills = data.get("bills", [])
        if not bills:
            logging.info(f"📄 No bills on page {params['page']}. Stopping.")
            break

        for summary in bills:
            bill_id = summary.get("bill_id")
            full_bill = fetch_bill_detail(api_domain, access_token, bill_id, org_id)
            if full_bill:
                try:
                    insert_bill(cursor, full_bill)
                    line_items = full_bill.get("line_items") or []
                    insert_line_items(cursor, bill_id, line_items)
                    total_inserted += 1
                    logging.info(f" Inserted {bill_id} with {len(line_items)} line items")
                except Exception as e:
                    logging.error(f" Failed to insert bill {bill_id}: {e}")
            time.sleep(RATE_LIMIT_SLEEP)

        page_ctx = data.get("page_context") or {}
        has_more = page_ctx.get("has_more_page")
        logging.info(f" Processed page {params['page']} with {len(bills)} bills")
        if has_more:
            params["page"] = (page_ctx.get("page") or params["page"]) + 1
        else:
            break

    logging.info(f" Done. Inserted/updated {total_inserted} bills in total.")

# ------------- Main -------------
def main():
    logging.info(" Starting Zoho Purchase Bills ETL (today only)...")

    if not all([SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, ZOHO_REFRESH_TOKEN, ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REDIRECT_URI, ZOHO_ORG_ID]):
        logging.error(" Missing required environment variables. Please check your .env.")
        sys.exit(2)

    access_token, api_domain = get_new_access_token()
    if not access_token or not api_domain:
        logging.error(" Failed to refresh access token")
        sys.exit(2)

    # date range: today → today (YYYY-MM-DD)
    today = today_yyyy_mm_dd()
    logging.info(f"Pulling all bills for {today}...")

    conn = None
    try:
        conn = connect_to_sql()
        cursor = conn.cursor()
        create_tables(cursor)

        fetch_all_bills(
            api_domain=api_domain,
            access_token=access_token,
            cursor=cursor,
            start_date=today,
            end_date=today,
            org_id=ZOHO_ORG_ID
        )

        cursor.close()
        conn.close()
        logging.info(" ETL job for bills completed successfully.")
    except Exception as e:
        logging.exception(f" Error during ETL: {e}")
        try:
            if conn:
                conn.close()
        finally:
            sys.exit(1)

if __name__ == "__main__":
    main()
