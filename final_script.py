import logging
import requests
import pyodbc
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 🔑 Load environment variables
load_dotenv()

# -------------------------
#  OAuth Token Refresh
# -------------------------
def get_new_access_token():
    payload = {
        "refresh_token": os.getenv("ZOHO_REFRESH_TOKEN"),
        "client_id": os.getenv("ZOHO_CLIENT_ID"),
        "client_secret": os.getenv("ZOHO_CLIENT_SECRET"),
        "redirect_uri": os.getenv("ZOHO_REDIRECT_URI"),
        "grant_type": "refresh_token"
    }

    response = requests.post("https://accounts.zoho.in/oauth/v2/token", data=payload)
    logging.info(f"Token Response: {response.status_code}")
    
    try:
        json_data = response.json()
        logging.info(f"Token JSON: {json_data}")
        return json_data.get("access_token")
    except Exception as e:
        logging.error(f"Failed to parse token JSON: {e}")
        return None

# -------------------------
#  SQL Connection
# -------------------------
def connect_to_sql():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('SQL_SERVER')};DATABASE={os.getenv('SQL_DATABASE')};"
        f"UID={os.getenv('SQL_USERNAME')};PWD={os.getenv('SQL_PASSWORD')}"
    )

# -------------------------
#  Create Tables
# -------------------------
def create_tables(cursor):
    cursor.execute('''
    IF OBJECT_ID('Invoices', 'U') IS NULL
    CREATE TABLE Invoices (
        invoice_id VARCHAR(100),
        customer_name VARCHAR(255),
        date DATE,
        status VARCHAR(100),
        total DECIMAL(18, 2),
        currency VARCHAR(10),
        billing_address NVARCHAR(MAX),
        shipping_address NVARCHAR(MAX),
        custom_fields NVARCHAR(MAX),
        taxes NVARCHAR(MAX),
        billing_state NVARCHAR(255),
        shipping_state NVARCHAR(255)
    )''')

    cursor.execute('''
    IF OBJECT_ID('InvoiceLineItems', 'U') IS NULL
    CREATE TABLE InvoiceLineItems (
        id INT IDENTITY(1,1) PRIMARY KEY,
        invoice_id VARCHAR(100),
        item_name NVARCHAR(255),
        description NVARCHAR(MAX),
        rate DECIMAL(18, 2),
        quantity DECIMAL(18, 2),
        amount DECIMAL(18, 2),
        item_total DECIMAL(18, 2),
        item_tax NVARCHAR(MAX)
    )''')

    cursor.execute('''
    IF OBJECT_ID('CreditNotes', 'U') IS NULL
    CREATE TABLE CreditNotes (
        creditnote_id VARCHAR(100),
        customer_name VARCHAR(255),
        date DATE,
        status VARCHAR(100),
        total DECIMAL(18, 2),
        currency VARCHAR(10),
        billing_address NVARCHAR(MAX),
        shipping_address NVARCHAR(MAX),
        custom_fields NVARCHAR(MAX),
        taxes NVARCHAR(MAX),
        billing_state NVARCHAR(255),
        shipping_state NVARCHAR(255)
    )''')

    cursor.execute('''
    IF OBJECT_ID('CreditNoteLineItems', 'U') IS NULL
    CREATE TABLE CreditNoteLineItems (
        id INT IDENTITY(1,1) PRIMARY KEY,
        creditnote_id VARCHAR(100),
        item_name NVARCHAR(255),
        description NVARCHAR(MAX),
        rate DECIMAL(18, 2),
        quantity DECIMAL(18, 2),
        amount DECIMAL(18, 2),
        item_total DECIMAL(18, 2),
        item_tax NVARCHAR(MAX)
    )''')

    cursor.connection.commit()

# -------------------------
#  Insert Functions
# -------------------------
def insert_invoice(cursor, inv):
    cursor.execute('''
        INSERT INTO Invoices (
            invoice_id, customer_name, date, status, total,
            currency, billing_address, shipping_address,
            custom_fields, taxes, billing_state, shipping_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        inv.get('invoice_id'),
        inv.get('customer_name'),
        inv.get('date'),
        inv.get('status'),
        inv.get('total'),
        inv.get('currency_code'),
        str(inv.get('billing_address')),
        str(inv.get('shipping_address')),
        str(inv.get('custom_fields')),
        str(inv.get('taxes')),
        inv.get('billing_address', {}).get('state'),
        inv.get('shipping_address', {}).get('state')
    ))
    cursor.connection.commit()

def insert_line_items(cursor, invoice_id, line_items):
    if not line_items:
        return
    for item in line_items:
        cursor.execute('''
            INSERT INTO InvoiceLineItems (
                invoice_id, item_name, description, rate, quantity,
                amount, item_total, item_tax
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            invoice_id,
            item.get('name'),
            item.get('description'),
            item.get('rate'),
            item.get('quantity'),
            item.get('amount'),
            item.get('item_total'),
            str(item.get('taxes'))
        ))
    cursor.connection.commit()

def insert_credit_note(cursor, cn):
    cursor.execute('''
        INSERT INTO CreditNotes (
            creditnote_id, customer_name, date, status, total,
            currency, billing_address, shipping_address,
            custom_fields, taxes, billing_state, shipping_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        cn.get('creditnote_id'),
        cn.get('customer_name'),
        cn.get('date'),
        cn.get('status'),
        cn.get('total'),
        cn.get('currency_code'),
        str(cn.get('billing_address')),
        str(cn.get('shipping_address')),
        str(cn.get('custom_fields')),
        str(cn.get('taxes')),
        cn.get('billing_address', {}).get('state'),
        cn.get('shipping_address', {}).get('state')
    ))
    cursor.connection.commit()

def insert_credit_note_line_items(cursor, creditnote_id, line_items):
    if not line_items:
        return
    for item in line_items:
        cursor.execute('''
            INSERT INTO CreditNoteLineItems (
                creditnote_id, item_name, description, rate, quantity,
                amount, item_total, item_tax
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            creditnote_id,
            item.get('name'),
            item.get('description'),
            item.get('rate'),
            item.get('quantity'),
            item.get('amount'),
            item.get('item_total'),
            str(item.get('taxes'))
        ))
    cursor.connection.commit()

# -------------------------
#  Fetch from Zoho
# -------------------------
def fetch_invoice_detail(access_token, invoice_id, org_id):
    url = f"https://www.zohoapis.in/books/v3/invoices/{invoice_id}?organization_id={org_id}"
    headers = { "Authorization": f"Zoho-oauthtoken {access_token}" }
    response = requests.get(url, headers=headers)
    return response.json().get("invoice")

def fetch_all_invoices(access_token, cursor, start_date, org_id):
    page = 1
    per_page = 200
    headers = { "Authorization": f"Zoho-oauthtoken {access_token}" }
    base_url = f"https://www.zohoapis.in/books/v3/invoices?organization_id={org_id}&date_start={start_date}"

    while True:
        response = requests.get(f"{base_url}&page={page}&per_page={per_page}", headers=headers)
        data = response.json()
        if "invoices" not in data:
            logging.error(f"API error: {data}")
            break

        invoices = data["invoices"]
        if not invoices:
            break

        for summary in invoices:
            invoice_id = summary.get("invoice_id")
            full_invoice = fetch_invoice_detail(access_token, invoice_id, org_id)
            if full_invoice:
                insert_invoice(cursor, full_invoice)
                line_items = full_invoice.get("line_items")
                insert_line_items(cursor, invoice_id, line_items)
                logging.info(f"Inserted Invoice {invoice_id} with {len(line_items) if line_items else 0} line items")

            time.sleep(0.2)

        logging.info(f"Processed page {page} with {len(invoices)} invoices")
        page += 1

def fetch_credit_note_detail(access_token, creditnote_id, org_id):
    url = f"https://www.zohoapis.in/books/v3/creditnotes/{creditnote_id}?organization_id={org_id}"
    headers = { "Authorization": f"Zoho-oauthtoken {access_token}" }
    response = requests.get(url, headers=headers)
    return response.json().get("creditnote")

def fetch_all_credit_notes(access_token, cursor, start_date, org_id):
    page = 1
    per_page = 200
    headers = { "Authorization": f"Zoho-oauthtoken {access_token}" }
    base_url = f"https://www.zohoapis.in/books/v3/creditnotes?organization_id={org_id}&date_start={start_date}"

    while True:
        response = requests.get(f"{base_url}&page={page}&per_page={per_page}", headers=headers)
        data = response.json()
        if "creditnotes" not in data:
            logging.error(f"API error: {data}")
            break

        creditnotes = data["creditnotes"]
        if not creditnotes:
            break

        for summary in creditnotes:
            cn_id = summary.get("creditnote_id")
            full_cn = fetch_credit_note_detail(access_token, cn_id, org_id)
            if full_cn:
                insert_credit_note(cursor, full_cn)
                line_items = full_cn.get("line_items")
                insert_credit_note_line_items(cursor, cn_id, line_items)
                logging.info(f"Inserted Credit Note {cn_id} with {len(line_items) if line_items else 0} line items")

            time.sleep(0.2)

        logging.info(f"Processed page {page} with {len(creditnotes)} credit notes")
        page += 1

# -------------------------
#  Main Function
# -------------------------
def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("⏰ Starting Zoho ETL function locally...")

    token = get_new_access_token()
    if not token:
        logging.error("❌ Failed to refresh access token")
        return

    try:
        conn = connect_to_sql()
        cursor = conn.cursor()
        create_tables(cursor)
        today = datetime.today().strftime('%Y-%m-%d')

        # Fetch invoices
        fetch_all_invoices(token, cursor, start_date=today, org_id=os.getenv("ZOHO_ORGANIZATION_ID"))

        # Fetch credit notes
        fetch_all_credit_notes(token, cursor, start_date=today, org_id=os.getenv("ZOHO_ORGANIZATION_ID"))

        cursor.close()
        conn.close()
        logging.info("✅ ETL job completed successfully.")
    except Exception as e:
        logging.exception(f"❌ Error during ETL: {e}")

if __name__ == "__main__":
    main()
