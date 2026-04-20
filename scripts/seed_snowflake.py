#!/usr/bin/env python3
"""
Snowflake seed data script for ECOMM_DATA_LAKE.CONFORMED tables.
Populates 11 tables with realistic ecommerce data designed to surface 3 planted bugs:
  1. PII bug: DIM_CUSTOMER data returned unfiltered
  2. Inventory bug: WHERE clause excludes Electronics category
  3. Profit bug: Discounts and refunds ignored in revenue calc
"""

import snowflake.connector
import random
from datetime import date, datetime, timedelta
import pytz

# ── Connection ──────────────────────────────────────────────────────────────
CONN_PARAMS = {
    "account": "LVSAHNU-PR54555",
    "user": "GVKPATEL2312",
    "password": "$RFVvfr4$RFVvfr4",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "ECOMM_DATA_LAKE",
    "schema": "CONFORMED",
}

IST = pytz.timezone("Asia/Kolkata")
NOW_IST = datetime.now(IST)

# ── Helpers ──────────────────────────────────────────────────────────────────
def ts(dt):
    """Return TIMESTAMP_TZ string for Snowflake."""
    if dt.tzinfo is None:
        dt = IST.localize(dt)
    return dt.strftime("%Y-%m-%d %H:%M:%S %z")

def date_sk(d):
    return int(d.strftime("%Y%m%d"))

def rand_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def batch_insert(cur, table, columns, rows, batch_size=500):
    col_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO ECOMM_DATA_LAKE.CONFORMED.{table} ({col_str}) VALUES ({placeholders})"
    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        cur.executemany(sql, batch)
        inserted += len(batch)
    print(f"  ✓ {table}: {inserted} rows inserted")
    return inserted

# ── Static reference data ─────────────────────────────────────────────────
FIRST_NAMES = [
    "Aarav","Vivaan","Aditya","Vihaan","Arjun","Sai","Reyansh","Ayaan","Dhruv","Kabir",
    "Ishaan","Shaurya","Atharv","Advik","Pranav","Rohan","Kiran","Nikhil","Rahul","Vikram",
    "Priya","Ananya","Diya","Anika","Kavya","Meera","Nisha","Pooja","Riya","Sanya",
    "Shreya","Tanvi","Usha","Vidya","Zara","Arya","Bhavna","Charu","Deepa","Esha",
    "John","Michael","David","James","Robert","William","Richard","Joseph","Thomas","Chris",
    "Emma","Olivia","Ava","Sophia","Isabella","Mia","Charlotte","Amelia","Harper","Evelyn",
    "Raj","Amit","Suresh","Ramesh","Harish","Girish","Mahesh","Ganesh","Dinesh","Yogesh",
]
LAST_NAMES = [
    "Sharma","Verma","Singh","Kumar","Patel","Gupta","Joshi","Yadav","Mishra","Tiwari",
    "Reddy","Rao","Nair","Iyer","Menon","Pillai","Naidu","Chandra","Bose","Das",
    "Shah","Mehta","Desai","Jain","Kapoor","Malhotra","Chopra","Khanna","Aggarwal","Bansal",
    "Smith","Jones","Taylor","Brown","Williams","Wilson","Evans","Thomas","Roberts","Johnson",
]
CITIES_STATES = [
    ("Mumbai", "MH"), ("Delhi", "DL"), ("Bengaluru", "KA"), ("Hyderabad", "TS"),
    ("Chennai", "TN"), ("Kolkata", "WB"), ("Pune", "MH"), ("Ahmedabad", "GJ"),
    ("Jaipur", "RJ"), ("Lucknow", "UP"), ("Kochi", "KL"), ("Chandigarh", "PB"),
    ("Nagpur", "MH"), ("Surat", "GJ"), ("Indore", "MP"), ("Bhopal", "MP"),
    ("Vadodara", "GJ"), ("Coimbatore", "TN"), ("Patna", "BR"), ("Guwahati", "AS"),
]
DOMAINS = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "rediffmail.com", "email.com"]
BRANDS = {
    "Electronics": ["Samsung", "Apple", "OnePlus", "Xiaomi", "Sony", "LG", "Realme", "Asus", "Dell", "HP"],
    "Fashion": ["Zara", "H&M", "Fabindia", "Myntra", "Biba", "W", "Allen Solly", "Van Heusen", "Peter England", "Levi's"],
    "Home & Kitchen": ["Prestige", "Bajaj", "Philips", "Pigeon", "Borosil", "Cello", "Milton", "Tupperware", "IKEA", "Nilkamal"],
    "Beauty": ["Lakme", "Maybelline", "L'Oreal", "Nykaa", "Biotique", "Himalaya", "Pond's", "Dove", "Garnier", "Mamaearth"],
    "Sports": ["Nike", "Adidas", "Puma", "Reebok", "Decathlon", "Vector X", "Yonex", "Cosco", "SG", "Nivia"],
}

# ── 1. DIM_DATE ───────────────────────────────────────────────────────────────
def seed_dim_date(cur):
    print("Seeding DIM_DATE...")
    start = date(2026, 1, 1)
    rows = []
    day_names = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]
    day_short = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]
    month_names = ["","January","February","March","April","May","June",
                   "July","August","September","October","November","December"]
    month_short = ["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    indian_holidays = {
        date(2026, 1, 26): "Republic Day",
        date(2026, 3, 25): "Holi",
        date(2026, 4, 14): "Ambedkar Jayanti",
        date(2026, 8, 15): "Independence Day",
        date(2026, 10, 2): "Gandhi Jayanti",
        date(2026, 10, 24): "Dussehra",
        date(2026, 11, 12): "Diwali",
        date(2026, 12, 25): "Christmas",
    }
    sale_events = {
        date(2026, 1, 26): "Republic Day Sale",
        date(2026, 1, 27): "Republic Day Sale",
        date(2026, 1, 28): "Republic Day Sale",
        date(2026, 3, 25): "Holi Sale",
        date(2026, 3, 26): "Holi Sale",
        date(2026, 11, 11): "Big Billion Day",
        date(2026, 11, 12): "Big Billion Day",
        date(2026, 11, 13): "Big Billion Day",
    }
    for i in range(365):
        d = start + timedelta(days=i)
        dow = d.weekday()  # 0=Mon, 6=Sun
        snow_dow = (dow + 1) % 7 + 1  # Snowflake: 1=Sun,...7=Sat
        is_weekend = dow >= 5
        q = (d.month - 1) // 3 + 1
        fy = 2026 if d.month >= 4 else 2025
        fq = ((d.month - 4) % 12) // 3 + 1
        season = ("Winter" if d.month in [12,1,2] else
                  "Spring" if d.month in [3,4,5] else
                  "Summer" if d.month in [6,7,8] else "Autumn")
        is_holiday = d in indian_holidays
        holiday_name = indian_holidays.get(d)
        is_sale = d in sale_events
        sale_name = sale_events.get(d)
        rows.append((
            date_sk(d), d.isoformat(),
            snow_dow, day_names[dow], day_short[dow],
            d.isocalendar()[1],
            d.month, month_names[d.month], month_short[d.month],
            q, f"Q{q}",
            d.year, f"{d.year}-{str(d.month).zfill(2)}",
            fy, fq, f"FY{fy}",
            is_weekend, is_holiday, holiday_name,
            is_sale, sale_name, season,
        ))
    cols = [
        "DATE_SK","FULL_DATE","DAY_OF_WEEK","DAY_NAME","DAY_NAME_SHORT",
        "WEEK_OF_YEAR","MONTH_NUM","MONTH_NAME","MONTH_NAME_SHORT",
        "QUARTER_NUM","QUARTER_LABEL","YEAR_NUM","YEAR_MONTH",
        "FISCAL_YEAR","FISCAL_QUARTER","FISCAL_YEAR_LABEL",
        "IS_WEEKEND","IS_PUBLIC_HOLIDAY","HOLIDAY_NAME",
        "IS_SALE_EVENT","SALE_EVENT_NAME","SEASON",
    ]
    batch_insert(cur, "DIM_DATE", cols, rows)

# ── 2. DIM_CATEGORY ───────────────────────────────────────────────────────────
def seed_dim_category(cur):
    print("Seeding DIM_CATEGORY...")
    now_str = ts(NOW_IST)
    categories = [
        # (category_id, l1_id, l1_name, l2_id, l2_name, l3_id, l3_name)
        ("CAT001", "L1-ELEC", "Electronics", None, None, None, None),
        ("CAT002", "L1-ELEC", "Electronics", "L2-MOB", "Mobile Phones", None, None),
        ("CAT003", "L1-ELEC", "Electronics", "L2-LAPTOP", "Laptops", None, None),
        ("CAT004", "L1-ELEC", "Electronics", "L2-TV", "Televisions", None, None),
        ("CAT005", "L1-FASH", "Fashion", None, None, None, None),
        ("CAT006", "L1-FASH", "Fashion", "L2-MENS", "Men's Clothing", None, None),
        ("CAT007", "L1-FASH", "Fashion", "L2-WOMENS", "Women's Clothing", None, None),
        ("CAT008", "L1-FASH", "Fashion", "L2-FOOTWEAR", "Footwear", None, None),
        ("CAT009", "L1-HOME", "Home & Kitchen", None, None, None, None),
        ("CAT010", "L1-HOME", "Home & Kitchen", "L2-APPLIANCES", "Appliances", None, None),
        ("CAT011", "L1-HOME", "Home & Kitchen", "L2-COOKWARE", "Cookware", None, None),
        ("CAT012", "L1-HOME", "Home & Kitchen", "L2-FURNITURE", "Furniture", None, None),
        ("CAT013", "L1-BEAUTY", "Beauty", None, None, None, None),
        ("CAT014", "L1-BEAUTY", "Beauty", "L2-SKINCARE", "Skincare", None, None),
        ("CAT015", "L1-BEAUTY", "Beauty", "L2-MAKEUP", "Makeup", None, None),
        ("CAT016", "L1-SPORTS", "Sports", None, None, None, None),
        ("CAT017", "L1-SPORTS", "Sports", "L2-OUTDOOR", "Outdoor Sports", None, None),
        ("CAT018", "L1-SPORTS", "Sports", "L2-FITNESS", "Fitness", None, None),
        ("CAT019", "L1-SPORTS", "Sports", "L2-CRICKET", "Cricket", None, None),
        ("CAT020", "L1-BEAUTY", "Beauty", "L2-HAIRCARE", "Haircare", None, None),
    ]
    rows = []
    for i, (cat_id, l1id, l1name, l2id, l2name, l3id, l3name) in enumerate(categories, 1):
        rows.append((
            cat_id, 1, True,
            l1id, l1name,
            l2id, l2name,
            l3id, l3name,
            now_str, now_str, now_str,
        ))
    cols = [
        "CATEGORY_ID","CATEGORY_VERSION","IS_ACTIVE",
        "CATEGORY_L1_ID","CATEGORY_L1_NAME",
        "CATEGORY_L2_ID","CATEGORY_L2_NAME",
        "CATEGORY_L3_ID","CATEGORY_L3_NAME",
        "CREATED_WHEN","UPDATED_WHEN","INGESTED_AT",
    ]
    # Insert and capture assigned SKs
    col_str = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO ECOMM_DATA_LAKE.CONFORMED.DIM_CATEGORY ({col_str}) VALUES ({placeholders})"
    cur.executemany(sql, rows)
    print(f"  ✓ DIM_CATEGORY: {len(rows)} rows inserted")

    # Fetch back SKs keyed by category_id
    cur.execute("SELECT CATEGORY_SK, CATEGORY_ID FROM ECOMM_DATA_LAKE.CONFORMED.DIM_CATEGORY")
    cat_map = {row[1]: row[0] for row in cur.fetchall()}
    return cat_map, categories

# ── 3. DIM_MERCHANT ───────────────────────────────────────────────────────────
def seed_dim_merchant(cur):
    print("Seeding DIM_MERCHANT...")
    now_str = ts(NOW_IST)
    merchants = [
        ("MER001", "TechZone India", "SELLER", "FBP", "IN", "Maharashtra", 4.5),
        ("MER002", "Fashion Hub", "SELLER", "FBS", "IN", "Karnataka", 4.2),
        ("MER003", "Home Essentials", "SELLER", "FBP", "IN", "Delhi", 4.7),
        ("MER004", "Beauty World", "SELLER", "FBS", "IN", "Tamil Nadu", 4.3),
        ("MER005", "SportsPro", "SELLER", "FBP", "IN", "Gujarat", 4.6),
        ("MER006", "Gadget Galaxy", "BRAND", "FBP", "IN", "Maharashtra", 4.8),
        ("MER007", "Style Street", "SELLER", "FBS", "IN", "West Bengal", 3.9),
        ("MER008", "Kitchen King", "SELLER", "FBP", "IN", "Rajasthan", 4.4),
        ("MER009", "Glow & Go", "BRAND", "FBS", "IN", "Karnataka", 4.5),
        ("MER010", "ActiveLife", "SELLER", "FBP", "IN", "Punjab", 4.1),
    ]
    rows = []
    for m_id, m_name, m_type, fulfil, country, state, rating in merchants:
        rows.append((
            m_id, 1, True,
            m_name, m_type, fulfil,
            None, country, state, rating,
            date(2024, 1, 1), "STANDARD",
            now_str, now_str, now_str,
        ))
    cols = [
        "MERCHANT_ID","MERCHANT_VERSION","IS_ACTIVE",
        "MERCHANT_NAME","MERCHANT_TYPE","FULFILLMENT_TYPE",
        "GSTIN","COUNTRY_CODE","STATE","SELLER_RATING",
        "ONBOARDED_DATE","SLA_TIER",
        "CREATED_WHEN","UPDATED_WHEN","INGESTED_AT",
    ]
    batch_insert(cur, "DIM_MERCHANT", cols, rows)
    cur.execute("SELECT MERCHANT_SK, MERCHANT_ID FROM ECOMM_DATA_LAKE.CONFORMED.DIM_MERCHANT")
    return {row[1]: row[0] for row in cur.fetchall()}

# ── 4. DIM_CHANNEL ────────────────────────────────────────────────────────────
def seed_dim_channel(cur):
    print("Seeding DIM_CHANNEL...")
    now_str = ts(NOW_IST)
    channels = [
        ("CH001", "Website", "WEB", "ShopEasy", 2.5),
        ("CH002", "Android App", "APP", "ShopEasy", 2.5),
        ("CH003", "iOS App", "APP", "ShopEasy", 2.5),
        ("CH004", "Call Centre", "OFFLINE", None, 0.0),
    ]
    rows = []
    for c_id, c_name, c_type, mkt_name, comm_rate in channels:
        rows.append((
            c_id, c_name, c_type, mkt_name, comm_rate, "IN", True,
            now_str, now_str,
        ))
    cols = [
        "CHANNEL_ID","CHANNEL_NAME","CHANNEL_TYPE","MARKETPLACE_NAME",
        "COMMISSION_RATE_PCT","COUNTRY_CODE","IS_ACTIVE",
        "CREATED_WHEN","UPDATED_WHEN",
    ]
    batch_insert(cur, "DIM_CHANNEL", cols, rows)
    cur.execute("SELECT CHANNEL_SK, CHANNEL_ID FROM ECOMM_DATA_LAKE.CONFORMED.DIM_CHANNEL")
    return {row[1]: row[0] for row in cur.fetchall()}

# ── 5. DIM_LOCATION ───────────────────────────────────────────────────────────
def seed_dim_location(cur):
    print("Seeding DIM_LOCATION...")
    now_str = ts(NOW_IST)
    locations = [
        ("LOC001", "Mumbai FC1", "FULFILLMENT_CENTER", "Mumbai", "MH", "400001"),
        ("LOC002", "Delhi FC1", "FULFILLMENT_CENTER", "Delhi", "DL", "110001"),
        ("LOC003", "Bengaluru FC1", "FULFILLMENT_CENTER", "Bengaluru", "KA", "560001"),
        ("LOC004", "Hyderabad FC1", "FULFILLMENT_CENTER", "Hyderabad", "TS", "500001"),
        ("LOC005", "Chennai FC1", "FULFILLMENT_CENTER", "Chennai", "TN", "600001"),
        ("LOC006", "Kolkata WH1", "WAREHOUSE", "Kolkata", "WB", "700001"),
        ("LOC007", "Pune WH1", "WAREHOUSE", "Pune", "MH", "411001"),
        ("LOC008", "Ahmedabad WH1", "WAREHOUSE", "Ahmedabad", "GJ", "380001"),
        ("LOC009", "Jaipur WH1", "WAREHOUSE", "Jaipur", "RJ", "302001"),
        ("LOC010", "Lucknow WH1", "WAREHOUSE", "Lucknow", "UP", "226001"),
        ("LOC011", "Kochi HUB1", "SORTATION_CENTER", "Kochi", "KL", "682001"),
        ("LOC012", "Chandigarh HUB1", "SORTATION_CENTER", "Chandigarh", "PB", "160001"),
        ("LOC013", "Nagpur HUB1", "SORTATION_CENTER", "Nagpur", "MH", "440001"),
        ("LOC014", "Surat HUB1", "SORTATION_CENTER", "Surat", "GJ", "395001"),
        ("LOC015", "Indore HUB1", "SORTATION_CENTER", "Indore", "MP", "452001"),
        ("LOC016", "Bhopal STORE1", "STORE", "Bhopal", "MP", "462001"),
        ("LOC017", "Vadodara STORE1", "STORE", "Vadodara", "GJ", "390001"),
        ("LOC018", "Coimbatore STORE1", "STORE", "Coimbatore", "TN", "641001"),
        ("LOC019", "Patna STORE1", "STORE", "Patna", "BR", "800001"),
        ("LOC020", "Guwahati STORE1", "STORE", "Guwahati", "AS", "781001"),
    ]
    rows = []
    for l_id, l_name, l_type, city, state, pin in locations:
        rows.append((
            l_id, l_name, l_type,
            f"Plot 1, Industrial Area", city, state, "IN", pin,
            None, None, 50000.0, True,
            now_str, now_str,
        ))
    cols = [
        "LOCATION_ID","LOCATION_NAME","LOCATION_TYPE",
        "ADDRESS_LINE1","CITY","STATE_CODE","COUNTRY_CODE","PINCODE",
        "LATITUDE","LONGITUDE","STORAGE_CAPACITY_SQFT","IS_ACTIVE",
        "CREATED_WHEN","UPDATED_WHEN",
    ]
    batch_insert(cur, "DIM_LOCATION", cols, rows)
    cur.execute("SELECT LOCATION_SK, LOCATION_ID FROM ECOMM_DATA_LAKE.CONFORMED.DIM_LOCATION")
    return {row[1]: row[0] for row in cur.fetchall()}

# ── 6. DIM_CUSTOMER ───────────────────────────────────────────────────────────
def seed_dim_customer(cur):
    print("Seeding DIM_CUSTOMER (200 customers with PII)...")
    now_str = ts(NOW_IST)
    random.seed(42)
    loyalty_tiers = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
    segments = ["BUDGET_SHOPPER", "PREMIUM_BUYER", "FREQUENT_BUYER", "OCCASIONAL", "LAPSED"]
    rows = []
    for i in range(1, 201):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        full_name = f"{first} {last}"
        email_user = f"{first.lower()}.{last.lower()}{random.randint(1,99)}"
        email = f"{email_user}@{random.choice(DOMAINS)}"
        phone = f"+91{random.randint(7000000000, 9999999999)}"
        dob = rand_date(date(1970, 1, 1), date(2002, 12, 31))
        gender = random.choice(["M", "F", "M", "M", "F"])
        city, state = random.choice(CITIES_STATES)
        pincode = str(random.randint(100000, 999999))
        tier = random.choices(loyalty_tiers, weights=[40, 30, 20, 10])[0]
        points = random.randint(0, 50000) if tier in ["GOLD","PLATINUM"] else random.randint(0, 5000)
        segment = random.choice(segments)
        first_order = rand_date(date(2023, 1, 1), date(2025, 12, 31))
        is_verified = random.random() > 0.1
        rows.append((
            f"CUST{str(i).zfill(6)}", 1, True,
            full_name, email, phone,
            dob.isoformat(), gender,
            "IN", city, state, pincode,
            tier, float(points), segment,
            None,  # acquisition_channel_sk
            first_order.isoformat(), is_verified, False,
            now_str, now_str, now_str,
        ))
    cols = [
        "CUSTOMER_ID","CUSTOMER_VERSION","IS_ACTIVE",
        "FULL_NAME","EMAIL","PHONE_NUMBER",
        "DATE_OF_BIRTH","GENDER",
        "COUNTRY_CODE","CITY","STATE_CODE","PINCODE",
        "LOYALTY_TIER","LOYALTY_POINTS","CUSTOMER_SEGMENT",
        "ACQUISITION_CHANNEL_SK",
        "FIRST_ORDER_DATE","IS_VERIFIED","IS_BLOCKED",
        "CREATED_WHEN","UPDATED_WHEN","INGESTED_AT",
    ]
    batch_insert(cur, "DIM_CUSTOMER", cols, rows)
    cur.execute("SELECT CUSTOMER_SK, CUSTOMER_ID FROM ECOMM_DATA_LAKE.CONFORMED.DIM_CUSTOMER")
    return {row[1]: row[0] for row in cur.fetchall()}

# ── 7. DIM_PRODUCT ────────────────────────────────────────────────────────────
def seed_dim_product(cur, cat_map, cat_defs, merchant_sk_map):
    print("Seeding DIM_PRODUCT (50 products, ~10 Electronics for inventory bug)...")
    now_str = ts(NOW_IST)

    # Build category L1->SK lookup
    l1_to_cat_id = {}
    for cat_id, l1id, l1name, l2id, l2name, l3id, l3name in cat_defs:
        if l2id is None:  # top-level categories
            l1_to_cat_id[l1name] = cat_id

    # Map L1 name to sub-category ids (non-root)
    l1_subcats = {}
    for cat_id, l1id, l1name, l2id, l2name, l3id, l3name in cat_defs:
        if l2id is not None:
            l1_subcats.setdefault(l1name, []).append(cat_id)

    products = [
        # Electronics (10 products — intentionally significant stock for inventory bug)
        ("PRD001", "Samsung Galaxy S25 Ultra", "Electronics", "Samsung", 129999.00, 95000.00, True, 7),
        ("PRD002", "Apple iPhone 16 Pro", "Electronics", "Apple", 134900.00, 100000.00, True, 7),
        ("PRD003", "OnePlus 13R", "Electronics", "OnePlus", 39999.00, 28000.00, True, 7),
        ("PRD004", "Xiaomi 14 Pro", "Electronics", "Xiaomi", 54999.00, 38000.00, True, 7),
        ("PRD005", "Sony Bravia 55 4K TV", "Electronics", "Sony", 89990.00, 65000.00, False, 0),
        ("PRD006", "LG OLED 65 TV", "Electronics", "LG", 179900.00, 130000.00, False, 0),
        ("PRD007", "Dell XPS 15 Laptop", "Electronics", "Dell", 159990.00, 120000.00, True, 14),
        ("PRD008", "HP Pavilion 15 Laptop", "Electronics", "HP", 69990.00, 50000.00, True, 14),
        ("PRD009", "Apple MacBook Air M4", "Electronics", "Apple", 114900.00, 85000.00, True, 14),
        ("PRD010", "Asus ZenBook 14", "Electronics", "Asus", 84990.00, 62000.00, True, 14),
        # Fashion (10 products)
        ("PRD011", "Levi's 511 Slim Fit Jeans", "Fashion", "Levi's", 3499.00, 1200.00, True, 30),
        ("PRD012", "Allen Solly Formal Shirt", "Fashion", "Allen Solly", 1899.00, 600.00, True, 30),
        ("PRD013", "Zara Maxi Dress", "Fashion", "Zara", 4990.00, 1800.00, True, 30),
        ("PRD014", "Nike Air Max 270", "Fashion", "Nike", 9995.00, 4500.00, True, 30),
        ("PRD015", "Biba Ethnic Kurta Set", "Fashion", "Biba", 2799.00, 900.00, True, 30),
        ("PRD016", "H&M Casual Tshirt", "Fashion", "H&M", 799.00, 250.00, True, 30),
        ("PRD017", "Van Heusen Blazer", "Fashion", "Van Heusen", 7499.00, 2800.00, True, 30),
        ("PRD018", "Adidas Ultraboost 22", "Fashion", "Adidas", 12999.00, 6000.00, True, 30),
        ("PRD019", "Fabindia Cotton Saree", "Fashion", "Fabindia", 3200.00, 1100.00, True, 30),
        ("PRD020", "Puma Sports Jacket", "Fashion", "Puma", 4499.00, 1600.00, True, 30),
        # Home & Kitchen (10 products)
        ("PRD021", "Prestige Induction Cooktop", "Home & Kitchen", "Prestige", 2799.00, 1100.00, True, 30),
        ("PRD022", "Bajaj Mixer Grinder 750W", "Home & Kitchen", "Bajaj", 2199.00, 850.00, True, 30),
        ("PRD023", "Philips Air Fryer HD9252", "Home & Kitchen", "Philips", 7995.00, 4000.00, True, 30),
        ("PRD024", "Borosil Vision Glass Set", "Home & Kitchen", "Borosil", 899.00, 300.00, True, 30),
        ("PRD025", "Cello Non-Stick Tawa", "Home & Kitchen", "Cello", 649.00, 220.00, True, 30),
        ("PRD026", "Milton Thermosteel Flask", "Home & Kitchen", "Milton", 1299.00, 420.00, True, 30),
        ("PRD027", "IKEA KALLAX Shelf Unit", "Home & Kitchen", "IKEA", 8990.00, 4500.00, False, 0),
        ("PRD028", "Nilkamal Plastic Chair", "Home & Kitchen", "Nilkamal", 1599.00, 550.00, False, 0),
        ("PRD029", "Pigeon Hot Pot Set", "Home & Kitchen", "Pigeon", 1199.00, 380.00, True, 30),
        ("PRD030", "Tupperware Dry Storage", "Home & Kitchen", "Tupperware", 2499.00, 900.00, True, 30),
        # Beauty (10 products)
        ("PRD031", "Lakme Absolute Foundation", "Beauty", "Lakme", 699.00, 200.00, False, 0),
        ("PRD032", "Maybelline Fit Me Concealer", "Beauty", "Maybelline", 549.00, 180.00, False, 0),
        ("PRD033", "L'Oreal Revitalift Serum", "Beauty", "L'Oreal", 1299.00, 450.00, False, 0),
        ("PRD034", "Himalaya Face Wash Neem", "Beauty", "Himalaya", 219.00, 60.00, True, 15),
        ("PRD035", "Biotique Bio Sunscreen SPF30", "Beauty", "Biotique", 349.00, 100.00, False, 0),
        ("PRD036", "Mamaearth Onion Shampoo", "Beauty", "Mamaearth", 399.00, 120.00, False, 0),
        ("PRD037", "Garnier Micellar Cleansing Water", "Beauty", "Garnier", 299.00, 90.00, False, 0),
        ("PRD038", "Nykaa Cosmetics Lipstick", "Beauty", "Nykaa", 449.00, 130.00, False, 0),
        ("PRD039", "Pond's Moisturising Cold Cream", "Beauty", "Pond's", 279.00, 80.00, False, 0),
        ("PRD040", "Dove Body Lotion 400ml", "Beauty", "Dove", 369.00, 110.00, False, 0),
        # Sports (10 products)
        ("PRD041", "Decathlon Domyos Yoga Mat", "Sports", "Decathlon", 999.00, 300.00, True, 30),
        ("PRD042", "Nike Dri-FIT Training Shorts", "Sports", "Nike", 2499.00, 850.00, True, 30),
        ("PRD043", "Yonex ZR100 Badminton Racket", "Sports", "Yonex", 1299.00, 420.00, True, 30),
        ("PRD044", "SG Cricket Bat English Willow", "Sports", "SG", 4999.00, 1800.00, True, 30),
        ("PRD045", "Nivia Football Size 5", "Sports", "Nivia", 899.00, 270.00, True, 30),
        ("PRD046", "Cosco Tennis Racket", "Sports", "Cosco", 1499.00, 500.00, True, 30),
        ("PRD047", "Adidas Running Shoes", "Sports", "Adidas", 6999.00, 2800.00, True, 30),
        ("PRD048", "Reebok Gym Gloves", "Sports", "Reebok", 799.00, 250.00, True, 30),
        ("PRD049", "Vector X Boxing Gloves", "Sports", "Vector X", 1999.00, 650.00, True, 30),
        ("PRD050", "Decathlon Kettlebell 16kg", "Sports", "Decathlon", 3499.00, 1100.00, True, 30),
    ]

    # Assign category SKs — use sub-categories where available, else root
    def get_cat_sk(l1name):
        subcats = l1_subcats.get(l1name, [])
        chosen = random.choice(subcats) if subcats else l1_to_cat_id.get(l1name, "CAT001")
        return cat_map.get(chosen, 1)

    merchant_sks = list(merchant_sk_map.values())
    rows = []
    for p_id, p_name, cat_l1, brand, mrp, cost, returnable, return_days in products:
        cat_sk_val = get_cat_sk(cat_l1)
        gst = 18.0 if cat_l1 == "Electronics" else (12.0 if cat_l1 == "Fashion" else 5.0)
        rows.append((
            p_id, None, 1, True,
            p_name, None,
            cat_sk_val, cat_l1, None, None,
            brand, random.choice(merchant_sks),
            mrp, cost,
            random.randint(100, 2000), "20x10x5",
            returnable, return_days,
            None, gst,
            now_str, now_str, now_str,
        ))
    cols = [
        "PRODUCT_ID","MARKETPLACE_ID","PRODUCT_VERSION","IS_ACTIVE",
        "PRODUCT_NAME","PRODUCT_DESCRIPTION",
        "CATEGORY_SK","CATEGORY_L1","CATEGORY_L2","CATEGORY_L3",
        "BRAND","MERCHANT_SK",
        "MRP","COST_PRICE",
        "WEIGHT_GRAMS","DIMENSIONS_CM",
        "IS_RETURNABLE","RETURN_WINDOW_DAYS",
        "HSN_CODE","GST_RATE_PCT",
        "CREATED_WHEN","UPDATED_WHEN","INGESTED_AT",
    ]
    batch_insert(cur, "DIM_PRODUCT", cols, rows)
    cur.execute("SELECT PRODUCT_SK, PRODUCT_ID, CATEGORY_L1, MRP FROM ECOMM_DATA_LAKE.CONFORMED.DIM_PRODUCT")
    prod_data = {}
    for row in cur.fetchall():
        prod_data[row[1]] = {"sk": row[0], "cat_l1": row[2], "mrp": float(row[3])}
    return prod_data

# ── 8. FACT_ORDER + FACT_ORDER_ITEM ──────────────────────────────────────────
def seed_orders_and_items(cur, customer_sk_map, channel_sk_map, merchant_sk_map, prod_data):
    print("Seeding FACT_ORDER (2000 orders) + FACT_ORDER_ITEM (~4000 items)...")
    now_str = ts(NOW_IST)
    random.seed(1234)

    start_date = date(2026, 1, 1)
    end_date = date(2026, 6, 30)  # H1 2026

    cust_ids = list(customer_sk_map.keys())
    channel_sks = list(channel_sk_map.values())
    merchant_sks = list(merchant_sk_map.values())
    prod_ids = list(prod_data.keys())
    statuses = ["DELIVERED", "DELIVERED", "DELIVERED", "DELIVERED",
                "CANCELLED", "RETURNED"]

    order_rows = []
    item_rows = []
    order_meta = []  # for refunds: (order_sk_seq, customer_sk, items)

    order_sk = 1
    item_sk = 1

    for i in range(2000):
        cust_id = random.choice(cust_ids)
        cust_sk = customer_sk_map[cust_id]
        channel_sk = random.choice(channel_sks)
        merchant_sk = random.choice(merchant_sks)
        order_date = rand_date(start_date, end_date)
        o_date_sk = date_sk(order_date)
        hour = random.randint(8, 23)
        minute = random.randint(0, 59)
        placed_at_naive = datetime(order_date.year, order_date.month, order_date.day, hour, minute)
        placed_at = ts(IST.localize(placed_at_naive))
        status = random.choice(statuses)
        is_first = random.random() < 0.15
        has_discount = random.random() < 0.30  # 30% of orders have discounts

        # Pick 1-3 products for this order
        n_items = random.choices([1, 2, 3], weights=[50, 35, 15])[0]
        chosen_prods = random.sample(prod_ids, min(n_items, len(prod_ids)))

        gmv = 0.0
        total_discount = 0.0
        total_tax = 0.0
        order_items_meta = []

        for j, p_id in enumerate(chosen_prods):
            pd = prod_data[p_id]
            qty = random.randint(1, 3)
            unit_mrp = pd["mrp"]
            # selling price: 5-20% below MRP
            discount_pct = random.uniform(0.05, 0.25) if has_discount else random.uniform(0.0, 0.05)
            unit_sell = round(unit_mrp * (1 - discount_pct), 2)
            item_discount = round((unit_mrp - unit_sell) * qty, 2)
            gst_rate = 0.18 if pd["cat_l1"] == "Electronics" else (0.12 if pd["cat_l1"] == "Fashion" else 0.05)
            item_tax = round(unit_sell * qty * gst_rate, 2)
            item_net = round(unit_sell * qty + item_tax, 2)

            # Derive category_sk from product's category
            cat_sk_for_item = 1  # default

            gmv += unit_mrp * qty
            total_discount += item_discount
            total_tax += item_tax

            item_id = f"OI{str(item_sk).zfill(8)}"
            item_status = status
            item_rows.append((
                item_id,
                order_sk,    # we use sequential integer as placeholder
                pd["sk"],
                cat_sk_for_item,
                merchant_sk,
                None,  # warehouse_sk
                None,  # promotion_sk
                o_date_sk, placed_at,
                qty, unit_mrp, unit_sell,
                item_discount, item_tax, item_net,
                item_status, now_str,
            ))
            order_items_meta.append({
                "item_sk_seq": item_sk,
                "item_id": item_id,
                "product_sk": pd["sk"],
                "item_net": item_net,
                "item_discount": item_discount,
            })
            item_sk += 1

        gmv = round(gmv, 2)
        total_discount = round(total_discount, 2) if has_discount else 0.0
        total_tax = round(total_tax, 2)
        shipping = round(random.uniform(0, 99), 2) if gmv < 500 else 0.0
        net_revenue = round(gmv - total_discount + shipping + total_tax, 2)

        order_id = f"ORD{str(order_sk).zfill(8)}"
        order_rows.append((
            order_id,
            cust_sk, channel_sk, merchant_sk,
            None,  # payment_method_sk
            None,  # delivery_location_sk
            o_date_sk, placed_at,
            status, len(chosen_prods),
            gmv, total_discount, shipping, total_tax, net_revenue,
            "INR", is_first, has_discount, now_str,
        ))
        order_meta.append({
            "order_sk_seq": order_sk,
            "order_id": order_id,
            "cust_sk": cust_sk,
            "status": status,
            "order_date": order_date,
            "items": order_items_meta,
            "net_revenue": net_revenue,
        })
        order_sk += 1

    # Insert FACT_ORDER
    order_cols = [
        "ORDER_ID",
        "CUSTOMER_SK","CHANNEL_SK","MERCHANT_SK",
        "PAYMENT_METHOD_SK","DELIVERY_LOCATION_SK",
        "ORDER_DATE_SK","ORDER_PLACED_AT",
        "ORDER_STATUS","ITEM_COUNT",
        "GMV_AMOUNT","TOTAL_DISCOUNT_AMOUNT","SHIPPING_CHARGE","TAX_AMOUNT","NET_REVENUE",
        "CURRENCY_CODE","IS_FIRST_ORDER","HAS_DISCOUNT","INGESTED_AT",
    ]
    batch_insert(cur, "FACT_ORDER", order_cols, order_rows)

    # Fetch back order SKs (assigned by sequence)
    cur.execute("SELECT ORDER_SK, ORDER_ID FROM ECOMM_DATA_LAKE.CONFORMED.FACT_ORDER ORDER BY ORDER_SK")
    order_sk_map = {row[1]: row[0] for row in cur.fetchall()}

    # Fix item_rows — replace sequential placeholder with real order_sk
    # item_rows structure: item_id, order_sk_placeholder(seq), product_sk, ...
    # We stored order_sk as 1-based integer matching order_meta index
    fixed_item_rows = []
    item_idx = 0
    for om in order_meta:
        real_order_sk = order_sk_map[om["order_id"]]
        for item_meta in om["items"]:
            row = list(item_rows[item_idx])
            row[1] = real_order_sk  # fix order_sk
            fixed_item_rows.append(tuple(row))
            item_idx += 1

    item_cols = [
        "ORDER_ITEM_ID",
        "ORDER_SK","PRODUCT_SK","CATEGORY_SK","MERCHANT_SK",
        "WAREHOUSE_SK","PROMOTION_SK",
        "ORDER_DATE_SK","ORDER_PLACED_AT",
        "QUANTITY","UNIT_MRP","UNIT_SELLING_PRICE",
        "ITEM_DISCOUNT_AMOUNT","ITEM_TAX_AMOUNT","ITEM_NET_AMOUNT",
        "ITEM_STATUS","INGESTED_AT",
    ]
    batch_insert(cur, "FACT_ORDER_ITEM", item_cols, fixed_item_rows)

    # Fetch item SKs for refunds
    cur.execute("SELECT ORDER_ITEM_SK, ORDER_ITEM_ID FROM ECOMM_DATA_LAKE.CONFORMED.FACT_ORDER_ITEM")
    item_sk_map = {row[1]: row[0] for row in cur.fetchall()}

    return order_sk_map, item_sk_map, order_meta

# ── 9. FACT_REFUND ─────────────────────────────────────────────────────────────
def seed_refunds(cur, order_sk_map, item_sk_map, order_meta):
    print("Seeding FACT_REFUND (~200 refunds)...")
    now_str = ts(NOW_IST)
    random.seed(9999)

    refund_reasons = [
        "WRONG_ITEM", "DAMAGED_PRODUCT", "QUALITY_ISSUE",
        "NOT_AS_DESCRIBED", "CHANGE_OF_MIND", "LATE_DELIVERY",
        "MISSING_PARTS", "DEFECTIVE",
    ]
    refund_modes = ["ORIGINAL_PAYMENT", "WALLET", "BANK_TRANSFER"]
    refund_statuses = ["COMPLETED", "COMPLETED", "COMPLETED", "PENDING"]

    # Pick ~10% of DELIVERED/RETURNED orders for refunds
    eligible = [om for om in order_meta if om["status"] in ("RETURNED", "DELIVERED")]
    refund_candidates = random.sample(eligible, min(200, len(eligible)))

    rows = []
    for i, om in enumerate(refund_candidates, 1):
        real_order_sk = order_sk_map[om["order_id"]]
        order_date = om["order_date"]
        refund_date = order_date + timedelta(days=random.randint(3, 20))

        # Pick one item to refund
        if not om["items"]:
            continue
        item_meta = random.choice(om["items"])
        real_item_sk = item_sk_map.get(item_meta["item_id"])
        refund_amount = round(item_meta["item_net"] * random.uniform(0.8, 1.0), 2)
        r_date_sk = date_sk(refund_date)
        initiated = IST.localize(datetime(refund_date.year, refund_date.month, refund_date.day,
                                          random.randint(8, 20), random.randint(0, 59)))
        completed_dt = initiated + timedelta(days=random.randint(1, 5))
        r_status = random.choice(refund_statuses)
        rows.append((
            f"REF{str(i).zfill(8)}",
            real_order_sk,
            real_item_sk,
            om["cust_sk"],
            item_meta["product_sk"],
            r_date_sk,
            random.choice(refund_reasons),
            random.choice(refund_modes),
            r_status,
            refund_amount,
            ts(initiated),
            ts(completed_dt) if r_status == "COMPLETED" else None,
            now_str,
        ))

    cols = [
        "REFUND_ID",
        "ORDER_SK","ORDER_ITEM_SK","CUSTOMER_SK","PRODUCT_SK",
        "REFUND_DATE_SK",
        "REFUND_REASON_CODE","REFUND_MODE","REFUND_STATUS","REFUND_AMOUNT",
        "REFUND_INITIATED_AT","REFUND_COMPLETED_AT","INGESTED_AT",
    ]
    batch_insert(cur, "FACT_REFUND", cols, rows)

# ── 10. FACT_INVENTORY_SNAPSHOT ───────────────────────────────────────────────
def seed_inventory(cur, prod_data, location_sk_map):
    print("Seeding FACT_INVENTORY_SNAPSHOT (last 30 days × 50 products × 5 locations)...")
    now_str = ts(NOW_IST)
    random.seed(7777)

    today = date(2026, 4, 19)
    snapshot_days = [today - timedelta(days=d) for d in range(29, -1, -1)]
    loc_ids = list(location_sk_map.keys())[:5]  # use first 5 locations (FCs)
    prod_ids = list(prod_data.keys())

    rows = []
    inv_sk = 1
    for snap_date in snapshot_days:
        s_date_sk = date_sk(snap_date)
        for p_id in prod_ids:
            pd = prod_data[p_id]
            for loc_id in loc_ids:
                loc_sk = location_sk_map[loc_id]
                # Electronics get higher stock to highlight the inventory bug
                if pd["cat_l1"] == "Electronics":
                    qty_avail = random.randint(50, 300)
                    qty_reserved = random.randint(5, 30)
                    qty_transit = random.randint(0, 20)
                else:
                    qty_avail = random.randint(10, 150)
                    qty_reserved = random.randint(0, 20)
                    qty_transit = random.randint(0, 10)
                qty_damaged = random.randint(0, 3)
                qty_total = qty_avail + qty_reserved + qty_transit + qty_damaged
                days_supply = round(qty_avail / max(random.uniform(0.5, 5.0), 0.1), 2)
                inv_value = round(qty_total * pd["mrp"], 2)
                reorder = qty_avail < 20

                rows.append((
                    pd["sk"], loc_sk,
                    s_date_sk, snap_date.isoformat(),
                    qty_avail, qty_reserved, qty_transit, qty_damaged, qty_total,
                    days_supply, inv_value, reorder, now_str,
                ))
                inv_sk += 1

    cols = [
        "PRODUCT_SK","LOCATION_SK",
        "SNAPSHOT_DATE_SK","SNAPSHOT_DATE",
        "QTY_AVAILABLE","QTY_RESERVED","QTY_IN_TRANSIT","QTY_DAMAGED","QTY_TOTAL",
        "DAYS_OF_SUPPLY","INVENTORY_VALUE","REORDER_FLAG","INGESTED_AT",
    ]
    # Inventory is large (~7500 rows), use batch_insert which handles batching
    batch_insert(cur, "FACT_INVENTORY_SNAPSHOT", cols, rows)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Connecting to Snowflake ECOMM_DATA_LAKE.CONFORMED...")
    conn = snowflake.connector.connect(**CONN_PARAMS)
    cur = conn.cursor()
    print("Connected.\n")

    try:
        # 1. DIM_DATE
        seed_dim_date(cur)
        conn.commit()

        # 2. DIM_CATEGORY (returns cat_map and cat_defs)
        cat_map, cat_defs = seed_dim_category(cur)
        conn.commit()

        # 3. DIM_MERCHANT
        merchant_sk_map = seed_dim_merchant(cur)
        conn.commit()

        # 4. DIM_CHANNEL
        channel_sk_map = seed_dim_channel(cur)
        conn.commit()

        # 5. DIM_LOCATION
        location_sk_map = seed_dim_location(cur)
        conn.commit()

        # 6. DIM_CUSTOMER
        customer_sk_map = seed_dim_customer(cur)
        conn.commit()

        # 7. DIM_PRODUCT
        prod_data = seed_dim_product(cur, cat_map, cat_defs, merchant_sk_map)
        conn.commit()

        # 8. FACT_ORDER + FACT_ORDER_ITEM
        order_sk_map, item_sk_map, order_meta = seed_orders_and_items(
            cur, customer_sk_map, channel_sk_map, merchant_sk_map, prod_data
        )
        conn.commit()

        # 9. FACT_REFUND
        seed_refunds(cur, order_sk_map, item_sk_map, order_meta)
        conn.commit()

        # 10. FACT_INVENTORY_SNAPSHOT
        seed_inventory(cur, prod_data, location_sk_map)
        conn.commit()

        # ── Verification ──
        print("\n" + "=" * 60)
        print("Row count verification:")
        for tbl in [
            "DIM_DATE", "DIM_CATEGORY", "DIM_MERCHANT", "DIM_CHANNEL",
            "DIM_LOCATION", "DIM_CUSTOMER", "DIM_PRODUCT",
            "FACT_ORDER", "FACT_ORDER_ITEM", "FACT_REFUND", "FACT_INVENTORY_SNAPSHOT",
        ]:
            cur.execute(f"SELECT COUNT(*) FROM ECOMM_DATA_LAKE.CONFORMED.{tbl}")
            cnt = cur.fetchone()[0]
            print(f"  {tbl:<35} {cnt:>6} rows")

        print("\nSeed complete!")

    except Exception as e:
        print(f"\nERROR: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
