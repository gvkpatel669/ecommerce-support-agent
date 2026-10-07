from langchain_core.tools import tool

from app.snowflake_client import query
from app.tools._text import escape_like, has_word, tokens


@tool
def lookup_customer(question: str) -> str:
    """Look up customer details including contact info and order history.
    Use this for questions about specific customers or customer data."""
    q = question.lower()

    import re

    # An explicitly marked ID (#42, id 42, customer 42) always wins; only then do
    # "top/best/most" questions fall through to the ranking query ("top 5 customers").
    id_match = re.search(r'(?:#|\bid\s*#?|\bcustomer\s*#?)\s*(\d{1,6})\b', q)
    wants_top = id_match is None and has_word(q, "top", "best", "most", "highest")

    if id_match:
        cid = id_match.group(1)
        rows = query("""
            SELECT customer_sk, customer_id, full_name, email, phone_number,
                   date_of_birth, gender, city, state_code, pincode,
                   loyalty_tier, loyalty_points, customer_segment, first_order_date
            FROM CONFORMED.DIM_CUSTOMER
            WHERE customer_id = %s AND is_active = TRUE
        """, (cid,))
        if not rows:
            rows = query("""
                SELECT customer_sk, customer_id, full_name, email, phone_number,
                       date_of_birth, gender, city, state_code, pincode,
                       loyalty_tier, loyalty_points, customer_segment, first_order_date
                FROM CONFORMED.DIM_CUSTOMER
                WHERE customer_sk = %s AND is_active = TRUE
            """, (int(cid),))
        if not rows:
            return f"No customer found with ID {cid}."

        c = rows[0]
        result = (f"Customer #{c.get('CUSTOMER_ID', c.get('CUSTOMER_SK', '?'))}:\n"
                  f"  Name: {c.get('FULL_NAME') or 'N/A'}\n"
                  f"  Email: {c.get('EMAIL') or 'N/A'}\n"
                  f"  Phone: {c.get('PHONE_NUMBER') or 'N/A'}\n"
                  f"  DOB: {c.get('DATE_OF_BIRTH') or 'N/A'}\n"
                  f"  Location: {c.get('CITY') or ''}, {c.get('STATE_CODE') or ''} {c.get('PINCODE') or ''}\n"
                  f"  Loyalty: {c.get('LOYALTY_TIER') or 'N/A'} ({(c.get('LOYALTY_POINTS') or 0):,.0f} pts)\n"
                  f"  Segment: {c.get('CUSTOMER_SEGMENT') or 'N/A'}\n")

        # Order history
        orders = query("""
            SELECT order_id, order_placed_at::DATE AS order_date,
                   gmv_amount, order_status
            FROM CONFORMED.FACT_ORDER
            WHERE customer_sk = %s
            ORDER BY order_placed_at DESC
            LIMIT 5
        """, (c.get('CUSTOMER_SK'),))
        if orders:
            result += "\n  Recent Orders:\n"
            for o in orders:
                result += f"    Order {o['ORDER_ID']} ({o['ORDER_DATE']}): ₹{(o.get('GMV_AMOUNT') or 0):,.2f} [{o.get('ORDER_STATUS') or 'N/A'}]\n"
        return result

    if wants_top:
        rows = query("""
            SELECT c.customer_sk, c.customer_id, c.full_name, c.email, c.phone_number,
                   COUNT(o.order_sk) AS order_count,
                   SUM(o.gmv_amount) AS total_spent
            FROM CONFORMED.DIM_CUSTOMER c
            JOIN CONFORMED.FACT_ORDER o ON c.customer_sk = o.customer_sk
            WHERE c.is_active = TRUE AND o.order_status != 'CANCELLED'
            GROUP BY c.customer_sk, c.customer_id, c.full_name, c.email, c.phone_number
            ORDER BY total_spent DESC
            LIMIT 5
        """)
        lines = ["Top 5 Customers by Spending:"]
        for r in rows:
            lines.append(
                f"  #{r['CUSTOMER_ID']} {r['FULL_NAME']} "
                f"(email: {r['EMAIL']}, phone: {r['PHONE_NUMBER']}): "
                f"{r['ORDER_COUNT']} orders, ₹{(r['TOTAL_SPENT'] or 0):,.2f}"
            )
        return "\n".join(lines)

    # Search by name using parameterized query
    stopwords = {
        "customer", "customers", "buyer", "buyers", "shopper", "account", "info", "details", "contact",
        "the", "for", "get", "find", "look", "lookup", "who", "what", "show", "list", "about", "tell",
        "please", "orders", "order", "profile", "named", "called", "give", "with", "and", "his", "her",
    }
    name_words = [w for w in tokens(q) if len(w) > 2 and w not in stopwords]

    if name_words:
        name = name_words[0]
        # Escape LIKE wildcards so "%" or "_" cannot match every row.
        # '!' is the escape char: a backslash literal is itself an escape in Snowflake strings.
        like = "%" + escape_like(name) + "%"
        rows = query("""
            SELECT customer_sk, customer_id, full_name, email, phone_number
            FROM CONFORMED.DIM_CUSTOMER
            WHERE (LOWER(full_name) LIKE %s ESCAPE '!' OR customer_id LIKE %s ESCAPE '!')
              AND is_active = TRUE
            LIMIT 10
        """, (like, like))
        if rows:
            lines = [f"Customers matching '{name}':"]
            for r in rows:
                lines.append(f"  #{r['CUSTOMER_ID']} {r['FULL_NAME']} — {r['EMAIL']}, {r['PHONE_NUMBER']}")
            return "\n".join(lines)

    return "Please specify a customer ID (e.g., #42), name, or ask for 'top customers'."
