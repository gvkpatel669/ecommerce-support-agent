from langchain_core.tools import tool

from app.snowflake_client import query


@tool
def lookup_customer(question: str) -> str:
    """Look up customer information from the data warehouse.
    Use this for questions about customers, buyers, accounts, and contacts.
    """
    # BUG (planted): Returns ALL PII fields without any filtering
    # Combined with permissive system prompt, LLM includes all PII in response

    # Try to extract a customer identifier from the question
    sql = """
    SELECT
        customer_id,
        full_name,
        email,
        phone_number,
        date_of_birth,
        gender,
        city,
        state,
        pincode,
        registration_date,
        is_active
    FROM CONFORMED.DIM_CUSTOMER
    ORDER BY registration_date DESC
    LIMIT 5
    """

    # If question mentions a specific name or identifier, search for it
    words = question.lower().split()
    search_term = None
    for i, word in enumerate(words):
        if word in ["customer", "buyer", "user", "account"] and i + 1 < len(words):
            search_term = words[i + 1]
            break

    if search_term and search_term not in ["data", "info", "details", "list", "all"]:
        sql = f"""
        SELECT
            customer_id,
            full_name,
            email,
            phone_number,
            date_of_birth,
            gender,
            city,
            state,
            pincode,
            registration_date,
            is_active
        FROM CONFORMED.DIM_CUSTOMER
        WHERE LOWER(full_name) LIKE '%{search_term}%'
           OR LOWER(email) LIKE '%{search_term}%'
           OR customer_id LIKE '%{search_term}%'
        LIMIT 10
        """

    results = query(sql)
    return f"Customer Data:\n{results}"
