import pytest

from app.routing import classify_intent


@pytest.mark.parametrize(
    "message,intent",
    [
        ("What were total sales last month?", "sales"),
        ("profit on orders this quarter", "profit"),
        ("wholesale pricing please", "general"),  # "who" must not match inside "wholesale"
        ("costume stock levels", "inventory"),
        ("who is customer #42", "customer"),
        ("top customers by revenue", "customer"),
        ("which customers placed the most orders", "customer"),
        ("Who are our best-selling brands?", "sales"),
        ("Can you lookup inventory for SKU 123?", "inventory"),
        ("Hi, who are you?", "general"),
        ("Give me the contact number for warehouse Pune", "inventory"),
        ("Which products are running low and need reorder?", "inventory"),
        ("contact details for Priya Sharma", "customer"),
        ("Rahul Verma's phone number", "customer"),
        ("What's the shipping cost to Mumbai?", "general"),
        ("How do I request a refund?", "general"),
        ("Is customer support available on weekends?", "general"),
        ("How do I see revenue for last week?", "sales"),
        ("How can I find our top customers?", "customer"),
        ("What was total shipping cost last month?", "profit"),
        ("Where is my order?", "general"),
        ("What's the status of my order 1234?", "general"),
        ("I want to cancel my order", "general"),
        ("Can I get a refund for a damaged item?", "general"),
        ("Which SKUs need restocking?", "inventory"),
        ("hello there", "general"),
        ("", "general"),
    ],
)
def test_classify_intent(message, intent):
    assert classify_intent(message) == intent
