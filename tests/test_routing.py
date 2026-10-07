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
        ("hello there", "general"),
        ("", "general"),
    ],
)
def test_classify_intent(message, intent):
    assert classify_intent(message) == intent
