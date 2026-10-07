from app.graph import last_user_message, strip_reasoning


def test_strip_reasoning_removes_think_block():
    assert strip_reasoning("<think>\nplanning...\n</think>\nTotal sales were ₹1,000.") == "Total sales were ₹1,000."


def test_strip_reasoning_keeps_plain_text():
    assert strip_reasoning("Plain answer") == "Plain answer"


def test_last_user_message_skips_trailing_assistant_turn():
    msgs = [{"role": "user", "content": "sales?"}, {"role": "assistant", "content": "..."}]
    assert last_user_message(msgs) == "sales?"


def test_last_user_message_handles_multipart_content():
    msgs = [{"role": "user", "content": [{"type": "text", "text": "stock levels"}]}]
    assert last_user_message(msgs) == "stock levels"


def test_strip_reasoning_drops_unclosed_think_block():
    assert strip_reasoning("<think>cut off mid reasoning") == ""


def test_history_excludes_current_question_and_keeps_trailing_assistant():
    from app.graph import _last_user_index
    msgs = [{"role": "user", "content": "a"}, {"role": "assistant", "content": "b"},
            {"role": "user", "content": "c"}, {"role": "assistant", "content": "d"}]
    assert _last_user_index(msgs) == 2
