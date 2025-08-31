import pytest
import capitalBot


def test_sum_as_string():
    assert capitalBot.sum_as_string(1, 1) == "2"
