from datetime import datetime, timedelta, timezone

from cdp_engine.rules import account_properties, eval_rule, referenced_properties, rule_to_sql, trigger_properties


def test_eval_rule_and_refs():
    rule = {
        "op": "and",
        "rules": [
            {"op": "eq", "property": "acct_tier", "value": "enterprise"},
            {"op": "gte", "property": "prof_visits_30d", "value": 3},
        ],
    }
    assert referenced_properties(rule) == {"acct_tier", "prof_visits_30d"}
    assert eval_rule(rule, {"acct_tier": "enterprise", "prof_visits_30d": 4})
    assert not eval_rule(rule, {"acct_tier": "smb", "prof_visits_30d": 4})


def test_rule_to_sql():
    rule = {"op": "in", "property": "acct_region", "value": ["na", "emea"]}
    assert rule_to_sql(rule) == "`acct_region` IN ('na', 'emea')"


def test_customer_nested_rule_operators():
    recent = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    rule = {
        "op": "and",
        "rules": [
            {
                "op": "or",
                "rules": [
                    {"op": "contains", "property": "DL_C_PageCountryCode", "value": "us"},
                    {"op": "contains", "property": "ForceQualifyTestUser", "value": "segment"},
                ],
            },
            {"op": "exists", "property": "AZ_C_EmailAddress"},
            {"op": "not_contains", "property": "AZ_C_MultipleAccountIndicator", "value": "1"},
            {"op": "between", "property": "Sample_ID", "value": [21, 100]},
            {"op": "within_last", "property": "DL_C_LastShippingCompleted", "value": "1 days (24 hours)"},
            {"op": "gte", "property": "AZ_A_NetRev13Week", "value": 375, "source": "ACCOUNTS"},
        ],
    }

    attrs = {
        "AZ_C_EmailAddress": "person@example.com",
        "AZ_C_MultipleAccountIndicator": "0",
        "Sample_ID": 50,
        "AZ_A_NetRev13Week": 500,
    }
    event = {
        "DL_C_PageCountryCode": "US",
        "DL_C_LastShippingCompleted": recent,
    }

    assert eval_rule(rule, attrs, event)
    assert account_properties(rule) == {"AZ_A_NetRev13Week"}
    assert trigger_properties(rule) == {
        "AZ_C_EmailAddress",
        "AZ_C_MultipleAccountIndicator",
        "DL_C_LastShippingCompleted",
        "DL_C_PageCountryCode",
        "ForceQualifyTestUser",
        "Sample_ID",
    }
