from cdp_engine.rules import eval_rule, referenced_properties, rule_to_sql


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
