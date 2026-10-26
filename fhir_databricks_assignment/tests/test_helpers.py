import hashlib
import json
import unittest

# These tests are intentionally independent of Spark so they can run locally.
def canonical_json(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

class TestCanonicalJson(unittest.TestCase):
    def test_key_order_does_not_change_hash(self):
        a = {"resourceType": "Patient", "id": "p1", "active": True}
        b = {"active": True, "id": "p1", "resourceType": "Patient"}
        self.assertEqual(
            hashlib.sha256(canonical_json(a).encode()).hexdigest(),
            hashlib.sha256(canonical_json(b).encode()).hexdigest()
        )

    def test_content_change_changes_hash(self):
        a = {"resourceType": "Patient", "id": "p1", "active": True}
        b = {"resourceType": "Patient", "id": "p1", "active": False}
        self.assertNotEqual(
            hashlib.sha256(canonical_json(a).encode()).hexdigest(),
            hashlib.sha256(canonical_json(b).encode()).hexdigest()
        )

if __name__ == "__main__":
    unittest.main()
