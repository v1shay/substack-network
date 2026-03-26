import unittest

from scripts.get_recommendations import recommendation_records, redact_sensitive_fields


class TestGetRecommendations(unittest.TestCase):
    def test_recommendation_records_use_best_effort_ui_guess(self):
        records = recommendation_records(
            [
                {
                    "recommendedPublication": {
                        "is_personal_mode": True,
                        "subdomain": "writer",
                    }
                },
                {
                    "recommendedPublication": {
                        "is_personal_mode": False,
                        "custom_domain": "news.example",
                    }
                },
                {
                    "recommendedPublication": {
                        "subdomain": "mystery",
                    }
                },
            ]
        )

        self.assertEqual("person", records[0]["ui_guess"])
        self.assertIn("best-effort", records[0]["provenance"])
        self.assertEqual("https://writer.substack.com", records[0]["url"])

        self.assertEqual("publication", records[1]["ui_guess"])
        self.assertEqual("https://news.example", records[1]["url"])

        self.assertEqual("unknown", records[2]["ui_guess"])
        self.assertEqual("https://mystery.substack.com", records[2]["url"])

    def test_recommendation_records_force_people_when_requested(self):
        records = recommendation_records(
            [
                {
                    "recommendedPublication": {
                        "is_personal_mode": False,
                        "subdomain": "writer",
                    }
                }
            ],
            force_people=True,
        )
        self.assertEqual("person", records[0]["ui_guess"])
        self.assertEqual("forced via --as-people", records[0]["provenance"])

    def test_redact_sensitive_fields_masks_tokens_and_auth(self):
        payload = {
            "subscribe_auth_token": "secret-value",
            "nested": {
                "apiToken": "abc123",
                "safe": "ok",
            },
            "items": [
                {"secret_key": "hidden"},
                {"title": "visible"},
            ],
        }
        redacted = redact_sensitive_fields(payload)
        self.assertEqual("[REDACTED]", redacted["subscribe_auth_token"])
        self.assertEqual("[REDACTED]", redacted["nested"]["apiToken"])
        self.assertEqual("ok", redacted["nested"]["safe"])
        self.assertEqual("[REDACTED]", redacted["items"][0]["secret_key"])
        self.assertEqual("visible", redacted["items"][1]["title"])


if __name__ == "__main__":
    unittest.main()
