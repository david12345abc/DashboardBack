from types import SimpleNamespace
from unittest.mock import Mock, patch

import requests
from django.test import SimpleTestCase, override_settings


@override_settings(
    LM_STUDIO_BASE_URL="http://lm-studio.test/v1",
    LM_STUDIO_MODEL="qwen3-vl-8b-thinking",
    LM_STUDIO_API_KEY="test-key",
    LM_STUDIO_TIMEOUT_SECONDS=15,
    LM_STUDIO_AUTO_LOAD_MODEL=False,
    LM_STUDIO_CONTEXT_LENGTH=32768,
)
class LmStudioProxyTests(SimpleTestCase):
    def _user(self):
        return SimpleNamespace(id=1, nickname="tester")

    @patch("lmstudio_proxy.services.requests.request")
    def test_health_does_not_require_authentication(self, request_mock):
        upstream = Mock(status_code=200)
        upstream.json.return_value = {"data": []}
        request_mock.return_value = upstream
        response = self.client.get("/api/lmstudio/health/")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])

    @patch("User.views._get_current_user")
    @patch("lmstudio_proxy.services.requests.request")
    def test_chat_forces_configured_model(self, request_mock, user_mock):
        user_mock.return_value = self._user()
        upstream = Mock(status_code=200)
        upstream.json.return_value = {
            "choices": [{"message": {"role": "assistant", "content": "Готово"}}]
        }
        request_mock.return_value = upstream

        response = self.client.post(
            "/api/lmstudio/v1/chat/completions/",
            data={
                "model": "another-model",
                "messages": [{"role": "user", "content": "Привет"}],
                "temperature": 0.2,
            },
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer test-token",
        )

        self.assertEqual(response.status_code, 200)
        kwargs = request_mock.call_args.kwargs
        self.assertEqual(kwargs["json"]["model"], "qwen3-vl-8b-thinking")
        self.assertEqual(kwargs["json"]["messages"][0]["content"], "Привет")
        self.assertFalse(kwargs["json"]["stream"])
        self.assertEqual(kwargs["timeout"], 15)

    @patch("User.views._get_current_user")
    def test_rejects_empty_messages(self, user_mock):
        user_mock.return_value = self._user()
        response = self.client.post(
            "/api/lmstudio/v1/chat/completions/",
            data={"messages": []},
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer test-token",
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("messages", response.json()["error"])

    @patch("User.views._get_current_user")
    @patch("lmstudio_proxy.services.requests.request")
    def test_timeout_returns_gateway_timeout(self, request_mock, user_mock):
        user_mock.return_value = self._user()
        request_mock.side_effect = requests.Timeout()
        response = self.client.get(
            "/api/lmstudio/v1/models/",
            HTTP_AUTHORIZATION="Bearer test-token",
        )
        self.assertEqual(response.status_code, 504)

    @override_settings(LM_STUDIO_AUTO_LOAD_MODEL=True)
    @patch("lmstudio_proxy.services.requests.request")
    def test_chat_loads_target_model_when_needed(self, request_mock):
        models_response = Mock(status_code=200)
        models_response.json.return_value = {
            "data": [
                {
                    "id": "qwen3-vl-8b-thinking",
                    "state": "not-loaded",
                }
            ]
        }
        load_response = Mock(status_code=200)
        load_response.json.return_value = {"status": "loaded"}
        chat_response = Mock(status_code=200)
        chat_response.json.return_value = {"choices": []}
        request_mock.side_effect = [
            models_response,
            load_response,
            chat_response,
        ]

        response = self.client.post(
            "/api/lmstudio/v1/chat/completions/",
            data={"messages": [{"role": "user", "content": "Привет"}]},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(request_mock.call_count, 3)
        self.assertTrue(request_mock.call_args_list[1].args[1].endswith("/api/v1/models/load"))
