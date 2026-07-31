from types import SimpleNamespace
from unittest.mock import Mock, patch

import requests
from django.test import SimpleTestCase, override_settings


def _json_response(payload, status_code=200):
    upstream = Mock(status_code=status_code)
    upstream.json.return_value = payload
    return upstream


@override_settings(
    LM_STUDIO_BASE_URL="http://lm-studio.test/v1",
    LM_STUDIO_MODEL="qwen3-vl-8b-thinking",
    LM_STUDIO_EMBEDDING_MODEL="text-embedding-user-bge-m3",
    LM_STUDIO_API_KEY="test-key",
    LM_STUDIO_TIMEOUT_SECONDS=15,
    LM_STUDIO_AUTO_LOAD_MODEL=False,
    LM_STUDIO_CONTEXT_LENGTH=32768,
    LM_STUDIO_EMBEDDING_CONTEXT_LENGTH=8192,
)
class LmStudioProxyTests(SimpleTestCase):
    def _user(self):
        return SimpleNamespace(id=1, nickname="tester")

    @patch("lmstudio_proxy.services.requests.request")
    def test_health_does_not_require_authentication(self, request_mock):
        request_mock.return_value = _json_response({"data": []})
        response = self.client.get("/api/lmstudio/health/")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["embedding_model"], "text-embedding-user-bge-m3")

    @patch("User.views._get_current_user")
    @patch("lmstudio_proxy.services.requests.request")
    def test_chat_forces_configured_model(self, request_mock, user_mock):
        user_mock.return_value = self._user()
        request_mock.side_effect = [
            _json_response(
                {"data": [{"id": "qwen3-vl-8b-thinking", "key": "qwen3-vl-8b-thinking"}]}
            ),
            _json_response(
                {"choices": [{"message": {"role": "assistant", "content": "Готово"}}]}
            ),
        ]

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
        request_mock.side_effect = [
            _json_response(
                {
                    "data": [
                        {
                            "id": "qwen3-vl-8b-thinking",
                            "state": "not-loaded",
                        }
                    ]
                }
            ),
            _json_response({"status": "loaded"}),
            _json_response({"choices": []}),
        ]

        response = self.client.post(
            "/api/lmstudio/v1/chat/completions/",
            data={"messages": [{"role": "user", "content": "Привет"}]},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(request_mock.call_count, 3)
        self.assertTrue(request_mock.call_args_list[1].args[1].endswith("/api/v1/models/load"))

    @patch("User.views._get_current_user")
    @patch("lmstudio_proxy.services.requests.request")
    def test_embeddings_forces_configured_embedding_model(self, request_mock, user_mock):
        user_mock.return_value = self._user()
        request_mock.side_effect = [
            _json_response(
                {
                    "data": [
                        {
                            "key": "text-embedding-user-bge-m3",
                            "publisher": "Content-AI",
                            "display_name": "User Bge M3",
                            "type": "embedding",
                        }
                    ]
                }
            ),
            _json_response(
                {
                    "object": "list",
                    "data": [
                        {"object": "embedding", "embedding": [0.1, 0.2], "index": 0}
                    ],
                    "model": "text-embedding-user-bge-m3",
                }
            ),
        ]

        response = self.client.post(
            "/api/lmstudio/v1/embeddings/",
            data={
                "model": "ignored-model",
                "input": "поиск подразделений",
                "encoding_format": "float",
            },
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer test-token",
        )

        self.assertEqual(response.status_code, 200)
        kwargs = request_mock.call_args.kwargs
        self.assertTrue(request_mock.call_args.args[1].endswith("/embeddings"))
        self.assertEqual(kwargs["json"]["model"], "text-embedding-user-bge-m3")
        self.assertEqual(kwargs["json"]["input"], "поиск подразделений")
        self.assertEqual(kwargs["json"]["encoding_format"], "float")
        self.assertEqual(len(response.json()["data"][0]["embedding"]), 2)

    @override_settings(
        LM_STUDIO_EMBEDDING_MODEL="Content-AI/USER-bge-m3-Q8_0-GGUF",
        LM_STUDIO_AUTO_LOAD_MODEL=False,
    )
    @patch("User.views._get_current_user")
    @patch("lmstudio_proxy.services.requests.request")
    def test_embeddings_resolves_hf_repo_to_lm_studio_key(self, request_mock, user_mock):
        user_mock.return_value = self._user()
        request_mock.side_effect = [
            _json_response(
                {
                    "data": [
                        {
                            "key": "text-embedding-user-bge-m3",
                            "publisher": "Content-AI",
                            "display_name": "User Bge M3",
                            "type": "embedding",
                            "quantization": {"name": "Q8_0"},
                        }
                    ]
                }
            ),
            _json_response(
                {
                    "object": "list",
                    "data": [{"object": "embedding", "embedding": [0.3], "index": 0}],
                }
            ),
        ]

        response = self.client.post(
            "/api/lmstudio/v1/embeddings/",
            data={"input": "тест"},
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer test-token",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            request_mock.call_args.kwargs["json"]["model"],
            "text-embedding-user-bge-m3",
        )

    @patch("User.views._get_current_user")
    def test_embeddings_rejects_empty_input(self, user_mock):
        user_mock.return_value = self._user()
        response = self.client.post(
            "/api/lmstudio/v1/embeddings/",
            data={"input": []},
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer test-token",
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("input", response.json()["error"])

    @override_settings(LM_STUDIO_AUTO_LOAD_MODEL=True)
    @patch("lmstudio_proxy.services.requests.request")
    def test_embeddings_loads_embedding_model_when_needed(self, request_mock):
        request_mock.side_effect = [
            _json_response(
                {
                    "data": [
                        {
                            "key": "text-embedding-user-bge-m3",
                            "id": "user-bge-m3-q8_0.gguf",
                            "state": "not-loaded",
                            "publisher": "Content-AI",
                        }
                    ]
                }
            ),
            _json_response({"status": "loaded"}),
            _json_response(
                {
                    "object": "list",
                    "data": [{"object": "embedding", "embedding": [0.5], "index": 0}],
                }
            ),
        ]

        response = self.client.post(
            "/api/lmstudio/v1/embeddings/",
            data={"input": ["текст A", "текст B"]},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(request_mock.call_count, 3)
        load_kwargs = request_mock.call_args_list[1].kwargs
        self.assertEqual(load_kwargs["json"]["model"], "text-embedding-user-bge-m3")
        self.assertEqual(load_kwargs["json"]["context_length"], 8192)
        embed_kwargs = request_mock.call_args_list[2].kwargs
        self.assertEqual(embed_kwargs["json"]["input"], ["текст A", "текст B"])
