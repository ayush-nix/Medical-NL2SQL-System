"""
LLM Manager — centralized Ollama model interaction.
Handles SQL generation model, fast model, and embeddings.
All local via Ollama, no internet.
"""
import httpx
import json
import logging
from config import Config

logger = logging.getLogger("nl2sql.llm")


class LLMManager:
    """Manages all LLM calls via Ollama REST API."""

    def __init__(self, base_url: str = None):
        self.base_url = base_url or Config.OLLAMA_BASE_URL
        self.client = httpx.Client(timeout=300.0)
        self.async_client = httpx.AsyncClient(timeout=300.0)

    def warmup_models(self):
        """Pre-load SQL and Brain models into memory with keep_alive=-1.
        This prevents model swapping latency between pipeline stages."""
        for model in [Config.SQL_MODEL, Config.FAST_MODEL]:
            try:
                resp = self.client.post(
                    f"{self.base_url}/api/generate",
                    json={"model": model, "prompt": "", "keep_alive": -1},
                    timeout=120.0,
                )
                if resp.status_code == 200:
                    logger.info(f"Model pre-warmed: {model} (keep_alive=-1)")
                else:
                    logger.warning(f"Warmup returned {resp.status_code} for {model}")
            except Exception as e:
                logger.warning(f"Could not warm up {model}: {e}")

    async def generate(self, prompt: str, model: str = None,
                       temperature: float = None,
                       num_ctx: int = None) -> str:
        """Generate text from Ollama model."""
        model = model or Config.SQL_MODEL
        temperature = temperature if temperature is not None else Config.SQL_TEMPERATURE
        num_ctx = num_ctx or Config.SQL_NUM_CTX

        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "keep_alive": -1,  # Keep model loaded — prevents swap latency
            "options": {
                "temperature": temperature,
                "num_ctx": num_ctx,
            }
        }

        try:
            response = await self.async_client.post(
                f"{self.base_url}/api/generate",
                json=payload,
                timeout=300.0,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("response", "").strip()
        except httpx.ConnectError:
            logger.error(f"Cannot connect to Ollama at {self.base_url}")
            raise ConnectionError(
                f"Cannot connect to Ollama at {self.base_url}. "
                f"Please ensure Ollama is running."
            )
        except httpx.ReadTimeout:
            logger.error(f"Ollama request timed out for model {model}")
            raise TimeoutError(
                f"The model took too long to respond. "
                f"Try a simpler question or a smaller/faster model."
            )
        except Exception as e:
            logger.error(f"LLM generation error: {e}")
            raise

    def generate_sync(self, prompt: str, model: str = None,
                      temperature: float = None,
                      num_ctx: int = None) -> str:
        """Synchronous generate for startup/init tasks."""
        model = model or Config.SQL_MODEL
        temperature = temperature if temperature is not None else Config.SQL_TEMPERATURE
        num_ctx = num_ctx or Config.SQL_NUM_CTX

        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_ctx": num_ctx,
            }
        }

        try:
            response = self.client.post(
                f"{self.base_url}/api/generate",
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("response", "").strip()
        except Exception as e:
            logger.error(f"LLM sync generation error: {e}")
            raise

    async def embed(self, text: str, model: str = None) -> list[float]:
        """Get embedding vector for text."""
        model = model or Config.EMBEDDING_MODEL
        payload = {
            "model": model,
            "input": text,
        }

        try:
            response = await self.async_client.post(
                f"{self.base_url}/api/embed",
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            embeddings = data.get("embeddings", [[]])
            return embeddings[0] if embeddings else []
        except Exception as e:
            logger.error(f"Embedding error: {e}")
            return []

    async def check_model_available(self, model: str) -> bool:
        """Check if a model is available in Ollama."""
        try:
            response = await self.async_client.get(
                f"{self.base_url}/api/tags"
            )
            response.raise_for_status()
            data = response.json()
            models = [m["name"] for m in data.get("models", [])]
            return model in models or any(model in m for m in models)
        except Exception:
            return False

    async def list_models(self) -> list[str]:
        """List all available Ollama models."""
        try:
            response = await self.async_client.get(
                f"{self.base_url}/api/tags"
            )
            response.raise_for_status()
            data = response.json()
            return [m["name"] for m in data.get("models", [])]
        except Exception:
            return []

    async def pull_model(self, model: str) -> bool:
        """Pull a model (for initial setup)."""
        try:
            response = await self.async_client.post(
                f"{self.base_url}/api/pull",
                json={"name": model},
                timeout=600.0  # 10 min timeout for large models
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Model pull error: {e}")
            return False


# Singleton
llm_manager = LLMManager()
