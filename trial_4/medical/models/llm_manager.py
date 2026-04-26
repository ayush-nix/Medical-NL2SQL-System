"""
LLM Manager — Centralized Ollama model interaction.
Dual-model architecture with automatic fallback.

PRIMARY (Army GPU):
  - gpt_oss_120b:latest  (reasoning)
  - sqlcoder:15b         (SQL generation)

FALLBACK (Local PC):
  - llama3.1:8b          (both reasoning + SQL)

Auto-detection: On startup, checks which models are available in Ollama.
If primary isn't found, silently falls back. Zero manual config needed.
"""
import httpx
import logging
from config import MedicalConfig as Config

logger = logging.getLogger("nl2sql.llm")


class LLMManager:
    """Manages all LLM calls via Ollama REST API. Auto-fallback model routing."""

    def __init__(self, base_url: str = None):
        self.base_url = base_url or Config.OLLAMA_BASE_URL
        self.client = httpx.Client(timeout=1200.0)
        self.async_client = httpx.AsyncClient(timeout=1200.0)

        # Resolved models (set during warmup after availability check)
        self._reasoning_model = None
        self._sql_model = None

    def _get_available_models(self) -> set:
        """Query Ollama for all available models."""
        try:
            resp = self.client.get(f"{self.base_url}/api/tags", timeout=10.0)
            resp.raise_for_status()
            data = resp.json()
            models = set()
            for m in data.get("models", []):
                models.add(m["name"])
                # Also add without tag for flexible matching
                base = m["name"].split(":")[0]
                models.add(base)
            return models
        except Exception as e:
            logger.warning(f"Could not query Ollama models: {e}")
            return set()

    def _resolve_model(self, primary: str, fallback: str, available: set) -> str:
        """
        Pick the best available model.
        
        Logic:
          1. If primary model is in Ollama (EXACT match) → use it
          2. If a variant of the primary base name exists → use it
             (e.g., sqlcoder:15b not found but sqlcoder:7b exists → use sqlcoder:7b)
          3. If fallback model is in Ollama → use it
          4. If neither → return fallback
        """
        # Check 1: Exact match
        if primary in available:
            return primary

        # Check 2: Same base name, different tag
        primary_base = primary.split(":")[0]
        for model in available:
            if ":" in model and model.split(":")[0] == primary_base:
                logger.info(f"Primary '{primary}' not found → using available variant '{model}'")
                return model

        # Check 3: Fallback exact match
        if fallback in available:
            logger.info(f"Primary model '{primary}' not found → falling back to '{fallback}'")
            return fallback

        # Check 4: Fallback variant 
        fallback_base = fallback.split(":")[0]
        for model in available:
            if ":" in model and model.split(":")[0] == fallback_base:
                logger.info(f"Fallback '{fallback}' not found → using variant '{model}'")
                return model

        # Neither found
        logger.warning(f"Neither '{primary}' nor '{fallback}' found in Ollama. Using '{fallback}'.")
        return fallback

    def warmup(self):
        """
        Auto-detect available models and pre-load into VRAM.
        
        On Army GPU: finds gpt_oss_120b + sqlcoder:15b → uses them.
        On local PC: finds llama3.1:8b only → falls back automatically.
        """
        available = self._get_available_models()
        logger.info(f"Ollama models available: {sorted(available)}")

        # Resolve which models to actually use
        self._reasoning_model = self._resolve_model(
            Config.REASONING_MODEL, Config.FALLBACK_REASONING_MODEL, available
        )
        self._sql_model = self._resolve_model(
            Config.SQL_MODEL, Config.FALLBACK_SQL_MODEL, available
        )

        logger.info(f"Resolved REASONING model: {self._reasoning_model}")
        logger.info(f"Resolved SQL model: {self._sql_model}")

        # Pre-warm resolved models
        seen = set()
        for model in [self._reasoning_model, self._sql_model]:
            if model in seen:
                continue
            seen.add(model)
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

    @property
    def reasoning_model(self) -> str:
        """Get resolved reasoning model (with fallback applied)."""
        return self._reasoning_model or Config.FALLBACK_REASONING_MODEL

    @property
    def sql_model(self) -> str:
        """Get resolved SQL model (with fallback applied)."""
        return self._sql_model or Config.FALLBACK_SQL_MODEL

    async def generate(self, prompt: str, temperature: float = None,
                       num_ctx: int = None, model: str = None) -> str:
        """Generate text from Ollama model."""
        model = model or self.reasoning_model
        temperature = temperature if temperature is not None else Config.SQL_TEMPERATURE
        num_ctx = num_ctx or Config.SQL_NUM_CTX

        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "keep_alive": -1,
            "options": {
                "temperature": temperature,
                "num_ctx": num_ctx,
            }
        }

        try:
            response = await self.async_client.post(
                f"{self.base_url}/api/generate",
                json=payload,
                timeout=1200.0,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("response", "").strip()
        except httpx.ConnectError:
            logger.error(f"Cannot connect to Ollama at {self.base_url}")
            raise ConnectionError(
                f"Cannot connect to Ollama at {self.base_url}. "
                f"Ensure Ollama is running: 'ollama serve'"
            )
        except httpx.ReadTimeout:
            logger.error(f"Ollama request timed out for model {model}")
            raise TimeoutError(
                "The model took too long to respond. "
                "Try a simpler question or check GPU memory."
            )
        except Exception as e:
            logger.error(f"LLM generation error: {e}")
            raise

    # ── Dual-Model Convenience Methods ───────────────────────
    async def reason(self, prompt: str, temperature: float = None,
                     num_ctx: int = None) -> str:
        """Generate using the REASONING model (gpt_oss_120b or llama3.1 fallback)."""
        return await self.generate(
            prompt=prompt,
            temperature=temperature if temperature is not None else Config.COT_TEMPERATURE,
            num_ctx=num_ctx or Config.COT_NUM_CTX,
            model=self.reasoning_model,
        )

    async def generate_sql(self, prompt: str, temperature: float = None,
                           num_ctx: int = None) -> str:
        """Generate using the SQL model (sqlcoder:15b or llama3.1 fallback)."""
        return await self.generate(
            prompt=prompt,
            temperature=temperature if temperature is not None else Config.SQL_TEMPERATURE,
            num_ctx=num_ctx or Config.SQL_NUM_CTX,
            model=self.sql_model,
        )

    def generate_sync(self, prompt: str, temperature: float = None,
                      num_ctx: int = None) -> str:
        """Synchronous generate for startup/init tasks."""
        model = self.reasoning_model
        temperature = temperature if temperature is not None else 0.0
        num_ctx = num_ctx or Config.SQL_NUM_CTX

        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": temperature, "num_ctx": num_ctx},
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

    async def check_available(self) -> bool:
        """Check if the resolved models are available in Ollama."""
        try:
            response = await self.async_client.get(f"{self.base_url}/api/tags")
            response.raise_for_status()
            data = response.json()
            models = [m["name"] for m in data.get("models", [])]
            for target in [self.reasoning_model, self.sql_model]:
                if target not in models and not any(target in m for m in models):
                    logger.warning(f"Model {target} not found in Ollama")
                    return False
            return True
        except Exception:
            return False


# Singleton
llm_manager = LLMManager()
