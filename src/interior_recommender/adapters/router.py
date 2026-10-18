from .cloudflare import CloudflareAdapter
from .gemini import GeminiAdapter


class ModelAdapter:
    """Instantiate each provider only when its own operation is requested."""
    def __init__(self, settings):
        self.settings = settings
        self.providers = {}

    def call(self, kind, prompt, images, schema=None):
        provider = self.settings.image_provider if kind == "generate" else "gemini"
        if provider not in self.providers:
            factory = CloudflareAdapter if provider == "cloudflare" else GeminiAdapter
            self.providers[provider] = factory(self.settings)
        return self.providers[provider].call(kind, prompt, images, schema)

    def close(self):
        for adapter in self.providers.values():
            adapter.close()

    def prepare_async(self):
        # Called in the shared synchronous pool before entering the event loop.
        if "gemini" not in self.providers:
            self.providers["gemini"] = GeminiAdapter(self.settings)

    async def acall(self, kind, prompt, images, schema=None):
        if kind not in {"direct_context", "embed_text"}:
            raise ValueError("Unsupported asynchronous recommendation operation")
        return await self.providers["gemini"].acall(kind, prompt, images, schema)

    async def aclose(self):
        for adapter in self.providers.values():
            if hasattr(adapter, "aclose"):
                await adapter.aclose()
