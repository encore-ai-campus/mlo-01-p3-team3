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
