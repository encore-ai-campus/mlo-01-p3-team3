"""Cloudflare Workers AI multipart image generation; no automatic provider fallback."""
import math
import os
import re

import httpx

from ..assets import png_bytes
from ..config import load_environment


class CloudflareError(Exception):
    def __init__(self, code, response, provider_code=None):
        super().__init__("Cloudflare generation failed")
        self.code = code
        self.response = response
        self.provider_code = provider_code
        self.budget_exhausted = provider_code == 3036


class CloudflareAdapter:
    def __init__(self, settings, client=None):
        load_environment()
        self.settings = settings
        self.account = os.getenv("CLOUDFLARE_ACCOUNT_ID", "").strip()
        self.token = os.getenv("CLOUDFLARE_API_TOKEN", "").strip()
        if not self.account or not self.token:
            raise ValueError("CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN are required")
        if not re.fullmatch(r"[a-fA-F0-9]{32}", self.account):
            raise ValueError("CLOUDFLARE_ACCOUNT_ID must be a 32-character hexadecimal account ID")
        if settings.image_model != "@cf/black-forest-labs/flux-2-klein-4b":
            raise ValueError("Cloudflare MVP supports only the documented FLUX.2 klein 4B model")
        self.client = client or httpx.Client(timeout=settings.timeout_seconds)
        self.async_client = None

    def close(self):
        self.client.close()

    async def aclose(self):
        if self.async_client:
            await self.async_client.aclose()

    def call(self, kind, prompt, images, schema=None):
        files, sizes = self._inputs(kind, prompt, images)
        response = self.client.post(
            f"https://api.cloudflare.com/client/v4/accounts/{self.account}/ai/run/{self.settings.image_model}",
            headers={"Authorization": "Bearer " + self.token}, files=files)
        return self._result(response, sizes)

    async def acall(self, kind, prompt, images, schema=None):
        # Header/size checks are CPU-small (references are already <=480px).
        files, sizes = self._inputs(kind, prompt, images)
        if self.async_client is None:
            self.async_client = httpx.AsyncClient(timeout=self.settings.timeout_seconds)
        response = await self.async_client.post(
            f"https://api.cloudflare.com/client/v4/accounts/{self.account}/ai/run/{self.settings.image_model}",
            headers={"Authorization": "Bearer " + self.token}, files=files)
        return self._result(response, sizes)

    def _inputs(self, kind, prompt, images):
        if kind != "generate" or len(images) > 4:
            raise ValueError("Cloudflare generation supports at most four reference images")
        sizes = []
        for data in images:
            _, size = png_bytes(data)
            if max(size) > 480:
                raise ValueError("Generation references must be at most 480px")
            sizes.append(size)
        # Text fields are multipart too, even when there are no reference images.
        files = {"prompt": (None, prompt), "width": (None, str(self.settings.output_width)),
                 "height": (None, str(self.settings.output_height))}
        files.update({f"input_image_{i}": (f"reference-{i}.png", data, "image/png")
                      for i, data in enumerate(images)})
        return files, sizes

    def _result(self, response, sizes):
        try:
            body = response.json()
        except ValueError:
            if response.is_error:
                raise CloudflareError(response.status_code, response) from None
            raise ValueError("Cloudflare returned a non-JSON response") from None
        if not isinstance(body, dict):
            raise ValueError("Cloudflare returned an invalid response envelope")
        if response.is_error or body.get("success") is False:
            codes = [e.get("code") for e in body.get("errors", []) if isinstance(e, dict)]
            raise CloudflareError(response.status_code, response, 3036 if 3036 in codes else (codes[0] if codes else None))
        result = body.get("result")
        image = result.get("image") if isinstance(result, dict) else None
        if not isinstance(image, str) or not image:
            raise ValueError("Cloudflare response contains no image")
        # Preserve the response before final decoding/size validation in the runner.
        estimated = round(sum(math.ceil(w / 512) * math.ceil(h / 512) for w, h in sizes) * 5.37 +
                          math.ceil(self.settings.output_width / 512) * math.ceil(self.settings.output_height / 512) * 26.05, 2)
        return {"payload": {"image_base64": image}, "provider_request_id": response.headers.get("cf-ray"),
                "usage_json": {"provider": "cloudflare", "estimated_neurons": estimated,
                               "measured_usage_available": False, "pricing_date": "2026-10-14",
                               "output_width": self.settings.output_width, "output_height": self.settings.output_height,
                               "reference_sizes": sizes}}
