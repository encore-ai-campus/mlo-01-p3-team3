import base64
import json
import random
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

import httpx
from pydantic import ValidationError

from .assets import checksum, crop_bytes, generation_reference, png_bytes, read_json, write_bytes, write_json
from .config import Settings
from .local_processing import search_query
from .errors import provider_message
from .schemas import (Box, InputRequest, Interpretation, ObjectAnalysis, ScenePlan, Template,
                      normalized_vector, validate_categories)
from .services import (PROMPT_VERSION, SCHEMA_VERSION, SCHEMAS, analysis_prompt,
                       WEB_PROMPT_VERSION, generation_prompt, interpretation_prompt)


def now():
    return datetime.now(timezone.utc).isoformat()


def contract_hash(manifest):
    frozen = {key: manifest[key] for key in ("request", "templates", "settings", "prompt_version", "schema_version")}
    frozen["input_assets"] = {name: value for name, value in manifest["assets"].items() if name.startswith("inputs/")}
    if "web_prompt_version" in manifest:
        frozen["web_prompt_version"] = manifest["web_prompt_version"]
    if "direct_contract" in manifest:
        frozen["direct_contract"] = manifest["direct_contract"]
    return checksum(json.dumps(frozen, sort_keys=True, ensure_ascii=False).encode("utf-8"))


@contextmanager
def run_lock(directory):
    lock = directory / ".lock"
    try:
        stream = lock.open("x")
    except FileExistsError:
        raise ValueError("Run is locked; verify no runner is active before removing .lock") from None
    try:
        with stream:
            stream.write(now())
        yield
    finally:
        lock.unlink(missing_ok=True)


def create_run(request_path, templates_path, output_root, settings):
    request_path, templates_path = Path(request_path).resolve(), Path(templates_path).resolve()
    request = InputRequest.model_validate(read_json(request_path))
    templates = [Template.model_validate(t) for t in read_json(templates_path)["templates"]]
    if len({t.id for t in templates}) != len(templates):
        raise ValueError("Template IDs must be unique within the manifest")
    # Validate every file before creating a run; approved assets only are snapshotted.
    sources = []
    for role in ("room_image", "sketch_image"):
        filename = getattr(request, role)
        if filename:
            path = (request_path.parent / filename).resolve()
            original = path.read_bytes()
            data, size = png_bytes(original)
            sources.append((role, data, {"source_checksum": checksum(original), "size": size}))
    approved = []
    for template in templates:
        if template.active and template.review_status == "APPROVED":
            original = (templates_path.parent / template.image).resolve().read_bytes()
            data, size = png_bytes(original)
            approved.append((template, data, size, checksum(original)))
    run_id = str(uuid4())
    directory = Path(output_root).resolve() / run_id
    directory.mkdir(parents=True)
    assets, input_data = {}, request.model_dump(mode="json")
    for role, data, metadata in sources:
        name = f"inputs/{role}.png"
        write_bytes(directory / name, data)
        assets[name] = {"asset_id": str(uuid4()), "checksum": checksum(data), **metadata}
        input_data[role] = name
    frozen_templates = []
    for i, (template, data, size, original_hash) in enumerate(approved):
        name = f"inputs/template-{i}.png"
        write_bytes(directory / name, data)
        assets[name] = {"asset_id": str(uuid4()), "checksum": checksum(data), "size": size,
                        "source_checksum": original_hash}
        frozen_templates.append({**template.model_dump(mode="json"), "image": name})
    manifest = {"run_id": run_id, "created_at": now(), "mode": "LOCAL_SAMPLE",
                "bronze_verified": False, "schema_version": SCHEMA_VERSION,
                "prompt_version": PROMPT_VERSION, "settings": settings.model_dump(),
                "request": input_data, "templates": frozen_templates, "assets": assets,
                "status": "PENDING", "stages": {}, "selected_template": None}
    manifest["input_hash"] = contract_hash(manifest)
    write_json(directory / "manifest.json", manifest)
    return directory


class Pipeline:
    def __init__(self, directory, adapter_factory):
        self.directory = Path(directory).resolve()
        self.manifest = read_json(self.directory / "manifest.json")
        saved_settings = dict(self.manifest["settings"])
        if "image_provider" not in saved_settings and saved_settings.get("image_model", "").startswith("gemini-"):
            saved_settings["image_provider"] = "gemini"
        self.settings = Settings.model_validate(saved_settings)
        if self.manifest["prompt_version"] != PROMPT_VERSION or self.manifest["schema_version"] != SCHEMA_VERSION:
            raise ValueError("Run contract version differs; create a new run")
        self.adapter_factory = adapter_factory
        self.adapter = None

    def save(self):
        self.manifest["updated_at"] = now()
        write_json(self.directory / "manifest.json", self.manifest)

    def asset(self, name):
        data = (self.directory / name).read_bytes()
        if checksum(data) != self.manifest["assets"][name]["checksum"]:
            raise ValueError("Asset checksum mismatch: " + name)
        return data

    def persist_asset(self, name, data, size):
        write_bytes(self.directory / name, data)
        self.manifest["assets"][name] = {"asset_id": str(uuid4()), "checksum": checksum(data), "size": size}
        self.save()

    def response(self, key, kind, prompt, images):
        stage = self.manifest["stages"].setdefault(key, {"status": "PENDING", "attempts": []})
        receipt_path = self.directory / f"receipts/{key}.json"
        if receipt_path.exists():
            # Recover the provider response without repeating a paid call after persistence failure.
            if stage.get("receipt_checksum") and checksum(receipt_path.read_bytes()) != stage["receipt_checksum"]:
                raise ValueError("Provider receipt checksum mismatch")
            receipt = read_json(receipt_path)
            for attempt in stage["attempts"]:
                if attempt["id"] == receipt["attempt_id"]:
                    attempt.update(status="SUCCEEDED", usage_json=receipt.get("usage_json"),
                                   provider_request_id=receipt.get("provider_request_id"))
            self.save()
            return receipt["payload"]
        if stage.get("next_retry_at", 0) > time.time():
            raise ValueError("Retry-After wait has not elapsed")
        if self.adapter is None:
            self.adapter = self.adapter_factory(self.settings)
        model = (self.settings.embedding_model if kind in {"embed", "embed_text"} else
                 self.settings.image_model if kind == "generate" else self.settings.analysis_model)
        for local_attempt in range(self.settings.max_attempts):
            attempt = {"id": str(uuid4()), "attempt_no": len(stage["attempts"]) + 1,
                       "started_at": now(), "model": model, "status": "RUNNING",
                       "estimated_cost_usd": None, "usage_json": None,
                       "prompt_version": PROMPT_VERSION, "schema_version": SCHEMA_VERSION}
            if kind == "generate":
                attempt.update(provider=self.settings.image_provider, output_width=self.settings.output_width,
                               output_height=self.settings.output_height,
                               preprocessing_version=self.settings.generation_preprocessing_version)
            stage["attempts"].append(attempt)
            stage["status"] = "RUNNING"
            self.save()
            start = time.monotonic()
            try:
                response = self.adapter.call(kind, prompt, images,
                                             self.response_schema(kind).model_json_schema() if kind in SCHEMAS else None)
            except Exception as error:
                code = getattr(error, "code", None)
                if getattr(error, "budget_exhausted", False):
                    reset = (datetime.now(timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                    attempt.update(status="BUDGET_WAIT", error_code="3036", finished_at=now(),
                                   latency_ms=round((time.monotonic() - start) * 1000))
                    stage.update(status="BUDGET_WAIT", next_retry_at=reset.timestamp())
                    self.manifest["status"] = "BUDGET_WAIT"
                    self.save()
                    raise ValueError("Cloudflare daily quota exhausted; resume after next_retry_at") from None
                transient = code in {429, 500, 502, 503, 504} or isinstance(error, (httpx.TimeoutException, httpx.NetworkError, TimeoutError))
                attempt.update(status="FAILED", error_code=str(code or type(error).__name__),
                               finished_at=now(), latency_ms=round((time.monotonic() - start) * 1000))
                diagnostic = provider_message(error)
                if diagnostic:
                    attempt["error_message"] = diagnostic
                # Never persist SDK exception text: it can include key-bearing request URLs.
                stage["status"] = "FAILED"
                self.save()
                if str(error) in {"GEMINI_API_KEY is not configured", "CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN are required"}:
                    raise ValueError(str(error)) from None
                if not transient or local_attempt + 1 >= self.settings.max_attempts:
                    raise ValueError(f"{kind} API failed ({attempt['error_code']}); see manifest attempts") from None
                delay = min(2 ** local_attempt + random.random(), 30)
                headers = getattr(getattr(error, "response", None), "headers", {})
                retry_after = headers.get("retry-after") if headers else None
                if retry_after:
                    try:
                        delay = max(delay, float(retry_after))
                    except ValueError:
                        from email.utils import parsedate_to_datetime
                        try:
                            delay = max(delay, parsedate_to_datetime(retry_after).timestamp() - time.time())
                        except (ValueError, TypeError):
                            pass
                stage.update(status="RETRY_WAIT", next_retry_at=time.time() + delay)
                self.save()
                if delay > 30:
                    raise ValueError("Provider requested a longer wait; resume after next_retry_at") from None
                time.sleep(delay)
                continue
            # Receipt is durable before local validation and final artifact storage.
            write_json(receipt_path, {**response, "attempt_id": attempt["id"]})
            stage["receipt_checksum"] = checksum(receipt_path.read_bytes())
            attempt.update(status="SUCCEEDED", usage_json=response.get("usage_json"),
                           provider_request_id=response.get("provider_request_id"), finished_at=now(),
                           latency_ms=round((time.monotonic() - start) * 1000))
            self.save()
            return response["payload"]

    def stage(self, key, kind, prompt, images, transform):
        state = self.manifest["stages"].get(key, {})
        if state.get("status") == "SUCCEEDED":
            output = self.directory / state["output"]
            if checksum(output.read_bytes()) != state["output_checksum"]:
                raise ValueError("Stage output checksum mismatch")
            return read_json(output)
        state.pop("error_message", None)
        state.pop("error_code", None)
        try:
            payload = self.response(key, kind, prompt, images)
            value = transform(payload)
            name = f"{key}.json"
            write_json(self.directory / name, value)
            state = self.manifest["stages"][key]
            state.update(status="SUCCEEDED", output=name,
                         output_checksum=checksum((self.directory / name).read_bytes()))
            self.save()
            return value
        except Exception as error:
            state = self.manifest["stages"].get(key)
            if state and state.get("status") not in {"RETRY_WAIT", "BUDGET_WAIT"}:
                state["status"] = "FAILED"
                state["error_code"] = "API_OR_OUTPUT_ERROR"
                attempts = state.get("attempts", [])
                if attempts and attempts[-1].get("status") == "FAILED":
                    state["error_code"] = attempts[-1].get("error_code", state["error_code"])
                    if attempts[-1].get("error_message"):
                        state["error_message"] = attempts[-1]["error_message"]
                if isinstance(error, ValidationError):
                    state["error_code"] = "OUTPUT_VALIDATION_ERROR"
                    state["error_message"] = "; ".join(
                        ".".join(str(part) for part in item["loc"]) + ": " + item["type"]
                        for item in error.errors(include_input=False, include_url=False))[:1500]
            self.manifest["status"] = "BUDGET_WAIT" if state and state.get("status") == "BUDGET_WAIT" else "FAILED"
            self.save()
            detail = ": " + str(error) if str(error) in {"GEMINI_API_KEY is not configured", "CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN are required"} else ""
            if state and state.get("error_message"):
                detail = ": " + state["error_message"]
            raise ValueError(f"Stage {key} failed ({type(error).__name__}){detail}; inspect manifest and receipt") from None

    def response_schema(self, kind):
        if kind == "plan" and self.manifest.get("web_prompt_version") != WEB_PROMPT_VERSION:
            return ScenePlan
        return SCHEMAS[kind]

    def structured(self, kind, payload):
        data = json.loads(payload["text"])
        if kind in {"analyze", "compare"}:
            space = payload.get("coordinate_space", "normalized_1_xyxy")
            if space not in {"normalized_1_xyxy", "normalized_1000_xyxy"}:
                raise ValueError("Unsupported provider coordinate space")
            if space == "normalized_1000_xyxy":
                for obj in data.get("objects", []):
                    for field in ("x_min", "y_min", "x_max", "y_max"):
                        value = obj["box"][field]
                        if isinstance(value, bool) or not isinstance(value, (int, float)):
                            raise ValueError("Provider coordinates must be numbers")
                        obj["box"][field] = value / 1000
        result = self.response_schema(kind).model_validate(data)
        if isinstance(result, (Interpretation, ObjectAnalysis)):
            validate_categories(result, self.manifest["request"]["categories"])
        value = result.model_dump(mode="json")
        if kind == "plan" and self.manifest.get("web_prompt_version") == WEB_PROMPT_VERSION:
            value["search_text"] = search_query(value)
        return value

    def execute(self, until="analyze", object_id=None, retry_rejected=False):
        with run_lock(self.directory):
            # Reload under lock, so a previously completed runner is never overwritten.
            self.manifest = read_json(self.directory / "manifest.json")
            if contract_hash(self.manifest) != self.manifest["input_hash"]:
                raise ValueError("Frozen run input/settings were modified; create a new run")
            for name in self.manifest["assets"]:
                if name.startswith("inputs/"):
                    self.asset(name)
            if retry_rejected:
                key = f"embed-{str(UUID(object_id))}" if until == "embed" and object_id else until
                state = self.manifest["stages"].get(key, {})
                receipt = self.directory / f"receipts/{key}.json"
                if state.get("status") != "FAILED" or not receipt.exists():
                    raise ValueError("--retry-rejected requires a failed stage with a saved provider response")
                receipt.rename(receipt.with_name(key + ".rejected-" + uuid4().hex + ".json"))
                state["status"] = "PENDING"
                state.pop("receipt_checksum", None)
                self.save()
            try:
                return self._execute(until, object_id)
            except Exception:
                if self.manifest["status"] != "BUDGET_WAIT":
                    self.manifest["status"] = "FAILED"
                self.save()
                raise
            finally:
                if self.adapter is not None:
                    self.adapter.close()
                    self.adapter = None

    def _execute(self, until, object_id):
        request = self.manifest["request"]
        images = [self.asset(request[role]) for role in ("room_image", "sketch_image") if request.get(role)]
        interpretation = self.stage("interpret", "interpret", interpretation_prompt(request), images,
                                    lambda p: self.structured("interpret", p))
        if interpretation["decision"] != "ACCEPTED":
            self.manifest["status"] = interpretation["decision"]
            self.save()
            return self.manifest
        if until == "interpret":
            self.manifest["status"] = "INTERPRETED"
            self.save()
            return self.manifest
        template = self.manifest["selected_template"]
        if template is None:
            matches = [t for t in self.manifest["templates"]
                       if t["category_code"] == interpretation["category_code"]]
            if request.get("template_id"):
                matches = [t for t in matches if t["id"] == request["template_id"]]
            if len(matches) != 1:
                self.manifest["status"] = "NEEDS_TEMPLATE_SELECTION" if matches else "TEMPLATE_UNAVAILABLE"
                self.save()
                return self.manifest
            template = matches[0]
            self.manifest["selected_template"] = template
            self.save()

        def store_image(payload):
            data, size = png_bytes(base64.b64decode(payload["image_base64"], validate=True))
            if self.settings.image_provider == "cloudflare" and size != (self.settings.output_width, self.settings.output_height):
                raise ValueError("Generated image dimensions do not match the requested output")
            self.persist_asset("generated.png", data, size)
            return {"result_asset_id": self.manifest["assets"]["generated.png"]["asset_id"],
                    "image": "generated.png", "size": size}

        generation_images = images + [self.asset(template["image"])]
        generation_request = dict(request)
        if self.settings.image_provider == "cloudflare":
            if len(generation_images) > 4:
                raise ValueError("Adjust template selection to at most four references")
            refs, prepared = [], []
            names = [request[role] for role in ("room_image", "sketch_image") if request.get(role)] + [template["image"]]
            for i, (data, name) in enumerate(zip(generation_images, names)):
                transformed, mapping = generation_reference(data)
                target = f"generation_refs/reference-{i}.png"
                if target not in self.manifest["assets"]:
                    self.persist_asset(target, transformed, mapping["size"])
                prepared.append(self.asset(target))
                refs.append({"source": name, "source_asset_id": self.manifest["assets"][name]["asset_id"],
                             "source_checksum": checksum(data), "api_asset": target,
                             "api_asset_id": self.manifest["assets"][target]["asset_id"],
                             "api_checksum": checksum(transformed), "order": i,
                             "preprocessing_version": self.settings.generation_preprocessing_version, **mapping})
            if request.get("requested_region"):
                mapping = refs[0]
                box = request["requested_region"]
                mapped = {}
                for field in ("x_min", "x_max", "y_min", "y_max"):
                    axis = 0 if field.startswith("x") else 1
                    mapped[field] = (box[field] * mapping["original_size"][axis] * mapping["scale"][axis] + mapping["offset"][axis]) / mapping["size"][axis]
                generation_request["requested_region"] = mapped
            self.manifest["generation_input_refs"] = {"references": refs, "api_requested_region": generation_request.get("requested_region"),
                                                       "provider": "cloudflare", "model": self.settings.image_model,
                                                       "output_width": self.settings.output_width, "output_height": self.settings.output_height}
            self.save()
            generation_images = prepared
        self.stage("generate", "generate", generation_prompt(generation_request, interpretation, template),
                   generation_images, store_image)
        if until == "generate":
            self.manifest["status"] = "GENERATED"
            self.save()
            return self.manifest

        def store_objects(payload):
            result = self.structured("analyze", payload)
            for obj in result["objects"]:
                obj["id"] = str(uuid4())
                obj["reference_asset_id"] = self.manifest["assets"]["generated.png"]["asset_id"]
                obj["reference_size"] = self.manifest["assets"]["generated.png"]["size"]
            return result

        objects = self.stage("analyze", "analyze", analysis_prompt(request["categories"]),
                             [self.asset("generated.png")], store_objects)
        if until == "analyze" or object_id is None:
            self.manifest["status"] = "NEEDS_OBJECT_SELECTION" if objects["objects"] else "NO_OBJECTS"
            self.save()
            return self.manifest
        object_id = str(UUID(object_id))
        obj = next((obj for obj in objects["objects"] if obj["id"] == object_id), None)
        if obj is None:
            raise ValueError("Selected object ID does not belong to this run")
        crop, size = crop_bytes(self.asset("generated.png"), Box.model_validate(obj["box"]))
        crop_name = f"crops/{object_id}.png"
        if crop_name not in self.manifest["assets"]:
            self.persist_asset(crop_name, crop, size)
        else:
            crop = self.asset(crop_name)
        prepared, _ = png_bytes(crop, resize=True)

        def store_vector(payload):
            values = normalized_vector(payload["values"], self.settings.dimension)
            return {"object_id": object_id, "asset_id": self.manifest["assets"][crop_name]["asset_id"],
                    "image_checksum": checksum(crop), "preprocessed_checksum": checksum(prepared),
                    "model": self.settings.embedding_model, "dimension": self.settings.dimension,
                    "preprocessing_version": self.settings.preprocessing_version,
                    "normalization": "L2", "values": values}

        self.stage(f"embed-{object_id}", "embed", "", [prepared], store_vector)
        self.manifest["status"] = "SUCCEEDED"
        self.save()
        return self.manifest
