"""Direct recommendations on the existing owned-run/Canvas API, without generation."""
import json
import time
from pathlib import Path
from uuid import UUID, uuid4

import numpy as np

from ..assets import checksum, png_bytes, read_json, write_bytes, write_json
from ..config import Settings
from ..pipeline import Pipeline, contract_hash, now, run_lock
from ..domain.recommendation import (CONTEXT_VERSION, ProductIndex, RankingPolicy, RecommendationContext,
                              context_prompt, parse_context, validate_context)
from ..schemas import InputRequest, normalized_vector
from ..services import PROMPT_VERSION, SCHEMA_VERSION
from ..catalog.danawa import DanawaCatalog
from ..web.workflow import DemoAdapter, Studio


# Versioned SketchOn choices describe color/tone, not required product materials.
STYLE_COLORS_V2 = {
    "크림 패브릭": ("ivory", "크림색 아이보리 밝은 뉴트럴 톤 cream ivory color"),
    "오크 우드": ("brown", "밝은 갈색 베이지 내추럴 톤 light brown beige color"),
    "세이지 그린": ("green", "차분한 세이지 초록색 톤 muted sage green color"),
}


class DirectDemoAdapter(DemoAdapter):
    def call(self, kind, prompt, images, schema=None):
        if kind == "direct_context":
            value = RecommendationContext(decision="ACCEPTED", category_code="CHAIR", search_terms=["의자"],
                                          reason="데모 고정 해석입니다. 실제 사진/요구사항 분석이 아닙니다.")
            return {"payload": {"text": value.model_dump_json()}, "usage_json": {"demo": True}, "provider_request_id": None}
        if kind not in {"embed_text"}:
            raise ValueError("Direct recommendation does not generate or inspect generated images")
        return super().call(kind, prompt, images, schema)


class DirectStudio(Studio):
    direct = True

    def __init__(self, root, *, products=None, danawa_index=None, demo=False, adapter_factory=None, policy=None, index=None):
        super().__init__(root, demo=demo, adapter_factory=adapter_factory or (DirectDemoAdapter if demo else None))
        self.policy = RankingPolicy.model_validate(policy or {})
        settings = Settings.from_env()
        if index is not None:
            self.index = index
        elif products:
            self.index = ProductIndex.load(products)
        elif demo:
            self.index = self._demo_catalog(settings)
        elif danawa_index:
            catalog = DanawaCatalog(danawa_index)
            catalog.validate_settings(settings)
            rows = [{"image_id": t["id"], "product_id": t["id"], "name": "상품 연결 미확인 이미지",
                     "image": t["image"], "image_checksum": catalog.image_checksums[t["id"]]} for t in catalog.templates]
            self.index = ProductIndex(rows, np.asarray([catalog.vectors[t["id"]] for t in catalog.templates], dtype=np.float32),
                                      model=settings.embedding_model, preprocessing=settings.preprocessing_version,
                                      version=checksum(Path(danawa_index).read_bytes() + json.dumps(rows, sort_keys=True).encode()))
            self.index.version = checksum((self.index.version + checksum(memoryview(self.index.vectors))).encode())
        else:
            self.index = None
        if self.index:
            self.index.validate_settings(settings)

    def _demo_catalog(self, settings):
        from PIL import Image, ImageDraw
        import io
        rows, vectors = [], []
        adapter = DirectDemoAdapter(settings)
        for n, color in enumerate(("brown", "white", "black")):
            image = Image.new("RGB", (180, 180), "#f4f3ec")
            draw = ImageDraw.Draw(image)
            draw.rectangle((40, 35, 140, 110), fill=color)
            draw.line((45, 110, 45, 155), fill=color, width=8)
            draw.line((135, 110, 135, 155), fill=color, width=8)
            output = io.BytesIO()
            image.save(output, format="PNG")
            data = output.getvalue()
            path = self.root / "demo-products" / f"chair-{n}.png"
            write_bytes(path, data)
            rows.append({"image_id": f"demo-{n}", "product_id": f"demo-{n}", "name": f"데모 의자 {n + 1}",
                         "image": str(path), "image_checksum": checksum(data), "category_code": "CHAIR", "color": color,
                         "metadata_source": "synthetic demo fixture"})
            vectors.append(adapter.call("embed_text", color, [])["payload"]["values"])
        return ProductIndex(rows, np.asarray(vectors, dtype=np.float32), model=settings.embedding_model,
                            preprocessing=settings.preprocessing_version, version="synthetic-demo-v1")

    def catalog(self):
        # The direct UI receives only bounded recommendation results, never the entire catalog.
        return []

    def add_template(self, *args, **kwargs):
        raise ValueError("직접 추천은 사전 상품 카탈로그를 사용합니다.")

    def clone_selection(self, *args, **kwargs):
        raise ValueError("직접 추천에서는 템플릿을 선택하지 않습니다.")

    def submit(self, room, sketch, text, strokes, region, owner, event_id, *, canvas_document=None, project_metadata=None):
        owner, event_id = str(UUID(owner)), str(UUID(event_id))
        if not room or not (text.strip() or sketch):
            raise ValueError("방 사진과 텍스트 또는 스케치를 입력하세요.")
        room_png, size = png_bytes(room, white_background=True, allow_webp=True)
        if size[0] * size[1] > 16_000_000:
            raise ValueError("방 사진은 1,600만 화소 이하로 준비하세요.")
        sketch_png = None
        if sketch:
            sketch_png, sketch_size = png_bytes(sketch, white_background=True)
            if sketch_size != size:
                raise ValueError("스케치와 사진 크기가 다릅니다.")
        categories = sorted({p.category_code for p in self.index.products if p.category_code} | {"CHAIR", "SOFA", "TABLE"}) if self.index else ["CHAIR", "SOFA", "TABLE"]
        request = InputRequest(room_image="inputs/room.png", sketch_image="inputs/sketch.png" if sketch else None,
                               text=text, categories=categories, requested_region=region).model_dump(mode="json")
        raw_hash = checksum(room + (sketch or b"") + json.dumps([text, strokes, region, canvas_document, project_metadata], sort_keys=True).encode())
        event_path = self.root / "events" / f"{owner}-{event_id}.json"
        with self.guard:
            if event_path.exists():
                saved = read_json(event_path)
                if saved["hash"] != raw_hash:
                    raise ValueError("같은 요청 ID의 입력이 다릅니다.")
                return saved["run_id"]
            run_id = str(uuid4())
            directory = self.root / "runs" / run_id
            assets = {}
            for name, data in (("inputs/room.png", room_png), ("inputs/sketch.png", sketch_png),
                               ("inputs/original-room.bin", room), ("inputs/original-sketch.bin", sketch)):
                if data:
                    write_bytes(directory / name, data)
                    assets[name] = {"asset_id": str(uuid4()), "checksum": checksum(data)}
            request.update(owner_id=owner, source_event_id=event_id, revision_no=1, parent_run_id=None,
                           sketch_strokes=strokes, canvas_document=canvas_document, external_project=project_metadata)
            manifest = {"run_id": run_id, "created_at": now(), "mode": "LOCAL_DIRECT_SAMPLE", "bronze_verified": False,
                        "web_demo": self.demo, "prompt_version": PROMPT_VERSION, "schema_version": SCHEMA_VERSION,
                        "settings": Settings.from_env().model_dump(), "request": request, "templates": [], "assets": assets,
                        "stages": {}, "status": "PENDING", "selected_template": None,
                        "direct_contract": {"context_version": CONTEXT_VERSION, "ranking_policy": self.policy.model_dump(),
                                            "index_version": self.index.version if self.index else None}}
            manifest["input_hash"] = contract_hash(manifest)
            write_json(directory / "manifest.json", manifest)
            write_json(event_path, {"run_id": run_id, "hash": raw_hash})
            return run_id

    def launch(self, run_id, owner, action, retry=False):
        if action != "prepare":
            raise ValueError("직접 추천에서는 추천 준비/재시도만 지원합니다.")
        return super().launch(run_id, owner, action, retry)

    def _work(self, directory, action, job, retry):
        started = time.monotonic()
        try:
            with run_lock(directory):
                pipeline = Pipeline(directory, self.factory)
                try:
                    if contract_hash(pipeline.manifest) != pipeline.manifest["input_hash"]:
                        raise ValueError("입력/정책 스냅샷이 변경되었습니다.")
                    contract = pipeline.manifest.get("direct_contract", {})
                    if pipeline.manifest.get("web_demo") != self.demo or contract.get("context_version") != CONTEXT_VERSION:
                        raise ValueError("실행 모드/해석 버전이 다릅니다. 새 요청을 제출하세요.")
                    if not self.index or contract.get("index_version") != self.index.version:
                        pipeline.manifest["status"] = "INDEX_NOT_READY"
                        pipeline.save()
                        raise ValueError("요청에 고정된 상품 인덱스가 준비되지 않았습니다.")
                    self.index.validate_settings(pipeline.settings)
                    self._prepare(pipeline)
                finally:
                    if pipeline.adapter is not None:
                        pipeline.adapter.close()
            job["status"] = "SUCCEEDED"
        except Exception as error:
            job.update(status="FAILED", error_code=type(error).__name__,
                       error=str(error)[:1000] if isinstance(error, ValueError) else "추천 처리 중 오류가 발생했습니다.")
        job.update(finished_at=now(), duration_ms=round((time.monotonic() - started) * 1000))
        write_json(directory / "job.json", job)

    def _prepare(self, p):
        previous = p.manifest["stages"].get("ranking", {})
        if previous.get("status") == "SUCCEEDED":
            if checksum((p.directory / previous["output"]).read_bytes()) != previous["output_checksum"]:
                raise ValueError("저장 추천 결과 checksum이 다릅니다.")
            saved = read_json(p.directory / previous["output"])
            p.manifest["status"] = "COMPLETED" if saved["items"] else "UNVERIFIED_ONLY" if saved["unverified_items"] else "NO_RESULTS"
            p.save()
            return
        request = p.manifest["request"]
        model_request = {k: request.get(k) for k in ("text", "categories", "requested_region")}
        images = [png_bytes(p.asset(request[k]), resize=True)[0] for k in ("room_image", "sketch_image") if request.get(k)]
        context = p.stage("context", "direct_context", context_prompt(model_request), images,
                          lambda value: parse_context(value, request["text"], request["categories"]).model_dump(mode="json"))
        context = validate_context(context, request["text"], request["categories"])
        if context.decision != "ACCEPTED":
            p.manifest["status"] = context.decision
            p.save()
            return
        policy = RankingPolicy.model_validate(p.manifest["direct_contract"]["ranking_policy"])
        query_text = " | ".join([context.category_code, *context.search_terms, context.room_context])
        if policy.version == "weighted-content-v2":
            # Preserve the original wording (including negation) in semantic search.
            query_text = " | ".join([query_text, *(c.source_quote for c in context.required if c.field == "material")])
        # SketchOn's optional style is query context, never evidence for required conditions.
        external = request.get("external_project")
        style_color = None
        color_source = "context" if context.preferred_color else "none"
        if isinstance(external, dict) and external.get("style_policy") in {"search-only-v1", "color-first-v2"}:
            preferences = external.get("style_preferences", [])
            if (not isinstance(preferences, list) or len(preferences) > 1
                    or any(not isinstance(value, str) or len(value) > 100 for value in preferences)):
                raise ValueError("Invalid optional style preferences")
            if external["style_policy"] == "color-first-v2":
                if preferences:
                    if preferences[0] not in STYLE_COLORS_V2:
                        raise ValueError("Unsupported style color preference")
                    style_color, color_query = STYLE_COLORS_V2[preferences[0]]
                    query_text = " | ".join([query_text, color_query])
                    # Explicit requirements and preferences from the original text take precedence.
                    required_color = next((c.value for c in context.required if c.field == "color" and c.operator == "EQ"), None)
                    if required_color:
                        context = context.model_copy(update={"preferred_color": required_color})
                        color_source = "context"
                    elif not context.preferred_color:
                        context = context.model_copy(update={"preferred_color": style_color})
                        color_source = "style"
            else:
                query_text = " | ".join([query_text, *preferences])
        query = p.stage("query", "embed_text", "task: search result | query: " + query_text, [],
                        lambda value: {"values": normalized_vector(value["values"], p.settings.dimension)})["values"]
        started = time.monotonic()
        try:
            result = self.index.rank(query, context, policy)
            if isinstance(external, dict) and external.get("style_policy") == "color-first-v2":
                result["color_preference"] = {"value": context.preferred_color, "source": color_source,
                                              "style_color": style_color}
            result["ranking_duration_ms"] = round((time.monotonic() - started) * 1000)
            # Snapshot only returned images. The full catalog/vector matrix is reused in memory.
            for item in result["items"] + result["unverified_items"]:
                product = self.index.by_image[item["image_id"]]
                data = Path(product.image).read_bytes()
                if checksum(data) != product.image_checksum:
                    raise ValueError("상품 이미지가 인덱스 생성 후 변경되었습니다.")
                name = "products/" + checksum(product.image_id.encode()) + ".png"
                png, size = png_bytes(data)
                p.persist_asset(name, png, size)
                item["image"] = name
            write_json(p.directory / "recommendations.json", result)
            p.manifest["stages"]["ranking"] = {"status": "SUCCEEDED", "output": "recommendations.json",
                "output_checksum": checksum((p.directory / "recommendations.json").read_bytes()), "attempts": [],
                "duration_ms": result["ranking_duration_ms"]}
            p.manifest["status"] = "COMPLETED" if result["items"] else "UNVERIFIED_ONLY" if result["unverified_items"] else "NO_RESULTS"
            p.save()
        except Exception:
            p.manifest["status"] = "RANKING_FAILED"
            p.save()
            raise

    def state(self, run_id, owner):
        output = super().state(run_id, owner)
        directory = self.directory(run_id, owner)
        manifest = read_json(directory / "manifest.json")
        output.update(flow="direct", contract=manifest["direct_contract"])
        for key, name in (("context", "context.json"), ("recommendations", "recommendations.json")):
            if (directory / name).exists():
                stage = manifest["stages"].get("context" if key == "context" else "ranking", {})
                if stage.get("status") == "SUCCEEDED":
                    if checksum((directory / name).read_bytes()) != stage["output_checksum"]:
                        raise ValueError("저장 결과 checksum이 다릅니다.")
                    output[key] = read_json(directory / name)
        return output

    def product_link(self, run_id, owner, image_id):
        from ..domain.product_links import comparison_url
        state = self.state(run_id, owner)
        result = state.get("recommendations", {})
        item = next((item for item in result.get("items", []) + result.get("unverified_items", [])
                     if item["image_id"] == image_id), None)
        if item is None:
            raise FileNotFoundError
        metadata = item.get("metadata", {})
        url = comparison_url(metadata.get("comparison_url"), item["product_id"])
        if not url or not metadata.get("metadata_source"):
            raise FileNotFoundError
        return url
