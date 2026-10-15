import base64
import copy
import io
import json
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import UUID, uuid4

from PIL import Image, ImageDraw

from ..adapters.router import ModelAdapter
from ..assets import checksum, generation_reference, png_bytes, read_json, write_bytes, write_json
from ..config import Settings
from ..pipeline import Pipeline, contract_hash, create_run, now, run_lock
from ..local_processing import LOCAL_PROCESSING_VERSION, assess_comparison, map_region, rank_candidates, search_query
from ..schemas import ReferenceStructure, Template, normalized_vector
from ..scale import api_scale_bounds
from ..services import (comparison_prompt, generation_prompt, scene_plan_prompt, reference_structure_prompt,
                        REFERENCE_PROMPT_VERSION, WEB_PROMPT_VERSION, scale_plan_prompt)
from .danawa import DanawaCatalog


class DemoAdapter:
    """Clearly labeled local UI fixtures; never makes an external request."""
    def __init__(self, settings):
        self.settings = settings

    def call(self, kind, prompt, images, schema=None):
        if kind == "plan":
            category = "SOFA" if "소파" in prompt else "CHAIR"
            data = {"decision": "ACCEPTED", "category_code": category, "candidates": [], "color": None,
                    "material": None, "shape": None, "reason": "로컬 데모 응답입니다.",
                    "search_terms": [], "placement_intent": "스케치 영역 또는 방의 왼쪽 빈 공간",
                    "keep_elements": ["벽", "바닥", "창문"],
                    "sketch_role": "PLACEMENT_ONLY" if len(images) > 1 else "NONE", "shape_evidence": "NONE",
                    "placement_region": {"x_min": .55, "y_min": .5, "x_max": .8, "y_max": .9} if len(images) > 1 else None}
            if schema and "search_text" in schema.get("properties", {}):
                data["search_text"] = category
            payload = {"text": json.dumps(data)}
        elif kind == "scale":
            payload = {"text": json.dumps({"basis": "CONTEXT", "confidence": "MEDIUM",
                "anchors": [{"label": "데모 기준 물체", "box": {"x_min": .1, "y_min": .2, "x_max": .2, "y_max": .5},
                             "dimension": "HEIGHT", "typical_size_mm": {"minimum": 600, "maximum": 900},
                             "depth_relation": "데모", "uncertainty": "실제 측정 아님"}],
                "furniture_size_prior": "데모 고정 범위", "placement_depth": "데모", "contact_point": "데모",
                "perspective_reason": "데모 고정 응답; 품질 검증 아님", "explicit_size_intent": None,
                "width_ratio": {"minimum": .15, "maximum": .3}, "height_ratio": {"minimum": .3, "maximum": .5},
                "reason": "데모 고정 크기 계획입니다. 실제 공간을 분석하지 않습니다."})}
        elif kind == "reference":
            payload = {"text": json.dumps({"description": "데모 참조 가구", "support_structure": None,
                       "components": [], "connections": [], "preserve": ["참조 이미지의 형태와 지지 구조"],
                       "uncertain_details": ["데모 고정 응답으로 실제 구조를 분석하지 않습니다."],
                       "exact_leg_count": None, "exact_caster_count": None})}
        elif kind in {"embed", "embed_text"}:
            seed = (images[0] if images else prompt.encode())
            value = int(checksum(seed)[:8], 16)
            payload = {"values": [1 + ((value + i) % 17) for i in range(self.settings.dimension)]}
        elif kind == "generate":
            with Image.open(io.BytesIO(images[0])) as source:
                image = source.resize((self.settings.output_width, self.settings.output_height)).convert("RGB")
            draw = ImageDraw.Draw(image)
            w, h = image.size
            draw.rectangle((w * .2, h * .35, w * .4, h * .65), outline="#b66542", width=5)
            draw.text((16, 16), "DEMO - NO IMAGE API CALL", fill="#b66542")
            output = io.BytesIO()
            image.save(output, format="PNG")
            payload = {"image_base64": base64.b64encode(output.getvalue()).decode()}
        elif kind == "compare":
            payload = {"text": json.dumps({"objects": [{"category_code": "CHAIR", "box": {
                "x_min": .2, "y_min": .35, "x_max": .4, "y_max": .65}, "change_type": "ADDED",
                "reason": "데모용 고정 비교 결과입니다. 실제 생성 품질을 평가하지 않습니다."}],
                "reference_fidelity": {"status": "MATCH", "differences": [], "reason": "데모 고정 판정입니다. 실제 구조 검증이 아닙니다."},
                "annotation_check": {"status": "CLEAR", "findings": [], "reason": "데모 고정 판정입니다. 실제 주석 검증이 아닙니다."},
                "scale_check": {"status": "APPROPRIATE", "room_geometry": "PRESERVED", "findings": [],
                                "reason": "데모 고정 판정입니다. 실제 크기 검증이 아닙니다."}})}
        else:
            raise ValueError("Unsupported demo operation")
        return {"payload": payload, "usage_json": {"demo": True}, "provider_request_id": None}

    def close(self):
        pass


class Studio:
    def __init__(self, root, templates=None, demo=False, adapter_factory=None, danawa_index=None):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.demo = demo
        self.factory = adapter_factory or (DemoAdapter if demo else ModelAdapter)
        self.guard = threading.RLock()
        self.vector_guard = threading.Lock()
        self.reference_guard = threading.Lock()
        self.pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="scene-mvp")
        self.instance = str(uuid4())
        self.danawa = DanawaCatalog(danawa_index) if danawa_index else None
        if self.danawa:
            self.danawa.validate_settings(Settings.from_env())
        if not (self.root / "catalog.json").exists():
            write_json(self.root / "catalog.json", {"templates": []})
        if templates and not self.danawa:
            self.import_catalog(Path(templates).resolve())

    def import_catalog(self, path):
        for item in read_json(path)["templates"]:
            template = Template.model_validate(item)
            if template.active and template.review_status == "APPROVED":
                if any(t["id"] == template.id and t["version"] == template.version for t in self.catalog()):
                    continue
                self.add_template((path.parent / template.image).read_bytes(), template)

    def catalog(self):
        if self.danawa:
            return self.danawa.templates
        return read_json(self.root / "catalog.json")["templates"]

    def add_template(self, data, template):
        if self.danawa:
            raise ValueError("현재 다나와 인덱스를 사용 중입니다. 별도 템플릿 등록은 템플릿 모드에서 사용하세요.")
        png, size = png_bytes(data, white_background=True)
        with self.guard:
            entries = self.catalog()
            if any(t["id"] == template.id and t["version"] == template.version for t in entries):
                raise ValueError("이미 등록된 템플릿 ID와 버전입니다.")
            filename = f"catalog/{uuid4()}.png"
            write_bytes(self.root / filename, png)
            entries.append({**template.model_dump(mode="json"), "image": filename})
            write_json(self.root / "catalog.json", {"templates": entries})

    def directory(self, run_id, owner):
        directory = self.root / "runs" / str(UUID(str(run_id)))
        manifest = read_json(directory / "manifest.json")
        if manifest["request"].get("owner_id") != owner:
            raise PermissionError("이 브라우저에서 만든 실행만 열 수 있습니다.")
        return directory

    def submit(self, room, sketch, text, strokes, region, owner, event_id, *, canvas_document=None, project_metadata=None):
        if not room:
            raise ValueError("방 사진을 먼저 선택하세요.")
        if not text.strip() and not sketch:
            raise ValueError("스케치 또는 텍스트 요구사항을 입력하세요.")
        png, room_size = png_bytes(room, white_background=True, allow_webp=True)
        if room_size[0] * room_size[1] > 16_000_000:
            raise ValueError("방 사진은 1,600만 화소 이하로 준비하세요.")
        if sketch:
            _, sketch_size = png_bytes(sketch)
            if sketch_size != room_size:
                raise ValueError("스케치와 원본 사진의 크기가 다릅니다. 사진을 다시 올려 그려주세요.")
        event_id = str(UUID(event_id))
        payload_hash = checksum(room + (sketch or b"") + json.dumps({"text": text, "strokes": strokes, "region": region,
                                "document": canvas_document, "project": project_metadata}, sort_keys=True).encode())
        event_file = self.root / "events" / f"{owner}-{event_id}.json"
        with self.guard:
            if event_file.exists():
                previous = read_json(event_file)
                if previous["hash"] != payload_hash:
                    raise ValueError("같은 요청 ID에 다른 입력이 전달되었습니다.")
                return previous["run_id"]
            staging = self.root / "uploads" / str(uuid4())
            staging.mkdir(parents=True)
            try:
                write_bytes(staging / "room.png", png)
                if sketch:
                    write_bytes(staging / "sketch.png", png_bytes(sketch, white_background=True)[0])
                categories = sorted({t["category_code"] for t in self.catalog() if t["category_code"] != "UNCLASSIFIED"} | {"CHAIR", "SOFA", "TABLE"})
                write_json(staging / "request.json", {"room_image": "room.png", "sketch_image": "sketch.png" if sketch else None,
                           "text": text, "categories": categories, "requested_region": region})
                settings = Settings.from_env()
                if self.danawa:
                    self.danawa.validate_settings(settings)
                # Freeze the selected catalog, including precomputed vectors, per input.
                frozen_catalog = [{**t, "image": str((self.root / t["image"]).resolve())} for t in self.catalog()]
                if self.danawa:
                    for index, template in enumerate(frozen_catalog):
                        data = Path(template["image"]).read_bytes()
                        if checksum(data) != self.danawa.image_checksums[template["id"]]:
                            raise ValueError("다나와 이미지가 시작 후 변경되었습니다. 인덱스를 확인하고 서버를 재시작하세요.")
                        frozen_image = staging / f"danawa-{index}.png"
                        write_bytes(frozen_image, data)
                        template["image"] = str(frozen_image)
                write_json(staging / "templates.json", {"templates": frozen_catalog})
                path = create_run(staging / "request.json", staging / "templates.json", self.root / "runs", settings)
                manifest = read_json(path / "manifest.json")
                manifest.update(mode="LOCAL_WEB_SAMPLE", web_demo=self.demo, web_prompt_version=WEB_PROMPT_VERSION)
                if self.danawa:
                    manifest["reference_catalog"] = self.danawa.info()
                    vector_name = "inputs/danawa-vectors.json"
                    write_json(path / vector_name, self.danawa.vectors)
                    manifest["assets"][vector_name] = {"asset_id": str(uuid4()), "checksum": checksum((path / vector_name).read_bytes())}
                manifest["request"].update(owner_id=owner, source_event_id=event_id, revision_no=1,
                                            sketch_strokes=strokes, parent_run_id=None,
                                            canvas_document=canvas_document, external_project=project_metadata)
                for role, raw in (("room", room), ("sketch", sketch)):
                    if raw:
                        name = f"inputs/original-{role}.bin"
                        write_bytes(path / name, raw)
                        manifest["assets"][name] = {"asset_id": str(uuid4()), "checksum": checksum(raw)}
                manifest["input_hash"] = contract_hash(manifest)
                write_json(path / "manifest.json", manifest)
                write_json(event_file, {"run_id": manifest["run_id"], "hash": payload_hash})
                return manifest["run_id"]
            finally:
                # Only our UUID staging directory, not user source files.
                shutil.rmtree(staging)

    def clone_selection(self, run_id, owner, template_id=None, category=None):
        parent = self.directory(run_id, owner)
        with run_lock(parent):
            original = read_json(parent / "manifest.json")
            if contract_hash(original) != original["input_hash"]:
                raise ValueError("원본 입력 스냅샷이 변경되었습니다.")
            if (parent / "job.json").exists() and read_json(parent / "job.json")["status"] == "RUNNING":
                raise ValueError("현재 작업이 끝난 뒤 선택을 변경하세요.")
            plan = read_json(parent / "plan.json")
            category = category or plan["category_code"]
            if category not in original["request"]["categories"]:
                raise ValueError("지원하지 않는 품목입니다.")
            if template_id and not any(t["id"] == template_id and (t["category_code"] == category or original.get("reference_catalog", {}).get("source") == "danawa") for t in original["templates"]):
                raise ValueError("이 요청의 품목과 일치하는 템플릿을 선택하세요.")
            new_id = str(uuid4())
            target = self.root / "runs" / new_id
            shutil.copytree(parent / "inputs", target / "inputs")
            manifest = copy.deepcopy(original)
            manifest.update(run_id=new_id, created_at=now(), status="PENDING", stages={}, selected_template=None)
            manifest["assets"] = {k: v for k, v in manifest["assets"].items() if k.startswith("inputs/")}
            for field in ("generation_input_refs", "preview_checksum", "candidate_search"):
                manifest.pop(field, None)
            manifest["request"].update(template_id=template_id, confirmed_category=category,
                                       parent_run_id=run_id, source_event_id=str(uuid4()), revision_no=original["request"]["revision_no"] + 1)
            plan.update(decision="ACCEPTED", category_code=category, candidates=[], reason="사용자 품목·템플릿 선택을 반영했습니다.")
            if manifest.get("web_prompt_version") == WEB_PROMPT_VERSION:
                plan["search_text"] = search_query(plan)
            write_json(target / "plan.json", plan)
            manifest["stages"]["plan"] = {"status": "SUCCEEDED", "attempts": [], "output": "plan.json",
                                           "output_checksum": checksum((target / "plan.json").read_bytes()), "reused_from_run": run_id}
            manifest["input_hash"] = contract_hash(manifest)
            write_json(target / "manifest.json", manifest)
            return new_id

    def launch(self, run_id, owner, action, retry=False):
        directory = self.directory(run_id, owner)
        if action not in {"prepare", "generate", "compare"}:
            raise ValueError("지원하지 않는 작업입니다.")
        with self.guard:
            path = directory / "job.json"
            if path.exists():
                job = read_json(path)
                if job["status"] == "RUNNING" and job["instance"] == self.instance:
                    return job
                if job["status"] == "SUCCEEDED" and job["action"] == action and not retry:
                    return job
            if sum(not f.done() for f in getattr(self, "futures", [])) >= 8:
                raise ValueError("처리 대기 중인 요청이 많습니다. 잠시 후 다시 시도하세요.")
            job = {"id": str(uuid4()), "instance": self.instance, "action": action, "status": "RUNNING",
                   "started_at": now(), "duration_ms": 0, "finished_at": None}
            write_json(path, job)
            future = self.pool.submit(self._work, directory, action, job, retry)
            self.futures = [f for f in getattr(self, "futures", []) if not f.done()] + [future]
            return job

    def _work(self, directory, action, job, retry):
        start = time.monotonic()
        try:
            pipeline = Pipeline(directory, self.factory)
            with run_lock(directory):
                if contract_hash(pipeline.manifest) != pipeline.manifest["input_hash"]:
                    raise ValueError("입력 스냅샷이 변경되었습니다.")
                if pipeline.manifest.get("web_demo", False) != self.demo:
                    raise ValueError("데모 실행과 실제 API 실행은 서로 다른 출력 폴더를 사용하세요.")
                if action in {"prepare", "generate"} and pipeline.manifest.get("web_prompt_version") != WEB_PROMPT_VERSION:
                    raise ValueError("프롬프트가 변경되었습니다. 사진·스케치로 새 입력 요청을 제출하세요.")
                try:
                    if retry and action in {"prepare", "compare"}:
                        key = next((name for name in ("scale", "reference", "plan")
                                    if pipeline.manifest["stages"].get(name, {}).get("status") == "FAILED"), "plan") if action == "prepare" else "compare"
                        receipt = directory / f"receipts/{key}.json"
                        state = pipeline.manifest["stages"].get(key, {})
                        if (state.get("status") == "FAILED" or action == "compare") and receipt.exists():
                            receipt.rename(receipt.with_name(f"{key}.rejected-{uuid4()}.json"))
                            state.pop("receipt_checksum", None)
                            if (directory / f"{key}.json").exists():
                                shutil.copy2(directory / f"{key}.json", directory / f"{key}.previous-{uuid4()}.json")
                            state["status"] = "PENDING"
                    getattr(self, "_" + action)(pipeline)
                finally:
                    if pipeline.adapter is not None:
                        pipeline.adapter.close()
            job["status"] = "SUCCEEDED"
        except Exception as error:
            job.update(status="FAILED", error_code=type(error).__name__, error=str(error)[:1800] if isinstance(error, ValueError) else "파일 또는 작업 처리 중 오류가 발생했습니다.")
            if isinstance(error, OSError) and error.filename:
                job["error_file"] = Path(error.filename).name
        job.update(finished_at=now(), duration_ms=round((time.monotonic() - start) * 1000))
        write_json(directory / "job.json", job)

    def _prepare(self, p):
        request = p.manifest["request"]
        model_request = {k: request.get(k) for k in ("room_image", "sketch_image", "text", "categories", "requested_region")}
        images = [p.asset(request[k]) for k in ("room_image", "sketch_image") if request.get(k)]
        plan = p.stage("plan", "plan", scene_plan_prompt(model_request), images, lambda value: p.structured("plan", value))
        if plan["decision"] != "ACCEPTED":
            p.manifest["status"] = plan["decision"]
            p.save()
            return
        danawa = p.manifest.get("reference_catalog", {}).get("source") == "danawa"
        templates = [t for t in p.manifest["templates"] if danawa or t["category_code"] == plan["category_code"]]
        if not templates:
            p.manifest["status"] = "TEMPLATE_UNAVAILABLE"
            p.save()
            return
        query_text = search_query(plan)
        query = p.stage("template-query", "embed_text", "task: search result | query: " + query_text, [],
                        lambda value: {"values": normalized_vector(value["values"], p.settings.dimension)})["values"]
        vectors, reused_vectors = {}, {}
        saved_vectors = json.loads(p.asset("inputs/danawa-vectors.json")) if danawa else {}
        for template in templates:
            if danawa:
                vector = normalized_vector(saved_vectors[template["id"]], p.settings.dimension)
                # Replace the raw list instead of retaining two full vector sets.
                saved_vectors[template["id"]] = vector
                reused = True
            else:
                source = p.asset(template["image"])
                prepared, _ = png_bytes(source, resize=True)
                key = checksum(prepared + json.dumps([p.settings.embedding_model, p.settings.dimension, p.settings.preprocessing_version]).encode())
                cache = self.root / "vectors" / f"{key}.json"
                with self.vector_guard:
                    if cache.exists():
                        vector = normalized_vector(read_json(cache)["values"], p.settings.dimension)
                        reused = True
                    else:
                        result = p.stage("template-" + key, "embed", "", [prepared],
                                         lambda value: {"values": normalized_vector(value["values"], p.settings.dimension)})
                        vector = result["values"]
                        write_json(cache, {**result, "model": p.settings.embedding_model, "dimension": p.settings.dimension,
                                          "preprocessing_version": p.settings.preprocessing_version, "checksum": checksum(prepared)})
                        reused = False
            vectors[template["id"]] = vector
            reused_vectors[template["id"]] = reused
        start = time.monotonic()
        candidates = rank_candidates(query, templates, vectors, reused_vectors, plan, limit=6 if danawa else None)
        ranking_ms = round((time.monotonic() - start) * 1000)
        p.manifest["candidate_search"] = {"model": p.settings.embedding_model, "dimension": p.settings.dimension,
                                           "candidates": candidates, "similarity_is_accuracy": False,
                                           "source": "danawa" if danawa else "templates", "searched_count": len(templates),
                                           "query_text": query_text, "local_processing_version": LOCAL_PROCESSING_VERSION,
                                           "ranking": "cosine-desc-id-asc-v1", "ranking_duration_ms": ranking_ms,
                                           "attribute_policy": "diagnostic-only; missing=UNKNOWN"}
        write_json(p.directory / "template_candidates.json", p.manifest["candidate_search"])
        selected = request.get("template_id")
        if selected:
            p.manifest["selected_template"] = next(t for t in templates if t["id"] == selected)
            self._preview(p, plan)
        else:
            p.manifest["status"] = "NEEDS_TEMPLATE_SELECTION"
        p.save()

    def _preview(self, p, plan):
        request = p.manifest["request"]
        template = p.manifest["selected_template"]
        structure = self._reference_structure(p)
        scale_request = {k: request.get(k) for k in ("text", "requested_region")}
        scale_plan = p.stage("scale", "scale", scale_plan_prompt(scale_request, plan, structure),
                             [p.asset(request["room_image"]), p.asset(template["image"])],
                             lambda value: p.structured("scale", value))
        refs = []
        mapped_request = {k: copy.deepcopy(request.get(k)) for k in ("room_image", "sketch_image", "text", "categories", "requested_region")}
        send_sketch = bool(request.get("sketch_image") and plan.get("sketch_role") == "FURNITURE_SHAPE")
        if not send_sketch:
            mapped_request["sketch_image"] = None
        roles = [(key, mapped_request[key]) for key in ("room_image", "sketch_image") if mapped_request.get(key)] + [("template", template["image"])]
        for index, (role, source) in enumerate(roles):
            data, mapping = generation_reference(p.asset(source))
            target = f"generation_refs/reference-{index}.png"
            if target not in p.manifest["assets"]:
                p.persist_asset(target, data, mapping["size"])
            refs.append({"role": role, "order": index, "source": source, "source_checksum": checksum(p.asset(source)),
                         "api_asset": target, "api_checksum": checksum(data), **mapping})
        box = request.get("requested_region") or plan.get("placement_region")
        region_source = "request" if request.get("requested_region") else "plan" if plan.get("placement_region") else "placement_intent_only"
        mapped_request["requested_region"] = map_region(box, refs[0])
        bounds = api_scale_bounds(scale_plan, refs[0])
        generation_plan = {**plan, "scale_plan": scale_plan, "api_scale_bounds": bounds}
        prompt = generation_prompt(mapped_request, generation_plan, template, structure)
        preview = {"provider": p.settings.image_provider, "model": p.settings.image_model,
                   "width": p.settings.output_width, "height": p.settings.output_height, "prompt": prompt,
                   "images": refs, "api_requested_region": mapped_request.get("requested_region"),
                   "original_placement_region": copy.deepcopy(box), "placement_region_source": region_source,
                   "sketch_policy": {"role": plan.get("sketch_role", "UNCERTAIN"), "sent_to_generator": send_sketch,
                                     "original_preserved": bool(request.get("sketch_image"))},
                   "template_id": template["id"], "template_version": template["version"],
                   "reference_structure": structure, "prompt_version": WEB_PROMPT_VERSION,
                   "scale_plan": scale_plan, "api_scale_bounds": bounds,
                   "preprocessing_version": p.settings.generation_preprocessing_version,
                   "local_processing_version": LOCAL_PROCESSING_VERSION}
        write_json(p.directory / "generation_request.json", preview)
        p.manifest["preview_checksum"] = checksum((p.directory / "generation_request.json").read_bytes())
        p.manifest["status"] = "READY_TO_GENERATE"

    def _reference_structure(self, p):
        data = p.asset(p.manifest["selected_template"]["image"])
        metadata = {"image_checksum": checksum(data), "model": p.settings.analysis_model,
                    "prompt_version": REFERENCE_PROMPT_VERSION, "demo": self.demo,
                    "schema": ReferenceStructure.model_json_schema()}
        key = checksum(json.dumps(metadata, sort_keys=True).encode())
        cache = self.root / "reference-structures" / f"{key}.json"
        with self.reference_guard:
            if cache.exists() and p.manifest["stages"].get("reference", {}).get("status") != "SUCCEEDED":
                saved = read_json(cache)
                if saved["metadata"] != metadata or checksum(json.dumps(saved["structure"], sort_keys=True).encode()) != saved["checksum"]:
                    raise ValueError("참조 구조 캐시의 checksum이 다릅니다.")
                structure = ReferenceStructure.model_validate(saved["structure"]).model_dump(mode="json")
                output = p.directory / "reference.json"
                write_json(output, structure)
                p.manifest["stages"]["reference"] = {"status": "SUCCEEDED", "attempts": [], "output": "reference.json",
                    "output_checksum": checksum(output.read_bytes()), "cache_key": key, "cache_reused": True}
                p.save()
            structure = p.stage("reference", "reference", reference_structure_prompt(), [data],
                                lambda value: p.structured("reference", value))
            if not cache.exists():
                write_json(cache, {"metadata": metadata, "structure": structure,
                                  "checksum": checksum(json.dumps(structure, sort_keys=True).encode())})
            return structure

    def _generate(self, p):
        path = p.directory / "generation_request.json"
        if not path.exists() or checksum(path.read_bytes()) != p.manifest.get("preview_checksum"):
            raise ValueError("템플릿을 선택하고 생성 요청을 먼저 확인하세요.")
        preview = read_json(path)
        if len(preview["images"]) > 4:
            raise ValueError("참조 이미지는 최대 4장입니다.")
        def store(value):
            data, size = png_bytes(base64.b64decode(value["image_base64"], validate=True))
            if size != (p.settings.output_width, p.settings.output_height):
                raise ValueError("요청한 출력 크기와 생성 이미지 크기가 다릅니다.")
            p.persist_asset("generated.png", data, size)
            return {"image": "generated.png", "size": size}
        p.stage("generate", "generate", preview["prompt"], [p.asset(r["api_asset"]) for r in preview["images"]], store)
        p.manifest["status"] = "GENERATED"
        p.save()
        self._compare(p)

    def _compare(self, p):
        request = p.manifest["request"]
        plan = read_json(p.directory / "plan.json")
        structure = self._reference_structure(p)
        preview_path = p.directory / "generation_request.json"
        if preview_path.exists() and checksum(preview_path.read_bytes()) != p.manifest.get("preview_checksum"):
            raise ValueError("고정된 생성 요청·크기 계획의 checksum이 다릅니다.")
        preview = read_json(preview_path) if preview_path.exists() else {}
        scale_plan = (preview.get("scale_plan") if p.manifest.get("web_prompt_version")
                      in {WEB_PROMPT_VERSION, "scene-context-scale-v4"} else None)
        bounds = preview.get("api_scale_bounds") if scale_plan else None
        def store(value):
            raw = p.structured("compare", value)
            start = time.monotonic()
            result = assess_comparison(raw, scale_plan, bounds, plan["category_code"])
            result["local_processing"]["duration_ms"] = round((time.monotonic() - start) * 1000)
            for obj in result["objects"]:
                obj.update(id=str(uuid4()), reference_asset_id=p.manifest["assets"]["generated.png"]["asset_id"],
                           reference_size=p.manifest["assets"]["generated.png"]["size"])
            return result
        model_request = {k: request.get(k) for k in ("room_image", "sketch_image", "text", "categories", "requested_region")}
        result = p.stage("compare", "compare", comparison_prompt(model_request, {**plan, "scale_plan": scale_plan, "api_scale_bounds": bounds}, p.manifest["selected_template"], structure),
                         [p.asset(request["room_image"]), p.asset("generated.png"), p.asset(p.manifest["selected_template"]["image"])], store)
        fidelity = result.get("reference_fidelity", {"status": "UNCERTAIN"})
        annotations = result.get("annotation_check", {"status": "UNCERTAIN"})
        p.manifest["status"] = result.get("local_processing", {}).get("status") or ("NEEDS_ANNOTATION_REVIEW" if annotations["status"] != "CLEAR" else
                                "NEEDS_STRUCTURE_REVIEW" if fidelity["status"] != "MATCH" else
                                "NEEDS_SCALE_REVIEW" if result["scale_check"]["status"] != "APPROPRIATE" else
                                "COMPLETED" if result["objects"] else "NO_OBJECTS")
        p.save()

    def state(self, run_id, owner):
        directory = self.directory(run_id, owner)
        m = read_json(directory / "manifest.json")
        output = {"run_id": run_id, "status": m["status"], "revision_no": m["request"]["revision_no"],
                  "parent_run_id": m["request"].get("parent_run_id"), "demo": self.demo,
                  "room_image": m["request"]["room_image"], "sketch_image": m["request"].get("sketch_image"),
                  "text": m["request"]["text"], "categories": m["request"]["categories"],
                  "external_project": m["request"].get("external_project"),
                  "sketch_strokes": m["request"].get("sketch_strokes", []),
                  "candidates": m.get("candidate_search", {}).get("candidates", []), "stages": m["stages"],
                  "local_sample": True, "bronze_verified": False}
        output["reference_catalog"] = m.get("reference_catalog", {"source": "templates"})
        output["selected_template_id"] = (m.get("selected_template") or {}).get("id")
        for key, name in (("plan", "plan.json"), ("preview", "generation_request.json"), ("comparison", "compare.json")):
            if (directory / name).exists():
                output[key] = read_json(directory / name)
        if "generated.png" in m["assets"]:
            output["result_image"] = "generated.png"
        if (directory / "job.json").exists():
            job = read_json(directory / "job.json")
            if job["status"] == "RUNNING" and job["instance"] != self.instance:
                job.update(status="FAILED", error="서버가 재시작되었습니다. 해당 단계를 재시도하세요.")
            output["job"] = job
        return output
