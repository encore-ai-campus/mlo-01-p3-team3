"""Sequential file embeddings with immutable inputs and resumable per-image stages."""
import time
from pathlib import Path
from uuid import uuid4

from PIL import UnidentifiedImageError

from .assets import checksum, png_bytes, read_json, write_bytes, write_json
from .pipeline import Pipeline, contract_hash, now, run_lock
from .schemas import normalized_vector
from .services import PROMPT_VERSION, SCHEMA_VERSION


def image_files(directory):
    directory = Path(directory).resolve()
    if not directory.is_dir():
        raise ValueError("Input image directory does not exist: " + str(directory))
    return sorted((p for p in directory.rglob("*") if p.is_file() and not p.is_symlink()
                   and p.suffix.lower() in {".jpg", ".jpeg", ".png"}),
                  key=lambda p: p.relative_to(directory).as_posix())


def prepared_image(path):
    data = path.read_bytes()
    image, size = png_bytes(data, resize=True)
    return image, size, checksum(data)


def inspect_directory(directory):
    files = image_files(directory)
    valid, errors, hashes = 0, [], set()
    for path in files:
        try:
            _, _, original_hash = prepared_image(path)
            valid += 1
            hashes.add(original_hash)
        except (OSError, ValueError, UnidentifiedImageError) as error:
            errors.append({"file": path.relative_to(directory).as_posix(), "error_code": type(error).__name__})
    return {"status": "DRY_RUN", "total_images": len(files), "valid_images": valid,
            "unique_images": len(hashes), "duplicate_images": valid - len(hashes), "errors": errors,
            "api_calls": 0}


def create_batch(input_dir, output_dir, settings):
    input_dir, output_dir = Path(input_dir).resolve(), Path(output_dir).resolve()
    if input_dir == output_dir or input_dir in output_dir.parents:
        raise ValueError("Output directory must be outside the input image directory")
    files = image_files(input_dir)
    if not files:
        raise ValueError("No PNG/JPEG images were found")
    if output_dir.exists():
        raise ValueError("Output directory already exists without a batch manifest; use a new directory")
    output_dir.mkdir(parents=True)
    assets, records, errors = {}, [], []
    with run_lock(output_dir):
        for path in files:
            relative = path.relative_to(input_dir).as_posix()
            try:
                prepared, size, source_hash = prepared_image(path)
            except (OSError, ValueError, UnidentifiedImageError) as error:
                errors.append({"file": relative, "error_code": type(error).__name__})
                continue
            name = f"inputs/{source_hash}.png"
            if name not in assets:
                write_bytes(output_dir / name, prepared)
                assets[name] = {"asset_id": str(uuid4()), "checksum": checksum(prepared),
                                "source_checksum": source_hash, "size": list(size)}
            records.append({"file": relative, "source_checksum": source_hash, "asset": name,
                            "stage": "embed-" + source_hash})
        manifest = {"run_id": str(uuid4()), "created_at": now(), "mode": "LOCAL_IMAGE_BATCH",
                    "bronze_verified": False, "schema_version": SCHEMA_VERSION, "prompt_version": PROMPT_VERSION,
                    "settings": settings.model_dump(), "request": {"input_dir": str(input_dir), "files": records,
                    "input_errors": errors}, "templates": [], "assets": assets, "stages": {},
                    "status": "PENDING", "selected_template": None}
        manifest["input_hash"] = contract_hash(manifest)
        write_json(output_dir / "manifest.json", manifest)
    return output_dir


class BatchEmbedding(Pipeline):
    def run(self, input_dir, current_settings, *, limit=None, requests_per_minute=10, progress=print):
        with run_lock(self.directory):
            self.manifest = read_json(self.directory / "manifest.json")
            if self.manifest.get("mode") != "LOCAL_IMAGE_BATCH":
                raise ValueError("Output directory belongs to a different kind of run")
            if contract_hash(self.manifest) != self.manifest["input_hash"]:
                raise ValueError("Batch snapshot was modified; use a new output directory")
            if self.manifest["request"]["input_dir"] != str(Path(input_dir).resolve()):
                raise ValueError("Input directory differs from the saved batch")
            for name in ("embedding_model", "dimension", "preprocessing_version"):
                if getattr(current_settings, name) != getattr(self.settings, name):
                    raise ValueError("Embedding settings changed; use a new output directory")
            last_call = None
            records = self.manifest["request"]["files"]
            selected = records[:limit] if limit else records
            try:
                for index, record in enumerate(selected, start=1):
                    key, name = record["stage"], record["asset"]
                    data = self.asset(name)
                    cached = self.manifest["stages"].get(key, {}).get("status") == "SUCCEEDED"
                    has_receipt = (self.directory / f"receipts/{key}.json").exists()
                    if not cached and not has_receipt:
                        if last_call is not None:
                            delay = 60 / requests_per_minute - (time.monotonic() - last_call)
                            if delay > 0:
                                time.sleep(delay)
                        last_call = time.monotonic()

                    def vector(payload):
                        return {"asset_id": self.manifest["assets"][name]["asset_id"],
                                "image_checksum": record["source_checksum"], "preprocessed_checksum": checksum(data),
                                "model": self.settings.embedding_model, "dimension": self.settings.dimension,
                                "preprocessing_version": self.settings.preprocessing_version, "normalization": "L2",
                                "values": normalized_vector(payload["values"], self.settings.dimension)}

                    self.stage(key, "embed", "", [data], vector)
                    progress(f"[{index}/{len(records)}] {'CACHED' if cached else 'EMBEDDED'} {record['file']}")
            except Exception:
                self.manifest["status"] = "FAILED"
                self.save_index()
                self.save()
                raise
            finally:
                if self.adapter is not None:
                    self.adapter.close()
                    self.adapter = None
            complete = sum(self.manifest["stages"].get(r["stage"], {}).get("status") == "SUCCEEDED" for r in records)
            self.manifest["status"] = ("SUCCEEDED" if complete == len(records) and not self.manifest["request"]["input_errors"]
                                       else "COMPLETED_WITH_INPUT_ERRORS" if complete == len(records) else "PARTIAL")
            self.save_index()
            self.save()
            return {"status": self.manifest["status"], "total_images": len(records) + len(self.manifest["request"]["input_errors"]),
                    "completed_images": complete, "input_errors": len(self.manifest["request"]["input_errors"]),
                    "output_dir": str(self.directory)}

    def save_index(self):
        records = []
        for record in self.manifest["request"]["files"]:
            state = self.manifest["stages"].get(record["stage"], {})
            records.append({**record, "status": state.get("status", "PENDING"),
                            "embedding_file": state.get("output"), "error_code": state.get("error_code"),
                            "next_retry_at": state.get("next_retry_at")})
        write_json(self.directory / "index.json", {"input_dir": self.manifest["request"]["input_dir"],
                   "model": self.settings.embedding_model, "dimension": self.settings.dimension,
                   "preprocessing_version": self.settings.preprocessing_version, "files": records,
                   "input_errors": self.manifest["request"]["input_errors"]})
