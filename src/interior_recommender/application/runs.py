"""Read direct runs without loading a catalog, creating an executor or calling AI."""
from pathlib import Path
from uuid import UUID

from ..assets import checksum, read_json
from ..domain.product_links import comparison_url


class DirectRunReader:
    def __init__(self, root, demo=False):
        self.root, self.demo = Path(root).resolve(), demo

    def directory(self, run_id, owner):
        directory = self.root / "runs" / str(UUID(str(run_id)))
        manifest = read_json(directory / "manifest.json")
        if manifest["request"].get("owner_id") != owner:
            raise PermissionError("Run owner mismatch")
        return directory

    def state(self, run_id, owner):
        directory = self.directory(run_id, owner)
        manifest = read_json(directory / "manifest.json")
        request = manifest["request"]
        output = {"run_id": str(run_id), "status": manifest["status"], "flow": "direct", "demo": self.demo,
                  "local_sample": True, "bronze_verified": False, "contract": manifest["direct_contract"],
                  "stages": manifest["stages"], "revision_no": request["revision_no"]}
        for key in ("text", "categories", "external_project", "sketch_strokes", "room_image", "sketch_image"):
            output[key] = request.get(key)
        for key in ("room_image", "sketch_image"):
            if output.get(key) not in manifest["assets"]:
                output[key] = None
        for key, stage_name, filename in (("context", "context", "context.json"),
                                           ("recommendations", "ranking", "recommendations.json")):
            stage = manifest["stages"].get(stage_name, {})
            if stage.get("status") == "SUCCEEDED":
                path = directory / filename
                if checksum(path.read_bytes()) != stage["output_checksum"]:
                    raise ValueError("Saved output checksum mismatch")
                output[key] = read_json(path)
        # Old file-backed runs stay readable without interpreting another
        # process's instance ID as a failure. New jobs are overlaid by the API.
        if (directory / "job.json").exists():
            output["job"] = read_json(directory / "job.json")
        return output

    def product_link(self, run_id, owner, image_id):
        result = self.state(run_id, owner).get("recommendations", {})
        item = next((item for item in result.get("items", []) + result.get("unverified_items", [])
                     if item["image_id"] == image_id), None)
        if item is None:
            raise FileNotFoundError
        metadata = item.get("metadata", {})
        url = comparison_url(metadata.get("comparison_url"), item["product_id"])
        if not url or not metadata.get("metadata_source"):
            raise FileNotFoundError
        return url
