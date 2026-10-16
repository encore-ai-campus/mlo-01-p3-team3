"""Read-only reuse of embed-directory snapshots for the local reference-image MVP."""
from pathlib import Path

from ..assets import checksum, read_json
from ..schemas import normalized_vector


class DanawaCatalog:
    def __init__(self, index_path):
        self.path = Path(index_path).resolve()
        self.root = self.path.parent
        self.index = read_json(self.path)
        manifest = read_json(self.root / "manifest.json")
        self.templates = []
        self.vectors = {}
        self.image_checksums = {}
        self.skipped = 0
        for item in self.index["files"]:
            if item.get("status") != "SUCCEEDED":
                self.skipped += 1
                continue
            try:
                embedding_path = self.inside(item["embedding_file"])
                image_path = self.inside(item["asset"])
                raw_vector = embedding_path.read_bytes()
                stage = manifest["stages"][item["stage"]]
                if stage["status"] != "SUCCEEDED" or checksum(raw_vector) != stage["output_checksum"]:
                    raise ValueError("Danawa embedding checksum mismatch")
                embedding = read_json(embedding_path)
                for key in ("model", "dimension", "preprocessing_version"):
                    if embedding[key] != self.index[key]:
                        raise ValueError("Danawa embedding metadata mismatch")
                image_hash = checksum(image_path.read_bytes())
                if (image_hash != manifest["assets"][item["asset"]]["checksum"]
                        or image_hash != embedding["preprocessed_checksum"]
                        or embedding["image_checksum"] != item["source_checksum"]):
                    raise ValueError("Danawa image checksum mismatch")
                template_id = "danawa-" + item["source_checksum"]
                if template_id in self.vectors:
                    continue
                self.vectors[template_id] = normalized_vector(embedding["values"], self.index["dimension"])
                self.image_checksums[template_id] = image_hash
                self.templates.append({"id": template_id, "version": 1, "category_code": "UNCLASSIFIED",
                                       "image": str(image_path), "review_status": "APPROVED", "active": True,
                                       "provenance": "Danawa image reference: " + item["file"],
                                       "attributes": {"source": "danawa", "category_verified": "false"}})
            except (FileNotFoundError, KeyError, ValueError, TypeError):
                self.skipped += 1
        if not self.templates:
            raise ValueError("다나와 인덱스에 검증 가능한 완료 벡터가 없습니다.")

    def inside(self, name):
        path = (self.root / name).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError("Danawa index path is outside its snapshot")
        return path

    def validate_settings(self, settings):
        expected = (settings.embedding_model, settings.dimension, settings.preprocessing_version)
        actual = tuple(self.index[key] for key in ("model", "dimension", "preprocessing_version"))
        if actual != expected:
            raise ValueError("다나와 인덱스의 임베딩 모델·차원·전처리가 현재 설정과 다릅니다.")

    def info(self):
        return {"source": "danawa", "count": len(self.templates), "skipped": self.skipped,
                "model": self.index["model"], "dimension": self.index["dimension"],
                "category_verified": False}
