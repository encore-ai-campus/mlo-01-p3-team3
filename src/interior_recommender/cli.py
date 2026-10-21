import argparse
import json
import sys
from pathlib import Path

from .adapters.router import ModelAdapter
from .config import Settings, load_environment
from .pipeline import Pipeline, create_run
from .catalog.batch_embedding import BatchEmbedding, create_batch, inspect_directory


def main(argv=None):
    parser = argparse.ArgumentParser(description="Local Gemini analysis/embedding and Cloudflare generation; no Bronze/service integration")
    sub = parser.add_subparsers(dest="command", required=True)
    start = sub.add_parser("start", help="Snapshot new input and execute through object analysis")
    start.add_argument("--request", type=Path, required=True)
    start.add_argument("--templates", type=Path, default=Path(__file__).resolve().parent / "templates" / "manifest.json")
    start.add_argument("--output-root", type=Path, default=Path("outputs"))
    start.add_argument("--until", choices=["interpret", "generate", "analyze"], default="analyze")
    batch = sub.add_parser("embed-directory", help="Embed PNG/JPEG files recursively; reuse completed results on rerun")
    batch.add_argument("--input-dir", type=Path, required=True)
    batch.add_argument("--output-dir", type=Path, required=True)
    batch.add_argument("--limit", type=int, help="Process only the first N image files of the snapshot")
    batch.add_argument("--requests-per-minute", type=int, default=10, help="Local pacing, not a provider quota guarantee (1-60)")
    batch.add_argument("--dry-run", action="store_true", help="Validate images without API calls or writing output")
    for command in ("resume", "interpret", "generate", "analyze", "embed"):
        child = sub.add_parser(command, help="Reuse completed prerequisite stages in a saved run")
        child.add_argument("--run-dir", type=Path, required=True)
        if command == "resume":
            child.add_argument("--until", choices=["interpret", "generate", "analyze", "embed"], default="analyze")
        child.add_argument("--object-id", help="Explicit object UUID from analyze.json; required for embed")
        child.add_argument("--retry-rejected", action="store_true", help="Archive a failed stage response and allow a new paid call")
    args = parser.parse_args(argv)
    try:
        # Resume keeps saved model settings, but still needs credentials from .env.
        load_environment()
        if args.command == "embed-directory":
            if args.limit is not None and args.limit < 1:
                raise ValueError("--limit must be positive")
            if not 1 <= args.requests_per_minute <= 60:
                raise ValueError("--requests-per-minute must be between 1 and 60")
            if args.dry_run:
                result = inspect_directory(args.input_dir.resolve())
                print(json.dumps(result, ensure_ascii=False))
                return 0 if not result["errors"] else 2
            settings = Settings.from_env()
            directory = args.output_dir.resolve()
            if not (directory / "manifest.json").exists():
                create_batch(args.input_dir, directory, settings)
            result = BatchEmbedding(directory, ModelAdapter).run(args.input_dir, settings, limit=args.limit,
                                                                requests_per_minute=args.requests_per_minute)
            print(json.dumps(result, ensure_ascii=False))
            return 0 if result["status"] in {"SUCCEEDED", "PARTIAL"} else 2
        if args.command == "start":
            directory = create_run(args.request, args.templates, args.output_root, Settings.from_env())
            print("run_dir=" + str(directory), flush=True)
            until, object_id = args.until, None
        else:
            directory = args.run_dir
            until = args.until if args.command == "resume" else args.command
            object_id = args.object_id
            if until == "embed" and not object_id:
                raise ValueError("--object-id is required for embedding")
        result = Pipeline(directory, ModelAdapter).execute(until, object_id, getattr(args, "retry_rejected", False))
        print(json.dumps({"run_dir": str(directory.resolve()), "status": result["status"]}, ensure_ascii=False))
        return 0 if result["status"] not in {"FAILED", "UNSUPPORTED", "BUDGET_WAIT"} else 2
    except (ValueError, OSError, KeyError) as error:
        # SDK errors are sanitized by Pipeline; local errors contain no API credentials.
        print("Error: " + str(error), file=sys.stderr)
        return 2
