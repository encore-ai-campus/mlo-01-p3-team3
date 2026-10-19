"""Submit with spark-submit using an environment containing this package."""
from product_pipeline.bronze.cli import main

if __name__ == '__main__':
    raise SystemExit(main())
