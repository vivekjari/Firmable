from __future__ import annotations

import argparse
import random
from pathlib import Path


def sample_jsonl_records(
    input_dir: Path,
    output_file: Path,
    sample_size: int = 100,
    seed: int | None = None,
) -> tuple[int, int]:
    """
    Sample JSONL records from all files in a directory using reservoir sampling.

    Returns:
        (total_records_seen, sampled_records_written)
    """
    files = sorted(input_dir.glob("*.jsonl"))
    if not files:
        raise FileNotFoundError(f"No JSONL files found in: {input_dir}")

    rng = random.Random(seed)
    reservoir: list[str] = []
    seen = 0

    for file_path in files:
        with file_path.open("r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue

                seen += 1
                if len(reservoir) < sample_size:
                    reservoir.append(line)
                else:
                    idx = rng.randint(0, seen - 1)
                    if idx < sample_size:
                        reservoir[idx] = line

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as f:
        for line in reservoir:
            f.write(line + "\n")

    return seen, len(reservoir)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Randomly sample JSONL records across all JSONL files in a folder."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("Datasets-2025-08-08"),
        help="Folder containing source .jsonl files",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=Path("data/sample_100.jsonl"),
        help="Output JSONL path for sampled records",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100,
        help="Number of records to sample",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed (omit for a different sample each run)",
    )
    args = parser.parse_args()

    if args.sample_size <= 0:
        raise ValueError("--sample-size must be a positive integer")

    total_seen, sampled = sample_jsonl_records(
        input_dir=args.input_dir,
        output_file=args.output_file,
        sample_size=args.sample_size,
        seed=args.seed,
    )
    print(f"Scanned {total_seen} total records from: {args.input_dir}")
    print(f"Wrote {sampled} sampled records to: {args.output_file}")


if __name__ == "__main__":
    main()
