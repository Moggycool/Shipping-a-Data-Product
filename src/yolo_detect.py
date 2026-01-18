# src/yolo_detect.py
"""
Run YOLOv8 object detection on Telegram-scraped images and save results to CSV.

Task 3 requirements satisfied:
- Scans images in: data/raw/images/{channel_name}/{message_id}.jpg
- Runs YOLOv8 nano: yolov8n.pt
- Records detected objects with confidence scores + bounding boxes
- Saves results to: data/processed/yolo/detections.csv
- Classifies each image into:
    promotional: person + bottle (product proxy)
    product_display: bottle only
    lifestyle: person only
    other: neither (or unreadable image)

Robustness improvements (fixes OpenCV imdecode !buf.empty crash):
- Skips zero-byte / unreadable / corrupted images safely
- Wraps inference in try/except so one bad image doesn't stop the run
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
from tqdm import tqdm
from ultralytics import YOLO

# Optional: stronger image integrity checks (recommended).
# If Pillow isn't installed, the script will fall back to size-only checks.
try:
    from PIL import Image  # type: ignore

    PIL_AVAILABLE = True
except Exception:
    PIL_AVAILABLE = False


IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}

# Task 1 expected image layout:
# data/raw/images/{channel_name}/{message_id}.jpg
DEFAULT_IMAGES_ROOT = Path("data") / "raw" / "images"
DEFAULT_OUT_CSV = Path("data") / "processed" / "yolo" / "detections.csv"

# Product proxy classes for generic YOLO (keep conservative to reduce false positives)
PRODUCT_PROXY_CLASSES = {"bottle"}

ALLOWED_CATEGORIES = {"promotional", "product_display", "lifestyle", "other"}


@dataclass
class DetectionRow:
    """A single output row (one row per detection; also emits one row for no-detection images)."""

    image_path: str
    channel_name: Optional[str]
    message_id: Optional[int]
    detected_class: Optional[str]
    confidence_score: Optional[float]
    bbox_x1: Optional[float]
    bbox_y1: Optional[float]
    bbox_x2: Optional[float]
    bbox_y2: Optional[float]
    image_category: str
    model_name: str
    inference_ts: str


def iter_images(root: Path) -> Iterable[Path]:
    """Recursively yield image file paths under root."""
    if not root.exists():
        return
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            p = Path(dirpath) / fn
            if p.suffix.lower() in IMAGE_EXTS:
                yield p


def parse_channel_and_message_id(image_path: Path, images_root: Path) -> tuple[Optional[str], Optional[int]]:
    """
    Parse channel name and message ID from image path.

    Expected: data/raw/images/{channel_name}/{message_id}.jpg
    So relative path should be: <channel_name>/<message_id>.jpg
    """
    try:
        rel = image_path.relative_to(images_root)
        parts = rel.parts
        if len(parts) >= 2:
            channel = parts[0]
            stem = Path(parts[1]).stem  # filename without extension

            # message_id should be numeric
            if re.fullmatch(r"\d+", stem):
                return channel, int(stem)

            # fallback: extract first numeric group if name is like msg_123.jpg
            m = re.search(r"(\d+)", stem)
            return channel, int(m.group(1)) if m else None
    except Exception:
        pass
    return None, None


def categorize(detected_classes: set[str]) -> str:
    """Categorize image based on detected classes."""
    has_person = "person" in detected_classes
    has_product = len(detected_classes.intersection(PRODUCT_PROXY_CLASSES)) > 0

    if has_person and has_product:
        return "promotional"
    if (not has_person) and has_product:
        return "product_display"
    if has_person and (not has_product):
        return "lifestyle"
    return "other"


def is_readable_image(path: Path) -> bool:
    """
    Return True if the file looks like a valid image.

    - Always checks: exists, is file, non-empty
    - If Pillow is available: verifies image integrity (catches corrupted JPEG/PNG)
    """
    try:
        if not (path.exists() and path.is_file()):
            return False
        if path.stat().st_size <= 0:
            return False

        if PIL_AVAILABLE:
            # verify() is fast and catches many corruptions without decoding fully
            with Image.open(path) as im:
                im.verify()

        return True
    except Exception:
        return False


def _append_no_detection_row(
    rows: list[DetectionRow],
    img_path: Path,
    channel_name: Optional[str],
    message_id: Optional[int],
    image_category: str,
    model_name: str,
    inference_ts: str,
) -> None:
    """Append a placeholder row for images with no detections or unreadable images."""
    rows.append(
        DetectionRow(
            image_path=img_path.as_posix(),
            channel_name=channel_name,
            message_id=message_id,
            detected_class=None,
            confidence_score=None,
            bbox_x1=None,
            bbox_y1=None,
            bbox_x2=None,
            bbox_y2=None,
            image_category=image_category,
            model_name=model_name,
            inference_ts=inference_ts,
        )
    )


def run(
    images_root: Path = DEFAULT_IMAGES_ROOT,
    out_csv: Path = DEFAULT_OUT_CSV,
    model_path: str = "yolov8n.pt",
    conf: float = 0.25,
) -> Path:
    """Run YOLO object detection on images and save results to CSV."""
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    model = YOLO(model_path)  # auto-downloads weights on first run
    model_name = Path(model_path).stem
    inference_ts = datetime.now(timezone.utc).isoformat()

    image_paths = list(iter_images(images_root))
    if not image_paths:
        raise FileNotFoundError(
            f"No images found under: {images_root.resolve()} "
            f"(expected: data/raw/images/<channel>/<message_id>.jpg)"
        )

    rows: list[DetectionRow] = []
    skipped_unreadable = 0
    skipped_inference_errors = 0

    for img_path in tqdm(image_paths, desc="Running YOLO detections"):
        channel_name, message_id = parse_channel_and_message_id(
            img_path, images_root)

        # Prevent OpenCV imdecode !buf.empty by skipping bad files early
        if not is_readable_image(img_path):
            skipped_unreadable += 1
            _append_no_detection_row(
                rows=rows,
                img_path=img_path,
                channel_name=channel_name,
                message_id=message_id,
                image_category="other",
                model_name=model_name,
                inference_ts=inference_ts,
            )
            continue

        # Run inference safely (one broken image should not crash the whole job)
        try:
            results = model.predict(source=str(
                img_path), conf=conf, verbose=False)
        except Exception as e:
            skipped_inference_errors += 1
            _append_no_detection_row(
                rows=rows,
                img_path=img_path,
                channel_name=channel_name,
                message_id=message_id,
                image_category="other",
                model_name=model_name,
                inference_ts=inference_ts,
            )
            print(
                f"[WARN] Inference failed, skipping: {img_path} -> {type(e).__name__}: {e}")
            continue

        r = results[0]
        names = r.names  # class_id -> class_name

        detected_classes: set[str] = set()
        dets: list[tuple[str, float, float, float, float, float]] = []

        if r.boxes is not None and len(r.boxes) > 0:
            for b in r.boxes:
                cls_id = int(b.cls.item())
                cls_name = names[cls_id]
                score = float(b.conf.item())
                x1, y1, x2, y2 = [float(v) for v in b.xyxy[0].tolist()]

                detected_classes.add(cls_name)
                dets.append((cls_name, score, x1, y1, x2, y2))

        image_category = categorize(detected_classes)

        if dets:
            for cls_name, score, x1, y1, x2, y2 in dets:
                rows.append(
                    DetectionRow(
                        image_path=img_path.as_posix(),
                        channel_name=channel_name,
                        message_id=message_id,
                        detected_class=cls_name,
                        confidence_score=score,
                        bbox_x1=x1,
                        bbox_y1=y1,
                        bbox_x2=x2,
                        bbox_y2=y2,
                        image_category=image_category,
                        model_name=model_name,
                        inference_ts=inference_ts,
                    )
                )
        else:
            _append_no_detection_row(
                rows=rows,
                img_path=img_path,
                channel_name=channel_name,
                message_id=message_id,
                image_category=image_category,
                model_name=model_name,
                inference_ts=inference_ts,
            )

    df = pd.DataFrame([r.__dict__ for r in rows])

    # Safety: enforce required categories
    bad = set(df["image_category"].dropna().unique()) - ALLOWED_CATEGORIES
    if bad:
        raise ValueError(f"Unexpected image_category values: {bad}")

    df.to_csv(out_csv, index=False)

    print(f"[OK] Wrote YOLO detections CSV: {out_csv} (rows={len(df)})")
    print(f"[INFO] Images scanned: {len(image_paths)}")
    print(f"[INFO] Skipped unreadable images: {skipped_unreadable}")
    print(f"[INFO] Skipped inference errors: {skipped_inference_errors}")
    if not PIL_AVAILABLE:
        print("[INFO] Pillow not installed; using size-only image checks. "
              "For better corruption detection: pip install pillow")

    return out_csv


if __name__ == "__main__":
    run(
        images_root=DEFAULT_IMAGES_ROOT,
        out_csv=DEFAULT_OUT_CSV,
        model_path="yolov8n.pt",
        conf=0.25,
    )
