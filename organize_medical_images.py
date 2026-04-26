from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import re
import shutil
import struct
import sys
import unicodedata
import warnings
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import pydicom
except ImportError:  # pragma: no cover - optional dependency
    pydicom = None

warnings.filterwarnings(
    "ignore",
    message=r"Invalid value for VR UI: .*",
    category=UserWarning,
    module=r"pydicom\..*",
)


class ArgumentHelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawTextHelpFormatter):
    pass


DICOM_EXTENSIONS = {".dcm", ".dicom", ".ima"}
NIFTI_EXTENSIONS = {".nii", ".nii.gz"}
ANALYZE_EXTENSIONS = {".hdr"}
METAIMAGE_EXTENSIONS = {".mha", ".mhd"}
NRRD_EXTENSIONS = {".nrrd", ".nhdr"}
SUPPORTED_EXTENSIONS = (
    DICOM_EXTENSIONS | NIFTI_EXTENSIONS | ANALYZE_EXTENSIONS | METAIMAGE_EXTENSIONS | NRRD_EXTENSIONS
)
IGNORED_EXTENSIONS = {
    ".txt",
    ".md",
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".csv",
    ".tsv",
    ".json",
    ".yaml",
    ".yml",
    ".py",
    ".pyc",
    ".ipynb",
    ".zip",
    ".rar",
    ".7z",
    ".tar",
    ".gz",
    ".jpg",
    ".jpeg",
    ".png",
    ".bmp",
    ".gif",
    ".svg",
    ".xml",
    ".html",
    ".htm",
    ".log",
    ".tmp",
    ".db",
    ".sqlite",
}
IGNORED_NAMES = {"dicomdir"}
PATH_SAFE_PATTERN = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
SPACE_PATTERN = re.compile(r"\s+")
SEPARATOR_PATTERN = re.compile(r"[_-]{2,}")
VENDOR_RULES = (
    ("siemens", "Siemens"),
    ("syngo", "Siemens"),
    ("philips", "Philips"),
    ("ingenia", "Philips"),
    ("achieva", "Philips"),
    ("ge medical", "GE"),
    ("ge healthcare", "GE"),
    ("signa", "GE"),
    ("discovery", "GE"),
    ("canon", "Canon"),
    ("toshiba", "Canon"),
    ("uih", "UnitedImaging"),
    ("united imaging", "UnitedImaging"),
    ("联影", "UnitedImaging"),
)


@dataclass(frozen=True)
class ExtraAsset:
    source_path: Path
    relative_name: str


@dataclass
class ImageRecord:
    source_path: Path
    format_name: str
    patient_id: str
    manufacturer: str
    study_date: str
    study_description: str
    study_uid: str
    series_uid: str
    series_number: int | None
    series_description: str
    protocol_name: str
    sequence_name: str
    modality: str
    body_part: str
    instance_number: int | None
    acquisition_number: int | None
    image_type_text: str = ""
    scanning_sequence: str = ""
    sequence_variant: str = ""
    embedded_slice_count: int = 1
    embedded_frame_count: int = 1
    slice_signature: str | None = None
    slice_sort_value: tuple[float, ...] | None = None
    temporal_signature: str | None = None
    temporal_sort_value: float | None = None
    sop_instance_uid: str = ""
    source_patient_hint: str = ""
    source_series_hint: str = ""
    extra_assets: tuple[ExtraAsset, ...] = ()
    warnings: tuple[str, ...] = ()


@dataclass
class SkippedItem:
    path: Path
    reason: str


@dataclass
class FileAction:
    source_path: Path
    target_path: Path
    series_key: str
    status: str = "planned"
    error_message: str = ""


@dataclass
class RecordPlacement:
    record: ImageRecord
    slice_index: int | None
    frame_index: int | None
    order_index: int = 0
    target_name: str = ""


@dataclass
class SeriesPlan:
    key: str
    study_key: str
    records: list[ImageRecord]
    patient_id: str
    manufacturer: str
    study_date: str
    study_description: str
    study_uid: str
    modality: str
    body_part: str
    series_uid: str
    series_number: int | None
    series_description: str
    protocol_name: str
    sequence_name: str
    image_type_text: str
    scanning_sequence: str
    sequence_variant: str
    source_series_hint: str
    sequence_label: str
    slice_count: int
    frame_count: int
    is_cine: bool
    has_time_dimension: bool
    warnings: list[str] = field(default_factory=list)
    patient_folder_base: str = ""
    patient_folder_name: str = ""
    series_folder_base: str = ""
    series_folder_name: str = ""
    target_dir: Path | None = None
    placements: list[RecordPlacement] = field(default_factory=list)

    @property
    def file_count(self) -> int:
        return len(self.records)

    @property
    def primary_format(self) -> str:
        counter = Counter(record.format_name for record in self.records)
        return counter.most_common(1)[0][0]

    @property
    def source_example(self) -> str:
        return str(self.records[0].source_path)


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore").strip()
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple)):
        parts = [normalize_text(item) for item in value]
        return " ".join(part for part in parts if part)
    if hasattr(value, "__iter__") and not isinstance(value, (dict, set)):
        try:
            parts = [normalize_text(item) for item in value]
            return " ".join(part for part in parts if part)
        except TypeError:
            pass
    return str(value).strip()


def pick_first(*values: Any, default: str = "") -> str:
    for value in values:
        text = normalize_text(value)
        if text:
            return text
    return default


def safe_int(value: Any) -> int | None:
    text = normalize_text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def safe_float(value: Any) -> float | None:
    text = normalize_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def normalize_date(value: Any) -> str:
    text = re.sub(r"\D", "", normalize_text(value))
    if len(text) >= 8:
        return text[:8]
    if len(text) == 6:
        return f"{text}01"
    return "unknown-date"


def sanitize_component(value: Any, fallback: str = "unknown", max_length: int = 48) -> str:
    text = unicodedata.normalize("NFKC", normalize_text(value))
    text = PATH_SAFE_PATTERN.sub("_", text)
    text = SPACE_PATTERN.sub("_", text)
    text = SEPARATOR_PATTERN.sub("_", text)
    text = text.strip(" ._-")
    if not text:
        text = fallback
    if len(text) > max_length:
        text = text[:max_length].rstrip(" ._-")
    return text or fallback


def short_id(value: str, length: int = 8) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:length]


def canonical_extension(path: Path) -> str:
    lower_name = path.name.lower()
    if lower_name.endswith(".nii.gz"):
        return ".nii.gz"
    return path.suffix.lower()


def split_extension(name: str) -> tuple[str, str]:
    lower_name = name.lower()
    if lower_name.endswith(".nii.gz"):
        return name[:-7], name[-7:]
    suffix = Path(name).suffix
    if suffix:
        return name[: -len(suffix)], suffix
    return name, ""


def build_counter_name(path: Path, counter: int) -> Path:
    stem, extension = split_extension(path.name)
    return path.with_name(f"{stem}__dup{counter}{extension}")


def unique_target_path(path: Path, used: set[Path]) -> Path:
    candidate = path
    counter = 2
    while candidate in used or candidate.exists():
        candidate = build_counter_name(path, counter)
        counter += 1
    used.add(candidate)
    return candidate


def ensure_tuple(value: tuple[float, ...] | None) -> tuple[int, tuple[float, ...]]:
    if value is None:
        return (1, ())
    return (0, value)


def ensure_number(value: float | None) -> tuple[int, float]:
    if value is None:
        return (1, 0.0)
    return (0, value)


def infer_vendor_from_text(*values: Any) -> str:
    combined = " ".join(normalize_text(value).lower() for value in values if normalize_text(value))
    for needle, vendor in VENDOR_RULES:
        if needle in combined:
            return vendor
    return "UnknownManufacturer"


def infer_modality_from_text(*values: Any) -> str:
    combined = " ".join(normalize_text(value).lower() for value in values if normalize_text(value))
    if any(token in combined for token in (" cardiac ", " cmr ", "mri", " mr ", "ssfp", "t1", "t2")):
        return "MR"
    if " ct " in combined or "cta" in combined:
        return "CT"
    if " pet " in combined:
        return "PT"
    if " us " in combined or "ultrasound" in combined:
        return "US"
    if " xa " in combined or "angiography" in combined:
        return "XA"
    return "UNK"


def source_patient_hint(path: Path, input_root: Path) -> str:
    try:
        relative = path.relative_to(input_root)
        if relative.parts:
            return relative.parts[0]
    except ValueError:
        pass
    if path.parent.name:
        return path.parent.name
    return path.stem or "unknown_patient"


def looks_like_dicom(path: Path) -> bool:
    extension = canonical_extension(path)
    if extension in DICOM_EXTENSIONS:
        return True
    if not extension:
        return True
    if extension in SUPPORTED_EXTENSIONS or extension in IGNORED_EXTENSIONS:
        return False
    return True


def is_under(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def should_skip_directory(path: Path, output_root: Path) -> bool:
    if path.name.startswith("."):
        return True
    if path.name in {"__pycache__", ".git"}:
        return True
    return is_under(path, output_root)


def filename_priority(name: str) -> tuple[int, str]:
    lower_name = name.lower()
    if lower_name.endswith((".nii.gz", ".nii", ".hdr", ".mhd", ".mha", ".nhdr", ".nrrd", ".dcm", ".dicom", ".ima")):
        return (0, lower_name)
    if lower_name.endswith((".json", ".bval", ".bvec", ".img", ".img.gz", ".raw", ".zraw", ".mat")):
        return (2, lower_name)
    return (1, lower_name)


def read_binary_prefix(path: Path, size: int = 65536) -> bytes:
    opener = gzip.open if path.name.lower().endswith(".gz") else open
    with opener(path, "rb") as handle:
        return handle.read(size)


def parse_key_value_header(prefix: bytes) -> dict[str, str]:
    text = prefix.decode("latin-1", errors="ignore")
    lines = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            break
        lines.append(line)
    header: dict[str, str] = {}
    for line in lines:
        if line.startswith("#"):
            continue
        if "=" in line:
            key, value = line.split("=", 1)
        elif ":" in line:
            key, value = line.split(":", 1)
        else:
            continue
        header[key.strip().lower()] = value.strip()
    return header


def parse_int_sequence(value: str) -> list[int]:
    numbers = []
    for token in re.split(r"[,\s]+", value.strip()):
        if not token:
            continue
        try:
            numbers.append(int(float(token)))
        except ValueError:
            continue
    return numbers


def parse_multi_file_reference(value: str) -> str | None:
    lowered = value.strip().lower()
    if not lowered or lowered == "local":
        return None
    if lowered.startswith("list"):
        return None
    if "%" in value or "*" in value:
        return None
    return value.strip().strip('"')


def safe_relative_name(relative_name: str) -> Path:
    parts = []
    for part in relative_name.replace("\\", "/").split("/"):
        if not part or part == ".":
            continue
        if part == "..":
            parts.append("_parent_")
            continue
        parts.append(part)
    if not parts:
        return Path("unknown_asset.bin")
    return Path(*parts)


def discover_nifti_assets(path: Path) -> tuple[ExtraAsset, ...]:
    base_name = path.name[:-7] if path.name.lower().endswith(".nii.gz") else path.stem
    assets = []
    for extension in (".json", ".bval", ".bvec"):
        candidate = path.with_name(f"{base_name}{extension}")
        if candidate.exists():
            assets.append(ExtraAsset(candidate, candidate.name))
    return tuple(assets)


def discover_analyze_assets(path: Path) -> tuple[ExtraAsset, ...]:
    assets = []
    for extension in (".img", ".img.gz", ".mat"):
        candidate = path.with_name(f"{path.stem}{extension}")
        if candidate.exists():
            assets.append(ExtraAsset(candidate, candidate.name))
    return tuple(assets)


def discover_header_assets(path: Path, reference_key: str) -> tuple[ExtraAsset, ...]:
    try:
        header = parse_key_value_header(read_binary_prefix(path, size=32768))
    except OSError:
        return ()
    reference_value = parse_multi_file_reference(header.get(reference_key, ""))
    if not reference_value:
        return ()
    raw_relative = path.parent / reference_value
    candidate = raw_relative.resolve()
    try:
        relative_name = str(raw_relative.relative_to(path.parent))
    except ValueError:
        relative_name = Path(reference_value).name
    if candidate.exists():
        return (ExtraAsset(candidate, relative_name),)
    return ()


def parse_nifti_metadata(path: Path, input_root: Path) -> ImageRecord:
    prefix = read_binary_prefix(path, size=352)
    if len(prefix) < 348:
        raise ValueError("NIfTI header too small")
    endianness = "<"
    if struct.unpack("<I", prefix[0:4])[0] != 348:
        if struct.unpack(">I", prefix[0:4])[0] == 348:
            endianness = ">"
        else:
            raise ValueError("Invalid NIfTI header")
    dims = struct.unpack(f"{endianness}8h", prefix[40:56])
    ndim = max(int(dims[0]), 0)
    shape = [int(value) for value in dims[1 : ndim + 1] if int(value) > 0]
    slices = shape[2] if len(shape) >= 3 else 1
    frames = shape[3] if len(shape) >= 4 else 1
    description = prefix[148:228].decode("latin-1", errors="ignore").strip("\x00 ").strip()
    hint = source_patient_hint(path, input_root)
    manufacturer = infer_vendor_from_text(path.parent, description)
    modality = infer_modality_from_text(path.parent, description)
    series_description = pick_first(description, path.stem, default="nifti_volume")
    return ImageRecord(
        source_path=path,
        format_name="nifti",
        patient_id=hint,
        manufacturer=manufacturer,
        study_date="unknown-date",
        study_description=hint,
        study_uid=f"path-study::{hint}",
        series_uid=f"path-series::{path.resolve()}",
        series_number=None,
        series_description=series_description,
        protocol_name="",
        sequence_name="",
        modality=modality,
        body_part="",
        instance_number=None,
        acquisition_number=None,
        embedded_slice_count=max(slices, 1),
        embedded_frame_count=max(frames, 1),
        source_patient_hint=hint,
        source_series_hint=path.parent.name,
        extra_assets=discover_nifti_assets(path),
        warnings=("patient_id_from_path", "manufacturer_inferred_or_unknown"),
    )


def parse_analyze_metadata(path: Path, input_root: Path) -> ImageRecord:
    prefix = read_binary_prefix(path, size=348)
    if len(prefix) < 348:
        raise ValueError("Analyze header too small")
    endianness = "<"
    if struct.unpack("<I", prefix[0:4])[0] not in {348, 384, 540}:
        if struct.unpack(">I", prefix[0:4])[0] in {348, 384, 540}:
            endianness = ">"
        else:
            raise ValueError("Invalid Analyze header")
    dims = struct.unpack(f"{endianness}8h", prefix[40:56])
    ndim = max(int(dims[0]), 0)
    shape = [int(value) for value in dims[1 : ndim + 1] if int(value) > 0]
    slices = shape[2] if len(shape) >= 3 else 1
    frames = shape[3] if len(shape) >= 4 else 1
    hint = source_patient_hint(path, input_root)
    manufacturer = infer_vendor_from_text(path.parent)
    modality = infer_modality_from_text(path.parent)
    return ImageRecord(
        source_path=path,
        format_name="analyze",
        patient_id=hint,
        manufacturer=manufacturer,
        study_date="unknown-date",
        study_description=hint,
        study_uid=f"path-study::{hint}",
        series_uid=f"path-series::{path.resolve()}",
        series_number=None,
        series_description=path.stem,
        protocol_name="",
        sequence_name="",
        modality=modality,
        body_part="",
        instance_number=None,
        acquisition_number=None,
        embedded_slice_count=max(slices, 1),
        embedded_frame_count=max(frames, 1),
        source_patient_hint=hint,
        source_series_hint=path.parent.name,
        extra_assets=discover_analyze_assets(path),
        warnings=("patient_id_from_path", "manufacturer_inferred_or_unknown"),
    )


def parse_metaimage_metadata(path: Path, input_root: Path) -> ImageRecord:
    header = parse_key_value_header(read_binary_prefix(path, size=65536))
    dims = parse_int_sequence(header.get("dimsize", ""))
    slices = dims[2] if len(dims) >= 3 else 1
    frames = dims[3] if len(dims) >= 4 else 1
    hint = source_patient_hint(path, input_root)
    description = header.get("comment", "") or path.stem
    assets = discover_header_assets(path, "elementdatafile") if path.suffix.lower() == ".mhd" else ()
    return ImageRecord(
        source_path=path,
        format_name="metaimage",
        patient_id=hint,
        manufacturer=infer_vendor_from_text(path.parent, description),
        study_date="unknown-date",
        study_description=hint,
        study_uid=f"path-study::{hint}",
        series_uid=f"path-series::{path.resolve()}",
        series_number=None,
        series_description=description,
        protocol_name="",
        sequence_name="",
        modality=infer_modality_from_text(path.parent, description),
        body_part="",
        instance_number=None,
        acquisition_number=None,
        embedded_slice_count=max(slices, 1),
        embedded_frame_count=max(frames, 1),
        source_patient_hint=hint,
        source_series_hint=path.parent.name,
        extra_assets=assets,
        warnings=("patient_id_from_path", "manufacturer_inferred_or_unknown"),
    )


def parse_nrrd_metadata(path: Path, input_root: Path) -> ImageRecord:
    header = parse_key_value_header(read_binary_prefix(path, size=65536))
    dims = parse_int_sequence(header.get("sizes", ""))
    slices = dims[2] if len(dims) >= 3 else 1
    frames = dims[3] if len(dims) >= 4 else 1
    hint = source_patient_hint(path, input_root)
    description = header.get("content", "") or path.stem
    assets = discover_header_assets(path, "data file") if path.suffix.lower() == ".nhdr" else ()
    return ImageRecord(
        source_path=path,
        format_name="nrrd",
        patient_id=hint,
        manufacturer=infer_vendor_from_text(path.parent, description),
        study_date="unknown-date",
        study_description=hint,
        study_uid=f"path-study::{hint}",
        series_uid=f"path-series::{path.resolve()}",
        series_number=None,
        series_description=description,
        protocol_name="",
        sequence_name="",
        modality=infer_modality_from_text(path.parent, description),
        body_part="",
        instance_number=None,
        acquisition_number=None,
        embedded_slice_count=max(slices, 1),
        embedded_frame_count=max(frames, 1),
        source_patient_hint=hint,
        source_series_hint=path.parent.name,
        extra_assets=assets,
        warnings=("patient_id_from_path", "manufacturer_inferred_or_unknown"),
    )


def build_slice_signature(position: Any) -> tuple[str | None, tuple[float, ...] | None]:
    if position is None:
        return None, None
    if isinstance(position, (list, tuple)):
        numbers = []
        for value in position:
            numeric = safe_float(value)
            if numeric is None:
                return None, None
            numbers.append(round(numeric, 4))
        return "|".join(f"{value:.4f}" for value in numbers), tuple(numbers)
    numeric = safe_float(position)
    if numeric is None:
        return None, None
    return f"{numeric:.4f}", (round(numeric, 4),)


def summarize_per_frame_groups(dataset: Any, frame_count: int) -> tuple[int, int]:
    try:
        groups = dataset.get("PerFrameFunctionalGroupsSequence")
    except Exception:
        return 1, frame_count
    if not groups:
        return 1, frame_count
    slice_signatures = set()
    temporal_signatures = set()
    for index, group in enumerate(groups, start=1):
        plane_sequence = group.get("PlanePositionSequence")
        if plane_sequence and len(plane_sequence) > 0:
            signature, _ = build_slice_signature(plane_sequence[0].get("ImagePositionPatient"))
            if signature:
                slice_signatures.add(signature)
        frame_content_sequence = group.get("FrameContentSequence")
        if frame_content_sequence and len(frame_content_sequence) > 0:
            item = frame_content_sequence[0]
            temporal_value = pick_first(
                item.get("TemporalPositionIndex"),
                item.get("TemporalPositionTimeOffset"),
                item.get("FrameAcquisitionNumber"),
                default="",
            )
            if temporal_value:
                temporal_signatures.add(temporal_value)
        if not temporal_signatures:
            cardiac_sync_sequence = group.get("CardiacSynchronizationSequence")
            if cardiac_sync_sequence and len(cardiac_sync_sequence) > 0:
                temporal_value = pick_first(
                    cardiac_sync_sequence[0].get("CardiacCyclePosition"),
                    cardiac_sync_sequence[0].get("RRIntervalTimeNominal"),
                    default="",
                )
                if temporal_value:
                    temporal_signatures.add(temporal_value)
        if not slice_signatures and not temporal_signatures and index >= frame_count:
            break
    slices = len(slice_signatures) or 1
    if temporal_signatures:
        frames = len(temporal_signatures)
    elif slices > 1:
        frames = max(frame_count // slices, 1)
    else:
        frames = frame_count
    return max(slices, 1), max(frames, 1)


def parse_dicom_metadata(path: Path, input_root: Path) -> ImageRecord:
    if pydicom is None:
        raise RuntimeError("pydicom_not_installed")
    dataset = pydicom.dcmread(str(path), stop_before_pixels=True, force=True)
    if not any(hasattr(dataset, attribute) for attribute in ("SOPClassUID", "SeriesInstanceUID", "StudyInstanceUID")):
        raise ValueError("Not a DICOM file")
    hint = source_patient_hint(path, input_root)
    study_date = normalize_date(
        pick_first(
            dataset.get("StudyDate"),
            dataset.get("SeriesDate"),
            dataset.get("AcquisitionDate"),
            dataset.get("ContentDate"),
        )
    )
    study_description = pick_first(dataset.get("StudyDescription"), dataset.get("PerformedProcedureStepDescription"), hint)
    series_description = pick_first(
        dataset.get("SeriesDescription"),
        dataset.get("ProtocolName"),
        dataset.get("SequenceName"),
        path.parent.name,
    )
    protocol_name = pick_first(dataset.get("ProtocolName"), dataset.get("PerformedProtocolCodeSequence"))
    sequence_name = pick_first(dataset.get("SequenceName"), dataset.get("ScanningSequence"), dataset.get("SequenceVariant"))
    manufacturer = pick_first(
        dataset.get("Manufacturer"),
        dataset.get("ManufacturerModelName"),
        infer_vendor_from_text(path.parent, series_description, protocol_name),
    )
    patient_id = pick_first(dataset.get("PatientID"), hint)
    modality = pick_first(dataset.get("Modality"), infer_modality_from_text(series_description, protocol_name, sequence_name))
    body_part = pick_first(dataset.get("BodyPartExamined"), dataset.get("StudyDescription"))
    study_uid = pick_first(
        dataset.get("StudyInstanceUID"),
        f"fallback-study::{patient_id}::{study_date}::{study_description}",
    )
    series_number = safe_int(dataset.get("SeriesNumber"))
    series_uid = pick_first(
        dataset.get("SeriesInstanceUID"),
        f"fallback-series::{study_uid}::{series_number}::{series_description}::{protocol_name}",
    )
    slice_signature, slice_sort_value = build_slice_signature(dataset.get("ImagePositionPatient"))
    if slice_signature is None:
        slice_signature, slice_sort_value = build_slice_signature(
            pick_first(dataset.get("SliceLocation"), dataset.get("InStackPositionNumber"))
        )
    temporal_signature = pick_first(
        dataset.get("TemporalPositionIdentifier"),
        dataset.get("TemporalPositionIndex"),
        dataset.get("CardiacCyclePosition"),
        dataset.get("TriggerTime"),
    )
    temporal_sort_value = safe_float(temporal_signature)
    frame_count = safe_int(dataset.get("NumberOfFrames")) or 1
    embedded_slice_count, embedded_frame_count = summarize_per_frame_groups(dataset, frame_count)
    image_type_text = normalize_text(dataset.get("ImageType"))
    warnings = []
    if patient_id == hint:
        warnings.append("patient_id_from_path")
    if manufacturer == "UnknownManufacturer":
        warnings.append("manufacturer_missing")
    return ImageRecord(
        source_path=path,
        format_name="dicom",
        patient_id=patient_id,
        manufacturer=manufacturer,
        study_date=study_date,
        study_description=study_description,
        study_uid=study_uid,
        series_uid=series_uid,
        series_number=series_number,
        series_description=series_description,
        protocol_name=protocol_name,
        sequence_name=sequence_name,
        modality=modality or "UNK",
        body_part=body_part,
        instance_number=safe_int(dataset.get("InstanceNumber")),
        acquisition_number=safe_int(dataset.get("AcquisitionNumber")),
        image_type_text=image_type_text,
        scanning_sequence=normalize_text(dataset.get("ScanningSequence")),
        sequence_variant=normalize_text(dataset.get("SequenceVariant")),
        embedded_slice_count=max(embedded_slice_count, 1),
        embedded_frame_count=max(embedded_frame_count, 1),
        slice_signature=slice_signature,
        slice_sort_value=slice_sort_value,
        temporal_signature=temporal_signature or None,
        temporal_sort_value=temporal_sort_value,
        sop_instance_uid=pick_first(dataset.get("SOPInstanceUID"), path.name),
        source_patient_hint=hint,
        source_series_hint=path.parent.name,
        warnings=tuple(warnings),
    )


def extract_record(path: Path, input_root: Path) -> ImageRecord:
    extension = canonical_extension(path)
    if extension in NIFTI_EXTENSIONS:
        return parse_nifti_metadata(path, input_root)
    if extension in ANALYZE_EXTENSIONS:
        return parse_analyze_metadata(path, input_root)
    if extension in METAIMAGE_EXTENSIONS:
        return parse_metaimage_metadata(path, input_root)
    if extension in NRRD_EXTENSIONS:
        return parse_nrrd_metadata(path, input_root)
    if looks_like_dicom(path):
        return parse_dicom_metadata(path, input_root)
    raise ValueError("unsupported_file_type")


def collect_records(input_root: Path, output_root: Path) -> tuple[list[ImageRecord], list[SkippedItem]]:
    records: list[ImageRecord] = []
    skipped: list[SkippedItem] = []
    handled_assets: set[Path] = set()
    for current_root, dirnames, filenames in os.walk(input_root):
        root_path = Path(current_root)
        dirnames[:] = [
            dirname for dirname in dirnames if not should_skip_directory(root_path / dirname, output_root)
        ]
        for filename in sorted(filenames, key=filename_priority):
            path = root_path / filename
            if path.resolve() in handled_assets:
                continue
            extension = canonical_extension(path)
            lower_name = path.name.lower()
            if lower_name in IGNORED_NAMES:
                skipped.append(SkippedItem(path, "ignored_index_file"))
                continue
            if extension in IGNORED_EXTENSIONS:
                skipped.append(SkippedItem(path, "ignored_non_image_extension"))
                continue
            try:
                record = extract_record(path, input_root)
                records.append(record)
                for asset in record.extra_assets:
                    handled_assets.add(asset.source_path.resolve())
            except RuntimeError as error:
                if str(error) == "pydicom_not_installed":
                    skipped.append(SkippedItem(path, "dicom_requires_pydicom"))
                else:
                    skipped.append(SkippedItem(path, f"runtime_error:{error}"))
            except Exception as error:
                skipped.append(SkippedItem(path, f"unsupported_or_invalid:{type(error).__name__}"))
    return records, skipped


def group_key(record: ImageRecord) -> str:
    if record.format_name == "dicom":
        return "||".join(
            [
                sanitize_component(record.patient_id, fallback="unknown_patient", max_length=64),
                sanitize_component(record.study_uid, fallback="unknown_study", max_length=96),
                sanitize_component(record.series_uid, fallback="unknown_series", max_length=96),
                sanitize_component(
                    "" if record.series_number is None else f"{record.series_number:06d}",
                    fallback="unknown_series_number",
                    max_length=24,
                ),
                sanitize_component(record.series_description, fallback="unknown_series_desc", max_length=64),
                sanitize_component(record.protocol_name, fallback="no_protocol", max_length=64),
                sanitize_component(record.sequence_name, fallback="no_sequence_name", max_length=64),
            ]
        )
    return f"path-series::{record.source_path.resolve()}"


def most_common_text(values: list[str], fallback: str) -> str:
    cleaned = [value for value in values if normalize_text(value)]
    if not cleaned:
        return fallback
    return Counter(cleaned).most_common(1)[0][0]


def sequence_context_text(
    series_description: str,
    protocol_name: str,
    sequence_name: str,
    modality: str,
    body_part: str,
    image_type_text: str = "",
    source_series_hint: str = "",
    scanning_sequence: str = "",
    sequence_variant: str = "",
) -> str:
    return " ".join(
        [
            normalize_text(series_description),
            normalize_text(protocol_name),
            normalize_text(sequence_name),
            normalize_text(modality),
            normalize_text(body_part),
            normalize_text(image_type_text),
            normalize_text(source_series_hint),
            normalize_text(scanning_sequence),
            normalize_text(sequence_variant),
        ]
    ).lower()


def infer_dynamic_series(series_text: str) -> bool:
    dynamic_keywords = (
        "cine",
        "cardiac",
        "time_course",
        "perfusion",
        "first pass",
        "perf",
        "flow",
        "temporal",
    )
    return any(keyword in series_text for keyword in dynamic_keywords)


def record_time_sort_key(record: ImageRecord) -> tuple[Any, ...]:
    return (
        ensure_number(record.temporal_sort_value),
        10**9 if record.instance_number is None else record.instance_number,
        record.sop_instance_uid or record.source_path.name.lower(),
        str(record.source_path).lower(),
    )


def record_slice_sort_key(item: tuple[str, list[ImageRecord]]) -> tuple[Any, ...]:
    _, group_records = item
    representative = min(group_records, key=record_sort_key)
    return (
        ensure_tuple(representative.slice_sort_value),
        10**9 if representative.instance_number is None else representative.instance_number,
        str(representative.source_path).lower(),
    )


def build_series_placements(records: list[ImageRecord], series_text: str) -> tuple[list[RecordPlacement], int, int, bool]:
    if not records:
        return [], 0, 0, False

    if len(records) == 1 and (
        records[0].embedded_slice_count > 1 or records[0].embedded_frame_count > 1
    ):
        placement = RecordPlacement(record=records[0], slice_index=None, frame_index=None, order_index=1)
        return [placement], max(records[0].embedded_slice_count, 1), max(records[0].embedded_frame_count, 1), (
            records[0].embedded_frame_count > 1
        )

    slice_groups: dict[str, list[ImageRecord]] = defaultdict(list)
    for record in records:
        if record.slice_signature:
            slice_groups[record.slice_signature].append(record)

    if len(slice_groups) > 1:
        placements: list[RecordPlacement] = []
        ordered_groups = sorted(slice_groups.items(), key=record_slice_sort_key)
        frame_count = max(len(group_records) for _, group_records in ordered_groups)
        order_index = 1
        for slice_index, (_, group_records) in enumerate(ordered_groups, start=1):
            ordered_records = sorted(group_records, key=record_time_sort_key)
            for frame_index, record in enumerate(ordered_records, start=1):
                placements.append(
                    RecordPlacement(
                        record=record,
                        slice_index=slice_index,
                        frame_index=frame_index,
                        order_index=order_index,
                    )
                )
                order_index += 1
        return placements, len(ordered_groups), max(frame_count, 1), (frame_count > 1)

    ordered_records = sorted(records, key=record_time_sort_key)
    dynamic = infer_dynamic_series(series_text) or any(record.temporal_signature for record in records)
    placements = []
    for order_index, record in enumerate(ordered_records, start=1):
        if dynamic:
            placements.append(
                RecordPlacement(record=record, slice_index=1, frame_index=order_index, order_index=order_index)
            )
        else:
            placements.append(
                RecordPlacement(record=record, slice_index=order_index, frame_index=1, order_index=order_index)
            )
    if dynamic:
        return placements, 1, len(ordered_records), len(ordered_records) > 1
    return placements, len(ordered_records), 1, False


def infer_cine(series_text: str) -> bool:
    cine_keywords = (
        "cine",
        "cinesax",
        "cinelax",
        "cardiac",
        "bssfp",
        "truefisp",
        "tfisp",
        "ffe",
        "lv function",
    )
    return any(keyword in series_text for keyword in cine_keywords)


def infer_sequence_label(
    series_description: str,
    protocol_name: str,
    sequence_name: str,
    modality: str,
    image_type_text: str,
    source_series_hint: str,
    scanning_sequence: str,
    sequence_variant: str,
    slice_count: int,
    frame_count: int,
) -> str:
    combined = sequence_context_text(
        series_description=series_description,
        protocol_name=protocol_name,
        sequence_name=sequence_name,
        modality=modality,
        body_part="",
        image_type_text=image_type_text,
        source_series_hint=source_series_hint,
        scanning_sequence=scanning_sequence,
        sequence_variant=sequence_variant,
    )
    plane = ""
    if any(keyword in combined for keyword in ("short axis", "short-axis", "sax", "cinesax", "saxcine")):
        plane = "sax"
    elif any(keyword in combined for keyword in ("4ch", "4 chamber", "four chamber")):
        plane = "lax_4ch"
    elif any(keyword in combined for keyword in ("3ch", "3 chamber", "three chamber", "lvot")):
        plane = "lax_3ch"
    elif any(keyword in combined for keyword in ("2ch", "2 chamber", "two chamber")):
        plane = "lax_2ch"
    elif any(keyword in combined for keyword in ("long axis", "lax", "cinelax")):
        plane = "lax"
    elif "axial" in combined:
        plane = "axial"
    elif "coronal" in combined:
        plane = "coronal"
    elif "sagittal" in combined:
        plane = "sagittal"

    sequence_type = ""
    if any(keyword in combined for keyword in ("localizer", "scout", "survey")):
        sequence_type = "localizer"
    elif any(keyword in combined for keyword in ("psir", "phase sensitive")):
        sequence_type = "psir_lge"
    elif any(keyword in combined for keyword in ("late gad", "delayed enhancement", " myo scar ", " lge ")):
        sequence_type = "lge"
    elif any(keyword in combined for keyword in ("t1 map", "t1map", "t1 mapping")):
        sequence_type = "t1_map"
    elif any(keyword in combined for keyword in ("t2 map", "t2map", "t2 mapping")):
        sequence_type = "t2_map"
    elif any(keyword in combined for keyword in ("perfusion", "first pass", "time_course", " perf")) or combined.startswith("perf"):
        sequence_type = "perfusion"
    elif any(keyword in combined for keyword in ("flow", "phase contrast", "pc-mri")):
        sequence_type = "flow"
    elif any(keyword in combined for keyword in ("dwi", "diffusion")):
        sequence_type = "dwi"
    elif "adc" in combined:
        sequence_type = "adc"
    elif "mra" in combined:
        sequence_type = "mra"
    elif "myocard_eval" in combined:
        sequence_type = "myocard_eval"
    elif infer_cine(combined):
        sequence_type = "cine"
    elif "3d" in combined and slice_count > 1:
        sequence_type = "3d_volume"
    elif frame_count > 1:
        sequence_type = "dynamic_series"
    else:
        base = sanitize_component(
            series_description or protocol_name or sequence_name or image_type_text or source_series_hint or modality,
            "unknown_series",
            36,
        )
        sequence_type = base.lower()

    if plane and sequence_type and not sequence_type.startswith(plane):
        return f"{plane}_{sequence_type}"
    return sequence_type or plane or "unknown_series"


def build_patient_folder_base(patient_id: str, manufacturer: str, study_date: str, study_description: str) -> str:
    parts = [
        f"PID-{sanitize_component(patient_id, fallback='unknown_patient', max_length=32)}",
        f"MFR-{sanitize_component(manufacturer, fallback='UnknownManufacturer', max_length=24)}",
        f"DATE-{sanitize_component(study_date, fallback='unknown-date', max_length=12)}",
        f"STUDY-{sanitize_component(study_description, fallback='unknown_study', max_length=32)}",
    ]
    return "__".join(parts)


def build_series_folder_base(plan: SeriesPlan) -> str:
    series_number = f"{plan.series_number:03d}" if plan.series_number is not None else "UNK"
    parts = [
        f"SER-{series_number}",
        f"SEQ-{sanitize_component(plan.sequence_label, fallback='unknown_series', max_length=40)}",
        f"MOD-{sanitize_component(plan.modality, fallback='UNK', max_length=8)}",
        f"SL-{plan.slice_count:03d}",
    ]
    if plan.frame_count > 1 or plan.is_cine:
        parts.append(f"FR-{plan.frame_count:03d}")
    raw_description = sanitize_component(
        plan.series_description or plan.protocol_name or plan.sequence_name,
        fallback="",
        max_length=32,
    )
    if raw_description:
        label_part = sanitize_component(plan.sequence_label, fallback="unknown_series", max_length=40).lower()
        if raw_description.lower() != label_part:
            parts.append(f"DESC-{raw_description}")
    return "__".join(parts)


def build_series_plans(records: list[ImageRecord]) -> list[SeriesPlan]:
    grouped: dict[str, list[ImageRecord]] = defaultdict(list)
    for record in records:
        grouped[group_key(record)].append(record)

    plans: list[SeriesPlan] = []
    for key, series_records in grouped.items():
        patient_id = most_common_text([record.patient_id for record in series_records], "unknown_patient")
        manufacturer = most_common_text(
            [record.manufacturer for record in series_records if record.manufacturer != "UnknownManufacturer"],
            "UnknownManufacturer",
        )
        study_date = most_common_text([record.study_date for record in series_records], "unknown-date")
        study_description = most_common_text([record.study_description for record in series_records], "unknown_study")
        modality = most_common_text([record.modality for record in series_records], "UNK")
        body_part = most_common_text([record.body_part for record in series_records], "")
        series_description = most_common_text([record.series_description for record in series_records], "unknown_series")
        protocol_name = most_common_text([record.protocol_name for record in series_records], "")
        sequence_name = most_common_text([record.sequence_name for record in series_records], "")
        image_type_text = most_common_text([record.image_type_text for record in series_records], "")
        scanning_sequence = most_common_text([record.scanning_sequence for record in series_records], "")
        sequence_variant = most_common_text([record.sequence_variant for record in series_records], "")
        source_series_hint = most_common_text([record.source_series_hint for record in series_records], "")
        study_uid = most_common_text([record.study_uid for record in series_records], "unknown_study")
        series_uid = most_common_text([record.series_uid for record in series_records], key)
        series_numbers = [record.series_number for record in series_records if record.series_number is not None]
        series_number = Counter(series_numbers).most_common(1)[0][0] if series_numbers else None
        series_text = sequence_context_text(
            series_description=series_description,
            protocol_name=protocol_name,
            sequence_name=sequence_name,
            modality=modality,
            body_part=body_part,
            image_type_text=image_type_text,
            source_series_hint=source_series_hint,
            scanning_sequence=scanning_sequence,
            sequence_variant=sequence_variant,
        )
        placements, slice_count, frame_count, has_time_dimension = build_series_placements(series_records, series_text)
        sequence_label = infer_sequence_label(
            series_description,
            protocol_name,
            sequence_name,
            modality,
            image_type_text,
            source_series_hint,
            scanning_sequence,
            sequence_variant,
            slice_count,
            frame_count,
        )
        is_cine = sequence_label.endswith("cine") or sequence_label == "cine"
        warnings = sorted({warning for record in series_records for warning in record.warnings})
        study_key = "||".join([patient_id, manufacturer, study_date, study_description, study_uid])
        plan = SeriesPlan(
            key=key,
            study_key=study_key,
            records=series_records,
            patient_id=patient_id,
            manufacturer=manufacturer,
            study_date=study_date,
            study_description=study_description,
            study_uid=study_uid,
            modality=modality,
            body_part=body_part,
            series_uid=series_uid,
            series_number=series_number,
            series_description=series_description,
            protocol_name=protocol_name,
            sequence_name=sequence_name,
            image_type_text=image_type_text,
            scanning_sequence=scanning_sequence,
            sequence_variant=sequence_variant,
            source_series_hint=source_series_hint,
            sequence_label=sequence_label,
            slice_count=slice_count,
            frame_count=frame_count,
            is_cine=is_cine,
            has_time_dimension=has_time_dimension,
            warnings=warnings,
            placements=placements,
        )
        plan.patient_folder_base = build_patient_folder_base(
            patient_id=plan.patient_id,
            manufacturer=plan.manufacturer,
            study_date=plan.study_date,
            study_description=plan.study_description,
        )
        plan.series_folder_base = build_series_folder_base(plan)
        plans.append(plan)
    assign_target_names(plans)
    return sorted(
        plans,
        key=lambda plan: (
            plan.patient_folder_name,
            10**9 if plan.series_number is None else plan.series_number,
            plan.sequence_label,
            plan.series_uid,
        ),
    )


def assign_target_names(plans: list[SeriesPlan]) -> None:
    study_groups: dict[str, str] = {}
    base_to_studies: dict[str, list[str]] = defaultdict(list)
    for plan in plans:
        if plan.study_key not in study_groups:
            study_groups[plan.study_key] = plan.patient_folder_base
            base_to_studies[plan.patient_folder_base].append(plan.study_key)
    study_folder_names: dict[str, str] = {}
    for base_name, study_keys in base_to_studies.items():
        unique_keys = sorted(set(study_keys))
        if len(unique_keys) == 1:
            study_folder_names[unique_keys[0]] = base_name
            continue
        for study_key in unique_keys:
            study_folder_names[study_key] = f"{base_name}__STU-{short_id(study_key)}"
    series_name_groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for plan in plans:
        plan.patient_folder_name = study_folder_names[plan.study_key]
        series_name_groups[(plan.patient_folder_name, plan.series_folder_base)].append(plan.key)
    series_folder_names: dict[str, str] = {}
    for (patient_folder_name, base_name), series_keys in series_name_groups.items():
        unique_keys = sorted(set(series_keys))
        if len(unique_keys) == 1:
            series_folder_names[unique_keys[0]] = base_name
            continue
        for series_key in unique_keys:
            series_folder_names[series_key] = f"{base_name}__SID-{short_id(series_key)}"
    for plan in plans:
        plan.series_folder_name = series_folder_names[plan.key]


def record_sort_key(record: ImageRecord) -> tuple[Any, ...]:
    return (
        10**9 if record.acquisition_number is None else record.acquisition_number,
        ensure_number(record.temporal_sort_value),
        ensure_tuple(record.slice_sort_value),
        10**9 if record.instance_number is None else record.instance_number,
        record.sop_instance_uid or record.source_path.name.lower(),
        str(record.source_path).lower(),
    )


def build_file_actions(plans: list[SeriesPlan], output_root: Path) -> list[FileAction]:
    actions: list[FileAction] = []
    used_targets: set[Path] = set()
    for plan in plans:
        target_dir = output_root / plan.patient_folder_name / plan.series_folder_name
        plan.target_dir = target_dir
        dicom_counter = 1
        placements_by_source = {placement.record.source_path: placement for placement in plan.placements}
        ordered_placements = sorted(plan.placements, key=lambda placement: placement.order_index)
        ordered_records = [placement.record for placement in ordered_placements] if ordered_placements else sorted(
            plan.records, key=record_sort_key
        )
        for record in ordered_records:
            if record.format_name == "dicom":
                target_name = f"IMG_{dicom_counter:04d}.dcm"
                dicom_counter += 1
                target_path = unique_target_path(target_dir / target_name, used_targets)
                placement = placements_by_source.get(record.source_path)
                if placement is not None:
                    placement.target_name = target_name
                actions.append(FileAction(record.source_path, target_path, plan.key))
                continue
            primary_target = unique_target_path(target_dir / record.source_path.name, used_targets)
            actions.append(FileAction(record.source_path, primary_target, plan.key))
            for asset in record.extra_assets:
                relative_name = safe_relative_name(asset.relative_name)
                asset_target = unique_target_path(target_dir / relative_name, used_targets)
                actions.append(FileAction(asset.source_path, asset_target, plan.key))
    return actions


def transfer_file(source_path: Path, target_path: Path, mode: str) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if mode == "copy":
        shutil.copy2(source_path, target_path)
        return
    if mode == "move":
        shutil.move(str(source_path), str(target_path))
        return
    if mode == "hardlink":
        os.link(source_path, target_path)
        return
    if mode == "symlink":
        os.symlink(source_path, target_path)
        return
    raise ValueError(f"Unsupported mode: {mode}")


def execute_actions(actions: list[FileAction], mode: str, dry_run: bool) -> list[FileAction]:
    for action in actions:
        if dry_run:
            action.status = "dry_run"
            continue
        try:
            transfer_file(action.source_path, action.target_path, mode)
            action.status = "done"
        except Exception as error:
            action.status = "failed"
            action.error_message = f"{type(error).__name__}: {error}"
    return actions


def sequence_general_meaning(plan: SeriesPlan) -> str:
    label = plan.sequence_label.lower()
    if label == "sax_cine":
        return (
            "这是心脏短轴电影序列（SAX cine）。它通常用于观察左心室和右心室在整个心动周期中的动态收缩与舒张，"
            "常用于后续的心室容积、射血分数、心肌壁运动和整体心功能分析。"
        )
    if label in {"lax_cine", "lax_2ch_cine", "lax_3ch_cine", "lax_4ch_cine", "cine"}:
        return (
            "这是心脏长轴或通用电影序列（cine MRI）。它通常用于观察心腔、瓣膜和心肌在一个心动周期中的动态变化，"
            "适合做功能分析、壁运动观察以及心腔形态评估。"
        )
    if label == "perfusion":
        return (
            "这是灌注或时间过程序列。它通常用于观察造影剂经过心肌或心腔时的信号变化，"
            "可用于灌注、首过增强或时间变化相关分析。"
        )
    if label == "localizer":
        return "这是定位像（localizer/scout），主要用于快速定位解剖位置，通常不作为精细定量分析的主序列。"
    if label == "myocard_eval":
        return (
            "这是心肌评估相关图像。当前 DICOM 中缺少明确的序列名称，但从 ImageType 看属于心肌评估用途，"
            "后续可能需要结合临床背景或原始导出说明再进一步细分。"
        )
    if label == "lge":
        return "这是延迟强化相关序列，通常用于观察心肌瘢痕、纤维化或梗死相关增强表现。"
    if label == "t1_map":
        return "这是 T1 mapping 序列，通常用于定量评估心肌组织特性。"
    if label == "t2_map":
        return "这是 T2 mapping 序列，通常用于定量评估水肿或炎症相关变化。"
    if label == "flow":
        return "这是血流或相位对比相关序列，通常用于评估血流方向、速度或通量。"
    return (
        "当前可以确认这是一个医学影像序列，但仅凭现有 DICOM 标签还不能可靠给出更精确的临床序列名称。"
    )


def describe_sequence_structure(plan: SeriesPlan) -> str:
    if len(plan.records) == 1 and plan.records[0].embedded_frame_count > 1:
        return (
            f"该序列当前只有 1 个多帧 DICOM 文件，文件内部包含约 {plan.slice_count} 层、{plan.frame_count} 帧的数据。"
        )
    if plan.has_time_dimension and plan.slice_count > 1:
        return (
            f"该序列当前由 {plan.file_count} 个单帧 DICOM 文件组成，可整理为 {plan.slice_count} 层 x {plan.frame_count} 帧。"
            " 整个序列表示多个空间层面上的时间序列；单个文件表示某一层在某一个时间点的一张图像。"
        )
    if plan.has_time_dimension and plan.slice_count == 1:
        return (
            f"该序列当前由 {plan.file_count} 个单帧 DICOM 文件组成，可整理为单层 x {plan.frame_count} 帧。"
            " 整个序列表示同一层面的时间序列；单个文件表示该层面的某一个时间点。"
        )
    if plan.slice_count > 1:
        return (
            f"该序列当前由 {plan.file_count} 个单帧 DICOM 文件组成，可整理为 {plan.slice_count} 个静态层面。"
            " 整个序列表示一个空间层叠的切面集合；单个文件表示其中一层的静态图像。"
        )
    return "该序列当前只有单张图像，单个 DICOM 文件就是该序列本身。"


def build_example_lines(plan: SeriesPlan) -> list[str]:
    if not plan.placements:
        return ["- 当前未生成可用的层/帧映射。"]
    example = min(plan.placements, key=lambda placement: placement.order_index)
    lines = []
    if example.target_name:
        if example.slice_index is None and example.frame_index is None:
            lines.append(
                f"- `{example.target_name}` 是该序列的第 1 个输出文件；它本身是一个多帧 DICOM 对象，内部包含整个序列。"
            )
        else:
            lines.append(
                f"- `{example.target_name}` 对应第 {example.slice_index} 层、第 {example.frame_index} 帧的影像数据。"
            )
    lines.append(f"- 它来源于原始文件 `{example.record.source_path.name}`。")
    if example.record.slice_signature:
        lines.append(f"- 该文件的 `SliceLocation/ImagePosition` 对应层面标识为 `{example.record.slice_signature}`。")
    if example.record.temporal_signature:
        lines.append(f"- 该文件的时间相关标签值为 `{example.record.temporal_signature}`。")
    if example.record.instance_number is not None:
        lines.append(f"- 该文件的 `InstanceNumber` 为 `{example.record.instance_number}`。")
    return lines


def build_series_description(plan: SeriesPlan) -> str:
    raw_name = plan.series_description or plan.protocol_name or plan.sequence_name or plan.source_series_hint
    example_lines = build_example_lines(plan)
    lines = [
        "# 序列说明",
        "",
        "## 1. 这个序列是什么",
        sequence_general_meaning(plan),
        "",
        "## 2. 当前数据中采集了什么",
        describe_sequence_structure(plan),
        "",
        "## 3. 本序列的关键标签",
        f"- 病人 ID: `{plan.patient_id}`",
        f"- 厂家: `{plan.manufacturer}`",
        f"- 模态: `{plan.modality}`",
        f"- 原始序列号: `{'' if plan.series_number is None else plan.series_number}`",
        f"- 原始序列描述: `{raw_name or 'unknown'}`",
        f"- 统一命名标签: `{plan.sequence_label}`",
        f"- 层数: `{plan.slice_count}`",
        f"- 帧数: `{plan.frame_count}`",
        f"- 文件数: `{plan.file_count}`",
    ]
    if plan.image_type_text:
        lines.append(f"- ImageType: `{plan.image_type_text}`")
    if plan.scanning_sequence:
        lines.append(f"- ScanningSequence: `{plan.scanning_sequence}`")
    if plan.sequence_variant:
        lines.append(f"- SequenceVariant: `{plan.sequence_variant}`")
    lines.extend(
        [
            "",
            "## 4. 单个 DICOM 文件表示什么",
            *example_lines,
            "",
            "## 5. 当前目录中的编号规则",
            "- 本目录中的 `IMG_XXXX.dcm` 按脚本推断的层优先、层内时间顺序次之进行编号。",
            "- 对于动态序列，这意味着通常会先列出第 1 层的全部时间帧，再列出第 2 层，依次类推。",
            "- 对于静态层叠序列，`IMG_0001.dcm`、`IMG_0002.dcm` 等通常对应不同层面的静态切片。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_series_descriptions(plans: list[SeriesPlan], dry_run: bool) -> None:
    for plan in plans:
        if plan.target_dir is None:
            continue
        description_path = plan.target_dir / "SERIES_DESCRIPTION.md"
        if dry_run:
            continue
        description_path.parent.mkdir(parents=True, exist_ok=True)
        description_path.write_text(build_series_description(plan), encoding="utf-8-sig")


def write_reports(
    output_root: Path,
    plans: list[SeriesPlan],
    actions: list[FileAction],
    skipped: list[SkippedItem],
    mode: str,
) -> None:
    report_dir = output_root / "_reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    series_manifest_path = report_dir / "series_manifest.csv"
    with series_manifest_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "patient_folder",
                "series_folder",
                "patient_id",
                "manufacturer",
                "study_date",
                "study_description",
                "modality",
                "sequence_label",
                "series_number",
                "series_description",
                "series_uid",
                "slice_count",
                "frame_count",
                "file_count",
                "primary_format",
                "source_example",
                "warnings",
            ]
        )
        for plan in plans:
            writer.writerow(
                [
                    plan.patient_folder_name,
                    plan.series_folder_name,
                    plan.patient_id,
                    plan.manufacturer,
                    plan.study_date,
                    plan.study_description,
                    plan.modality,
                    plan.sequence_label,
                    "" if plan.series_number is None else plan.series_number,
                    plan.series_description,
                    plan.series_uid,
                    plan.slice_count,
                    plan.frame_count,
                    plan.file_count,
                    plan.primary_format,
                    plan.source_example,
                    ";".join(plan.warnings),
                ]
            )

    file_manifest_path = report_dir / "file_manifest.csv"
    with file_manifest_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["status", "source_path", "target_path", "series_key", "error_message"])
        for action in actions:
            writer.writerow(
                [action.status, str(action.source_path), str(action.target_path), action.series_key, action.error_message]
            )

    skipped_manifest_path = report_dir / "skipped_files.csv"
    with skipped_manifest_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["path", "reason"])
        for item in skipped:
            writer.writerow([str(item.path), item.reason])

    summary_path = report_dir / "run_summary.json"
    skipped_counter = Counter(item.reason for item in skipped)
    action_counter = Counter(action.status for action in actions)
    summary = {
        "mode": mode,
        "patients": len({plan.patient_folder_name for plan in plans}),
        "series": len(plans),
        "planned_file_actions": len(actions),
        "action_status_counts": dict(action_counter),
        "skipped_reason_counts": dict(skipped_counter),
        "reports": {
            "series_manifest": str(series_manifest_path),
            "file_manifest": str(file_manifest_path),
            "skipped_files": str(skipped_manifest_path),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")


def print_summary(plans: list[SeriesPlan], actions: list[FileAction], skipped: list[SkippedItem], dry_run: bool, output_root: Path) -> None:
    patient_count = len({plan.patient_folder_name for plan in plans})
    status_counter = Counter(action.status for action in actions)
    skipped_counter = Counter(item.reason for item in skipped)
    print(f"Patients: {patient_count}")
    print(f"Series: {len(plans)}")
    print(f"Planned file actions: {len(actions)}")
    print(f"Output root: {output_root}")
    if dry_run:
        print("Run mode: dry-run")
    if status_counter:
        print(f"Action status: {dict(status_counter)}")
    if skipped_counter:
        print(f"Skipped: {dict(skipped_counter)}")
    preview_count = min(8, len(plans))
    if preview_count:
        print("\nPreview:")
        for plan in plans[:preview_count]:
            patient_folder = plan.patient_folder_name
            series_folder = plan.series_folder_name
            print(f"  {patient_folder}\\{series_folder}")


def validate_args(input_root: Path, output_root: Path, mode: str) -> None:
    if not input_root.exists():
        raise FileNotFoundError(f"Input root does not exist: {input_root}")
    if not input_root.is_dir():
        raise NotADirectoryError(f"Input root is not a directory: {input_root}")
    if input_root.resolve() == output_root.resolve():
        raise ValueError("Input root and output root cannot be the same directory")
    if mode in {"hardlink", "symlink"} and sys.platform.startswith("win"):
        print(
            "Warning: hardlink/symlink mode on Windows may need extra permissions. "
            "If it fails, switch to --mode copy.",
            file=sys.stderr,
        )


def parse_args() -> argparse.Namespace:
    # When you copy this script to another machine, you usually do NOT need to edit the
    # Python source code to change input/output paths. Just replace the command-line paths:
    #
    # Absolute-path example:
    #   python organize_medical_images.py "D:\dataset-Sunnybrook\data" --output-root "D:\test\directory1"
    #
    # Relative-path example (run from the script directory):
    #   python organize_medical_images.py ".\data" --output-root ".\directory1"
    #
    # Here:
    # - input_root: the raw parent folder that contains patient folders or nested DICOM files
    # - --output-root: the normalized output folder you want to create
    parser = argparse.ArgumentParser(
        description="Organize raw medical imaging files into a normalized patient/series folder layout.",
        formatter_class=ArgumentHelpFormatter,
        epilog=(
            "Examples:\n"
            '  Absolute path: python organize_medical_images.py "D:\\dataset-Sunnybrook\\data" --output-root "D:\\test\\directory1"\n'
            '  Relative path: python organize_medical_images.py ".\\data" --output-root ".\\directory1"\n'
        ),
    )
    # This is the INPUT path you change most often.
    parser.add_argument("input_root", help="Raw parent directory that contains patient folders and nested image files.")
    # This is the OUTPUT path you change most often.
    parser.add_argument(
        "--output-root",
        default="data",
        help="Organized output root. A reports folder will be created here after a real run.",
    )
    parser.add_argument(
        "--mode",
        choices=("copy", "move", "hardlink", "symlink"),
        default="copy",
        help="How to place files into the normalized layout.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and build the plan without creating output files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_root = Path(args.input_root).resolve()
    output_root = Path(args.output_root).resolve()

    try:
        validate_args(input_root, output_root, args.mode)
    except Exception as error:
        print(f"Argument error: {error}", file=sys.stderr)
        return 2

    records, skipped = collect_records(input_root, output_root)
    if not records:
        print("No supported medical image files were discovered.")
        if skipped:
            skipped_counter = Counter(item.reason for item in skipped)
            print(f"Skipped summary: {dict(skipped_counter)}")
        if any(item.reason == "dicom_requires_pydicom" for item in skipped):
            print("Install pydicom first if your source folder mainly contains DICOM files.")
        return 0

    plans = build_series_plans(records)
    actions = build_file_actions(plans, output_root)
    actions = execute_actions(actions, mode=args.mode, dry_run=args.dry_run)
    write_series_descriptions(plans, dry_run=args.dry_run)
    print_summary(plans, actions, skipped, args.dry_run, output_root)

    if not args.dry_run:
        write_reports(output_root, plans, actions, skipped, mode=args.mode)
        print(f"\nReports saved under: {output_root / '_reports'}")
    else:
        print("\nDry-run complete. No files were written.")
    failed_count = sum(1 for action in actions if action.status == "failed")
    return 1 if failed_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
