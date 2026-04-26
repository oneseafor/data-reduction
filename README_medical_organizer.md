# Medical Image Organizer

This script reorganizes raw hospital imaging data into a normalized folder layout that is easier to manage before downstream analysis.

## Folder layout

```text
data/
  PID-{patient_id}__MFR-{manufacturer}__DATE-{study_date}__STUDY-{study_description}/
    SER-{series_number}__SEQ-{sequence_label}__MOD-{modality}__SL-{slice_count}__FR-{frame_count}/
      IMG_0001.dcm
      IMG_0002.dcm
      ...
```

Notes:

- The patient-level folder stores shared study information.
- The series-level folder stores sequence-specific information such as cine/static type, modality, slice count, and frame count.
- DICOM files are renamed to a uniform `IMG_XXXX.dcm` pattern.
- Non-DICOM medical volume files keep their original filenames to avoid breaking sidecar/header references.

## Supported input formats

- DICOM (`.dcm`, `.dicom`, `.ima`, and many extensionless files)
- NIfTI (`.nii`, `.nii.gz`)
- Analyze (`.hdr` with paired `.img`)
- MetaImage (`.mha`, `.mhd`)
- NRRD (`.nrrd`, `.nhdr`)

## Dependency

Install `pydicom` if you need to parse DICOM metadata:

```powershell
python -m pip install -r requirements-medical-organizer.txt
```

The other supported volume formats can be parsed with the Python standard library.

## Which path should you change

You usually do not need to edit the Python source code itself.
Just change these two command-line paths when running the script:

- `input_root`: the raw source folder
- `--output-root`: the normalized output folder

These arguments are defined in [organize_medical_images.py](/D:/test/organize_medical_images.py:1619).

Absolute-path example:

```powershell
python organize_medical_images.py "D:\dataset-Sunnybrook\data" --output-root "D:\test\directory1"
```

Relative-path example:

```powershell
python organize_medical_images.py ".\data" --output-root ".\directory1"
```

If you run the relative-path example, the relative paths are resolved from your current terminal working directory.

## Recommended workflow

1. Run a dry-run first.
2. Check the preview in the terminal.
3. Run the real copy after the naming looks reasonable.
4. Review `_reports/series_manifest.csv` and `_reports/file_manifest.csv`.

## Example commands

```powershell
python organize_medical_images.py D:\raw_hospital_data --output-root D:\test\data --dry-run
python organize_medical_images.py D:\raw_hospital_data --output-root D:\test\data --mode copy
```

## Output reports

After a real run, the script writes:

- `data/_reports/series_manifest.csv`
- `data/_reports/file_manifest.csv`
- `data/_reports/skipped_files.csv`
- `data/_reports/run_summary.json`
