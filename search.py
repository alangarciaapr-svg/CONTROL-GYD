from __future__ import annotations

import io
import os
import zipfile


def build_zip_from_entries(entries, load_bytes) -> tuple:
    """Build a ZIP from a list of (arcpath, file_path, bucket, object_path) entries.

    Returns (zip_bytes, included_count, skipped_count, skipped_names).
    """
    mem = io.BytesIO()
    included = 0
    skipped = 0
    skipped_names = []
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        seen_paths = {}
        for arcpath, file_path, bucket, object_path in entries:
            try:
                file_bytes = load_bytes(file_path, bucket, object_path)
            except Exception:
                skipped += 1
                skipped_names.append(os.path.basename(arcpath))
                continue
            base, ext = os.path.splitext(arcpath)
            counter = seen_paths.get(arcpath, 0)
            seen_paths[arcpath] = counter + 1
            if counter > 0:
                arcpath = f"{base}_{counter}{ext}"
            zf.writestr(arcpath, file_bytes)
            included += 1
    return mem.getvalue(), included, skipped, skipped_names
