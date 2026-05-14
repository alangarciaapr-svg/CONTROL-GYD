import io
import zipfile

from segav_core.export_utils import build_zip_from_entries


FILES = {
    "a.pdf": b"uno",
    "b.pdf": b"dos",
}


def loader(file_path, bucket, object_path):
    return FILES[file_path]


def test_build_zip_from_entries_renames_duplicates():
    entries = [
        ("Docs/a.pdf", "a.pdf", None, None),
        ("Docs/a.pdf", "b.pdf", None, None),
    ]
    payload, included, skipped, skipped_names = build_zip_from_entries(entries, loader)
    assert included == 2
    assert skipped == 0
    with zipfile.ZipFile(io.BytesIO(payload), "r") as zf:
        names = sorted(zf.namelist())
        assert names == ["Docs/a.pdf", "Docs/a_1.pdf"]


def test_build_zip_from_entries_counts_skipped():
    def failing_loader(file_path, bucket, object_path):
        if file_path == "missing.pdf":
            raise FileNotFoundError("not found")
        return FILES[file_path]

    entries = [
        ("Docs/a.pdf", "a.pdf", None, None),
        ("Docs/missing.pdf", "missing.pdf", None, None),
    ]
    payload, included, skipped, skipped_names = build_zip_from_entries(entries, failing_loader)
    assert included == 1
    assert skipped == 1
    assert "missing.pdf" in skipped_names
