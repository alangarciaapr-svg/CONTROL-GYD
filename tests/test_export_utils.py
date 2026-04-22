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
    payload = build_zip_from_entries(entries, loader)
    with zipfile.ZipFile(io.BytesIO(payload), "r") as zf:
        names = sorted(zf.namelist())
        assert names == ["Docs/a.pdf", "Docs/a_1.pdf"]
