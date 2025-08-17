import logging
from pathlib import Path


def _clean(folder: Path, ext: str, keep_count: int):
    files = sorted(folder.glob(ext), key=lambda p: p.stat().st_mtime, reverse=True)
    for f in files[keep_count:]:
        try:
            f.unlink()
        except Exception as e:
            logging.warning("Cannot delete %s: %s", f, e)

def clean_folders(folders_res: "tuple[str, str]", keep_count = 10):
    for folder, res in folders_res:
        _clean(folder, res, keep_count)

