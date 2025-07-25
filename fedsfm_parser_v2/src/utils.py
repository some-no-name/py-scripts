import re, hashlib

def clean_field(value: str) -> str:
    """
    Очищает строку от пробелов и лишних знаков пунктуации по краям.
    """
    if not isinstance(value, str):
        return value
    # экранируем обе кавычки \' и \", используем двойные кавычки-делимитеры
    return re.sub(r"^[\s,;.\'\"“”«»]+|[\s,;.\'\"“”«»]+$", "", value.strip())


def generate_record_id(name: str, birth_date: str, counter: int = 0) -> str:
    base = f"{name.strip().lower()}|{birth_date.strip()}"
    if counter:
        base += f"|{counter}"
    return hashlib.md5(base.encode('utf-8')).hexdigest()

def get_file_name(full_path):
    return (str(full_path).split("/"))[-1]
