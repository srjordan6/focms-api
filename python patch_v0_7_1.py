# patch_v0_7_1.py - fixes _fetch_current_values robustness
import re, ast, pathlib, sys

p = pathlib.Path('focms_parent_portal.py')
src = p.read_text(encoding='utf-8')

# Fix 1: enhance _coerce_for_json to handle bytes, lists, dicts
old1 = '''def _coerce_for_json(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (str, int, float, bool)):
        return val
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)'''
new1 = '''def _coerce_for_json(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (str, int, float, bool)):
        return val
    if isinstance(val, bytes):
        return f"<encrypted:{len(val)}b>"
    if isinstance(val, list):
        return [_coerce_for_json(v) for v in val]
    if isinstance(val, dict):
        return {str(k): _coerce_for_json(v) for k, v in val.items()}
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)'''
assert old1 in src, "Fix 1 anchor not found"
src = src.replace(old1, new1)

# Fix 2: wrap student_personal_details + students + student_addresses in try/except
# AND exclude *_ciphertext columns
def wrap(table_block_old, table_block_new):
    global src
    assert table_block_old in src, f"Fix anchor not found for table block"
    src = src.replace(table_block_old, table_block_new)

# student_personal_details
old_spd = '''    if "student_personal_details" in by_table:
        cols = [f["source_column"] for f in by_table["student_personal_details"]
                if f.get("source_column") and SAFE_IDENTIFIER.match(f["source_column"])]
        if cols:
            col_list = ", ".join(f'"{c}"' for c in cols)
            row = await conn.fetchrow(
                f"SELECT {col_list} FROM public.student_personal_details WHERE student_id = $1",
                student_id,
            )
            if row:
                for field in by_table["student_personal_details"]:
                    col = field.get("source_column")
                    if col and col in row:
                        values[field["field_code"]] = _coerce_for_json(row[col])'''
new_spd = '''    if "student_personal_details" in by_table:
        cols = [f["source_column"] for f in by_table["student_personal_details"]
                if f.get("source_column") and SAFE_IDENTIFIER.match(f["source_column"])
                and not f["source_column"].endswith("_ciphertext")]
        if cols:
            try:
                col_list = ", ".join(f'"{c}"' for c in cols)
                row = await conn.fetchrow(
                    f"SELECT {col_list} FROM public.student_personal_details WHERE student_id = $1",
                    student_id,
                )
                if row:
                    for field in by_table["student_personal_details"]:
                        col = field.get("source_column")
                        if col and col in row:
                            values[field["field_code"]] = _coerce_for_json(row[col])
            except Exception:
                logger.warning("student_personal_details read failed", exc_info=True)'''
wrap(old_spd, new_spd)

# students table
old_s = '''    if "students" in by_table:
        cols = [f["source_column"] for f in by_table["students"]
                if f.get("source_column") and SAFE_IDENTIFIER.match(f["source_column"])]
        if cols:
            col_list = ", ".join(f'"{c}"' for c in cols)
            row = await conn.fetchrow(
                f"SELECT {col_list} FROM public.students WHERE id = $1",
                student_id,
            )
            if row:
                for field in by_table["students"]:
                    col = field.get("source_column")
                    if col and col in row:
                        values[field["field_code"]] = _coerce_for_json(row[col])'''
new_s = '''    if "students" in by_table:
        cols = [f["source_column"] for f in by_table["students"]
                if f.get("source_column") and SAFE_IDENTIFIER.match(f["source_column"])
                and not f["source_column"].endswith("_ciphertext")]
        if cols:
            try:
                col_list = ", ".join(f'"{c}"' for c in cols)
                row = await conn.fetchrow(
                    f"SELECT {col_list} FROM public.students WHERE id = $1",
                    student_id,
                )
                if row:
                    for field in by_table["students"]:
                        col = field.get("source_column")
                        if col and col in row:
                            values[field["field_code"]] = _coerce_for_json(row[col])
            except Exception:
                logger.warning("students read failed", exc_info=True)'''
wrap(old_s, new_s)

# student_addresses
old_addr = '''    if "student_addresses" in by_table:
        by_type: dict[str, list[dict]] = {}
        for f in by_table["student_addresses"]:
            key = f["field_code"].split(".")[1] if "." in f["field_code"] else "permanent"
            by_type.setdefault(key, []).append(f)
        for addr_type, fields in by_type.items():
            cols = [f["source_column"] for f in fields
                    if f.get("source_column") and SAFE_IDENTIFIER.match(f["source_column"])]
            if cols:
                col_list = ", ".join(f'"{c}"' for c in cols)
                row = await conn.fetchrow(
                    f"SELECT {col_list} FROM public.student_addresses WHERE student_id=$1 AND address_type=$2",
                    student_id, addr_type,
                )
                if row:
                    for field in fields:
                        col = field.get("source_column")
                        if col and col in row:
                            values[field["field_code"]] = _coerce_for_json(row[col])'''
new_addr = '''    if "student_addresses" in by_table:
        by_type: dict[str, list[dict]] = {}
        for f in by_table["student_addresses"]:
            key = f["field_code"].split(".")[1] if "." in f["field_code"] else "permanent"
            by_type.setdefault(key, []).append(f)
        for addr_type, fields in by_type.items():
            cols = [f["source_column"] for f in fields
                    if f.get("source_column") and SAFE_IDENTIFIER.match(f["source_column"])
                    and not f["source_column"].endswith("_ciphertext")]
            if cols:
                try:
                    col_list = ", ".join(f'"{c}"' for c in cols)
                    row = await conn.fetchrow(
                        f"SELECT {col_list} FROM public.student_addresses WHERE student_id=$1 AND address_type=$2",
                        student_id, addr_type,
                    )
                    if row:
                        for field in fields:
                            col = field.get("source_column")
                            if col and col in row:
                                values[field["field_code"]] = _coerce_for_json(row[col])
                except Exception:
                    logger.warning("student_addresses %s read failed", addr_type, exc_info=True)'''
wrap(old_addr, new_addr)

# Validate syntax
ast.parse(src)
p.write_text(src, encoding='utf-8', newline='')
print(f"OK - patched focms_parent_portal.py to v0.7.1 ({len(src)} chars)")