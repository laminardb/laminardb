#!/usr/bin/env python3
"""Independently re-derive FpML profile goldens.

The Rust decoder parses documents with libxml2 and projects them through compiled profiles.
This oracle re-reads the same fixtures with Python's ElementTree (expat), evaluates each
profile's selectors with its own implementation of the documented selector grammar, converts
values with `decimal` and `datetime`, and compares every golden row field by field. It shares no
code with the decoder.

Attribute defaults are the one place the two can legitimately differ in method: libxml2
materializes XML Schema attribute defaults and fixed values into the validated tree, while ElementTree never sees
them. For an attribute absent from a document, the oracle accepts a golden value only if the
schema closure declares it as a default of an attribute with that name.

usage: python3 tools/fpml_oracle.py [--crate crates/laminar-connectors]
"""

from __future__ import annotations

import argparse
import decimal
import hashlib
import json
import pathlib
import re
import sys
import tomllib
import xml.etree.ElementTree as ET

XS = "{http://www.w3.org/2001/XMLSchema}"
XSI_TYPE = "{http://www.w3.org/2001/XMLSchema-instance}type"
WHITESPACE = " \t\n\r"
DECIMAL_DEFAULT = (38, 18)
# Enough digits that quantizing a 38-digit value never rounds.
decimal.getcontext().prec = 80
ABSENT = object()


class Defaultable:
    """An attribute absent from the document; libxml2 may have materialized a schema default."""

    def __init__(self, name: str):
        self.name = name


class Mismatch(Exception):
    pass


# ── Schema closure facts ─────────────────────────────────────────────────────


class Closure:
    def __init__(self, directory: pathlib.Path):
        self.substitutions: dict[str, list[str]] = {}
        self.defaults: dict[str, set[str]] = {}
        for path in sorted(directory.glob("*.xsd")):
            root = ET.parse(path).getroot()
            target = root.get("targetNamespace", "")
            for element in root.iter(XS + "element"):
                head = element.get("substitutionGroup")
                if head and element.get("name"):
                    self.substitutions.setdefault(head.split(":")[-1], []).append(
                        "{%s}%s" % (target, element.get("name"))
                    )
            for attribute in root.iter(XS + "attribute"):
                value = attribute.get("default", attribute.get("fixed"))
                if attribute.get("name") and value is not None:
                    self.defaults.setdefault(attribute.get("name"), set()).add(value)

    def members(self, head_local: str) -> set[str]:
        out: set[str] = set()
        pending = [head_local]
        while pending:
            for member in self.substitutions.get(pending.pop(), []):
                if member not in out:
                    out.add(member)
                    pending.append(member.split("}")[1])
        return out


# ── Profiles ─────────────────────────────────────────────────────────────────


class Profile:
    def __init__(self, root_file: pathlib.Path, bundled: pathlib.Path, closure: Closure):
        self.namespaces: dict[str, str] = {}
        self.sets: dict[str, set[str]] = {}
        self.structs: dict[str, list[dict]] = {}
        self.header: dict = {}
        self.columns: list[dict] = []
        self._load(root_file, bundled, True, set())
        self.namespaces.setdefault("", "http://www.fpml.org/FpML-5/confirmation")
        for name, spec in self._set_specs.items():
            if isinstance(spec, list):
                self.sets[name] = {self.element_name(member) for member in spec}
            else:
                self.sets[name] = closure.members(spec["substitution"])

    _set_specs: dict = {}

    def _load(self, path: pathlib.Path, bundled: pathlib.Path, root: bool, seen: set) -> None:
        if path in seen:
            return
        seen.add(path)
        data = tomllib.loads(path.read_text(encoding="utf-8"))
        if root:
            self.header = data["profile"]
            self.columns = []
            self._set_specs = {}
        self.namespaces.update(data.get("namespaces", {}))
        self._set_specs.update(data.get("sets", {}))
        for name, spec in data.get("structs", {}).items():
            self.structs[name] = spec["fields"]
        for include in data.get("include", []):
            if include.startswith("bundled:"):
                target = bundled / include[len("bundled:"):]
            else:
                target = (path.parent / include).resolve()
            self._load(target, bundled, False, seen)
        # An included file's columns precede the including file's own.
        self.columns.extend(data.get("columns", []))

    def element_name(self, text: str) -> str:
        prefix, _, local = text.rpartition(":")
        return "{%s}%s" % (self.namespaces[prefix], local)

    def name_test(self, text: str):
        """Predicate `[name]` or `[{set}]` as a test on a child's tag."""
        if text.startswith("{"):
            return self.sets[text[1:-1]].__contains__
        return self.element_name(text).__eq__

    def attribute_name(self, text: str) -> str:
        prefix, _, local = text.rpartition(":")
        if not prefix:
            return local
        return "{%s}%s" % (self.namespaces[prefix], local)


# ── Documents ────────────────────────────────────────────────────────────────


class Document:
    def __init__(self, data: bytes, reference_namespace: str):
        self.data = data
        self.root = ET.fromstring(data)
        self.parent: dict[ET.Element, ET.Element] = {}
        self.ids: dict[str, ET.Element] = {}
        for element in self.root.iter():
            for child in element:
                self.parent[child] = element
            if element.tag.startswith("{%s}" % reference_namespace) and "id" in element.attrib:
                self.ids[element.attrib["id"]] = element

    def locator(self, element: ET.Element) -> str:
        steps = []
        while element is not None:
            parent = self.parent.get(element)
            local = element.tag.split("}")[-1]
            if parent is None:
                steps.append(local)
            else:
                same = [child for child in parent if child.tag == element.tag]
                ordinal = same.index(element) + 1
                steps.append(local if ordinal == 1 else f"{local}[{ordinal}]")
            element = parent
        return "/" + "/".join(reversed(steps))


def direct_text(element: ET.Element) -> str:
    if len(element):
        raise Mismatch(f"selector reaches element content <{element.tag}>")
    return element.text or ""


# ── Selectors (independent implementation of the documented grammar) ─────────


def split_steps(selector: str) -> list[str]:
    tokens: list[str] = []
    selector = selector.strip()
    if selector.startswith("/"):
        tokens.append("/")
        selector = selector[1:]
    for part in re.split(r"(->)", selector):
        if part == "->":
            tokens.append("->")
        elif part:
            tokens.extend(step for step in part.split("/") if step)
    return tokens


def evaluate(profile: Profile, document: Document, selector: str, context: ET.Element) -> list:
    out: list = []
    for alternative in selector.split("|"):
        out.extend(evaluate_alternative(profile, document, alternative, context))
    return out


def evaluate_alternative(profile, document, selector, context) -> list:
    current = [context]
    for token in split_steps(selector):
        if token == "/":
            current = [document.root]
        elif token == ".":
            pass
        elif token.startswith("self::"):
            has = profile.name_test(token[6:])
            current = [e for e in current if isinstance(e.tag, str) and has(e.tag)]
        elif token.startswith(".["):
            required = [profile.name_test(p) for p in re.findall(r"\[([^\]]+)\]", token)]
            current = [e for e in current if all(any(test(c.tag) for c in e) for test in required)]
        elif token == "..":
            current = [document.parent[e] for e in current if e in document.parent]
        elif token == "**":
            current = [d for e in current for d in e.iter()]
        elif token == "->":
            targets = []
            for element in current:
                href = element.get("href")
                if href is not None:
                    targets.append(document.ids[href.strip()])
            current = targets
        elif token.startswith("@"):
            name = profile.attribute_name(token[1:])
            values = []
            for element in current:
                value = element.get(name)
                values.append(value if value is not None else Defaultable(name))
            return values
        elif token == "#name":
            return [e.tag.split("}")[-1] for e in current]
        elif token == "#type":
            return [e.get(XSI_TYPE).strip().split(":")[-1] for e in current if e.get(XSI_TYPE)]
        elif token == "#locator":
            return [document.locator(e) for e in current]
        elif token == "#text":
            break
        else:
            match = re.fullmatch(r"([^\[]+)((?:\[[^\]]+\])*)", token)
            head, predicates = match.group(1), re.findall(r"\[([^\]]+)\]", match.group(2))
            if head == "*":
                test = lambda tag: True  # noqa: E731
            elif head.startswith("{"):
                members = profile.sets[head[1:-1]]
                test = members.__contains__
            else:
                name = profile.element_name(head)
                test = name.__eq__
            required = [profile.name_test(p) for p in predicates]
            current = [
                child
                for element in current
                for child in element
                if isinstance(child.tag, str)
                and test(child.tag)
                and all(any(isinstance(g.tag, str) and has(g.tag) for g in child) for has in required)
            ]
    return current


# ── Conversions ──────────────────────────────────────────────────────────────


def parse_type(text: str):
    text = text.strip()
    if text.startswith("list<") and text.endswith(">"):
        return ("list", parse_type(text[5:-1]))
    if text.startswith("struct<") and text.endswith(">"):
        return ("struct", text[7:-1].strip())
    if text.startswith("decimal(") and text.endswith(")"):
        precision, scale = (int(part) for part in text[8:-1].split(","))
        return ("decimal", precision, scale)
    if text == "decimal":
        return ("decimal",) + DECIMAL_DEFAULT
    if text.startswith("timestamp("):
        inner = [part.strip() for part in text[10:-1].split(",")]
        return ("timestamp", inner[0], len(inner) > 1)
    return (text,)


def collapse(text: str) -> str:
    return " ".join(text.split())


def convert(kind, value, field: dict):
    if isinstance(value, Defaultable):
        return value
    text = value if isinstance(value, str) else direct_text(value)
    name = kind[0]
    if name == "utf8":
        return collapse(text) if field.get("whitespace") == "collapse" else text
    token = text.strip(WHITESPACE)
    if name == "bool":
        return {"true": True, "1": True, "false": False, "0": False}[token]
    if name in ("int32", "int64", "uint32", "uint64"):
        return int(token)
    if name == "decimal":
        _, precision, scale = kind
        number = decimal.Decimal(token)
        quantum = decimal.Decimal(1).scaleb(-scale)
        if number != number.quantize(quantum):
            raise Mismatch(f"decimal {token} has more than {scale} fractional digits")
        return format(number.quantize(quantum), "f")
    if name == "date32":
        return token[:10]
    if name == "datetime":
        return datetime_struct(token)
    if name == "timestamp":
        _, unit, utc = kind
        parts = datetime_struct(token)
        if not utc or unit != "ns":
            raise Mismatch(f"oracle has no conversion for {kind}")
        if "utc" in parts:
            return parts["utc"]
        if field.get("offset") == "optional":
            return ABSENT
        raise Mismatch(f"{token} has no UTC offset")
    raise Mismatch(f"oracle has no conversion for {kind}")


def datetime_struct(token: str) -> dict:
    match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})?", token)
    day, hour, minute, second, fraction, zone = match.groups()
    nanos = int((fraction or "").ljust(9, "0")[:9] or "0")
    import datetime as dt

    base = dt.datetime.fromisoformat(day)
    if hour == "24":
        base += dt.timedelta(days=1)
        hour = "00"
    local = base.replace(hour=int(hour), minute=int(minute), second=int(second))
    result = {"local": render_time(local, nanos, False)}
    if zone:
        offset = 0 if zone == "Z" else (1 if zone[0] == "+" else -1) * (int(zone[1:3]) * 60 + int(zone[4:6]))
        utc = local - dt.timedelta(minutes=offset)
        result["utc"] = render_time(utc, nanos, True)
        result["offset_minutes"] = offset
    return result


def render_time(moment, nanos: int, utc: bool) -> str:
    text = moment.strftime("%Y-%m-%dT%H:%M:%S")
    if nanos:
        digits = 9
        if nanos % 1_000_000 == 0:
            digits = 3
        elif nanos % 1_000 == 0:
            digits = 6
        text += "." + str(nanos).rjust(9, "0")[:digits]
    return text + ("Z" if utc else "")


# ── Row evaluation ───────────────────────────────────────────────────────────


def project_field(profile, document, field: dict, context, meta) -> object:
    kind = parse_type(field["type"])
    if "meta" in field:
        value = meta[field["meta"]]
        return value
    values = evaluate(profile, document, field["path"], context)
    if not values:
        return ABSENT
    if kind[0] == "list":
        item = kind[1]
        return [project_value(profile, document, item, value, field, meta) for value in values]
    if len(values) != 1:
        raise Mismatch(f"{field['name']}: {len(values)} values for a scalar")
    return project_value(profile, document, kind, values[0], field, meta)


def project_value(profile, document, kind, value, field, meta):
    if kind[0] == "struct":
        object_ = {}
        for child in profile.structs[kind[1]]:
            child_value = project_field(profile, document, child, value, meta)
            if child_value is not ABSENT:
                object_[child["name"]] = child_value
        return object_
    return convert(kind, value, field)


def project(profile: Profile, document: Document) -> list[dict]:
    rows = evaluate(profile, document, profile.header["rows"], document.root)
    out = []
    for ordinal, row in enumerate(rows):
        meta = {
            "document_sha256": hashlib.sha256(document.data).hexdigest(),
            "document_bytes": len(document.data),
            "root": document.root.tag.split("}")[-1],
            "fpml_version": document.root.get("fpmlVersion"),
            "row": ordinal,
            "row_count": len(rows),
            "row_locator": document.locator(row),
        }
        projected = {}
        for column in profile.columns:
            value = project_field(profile, document, column, row, meta)
            if value is not ABSENT:
                projected[column["name"]] = value
        out.append(projected)
    return out


# ── Comparison ───────────────────────────────────────────────────────────────


def compare(expected, actual, closure: Closure, path: str, problems: list[str]) -> None:
    if isinstance(expected, Defaultable):
        lexical = json.dumps(actual) if isinstance(actual, (bool, int)) else actual
        if actual is not ABSENT and lexical not in closure.defaults.get(expected.name, set()):
            problems.append(f"{path}: {actual!r} is not a schema default of @{expected.name}")
        return
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key in sorted(set(expected) | set(actual)):
            left = expected.get(key, ABSENT)
            right = actual.get(key, ABSENT)
            if isinstance(left, Defaultable):
                compare(left, right, closure, f"{path}.{key}", problems)
            elif left is ABSENT or right is ABSENT:
                problems.append(f"{path}.{key}: oracle {describe(left)}, golden {describe(right)}")
            else:
                compare(left, right, closure, f"{path}.{key}", problems)
        return
    if isinstance(expected, list) and isinstance(actual, list):
        if len(expected) != len(actual):
            problems.append(f"{path}: oracle has {len(expected)} items, golden {len(actual)}")
            return
        for index, (left, right) in enumerate(zip(expected, actual)):
            compare(left, right if right is not None else ABSENT, closure, f"{path}[{index}]", problems)
        return
    if expected != actual:
        problems.append(f"{path}: oracle {expected!r}, golden {actual!r}")


def describe(value) -> str:
    return "absent" if value is ABSENT else repr(value)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--crate", default="crates/laminar-connectors", type=pathlib.Path)
    args = parser.parse_args()
    crate = args.crate
    closure = Closure(crate / "fpml/artifacts/fpml-5-13-8-rec-2/confirmation")
    bundled = crate / "fpml/profiles"
    profiles = {}
    for path in sorted(bundled.rglob("*.toml")):
        header = tomllib.loads(path.read_text(encoding="utf-8")).get("profile")
        if header:
            profiles[header["name"]] = path
    fixtures = crate / "tests/fixtures/fpml"
    listing = (fixtures / "fpml-5-13-8-rec-2/reference-annotation-violations.txt").read_text(encoding="utf-8")
    violations = {line.strip() for line in listing.splitlines() if line.strip() and not line.startswith("#")}
    checked = rows = 0
    problems: list[str] = []
    for name, root_file in profiles.items():
        profile = Profile(root_file, bundled, closure)
        golden_root = fixtures / "goldens" / name
        for relative in (golden_root / "zero-rows.txt").read_text(encoding="utf-8").split():
            kind, _, rest = relative.partition("/")
            source = fixtures / ("fpml-5-13-8-rec-2/confirmation" if kind == "official" else "synthetic") / rest
            document = Document(source.read_bytes(), "http://www.fpml.org/FpML-5/confirmation")
            try:
                produced = len(project(profile, document))
            except Mismatch as error:
                problems.append(f"{name} {relative}: {error}")
                continue
            if produced:
                problems.append(f"{name} {relative}: listed with zero rows, oracle has {produced}")
            checked += 1
        for golden in sorted(golden_root.rglob("*.json")):
            relative = golden.relative_to(golden_root).as_posix()[: -len(".json")]
            kind, _, rest = relative.partition("/")
            source = fixtures / ("fpml-5-13-8-rec-2/confirmation" if kind == "official" else "synthetic") / rest
            actual = json.loads(golden.read_text(encoding="utf-8"))
            if isinstance(actual, dict):
                # A rejection is accepted only for a document the schema-policy reference check
                # flags; the decoder's own suites pin the exact reason.
                reason = actual.get("rejected", "")
                if kind != "official" or rest not in violations or "rejected at references" not in reason:
                    problems.append(f"{name} {relative}: unexpected rejection golden {actual!r}")
                checked += 1
                continue
            document = Document(source.read_bytes(), "http://www.fpml.org/FpML-5/confirmation")
            try:
                expected = project(profile, document)
            except Mismatch as error:
                problems.append(f"{name} {relative}: {error}")
                continue
            local: list[str] = []
            if len(expected) != len(actual):
                local.append(f"oracle has {len(expected)} rows, golden {len(actual)}")
            else:
                for index, (left, right) in enumerate(zip(expected, actual)):
                    compare(left, right, closure, f"row[{index}]", local)
            problems.extend(f"{name} {relative}: {problem}" for problem in local[:5])
            checked += 1
            rows += len(actual)
    print(f"fpml oracle: {checked} goldens, {rows} rows, {len(problems)} disagreements")
    for problem in problems[:200]:
        print(problem)
    return 1 if problems or checked == 0 else 0


if __name__ == "__main__":
    sys.exit(main())
