#!/usr/bin/env python3
"""Convert IDMC JSON/XML exports or ZIP archives into CIR JSON.

Examples:
    python idmc_to_cir.py export.xml --output canonical.json
    python idmc_to_cir.py export.json --output canonical.json
    python idmc_to_cir.py export.zip --output canonical.json

This script extracts transformation metadata for downstream processing.
It does not itself generate or execute PySpark code.
"""

from __future__ import annotations

import argparse
import json
import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Iterator


SUPPORTED_TRANSFORMATIONS = {
    "expression": "Expression",
    "filter": "Filter",
    "router": "Router",
    "aggregator": "Aggregator",
    "joiner": "Joiner",
    "lookup": "Lookup",
    "sorter": "Sorter",
    "rank": "Rank",
    "union": "Union",
    "sequencegenerator": "Sequence Generator",
    "updatestrategy": "Update Strategy",
    "normalizer": "Normalizer",
    "hierarchyparser": "Hierarchy Parser",
    "hierarchybuilder": "Hierarchy Builder",
    "storedprocedure": "Stored Procedure",
}


def norm(value: Any) -> str:
    """Normalize names for case-insensitive alias matching."""
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def get_value(
    obj: Any,
    *names: str,
    default: Any = "",
) -> Any:
    """Get a dictionary value using normalized key matching."""
    if not isinstance(obj, dict):
        return default

    wanted = {norm(name) for name in names}

    for key, value in obj.items():
        if norm(key) in wanted and value is not None:
            return value

    return default


def as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def xml_element_to_dict(element: ET.Element) -> dict[str, Any]:
    """Convert XML into dictionaries while retaining tag, attributes, and text."""
    result: dict[str, Any] = {"__tag__": element.tag}
    result.update(element.attrib)

    for child in list(element):
        child_value = xml_element_to_dict(child)
        existing = result.get(child.tag)

        if existing is None:
            result[child.tag] = child_value
        elif isinstance(existing, list):
            existing.append(child_value)
        else:
            result[child.tag] = [existing, child_value]

    text = (element.text or "").strip()
    if text:
        result["#text"] = text

    return result


def parse_document(data: bytes, filename: str) -> Any:
    """Parse a JSON or XML document based on its extension/content."""
    suffix = Path(filename).suffix.lower()
    text = data.decode("utf-8-sig")

    if suffix == ".xml" or text.lstrip().startswith("<"):
        return xml_element_to_dict(ET.fromstring(text))

    if suffix == ".json" or text.lstrip().startswith(("{", "[")):
        return json.loads(text)

    raise ValueError(f"Unsupported input file type: {filename}")


def load_documents(path: Path) -> list[tuple[str, Any]]:
    """Load JSON/XML documents from a file, ZIP archive, or directory."""
    loaded: list[tuple[str, Any]] = []

    def load_bytes(data: bytes, name: str) -> None:
        try:
            loaded.append((name, parse_document(data, name)))
        except (json.JSONDecodeError, ET.ParseError, UnicodeDecodeError, ValueError) as exc:
            print(f"Skipping invalid document {name}: {exc}")

    if path.is_file() and path.suffix.lower() == ".zip":
        try:
            with zipfile.ZipFile(path, "r") as archive:
                for member in archive.infolist():
                    if member.is_dir():
                        continue
                    if Path(member.filename).suffix.lower() not in {".json", ".xml"}:
                        continue
                    with archive.open(member) as stream:
                        load_bytes(stream.read(), member.filename)
        except (OSError, zipfile.BadZipFile) as exc:
            raise SystemExit(f"Unable to read ZIP {path}: {exc}") from exc

    elif path.is_file():
        try:
            load_bytes(path.read_bytes(), str(path))
        except OSError as exc:
            raise SystemExit(f"Unable to read {path}: {exc}") from exc

    elif path.is_dir():
        for file in sorted(path.rglob("*")):
            if file.is_file() and file.suffix.lower() in {".json", ".xml"}:
                try:
                    load_bytes(file.read_bytes(), str(file))
                except OSError as exc:
                    print(f"Skipping {file}: {exc}")

    else:
        raise FileNotFoundError(f"Input does not exist: {path}")

    return loaded


def walk(
    value: Any,
    parent_key: str = "",
) -> Iterator[tuple[dict[str, Any], str]]:
    """Yield every dictionary and its containing key/tag."""
    if isinstance(value, dict):
        tag = str(value.get("__tag__") or parent_key)
        yield value, tag

        for key, child in value.items():
            if key in {"__tag__", "#text"}:
                continue
            yield from walk(child, str(key))

    elif isinstance(value, list):
        for child in value:
            yield from walk(child, parent_key)


def object_type(obj: dict[str, Any], parent_key: str = "") -> str:
    raw = get_value(
        obj,
        "@type",
        "type",
        "objectType",
        "documentType",
        "transformationType",
        "assetType",
        "className",
        default="",
    )

    text = str(raw or obj.get("__tag__") or parent_key).split(".")[-1]
    compact = norm(text)

    if compact in {"mapping", "mappingtemplate"} or compact.endswith("mapping"):
        return "Mapping"
    if "mappingtask" in compact:
        return "MappingTask"
    if "workflow" in compact:
        return "Workflow"
    if "mapplet" in compact:
        return "Mapplet"
    if "connection" in compact:
        return "Connection"
    if "function" in compact:
        return "Function"

    # Check specific transformation names before generic source/target matching.
    for key, display_name in SUPPORTED_TRANSFORMATIONS.items():
        if key in compact:
            return display_name

    if "sourcequalifier" in compact or compact == "source":
        return "Source"
    if "target" in compact:
        return "Target"

    return str(raw or "")


def object_id(obj: Any) -> str:
    if not isinstance(obj, dict):
        return ""
    return str(
        get_value(
            obj,
            "id",
            "objectId",
            "assetId",
            "guid",
            "uuid",
            "assetFrsGuid",
            default="",
        )
        or ""
    )


def object_name(obj: Any) -> str:
    if not isinstance(obj, dict):
        return ""
    return str(
        get_value(
            obj,
            "name",
            "objectName",
            "assetName",
            "label",
            default="",
        )
        or ""
    )


def canonical_id(kind: str, obj: dict[str, Any]) -> str:
    return f"{norm(kind)}:{object_id(obj) or object_name(obj) or 'unknown'}"


def trace(
    canonical_id_value: str,
    obj: dict[str, Any],
    kind: str,
) -> dict[str, str]:
    original_type = get_value(
        obj,
        "@type",
        "type",
        "objectType",
        "documentType",
        "transformationType",
        "assetType",
        "className",
        default=kind,
    )
    return {
        "canonicalId": canonical_id_value,
        "originalAssetId": object_id(obj),
        "originalType": str(original_type or kind),
        "originalName": object_name(obj),
    }


def extract_fields(obj: Any) -> list[dict[str, Any]]:
    """Extract field definitions and transformation-specific field attributes."""
    if not isinstance(obj, dict):
        return []

    fields = get_value(
        obj,
        "fields",
        "field",
        "columns",
        "ports",
        "attributes",
        default=[],
    )

    result = []
    for field in as_list(fields):
        if isinstance(field, str):
            result.append({"name": field})
            continue
        if not isinstance(field, dict):
            continue

        name = object_name(field) or str(
            get_value(field, "fieldName", "columnName", default="")
        )
        if not name:
            continue

        entry: dict[str, Any] = {"name": name}

        datatype = get_value(field, "dataType", "datatype", "type")
        if datatype:
            entry["dataType"] = str(datatype)

        for output_key, aliases in (
            ("precision", ("precision",)),
            ("scale", ("scale",)),
            ("nullable", ("nullable", "isNullable")),
            ("expression", ("expression", "logic", "value")),
            ("groupBy", ("groupBy", "isGroupBy")),
            ("direction", ("direction", "sortDirection")),
        ):
            value = get_value(field, *aliases)
            if value != "":
                entry[output_key] = value

        result.append(entry)

    return result


def child_items(obj: dict[str, Any], *names: str) -> list[Any]:
    """Get child objects from named aliases, including XML tag names."""
    return as_list(get_value(obj, *names, default=[]))


def child_text(value: Any) -> str:
    """Read text from a string or converted XML element."""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        return str(
            get_value(value, "#text", "text", "value", default="")
        ).strip()
    return ""


def extract_groups(obj: dict[str, Any]) -> list[dict[str, str]]:
    groups = child_items(obj, "groups", "groupFilters", "group")
    result = []

    for group in groups:
        if isinstance(group, dict):
            name = object_name(group) or "group"
            condition = child_text(
                get_value(
                    group,
                    "condition",
                    "filterCondition",
                    "#text",
                    "text",
                    default="",
                )
            )
            result.append({"name": name, "condition": condition})
        elif isinstance(group, str):
            result.append({"name": "group", "condition": group.strip()})

    return result


def extract_sort_fields(obj: dict[str, Any]) -> list[dict[str, str]]:
    sort_keys = child_items(obj, "sortKeys", "sortKey", "sortFields", "orderBy")
    result = []

    for item in sort_keys:
        if isinstance(item, str):
            result.append({"name": item, "direction": "ASC"})
        elif isinstance(item, dict):
            name = object_name(item) or str(
                get_value(item, "fieldName", "columnName", default="")
            )
            if name:
                result.append({
                    "name": name,
                    "direction": str(
                        get_value(item, "direction", "order", default="ASC")
                    ).upper(),
                })

    return result


def extract_aggregates(obj: dict[str, Any]) -> tuple[list[str], list[dict[str, str]]]:
    """Extract GROUP BY fields and aggregate expressions from field metadata."""
    group_by = as_list(
        get_value(obj, "groupBy", "groupByFields", default=[])
    )
    group_names = [
        str(item) if isinstance(item, str) else object_name(item)
        for item in group_by
    ]
    aggregates = []

    for field in extract_fields(obj):
        group_flag = str(field.get("groupBy", "")).lower()
        if group_flag in {"yes", "true", "1"}:
            group_names.append(field["name"])

        expression = str(field.get("expression") or "")
        if expression:
            aggregates.append({
                "targetField": field["name"],
                "expression": expression,
            })

    return sorted(set(name for name in group_names if name)), aggregates


def extract_transform_properties(
    obj: dict[str, Any],
    kind: str,
) -> dict[str, Any]:
    """Extract operation-specific properties used by downstream generators."""
    properties: dict[str, Any] = {}
    kind_norm = norm(kind)

    if kind_norm == "expression":
        properties["expressions"] = [
            {
                "targetColumn": field["name"],
                "expression": field["expression"],
            }
            for field in extract_fields(obj)
            if field.get("expression")
        ]

    elif kind_norm == "filter":
        properties["condition"] = child_text(
            get_value(
                obj,
                "condition",
                "filterCondition",
                "logic",
                "expression",
                "#text",
                default="",
            )
        )

    elif kind_norm == "router":
        properties["groups"] = extract_groups(obj)

    elif kind_norm == "aggregator":
        group_by, aggregates = extract_aggregates(obj)
        properties["groupBy"] = group_by
        properties["aggregates"] = aggregates

    elif kind_norm in {"joiner", "lookup"}:
        properties.update({
            "joinType": str(get_value(obj, "joinType", default="inner")),
            "master": str(get_value(obj, "master", "masterSource", default="")),
            "detail": str(get_value(obj, "detail", "detailSource", default="")),
            "lookupObject": str(
                get_value(
                    obj,
                    "lookupObject",
                    "lookupSource",
                    "referenceObject",
                    default="",
                )
            ),
            "condition": child_text(
                get_value(
                    obj,
                    "condition",
                    "joinCondition",
                    "lookupCondition",
                    default="",
                )
            ),
            "returnColumns": [
                field["name"]
                for field in extract_fields(obj)
            ],
        })

    elif kind_norm == "sorter":
        properties["sortFields"] = extract_sort_fields(obj)

    elif kind_norm == "rank":
        properties["rankCondition"] = child_text(
            get_value(
                obj,
                "rankCondition",
                "condition",
                "#text",
                default="",
            )
        )
        properties["rankType"] = str(
            get_value(obj, "rankType", "topBottom", default="TOP")
        ).upper()
        properties["rankNumber"] = get_value(
            obj,
            "rankNumber",
            "numberOfRanks",
            "topN",
            "bottomN",
            default="",
        )
        properties["sortFields"] = extract_sort_fields(obj)
        properties["fields"] = extract_fields(obj)

    elif kind_norm == "union":
        inputs = child_items(obj, "inputs", "input")
        properties["inputFrames"] = [
            object_name(item) or str(get_value(item, "name", default=""))
            for item in inputs
            if isinstance(item, dict)
        ]

    elif kind_norm == "sequencegenerator":
        properties["targetField"] = str(
            get_value(obj, "targetField", "fieldName", default="")
        )
        properties["startValue"] = get_value(
            obj,
            "startValue",
            "start",
            default="",
        )
        properties["incrementBy"] = get_value(
            obj,
            "incrementBy",
            "increment",
            default="",
        )

    elif kind_norm == "updatestrategy":
        properties["strategy"] = child_text(
            get_value(
                obj,
                "strategy",
                "updateStrategy",
                "#text",
                default="",
            )
        )
        properties["fields"] = extract_fields(obj)

    elif kind_norm == "normalizer":
        properties["fields"] = extract_fields(obj)
        properties["occurs"] = get_value(obj, "occurs", "repeatCount", default="")

    else:
        properties["fields"] = extract_fields(obj)

    # Preserve simple scalar properties not already represented above.
    excluded = {
        norm(key)
        for key in (
            "__tag__",
            "#text",
            "type",
            "objectType",
            "transformationType",
            "id",
            "objectId",
            "name",
            "fields",
            "field",
            "columns",
            "ports",
            "incomingPorts",
            "outgoingPorts",
        )
    }

    for key, value in obj.items():
        if (
            norm(key) not in excluded
            and not isinstance(value, (dict, list))
            and key not in properties
        ):
            properties.setdefault(key, value)

    return properties


def extract_ports(obj: dict[str, Any], *aliases: str) -> list[Any]:
    return as_list(get_value(obj, *aliases, default=[]))


def transformation(
    obj: dict[str, Any],
    kind: str,
) -> dict[str, Any]:
    logic = get_value(
        obj,
        "logic",
        "expression",
        "condition",
        "filterCondition",
        "sql",
        default="",
    )

    return {
        "id": object_id(obj),
        "name": object_name(obj),
        "type": kind,
        "incomingPorts": extract_ports(
            obj,
            "incomingPorts",
            "inputPorts",
            "inputs",
        ),
        "outgoingPorts": extract_ports(
            obj,
            "outgoingPorts",
            "outputPorts",
            "outputs",
        ),
        "fields": extract_fields(obj),
        "properties": extract_transform_properties(obj, kind),
        "logic": child_text(logic),
    }


def endpoint(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()

    if isinstance(value, dict):
        name = object_name(value) or str(
            get_value(value, "fieldName", "portName", default="")
        )
        parent = str(
            get_value(value, "transformationName", "objectName", default="")
        )
        return f"{parent}.{name}" if parent and name else name or parent

    return ""


def extract_links(mapping_obj: dict[str, Any]) -> list[Any]:
    return as_list(
        get_value(
            mapping_obj,
            "links",
            "connectors",
            "fieldLinks",
            default=[],
        )
    )


def mapping_lineage(
    links: list[Any],
) -> list[dict[str, Any]]:
    lineage = []

    for link in links:
        if not isinstance(link, dict):
            continue

        source = endpoint(
            get_value(link, "source", "from", "sourceField", "fromPort")
        )
        target = endpoint(
            get_value(link, "target", "to", "targetField", "toPort")
        )

        if source and target:
            lineage.append({
                "sourceField": source,
                "targetField": target,
                "transformationPath": as_list(
                    get_value(link, "transformationPath", default=[])
                ),
            })

    return lineage


def collect_nested_assets(
    mapping_obj: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    sources: list[dict[str, Any]] = []
    targets: list[dict[str, Any]] = []
    transforms: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    for obj, parent_key in walk(mapping_obj):
        if obj is mapping_obj:
            continue

        kind = object_type(obj, parent_key)
        key = (kind, object_id(obj), object_name(obj))
        if key in seen:
            continue
        seen.add(key)

        if kind == "Source":
            sources.append({
                "id": object_id(obj),
                "name": object_name(obj),
                "objectType": str(
                    get_value(obj, "objectType", "type", default="")
                ),
                "connectionId": str(
                    get_value(obj, "connectionId", "connectionRef", default="")
                ),
                "schema": str(
                    get_value(obj, "schema", "schemaName", default="")
                ),
                "objectName": str(
                    get_value(
                        obj,
                        "objectName",
                        "tableName",
                        "sourceName",
                        default=object_name(obj),
                    )
                ),
                "format": str(get_value(obj, "format", "fileFormat", default="")),
                "path": str(get_value(obj, "path", "filePath", default="")),
                "fields": extract_fields(obj),
            })

        elif kind == "Target":
            targets.append({
                "id": object_id(obj),
                "name": object_name(obj),
                "objectType": str(
                    get_value(obj, "objectType", "type", default="")
                ),
                "connectionId": str(
                    get_value(obj, "connectionId", "connectionRef", default="")
                ),
                "schema": str(
                    get_value(obj, "schema", "schemaName", default="")
                ),
                "objectName": str(
                    get_value(
                        obj,
                        "objectName",
                        "tableName",
                        "targetName",
                        default=object_name(obj),
                    )
                ),
                "format": str(get_value(obj, "format", "fileFormat", default="")),
                "path": str(get_value(obj, "path", "filePath", default="")),
                "writeMode": str(
                    get_value(obj, "writeMode", "mode", default="")
                ),
                "fields": extract_fields(obj),
            })

        elif kind in SUPPORTED_TRANSFORMATIONS.values():
            transforms.append(transformation(obj, kind))

    return sources, targets, transforms


def make_mapping(
    obj: dict[str, Any],
    traceability: list[dict[str, str]],
) -> dict[str, Any]:
    sources, targets, transforms = collect_nested_assets(obj)
    links = extract_links(obj)

    mapping = {
        "id": object_id(obj),
        "name": object_name(obj),
        "sources": sources,
        "targets": targets,
        "parameters": as_list(
            get_value(obj, "parameters", "mappingParameters", default=[])
        ),
        "transformations": transforms,
        "links": links,
    }

    traceability.append(trace(canonical_id("Mapping", obj), obj, "Mapping"))

    for kind, entries in (
        ("Source", sources),
        ("Target", targets),
        ("Transformation", transforms),
    ):
        for entry in entries:
            traceability.append({
                "canonicalId": (
                    f"{norm(kind)}:"
                    f"{entry.get('id') or entry.get('name') or 'unknown'}"
                ),
                "originalAssetId": str(entry.get("id", "") or ""),
                "originalType": str(
                    entry.get("type")
                    or entry.get("objectType")
                    or kind
                ),
                "originalName": str(entry.get("name", "") or ""),
            })

    return mapping


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Convert IDMC JSON/XML exports, directories, or ZIP archives "
            "into CIR JSON."
        )
    )
    parser.add_argument(
        "input",
        type=Path,
        help="A JSON/XML file, directory, or ZIP archive",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("canonical.json"),
        help="Output CIR JSON path",
    )
    args = parser.parse_args()

    documents = load_documents(args.input)
    all_objects: list[tuple[dict[str, Any], str]] = []

    project_name = args.input.stem if args.input.is_file() else args.input.name

    for _, data in documents:
        if isinstance(data, dict):
            project_name = str(
                get_value(data, "projectName", "project", default=project_name)
                or project_name
            )
        all_objects.extend(walk(data))

    result: dict[str, Any] = {
        "project": {
            "name": project_name,
            "id": "",
            "type": "IDMC",
        },
        "connections": [],
        "functions": [],
        "mapplets": [],
        "mappings": [],
        "mappingTasks": [],
        "workflows": [],
        "dependencies": [],
        "lineage": [],
        "metrics": {},
        "traceability": [],
    }

    seen: set[tuple[str, str, str]] = set()

    for obj, parent_key in all_objects:
        kind = object_type(obj, parent_key)
        oid = object_id(obj)
        name = object_name(obj)

        unique_key = (kind, oid, name)
        if unique_key in seen:
            continue
        seen.add(unique_key)

        if kind == "Connection":
            result["connections"].append({
                "id": oid,
                "name": name,
                "type": str(
                    get_value(
                        obj,
                        "connectionType",
                        "type",
                        "connectorType",
                        default="",
                    )
                ),
                "database": str(
                    get_value(obj, "database", "databaseName", default="")
                ),
                "host": str(
                    get_value(obj, "host", "hostname", "server", default="")
                ),
                "schema": str(
                    get_value(obj, "schema", "schemaName", default="")
                ),
                "runtimeEnvironment": str(
                    get_value(
                        obj,
                        "runtimeEnvironment",
                        "runtimeEnvironmentName",
                        default="",
                    )
                ),
            })
            result["traceability"].append(
                trace(canonical_id(kind, obj), obj, kind)
            )

        elif kind == "Mapping":
            mapping = make_mapping(obj, result["traceability"])
            result["mappings"].append(mapping)
            result["lineage"].extend(mapping_lineage(mapping["links"]))

        elif kind == "MappingTask":
            result["mappingTasks"].append({
                "id": oid,
                "name": name,
                "mappingId": str(
                    get_value(
                        obj,
                        "mappingId",
                        "mappingReference",
                        default="",
                    )
                ),
                "runtimeEnvironment": str(
                    get_value(
                        obj,
                        "runtimeEnvironment",
                        "runtimeEnvironmentName",
                        default="",
                    )
                ),
                "schedule": str(
                    get_value(obj, "schedule", "scheduleName", default="")
                ),
                "properties": obj,
            })
            result["traceability"].append(
                trace(canonical_id(kind, obj), obj, kind)
            )

        elif kind == "Workflow":
            steps = []
            for step, step_parent in walk(obj):
                if step is obj:
                    continue

                step_type = object_type(step, step_parent)
                if step_type:
                    steps.append({
                        "type": step_type,
                        "assetReference": str(
                            get_value(
                                step,
                                "assetReference",
                                "assetId",
                                "taskId",
                                "objectName",
                                default="",
                            )
                        ),
                        "nextStep": str(
                            get_value(
                                step,
                                "nextStep",
                                "next",
                                "nextStepName",
                                default="",
                            )
                        ),
                    })

            result["workflows"].append({
                "id": oid,
                "name": name,
                "steps": steps,
            })
            result["traceability"].append(
                trace(canonical_id(kind, obj), obj, kind)
            )

        elif kind == "Function":
            result["functions"].append({
                "id": oid,
                "name": name,
                "returnType": str(
                    get_value(obj, "returnType", "dataType", default="")
                ),
                "logic": str(
                    get_value(obj, "logic", "expression", "body", default="")
                ),
            })
            result["traceability"].append(
                trace(canonical_id(kind, obj), obj, kind)
            )

        elif kind == "Mapplet":
            result["mapplets"].append({
                "id": oid,
                "name": name,
                "transformations": [],
                "properties": obj,
            })
            result["traceability"].append(
                trace(canonical_id(kind, obj), obj, kind)
            )

    for obj, parent_key in all_objects:
        parent_type = object_type(obj, parent_key)
        parent_name = object_name(obj)
        refs = get_value(obj, "references", "dependencies", default=[])

        for ref in as_list(refs):
            if not isinstance(ref, dict):
                continue

            child_type = str(
                get_value(ref, "refType", "type", "objectType", default="")
            )
            child_name = str(
                get_value(ref, "name", "objectName", default="")
            )
            child_id = str(
                get_value(ref, "refObjectId", "objectId", "id", default="")
            )

            result["dependencies"].append({
                "parentType": parent_type,
                "parentName": parent_name,
                "childType": child_type,
                "childName": child_name or child_id,
            })

    transformations = [
        item
        for mapping in result["mappings"]
        for item in mapping["transformations"]
    ]
    transform_types = [norm(item["type"]) for item in transformations]

    source_count = sum(
        len(mapping["sources"])
        for mapping in result["mappings"]
    )
    target_count = sum(
        len(mapping["targets"])
        for mapping in result["mappings"]
    )

    counts = {
        "sourceCount": source_count,
        "targetCount": target_count,
        "transformationCount": len(transformations),
    }

    for key, kind in (
        ("expressionCount", "expression"),
        ("filterCount", "filter"),
        ("routerCount", "router"),
        ("aggregatorCount", "aggregator"),
        ("joinerCount", "joiner"),
        ("lookupCount", "lookup"),
        ("sorterCount", "sorter"),
        ("rankCount", "rank"),
        ("unionCount", "union"),
        ("sequenceGeneratorCount", "sequencegenerator"),
        ("updateStrategyCount", "updatestrategy"),
        ("normalizerCount", "normalizer"),
    ):
        counts[key] = sum(item == kind for item in transform_types)

    counts["workflowCount"] = len(result["workflows"])
    counts["taskCount"] = len(result["mappingTasks"])

    total_assets = (
        counts["sourceCount"]
        + counts["targetCount"]
        + counts["transformationCount"]
        + counts["workflowCount"]
        + counts["taskCount"]
    )
    counts["estimatedComplexity"] = (
        "Simple"
        if total_assets <= 5
        else "Medium"
        if total_assets <= 15
        else "Complex"
        if total_assets <= 30
        else "VeryComplex"
    )

    result["metrics"] = counts

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"Created: {args.output}")


if __name__ == "__main__":
    main()