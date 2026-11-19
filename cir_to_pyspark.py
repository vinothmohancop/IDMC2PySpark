#!/usr/bin/env python3
"""Generate PySpark code and migration reports from CIR or canonical IDMC JSON.

Usage:
    python cir_to_pyspark.py canonical.json --output-dir migration_output

The generated PySpark script uses best-effort conversions based on the CIR fields
available. Unsupported or incomplete metadata is recorded in the risk and
unresolved-logic reports, and unsupported runtime operations raise an exception.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


GENERATOR_VERSION = "1.0.0"

TOP_LEVEL_ASSET_TYPES = (
    "connections",
    "functions",
    "mapplets",
    "mappings",
    "mappingTasks",
    "workflows",
)


def safe_name(value: Any, fallback: str = "unnamed") -> str:
    """Convert an arbitrary name to a Python identifier."""
    text = re.sub(r"\W+", "_", str(value or "")).strip("_")
    if not text:
        text = fallback
    if text[0].isdigit():
        text = f"_{text}"
    return text


def object_name(obj: Any, fallback: str = "unnamed") -> str:
    if isinstance(obj, dict):
        return str(obj.get("name") or obj.get("objectName") or fallback)
    return fallback


def object_id(obj: Any) -> str:
    if not isinstance(obj, dict):
        return ""
    return str(
        obj.get("id")
        or obj.get("assetId")
        or obj.get("assetFrsGuid")
        or obj.get("objectId")
        or ""
    )


def get_ci(obj: Any, *names: str, default: Any = None) -> Any:
    """Get a dictionary value using case-insensitive key matching."""
    if not isinstance(obj, dict):
        return default

    lowered = {str(key).lower(): value for key, value in obj.items()}
    for name in names:
        if name.lower() in lowered:
            return lowered[name.lower()]
    return default


def as_list(value: Any) -> list[Any]:
    """Normalize a single item or list to a list."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def normalize_field(field: Any) -> dict[str, Any] | None:
    """Normalize common IDMC field attributes."""
    if not isinstance(field, dict):
        return None

    name = get_ci(field, "name", "fieldName", "columnName")
    if not name:
        return None

    normalized: dict[str, Any] = {"name": str(name)}

    datatype = get_ci(field, "datatype", "dataType", "type")
    if datatype:
        normalized["datatype"] = str(datatype)

    expression = get_ci(field, "expression", "logic", "value")
    if expression:
        normalized["expression"] = str(expression)

    return normalized


def normalize_canonical_input(cir: dict[str, Any]) -> dict[str, Any]:
    """Normalize canonical JSON into a CIR structure with a mappings array.

    Inputs that already contain a non-empty CIR mappings array are returned
    unchanged. For the canonical app format, SOURCEQUALIFIER and EXPRESSION
    are read from raw_idmc_export, including uppercase IDMC field keys.
    """
    if cir.get("mappings"):
        return cir

    raw_export = cir.get("raw_idmc_export") or cir.get("rawIdmcExport")
    if not isinstance(raw_export, dict):
        return cir

    raw_mapping_name = get_ci(
        raw_export,
        "name",
        "mappingName",
        default="idmc_mapping",
    )

    source_obj = get_ci(
        raw_export,
        "SOURCEQUALIFIER",
        "sourceQualifier",
        "source",
        default={},
    )
    expression_obj = get_ci(
        raw_export,
        "EXPRESSION",
        "expression",
        default={},
    )

    source_name = get_ci(
        source_obj,
        "name",
        "objectName",
        default="source",
    )

    source_fields = []
    for field in as_list(
        get_ci(source_obj, "field", "fields", default=[])
    ):
        normalized_field = normalize_field(field)
        if normalized_field is not None:
            source_fields.append(normalized_field)

    expression_entries = []
    for field in as_list(
        get_ci(expression_obj, "field", "fields", default=[])
    ):
        normalized_field = normalize_field(field)
        if normalized_field is None:
            continue

        expression = normalized_field.get("expression")
        if expression:
            expression_entries.append(
                {
                    "targetColumn": normalized_field["name"],
                    "expression": expression,
                }
            )

    # Prefer populated canonical top-level arrays. The example app emits empty
    # arrays, so its raw IDMC export is used when those arrays have no entries.
    sources = cir.get("sources") or [
        {
            "name": str(source_name),
            "objectName": str(source_name),
            "fields": source_fields,
        }
    ]

    transformations = cir.get("transformations") or []
    if not transformations and expression_entries:
        expression_name = get_ci(
            expression_obj,
            "name",
            "objectName",
            default="expression",
        )
        transformations = [
            {
                "name": str(expression_name),
                "type": "Expression",
                "properties": {
                    "expressions": expression_entries,
                },
            }
        ]

    targets = cir.get("targets") or []

    normalized_cir = dict(cir)
    normalized_cir["mappings"] = [
        {
            "name": str(raw_mapping_name),
            "sources": sources,
            "transformations": transformations,
            "targets": targets,
        }
    ]

    return normalized_cir


def add_risk(
    risks: list[dict[str, str]],
    unresolved: list[dict[str, str]],
    *,
    severity: str,
    asset_name: str,
    asset_type: str,
    issue: str,
    impact: str,
    recommendation: str,
    source_logic: str = "",
) -> None:
    risk_id = f"RISK-{len(risks) + 1:04d}"
    risks.append(
        {
            "riskId": risk_id,
            "severity": severity,
            "assetName": asset_name,
            "assetType": asset_type,
            "issue": issue,
            "impact": impact,
            "recommendation": recommendation,
        }
    )
    unresolved.append(
        {
            "assetName": asset_name,
            "assetType": asset_type,
            "sourceLogic": source_logic,
            "reason": issue,
            "recommendedAction": recommendation,
        }
    )


def iter_assets(cir: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Collect top-level and mapping-contained assets for traceability."""
    assets: list[tuple[str, dict[str, Any]]] = []

    for key, asset_type in (
        ("connections", "Connection"),
        ("functions", "Function"),
        ("mapplets", "Mapplet"),
        ("mappingTasks", "MappingTask"),
        ("workflows", "Workflow"),
        ("mappings", "Mapping"),
    ):
        for obj in cir.get(key, []) or []:
            if isinstance(obj, dict):
                assets.append((asset_type, obj))

                if asset_type == "Mapping":
                    for child_key, child_type in (
                        ("sources", "Source"),
                        ("targets", "Target"),
                        ("transformations", "Transformation"),
                    ):
                        for child in obj.get(child_key, []) or []:
                            if isinstance(child, dict):
                                assets.append((child_type, child))

    return assets


def expression_entries(transformation: dict[str, Any]) -> list[dict[str, str]]:
    """Read derived-column expression metadata using common CIR aliases."""
    properties = transformation.get("properties") or {}
    candidates = (
        properties.get("expressions"),
        properties.get("derivedColumns"),
        properties.get("columns"),
        transformation.get("expressions"),
    )
    entries: list[dict[str, str]] = []

    for candidate in candidates:
        if not candidate:
            continue

        values = candidate if isinstance(candidate, list) else [candidate]
        for item in values:
            if isinstance(item, dict):
                target = (
                    item.get("targetColumn")
                    or item.get("name")
                    or item.get("column")
                    or item.get("fieldName")
                    or ""
                )
                expression = (
                    item.get("expression")
                    or item.get("logic")
                    or item.get("value")
                    or ""
                )
                if target and expression:
                    entries.append(
                        {
                            "targetColumn": str(target),
                            "expression": str(expression),
                        }
                    )

    return entries


def assess_assets(
    cir: dict[str, Any],
) -> tuple[
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    int,
    int,
    int,
]:
    risks: list[dict[str, str]] = []
    unresolved: list[dict[str, str]] = []
    traceability: list[dict[str, str]] = []
    converted = partial = not_converted = 0

    for asset_type, asset in iter_assets(cir):
        name = object_name(asset)
        asset_id = object_id(asset)
        status = "CONVERTED"
        module = "generated_pyspark.py"

        if asset_type == "Connection":
            status = "PARTIALLY_CONVERTED"
            add_risk(
                risks,
                unresolved,
                severity="MEDIUM",
                asset_name=name,
                asset_type=asset_type,
                issue="Connection metadata does not establish a secure runtime credential strategy.",
                impact="The generated job cannot connect until credentials and provider-specific options are configured.",
                recommendation="Use a secret manager or the platform's credential service; do not place secrets in CIR or source code.",
            )

        elif asset_type == "Source":
            if not asset.get("objectName") and not asset.get("name"):
                status = "NOT_CONVERTED"
                add_risk(
                    risks,
                    unresolved,
                    severity="HIGH",
                    asset_name=name,
                    asset_type=asset_type,
                    issue="Source object name is missing.",
                    impact="The source cannot be read reliably.",
                    recommendation="Provide the source path/table and format in CIR or runtime configuration.",
                )
            elif not asset.get("connectionId") and not asset.get("schema"):
                status = "PARTIALLY_CONVERTED"
                add_risk(
                    risks,
                    unresolved,
                    severity="MEDIUM",
                    asset_name=name,
                    asset_type=asset_type,
                    issue="Source connection and schema metadata are incomplete.",
                    impact="The generated reader requires runtime path/table configuration.",
                    recommendation="Supply the source format, path or table, schema, and connection details.",
                )

        elif asset_type == "Target":
            status = "PARTIALLY_CONVERTED"
            add_risk(
                risks,
                unresolved,
                severity="MEDIUM",
                asset_name=name,
                asset_type=asset_type,
                issue="Target output location, format, or write mode may be absent.",
                impact="The generated writer requires target configuration before production use.",
                recommendation="Provide the target format, path/table, write mode, and connection configuration.",
            )

        elif asset_type == "Transformation":
            kind = str(asset.get("type") or "")
            supported = {
                "expression",
                "filter",
                "joiner",
                "lookup",
                "router",
                "union",
                "sorter",
                "aggregator",
                "sequencegenerator",
                "rank",
                "updatestrategy",
                "normalizer",
                "hierarchyparser",
                "hierarchybuilder",
            }

            normalized_kind = re.sub(r"[^a-z0-9]", "", kind.lower())

            if normalized_kind not in supported:
                status = "NOT_CONVERTED"
                add_risk(
                    risks,
                    unresolved,
                    severity="HIGH",
                    asset_name=name,
                    asset_type=asset_type,
                    issue=f"Transformation type {kind!r} is not implemented by the generator.",
                    impact="This transformation will raise NotImplementedError at runtime.",
                    recommendation="Implement and validate an equivalent Spark operation before deployment.",
                    source_logic=str(asset.get("logic") or ""),
                )
            elif normalized_kind in {
                "hierarchyparser",
                "hierarchybuilder",
                "normalizer",
            }:
                status = "PARTIALLY_CONVERTED"
                add_risk(
                    risks,
                    unresolved,
                    severity="HIGH",
                    asset_name=name,
                    asset_type=asset_type,
                    issue=f"{kind} conversion depends on structure-specific metadata not guaranteed by CIR.",
                    impact="The generated operation may not preserve the original hierarchical semantics.",
                    recommendation="Validate schemas and semantics against IDMC output; implement explicit parsing/explode logic.",
                    source_logic=str(asset.get("logic") or ""),
                )
            elif normalized_kind in {"lookup", "joiner"}:
                status = "PARTIALLY_CONVERTED"
                condition = (asset.get("properties") or {}).get("condition")
                if not condition:
                    add_risk(
                        risks,
                        unresolved,
                        severity="HIGH",
                        asset_name=name,
                        asset_type=asset_type,
                        issue="Join or lookup condition is missing.",
                        impact="The join cannot be safely inferred.",
                        recommendation="Provide the join condition and join type; validate duplicate and null-key behavior.",
                    )
                else:
                    add_risk(
                        risks,
                        unresolved,
                        severity="MEDIUM",
                        asset_name=name,
                        asset_type=asset_type,
                        issue="IDMC lookup cache and connected/unconnected lookup behavior are not fully represented.",
                        impact="Performance, defaults, and no-match behavior may differ.",
                        recommendation="Validate lookup cache, multiple-match, no-match, and default-value semantics.",
                    )
            elif normalized_kind == "expression":
                entries = expression_entries(asset)
                if not entries and not asset.get("logic"):
                    status = "PARTIALLY_CONVERTED"
                    add_risk(
                        risks,
                        unresolved,
                        severity="HIGH",
                        asset_name=name,
                        asset_type=asset_type,
                        issue="Expression transformation has no derived-column definitions or logic.",
                        impact="No expression can be generated.",
                        recommendation="Include each target column and its exact Informatica expression in CIR.",
                    )
                elif asset.get("logic"):
                    status = "PARTIALLY_CONVERTED"
                    add_risk(
                        risks,
                        unresolved,
                        severity="MEDIUM",
                        asset_name=name,
                        asset_type=asset_type,
                        issue="Expression syntax will be translated using best-effort textual rules.",
                        impact="Informatica-specific syntax may not have identical Spark semantics.",
                        recommendation="Review generated expressions and test null, date, decimal, and string behavior.",
                        source_logic=str(asset.get("logic")),
                    )

        elif asset_type == "Function":
            status = "NOT_CONVERTED"
            add_risk(
                risks,
                unresolved,
                severity="HIGH",
                asset_name=name,
                asset_type=asset_type,
                issue="User-defined function requires language and runtime semantics not represented by generic CIR.",
                impact="Function calls cannot be guaranteed equivalent.",
                recommendation="Port the function to a Spark SQL/Python UDF and test it against Informatica results.",
                source_logic=str(asset.get("logic") or ""),
            )

        elif asset_type == "Mapplet":
            status = "PARTIALLY_CONVERTED"
            add_risk(
                risks,
                unresolved,
                severity="HIGH",
                asset_name=name,
                asset_type=asset_type,
                issue="Mapplet contents are not expanded into executable transformation steps.",
                impact="Reusable logic may be absent from the generated mapping.",
                recommendation="Expand mapplet transformations in CIR or implement a reusable Spark function.",
            )

        elif asset_type in {"MappingTask", "Workflow"}:
            status = "PARTIALLY_CONVERTED"
            add_risk(
                risks,
                unresolved,
                severity="MEDIUM",
                asset_name=name,
                asset_type=asset_type,
                issue="Scheduling, retries, runtime settings, and control-flow semantics require deployment-specific configuration.",
                impact="The generated script does not reproduce the full IDMC orchestration contract.",
                recommendation="Review dependencies and implement orchestration in the target platform.",
            )

        if status == "CONVERTED":
            converted += 1
        elif status == "PARTIALLY_CONVERTED":
            partial += 1
        else:
            not_converted += 1

        traceability.append(
            {
                "sourceAsset": asset_id or name,
                "assetType": asset_type,
                "canonicalObject": name,
                "generatedCodeModule": module,
                "conversionStatus": status,
            }
        )

    for mapping in cir.get("mappings", []) or []:
        if not isinstance(mapping, dict):
            continue
        for target in mapping.get("targets", []) or []:
            if not isinstance(target, dict):
                continue
            if not target.get("fields"):
                add_risk(
                    risks,
                    unresolved,
                    severity="HIGH",
                    asset_name=object_name(target),
                    asset_type="Target",
                    issue="Target field definitions are missing.",
                    impact="Column mapping and output schema cannot be validated.",
                    recommendation="Add target fields and explicit source-to-target column mappings to CIR.",
                )

    return risks, unresolved, traceability, converted, partial, not_converted


def generate_pyspark_script(cir: dict[str, Any]) -> str:
    """Create a generic PySpark runner with CIR embedded as data."""
    serialized = json.dumps(cir, ensure_ascii=False, indent=2)
    generated_at = datetime.now(timezone.utc).isoformat()
    first_mapping = (cir.get("mappings") or [{}])[0]
    mapping_name = str(first_mapping.get("name") or "IDMC_CIR_Migration")
    mapping_id = str(first_mapping.get("id") or "")

    header = f'''"""
Migration Generated Code

Source System: Informatica IDMC

Mapping Name: {mapping_name}
Original Asset Id: {mapping_id}
Generated Timestamp: {generated_at}
Migration Agent Version: {GENERATOR_VERSION}
"""

'''
    runtime = r'''
from __future__ import annotations

from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging
import json
from datetime import datetime
import time
import re


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
LOGGER = logging.getLogger("idmc_migration")

CIR: dict[str, Any] = json.loads(__CIR_JSON_LITERAL__)
MAPPING_NAME = __MAPPING_NAME__
MAPPING_ID = __MAPPING_ID__

# Supply environment-specific configuration here. Do not commit credentials.
RUNTIME_CONFIG: dict[str, Any] = {
    "sources": {},
    "targets": {},
    "connections": {},
}


def create_spark_session(mapping_name: str) -> SparkSession:
    return SparkSession.builder.appName(mapping_name).getOrCreate()


def read_source(spark: SparkSession, source: dict[str, Any]) -> DataFrame:
    """Read a source using its configured format and runtime options."""
    name = str(source.get("name") or source.get("objectName") or "source")
    cfg = RUNTIME_CONFIG["sources"].get(name, {})
    fmt = str(cfg.get("format") or source.get("format") or "parquet").lower()
    path = cfg.get("path") or source.get("path")
    table = cfg.get("table") or source.get("objectName")

    reader = spark.read
    options = cfg.get("options") or {}
    for key, value in options.items():
        reader = reader.option(str(key), str(value))

    if fmt == "jdbc":
        jdbc_options = dict(options)
        jdbc_options.setdefault("url", cfg.get("url", ""))
        jdbc_options.setdefault("dbtable", table or "")
        if cfg.get("user"):
            jdbc_options["user"] = cfg["user"]
        if cfg.get("password"):
            jdbc_options["password"] = cfg["password"]
        if not jdbc_options.get("url") or not jdbc_options.get("dbtable"):
            raise ValueError(f"JDBC source {name!r} needs url and dbtable configuration")
        return reader.format("jdbc").options(**jdbc_options).load()

    if not path:
        raise ValueError(
            f"Source {name!r} has no runtime path; configure "
            "RUNTIME_CONFIG['sources'][name]['path']"
        )

    if fmt not in {"csv", "parquet", "json", "delta", "avro"}:
        raise ValueError(f"Unsupported source format {fmt!r} for {name!r}")

    if fmt == "csv":
        reader = reader.option("header", cfg.get("header", "true"))
    return reader.format(fmt).load(path)


def write_target(df: DataFrame, target: dict[str, Any]) -> None:
    """Write a target using its configured format and runtime options."""
    name = str(target.get("name") or target.get("objectName") or "target")
    cfg = RUNTIME_CONFIG["targets"].get(name, {})
    fmt = str(cfg.get("format") or target.get("format") or "parquet").lower()
    mode = str(cfg.get("mode") or target.get("writeMode") or "errorifexists")
    path = cfg.get("path") or target.get("path")
    table = cfg.get("table") or target.get("objectName")
    options = cfg.get("options") or {}

    if fmt == "jdbc":
        jdbc_options = dict(options)
        jdbc_options.setdefault("url", cfg.get("url", ""))
        jdbc_options.setdefault("dbtable", table or "")
        if cfg.get("user"):
            jdbc_options["user"] = cfg["user"]
        if cfg.get("password"):
            jdbc_options["password"] = cfg["password"]
        if not jdbc_options.get("url") or not jdbc_options.get("dbtable"):
            raise ValueError(f"JDBC target {name!r} needs url and dbtable configuration")
        df.write.format("jdbc").mode(mode).options(**jdbc_options).save()
        return

    if fmt not in {"csv", "parquet", "delta"}:
        raise ValueError(f"Unsupported target format {fmt!r} for {name!r}")
    if not path:
        raise ValueError(
            f"Target {name!r} has no runtime path; configure "
            "RUNTIME_CONFIG['targets'][name]['path']"
        )

    writer = df.write.format(fmt).mode(mode)
    for key, value in options.items():
        writer = writer.option(str(key), str(value))
    if fmt == "csv":
        writer = writer.option("header", cfg.get("header", "true"))
    writer.save(path)


def translate_expression(expression: str) -> str:
    """Translate a subset of Informatica expressions to Spark SQL expression text."""
    result = expression.strip()
    replacements = [
        (r"\bNVL\s*\(", "COALESCE("),
        (r"\bSYSDATE\b", "CURRENT_TIMESTAMP()"),
        (r"\bCURRENT_DATE\b", "CURRENT_DATE()"),
        (r"\bISNULL\s*\(", "ISNULL("),
        (r"\bTO_DATE\s*\(", "TO_DATE("),
        (r"\bUPPER\s*\(", "UPPER("),
        (r"\bLOWER\s*\(", "LOWER("),
        (r"\bTRIM\s*\(", "TRIM("),
        (r"\bLTRIM\s*\(", "LTRIM("),
        (r"\bRTRIM\s*\(", "RTRIM("),
        (r"\bABS\s*\(", "ABS("),
        (r"\bROUND\s*\(", "ROUND("),
        (r"\bSUBSTR\s*\(", "SUBSTRING("),
        (r"\bREPLACE\s*\(", "REGEXP_REPLACE("),
    ]
    for pattern, replacement in replacements:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

    # Convert simple IIF(condition, true_value, false_value) calls. Nested commas
    # require a proper expression parser and are intentionally not guessed.
    match = re.fullmatch(r"\s*IIF\s*\((.*)\)\s*", result, flags=re.IGNORECASE)
    if match:
        args = split_top_level_args(match.group(1))
        if len(args) == 3:
            result = f"CASE WHEN {args[0]} THEN {args[1]} ELSE {args[2]} END"

    return result


def split_top_level_args(text: str) -> list[str]:
    parts: list[str] = []
    start = depth = 0
    quote: str | None = None

    for index, char in enumerate(text):
        if quote:
            if char == quote:
                quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(text[start:index].strip())
            start = index + 1

    parts.append(text[start:].strip())
    return parts


def get_expression_entries(transform: dict[str, Any]) -> list[dict[str, str]]:
    props = transform.get("properties") or {}
    candidates = [
        props.get("expressions"),
        props.get("derivedColumns"),
        props.get("columns"),
        transform.get("expressions"),
    ]
    result: list[dict[str, str]] = []

    for candidate in candidates:
        if not candidate:
            continue
        entries = candidate if isinstance(candidate, list) else [candidate]

        for item in entries:
            if isinstance(item, dict):
                target = (
                    item.get("targetColumn")
                    or item.get("name")
                    or item.get("column")
                    or item.get("fieldName")
                )
                expression = (
                    item.get("expression")
                    or item.get("logic")
                    or item.get("value")
                )
                if target and expression:
                    result.append(
                        {
                            "targetColumn": str(target),
                            "expression": str(expression),
                        }
                    )

    return result


def input_frame(
    transform: dict[str, Any],
    frames: dict[str, DataFrame],
) -> DataFrame:
    incoming = transform.get("incomingPorts") or []
    candidates: list[str] = []

    for item in incoming:
        if isinstance(item, str):
            candidates.append(item.split(".")[0])
        elif isinstance(item, dict):
            candidates.append(
                str(item.get("transformationName") or item.get("objectName") or "")
            )

    for candidate in candidates:
        if candidate in frames:
            return frames[candidate]

    if len(frames) == 1:
        return next(iter(frames.values()))

    name = str(transform.get("name") or "")
    if name in frames:
        return frames[name]

    raise ValueError(
        f"Cannot determine input DataFrame for transformation {name!r}; "
        "check CIR incomingPorts and links"
    )


def apply_transformation(
    transform: dict[str, Any],
    frames: dict[str, DataFrame],
) -> dict[str, DataFrame]:
    """Apply one transformation and return its named output frame(s)."""
    name = str(transform.get("name") or "transformation")
    kind = re.sub(
        r"[^a-z0-9]",
        "",
        str(transform.get("type") or "").lower(),
    )
    props = transform.get("properties") or {}
    logic = str(transform.get("logic") or "").strip()
    df = input_frame(transform, frames)

    if kind == "expression":
        entries = get_expression_entries(transform)
        if not entries and logic:
            # One-expression fallback only when a target column is explicit.
            target = props.get("targetColumn")
            if target:
                entries = [{"targetColumn": str(target), "expression": logic}]
        if not entries:
            raise ValueError(
                f"Expression {name!r} has no derived-column metadata"
            )

        for entry in entries:
            expr = translate_expression(entry["expression"])
            df = df.withColumn(
                entry["targetColumn"],
                F.expr(expr),
            )
        return {name: df}

    if kind == "filter":
        condition = logic or str(
            props.get("condition")
            or props.get("filterCondition")
            or ""
        )
        if not condition:
            raise ValueError(f"Filter {name!r} has no condition")
        return {
            name: df.filter(F.expr(translate_expression(condition)))
        }

    if kind in {"joiner", "lookup"}:
        condition = str(props.get("condition") or "")
        join_type = str(props.get("joinType") or "inner").lower()
        right_name = str(
            props.get("detail")
            or props.get("lookupObject")
            or props.get("master")
            or ""
        )
        right = frames.get(right_name)

        if right is None:
            raise ValueError(
                f"{kind} {name!r} requires right-side frame {right_name!r}; "
                "verify the mapping links and source names"
            )
        if not condition:
            raise ValueError(f"{kind} {name!r} has no join condition")

        if join_type in {"full", "full outer"}:
            join_type = "full_outer"
        elif join_type == "left":
            join_type = "left_outer"
        elif join_type == "right":
            join_type = "right_outer"
        elif join_type in {"leftsemi", "left_semi"}:
            join_type = "left_semi"
        elif join_type in {"leftanti", "left_anti"}:
            join_type = "left_anti"

        return {
            name: df.join(
                right,
                F.expr(translate_expression(condition)),
                join_type,
            )
        }

    if kind == "union":
        others = props.get("inputFrames") or []
        result = df
        for other_name in others:
            if other_name not in frames:
                raise ValueError(
                    f"Union {name!r} input {other_name!r} is missing"
                )
            result = result.unionByName(
                frames[other_name],
                allowMissingColumns=False,
            )
        return {name: result}

    if kind == "sorter":
        sort_fields = props.get("sortFields") or props.get("orderBy") or []
        if not sort_fields:
            raise ValueError(f"Sorter {name!r} has no sort fields")

        columns = []
        for field in sort_fields:
            if isinstance(field, str):
                columns.append(F.col(field))
            elif isinstance(field, dict):
                field_name = field.get("name") or field.get("fieldName")
                if field_name:
                    column = F.col(str(field_name))
                    direction = str(field.get("direction") or "asc").lower()
                    columns.append(
                        column.desc() if direction == "desc" else column.asc()
                    )

        if not columns:
            raise ValueError(f"Sorter {name!r} has invalid sort fields")
        return {name: df.orderBy(*columns)}

    if kind == "aggregator":
        group_by = props.get("groupBy") or []
        aggregates = props.get("aggregates") or []
        group_names = [
            item if isinstance(item, str)
            else item.get("name") or item.get("fieldName")
            for item in group_by
        ]

        expressions = []
        for item in aggregates:
            if not isinstance(item, dict):
                continue

            function = str(item.get("function") or "").upper()
            source = item.get("sourceField") or item.get("field")
            target = item.get("targetField") or item.get("name") or source
            if not source or not target:
                continue

            source_col = F.col(str(source))
            functions = {
                "SUM": F.sum,
                "COUNT": F.count,
                "AVG": F.avg,
                "MIN": F.min,
                "MAX": F.max,
            }

            if function == "COUNT_DISTINCT":
                expressions.append(
                    F.countDistinct(source_col).alias(str(target))
                )
            elif function in functions:
                expressions.append(
                    functions[function](source_col).alias(str(target))
                )
            else:
                raise NotImplementedError(
                    f"Aggregator function {function!r} is unsupported"
                )

        if not expressions:
            raise ValueError(
                f"Aggregator {name!r} has no valid aggregate definitions"
            )

        if group_names:
            return {
                name: df.groupBy(
                    *[F.col(str(col_name)) for col_name in group_names]
                ).agg(*expressions)
            }
        return {name: df.agg(*expressions)}

    if kind in {"sequencegenerator", "rank"}:
        order_fields = props.get("orderBy") or props.get("sortFields") or []
        partition_fields = props.get("partitionBy") or props.get("groupBy") or []
        target = str(
            props.get("targetField")
            or props.get("sequenceField")
            or f"{name}_value"
        )

        if not order_fields:
            raise ValueError(
                f"{name!r} needs orderBy metadata for deterministic row_number"
            )

        order_columns = [
            F.col(
                item if isinstance(item, str)
                else str(item.get("name") or item.get("fieldName"))
            )
            for item in order_fields
        ]
        partition_columns = [
            F.col(
                item if isinstance(item, str)
                else str(item.get("name") or item.get("fieldName"))
            )
            for item in partition_fields
        ]

        if partition_columns:
            window = Window.partitionBy(*partition_columns).orderBy(*order_columns)
        else:
            window = Window.orderBy(*order_columns)

        return {
            name: df.withColumn(
                target,
                F.row_number().over(window),
            )
        }

    if kind == "router":
        groups = props.get("groups") or []
        outputs: dict[str, DataFrame] = {}

        for group in groups:
            if not isinstance(group, dict):
                continue
            group_name = str(group.get("name") or f"{name}_group")
            condition = str(group.get("condition") or "")
            if not condition:
                raise ValueError(
                    f"Router group {group_name!r} has no condition"
                )
            outputs[group_name] = df.filter(
                F.expr(translate_expression(condition))
            )

        if not outputs:
            raise ValueError(f"Router {name!r} has no groups")
        return outputs

    if kind == "updatestrategy":
        strategy = props.get("strategy") or logic
        if not strategy:
            raise ValueError(
                f"Update Strategy {name!r} has no strategy logic"
            )
        raise NotImplementedError(
            f"Update Strategy {name!r} requires a target CDC implementation: "
            f"{strategy}"
        )

    if kind in {"normalizer", "hierarchyparser", "hierarchybuilder"}:
        raise NotImplementedError(
            f"{transform.get('type')} {name!r} needs structure-specific metadata"
        )

    raise NotImplementedError(
        f"Unsupported transformation {transform.get('type')!r} ({name!r})"
    )


def validate_schema(
    df: DataFrame,
    expected_fields: list[Any],
    asset_name: str,
) -> None:
    expected = []
    for field in expected_fields:
        if isinstance(field, str):
            expected.append(field)
        elif isinstance(field, dict):
            name = field.get("name") or field.get("fieldName")
            if name:
                expected.append(str(name))

    missing = sorted(set(expected) - set(df.columns))
    if missing:
        raise ValueError(
            f"{asset_name}: missing expected columns: {missing}"
        )


def quality_checks(
    df: DataFrame,
    asset_name: str,
) -> tuple[DataFrame, DataFrame, dict[str, int]]:
    """Run baseline null/duplicate checks and return accepted/rejected frames."""
    input_count = df.count()
    null_condition = None

    for field in df.columns:
        condition = F.col(field).isNull()
        null_condition = (
            condition
            if null_condition is None
            else (null_condition | condition)
        )

    rejected = (
        df.filter(null_condition)
        if null_condition is not None
        else df.limit(0)
    )
    accepted = (
        df.filter(~null_condition)
        if null_condition is not None
        else df
    )
    duplicate_count = input_count - accepted.dropDuplicates().count()
    rejected_count = rejected.count()

    LOGGER.info(
        "DQ asset=%s input=%d rejected_null=%d duplicate_rows=%d",
        asset_name,
        input_count,
        rejected_count,
        duplicate_count,
    )

    return accepted, rejected, {
        "inputCount": input_count,
        "rejectedCount": rejected_count,
        "duplicateCount": duplicate_count,
    }


def run_mapping(
    spark: SparkSession,
    mapping: dict[str, Any],
) -> dict[str, Any]:
    mapping_name = str(mapping.get("name") or "mapping")
    start = datetime.utcnow()
    started = time.monotonic()
    frames: dict[str, DataFrame] = {}
    audit: dict[str, Any] = {
        "mappingName": mapping_name,
        "processingStartTime": start.isoformat(),
        "inputCount": 0,
        "outputCount": 0,
        "rejectedCount": 0,
    }

    try:
        for source in mapping.get("sources") or []:
            source_name = str(
                source.get("name")
                or source.get("objectName")
                or "source"
            )
            if source_name not in frames:
                frames[source_name] = read_source(spark, source).cache()

        transformations = mapping.get("transformations") or []
        for transform in transformations:
            transform_name = str(
                transform.get("name") or "transformation"
            )
            LOGGER.info(
                "Applying transformation in CIR order: %s",
                transform_name,
            )
            outputs = apply_transformation(transform, frames)
            frames.update(outputs)

        targets = mapping.get("targets") or []

        if targets:
            for target in targets:
                target_name = str(
                    target.get("name")
                    or target.get("objectName")
                    or "target"
                )
                frame = frames.get(target_name)

                if frame is None:
                    # Best effort only when exactly one output frame is unambiguous.
                    if len(frames) == 1:
                        frame = next(iter(frames.values()))
                    else:
                        raise ValueError(
                            f"No DataFrame identified for target {target_name!r}; "
                            "check links and transformation output names"
                        )

                validate_schema(
                    frame,
                    target.get("fields") or [],
                    target_name,
                )
                accepted, rejected, counts = quality_checks(
                    frame,
                    target_name,
                )
                audit["inputCount"] += counts["inputCount"]
                audit["rejectedCount"] += counts["rejectedCount"]
                audit["outputCount"] += accepted.count()

                reject_cfg = (
                    RUNTIME_CONFIG["targets"]
                    .get(target_name, {})
                    .get("rejectedPath")
                )
                if reject_cfg:
                    rejected.write.mode("overwrite").parquet(reject_cfg)

                write_target(accepted, target)

        else:
            # With no target in the input, show the final transformed frame.
            # Prefer the last transformation output, otherwise use the sole
            # available frame when there is only one unambiguous result.
            final_frame = None

            if transformations:
                final_name = str(
                    transformations[-1].get("name") or ""
                )
                final_frame = frames.get(final_name)

            if final_frame is None and len(frames) == 1:
                final_frame = next(iter(frames.values()))

            if final_frame is not None:
                final_frame.show(20, truncate=False)
                output_count = final_frame.count()
                audit["inputCount"] = output_count
                audit["outputCount"] = output_count

        audit["processingEndTime"] = datetime.utcnow().isoformat()
        audit["durationSeconds"] = time.monotonic() - started
        return audit

    except Exception:
        LOGGER.exception("Mapping execution failed: %s", mapping_name)
        raise


def generate_pyspark_script(cir: dict[str, Any]) -> str:
    """Create a generic PySpark runner with CIR embedded as data."""
    serialized = json.dumps(cir, ensure_ascii=False, indent=2)
    generated_at = datetime.now(timezone.utc).isoformat()
    first_mapping = (cir.get("mappings") or [{}])[0]
    mapping_name = str(first_mapping.get("name") or "IDMC_CIR_Migration")
    mapping_id = str(first_mapping.get("id") or "")

    header = f'''"""
Migration Generated Code

Source System: Informatica IDMC

Mapping Name: {mapping_name}
Original Asset Id: {mapping_id}
Generated Timestamp: {generated_at}
Migration Agent Version: {GENERATOR_VERSION}
"""

'''
    runtime = r'''
from __future__ import annotations

from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging
import json
from datetime import datetime
import time
import re


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
LOGGER = logging.getLogger("idmc_migration")

CIR: dict[str, Any] = json.loads(__CIR_JSON_LITERAL__)
MAPPING_NAME = __MAPPING_NAME__
MAPPING_ID = __MAPPING_ID__

# Supply environment-specific configuration here. Do not commit credentials.
RUNTIME_CONFIG: dict[str, Any] = {
    "sources": {},
    "targets": {},
    "connections": {},
}


def create_spark_session(mapping_name: str) -> SparkSession:
    return SparkSession.builder.appName(mapping_name).getOrCreate()


def read_source(spark: SparkSession, source: dict[str, Any]) -> DataFrame:
    """Read a source using its configured format and runtime options."""
    name = str(source.get("name") or source.get("objectName") or "source")
    cfg = RUNTIME_CONFIG["sources"].get(name, {})
    fmt = str(cfg.get("format") or source.get("format") or "parquet").lower()
    path = cfg.get("path") or source.get("path")
    table = cfg.get("table") or source.get("objectName")

    reader = spark.read
    options = cfg.get("options") or {}
    for key, value in options.items():
        reader = reader.option(str(key), str(value))

    if fmt == "jdbc":
        jdbc_options = dict(options)
        jdbc_options.setdefault("url", cfg.get("url", ""))
        jdbc_options.setdefault("dbtable", table or "")
        if cfg.get("user"):
            jdbc_options["user"] = cfg["user"]
        if cfg.get("password"):
            jdbc_options["password"] = cfg["password"]
        if not jdbc_options.get("url") or not jdbc_options.get("dbtable"):
            raise ValueError(f"JDBC source {name!r} needs url and dbtable configuration")
        return reader.format("jdbc").options(**jdbc_options).load()

    if not path:
        raise ValueError(
            f"Source {name!r} has no runtime path; configure "
            "RUNTIME_CONFIG['sources'][name]['path']"
        )

    if fmt not in {"csv", "parquet", "json", "delta", "avro"}:
        raise ValueError(f"Unsupported source format {fmt!r} for {name!r}")

    if fmt == "csv":
        reader = reader.option("header", cfg.get("header", "true"))
    return reader.format(fmt).load(path)


def write_target(df: DataFrame, target: dict[str, Any]) -> None:
    """Write a target using its configured format and runtime options."""
    name = str(target.get("name") or target.get("objectName") or "target")
    cfg = RUNTIME_CONFIG["targets"].get(name, {})
    fmt = str(cfg.get("format") or target.get("format") or "parquet").lower()
    mode = str(cfg.get("mode") or target.get("writeMode") or "errorifexists")
    path = cfg.get("path") or target.get("path")
    table = cfg.get("table") or target.get("objectName")
    options = cfg.get("options") or {}

    if fmt == "jdbc":
        jdbc_options = dict(options)
        jdbc_options.setdefault("url", cfg.get("url", ""))
        jdbc_options.setdefault("dbtable", table or "")
        if cfg.get("user"):
            jdbc_options["user"] = cfg["user"]
        if cfg.get("password"):
            jdbc_options["password"] = cfg["password"]
        if not jdbc_options.get("url") or not jdbc_options.get("dbtable"):
            raise ValueError(f"JDBC target {name!r} needs url and dbtable configuration")
        df.write.format("jdbc").mode(mode).options(**jdbc_options).save()
        return

    if fmt not in {"csv", "parquet", "delta"}:
        raise ValueError(f"Unsupported target format {fmt!r} for {name!r}")
    if not path:
        raise ValueError(
            f"Target {name!r} has no runtime path; configure "
            "RUNTIME_CONFIG['targets'][name]['path']"
        )

    writer = df.write.format(fmt).mode(mode)
    for key, value in options.items():
        writer = writer.option(str(key), str(value))
    if fmt == "csv":
        writer = writer.option("header", cfg.get("header", "true"))
    writer.save(path)


def translate_expression(expression: str) -> str:
    """Translate a subset of Informatica expressions to Spark SQL expression text."""
    result = expression.strip()
    replacements = [
        (r"\bNVL\s*\(", "COALESCE("),
        (r"\bSYSDATE\b", "CURRENT_TIMESTAMP()"),
        (r"\bCURRENT_DATE\b", "CURRENT_DATE()"),
        (r"\bISNULL\s*\(", "ISNULL("),
        (r"\bTO_DATE\s*\(", "TO_DATE("),
        (r"\bUPPER\s*\(", "UPPER("),
        (r"\bLOWER\s*\(", "LOWER("),
        (r"\bTRIM\s*\(", "TRIM("),
        (r"\bLTRIM\s*\(", "LTRIM("),
        (r"\bRTRIM\s*\(", "RTRIM("),
        (r"\bABS\s*\(", "ABS("),
        (r"\bROUND\s*\(", "ROUND("),
        (r"\bSUBSTR\s*\(", "SUBSTRING("),
        (r"\bREPLACE\s*\(", "REGEXP_REPLACE("),
    ]
    for pattern, replacement in replacements:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

    # Convert simple IIF(condition, true_value, false_value) calls. Nested commas
    # require a proper expression parser and are intentionally not guessed.
    match = re.fullmatch(r"\s*IIF\s*\((.*)\)\s*", result, flags=re.IGNORECASE)
    if match:
        args = split_top_level_args(match.group(1))
        if len(args) == 3:
            result = f"CASE WHEN {args[0]} THEN {args[1]} ELSE {args[2]} END"

    return result


def split_top_level_args(text: str) -> list[str]:
    parts: list[str] = []
    start = depth = 0
    quote: str | None = None
    for index, char in enumerate(text):
        if quote:
            if char == quote:
                quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(text[start:index].strip())
            start = index + 1
    parts.append(text[start:].strip())
    return parts


def get_expression_entries(transform: dict[str, Any]) -> list[dict[str, str]]:
    props = transform.get("properties") or {}
    candidates = [
        props.get("expressions"),
        props.get("derivedColumns"),
        props.get("columns"),
        transform.get("expressions"),
    ]
    result: list[dict[str, str]] = []
    for candidate in candidates:
        if not candidate:
            continue
        entries = candidate if isinstance(candidate, list) else [candidate]
        for item in entries:
            if isinstance(item, dict):
                target = (
                    item.get("targetColumn")
                    or item.get("name")
                    or item.get("column")
                    or item.get("fieldName")
                )
                expression = item.get("expression") or item.get("logic") or item.get("value")
                if target and expression:
                    result.append(
                        {"targetColumn": str(target), "expression": str(expression)}
                    )
    return result


def input_frame(
    transform: dict[str, Any],
    frames: dict[str, DataFrame],
) -> DataFrame:
    incoming = transform.get("incomingPorts") or []
    candidates: list[str] = []
    for item in incoming:
        if isinstance(item, str):
            candidates.append(item.split(".")[0])
        elif isinstance(item, dict):
            candidates.append(
                str(item.get("transformationName") or item.get("objectName") or "")
            )
    for candidate in candidates:
        if candidate in frames:
            return frames[candidate]

    if len(frames) == 1:
        return next(iter(frames.values()))

    name = str(transform.get("name") or "")
    if name in frames:
        return frames[name]

    raise ValueError(
        f"Cannot determine input DataFrame for transformation {name!r}; "
        "check CIR incomingPorts and links"
    )


def apply_transformation(
    transform: dict[str, Any],
    frames: dict[str, DataFrame],
) -> dict[str, DataFrame]:
    """Apply one transformation and return its named output frame(s)."""
    name = str(transform.get("name") or "transformation")
    kind = re.sub(r"[^a-z0-9]", "", str(transform.get("type") or "").lower())
    props = transform.get("properties") or {}
    logic = str(transform.get("logic") or "").strip()
    df = input_frame(transform, frames)

    if kind == "expression":
        entries = get_expression_entries(transform)
        if not entries and logic:
            # One-expression fallback only when a target column is explicit.
            target = props.get("targetColumn")
            if target:
                entries = [{"targetColumn": str(target), "expression": logic}]
        if not entries:
            raise ValueError(f"Expression {name!r} has no derived-column metadata")
        for entry in entries:
            expr = translate_expression(entry["expression"])
            df = df.withColumn(entry["targetColumn"], F.expr(expr))
        return {name: df}

    if kind == "filter":
        condition = logic or str(props.get("condition") or props.get("filterCondition") or "")
        if not condition:
            raise ValueError(f"Filter {name!r} has no condition")
        return {name: df.filter(F.expr(translate_expression(condition)))}

    if kind in {"joiner", "lookup"}:
        condition = str(props.get("condition") or "")
        join_type = str(props.get("joinType") or "inner").lower()
        right_name = str(
            props.get("detail")
            or props.get("lookupObject")
            or props.get("master")
            or ""
        )
        right = frames.get(right_name)
        if right is None:
            raise ValueError(
                f"{kind} {name!r} requires right-side frame {right_name!r}; "
                "verify the mapping links and source names"
            )
        if not condition:
            raise ValueError(f"{kind} {name!r} has no join condition")
        if join_type in {"full", "full outer"}:
            join_type = "full_outer"
        elif join_type in {"left"}:
            join_type = "left_outer"
        elif join_type in {"right"}:
            join_type = "right_outer"
        elif join_type in {"leftsemi", "left_semi"}:
            join_type = "left_semi"
        elif join_type in {"leftanti", "left_anti"}:
            join_type = "left_anti"
        return {
            name: df.join(
                right,
                F.expr(translate_expression(condition)),
                join_type,
            )
        }

    if kind == "union":
        others = props.get("inputFrames") or []
        result = df
        for other_name in others:
            if other_name not in frames:
                raise ValueError(f"Union {name!r} input {other_name!r} is missing")
            result = result.unionByName(frames[other_name], allowMissingColumns=False)
        return {name: result}

    if kind == "sorter":
        sort_fields = props.get("sortFields") or props.get("orderBy") or []
        if not sort_fields:
            raise ValueError(f"Sorter {name!r} has no sort fields")
        columns = []
        for field in sort_fields:
            if isinstance(field, str):
                columns.append(F.col(field))
            elif isinstance(field, dict):
                field_name = field.get("name") or field.get("fieldName")
                if field_name:
                    column = F.col(str(field_name))
                    direction = str(field.get("direction") or "asc").lower()
                    columns.append(column.desc() if direction == "desc" else column.asc())
        if not columns:
            raise ValueError(f"Sorter {name!r} has invalid sort fields")
        return {name: df.orderBy(*columns)}

    if kind == "aggregator":
        group_by = props.get("groupBy") or []
        aggregates = props.get("aggregates") or []
        group_names = [
            item if isinstance(item, str)
            else item.get("name") or item.get("fieldName")
            for item in group_by
        ]
        expressions = []
        for item in aggregates:
            if not isinstance(item, dict):
                continue
            function = str(item.get("function") or "").upper()
            source = item.get("sourceField") or item.get("field")
            target = item.get("targetField") or item.get("name") or source
            if not source or not target:
                continue
            source_col = F.col(str(source))
            functions = {
                "SUM": F.sum,
                "COUNT": F.count,
                "AVG": F.avg,
                "MIN": F.min,
                "MAX": F.max,
            }
            if function == "COUNT_DISTINCT":
                expressions.append(F.countDistinct(source_col).alias(str(target)))
            elif function in functions:
                expressions.append(functions[function](source_col).alias(str(target)))
            else:
                raise NotImplementedError(
                    f"Aggregator function {function!r} is unsupported"
                )
        if not expressions:
            raise ValueError(f"Aggregator {name!r} has no valid aggregate definitions")
        if group_names:
            return {name: df.groupBy(*[F.col(str(col_name)) for col_name in group_names]).agg(*expressions)}
        return {name: df.agg(*expressions)}

    if kind in {"sequencegenerator", "rank"}:
        order_fields = props.get("orderBy") or props.get("sortFields") or []
        partition_fields = props.get("partitionBy") or props.get("groupBy") or []
        target = str(props.get("targetField") or props.get("sequenceField") or f"{name}_value")
        if not order_fields:
            raise ValueError(f"{name!r} needs orderBy metadata for deterministic row_number")
        order_columns = [
            F.col(item if isinstance(item, str) else str(item.get("name") or item.get("fieldName")))
            for item in order_fields
        ]
        partition_columns = [
            F.col(item if isinstance(item, str) else str(item.get("name") or item.get("fieldName")))
            for item in partition_fields
        ]
        window = Window.partitionBy(*partition_columns).orderBy(*order_columns) if partition_columns else Window.orderBy(*order_columns)
        return {name: df.withColumn(target, F.row_number().over(window))}

    if kind == "router":
        groups = props.get("groups") or []
        outputs: dict[str, DataFrame] = {}
        for group in groups:
            if not isinstance(group, dict):
                continue
            group_name = str(group.get("name") or f"{name}_group")
            condition = str(group.get("condition") or "")
            if not condition:
                raise ValueError(f"Router group {group_name!r} has no condition")
            outputs[group_name] = df.filter(F.expr(translate_expression(condition)))
        if not outputs:
            raise ValueError(f"Router {name!r} has no groups")
        return outputs

    if kind == "updatestrategy":
        strategy = props.get("strategy") or logic
        if not strategy:
            raise ValueError(f"Update Strategy {name!r} has no strategy logic")
        # Strategy codes need an explicit target-side CDC contract.
        raise NotImplementedError(
            f"Update Strategy {name!r} requires a target CDC implementation: {strategy}"
        )

    if kind in {"normalizer", "hierarchyparser", "hierarchybuilder"}:
        raise NotImplementedError(
            f"{transform.get('type')} {name!r} needs structure-specific metadata"
        )

    raise NotImplementedError(
        f"Unsupported transformation {transform.get('type')!r} ({name!r})"
    )


def validate_schema(df: DataFrame, expected_fields: list[Any], asset_name: str) -> None:
    expected = []
    for field in expected_fields:
        if isinstance(field, str):
            expected.append(field)
        elif isinstance(field, dict):
            name = field.get("name") or field.get("fieldName")
            if name:
                expected.append(str(name))
    missing = sorted(set(expected) - set(df.columns))
    if missing:
        raise ValueError(f"{asset_name}: missing expected columns: {missing}")


def quality_checks(df: DataFrame, asset_name: str) -> tuple[DataFrame, DataFrame, dict[str, int]]:
    """Run baseline null/duplicate checks and return accepted/rejected frames."""
    input_count = df.count()
    null_condition = None
    for field in df.columns:
        condition = F.col(field).isNull()
        null_condition = condition if null_condition is None else (null_condition | condition)

    rejected = df.filter(null_condition) if null_condition is not None else df.limit(0)
    accepted = df.filter(~null_condition) if null_condition is not None else df
    duplicate_count = input_count - accepted.dropDuplicates().count()
    rejected_count = rejected.count()

    LOGGER.info(
        "DQ asset=%s input=%d rejected_null=%d duplicate_rows=%d",
        asset_name,
        input_count,
        rejected_count,
        duplicate_count,
    )
    return accepted, rejected, {
        "inputCount": input_count,
        "rejectedCount": rejected_count,
        "duplicateCount": duplicate_count,
    }


def run_mapping(spark: SparkSession, mapping: dict[str, Any]) -> dict[str, Any]:
    mapping_name = str(mapping.get("name") or "mapping")
    start = datetime.utcnow()
    started = time.monotonic()
    frames: dict[str, DataFrame] = {}
    audit: dict[str, Any] = {
        "mappingName": mapping_name,
        "processingStartTime": start.isoformat(),
        "inputCount": 0,
        "outputCount": 0,
        "rejectedCount": 0,
    }

    try:
        for source in mapping.get("sources") or []:
            source_name = str(source.get("name") or source.get("objectName") or "source")
            if source_name not in frames:
                frames[source_name] = read_source(spark, source).cache()

        transformations = mapping.get("transformations") or []
        for transform in transformations:
            transform_name = str(transform.get("name") or "transformation")
            LOGGER.info("Applying transformation in CIR order: %s", transform_name)
            outputs = apply_transformation(transform, frames)
            frames.update(outputs)

        targets = mapping.get("targets") or []

        if targets:
            for target in targets:
                target_name = str(
                    target.get("name") or target.get("objectName") or "target"
                )
                frame = frames.get(target_name)

                if frame is None:
                    # Best effort only when exactly one output frame is unambiguous.
                    if len(frames) == 1:
                        frame = next(iter(frames.values()))
                    else:
                        raise ValueError(
                            f"No DataFrame identified for target {target_name!r}; "
                            "check links and transformation output names"
                        )

                validate_schema(frame, target.get("fields") or [], target_name)
                accepted, rejected, counts = quality_checks(frame, target_name)
                audit["inputCount"] += counts["inputCount"]
                audit["rejectedCount"] += counts["rejectedCount"]
                audit["outputCount"] += accepted.count()

                reject_cfg = (
                    RUNTIME_CONFIG["targets"]
                    .get(target_name, {})
                    .get("rejectedPath")
                )
                if reject_cfg:
                    rejected.write.mode("overwrite").parquet(reject_cfg)

                write_target(accepted, target)

        else:
            # No target is present in the input. Preview the final transformed
            # frame so the generated job still demonstrates its result.
            final_frame = None

            if transformations:
                final_name = str(transformations[-1].get("name") or "")
                final_frame = frames.get(final_name)

            if final_frame is None and len(frames) == 1:
                final_frame = next(iter(frames.values()))

            if final_frame is not None:
                final_frame.show(20, truncate=False)
                output_count = final_frame.count()
                audit["inputCount"] = output_count
                audit["outputCount"] = output_count

        audit["processingEndTime"] = datetime.utcnow().isoformat()
        audit["durationSeconds"] = time.monotonic() - started
        return audit

    except Exception:
        LOGGER.exception("Mapping execution failed: %s", mapping_name)
        raise


def main() -> None:
    spark = create_spark_session(MAPPING_NAME)
    try:
        mappings = CIR.get("mappings") or []
        if not mappings:
            raise ValueError("CIR contains no mappings")
        for mapping in mappings:
            audit = run_mapping(spark, mapping)
            LOGGER.info("AUDIT %s", json.dumps(audit, default=str))
    except Exception:
        LOGGER.exception("Migration job failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
'''
    runtime = runtime.replace(
        "__CIR_JSON_LITERAL__",
        repr(serialized),
    )
    runtime = runtime.replace("__MAPPING_NAME__", repr(mapping_name))
    runtime = runtime.replace("__MAPPING_ID__", repr(mapping_id))
    return header + runtime.lstrip()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate PySpark code and migration reports from CIR JSON."
    )
    parser.add_argument(
        "cir_json",
        type=Path,
        help="Path to canonical CIR JSON",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("migration_output"),
        help="Directory to write generated artifacts",
    )
    args = parser.parse_args()

    try:
        cir = json.loads(
            args.cir_json.read_text(encoding="utf-8-sig")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Could not read CIR JSON: {exc}") from exc

    if not isinstance(cir, dict):
        raise SystemExit("CIR root must be a JSON object")

    # Preserve the original report and runtime generator while adapting the
    # Streamlit app's canonical raw_idmc_export shape into CIR mappings.
    cir = normalize_canonical_input(cir)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    risks, unresolved, traceability, converted, partial, not_converted = assess_assets(cir)
    mappings = cir.get("mappings") or []
    first_mapping = mappings[0] if mappings else {}

    total_objects = converted + partial + not_converted
    completeness = (
        round((converted / total_objects) * 100, 2)
        if total_objects
        else 0.0
    )

    generated_at = datetime.now(timezone.utc).isoformat()
    metrics = cir.get("metrics") or {}

    migration_report = {
        "mappingName": str(first_mapping.get("name") or ""),
        "assetId": str(first_mapping.get("id") or ""),
        "conversionStatus": (
            "CONVERTED"
            if risks == []
            else "PARTIALLY_CONVERTED"
            if converted + partial > 0
            else "NOT_CONVERTED"
        ),
        "generatedTimestamp": generated_at,
        "complexity": metrics.get("estimatedComplexity", "Unknown"),
        "totalObjects": total_objects,
        "convertedObjects": converted,
        "warnings": sum(
            1
            for risk in risks
            if risk["severity"] in {"LOW", "MEDIUM"}
        ),
        "risks": len(risks),
        "manualRemediationCount": len(unresolved),
    }

    completeness_report = {
        "conversionCompletenessPercentage": completeness,
        "assetsConverted": converted,
        "assetsPartiallyConverted": partial,
        "assetsNotConverted": not_converted,
    }

    artifacts: dict[str, Any] = {
        "migration_report.json": migration_report,
        "traceability_report.json": traceability,
        "risk_register.json": risks,
        "unresolved_logic_registry.json": unresolved,
        "migration_completeness_score.json": completeness_report,
    }

    for filename, content in artifacts.items():
        (args.output_dir / filename).write_text(
            json.dumps(content, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    (args.output_dir / "generated_pyspark.py").write_text(
        generate_pyspark_script(cir),
        encoding="utf-8",
    )

    # Save the normalized structure as well, making it easier to inspect the
    # mapping that was passed to the generator.
    (args.output_dir / "normalized_cir.json").write_text(
        json.dumps(cir, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print(f"Generated artifacts in: {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()