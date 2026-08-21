#!/usr/bin/env python3
"""Inventory public datapipe-core "pipe" surfaces and check docs coverage.

Discovers symbols via AST (no optional-deps imports), maps them to mdBook pages,
and reports gaps against docs/source/SUMMARY.md.

Usage (from repo root or docs/):
  python docs/scripts/inventory_core.py
  python docs/scripts/inventory_core.py --write
  python docs/scripts/inventory_core.py --strict   # exit 1 if any symbol lacks a docs page
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]

DOCS_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = DOCS_ROOT.parent
CORE_PKG = REPO_ROOT / "libs" / "datapipe-core" / "datapipe"
SUMMARY = DOCS_ROOT / "source" / "SUMMARY.md"
DEFAULT_OUT = DOCS_ROOT / "inventory-core.yaml"
DEFAULT_MAP = DOCS_ROOT / "inventory-map.yaml"

# Base class names that mark a public pipe surface when subclassed in core.
PIPELINE_STEP_BASES = frozenset({"PipelineStep"})
TABLE_STORE_BASES = frozenset({"TableStore", "TableDataSingleFileStore"})
EXECUTOR_BASES = frozenset({"Executor"})

# Explicit public symbols in fixed modules (not discovered via subclassing).
EXPLICIT_SYMBOLS: dict[str, list[tuple[str, str]]] = {
    # module_relative_path: [(class_or_func_name, kind), ...]
    "compute.py": [
        ("Table", "catalog"),
        ("Catalog", "catalog"),
        ("Pipeline", "pipeline"),
        ("PipelineStep", "pipeline"),
        ("ComputeStep", "pipeline"),
        ("DatapipeApp", "pipeline"),
        ("build_compute", "pipeline"),
        ("run_pipeline", "pipeline"),
        ("run_steps", "pipeline"),
        ("run_changelist", "pipeline"),
        ("run_steps_changelist", "pipeline"),
    ],
    "datatable.py": [
        ("DataTable", "datatable"),
        ("DataStore", "datatable"),
    ],
    "types.py": [
        ("Required", "types"),
        ("InputSpec", "types"),
        ("OutputSpec", "types"),
        ("ChangeList", "types"),
    ],
    "executor/__init__.py": [
        ("Executor", "executor"),
        ("ExecutorConfig", "executor"),
        ("SingleThreadExecutor", "executor"),
    ],
    "store/table_store.py": [
        ("TableStore", "store"),
        ("TableStoreCaps", "store"),
    ],
}

# Skip private / internal helper classes even if they subclass a base.
SKIP_NAMES = frozenset(
    {
        "BaseBatchTransformStep",
        "DatatableBatchTransformStep",
        "BatchTransformStep",
        "DatatableTransformStep",
        "TableDataSingleFileStore",
        "ItemStoreFileAdapter",
        "JSONFile",
        "BytesFile",
        "PILFile",
        "PandasParquetFile",
        "Replacer",
        "MetaKey",
        "DBConn",
        "CollectionParams",
        "ElasticStoreState",
        "_NodePK",
        "_EdgePK",
    }
)

WIP_RE = re.compile(
    r">\s*\*\*(?:Work in progress|Needs review)\.\*\*",
    re.IGNORECASE,
)
SUMMARY_LINK_RE = re.compile(r"\[([^\]]+)\]\((\./[^)]+\.md)\)")


@dataclass
class Symbol:
    name: str
    kind: str
    module: str
    qualname: str
    bases: list[str] = field(default_factory=list)
    line: int | None = None


@dataclass
class PageStatus:
    path: str  # relative to docs/source/
    in_summary: bool
    exists: bool
    stub: bool  # WIP / Needs review marker
    title: str | None = None


def _rel_module(path: Path) -> str:
    return path.relative_to(CORE_PKG).as_posix()


def _bases_of(node: ast.ClassDef) -> list[str]:
    out: list[str] = []
    for base in node.bases:
        if isinstance(base, ast.Name):
            out.append(base.id)
        elif isinstance(base, ast.Attribute):
            out.append(base.attr)
    return out


def _walk_py_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in sorted(root.rglob("*.py")):
        parts = set(path.parts)
        if "tests" in parts or "migrations" in parts:
            continue
        if path.name.startswith("test_"):
            continue
        files.append(path)
    return files


def discover_subclasses(pkg: Path) -> list[Symbol]:
    symbols: list[Symbol] = []
    for path in _walk_py_files(pkg):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        mod = _rel_module(path)
        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            if node.name.startswith("_") or node.name in SKIP_NAMES:
                continue
            bases = _bases_of(node)
            kind: str | None = None
            if PIPELINE_STEP_BASES & set(bases):
                kind = "pipeline_step"
            elif TABLE_STORE_BASES & set(bases):
                kind = "store"
            elif EXECUTOR_BASES & set(bases):
                kind = "executor"
            if kind is None:
                continue
            symbols.append(
                Symbol(
                    name=node.name,
                    kind=kind,
                    module=mod,
                    qualname=f"datapipe.{mod.replace('/', '.').removesuffix('.py')}.{node.name}".replace(
                        ".__init__.", "."
                    ),
                    bases=bases,
                    line=node.lineno,
                )
            )
    return symbols


def discover_explicit(pkg: Path) -> list[Symbol]:
    symbols: list[Symbol] = []
    for rel, items in EXPLICIT_SYMBOLS.items():
        path = pkg / rel
        if not path.exists():
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        defined = {
            n.name: n.lineno
            for n in tree.body
            if isinstance(n, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        }
        mod = rel
        for name, kind in items:
            if name not in defined:
                continue
            dotted = f"datapipe.{rel.replace('/', '.').removesuffix('.py')}.{name}".replace(
                ".__init__.", "."
            )
            symbols.append(
                Symbol(
                    name=name,
                    kind=kind,
                    module=mod,
                    qualname=dotted,
                    line=defined[name],
                )
            )
    return symbols


def discover_cli_commands(pkg: Path) -> list[Symbol]:
    """Collect click command function names from cli.py (public CLI surface)."""
    path = pkg / "cli.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    symbols: list[Symbol] = []

    def has_click_decorator(node: ast.FunctionDef) -> bool:
        for dec in node.decorator_list:
            # @cli.command / @table.command / @click.group / @step.command …
            if isinstance(dec, ast.Call):
                dec = dec.func  # type: ignore[assignment]
            if isinstance(dec, ast.Attribute) and dec.attr in {"command", "group"}:
                return True
            if isinstance(dec, ast.Name) and dec.id in {"command", "group"}:
                return True
        return False

    for node in tree.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        if node.name.startswith("_") or node.name == "main":
            continue
        if not has_click_decorator(node):
            continue
        symbols.append(
            Symbol(
                name=node.name,
                kind="cli",
                module="cli.py",
                qualname=f"datapipe.cli.{node.name}",
                line=node.lineno,
            )
        )
    return symbols


def load_map(path: Path) -> dict[str, str]:
    """Map keys → docs page relative to docs/source/.

    Keys may be bare symbol names (`BatchTransform`) or disambiguated
    `kind:name` (`cli:run_changelist`) when the same name appears twice.
    """
    if not path.exists():
        return {}
    if yaml is None:
        raise SystemExit("PyYAML is required to read inventory-map.yaml (pip install PyYAML)")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    mapping = data.get("symbols") or data
    return {str(k): str(v) for k, v in mapping.items()}


def resolve_docs_page(symbol: Symbol, mapping: dict[str, str]) -> str | None:
    return mapping.get(f"{symbol.kind}:{symbol.name}") or mapping.get(symbol.name)


def parse_summary(path: Path) -> dict[str, str]:
    """path_rel -> title from SUMMARY.md."""
    text = path.read_text(encoding="utf-8")
    return {href[2:]: title for title, href in SUMMARY_LINK_RE.findall(text)}


def page_status(source_root: Path, rel: str, summary_pages: dict[str, str]) -> PageStatus:
    path = source_root / rel
    exists = path.is_file()
    stub = False
    if exists:
        head = path.read_text(encoding="utf-8")[:2000]
        stub = bool(WIP_RE.search(head))
    return PageStatus(
        path=rel,
        in_summary=rel in summary_pages,
        exists=exists,
        stub=stub,
        title=summary_pages.get(rel),
    )


def dedupe(symbols: list[Symbol]) -> list[Symbol]:
    seen: set[str] = set()
    out: list[Symbol] = []
    for s in sorted(symbols, key=lambda x: (x.kind, x.name, x.module)):
        key = f"{s.kind}:{s.name}"
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
    return out


def build_inventory(
    symbols: list[Symbol],
    mapping: dict[str, str],
    summary_pages: dict[str, str],
    source_root: Path,
) -> dict:
    rows = []
    missing_page: list[str] = []
    stub_pages: list[str] = []
    not_in_summary: list[str] = []

    for s in symbols:
        doc = resolve_docs_page(s, mapping)
        status = page_status(source_root, doc, summary_pages) if doc else None
        row = {
            **asdict(s),
            "docs_page": doc,
            "docs_in_summary": status.in_summary if status else False,
            "docs_exists": status.exists if status else False,
            "docs_stub": status.stub if status else None,
        }
        rows.append(row)
        if not doc or not (status and status.exists):
            missing_page.append(s.name)
        elif status and not status.in_summary:
            not_in_summary.append(s.name)
        if status and status.exists and status.stub:
            stub_pages.append(s.name)

    return {
        "package": "datapipe-core",
        "core_path": str(CORE_PKG.relative_to(REPO_ROOT)),
        "symbol_count": len(rows),
        "coverage": {
            "with_page": len(rows) - len(missing_page),
            "missing_page": missing_page,
            "stub_or_needs_review": sorted(set(stub_pages)),
            "page_not_in_summary": not_in_summary,
        },
        "symbols": rows,
    }


def dump_yaml(data: dict, path: Path | None) -> str:
    if yaml is None:
        raise SystemExit("PyYAML is required (datapipe-core depends on it)")
    text = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
    if path:
        path.write_text(text, encoding="utf-8")
    return text


def print_report(inv: dict) -> None:
    cov = inv["coverage"]
    total = inv["symbol_count"]
    ok = cov["with_page"]
    print(f"datapipe-core inventory: {ok}/{total} symbols have a docs page")
    if cov["missing_page"]:
        print("\nMissing docs page:")
        for name in cov["missing_page"]:
            print(f"  - {name}")
    if cov["page_not_in_summary"]:
        print("\nPage exists but not in SUMMARY.md:")
        for name in cov["page_not_in_summary"]:
            print(f"  - {name}")
    stubs = cov["stub_or_needs_review"]
    if stubs:
        print(f"\nStub / needs-review ({len(stubs)} symbols):")
        for name in stubs:
            print(f"  - {name}")
    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write",
        action="store_true",
        help=f"Write inventory YAML to {DEFAULT_OUT.relative_to(REPO_ROOT)}",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help="Output path for --write",
    )
    parser.add_argument(
        "--map",
        type=Path,
        default=DEFAULT_MAP,
        help="Symbol → docs page map YAML",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit 1 if any inventoried symbol lacks an existing docs page",
    )
    parser.add_argument(
        "--fail-on-stubs",
        action="store_true",
        help="With --strict, also fail if mapped pages are still WIP / Needs review",
    )
    args = parser.parse_args(argv)

    if not CORE_PKG.is_dir():
        print(f"error: core package not found: {CORE_PKG}", file=sys.stderr)
        return 2

    mapping = load_map(args.map)
    summary_pages = parse_summary(SUMMARY) if SUMMARY.exists() else {}

    symbols = dedupe(
        discover_subclasses(CORE_PKG)
        + discover_explicit(CORE_PKG)
        + discover_cli_commands(CORE_PKG)
    )
    inv = build_inventory(symbols, mapping, summary_pages, DOCS_ROOT / "source")

    print_report(inv)

    if args.write:
        dump_yaml(inv, args.out)
        print(f"Wrote {args.out.relative_to(REPO_ROOT)}")

    if args.strict:
        failed = bool(inv["coverage"]["missing_page"])
        if args.fail_on_stubs and inv["coverage"]["stub_or_needs_review"]:
            failed = True
        return 1 if failed else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
