from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path


def _requirement_name(requirement: str) -> str:
    match = re.match(r"\s*([A-Za-z0-9_.-]+)", requirement)
    if match is None:
        raise ValueError(f"Cannot parse requirement name from {requirement!r}")
    return match.group(1).replace("_", "-").lower()


def _get_source(sources: dict[str, object], name: str) -> object | None:
    normalized = _requirement_name(name)
    for key, value in sources.items():
        if _requirement_name(str(key)) == normalized:
            return value
    return None


def _source_requirement(requirement: str, sources: dict[str, object]) -> str:
    name = _requirement_name(requirement)
    source = _get_source(sources, name)
    if not isinstance(source, dict):
        return requirement
    if source.get("workspace") is True:
        return ""

    git = source.get("git")
    rev = source.get("rev")
    tag = source.get("tag")
    branch = source.get("branch")
    if not isinstance(git, str):
        return requirement

    suffix = ""
    if isinstance(rev, str):
        suffix = f"@{rev}"
    elif isinstance(tag, str):
        suffix = f"@{tag}"
    elif isinstance(branch, str):
        suffix = f"@{branch}"
    return f"{name} @ git+{git}{suffix}"


def _iter_requirements(pyproject: dict[str, object], extras: list[str]) -> list[str]:
    project = pyproject["project"]
    assert isinstance(project, dict)
    dependencies = list(project.get("dependencies", []))

    optional_dependencies = project.get("optional-dependencies", {})
    assert isinstance(optional_dependencies, dict)
    for extra in extras:
        extra_dependencies = optional_dependencies.get(extra, [])
        dependencies.extend(extra_dependencies)

    tool = pyproject.get("tool", {})
    assert isinstance(tool, dict)
    uv = tool.get("uv", {})
    assert isinstance(uv, dict)
    sources = uv.get("sources", {})
    assert isinstance(sources, dict)

    return [_source_requirement(str(dependency), sources) for dependency in dependencies]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pyproject", type=Path, required=True)
    parser.add_argument("--extra", action="append", default=[])
    parser.add_argument("--exclude", action="append", default=[])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    pyproject = tomllib.loads(args.pyproject.read_text())
    excluded = {_requirement_name(requirement) for requirement in args.exclude}

    requirements = []
    for requirement in _iter_requirements(pyproject, args.extra):
        if not requirement:
            continue
        name = _requirement_name(requirement)
        if name in excluded:
            continue
        requirements.append(requirement)

    output = "\n".join(requirements) + "\n"
    if args.output is None:
        sys.stdout.write(output)
    else:
        args.output.write_text(output)


if __name__ == "__main__":
    main()
