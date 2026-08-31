#!/usr/bin/env python3
"""Render static before / during / after table panels for incremental docs.

Each case is one wide PNG with labeled panels. Highlighted rows = indexes
being processed (amber). Untouched rows stay neutral. Deleted rows are red
ghosts. No GIFs — readers can study each state.

Output under docs/source/assets/incremental/:
  01-insert.png … 04-unchanged.png
  05-processed-idx.png
  06-resurrection.png

Usage: make -C docs panels
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

OUT = Path(__file__).resolve().parents[1] / "source" / "assets" / "incremental"

BG = (250, 250, 252)
INK = (28, 28, 35)
MUTED = (110, 110, 125)
BORDER = (210, 214, 220)
HEADER = (241, 245, 249)
HL = (255, 237, 213)          # amber — active / dirty index
HL_BORDER = (217, 119, 6)
NEW = (204, 251, 241)         # teal — newly written
NEW_BORDER = (13, 148, 136)
GONE = (254, 226, 226)        # red — deleted
GONE_BORDER = (220, 38, 38)
SKIP = (241, 245, 249)        # slate — skipped / unchanged
WHITE = (255, 255, 255)


def font(size: int, bold: bool = False):
    paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for p in paths:
        if Path(p).exists():
            return ImageFont.truetype(p, size)
    return ImageFont.load_default()


F_TITLE = font(20, True)
F_PANEL = font(14, True)
F_TABLE = font(12, True)
F_CELL = font(12)
F_CAP = font(12)
F_LEGEND = font(11)


@dataclass
class Row:
    cells: list[str]
    kind: str = "normal"  # normal | active | new | gone | skip


@dataclass
class TableView:
    title: str
    headers: list[str]
    rows: list[Row] = field(default_factory=list)
    note: str = ""


@dataclass
class Panel:
    label: str  # Before / During · … / After
    caption: str
    tables: list[TableView]


def _text_w(draw, text, fnt) -> int:
    b = draw.textbbox((0, 0), text, font=fnt)
    return b[2] - b[0]


def measure_table(tv: TableView, col_w: int = 72) -> tuple[int, int]:
    cols = max(len(tv.headers), max((len(r.cells) for r in tv.rows), default=0))
    w = 16 + cols * col_w
    h = 28 + 26 + 24 * max(len(tv.rows), 1) + (18 if tv.note else 0)
    return w, h


def draw_table(draw: ImageDraw.ImageDraw, x: int, y: int, tv: TableView, col_w: int = 72) -> int:
    """Draw table; return bottom y."""
    cols = len(tv.headers)
    tw = 16 + cols * col_w
    # title
    draw.text((x, y), tv.title, font=F_TABLE, fill=INK)
    y += 20
    # header
    hh = 24
    draw.rounded_rectangle((x, y, x + tw, y + hh), radius=6, fill=HEADER, outline=BORDER)
    for i, h in enumerate(tv.headers):
        draw.text((x + 8 + i * col_w, y + 5), h, font=F_CELL, fill=MUTED)
    y += hh
    # rows
    for row in tv.rows:
        fill, outline = WHITE, BORDER
        if row.kind == "active":
            fill, outline = HL, HL_BORDER
        elif row.kind == "new":
            fill, outline = NEW, NEW_BORDER
        elif row.kind == "gone":
            fill, outline = GONE, GONE_BORDER
        elif row.kind == "skip":
            fill, outline = SKIP, BORDER
        draw.rectangle((x, y, x + tw, y + 24), fill=fill, outline=outline)
        for i, c in enumerate(row.cells):
            color = GONE_BORDER if row.kind == "gone" else INK
            draw.text((x + 8 + i * col_w, y + 5), c, font=F_CELL, fill=color)
        y += 24
    if not tv.rows:
        draw.rectangle((x, y, x + tw, y + 24), fill=WHITE, outline=BORDER)
        draw.text((x + 8, y + 5), "(empty)", font=F_CELL, fill=MUTED)
        y += 24
    if tv.note:
        draw.text((x, y + 4), tv.note, font=F_CAP, fill=MUTED)
        y += 18
    return y


def render_case(title: str, panels: list[Panel], path: Path, legend: bool = True) -> None:
    """One wide image: panels left→right."""
    pad = 24
    gap = 20
    panel_w = 300
    # estimate height
    max_h = 0
    for p in panels:
        h = 70
        for tv in p.tables:
            _, th = measure_table(tv)
            h += th + 12
        h += 40
        max_h = max(max_h, h)
    legend_h = 36 if legend else 0
    W = pad * 2 + len(panels) * panel_w + (len(panels) - 1) * gap
    H = pad + 40 + max_h + legend_h + pad

    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    draw.text((pad, pad), title, font=F_TITLE, fill=INK)

    x = pad
    y0 = pad + 36
    for p in panels:
        # panel chrome
        draw.rounded_rectangle((x, y0, x + panel_w, y0 + max_h), radius=12, fill=WHITE, outline=BORDER, width=2)
        draw.text((x + 14, y0 + 12), p.label, font=F_PANEL, fill=HL_BORDER if p.label.startswith("During") else INK)
        # wrap caption
        cy = y0 + 34
        words = p.caption.split()
        line = ""
        for w_ in words:
            test = f"{line} {w_}".strip()
            if _text_w(draw, test, F_CAP) > panel_w - 28:
                draw.text((x + 14, cy), line, font=F_CAP, fill=MUTED)
                cy += 16
                line = w_
            else:
                line = test
        if line:
            draw.text((x + 14, cy), line, font=F_CAP, fill=MUTED)
            cy += 20

        ty = cy + 6
        for tv in p.tables:
            ty = draw_table(draw, x + 14, ty, tv) + 14

        x += panel_w + gap

    if legend:
        ly = H - pad - 24
        items = [
            (HL, HL_BORDER, "active index"),
            (NEW, NEW_BORDER, "written / updated"),
            (GONE, GONE_BORDER, "deleted"),
            (SKIP, BORDER, "untouched"),
        ]
        lx = pad
        for fill, outline, label in items:
            draw.rounded_rectangle((lx, ly, lx + 16, ly + 16), radius=3, fill=fill, outline=outline)
            draw.text((lx + 22, ly + 1), label, font=F_LEGEND, fill=MUTED)
            lx += 22 + _text_w(draw, label, F_LEGEND) + 18

    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path, "PNG")
    print(f"wrote {path.name} ({W}x{H})")


# ── cases ──────────────────────────────────────────────────────────────


def case_insert() -> None:
    render_case(
        "1 · Insert — new key appears",
        [
            Panel(
                "Before",
                "A and B are empty. Nothing scheduled.",
                [
                    TableView("Table A", ["id", "value"], []),
                    TableView("step_meta", ["id", "ok?"], []),
                    TableView("Table B", ["id", "value"], []),
                ],
            ),
            Panel(
                "During · schedule + run",
                "New row id=1 is active. Step has no success yet → func runs.",
                [
                    TableView("Table A", ["id", "value"], [Row(["1", "hello"], "active")]),
                    TableView("step_meta", ["id", "ok?"], [Row(["1", "—"], "active")], note="dirty: never processed"),
                    TableView("Table B", ["id", "value"], []),
                ],
            ),
            Panel(
                "After",
                "B written for id=1. Step marked success. Other keys untouched.",
                [
                    TableView("Table A", ["id", "value"], [Row(["1", "hello"], "skip")]),
                    TableView("step_meta", ["id", "ok?"], [Row(["1", "yes"], "new")]),
                    TableView("Table B", ["id", "value"], [Row(["1", "HELLO"], "new")]),
                ],
            ),
        ],
        OUT / "01-insert.png",
    )


def case_update() -> None:
    render_case(
        "2 · Update — same key, new content",
        [
            Panel(
                "Before",
                "id=1 already processed. Pipeline idle.",
                [
                    TableView("Table A", ["id", "value"], [Row(["1", "hello"], "skip"), Row(["2", "foo"], "skip")]),
                    TableView("step_meta", ["id", "ok?"], [Row(["1", "yes"], "skip"), Row(["2", "yes"], "skip")]),
                    TableView("Table B", ["id", "value"], [Row(["1", "HELLO"], "skip"), Row(["2", "FOO"], "skip")]),
                ],
            ),
            Panel(
                "During · only id=1",
                "Content of id=1 changed → only that index is active. id=2 stays idle.",
                [
                    TableView("Table A", ["id", "value"], [Row(["1", "world"], "active"), Row(["2", "foo"], "skip")]),
                    TableView("step_meta", ["id", "ok?"], [Row(["1", "yes"], "active"), Row(["2", "yes"], "skip")], note="id=1 dirty again"),
                    TableView("Table B", ["id", "value"], [Row(["1", "HELLO"], "skip"), Row(["2", "FOO"], "skip")]),
                ],
            ),
            Panel(
                "After",
                "Only B[id=1] updated. id=2 never entered the batch.",
                [
                    TableView("Table A", ["id", "value"], [Row(["1", "world"], "skip"), Row(["2", "foo"], "skip")]),
                    TableView("step_meta", ["id", "ok?"], [Row(["1", "yes"], "new"), Row(["2", "yes"], "skip")]),
                    TableView("Table B", ["id", "value"], [Row(["1", "WORLD"], "new"), Row(["2", "FOO"], "skip")]),
                ],
            ),
        ],
        OUT / "02-update.png",
    )


def case_delete() -> None:
    render_case(
        "3 · Delete — key removed from A",
        [
            Panel(
                "Before",
                "id=1 lives in A and B.",
                [
                    TableView("Table A", ["id", "value"], [Row(["1", "world"], "skip")]),
                    TableView("step_meta", ["id", "ok?"], [Row(["1", "yes"], "skip")]),
                    TableView("Table B", ["id", "value"], [Row(["1", "WORLD"], "skip")]),
                ],
            ),
            Panel(
                "During · cleanup",
                "A lost id=1 (active). Func sees empty input → cleanup B for that idx.",
                [
                    TableView("Table A", ["id", "value"], [Row(["1", "∅"], "gone")], note="hard-deleted in store"),
                    TableView("step_meta", ["id", "ok?"], [Row(["1", "yes"], "active")], note="still scheduled"),
                    TableView("Table B", ["id", "value"], [Row(["1", "WORLD"], "active")]),
                ],
            ),
            Panel(
                "After",
                "B cleaned for id=1. Delete propagated.",
                [
                    TableView("Table A", ["id", "value"], []),
                    TableView("step_meta", ["id", "ok?"], [Row(["1", "yes"], "new")]),
                    TableView("Table B", ["id", "value"], [Row(["1", "∅"], "gone")]),
                ],
            ),
        ],
        OUT / "03-delete.png",
    )


def case_unchanged() -> None:
    render_case(
        "4 · Unchanged — same content rewritten",
        [
            Panel(
                "Before",
                "id=1 already in sync.",
                [
                    TableView("Table A", ["id", "value"], [Row(["1", "world"], "skip")]),
                    TableView("step_meta", ["id", "ok?"], [Row(["1", "yes"], "skip")]),
                    TableView("Table B", ["id", "value"], [Row(["1", "WORLD"], "skip")]),
                ],
            ),
            Panel(
                "During · no schedule",
                "Same values written again. No index is active — func is not called.",
                [
                    TableView("Table A", ["id", "value"], [Row(["1", "world"], "skip")], note="fingerprint match"),
                    TableView("step_meta", ["id", "ok?"], [Row(["1", "yes"], "skip")], note="not dirty"),
                    TableView("Table B", ["id", "value"], [Row(["1", "WORLD"], "skip")]),
                ],
            ),
            Panel(
                "After",
                "Identical rewrite is free. B untouched.",
                [
                    TableView("Table A", ["id", "value"], [Row(["1", "world"], "skip")]),
                    TableView("step_meta", ["id", "ok?"], [Row(["1", "yes"], "skip")]),
                    TableView("Table B", ["id", "value"], [Row(["1", "WORLD"], "skip")]),
                ],
            ),
        ],
        OUT / "04-unchanged.png",
    )


def case_processed_idx() -> None:
    render_case(
        "5 · processed_idx — partial output cleans missing children",
        [
            Panel(
                "Before",
                "Parent id=1 has children a,b,c in B.",
                [
                    TableView("A (parents)", ["id"], [Row(["1"], "skip")]),
                    TableView("B (children)", ["id", "child"], [
                        Row(["1", "a"], "skip"),
                        Row(["1", "b"], "skip"),
                        Row(["1", "c"], "skip"),
                    ]),
                ],
            ),
            Panel(
                "During · batch idx={1}",
                "Func returns only a,b for parent 1. child c is in processed_idx but missing → deleted.",
                [
                    TableView("A", ["id"], [Row(["1"], "active")]),
                    TableView("B result", ["id", "child"], [
                        Row(["1", "a"], "new"),
                        Row(["1", "b"], "new"),
                        Row(["1", "c"], "gone"),
                    ], note="c omitted from return"),
                ],
            ),
            Panel(
                "After",
                "Only a,b remain. Omitting rows is a delete.",
                [
                    TableView("A", ["id"], [Row(["1"], "skip")]),
                    TableView("B", ["id", "child"], [
                        Row(["1", "a"], "skip"),
                        Row(["1", "b"], "skip"),
                    ]),
                ],
            ),
        ],
        OUT / "05-processed-idx.png",
    )


def case_resurrection() -> None:
    render_case(
        "6 · Resurrection — soft-deleted key comes back",
        [
            Panel(
                "Before",
                "id=1 soft-deleted in meta; gone from A data.",
                [
                    TableView("A data", ["id", "value"], []),
                    TableView("A_meta", ["id", "deleted?"], [Row(["1", "yes"], "gone")]),
                    TableView("B", ["id", "value"], []),
                ],
            ),
            Panel(
                "During · re-insert",
                "Same id=1 written again → active. Downstream schedules.",
                [
                    TableView("A data", ["id", "value"], [Row(["1", "hello"], "active")]),
                    TableView("A_meta", ["id", "deleted?"], [Row(["1", "no"], "active")], note="delete_ts cleared"),
                    TableView("B", ["id", "value"], []),
                ],
            ),
            Panel(
                "After",
                "Transform ran again; B restored for id=1.",
                [
                    TableView("A data", ["id", "value"], [Row(["1", "hello"], "skip")]),
                    TableView("A_meta", ["id", "deleted?"], [Row(["1", "no"], "new")]),
                    TableView("B", ["id", "value"], [Row(["1", "HELLO"], "new")]),
                ],
            ),
        ],
        OUT / "06-resurrection.png",
    )


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    case_insert()
    case_update()
    case_delete()
    case_unchanged()
    case_processed_idx()
    case_resurrection()
    # remove old gifs so docs don't accidentally keep broken motion
    for g in OUT.glob("*.gif"):
        g.unlink()
        print(f"removed {g.name}")
    print(f"done → {OUT}")


if __name__ == "__main__":
    main()
