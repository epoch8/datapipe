#!/usr/bin/env python3
"""Render four incremental-processing explainers as animated GIFs.

Output: docs/source/assets/incremental/{01-insert,02-update,03-delete,04-unchanged}.gif

Uses Pillow only (no imageio). Regenerate with: make -C docs gifs
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

OUT_DIR = Path(__file__).resolve().parents[1] / "source" / "assets" / "incremental"
W, H = 880, 520
BG = (250, 250, 252)
INK = (28, 28, 35)
MUTED = (110, 110, 125)
ACCENT = (37, 99, 235)  # blue — scheduled / active
OK = (22, 163, 74)  # green — success / written
WARN = (217, 119, 6)  # amber — changed
DANGER = (220, 38, 38)  # red — deleted
SKIP = (148, 163, 184)  # slate — skipped
CARD = (255, 255, 255)
BORDER = (226, 232, 240)
HOLD = 18  # frames to hold each scene (~0.6s at 30fps-ish; we use duration ms)


def font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for path in candidates:
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


F_TITLE = font(22, bold=True)
F_LABEL = font(14, bold=True)
F_BODY = font(13)
F_SMALL = font(11)


def rounded_rect(draw: ImageDraw.ImageDraw, xy, fill, outline=BORDER, width=2, radius=12):
    draw.rounded_rectangle(xy, radius=radius, fill=fill, outline=outline, width=width)


def draw_table_card(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    w: int,
    h: int,
    title: str,
    rows: list[tuple[str, str]],
    highlight: str | None = None,
    badge: str | None = None,
    badge_color=ACCENT,
):
    outline = badge_color if highlight == "active" else (DANGER if highlight == "deleted" else BORDER)
    rounded_rect(draw, (x, y, x + w, y + h), CARD, outline=outline, width=3 if highlight else 2)
    draw.text((x + 14, y + 10), title, font=F_LABEL, fill=INK)
    if badge:
        bw = 8 + len(badge) * 7
        bx = x + w - bw - 12
        by = y + 8
        rounded_rect(draw, (bx, by, bx + bw, by + 22), badge_color, outline=badge_color, radius=8)
        draw.text((bx + 6, by + 3), badge, font=F_SMALL, fill=(255, 255, 255))
    yy = y + 40
    draw.line((x + 12, yy - 6, x + w - 12, yy - 6), fill=BORDER, width=1)
    for left, right in rows:
        draw.text((x + 14, yy), left, font=F_BODY, fill=MUTED)
        draw.text((x + w // 2, yy), right, font=F_BODY, fill=INK)
        yy += 22


def draw_arrow(draw: ImageDraw.ImageDraw, x1: int, y1: int, x2: int, y2: int, color=ACCENT, label: str = ""):
    draw.line((x1, y1, x2, y2), fill=color, width=3)
    # arrow head
    if x2 >= x1:
        draw.polygon([(x2, y2), (x2 - 10, y2 - 6), (x2 - 10, y2 + 6)], fill=color)
    if label:
        mx, my = (x1 + x2) // 2, (y1 + y2) // 2 - 16
        draw.text((mx - len(label) * 3, my), label, font=F_SMALL, fill=color)


def base_frame(title: str, subtitle: str) -> tuple[Image.Image, ImageDraw.ImageDraw]:
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    draw.text((28, 18), title, font=F_TITLE, fill=INK)
    draw.text((28, 48), subtitle, font=F_BODY, fill=MUTED)
    return img, draw


def scene_layout(
    title: str,
    subtitle: str,
    a_rows: list[tuple[str, str]],
    meta_rows: list[tuple[str, str]],
    step_rows: list[tuple[str, str]],
    b_rows: list[tuple[str, str]],
    *,
    a_badge=None,
    a_badge_color=ACCENT,
    a_hl=None,
    meta_badge=None,
    meta_badge_color=ACCENT,
    meta_hl=None,
    step_badge=None,
    step_badge_color=ACCENT,
    step_hl=None,
    b_badge=None,
    b_badge_color=OK,
    b_hl=None,
    arrow_label: str = "",
    arrow_color=ACCENT,
    show_arrow: bool = True,
    footer: str = "",
) -> Image.Image:
    img, draw = base_frame(title, subtitle)
    # Four cards: A | A_meta | step_meta | B
    card_w, card_h = 190, 160
    y0 = 100
    gap = 18
    xs = [28]
    for _ in range(3):
        xs.append(xs[-1] + card_w + gap)

    draw_table_card(draw, xs[0], y0, card_w, card_h, "Table A", a_rows, a_hl, a_badge, a_badge_color)
    draw_table_card(draw, xs[1], y0, card_w, card_h, "A_meta", meta_rows, meta_hl, meta_badge, meta_badge_color)
    draw_table_card(draw, xs[2], y0, card_w, card_h, "step_meta", step_rows, step_hl, step_badge, step_badge_color)
    draw_table_card(draw, xs[3], y0, card_w, card_h, "Table B", b_rows, b_hl, b_badge, b_badge_color)

    if show_arrow:
        # A -> B arc under cards
        draw_arrow(draw, xs[0] + card_w // 2, y0 + card_h + 30, xs[3] + card_w // 2, y0 + card_h + 30, arrow_color, arrow_label)
        # small markers under mid cards
        draw.ellipse((xs[1] + card_w // 2 - 4, y0 + card_h + 26, xs[1] + card_w // 2 + 4, y0 + card_h + 34), fill=arrow_color)
        draw.ellipse((xs[2] + card_w // 2 - 4, y0 + card_h + 26, xs[2] + card_w // 2 + 4, y0 + card_h + 34), fill=arrow_color)

    if footer:
        rounded_rect(draw, (28, H - 70, W - 28, H - 24), (241, 245, 249), outline=BORDER, radius=10)
        draw.text((44, H - 56), footer, font=F_BODY, fill=INK)
    return img


def empty_row() -> list[tuple[str, str]]:
    return [("id", "—"), ("value", "—")]


def save_gif(frames: list[Image.Image], path: Path, duration_ms: int = 900) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frames[0].save(
        path,
        save_all=True,
        append_images=frames[1:],
        duration=duration_ms,
        loop=0,
        optimize=False,
    )
    print(f"wrote {path} ({len(frames)} frames)")


def gif_insert() -> None:
    frames = [
        scene_layout(
            "1 · Insert",
            "A new row appears in the input table",
            empty_row(),
            [("id", "—"), ("hash", "—"), ("update_ts", "—"), ("delete_ts", "NULL")],
            [("id", "—"), ("process_ts", "—"), ("is_success", "—")],
            empty_row(),
            show_arrow=False,
            footer="Idle pipeline — nothing scheduled yet",
        ),
        scene_layout(
            "1 · Insert",
            "store_chunk writes data + meta (update_ts set)",
            [("id", "1"), ("value", "hello")],
            [("id", "1"), ("hash", "H1"), ("update_ts", "t1 ↑"), ("delete_ts", "NULL")],
            [("id", "—"), ("process_ts", "—"), ("is_success", "—")],
            empty_row(),
            a_badge="NEW",
            a_badge_color=OK,
            a_hl="active",
            meta_badge="NEW",
            meta_badge_color=OK,
            meta_hl="active",
            show_arrow=False,
            footer="A_meta.update_ts = now  →  key becomes dirty",
        ),
        scene_layout(
            "1 · Insert",
            "Scheduler: no step_meta row → process_ts IS NULL",
            [("id", "1"), ("value", "hello")],
            [("id", "1"), ("hash", "H1"), ("update_ts", "t1"), ("delete_ts", "NULL")],
            [("id", "1"), ("process_ts", "NULL"), ("is_success", "—")],
            empty_row(),
            step_badge="RUN",
            step_badge_color=ACCENT,
            step_hl="active",
            arrow_label="BatchTransform scheduled",
            footer="Selected for processing",
        ),
        scene_layout(
            "1 · Insert",
            "User func runs → B written → step marked success",
            [("id", "1"), ("value", "hello")],
            [("id", "1"), ("hash", "H1"), ("update_ts", "t1"), ("delete_ts", "NULL")],
            [("id", "1"), ("process_ts", "t2"), ("is_success", "true")],
            [("id", "1"), ("value", "HELLO")],
            step_badge="OK",
            step_badge_color=OK,
            b_badge="WRITE",
            b_badge_color=OK,
            b_hl="active",
            arrow_label="func(A) → B",
            arrow_color=OK,
            footer="Only the new key ran. Unrelated keys stay skipped.",
        ),
    ]
    save_gif(frames, OUT_DIR / "01-insert.gif")


def gif_update() -> None:
    frames = [
        scene_layout(
            "2 · Update",
            "Row already processed — pipeline is idle",
            [("id", "1"), ("value", "hello")],
            [("id", "1"), ("hash", "H1"), ("update_ts", "t1"), ("delete_ts", "NULL")],
            [("id", "1"), ("process_ts", "t2"), ("is_success", "true")],
            [("id", "1"), ("value", "HELLO")],
            show_arrow=False,
            footer="update_ts ≤ process_ts  →  nothing to do",
        ),
        scene_layout(
            "2 · Update",
            "Content changes → hash differs → update_ts bumps",
            [("id", "1"), ("value", "hola")],
            [("id", "1"), ("hash", "H2"), ("update_ts", "t3 ↑"), ("delete_ts", "NULL")],
            [("id", "1"), ("process_ts", "t2"), ("is_success", "true")],
            [("id", "1"), ("value", "HELLO")],
            a_badge="CHG",
            a_badge_color=WARN,
            a_hl="active",
            meta_badge="hash≠",
            meta_badge_color=WARN,
            meta_hl="active",
            show_arrow=False,
            footer="Same primary key, new CityHash → dirty again",
        ),
        scene_layout(
            "2 · Update",
            "Scheduler: update_ts > process_ts",
            [("id", "1"), ("value", "hola")],
            [("id", "1"), ("hash", "H2"), ("update_ts", "t3"), ("delete_ts", "NULL")],
            [("id", "1"), ("process_ts", "t2"), ("is_success", "true")],
            [("id", "1"), ("value", "HELLO")],
            step_badge="RUN",
            step_badge_color=ACCENT,
            step_hl="active",
            arrow_label="re-process key 1",
            footer="Only changed keys are selected",
        ),
        scene_layout(
            "2 · Update",
            "func re-runs → B updated → new process_ts",
            [("id", "1"), ("value", "hola")],
            [("id", "1"), ("hash", "H2"), ("update_ts", "t3"), ("delete_ts", "NULL")],
            [("id", "1"), ("process_ts", "t4"), ("is_success", "true")],
            [("id", "1"), ("value", "HOLA")],
            b_badge="UPD",
            b_badge_color=WARN,
            b_hl="active",
            step_badge="OK",
            step_badge_color=OK,
            arrow_label="func(A) → B",
            arrow_color=OK,
            footer="Downstream catches up; unrelated rows stay untouched",
        ),
    ]
    save_gif(frames, OUT_DIR / "02-update.gif")


def gif_delete() -> None:
    frames = [
        scene_layout(
            "3 · Delete",
            "Row exists in A and B",
            [("id", "1"), ("value", "hola")],
            [("id", "1"), ("hash", "H2"), ("update_ts", "t3"), ("delete_ts", "NULL")],
            [("id", "1"), ("process_ts", "t4"), ("is_success", "true")],
            [("id", "1"), ("value", "HOLA")],
            show_arrow=False,
            footer="Delete via delete_by_idx or store_chunk(..., processed_idx=...)",
        ),
        scene_layout(
            "3 · Delete",
            "Hard delete data + soft-delete meta (delete_ts set, update_ts bumps)",
            [("id", "—"), ("value", "—")],
            [("id", "1"), ("hash", "0"), ("update_ts", "t5 ↑"), ("delete_ts", "t5")],
            [("id", "1"), ("process_ts", "t4"), ("is_success", "true")],
            [("id", "1"), ("value", "HOLA")],
            a_badge="GONE",
            a_badge_color=DANGER,
            a_hl="deleted",
            meta_badge="soft",
            meta_badge_color=DANGER,
            meta_hl="deleted",
            show_arrow=False,
            footer="Data store: hard delete. Meta: soft delete_ts (still drives scheduling)",
        ),
        scene_layout(
            "3 · Delete",
            "Scheduler runs key — get_data(A) is empty",
            [("id", "—"), ("value", "—")],
            [("id", "1"), ("hash", "0"), ("update_ts", "t5"), ("delete_ts", "t5")],
            [("id", "1"), ("process_ts", "t4"), ("is_success", "true")],
            [("id", "1"), ("value", "HOLA")],
            step_badge="RUN",
            step_badge_color=ACCENT,
            step_hl="active",
            arrow_label="empty input → cleanup",
            footer="Typical BatchTransform returns None (no idx param) → delete B",
        ),
        scene_layout(
            "3 · Delete",
            "B cleaned for that idx; step marked success",
            [("id", "—"), ("value", "—")],
            [("id", "1"), ("hash", "0"), ("update_ts", "t5"), ("delete_ts", "t5")],
            [("id", "1"), ("process_ts", "t6"), ("is_success", "true")],
            [("id", "—"), ("value", "—")],
            b_badge="DEL",
            b_badge_color=DANGER,
            b_hl="deleted",
            step_badge="OK",
            step_badge_color=OK,
            arrow_label="B.delete_by_idx",
            arrow_color=DANGER,
            footer="Deletes propagate downstream without reprocessing the world",
        ),
    ]
    save_gif(frames, OUT_DIR / "03-delete.gif")


def gif_unchanged() -> None:
    frames = [
        scene_layout(
            "4 · Unchanged (same hash)",
            "Row already processed",
            [("id", "1"), ("value", "hola")],
            [("id", "1"), ("hash", "H2"), ("update_ts", "t3"), ("delete_ts", "NULL")],
            [("id", "1"), ("process_ts", "t6"), ("is_success", "true")],
            [("id", "1"), ("value", "HOLA")],
            show_arrow=False,
            footer="Contrast case: rewrite identical content",
        ),
        scene_layout(
            "4 · Unchanged (same hash)",
            "store_chunk again with the same values",
            [("id", "1"), ("value", "hola")],
            [("id", "1"), ("hash", "H2"), ("update_ts", "t3"), ("delete_ts", "NULL")],
            [("id", "1"), ("process_ts", "t6"), ("is_success", "true")],
            [("id", "1"), ("value", "HOLA")],
            a_badge="same",
            a_badge_color=SKIP,
            show_arrow=False,
            footer="Hash matches existing meta → not a content change",
        ),
        scene_layout(
            "4 · Unchanged (same hash)",
            "Data not rewritten; process_ts on A_meta may bump — update_ts does NOT",
            [("id", "1"), ("value", "hola")],
            [("id", "1"), ("hash", "H2"), ("update_ts", "t3"), ("process_ts", "t7*")],
            [("id", "1"), ("process_ts", "t6"), ("is_success", "true")],
            [("id", "1"), ("value", "HOLA")],
            meta_badge="proc only",
            meta_badge_color=SKIP,
            meta_hl="active",
            show_arrow=False,
            footer="* A_meta.process_ts can move; scheduling ignores it",
        ),
        scene_layout(
            "4 · Unchanged (same hash)",
            "Step NOT scheduled — func never runs — B untouched",
            [("id", "1"), ("value", "hola")],
            [("id", "1"), ("hash", "H2"), ("update_ts", "t3"), ("delete_ts", "NULL")],
            [("id", "1"), ("process_ts", "t6"), ("is_success", "true")],
            [("id", "1"), ("value", "HOLA")],
            step_badge="SKIP",
            step_badge_color=SKIP,
            step_hl="active",
            b_badge="idle",
            b_badge_color=SKIP,
            show_arrow=True,
            arrow_label="no work",
            arrow_color=SKIP,
            footer="This is the incremental win: identical rewrites are free",
        ),
    ]
    save_gif(frames, OUT_DIR / "04-unchanged.gif")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    gif_insert()
    gif_update()
    gif_delete()
    gif_unchanged()
    print(f"done → {OUT_DIR}")


if __name__ == "__main__":
    main()
