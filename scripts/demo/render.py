"""Render the two-minute Attesta explainer from captured, real output.

Every command and result line in the terminal comes from capture.json,
written by scripts/demo/capture.sh from a run on a kind cluster (the Demo
capture workflow uploads it). Layout, timing, `#` comment lines and the
trimming of long lines are editorial; nothing in a result line is invented.

    python3 scripts/demo/render.py   # scripts/demo/capture.json -> docs/assets

Requires Pillow, ffmpeg, and the Inter and DejaVu Sans Mono fonts
(Debian/Ubuntu: fonts-inter fonts-dejavu-core).
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

HERE = Path(__file__).resolve().parent
_parser = argparse.ArgumentParser(description="Render the Attesta explainer video, README GIF and poster.")
_parser.add_argument("--capture", type=Path, default=HERE / "capture.json")
_parser.add_argument("--out-dir", type=Path, default=HERE.parents[1] / "docs" / "assets")
_parser.add_argument("--stills", type=Path, help="also save the last frame of every scene here")
ARGS = _parser.parse_args()
CAP = json.loads(ARGS.capture.read_text())
STEPS = CAP["steps"]
TERMINAL_LABEL = f"kind · Kubernetes {CAP['kubernetes']} · attesta@{CAP['commit']}"

W, H, FPS = 1920, 1080, 30
INTER = os.environ.get("ATTESTA_FONT_INTER", "/usr/share/fonts/opentype/inter/Inter-{}.otf")
MONO = os.environ.get("ATTESTA_FONT_MONO", "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf")
MONO_B = os.environ.get("ATTESTA_FONT_MONO_BOLD", "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf")
_cache: dict[tuple[str, int], ImageFont.FreeTypeFont] = {}


def F(path: str, size: int) -> ImageFont.FreeTypeFont:
    key = (path, size)
    if key not in _cache:
        _cache[key] = ImageFont.truetype(path, size)
    return _cache[key]


def inter(weight: str, size: int) -> ImageFont.FreeTypeFont:
    return F(INTER.format(weight), size)


BG = (9, 12, 17)
PANEL = (16, 21, 29)
CARD = (20, 26, 36)
BORDER = (38, 47, 60)
WHITE = (236, 240, 245)
MUTED = (140, 151, 166)
DIM = (92, 101, 115)
PROMPT = (126, 231, 135)
CYAN = (110, 200, 255)
ACCENT = (255, 196, 87)
RED = (255, 107, 107)
GREEN_BG, RED_BG, AMBER_BG, PURPLE_BG = (24, 92, 55), (140, 32, 44), (150, 98, 18), (98, 52, 160)
BLUE = (88, 166, 255)

MONO_SIZE = 23
LINE_H = 35
TERM = (760, 168, 1848, 968)
TPAD = 32
CHAR_W = F(MONO, MONO_SIZE).getlength("M")
MAX_CHARS = int((TERM[2] - TERM[0] - 2 * TPAD) // CHAR_W)


def mix(color: tuple[int, int, int], a: float, base: tuple[int, int, int] = BG) -> tuple[int, int, int]:
    a = max(0.0, min(1.0, a))
    return tuple(int(base[i] + (color[i] - base[i]) * a) for i in range(3))


def ease(t: float, start: float, length: float = 0.35) -> float:
    return max(0.0, min(1.0, (t - start) / length))


def wrap_px(text: str, font: ImageFont.FreeTypeFont, width: float) -> list[str]:
    rows: list[str] = []
    for paragraph in text.split("\n"):
        row = ""
        for word in paragraph.split(" "):
            candidate = word if not row else f"{row} {word}"
            if font.getlength(candidate) <= width:
                row = candidate
            else:
                if row:
                    rows.append(row)
                row = word
        rows.append(row)
    return rows


def wrap_chars(text: str, width: int) -> list[str]:
    rows, row = [], ""
    for word in text.split(" "):
        candidate = word if not row else f"{row} {word}"
        if len(candidate) <= width:
            row = candidate
        else:
            if row:
                rows.append(row)
            while len(word) > width:
                rows.append(word[:width])
                word = word[width:]
            row = word
    if row:
        rows.append(row)
    return rows or [""]


# ------------------------------------------------------------------ capture
def cmd(step: str) -> str:
    return STEPS[step]["command"]


def out(step: str) -> list[str]:
    return [line.rstrip() for line in STEPS[step]["output"].splitlines() if line.strip()]


def rc(step: str) -> int:
    return STEPS[step]["rc"]


def short(line: str, width: int = MAX_CHARS) -> str:
    """Cuts a line to the terminal's width; an edit, so it is marked."""
    return line if len(line) <= width else line[: width - 1] + "…"


# ------------------------------------------------------------------ terminal model
@dataclass
class TL:
    kind: str  # cmd | out | note | badge | gap | log | comment
    text: str = ""
    at: float = 0.0
    typed: bool = False
    badge: str = ""
    color: tuple[int, int, int] = GREEN_BG
    detail: str = ""
    keep: bool = False  # never wrap: cut to one row instead


TYPE_CPS = 60


def draw_terminal(d: ImageDraw.ImageDraw, lines: list[TL], t: float, alpha: float, label: str) -> None:
    x0, y0, x1, _y1 = TERM
    d.rounded_rectangle(TERM, radius=24, fill=mix(PANEL, alpha), outline=mix(BORDER, alpha), width=2)
    for i, c in enumerate(((255, 95, 86), (255, 189, 46), (39, 201, 63))):
        cx = x0 + 34 + i * 28
        d.ellipse((cx - 8, y0 + 26, cx + 8, y0 + 42), fill=mix(c, alpha))
    f_label = inter("Medium", 22)
    d.text((x1 - TPAD - f_label.getlength(label), y0 + 22), label, font=f_label, fill=mix(DIM, alpha))
    mono, mono_b, badge_font = F(MONO, MONO_SIZE), F(MONO_B, MONO_SIZE), F(MONO_B, 23)
    y, x = y0 + 78, x0 + TPAD
    for line in lines:
        if t < line.at:
            break
        a = alpha * ease(t, line.at, 0.15)
        if line.kind == "gap":
            y += LINE_H // 2
            continue
        if line.kind == "badge":
            pop = ease(t, line.at, 0.18)
            bw = badge_font.getlength(line.badge) + 28
            g = int((1 - pop) * 6)
            d.rounded_rectangle((x - g, y - 2 - g, x + bw + g, y + 36 + g), radius=10, fill=mix(line.color, alpha))
            d.text((x + 14, y + 4), line.badge, font=badge_font, fill=mix(WHITE, alpha))
            d.text((x + bw + 18, y + 3), line.detail, font=mono, fill=mix(MUTED, a))
            y += LINE_H + 8
            continue
        styles = {
            "cmd": ("$ ", PROMPT, WHITE, mono_b),
            "note": ("", ACCENT, ACCENT, mono),
            "log": ("", RED, RED, mono),
            "ok": ("", PROMPT, PROMPT, mono),
            "comment": ("# ", DIM, DIM, mono),
            "out": ("", MUTED, MUTED, mono),
        }
        prefix, pcol, bcol, bfont = styles[line.kind]
        text = line.text
        if line.typed:
            text = text[: int(len(text) * min(1.0, max(0.0, t - line.at) / typed_for(text)))]
        width = MAX_CHARS - len(prefix)
        rows = [short(text, width)] if line.keep else wrap_chars(text, width)
        for i, row in enumerate(rows):
            if i == 0 and prefix:
                d.text((x, y), prefix, font=mono_b, fill=mix(pcol, a))
            d.text((x + CHAR_W * len(prefix), y), row, font=bfont, fill=mix(bcol, a))
            y += LINE_H
        if line.typed and len(text) < len(line.text):
            cx = x + CHAR_W * (len(prefix) + len(rows[-1]))
            d.rectangle((cx + 2, y - LINE_H + 5, cx + 14, y - 7), fill=mix(WHITE, alpha))


def typed_for(text: str) -> float:
    """Seconds a typed command takes to appear: long ones type faster."""
    return min(len(text) / TYPE_CPS, 1.6)


# ------------------------------------------------------------------ scenes
@dataclass
class Scene:
    chapter: str
    duration: float
    draw: Callable[[ImageDraw.ImageDraw, float, Scene], None]
    title: str = ""
    body: str = ""
    why: str = ""
    lines: list[TL] = field(default_factory=list)
    step: str = ""
    fade: int = 8  # crossfade frames from the previous scene


def draw_header(d: ImageDraw.ImageDraw, chapter: str) -> None:
    d.text((80, 52), "Attesta", font=inter("ExtraBold", 38), fill=WHITE)
    f = inter("SemiBold", 24)
    d.text((W - 80 - f.getlength(chapter), 62), chapter, font=f, fill=MUTED)


def live_scene(d: ImageDraw.ImageDraw, t: float, s: Scene) -> None:
    draw_header(d, s.chapter)
    a = ease(t, 0.0, 0.4)
    d.text((80, 190), s.step, font=inter("SemiBold", 26), fill=mix(BLUE, a))
    y = 236
    f_title = inter("Bold", 50)
    for row in wrap_px(s.title, f_title, 620):
        d.text((80, y), row, font=f_title, fill=mix(WHITE, a))
        y += 62
    y += 18
    f_body = inter("Regular", 29)
    for row in wrap_px(s.body, f_body, 620):
        d.text((80, y), row, font=f_body, fill=mix(MUTED, ease(t, 0.25, 0.4)))
        y += 42
    if s.why:
        y += 26
        wa = ease(t, 0.6, 0.4)
        f_why = inter("SemiBold", 26)
        rows = wrap_px(s.why, f_why, 590)
        d.rectangle((80, y + 2, 85, y + len(rows) * 38 - 6), fill=mix(ACCENT, wa))
        for row in rows:
            d.text((104, y), row, font=f_why, fill=mix(ACCENT, wa))
            y += 38
    draw_terminal(d, s.lines, t, ease(t, 0.0, 0.3), TERMINAL_LABEL)


def card(d: ImageDraw.ImageDraw, box: tuple[int, int, int, int], a: float, title: str, body: str,
         tag: str = "", accent: tuple[int, int, int] = BLUE, title_size: int = 34, body_size: int = 27) -> None:
    x0, y0, x1, y1 = box
    d.rounded_rectangle(box, radius=22, fill=mix(CARD, a), outline=mix(BORDER, a), width=2)
    d.rectangle((x0 + 28, y0 + 30, x0 + 34, y0 + 30 + title_size + 4), fill=mix(accent, a))
    d.text((x0 + 50, y0 + 28), title, font=inter("Bold", title_size), fill=mix(WHITE, a))
    y = y0 + 28 + title_size + 24
    f_body = inter("Regular", body_size)
    for row in wrap_px(body, f_body, x1 - x0 - 80):
        d.text((x0 + 50, y), row, font=f_body, fill=mix(MUTED, a))
        y += body_size + 14
    if tag:
        f_tag = F(MONO_B, 22)
        tw = f_tag.getlength(tag) + 28
        d.rounded_rectangle((x0 + 50, y1 - 66, x0 + 50 + tw, y1 - 28), radius=10, fill=mix((30, 38, 52), a))
        d.text((x0 + 64, y1 - 60), tag, font=f_tag, fill=mix(accent, a))


def arrow(d: ImageDraw.ImageDraw, p0: tuple[int, int], p1: tuple[int, int], a: float, color=MUTED, width: int = 4) -> None:
    d.line((p0, p1), fill=mix(color, a), width=width)
    ang = math.atan2(p1[1] - p0[1], p1[0] - p0[0])
    size = 16
    left = (p1[0] - size * math.cos(ang - 0.45), p1[1] - size * math.sin(ang - 0.45))
    right = (p1[0] - size * math.cos(ang + 0.45), p1[1] - size * math.sin(ang + 0.45))
    d.polygon((p1, left, right), fill=mix(color, a))


def centered(d: ImageDraw.ImageDraw, y: int, text: str, font: ImageFont.FreeTypeFont, color, a: float) -> None:
    d.text(((W - font.getlength(text)) / 2, y), text, font=font, fill=mix(color, a))


# Act 1 ------------------------------------------------------------
def s_title(d, t, s):
    # The wordmark is visible from the first frame so thumbnails are never blank.
    centered(d, 330, "Attesta", inter("Black", 150), WHITE, 1.0)
    centered(d, 560, "Kubernetes rollouts that leave a record you can verify", inter("Medium", 40), MUTED, ease(t, 0.6, 0.8))
    half = int(160 * ease(t, 1.4, 0.8))
    if half:
        d.rectangle((W // 2 - half, 652, W // 2 + half, 655), fill=BLUE)
    centered(d, 690, "Why it matters   ·   Live on a real cluster   ·   Try it", inter("Medium", 30), DIM, ease(t, 2.0, 0.8))


def s_context(d, t, s):
    draw_header(d, s.chapter)
    centered(d, 170, "Every production change is a decision", inter("Bold", 60), WHITE, ease(t, 0.2, 0.6))
    centered(d, 258, "More and more of them are made by controllers, not people.", inter("Regular", 32), MUTED, ease(t, 0.8, 0.6))
    rows = [
        ("A rollout starts", "a new image, a config change, a sync", "kubectl apply"),
        ("Something judges it", "promote it, pause it, or roll it back", "error rate 20%"),
        ("Someone asks later", "the auditor, the postmortem, on-call", "who decided, why?"),
    ]
    for i, (who, what, chip) in enumerate(rows):
        a = ease(t, 1.6 + i * 0.7, 0.5)
        y0 = 370 + i * 150
        box = (240, y0, 1680, y0 + 118)
        d.rounded_rectangle(box, radius=18, fill=mix(CARD, a), outline=mix(BORDER, a), width=2)
        d.rectangle((240, y0 + 22, 246, y0 + 96), fill=mix(CYAN, a))
        d.text((280, y0 + 36), who, font=inter("Bold", 36), fill=mix(WHITE, a))
        d.text((680, y0 + 40), what, font=inter("Regular", 30), fill=mix(MUTED, a))
        f_chip = F(MONO, 26)
        d.text((1640 - f_chip.getlength(chip), y0 + 42), chip, font=f_chip, fill=mix(ACCENT, a))
    centered(d, 860, "The last step is where most tooling has nothing to show.",
             inter("SemiBold", 34), WHITE, ease(t, 4.0, 0.6))


def s_question(d, t, s):
    centered(d, 400, "A controller rolled back production at 3 a.m.", inter("SemiBold", 62), WHITE, ease(t, 0.3, 0.7))
    centered(d, 492, "Can you prove what it saw, and that nobody changed it?", inter("SemiBold", 56), ACCENT, ease(t, 1.5, 0.7))


def s_risks(d, t, s):
    draw_header(d, s.chapter)
    centered(d, 150, "Three ways rollouts go wrong", inter("Bold", 60), WHITE, ease(t, 0))
    cards = [
        ("Bad revisions hit everyone",
         "A rolling update replaces every pod. By the time metrics show the problem, all users have the new version.",
         "blast radius", RED),
        ("Deploys on a spent budget",
         "Shipping while the service is already burning its error budget turns a bad day into an outage.",
         "SLOs & error budgets", ACCENT),
        ("No proof of what happened",
         "Events expire in an hour. Annotations and logs can be edited or deleted, and nothing signs them.",
         "audit & change control", BLUE),
    ]
    for i, (title, body, tag, accent) in enumerate(cards):
        a = ease(t, 0.7 + i * 1.6, 0.45)
        x0 = 110 + i * 580
        card(d, (x0, 300, x0 + 540, 700), a, title, body, tag=tag, accent=accent, title_size=36, body_size=29)
    centered(d, 800, "Rollout tools handle the first two. The third is usually left to logs.", inter("SemiBold", 32), WHITE, ease(t, 6.0))


def s_solution(d, t, s):
    draw_header(d, s.chapter)
    centered(d, 140, "Attesta acts on a rollout, then proves what it did", inter("Bold", 56), WHITE, ease(t, 0))
    a1, a2, a3, a4 = ease(t, 0.4), ease(t, 0.9), ease(t, 1.6), ease(t, 2.3)
    card(d, (90, 420, 450, 620), a1, "Deployment", "any image, any GitOps tool", accent=CYAN)
    arrow(d, (450, 520), (556, 520), a2)
    gate = (560, 300, 1180, 760)
    d.rounded_rectangle(gate, radius=26, fill=mix((18, 30, 46), a2), outline=mix(BLUE, a2), width=3)
    d.text((600, 330), "attesta-controller", font=inter("ExtraBold", 40), fill=mix(WHITE, a2))
    items = ["Canary traffic in steps", "Analysis of the new revision only",
             "SLO gate on the error budget", "Rolls back, pauses, promotes",
             "Records every decision"]
    for i, item in enumerate(items):
        a = ease(t, 1.1 + i * 0.25)
        yy = 420 + i * 62
        d.ellipse((604, yy + 12, 618, yy + 26), fill=mix(PROMPT, a))
        d.text((636, yy), item, font=inter("Medium", 29), fill=mix(WHITE, a))
    arrow(d, (1180, 440), (1306, 420), a3)
    card(d, (1310, 300, 1830, 520), a3, "RolloutDecision", "hash-chained, Ed25519-signed, undeletable", accent=PROMPT,
         title_size=32, body_size=26)
    arrow(d, (1570, 520), (1570, 566), a4)
    card(d, (1310, 570, 1830, 760), a4, "Off-cluster archive", "append-only, flags missing records", accent=ACCENT,
         title_size=32, body_size=26)
    centered(d, 840, "Controllers act. Records prove.", inter("Bold", 46), ACCENT, ease(t, 3.4))
    centered(d, 912, "Open source  ·  plain Kubernetes  ·  verify with attesta, jq or sha256sum", inter("Medium", 28), MUTED, ease(t, 4.0))


# Act 3 ------------------------------------------------------------
def s_outro(d, t, s):
    centered(d, 230, "Guard your own Deployments", inter("SemiBold", 48), MUTED, 1.0)
    pill_font = F(MONO_B, 42)
    lines = ["kubectl apply -k deploy/crds", "kubectl apply -k deploy/controller"]
    pw = max(pill_font.getlength("$ " + x) for x in lines) + 100
    px = (W - pw) / 2
    d.rounded_rectangle((px, 330, px + pw, 510), radius=24, fill=PANEL, outline=PROMPT, width=3)
    for i, text in enumerate(lines):
        d.text((px + 50, 360 + i * 70), "$ ", font=pill_font, fill=PROMPT)
        d.text((px + 50 + pill_font.getlength("$ "), 360 + i * 70), text, font=pill_font, fill=WHITE)
    centered(d, 590, "github.com/gaurav-gs7/Attesta", inter("Bold", 52), WHITE, ease(t, 0.3))
    centered(d, 700, "Apache-2.0  ·  Go  ·  controller-runtime  ·  tested end to end on kind in CI", inter("Medium", 30), MUTED, ease(t, 0.8))
    centered(d, 750, "Every terminal line in this video is from a real run on a kind cluster", inter("Medium", 26), DIM, ease(t, 1.2))


# ------------------------------------------------------------------ live scenes
def seq(*items: tuple[str, object], start: float = 0.3, pause: float = 0.35) -> list[TL]:
    """Lays lines out in time: commands are typed, output follows."""
    lines, at = [], start
    for kind, value in items:
        if kind == "cmd":
            text = str(value)
            lines.append(TL("cmd", text, at, True, keep=False))
            at += typed_for(text) + 0.45
        elif kind == "outs":
            for row in value:  # type: ignore[union-attr]
                lines.append(TL("out", row, at, keep=True))
                at += 0.12
            at += pause
        elif kind == "gap":
            lines.append(TL("gap", at=at))
        elif kind == "wait":
            at += float(value)  # type: ignore[arg-type]
        elif isinstance(value, TL):
            value.at = at
            lines.append(value)
            at += 0.3 if value.kind != "comment" else 0.5
        else:
            lines.append(TL(kind, str(value), at))
            at += 0.3 if kind != "comment" else 0.5
    return lines


def last_at(lines: list[TL]) -> float:
    return max(line.at + (typed_for(line.text) if line.typed else 0) for line in lines)


def first_match(step: str, pattern: str) -> str:
    return next((line for line in out(step) if re.search(pattern, line)), "")


share = out("canary-share")[-1]
aborted = out("gw-record")
paused = out("gate-paused")[-1]
verify_ok = [line for line in out("records-verify") if line.startswith("OK")]
tamper = [line for line in out("tamper-export") if line.startswith(("OK", "BROKEN"))]
api_line = out("api-verify")[-1]
api = json.loads(api_line)
# One line of jq -c output, broken before "archive" to fit.
api_rows = [api_line[: api_line.index(',"archive"') + 1], api_line[api_line.index(',"archive"') + 1:]]


def verdicts(lines: list[str]) -> list[tuple[str, str]]:
    return [("ok" if line.startswith("OK") else "log", line) for line in lines]


LIVE = [
    {"title": "Installed with kubectl, nothing else",
     "body": "Two controller replicas with leader election, and the API. The controller signs each decision with an Ed25519 key and copies it to the API's archive.",
     "lines": seq(("cmd", cmd("install")), ("outs", out("install"))),
     "duration": 6.5},
    {"title": "A canary gets a real share of traffic",
     "body": "The last good revision runs in a managed checkout-primary. A new revision gets one pod beside the primary's two: a third of real requests.",
     "why": "Limits the blast radius of a bad revision",
     "lines": seq(("cmd", cmd("canary-rest")), ("outs", out("canary-rest")),
                  ("cmd", cmd("canary-new")),
                  ("cmd", cmd("canary-status")), ("outs", out("canary-status")),
                  ("comment", "300 requests through the checkout Service:"),
                  ("cmd", cmd("canary-share")), ("note", share)),
     "duration": 13.0},
    {"title": "Promoted, and on record",
     "body": "When the step's pause passes, the new template is copied into the primary and the canary scales back to zero. Every step is a RolloutDecision.",
     "lines": seq(("cmd", cmd("canary-promoted")), ("outs", out("canary-promoted"))),
     "duration": 7.0},
    {"title": "A failing canary is pulled from traffic",
     "body": "With Gateway API, Attesta sets the HTTPRoute's weights. This revision answers 20% errors at its 10% step, so the route goes back to the primary.",
     "lines": seq(("comment", "HTTPRoute weights, primary then canary:"),
                  ("cmd", cmd("gw-rest")), ("outs", out("gw-rest")),
                  ("cmd", cmd("gw-new")),
                  ("cmd", cmd("gw-ten")), ("outs", out("gw-ten")),
                  ("comment", "two failed checks later:"),
                  ("cmd", cmd("gw-after")), ("outs", out("gw-after")),
                  ("badge", TL("badge", badge="ABORTED", color=RED_BG, detail=aborted[1]))),
     "duration": 14.0},
    {"title": "Rollouts wait while the error budget is spent",
     "body": "The SLO gate uses the same burn rates and 30-day budget as the API. It pauses each new revision until the service recovers, then resumes it.",
     "why": "Keeps deploys from landing on an outage",
     "lines": seq(("cmd", cmd("gate-new")),
                  ("cmd", cmd("gate-paused")), ("note", paused),
                  ("comment", "the error rate drops back to zero:"),
                  ("cmd", cmd("gate-record")), ("outs", out("gate-record"))),
     "duration": 11.5},
    {"title": "Every decision is chained and signed",
     "body": "Each record carries the hash of the one before it and the controller's signature. Anyone with the public key can check the whole history.",
     "lines": seq(("cmd", cmd("records-verify")), *verdicts(verify_ok)),
     "duration": 7.5},
    {"title": "Nobody can delete or rewrite a record",
     "body": "An admission policy refuses deletes, cluster-admins included, and a record's spec cannot change once it is written.",
     "lines": seq(("cmd", cmd("delete-refused")), ("log", out("delete-refused")[0]), ("gap", ""),
                  ("cmd", cmd("edit-refused")), ("log", out("edit-refused")[0])),
     "duration": 10.0},
    {"title": "An edited export does not verify",
     "body": "Change the aborted canary's error rate in an export and its chain breaks. The API checks every chain on read and compares it with the archive.",
     "lines": seq(("cmd", cmd("tamper-export")), *verdicts(tamper), ("gap", ""),
                  ("cmd", cmd("api-verify")), *[("ok", part) for part in api_rows]),
     "duration": 11.0},
]

SCENES: list[Scene] = [
    Scene("", 4.5, s_title),
    Scene("Introduction", 7.0, s_context, fade=18),
    Scene("", 4.5, s_question, fade=18),
    Scene("1 · Why it matters", 9.5, s_risks, fade=12),
    Scene("1 · Why it matters", 7.0, s_solution, fade=12),
]
for i, spec in enumerate(LIVE):
    SCENES.append(Scene("2 · Live on kind", spec["duration"], live_scene, title=spec["title"], body=spec["body"],
                        why=spec.get("why", ""), lines=spec["lines"], step=f"{i + 1:02d} / {len(LIVE):02d}"))
SCENES.append(Scene("", 7.0, s_outro, fade=12))
TOTAL = sum(s.duration for s in SCENES)


def draw_progress(d: ImageDraw.ImageDraw, elapsed: float) -> None:
    y = H - 10
    d.rectangle((0, y, W, H), fill=(22, 28, 38))
    d.rectangle((0, y, int(W * elapsed / TOTAL), H), fill=BLUE)


POSTER_SCENE = next(i for i, scene in enumerate(SCENES) if scene.draw is s_solution)


def frame(index: int, t: float, elapsed: float | None) -> Image.Image:
    scene = SCENES[index]
    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)
    scene.draw(d, t, scene)
    if elapsed is not None:
        draw_progress(d, elapsed + t)
    return img


def check_timing() -> None:
    for scene in SCENES:
        if scene.lines:
            end = last_at(scene.lines)
            if end > scene.duration - 2.0:
                raise SystemExit(f"{scene.title!r}: its last line appears at {end:.1f}s of {scene.duration}s")


def render() -> None:
    check_timing()
    out_dir = ARGS.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    video = out_dir / "attesta-explainer.mp4"
    ff = subprocess.Popen(
        ["ffmpeg", "-y", "-loglevel", "error", "-f", "rawvideo", "-pix_fmt", "rgb24", "-s", f"{W}x{H}",
         "-r", str(FPS), "-i", "-", "-c:v", "libx264", "-preset", "slow", "-crf", "18",
         "-pix_fmt", "yuv420p", "-profile:v", "high", "-movflags", "+faststart", str(video)],
        stdin=subprocess.PIPE,
    )
    assert ff.stdin is not None
    if ARGS.stills:
        ARGS.stills.mkdir(parents=True, exist_ok=True)
    elapsed = 0.0
    previous: Image.Image | None = None
    for index, scene in enumerate(SCENES):
        frames = round(scene.duration * FPS)
        last = previous
        for f in range(frames):
            img = frame(index, f / FPS, elapsed)
            if previous is not None and f < scene.fade:
                img = Image.blend(previous, img, (f + 1) / (scene.fade + 1))
            if f == frames - 1:
                last = img
                if ARGS.stills:
                    img.save(ARGS.stills / f"{index:02d}.png")
            ff.stdin.write(img.tobytes())
        previous = last
        elapsed += scene.duration
    ff.stdin.close()
    if ff.wait() != 0:
        raise SystemExit("ffmpeg failed")

    poster_scene = SCENES[POSTER_SCENE]
    poster = frame(POSTER_SCENE, poster_scene.duration - 1 / FPS, None).resize((1280, 720), Image.LANCZOS)
    poster.save(out_dir / "attesta-demo-poster.png", optimize=True)

    palette = (
        "fps=8,scale=960:-1:flags=lanczos,split[a][b];"
        "[a]palettegen=max_colors=128:stats_mode=full[p];"
        "[b][p]paletteuse=dither=bayer:bayer_scale=5:diff_mode=rectangle"
    )
    gif = subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error", "-i", str(video), "-vf", palette, str(out_dir / "attesta-demo.gif")],
        check=False,
    )
    if gif.returncode != 0:
        raise SystemExit("GIF conversion failed")
    print(f"total {TOTAL:.1f}s, {len(SCENES)} scenes -> {video.name}, attesta-demo.gif, attesta-demo-poster.png")


if __name__ == "__main__":
    render()
