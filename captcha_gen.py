#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Text CAPTCHA generator: PIL-drawn distorted text captcha -> JPEG bytes.

Used by tg-watchbot human verification for new users before relaying messages.
"""
from __future__ import annotations

import io
import random
import string

from PIL import Image, ImageDraw, ImageFilter, ImageFont

ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # no I/1/O/0 ambiguities
FONT_CANDIDATES = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
]


def _load_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont | None:
    for path in FONT_CANDIDATES:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    try:
        return ImageFont.load_default()
    except Exception:
        return None


def generate_captcha_text(length: int = 4) -> str:
    return "".join(random.SystemRandom().choice(ALPHABET) for _ in range(length))


def render_captcha_image(text: str, width: int = 220, height: int = 90) -> bytes:
    """Render distorted text captcha; returns JPEG bytes (~small, fine to send in TG)."""
    bg = (245, 245, 245)
    img = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(img)

    # background noise curves
    for _ in range(6):
        x0 = random.randint(0, width // 4)
        y0 = random.randint(0, height)
        x1 = random.randint(width // 2, width)
        y1 = random.randint(0, height)
        if y1 < y0:
            y0, y1 = y1, y0
        color = tuple(random.randint(120, 200) for _ in range(3))
        draw.arc([x0, y0, x1, y1], start=random.randint(0, 180), end=random.randint(180, 360), fill=color, width=2)

    font = _load_font(48)
    char_count = len(text)
    slot = width // max(char_count, 1)
    for i, ch in enumerate(text):
        fsize = random.randint(40, 54)
        f = _load_font(fsize)
        f = f or font
        color = tuple(random.randint(20, 110) for _ in range(3))
        # per-char vertical wobble
        x = i * slot + random.randint(2, max(4, slot // 6))
        y = (height - fsize) // 2 + random.randint(-8, 8)
        # per-char rotation
        rotation = random.randint(-30, 30)
        try:
            char_img = Image.new("RGBA", (fsize + 16, fsize + 16), (0, 0, 0, 0))
            cdraw = ImageDraw.Draw(char_img)
            cdraw.text((8, 4), ch, font=f, fill=color)
            char_img = char_img.rotate(rotation, resample=Image.Resampling.BICUBIC, expand=False)
            img.paste(char_img, (x, max(0, y - 8)), char_img)
        except Exception:
            draw.text((x, max(0, y)), ch, font=f, fill=color)

    # slight blur + noise dots
    img = img.filter(ImageFilter.GaussianBlur(0.6))
    for _ in range(60):
        xy = (random.randint(0, width - 1), random.randint(0, height - 1))
        draw.point(xy, fill=tuple(random.randint(80, 200) for _ in range(3)))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=82)
    return buf.getvalue()


if __name__ == "__main__":
    t = generate_captcha_text()
    data = render_captcha_image(t)
    print(f"captcha text={t} bytes={len(data)}")
    with open("/tmp/captcha-sample.jpg", "wb") as f:
        f.write(data)
