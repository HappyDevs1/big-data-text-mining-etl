"""Builds the submission PDF on the Eduvos assessment template.

Run build_report.py first so report.md is up to date, then:

    python3 build_pdf.py

The PDF is put together from four parts:
  1. the Eduvos coversheet and declaration (template/eduvos_template.pdf, pages 1 and 2)
  2. a table of contents page
  3. the report body (report.md converted to Word with pandoc, then to PDF with LibreOffice)
  4. the AI evidence section and AI disclosure appendix (template pages 3 and 4)
Page numbers are added at the top right from page 3 onwards.
"""
import pathlib
import re
import shutil
import subprocess
import tempfile
import zipfile

import fitz

DOCS = pathlib.Path(__file__).resolve().parent
OUT = DOCS / "ITBDA4-14_Project2_EDUV13317839.pdf"

COVER_FIELDS = {
    "ITSMA4-14": "ITBDA4-14",
    "Mr Sydney Khumalo": "Mr Gilbert Lekula (Ngobeni)",
}
NAVY = (0, 0x20 / 255, 0x60 / 255)


def reference_doc(tmp):
    """Pandoc's default reference.docx restyled to match the template: Arial, black bold headings."""
    ref = tmp / "ref.docx"
    with open(ref, "wb") as fh:
        subprocess.run(["pandoc", "--print-default-data-file", "reference.docx"], stdout=fh, check=True)
    with zipfile.ZipFile(ref) as z:
        files = {n: z.read(n) for n in z.namelist()}
    styles = files["word/styles.xml"].decode("utf-8")
    arial = '<w:rFonts w:ascii="Arial" w:hAnsi="Arial" w:eastAsia="Arial" w:cs="Arial" />'
    styles = re.sub(r"<w:rFonts [^>]*Theme[^>]*/>", arial, styles, flags=re.S)
    styles = styles.replace('<w:sz w:val="24" />\n        <w:szCs w:val="24" />',
                            '<w:sz w:val="21" />\n        <w:szCs w:val="21" />', 1)
    sizes = {"Heading1": 36, "Heading2": 26, "Heading3": 23}

    def fix_heading(match):
        block = match.group(0)
        sid = match.group(1)
        block = re.sub(r"<w:color [^>]*/>", '<w:color w:val="000000" />', block)
        block = re.sub(r"<w:sz w:val=\"\d+\" />", f'<w:sz w:val="{sizes[sid]}" />', block)
        block = re.sub(r"<w:szCs w:val=\"\d+\" />", f'<w:szCs w:val="{sizes[sid]}" />', block)
        block = re.sub(r"<w:i />|<w:iCs />", "", block)
        if sid == "Heading1":
            block = block.replace("<w:keepNext />", "<w:keepNext />\n      <w:pageBreakBefore />", 1)
        if "<w:b />" not in block:
            block = block.replace("<w:rPr>", "<w:rPr>\n      <w:b />", 1)
        return block

    styles = re.sub(r'<w:style [^>]*w:styleId="(Heading[123])".*?</w:style>', fix_heading, styles, flags=re.S)
    styles = styles.replace('<w:rFonts w:ascii="Consolas" w:hAnsi="Consolas" />\n      <w:sz w:val="22" />',
                            '<w:rFonts w:ascii="Courier New" w:hAnsi="Courier New" />\n      <w:sz w:val="16" />')
    files["word/styles.xml"] = styles.encode("utf-8")
    with zipfile.ZipFile(ref, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in files.items():
            z.writestr(name, data)
    return ref


def size_tables(text):
    """Give pipe table columns widths in proportion to their longest cell.

    The template uses equal dashes in every separator row, which makes pandoc
    give each column the same width and squeezes long text columns.
    """
    lines = text.split("\n")
    in_code = False
    for i, line in enumerate(lines):
        if line.startswith("```"):
            in_code = not in_code
        if in_code or not re.fullmatch(r"\|(\s*:?-+:?\s*\|)+", line) or i == 0:
            continue
        rows = [lines[i - 1]]
        j = i + 1
        while j < len(lines) and lines[j].startswith("|"):
            rows.append(lines[j])
            j += 1
        cells = [r.strip().strip("|").split("|") for r in rows]
        ncol = len(line.strip().strip("|").split("|"))
        widths = [max(11, min(40, max(len(c[k].strip()) if k < len(c) else 0 for c in cells)))
                  for k in range(ncol)]
        lines[i] = "|" + "|".join("-" * w for w in widths) + "|"
    return "\n".join(lines)


def body_pdf(tmp):
    text = (DOCS / "report.md").read_text(encoding="utf-8")
    text = re.sub(r"\A---\n.*?\n---\n", "", text, flags=re.S)
    text = size_tables(text)
    md = DOCS / "_body.md"
    md.write_text(text, encoding="utf-8")
    docx = tmp / "body.docx"
    try:
        subprocess.run(["pandoc", str(md), "-o", str(docx), "-f", "markdown-smart",
                        "--resource-path=.:..", "--columns=20", f"--reference-doc={reference_doc(tmp)}"],
                       cwd=DOCS, check=True)
    finally:
        md.unlink()
    subprocess.run(["soffice", "--headless", "--convert-to", "pdf", "--outdir", str(tmp), str(docx)],
                   check=True, capture_output=True)
    return fitz.open(tmp / "body.pdf"), text


def headings(markdown):
    found = []
    in_code = False
    for line in markdown.splitlines():
        if line.startswith("```"):
            in_code = not in_code
        elif not in_code:
            m = re.match(r"(#{1,2}) (.+)", line)
            if m:
                found.append((len(m.group(1)), m.group(2).strip()))
    return found


def heading_pages(body, items):
    """Find the body page of each heading by looking for large text that matches it."""
    pages = []
    start = 0
    for level, title in items:
        hit = None
        for pno in range(start, len(body)):
            for block in body[pno].get_text("dict")["blocks"]:
                for line in block.get("lines", []):
                    spans = line["spans"]
                    text = "".join(s["text"] for s in spans).strip()
                    if text == title and spans and spans[0]["size"] >= 12:
                        hit = pno
                        break
                if hit is not None:
                    break
            if hit is not None:
                break
        if hit is None:
            raise SystemExit(f"Heading not found in body PDF: {title}")
        pages.append(hit)
        start = hit
    return pages


def toc_page(doc, items, pages, offset):
    page = doc.new_page(width=596, height=842)
    page.insert_text((40, 95), "Assessment", fontname="hebo", fontsize=22)
    page.insert_text((40, 135), "Table of Contents", fontname="hebo", fontsize=15)
    y = 165
    right = 556
    links = []
    for (level, title), pno in zip(items, pages):
        font, size, x = ("hebo", 9.5, 40) if level == 1 else ("helv", 9.5, 56)
        y += 8 if level == 1 else 0
        num = str(pno + offset)
        page.insert_text((x, y), title, fontname=font, fontsize=size)
        tw = fitz.get_text_length(title, fontname=font, fontsize=size)
        nw = fitz.get_text_length(num, fontname=font, fontsize=size)
        dots_from, dots_to = x + tw + 3, right - nw - 3
        dot_w = fitz.get_text_length(".", fontname="helv", fontsize=size)
        page.insert_text((dots_from, y), "." * max(0, int((dots_to - dots_from) / dot_w)),
                         fontname="helv", fontsize=size)
        page.insert_text((right - nw, y), num, fontname=font, fontsize=size)
        link = {"kind": fitz.LINK_GOTO, "page": pno + offset - 1,
                "from": fitz.Rect(x, y - 10, right, y + 3)}
        links.append(link)
        y += 17
    return links


def replace_text(page, old, new, fontname="hebo", fontsize=12, color=NAVY):
    for rect in page.search_for(old):
        page.add_redact_annot(rect, fill=(1, 1, 1))
        page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE)
        if new:
            page.insert_text((rect.x0, rect.y1 - 3.1), new, fontname=fontname, fontsize=fontsize, color=color)


def main():
    template = fitz.open(DOCS / "template" / "eduvos_template.pdf")
    with tempfile.TemporaryDirectory() as t:
        tmp = pathlib.Path(t)
        body, markdown = body_pdf(tmp)
        items = headings(markdown)
        pages = heading_pages(body, items)

        doc = fitz.open()
        doc.insert_pdf(template, from_page=0, to_page=1)
        for old, new in COVER_FIELDS.items():
            replace_text(doc[0], old, new)
        offset = 4  # 2 cover pages, 1 contents page, then body page 0 is page 4
        links = toc_page(doc, items, pages, offset)
        doc.insert_pdf(body)
        for link in links:
            doc[2].insert_link(link)
        first_appendix = len(doc)
        doc.insert_pdf(template, from_page=2, to_page=3)
        for i, old in ((first_appendix, "17"), (first_appendix + 1, "18")):
            for rect in doc[i].search_for(old, clip=fitz.Rect(500, 20, 596, 60)):
                doc[i].add_redact_annot(rect, fill=(1, 1, 1))
            doc[i].apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE)

        for i in range(2, len(doc)):
            num = str(i + 1)
            w = fitz.get_text_length(num, fontname="helv", fontsize=11)
            doc[i].insert_text((552.8 - w, 46), num, fontname="helv", fontsize=11)

        outline = [[1, "Eduvos Assessment Coversheet", 1], [1, "Table of Contents", 3]]
        outline += [[lvl, title, p + offset] for (lvl, title), p in zip(items, pages)]
        outline += [[1, "AI Evidence Section", first_appendix + 1],
                    [1, "AI Disclosure Appendix", first_appendix + 2]]
        doc.set_toc(outline)
        doc.set_metadata({"title": "ITBDA4-14 Project 2", "author": "Happy Mahlangu"})
        doc.save(OUT, garbage=4, deflate=True)
    print(f"Wrote {OUT.name} with {len(fitz.open(OUT))} pages")


if __name__ == "__main__":
    if not shutil.which("soffice"):
        raise SystemExit("LibreOffice (soffice) is needed to convert the report to PDF")
    main()
