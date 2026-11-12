"""Builds the project report.

Reads report_template.md, replaces include markers with real code from the
project, writes report.md and converts it to Word with pandoc.

Markers:
  {{file:relative/path}}            whole file
  {{chunk:relative/path.Rmd:label}} one R or SQL chunk from a notebook

Run from the docs folder:  python3 build_report.py
"""
import pathlib
import re
import subprocess

DOCS = pathlib.Path(__file__).resolve().parent
ROOT = DOCS.parent

LANG = {".java": "java", ".xml": "xml", ".sql": "sql", ".sh": "bash", ".R": "r", ".Rmd": "r"}


def read_chunk(path, label):
    text = (ROOT / path).read_text(encoding="utf-8")
    pattern = r"```\{r " + re.escape(label) + r"[^}]*\}\n(.*?)```"
    match = re.search(pattern, text, re.S)
    if not match:
        raise SystemExit(f"Chunk {label} not found in {path}")
    return match.group(1).rstrip()


def replace(match):
    kind, rest = match.group(1), match.group(2)
    if kind == "file":
        path = ROOT / rest
        code = path.read_text(encoding="utf-8").rstrip()
        lang = LANG.get(path.suffix, "")
        return f"```{lang}\n{code}\n```"
    path, label = rest.rsplit(":", 1)
    return f"```r\n{read_chunk(path, label)}\n```"


template = (DOCS / "report_template.md").read_text(encoding="utf-8")
report = re.sub(r"\{\{(file|chunk):([^}]+)\}\}", replace, template)

if chr(0x2014) in report:
    raise SystemExit("Report contains an em dash, please remove it")

(DOCS / "report.md").write_text(report, encoding="utf-8")
subprocess.run(
    ["pandoc", "report.md", "-o", "ITBDA4-14_Project2_Report.docx",
     "--toc", "--toc-depth=2", "--resource-path=.:..", "-f", "markdown-smart"],
    cwd=DOCS, check=True,
)
print("Wrote report.md and ITBDA4-14_Project2_Report.docx")
