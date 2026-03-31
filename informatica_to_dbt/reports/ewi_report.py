"""EWI (Errors, Warnings, Informational) assessment report generator.

Produces an HTML report summarising the results of an Informatica-to-dbt
conversion run.  Modelled after the SnowConvert Assessment Report format:

- **E**rrors — transformations or mappings that failed to convert
- **W**arnings — partial conversions with known limitations
- **I**nformational — successful conversions with notes

The report includes:
  1. Executive summary (total mappings, success rate, quality score)
  2. Per-mapping detail table (complexity, strategy, status, files, timing)
  3. Error breakdown by category
  4. Transformation type coverage
  5. Recommendations
"""

from __future__ import annotations

import datetime
import html
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from informatica_to_dbt.metrics import MappingMetrics, RepositoryMetrics

logger = logging.getLogger("informatica_dbt.reports")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class EWIItem:
    """A single EWI entry."""

    level: str               # "error" | "warning" | "info"
    mapping_name: str
    code: str                # e.g. "E001", "W003", "I010"
    message: str
    detail: str = ""

    @property
    def css_class(self) -> str:
        return {"error": "ewi-error", "warning": "ewi-warning", "info": "ewi-info"}.get(
            self.level, "ewi-info"
        )


@dataclass
class EWIReport:
    """Complete EWI assessment report data."""

    title: str = "Informatica-to-dbt Migration Assessment"
    generated_at: str = field(
        default_factory=lambda: datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    repository_metrics: Optional[RepositoryMetrics] = None
    items: List[EWIItem] = field(default_factory=list)

    @property
    def errors(self) -> List[EWIItem]:
        return [i for i in self.items if i.level == "error"]

    @property
    def warnings(self) -> List[EWIItem]:
        return [i for i in self.items if i.level == "warning"]

    @property
    def infos(self) -> List[EWIItem]:
        return [i for i in self.items if i.level == "info"]

    def to_json(self) -> str:
        """Serialise the report data to JSON."""
        data = {
            "title": self.title,
            "generated_at": self.generated_at,
            "summary": self.repository_metrics.to_dict() if self.repository_metrics else {},
            "items": [
                {
                    "level": i.level,
                    "mapping_name": i.mapping_name,
                    "code": i.code,
                    "message": i.message,
                    "detail": i.detail,
                }
                for i in self.items
            ],
        }
        return json.dumps(data, indent=2)


# ---------------------------------------------------------------------------
# Report generator
# ---------------------------------------------------------------------------

class EWIReportGenerator:
    """Generate EWI assessment reports from conversion metrics.

    Usage::

        gen = EWIReportGenerator()
        report = gen.generate(repo_metrics)
        gen.write_html(report, "assessment_report.html")
    """

    def generate(self, repo_metrics: RepositoryMetrics) -> EWIReport:
        """Build an EWI report from repository-level metrics."""
        report = EWIReport(repository_metrics=repo_metrics)

        for m in repo_metrics.mapping_metrics:
            self._classify_mapping(m, report)

        return report

    def write_html(self, report: EWIReport, output_path: str) -> str:
        """Write the report as a self-contained HTML file.

        Returns the absolute path to the written file.
        """
        html_content = self._render_html(report)
        path = Path(output_path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(html_content, encoding="utf-8")
        logger.info("EWI report written to %s", path)
        return str(path)

    def write_json(self, report: EWIReport, output_path: str) -> str:
        """Write the report as a JSON file.

        Returns the absolute path to the written file.
        """
        path = Path(output_path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(report.to_json(), encoding="utf-8")
        logger.info("EWI JSON report written to %s", path)
        return str(path)

    # ------------------------------------------------------------------
    # Classification logic
    # ------------------------------------------------------------------

    def _classify_mapping(self, m: MappingMetrics, report: EWIReport) -> None:
        """Classify a mapping's results into EWI items."""
        if m.status == "failed":
            report.items.append(EWIItem(
                level="error",
                mapping_name=m.mapping_name,
                code="E001",
                message=f"Mapping conversion failed: {m.mapping_name}",
                detail=m.error_message or "No error details available",
            ))
        elif m.status == "partial":
            report.items.append(EWIItem(
                level="warning",
                mapping_name=m.mapping_name,
                code="W001",
                message=f"Partial conversion: {m.mapping_name}",
                detail=f"Generated {m.files_generated} files with "
                       f"{m.total_errors} errors, {m.total_warnings} warnings",
            ))
        else:
            report.items.append(EWIItem(
                level="info",
                mapping_name=m.mapping_name,
                code="I001",
                message=f"Successfully converted: {m.mapping_name}",
                detail=f"Generated {m.files_generated} files, "
                       f"quality score {m.quality_score}/100",
            ))

        # Additional warnings for specific issues
        if m.sql_errors > 0:
            report.items.append(EWIItem(
                level="warning",
                mapping_name=m.mapping_name,
                code="W010",
                message=f"SQL validation errors in {m.mapping_name}",
                detail=f"{m.sql_errors} SQL error(s) detected in generated models",
            ))
        if m.yaml_errors > 0:
            report.items.append(EWIItem(
                level="warning",
                mapping_name=m.mapping_name,
                code="W011",
                message=f"YAML validation errors in {m.mapping_name}",
                detail=f"{m.yaml_errors} YAML error(s) in schema/source files",
            ))
        if m.heal_attempts > 0:
            report.items.append(EWIItem(
                level="info",
                mapping_name=m.mapping_name,
                code="I010",
                message=f"Self-healing applied to {m.mapping_name}",
                detail=f"{m.heal_attempts} correction round(s) via LLM",
            ))
        if m.complexity_score >= 80:
            report.items.append(EWIItem(
                level="info",
                mapping_name=m.mapping_name,
                code="I020",
                message=f"High complexity mapping: {m.mapping_name}",
                detail=f"Complexity score {m.complexity_score}/100, "
                       f"strategy: {m.strategy}",
            ))

    # ------------------------------------------------------------------
    # HTML rendering
    # ------------------------------------------------------------------

    def _render_html(self, report: EWIReport) -> str:
        """Render a self-contained HTML report."""
        rm = report.repository_metrics
        e = html.escape

        # Summary stats
        total = rm.total_mappings if rm else 0
        success = rm.success_count if rm else 0
        partial = rm.partial_count if rm else 0
        failed = rm.failed_count if rm else 0
        rate = rm.success_rate * 100 if rm else 0
        avg_q = rm.avg_quality_score if rm else 0
        total_files = rm.total_files_generated if rm else 0
        total_llm = rm.total_llm_calls if rm else 0
        total_time = rm.total_seconds if rm else 0

        # Build mapping rows
        mapping_rows = ""
        if rm:
            for m in rm.mapping_metrics:
                status_class = {
                    "success": "status-success",
                    "partial": "status-partial",
                    "failed": "status-failed",
                }.get(m.status, "")
                mapping_rows += f"""
                <tr>
                    <td>{e(m.mapping_name)}</td>
                    <td>{e(m.workflow_name)}</td>
                    <td>{m.complexity_score}</td>
                    <td>{e(m.strategy)}</td>
                    <td class="{status_class}">{e(m.status.upper())}</td>
                    <td>{m.files_generated}</td>
                    <td>{m.total_errors}</td>
                    <td>{m.total_warnings}</td>
                    <td>{m.quality_score}</td>
                    <td>{m.llm_calls}</td>
                    <td>{m.heal_attempts}</td>
                    <td>{m.total_seconds:.1f}s</td>
                </tr>"""

        # Build EWI rows
        ewi_rows = ""
        for item in report.items:
            ewi_rows += f"""
                <tr class="{item.css_class}">
                    <td>{e(item.code)}</td>
                    <td>{e(item.level.upper())}</td>
                    <td>{e(item.mapping_name)}</td>
                    <td>{e(item.message)}</td>
                    <td>{e(item.detail)}</td>
                </tr>"""

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{e(report.title)}</title>
<style>
  :root {{
    --bg: #0f1117;
    --card: #1a1d27;
    --border: #2d3748;
    --text: #e2e8f0;
    --muted: #a0aec0;
    --blue: #4299e1;
    --green: #48bb78;
    --yellow: #ecc94b;
    --red: #fc8181;
    --cyan: #76e4f7;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg);
    color: var(--text);
    padding: 2rem;
    line-height: 1.6;
  }}
  h1 {{ color: var(--cyan); margin-bottom: 0.5rem; font-size: 1.8rem; }}
  h2 {{ color: var(--blue); margin: 2rem 0 1rem; font-size: 1.3rem; border-bottom: 1px solid var(--border); padding-bottom: 0.5rem; }}
  .subtitle {{ color: var(--muted); margin-bottom: 2rem; }}
  .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin: 1.5rem 0; }}
  .card {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1.2rem;
    text-align: center;
  }}
  .card .value {{ font-size: 2rem; font-weight: bold; color: var(--cyan); }}
  .card .label {{ font-size: 0.85rem; color: var(--muted); margin-top: 0.3rem; }}
  .card.success .value {{ color: var(--green); }}
  .card.warning .value {{ color: var(--yellow); }}
  .card.error .value {{ color: var(--red); }}
  table {{
    width: 100%;
    border-collapse: collapse;
    margin: 1rem 0;
    font-size: 0.9rem;
  }}
  th, td {{
    padding: 0.6rem 0.8rem;
    text-align: left;
    border-bottom: 1px solid var(--border);
  }}
  th {{ background: var(--card); color: var(--blue); font-weight: 600; position: sticky; top: 0; }}
  tr:hover {{ background: rgba(66, 153, 225, 0.08); }}
  .status-success {{ color: var(--green); font-weight: 600; }}
  .status-partial {{ color: var(--yellow); font-weight: 600; }}
  .status-failed {{ color: var(--red); font-weight: 600; }}
  .ewi-error {{ background: rgba(252, 129, 129, 0.08); }}
  .ewi-warning {{ background: rgba(236, 201, 75, 0.08); }}
  .ewi-info {{ background: rgba(118, 228, 247, 0.05); }}
  .bar {{ display: flex; height: 24px; border-radius: 4px; overflow: hidden; margin: 0.5rem 0; }}
  .bar-success {{ background: var(--green); }}
  .bar-partial {{ background: var(--yellow); }}
  .bar-failed {{ background: var(--red); }}
  .legend {{ display: flex; gap: 1.5rem; margin: 0.5rem 0; font-size: 0.85rem; color: var(--muted); }}
  .legend span::before {{ content: ''; display: inline-block; width: 12px; height: 12px; border-radius: 2px; margin-right: 0.4rem; vertical-align: middle; }}
  .legend .l-success::before {{ background: var(--green); }}
  .legend .l-partial::before {{ background: var(--yellow); }}
  .legend .l-failed::before {{ background: var(--red); }}
  footer {{ margin-top: 3rem; padding-top: 1rem; border-top: 1px solid var(--border); color: var(--muted); font-size: 0.8rem; text-align: center; }}
</style>
</head>
<body>

<h1>{e(report.title)}</h1>
<p class="subtitle">Generated: {e(report.generated_at)} &mdash; Powered by Snowflake Cortex LLM</p>

<!-- ===== Executive Summary ===== -->
<h2>Executive Summary</h2>
<div class="cards">
  <div class="card"><div class="value">{total}</div><div class="label">Total Mappings</div></div>
  <div class="card success"><div class="value">{success}</div><div class="label">Successful</div></div>
  <div class="card warning"><div class="value">{partial}</div><div class="label">Partial</div></div>
  <div class="card error"><div class="value">{failed}</div><div class="label">Failed</div></div>
  <div class="card"><div class="value">{rate:.0f}%</div><div class="label">Success Rate</div></div>
  <div class="card"><div class="value">{avg_q:.0f}</div><div class="label">Avg Quality</div></div>
  <div class="card"><div class="value">{total_files}</div><div class="label">Files Generated</div></div>
  <div class="card"><div class="value">{total_llm}</div><div class="label">LLM Calls</div></div>
</div>

<!-- Conversion status bar -->
<div class="bar">
  {"<div class='bar-success' style='flex:" + str(success) + "'></div>" if success else ""}
  {"<div class='bar-partial' style='flex:" + str(partial) + "'></div>" if partial else ""}
  {"<div class='bar-failed' style='flex:" + str(failed) + "'></div>" if failed else ""}
</div>
<div class="legend">
  <span class="l-success">Success ({success})</span>
  <span class="l-partial">Partial ({partial})</span>
  <span class="l-failed">Failed ({failed})</span>
</div>

<!-- ===== EWI Summary ===== -->
<h2>EWI Summary</h2>
<div class="cards">
  <div class="card error"><div class="value">{len(report.errors)}</div><div class="label">Errors (E)</div></div>
  <div class="card warning"><div class="value">{len(report.warnings)}</div><div class="label">Warnings (W)</div></div>
  <div class="card"><div class="value">{len(report.infos)}</div><div class="label">Informational (I)</div></div>
</div>

<!-- ===== Mapping Detail ===== -->
<h2>Mapping Detail</h2>
<div style="overflow-x:auto;">
<table>
<thead>
<tr>
  <th>Mapping</th><th>Workflow</th><th>Complexity</th><th>Strategy</th>
  <th>Status</th><th>Files</th><th>Errors</th><th>Warnings</th>
  <th>Quality</th><th>LLM Calls</th><th>Heal Rounds</th><th>Time</th>
</tr>
</thead>
<tbody>
{mapping_rows}
</tbody>
</table>
</div>

<!-- ===== EWI Detail ===== -->
<h2>EWI Detail</h2>
<div style="overflow-x:auto;">
<table>
<thead>
<tr><th>Code</th><th>Level</th><th>Mapping</th><th>Message</th><th>Detail</th></tr>
</thead>
<tbody>
{ewi_rows}
</tbody>
</table>
</div>

<!-- ===== Performance ===== -->
<h2>Performance</h2>
<div class="cards">
  <div class="card"><div class="value">{total_time:.1f}s</div><div class="label">Total Time</div></div>
  <div class="card"><div class="value">{total_llm}</div><div class="label">LLM Calls</div></div>
  <div class="card"><div class="value">{rm.total_heal_attempts if rm else 0}</div><div class="label">Heal Rounds</div></div>
  <div class="card"><div class="value">{rm.estimated_total_tokens if rm else 0:,}</div><div class="label">Est. Tokens</div></div>
</div>

<footer>
  infa2dbt &mdash; Informatica PowerCenter to dbt Migration Framework
  &mdash; Powered by Snowflake Cortex LLM
</footer>

</body>
</html>"""
