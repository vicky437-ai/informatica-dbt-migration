"""Click CLI for the Informatica-to-dbt converter (``infa2dbt``).

This is the primary user-facing entry point.  It provides a command-line
interface modelled after SnowConvert AI's workflow:

    infa2dbt convert --input ./xmls --output ./dbt_project
    infa2dbt convert --input ./xmls --output ./existing_project --mode merge
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import click

from informatica_to_dbt import __version__

logger = logging.getLogger("informatica_dbt")


@click.group()
@click.version_option(version=__version__, prog_name="infa2dbt")
def main() -> None:
    """Informatica PowerCenter XML to dbt project converter for Snowflake.

    An LLM-powered framework that converts Informatica ETL workflows into
    production-ready dbt projects, deployable to Snowflake's native dbt runtime.
    """


@main.command()
@click.option(
    "--input", "-i", "input_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to an Informatica XML file or directory of XML files.",
)
@click.option(
    "--output", "-o", "output_path",
    default="./dbt_output",
    type=click.Path(),
    help="Target dbt project directory (created if it doesn't exist).",
)
@click.option(
    "--mode", "-m",
    type=click.Choice(["new", "merge"], case_sensitive=False),
    default="new",
    help="'new' creates a fresh project; 'merge' adds into an existing project.",
)
@click.option(
    "--project-name", "-n",
    default=None,
    help="dbt project name (defaults to 'informatica_dbt').",
)
@click.option(
    "--model", "llm_model",
    default="claude-4-sonnet",
    help="Snowflake Cortex LLM model for code generation.",
)
@click.option(
    "--connection",
    default=None,
    help="Snowflake connection name (from ~/.snowflake/connections.toml).",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    default="INFO",
    help="Logging verbosity.",
)
@click.option(
    "--no-cache",
    is_flag=True,
    default=False,
    help="Disable output caching (always regenerate).",
)
@click.option(
    "--clear-cache",
    is_flag=True,
    default=False,
    help="Clear all cached conversions before running.",
)
@click.option(
    "--source-schema",
    default=None,
    help="Override source schema in all _sources.yml (e.g. 'MOCK_SOURCES').",
)
def convert(
    input_path: str,
    output_path: str,
    mode: str,
    project_name: str | None,
    llm_model: str,
    connection: str | None,
    log_level: str,
    no_cache: bool,
    clear_cache: bool,
    source_schema: str | None,
) -> None:
    """Convert Informatica PowerCenter XML exports to a dbt project.

    Parses XML workflows/mappings, uses Snowflake Cortex LLM to generate
    dbt models, validates the output, and assembles a unified dbt project.

    \b
    Examples:
      # Convert a single XML file into a new dbt project
      infa2dbt convert -i workflow_export.xml -o ./my_dbt_project

      # Convert a directory of XML files
      infa2dbt convert -i ./xml_exports/ -o ./my_dbt_project

      # Merge into an existing dbt project
      infa2dbt convert -i ./new_xmls/ -o ./existing_project --mode merge
    """
    from informatica_to_dbt.config import Config
    from informatica_to_dbt.cache.conversion_cache import ConversionCache
    from informatica_to_dbt.generator.llm_client import LLMClient
    from informatica_to_dbt.merger.project_merger import ProjectMerger
    from informatica_to_dbt.notebook_entry import PipelineResult, format_report
    from informatica_to_dbt.orchestrator import convert_repository
    from informatica_to_dbt.persistence.snowflake_io import SnowflakeIO
    from informatica_to_dbt.progress import (
        CompositeProgressCallback,
        LoggingProgressCallback,
        PrintProgressCallback,
    )
    from informatica_to_dbt.metrics import RepositoryMetrics
    from informatica_to_dbt.utils import setup_logging
    from informatica_to_dbt.xml_parser.parser import InformaticaXMLParser

    # Setup logging
    setup_logging(log_level)

    click.echo(f"infa2dbt v{__version__} — Informatica to dbt Converter")
    click.echo(f"  Input:   {input_path}")
    click.echo(f"  Output:  {output_path}")
    click.echo(f"  Mode:    {mode}")
    click.echo(f"  Model:   {llm_model}")
    click.echo()

    # Build config
    config = Config(
        local_mode=True,
        llm_model=llm_model,
        log_level=log_level,
        project_dir=output_path,
        merge_mode=mode,
        dbt_project_name=project_name,
        cache_enabled=not no_cache,
        connection_name=connection,
        source_schema_override=source_schema,
    )
    errors = config.validate()
    if errors:
        for e in errors:
            click.echo(f"  Config error: {e}", err=True)
        raise SystemExit(1)

    # Initialize components
    parser = InformaticaXMLParser()
    llm_client = LLMClient(config)
    io = SnowflakeIO(config)

    click.echo(f"LLM client mode: {llm_client.mode}")

    progress = CompositeProgressCallback([
        LoggingProgressCallback(),
        PrintProgressCallback(),
    ])

    # Discover and parse XML files
    input_p = Path(input_path)
    xml_files: list[str] = []
    if input_p.is_file():
        xml_files = [str(input_p)]
    elif input_p.is_dir():
        xml_files = sorted(
            str(p) for p in input_p.glob("*.XML")
        ) + sorted(
            str(p) for p in input_p.glob("*.xml")
        )
        # Deduplicate (case-insensitive filesystems)
        seen: set[str] = set()
        unique: list[str] = []
        for f in xml_files:
            real = os.path.realpath(f)
            if real not in seen:
                seen.add(real)
                unique.append(f)
        xml_files = unique

    if not xml_files:
        click.echo("No XML files found at the input path.", err=True)
        raise SystemExit(1)

    click.echo(f"Found {len(xml_files)} XML file(s) to convert")
    click.echo()

    # Convert all XML files
    from informatica_to_dbt.orchestrator import MappingResult
    all_results: list[MappingResult] = []
    combined_metrics = RepositoryMetrics()

    # Initialize cache
    cache = ConversionCache(
        cache_dir=config.cache_dir,
        enabled=config.cache_enabled,
    )
    if clear_cache:
        cleared = cache.clear()
        click.echo(f"Cache: cleared {cleared} cached entries")
    if not config.cache_enabled:
        click.echo("Cache: disabled (--no-cache)")
    else:
        stats = cache.stats()
        click.echo(f"Cache: enabled ({stats['entries']} cached entries)")

    # Build extra context for LLM prompts
    extra_context_parts: list[str] = []
    if source_schema:
        extra_context_parts.append(
            f"IMPORTANT: All source tables reside in the schema '{source_schema}'. "
            f"In _sources.yml files, set 'schema: {source_schema}' for every source. "
            f"Do NOT use schema names from the Informatica XML (e.g. ORACLE_RAW, SQLSERVER_RAW)."
        )
    extra_context = "\n".join(extra_context_parts) if extra_context_parts else None

    for idx, fpath in enumerate(xml_files, 1):
        fname = os.path.basename(fpath)
        click.echo(f"{'='*60}")
        click.echo(f"[{idx}/{len(xml_files)}] {fname}")
        click.echo(f"{'='*60}")

        try:
            raw_xml = Path(fpath).read_text(encoding="utf-8")
            repo = parser.parse_file(fpath)
            results, repo_metrics = convert_repository(
                repo, config, llm_client, io, progress,
                cache=cache,
                xml_content=raw_xml,
                extra_context=extra_context,
            )
            all_results.extend(results)
            for mm in repo_metrics.mapping_metrics:
                combined_metrics.add(mm)
        except Exception as exc:
            logger.error("Failed to process '%s': %s", fname, exc)
            click.echo(f"  ERROR: {exc}", err=True)

    combined_metrics.total_seconds = sum(
        m.total_seconds for m in combined_metrics.mapping_metrics
    )

    # Merge into unified project
    click.echo()
    click.echo(f"{'='*60}")
    click.echo("MERGING INTO UNIFIED DBT PROJECT")
    click.echo(f"{'='*60}")

    merger = ProjectMerger(config)
    merge_result = merger.merge(all_results)

    click.echo(f"  Files written:         {merge_result.files_written}")
    click.echo(f"  Files skipped:         {merge_result.files_skipped}")
    click.echo(f"  Sources consolidated:  {merge_result.sources_consolidated}")
    click.echo(f"  Conflicts resolved:    {merge_result.conflicts_resolved}")
    if merge_result.warnings:
        for w in merge_result.warnings:
            click.echo(f"  WARNING: {w}")

    # Print conversion report
    click.echo()
    pipeline_result = PipelineResult(all_results, combined_metrics)
    click.echo(pipeline_result.report)

    # Generate EWI assessment report
    from informatica_to_dbt.reports.ewi_report import EWIReportGenerator
    ewi_gen = EWIReportGenerator()
    ewi_report = ewi_gen.generate(combined_metrics)
    report_dir = Path(output_path) / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    html_path = ewi_gen.write_html(ewi_report, str(report_dir / "ewi_assessment_report.html"))
    json_path = ewi_gen.write_json(ewi_report, str(report_dir / "ewi_assessment_report.json"))
    click.echo(f"EWI Report (HTML): {html_path}")
    click.echo(f"EWI Report (JSON): {json_path}")

    # Save metrics for standalone report command
    metrics_path = Path(output_path) / ".infa2dbt" / "last_metrics.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    import json as _json
    metrics_path.write_text(
        _json.dumps(combined_metrics.to_dict(), indent=2),
        encoding="utf-8",
    )

    # Final summary
    click.echo()
    success = sum(1 for r in all_results if r.status == "success")
    partial = sum(1 for r in all_results if r.status == "partial")
    failed = sum(1 for r in all_results if r.status == "failed")

    if failed > 0:
        click.echo(
            f"Completed with {failed} failure(s). "
            f"Check logs for details.",
            err=True,
        )
        raise SystemExit(1 if success == 0 else 0)

    click.echo(
        f"Project ready at: {os.path.abspath(output_path)}"
    )
    click.echo("Next steps:")
    click.echo("  1. cd " + os.path.abspath(output_path))
    click.echo("  2. dbt deps      # install packages")
    click.echo("  3. dbt compile   # validate the project")
    click.echo("  4. dbt run        # execute models")


@main.command()
@click.option(
    "--input", "-i", "input_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to an Informatica XML file or directory of XML files.",
)
@click.option(
    "--schema-source",
    type=click.Choice(["xml", "snowflake", "json"], case_sensitive=False),
    default="xml",
    help="Schema discovery mode: 'xml' from parsed XML, 'snowflake' from INFORMATION_SCHEMA, 'json' from file.",
)
@click.option(
    "--database",
    default=None,
    help="Snowflake database name (required for --schema-source=snowflake).",
)
@click.option(
    "--schema", "sf_schema",
    default=None,
    help="Snowflake schema name (required for --schema-source=snowflake).",
)
@click.option(
    "--json-path",
    default=None,
    type=click.Path(exists=True),
    help="Path to source_map.json (required for --schema-source=json).",
)
@click.option(
    "--output", "-o", "output_path",
    default=None,
    type=click.Path(),
    help="Save discovered schema to a JSON file.",
)
def discover(
    input_path: str,
    schema_source: str,
    database: str | None,
    sf_schema: str | None,
    json_path: str | None,
    output_path: str | None,
) -> None:
    """Discover and inventory Informatica XML files and source schemas.

    Scans a directory for Informatica PowerCenter XML files, reports metadata
    (mapping count, sources, targets), and optionally discovers source table
    schemas from Snowflake, the XML itself, or a JSON file.

    \b
    Examples:
      # Scan a directory and show inventory
      infa2dbt discover -i ./xml_exports/

      # Discover schemas from parsed XML and save to JSON
      infa2dbt discover -i ./xml_exports/ --schema-source xml -o source_map.json

      # Discover schemas from Snowflake
      infa2dbt discover -i ./xml_exports/ --schema-source snowflake \\
          --database MY_DB --schema RAW
    """
    from informatica_to_dbt.discovery.xml_inventory import XMLInventory
    from informatica_to_dbt.discovery.schema_discovery import SchemaDiscovery
    from informatica_to_dbt.xml_parser.parser import InformaticaXMLParser

    # --- XML Inventory ---
    click.echo("Scanning for Informatica XML files...")
    inventory = XMLInventory.scan(input_path)
    click.echo()
    click.echo(inventory.summary())
    click.echo()

    if not inventory.valid_files:
        click.echo("No valid Informatica XML files found.", err=True)
        raise SystemExit(1)

    for info in inventory.valid_files:
        click.echo(f"  {info.filename}")
        click.echo(f"    Repository:      {info.repository_name}")
        click.echo(f"    Folders:         {info.folder_count}")
        click.echo(f"    Mappings:        {info.mapping_count}")
        click.echo(f"    Sources:         {info.source_count}")
        click.echo(f"    Targets:         {info.target_count}")
        click.echo(f"    Transformations: {info.transformation_count}")
        if info.mapping_names:
            click.echo(f"    Mapping names:   {', '.join(info.mapping_names[:10])}")
            if len(info.mapping_names) > 10:
                click.echo(f"                     ... and {len(info.mapping_names) - 10} more")

    for info in inventory.invalid_files:
        click.echo(f"  {info.filename} — SKIPPED: {info.error}")

    # --- Schema Discovery ---
    discovery: SchemaDiscovery | None = None

    if schema_source == "xml":
        click.echo()
        click.echo("Discovering schemas from XML...")
        parser = InformaticaXMLParser()
        combined = SchemaDiscovery()
        for info in inventory.valid_files:
            repo = parser.parse_file(info.path)
            partial = SchemaDiscovery.from_xml_repository(repo)
            for ts in partial.tables.values():
                combined.add(ts)
        discovery = combined
        click.echo(f"  Discovered {discovery.table_count} table(s) from XML")

    elif schema_source == "snowflake":
        if not database or not sf_schema:
            click.echo(
                "ERROR: --database and --schema are required for --schema-source=snowflake",
                err=True,
            )
            raise SystemExit(1)
        click.echo()
        click.echo(f"Discovering schemas from Snowflake ({database}.{sf_schema})...")
        try:
            from snowflake.snowpark import Session
            session = Session.builder.getOrCreate()
            discovery = SchemaDiscovery.from_snowflake(session, database, sf_schema)
            click.echo(f"  Discovered {discovery.table_count} table(s) from Snowflake")
        except Exception as exc:
            click.echo(f"  ERROR: Snowflake connection failed: {exc}", err=True)
            raise SystemExit(1)

    elif schema_source == "json":
        if not json_path:
            click.echo(
                "ERROR: --json-path is required for --schema-source=json",
                err=True,
            )
            raise SystemExit(1)
        click.echo()
        click.echo(f"Loading schemas from {json_path}...")
        discovery = SchemaDiscovery.from_json(json_path)
        click.echo(f"  Loaded {discovery.table_count} table(s) from JSON")

    # Save if requested
    if discovery and output_path:
        discovery.save_json(output_path)
        click.echo(f"  Saved source map to {output_path}")


@main.group()
def cache() -> None:
    """Manage the conversion output cache."""


@cache.command("list")
@click.option(
    "--cache-dir",
    default=".infa2dbt/cache",
    help="Cache directory path.",
)
def cache_list(cache_dir: str) -> None:
    """List all cached conversion entries."""
    from informatica_to_dbt.cache.conversion_cache import ConversionCache

    c = ConversionCache(cache_dir=cache_dir)
    entries = c.list_entries()

    if not entries:
        click.echo("Cache is empty.")
        return

    click.echo(f"{'Key (short)':<14} {'Mapping':<30} {'Files':>5} {'Model':<20} {'Score':>6}")
    click.echo("-" * 80)
    for e in entries:
        score = f"{e.quality_score:.0f}" if e.quality_score is not None else "—"
        click.echo(
            f"{e.cache_key[:12]:<14} {e.mapping_name:<30} "
            f"{e.file_count:>5} {e.llm_model:<20} {score:>6}"
        )

    stats = c.stats()
    click.echo()
    click.echo(f"Total: {stats['entries']} entries, {stats['total_files']} files")


@cache.command("clear")
@click.option(
    "--cache-dir",
    default=".infa2dbt/cache",
    help="Cache directory path.",
)
@click.confirmation_option(prompt="Are you sure you want to clear the entire cache?")
def cache_clear(cache_dir: str) -> None:
    """Clear all cached conversion entries."""
    from informatica_to_dbt.cache.conversion_cache import ConversionCache

    c = ConversionCache(cache_dir=cache_dir)
    count = c.clear()
    click.echo(f"Cleared {count} cached entries.")


@cache.command("stats")
@click.option(
    "--cache-dir",
    default=".infa2dbt/cache",
    help="Cache directory path.",
)
def cache_stats(cache_dir: str) -> None:
    """Show cache statistics."""
    from informatica_to_dbt.cache.conversion_cache import ConversionCache

    c = ConversionCache(cache_dir=cache_dir)
    stats = c.stats()
    click.echo(f"Cache directory: {cache_dir}")
    click.echo(f"Entries:         {stats['entries']}")
    click.echo(f"Total files:     {stats['total_files']}")


@main.command()
@click.option(
    "--project", "-p", "project_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to the dbt project directory.",
)
@click.option(
    "--profiles-dir",
    default=None,
    type=click.Path(exists=True),
    help="Path to profiles.yml directory (defaults to project dir).",
)
@click.option(
    "--compile-only",
    is_flag=True,
    default=False,
    help="Only run 'dbt compile' (skip 'dbt run').",
)
@click.option(
    "--run-tests",
    is_flag=True,
    default=False,
    help="Also run 'dbt test' after 'dbt run'.",
)
@click.option(
    "--select", "-s",
    default=None,
    help="dbt model selection syntax (e.g. 'tag:my_mapping').",
)
@click.option(
    "--full-refresh",
    is_flag=True,
    default=False,
    help="Pass --full-refresh to dbt run.",
)
@click.option(
    "--install-deps",
    is_flag=True,
    default=False,
    help="Run 'dbt deps' before validation.",
)
@click.option(
    "--dbt-path",
    default="dbt",
    help="Path to dbt executable.",
)
def validate(
    project_path: str,
    profiles_dir: str | None,
    compile_only: bool,
    run_tests: bool,
    select: str | None,
    full_refresh: bool,
    install_deps: bool,
    dbt_path: str,
) -> None:
    """Validate a generated dbt project using dbt compile and dbt run.

    Runs the dbt project against the target Snowflake warehouse to verify
    that all models compile and (optionally) execute successfully.

    \b
    Examples:
      # Compile-only validation
      infa2dbt validate -p ./my_dbt_project --compile-only

      # Full validation (compile + run)
      infa2dbt validate -p ./my_dbt_project

      # Validate + run tests
      infa2dbt validate -p ./my_dbt_project --run-tests

      # Validate a specific mapping
      infa2dbt validate -p ./my_dbt_project -s tag:m_load_orders
    """
    from informatica_to_dbt.validation.dbt_validator import DbtValidator

    validator = DbtValidator(
        project_dir=project_path,
        profiles_dir=profiles_dir,
        dbt_executable=dbt_path,
    )

    # Quick structure check
    errors = validator.validate_project_structure()
    if errors:
        for e in errors:
            click.echo(f"  ERROR: {e}", err=True)
        raise SystemExit(1)

    click.echo(f"Validating dbt project: {os.path.abspath(project_path)}")
    click.echo()

    # Install deps
    if install_deps:
        click.echo("Installing dbt packages (dbt deps)...")
        deps_result = validator.deps()
        if not deps_result.success:
            click.echo(f"  dbt deps FAILED: {deps_result.stderr[:300]}", err=True)
            raise SystemExit(1)
        click.echo("  dbt deps: OK")
        click.echo()

    # Compile
    click.echo("Running dbt compile...")
    compile_result = validator.compile(select=select)
    click.echo(compile_result.summary())
    click.echo()

    if not compile_result.success:
        click.echo("dbt compile FAILED — fix errors before proceeding.", err=True)
        raise SystemExit(1)

    if compile_only:
        click.echo("Compile-only mode — skipping dbt run.")
        return

    # Run
    click.echo("Running dbt run...")
    run_result = validator.run(select=select, full_refresh=full_refresh)
    click.echo(run_result.summary())
    click.echo()

    if not run_result.success:
        click.echo("dbt run FAILED — check error details above.", err=True)
        raise SystemExit(1)

    # Test
    if run_tests:
        click.echo("Running dbt test...")
        test_result = validator.test(select=select)
        click.echo(test_result.summary())
        click.echo()

        if not test_result.success:
            click.echo("dbt test had failures — check details above.", err=True)
            raise SystemExit(1)

    click.echo("Validation PASSED.")


@main.command("git-push")
@click.option(
    "--project", "-p", "project_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to the dbt project directory.",
)
@click.option(
    "--remote",
    default="origin",
    help="Git remote name (default: origin).",
)
@click.option(
    "--branch", "-b",
    default=None,
    help="Branch name to push to (creates if needed).",
)
@click.option(
    "--remote-url",
    default=None,
    help="Git remote URL (added/updated if provided).",
)
@click.option(
    "--message", "-m",
    default=None,
    help="Commit message (auto-generated if not provided).",
)
def git_push(
    project_path: str,
    remote: str,
    branch: str | None,
    remote_url: str | None,
    message: str | None,
) -> None:
    """Commit and push the dbt project to a Git repository.

    Initializes a Git repo if needed, stages all changes, commits, and pushes
    to the specified remote and branch.

    \b
    Examples:
      # Push with auto-generated commit message
      infa2dbt git-push -p ./my_dbt_project --remote-url https://github.com/org/repo.git

      # Push to a feature branch
      infa2dbt git-push -p ./my_dbt_project -b feature/new-mapping

      # Push with custom message
      infa2dbt git-push -p ./my_dbt_project -m "Add new journals mapping"
    """
    from informatica_to_dbt.git.git_manager import GitManager
    import datetime

    git = GitManager(project_dir=project_path)

    if not message:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        message = f"infa2dbt: update dbt project ({timestamp})"

    click.echo(f"Git push: {os.path.abspath(project_path)}")
    click.echo(f"  Remote:  {remote}")
    click.echo(f"  Branch:  {branch or '(current)'}")
    click.echo(f"  Message: {message}")
    click.echo()

    results = git.full_commit_and_push(
        message=message,
        remote=remote,
        branch=branch,
        remote_url=remote_url,
    )

    for r in results:
        status = "OK" if r.success else "FAILED"
        click.echo(f"  {r.command}: {status}")
        if r.stdout.strip():
            for line in r.stdout.strip().splitlines()[:5]:
                click.echo(f"    {line}")
        if not r.success and r.stderr.strip():
            for line in r.stderr.strip().splitlines()[:5]:
                click.echo(f"    {line}", err=True)
            click.echo()
            click.echo("Git push FAILED — check errors above.", err=True)
            raise SystemExit(1)

    click.echo()
    click.echo("Git push completed successfully.")
    final_branch = git.current_branch()
    if final_branch:
        click.echo(f"  Branch: {final_branch}")


@main.command()
@click.option(
    "--project", "-p", "project_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to the local dbt project directory.",
)
@click.option(
    "--database", "-d",
    default="TPC_DI_RAW_DATA",
    help="Snowflake database for the dbt project object.",
)
@click.option(
    "--schema", "-s", "sf_schema",
    default="INFORMATICA_TO_DBT",
    help="Snowflake schema for the dbt project object.",
)
@click.option(
    "--project-name", "-n",
    default="INFORMATICA_TO_DBT",
    help="Name for the Snowflake dbt project object.",
)
@click.option(
    "--warehouse", "-w",
    default="SMALL_WH",
    help="Snowflake warehouse for execution.",
)
@click.option(
    "--connection",
    default=None,
    help="Snowflake connection name.",
)
@click.option(
    "--mode",
    type=click.Choice(["direct", "git", "schedule"], case_sensitive=False),
    default="direct",
    help="Deployment mode: 'direct' (snow dbt deploy), 'git' (from Git repo), 'schedule' (TASK).",
)
@click.option(
    "--git-url",
    default=None,
    help="Git repository HTTPS URL (required for --mode=git).",
)
@click.option(
    "--git-repo-name",
    default=None,
    help="Snowflake GIT REPOSITORY object name (for --mode=git).",
)
@click.option(
    "--git-branch",
    default="main",
    help="Git branch to deploy from (for --mode=git).",
)
@click.option(
    "--cron",
    default="0 2 * * *",
    help="Cron schedule (for --mode=schedule). Default: daily at 2 AM.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show generated SQL without executing (for git/schedule modes).",
)
def deploy(
    project_path: str,
    database: str,
    sf_schema: str,
    project_name: str,
    warehouse: str,
    connection: str | None,
    mode: str,
    git_url: str | None,
    git_repo_name: str | None,
    git_branch: str,
    cron: str,
    dry_run: bool,
) -> None:
    """Deploy the dbt project to Snowflake.

    Supports three deployment modes:

    \b
    - direct:   Uses 'snow dbt deploy' to push the project directly
    - git:      Creates a Snowflake Git Repository and deploys from it
    - schedule: Creates a Snowflake TASK to run the project on a cron schedule

    \b
    Examples:
      # Direct deployment
      infa2dbt deploy -p ./my_dbt_project -d MY_DB -s MY_SCHEMA

      # Git-based deployment
      infa2dbt deploy -p ./my_dbt_project --mode git \\
          --git-url https://github.com/org/repo.git \\
          --git-repo-name my_git_repo

      # Schedule (daily at 2 AM)
      infa2dbt deploy -p ./my_dbt_project --mode schedule --cron "0 2 * * *"

      # Dry-run to preview SQL
      infa2dbt deploy -p ./my_dbt_project --mode git --dry-run \\
          --git-url https://github.com/org/repo.git --git-repo-name my_repo
    """
    from informatica_to_dbt.deployment.deployer import Deployer

    deployer = Deployer(
        project_dir=project_path,
        database=database,
        schema=sf_schema,
        project_name=project_name,
        warehouse=warehouse,
        connection=connection,
    )

    click.echo(f"Deploy: {os.path.abspath(project_path)}")
    click.echo(f"  Target:  {deployer.project_fqn}")
    click.echo(f"  Mode:    {mode}")
    click.echo()

    if mode == "direct":
        if dry_run:
            click.echo("Dry-run (direct mode):")
            click.echo(f"  snow dbt deploy --source {project_path} "
                       f"--database {database} --schema {sf_schema} {project_name}")
            return

        result = deployer.deploy_direct()
        click.echo(result.summary())
        if not result.success:
            raise SystemExit(1)

    elif mode == "git":
        if not git_url or not git_repo_name:
            click.echo(
                "ERROR: --git-url and --git-repo-name are required for --mode=git",
                err=True,
            )
            raise SystemExit(1)

        if dry_run:
            stmts = deployer.generate_git_deploy_sql(
                git_repo_name=git_repo_name,
                git_url=git_url,
                git_branch=git_branch,
            )
            click.echo("Dry-run (git mode) — SQL statements:")
            click.echo()
            for stmt in stmts:
                click.echo(stmt)
                click.echo()
            return

        result = deployer.deploy_from_git(
            git_repo_name=git_repo_name,
            git_url=git_url,
            git_branch=git_branch,
        )
        click.echo(result.summary())
        if not result.success:
            raise SystemExit(1)

    elif mode == "schedule":
        if dry_run:
            stmts = deployer.generate_schedule_sql(cron=cron)
            click.echo("Dry-run (schedule mode) — SQL statements:")
            click.echo()
            for stmt in stmts:
                click.echo(stmt)
                click.echo()
            return

        result = deployer.deploy_with_schedule(cron=cron)
        click.echo(result.summary())
        if not result.success:
            raise SystemExit(1)

    click.echo()
    click.echo("Deployment completed successfully.")


@main.command()
@click.option(
    "--metrics-file", "-m",
    type=click.Path(exists=True),
    default=None,
    help="Path to a metrics JSON file (produced by 'convert'). "
         "Defaults to <project-dir>/.infa2dbt/last_metrics.json.",
)
@click.option(
    "--project-dir", "-p",
    type=click.Path(exists=True),
    default="dbt_project",
    help="dbt project directory (used to locate default metrics file).",
)
@click.option(
    "--output", "-o",
    type=click.Path(),
    default=None,
    help="Output directory for reports. Defaults to <project-dir>/reports.",
)
@click.option(
    "--format", "-f", "fmt",
    type=click.Choice(["html", "json", "both"], case_sensitive=False),
    default="both",
    help="Report format to generate.",
)
def report(metrics_file: str, project_dir: str, output: str, fmt: str) -> None:
    """Generate an EWI assessment report from saved conversion metrics."""
    import json as _json
    from informatica_to_dbt.metrics import RepositoryMetrics
    from informatica_to_dbt.reports.ewi_report import EWIReportGenerator

    project_path = Path(project_dir)

    # Resolve metrics file
    if metrics_file is None:
        default_path = project_path / ".infa2dbt" / "last_metrics.json"
        if not default_path.exists():
            click.echo(
                f"No metrics file found at {default_path}. "
                "Run 'infa2dbt convert' first, or pass --metrics-file.",
                err=True,
            )
            raise SystemExit(1)
        metrics_file = str(default_path)

    click.echo(f"Loading metrics from: {metrics_file}")
    raw = _json.loads(Path(metrics_file).read_text(encoding="utf-8"))
    repo_metrics = RepositoryMetrics.from_dict(raw)
    click.echo(f"  Mappings: {repo_metrics.total_mappings}")

    # Resolve output directory
    report_dir = Path(output) if output else project_path / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    gen = EWIReportGenerator()
    ewi_report = gen.generate(repo_metrics)

    if fmt in ("html", "both"):
        html_path = gen.write_html(ewi_report, str(report_dir / "ewi_assessment_report.html"))
        click.echo(f"EWI Report (HTML): {html_path}")
    if fmt in ("json", "both"):
        json_path = gen.write_json(ewi_report, str(report_dir / "ewi_assessment_report.json"))
        click.echo(f"EWI Report (JSON): {json_path}")

    click.echo()
    click.echo(
        f"Report complete — "
        f"{len(ewi_report.errors)} error(s), "
        f"{len(ewi_report.warnings)} warning(s), "
        f"{len(ewi_report.infos)} info(s)"
    )


@main.command()
def version() -> None:
    """Show version information."""
    click.echo(f"infa2dbt v{__version__}")
    click.echo("Informatica PowerCenter XML to dbt project converter")
    click.echo("Powered by Snowflake Cortex LLM")


if __name__ == "__main__":
    main()
