#!/usr/bin/env python3
"""Automatically discover and validate settings documentation.

This script:
1. Syncs the built-in settings_doc template to avoid recursion
2. Discovers all Settings classes across the DiracX codebase
3. Checks which classes are documented in templates
4. Warns about undocumented classes
5. Generates documentation for all templates
6. Generates .env.example file with all settings
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import settings_doc
from jinja2 import Environment, FileSystemLoader, select_autoescape
from pydantic_settings import BaseSettings
from settings_doc import OutputFormat, importing, render
from settings_doc.main import _model_fields
from settings_doc.template_functions import JINJA_ENV_GLOBALS

from diracx.core.settings import ServiceSettingsBase


def sync_builtin_template(docs_dir: Path) -> None:
    """Sync the built-in settings_doc markdown template.

    This copies the built-in template to '_builtin_markdown.jinja' so our custom
    'markdown.jinja' can include it without causing recursion issues in Jinja2.

    Args:
        docs_dir: The docs directory where templates are stored

    """
    # Get the path to the built-in settings_doc template
    builtin_template_dir = Path(settings_doc.__file__).parent / "templates"
    builtin_markdown = builtin_template_dir / "markdown.jinja"

    # Define our custom templates directory
    custom_template_dir = docs_dir / "templates"
    custom_template_dir.mkdir(parents=True, exist_ok=True)

    # Copy the built-in template with a different name to avoid recursion
    target_template = custom_template_dir / "_builtin_markdown.jinja"
    shutil.copy2(builtin_markdown, target_template)


def discover_all_settings_classes() -> dict[str, dict[str, Any]]:
    """Automatically discover all Settings classes in the DiracX codebase.

    Uses pkgutil.walk_packages() to walk all diracx.* packages.

    Returns:
        Dict mapping class names to their info (module, class object, etc.)

    """
    settings_classes = {}

    import diracx

    # Use pkgutil to walk all packages and subpackages
    # This is the canonical way to discover modules in a package
    if hasattr(diracx, "__path__"):
        for _, module_name, _ in pkgutil.walk_packages(
            path=diracx.__path__,
            prefix="diracx.",
            onerror=lambda x: None,  # Silently skip modules with import errors
        ):
            try:
                module = importlib.import_module(module_name)

                # Inspect the module for Settings classes
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    # Check if it's a subclass of ServiceSettingsBase
                    if (
                        issubclass(obj, ServiceSettingsBase)
                        and obj is not ServiceSettingsBase
                        and obj is not BaseSettings
                        # Only include classes defined in this module (not imported)
                        and obj.__module__ == module_name
                    ):
                        settings_classes[name] = {
                            "module": module_name,
                            "class": obj,
                            "file": inspect.getfile(obj),
                        }
            except (ImportError, AttributeError, TypeError):
                # Skip modules that can't be imported or inspected
                continue
    else:
        raise ImportError("Cannot find diracx package paths")

    return settings_classes


def discover_templates(docs_dir: Path) -> dict[str, Path]:
    """Discover all Jinja2 template files in the docs directory.

    Returns:
        Dict mapping template names to their paths

    """
    templates = {}

    # Look for .j2 and .jinja files
    for pattern in ["**/*.j2", "**/*.jinja"]:
        for template_file in docs_dir.glob(pattern):
            # Skip the helper templates (those starting with _)
            if template_file.name.startswith("_"):
                continue

            # Use relative path from docs dir as the key
            rel_path = template_file.relative_to(docs_dir)
            templates[str(rel_path)] = template_file

    return templates


def extract_documented_classes(template_file: Path) -> set[str]:
    """Extract which Settings classes are documented in a template.

    Looks for {{ render_class('ClassName') }} patterns.
    """
    content = template_file.read_text()

    # Pattern to match render_class('ClassName') or render_class("ClassName")
    pattern = r"render_class\(['\"](\w+)['\"]\)"

    matches = re.findall(pattern, content)
    return set(matches)


def validate_documentation(
    settings_classes: dict[str, dict],
    templates: dict[str, Path],
) -> tuple[bool, list[str]]:
    """Validate that all Settings classes are documented.

    Returns:
        Tuple of (all_documented, warnings)

    """
    warnings = []
    all_documented = True

    # Track which classes are documented in which templates
    class_to_templates: dict[str, list[str]] = {}

    # Get all documented classes across all templates
    for template_name, template_file in templates.items():
        documented = extract_documented_classes(template_file)

        if documented:
            print(f"📄 {template_name}: documents {len(documented)} classes")
            for cls_name in sorted(documented):
                # Track which template documents this class
                if cls_name not in class_to_templates:
                    class_to_templates[cls_name] = []
                class_to_templates[cls_name].append(template_name)

                # Check if class exists
                if cls_name not in settings_classes:
                    warnings.append(
                        f"  ⚠️  Template '{template_name}' references unknown class: {cls_name}"
                    )

    # Check for classes documented in multiple templates
    for cls_name, template_list in class_to_templates.items():
        if len(template_list) > 1:
            warnings.append(
                f"\n⚠️  Class '{cls_name}' is documented in multiple templates:"
            )
            for tmpl in template_list:
                warnings.append(f"    - {tmpl}")
            warnings.append(
                "   This might be intentional, but verify it's not a mistake."
            )

    # Get all documented classes (from any template)
    all_documented_classes = set(class_to_templates.keys())

    # Check for undocumented classes
    undocumented = set(settings_classes.keys()) - all_documented_classes

    # Exclude ServiceSettingsBase as it's the base class
    undocumented.discard("ServiceSettingsBase")

    if undocumented:
        all_documented = False
        warnings.append("\n❌ Undocumented Settings classes found:")
        for cls_name in sorted(undocumented):
            info = settings_classes[cls_name]
            warnings.append(f"  - {cls_name} (in {info['module']})")
            warnings.append(
                f"    Add to template: {{{{ render_class('{cls_name}') }}}}"
            )

    return all_documented, warnings


def generate_all_templates(
    templates: dict[str, Path],
    settings_classes: dict[str, dict],
    docs_dir: Path,
    repo_root: Path,
) -> list:
    """Generate documentation for all discovered templates."""
    # Collect all unique modules that contain settings
    modules = sorted(set(info["module"] for info in settings_classes.values()))

    # Import settings classes from all modules
    settings = importing.import_module_path(tuple(modules))

    if not settings:
        raise ValueError(f"No settings classes found in {modules}")

    # Prepare data structures for templates
    fields = list(
        (env_name, field) for cls in settings for env_name, field in _model_fields(cls)
    )

    classes = {cls: list(cls.model_fields.values()) for cls in settings}

    # Set up Jinja2 environment
    env = Environment(
        loader=FileSystemLoader([str(docs_dir), str(docs_dir / "templates")]),
        autoescape=select_autoescape(),
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )

    env.globals.update(JINJA_ENV_GLOBALS)
    env.globals.update(
        {
            "heading_offset": 2,
            "fields": fields,
            "classes": classes,
            "all_classes": settings,
        }
    )

    # Generate each template to .bak file
    generated_files = []
    for template_name, template_file in templates.items():
        template = env.get_template(str(template_name))
        rendered = template.render().strip() + "\n"

        # Output file is the template file without the .j2/.jinja extension
        if template_file.suffix == ".j2":
            output_file = template_file.with_suffix("")
        elif template_file.suffix == ".jinja":
            output_file = template_file.with_name(template_file.stem + ".md")
        else:
            output_file = template_file.with_suffix(".md")

        # Write to .bak file
        backup_file = output_file.with_suffix(output_file.suffix + ".bak")
        backup_file.write_text(rendered)
        generated_files.append((output_file, backup_file))

    return generated_files


def generate_dotenv_file(
    settings_classes: dict[str, dict[str, Any]],
    output_path: Path,
) -> Path:
    """Generate a .env file with all Settings classes.

    Args:
        settings_classes: Dictionary of discovered Settings classes
        output_path: Path where the .env file should be written

    Returns:
        Path to the .bak file that was created

    """
    dotenv_sections = []

    # Generate .env content for each Settings class
    for class_name in sorted(settings_classes.keys()):
        info = settings_classes[class_name]

        # Use settings_doc.render to generate DOTENV format
        # class_path should be in format "module.ClassName"
        class_path = f"{info['module']}.{class_name}"
        dotenv_content = render(
            output_format=OutputFormat.DOTENV,
            class_path=(class_path,),  # Must be a tuple
        )

        # Add a header comment for the class
        section = f"# {class_name} (from {info['module']})\n{dotenv_content}\n"
        dotenv_sections.append(section)

    # Combine all sections
    full_content = "# Auto-generated .env file with all DiracX settings\n"
    full_content += "# This file contains all available environment variables.\n"
    full_content += "# Uncomment and set the values you need.\n\n"
    full_content += "\n".join(dotenv_sections)

    # Write to .bak file
    backup_path = output_path.with_suffix(output_path.suffix + ".bak")
    backup_path.write_text(full_content)

    return backup_path


def compare_and_update_files(
    generated_files: list[tuple[Path, Path]], repo_root: Path
) -> bool:
    """Compare generated .bak files with originals and update if needed.

    Args:
        generated_files: List of (original_path, backup_path) tuples
        repo_root: Repository root for relative path display

    Returns:
        True if all files were already up to date, False if any were updated

    """

    # Helper to normalize content (remove empty lines for comparison)
    def normalize(text: str) -> str:
        return "\n".join(line for line in text.splitlines() if line.strip())

    # First, run mdformat on all .bak files to ensure consistent formatting
    backup_files = [backup for _, backup in generated_files]
    if backup_files:
        try:
            subprocess.run(  # noqa: S603
                ["mdformat", "--number"] + [str(f) for f in backup_files],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"⚠️  mdformat failed: {e.stderr}")
            return False
        except FileNotFoundError:
            print("⚠️  mdformat not found in environment")
            return False

    all_up_to_date = True

    for original_file, backup_file in generated_files:
        if original_file.exists():
            original_content = normalize(original_file.read_text())
            backup_content = normalize(backup_file.read_text())

            if original_content != backup_content:
                # Replace original with backup
                backup_file.replace(original_file)
                print(f"✓ Updated {original_file.relative_to(repo_root)}")
                all_up_to_date = False
            else:
                # Files match, remove backup
                backup_file.unlink()
                print(f"  No changes: {original_file.relative_to(repo_root)}")
        else:
            # New file - move backup to original
            backup_file.replace(original_file)
            print(f"✓ Created {original_file.relative_to(repo_root)}")
            all_up_to_date = False

    return all_up_to_date


def main():
    """Generate settings: main entry point."""
    repo_root = Path(__file__).parent.parent
    docs_dir = repo_root / "docs"

    print("🔄 Syncing built-in template...")
    sync_builtin_template(docs_dir)

    print("🔍 Discovering Settings classes...")
    settings_classes = discover_all_settings_classes()

    print(f"✓ Found {len(settings_classes)} Settings classes:")
    for name, info in sorted(settings_classes.items()):
        print(f"  - {name} ({info['module']})")

    print("\n🔍 Discovering documentation templates...")
    templates = discover_templates(docs_dir)

    print(f"✓ Found {len(templates)} template(s):")
    for name in sorted(templates.keys()):
        print(f"  - {name}")

    print("\n🔍 Validating documentation coverage...")
    all_documented, warnings = validate_documentation(settings_classes, templates)

    if warnings:
        for warning in warnings:
            print(warning)

    if not all_documented:
        print("\n❌ Documentation is incomplete!")
        print("   Add missing classes to your templates.")
        return 1

    print("\n✓ All Settings classes are documented!")

    # Generate documentation (always to .bak files first)
    print("\n📝 Generating documentation from templates...")
    generated_files = generate_all_templates(
        templates, settings_classes, docs_dir, repo_root
    )

    # print("\n📝 Generating .env file...")
    # dotenv_path = repo_root / ".env.example"
    # dotenv_backup = generate_dotenv_file(settings_classes, dotenv_path)
    # generated_files.append((dotenv_path, dotenv_backup))

    # Update files if they're different
    print("\n📝 Updating files...")
    all_up_to_date = compare_and_update_files(generated_files, repo_root)

    if all_up_to_date:
        print("\n✅ All files were already up to date!")
    else:
        print("\n✅ Documentation updated successfully!")

    return 0


if __name__ == "__main__":
    sys.exit(main())
