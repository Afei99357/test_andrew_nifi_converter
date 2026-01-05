"""
Stub converter for unsupported NiFi processors.

This module provides a fallback converter that generates stub implementations
for processors that don't have dedicated converters yet.
"""

from typing import List, Optional
import re

from nifi2py.models import Processor, ConversionResult
from nifi2py.converters.base import ProcessorConverter, register_converter


@register_converter
class StubConverter(ProcessorConverter):
    """
    Fallback converter for unsupported processors.

    This converter generates stub implementations with TODO comments
    and migration hints for processors that don't have dedicated converters.
    It's registered with processor_types = ["*"] to catch all unhandled types.
    """

    processor_types = ["*"]  # Matches all processor types

    def convert(self, processor: Processor) -> ConversionResult:
        """
        Generate a stub implementation for an unsupported processor.

        Creates a Python function stub with:
        - TODO comments
        - Original processor configuration as comments
        - Migration hints based on processor type detection
        - NotImplementedError exception

        Args:
            processor: The processor to generate a stub for

        Returns:
            ConversionResult marked as a stub
        """
        migration_hints = self._detect_migration_hints(processor)
        notes = self._generate_notes(processor)

        return self.create_stub_result(
            processor=processor,
            notes=notes,
            migration_hints=migration_hints
        )

    def _detect_migration_hints(self, processor: Processor) -> List[str]:
        """
        Detect migration hints based on processor type and configuration.

        Analyzes the processor type and properties to suggest migration paths.

        Args:
            processor: The processor to analyze

        Returns:
            List of migration hint strings
        """
        hints = []
        proc_type = processor.type.lower()
        simple_type = processor.processor_simple_type

        # Detect Impala/Hive patterns
        if any(keyword in proc_type for keyword in ['impala', 'hive', 'sql']):
            hints.append("Consider migrating SQL queries to Databricks using spark.sql()")
            hints.append("Check if query uses Impala-specific syntax that needs adjustment")

        # Detect ExecuteStreamCommand patterns
        if 'executestreamcommand' in proc_type or 'executeprocess' in proc_type:
            command_path = processor.get_property('Command Path') or ''
            command_args = processor.get_property('Command Arguments') or ''

            if 'impala' in command_path.lower():
                hints.append("Detected Impala shell command")
                hints.append("Migrate to: spark.sql(query) in Databricks")
            elif 'python' in command_path.lower():
                hints.append("Detected Python script execution")
                hints.append("Consider importing Python script as module")
            elif 'bash' in command_path.lower() or 'sh' in command_path.lower():
                hints.append("Detected shell script execution")
                hints.append("Consider using subprocess module or rewriting in Python")
            else:
                hints.append(f"Command: {command_path} {command_args}")
                hints.append("Review if command can be replaced with Python equivalent")

        # Detect HDFS patterns
        if 'hdfs' in proc_type:
            hints.append("Detected HDFS operation")
            hints.append("Migrate to: dbutils.fs operations in Databricks")

        # Detect SFTP/FTP patterns
        if 'sftp' in proc_type or 'ftp' in proc_type:
            hints.append("Detected file transfer operation")
            hints.append("Consider using: paramiko library for SFTP")

        # Detect Wait/Notify patterns
        if 'wait' in proc_type or 'notify' in proc_type:
            hints.append("Detected state management processor")
            hints.append("Consider using: explicit state tracking with database or cache")

        # Detect ControlRate patterns
        if 'controlrate' in proc_type:
            hints.append("Detected rate limiting processor")
            hints.append("May not be needed in batch processing")
            hints.append("Consider using: time.sleep() or scheduler configuration")

        # Detect SplitContent patterns
        if 'split' in proc_type:
            hints.append("Detected content splitting operation")
            hints.append("Review split logic and implement using Python string/bytes operations")

        # Detect ExtractText patterns
        if 'extract' in proc_type and 'text' in proc_type:
            hints.append("Detected text extraction with regex")
            hints.append("Migrate regex patterns to: re.search() or re.findall()")

        # Detect ReplaceText patterns
        if 'replace' in proc_type and 'text' in proc_type:
            hints.append("Detected text replacement operation")
            hints.append("Migrate to: str.replace() or re.sub()")

        # Generic hint if no specific patterns detected
        if not hints:
            hints.append(f"Review {simple_type} processor documentation")
            hints.append("Implement equivalent logic in Python")

        return hints

    def _generate_notes(self, processor: Processor) -> str:
        """
        Generate notes about why this processor is stubbed.

        Args:
            processor: The processor to generate notes for

        Returns:
            Note string
        """
        simple_type = processor.processor_simple_type

        # Count properties that are set
        set_properties = sum(1 for v in processor.properties.values() if v)

        notes = (
            f"No converter available for {simple_type}. "
            f"Processor has {set_properties} configured properties. "
            f"Manual implementation required."
        )

        return notes
