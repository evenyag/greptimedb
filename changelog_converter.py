#!/usr/bin/env python3
"""
Changelog Converter

This script converts GitHub-generated changelog files to the format defined in cliff.toml.
It parses GitHub changelog entries and reformats them according to the cliff.toml template.
"""

import re
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class ChangelogEntry:
    """Represents a single changelog entry"""
    title: str
    author: str
    pr_number: int
    category: str
    is_breaking: bool = False


@dataclass
class Contributor:
    """Represents a contributor"""
    username: str
    pr_number: Optional[int] = None
    is_first_time: bool = False


class ChangelogConverter:
    """Converts GitHub changelog to cliff.toml format"""
    
    # Commit type mapping based on cliff.toml configuration
    COMMIT_CATEGORIES = {
        'feat': '🚀 Features',
        'fix': '🐛 Bug Fixes', 
        'doc': '📚 Documentation',
        'perf': '⚡ Performance',
        'refactor': '🚜 Refactor',
        'style': '🎨 Styling',
        'test': '🧪 Testing',
        'chore': '⚙️ Miscellaneous Tasks',
        'ci': '⚙️ Miscellaneous Tasks',
        'revert': '◀️ Revert',
        'security': '🛡️ Security'
    }
    
    def __init__(self, version: str, release_date: Optional[str] = None):
        self.version = version
        self.release_date = release_date or datetime.now().strftime("%B %d, %Y")
        self.entries: List[ChangelogEntry] = []
        self.contributors: Set[str] = set()
        self.new_contributors: List[Contributor] = []
        
    def parse_github_changelog(self, changelog_path: Path) -> None:
        """Parse GitHub changelog file"""
        content = changelog_path.read_text(encoding='utf-8')
        
        # Parse changelog entries
        entry_pattern = r'\* (.+?) by @(\w+) in https://github\.com/[^/]+/[^/]+/pull/(\d+)'
        entries = re.findall(entry_pattern, content)
        
        for title, author, pr_number in entries:
            category = self._categorize_entry(title)
            is_breaking = self._is_breaking_change(title)
            
            entry = ChangelogEntry(
                title=title,
                author=author,
                pr_number=int(pr_number),
                category=category,
                is_breaking=is_breaking
            )
            self.entries.append(entry)
            self.contributors.add(author)
            
        # Parse new contributors section
        new_contributors_section = re.search(r'## New Contributors\n(.*?)(?=\n##|\Z)', content, re.DOTALL)
        if new_contributors_section:
            new_contributor_pattern = r'\* @(\w+) made their first contribution in https://github\.com/[^/]+/[^/]+/pull/(\d+)'
            new_contributors = re.findall(new_contributor_pattern, new_contributors_section.group(1))
            
            for username, pr_number in new_contributors:
                contributor = Contributor(
                    username=username,
                    pr_number=int(pr_number),
                    is_first_time=True
                )
                self.new_contributors.append(contributor)
    
    def _categorize_entry(self, title: str) -> str:
        """Categorize entry based on title prefix"""
        title_lower = title.lower()
        
        for prefix, category in self.COMMIT_CATEGORIES.items():
            if title_lower.startswith(f"{prefix}:") or title_lower.startswith(f"{prefix}("):
                return category
                
        # Default category for entries that don't match patterns
        return "⚙️ Miscellaneous Tasks"
    
    def _is_breaking_change(self, title: str) -> bool:
        """Check if entry represents a breaking change"""
        breaking_indicators = ['breaking', 'break', 'BREAKING', 'BREAK', '!:']
        return any(indicator in title for indicator in breaking_indicators)
    
    def generate_cliff_changelog(self) -> str:
        """Generate changelog in cliff.toml format"""
        output = []
        
        # Header
        output.append(f"# {self.version}")
        output.append("")
        output.append(f"Release date: {self.release_date}")
        output.append("")
        
        # Breaking changes section
        breaking_changes = [entry for entry in self.entries if entry.is_breaking]
        if breaking_changes:
            output.append("## Breaking changes")
            for entry in breaking_changes:
                output.append(f"  * {entry.title} by [@{entry.author}](https://github.com/{entry.author}) in [#{entry.pr_number}](https://github.com/GreptimeTeam/greptimedb/pull/{entry.pr_number})")
            output.append("")
        
        # Group non-breaking changes by category
        non_breaking = [entry for entry in self.entries if not entry.is_breaking]
        grouped_entries = defaultdict(list)
        
        for entry in non_breaking:
            grouped_entries[entry.category].append(entry)
        
        # Sort categories by the order defined in cliff.toml
        category_order = [
            '🚀 Features',
            '🐛 Bug Fixes', 
            '🚜 Refactor',
            '📚 Documentation',
            '⚡ Performance',
            '🎨 Styling',
            '🧪 Testing',
            '⚙️ Miscellaneous Tasks',
            '🛡️ Security',
            '◀️ Revert'
        ]
        
        for category in category_order:
            if category in grouped_entries:
                output.append(f"### {category}")
                for entry in grouped_entries[category]:
                    output.append(f"    * {entry.title} by [@{entry.author}](https://github.com/{entry.author}) in [#{entry.pr_number}](https://github.com/GreptimeTeam/greptimedb/pull/{entry.pr_number})")
                output.append("")
        
        # New contributors section
        if self.new_contributors:
            output.append("## New Contributors")
            for contributor in self.new_contributors:
                output.append(f"  * [@{contributor.username}](https://github.com/{contributor.username}) made their first contribution in [#{contributor.pr_number}](https://github.com/GreptimeTeam/greptimedb/pull/{contributor.pr_number})")
            output.append("")
        
        # All contributors section
        if self.contributors:
            output.append("## All Contributors")
            output.append("")
            output.append("We would like to thank the following contributors from the GreptimeDB community:")
            output.append("")
            
            # Sort contributors alphabetically and format
            sorted_contributors = sorted(self.contributors)
            # Filter out bots
            bots = ['dependabot[bot]']
            filtered_contributors = [c for c in sorted_contributors if c not in bots]
            
            if filtered_contributors:
                contributor_links = [f"[@{c}](https://github.com/{c})" for c in filtered_contributors]
                output.append(", ".join(contributor_links))
            output.append("")
        
        return "\n".join(output)
    
    def save_changelog(self, output_path: Path) -> None:
        """Save the generated changelog to file"""
        changelog_content = self.generate_cliff_changelog()
        output_path.write_text(changelog_content, encoding='utf-8')
        print(f"Changelog saved to: {output_path}")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Convert GitHub changelog to cliff.toml format')
    parser.add_argument('input_file', help='Path to the GitHub changelog file')
    parser.add_argument('--output', '-o', help='Output file path (default: cliff_changelog.md)')
    parser.add_argument('--version', '-v', help='Version number (extracted from filename if not provided)')
    parser.add_argument('--date', '-d', help='Release date (default: current date)')
    
    args = parser.parse_args()
    
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Error: Input file '{input_path}' does not exist")
        return 1
    
    # Extract version from filename if not provided
    version = args.version
    if not version:
        # Try to extract version from filename like "changelog-0.15.md"
        version_match = re.search(r'changelog-(.+?)\.md', input_path.name)
        if version_match:
            version = version_match.group(1)
        else:
            version = "Unknown"
    
    # Set output path
    output_path = Path(args.output) if args.output else Path(f"cliff_changelog_{version}.md")
    
    # Convert changelog
    converter = ChangelogConverter(version, args.date)
    converter.parse_github_changelog(input_path)
    converter.save_changelog(output_path)
    
    print(f"Conversion completed successfully!")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(f"Version: {version}")
    print(f"Total entries: {len(converter.entries)}")
    print(f"Breaking changes: {len([e for e in converter.entries if e.is_breaking])}")
    print(f"Contributors: {len(converter.contributors)}")
    print(f"New contributors: {len(converter.new_contributors)}")
    
    return 0


if __name__ == "__main__":
    exit(main())