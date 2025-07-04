#!/usr/bin/env python3
"""
Cherry-pick PR Expander

This script finds cherry-pick entries in GitHub changelog files, extracts the PR numbers
that were cherry-picked, fetches their original titles from GitHub API, and outputs
them in the same format as the original changelog entries.
"""

import re
import json
import urllib.request
import urllib.error
import argparse
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass


@dataclass
class PRInfo:
    """Information about a pull request"""
    number: int
    title: str
    author: str
    url: str


@dataclass
class CherryPickEntry:
    """Cherry-pick changelog entry"""
    original_title: str
    pr_number: int
    author: str
    cherry_picked_prs: List[int]


class GitHubAPIError(Exception):
    """Custom exception for GitHub API errors"""
    pass


class CherryPickExpander:
    """Expands cherry-pick entries in changelog files"""
    
    def __init__(self, github_token: Optional[str] = None):
        self.github_token = github_token
        self.repo_owner = "GreptimeTeam"
        self.repo_name = "greptimedb"
        self.pr_cache: Dict[int, PRInfo] = {}
        self.rate_limit_delay = 1.0  # seconds between API calls
        
    def _make_github_request(self, url: str) -> Dict:
        """Make a request to GitHub API with proper headers and error handling"""
        headers = {
            'User-Agent': 'Cherry-Pick-Expander/1.0',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        if self.github_token:
            headers['Authorization'] = f'token {self.github_token}'
            
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            if e.code == 403:
                raise GitHubAPIError(f"GitHub API rate limit exceeded or access denied. Consider using a GitHub token.")
            elif e.code == 404:
                raise GitHubAPIError(f"PR not found: {url}")
            else:
                raise GitHubAPIError(f"GitHub API error {e.code}: {e.reason}")
        except Exception as e:
            raise GitHubAPIError(f"Failed to fetch from GitHub: {e}")
    
    def get_pr_info(self, pr_number: int) -> PRInfo:
        """Get PR information from GitHub API with caching"""
        if pr_number in self.pr_cache:
            return self.pr_cache[pr_number]
            
        url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/pulls/{pr_number}"
        
        print(f"Fetching PR #{pr_number} from GitHub API...")
        time.sleep(self.rate_limit_delay)  # Rate limiting
        
        try:
            data = self._make_github_request(url)
            
            pr_info = PRInfo(
                number=pr_number,
                title=data['title'],
                author=data['user']['login'],
                url=data['html_url']
            )
            
            self.pr_cache[pr_number] = pr_info
            return pr_info
            
        except GitHubAPIError as e:
            print(f"Warning: Could not fetch PR #{pr_number}: {e}")
            # Return a placeholder if we can't fetch the PR
            return PRInfo(
                number=pr_number,
                title=f"PR #{pr_number} (title unavailable)",
                author="unknown",
                url=f"https://github.com/{self.repo_owner}/{self.repo_name}/pull/{pr_number}"
            )
    
    def find_cherry_pick_entries(self, changelog_content: str) -> List[CherryPickEntry]:
        """Find all cherry-pick entries in the changelog"""
        cherry_pick_entries = []
        
        # Pattern to match cherry-pick entries
        # Examples:
        # * feat: cherry-pick #6384 #6388 #6396 #6403 #6412 #6405 to 0.15 branch by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6414
        # * chore: cherry pick #6385, #6390, #6432, #6446, #6444 to 0.15 branch by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6447
        # * chore: cherry pick pr 6391 to release/v0.15 by @fengys1996 in https://github.com/GreptimeTeam/greptimedb/pull/6449
        # * feat: pick #6416 to release/0.15 by @zhongzc in https://github.com/GreptimeTeam/greptimedb/pull/6445
        # * chore: pick #6399, #6443, #6431, #6453, #6454 to v0.15 by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6455
        
        # Updated pattern to match both "cherry pick" and standalone "pick"
        cherry_pick_pattern = r'\* (.+(?:cherry.)?pick.+?) by @(\w+) in https://github\.com/[^/]+/[^/]+/pull/(\d+)'
        
        for match in re.finditer(cherry_pick_pattern, changelog_content, re.IGNORECASE):
            title = match.group(1)
            author = match.group(2)
            pr_number = int(match.group(3))
            
            # Extract PR numbers from the title
            # Look for patterns like #1234, #1234,, pr 1234, etc.
            pr_numbers = []
            
            # Pattern 1: #1234 format
            hash_pattern = r'#(\d+)'
            pr_numbers.extend([int(num) for num in re.findall(hash_pattern, title)])
            
            # Pattern 2: pr 1234 format (without #)
            pr_pattern = r'(?:pr|PR)\s+(\d+)'
            pr_numbers.extend([int(num) for num in re.findall(pr_pattern, title)])
            
            # Remove duplicates and sort
            pr_numbers = sorted(list(set(pr_numbers)))
            
            if pr_numbers:
                entry = CherryPickEntry(
                    original_title=title,
                    pr_number=pr_number,
                    author=author,
                    cherry_picked_prs=pr_numbers
                )
                cherry_pick_entries.append(entry)
        
        return cherry_pick_entries
    
    def expand_cherry_pick_entries(self, cherry_pick_entries: List[CherryPickEntry]) -> Dict[int, List[str]]:
        """Expand cherry-pick entries into individual PR entries, keyed by cherry-pick PR number"""
        expanded_entries = {}
        
        for entry in cherry_pick_entries:
            print(f"\nProcessing cherry-pick PR #{entry.pr_number}: {entry.original_title}")
            print(f"  Cherry-picked PRs: {', '.join(f'#{pr}' for pr in entry.cherry_picked_prs)}")
            
            replacement_entries = []
            
            for pr_number in entry.cherry_picked_prs:
                try:
                    pr_info = self.get_pr_info(pr_number)
                    
                    # Format as changelog entry (match original GitHub format)
                    changelog_entry = f"* {pr_info.title} by @{pr_info.author} in https://github.com/{self.repo_owner}/{self.repo_name}/pull/{pr_info.number}"
                    replacement_entries.append(changelog_entry)
                    print(f"    → {pr_info.title} by @{pr_info.author} in #{pr_info.number}")
                    
                except Exception as e:
                    print(f"    Error processing PR #{pr_number}: {e}")
                    # Add a placeholder entry
                    changelog_entry = f"* PR #{pr_number} (could not fetch details) by @unknown in https://github.com/{self.repo_owner}/{self.repo_name}/pull/{pr_number}"
                    replacement_entries.append(changelog_entry)
                    print(f"    → PR #{pr_number} (could not fetch details)")
            
            expanded_entries[entry.pr_number] = replacement_entries
        
        return expanded_entries
    
    def process_changelog(self, changelog_path: Path) -> Tuple[str, List[CherryPickEntry], Dict[int, List[str]]]:
        """Process changelog file and return content, cherry-pick entries, and their expansions"""
        content = changelog_path.read_text(encoding='utf-8')
        
        # Find cherry-pick entries
        cherry_pick_entries = self.find_cherry_pick_entries(content)
        
        if not cherry_pick_entries:
            print("No cherry-pick entries found in the changelog.")
            return content, [], {}
        
        print(f"Found {len(cherry_pick_entries)} cherry-pick entries")
        
        # Expand them
        expanded_entries = self.expand_cherry_pick_entries(cherry_pick_entries)
        
        return content, cherry_pick_entries, expanded_entries
    
    def create_expanded_changelog(self, original_content: str, cherry_pick_entries: List[CherryPickEntry], expanded_entries: Dict[int, List[str]]) -> str:
        """Create a new changelog with cherry-pick entries replaced by their individual PRs"""
        new_content = original_content
        
        # Replace each cherry-pick entry with its expanded PRs
        for entry in cherry_pick_entries:
            # Find the original cherry-pick line (need to escape special regex characters)
            cherry_pick_line = f"* {entry.original_title} by @{entry.author} in https://github.com/{self.repo_owner}/{self.repo_name}/pull/{entry.pr_number}"
            
            # Create replacement content (join all expanded entries)
            replacement_content = "\n".join(expanded_entries[entry.pr_number])
            
            print(f"\nReplacing cherry-pick entry:")
            print(f"  Original: {cherry_pick_line}")
            print(f"  With {len(expanded_entries[entry.pr_number])} expanded entries")
            
            # Replace the cherry-pick line with the expanded entries
            if cherry_pick_line in new_content:
                new_content = new_content.replace(cherry_pick_line, replacement_content)
                print(f"  ✓ Successfully replaced")
            else:
                print(f"  ✗ Could not find exact match in content")
        
        return new_content
    
    def save_expanded_changelog(self, content: str, output_path: Path) -> None:
        """Save the expanded changelog to a file"""
        output_path.write_text(content, encoding='utf-8')
        print(f"Expanded changelog saved to: {output_path}")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Expand cherry-pick entries in changelog files')
    parser.add_argument('input_file', help='Path to the changelog file')
    parser.add_argument('--output', '-o', help='Output file path (default: expanded_changelog.md)')
    parser.add_argument('--token', '-t', help='GitHub API token (optional, for higher rate limits)')
    parser.add_argument('--delay', '-d', type=float, default=1.0, help='Delay between API calls in seconds (default: 1.0)')
    
    args = parser.parse_args()
    
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Error: Input file '{input_path}' does not exist")
        return 1
    
    # Create output filename based on input if not specified
    if args.output:
        output_path = Path(args.output)
    else:
        input_stem = input_path.stem
        output_path = input_path.parent / f"{input_stem}_expanded.md"
    
    # Initialize expander
    expander = CherryPickExpander(github_token=args.token)
    expander.rate_limit_delay = args.delay
    
    try:
        # Process changelog
        original_content, cherry_pick_entries, expanded_entries = expander.process_changelog(input_path)
        
        if not cherry_pick_entries:
            print("No cherry-pick entries found. Copying original file.")
            output_path.write_text(original_content, encoding='utf-8')
            print(f"Original changelog copied to: {output_path}")
            return 0
        
        # Create expanded changelog
        expanded_content = expander.create_expanded_changelog(original_content, cherry_pick_entries, expanded_entries)
        
        # Save results
        expander.save_expanded_changelog(expanded_content, output_path)
        
        # Calculate total PRs expanded
        total_prs_expanded = sum(len(entries) for entries in expanded_entries.values())
        
        # Print summary
        print(f"\n=== Summary ===")
        print(f"Input file: {input_path}")
        print(f"Output file: {output_path}")
        print(f"Cherry-pick entries found: {len(cherry_pick_entries)}")
        print(f"Total PRs expanded: {total_prs_expanded}")
        
        return 0
        
    except GitHubAPIError as e:
        print(f"GitHub API Error: {e}")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())