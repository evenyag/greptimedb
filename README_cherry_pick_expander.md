# Cherry-pick PR Expander

This Python script processes GitHub changelog files to expand cherry-pick entries into their individual PR details. It automatically detects cherry-pick entries, fetches the original PR titles and authors from GitHub API, and replaces them in the changelog.

## Features

- **Automatic Detection**: Finds cherry-pick entries in changelog files using pattern matching
- **GitHub API Integration**: Fetches original PR titles and author information
- **Complete Replacement**: Creates a new changelog with cherry-pick entries replaced by individual PRs
- **Progress Tracking**: Shows detailed progress during processing with replacement confirmation
- **Rate Limiting**: Respects GitHub API rate limits with configurable delays
- **Error Handling**: Gracefully handles API errors and provides fallback entries

## Usage

### Basic Usage

```bash
python3 cherry_pick_expander.py changelog-0.15.md
```

This will create `changelog-0.15_expanded.md` with cherry-pick entries expanded.

### With Custom Output

```bash
python3 cherry_pick_expander.py changelog-0.15.md --output my_expanded_changelog.md
```

### With GitHub Token (Recommended)

```bash
python3 cherry_pick_expander.py changelog-0.15.md --token your_github_token
```

Using a GitHub token increases API rate limits from 60 to 5000 requests per hour.

### Command Line Options

- `input_file`: Path to the changelog file (required)
- `--output`, `-o`: Output file path (default: `<input>_expanded.md`)
- `--token`, `-t`: GitHub API token for higher rate limits
- `--delay`, `-d`: Delay between API calls in seconds (default: 1.0)

## Example Processing

The script detects entries like:

```markdown
* feat: cherry-pick #6384 #6388 #6396 #6403 #6412 #6405 to 0.15 branch by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6414
```

And expands them to:

```markdown
* feat: supports CsvWithNames and CsvWithNamesAndTypes formats by [@killme2008](https://github.com/killme2008) in [#6384](https://github.com/GreptimeTeam/greptimedb/pull/6384)
* feat: introduce /v1/health for healthcheck from external by [@sunng87](https://github.com/sunng87) in [#6388](https://github.com/GreptimeTeam/greptimedb/pull/6388)
* feat: update dashboard to v0.10.1 by [@ZonaHex](https://github.com/ZonaHex) in [#6396](https://github.com/GreptimeTeam/greptimedb/pull/6396)
* fix: complete partial index search results in cache by [@zhongzc](https://github.com/zhongzc) in [#6403](https://github.com/GreptimeTeam/greptimedb/pull/6403)
* refactor: pass pipeline name through http header and get db from query context by [@zyy17](https://github.com/zyy17) in [#6405](https://github.com/GreptimeTeam/greptimedb/pull/6405)
* fix: skip failing nodes when gathering porcess info by [@v0y4g3r](https://github.com/v0y4g3r) in [#6412](https://github.com/GreptimeTeam/greptimedb/pull/6412)
```

## Processing Output

During execution, you'll see detailed progress:

```
Found 5 cherry-pick entries

Processing cherry-pick PR #6414: feat: cherry-pick #6384 #6388 #6396 #6403 #6412 #6405 to 0.15 branch
  Cherry-picked PRs: #6384, #6388, #6396, #6403, #6405, #6412
Fetching PR #6384 from GitHub API...
    → feat: supports CsvWithNames and CsvWithNamesAndTypes formats by @killme2008 in #6384
Fetching PR #6388 from GitHub API...
    → feat: introduce /v1/health for healthcheck from external by @sunng87 in #6388
...

Processing cherry-pick PR #6445: feat: pick #6416 to release/0.15
  Cherry-picked PRs: #6416
Fetching PR #6416 from GitHub API...
    → feat: add `granularity` and `false_positive_rate` options for indexes by @zhongzc in #6416

Replacing cherry-pick entry:
  Original: * feat: cherry-pick #6384 #6388 #6396 #6403 #6412 #6405 to 0.15 branch by @evenyag in https://github.com/GreptimeTeam/greptimedb/pull/6414
  With 6 expanded entries
  ✓ Successfully replaced

Replacing cherry-pick entry:
  Original: * feat: pick #6416 to release/0.15 by @zhongzc in https://github.com/GreptimeTeam/greptimedb/pull/6445
  With 1 expanded entries
  ✓ Successfully replaced

=== Summary ===
Input file: changelog-0.15.md
Output file: expanded_changelog_0.15.md
Cherry-pick entries found: 5
Total PRs expanded: 18
```

## Supported Cherry-pick Formats

The script recognizes various cherry-pick entry formats:

- `feat: cherry-pick #6384 #6388 #6396 to 0.15 branch`
- `chore: cherry pick #6385, #6390, #6432 to 0.15 branch`
- `chore: cherry pick pr 6391 to release/v0.15`
- `feat: pick #6416 to release/0.15`
- `chore: pick #6399, #6443, #6431, #6453, #6454 to v0.15`

The script handles both "cherry pick" and standalone "pick" keywords.

## Requirements

- Python 3.6+
- Internet connection for GitHub API access
- No external dependencies (uses only standard library)

## GitHub API Rate Limits

- **Without token**: 60 requests per hour
- **With token**: 5000 requests per hour

For processing large changelogs with many cherry-pick entries, a GitHub token is recommended.

## Error Handling

- If a PR cannot be fetched, a placeholder entry is created
- API errors are logged but don't stop processing
- Rate limiting is respected with configurable delays
- Original changelog is preserved if no cherry-pick entries are found