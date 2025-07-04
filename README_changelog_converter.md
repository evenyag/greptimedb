# Changelog Converter

This Python script converts GitHub-generated changelog files to the format defined in `cliff.toml`.

## Features

- Parses GitHub changelog entries with pull request links and author information
- Categorizes entries based on conventional commit prefixes (feat, fix, docs, etc.)
- Identifies breaking changes (entries containing "breaking", "break", or "!")
- Generates formatted output matching the cliff.toml template structure
- Includes sections for:
  - Breaking changes
  - Categorized changes (Features, Bug Fixes, etc.)
  - New contributors
  - All contributors

## Usage

### Basic Usage

```bash
python3 changelog_converter.py changelog-0.15.md
```

This will generate `cliff_changelog_0.15.md` with the version extracted from the filename.

### With Custom Output

```bash
python3 changelog_converter.py changelog-0.15.md --output my_changelog.md
```

### With Custom Version and Date

```bash
python3 changelog_converter.py changelog-0.15.md --version "v0.15.0" --date "December 15, 2024"
```

### Command Line Options

- `input_file`: Path to the GitHub changelog file (required)
- `--output`, `-o`: Output file path (default: `cliff_changelog_<version>.md`)
- `--version`, `-v`: Version number (extracted from filename if not provided)
- `--date`, `-d`: Release date (default: current date)

## Input Format

The script expects GitHub changelog files with the following format:

```markdown
## What's Changed
* feat: some feature by @username in https://github.com/owner/repo/pull/123
* fix: some bug fix by @username in https://github.com/owner/repo/pull/124

## New Contributors
* @newuser made their first contribution in https://github.com/owner/repo/pull/125
```

## Output Format

The script generates a changelog matching the cliff.toml template:

```markdown
# 0.15

Release date: December 15, 2024

## Breaking changes
  * feat!: breaking change by [@username](https://github.com/username) in [#123](https://github.com/owner/repo/pull/123)

### 🚀 Features
    * feat: some feature by [@username](https://github.com/username) in [#123](https://github.com/owner/repo/pull/123)

### 🐛 Bug Fixes
    * fix: some bug fix by [@username](https://github.com/username) in [#124](https://github.com/owner/repo/pull/124)

## New Contributors
  * [@newuser](https://github.com/newuser) made their first contribution in [#125](https://github.com/owner/repo/pull/125)

## All Contributors

We would like to thank the following contributors from the GreptimeDB community:

[@username](https://github.com/username), [@newuser](https://github.com/newuser)
```

## Category Mapping

The script maps conventional commit prefixes to categories as defined in `cliff.toml`:

- `feat:` → 🚀 Features
- `fix:` → 🐛 Bug Fixes
- `refactor:` → 🚜 Refactor
- `doc:` → 📚 Documentation
- `perf:` → ⚡ Performance
- `style:` → 🎨 Styling
- `test:` → 🧪 Testing
- `chore:`, `ci:` → ⚙️ Miscellaneous Tasks
- `revert:` → ◀️ Revert
- `security` → 🛡️ Security

## Breaking Changes Detection

The script identifies breaking changes by looking for:
- Entries containing "breaking", "break", "BREAKING", or "BREAK"
- Entries with "!:" in the title (conventional commits breaking change format)

## Requirements

- Python 3.6+
- No external dependencies (uses only standard library modules)

## Example

```bash
# Convert changelog-0.15.md to cliff format
python3 changelog_converter.py changelog-0.15.md

# Output:
# Changelog saved to: cliff_changelog_0.15.md
# Conversion completed successfully!
# Input: changelog-0.15.md
# Output: cliff_changelog_0.15.md
# Version: 0.15
# Total entries: 244
# Breaking changes: 3
# Contributors: 26
# New contributors: 6
```