# MemTap

**memtap.py** — a utility for dumping data from memcached instances. Allows you to quickly and efficiently extract all keys and values from memcached servers.

```bash
└─$ python3 memtap.py -h

,--.   ,--.                 ,--------.
|   `.'   | ,---. ,--,--,--.'--.  .--',--,--. ,---.
|  |'.'|  || .-. :|        |   |  |  ' ,-.  || .-. |
|  |   |  |\   --.|  |  |  |   |  |  \ '-'  || '-' '
`--'   `--' `----'`--`--`--'   `--'   `--`--'|  |-'.py (ver1.0) @Kraus17th
        # one tap to dump memcache           `--'

==========================================================================

Usage:
  memtap.py -t TARGET [OPTIONS]
  memtap.py -f FILE [OPTIONS]

Required Arguments:
  -t, --target TARGET              Target memcached host (mutually exclusive with -f)
  -f, --file-with-targets FILE     File containing targets, one per line

Connection Options:
  -p, --port PORT                  Memcached port (default: 11211)
  -x, --proxy PROXY                Example: socks5:127.0.0.1:5555

Output Options:
  -o, --output FILE                Output filename (default: TARGET_dump.txt)
  -v, --verbose                    Enable verbose output with debug information

Performance Options:
  -l, --limit-requests NUM         Limit number of values per target (default: no limit)
  -d, --delay SECONDS              Delay between requests in seconds (default: 1.0)
  -T, --threads NUM                Number of threads for parallel dumping (default: 1)

Utility Options:
  -V, --only-check-version         Only check memcached version for each target
  -c, --only-count-keys            Only count keys for each target
  -h, --help                       Show this help message
```

## 🎯 Features

- **Complete data dump**: Extracts all keys and values from memcached server
- **Proxy support**: Works through SOCKS5, SOCKS4, and HTTP proxies
- **Multithreading**: Parallel key processing for faster execution
- **Flexible configuration**: Configurable delays, limits, and thread count
- **Batch processing**: Process multiple targets from a file
- **Progress bar**: Visual progress indication using tqdm
- **Recovery**: Saves partial results on interruption
- **Utilities**: Version check and key counting without full dump

## 🚀 Installation

### Option 1: Install via pipx (recommended)

[pipx](https://github.com/pypa/pipx) allows you to install and run Python applications in isolated environments:

```bash
# Install pipx if not already installed
# On macOS/Linux:
python3 -m pip install --user pipx
python3 -m pipx ensurepath

# On Windows:
python -m pip install --user pipx
python -m pipx ensurepath

# Install MemTap via pipx
pipx install git+https://github.com/Kraus17th/memtap.git

# Or if the repository is already cloned locally:
pipx install /path/to/memtap
```

After installation, the `memtap` command will be available globally.

### Option 2: Install via git clone

```bash
# Clone the repository
git clone https://github.com/Kraus17th/memtap.git
cd memtap

# Create a virtual environment (recommended)
python3 -m venv venv

# Activate the virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the utility
python memtap.py -t TARGET
```

## 📖 Usage

### Basic usage

```bash
# Dump a single server
memtap -t 192.168.1.100

# With port specification
memtap -t 192.168.1.100 -p 11211

# Save to a specific file
memtap -t 192.168.1.100 -o results.txt
```

### Working with multiple targets

```bash
# Create a targets.txt file with a list of IP addresses (one per line)
# Example content:
# 192.168.1.100
# 192.168.1.101
# 192.168.1.102

# Process all targets from file
memtap -f targets.txt
```

### Working through proxy

```bash
# Through SOCKS5 proxy
memtap -t 192.168.1.100 -x socks5:127.0.0.1:1080

# Through SOCKS4 proxy
memtap -t 192.168.1.100 -x socks4:127.0.0.1:1080

# Through HTTP proxy
memtap -t 192.168.1.100 -x http:127.0.0.1:8080
```

### Performance tuning

```bash
# Fast dump with minimal delay and multiple threads
memtap -t 192.168.1.100 -d 0.1 -T 8

# Limit number of keys
memtap -t 192.168.1.100 -l 100

# Configure delay between requests (in seconds)
memtap -t 192.168.1.100 -d 0.5
```

### Utilities

```bash
# Check memcached version
memtap -t 192.168.1.100 -V

# Count number of keys
memtap -t 192.168.1.100 -c
```

### Verbose mode

```bash
# Enable verbose output for debugging
memtap -t 192.168.1.100 -v
```

## 📝 Command-line parameters

### Required arguments

- `-t, --target TARGET` - Target memcached host (mutually exclusive with `-f`)
- `-f, --file-with-targets FILE` - File with targets (one per line)

### Connection parameters

- `-p, --port PORT` - Memcached port (default: 11211)
- `-x, --proxy PROXY` - Proxy in format `TYPE:HOST:PORT` (e.g.: `socks5:127.0.0.1:5555`)

### Output parameters

- `-o, --output FILE` - Output filename (default: `TARGET_dump.txt`)
- `-v, --verbose` - Enable verbose output with debug information

### Performance parameters

- `-l, --limit-requests NUM` - Limit number of values per target (default: no limit)
- `-d, --delay SECONDS` - Delay between requests in seconds (default: 1.0)
- `-T, --threads NUM` - Number of threads for parallel dumping (default: 1)

### Utilities

- `-V, --only-check-version` - Only check memcached version for each target
- `-c, --only-count-keys` - Only count keys for each target
- `-h, --help` - Show help message

## 📄 Output file format

Results are saved to a text file with the following format:

```
# MemCache dump for: 192.168.1.100:11211
# Started at "2024-01-15 10:30:00"
# Finished at "2024-01-15 10:35:00"

===== KEY: example_key_1 =====
<key value>
===== DONE: example_key_1 =====

===== KEY: example_key_2 =====
<key value>
===== DONE: example_key_2 =====
```

## 🔧 How it works

1. **Metadata retrieval**: The utility sends the `lru_crawler metadump all` command to the memcached server to get a list of all keys
2. **Key parsing**: Extracts all keys from metadata
3. **Value retrieval**: For each key, sends a `get` command and retrieves the corresponding value
4. **Saving**: Writes all key-value pairs to the output file

## ⚠️ Important notes

- The utility requires the memcached server to support the `lru_crawler metadump all` command
- When working with large amounts of data, the process may take significant time
- Use the `-d` parameter to configure delay to avoid overloading the server
- On interruption (Ctrl+C), partial results are automatically saved
- Some keys may be unavailable (deleted, expired, etc.)

## 🐛 Troubleshooting

### Error "Failed to retrieve metadata"

Possible causes:
- Server is unreachable or port is incorrect
- Incorrect proxy configuration
- Server does not support the `lru_crawler metadump all` command
- Network connectivity issues

**Solution**: Check server accessibility, correct port, and proxy configuration. Use `-v` for detailed information.

### Error "pysocks not installed"

**Solution**: Install the dependency:
```bash
pip install pysocks
```

### Error "tqdm not installed"

**Solution**: Install the dependency (optional but recommended):
```bash
pip install tqdm
```

## 📦 Dependencies

- `pysocks>=1.7.1` - Proxy support (SOCKS4, SOCKS5, HTTP)
- `tqdm>=4.67.0` - Progress bar (optional but recommended)

## 📄 License

This project is distributed under the MIT license. See the `LICENSE` file for details.

## ⚖️ Disclaimer

This utility is intended for legal use only on servers to which you have explicit permission to access. Use on servers without permission may be illegal. The authors are not responsible for misuse of this tool. This utility is created for security and auditing. Always ensure you have the right to access target servers before use.
