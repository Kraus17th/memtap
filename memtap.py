#!/usr/bin/env python3

import argparse
import socket
import sys
import time
import threading
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import re
from queue import Queue
import os
from urllib.parse import unquote

try:
    import socks
    SOCKS_AVAILABLE = True
except ImportError:
    SOCKS_AVAILABLE = False
    print("Warning: pysocks not installed. Proxy support will be limited.", file=sys.stderr)

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Warning: tqdm not installed. Progress bar will not be available.", file=sys.stderr)


BANNER = r"""
,--.   ,--.                 ,--------.
|   `.'   | ,---. ,--,--,--.'--.  .--',--,--. ,---.
|  |'.'|  || .-. :|        |   |  |  ' ,-.  || .-. |
|  |   |  |\   --.|  |  |  |   |  |  \ '-'  || '-' '
`--'   `--' `----'`--`--`--'   `--'   `--`--'|  |-'.py (ver1.0) @Kraus17th
        # one tap to dump memcache           `--'
"""


class MemTap:
    def __init__(self, target: str, port: int = 11211, proxy: Optional[str] = None, 
                 verbose: bool = False, delay: float = 1.0):
        self.target = target
        self.port = port
        self.proxy = proxy
        self.verbose = verbose
        self.delay = delay
        self.sock = None
        self.proxy_type = None
        self.proxy_host = None
        self.proxy_port = None
        
        if proxy:
            self._parse_proxy(proxy)
    
    def _parse_proxy(self, proxy_str: str):
        """Parse proxy string (e.g., socks5:127.0.0.1:5555)"""
        try:
            parts = proxy_str.split(':')
            if len(parts) != 3:
                raise ValueError("Invalid proxy format")
            
            proxy_type_str = parts[0].lower()
            proxy_host = parts[1]
            proxy_port = int(parts[2])
            
            if proxy_type_str == 'socks5':
                self.proxy_type = socks.SOCKS5
            elif proxy_type_str == 'socks4':
                self.proxy_type = socks.SOCKS4
            elif proxy_type_str == 'http':
                self.proxy_type = socks.HTTP
            else:
                raise ValueError(f"Unsupported proxy type: {proxy_type_str}")
            
            self.proxy_host = proxy_host
            self.proxy_port = proxy_port
            
            if not SOCKS_AVAILABLE:
                raise ValueError("pysocks library is required for proxy support. Install with: pip install pysocks")
                
        except Exception as e:
            raise ValueError(f"Failed to parse proxy '{proxy_str}': {str(e)}")
    
    def _connect(self) -> bool:
        """Establish connection to memcached server"""
        try:
            if self.proxy and SOCKS_AVAILABLE:
                self.sock = socks.socksocket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.set_proxy(self.proxy_type, self.proxy_host, self.proxy_port)
            else:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            self.sock.settimeout(10)
            self.sock.connect((self.target, self.port))
            return True
        except Exception as e:
            if self.verbose:
                print(f"Connection error: {str(e)}", file=sys.stderr)
            return False
    
    def _disconnect(self):
        """Close connection"""
        if self.sock:
            try:
                self.sock.close()
            except:
                pass
            self.sock = None
    
    def _send_command(self, command: str) -> Optional[str]:
        """Send command to memcached and receive response"""
        try:
            if not self.sock:
                if not self._connect():
                    return None
            
            self.sock.sendall((command + '\r\n').encode('utf-8'))
            response = b''
            
            # For version command, it returns single line: "VERSION <version>\r\n"
            if command.lower() == 'version':
                chunk = self.sock.recv(4096)
                if chunk:
                    response = chunk
            else:
                # For other commands, read until END or ERROR
                while True:
                    chunk = self.sock.recv(4096)
                    if not chunk:
                        break
                    response += chunk
                    if b'END\r\n' in response or b'ERROR\r\n' in response:
                        break
                    # Also check for single-line responses that end with \r\n
                    if response.endswith(b'\r\n') and len(response.split(b'\r\n')) == 2:
                        break
            
            return response.decode('utf-8', errors='ignore')
        except Exception as e:
            if self.verbose:
                print(f"Error sending command '{command}': {str(e)}", file=sys.stderr)
            return None
    
    def get_metadata(self) -> Optional[str]:
        """Get metadata dump using lru_crawler metadump all"""
        if self.verbose:
            print(f"[*] Sending 'lru_crawler metadump all' to {self.target}:{self.port}")
        
        response = self._send_command("lru_crawler metadump all")
        
        if response:
            if self.verbose:
                print(f"[+] Received metadata response ({len(response)} bytes)")
            return response
        else:
            print(f"[-] Failed to retrieve metadata from {self.target}:{self.port}", file=sys.stderr)
            return None
    
    def get_version(self) -> Optional[str]:
        """Get memcached version"""
        if self.verbose:
            print(f"[*] Requesting version from {self.target}:{self.port}")
        
        response = self._send_command("version")
        
        if response:
            # Version response format: "VERSION <version>\r\n"
            version = response.strip().replace('VERSION ', '').replace('\r\n', '').strip()
            if self.verbose:
                print(f"[+] Received version: {version}")
            return version
        else:
            if self.verbose:
                print(f"[-] Failed to retrieve version from {self.target}:{self.port}", file=sys.stderr)
            return None
    
    def count_keys(self) -> Optional[int]:
        """Count keys in memcached instance"""
        if self.verbose:
            print(f"[*] Counting keys for {self.target}:{self.port}")
        
        metadata = self.get_metadata()
        if not metadata:
            return None
        
        keys = self.parse_keys(metadata)
        return len(keys)
    
    def parse_keys(self, metadata: str) -> List[str]:
        """Parse keys from metadata dump"""
        keys = []
        # Metadata format: key=<key> exp=<exp> la=<la> cas=<cas> fetch=<fetch> cls=<cls> size=<size>
        # Key can contain spaces and special characters, so we need to parse more carefully
        
        for line in metadata.split('\n'):
            if 'key=' in line:
                # Find the start of the key
                key_start = line.find('key=')
                if key_start == -1:
                    continue
                
                # Extract everything after 'key='
                key_part = line[key_start + 4:]
                
                # Find where the key ends - it ends before the next parameter
                # Parameters are: exp=, la=, cas=, fetch=, cls=, size=
                key_end = len(key_part)
                for param in [' exp=', ' la=', ' cas=', ' fetch=', ' cls=', ' size=']:
                    param_pos = key_part.find(param)
                    if param_pos != -1 and param_pos < key_end:
                        key_end = param_pos
                
                # Extract the key (everything up to the first parameter)
                key = key_part[:key_end].strip()
                
                # Remove trailing spaces that might be part of the key
                # But be careful - some keys might legitimately end with spaces
                # For now, just strip and add
                if key:
                    keys.append(key)
        
        if self.verbose:
            print(f"[*] Parsed {len(keys)} keys from metadata")
        
        return keys
    
    def get_value(self, key: str) -> Optional[bytes]:
        """Get value for a specific key. Tries both encoded and decoded versions if needed."""
        # Try with key as-is first (memcached stores keys as they are)
        result = self._get_value_internal(key)
        if result is not None:
            return result
        
        # If failed, try with URL-decoded key
        try:
            decoded_key = unquote(key)
            if decoded_key != key:
                if self.verbose:
                    print(f"[*] Trying URL-decoded key for '{key[:50]}...'", file=sys.stderr)
                result = self._get_value_internal(decoded_key)
                if result is not None:
                    return result
        except Exception as e:
            if self.verbose:
                print(f"Error decoding key '{key[:50]}...': {str(e)}", file=sys.stderr)
        
        return None
    
    def _get_value_internal(self, key: str) -> Optional[bytes]:
        """Internal method to get value for a key"""
        try:
            if not self.sock:
                if not self._connect():
                    return None
            
            # Use get command - memcached protocol handles keys as-is
            command = f"get {key}"
            self.sock.sendall((command + '\r\n').encode('utf-8'))
            
            response = b''
            timeout_count = 0
            max_timeouts = 3
            while True:
                try:
                    chunk = self.sock.recv(4096)
                    if not chunk:
                        break
                    response += chunk
                    if b'END\r\n' in response:
                        break
                    # Also check for ERROR response
                    if b'ERROR\r\n' in response or b'NOT_FOUND\r\n' in response:
                        break
                except (socket.timeout, TimeoutError):
                    timeout_count += 1
                    if timeout_count > max_timeouts:
                        if self.verbose:
                            print(f"Timeout receiving data for key '{key[:50]}...'", file=sys.stderr)
                        break
                    continue
                except Exception as e:
                    if self.verbose:
                        print(f"Error receiving data for key '{key[:50]}...': {str(e)}", file=sys.stderr)
                    break
            
            # Parse response: VALUE <key> <flags> <bytes>\r\n<data>\r\nEND\r\n
            decoded = response.decode('utf-8', errors='ignore')
            
            # Check for errors - if NOT_FOUND or ERROR, return None (will try decoded version)
            if 'ERROR' in decoded or 'NOT_FOUND' in decoded or 'END' not in decoded:
                return None
            
            lines = decoded.split('\r\n')
            
            # Find VALUE line and extract data
            value_data = []
            in_value = False
            for i, line in enumerate(lines):
                if line.startswith('VALUE'):
                    in_value = True
                    # Parse VALUE line: VALUE <key> <flags> <bytes>
                    parts = line.split()
                    if len(parts) >= 4:
                        try:
                            bytes_count = int(parts[3])
                            # Next line(s) contain the actual value
                            if i + 1 < len(lines):
                                # Value might span multiple lines, but typically it's the next line
                                value_line = lines[i + 1]
                                # Take exactly bytes_count bytes
                                value_bytes = value_line[:bytes_count].encode('utf-8', errors='ignore')
                                return value_bytes
                        except (ValueError, IndexError):
                            pass
                elif line == 'END' and in_value:
                    break
            
            # Fallback: try to extract value between VALUE and END
            if 'VALUE' in decoded and 'END' in decoded:
                start_idx = decoded.find('VALUE')
                end_idx = decoded.find('END', start_idx)
                if start_idx < end_idx:
                    value_section = decoded[start_idx:end_idx]
                    lines_in_section = value_section.split('\r\n')
                    if len(lines_in_section) >= 2:
                        # First line is VALUE, rest is data
                        value_lines = lines_in_section[1:]
                        value = '\r\n'.join(value_lines).rstrip()
                        return value.encode('utf-8', errors='ignore')
            
            return None
        except Exception as e:
            if self.verbose:
                print(f"Error getting value for key '{key}': {str(e)}", file=sys.stderr)
            return None
    
    def dump_all(self, keys: List[str], limit: Optional[int] = None, num_threads: int = 1) -> Tuple[Dict[str, bytes], bool]:
        """Dump all values for given keys. Returns (results, interrupted)"""
        keys_to_process = keys[:limit] if limit else keys
        
        if self.verbose:
            print(f"[*] Starting dump of {len(keys_to_process)} keys using {num_threads} thread(s)")
        
        interrupted = False
        
        if num_threads == 1:
            # Single-threaded mode
            results = {}
            if TQDM_AVAILABLE:
                iterator = tqdm(keys_to_process, desc=f"Dumping {self.target}", unit="key")
            else:
                iterator = keys_to_process
                if not self.verbose:
                    print(f"[*] Processing {len(keys_to_process)} keys...")
            
            try:
                for key in iterator:
                    value = self.get_value(key)
                    if value is not None:
                        results[key] = value
                    else:
                        if self.verbose:
                            print(f"[-] Failed to get value for key: {key}", file=sys.stderr)
                    
                    if self.delay > 0:
                        time.sleep(self.delay)
            except KeyboardInterrupt:
                interrupted = True
                print(f"\n[!] Interrupted by user. Saving {len(results)} keys collected so far...", file=sys.stderr)
        else:
            # Multi-threaded mode
            results = {}
            results_lock = threading.Lock()
            keys_queue = Queue()
            
            for key in keys_to_process:
                keys_queue.put(key)
            
            interrupted_event = threading.Event()
            interrupted_flag = False
            
            def worker():
                """Worker thread function"""
                # Each thread creates its own connection
                worker_tap = MemTap(
                    self.target, self.port, self.proxy, self.verbose, self.delay
                )
                
                try:
                    while not interrupted_event.is_set():
                        try:
                            key = keys_queue.get_nowait()
                        except:
                            break
                        
                        if interrupted_event.is_set():
                            break
                        
                        value = worker_tap.get_value(key)
                        if value is not None:
                            with results_lock:
                                results[key] = value
                        else:
                            if self.verbose:
                                print(f"[-] Failed to get value for key: {key}", file=sys.stderr)
                        
                        if self.delay > 0 and not interrupted_event.is_set():
                            try:
                                time.sleep(self.delay)
                            except KeyboardInterrupt:
                                interrupted_event.set()
                                with results_lock:
                                    interrupted_flag = True
                                break
                        
                        keys_queue.task_done()
                except KeyboardInterrupt:
                    interrupted_event.set()
                    with results_lock:
                        interrupted_flag = True
                finally:
                    worker_tap._disconnect()
            
            # Create and start threads
            threads = []
            try:
                for _ in range(min(num_threads, len(keys_to_process))):
                    t = threading.Thread(target=worker)
                    t.daemon = True
                    t.start()
                    threads.append(t)
                
                # Progress bar for multi-threaded mode
                if TQDM_AVAILABLE:
                    pbar = tqdm(total=len(keys_to_process), desc=f"Dumping {self.target}", unit="key")
                    try:
                        while not keys_queue.empty() and not interrupted_event.is_set():
                            pbar.n = len(keys_to_process) - keys_queue.qsize()
                            pbar.refresh()
                            time.sleep(0.1)
                        if not interrupted_event.is_set():
                            pbar.n = len(keys_to_process)
                        pbar.close()
                    except KeyboardInterrupt:
                        interrupted_event.set()
                        interrupted_flag = True
                        print(f"\n[!] Interrupted by user. Saving {len(results)} keys collected so far...", file=sys.stderr)
                        pbar.close()
                
                # Wait for all threads to complete
                for t in threads:
                    t.join(timeout=1)
            except KeyboardInterrupt:
                interrupted_event.set()
                interrupted_flag = True
                print(f"\n[!] Interrupted by user. Saving {len(results)} keys collected so far...", file=sys.stderr)
                # Give threads a moment to finish current operations
                for t in threads:
                    t.join(timeout=0.5)
            
            interrupted = interrupted_flag
        
        if not interrupted:
            if self.verbose:
                print(f"[+] Successfully dumped {len(results)}/{len(keys_to_process)} keys")
        else:
            print(f"[!] Dump interrupted. Collected {len(results)}/{len(keys_to_process)} keys", file=sys.stderr)
        
        return results, interrupted


def write_dump_file(target: str, port: int, output_file: str, results: Dict[str, bytes], 
                   start_time: datetime, end_time: datetime, interrupted: bool = False):
    """Write dump results to file"""
    try:
        with open(output_file, 'w', encoding='utf-8', errors='ignore') as f:
            f.write(f"# MemCache dump for: {target}:{port}\n")
            f.write(f"# Started at \"{start_time.strftime('%Y-%m-%d %H:%M:%S')}\"\n")
            if interrupted:
                f.write(f"# Finished at \"{end_time.strftime('%Y-%m-%d %H:%M:%S')}\" (INTERRUPTED - partial dump)\n")
            else:
                f.write(f"# Finished at \"{end_time.strftime('%Y-%m-%d %H:%M:%S')}\"\n")
            f.write("\n")
            
            for key, value in results.items():
                f.write(f"===== KEY: {key} =====\n")
                try:
                    # Try to decode as text, fallback to hex if not possible
                    try:
                        decoded_value = value.decode('utf-8', errors='ignore')
                        f.write(decoded_value)
                    except:
                        # If decoding fails, write as hex representation
                        f.write(value.hex())
                    f.write("\n")
                    f.write(f"===== DONE: {key} =====\n\n")
                except Exception as e:
                    f.write(f"[Error writing value: {str(e)}]\n")
                    f.write(f"===== DONE: {key} =====\n\n")
        
        return True
    except Exception as e:
        print(f"[-] Error writing to file '{output_file}': {str(e)}", file=sys.stderr)
        return False


def check_version(target: str, port: int, proxy: Optional[str], verbose: bool):
    """Check version for a single target"""
    tap = MemTap(target, port, proxy, verbose, 0)
    
    try:
        version = tap.get_version()
        if version:
            print(f"{target}:{port} - Version: {version}")
            return True
        else:
            print(f"{target}:{port} - Failed to get version", file=sys.stderr)
            return False
    except Exception as e:
        print(f"{target}:{port} - Error: {str(e)}", file=sys.stderr)
        return False
    finally:
        tap._disconnect()


def count_keys(target: str, port: int, proxy: Optional[str], verbose: bool):
    """Count keys for a single target"""
    tap = MemTap(target, port, proxy, verbose, 0)
    
    try:
        count = tap.count_keys()
        if count is not None:
            print(f"{target}:{port} - Keys: {count}")
            return True
        else:
            print(f"{target}:{port} - Failed to count keys", file=sys.stderr)
            return False
    except Exception as e:
        print(f"{target}:{port} - Error: {str(e)}", file=sys.stderr)
        return False
    finally:
        tap._disconnect()


def process_target(target: str, port: int, proxy: Optional[str], output: str,
                  limit: Optional[int], delay: float, threads: int, verbose: bool):
    """Process a single target"""
    print(f"\n[*] Processing target: {target}:{port}")
    
    tap = MemTap(target, port, proxy, verbose, delay)
    
    try:
        # Step 1: Get metadata
        if verbose:
            print(f"[*] Step 1: Retrieving metadata...")
        
        metadata = tap.get_metadata()
        if not metadata:
            print(f"[-] Failed to retrieve metadata from {target}:{port}", file=sys.stderr)
            print(f"    Possible reasons:", file=sys.stderr)
            print(f"    - Server is not accessible", file=sys.stderr)
            print(f"    - Port is incorrect", file=sys.stderr)
            print(f"    - Proxy configuration is wrong", file=sys.stderr)
            print(f"    - Server does not support 'lru_crawler metadump all' command", file=sys.stderr)
            return False
        
        # Step 2: Parse keys
        if verbose:
            print(f"[*] Step 2: Parsing keys from metadata...")
        
        keys = tap.parse_keys(metadata)
        if not keys:
            print(f"[-] No keys found in metadata for {target}:{port}", file=sys.stderr)
            return False
        
        print(f"[+] Found {len(keys)} keys")
        
        # Step 3: Dump values
        if verbose:
            print(f"[*] Step 3: Dumping values...")
        
        start_time = datetime.now()
        results, interrupted = tap.dump_all(keys, limit, threads)
        
        if interrupted:
            print(f"\n[!] Interrupted by user during dump. Saving partial results...", file=sys.stderr)
            if verbose:
                print(f"[*] Results dictionary contains {len(results)} keys after interruption", file=sys.stderr)
        
        end_time = datetime.now()
        
        if not results:
            if interrupted:
                print(f"[-] No values retrieved before interruption for {target}:{port}", file=sys.stderr)
            else:
                print(f"[-] No values retrieved for {target}:{port}", file=sys.stderr)
            return False
        
        # Step 4: Write to file
        if verbose:
            print(f"[*] Step 4: Writing results to file...")
        
        # Format output filename: $TARGET_$OUTPUT
        if output:
            # Remove extension if present, we'll add it back
            base_output = os.path.splitext(output)[0]
            ext = os.path.splitext(output)[1] or '.txt'
            output_file = f"{target}_{base_output}{ext}"
        else:
            output_file = f"{target}_dump.txt"
        
        if write_dump_file(target, port, output_file, results, start_time, end_time, interrupted):
            if interrupted:
                print(f"[!] Partial dump saved: {len(results)} keys saved to '{output_file}' (interrupted)")
            else:
                print(f"[+] Successfully dumped {len(results)} keys to '{output_file}'")
            return True
        else:
            return False
    
    except KeyboardInterrupt:
        # Handle KeyboardInterrupt at process_target level
        print(f"\n[!] Interrupted by user. Attempting to save partial results...", file=sys.stderr)
        # Try to save whatever we have
        if 'results' in locals() and results:
            try:
                if output:
                    base_output = os.path.splitext(output)[0]
                    ext = os.path.splitext(output)[1] or '.txt'
                    output_file = f"{target}_{base_output}{ext}"
                else:
                    output_file = f"{target}_dump.txt"
                
                if 'start_time' not in locals():
                    start_time = datetime.now()
                end_time = datetime.now()
                
                if write_dump_file(target, port, output_file, results, start_time, end_time, True):
                    print(f"[!] Partial dump saved: {len(results)} keys saved to '{output_file}' (interrupted)")
            except Exception as e:
                print(f"[-] Failed to save partial results: {str(e)}", file=sys.stderr)
        raise
    
    except Exception as e:
        print(f"[-] Error processing {target}:{port}: {str(e)}", file=sys.stderr)
        if verbose:
            import traceback
            traceback.print_exc()
        return False
    
    finally:
        tap._disconnect()


def read_targets_from_file(filepath: str) -> List[str]:
    """Read targets from file (one per line)"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            targets = [line.strip() for line in f if line.strip()]
        return targets
    except Exception as e:
        print(f"[-] Error reading targets file '{filepath}': {str(e)}", file=sys.stderr)
        return []


def print_usage():
    """Print usage information"""
    print(BANNER)
    print("==========================================================================")
    print("\nUsage:")
    print("  memtap.py -t TARGET [OPTIONS]")
    print("  memtap.py -f FILE [OPTIONS]")
    
    print("\nRequired Arguments:")
    print("  -t, --target TARGET              Target memcached host (mutually exclusive with -f)")
    print("  -f, --file-with-targets FILE     File containing targets, one per line")
    
    print("\nConnection Options:")
    print("  -p, --port PORT                  Memcached port (default: 11211)")
    print("  -x, --proxy PROXY                Example: socks5:127.0.0.1:5555")
    
    print("\nOutput Options:")
    print("  -o, --output FILE                Output filename (default: TARGET_dump.txt)")
    print("  -v, --verbose                    Enable verbose output with debug information")
    
    print("\nPerformance Options:")
    print("  -l, --limit-requests NUM         Limit number of values per target (default: no limit)")
    print("  -d, --delay SECONDS              Delay between requests in seconds (default: 1.0)")
    print("  -T, --threads NUM                Number of threads for parallel dumping (default: 1)")
    
    print("\nUtility Options:")
    print("  -V, --only-check-version         Only check memcached version for each target")
    print("  -c, --only-count-keys            Only count keys for each target")
    print("  -h, --help                       Show this help message")
    
    print("\nExamples:")
    print("  # Basic dump of a single target")
    print("  memtap.py -t 192.168.1.100")
    print()
    print("  # Dump with custom port and output file")
    print("  memtap.py -t 192.168.1.100 -p 11211 -o results.txt")
    print()
    print("  # Dump multiple targets from file through socks proxy")
    print("  memtap.py -f targets.txt -x socks5:127.0.0.1:1080 -v")
    print()
    print("  # Limited dump with custom delay and threading")
    print("  memtap.py -t 192.168.1.100 -l 100 -d 0.5 -T 4 -v")
    print()
    print("  # Check version of memcached instance")
    print("  memtap.py -t 192.168.1.100 -V")
    print()
    print("  # Count keys in memcached instance")
    print("  memtap.py -t 192.168.1.100 -c")
    print()
    print("  # Fast dump with minimal delay and multiple threads")
    print("  memtap.py -t 192.168.1.100 -d 0.1 -T 8 -o fast_dump.txt")
    print()
    print("  # Dump through HTTP proxy with verbose output")
    print("  memtap.py -t 192.168.1.100 -x http:127.0.0.1:8080 -v")


def main():
    parser = argparse.ArgumentParser(
        description='MemTap - Utility for dumping all data from memcached instances',
        add_help=False
    )
    
    parser.add_argument('-t', '--target', help='Target memcached host')
    parser.add_argument('-f', '--file-with-targets', dest='file_targets', help='File containing targets')
    parser.add_argument('-p', '--port', type=int, default=11211, help='Memcached port (default: 11211)')
    parser.add_argument('-x', '--proxy', help='Proxy in format TYPE:HOST:PORT')
    parser.add_argument('-o', '--output', help='Output filename')
    parser.add_argument('-l', '--limit-requests', type=int, dest='limit', help='Limit number of values per target')
    parser.add_argument('-d', '--delay', type=float, default=1.0, help='Delay between requests (default: 1.0)')
    parser.add_argument('-T', '--threads', type=int, default=1, help='Number of threads (default: 1)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument('-V', '--only-check-version', action='store_true', dest='check_version', help='Only check version for each target')
    parser.add_argument('-c', '--only-count-keys', action='store_true', dest='count_keys', help='Only count keys for each target')
    parser.add_argument('-h', '--help', action='store_true', help='Show help message')
    
    args = parser.parse_args()
    
    # Handle help
    if args.help:
        print_usage()
        sys.exit(0)
    
    # Check that -t/--target is specified only once
    target_count = sys.argv.count('-t') + sys.argv.count('--target')
    if target_count > 1:
        print("[-] Error: -t/--target can only be specified once", file=sys.stderr)
        print_usage()
        sys.exit(1)
    
    # Validate arguments
    if not args.target and not args.file_targets:
        print("[-] Error: Either -t/--target or -f/--file-with-targets must be specified", file=sys.stderr)
        print_usage()
        sys.exit(1)
    
    if args.target and args.file_targets:
        print("[-] Error: -t/--target and -f/--file-with-targets cannot be used simultaneously", file=sys.stderr)
        print_usage()
        sys.exit(1)
    
    if args.delay < 0:
        print("[-] Error: Delay must be non-negative", file=sys.stderr)
        sys.exit(1)
    
    if args.threads < 1:
        print("[-] Error: Number of threads must be at least 1", file=sys.stderr)
        sys.exit(1)
    
    # Show banner
    print(BANNER)
    
    # Print initialization and used parameters
    print("[+] Initialising")
    if args.target:
        print(f"[+] Target = {args.target}")
    if args.file_targets:
        print(f"[+] File with targets = {args.file_targets}")
    print(f"[+] Port = {args.port}")
    if args.proxy:
        print(f"[+] Proxy = {args.proxy}")
    if args.output:
        print(f"[+] Output = {args.output}")
    if args.limit:
        print(f"[+] Limit requests = {args.limit}")
    print(f"[+] Delay = {args.delay}")
    print(f"[+] Threads = {args.threads}")
    if args.verbose:
        print(f"[+] Verbose = enabled")
    if args.check_version:
        print(f"[+] Only check version = enabled")
    if args.count_keys:
        print(f"[+] Only count keys = enabled")
    print()
    
    # Get targets
    if args.file_targets:
        targets = read_targets_from_file(args.file_targets)
        if not targets:
            print("[-] No valid targets found in file", file=sys.stderr)
            sys.exit(1)
        if args.verbose:
            print(f"[*] Loaded {len(targets)} targets from file")
    else:
        targets = [args.target]
    
    # Validate that -V and -c are not used together
    if args.check_version and args.count_keys:
        print("[-] Error: -V/--only-check-version and -c/--only-count-keys cannot be used simultaneously", file=sys.stderr)
        sys.exit(1)
    
    # Process each target
    success_count = 0
    interrupted = False
    
    try:
        for target in targets:
            if args.check_version:
                # Only check version
                try:
                    if check_version(target, args.port, args.proxy, args.verbose):
                        success_count += 1
                except KeyboardInterrupt:
                    interrupted = True
                    print(f"\n[!] Interrupted by user", file=sys.stderr)
                    break
            elif args.count_keys:
                # Only count keys
                try:
                    if count_keys(target, args.port, args.proxy, args.verbose):
                        success_count += 1
                except KeyboardInterrupt:
                    interrupted = True
                    print(f"\n[!] Interrupted by user", file=sys.stderr)
                    break
            else:
                # Full dump process
                try:
                    if process_target(
                        target, args.port, args.proxy, args.output,
                        args.limit, args.delay, args.threads, args.verbose
                    ):
                        success_count += 1
                except KeyboardInterrupt:
                    # process_target already handles saving partial results
                    interrupted = True
                    break
    except KeyboardInterrupt:
        interrupted = True
        print(f"\n[!] Interrupted by user", file=sys.stderr)
    
    if interrupted:
        if args.check_version or args.count_keys:
            print(f"\n[!] Interrupted: {success_count}/{len(targets)} targets processed before interruption")
        else:
            print(f"\n[!] Interrupted: {success_count}/{len(targets)} targets processed before interruption")
        sys.exit(130)  # Standard exit code for SIGINT/Ctrl+C
    else:
        if args.check_version or args.count_keys:
            print(f"\n[*] Completed: {success_count}/{len(targets)} targets processed successfully")
        else:
            print(f"\n[*] Completed: {success_count}/{len(targets)} targets processed successfully")
        
        if success_count == 0:
            sys.exit(1)


if __name__ == '__main__':
    main()

