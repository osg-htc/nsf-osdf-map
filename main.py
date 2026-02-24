from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.dsl import Search, A
import os
import sys
import duckdb
import pyarrow as pa
import json
import math
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import queue
from urllib.parse import urlparse
import urllib.request
import plotly.graph_objects as go
import argparse
from collections import defaultdict

INDEX = "adstash-ospool-transfer-*"
BATCH_SIZE = 10000  # Larger batches for efficiency
NUM_SLICES = 8  # Number of parallel scroll slices


def create_duckdb_table(conn):
    """Create the DuckDB table for transfer records."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transfers (
            id VARCHAR PRIMARY KEY,
            record_time TIMESTAMP,
            schedd_name VARCHAR,
            cluster_id BIGINT,
            proc_id BIGINT,
            startd_slot VARCHAR,
            startd_name VARCHAR,
            epoch_ad_type VARCHAR,
            run_instance_id VARCHAR,
            transfer_class VARCHAR,
            epoch_write_date TIMESTAMP,
            num_shadow_starts INTEGER,
            machine_attr_name0 VARCHAR,
            machine_attr_glidein_resourcename0 VARCHAR,
            attempts INTEGER,
            pelican_client_version VARCHAR,
            attempt INTEGER,
            final_attempt BOOLEAN,
            endpoint VARCHAR,
            server_version VARCHAR,
            attempt_time DOUBLE,
            attempt_file_bytes BIGINT,
            time_to_first_byte DOUBLE,
            attempt_end_time TIMESTAMP,
            data_age VARCHAR,
            transfer_url VARCHAR,
            transfer_type VARCHAR,
            transfer_end_time TIMESTAMP,
            transfer_success BOOLEAN,
            transfer_file_name VARCHAR,
            transfer_protocol VARCHAR,
            transfer_file_bytes BIGINT,
            transfer_start_time TIMESTAMP,
            transfer_total_bytes BIGINT,
            metadata_hostname VARCHAR,
            metadata_username VARCHAR,
            metadata_runtime BIGINT,
            metadata_version VARCHAR,
            metadata_platform VARCHAR,
            metadata_source VARCHAR,
            metadata_history_runtime BIGINT,
            metadata_history_host_version VARCHAR,
            metadata_history_host_platform VARCHAR,
            metadata_history_host_machine VARCHAR,
            metadata_history_host_name VARCHAR
        )
    """)


def parse_timestamp(value):
    """Convert Unix timestamp to datetime."""
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (ValueError, TypeError):
        return None


def transform_doc(doc):
    """Transform an Elasticsearch document to a dict for PyArrow."""
    src = doc.to_dict()
    metadata = src.get("metadata") or {}

    return {
        "id": doc.meta.id,
        "record_time": parse_timestamp(src.get("RecordTime")),
        "schedd_name": src.get("ScheddName"),
        "cluster_id": src.get("ClusterId"),
        "proc_id": src.get("ProcId"),
        "startd_slot": src.get("StartdSlot"),
        "startd_name": src.get("StartdName"),
        "epoch_ad_type": src.get("epochadtype"),
        "run_instance_id": src.get("runinstanceid"),
        "transfer_class": src.get("transferclass"),
        "epoch_write_date": parse_timestamp(src.get("EpochWriteDate")),
        "num_shadow_starts": src.get("NumShadowStarts"),
        "machine_attr_name0": src.get("machineattrname0"),
        "machine_attr_glidein_resourcename0": src.get("machineattrglidein_resourcename0"),
        "attempts": src.get("Attempts"),
        "pelican_client_version": src.get("PelicanClientVersion"),
        "attempt": src.get("Attempt"),
        "final_attempt": src.get("FinalAttempt"),
        "endpoint": src.get("Endpoint"),
        "server_version": src.get("ServerVersion"),
        "attempt_time": src.get("AttemptTime"),
        "attempt_file_bytes": src.get("AttemptFileBytes"),
        "time_to_first_byte": src.get("TimeToFirstByte"),
        "attempt_end_time": parse_timestamp(src.get("AttemptEndTime")),
        "data_age": src.get("dataage"),
        "transfer_url": src.get("TransferUrl"),
        "transfer_type": src.get("TransferType"),
        "transfer_end_time": parse_timestamp(src.get("TransferEndTime")),
        "transfer_success": src.get("TransferSuccess"),
        "transfer_file_name": src.get("TransferFileName"),
        "transfer_protocol": src.get("TransferProtocol"),
        "transfer_file_bytes": src.get("TransferFileBytes"),
        "transfer_start_time": parse_timestamp(src.get("TransferStartTime")),
        "transfer_total_bytes": src.get("TransferTotalBytes"),
        "metadata_hostname": metadata.get("condor_adstash_hostname"),
        "metadata_username": metadata.get("condor_adstash_username"),
        "metadata_runtime": metadata.get("condor_adstash_runtime"),
        "metadata_version": metadata.get("condor_adstash_version"),
        "metadata_platform": metadata.get("condor_adstash_platform"),
        "metadata_source": metadata.get("condor_adstash_source"),
        "metadata_history_runtime": metadata.get("condor_history_runtime"),
        "metadata_history_host_version": metadata.get("condor_history_host_version"),
        "metadata_history_host_platform": metadata.get("condor_history_host_platform"),
        "metadata_history_host_machine": metadata.get("condor_history_host_machine"),
        "metadata_history_host_name": metadata.get("condor_history_host_name"),
    }


def parse_date_arg(value):
    """Parse a date argument as either ISO 8601 string or Unix timestamp."""
    # Try parsing as ISO 8601 format (e.g., 2025-12-01T06:00:00.000Z)
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except ValueError:
        pass

    # Fall back to Unix timestamp
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"Invalid date format: {value}. Use ISO 8601 (e.g., 2025-12-01T06:00:00.000Z) or Unix timestamp.")


def fetch_slice(client, index, start, end, slice_id, max_slices, result_queue, progress_counter, counter_lock):
    """Fetch documents for a single slice and put batches into the queue."""
    s = Search(using=client, index=index) \
        .filter("range", RecordTime={"gte": start, "lte": end}) \
        .extra(slice={"id": slice_id, "max": max_slices})

    batch = []
    local_count = 0

    for doc in s.scan():
        batch.append(transform_doc(doc))
        local_count += 1

        if len(batch) >= BATCH_SIZE:
            result_queue.put(batch)
            with counter_lock:
                progress_counter[0] += len(batch)
            batch = []

    # Put remaining docs
    if batch:
        result_queue.put(batch)
        with counter_lock:
            progress_counter[0] += len(batch)

    return local_count


def batch_to_arrow(batch):
    """Convert a batch of dicts to a PyArrow table."""
    # Transpose list of dicts to dict of lists
    if not batch:
        return None

    columns = {key: [] for key in batch[0].keys()}
    for row in batch:
        for key, value in row.items():
            columns[key].append(value)

    return pa.table(columns)


def main():
    load_dotenv()

    if len(sys.argv) < 3:
        print("Usage: python main.py <start> <end>")
        print("  Dates can be ISO 8601 (e.g., 2025-12-01T06:00:00.000Z) or Unix timestamps")
        sys.exit(1)

    try:
        start = parse_date_arg(sys.argv[1])
        end = parse_date_arg(sys.argv[2])
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    print(f"Querying transfers from {datetime.fromtimestamp(start, tz=timezone.utc)} to {datetime.fromtimestamp(end, tz=timezone.utc)}")

    # Connect to Elasticsearch
    client = Elasticsearch(
        hosts=os.getenv("ELASTIC_HOST"),
        basic_auth=(os.getenv("ELASTIC_USER"), os.getenv("ELASTIC_PASSWORD")),
    )

    # Build the search query with date range filter on RecordTime
    s = Search(using=client, index=INDEX) \
        .filter("range", RecordTime={"gte": start, "lte": end})

    # Get total count
    total = s.count()
    print(f"Found {total} documents to process")
    print(f"Using {NUM_SLICES} parallel slices with batch size {BATCH_SIZE}")

    # Connect to DuckDB
    conn = duckdb.connect("transfers.duckdb")
    create_duckdb_table(conn)

    # Queue for batches from workers
    result_queue = queue.Queue(maxsize=NUM_SLICES * 2)

    # Shared progress counter
    progress_counter = [0]
    counter_lock = threading.Lock()

    # Track inserted count
    inserted = 0
    start_time = datetime.now()

    # Start worker threads for parallel sliced scroll
    with ThreadPoolExecutor(max_workers=NUM_SLICES) as executor:
        # Submit all slice fetchers
        futures = [
            executor.submit(
                fetch_slice, client, INDEX, start, end,
                slice_id, NUM_SLICES, result_queue, progress_counter, counter_lock
            )
            for slice_id in range(NUM_SLICES)
        ]

        # Process batches as they arrive
        completed_slices = 0
        while completed_slices < NUM_SLICES or not result_queue.empty():
            # Check if any futures completed
            done_futures = [f for f in futures if f.done()]
            completed_slices = len(done_futures)

            try:
                batch = result_queue.get(timeout=0.5)
                arrow_table = batch_to_arrow(batch)
                if arrow_table:
                    conn.execute("INSERT OR REPLACE INTO transfers SELECT * FROM arrow_table")
                    inserted += len(batch)

                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = inserted / elapsed if elapsed > 0 else 0
                    eta = (total - inserted) / rate if rate > 0 else 0
                    print(f"Inserted {inserted:,}/{total:,} ({100*inserted/total:.1f}%) - {rate:,.0f} docs/sec - ETA: {eta/60:.1f} min")
            except queue.Empty:
                continue

        # Wait for all futures to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error in slice fetcher: {e}")

    # Drain any remaining items in the queue
    while not result_queue.empty():
        try:
            batch = result_queue.get_nowait()
            arrow_table = batch_to_arrow(batch)
            if arrow_table:
                conn.execute("INSERT OR REPLACE INTO transfers SELECT * FROM arrow_table")
                inserted += len(batch)
        except queue.Empty:
            break

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\nDone! Inserted {inserted:,} documents in {elapsed:.1f} seconds ({inserted/elapsed:,.0f} docs/sec)")
    print("Data saved to transfers.duckdb")

    conn.close()


SERVERS_URL = "https://osdf-director.osg-htc.org/api/v1.0/director_ui/servers"
RESOURCE_GROUP_URL = "https://map.osg-htc.org/api/resource_group_summary"


def _fetch_json(url, cache_file):
    """Return parsed JSON from cache_file if present, otherwise fetch from url and cache it."""
    if os.path.exists(cache_file):
        with open(cache_file) as f:
            return json.load(f)
    print(f"Fetching {url} ...")
    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read().decode())
    with open(cache_file, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Cached to {cache_file}")
    return data


def load_server_info(servers_file="servers.json"):
    """Load server info, fetching from the OSDF director API if the local cache is missing."""
    servers = _fetch_json(SERVERS_URL, servers_file)
    if not servers:
        return {}
    
    # Index by hostname (extracted from URL) and also by full netloc
    server_by_host = {}
    for server in servers:
        url = server.get("url", "")
        web_url = server.get("webUrl", "")
        
        for u in [url, web_url]:
            if u:
                parsed = urlparse(u)
                # Index by full netloc (hostname:port)
                if parsed.netloc:
                    server_by_host[parsed.netloc] = server
                # Also index by hostname only (without port)
                hostname = parsed.netloc.split(":")[0]
                if hostname:
                    server_by_host[hostname] = server
    
    return server_by_host


def lookup_server(endpoint, server_info):
    """Look up server info for an endpoint, trying multiple match strategies."""
    # Try exact match first (hostname:port)
    if endpoint in server_info:
        return server_info[endpoint]
    
    # Try hostname only (strip port)
    hostname = endpoint.split(":")[0]
    if hostname in server_info:
        return server_info[hostname]
    
    return None


def load_resource_groups(resource_file="resource_group_summary.json"):
    """Load resource group info, fetching from the OSG topology API if the local cache is missing.

    Returns a dict keyed by both resource group names (e.g., 'MTState-Tempest')
    and individual resource/CE names (e.g., 'MTState-Tempest-CE1'), so that
    direct CE-name lookups work without needing a separate reverse-lookup step.
    """
    raw = _fetch_json(RESOURCE_GROUP_URL, resource_file)
    if not raw:
        return {}

    # Start with the group-name-keyed entries, then index each individual resource
    # by its own name so CE names (e.g. 'MTState-Tempest-CE1') resolve directly.
    lookup = dict(raw)
    for group_data in raw.values():
        resources = group_data.get("Resources") or {}
        resource_list = resources.get("Resource", [])
        if isinstance(resource_list, dict):
            resource_list = [resource_list]
        for resource in resource_list:
            res_name = resource.get("Name")
            if res_name and res_name not in lookup:
                lookup[res_name] = group_data

    return lookup


def load_topology_institutions(topology_file="topology_institutions.json"):
    """Load institution info from the OSG topology API, with caching."""
    raw = _fetch_json("https://topology-institutions.osg-htc.org/api/institution_ids", topology_file)
    if not raw:
        return {}
    return {i['id']: i for i in raw}


def get_site_info(site_name, resource_groups):
    """Get site info from resource groups, with flexible matching."""
    # Try exact match
    if site_name in resource_groups:
        return resource_groups[site_name]
    
    # Try case-insensitive match
    for key in resource_groups:
        if key.lower() == site_name.lower():
            return resource_groups[key]
    
    return None


def format_bytes(num_bytes):
    """Format bytes into human-readable string (KB, MB, GB, TB)."""
    if num_bytes is None:
        return "N/A"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} EB"


def endpoint_to_site_map_from_elasticsearch(start=None, end=None):
    """Create a map from endpoint to execution sites using Elasticsearch aggregations.
    
    This is more efficient than fetching all documents and aggregating in DuckDB.
    
    Args:
        start: Optional start timestamp (Unix timestamp)
        end: Optional end timestamp (Unix timestamp)
    
    Returns:
        dict: endpoint -> list of (site, count, total_bytes, unique_objects)
    """
    load_dotenv()
    
    # Connect to Elasticsearch with increased timeout for large aggregations
    client = Elasticsearch(
        hosts=os.getenv("ELASTIC_HOST"),
        basic_auth=(os.getenv("ELASTIC_USER"), os.getenv("ELASTIC_PASSWORD")),
        request_timeout=120,  # 2 minutes for large aggregation queries
    )
    
    # Build the search query
    s = Search(using=client, index=INDEX)
    
    # Add date range filter if provided
    if start is not None and end is not None:
        s = s.filter("range", RecordTime={"gte": start, "lte": end})
    
    # Add filter to exclude null values
    # These field names match the actual document structure
    s = s.filter("exists", field="Endpoint")
    s = s.filter("exists", field="machineattrglidein_resourcename0")
    
    # Build aggregations:
    # 1. Group by endpoint (terms aggregation)
    # 2. Within each endpoint, group by site (nested terms aggregation)
    # 3. For each endpoint+site combo, calculate:
    #    - count (doc_count from terms aggregation)
    #    - sum of transfer_total_bytes (sum aggregation)
    #    - distinct count of transfer_file_name (cardinality aggregation)
    #
    # Note: Field names use .keyword suffix for text fields. If your Elasticsearch
    # mapping uses different field names or types, you may need to adjust these.
    
    # Build the aggregation structure using the aggregation builder
    # Create the nested aggregation: sites grouped by machineattrglidein_resourcename0
    # Note: Size limits are set to 1000 to avoid timeouts. If you have more than 1000
    # unique endpoints or sites per endpoint, increase these values (but queries will take longer)
    # 
    # Based on the document structure, trying direct field names first.
    # If aggregations fail, may need to use .indexed versions (e.g., machineattrglidein_resourcename0.indexed)
    # or check Elasticsearch mapping for keyword field names
    site_terms = A('terms', field='machineattrglidein_resourcename0', size=1000)
    site_terms.metric('total_bytes', 'sum', field='TransferTotalBytes', missing=0)
    # For cardinality, use the field directly - Elasticsearch will use the appropriate field type
    site_terms.metric('unique_objects', 'cardinality', field='TransferFileName', missing=0)
    
    # Create the top-level aggregation: endpoints grouped by Endpoint
    endpoint_terms = A('terms', field='Endpoint', size=1000)
    endpoint_terms.bucket('sites', site_terms)
    
    # Add to search
    s.aggs.bucket('endpoints', endpoint_terms)
    
    # Execute the search (we don't need the actual documents, just aggregations)
    s = s[:0]  # Set size to 0 to only return aggregations
    
    print("Executing Elasticsearch aggregation query (this may take a minute for large date ranges)...")
    try:
        response = s.execute()
        print("Aggregation query completed successfully")
    except Exception as e:
        print(f"Error executing Elasticsearch query: {e}")
        # Try to print the query for debugging
        try:
            query_dict = s.to_dict()
            print(f"Query structure: {json.dumps(query_dict, indent=2)[:500]}...")  # First 500 chars
        except:
            pass
        raise
    
    # Build map: endpoint -> list of (site, count, total_bytes, unique_objects)
    endpoint_map = {}
    
    # Access aggregations - use dict interface which is more reliable
    try:
        aggs_dict = response.aggregations.to_dict() if hasattr(response.aggregations, 'to_dict') else {}
    except Exception as e:
        print(f"Error accessing aggregations: {e}")
        print(f"Response type: {type(response.aggregations)}")
        print(f"Response aggregations dir: {dir(response.aggregations)}")
        # Try to access directly
        try:
            aggs_dict = dict(response.aggregations) if hasattr(response.aggregations, '__iter__') else {}
        except:
            aggs_dict = {}
    
    if 'endpoints' not in aggs_dict:
        print(f"Warning: No 'endpoints' aggregation found. Available aggregations: {list(aggs_dict.keys())}")
        if aggs_dict:
            print(f"Full aggregation response: {json.dumps(aggs_dict, indent=2)}")
        return endpoint_map
    
    endpoints_buckets = aggs_dict.get('endpoints', {}).get('buckets', [])
    
    if not endpoints_buckets:
        print("Warning: Aggregation returned 0 endpoints. This could mean:")
        print("  - No data matches the date range/filters")
        print("  - Field names don't match Elasticsearch mapping (check Endpoint.keyword and machineattrglidein_resourcename0.keyword)")
        print("  - Fields might not have .keyword subfields")
        # Try a simple test query to see if we can get any data
        print("\nChecking sample documents to diagnose the issue...")
        test_query = Search(using=client, index=INDEX)[:5]
        if start is not None and end is not None:
            test_query = test_query.filter("range", RecordTime={"gte": start, "lte": end})
        try:
            test_response = test_query.execute()
            print(f"Found {len(test_response.hits)} sample documents in date range")
            if test_response.hits:
                sample_doc = test_response.hits[0].to_dict()
                print(f"\nSample document fields: {sorted(sample_doc.keys())}")
                print(f"  Endpoint field: {sample_doc.get('Endpoint', 'NOT FOUND')}")
                print(f"  machineattrglidein_resourcename0 field: {sample_doc.get('machineattrglidein_resourcename0', 'NOT FOUND')}")
                print(f"  TransferTotalBytes field: {sample_doc.get('TransferTotalBytes', 'NOT FOUND')}")
                print(f"  TransferFileName field: {sample_doc.get('TransferFileName', 'NOT FOUND')}")
                
                # Check if exists filters are working
                exists_query = Search(using=client, index=INDEX)[:0]
                if start is not None and end is not None:
                    exists_query = exists_query.filter("range", RecordTime={"gte": start, "lte": end})
                exists_query = exists_query.filter("exists", field="Endpoint")
                exists_count = exists_query.count()
                print(f"\nDocuments with Endpoint field: {exists_count:,}")
                
                exists_query2 = Search(using=client, index=INDEX)[:0]
                if start is not None and end is not None:
                    exists_query2 = exists_query2.filter("range", RecordTime={"gte": start, "lte": end})
                exists_query2 = exists_query2.filter("exists", field="machineattrglidein_resourcename0")
                exists_count2 = exists_query2.count()
                print(f"Documents with machineattrglidein_resourcename0 field: {exists_count2:,}")
        except Exception as e:
            print(f"  Could not fetch sample document: {e}")
        return endpoint_map
    
    for bucket in endpoints_buckets:
        endpoint = bucket.get('key')
        if endpoint is None:
            continue
        endpoint_map[endpoint] = []
        
        sites_buckets = bucket.get('sites', {}).get('buckets', [])
        for site_bucket in sites_buckets:
            site = site_bucket.get('key')
            if site is None:
                continue
            count = site_bucket.get('doc_count', 0)
            total_bytes = int(site_bucket.get('total_bytes', {}).get('value', 0))
            unique_objects = site_bucket.get('unique_objects', {}).get('value', 0)
            
            endpoint_map[endpoint].append((site, count, total_bytes, unique_objects))
        
        # Sort sites by count descending
        endpoint_map[endpoint].sort(key=lambda x: x[1], reverse=True)
    
    print(f"Found {len(endpoint_map)} endpoints with {sum(len(sites) for sites in endpoint_map.values())} total site mappings")
    
    return endpoint_map


def endpoint_to_site_map():
    """Create a map from endpoint to execution sites (glidein resource names).
    
    This version reads from DuckDB. For better performance with large datasets,
    use endpoint_to_site_map_from_elasticsearch() instead.
    """
    conn = duckdb.connect("transfers.duckdb", read_only=True)
    
    result = conn.execute("""
        SELECT 
            endpoint, 
            machine_attr_glidein_resourcename0 as site, 
            COUNT(*) as count,
            SUM(COALESCE(transfer_total_bytes, 0)) as total_bytes,
            COUNT(DISTINCT transfer_file_name) as unique_objects
        FROM transfers
        WHERE endpoint IS NOT NULL AND machine_attr_glidein_resourcename0 IS NOT NULL
        GROUP BY endpoint, machine_attr_glidein_resourcename0
        ORDER BY endpoint, count DESC
    """).fetchall()
    
    # Build map: endpoint -> list of (site, count, total_bytes, unique_objects)
    endpoint_map = {}
    for endpoint, site, count, total_bytes, unique_objects in result:
        if endpoint not in endpoint_map:
            endpoint_map[endpoint] = []
        endpoint_map[endpoint].append((site, count, total_bytes, unique_objects))
    
    conn.close()
    return endpoint_map


def build_endpoint_site_data(use_elasticsearch=False, start=None, end=None):
    """Build the endpoint to site mapping with server info as a data structure.
    
    Args:
        use_elasticsearch: If True, use Elasticsearch aggregations instead of DuckDB
        start: Optional start timestamp for Elasticsearch query (Unix timestamp)
        end: Optional end timestamp for Elasticsearch query (Unix timestamp)
    """
    if use_elasticsearch:
        endpoint_map = endpoint_to_site_map_from_elasticsearch(start=start, end=end)
    else:
        endpoint_map = endpoint_to_site_map()
    server_info = load_server_info()
    resource_groups = load_resource_groups()
    topology_institutions = load_topology_institutions()
    
    result = []

    for endpoint in sorted(endpoint_map.keys()):
        # Filter out bare numeric site names (e.g. '2') which are bad/missing data
        sites = [s for s in endpoint_map[endpoint] if not s[0].isdigit()]
        if not sites:
            continue
        total_transfers = sum(count for _, count, _, _ in sites)
        total_bytes = sum(bytes_transferred for _, _, bytes_transferred, _ in sites)
        total_objects = sum(objects for _, _, _, objects in sites)
        
        # Look up server info
        server = lookup_server(endpoint, server_info)
        
        # Build site entries with resource group info
        institution_entries = defaultdict(lambda: {"count": 0, "bytes": 0, "objects": 0, "sites": set()})
        sites_with_location = 0
        sites_without_location = 0
        unmatched_sites = []  # Track sites that don't match for debugging
        
        for site_name, count, bytes_transferred, unique_objects in sites:
            
            # Look up site info from resource groups
            rg = get_site_info(site_name, resource_groups)
            if rg is None:
                unmatched_sites.append(site_name)
                continue
            institution = topology_institutions.get(rg["Facility"].get("InstitutionID"))
            if institution is None:
                sites_without_location += 1
                print(f"Warning: Institution ID {rg['Facility'].get('InstitutionID')} not found in topology_institutions.json for site {site_name}")
                return

            existing_entry = institution_entries[institution['name']]
            new_entry = {
                "count": existing_entry["count"] + count,
                "bytes": existing_entry["bytes"] + bytes_transferred,
                "objects": existing_entry["objects"] + unique_objects,
                "sites": {*existing_entry["sites"], site_name},
                **institution
            }

            institution_entries[institution['name']] = new_entry

            sites_with_location += 1
        
        # Print summary for debugging (only if there are unmatched sites)
        if unmatched_sites and len(unmatched_sites) <= 10:
            print(f"  Endpoint {endpoint}: Sites not found in resource_group_summary.json: {unmatched_sites[:5]}")
        
        entry = {
            "endpoint": endpoint,
            "total_transfers": total_transfers,
            "total_bytes": total_bytes,
            "total_objects": total_objects,
            "institutions": [*institution_entries.values()]
        }
        
        if server:
            entry["server"] = {
                "name": server.get("name"),
                "type": server.get("type"),
                "latitude": server.get("latitude"),
                "longitude": server.get("longitude"),
                "namespaces": server.get("namespacePrefixes", []),
                "health_status": server.get("healthStatus"),
                "server_status": server.get("serverStatus"),
                "version": server.get("version"),
                "url": server.get("url"),
            }

        # Run through and convert all institution sets to lists for JSON serialization
        for institution_data in entry["institutions"]:
            institution_data["sites"] = list(institution_data["sites"])
        
        result.append(entry)

    return result


def write_endpoint_site_json(data, output_file="endpoint_site_map.json"):
    """Write the endpoint to site mapping to a JSON file."""
    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Wrote {len(data)} endpoints to {output_file}")


def print_endpoint_site_map(data):
    """Print the endpoint to site mapping with server info."""
    print(f"\nEndpoint to Site Map ({len(data)} endpoints)\n")
    print("=" * 80)
    
    for entry in data:
        endpoint = entry["endpoint"]
        total_transfers = entry["total_transfers"]
        sites = entry["sites"]
        server = entry.get("server")
        
        print(f"\n{endpoint}")
        
        if server:
            print(f"  Server Name: {server.get('name', 'N/A')}")
            print(f"  Type: {server.get('type', 'N/A')}")
            print(f"  Location: ({server.get('latitude', 'N/A')}, {server.get('longitude', 'N/A')})")
            namespaces = server.get('namespaces', [])
            if namespaces:
                print(f"  Namespaces: {', '.join(namespaces[:5])}")
                if len(namespaces) > 5:
                    print(f"              ... and {len(namespaces) - 5} more")
            print(f"  Status: {server.get('health_status', 'N/A')} / {server.get('server_status', 'N/A')}")
            print(f"  Version: {server.get('version', 'N/A')}")
        else:
            print("  Server Info: Not found in servers.json")
        
        print(f"  Total Transfers: {total_transfers:,}")
        print(f"  Sites ({len(sites)}):")
        for site in sites[:10]:  # Show top 10
            print(f"    - {site['name']}: {site['count']:,} ({site['percentage']:.1f}%)")
        if len(sites) > 10:
            print(f"    ... and {len(sites) - 10} more")


def lat_to_mercator_y(lat):
    """Convert latitude to Web Mercator y-coordinate."""
    # Clamp latitude to avoid infinity at poles
    lat = max(-85, min(85, lat))
    lat_rad = math.radians(lat)
    return math.log(math.tan(math.pi / 4 + lat_rad / 2))


def mercator_y_to_lat(y):
    """Convert Web Mercator y-coordinate back to latitude."""
    return math.degrees(2 * math.atan(math.exp(y)) - math.pi / 2)


def calculate_midpoint_mercator(lat1, lon1, lat2, lon2):
    """Calculate the visual midpoint of a line on a Web Mercator map.
    
    Simple lat/lon averaging doesn't work because Web Mercator distorts
    latitude non-linearly. This converts to Mercator, averages, then converts back.
    """
    # Longitude is linear in Mercator, so simple average works
    mid_lon = (lon1 + lon2) / 2
    
    # Latitude needs to be averaged in Mercator space
    y1 = lat_to_mercator_y(lat1)
    y2 = lat_to_mercator_y(lat2)
    mid_y = (y1 + y2) / 2
    mid_lat = mercator_y_to_lat(mid_y)
    
    return mid_lat, mid_lon


def calculate_arrow_angle(start_lat, start_lon, end_lat, end_lon):
    """Calculate the angle (in degrees) for an arrow pointing from start to end.
    
    Uses Mercator projection coordinates for accurate angle on the map.
    Plotly's triangle marker points up (north) at angle=0, and rotates clockwise.
    """
    # Calculate angle in Mercator space - both dx and dy must be in same units
    # Mercator x = R * lon_radians, Mercator y = R * ln(tan(π/4 + lat/2))
    # So we convert lon to radians to match the scale of mercator_y
    dx = math.radians(end_lon - start_lon)
    dy = lat_to_mercator_y(end_lat) - lat_to_mercator_y(start_lat)
    
    if dx == 0 and dy == 0:
        return 0
    
    # atan2: 0=east, 90=north, 180=west, -90=south (counter-clockwise from east)
    angle_rad = math.atan2(dy, dx)
    angle_deg = math.degrees(angle_rad)
    
    # Plotly: 0=north(up), 90=east(right), 180=south(down), 270=west(left) (clockwise from north)
    # Convert: plotly_angle = 90 - atan2_angle
    return 90 - angle_deg


def create_transfer_map(output_file="transfer_map.html", show_connections=False, 
                        site_filter=None, min_transfers=0, use_elasticsearch=False, start=None, end=None):
    """Create an interactive geographic map of OSDF transfers using Plotly.
    
    Args:
        output_file: Output HTML file path
        show_connections: Whether to show transfer flow lines with arrows
        site_filter: If set, only show data related to this site (partial match)
        min_transfers: Minimum number of transfers to include a connection
        use_elasticsearch: If True, use Elasticsearch aggregations instead of DuckDB
        start: Optional start timestamp for Elasticsearch query (Unix timestamp)
        end: Optional end timestamp for Elasticsearch query (Unix timestamp)
    """
    data = build_endpoint_site_data(use_elasticsearch=use_elasticsearch, start=start, end=end)
    
    # Filter by site if specified
    if site_filter:
        site_filter_lower = site_filter.lower()
        filtered_data = []
        
        # Collect all unique site names for debugging
        all_site_names = set()
        for entry in data:
            for site in entry["sites"]:
                all_site_names.add(site["name"])
        
        for entry in data:
            # Filter sites that match the filter
            # Try multiple matching strategies:
            # 1. Direct name match (case-insensitive substring)
            # 2. Facility name match
            # 3. City name match
            # 4. Common abbreviations/aliases
            matching_sites = []
            for s in entry["sites"]:
                site_name_lower = s["name"].lower()
                matches = (
                    site_filter_lower in site_name_lower or
                    site_name_lower in site_filter_lower or  # Also check reverse
                    (s.get("facility") and site_filter_lower in s.get("facility", "").lower()) or
                    (s.get("location") and s["location"].get("city") and 
                     site_filter_lower in s["location"]["city"].lower())
                )
                
                if matches:
                    matching_sites.append(s)
            if matching_sites:
                # Keep only matching sites
                entry_copy = entry.copy()
                entry_copy["sites"] = matching_sites
                entry_copy["total_transfers"] = sum(s["count"] for s in matching_sites)
                filtered_data.append(entry_copy)
        data = filtered_data
        
        if not filtered_data:
            print(f"Warning: No endpoints matched site filter '{site_filter}'")
            print(f"Available site names (showing first 20): {sorted(list(all_site_names))[:20]}")
            if len(all_site_names) > 20:
                print(f"... and {len(all_site_names) - 20} more sites")
        else:
            print(f"Filtered to {len(data)} endpoints matching site '{site_filter}'")
    
    # Filter to only endpoints with server location data
    endpoints_with_location = [e for e in data if e.get("server") and e["server"].get("latitude")]
    
    if not endpoints_with_location:
        print("No endpoints with location data found")
        return
    
    # Find max transfers for scaling
    max_transfers = max(e["total_transfers"] for e in endpoints_with_location)
    
    # Collect data for plotly traces
    origin_lats, origin_lons, origin_texts, origin_hovers = [], [], [], []
    cache_lats, cache_lons, cache_texts, cache_hovers = [], [], [], []
    site_lats, site_lons, site_texts, site_hovers = [], [], [], []
    
    # Arrow line coordinates and widths (using None to separate lines)
    arrow_lats, arrow_lons = [], []
    arrow_line_widths = []  # Width for each line segment (based on transfer count)
    # Arrow head markers (fixed pixel size, won't scale with zoom) - placed at midpoint
    arrow_head_lats, arrow_head_lons, arrow_head_angles = [], [], []
    arrow_head_hovers = []  # Hover text for each arrow
    
    # Track unique sites to avoid duplicates
    seen_sites = set()
    
    # Track coordinates to offset overlapping markers
    coord_counts = {}
    
    # Process each endpoint
    for entry in endpoints_with_location:
        server = entry["server"]
        endpoint_lat = server["latitude"]
        endpoint_lon = server["longitude"]
        
        # Skip if coordinates are 0,0 (likely missing data)
        if endpoint_lat == 0 and endpoint_lon == 0:
            continue
        
        # Apply small offset for overlapping markers
        coord_key = (round(endpoint_lat, 4), round(endpoint_lon, 4))
        if coord_key in coord_counts:
            coord_counts[coord_key] += 1
            n = coord_counts[coord_key]
            angle = n * 2.4  # Golden angle for even distribution
            offset = 0.15 * math.sqrt(n)
            endpoint_lat += offset * math.cos(angle)
            endpoint_lon += offset * math.sin(angle)
        else:
            coord_counts[coord_key] = 0
        
        server_type = server.get("type", "Unknown")
        server_name = server.get('name', entry['endpoint'])
        
        # Build hover text
        top_sites = entry["sites"][:5]
        sites_list = "<br>".join(
            f"  • {s['name']}: {s['count']:,} ({s['percentage']:.1f}%)"
            for s in top_sites
        )
        namespaces = server.get("namespaces", [])[:3]
        namespaces_str = ", ".join(namespaces) if namespaces else "N/A"
        
        hover_text = (
            f"<b>{server_name}</b><br>"
            f"Endpoint: {entry['endpoint']}<br>"
            f"Type: {server_type}<br>"
            f"Total Transfers: {entry['total_transfers']:,}<br>"
            f"Namespaces: {namespaces_str}<br>"
            f"Version: {server.get('version', 'N/A')}<br>"
            f"Status: {server.get('health_status', 'N/A')}<br>"
            f"<b>Top Sites:</b><br>{sites_list}"
        )
        
        # Add to appropriate list based on server type
        if server_type == "Origin":
            origin_lats.append(endpoint_lat)
            origin_lons.append(endpoint_lon)
            origin_texts.append(server_name)
            origin_hovers.append(hover_text)
        else:
            cache_lats.append(endpoint_lat)
            cache_lons.append(endpoint_lon)
            cache_texts.append(server_name)
            cache_hovers.append(hover_text)
        
        # Process sites and connections
        for site in entry["sites"]:
            site_name = site["name"]
            site_loc = site.get("location")
            
            if site_loc and site_loc.get("latitude") and site_loc.get("longitude"):
                site_lat = site_loc["latitude"]
                site_lon = site_loc["longitude"]
                
                # Skip 0,0 coordinates
                if site_lat == 0 and site_lon == 0:
                    continue
                
                # Add site marker (only once per site)
                if site_name not in seen_sites:
                    seen_sites.add(site_name)
                    site_lats.append(site_lat)
                    site_lons.append(site_lon)
                    site_texts.append(site_name)
                    site_hovers.append(
                        f"<b>{site_name}</b><br>"
                        f"Facility: {site.get('facility', 'N/A')}<br>"
                        f"Location: {site_loc.get('city', 'N/A')}, {site_loc.get('country', 'N/A')}<br>"
                        f"Description: {site_loc.get('description', 'N/A')}"
                    )
                
                # Add connection arrow (if enabled and meets threshold)
                if show_connections and site["count"] >= min_transfers:
                    transfer_count = site["count"]
                    transfer_bytes = site.get("bytes", 0)
                    transfer_objects = site.get("objects", 0)
                    
                    # Add line segment (with None separator)
                    arrow_lats.extend([site_lat, endpoint_lat, None])
                    arrow_lons.extend([site_lon, endpoint_lon, None])
                    arrow_line_widths.append(transfer_count)
                    
                    # Calculate midpoint in Mercator projection for accurate placement
                    mid_lat, mid_lon = calculate_midpoint_mercator(
                        site_lat, site_lon, endpoint_lat, endpoint_lon
                    )
                    
                    # Add arrow head marker at midpoint (fixed pixel size)
                    angle = calculate_arrow_angle(site_lat, site_lon, endpoint_lat, endpoint_lon)
                    arrow_head_lats.append(mid_lat)
                    arrow_head_lons.append(mid_lon)
                    arrow_head_angles.append(angle)
                    
                    # Create hover text for this transfer connection
                    arrow_hover = (
                        f"<b>Transfer Flow</b><br>"
                        f"From: {site_name}<br>"
                        f"To: {server_name}<br>"
                        f"Transfers: {transfer_count:,}<br>"
                        f"Objects: {transfer_objects:,}<br>"
                        f"Data Transferred: {format_bytes(transfer_bytes)}"
                    )
                    arrow_head_hovers.append(arrow_hover)
    
    # Create figure
    fig = go.Figure()
    
    # Add connection lines (arrows) first so they're behind markers
    if show_connections and arrow_line_widths:
        # Calculate line widths based on transfer counts (log scale for better visualization)
        max_count = max(arrow_line_widths) if arrow_line_widths else 1
        min_count = min(arrow_line_widths) if arrow_line_widths else 1
        
        # Scale line widths: min 1px, max 8px, using log scale
        def scale_width(count):
            if max_count == min_count:
                return 3
            # Log scale to handle large differences in transfer counts
            log_range = math.log(max_count) - math.log(min_count) if min_count > 0 else 1
            log_val = math.log(count) - math.log(min_count) if min_count > 0 else 0
            normalized = log_val / log_range if log_range > 0 else 0.5
            return 1 + normalized * 7  # Range: 1 to 8 pixels
        
        # Create individual line traces for variable widths
        # Group into segments (each line is: start, end, None)
        line_idx = 0
        for i, count in enumerate(arrow_line_widths):
            # Extract coordinates for this line segment
            start_lat = arrow_lats[line_idx]
            start_lon = arrow_lons[line_idx]
            end_lat = arrow_lats[line_idx + 1]
            end_lon = arrow_lons[line_idx + 1]
            line_idx += 3  # Skip past None separator
            
            width = scale_width(count)
            
            fig.add_trace(go.Scattermap(
                lat=[start_lat, end_lat],
                lon=[start_lon, end_lon],
                mode="lines",
                line=dict(width=width, color="#9b59b6"),
                name="Transfer Flow" if i == 0 else None,
                showlegend=(i == 0),
                hoverinfo="skip",
                opacity=0.6
            ))
        
        # Add arrow head markers at midpoint (fixed pixel size - won't scale with zoom)
        if arrow_head_lats:
            fig.add_trace(go.Scattermap(
                lat=arrow_head_lats,
                lon=arrow_head_lons,
                mode="markers",
                marker=dict(
                    symbol="triangle",
                    size=10,
                    color="#9b59b6",
                    angle=arrow_head_angles,  # Rotate each marker to point in direction
                    allowoverlap=True
                ),
                hovertext=arrow_head_hovers,
                hoverinfo="text",
                name="Flow Direction",
                showlegend=True
            ))
    
    # Add execution sites (green circles)
    if site_lats:
        fig.add_trace(go.Scattermap(
            lat=site_lats,
            lon=site_lons,
            mode="markers",
            marker=dict(size=12, color="#27ae60", opacity=0.8),
            text=site_texts,
            hovertext=site_hovers,
            hoverinfo="text",
            name="Execution Sites"
        ))
    
    # Add origin servers (red circles)
    if origin_lats:
        fig.add_trace(go.Scattermap(
            lat=origin_lats,
            lon=origin_lons,
            mode="markers",
            marker=dict(size=14, color="#e74c3c", opacity=0.7),
            text=origin_texts,
            hovertext=origin_hovers,
            hoverinfo="text",
            name="Origin Servers"
        ))
    
    # Add cache servers (blue circles)
    if cache_lats:
        fig.add_trace(go.Scattermap(
            lat=cache_lats,
            lon=cache_lons,
            mode="markers",
            marker=dict(size=14, color="#3498db", opacity=0.7),
            text=cache_texts,
            hovertext=cache_hovers,
            hoverinfo="text",
            name="Cache Servers"
        ))
    
    # Build title with filter info
    title_parts = ["OSDF Transfer Map"]
    if site_filter:
        title_parts.append(f"Site: {site_filter}")
    if min_transfers > 0:
        title_parts.append(f"Min transfers: {min_transfers:,}")
    title = " | ".join(title_parts)
    
    # Update layout
    fig.update_layout(
        title=title,
        map=dict(
            style="carto-positron",
            center=dict(lat=39.8283, lon=-98.5795),
            zoom=3.5
        ),
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255, 255, 255, 0.8)"
        ),
        margin=dict(l=0, r=0, t=40, b=0),
        height=800
    )
    
    # Save to HTML
    fig.write_html(output_file)
    print(f"\nCreated interactive map: {output_file}")
    print(f"  - {len(endpoints_with_location)} OSDF endpoints with location data")
    print(f"  - {len(data) - len(endpoints_with_location)} endpoints without location data")
    print(f"  - {len(seen_sites)} unique execution sites with location data")
    if site_filter:
        print(f"  - Filtered to sites matching: '{site_filter}'")
    if min_transfers > 0:
        print(f"  - Only showing connections with >= {min_transfers:,} transfers")
    if show_connections:
        print("  - Connection arrows show data flow direction (site → server)")


def parse_geomap_args():
    """Parse command-line arguments for the geomap command."""
    parser = argparse.ArgumentParser(description="Create OSDF transfer map")
    parser.add_argument("command", help="Command to run (geomap)")
    parser.add_argument("--connections", action="store_true",
                        help="Show transfer flow lines with directional arrows")
    parser.add_argument("--site", type=str, default=None,
                        help="Filter to a specific site (e.g., 'Wisconsin', 'Nebraska', 'UCSD')")
    parser.add_argument("--min-transfers", type=int, default=0,
                        help="Minimum transfers to show a connection (reduces noise)")
    parser.add_argument("-o", "--output", type=str, default="transfer_map.html",
                        help="Output file path (default: transfer_map.html)")
    parser.add_argument("--use-elasticsearch", action="store_true",
                        help="Use Elasticsearch aggregations instead of DuckDB (more efficient for large datasets)")
    parser.add_argument("--start", type=str, default=None,
                        help="Start date for Elasticsearch query (ISO 8601 or Unix timestamp)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date for Elasticsearch query (ISO 8601 or Unix timestamp)")
    return parser.parse_args()


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        cmd = sys.argv[1]
        if cmd == "map":
            # Parse optional arguments for map command
            parser = argparse.ArgumentParser(description="Print and save endpoint-to-site mapping")
            parser.add_argument("command", help="Command to run (map)")
            parser.add_argument("--use-elasticsearch", action="store_true",
                                help="Use Elasticsearch aggregations instead of DuckDB")
            parser.add_argument("--start", type=str, default=None,
                                help="Start date for Elasticsearch query (ISO 8601 or Unix timestamp)")
            parser.add_argument("--end", type=str, default=None,
                                help="End date for Elasticsearch query (ISO 8601 or Unix timestamp)")
            args = parser.parse_args()

            # Parse dates if provided
            start_ts = None
            end_ts = None
            if args.start:
                start_ts = parse_date_arg(args.start)
            if args.end:
                end_ts = parse_date_arg(args.end)

            # Build data once and share it between print and JSON write
            data = build_endpoint_site_data(
                use_elasticsearch=args.use_elasticsearch,
                start=start_ts,
                end=end_ts,
            )
            # print_endpoint_site_map(data)
            write_endpoint_site_json(data)
        elif cmd == "geomap":
            args = parse_geomap_args()

            # Parse dates if provided
            start_ts = None
            end_ts = None
            if args.start:
                start_ts = parse_date_arg(args.start)
            if args.end:
                end_ts = parse_date_arg(args.end)

            create_transfer_map(
                output_file=args.output,
                show_connections=args.connections,
                site_filter=args.site,
                min_transfers=args.min_transfers,
                use_elasticsearch=args.use_elasticsearch,
                start=start_ts,
                end=end_ts
            )
        elif len(sys.argv) >= 3:
            # Treat as: python main.py <start> <end>  (Elasticsearch ingest)
            main()
        else:
            print(f"Unknown command: {cmd}")
            print("Usage:")
            print("  python main.py map                  - Print and save endpoint-to-site mapping")
            print("  python main.py map --use-elasticsearch --start <date> --end <date> - Use ES aggregations")
            print("  python main.py geomap               - Create geographic map")
            print("  python main.py geomap --connections - Create map with directional flow arrows")
            print("  python main.py geomap --site Wisconsin - Filter to specific site")
            print("  python main.py geomap --min-transfers 100 - Only show connections with 100+ transfers")
            print("  python main.py geomap --use-elasticsearch --start <date> --end <date> - Use ES aggregations")
            print("  python main.py <start> <end>        - Fetch data from Elasticsearch into DuckDB")
            sys.exit(1)
    else:
        print("Usage:")
        print("  python main.py map                  - Print and save endpoint-to-site mapping")
        print("  python main.py geomap               - Create geographic map")
        print("  python main.py <start> <end>        - Fetch data from Elasticsearch into DuckDB")
        sys.exit(1)
