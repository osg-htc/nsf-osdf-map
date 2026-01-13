from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.dsl import Search
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
import folium
from folium.plugins import MarkerCluster, AntPath
import argparse

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


def load_server_info(servers_file="servers.json"):
    """Load server info from servers.json and index by hostname."""
    try:
        with open(servers_file) as f:
            servers = json.load(f)
    except FileNotFoundError:
        print(f"Warning: {servers_file} not found")
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
    """Load resource group info (execution sites) from resource_group_summary.json."""
    try:
        with open(resource_file) as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Warning: {resource_file} not found")
        return {}


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


def endpoint_to_site_map():
    """Create a map from endpoint to execution sites (glidein resource names)."""
    conn = duckdb.connect("transfers.duckdb", read_only=True)
    
    result = conn.execute("""
        SELECT endpoint, machine_attr_glidein_resourcename0 as site, COUNT(*) as count
        FROM transfers
        WHERE endpoint IS NOT NULL AND machine_attr_glidein_resourcename0 IS NOT NULL
        GROUP BY endpoint, machine_attr_glidein_resourcename0
        ORDER BY endpoint, count DESC
    """).fetchall()
    
    # Build map: endpoint -> list of (site, count)
    endpoint_map = {}
    for endpoint, site, count in result:
        if endpoint not in endpoint_map:
            endpoint_map[endpoint] = []
        endpoint_map[endpoint].append((site, count))
    
    conn.close()
    return endpoint_map


def build_endpoint_site_data():
    """Build the endpoint to site mapping with server info as a data structure."""
    endpoint_map = endpoint_to_site_map()
    server_info = load_server_info()
    resource_groups = load_resource_groups()
    
    result = []
    
    for endpoint in sorted(endpoint_map.keys()):
        sites = endpoint_map[endpoint]
        total_transfers = sum(count for _, count in sites)
        
        # Look up server info
        server = lookup_server(endpoint, server_info)
        
        # Build site entries with resource group info
        site_entries = []
        for site_name, count in sites:
            site_entry = {
                "name": site_name,
                "count": count,
                "percentage": round(100 * count / total_transfers, 2)
            }
            
            # Look up site info from resource groups
            rg = get_site_info(site_name, resource_groups)
            if rg and rg.get("Site"):
                site_data = rg["Site"]
                site_entry["location"] = {
                    "latitude": site_data.get("Latitude"),
                    "longitude": site_data.get("Longitude"),
                    "city": site_data.get("City"),
                    "country": site_data.get("Country"),
                    "description": site_data.get("Description"),
                }
                if rg.get("Facility"):
                    site_entry["facility"] = rg["Facility"].get("Name")
            
            site_entries.append(site_entry)
        
        entry = {
            "endpoint": endpoint,
            "total_transfers": total_transfers,
            "sites": site_entries
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
        
        result.append(entry)
    
    return result


def write_endpoint_site_json(output_file="endpoint_site_map.json"):
    """Write the endpoint to site mapping to a JSON file."""
    data = build_endpoint_site_data()
    
    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"Wrote {len(data)} endpoints to {output_file}")


def print_endpoint_site_map():
    """Print the endpoint to site mapping with server info."""
    data = build_endpoint_site_data()
    
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


def create_transfer_map(output_file="transfer_map.html", show_connections=False, 
                        site_filter=None, min_transfers=0):
    """Create an interactive geographic map of OSDF transfers.
    
    Args:
        output_file: Output HTML file path
        show_connections: Whether to show transfer flow lines
        site_filter: If set, only show data related to this site (partial match)
        min_transfers: Minimum number of transfers to include a connection
    """
    data = build_endpoint_site_data()
    resource_groups = load_resource_groups()
    
    # Filter by site if specified
    if site_filter:
        site_filter_lower = site_filter.lower()
        filtered_data = []
        for entry in data:
            # Filter sites that match the filter
            matching_sites = [
                s for s in entry["sites"]
                if site_filter_lower in s["name"].lower() or 
                   (s.get("facility") and site_filter_lower in s.get("facility", "").lower()) or
                   (s.get("location") and s["location"].get("city") and 
                    site_filter_lower in s["location"]["city"].lower())
            ]
            if matching_sites:
                # Keep only matching sites
                entry_copy = entry.copy()
                entry_copy["sites"] = matching_sites
                entry_copy["total_transfers"] = sum(s["count"] for s in matching_sites)
                filtered_data.append(entry_copy)
        data = filtered_data
        print(f"Filtered to {len(data)} endpoints matching site '{site_filter}'")
    
    # Filter to only endpoints with server location data
    endpoints_with_location = [e for e in data if e.get("server") and e["server"].get("latitude")]
    
    if not endpoints_with_location:
        print("No endpoints with location data found")
        return
    
    # Create the map centered on US
    m = folium.Map(location=[39.8283, -98.5795], zoom_start=4, tiles="CartoDB positron")
    
    # Create feature groups for layers
    origin_layer = folium.FeatureGroup(name="Origin Servers")
    cache_layer = folium.FeatureGroup(name="Cache Servers")
    site_layer = folium.FeatureGroup(name="Execution Sites")
    connection_layer = folium.FeatureGroup(name="Transfer Connections", show=False)
    
    # Find max transfers for scaling
    max_transfers = max(e["total_transfers"] for e in endpoints_with_location)
    
    # Color by server type
    type_colors = {
        "Origin": "#e74c3c",  # Red
        "Cache": "#3498db",   # Blue
    }
    
    # Track unique sites to avoid duplicates
    seen_sites = set()
    
    # Track coordinates to offset overlapping markers
    coord_counts = {}
    
    # Add markers for each endpoint
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
            # Spiral pattern offset
            n = coord_counts[coord_key]
            angle = n * 2.4  # Golden angle for even distribution
            offset = 0.15 * math.sqrt(n)  # Increase distance with count
            endpoint_lat += offset * math.cos(angle)
            endpoint_lon += offset * math.sin(angle)
        else:
            coord_counts[coord_key] = 0
        
        # Scale radius by transfer volume (sqrt scale for better visualization)
        transfer_ratio = entry["total_transfers"] / max_transfers
        radius = 5 + 25 * math.sqrt(transfer_ratio)  # 5-30 range
        
        # Get color by type
        server_type = server.get("type", "Unknown")
        color = type_colors.get(server_type, "#95a5a6")
        
        # Build popup content
        top_sites = entry["sites"][:5]
        sites_html = "".join(
            f"<tr><td>{s['name']}</td><td>{s['count']:,}</td><td>{s['percentage']:.1f}%</td></tr>"
            for s in top_sites
        )
        
        namespaces = server.get("namespaces", [])[:3]
        namespaces_str = ", ".join(namespaces) if namespaces else "N/A"
        
        popup_html = f"""
        <div style="min-width: 300px">
            <h4>{server.get('name', entry['endpoint'])}</h4>
            <p><b>Endpoint:</b> {entry['endpoint']}</p>
            <p><b>Type:</b> {server_type}</p>
            <p><b>Total Transfers:</b> {entry['total_transfers']:,}</p>
            <p><b>Namespaces:</b> {namespaces_str}</p>
            <p><b>Version:</b> {server.get('version', 'N/A')}</p>
            <p><b>Status:</b> {server.get('health_status', 'N/A')}</p>
            <h5>Top Sites:</h5>
            <table style="width:100%">
                <tr><th>Site</th><th>Count</th><th>%</th></tr>
                {sites_html}
            </table>
        </div>
        """
        
        # Choose layer based on server type
        target_layer = origin_layer if server_type == "Origin" else cache_layer
        
        folium.CircleMarker(
            location=[endpoint_lat, endpoint_lon],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.6,
            popup=folium.Popup(popup_html, max_width=400),
            tooltip=f"{server.get('name', entry['endpoint'])}: {entry['total_transfers']:,} transfers"
        ).add_to(target_layer)
        
        # Add site markers and connection lines
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
                    
                    site_popup = f"""
                    <div style="min-width: 200px">
                        <h4>{site_name}</h4>
                        <p><b>Facility:</b> {site.get('facility', 'N/A')}</p>
                        <p><b>Location:</b> {site_loc.get('city', 'N/A')}, {site_loc.get('country', 'N/A')}</p>
                        <p><b>Description:</b> {site_loc.get('description', 'N/A')}</p>
                    </div>
                    """
                    
                    folium.CircleMarker(
                        location=[site_lat, site_lon],
                        radius=6,
                        color="#27ae60",  # Green
                        fill=True,
                        fill_color="#27ae60",
                        fill_opacity=0.7,
                        popup=folium.Popup(site_popup, max_width=300),
                        tooltip=site_name
                    ).add_to(site_layer)
                
                # Add connection line (if enabled)
                if show_connections and site["count"] >= min_transfers:
                    # Line weight based on percentage
                    weight = 1 + (site["percentage"] / 20)
                    
                    # Use AntPath for animated directional arrows
                    # Direction: site → endpoint (execution site to origin/cache server)
                    AntPath(
                        locations=[[site_lat, site_lon], [endpoint_lat, endpoint_lon]],
                        weight=weight,
                        color="#9b59b6",  # Purple
                        pulse_color="#ffffff",
                        delay=1000,  # Animation speed
                        dash_array=[10, 20],  # Dash pattern for arrow effect
                        tooltip=f"{site_name} → {server.get('name', entry['endpoint'])}: {site['count']:,} ({site['percentage']:.1f}%)"
                    ).add_to(connection_layer)
    
    # Add layers to map
    origin_layer.add_to(m)
    cache_layer.add_to(m)
    site_layer.add_to(m)
    connection_layer.add_to(m)
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    # Build legend with dynamic info
    filter_info = ""
    if site_filter:
        filter_info += f"<p style='font-size: 11px; color: #2980b9;'><b>Site filter:</b> {site_filter}</p>"
    if min_transfers > 0:
        filter_info += f"<p style='font-size: 11px; color: #2980b9;'><b>Min transfers:</b> {min_transfers:,}</p>"
    
    legend_html = f"""
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000; 
                background-color: white; padding: 10px; border-radius: 5px;
                border: 2px solid grey; font-size: 14px;">
        <p><b>OSDF Transfer Map</b></p>
        <p><span style="color: #e74c3c;">●</span> Origin Server</p>
        <p><span style="color: #3498db;">●</span> Cache Server</p>
        <p><span style="color: #27ae60;">●</span> Execution Site</p>
        <p><span style="color: #9b59b6;">―▸</span> Transfer Flow (animated)</p>
        <p style="font-size: 11px; color: grey;">Endpoint size = transfer volume</p>
        <p style="font-size: 11px; color: grey;">Arrows show direction of data flow</p>
        <p style="font-size: 11px; color: grey;">Use layer control (top right) to toggle</p>
        {filter_info}
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    
    # Save the map
    m.save(output_file)
    print(f"\nCreated interactive map: {output_file}")
    print(f"  - {len(endpoints_with_location)} OSDF endpoints with location data")
    print(f"  - {len(data) - len(endpoints_with_location)} endpoints without location data")
    print(f"  - {len(seen_sites)} unique execution sites with location data")
    if site_filter:
        print(f"  - Filtered to sites matching: '{site_filter}'")
    if min_transfers > 0:
        print(f"  - Only showing connections with >= {min_transfers:,} transfers")
    if show_connections:
        print(f"  - Connection arrows show data flow direction (site → server)")


def parse_geomap_args():
    """Parse command-line arguments for the geomap command."""
    parser = argparse.ArgumentParser(description="Create OSDF transfer map")
    parser.add_argument("command", help="Command to run (geomap)")
    parser.add_argument("--connections", action="store_true",
                        help="Show transfer flow lines with animated arrows")
    parser.add_argument("--site", type=str, default=None,
                        help="Filter to a specific site (e.g., 'Wisconsin', 'Nebraska', 'UCSD')")
    parser.add_argument("--min-transfers", type=int, default=0,
                        help="Minimum transfers to show a connection (reduces noise)")
    parser.add_argument("-o", "--output", type=str, default="transfer_map.html",
                        help="Output file path (default: transfer_map.html)")
    return parser.parse_args()


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        cmd = sys.argv[1]
        if cmd == "map":
            print_endpoint_site_map()
            write_endpoint_site_json()
        elif cmd == "geomap":
            args = parse_geomap_args()
            create_transfer_map(
                output_file=args.output,
                show_connections=args.connections,
                site_filter=args.site,
                min_transfers=args.min_transfers
            )
        else:
            print(f"Unknown command: {cmd}")
            print("Usage:")
            print("  python main.py map                  - Print and save endpoint-to-site mapping")
            print("  python main.py geomap               - Create geographic map")
            print("  python main.py geomap --connections - Create map with animated flow arrows")
            print("  python main.py geomap --site Wisconsin - Filter to specific site")
            print("  python main.py geomap --min-transfers 100 - Only show connections with 100+ transfers")
            print("  python main.py <start> <end>        - Fetch data from Elasticsearch")
            sys.exit(1)
    else:
        main()
