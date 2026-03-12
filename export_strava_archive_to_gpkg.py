#!/usr/bin/env python3
"""
strava_to_gpkg.py

Converts a Strava export directory (containing .fit.gz, .gpx, .fit, .tcx, etc.)
into a single GeoPackage file with two layers:
  - tracks:   LineString geometries, one per activity
  - waypoints: Point geometries for individual track points (with timestamps)

Usage:
    python strava_to_gpkg.py <strava_export_dir> [output.gpkg]

Dependencies:
    pip install geopandas fiona shapely fitparse lxml
    pip install garmin-fit-sdk    # optional, fallback for FIT files
"""

import argparse
import gzip
import logging
import os
import shutil
import sys
import tempfile
import traceback
from datetime import datetime, timezone
from pathlib import Path

# ── third-party ──────────────────────────────────────────────────────────────
try:
    import geopandas as gpd
    from shapely.geometry import LineString, Point
    import pandas as pd
    import fiona
except ImportError:
    sys.exit(
        "Missing required packages.\n"
        "Install with: pip install geopandas fiona shapely pandas lxml"
    )

try:
    from lxml import etree as ET
except ImportError:
    import xml.etree.ElementTree as ET

try:
    import fitparse
    HAS_FITPARSE = True
except ImportError:
    HAS_FITPARSE = False
    logging.warning("fitparse not found – .fit/.fit.gz files will be skipped. "
                    "Install with: pip install fitparse")

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

CRS = "EPSG:4326"

# ─────────────────────────────────────────────────────────────────────────────
# GPX parser
# ─────────────────────────────────────────────────────────────────────────────
GPX_NS = {
    "gpx":  "http://www.topografix.com/GPX/1/1",
    "gpx10":"http://www.topografix.com/GPX/1/0",
    "gpxtpx":"http://www.garmin.com/xmlschemas/TrackPointExtension/v1",
}

def _ns(tag, ns="gpx"):
    return f"{{{GPX_NS[ns]}}}{tag}"


def parse_gpx(filepath: Path) -> list[dict]:
    """Return a list of activity dicts parsed from a GPX file."""
    try:
        raw = _read_xml_bytes(filepath)
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        log.warning("  Could not parse %s: %s", filepath.name, e)
        return []

    # Detect namespace
    tag = root.tag  # e.g. {http://www.topografix.com/GPX/1/1}gpx
    ns_uri = tag[1:tag.index("}")] if tag.startswith("{") else ""
    ns = {"g": ns_uri} if ns_uri else {}

    def find_all(element, path):
        if ns:
            return element.findall(path.replace("/", "/g:").replace("g:g:", "g:"),
                                   namespaces=ns)
        return element.findall(path)

    def find_one(element, path):
        if ns:
            return element.find(path.replace("/", "/g:").replace("g:g:", "g:"),
                                namespaces=ns)
        return element.find(path)

    activities = []

    for trk in find_all(root, "g:trk"):
        name_el = find_one(trk, "g:name")
        act_type_el = find_one(trk, "g:type")
        name = name_el.text.strip() if name_el is not None and name_el.text else filepath.stem
        act_type = act_type_el.text.strip() if act_type_el is not None else "unknown"

        coords = []
        timestamps = []
        elevations = []

        for trkpt in find_all(trk, "g:trkseg/g:trkpt"):
            try:
                lat = float(trkpt.get("lat"))
                lon = float(trkpt.get("lon"))
            except (TypeError, ValueError):
                continue

            ele_el = find_one(trkpt, "g:ele")
            ele = float(ele_el.text) if ele_el is not None and ele_el.text else None

            time_el = find_one(trkpt, "g:time")
            ts = None
            if time_el is not None and time_el.text:
                try:
                    ts = datetime.fromisoformat(
                        time_el.text.replace("Z", "+00:00")
                    )
                except ValueError:
                    pass

            coords.append((lon, lat))
            elevations.append(ele)
            timestamps.append(ts)

        if len(coords) < 2:
            continue

        start_time = next((t for t in timestamps if t is not None), None)
        end_time   = next((t for t in reversed(timestamps) if t is not None), None)
        distance_m = _haversine_total(coords)

        activities.append({
            "source_file": filepath.name,
            "name": name,
            "activity_type": act_type,
            "start_time": start_time,
            "end_time": end_time,
            "duration_s": (end_time - start_time).total_seconds()
                          if start_time and end_time else None,
            "distance_m": round(distance_m, 1),
            "point_count": len(coords),
            "geometry": LineString(coords),
            "_coords": coords,
            "_times": timestamps,
            "_eles": elevations,
        })

    return activities


# ─────────────────────────────────────────────────────────────────────────────
# FIT parser
# ─────────────────────────────────────────────────────────────────────────────

def parse_fit(filepath: Path) -> list[dict]:
    """Return a list of activity dicts parsed from a FIT file."""
    if not HAS_FITPARSE:
        return []

    try:
        fit = fitparse.FitFile(str(filepath))
    except Exception as e:
        log.warning("  Could not open FIT %s: %s", filepath.name, e)
        return []

    coords = []
    timestamps = []
    elevations = []
    sport = "unknown"

    try:
        for record in fit.get_messages():
            name = record.name

            if name == "sport":
                data = {f.name: f.value for f in record}
                sport = data.get("sport") or data.get("name") or "unknown"

            elif name == "record":
                data = {f.name: f.value for f in record}
                lat_raw = data.get("position_lat")
                lon_raw = data.get("position_long")
                if lat_raw is None or lon_raw is None:
                    continue
                # FIT uses semicircles
                lat = lat_raw * (180 / 2**31)
                lon = lon_raw * (180 / 2**31)
                ts  = data.get("timestamp")
                alt = data.get("altitude") or data.get("enhanced_altitude")

                coords.append((lon, lat))
                timestamps.append(ts if isinstance(ts, datetime) else None)
                elevations.append(float(alt) if alt is not None else None)

    except Exception as e:
        log.warning("  Error reading records from %s: %s", filepath.name, e)

    if len(coords) < 2:
        return []

    start_time = next((t for t in timestamps if t is not None), None)
    end_time   = next((t for t in reversed(timestamps) if t is not None), None)
    distance_m = _haversine_total(coords)

    return [{
        "source_file": filepath.name,
        "name": filepath.stem,
        "activity_type": str(sport),
        "start_time": start_time,
        "end_time": end_time,
        "duration_s": (end_time - start_time).total_seconds()
                      if start_time and end_time else None,
        "distance_m": round(distance_m, 1),
        "point_count": len(coords),
        "geometry": LineString(coords),
        "_coords": coords,
        "_times": timestamps,
        "_eles": elevations,
    }]


# ─────────────────────────────────────────────────────────────────────────────
# TCX parser (basic)
# ─────────────────────────────────────────────────────────────────────────────
TCX_NS_URI = "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"

def _read_xml_bytes(filepath: Path) -> bytes:
    """Read a file and strip any leading junk (BOM, whitespace, nulls) before the XML declaration or root tag."""
    raw = filepath.read_bytes()
    # Strip UTF-8 BOM if present
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    # Strip UTF-16 BOMs
    elif raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        raw = raw[2:]
    # Find the start of the XML content (either <?xml or <T or <t for TCX root)
    start = raw.find(b"<?xml")
    if start == -1:
        start = raw.find(b"<")
    if start > 0:
        raw = raw[start:]
    return raw


def parse_tcx(filepath: Path) -> list[dict]:
    try:
        raw = _read_xml_bytes(filepath)
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        log.warning("  Could not parse %s: %s", filepath.name, e)
        return []

    ns = {"t": TCX_NS_URI}

    activities = []
    for activity in root.findall(".//t:Activity", ns):
        sport = activity.get("Sport", "unknown")
        id_el = activity.find("t:Id", ns)
        name  = id_el.text.strip() if id_el is not None and id_el.text else filepath.stem

        coords, timestamps, elevations = [], [], []

        for tp in activity.findall(".//t:Trackpoint", ns):
            lat_el = tp.find("t:Position/t:LatitudeDegrees", ns)
            lon_el = tp.find("t:Position/t:LongitudeDegrees", ns)
            if lat_el is None or lon_el is None:
                continue
            try:
                lat, lon = float(lat_el.text), float(lon_el.text)
            except ValueError:
                continue

            alt_el  = tp.find("t:AltitudeMeters", ns)
            time_el = tp.find("t:Time", ns)
            ele = float(alt_el.text)  if alt_el  is not None and alt_el.text  else None
            ts  = None
            if time_el is not None and time_el.text:
                try:
                    ts = datetime.fromisoformat(time_el.text.replace("Z", "+00:00"))
                except ValueError:
                    pass

            coords.append((lon, lat))
            elevations.append(ele)
            timestamps.append(ts)

        if len(coords) < 2:
            continue

        start_time = next((t for t in timestamps if t is not None), None)
        end_time   = next((t for t in reversed(timestamps) if t is not None), None)
        distance_m = _haversine_total(coords)

        activities.append({
            "source_file": filepath.name,
            "name": name,
            "activity_type": sport,
            "start_time": start_time,
            "end_time": end_time,
            "duration_s": (end_time - start_time).total_seconds()
                          if start_time and end_time else None,
            "distance_m": round(distance_m, 1),
            "point_count": len(coords),
            "geometry": LineString(coords),
            "_coords": coords,
            "_times": timestamps,
            "_eles": elevations,
        })

    return activities


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _haversine_total(coords: list[tuple]) -> float:
    """Approximate total track length in metres."""
    import math
    R = 6_371_000
    total = 0.0
    for (lon1, lat1), (lon2, lat2) in zip(coords, coords[1:]):
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlam = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
        total += 2 * R * math.asin(min(1, math.sqrt(a)))
    return total


def decompress_gz(src: Path, dst_dir: Path) -> Path:
    """Decompress a .gz file into dst_dir, return the decompressed path."""
    inner_name = src.stem          # e.g. "abc.fit" from "abc.fit.gz"
    dst = dst_dir / inner_name
    with gzip.open(src, "rb") as f_in, open(dst, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    return dst


def collect_files(directory: Path) -> list[Path]:
    """Walk directory and return all recognisable activity files."""
    supported = {".gpx", ".fit", ".tcx"}
    files = []
    for p in sorted(directory.rglob("*")):
        if not p.is_file():
            continue
        suffixes = "".join(p.suffixes).lower()
        name_lower = p.name.lower()
        if name_lower.endswith(".fit.gz") or name_lower.endswith(".gpx.gz") \
                or name_lower.endswith(".tcx.gz"):
            files.append(p)
        elif p.suffix.lower() in supported:
            files.append(p)
    return files


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def build_geopackage(export_dir: Path, output_gpkg: Path) -> None:
    tmpdir = Path(tempfile.mkdtemp(prefix="strava_gpkg_"))

    all_tracks    = []
    all_waypoints = []

    files = collect_files(export_dir)
    log.info("Found %d candidate file(s) in %s", len(files), export_dir)

    for filepath in files:
        name_lower = filepath.name.lower()
        log.info("Processing: %s", filepath.name)

        # ── decompress ────────────────────────────────────────────────────
        if name_lower.endswith(".gz"):
            try:
                filepath = decompress_gz(filepath, tmpdir)
                name_lower = filepath.name.lower()
            except Exception as e:
                log.warning("  Failed to decompress %s: %s", filepath.name, e)
                continue

        # ── parse ─────────────────────────────────────────────────────────
        try:
            if name_lower.endswith(".gpx"):
                activities = parse_gpx(filepath)
            elif name_lower.endswith(".fit"):
                activities = parse_fit(filepath)
            elif name_lower.endswith(".tcx"):
                activities = parse_tcx(filepath)
            else:
                log.debug("  Skipping unsupported type: %s", filepath.name)
                continue
        except Exception:
            log.warning("  Unexpected error parsing %s:\n%s",
                        filepath.name, traceback.format_exc())
            continue

        if not activities:
            log.warning("  No usable tracks found in %s", filepath.name)
            continue

        for act in activities:
            # track row
            track_row = {k: v for k, v in act.items()
                         if not k.startswith("_")}
            # ensure datetimes are timezone-aware strings for GPKG compatibility
            for tf in ("start_time", "end_time"):
                val = track_row.get(tf)
                if isinstance(val, datetime) and val.tzinfo is None:
                    track_row[tf] = val.replace(tzinfo=timezone.utc)
            all_tracks.append(track_row)

            # waypoint rows
            for i, (coord, ts, ele) in enumerate(
                    zip(act["_coords"], act["_times"], act["_eles"])):
                wp = {
                    "source_file":   act["source_file"],
                    "activity_name": act["name"],
                    "seq":           i,
                    "elevation_m":   ele,
                    "timestamp":     ts.replace(tzinfo=timezone.utc)
                                     if isinstance(ts, datetime) and ts.tzinfo is None
                                     else ts,
                    "geometry":      Point(coord),
                }
                all_waypoints.append(wp)

        log.info("  → %d track(s) added", len(activities))

    # ── assemble GeoDataFrames ────────────────────────────────────────────────
    if not all_tracks:
        log.error("No tracks were parsed. GeoPackage not created.")
        shutil.rmtree(tmpdir, ignore_errors=True)
        return

    tracks_gdf = gpd.GeoDataFrame(all_tracks, crs=CRS)
    tracks_gdf = tracks_gdf.drop(
        columns=[c for c in tracks_gdf.columns if c.startswith("_")],
        errors="ignore"
    )

    waypoints_gdf = gpd.GeoDataFrame(all_waypoints, crs=CRS) \
        if all_waypoints else None

    # ── write GeoPackage ──────────────────────────────────────────────────────
    log.info("Writing %d tracks to %s …", len(tracks_gdf), output_gpkg)
    tracks_gdf.to_file(str(output_gpkg), layer="tracks", driver="GPKG")

    if waypoints_gdf is not None and not waypoints_gdf.empty:
        log.info("Writing %d waypoints …", len(waypoints_gdf))
        waypoints_gdf.to_file(str(output_gpkg), layer="waypoints", driver="GPKG")

    shutil.rmtree(tmpdir, ignore_errors=True)
    log.info("Done! GeoPackage written to: %s", output_gpkg)

    # ── summary ───────────────────────────────────────────────────────────────
    print("\n── Summary ──────────────────────────────────────────────────")
    print(f"  Tracks    : {len(tracks_gdf)}")
    if waypoints_gdf is not None:
        print(f"  Waypoints : {len(waypoints_gdf)}")
    if "activity_type" in tracks_gdf.columns:
        type_counts = tracks_gdf["activity_type"].value_counts()
        print("  By type   :")
        for t, c in type_counts.items():
            print(f"    {t:<25} {c}")
    total_km = tracks_gdf["distance_m"].sum() / 1000 \
        if "distance_m" in tracks_gdf.columns else 0
    print(f"  Total dist: {total_km:,.1f} km")
    print("─────────────────────────────────────────────────────────────\n")


# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Convert a Strava export directory to a GeoPackage."
    )
    parser.add_argument(
        "export_dir",
        help="Path to the Strava export directory (containing activity files)."
    )
    parser.add_argument(
        "output",
        nargs="?",
        default="strava_activities.gpkg",
        help="Output GeoPackage path (default: strava_activities.gpkg)."
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging."
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    export_dir = Path(args.export_dir).expanduser().resolve()
    if not export_dir.is_dir():
        sys.exit(f"Error: '{export_dir}' is not a directory.")

    output_gpkg = Path(args.output).expanduser().resolve()
    build_geopackage(export_dir, output_gpkg)


if __name__ == "__main__":
    main()
