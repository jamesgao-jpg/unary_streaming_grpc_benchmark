"""Export redacted VDBBench evidence, or verify the published report bundle."""

import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
PRIVATE = {"uri", "user", "password", "token", "api_key", "note", "endpoint",
           "reference_file", "route_evidence", "dir", "expected_result"}
FAMILIES = {"Q01": "Plain Query", "Q02": "Query iterator", "Q03": "Element Query / iterator",
            "Q04": "Query Group By", "S01": "Plain ANN Search", "S02": "Search iterator",
            "S04": "Embedding-list Search", "S05": "Range Search", "S06": "Iterative-filter Search",
            "S07": "Search ORDER BY", "S08": "Search Group By", "S09": "Search aggregation",
            "H01": "Hybrid ANN", "H02": "Hybrid range / iterative filter", "H05": "Hybrid embedding-list"}


# Hash original and public files independently to disclose publication redaction.
def digest(data):
    return hashlib.sha256(data).hexdigest()


# Preserve numerical measurements while removing connection details and row contents.
def redact(value, path="", changes=None):
    if isinstance(value, dict):
        output = {}
        for key, item in value.items():
            location = path + "/" + key
            if key in PRIVATE and item is not None:
                output[key] = None if key == "expected_result" else "<redacted>"
                changes.append(location)
            else:
                output[key] = redact(item, location, changes)
        return output
    if isinstance(value, list):
        return [redact(item, path + f"/{i}", changes) for i, item in enumerate(value)]
    if isinstance(value, str):
        clean = re.sub(r"10\.15\.\d+\.\d+", "<private-address>", value)
        if clean != value:
            changes.append(path)
        return clean
    return value


# Export only selected successful timing results and their RSS status/sample files.
def export(run):
    entries = []
    sources = [(p, "results/" + p.name) for p in sorted((run / "results/Milvus").glob("result_*.json"))]
    assert len(sources) == 652
    for path, _ in list(sources):
        label = json.loads(path.read_text())["task_label"]
        cell, phase = label.rsplit("-r1-", 1)
        sources.append((run / "matrix" / cell / f"r1-{phase}-proxy-rss.json", f"rss/{cell}-{phase}.json"))
    sources += [(run / "source.json", "source.json")]
    for path, target in sources:
        original = path.read_bytes()
        changes = []
        data = redact(json.loads(original), changes=changes)
        public = (json.dumps(data, indent=2, ensure_ascii=True) + "\n").encode()
        destination = HERE / "evidence" / target
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(public)
        entries.append({"file": target, "original_relative_path": str(path.relative_to(run)),
                        "original_sha256": digest(original), "published_sha256": digest(public),
                        "redacted_paths": changes})
    supporting = [{"file": name, "sha256": digest((HERE / "evidence" / name).read_bytes())}
                  for name in ("vdbbench-working-tree.patch.gz", "field-profile.json")]
    manifest = {"run": run.name, "originals_unchanged": True, "files": entries, "supporting_files": supporting}
    (HERE / "evidence/manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


# Verify raw units and A/B inputs before deriving any ratios or report tables.
def read_pairs():
    pairs = {}
    for path in sorted((HERE / "evidence/results").glob("result_*.json")):
        data = json.loads(path.read_text())
        cell, phase = data["task_label"].rsplit("-r1-", 1)
        result = data["results"][0]
        metrics, config = result["metrics"], result["task_config"]["case_config"]
        functional = metrics["additional_parameters"]["functional"]
        assert phase in ("capture", "verify") and functional["phase"] == phase
        assert config["nq"] == 1 and metrics["qps"] > 0
        assert config["concurrency_search_config"]["concurrency_duration"] == 45
        assert not functional["validate_during_timing"] and not functional["recall_measured"]
        assert functional["server_setup"]["chunk_bytes"] == 262144
        assert functional["server_setup"]["milvus_revision"] == "9b6b87c8716ce24bf7e1d79b58c7bd8e3a8a58ff"
        rss = json.loads((HERE / f"evidence/rss/{cell}-{phase}.json").read_text())
        pairs.setdefault(cell, {})[phase] = {"result": result, "metrics": metrics,
                                           "functional": functional, "rss": rss,
                                           "link": str(path.relative_to(HERE))}
    assert len(pairs) == 326
    for cell, modes in pairs.items():
        assert set(modes) == {"capture", "verify"}, cell
        batch, stream = modes["capture"], modes["verify"]
        for key in ("family", "operation", "request", "sub_searches", "aggregation", "ranker",
                    "reference_sha256", "expected_count", "expected_page_sizes", "batch_size"):
            assert batch["functional"].get(key) == stream["functional"].get(key), (cell, key)
        for key in ("k", "nq", "concurrency_search_config"):
            assert batch["result"]["task_config"]["case_config"][key] == stream["result"]["task_config"]["case_config"][key]
    return pairs


# Retain every matched configuration, not just the largest improvements.
def appendix(pairs):
    lines = ["# All Matched Configurations", "", "Generated from the linked redacted VDBBench results; numerical measurements are unchanged.",
             "B = Batch; S = Streaming. QPS is completed operations/s at NQ=1; iterator operations include all pages.",
             "Latency is end-to-end client p95 in milliseconds. RSS is sampled process peak in MiB, not heap.",
             "A dash means unavailable or partial RSS, never zero. Inspect each linked result for the exact request and all latency percentiles.",
             "The one blocked cell, 127-S08-composite-k1024-c1, has no timing pair and is not included in these 326 rows.", ""]
    for family, title in FAMILIES.items():
        lines += [f"## {family}: {title}", "", "| Cell | Limit / K | C | Output fields | B QPS | S QPS | S/B | B p95 ms | S p95 ms | B peak RSS MiB | S peak RSS MiB |",
                  "|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|"]
        for cell, modes in pairs.items():
            b, s = modes["capture"], modes["verify"]
            if b["functional"]["family"] != family:
                continue
            bm, sm = b["metrics"], s["metrics"]
            fields = ", ".join(b["functional"]["request"].get("output_fields", [])) or "aggregation-defined"
            peaks = [f"{v['rss']['sampled_peak_bytes'] / 2**20:,.1f}" if v["rss"]["status"] == "available" else "-" for v in (b, s)]
            lines.append(f"| {cell} ([B]({b['link']}), [S]({s['link']})) | {b['result']['task_config']['case_config']['k']} | {bm['conc_num_list'][0]} | {fields} | {bm['qps']:.4f} | {sm['qps']:.4f} | {sm['qps']/bm['qps']:.3f}x | {1000*bm['conc_latency_p95_list'][0]:.3f} | {1000*sm['conc_latency_p95_list'][0]:.3f} | {peaks[0]} | {peaks[1]} |")
        lines.append("")
    return "\n".join(lines)


# Validate published hashes, completeness, missingness and generated arithmetic.
def verify():
    manifest = json.loads((HERE / "evidence/manifest.json").read_text())
    for entry in manifest["supporting_files"]:
        assert digest((HERE / "evidence" / entry["file"]).read_bytes()) == entry["sha256"]
    for entry in manifest["files"]:
        data = (HERE / "evidence" / entry["file"]).read_bytes()
        assert digest(data) == entry["published_sha256"], entry["file"]
        assert not re.search(rb"(?:AKIA[A-Z0-9]{16}|glsa_[A-Za-z0-9_]+|ghp_[A-Za-z0-9]+)", data)
    pairs = read_pairs()
    counts = Counter(v["rss"]["status"] for modes in pairs.values() for v in modes.values())
    assert counts == {"available": 53, "partial": 3, "unavailable": 596}
    assert sum(all(v["rss"]["status"] == "available" for v in modes.values()) for modes in pairs.values()) == 26
    assert (HERE / "appendix.md").read_text() == appendix(pairs)
    print("PASS: 652 result files; 326 matched pairs; unchanged published hashes; 26 complete RSS pairs; appendix arithmetic.")


if __name__ == "__main__":
    if len(sys.argv) == 2:
        export(Path(sys.argv[1]).resolve())
        (HERE / "appendix.md").write_text(appendix(read_pairs()))
    verify()
