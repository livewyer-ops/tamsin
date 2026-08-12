#!/usr/bin/env python3
"""Extract checked image attestations into deterministic release assets."""

from __future__ import annotations

import argparse
from gzip import GzipFile
import hashlib
import io
import json
import os
from pathlib import Path
import re
import tarfile


DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
INDEX_MEDIA_TYPE = "application/vnd.oci.image.index.v1+json"
MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json"
ATTESTATION_MEDIA_TYPE = "application/vnd.in-toto+json"
PREDICATE_NAMES = {
    "https://spdx.dev/Document": "spdx-sbom",
    "https://slsa.dev/provenance/v0.2": "slsa-provenance",
}
MAX_JSON_BYTES = 32 << 20
DEFAULT_RUNTIME_CONTRACT = Path(__file__).resolve().parent.parent / "contracts" / "ffmpeg-runtime.json"


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--image-ref", required=True)
    parser.add_argument("--runtime-ref", required=True)
    parser.add_argument("--runtime-contract", type=Path, default=DEFAULT_RUNTIME_CONTRACT)
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--created", required=True)
    parser.add_argument("--metadata-output", required=True, type=Path)
    parser.add_argument("--archive-output", required=True, type=Path)
    return parser.parse_args()


def fail(message: str) -> None:
    raise ValueError(message)


def read_json(path: Path, description: str) -> dict:
    data = path.read_bytes()
    if len(data) > MAX_JSON_BYTES:
        fail(f"{description} is larger than {MAX_JSON_BYTES} bytes")
    value = json.loads(data)
    if not isinstance(value, dict):
        fail(f"{description} is not a JSON object")
    return value


def checked_blob(layout: Path, descriptor: dict, description: str) -> bytes:
    digest = descriptor.get("digest")
    size = descriptor.get("size")
    if not isinstance(digest, str) or not DIGEST.fullmatch(digest):
        fail(f"{description} has an invalid digest")
    if type(size) is not int or size < 0 or size > MAX_JSON_BYTES:
        fail(f"{description} has an invalid size")
    data = (layout / "blobs" / "sha256" / digest.removeprefix("sha256:")).read_bytes()
    actual = "sha256:" + hashlib.sha256(data).hexdigest()
    if len(data) != size or actual != digest:
        fail(f"{description} does not match its OCI descriptor")
    return data


def checked_json_blob(layout: Path, descriptor: dict, description: str) -> tuple[dict, bytes]:
    data = checked_blob(layout, descriptor, description)
    value = json.loads(data)
    if not isinstance(value, dict):
        fail(f"{description} is not a JSON object")
    return value, data


def identity(bundle: Path, name: str) -> str:
    value = (bundle / name).read_text(encoding="utf-8")
    if not value.endswith("\n") or "\n" in value.rstrip("\n") or "\r" in value:
        fail(f"{name} is not one newline-terminated value")
    return value[:-1]


def tar_bytes(files: dict[str, bytes], epoch: int) -> bytes:
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for name in sorted(files):
            info = tarfile.TarInfo(name)
            info.size = len(files[name])
            info.mode = 0o644
            info.mtime = epoch
            info.uid = 0
            info.gid = 0
            info.uname = "root"
            info.gname = "root"
            archive.addfile(info, io.BytesIO(files[name]))
    compressed = io.BytesIO()
    with GzipFile(filename="", fileobj=compressed, mode="wb", compresslevel=9, mtime=epoch) as output:
        output.write(stream.getvalue())
    return compressed.getvalue()


def main() -> None:
    options = arguments()
    epoch_text = os.environ.get("SOURCE_DATE_EPOCH", "0")
    if not epoch_text.isdigit():
        fail("SOURCE_DATE_EPOCH must be a non-negative integer")
    epoch = int(epoch_text)
    if not re.fullmatch(r"[^@]+@sha256:[0-9a-f]{64}", options.runtime_ref):
        fail("runtime reference must be a tag plus sha256 digest")
    runtime_contract = read_json(options.runtime_contract, "FFmpeg runtime contract")
    if runtime_contract.get("image") != options.runtime_ref.rsplit("@", 1)[0]:
        fail("runtime reference does not match the FFmpeg runtime contract")
    if runtime_contract.get("platforms") != ["linux/amd64", "linux/arm64"]:
        fail("FFmpeg runtime contract must declare amd64 and arm64 in stable order")

    layout = options.bundle / "tamsin-image"
    routing = read_json(layout / "index.json", "OCI routing index")
    roots = []
    for descriptor in routing.get("manifests", []):
        annotations = descriptor.get("annotations", {}) if isinstance(descriptor, dict) else {}
        if isinstance(annotations, dict) and annotations.get("io.containerd.image.name") == options.image_ref:
            roots.append(descriptor)
    if len(roots) != 1 or roots[0].get("mediaType") != INDEX_MEDIA_TYPE:
        fail("OCI layout does not contain exactly one named image index")
    image_index, _ = checked_json_blob(layout, roots[0], "image index")
    index_digest = roots[0].get("digest")
    if identity(options.bundle, "IMAGE_INDEX_DIGEST") != index_digest:
        fail("recorded and extracted image index digests differ")

    runnable: dict[str, str] = {}
    attestations: list[dict] = []
    for descriptor in image_index.get("manifests", []):
        if not isinstance(descriptor, dict) or not isinstance(descriptor.get("platform"), dict):
            fail("image index contains a descriptor without a platform")
        platform_data = descriptor["platform"]
        platform = f"{platform_data.get('os')}/{platform_data.get('architecture')}"
        if platform != "unknown/unknown":
            runnable[descriptor.get("digest")] = platform

    archive_files: dict[str, bytes] = {}
    for descriptor in image_index.get("manifests", []):
        platform_data = descriptor["platform"]
        if f"{platform_data.get('os')}/{platform_data.get('architecture')}" != "unknown/unknown":
            continue
        annotations = descriptor.get("annotations")
        subject = annotations.get("vnd.docker.reference.digest") if isinstance(annotations, dict) else None
        platform = runnable.get(subject)
        if platform is None:
            fail("attestation refers to an unknown image manifest")
        manifest, _ = checked_json_blob(layout, descriptor, f"{platform} attestation manifest")
        for layer in manifest.get("layers", []):
            if not isinstance(layer, dict) or layer.get("mediaType") != ATTESTATION_MEDIA_TYPE:
                fail(f"{platform} attestation manifest contains a non-in-toto layer")
            layer_annotations = layer.get("annotations")
            predicate = layer_annotations.get("in-toto.io/predicate-type") if isinstance(layer_annotations, dict) else None
            name = PREDICATE_NAMES.get(predicate)
            if name is None:
                fail(f"{platform} carries unsupported predicate {predicate!r}")
            statement, data = checked_json_blob(layout, layer, f"{platform} {name}")
            if statement.get("predicateType") != predicate:
                fail(f"{platform} {name} statement and descriptor disagree")
            path = f"attestations/{platform.replace('/', '-')}-{name}.json"
            if path in archive_files:
                fail(f"duplicate release attestation {path}")
            archive_files[path] = data
            attestations.append(
                {
                    "path": path,
                    "platform": platform,
                    "predicate_type": predicate,
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "subject_manifest_digest": subject,
                }
            )

    expected_attestations = len(runnable) * len(PREDICATE_NAMES)
    if set(runnable.values()) != {"linux/amd64", "linux/arm64"} or len(attestations) != expected_attestations:
        fail("release image must carry SPDX and SLSA statements for amd64 and arm64")

    metadata = {
        "schema_version": "1.0",
        "release": {
            "version": options.version,
            "commit": options.commit,
            "created": options.created,
        },
        "image": {
            "reference": options.image_ref,
            "index_digest": index_digest,
            "immutable_reference": f"{options.image_ref}@{index_digest}",
            "platforms": {
                "linux/amd64": {
                    "manifest_digest": identity(options.bundle, "IMAGE_AMD64_DIGEST"),
                    "config_digest": identity(options.bundle, "IMAGE_AMD64_CONFIG_DIGEST"),
                },
                "linux/arm64": {
                    "manifest_digest": identity(options.bundle, "IMAGE_ARM64_DIGEST"),
                    "config_digest": identity(options.bundle, "IMAGE_ARM64_CONFIG_DIGEST"),
                },
            },
        },
        "ffmpeg_runtime": {
            "immutable_reference": options.runtime_ref,
            "reference": runtime_contract["image"],
            "index_digest": options.runtime_ref.rsplit("@", 1)[1],
            "revision": runtime_contract["revision"],
            "debian_base": runtime_contract["debian_base"],
            "debian_snapshot": runtime_contract["debian_snapshot"],
            "ffmpeg_version": runtime_contract["ffmpeg_package"],
            "ca_certificates_version": runtime_contract["ca_certificates_package"],
        },
        "attestations": sorted(attestations, key=lambda item: item["path"]),
    }
    metadata_bytes = (json.dumps(metadata, indent=2, sort_keys=True) + "\n").encode()
    options.metadata_output.parent.mkdir(parents=True, exist_ok=True)
    options.metadata_output.write_bytes(metadata_bytes)
    archive_files["tamsin-container-metadata.json"] = metadata_bytes
    options.archive_output.parent.mkdir(parents=True, exist_ok=True)
    options.archive_output.write_bytes(tar_bytes(archive_files, epoch))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise SystemExit(f"cannot build supply-chain bundle: {error}") from error
