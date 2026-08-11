#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat >&2 <<'EOF'
usage:
  verify-oci-layout.sh record BUNDLE_DIR IMAGE_REF
  verify-oci-layout.sh verify BUNDLE_DIR IMAGE_REF INDEX_DIGEST
EOF
  exit 2
}

mode="${1:-}"
case "$mode" in
  record)
    [ "$#" -eq 3 ] || usage
    ;;
  verify)
    [ "$#" -eq 4 ] || usage
    ;;
  *) usage ;;
esac

bundle="$2"
image_ref="$3"
expected_index="${4:-}"

python3 - "$mode" "$bundle" "$image_ref" "$expected_index" <<'PY'
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

(
    mode,
    bundle_arg,
    image_ref,
    expected_index,
) = sys.argv[1:]

digest_pattern = re.compile(r"^sha256:[0-9a-f]{64}$")
index_media_type = "application/vnd.oci.image.index.v1+json"
manifest_media_type = "application/vnd.oci.image.manifest.v1+json"
config_media_type = "application/vnd.oci.image.config.v1+json"
attestation_media_type = "application/vnd.in-toto+json"
attestation_reference_type = "attestation-manifest"
statement_type = "https://in-toto.io/Statement/v0.1"
required_platforms = ("linux/amd64", "linux/arm64")
required_predicates = {
    "https://spdx.dev/Document",
    "https://slsa.dev/provenance/v0.2",
}
max_json_blob_bytes = 16 << 20
hash_chunk_bytes = 1 << 20


def fail(message):
    raise ValueError(message)


def read_json(path, description):
    try:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode):
            fail(f"{description} at {path} is not a regular file")
        if metadata.st_size > max_json_blob_bytes:
            fail(
                f"{description} at {path} is {metadata.st_size} bytes; "
                f"limit is {max_json_blob_bytes}"
            )
        value = json.loads(path.read_bytes())
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {description} at {path}: {error}")
    if not isinstance(value, dict):
        fail(f"{description} at {path} is not a JSON object")
    return value


def checked_blob(layout, descriptor, description, return_contents=False):
    if not isinstance(descriptor, dict):
        fail(f"{description} descriptor is not an object")
    digest = descriptor.get("digest")
    size = descriptor.get("size")
    if not isinstance(digest, str) or not digest_pattern.fullmatch(digest):
        fail(f"{description} has invalid digest {digest!r}")
    if type(size) is not int or size < 0:
        fail(f"{description} has invalid size {size!r}")

    algorithm, hexadecimal = digest.split(":", 1)
    path = layout / "blobs" / algorithm / hexadecimal
    try:
        metadata = path.lstat()
    except OSError as error:
        fail(f"cannot read {description} blob {digest}: {error}")
    if not stat.S_ISREG(metadata.st_mode):
        fail(f"{description} blob {digest} is not a regular file")
    if metadata.st_size != size:
        fail(
            f"{description} blob {digest} has size {metadata.st_size}, expected {size}"
        )
    if return_contents and size > max_json_blob_bytes:
        fail(
            f"{description} blob {digest} is {size} bytes; "
            f"JSON limit is {max_json_blob_bytes}"
        )
    hasher = hashlib.sha256()
    contents = bytearray() if return_contents else None
    total = 0
    try:
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
                fail(f"{description} blob {digest} changed while it was opened")
            while chunk := handle.read(hash_chunk_bytes):
                total += len(chunk)
                hasher.update(chunk)
                if contents is not None:
                    contents.extend(chunk)
    except OSError as error:
        fail(f"cannot hash {description} blob {digest}: {error}")
    if total != size:
        fail(f"{description} blob {digest} yielded {total} bytes, expected {size}")
    actual = "sha256:" + hasher.hexdigest()
    if actual != digest:
        fail(f"{description} blob hashes to {actual}, expected {digest}")
    return bytes(contents) if contents is not None else None


def checked_json_blob(layout, descriptor, description):
    contents = checked_blob(layout, descriptor, description, return_contents=True)
    try:
        value = json.loads(contents)
    except json.JSONDecodeError as error:
        fail(f"{description} is invalid JSON: {error}")
    if not isinstance(value, dict):
        fail(f"{description} is not a JSON object")
    return value


def checked_manifest(layout, descriptor, platform):
    if descriptor.get("mediaType") != manifest_media_type:
        fail(
            f"{platform} descriptor has media type {descriptor.get('mediaType')!r}, "
            f"expected {manifest_media_type!r}"
        )
    manifest = checked_json_blob(layout, descriptor, f"{platform} manifest")
    if not isinstance(manifest, dict) or manifest.get("schemaVersion") != 2:
        fail(f"{platform} manifest is not an OCI schema-version 2 object")
    if manifest.get("mediaType") != manifest_media_type:
        fail(f"{platform} manifest does not declare the OCI image media type")

    config_descriptor = manifest.get("config")
    if not isinstance(config_descriptor, dict) or config_descriptor.get(
        "mediaType"
    ) != config_media_type:
        fail(
            f"{platform} config descriptor has media type "
            f"{config_descriptor.get('mediaType') if isinstance(config_descriptor, dict) else None!r}, "
            f"expected {config_media_type!r}"
        )
    config = checked_json_blob(layout, config_descriptor, f"{platform} config")
    os_name, architecture = platform.split("/", 1)
    if config.get("os") != os_name or config.get("architecture") != architecture:
        fail(
            f"{platform} descriptor points to config for "
            f"{config.get('os')}/{config.get('architecture')}"
        )

    layers = manifest.get("layers")
    if not isinstance(layers, list) or not layers:
        fail(f"{platform} manifest has no filesystem layers")
    if len(layers) > 256:
        fail(f"{platform} manifest has {len(layers)} filesystem layers; limit is 256")
    for index, layer in enumerate(layers):
        checked_blob(layout, layer, f"{platform} layer {index}")
    return config_descriptor["digest"]


def checked_attestation(layout, descriptor, expected_subject):
    annotations = descriptor.get("annotations")
    if not isinstance(annotations, dict):
        fail("attestation descriptor has no annotations")
    if annotations.get("vnd.docker.reference.type") != attestation_reference_type:
        fail("unknown/unknown descriptor is not a BuildKit attestation")
    if annotations.get("vnd.docker.reference.digest") != expected_subject:
        fail(f"attestation does not refer to {expected_subject}")
    if descriptor.get("mediaType") != manifest_media_type:
        fail("attestation descriptor is not an OCI manifest")

    manifest = checked_json_blob(layout, descriptor, "attestation manifest")
    if manifest.get("schemaVersion") != 2 or manifest.get("mediaType") != manifest_media_type:
        fail("attestation manifest is not an OCI schema-version 2 object")
    config_descriptor = manifest.get("config")
    if not isinstance(config_descriptor, dict) or config_descriptor.get(
        "mediaType"
    ) != config_media_type:
        fail("attestation manifest does not have an OCI image config descriptor")
    checked_json_blob(layout, config_descriptor, "attestation config")

    layers = manifest.get("layers")
    if not isinstance(layers, list) or not layers or len(layers) > 8:
        fail("attestation manifest has an invalid layer count")
    predicates = set()
    subject_hex = expected_subject.removeprefix("sha256:")
    for index, layer in enumerate(layers):
        if not isinstance(layer, dict) or layer.get("mediaType") != attestation_media_type:
            fail(f"attestation layer {index} is not in-toto JSON")
        layer_annotations = layer.get("annotations")
        predicate = (
            layer_annotations.get("in-toto.io/predicate-type")
            if isinstance(layer_annotations, dict)
            else None
        )
        statement = checked_json_blob(layout, layer, f"attestation layer {index}")
        if (
            statement.get("_type") != statement_type
            or statement.get("predicateType") != predicate
            or not isinstance(statement.get("predicate"), dict)
        ):
            fail(f"attestation layer {index} has inconsistent predicate metadata")
        subjects = statement.get("subject")
        if not isinstance(subjects, list) or not any(
            isinstance(subject, dict)
            and isinstance(subject.get("digest"), dict)
            and subject["digest"].get("sha256") == subject_hex
            for subject in subjects
        ):
            fail(f"attestation layer {index} does not bind the image manifest")
        if predicate in predicates:
            fail(f"attestation repeats predicate {predicate!r}")
        predicates.add(predicate)
    if predicates != required_predicates:
        fail(
            "attestation predicates are "
            + ", ".join(sorted(str(value) for value in predicates))
            + "; expected SPDX SBOM and minimal SLSA provenance"
        )


def metadata_value(bundle, name):
    path = bundle / name
    try:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode):
            fail(f"release identity file {path} is not a regular file")
        if metadata.st_size > 256:
            fail(f"release identity file {path} is unexpectedly large")
        value = path.read_text(encoding="utf-8")
    except OSError as error:
        fail(f"cannot read release identity file {path}: {error}")
    if "\n" in value.rstrip("\n") or "\r" in value or not value.endswith("\n"):
        fail(f"release identity file {path} is not one newline-terminated value")
    return value[:-1]


def write_metadata(bundle, name, value):
    path = bundle / name
    temporary = bundle / f".{name}.tmp"
    temporary.write_text(value + "\n", encoding="utf-8")
    os.replace(temporary, path)


try:
    bundle = Path(bundle_arg)
    if mode == "verify" and not digest_pattern.fullmatch(expected_index):
        fail(f"expected image index {expected_index!r} is not a sha256 digest")
    layout_path = bundle / "tamsin-image"
    if layout_path.is_symlink():
        fail(f"OCI layout {layout_path} must not be a symlink")
    layout = layout_path.resolve(strict=True)
    if not layout.is_dir():
        fail(f"OCI layout {layout} is not a directory")
    if "@" in image_ref or ":" not in image_ref.rsplit("/", 1)[-1]:
        fail(f"image reference {image_ref!r} must contain one explicit tag and no digest")
    image_tag = image_ref.rsplit(":", 1)[1]

    layout_declaration = read_json(layout / "oci-layout", "OCI layout declaration")
    if layout_declaration.get("imageLayoutVersion") != "1.0.0":
        fail("OCI layout does not declare imageLayoutVersion 1.0.0")
    routing_index = read_json(layout / "index.json", "OCI layout index")
    if routing_index.get("schemaVersion") != 2:
        fail("OCI layout index is not schema version 2")

    routing_manifests = routing_index.get("manifests")
    if not isinstance(routing_manifests, list):
        fail("OCI layout index does not contain a manifests array")
    matches = []
    for descriptor in routing_manifests:
        if not isinstance(descriptor, dict):
            continue
        annotations = descriptor.get("annotations", {})
        if isinstance(annotations, dict) and annotations.get(
            "io.containerd.image.name"
        ) == image_ref:
            matches.append(descriptor)
    if len(matches) != 1:
        fail(
            f"OCI layout contains {len(matches)} descriptors named {image_ref!r}; expected one"
        )
    root_descriptor = matches[0]
    annotations = root_descriptor.get("annotations", {})
    if annotations.get("org.opencontainers.image.ref.name") != image_tag:
        fail(f"OCI layout descriptor is not tagged {image_tag!r}")
    if root_descriptor.get("mediaType") != index_media_type:
        fail("release root is not an OCI image index")

    index_contents = checked_blob(
        layout, root_descriptor, "release index", return_contents=True
    )
    index_digest = root_descriptor["digest"]
    try:
        image_index = json.loads(index_contents)
    except json.JSONDecodeError as error:
        fail(f"release index is invalid JSON: {error}")
    if (
        not isinstance(image_index, dict)
        or image_index.get("schemaVersion") != 2
        or image_index.get("mediaType") != index_media_type
    ):
        fail("release root blob is not an OCI schema-version 2 image index")

    platform_descriptors = {}
    attestation_descriptors = []
    manifests = image_index.get("manifests")
    if not isinstance(manifests, list):
        fail("release index does not contain a manifests array")
    for descriptor in manifests:
        if not isinstance(descriptor, dict) or not isinstance(
            descriptor.get("platform"), dict
        ):
            fail("release index contains a descriptor without a platform")
        platform_data = descriptor["platform"]
        platform = f"{platform_data.get('os')}/{platform_data.get('architecture')}"
        if platform == "unknown/unknown":
            attestation_descriptors.append(descriptor)
            continue
        if platform in platform_descriptors:
            fail(f"release index contains duplicate {platform} manifests")
        platform_descriptors[platform] = descriptor

    actual_platforms = tuple(sorted(platform_descriptors))
    if actual_platforms != tuple(sorted(required_platforms)):
        fail(
            "release index platforms are "
            + ", ".join(actual_platforms)
            + "; expected exactly linux/amd64 and linux/arm64"
        )
    config_digests = {}
    for platform in required_platforms:
        config_digests[platform] = checked_manifest(
            layout, platform_descriptors[platform], platform
        )
    runnable_digests = {
        descriptor["digest"] for descriptor in platform_descriptors.values()
    }
    attestations_by_subject = {}
    for descriptor in attestation_descriptors:
        annotations = descriptor.get("annotations")
        subject = (
            annotations.get("vnd.docker.reference.digest")
            if isinstance(annotations, dict)
            else None
        )
        if subject not in runnable_digests:
            fail(f"attestation refers to unknown manifest {subject!r}")
        if subject in attestations_by_subject:
            fail(f"release index repeats the attestation for {subject}")
        attestations_by_subject[subject] = descriptor
    if set(attestations_by_subject) != runnable_digests:
        fail("release index does not contain one attestation per platform manifest")
    for subject, descriptor in attestations_by_subject.items():
        checked_attestation(layout, descriptor, subject)

    amd64_digest = platform_descriptors["linux/amd64"]["digest"]
    arm64_digest = platform_descriptors["linux/arm64"]["digest"]
    amd64_config_digest = config_digests["linux/amd64"]
    arm64_config_digest = config_digests["linux/arm64"]
    identities = {
        "IMAGE_REF": image_ref,
        "IMAGE_INDEX_DIGEST": index_digest,
        "IMAGE_AMD64_DIGEST": amd64_digest,
        "IMAGE_ARM64_DIGEST": arm64_digest,
        "IMAGE_AMD64_CONFIG_DIGEST": amd64_config_digest,
        "IMAGE_ARM64_CONFIG_DIGEST": arm64_config_digest,
    }

    if mode == "record":
        bundle.mkdir(parents=True, exist_ok=True)
        for name, value in identities.items():
            write_metadata(bundle, name, value)
        print(f"image_index_digest={index_digest}")
    else:
        expected = {
            "IMAGE_REF": image_ref,
            "IMAGE_INDEX_DIGEST": expected_index,
        }
        for name, value in identities.items():
            expected_value = expected.get(name, value)
            if name != "IMAGE_REF" and not digest_pattern.fullmatch(value):
                fail(f"derived {name} value {value!r} is not a sha256 digest")
            recorded = metadata_value(bundle, name)
            if recorded != expected_value:
                fail(f"{name} records {recorded!r}, expected {expected_value!r}")
            if value != expected_value:
                fail(f"OCI layout {name} is {value!r}, expected {expected_value!r}")
        print(
            f"verified exact OCI index {image_ref}@{index_digest} "
            f"(linux/amd64 manifest {amd64_digest}, config {amd64_config_digest}; "
            f"linux/arm64 manifest {arm64_digest}, config {arm64_config_digest})"
        )
except (OSError, ValueError) as error:
    print(f"invalid release OCI layout: {error}", file=sys.stderr)
    raise SystemExit(1)
PY
