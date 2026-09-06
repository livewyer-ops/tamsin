"""Prepare the pinned TAMOSS source for the lean live-test deployment."""

import sys

path = sys.argv[1] + "/operator/config/manager/manager.yaml"
with open(path) as handle:
    documents = handle.read().split("\n---\n")

# Written as a text edit rather than through a YAML library, because the file is
# TAMOSS's and round-tripping it would reformat everything around the change.
for index, document in enumerate(documents):
    if "\nkind: Deployment\n" not in document:
        continue
    if "\n  strategy:\n" in document:
        break
    documents[index] = document.replace(
        "\n  replicas: 1\n",
        "\n  replicas: 1\n"
        "  strategy:\n"
        "    type: RollingUpdate\n"
        "    rollingUpdate:\n"
        "      maxSurge: 0\n"
        "      maxUnavailable: 1\n",
        1,
    )
    break
else:
    raise SystemExit("no Deployment found in the operator manifest")

with open(path, "w") as handle:
    handle.write("\n---\n".join(documents))

# Skip unused UI builds and apply the requested builder-cache policy.
path = sys.argv[1] + "/.tasks/kind.yaml"
prune_builder_cache = sys.argv[2] == "true"
with open(path) as handle:
    contents = handle.read()

ui_commands = (
    '        task_kind_build_image "TAMOSS UI" "{{.UI_IMAGE}}" "" "src/app/frontend"\n',
    '        task_kind_load_image "{{.PROJECT_NAME}}" "TAMOSS UI" "{{.UI_IMAGE}}"\n',
)
delete_task = "\n  delete:"
prune_command = "      - docker builder prune --all --force\n"
command_counts = [contents.count(command) for command in ui_commands]
if command_counts == [1, 1]:
    for command in ui_commands:
        contents = contents.replace(command, "", 1)
elif command_counts != [0, 0]:
    raise SystemExit(f"pinned TAMOSS kind task has partial UI commands: counts={command_counts}")

if contents.count(delete_task) != 1 or contents.count(prune_command) > 1:
    raise SystemExit("pinned TAMOSS kind task has an unexpected create/delete boundary")
if prune_builder_cache and prune_command not in contents:
    contents = contents.replace(delete_task, "\n" + prune_command + delete_task, 1)
elif not prune_builder_cache:
    contents = contents.replace(prune_command, "", 1)

with open(path, "w") as handle:
    handle.write(contents)
