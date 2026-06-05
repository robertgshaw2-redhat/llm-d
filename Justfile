# Justfile for running the EPP Flow Control load client (client.py) from inside the cluster.
#
# Spins up a throwaway Python pod, copies client.py into it, and runs it pointed
# at the clusterIP of the EPP service. The pod is deleted automatically on exit.
#
# Override any variable on the CLI, e.g.:
#   just namespace=my-ns guide_name=flow-control client
#   just client --time-factor 0.5 --avg-gen-tokens 200

# --- Configuration (override via CLI or environment) ------------------------

# Namespace the EPP/model server are deployed in.
namespace   := env_var_or_default("NAMESPACE", "rob-dev")
# Guide name; the EPP service is "<guide_name>-epp".
guide_name  := env_var_or_default("GUIDE_NAME", "flow-control")
# Derived EPP service name (the Service whose clusterIP we target).
epp_service := guide_name + "-epp"
# Model name as registered in vLLM (--served-model-name).
model       := env_var_or_default("MODEL_NAME", "google/gemma-4-31B-it")
# Container image used for the client pod (aiohttp is pip-installed at runtime).
image       := env_var_or_default("CLIENT_IMAGE", "python:3.12-slim")
# Name of the throwaway client pod.
pod         := "epp-client"
# Path to the load client on the host.
client_py   := justfile_directory() / "client.py"
# Path to the interactive web UI server + page on the host.
flow_ui_py  := justfile_directory() / "flow_ui_server.py"
flow_ui_html := justfile_directory() / "flow_ui.html"
# Local port the web UI is port-forwarded to (open http://localhost:<ui_port>).
ui_port     := env_var_or_default("UI_PORT", "8080")

# Show available recipes.
default:
    @just --list

# Print the clusterIP of the EPP service.
ip:
    @kubectl get service {{epp_service}} -n {{namespace}} -o jsonpath='{.spec.clusterIP}'
    @echo

# Launch client.py in an in-cluster pod, querying the EPP service clusterIP.
# Extra args are passed straight through to client.py.
client *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail

    IP=$(kubectl get service {{epp_service}} -n {{namespace}} -o jsonpath='{.spec.clusterIP}' 2>/dev/null || true)
    if [ -z "${IP}" ]; then
        echo "ERROR: could not resolve clusterIP for service '{{epp_service}}' in namespace '{{namespace}}'." >&2
        echo "       Is the router deployed? Check: kubectl get svc -n {{namespace}}" >&2
        exit 1
    fi
    echo "EPP service {{epp_service}} clusterIP: ${IP}"

    # Always clean up the pod, even on Ctrl+C / failure.
    cleanup() {
        kubectl delete pod {{pod}} -n {{namespace}} --ignore-not-found --wait=false >/dev/null 2>&1 || true
    }
    trap cleanup EXIT

    # Start a long-lived pod we can copy into and exec against.
    kubectl run {{pod}} -n {{namespace}} --image={{image}} --restart=Never \
        --command -- sleep infinity
    kubectl wait --for=condition=Ready pod/{{pod}} -n {{namespace}} --timeout=120s

    # Copy the load client into the pod (only needs a shell + cat, no tar dependency).
    kubectl exec -i {{pod}} -n {{namespace}} -- sh -c 'cat > /tmp/client.py' < {{client_py}}

    # Install the client's only runtime dependency (requires pod egress to PyPI).
    kubectl exec -i {{pod}} -n {{namespace}} -- pip install --quiet --no-cache-dir aiohttp

    # Run the client, injecting the EPP clusterIP as the EPP_IP env var.
    kubectl exec -it {{pod}} -n {{namespace}} -- \
        env EPP_IP="${IP}" python3 /tmp/client.py --model {{model}} {{ARGS}}

# Launch the interactive web UI (flow_ui_server.py) in an in-cluster pod and
# port-forward it to localhost. Open http://localhost:{{ui_port}} once it's up.
# Extra args are passed straight through to flow_ui_server.py (e.g. --capacity 32).
flow-ui *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail

    IP=$(kubectl get service {{epp_service}} -n {{namespace}} -o jsonpath='{.spec.clusterIP}' 2>/dev/null || true)
    if [ -z "${IP}" ]; then
        echo "ERROR: could not resolve clusterIP for service '{{epp_service}}' in namespace '{{namespace}}'." >&2
        echo "       Is the router deployed? Check: kubectl get svc -n {{namespace}}" >&2
        exit 1
    fi
    echo "EPP service {{epp_service}} clusterIP: ${IP}"

    # Always clean up the pod, even on Ctrl+C / failure.
    cleanup() {
        kubectl delete pod {{pod}} -n {{namespace}} --ignore-not-found --wait=false >/dev/null 2>&1 || true
    }
    trap cleanup EXIT

    # Start a long-lived pod we can copy into and exec against.
    kubectl run {{pod}} -n {{namespace}} --image={{image}} --restart=Never \
        --command -- sleep infinity
    kubectl wait --for=condition=Ready pod/{{pod}} -n {{namespace}} --timeout=120s

    # Copy the server, the UI page and the load engine into the pod. The server
    # reads flow_ui.html from its own directory and imports client.py, so all
    # three must sit side by side in /tmp.
    kubectl exec -i {{pod}} -n {{namespace}} -- sh -c 'cat > /tmp/flow_ui_server.py' < {{flow_ui_py}}
    kubectl exec -i {{pod}} -n {{namespace}} -- sh -c 'cat > /tmp/flow_ui.html' < {{flow_ui_html}}
    kubectl exec -i {{pod}} -n {{namespace}} -- sh -c 'cat > /tmp/client.py' < {{client_py}}

    # Install the only runtime dependency (requires pod egress to PyPI).
    kubectl exec -i {{pod}} -n {{namespace}} -- pip install --quiet --no-cache-dir aiohttp

    # Start the UI server detached inside the pod, listening on 8080. nohup +
    # redirect lets the exec session return while the server keeps running
    # (reparented to the pod's init).
    kubectl exec -i {{pod}} -n {{namespace}} -- sh -c \
        'cd /tmp && nohup env EPP_IP="'"${IP}"'" python3 flow_ui_server.py --model {{model}} {{ARGS}} > /tmp/flow_ui.log 2>&1 &'

    # Give it a moment to bind, then surface any startup errors.
    sleep 3
    kubectl exec -i {{pod}} -n {{namespace}} -- cat /tmp/flow_ui.log || true

    echo
    echo ">>> Web UI ready. Open http://localhost:{{ui_port}}  (Ctrl+C to stop and clean up the pod)"
    echo

    # Foreground port-forward; Ctrl+C here triggers the cleanup trap above.
    kubectl port-forward pod/{{pod}} -n {{namespace}} {{ui_port}}:8080

exec:
    kubectl exec -it {{pod}} -n {{namespace}} -- /bin/bash

# Delete the client pod if it is lingering.
clean:
    @kubectl delete pod {{pod}} -n {{namespace}} --ignore-not-found
