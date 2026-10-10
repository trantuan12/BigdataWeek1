import json, os, subprocess
from pathlib import Path

def k(*args):
    return subprocess.check_output(["kubectl", *args], text=True)

ns = os.environ.get("NS", "bd-g10")
cluster = json.loads(k("config", "view", "--minify", "--flatten",
                       "--raw", "-o", "json"))["clusters"][0]["cluster"]
token = k("-n", ns, "create", "token", "observer", "--duration=3h").strip()
cfg = {
    "apiVersion": "v1",
    "kind": "Config",
    "current-context": "obs",
    "clusters": [{"name": "lab", "cluster": cluster}],
    "users": [{"name": "obs", "user": {"token": token}}],
    "contexts": [{"name": "obs", "context":
                  {"cluster": "lab", "user": "obs", "namespace": ns}}]
}
f = Path("private/observer.json")
f.write_text(json.dumps(cfg, indent=2))
try:
    os.chmod(f, 0o600)
except Exception:
    pass
print("Generated private/observer.json successfully.")
