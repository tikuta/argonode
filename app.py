import os
import re
import json
import signal
import subprocess
import stat
from typing import Optional
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

def build_job_name(lab_name: str, source_folder: str) -> str:
    """Build a safe script/job name from lab + source folder."""

    def _norm(value: str) -> str:
        value = value.strip().lower()
        value = re.sub(r"[^a-z0-9_.@-]+", "-", value)
        return value.strip("-.")

    lab = _norm(lab_name)
    src = _norm(source_folder)
    if not lab or not src:
        raise ValueError("lab_name and source_folder must be non-empty")

    name = f"{lab}-{src}"[:120].strip("-.")
    if not name:
        raise ValueError("generated job name is empty")
    return name


def extract_dest_dir(script_content: str) -> Optional[str]:
    """Extract DEST_DIR value from script text, e.g. DEST_DIR="/path"."""
    m = re.search(r'^\s*DEST_DIR\s*=\s*"([^"]+)"\s*$', script_content, flags=re.MULTILINE)
    if not m:
        return None
    return m.group(1).strip()


@app.route("/")
def index():
    return render_template("index.html")


OFFLOAD_DIR = "/offload"
DEST_BASE_DIR = "/raid6/em/kriosdata"
RCLONE_LOG_PREFIX = "rclone_log"
RCLONE_LOG_SUFFIX = ".json"
LOG_TAIL_LINES = 40
MSG_WITH_FILENAME = {"Copied (new)", "Copied (replaced)", "Moved"}
FILE_NAME_KEYS = ("object", "file", "path", "name", "src")


def _is_safe_folder_name(name: str) -> bool:
    if not name:
        return False
    if "/" in name or "\\" in name:
        return False
    if name in {".", ".."}:
        return False
    return True


def _find_running_pids(script_path: str) -> list[int]:
    """Return PIDs for processes whose command line contains the script path."""
    try:
        result = subprocess.run(
            ["ps", "-eo", "pid=,args="],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:
        return []

    if result.returncode != 0:
        return []

    pids = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or script_path not in line:
            continue

        parts = line.split(maxsplit=1)
        if not parts:
            continue
        try:
            pids.append(int(parts[0]))
        except ValueError:
            continue
    return pids


def _build_script_path(lab_name: str, source_folder: str) -> str:
    job_name = build_job_name(lab_name, source_folder)
    return os.path.join(DEST_BASE_DIR, lab_name, source_folder, f"{job_name}.sh")


def _default_dest_dir(lab_name: str, source_folder: str) -> str:
    safe_lab = re.sub(r"[^a-zA-Z0-9_.@-]+", "-", lab_name).strip("-.")
    safe_source = re.sub(r"[^a-zA-Z0-9_.@-]+", "-", source_folder).strip("-.")
    return os.path.join(DEST_BASE_DIR, safe_lab, safe_source)


def _parse_transfer_request(require_script: bool = False):
    """Parse common transfer request fields from JSON payload."""
    data = request.get_json(silent=True)
    required_fields = ["lab_name", "source_folder"]
    if require_script:
        required_fields.insert(0, "script")

    if not data or any(field not in data for field in required_fields):
        return None, jsonify({"error": f"{', '.join(required_fields)} fields are required"}), 400

    lab_name = str(data["lab_name"]).strip()
    source_folder = str(data["source_folder"]).strip()
    if not _is_safe_folder_name(source_folder):
        return None, jsonify({"error": "valid source_folder is required"}), 400

    payload = {
        "lab_name": lab_name,
        "source_folder": source_folder,
    }
    if require_script:
        payload["script"] = str(data["script"])

    return payload, None, None


def _resolve_run_paths(lab_name: str, source_folder: str, script_content: str) -> tuple[str, str]:
    """Resolve destination directory and script path for run endpoint."""
    job_name = build_job_name(lab_name, source_folder)
    dest_dir = extract_dest_dir(script_content) or _default_dest_dir(lab_name, source_folder)
    script_path = os.path.join(dest_dir, f"{job_name}.sh")
    return dest_dir, script_path


def _start_script(script_path: str) -> int:
    """Start script in a detached session and return PID."""
    proc = subprocess.Popen(
        ["/bin/bash", script_path],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    return proc.pid


def _stop_pids(pids: list[int]) -> tuple[list[int], list[str]]:
    """Send SIGTERM to process groups for the provided pids."""
    stopped_pids = []
    errors = []
    for pid in pids:
        try:
            os.killpg(pid, signal.SIGTERM)
            stopped_pids.append(pid)
        except ProcessLookupError:
            continue
        except PermissionError:
            errors.append(f"permission denied for pid {pid}")
        except OSError as e:
            errors.append(f"pid {pid}: {e}")
    return stopped_pids, errors


def _find_latest_log_file(dest_dir: str) -> Optional[str]:
    try:
        log_files = [
            os.path.join(dest_dir, name)
            for name in os.listdir(dest_dir)
            if name.startswith(RCLONE_LOG_PREFIX)
            and name.endswith(RCLONE_LOG_SUFFIX)
            and os.path.isfile(os.path.join(dest_dir, name))
        ]
    except OSError:
        return None

    if not log_files:
        return None
    return max(log_files, key=os.path.getmtime)


def _format_latest_log_content(latest_path: str) -> tuple[Optional[str], Optional[str]]:
    """Return (formatted_log, error_message)."""
    try:
        with open(latest_path, "r", encoding="utf-8", errors="replace") as f:
            raw_lines = [line.strip() for line in f if line.strip()]
    except OSError as e:
        return None, str(e)

    tail_lines = raw_lines[-LOG_TAIL_LINES:]
    formatted_lines = []
    last_msg = ""

    for line in tail_lines:
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            formatted_lines.append(line)
            continue

        msg = ""
        if isinstance(item, dict) and isinstance(item.get("msg"), str):
            msg = item["msg"].strip()

        if msg:
            if msg in MSG_WITH_FILENAME:
                file_name = ""
                for key in FILE_NAME_KEYS:
                    value = item.get(key)
                    if isinstance(value, str) and value.strip():
                        file_name = value.strip()
                        break
                last_msg = f"{msg}: {file_name}" if file_name else msg
            else:
                last_msg = msg
            continue

        formatted_lines.append(json.dumps(item, ensure_ascii=False, indent=2))

    formatted_log = last_msg if last_msg else "\n\n".join(formatted_lines)
    return formatted_log, None


@app.route("/api/folders")
def list_folders():
    """List subdirectories of /offload, sorted by mtime descending (newest first)."""
    try:
        entries = [
            e for e in os.listdir(OFFLOAD_DIR)
            if os.path.isdir(os.path.join(OFFLOAD_DIR, e))
        ]
        entries.sort(
            key=lambda e: os.path.getmtime(os.path.join(OFFLOAD_DIR, e)),
            reverse=True,
        )
    except OSError as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"folders": entries})


@app.route("/api/latest-log")
def latest_log():
    """Return latest rclone log info for a lab name and source folder."""
    source_folder = (request.args.get("source_folder") or "").strip()
    lab_name = (request.args.get("lab_name") or "").strip()
    if not _is_safe_folder_name(source_folder):
        return jsonify({"error": "valid source_folder is required"}), 400
    if not lab_name:
        return jsonify({"error": "lab_name is required"}), 400

    script_path = None
    running_pids = []
    try:
        script_path = _build_script_path(lab_name, source_folder)
        running_pids = _find_running_pids(script_path)
    except ValueError:
        script_path = None

    dest_dir = _default_dest_dir(lab_name, source_folder)
    if not os.path.isdir(dest_dir):
        return jsonify(
            {
                "error": f"destination folder not found: {dest_dir}",
                "is_running": bool(running_pids),
                "running_pids": running_pids,
                "script_path": script_path,
            }
        ), 404

    latest_path = _find_latest_log_file(dest_dir)
    if not latest_path:
        return jsonify(
            {
                "error": "no log files found",
                "is_running": bool(running_pids),
                "running_pids": running_pids,
                "script_path": script_path,
            }
        ), 404

    stat_info = os.stat(latest_path)
    formatted_log, format_error = _format_latest_log_content(latest_path)
    if format_error:
        return jsonify({"error": format_error}), 500

    return jsonify(
        {
            "log_path": latest_path,
            "size_bytes": stat_info.st_size,
            "mtime": int(stat_info.st_mtime),
            "formatted_log": formatted_log,
            "is_running": bool(running_pids),
            "running_pids": running_pids,
            "script_path": script_path,
        }
    )


@app.route("/api/stop", methods=["POST"])
def stop_transfer():
    """Stop a running transfer for the selected lab and source folder."""
    payload, error_response, status = _parse_transfer_request(require_script=False)
    if error_response is not None:
        return error_response, status

    lab_name = payload["lab_name"]
    source_folder = payload["source_folder"]

    try:
        script_path = _build_script_path(lab_name, source_folder)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    running_pids = _find_running_pids(script_path)
    if not running_pids:
        return jsonify(
            {
                "message": "No running transfer found",
                "stopped_pids": [],
                "script_path": script_path,
            }
        )

    stopped_pids, errors = _stop_pids(running_pids)

    if errors and not stopped_pids:
        return jsonify({"error": "; ".join(errors), "script_path": script_path}), 500

    return jsonify(
        {
            "message": "Stop signal sent",
            "stopped_pids": stopped_pids,
            "errors": errors,
            "script_path": script_path,
        }
    )


@app.route("/api/run", methods=["POST"])
def run_transfer():
    """Run transfer script directly using /bin/bash."""
    payload, error_response, status = _parse_transfer_request(require_script=True)
    if error_response is not None:
        return error_response, status

    script_content = payload["script"]
    lab_name = payload["lab_name"]
    source_folder = payload["source_folder"]

    try:
        dest_dir, script_path = _resolve_run_paths(lab_name, source_folder, script_content)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    try:
        os.makedirs(dest_dir, exist_ok=True)

        with open(script_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(script_content)
        os.chmod(script_path, stat.S_IRWXU)

        pid = _start_script(script_path)

        return jsonify(
            {
                "message": "Started",
                "pid": pid,
                "command": f"/bin/bash {script_path}",
                "script_path": script_path,
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
