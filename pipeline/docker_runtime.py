"""DockerOperator helpers: mounts, container paths, env templates."""

from __future__ import annotations

import os
from pathlib import Path

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


class PipelineDockerOperator(DockerOperator):
    """DockerOperator without .sh template-file resolution (Airflow 3)."""

    template_ext: tuple[str, ...] = ()


CONTAINER_WORKDIR = "/mlops-assignment"
CONTAINER_MSA_ROOT = "/mini-swe-agent"
CONTAINER_SWEBENCH_CONFIG = (
    f"{CONTAINER_MSA_ROOT}/src/minisweagent/config/benchmarks/swebench.yaml"
)


def resolve_host_project_root(project_root: Path) -> Path:
    """Docker bind mounts must use host paths when Airflow runs inside Compose."""
    host = os.environ.get("HOST_REPO_DIR", "").strip()
    if host:
        return Path(host)
    return project_root


def resolve_mini_swe_agent_root(project_root: Path) -> Path | None:
    """Path visible to the current process (Airflow worker tasks)."""
    for candidate in (
        project_root / "mini-swe-agent",
        project_root.parent / "mini-swe-agent",
    ):
        if candidate.is_dir():
            return candidate.resolve()

    mini_swe_env = os.environ.get("MINI_SWE_AGENT_DIR", "").strip()
    if mini_swe_env:
        path = Path(mini_swe_env)
        if path.is_dir():
            return path.resolve()
    return None


def resolve_mini_swe_agent_host_path(project_root: Path) -> Path | None:
    """Host path for Docker bind mounts (daemon runs on the VM host)."""
    mini_swe_env = os.environ.get("MINI_SWE_AGENT_DIR", "").strip()
    if mini_swe_env:
        return Path(mini_swe_env)

    if os.environ.get("HOST_REPO_DIR", "").strip():
        return resolve_host_project_root(project_root).parent / "mini-swe-agent"

    return resolve_mini_swe_agent_root(project_root)


def container_agent_dir(run_id: str) -> str:
    return f"{CONTAINER_WORKDIR}/runs/{run_id}/run-agent"


def container_eval_dir(run_id: str) -> str:
    return f"{CONTAINER_WORKDIR}/runs/{run_id}/run-eval"


def container_preds_path(run_id: str) -> str:
    return f"{container_agent_dir(run_id)}/preds.json"


def build_pipeline_mounts(project_root: Path) -> list[Mount]:
    root = resolve_host_project_root(project_root)
    using_host_paths = bool(os.environ.get("HOST_REPO_DIR", "").strip())

    runs_source = root / "runs"
    if not using_host_paths:
        runs_source = runs_source.resolve()

    mounts: list[Mount] = [
        Mount(
            source=str(runs_source),
            target=f"{CONTAINER_WORKDIR}/runs",
            type="bind",
        ),
        Mount(
            source="/var/run/docker.sock",
            target="/var/run/docker.sock",
            type="bind",
        ),
    ]

    env_path = root / ".env"
    if env_path.is_file() or using_host_paths:
        env_source = str(env_path if using_host_paths else env_path.resolve())
        mounts.append(
            Mount(
                source=env_source,
                target=f"{CONTAINER_WORKDIR}/.env",
                type="bind",
                read_only=True,
            )
        )

    msa_root = resolve_mini_swe_agent_host_path(project_root)
    if msa_root is not None:
        mounts.append(
            Mount(
                source=str(msa_root),
                target=CONTAINER_MSA_ROOT,
                type="bind",
                read_only=True,
            )
        )

    scripts_source = root / "scripts"
    if scripts_source.is_dir() or using_host_paths:
        mounts.append(
            Mount(
                source=str(scripts_source if using_host_paths else scripts_source.resolve()),
                target=f"{CONTAINER_WORKDIR}/scripts",
                type="bind",
                read_only=True,
            )
        )

    return mounts


def pipeline_image() -> str:
    return os.environ.get("PIPELINE_IMAGE", "mlops-pipeline:latest")


def pipeline_container_user() -> str | None:
    """Return None — uv-managed Python in the image lives under /root (not traversable by AIRFLOW_UID).

    Re-enable AIRFLOW_UID after rebuilding with UV_PYTHON_INSTALL_DIR in the Dockerfile.
    """
    return None


def docker_network() -> str | None:
    network = os.environ.get("DOCKER_NETWORK", "").strip()
    return network or None


def _pipeline_container_env() -> dict[str, str]:
    return {
        "HOME": "/mlops-assignment/runs",
        "UV_CACHE_DIR": "/mlops-assignment/runs/.uv-cache",
        "AIRFLOW_RUNS_OWNER": os.environ.get("AIRFLOW_UID", ""),
        "AIRFLOW_RUNS_GROUP": "0",
    }


def agent_container_environment() -> dict[str, str]:
    return {
        **_pipeline_container_env(),
        "SUBSET": "{{ ti.xcom_pull(task_ids='prepare_run')['subset'] }}",
        "SPLIT": "{{ ti.xcom_pull(task_ids='prepare_run')['split'] }}",
        "MODEL": "{{ ti.xcom_pull(task_ids='prepare_run')['model'] }}",
        "SLICE": "{{ ti.xcom_pull(task_ids='prepare_run')['task_slice'] }}",
        "WORKERS": "{{ ti.xcom_pull(task_ids='prepare_run')['workers'] }}",
        "COST_LIMIT": "{{ ti.xcom_pull(task_ids='prepare_run')['cost_limit'] }}",
        "OUTPUT_DIR": "{{ ti.xcom_pull(task_ids='prepare_run')['container_agent_dir'] }}",
        "CONFIG_PATH": "{{ ti.xcom_pull(task_ids='prepare_run')['container_swebench_config'] }}",
        "NEBIUS_API_KEY": os.environ.get("NEBIUS_API_KEY", ""),
        "MSWEA_COST_TRACKING": "ignore_errors",
    }


def eval_container_environment() -> dict[str, str]:
    return {
        **_pipeline_container_env(),
        "PREDICTIONS_PATH": "{{ ti.xcom_pull(task_ids='prepare_run')['container_preds_path'] }}",
        "MAX_WORKERS": "{{ ti.xcom_pull(task_ids='prepare_run')['workers'] }}",
        "EVAL_RUN_ID": "{{ ti.xcom_pull(task_ids='prepare_run')['run_id'] }}",
        "DATASET_NAME": "{{ ti.xcom_pull(task_ids='prepare_run')['dataset_name'] }}",
        "SPLIT": "{{ ti.xcom_pull(task_ids='prepare_run')['split'] }}",
        "OUTPUT_DIR": "{{ ti.xcom_pull(task_ids='prepare_run')['container_eval_dir'] }}",
        "MSWEA_COST_TRACKING": "ignore_errors",
    }
