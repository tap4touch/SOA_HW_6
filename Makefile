.PHONY: lint clean

lint:
	uv run black ./
	uv run python -m mypy ./
	uv run ruff check ./

clean:
	uv run python -c "from pathlib import Path; import shutil; [shutil.rmtree(p, ignore_errors=True) for name in ('__pycache__','.mypy_cache','.pytest_cache','.ruff_cache','.hypothesis','.tox','.nox','htmlcov','build','dist','.eggs') for p in Path('.').rglob(name) if p.is_dir()]; [shutil.rmtree(p, ignore_errors=True) for p in Path('.').rglob('*.egg-info') if p.is_dir()]; [p.unlink() for pat in ('*.pyc','*.pyo','.coverage') for p in Path('.').rglob(pat) if p.is_file()]"