"""Application bootstrap entrypoint."""

from fastapi import FastAPI

from app.api.http_server import create_http_app
from app.api.routes import ControlPlaneRuntime
from app.config import Settings, get_settings
from app.logging_setup import setup_logging
from app.runtime import build_api_dependencies


def create_application(settings: Settings | None = None) -> FastAPI:
    """Create and return the configured FastAPI application."""
    resolved_settings = settings or get_settings()
    setup_logging(level=resolved_settings.log_level)
    runtime = ControlPlaneRuntime()
    dependencies = build_api_dependencies(resolved_settings, runtime=runtime)
    return create_http_app(settings=resolved_settings, dependencies=dependencies)


def main() -> None:
    """CLI-compatible process entrypoint."""
    _ = create_application()


if __name__ == "__main__":
    main()
