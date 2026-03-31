"""FastAPI entrypoint for the registration API."""

from runtime import app, router
import business

app.include_router(router)
