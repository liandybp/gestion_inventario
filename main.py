from __future__ import annotations

import os

import uvicorn


def main() -> None:
    app_port = os.getenv("APP_PORT", "8000")
    uvicorn.run("app.main:app", host="127.0.0.1", port=int(app_port), reload=True)


if __name__ == "__main__":
    main()
