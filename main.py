from __future__ import annotations

import os
import uvicorn


def main() -> None:
    app_port = int(os.getenv("APP_PORT", "10000"))
    reload = os.getenv("RELOAD", "0") == "1"

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=app_port,
        reload=reload,
    )


if __name__ == "__main__":
    main()
