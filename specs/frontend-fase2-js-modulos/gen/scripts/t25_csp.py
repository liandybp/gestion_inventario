edit("app/main.py", '''    """H-006: security headers básicos (CSP, frame, sniffing, referrer)."""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "''', '''    """H-006: security headers básicos (CSP, frame, sniffing, referrer).

    ``script-src 'self'`` sin ``'unsafe-inline'``: todo el JS vive en ``app/static/js/``
    (fase 2 del release 0.5.0). ``style-src`` conserva ``'unsafe-inline'`` hasta la fase 3.
    """
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    if request.url.path.startswith("/static/js/"):
        # Los módulos que importa main.js no llevan ?v=: revalidar siempre (304 si no cambió)
        # para que un despliegue nunca mezcle módulos viejos y nuevos.
        response.headers["Cache-Control"] = "no-cache"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self'; "''')
