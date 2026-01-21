from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import IO, Optional

import pdfplumber


@dataclass(frozen=True)
class InvoiceLine:
    sku: str
    name: str
    quantity: float
    net_unit_price: float


@dataclass(frozen=True)
class ParsedInvoice:
    invoice_number: Optional[str]
    invoice_date: Optional[datetime]
    lines: list[InvoiceLine]


def _parse_spanish_number(s: str) -> float:
    v = (s or "").strip()
    if not v:
        raise ValueError("empty")

    v = v.replace("€", "").replace("\u00a0", " ").strip()
    v = v.replace(" ", "")

    if "," in v:
        v = v.replace(".", "")
        v = v.replace(",", ".")

    return float(v)


def _parse_invoice_number(text: str) -> Optional[str]:
    m = re.search(r"N[úu]mero de factura:\s*([0-9]+)", text)
    if not m:
        return None
    return m.group(1).strip()


def _parse_invoice_date(text: str) -> Optional[datetime]:
    m = re.search(r"Fecha de factura:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})", text)
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group(1), "%d.%m.%Y")
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc)


def parse_autodoc_pdf(file_obj: IO[bytes]) -> ParsedInvoice:
    try:
        file_obj.seek(0)
    except Exception:
        pass

    raw_lines: list[str] = []
    with pdfplumber.open(file_obj) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            for ln in txt.splitlines():
                ln = (ln or "").strip()
                if ln:
                    raw_lines.append(ln)

    full_text = "\n".join(raw_lines)
    invoice_number = _parse_invoice_number(full_text)
    invoice_date = _parse_invoice_date(full_text)

    lines: list[InvoiceLine] = []

    for ln in raw_lines:
        if not re.match(r"^\d+\.\s+", ln):
            continue

        parts = ln.split()
        if len(parts) < 4:
            continue

        pos = parts[0]
        if not pos.endswith("."):
            continue

        sku = parts[1].strip()
        if not sku or sku == "-":
            continue

        euro_matches = list(re.finditer(r"(\d[\d\.]*,\d+)\s*€", ln))
        if not euro_matches:
            continue

        first_euro = euro_matches[0]
        price_str = first_euro.group(1)

        before_price = ln[: first_euro.start()].strip()
        qty_match = re.search(r"(\d+(?:[\.,]\d+)?)\s*$", before_price)
        if not qty_match:
            continue

        try:
            quantity = _parse_spanish_number(qty_match.group(1))
            net_unit_price = _parse_spanish_number(price_str)
        except ValueError:
            continue

        if quantity <= 0 or net_unit_price <= 0:
            continue

        name_span = ln

        code_match = re.search(r"\s(\d{8})\s", ln)
        if code_match:
            name_span = ln[: code_match.start()].strip()

        name_span = re.sub(r"^\d+\.\s+", "", name_span).strip()
        if name_span.startswith(sku):
            name_span = name_span[len(sku) :].strip()

        if not name_span:
            continue

        if name_span.lstrip("-").strip().lower().startswith("bono"):
            continue

        lines.append(
            InvoiceLine(
                sku=sku,
                name=name_span,
                quantity=float(quantity),
                net_unit_price=float(net_unit_price),
            )
        )

    try:
        file_obj.seek(0)
    except Exception:
        pass

    return ParsedInvoice(invoice_number=invoice_number, invoice_date=invoice_date, lines=lines)


def _parse_hm_invoice_number(text: str) -> Optional[str]:
    m = re.search(r"N[ÚU]MERO\s+DE\s+FACTURA\s+([A-Z0-9-]+)", text, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1).strip()


def _parse_hm_invoice_date(text: str) -> Optional[datetime]:
    m = re.search(
        r"FECHA\s+DE\s+FACTURA\s+([0-9]{1,2})\s+([A-Za-zÁÉÍÓÚáéíóúÑñ]+)\s+([0-9]{4})",
        text,
        flags=re.IGNORECASE,
    )
    if not m:
        return None
    day = int(m.group(1))
    mon = (m.group(2) or "").strip().lower()
    year = int(m.group(3))
    months = {
        "enero": 1,
        "febrero": 2,
        "marzo": 3,
        "abril": 4,
        "mayo": 5,
        "junio": 6,
        "julio": 7,
        "agosto": 8,
        "septiembre": 9,
        "setiembre": 9,
        "octubre": 10,
        "noviembre": 11,
        "diciembre": 12,
    }
    month = months.get(mon)
    if not month:
        return None
    return datetime(year, month, day, tzinfo=timezone.utc)


def parse_hm_pdf(file_obj: IO[bytes]) -> ParsedInvoice:
    try:
        file_obj.seek(0)
    except Exception:
        pass

    raw_lines: list[str] = []
    with pdfplumber.open(file_obj) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            for ln in txt.splitlines():
                ln = (ln or "").strip()
                if ln:
                    raw_lines.append(ln)

    full_text = "\n".join(raw_lines)
    up = full_text.upper()
    if "H&M" not in up and "NUMERO DE FACTURA" not in up and "NÚMERO DE FACTURA" not in full_text:
        raise ValueError("Not an H&M invoice")

    invoice_number = _parse_hm_invoice_number(full_text)
    invoice_date = _parse_hm_invoice_date(full_text)

    table_start = None
    for i, ln in enumerate(raw_lines):
        up_ln = ln.upper()
        if (
            ("NÚM" in up_ln or "NUM" in up_ln)
            and ("ART" in up_ln or "ARTÍCULO" in up_ln or "ARTICULO" in up_ln)
            and ("TOTAL" in up_ln or "IMPORTE" in up_ln)
        ):
            table_start = i + 1
            break
        if "NÚM. ARTÍCULO" in up_ln or "NUM. ARTICULO" in up_ln or "NÚMERO DE ARTÍCULO" in up_ln:
            table_start = i + 1
            break

    stop_words = (
        "PRODUCTO TOTAL",
        "DESCUENTOS:",
        "ENTREGA:",
        "TOTAL:",
        "BASE IMPONIBLE",
    )

    lines: list[InvoiceLine] = []
    scan_start = table_start if table_start is not None else 0
    row_start_re = re.compile(r"^\d{8,16}\b")
    current = ""
    for ln in raw_lines[scan_start:]:
        up_ln = ln.upper()
        if any(w in up_ln for w in stop_words):
            break

        if ("NÚM" in up_ln or "NUM" in up_ln) and ("ART" in up_ln or "ARTÍCULO" in up_ln or "ARTICULO" in up_ln):
            if current:
                parsed_line = _parse_hm_row(current)
                if parsed_line is not None:
                    lines.append(parsed_line)
            current = ""
            continue

        if row_start_re.match(ln) and current:
            row = current
            current = ln
            parsed_line = _parse_hm_row(row)
            if parsed_line is not None:
                lines.append(parsed_line)
            continue

        current = (current + " " + ln).strip() if current else ln

    if current:
        parsed_line = _parse_hm_row(current)
        if parsed_line is not None:
            lines.append(parsed_line)

    try:
        file_obj.seek(0)
    except Exception:
        pass

    return ParsedInvoice(invoice_number=invoice_number, invoice_date=invoice_date, lines=lines)


def _parse_hm_row(row: str) -> Optional[InvoiceLine]:
    s = (row or "").strip()
    if not s:
        return None
    msku = re.match(r"^(\d{8,16})\b", s)
    if not msku:
        return None
    sku = msku.group(1).strip()

    perc = None
    mperc = list(re.finditer(r"(\d+(?:[\.,]\d+)?)%", s))
    if mperc:
        try:
            perc = float(mperc[-1].group(1).replace(",", ".")) / 100.0
        except Exception:
            perc = None
    if perc is None:
        perc = 0.21

    rest = s[msku.end() :].strip()
    rest_no_pct = re.sub(r"\d+(?:[\.,]\d+)?\s*%", " ", rest)
    money_tokens = list(re.finditer(r"-?\d[\d\.,]*\d", rest_no_pct))
    if len(money_tokens) < 2:
        return None

    total_match = money_tokens[-1]

    qty_match_obj: Optional[re.Match[str]] = None
    if mperc:
        cutoff = mperc[-1].start()
        before_pct = s[:cutoff]
        before_pct_rest = before_pct[msku.end() :]
        qty_candidates = list(re.finditer(r"\d+(?:[\.,]\d+)?", before_pct_rest))
        for cand in reversed(qty_candidates):
            try:
                qv = _parse_spanish_number(cand.group(0))
            except Exception:
                continue
            if qv <= 0 or qv > 1000:
                continue
            txt = cand.group(0)
            if "," in txt or "." in txt:
                frac = txt.split(",", 1)[1] if "," in txt else txt.split(".", 1)[1]
                if frac not in ("0", "00", "000"):
                    continue
            qty_match_obj = cand
            break

    if qty_match_obj is None:
        for cand in reversed(money_tokens[:-1]):
            try:
                qv = _parse_spanish_number(cand.group(0))
            except Exception:
                continue
            if qv <= 0 or qv > 1000:
                continue
            txt = cand.group(0)
            if "," in txt or "." in txt:
                frac = txt.split(",", 1)[1] if "," in txt else txt.split(".", 1)[1]
                if frac not in ("0", "00", "000"):
                    continue
            qty_match_obj = cand
            break

    if qty_match_obj is None:
        return None

    qty_abs_start = msku.end() + qty_match_obj.start()
    prefix = s[:qty_abs_start].strip()
    qty_str = qty_match_obj.group(0)
    try:
        quantity = _parse_spanish_number(qty_str)
        total_gross = _parse_spanish_number(total_match.group(0))
    except Exception:
        return None

    if quantity <= 0 or total_gross <= 0:
        return None

    name_span = prefix
    if name_span.startswith(sku):
        name_span = name_span[len(sku) :].strip()
    name_span = re.sub(r"\s+" + re.escape(qty_str) + r"\s*$", "", name_span).strip()
    if not name_span:
        return None

    net_unit_price = (float(total_gross) / float(quantity)) / (1.0 + float(perc))
    if net_unit_price <= 0:
        return None

    return InvoiceLine(sku=sku, name=name_span, quantity=float(quantity), net_unit_price=float(net_unit_price))


def _parse_zara_invoice_number(text: str) -> Optional[str]:
    m = re.search(r"N[º°]\s*DOCUMENTO:\s*([^\s]+)", text, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1).strip()


def _parse_zara_invoice_date(text: str) -> Optional[datetime]:
    m = re.search(
        r"FECHA\s+OPERACI[ÓO]N/EXPEDICI[ÓO]N:\s*([0-9]{2}-[0-9]{2}-[0-9]{4})",
        text,
        flags=re.IGNORECASE,
    )
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group(1), "%d-%m-%Y")
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc)


def parse_zara_pdf(file_obj: IO[bytes]) -> ParsedInvoice:
    try:
        file_obj.seek(0)
    except Exception:
        pass

    raw_lines: list[str] = []
    with pdfplumber.open(file_obj) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            for ln in txt.splitlines():
                ln = (ln or "").strip()
                if ln:
                    raw_lines.append(ln)

    full_text = "\n".join(raw_lines)
    if "ZARA" not in full_text.upper():
        raise ValueError("Not a ZARA invoice")

    invoice_number = _parse_zara_invoice_number(full_text)
    invoice_date = _parse_zara_invoice_date(full_text)

    table_start = None
    for i, ln in enumerate(raw_lines):
        up = ln.upper()
        if "REFERENCIA" in up and "UNIDADES" in up and "IMPORTE" in up:
            table_start = i + 1
            break

    def _parse_zara_lines_by_words() -> list[InvoiceLine]:
        sku_re = re.compile(r"^\d+/\d{3,5}/\d{3}$")
        money_re = re.compile(r"^\d[\d\.]*,\d+$")
        lines_out: list[InvoiceLine] = []

        try:
            file_obj.seek(0)
        except Exception:
            pass

        with pdfplumber.open(file_obj) as pdf:
            for page in pdf.pages:
                words = page.extract_words(use_text_flow=True) or []
                if not words:
                    continue

                sorted_words = sorted(words, key=lambda w: (float(w.get("top", 0.0)), float(w.get("x0", 0.0))))
                min_x0 = None
                for w in sorted_words:
                    try:
                        x0 = float(w.get("x0", 0.0))
                    except Exception:
                        continue
                    min_x0 = x0 if min_x0 is None else min(min_x0, x0)
                ref_x_max = (min_x0 + 220.0) if min_x0 is not None else 220.0

                started = False
                current_tokens: list[str] = []
                for w in sorted_words:
                    txt = (w.get("text") or "").strip()
                    if not txt:
                        continue
                    up_txt = txt.upper()

                    if up_txt == "REFERENCIA":
                        started = True
                        continue
                    if not started:
                        continue
                    if up_txt == "DESGLOSE":
                        break

                    try:
                        x0 = float(w.get("x0", 0.0))
                    except Exception:
                        x0 = 0.0

                    is_row_start = (
                        (up_txt == "SHIPPING")
                        or (sku_re.match(txt) is not None)
                    ) and (x0 <= float(ref_x_max))

                    if is_row_start:
                        if current_tokens:
                            parsed = _parse_zara_row_tokens(current_tokens, sku_re=sku_re, money_re=money_re)
                            if parsed is not None:
                                lines_out.append(parsed)
                        current_tokens = [txt]
                    else:
                        current_tokens.append(txt)

                if current_tokens:
                    parsed = _parse_zara_row_tokens(current_tokens, sku_re=sku_re, money_re=money_re)
                    if parsed is not None:
                        lines_out.append(parsed)

        return lines_out

    def _parse_zara_row_tokens(
        row_tokens: list[str],
        *,
        sku_re: re.Pattern[str],
        money_re: re.Pattern[str],
    ) -> Optional[InvoiceLine]:
        if not row_tokens:
            return None
        sku = (row_tokens[0] or "").strip()
        if not sku or sku.upper() == "SHIPPING" or (not sku_re.match(sku)):
            return None

        up_all = " ".join(row_tokens).upper()
        if "SHIPPING" in up_all and "HANDLING" in up_all:
            return None

        perc = 0.21
        for t in row_tokens:
            if "%" in t:
                try:
                    perc = float(t.replace("%", "").replace(".", "").replace(",", ".")) / 100.0
                except Exception:
                    perc = 0.21

        # qty + importe from the end (avoid picking VAT like 21,00%)
        def _is_qty_token(tok: str) -> bool:
            if not re.fullmatch(r"\d+(?:[\.,]\d+)?", tok):
                return False
            try:
                v = _parse_spanish_number(tok)
            except Exception:
                return False
            if v <= 0 or v > 1000:
                return False
            if "," in tok or "." in tok:
                frac = tok.split(",", 1)[1] if "," in tok else tok.split(".", 1)[1]
                if frac not in ("0", "00", "000"):
                    return False
            return True

        qty_tok: Optional[str] = None
        imp_tok: Optional[str] = None
        for tok in reversed(row_tokens):
            t = (tok or "").strip()
            if not t:
                continue
            if imp_tok is None and ("%" not in t) and money_re.match(t):
                imp_tok = t
                continue
            if imp_tok is not None and qty_tok is None and _is_qty_token(t):
                qty_tok = t
                break

        if qty_tok is None or imp_tok is None:
            return None

        try:
            qty = _parse_spanish_number(qty_tok)
            gross_total = _parse_spanish_number(imp_tok)
        except Exception:
            return None

        if qty <= 0 or gross_total <= 0:
            return None

        # name up to first money token (net unit usually)
        first_money_idx = None
        for i, tok in enumerate(row_tokens[1:], start=1):
            if money_re.match(tok):
                first_money_idx = i
                break
        name_tokens = row_tokens[1:first_money_idx] if first_money_idx is not None else row_tokens[1:]
        name = " ".join(name_tokens).strip()
        if not name:
            name = sku

        if "ENVÍO" in name.upper() or "MANIPULACI" in name.upper():
            return None

        net_unit_price = (float(gross_total) / float(qty)) / (1.0 + float(perc))
        if net_unit_price <= 0:
            return None

        return InvoiceLine(sku=sku, name=name, quantity=float(qty), net_unit_price=float(net_unit_price))

    lines: list[InvoiceLine] = []
    try:
        lines = _parse_zara_lines_by_words()
    except Exception:
        lines = []

    # Fallback: old text-based parsing if word-based did not produce anything
    if not lines and table_start is not None:
        row_start_re = re.compile(r"^(?:\d+/\d{3,5}/\d{3}|SHIPPING)\b")
        stop_words = (
            "BASE IMPONIBLE",
            "CUOTA",
            "TOTAL FACTURA",
            "TOTAL DOCUMENTO",
            "RESUMEN",
        )
        tail_re = re.compile(
            r"(?P<price_sin>\d[\d\.,]*)\s+"
            r"(?P<iva>\d[\d\.,]*)\s*%\s+"
            r"(?P<price_con>\d[\d\.,]*)\s+"
            r"(?P<units>\d+(?:[\.,]\d+)?)\s+"
            r"(?P<importe>\d[\d\.,]*)\s*(?:€)?\s*$"
        )
        tail_fallback_re = re.compile(
            r"(?P<units>\d+(?:[\.,]\d+)?)\s+"
            r"(?P<importe>\d[\d\.,]*)\s*(?:€)?\s*$"
        )

        def _flush_zara_row(row: str) -> None:
            m = tail_re.search(row)
            mf = None if m else tail_fallback_re.search(row)
            if not m and not mf:
                return

            match_obj = m or mf
            prefix = row[: match_obj.start()].strip()
            sku = prefix.split()[0] if prefix else ""
            name = prefix[len(sku) :].strip() if sku else ""
            if not sku or sku.upper() == "SHIPPING":
                return
            if "ENVÍO" in name.upper() or "MANIPULACI" in name.upper():
                return

            try:
                qty = _parse_spanish_number(match_obj.group("units"))
                if m is not None:
                    net_price = _parse_spanish_number(m.group("price_sin"))
                else:
                    gross_total = _parse_spanish_number(match_obj.group("importe"))
                    net_price = (float(gross_total) / float(qty)) / 1.21
            except Exception:
                return

            if qty > 0 and net_price > 0 and name:
                lines.append(InvoiceLine(sku=sku, name=name, quantity=float(qty), net_unit_price=float(net_price)))

        current = ""
        for ln in raw_lines[table_start:]:
            up = ln.upper()
            if any(w in up for w in stop_words):
                break
            if "REFERENCIA" in up and "UNIDADES" in up and "IMPORTE" in up:
                if current:
                    _flush_zara_row(current)
                current = ""
                continue

            if row_start_re.match(ln) and current:
                _flush_zara_row(current)
                current = ln
            else:
                current = (current + " " + ln).strip() if current else ln

        if current:
            _flush_zara_row(current)

    try:
        file_obj.seek(0)
    except Exception:
        pass

    return ParsedInvoice(invoice_number=invoice_number, invoice_date=invoice_date, lines=lines)


def parse_invoice_pdf(file_obj: IO[bytes]) -> ParsedInvoice:
    try:
        parsed = parse_hm_pdf(file_obj)
        if parsed.lines:
            return parsed
    except Exception:
        pass
    try:
        parsed = parse_zara_pdf(file_obj)
        if parsed.lines:
            return parsed
    except Exception:
        pass
    return parse_autodoc_pdf(file_obj)
