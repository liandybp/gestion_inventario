import io, pdfplumber
from fpdf import FPDF
PHONE = b'<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24"><path fill="none" stroke="#115A9C" stroke-width="1.6" stroke-linejoin="round" d="M5 3h4l2 5-2.5 1.5a11 11 0 0 0 6 6L16 13l5 2v4a2 2 0 0 1-2 2A16 16 0 0 1 3 5a2 2 0 0 1 2-2z"/></svg>'
MAIL = b'<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24"><rect x="3" y="5" width="18" height="14" rx="2" fill="none" stroke="#115A9C" stroke-width="1.6"/><path d="M3 7l9 6 9-6" fill="none" stroke="#115A9C" stroke-width="1.6"/></svg>'
pdf = FPDF(format="A4", unit="mm"); pdf.add_page()
pdf.image(io.BytesIO(PHONE), x=107, y=20, w=4, h=4)
pdf.image(io.BytesIO(MAIL), x=107, y=26, w=4, h=4)
pdf.set_font("Helvetica", "", 9)
pdf.set_xy(112, 20); pdf.cell(80, 4, "54664732 / 50781349")
pdf.set_xy(112, 26); pdf.cell(80, 4, "autosandero@gmail.com")
b = bytes(pdf.output()); open("/tmp/sdd-probe/s9.pdf","wb").write(b)
with pdfplumber.open(io.BytesIO(b)) as p:
    pg = p.pages[0]; print(pg.extract_text()); print("curves", len(pg.curves), "rects", len(pg.rects), "lines", len(pg.lines))
