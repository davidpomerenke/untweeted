import io
import os
import re
import xml.etree.ElementTree as ET
from datetime import date
from io import BytesIO

import pymupdf
import requests
from atproto import Client, client_utils
from atproto_client.models.app.bsky.embed.images import Image, Main
from atproto_client.models.app.bsky.feed.post import ReplyRef
from atproto_client.models.app.bsky.richtext.facet import Link
from atproto_client.models.com.atproto.repo.strong_ref import Main as StrongRef
from docx import Document
from docx.table import Table
from dotenv import load_dotenv
from PIL import Image as PilImage

load_dotenv()


def extract_marc_data(xml_content):
    root = ET.fromstring(xml_content)
    ns = {"m": "http://www.loc.gov/MARC21/slim"}

    results = []
    for record in root.findall(".//m:record", ns):
        # Extract title (245 a + b)
        title_field = record.find('.//m:datafield[@tag="245"]', ns)
        title = ""
        if title_field is not None:
            parts = []
            for code in ["a", "b", "c"]:
                sf = title_field.find(f'm:subfield[@code="{code}"]', ns)
                if sf is not None and sf.text:
                    parts.append(sf.text.strip(":").strip("/").strip())
            title = " – ".join(parts)

        # Extract symbol (191 a)
        symbol_field = record.find(
            './/m:datafield[@tag="191"]/m:subfield[@code="a"]', ns
        )
        symbol = (
            symbol_field.text.strip()
            if symbol_field is not None and symbol_field.text
            else ""
        )

        # Extract date (269 a)
        date_field = record.find('.//m:datafield[@tag="269"]/m:subfield[@code="a"]', ns)
        date = (
            date_field.text.strip()
            if date_field is not None and date_field.text
            else ""
        )

        # Extract ID (001)
        id_field = record.find('.//m:controlfield[@tag="001"]', ns)
        id = id_field.text.strip() if id_field is not None and id_field.text else ""

        # Extract pages (300 a)
        pages_field = record.find(
            './/m:datafield[@tag="300"]/m:subfield[@code="a"]', ns
        )
        pages = (
            pages_field.text.strip(":").strip()
            if pages_field is not None and pages_field.text
            else ""
        )

        # Extract summary (500 a) - multiple entries
        summary_fields = record.findall(
            './/m:datafield[@tag="500"]/m:subfield[@code="a"]', ns
        )
        summary = [field.text.strip() for field in summary_fields if field.text]

        # Extract subject keywords (650 a)
        keywords = []
        for keyword_field in record.findall(
            './/m:datafield[@tag="650"]/m:subfield[@code="a"]', ns
        ):
            if keyword_field.text:
                keywords.append(keyword_field.text.strip())

        # Extract English PDF URL (856 u where y="English")
        pdf_url = ""
        for link_field in record.findall('.//m:datafield[@tag="856"]', ns):
            url_field = link_field.find('m:subfield[@code="u"]', ns)
            lang_field = link_field.find('m:subfield[@code="y"]', ns)
            if (
                url_field is not None
                and lang_field is not None
                and lang_field.text == "English"
            ):
                pdf_url = url_field.text.strip()
                break

        if title or symbol or date or id:
            results.append(
                {
                    "title": title,
                    "symbol": symbol,
                    "date": date,
                    "id": id,
                    "pdf_url": pdf_url,
                    "pages": pages,
                    "summary": summary,
                    "keywords": keywords,
                }
            )

    return results


def pdf_to_image(doc, page):
    try:
        if doc.page_count == 0:
            return None
        pix = doc[page].get_pixmap(matrix=pymupdf.Matrix(1.5, 1.5))  # Reduced DPI
        img = PilImage.open(io.BytesIO(pix.tobytes("png")))

        # # More aggressive resizing
        # if img.width > 800 or img.height > 1000:
        #     img.thumbnail((800, 1000), PilImage.Resampling.LANCZOS)

        buf = io.BytesIO()
        quality = 100
        img.save(
            buf, format="JPEG", quality=quality, optimize=True
        )
        # Check size and compress more if needed
        while len(buf.getvalue()) > 950_000:  # 950KB
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=quality, optimize=True)
            quality -= 10
            print(quality, len(buf.getvalue()))

        return buf.getvalue()
    except Exception as e:
        print(f"PDF error: {e}")
        return None


def chunk_text(text, max_length):
    """Split text by sentences first, then by words if needed."""
    chunks = []

    # Split by sentences (periods followed by space or end, but not after p., pp., paras.)
    sentences = re.split(r"(?<!\bp)(?<!\bpp)(?<!paras)\.(?:\s|$)", text)
    sentences = [s.strip() + "." for s in sentences if s.strip()]
    if sentences and sentences[-1] == ".":
        sentences.pop()  # Remove empty last element

    for sentence in sentences:
        if len(sentence) <= max_length:
            # Sentence fits, add as single chunk
            chunks.append(sentence)
        else:
            # Sentence too long, split by words with ellipsis
            words = sentence.split()
            current_chunk = ""

            for i, word in enumerate(words):
                if len(current_chunk + " " + word) > max_length - 4:
                    if current_chunk:
                        chunks.append(current_chunk + "...")
                    current_chunk = word
                else:
                    current_chunk = (
                        current_chunk + " " + word if current_chunk else word
                    )

            if current_chunk:
                # Add "..." at start if this was a continuation
                if len(chunks) > 0 and chunks[-1].endswith("..."):
                    chunks.append("..." + current_chunk)
                else:
                    chunks.append(current_chunk)

    return chunks


url = "https://digitallibrary.un.org/search"
params = {
    "cc": "Reports",
    "ln": "en",
    "sf": "year",
    "rg": 100,
    "c": "Reports",
    "of": "xm",
}
xml = requests.get(url, params=params).text
records = extract_marc_data(xml)[:10]

client = Client("https://bsky.social")
profile = client.login("unreported.bsky.social", os.environ["BSKY_PASSWORD"])

response = client.get_author_feed(
    "unreported.bsky.social", include_pins=False, filter="posts_no_replies"
)
feed = response.feed
while response.cursor is not None:
    response = client.get_author_feed(
        "unreported.bsky.social", include_pins=False, filter="posts_no_replies"
    )
    feed += response.feed

entries = [entry.post.record.facets for entry in feed]
facets = [facet for facets in entries for facet in facets]
links = [
    feat.uri for facet in facets for feat in facet.features if isinstance(feat, Link)
]

# for entry in feed:
#     client.delete_post(entry.post.uri)

unposted = [
    record for record in records if not any(record["id"] in link for link in links)
]


def post_record(record):
    images = []
    if record["pdf_url"]:
        response = requests.get(record["pdf_url"], timeout=30)
        response.raise_for_status()
        if not response.content:
            return None
        doc = pymupdf.open(stream=response.content, filetype="pdf")
        bytes = pdf_to_image(doc, 0)
        if bytes:
            ref = client.upload_blob(bytes)
            image = Image(
                alt="Screenshot of the first page of the report", image=ref.blob
            )
            images.append(image)
        bytes = pdf_to_image(doc, 1)
        if bytes:
            ref = client.upload_blob(bytes)
            image = Image(
                alt="Screenshot of the second page of the report", image=ref.blob
            )
            images.append(image)
        doc.close()

    MAX_LENGTH = 300
    BASE_LENGTH = 80
    title = (
        record["title"][: MAX_LENGTH - BASE_LENGTH - 3] + "..."
        if len(record["title"]) + BASE_LENGTH > MAX_LENGTH
        else record["title"]
    )
    date_ = date.fromisoformat(record["date"]).strftime("%A, %b %-d")
    text = (
        client_utils.TextBuilder()
        .text(f"New report released! From {date_}:\n\n")
        .text(f"❞ {title}")
        .text("\n\n→ ")
    )
    text.link(
        "Read it here",
        f"https://digitallibrary.un.org/record/{record['id']}?ln=en&v=pdf",
    ).text(f" ({record['pages']})\n\n")

    post = client.send_post(text, embed=Main(images=images) if images else None)

    root = StrongRef(cid=post.cid, uri=post.uri)
    prev = root

    for summary_text in record["summary"]:
        chunks = chunk_text(summary_text, MAX_LENGTH - 10)
        for chunk in chunks:
            post2 = client.send_post(chunk, reply_to=ReplyRef(parent=prev, root=root))
            prev = StrongRef(cid=post2.cid, uri=post2.uri)

    # try to extract summary from DOCX (but neither DOCX nor summary are always available)
    summary_detailed = None
    try:
        response = requests.get(
            f"https://documents.un.org/api/symbol/access?s={record['symbol']}&l=en&t=docx"
        )
        doc = Document(BytesIO(response.content))
        tables = [c for c in doc.iter_inner_content() if isinstance(c, Table)]
        for table in tables:
            row_texts = [
                " | ".join([c.text.strip() for c in row.cells]) for row in table.rows
            ]
            if row_texts[0].strip() == "Summary":
                summary_detailed = row_texts[1:]
                break
        for para in summary_detailed:
            chunks = chunk_text(para, MAX_LENGTH - 10)
            for chunk in chunks:
                postn = client.send_post(
                    chunk, reply_to=ReplyRef(parent=prev, root=root)
                )
                prev = StrongRef(cid=postn.cid, uri=postn.uri)
    except Exception as e:
        pass

    if len(record["keywords"]) > 0:
        text = client_utils.TextBuilder()
        for kw in record["keywords"]:
            if len(text.build_text() + kw.replace(" ", "")) + 2 < MAX_LENGTH:
                text.tag(
                    "#"
                    + kw.replace("'", "").title().replace(" ", "").replace("-", "")
                    + " ",
                    kw.replace("'", "").lower().replace(" ", "").replace("-", ""),
                )
        _post3 = client.send_post(text, reply_to=ReplyRef(parent=prev, root=root))


for record in unposted[::-1][:1]:
    print(record["date"], record["title"])
    post_record(record)
