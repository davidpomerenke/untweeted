import io
import json
import os
import xml.etree.ElementTree as ET
from datetime import date
from functools import partial
from io import BytesIO
from urllib.parse import urlencode

import pymupdf
import pysbd
import requests
import tweepy
from atproto import Client, Request, client_utils
from atproto_client.models.app.bsky.embed.images import Image, Main
from atproto_client.models.app.bsky.feed.post import ReplyRef
from atproto_client.models.app.bsky.richtext.facet import Link
from atproto_client.models.com.atproto.repo.strong_ref import Main as StrongRef
from docx import Document
from docx.table import Table
from dotenv import load_dotenv
from httpx import Timeout
from PIL import Image as PilImage
from playwright.sync_api import sync_playwright
from pycountry import countries
from pypopulation import get_population_a3
from tweepy.errors import Forbidden, TooManyRequests

load_dotenv()

ns = {"m": "http://www.loc.gov/MARC21/slim"}


def fetch_url_with_playwright(url, wait_until="networkidle", return_text=False):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        page = context.new_page()
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'platform', {get: () => 'MacIntel'});
        """)
        response = page.goto(url, wait_until=wait_until, timeout=60000)
        if return_text:
            result = page.evaluate("() => document.body.innerText").strip()
        else:
            result = response.body()
        browser.close()
        return result


def fetch_with_browser(url):
    content = fetch_url_with_playwright(url, wait_until="networkidle", return_text=False)
    xml = content.decode('utf-8')
    # Remove browser message line if present
    lines = xml.split("\n")
    xml = "\n".join(
        l for l in lines if not l.startswith("This XML file does not appear")
    )
    # Add XML declaration if missing
    if not xml.startswith("<?xml"):
        xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml
    return xml


def get_field(record, tag, code=None, multiple=True):
    query = f'.//m:datafield[@tag="{tag}"]' + (
        f'/m:subfield[@code="{code}"]' if code else ""
    )
    fields = record.findall(query, ns)
    fields = [field.text.strip(":").strip() for field in fields if field.text]
    if not multiple:
        fields = fields[0] if fields else None
    return fields


def marc_xml_to_reports(xml_content):
    root = ET.fromstring(xml_content)

    results = []
    for record in root.findall(".//m:record", ns):
        _get_field = partial(get_field, record)
        id = record.find('.//m:controlfield[@tag="001"]', ns).text.strip()
        titles = _get_field(245, "a") + _get_field(245, "b") + _get_field(245, "c")
        titles = [t.strip(":").strip("/").strip() for t in titles]
        title = " – ".join(titles)
        pages = _get_field("300", "a", False)
        pages = pages.replace("[", "").replace("]", "") if pages else None
        pdf_urls = _get_field("856", "u")
        pdf_urls = [url for url in pdf_urls if url.endswith("-EN.pdf")]
        if not pdf_urls:
            continue

        record = {
            "id": id,
            "symbol": _get_field("191", "a", False),
            "title": title,
            "date": _get_field("269", "a", False),
            "pages": pages,
            "summary": _get_field("500", "a"),
            "keywords": _get_field("650", "a"),
            "pdf_url": pdf_urls[0],
        }
        results.append(record)
    return results


def marc_xml_to_resolutions(xml_content):
    root = ET.fromstring(xml_content)

    results = []
    for record in root.findall(".//m:record", ns):
        _get_field = partial(get_field, record)
        id = record.find('.//m:controlfield[@tag="001"]', ns).text.strip()
        titles = _get_field(245, "a") + _get_field(245, "b") + _get_field(245, "c")
        titles = [t.strip(":").strip("/").strip() for t in titles]
        title = " – ".join(titles)
        votes = {}
        for field in record.findall('.//m:datafield[@tag="967"]', ns):
            country = field.find('.//m:subfield[@code="c"]', ns)
            vote = field.find('.//m:subfield[@code="d"]', ns)
            if country is not None and vote is not None:
                votes[country.text.strip()] = vote.text.strip()
        draft_resolution = _get_field("993", "a", False)
        record = {
            "id": id,
            "symbol": _get_field("191", "a", False),
            "title": title,
            "date": _get_field("269", "a", False),
            "votes": votes,
            "note": _get_field("591", "a"),
            "resolution": _get_field("791", "a", False),
            "draft_resolution": draft_resolution,
        }
        results.append(record)
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
        img.save(buf, format="JPEG", quality=quality, optimize=True)
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


def get_summary(record):
    try:
        url = f"https://documents.un.org/api/symbol/access?s={record['symbol']}&l=en&t=docx"
        content = fetch_url_with_playwright(url, wait_until="load")
        doc = Document(BytesIO(content))
        tables = [c for c in doc.iter_inner_content() if isinstance(c, Table)]
        for table in tables:
            row_texts = [
                " | ".join([c.text.strip() for c in row.cells]) for row in table.rows
            ]
            if row_texts[0].strip() == "Summary":
                return row_texts[1:]
        return []
    except Exception:
        return []


def get_images(pdf_url, page_nrs=[0, 1]):
    response = requests.get(pdf_url, timeout=60)
    pdf_content = response.content if response.status_code == 200 else None
    
    if not pdf_content:
        return []
    doc = pymupdf.open(stream=pdf_content, filetype="pdf")
    images = [pdf_to_image(doc, page_nr) for page_nr in page_nrs]
    images = [image for image in images if image is not None]
    doc.close()
    return images


def chunk_text(text, max_length):
    """Split text by sentences first, then by words if needed."""
    chunks = []

    seg = pysbd.Segmenter(language="en", clean=False)
    abbrev_instance = seg.language_module.Abbreviation()
    abbrev_instance.ABBREVIATIONS.extend(["paras", "pp", "p"])
    sentences = seg.segment(text)

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


def get_flags(countries_):
    countries_ = sorted(countries_, key=lambda c: get_population_a3(c) or 0, reverse=True)
    flags = [countries.get(alpha_3=c).flag for c in countries_ if countries.get(alpha_3=c)]
    return "".join(flags)


def get_votes(record):
    text = ""
    if record["votes"]:
        text += "Votes:\n\n"
        for letter, vote in [("Y", "Yes"), ("N", "No"), ("A", "Abstention")]:
            countries_ = [c for c, v in record["votes"].items() if v == letter]
            text += f"{len(countries_)}x {vote}{': ' if countries_ else ''}{get_flags(countries_)}\n"
        text += "\n"
    elif record["note"]:
        if record["note"][0].lower() == "adopted without vote":
            text += "Adopted without vote.\n\n"
    return text


def get_draft_resolution(record):
    if not record["draft_resolution"]:
        return None
    params = {
        "cc": "Draft resolutions and decisions",
        "ln": "en",
        "p": f"documentsymbol:{record['draft_resolution']}",
        "sf": "year",
        "rg": 20,
        "c": "Draft resolutions and decisions",
        "of": "xm",
    }
    url = f"https://digitallibrary.un.org/search?{urlencode(params)}"
    xml = fetch_with_browser(url)
    root = ET.fromstring(xml)
    dr_records = root.findall(".//m:record", ns)
    assert len(dr_records) <= 1, (
        f"expecting at most 1 draft resolution record, found {len(dr_records)}"
    )
    if not dr_records:
        return None
    dr_record = dr_records[0]
    _get_field = partial(get_field, dr_record)
    pdf_urls = _get_field("856", "u")
    pdf_urls = [url for url in pdf_urls if url.endswith("-EN.pdf")]
    return {
        "id": dr_record.find('.//m:controlfield[@tag="001"]', ns).text.strip(),
        "summary": _get_field("500", "a"),
        "keywords": _get_field("650", "a"),
        "authors": _get_field("710", "a"),
        "pdf_url": pdf_urls[0] if pdf_urls else None,
    }


def post_bsky_report(records):
    client = Client("https://bsky.social")
    client.login("un-reports.bsky.social", os.environ["BSKY_PASSWORD"])

    response = client.get_author_feed(
        "un-reports.bsky.social", include_pins=False, filter="posts_no_replies"
    )
    feed = response.feed
    while response.cursor is not None:
        response = client.get_author_feed(
            "un-reports.bsky.social",
            include_pins=False,
            filter="posts_no_replies",
            cursor=response.cursor,
        )
        feed += response.feed

    entries = [entry.post.record.facets for entry in feed]
    facets = [facet for facets in entries for facet in facets]
    links = [
        feat.uri
        for facet in facets
        for feat in facet.features
        if isinstance(feat, Link)
    ]

    # for entry in feed:
    #     client.delete_post(entry.post.uri)

    unposted = [
        record for record in records if not any(record["id"] in link for link in links)
    ]
    if not unposted:
        return

    record = unposted[-1]
    print(f"posting report {record['title']} from {record['date']} on bsky...")

    images = []
    for i, image in enumerate(get_images(record["pdf_url"])):
        ref = client.upload_blob(image)
        image = Image(alt=f"Screenshot of page {i} of the report", image=ref.blob)
        images.append(image)

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

    for para in get_summary(record):
        chunks = chunk_text(para, MAX_LENGTH - 10)
        for chunk in chunks:
            postn = client.send_post(chunk, reply_to=ReplyRef(parent=prev, root=root))
            prev = StrongRef(cid=postn.cid, uri=postn.uri)

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
        client.send_post(text, reply_to=ReplyRef(parent=prev, root=root))
    print(f"posted {record['title']} from {record['date']} on bsky!")


def post_bsky_resolution(records):
    client = Client("https://bsky.social")
    client.login("un-resolutions.bsky.social", os.environ["BSKY_PASSWORD"])

    response = client.get_author_feed(
        "un-resolutions.bsky.social", include_pins=False, filter="posts_no_replies"
    )
    feed = response.feed
    while response.cursor is not None:
        response = client.get_author_feed(
            "un-resolutions.bsky.social",
            include_pins=False,
            filter="posts_no_replies",
            cursor=response.cursor,
        )
        feed += response.feed

    entries = [entry.post.record.facets for entry in feed]
    facets = [facet for facets in entries for facet in facets]
    links = [
        feat.uri
        for facet in facets
        for feat in facet.features
        if isinstance(feat, Link)
    ]

    # for entry in feed:
    #     client.delete_post(entry.post.uri)

    for record in records[::-1]:
        dr_record = get_draft_resolution(record)
        if dr_record and dr_record["pdf_url"] and not any(dr_record["id"] in link for link in links):
            break
    else:
        return
    print(f"posting resolution {record['title']} from {record['date']} on bsky...")

    MAX_LENGTH = 300
    BASE_LENGTH = 100
    title = (
        record["title"][: MAX_LENGTH - BASE_LENGTH - 3] + "..."
        if len(record["title"]) + BASE_LENGTH > MAX_LENGTH
        else record["title"]
    )
    date_ = date.fromisoformat(record["date"]).strftime("%A, %b %-d")
    text = (
        client_utils.TextBuilder()
        .text(f"New resolution adopted! From {date_}:\n\n")
        .text(f"❞ {title}")
    )

    if dr_record["authors"]:
        countries_ = [countries.get(name=a) for a in dr_record["authors"]]
        countries_ = [c.alpha_3 for c in countries_ if c]
        if countries_:
            flag_text = f"\n\nAuthored by {get_flags(countries_)}"
            if BASE_LENGTH + len(title) + len(flag_text) <= MAX_LENGTH:
                text.text(flag_text)

    text.text("\n\n→ ").link(
        "Read the draft resolution here",
        f"https://digitallibrary.un.org/record/{dr_record['id']}?ln=en&v=pdf",
    )

    images = []
    for i, image in enumerate(get_images(dr_record["pdf_url"])):
        ref = client.upload_blob(image)
        image = Image(alt=f"Screenshot of page {i} of the report", image=ref.blob)
        images.append(image)

    post = client.send_post(text, embed=Main(images=images) if images else None)
    root = StrongRef(cid=post.cid, uri=post.uri)
    prev = root

    text = (
        client_utils.TextBuilder()
        .text(get_votes(record))
        .text("→ ")
        .link(
            "Find voting data and transcript here",
            f"https://digitallibrary.un.org/record/{record['id']}?ln=en&v=pdf",
        )
    )
    post = client.send_post(text, reply_to=ReplyRef(parent=prev, root=root))
    prev = StrongRef(cid=post.cid, uri=post.uri)

    for summary_text in dr_record["summary"]:
        chunks = chunk_text(summary_text, MAX_LENGTH - 10)
        for chunk in chunks:
            post2 = client.send_post(chunk, reply_to=ReplyRef(parent=prev, root=root))
            prev = StrongRef(cid=post2.cid, uri=post2.uri)

    if len(dr_record["keywords"]) > 0:
        text = client_utils.TextBuilder()
        for kw in dr_record["keywords"]:
            if len(text.build_text() + kw.replace(" ", "")) + 2 < MAX_LENGTH:
                text.tag(
                    "#"
                    + kw.replace("'", "").title().replace(" ", "").replace("-", "")
                    + " ",
                    kw.replace("'", "").lower().replace(" ", "").replace("-", ""),
                )
        client.send_post(text, reply_to=ReplyRef(parent=prev, root=root))
    print(f"posted {record['title']} from {record['date']} on bsky!")


def post_x_report(records):
    client = tweepy.Client(
        bearer_token=os.environ["X_BEARER_TOKEN"],
        consumer_key=os.environ["X_API_KEY"],
        consumer_secret=os.environ["X_API_KEY_SECRET"],
        access_token=os.environ["X_ACCESS_TOKEN"],
        access_token_secret=os.environ["X_ACCESS_TOKEN_SECRET"],
    )
    posted = json.load(open("posted.json"))["x"]
    unposted = [record for record in records if record["id"] not in posted]
    if not unposted:
        return

    record = unposted[-1]
    print(f"posting report {record['title']} from {record['date']} on x...")

    auth = tweepy.OAuth1UserHandler(
        consumer_key=os.environ["X_API_KEY"],
        consumer_secret=os.environ["X_API_KEY_SECRET"],
        access_token=os.environ["X_ACCESS_TOKEN"],
        access_token_secret=os.environ["X_ACCESS_TOKEN_SECRET"],
    )
    api = tweepy.API(auth)

    date_ = date.fromisoformat(record["date"]).strftime("%A, %b %-d")
    text = (
        f"New report released! From {date_}:\n\n"
        f"❞ {record['title']}\n\n"
        f"→ https://digitallibrary.un.org/record/{record['id']}?ln=en&v=pdf ({record['pages']})\n\n"
    )
    for summary_text in record["summary"]:
        text += f"{summary_text}\n\n"
    for para in get_summary(record):
        text += f"{para}\n\n"
    if len(record["keywords"]) > 0:
        text += "\n\n"
        for kw in record["keywords"]:
            tag = kw.replace("'", "").title().replace(" ", "").replace("-", "")
            text += f"#{tag} "
    media_ids = []
    for i, image in enumerate(get_images(record["pdf_url"])):
        media = api.simple_upload(filename=f"page_{i}.jpeg", file=image)
        media_ids.append(media.media_id)
    tweet_params = {"text": text}
    if media_ids:
        tweet_params["media_ids"] = media_ids
    client.create_tweet(**tweet_params)
    posted = [record["id"]] + posted
    json.dump({"x": posted}, open("posted.json", "w"), indent=2)
    print(f"posted report {record['title']} from {record['date']} on x!")


def post_x_resolution(records):
    client = tweepy.Client(
        bearer_token=os.environ["X_BEARER_TOKEN_2"],
        consumer_key=os.environ["X_API_KEY_2"],
        consumer_secret=os.environ["X_API_KEY_SECRET_2"],
        access_token=os.environ["X_ACCESS_TOKEN_2"],
        access_token_secret=os.environ["X_ACCESS_TOKEN_SECRET_2"],
    )
    posted = json.load(open("posted.json"))["x"]
    unposted = [record for record in records if record["id"] not in posted]
    if not unposted:
        return

    for record in unposted[::-1]:
        dr_record = get_draft_resolution(record)
        if dr_record and dr_record["pdf_url"]:
            break
    else:
        return
    print(f"posting resolution {record['title']} from {record['date']} on x...")

    auth = tweepy.OAuth1UserHandler(
        consumer_key=os.environ["X_API_KEY_2"],
        consumer_secret=os.environ["X_API_KEY_SECRET_2"],
        access_token=os.environ["X_ACCESS_TOKEN_2"],
        access_token_secret=os.environ["X_ACCESS_TOKEN_SECRET_2"],
    )
    api = tweepy.API(auth)

    date_ = date.fromisoformat(record["date"]).strftime("%A, %b %-d")
    text = f"New resolution adopted! From {date_}:\n\n❞ {record['title']}\n\n"
    if dr_record["authors"]:
        countries_ = [countries.get(name=a) for a in dr_record["authors"]]
        countries_ = [c.alpha_3 for c in countries_ if c]
        if countries_:
            text += f"Authored by {get_flags(countries_)}\n\n"
    text += f"→ https://digitallibrary.un.org/record/{record['id']}?ln=en&v=pdf (draft resolution)\n\n"

    text += get_votes(record)
    text += f"→ https://digitallibrary.un.org/record/{record['id']}?ln=en&v=pdf (voting data and transcript)"

    for summary_text in dr_record["summary"]:
        text += f"{summary_text}\n\n"
    if len(dr_record["keywords"]) > 0:
        text += "\n\n"
        for kw in dr_record["keywords"]:
            tag = kw.replace("'", "").title().replace(" ", "").replace("-", "")
            text += f"#{tag} "
    media_ids = []
    for i, image in enumerate(get_images(dr_record["pdf_url"])):
        media = api.simple_upload(filename=f"page_{i}.jpeg", file=image)
        media_ids.append(media.media_id)
    tweet_params = {"text": text}
    if media_ids:
        tweet_params["media_ids"] = media_ids
    client.create_tweet(**tweet_params)
    posted = [record["id"]] + posted
    json.dump({"x": posted}, open("posted.json", "w"), indent=2)
    print(f"posted resolution {record['title']} from {record['date']} on x!")


if __name__ == "__main__":
    print("retrieving reports ...")
    params = {
        "cc": "Reports",
        "ln": "en",
        "sf": "year",
        "rg": 20,
        "c": "Reports",
        "of": "xm",
    }
    url = f"https://digitallibrary.un.org/search?{urlencode(params)}"
    xml = fetch_with_browser(url)
    reports = marc_xml_to_reports(xml)
    print("retrieving resolutions ...")
    params = {
        "cc": "Voting Data",
        "ln": "en",
        "sf": "year",
        "rg": 20,
        "c": "Voting Data",
        "of": "xm",
    }
    url = f"https://digitallibrary.un.org/search?{urlencode(params)}"
    xml = fetch_with_browser(url)
    resolutions = marc_xml_to_resolutions(xml)
    exceptions = []
    try:
        print("posting on bsky ...")
        post_bsky_report(reports)
        post_bsky_resolution(resolutions)
    except Exception as e:
        exceptions.append(e)
    try:
        print("posting on x ...")
        post_x_report(reports)
        post_x_resolution(resolutions)
    except (TooManyRequests, Forbidden) as e:
        print(e)
    except Exception as e:
        exceptions.append(e)
    for e in exceptions:
        raise e
