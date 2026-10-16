#!/usr/bin/env python3
"""
GePNIC Organization Tenders Scraper
-----------------------------------
This script automates logging in and solving captchas for the GePNIC eprocure portal,
crawling all search pages for a specified organization (default: National Testing Agency - 684),
extracting tender details and offline HTMLs incrementally, and downloading all associated
documents (PDF summaries, comparative charts, BOQ evaluations).
"""

import argparse
import base64
import json
import os
import re
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup

# Import custom CNN captcha solver
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "captcha"))
try:
    import captcha_solver
except ImportError:
    captcha_solver = None

# Global configuration
USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/120.0.0.0 Safari/537.36'
)

# Global locks and thread-local session store
file_lock = threading.Lock()
captcha_lock = threading.Lock()
thread_local = threading.local()


def safe_request(session, method, url, max_retries=5, backoff=2.0, **kwargs):
    """
    Wraps requests calls with automatic retry and exponential backoff.
    """
    kwargs.setdefault('timeout', 25)
    for attempt in range(max_retries):
        try:
            if method.upper() == 'GET':
                res = session.get(url, **kwargs)
            else:
                res = session.post(url, **kwargs)
            if res.status_code == 200:
                return res
            print(f"[-] Received status {res.status_code} for {url}. Retrying {attempt + 1}/{max_retries}...")
        except Exception as e:
            print(f"[-] Request error ({type(e).__name__}): {e}. Attempt {attempt + 1}/{max_retries} retrying in {backoff:.1f}s...")
        time.sleep(backoff)
        backoff *= 1.5
    raise RuntimeError(f"Failed to request {url} after {max_retries} attempts.")


def clean_tapestry_redirects(html_content):
    """
    Strips or disables Tapestry javascript redirect blocks that prevent opening
    the offline HTML files directly in a local browser (which redirects to UnauthorisedAccessPage).
    """
    cleaned = re.sub(
        r'<script[^>]*>.*?UnauthorisedAccessPage.*?<\/script>',
        '<!-- Redirect script disabled by scraper to allow offline viewing -->',
        html_content,
        flags=re.DOTALL | re.IGNORECASE
    )
    cleaned = cleaned.replace("window.location.replace", "// window.location.replace")
    cleaned = cleaned.replace("window.location.href =", "// window.location.href =")
    return cleaned


def save_json_atomic(filepath, data):
    """
    Atomically writes JSON data to a temporary file then replaces target file.
    """
    tmp_path = filepath + ".tmp"
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    os.replace(tmp_path, filepath)


def solve_captcha(session, captcha_b64, output_dir=None):
    """
    Solves the captcha using the CNN solver automatically, with a thread-safe manual fallback.
    """
    try:
        header, encoded = captcha_b64.split(",", 1) if "," in captcha_b64 else ("", captcha_b64)
        img_data = base64.b64decode(encoded)

        # 1. Automatic CNN solver
        if captcha_solver:
            try:
                captcha_val, conf = captcha_solver.solve_captcha(img_data)
                print(f"[+] Solved captcha automatically via CNN: {captcha_val} (confidence: {conf:.2f})")
                return captcha_val
            except Exception as e:
                print(f"[-] CNN Solver failed: {e}. Falling back to manual entry...")

        # 2. Thread-safe manual fallback
        with captcha_lock:
            save_dir = output_dir or os.path.dirname(os.path.abspath(__file__))
            os.makedirs(save_dir, exist_ok=True)
            captcha_path = os.path.join(save_dir, f"captcha_prompt_{threading.get_ident()}.png")
            with open(captcha_path, "wb") as f:
                f.write(img_data)

            print("\n" + "=" * 60)
            print(f"[*] Captcha image saved to: {captcha_path}")
            print("Please view this image in your file viewer and enter the text.")
            print("=" * 60)

            try:
                if sys.platform.startswith('linux'):
                    import subprocess
                    subprocess.Popen(['xdg-open', captcha_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                pass

            print("\nEnter Captcha (6 chars): ", end="", flush=True)
            captcha_val = sys.stdin.readline().strip()
            try:
                if os.path.exists(captcha_path):
                    os.remove(captcha_path)
            except Exception:
                pass
            return captcha_val
    except Exception as e:
        print(f"[-] Error solving/prompting captcha: {e}")
        return None


def fetch_search_page(session, org_id='684', max_retries=10, output_dir=None, from_date="", to_date=""):
    """
    Initializes a new session, fetches the search form, and handles captcha authentication in a retry loop.
    Supports optional publication date filtering (DD/MM/YYYY).
    Returns the HTML content of the search results page.
    """
    search_url = 'https://eprocure.gov.in/eprocure/app?page=WebTenderStatusLists&service=page'

    for attempt in range(max_retries):
        print(f"[*] Accessing eprocure search page (attempt {attempt + 1}/{max_retries})...")
        response = safe_request(session, 'GET', search_url)

        soup = BeautifulSoup(response.text, 'html.parser')
        form = soup.find('form')
        if not form:
            print("[-] Could not locate search form in HTML. Retrying...")
            time.sleep(1.0)
            continue

        captcha_img = soup.find('img', id='captchaImage')
        if not captcha_img or not captcha_img.get('src'):
            print("[-] Captcha image element not found. Retrying...")
            time.sleep(1.0)
            continue

        captcha_val = solve_captcha(session, captcha_img.get('src'), output_dir=output_dir)
        if not captcha_val or len(captcha_val) != 6:
            print(f"[-] Captcha must be exactly 6 characters (got: {captcha_val!r}). Retrying...")
            time.sleep(1.0)
            continue

        # Construct form payload
        payload = []

        # 1. Routing parameters
        for name in ['formids', 'seedids', 'component', 'page', 'service', 'session', 'submitmode', 'submitname']:
            val = form.find('input', {'name': name})
            if val:
                payload.append((name, val.get('value', '')))

        # 2. Iterate through formids fields to construct full search request
        formids_tag = form.find('input', {'name': 'formids'})
        formids_str = formids_tag.get('value', '') if formids_tag else ''
        field_names = formids_str.split(',')

        for field in field_names:
            if not field:
                continue
            if field == 'OrganName':
                payload.append(('OrganName', str(org_id)))
            elif field == 'publishedFromDate' and from_date:
                payload.append(('publishedFromDate', str(from_date)))
            elif field == 'publishedToDate' and to_date:
                payload.append(('publishedToDate', str(to_date)))
            elif field == 'tenderStatus' and (from_date or to_date):
                payload.append(('tenderStatus', '0'))
            elif field == 'captchaText':
                payload.append(('captchaText', captcha_val))
            elif field == 'Search':
                payload.append(('Search', 'Search'))
            elif field in ['tenderId', 'tenderRefNo', 'KeyWord', 'KeyWord2', 'fromDate', 'toDate', 'publishedFromDate', 'publishedToDate']:
                payload.append((field, ''))
            else:
                input_tag = form.find('input', {'name': field})
                if input_tag:
                    payload.append((field, input_tag.get('value', '')))
                    continue

                select_tag = form.find('select', {'name': field})
                if select_tag:
                    selected_option = select_tag.find('option', selected=True) or select_tag.find('option')
                    val = selected_option.get('value', '0') if selected_option else '0'
                    payload.append((field, val))
                    continue

                payload.append((field, ''))

        action_path = form.get('action', '/eprocure/app')
        action_url = urllib.parse.urljoin("https://eprocure.gov.in", action_path)

        session.headers.update({
            'Origin': 'https://eprocure.gov.in',
            'Referer': search_url,
            'Content-Type': 'application/x-www-form-urlencoded'
        })

        date_info = f" (dates: {from_date} to {to_date})" if (from_date or to_date) else ""
        print(f"[*] Submitting search filter for Org ID: {org_id}{date_info}...")
        post_response = safe_request(session, 'POST', action_url, data=payload)

        # Verify results
        res_soup = BeautifulSoup(post_response.text, 'html.parser')
        table = res_soup.find('table', id='tabList')
        if table:
            print("[+] Captcha verified & search results retrieved successfully!")
            return post_response.text

        # Check for legitimate empty result
        page_text = res_soup.get_text()
        if any(msg in page_text for msg in ["No Records Found", "No records found", "No Tenders found", "No tenders found"]):
            print("[*] Portal reported: No Tenders Found.")
            return post_response.text

        # Captcha error or validation message
        err_msg = ""
        err_span = res_soup.find('span', class_='error')
        if err_span:
            err_msg = err_span.text.strip()
        else:
            err_font = res_soup.find('font', color='red')
            if err_font:
                err_msg = err_font.text.strip()
        print(f"[-] Search submission failed: {err_msg or 'Invalid Captcha / Submission Error'}. Retrying...")
        time.sleep(1.0)

    raise RuntimeError(f"Failed to authenticate and retrieve search page after {max_retries} attempts.")


def get_thread_session(org_id='684', output_dir=None, from_date="", to_date=""):
    """
    Returns the requests.Session assigned to the current thread.
    Initializes and authenticates it if it doesn't exist yet.
    """
    if not hasattr(thread_local, "session") or thread_local.session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=3)
        session.mount('https://', adapter)
        session.mount('http://', adapter)

        session.cookies.set("cookieWorked", "yes", domain="eprocure.gov.in", path="/")
        session.headers.update({
            'User-Agent': USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })

        print(f"[*] Thread {threading.current_thread().name} initializing session and solving captcha...")
        fetch_search_page(session, org_id=org_id, output_dir=output_dir, from_date=from_date, to_date=to_date)
        thread_local.session = session
    return thread_local.session


def renew_thread_session(org_id='684', output_dir=None, from_date="", to_date=""):
    """
    Forces re-authentication of the thread's session when expired.
    """
    thread_local.session = None
    return get_thread_session(org_id=org_id, output_dir=output_dir, from_date=from_date, to_date=to_date)


def parse_results_page(html):
    """
    Parses the tenders listed on a search results page.
    Returns:
      - tenders: list of dictionaries with tender metadata
      - next_url: URL string for the next page, or None if last page
    """
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', id='tabList')
    tenders = []

    if not table:
        return tenders, None

    rows = table.find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        if len(cells) < 6:
            continue

        sno = cells[0].text.strip()
        if not (sno.endswith('.') or sno.isdigit()):
            continue  # Skip header or footer rows

        tender_id = cells[1].text.strip()
        title_ref = cells[2].text.strip()
        org_chain = cells[3].text.strip()
        stage = cells[4].text.strip()

        # Parse Title and Ref No from the [Title][Ref No] format
        parts = re.findall(r'\[(.*?)\]', title_ref)
        title = parts[0] if len(parts) > 0 else title_ref
        ref_no = parts[1] if len(parts) > 1 else ""

        # Extract view link
        view_link = None
        a_tag = cells[5].find('a', href=True)
        if a_tag and 'sp=' in a_tag['href']:
            view_link = urllib.parse.urljoin("https://eprocure.gov.in", a_tag['href'].replace('&amp;', '&'))

        tenders.append({
            'sno': sno,
            'tender_id': tender_id,
            'title': title,
            'reference_no': ref_no,
            'organisation_chain': org_chain,
            'stage': stage,
            'view_link': view_link
        })

    # Check for Next page link
    next_link = soup.find('a', id='loadNext')
    next_url = None
    if next_link and next_link.get('href'):
        next_url = urllib.parse.urljoin("https://eprocure.gov.in", next_link['href'].replace('&amp;', '&'))
    elif next_link:
        next_url = "https://eprocure.gov.in/eprocure/app?component=loadNext&page=WebTenderStatusLists&service=direct&session=T"

    return tenders, next_url


def fetch_tender_details(view_link, tender_id, org_id='684', output_dir=None, from_date="", to_date=""):
    """
    Visits Page 0 (Tender Status), then visits:
      - Page 1 (Tender Details) via the "View More Details" link.
      - Page 2 (Tender Summary Reports) via the "all stage summary Details" link.
    Combines metadata from both pages and returns (metadata, html_page0, html_page1, html_page2).
    """
    if not view_link:
        return {}, "", "", ""

    session = get_thread_session(org_id=org_id, output_dir=output_dir, from_date=from_date, to_date=to_date)

    # 1. Access Page 0 (Tender Status overview page)
    res0 = safe_request(session, 'GET', view_link)
    if b'session in the client area has expired' in res0.content.lower():
        session = renew_thread_session(org_id=org_id, output_dir=output_dir, from_date=from_date, to_date=to_date)
        res0 = safe_request(session, 'GET', view_link)

    html_page0 = res0.text
    soup0 = BeautifulSoup(html_page0, 'html.parser')

    # 2. Find links to Page 1 and Page 2 on Page 0
    link_page1 = None
    link_page2 = None
    for a in soup0.find_all('a', href=True):
        href_val = a['href']
        if 'page=WebTenderStatus' in href_val:
            if 'component=%24DirectLink_0' in href_val or 'component=$DirectLink_0' in href_val:
                link_page2 = href_val
            elif 'component=%24DirectLink' in href_val or 'component=$DirectLink' in href_val:
                link_page1 = href_val

    # Fallback to standard DirectLink endpoints if not found
    if not link_page1:
        link_page1 = "https://eprocure.gov.in/eprocure/app?component=%24DirectLink&page=WebTenderStatus&service=direct&session=T"
    link_page1 = urllib.parse.urljoin("https://eprocure.gov.in", link_page1.replace('&amp;', '&'))

    if not link_page2:
        link_page2 = "https://eprocure.gov.in/eprocure/app?component=%24DirectLink_0&page=WebTenderStatus&service=direct&session=T"
    link_page2 = urllib.parse.urljoin("https://eprocure.gov.in", link_page2.replace('&amp;', '&'))

    # 3. Access Page 1 (Tender Details)
    res1 = safe_request(session, 'GET', link_page1)
    if b'session in the client area has expired' in res1.content.lower():
        session = renew_thread_session(org_id=org_id, output_dir=output_dir, from_date=from_date, to_date=to_date)
        safe_request(session, 'GET', view_link)
        res1 = safe_request(session, 'GET', link_page1)
    html_page1 = res1.text
    soup1 = BeautifulSoup(html_page1, 'html.parser')

    # 4. Access Page 2 (Tender Summary Reports)
    res2 = safe_request(session, 'GET', link_page2)
    if b'session in the client area has expired' in res2.content.lower():
        session = renew_thread_session(org_id=org_id, output_dir=output_dir, from_date=from_date, to_date=to_date)
        safe_request(session, 'GET', view_link)
        res2 = safe_request(session, 'GET', link_page2)
    html_page2 = res2.text
    soup2 = BeautifulSoup(html_page2, 'html.parser')

    # Extract metadata from Page 1 (Basic Details)
    metadata = {}
    table_list1 = soup1.find('table', class_='table_list')
    if table_list1:
        rows = table_list1.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 2:
                key = cells[0].text.strip().replace(":", "").strip()
                val = cells[1].text.strip()
                metadata[key] = val

    # Extract metadata from Page 2 (Tender Summary Reports)
    table_list2 = soup2.find('table', class_='table_list')
    if table_list2:
        rows = table_list2.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 2:
                key = cells[0].text.strip().replace(":", "").strip()
                val = cells[1].text.strip()
                if key not in metadata:
                    metadata[key] = val

    # Parse bids list from Page 2
    bids = []
    bids_table = soup2.find('table', id='bidValidTableView')
    if bids_table:
        rows = bids_table.find_all('tr')
        for row in rows[2:]:
            cells = row.find_all('td')
            if len(cells) >= 7:
                bids.append({
                    'sno': cells[0].text.strip(),
                    'bid_number': cells[1].text.strip(),
                    'bidder_name': cells[2].text.strip(),
                    'submitted_date': cells[3].text.strip(),
                    'status': cells[4].text.strip(),
                    'remarks': cells[5].text.strip(),
                    'status_updated_on': cells[6].text.strip(),
                })
    metadata['bids'] = bids

    # Extract PDF/HTML document URLs from Page 2
    pdf_info = []
    for a_tag in soup2.find_all('a', href=True):
        href = a_tag['href']
        text = a_tag.text.strip()

        is_pdf_ext = '.pdf' in text.lower() or '.pdf' in href.lower()
        is_print_page = 'FrontEndViewBidSummaryDetailsPrint' in href or 'FrontEndViewBidSummaryDetailsPrint' in text
        is_boq = 'boq' in text.lower() or 'boq' in href.lower()
        is_summary_report = 'all stage summary' in text.lower() or 'summary details' in text.lower() or 'evaluation' in text.lower()

        # Capture report summaries and comparative charts, excluding individual numeric bidder links
        if (is_pdf_ext or is_print_page or is_boq or is_summary_report) and not text.isdigit():
            clean_href = urllib.parse.urljoin("https://eprocure.gov.in", href.replace('&amp;', '&'))

            if is_boq:
                filename = "BOQ_Comparative_Chart.pdf"
                pdf_size = "Unknown"
                is_pdf = True
            elif is_summary_report and not is_pdf_ext:
                filename = f"{text.replace(' ', '_')}.pdf"
                pdf_size = "Unknown"
                is_pdf = True
            else:
                size_match = re.search(r'\(([\d\.]+\s*[KMG]B)\)', text, re.IGNORECASE)
                pdf_size = size_match.group(1) if size_match else "Unknown"

                clean_filename = re.sub(r'\s*\(.*\)', '', text).strip()
                filename = clean_filename if clean_filename.endswith('.pdf') else f"doc_{len(pdf_info)}.pdf"
                if not filename.endswith('.pdf'):
                    filename += ".pdf"
                is_pdf = True

            filename = f"{tender_id}_{filename}"

            pdf_info.append({
                'filename': filename,
                'url': clean_href,
                'size': pdf_size,
                'downloaded': False,
                'is_pdf': is_pdf
            })

    metadata['pdf_documents'] = pdf_info
    return metadata, html_page0, html_page1, html_page2


def download_tender_pdfs(tender_id, view_link, pdf_list, output_dir, org_id='684', from_date="", to_date=""):
    """
    Downloads PDFs/HTMLs for a specific tender by visiting Page 0 and then Page 2.
    Automatically handles session renewals, idempotent downloads, and content detection.
    """
    if not pdf_list or not view_link:
        return 0

    session = get_thread_session(org_id=org_id, output_dir=output_dir, from_date=from_date, to_date=to_date)
    pdf_dir = os.path.join(output_dir, tender_id)
    os.makedirs(pdf_dir, exist_ok=True)

    # 1. Access Page 0 (with retry on session expiration)
    soup0 = None
    for _ in range(2):
        try:
            res0 = safe_request(session, 'GET', view_link)
            if b'session in the client area has expired' in res0.content.lower():
                print(f"[*] Session expired for thread {threading.current_thread().name}. Renewing...")
                session = renew_thread_session(org_id=org_id, output_dir=output_dir, from_date=from_date, to_date=to_date)
                continue
            soup0 = BeautifulSoup(res0.text, 'html.parser')
            break
        except Exception as e:
            print(f"[-] Failed to access view_link (Page 0) for {tender_id}: {e}")
            return 0

    if not soup0:
        print(f"[-] Could not establish valid session for {tender_id}.")
        return 0

    # 2. Find Page 2 link (all stage summary details) on Page 0
    link_page2 = None
    for a in soup0.find_all('a', href=True):
        href_val = a['href']
        if 'page=WebTenderStatus' in href_val and ('component=%24DirectLink_0' in href_val or 'component=$DirectLink_0' in href_val):
            link_page2 = href_val
            break

    if not link_page2:
        link_page2 = "https://eprocure.gov.in/eprocure/app?component=%24DirectLink_0&page=WebTenderStatus&service=direct&session=T"
    link_page2 = urllib.parse.urljoin("https://eprocure.gov.in", link_page2.replace('&amp;', '&'))

    # 3. Access Page 2 (activates summary reports and direct link session scope)
    try:
        res2 = safe_request(session, 'GET', link_page2)
        if b'session in the client area has expired' in res2.content.lower():
            session = renew_thread_session(org_id=org_id, output_dir=output_dir, from_date=from_date, to_date=to_date)
            safe_request(session, 'GET', view_link)
            res2 = safe_request(session, 'GET', link_page2)
    except Exception as e:
        print(f"[-] Failed to access Page 2 for {tender_id}: {e}")
        return 0

    download_count = 0
    for pdf in pdf_list:
        filename = pdf['filename']
        filepath = os.path.join(pdf_dir, filename)

        # Check if already downloaded on disk with non-zero size, or permanently marked with error
        if (os.path.exists(filepath) and os.path.getsize(filepath) > 0) or pdf.get('download_error'):
            pdf['downloaded'] = True
            continue

        url = pdf['url']
        print(f"    - Downloading {filename}...")

        for attempt in range(2):
            try:
                pdf_res = safe_request(session, 'GET', url)

                # Check if session expired
                if b'session in the client area has expired' in pdf_res.content.lower():
                    if attempt == 0:
                        print(f"    [*] Session expired during download of {filename}. Renewing session and retrying once...")
                        session = renew_thread_session(org_id=org_id, output_dir=output_dir, from_date=from_date, to_date=to_date)
                        safe_request(session, 'GET', view_link)
                        safe_request(session, 'GET', link_page2)
                        continue
                    else:
                        print(f"    [-] Document {filename} unavailable (session expired / restricted on portal).")
                        pdf['download_error'] = 'Document unavailable or restricted on portal'
                        break

                # Determine content type dynamically
                is_response_pdf = b'%PDF' in pdf_res.content[:1024]
                is_response_html = b'<html' in pdf_res.content[:1024].lower() or b'<!doctype' in pdf_res.content[:1024].lower()

                if is_response_pdf:
                    if not filename.endswith('.pdf'):
                        base, _ = os.path.splitext(filename)
                        filename = base + '.pdf'
                        filepath = os.path.join(pdf_dir, filename)
                    with open(filepath, 'wb') as f:
                        f.write(pdf_res.content)
                    pdf['filename'] = filename
                    pdf['is_pdf'] = True
                    pdf['downloaded'] = True
                    download_count += 1
                    print(f"    [+] Downloaded PDF successfully: {filename}")
                    break
                elif is_response_html:
                    if not filename.endswith('.html'):
                        base, _ = os.path.splitext(filename)
                        filename = base + '.html'
                        filepath = os.path.join(pdf_dir, filename)
                    cleaned_html = clean_tapestry_redirects(pdf_res.text)
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(cleaned_html)
                    pdf['filename'] = filename
                    pdf['is_pdf'] = False
                    pdf['downloaded'] = True
                    download_count += 1
                    print(f"    [+] Downloaded HTML report successfully: {filename}")
                    break
                else:
                    with open(filepath, 'wb') as f:
                        f.write(pdf_res.content)
                    pdf['downloaded'] = True
                    download_count += 1
                    print(f"    [+] Downloaded file successfully: {filename}")
                    break
            except Exception as e:
                print(f"    [-] Failed to download {filename}: {e}")
                break

        time.sleep(0.3)

    return download_count


def process_single_tender_details(tender, details_file, details_map, output_dir, org_id='684', from_date="", to_date=""):
    """
    Worker task: Fetches Page 0, Page 1, Page 2 details for one tender and writes offline HTMLs and details.json.
    """
    tid = tender['tender_id']
    tender_dir = os.path.join(output_dir, tid)
    det_json_path = os.path.join(tender_dir, "details.json")
    if os.path.exists(det_json_path) and os.path.getsize(det_json_path) > 10:
        try:
            with open(det_json_path, 'r', encoding='utf-8') as f:
                existing_det = json.load(f)
            with file_lock:
                details_map[tid] = existing_det
            return
        except Exception:
            pass

    try:
        details, html_page0, html_page1, html_page2 = fetch_tender_details(
            tender.get('view_link'), tid, org_id=org_id, output_dir=output_dir, from_date=from_date, to_date=to_date
        )
        if details:
            details['sno'] = tender.get('sno', '')
            details['stage'] = tender.get('stage', '')
            details['view_link'] = tender.get('view_link', '')
            details['tender_id'] = tid

            tender_dir = os.path.join(output_dir, tid)
            os.makedirs(tender_dir, exist_ok=True)

            if html_page0:
                with open(os.path.join(tender_dir, "status.html"), "w", encoding="utf-8") as f:
                    f.write(clean_tapestry_redirects(html_page0))

            if html_page1:
                with open(os.path.join(tender_dir, "details.html"), "w", encoding="utf-8") as f:
                    f.write(clean_tapestry_redirects(html_page1))

            if html_page2:
                with open(os.path.join(tender_dir, "summary.html"), "w", encoding="utf-8") as f:
                    f.write(clean_tapestry_redirects(html_page2))

            save_json_atomic(os.path.join(tender_dir, "details.json"), details)

            with file_lock:
                details_map[tid] = details
                save_json_atomic(details_file, details_map)
            print(f"[+] Successfully fetched details for Tender ID: {tid}")
        else:
            print(f"[-] Warning: No details parsed for {tid}")
    except Exception as e:
        print(f"[-] Exception fetching details for {tid}: {e}")


def process_single_tender_downloads(tender, details_file, details_map, output_dir, org_id='684', from_date="", to_date=""):
    """
    Worker task: Downloads pending documents for one tender.
    """
    tid = tender['tender_id']
    pdfs = tender.get('pdf_documents', [])
    view_link = tender.get('view_link', '')

    downloaded = download_tender_pdfs(tid, view_link, pdfs, output_dir, org_id=org_id, from_date=from_date, to_date=to_date)
    with file_lock:
        details_map[tid]['pdf_documents'] = pdfs
        save_json_atomic(details_file, details_map)
    return downloaded


def parse_args():
    parser = argparse.ArgumentParser(description="GePNIC Organization Tenders Scraper")
    parser.add_argument(
        "--org-id",
        default="684",
        help="Organization ID from GePNIC search dropdown (default: 684 for National Testing Agency)"
    )
    parser.add_argument(
        "--org-name",
        default="NTA",
        help="Organization short code / directory name (default: NTA)"
    )
    parser.add_argument(
        "--from-date",
        default="",
        help="Date filter from (DD/MM/YYYY), e.g. 01/04/2025"
    )
    parser.add_argument(
        "--to-date",
        default="",
        help="Date filter to (DD/MM/YYYY), e.g. 31/03/2026"
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to save scraped tenders and files (default: ./<org-name>)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of concurrent worker threads for detail fetching and downloading (default: 4)"
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=10,
        help="Max retries for requests and captcha verification (default: 10)"
    )
    parser.add_argument(
        "--skip-list",
        action="store_true",
        help="Skip Step 1 (search & list parsing) if list file already exists"
    )
    parser.add_argument(
        "--skip-details",
        action="store_true",
        help="Skip Step 2 (fetching tender details & HTML)"
    )
    parser.add_argument(
        "--expected-tenders",
        type=int,
        default=0,
        help="Expected total tenders count. If existing list has fewer, re-fetches list."
    )
    parser.add_argument(
        "--skip-downloads",
        action="store_true",
        help="Skip Step 3 (downloading PDFs & reports)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    org_id = str(args.org_id)
    org_name = args.org_name
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = args.output_dir or os.path.join(script_dir, org_name)
    os.makedirs(output_dir, exist_ok=True)

    # Locate list and details cache files (check output_dir first, then script_dir for backward compatibility)
    list_filename = f"{org_name.lower()}_tenders_list.json"
    list_file = os.path.join(output_dir, list_filename)
    if not os.path.exists(list_file) and os.path.exists(os.path.join(script_dir, list_filename)):
        list_file = os.path.join(script_dir, list_filename)

    details_filename = f"{org_name.lower()}_tenders_details.json"
    details_file = os.path.join(output_dir, details_filename)
    if not os.path.exists(details_file) and os.path.exists(os.path.join(script_dir, details_filename)):
        details_file = os.path.join(script_dir, details_filename)

    print("=" * 60)
    print(f"[*] Starting GePNIC Scraper for Org: {org_name} (ID: {org_id})")
    if args.from_date or args.to_date:
        print(f"[*] Date Range Filter: {args.from_date} to {args.to_date}")
    print(f"[*] Output Directory: {output_dir}")
    print(f"[*] List File: {list_file}")
    print(f"[*] Details File: {details_file}")
    print("=" * 60)

    # ----------------------------------------------------
    # Step 1: Collect all tender metadata (list)
    # ----------------------------------------------------
    all_tenders = []
    if os.path.exists(list_file):
        print(f"[*] Found existing list file: {list_file}. Loading...")
        with open(list_file, 'r', encoding='utf-8') as f:
            all_tenders = json.load(f)
        if args.expected_tenders and len(all_tenders) < args.expected_tenders:
            print(f"[*] Existing list has {len(all_tenders)} tenders, but expected {args.expected_tenders}. Re-fetching search list...")
            all_tenders = []
        else:
            print(f"[+] Loaded {len(all_tenders)} tenders from list.")

    if not all_tenders and not args.skip_list:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=3)
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        session.cookies.set("cookieWorked", "yes", domain="eprocure.gov.in", path="/")
        session.headers.update({
            'User-Agent': USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })

        try:
            html = fetch_search_page(
                session, org_id=org_id, max_retries=args.max_retries,
                output_dir=output_dir, from_date=args.from_date, to_date=args.to_date
            )
        except Exception as e:
            print(f"[-] Search page authentication failed: {e}")
            sys.exit(1)

        page_num = 1
        while True:
            print(f"[*] Parsing search results page {page_num}...")
            tenders, next_url = parse_results_page(html)
            all_tenders.extend(tenders)
            print(f"[+] Found {len(tenders)} tenders. Total collected so far: {len(all_tenders)}")

            # Save list incrementally
            save_list_path = os.path.join(output_dir, list_filename)
            save_json_atomic(save_list_path, all_tenders)
            list_file = save_list_path

            if not next_url:
                print("[+] Reached the last search results page.")
                break

            print(f"[*] Navigating to next results page: {next_url}...")
            next_response = safe_request(session, 'GET', next_url)
            html = next_response.text
            page_num += 1
            time.sleep(0.4)

    # ----------------------------------------------------
    # Step 2: Fetch detailed status & extract PDF URLs (Parallelized)
    # ----------------------------------------------------
    details_map = {}
    if os.path.exists(details_file):
        print(f"[*] Found existing details file: {details_file}. Loading progress...")
        with open(details_file, 'r', encoding='utf-8') as f:
            details_map = json.load(f)
        print(f"[+] Loaded details for {len(details_map)}/{len(all_tenders)} tenders.")
        for tid, details in details_map.items():
            if 'tender_id' not in details:
                details['tender_id'] = tid

    # Also scan output directory to discover any tenders already saved on disk
    if os.path.exists(output_dir):
        discovered = 0
        for item in os.listdir(output_dir):
            if item not in details_map:
                item_det = os.path.join(output_dir, item, "details.json")
                if os.path.exists(item_det) and os.path.getsize(item_det) > 10:
                    try:
                        with open(item_det, 'r', encoding='utf-8') as df:
                            d = json.load(df)
                            if 'tender_id' not in d:
                                d['tender_id'] = item
                            details_map[item] = d
                            discovered += 1
                    except Exception:
                        pass
        if discovered > 0:
            print(f"[+] Discovered {discovered} additional completed tenders on disk. Total ready: {len(details_map)}")

    save_details_path = os.path.join(output_dir, details_filename)
    details_file = save_details_path

    if not args.skip_details:
        print("\n" + "=" * 40)
        print(f"[*] Starting Step 2: Detail fetching (Total tenders: {len(all_tenders)})...")
        print("=" * 40)

        tenders_to_fetch = [t for t in all_tenders if t['tender_id'] not in details_map]
        if tenders_to_fetch:
            print(f"[*] Fetching details concurrently using {args.workers} worker threads ({len(tenders_to_fetch)} remaining)...")
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [
                    executor.submit(
                        process_single_tender_details,
                        tender,
                        details_file,
                        details_map,
                        output_dir,
                        org_id,
                        args.from_date,
                        args.to_date
                    )
                    for tender in tenders_to_fetch
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"[-] Worker thread error in Step 2: {e}")

        # Final save for Step 2
        with file_lock:
            save_json_atomic(details_file, details_map)
        print(f"[+] Step 2 finished: details stored for {len(details_map)} tenders.")

    # ----------------------------------------------------
    # Step 3: Download tender PDFs (Parallelized)
    # ----------------------------------------------------
    if not args.skip_downloads:
        print("\n" + "=" * 40)
        print("[*] Starting Step 3: Downloading tender documents...")
        print("=" * 40)

        tenders_with_pdfs = [
            t for t in details_map.values()
            if any(not p.get('downloaded') and not p.get('download_error') for p in t.get('pdf_documents', []))
        ]

        if tenders_with_pdfs:
            print(f"[*] Found {len(tenders_with_pdfs)} tenders with pending PDF downloads.")
            total_downloads = 0
            print(f"[*] Downloading documents concurrently using {args.workers} worker threads...")
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [
                    executor.submit(
                        process_single_tender_downloads,
                        tender,
                        details_file,
                        details_map,
                        output_dir,
                        org_id,
                        args.from_date,
                        args.to_date
                    )
                    for tender in tenders_with_pdfs
                ]
                for future in as_completed(futures):
                    try:
                        total_downloads += future.result()
                    except Exception as e:
                        print(f"[-] Worker thread error during download: {e}")

            # Final save for Step 3
            with file_lock:
                save_json_atomic(details_file, details_map)

            print(f"\n[+] SUCCESS! Completed Step 3. Downloaded {total_downloads} new documents.")
        else:
            print("[*] No pending PDF downloads.")

    print(f"[+] Output details saved to: {details_file}")


if __name__ == '__main__':
    main()
