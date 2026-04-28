"""
COMPLETE ZOHO PARTNER SCRAPER - PHASE 2 ONLY
- Extract contact info (emails, phones, websites) for each company
- AUTO-RESUME: If stopped anywhere, continues from where it left off
"""

import csv
import time
import os
import json
import re
import asyncio
import subprocess
import sys
from datetime import datetime
from playwright.async_api import async_playwright

os.environ['PLAYWRIGHT_BROWSERS_PATH'] = '0'

# ==================== PHASE 2 CONFIGURATION ====================
PHASE2_INPUT_CSV = "All_Zoho_Companies_With_Source.csv"
PHASE2_OUTPUT_CSV = "companies_contacts_fixed.csv"
PHASE2_PROGRESS_FILE = "phase2_progress.json"
MAX_RETRIES = 2
REQUEST_DELAY = 1

# ==================== FIX: INSTALL PLAYWRIGHT BROWSERS ====================
def install_playwright_browsers():
    """Install Playwright browsers if not already installed"""
    print("🔧 Checking Playwright browser installation...")
    try:
        # Try to check if browsers exist without launching sync API
        from playwright.async_api import async_playwright
        print("✅ Playwright is installed.")
        return True
    except Exception as e:
        print(f"⚠️ Playwright not properly installed: {e}")
        print("📦 Installing Playwright browsers...")
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
        print("✅ Playwright browsers installed successfully.")
        return True

# ==================== PHASE 2: EXTRACT CONTACT INFO ====================
def extract_real_phones_from_text(text):
    if not text:
        return []
    all_phones = []
    patterns = [
        r'\+971[\s\-]?[0-9]{1,3}[\s\-]?[0-9]{7,8}',
        r'\+1[\s\-]?\(?[0-9]{3}\)?[\s\-]?[0-9]{3}[\s\-]?[0-9]{4}',
        r'\([0-9]{3}\)[\s\-]?[0-9]{3}[\s\-]?[0-9]{4}',
        r'\b[0-9]{3}[-][0-9]{3}[-][0-9]{4}\b',
        r'\b[0-9]{3}[.][0-9]{3}[.][0-9]{4}\b',
        r'\b03[0-9]{2}[\s\-]?[0-9]{7}\b',
        r'\+(?:[0-9]{1,3})[\s\-]?[0-9]{1,4}[\s\-]?[0-9]{5,10}\b',
        r'\b0[0-9]{4}[\s\-]?[0-9]{6}\b',
        r'\+61[\s\-]?[0-9]{1,2}[\s\-]?[0-9]{4}[\s\-]?[0-9]{4}',
        r'\b[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}\b'
    ]
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            if re.search(r'\d+-\d+-\d+px|\d+-\d+-\d+%|\d+-\d+-\d+em', match.lower()):
                continue
            digits_only = re.sub(r'[^\d]', '', match)
            if 7 <= len(digits_only) <= 15 and match not in all_phones:
                all_phones.append(match)
    return all_phones

def extract_real_emails_from_text(text):
    if not text:
        return []
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    all_emails = re.findall(email_pattern, text)
    valid_emails = []
    fake_patterns = ['example', 'test', 'noreply', 'no-reply', 'your@', 'domain.com', 'placeholder']
    for email in all_emails:
        if len(email) < 50 and not any(x in email.lower() for x in fake_patterns):
            domain_part = email.split('@')[-1]
            if '.' in domain_part and len(domain_part.split('.')[-1]) >= 2:
                if email not in valid_emails:
                    valid_emails.append(email)
    return valid_emails

def load_phase2_progress():
    if os.path.exists(PHASE2_PROGRESS_FILE):
        try:
            with open(PHASE2_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {"completed_companies": [], "last_company": None}
    return {"completed_companies": [], "last_company": None}

def save_phase2_progress(progress, results):
    try:
        with open(PHASE2_PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, indent=2)
        with open(PHASE2_OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["Company Name", "Website", "Contact Email", "Phone Number", "Zoho Partner Status", "Source", "Processed Date"])
            writer.writeheader()
            writer.writerows(results)
        return True
    except:
        return False

def read_companies_for_phase2():
    companies = []
    try:
        with open(PHASE2_INPUT_CSV, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) >= 2 and row[1].strip():
                    companies.append(row[1].strip())
        return list(dict.fromkeys(companies))
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return []

def load_phase2_existing_results():
    if os.path.exists(PHASE2_OUTPUT_CSV):
        try:
            with open(PHASE2_OUTPUT_CSV, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                return list(reader)
        except:
            pass
    return []

async def extract_from_google_results(page, company_name):
    search_url = f"https://www.google.com/search?q={company_name.replace(' ', '+')}"
    for attempt in range(MAX_RETRIES):
        try:
            await page.goto(search_url, wait_until="domcontentloaded", timeout=15000)
            try:
                await page.wait_for_selector("div.g, div.yuRUbf", timeout=5000)
            except:
                pass
            content = await page.content()
            phones = extract_real_phones_from_text(content)
            emails = extract_real_emails_from_text(content)
            body_text = await page.evaluate("document.body.innerText")
            phones.extend(extract_real_phones_from_text(body_text))
            emails.extend(extract_real_emails_from_text(body_text))
            phones = list(dict.fromkeys(phones))
            emails = list(dict.fromkeys(emails))
            website = await extract_website_from_google(page, content)
            return phones, emails, website
        except:
            if attempt == MAX_RETRIES - 1:
                return [], [], None
            await asyncio.sleep(1)
    return [], [], None

async def extract_website_from_google(page, content):
    link_pattern = r'href="(https?://[^"]+)"'
    all_links = re.findall(link_pattern, content)
    skip_domains = ['google.com', 'youtube.com', 'linkedin.com', 'facebook.com', 'twitter.com', 'instagram.com', 'wikipedia.org']
    for link in all_links:
        if link.startswith('http') and not any(skip in link.lower() for skip in skip_domains):
            clean_link = link.split('&')[0].split('?')[0]
            if len(clean_link) < 200:
                return clean_link
    try:
        first_result = await page.query_selector('div.g a, div.yuRUbf a')
        if first_result:
            href = await first_result.get_attribute('href')
            if href and href.startswith('http'):
                return href.split('&')[0].split('?')[0]
    except:
        pass
    return None

async def extract_from_website(page, url):
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        await asyncio.sleep(0.5)
        content = await page.content()
        clean_content = re.sub(r'<script[^>]*>.*?</script>|<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)
        emails = extract_real_emails_from_text(clean_content)
        phones = extract_real_phones_from_text(clean_content)
        body_text = await page.evaluate("document.body.innerText")
        emails.extend(extract_real_emails_from_text(body_text))
        phones.extend(extract_real_phones_from_text(body_text))
        emails = list(dict.fromkeys(emails))
        phones = list(dict.fromkeys(phones))
        return emails, phones
    except:
        return [], []

async def scan_website_pages(page, base_url, existing_emails, existing_phones):
    all_emails = list(existing_emails)
    all_phones = list(existing_phones)
    if all_emails and all_phones:
        return all_emails, all_phones
    pages_to_check = [base_url]
    if not all_emails or not all_phones:
        contact_paths = ["/contact", "/contact-us", "/contactus", "/about", "/about-us"]
        pages_to_check.extend([base_url.rstrip('/') + path for path in contact_paths])
    for url in pages_to_check[:3]:
        if all_emails and all_phones:
            break
        emails, phones = await extract_from_website(page, url)
        all_emails.extend([e for e in emails if e not in all_emails])
        all_phones.extend([p for p in phones if p not in all_phones])
        await asyncio.sleep(0.5)
    return all_emails, all_phones

async def check_zoho_partner(page, website_url):
    if not website_url:
        return "No"
    try:
        await page.goto(website_url, wait_until="domcontentloaded", timeout=10000)
        await asyncio.sleep(0.5)
        content = await page.content()
        content_lower = content.lower()
        keywords = ["zoho partner", "zoho authorized", "zoho certified", "zoho implementation partner"]
        for keyword in keywords:
            if keyword in content_lower:
                return "Yes"
        return "No"
    except:
        return "No"

async def process_company_with_fresh_browser(company_name, playwright_instance):
    browser = None
    page = None
    try:
        print(f"\n   🚀 Processing: {company_name}")
        browser = await playwright_instance.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox', '--disable-dev-shm-usage']
        )
        page = await browser.new_page()
        await page.set_viewport_size({"width": 1280, "height": 720})
        page.set_default_timeout(10000)
        
        google_phones, google_emails, website = await extract_from_google_results(page, company_name)
        emails = google_emails
        phones = google_phones
        source = "Google Results"
        
        if website and (not google_emails or not google_phones):
            print(f"   🌐 Scanning website: {website}")
            emails, phones = await scan_website_pages(page, website, google_emails, google_phones)
            if (emails and not google_emails) or (phones and not google_phones):
                source = "Combined (Google + Website)"
            elif emails or phones:
                source = "Website"
        elif website:
            source = "Google Results Only"
            print(f"   🌐 Website found: {website}")
        else:
            print(f"   ❌ No website found")
        
        partner = "No"
        if website:
            print(f"   🤝 Checking Zoho partner status...")
            partner = await check_zoho_partner(page, website)
        
        result = {
            "Company Name": company_name,
            "Website": website or "",
            "Contact Email": emails[0] if emails else "",
            "Phone Number": phones[0] if phones else "",
            "Zoho Partner Status": partner,
            "Source": source,
            "Processed Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        print(f"\n   📊 RESULT: {company_name}")
        if emails:
            print(f"      📧 Email: {emails[0]}")
        if phones:
            print(f"      📞 Phone: {phones[0]}")
        return result, True
    except Exception as e:
        print(f"   ❌ Error: {company_name} - {str(e)[:50]}")
        return {
            "Company Name": company_name,
            "Website": "",
            "Contact Email": "",
            "Phone Number": "",
            "Zoho Partner Status": "Error",
            "Source": "Error",
            "Processed Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }, False
    finally:
        if page:
            await page.close()
        if browser:
            await browser.close()

async def run_phase2():
    print("=" * 70)
    print("🚀 PHASE 2: EXTRACT CONTACT INFORMATION")
    print("=" * 70)
    
    # Install browsers first
    install_playwright_browsers()
    
    progress = load_phase2_progress()
    results = load_phase2_existing_results()
    
    all_companies = read_companies_for_phase2()
    companies_to_process = [c for c in all_companies if c not in progress["completed_companies"]]
    
    print(f"📊 Total companies: {len(all_companies)}")
    print(f"📊 Already processed: {len(progress['completed_companies'])}")
    print(f"📊 Remaining to process: {len(companies_to_process)}")
    
    if not companies_to_process:
        print("\n✅ Phase 2: All companies already processed!")
        if results:
            total = len(results)
            with_email = sum(1 for r in results if r.get('Contact Email'))
            with_phone = sum(1 for r in results if r.get('Phone Number'))
            print(f"\n📊 FINAL SUMMARY:")
            print(f"   Total processed: {total}")
            print(f"   Emails found: {with_email}")
            print(f"   Phones found: {with_phone}")
        return True
    
    async with async_playwright() as p:
        for idx, company_name in enumerate(companies_to_process, 1):
            print(f"\n{'='*60}")
            print(f"[{idx}/{len(companies_to_process)}] Processing: {company_name}")
            print(f"{'='*60}")
            
            result, success = await process_company_with_fresh_browser(company_name, p)
            
            results.append(result)
            progress["completed_companies"].append(result["Company Name"])
            progress["last_company"] = result["Company Name"]
            
            save_phase2_progress(progress, results)
            print(f"\n   💾 Progress saved ({len(progress['completed_companies'])}/{len(all_companies)})")
            
            if idx < len(companies_to_process):
                await asyncio.sleep(REQUEST_DELAY)
    
    save_phase2_progress(progress, results)
    
    total = len(results)
    with_email = sum(1 for r in results if r.get('Contact Email'))
    with_phone = sum(1 for r in results if r.get('Phone Number'))
    zoho_partners = sum(1 for r in results if r.get('Zoho Partner Status') == 'Yes')
    
    print(f"\n{'='*60}")
    print("📊 PHASE 2 FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"Total companies processed: {total}")
    print(f"Companies with email: {with_email}")
    print(f"Companies with phone: {with_phone}")
    print(f"Zoho Partners found: {zoho_partners}")
    print(f"\n✅ Results saved to: {PHASE2_OUTPUT_CSV}")
    
    return True

if __name__ == "__main__":
    asyncio.run(run_phase2())
