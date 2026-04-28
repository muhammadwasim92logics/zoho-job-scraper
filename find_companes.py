"""
COMPLETE ZOHO PARTNER SCRAPER - SYNC VERSION
- Uses sync Playwright only (no async conflicts)
- Phase 1: Scrape company names
- Phase 2: Extract contact info (using requests + BeautifulSoup for speed)
- AUTO-RESUME: Continues from where it left off
"""

import csv
import time
import os
import json
import re
import requests
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# ==================== CONFIGURATION ====================
PHASE1_OUTPUT = "All_Zoho_Companies_With_Source.csv"
PHASE2_OUTPUT = "companies_contacts_fixed.csv"
PHASE1_PROGRESS = "phase1_progress.json"
PHASE2_PROGRESS = "phase2_progress.json"
MASTER_PROGRESS = "master_progress.json"
KEYWORDS = ["zoho"]

# ==================== PHASE 1: SCRAPE COMPANY NAMES ====================
ALL_COMPANIES = set()
COMPLETED_WEBSITES = set()

def load_phase1_progress():
    global COMPLETED_WEBSITES
    if os.path.exists(PHASE1_PROGRESS):
        try:
            with open(PHASE1_PROGRESS, 'r') as f:
                data = json.load(f)
                COMPLETED_WEBSITES = set(data.get('completed_websites', []))
            print(f"📂 Phase 1: Loaded {len(COMPLETED_WEBSITES)} completed websites")
        except:
            pass

def save_phase1_progress():
    with open(PHASE1_PROGRESS, 'w') as f:
        json.dump({'completed_websites': list(COMPLETED_WEBSITES)}, f)

def mark_completed(name):
    COMPLETED_WEBSITES.add(name)
    save_phase1_progress()

def is_completed(name):
    return name in COMPLETED_WEBSITES

def save_company(source, name):
    if name and name not in ALL_COMPANIES:
        ALL_COMPANIES.add(name)
        with open(PHASE1_OUTPUT, 'a', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow([source, name])
        return True
    return False

def setup_browser():
    p = sync_playwright().start()
    browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
    page = browser.new_page()
    return p, browser, page

# Phase 1 Scrapers
def scrape_blackboardjob():
    print("\n🌐 blackboardjob.com")
    p, browser, page = setup_browser()
    total = 0
    try:
        for page_num in range(1, 21):
            url = f"https://ae.blackboardjob.com/ads/index.php?q=zoho&w=&p={page_num}"
            page.goto(url, timeout=30000)
            time.sleep(2)
            elements = page.query_selector_all(".item__company")
            if not elements:
                break
            for elem in elements:
                name = elem.inner_text().strip()
                if save_company("blackboardjob.com", name):
                    total += 1
            print(f"   Page {page_num}: Found {len(elements)} companies")
            if len(elements) < 10:
                break
    finally:
        browser.close()
        p.stop()
    return total

def scrape_talent():
    print("\n🌐 talent.com")
    p, browser, page = setup_browser()
    total = 0
    try:
        for page_num in range(1, 30):
            url = f"https://ae.talent.com/jobs?k=zoho&l=United+Arab+Emirates&p={page_num}"
            page.goto(url, timeout=30000)
            time.sleep(2)
            html = page.content()
            soup = BeautifulSoup(html, 'html.parser')
            elements = soup.select("span.JobCard_company__NmRol")
            if not elements:
                break
            for elem in elements:
                name = elem.get_text(strip=True)
                if save_company("talent.com", name):
                    total += 1
            print(f"   Page {page_num}: Found {len(elements)} companies")
    finally:
        browser.close()
        p.stop()
    return total

def scrape_gulftalent():
    print("\n🌐 gulftalent.com")
    p, browser, page = setup_browser()
    total = 0
    try:
        for page_num in range(1, 6):
            url = f"https://www.gulftalent.com/mobile/uae/jobs/{page_num}?keyword=zoho"
            page.goto(url, timeout=30000)
            time.sleep(2)
            elements = page.query_selector_all('.company-name')
            if not elements:
                break
            for elem in elements:
                name = elem.inner_text().strip()
                if save_company("gulftalent.com", name):
                    total += 1
            print(f"   Page {page_num}: Found {len(elements)} companies")
    finally:
        browser.close()
        p.stop()
    return total

def scrape_jooble():
    print("\n🌐 jooble.org")
    p, browser, page = setup_browser()
    total = 0
    try:
        for page_num in range(1, 30):
            url = f"https://ae.jooble.org/jobs-zoho?p={page_num}"
            page.goto(url, timeout=30000)
            time.sleep(2)
            elements = page.query_selector_all('//p[@data-test-name="_companyName"]')
            if not elements:
                break
            for elem in elements:
                name = elem.inner_text().strip()
                if save_company("jooble.org", name):
                    total += 1
            print(f"   Page {page_num}: Found {len(elements)} companies")
    finally:
        browser.close()
        p.stop()
    return total

def scrape_adzuna():
    print("\n🌐 adzuna.com")
    p, browser, page = setup_browser()
    total = 0
    try:
        for page_num in range(1, 30):
            url = f"https://www.adzuna.com/search?loc=151946&q=zoho&page={page_num}"
            page.goto(url, timeout=30000)
            time.sleep(2)
            elements = page.query_selector_all('//div[@class="ui-company"]')
            if not elements:
                break
            for elem in elements:
                name = elem.inner_text().strip()
                if save_company("adzuna.com", name):
                    total += 1
            print(f"   Page {page_num}: Found {len(elements)} companies")
    finally:
        browser.close()
        p.stop()
    return total

def scrape_linkedin():
    print("\n🌐 linkedin.com")
    p, browser, page = setup_browser()
    total = 0
    companies = set()
    try:
        url = "https://www.linkedin.com/jobs/search/?keywords=zoho&geoId=92000000"
        page.goto(url, timeout=30000)
        time.sleep(5)
        last_height = 0
        for scroll in range(30):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(8)
            elements = page.query_selector_all('//h4[@class="base-search-card__subtitle"]')
            for elem in elements:
                try:
                    a = elem.query_selector("a")
                    name = a.inner_text().strip() if a else elem.inner_text().strip()
                    if name and name not in companies:
                        companies.add(name)
                        if save_company("linkedin.com", name):
                            total += 1
                except:
                    pass
            print(f"   Scroll {scroll+1}: Found {len(elements)} companies, Total: {len(companies)}")
            new_height = page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
    finally:
        browser.close()
        p.stop()
    return total

def run_phase1():
    print("="*70)
    print("🚀 PHASE 1: SCRAPE COMPANY NAMES")
    print("="*70)
    
    load_phase1_progress()
    
    if not os.path.exists(PHASE1_OUTPUT):
        with open(PHASE1_OUTPUT, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(["Source Website", "Company Name"])
    
    scrapers = [
        ("Blackboardjob", scrape_blackboardjob),
        ("Talent", scrape_talent),
        ("GulfTalent", scrape_gulftalent),
        ("Jooble", scrape_jooble),
        ("Adzuna", scrape_adzuna),
        ("LinkedIn", scrape_linkedin),
    ]
    
    for name, func in scrapers:
        if is_completed(name):
            print(f"\n⏭️ Skipping {name} (already completed)")
            continue
        print(f"\n🔄 Starting {name}...")
        try:
            func()
            mark_completed(name)
        except Exception as e:
            print(f"❌ Error in {name}: {e}")
            save_phase1_progress()
            print("💾 Progress saved. Run again to continue.")
            return False
    
    print(f"\n✅ Phase 1 Complete! Found {len(ALL_COMPANIES)} companies")
    return True

# ==================== PHASE 2: EXTRACT CONTACT INFO ====================
def extract_phones(text):
    if not text:
        return []
    phones = []
    patterns = [r'\+971[\s\-]?[0-9]{1,3}[\s\-]?[0-9]{7,8}', r'\+1[\s\-]?\(?[0-9]{3}\)?[\s\-]?[0-9]{3}[\s\-]?[0-9]{4}', r'\([0-9]{3}\)[\s\-]?[0-9]{3}[\s\-]?[0-9]{4}', r'\b[0-9]{3}[-][0-9]{3}[-][0-9]{4}\b', r'\b03[0-9]{2}[\s\-]?[0-9]{7}\b']
    for pattern in patterns:
        for match in re.findall(pattern, text):
            if 7 <= len(re.sub(r'[^\d]', '', match)) <= 15 and match not in phones:
                phones.append(match)
    return phones

def extract_emails(text):
    if not text:
        return []
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
    return [e for e in emails if not any(x in e.lower() for x in ['example', 'test', 'noreply'])]

def get_company_info(company_name):
    """Get website, email, phone using Google search"""
    try:
        url = f"https://html.duckduckgo.com/html/?q={company_name.replace(' ', '+')}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(resp.text, 'html.parser')
        text = soup.get_text()
        
        emails = extract_emails(text)
        phones = extract_phones(text)
        
        # Try to find website
        website = ""
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if 'http' in href and not any(x in href for x in ['duckduckgo', 'google', 'facebook', 'twitter']):
                website = href.split('&')[0]
                break
        
        return website, emails[0] if emails else "", phones[0] if phones else ""
    except:
        return "", "", ""

def run_phase2():
    print("\n" + "="*70)
    print("🚀 PHASE 2: EXTRACT CONTACT INFO")
    print("="*70)
    
    # Load companies from Phase 1
    companies = []
    with open(PHASE1_OUTPUT, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) >= 2 and row[1].strip():
                companies.append(row[1].strip())
    companies = list(dict.fromkeys(companies))
    
    print(f"📊 Total companies to process: {len(companies)}")
    
    # Load progress
    processed = set()
    if os.path.exists(PHASE2_PROGRESS):
        with open(PHASE2_PROGRESS, 'r') as f:
            processed = set(json.load(f))
    
    results = []
    if os.path.exists(PHASE2_OUTPUT):
        with open(PHASE2_OUTPUT, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            results = list(reader)
    
    remaining = [c for c in companies if c not in processed]
    print(f"📊 Already processed: {len(processed)}")
    print(f"📊 Remaining: {len(remaining)}")
    
    for idx, name in enumerate(remaining, 1):
        print(f"\n[{idx}/{len(remaining)}] Processing: {name}")
        website, email, phone = get_company_info(name)
        
        result = {
            "Company Name": name,
            "Website": website,
            "Contact Email": email,
            "Phone Number": phone,
            "Zoho Partner Status": "Checking",
            "Source": "DuckDuckGo",
            "Processed Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        results.append(result)
        processed.add(name)
        
        # Save progress
        with open(PHASE2_PROGRESS, 'w') as f:
            json.dump(list(processed), f)
        
        with open(PHASE2_OUTPUT, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["Company Name", "Website", "Contact Email", "Phone Number", "Zoho Partner Status", "Source", "Processed Date"])
            writer.writeheader()
            writer.writerows(results)
        
        print(f"   📧 Email: {email if email else 'Not found'}")
        print(f"   📞 Phone: {phone if phone else 'Not found'}")
        
        time.sleep(1)
    
    print(f"\n✅ Phase 2 Complete! Results saved to {PHASE2_OUTPUT}")
    return True

# ==================== MAIN ====================
def main():
    print("="*70)
    print("🎯 COMPLETE ZOHO PARTNER SCRAPER")
    print("="*70)
    
    master = {"phase1": False, "phase2": False}
    if os.path.exists(MASTER_PROGRESS):
        with open(MASTER_PROGRESS, 'r') as f:
            master = json.load(f)
    
    if not master.get("phase1", False):
        if run_phase1():
            master["phase1"] = True
            with open(MASTER_PROGRESS, 'w') as f:
                json.dump(master, f)
    else:
        print("\n⏭️ Phase 1 already completed")
    
    if not master.get("phase2", False):
        if run_phase2():
            master["phase2"] = True
            with open(MASTER_PROGRESS, 'w') as f:
                json.dump(master, f)
    else:
        print("\n⏭️ Phase 2 already completed")
    
    print("\n" + "="*70)
    print("🎉 ALL DONE!")
    print("="*70)

if __name__ == "__main__":
    main()
