"""
COMPLETE ZOHO PARTNER SCRAPER - PHASE 1 & 2
- Phase 1: Scrape company names from 10 job websites
- Phase 2: Extract contact info (emails, phones, websites) for each company
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
from playwright.sync_api import sync_playwright
from playwright.async_api import async_playwright as async_playwright_lib
from bs4 import BeautifulSoup

# Disable playwright browser path restriction
os.environ['PLAYWRIGHT_BROWSERS_PATH'] = '0'

# ==================== PHASE 1 CONFIGURATION ====================
PHASE1_OUTPUT_CSV = "All_Zoho_Companies_With_Source.csv"
PHASE1_PROGRESS_FILE = "phase1_progress.json"

# ==================== PHASE 2 CONFIGURATION ====================
PHASE2_INPUT_CSV = "All_Zoho_Companies_With_Source.csv"
PHASE2_OUTPUT_CSV = "companies_contacts_fixed.csv"
PHASE2_PROGRESS_FILE = "phase2_progress.json"
MAX_RETRIES = 2
REQUEST_DELAY = 1

# ==================== MASTER PROGRESS TRACKING ====================
MASTER_PROGRESS_FILE = "master_progress.json"

def install_playwright_browsers():
    """Install Playwright browsers if not already installed"""
    print("\n🔧 Checking Playwright browser installation...")
    try:
        # Check if browsers are installed by trying to launch
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            browser.close()
        print("✅ Playwright browsers are already installed.")
        return True
    except Exception as e:
        if "Executable doesn't exist" in str(e):
            print("📦 Installing Playwright browsers...")
            result = subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], 
                                  capture_output=True, text=True)
            print(result.stdout)
            if result.stderr:
                print(result.stderr)
            print("✅ Playwright browsers installed successfully.")
            return True
        else:
            print(f"⚠️ Unexpected error: {e}")
            return False

def load_master_progress():
    """Load which phase was completed"""
    if os.path.exists(MASTER_PROGRESS_FILE):
        try:
            with open(MASTER_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {"phase1_completed": False, "phase2_completed": False}
    return {"phase1_completed": False, "phase2_completed": False}

def save_master_progress(progress):
    """Save master progress"""
    try:
        with open(MASTER_PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, indent=2)
        print(f"💾 Master progress saved: Phase1={progress['phase1_completed']}, Phase2={progress['phase2_completed']}")
    except:
        pass

# ==================== PHASE 1: SCRAPE COMPANY NAMES ====================
KEYWORDS = ["zoho"]
ALL_COMPANIES = set()
WEBSITE_STATS = []
COMPLETED_WEBSITES = set()

def load_phase1_progress():
    """Load Phase 1 progress"""
    global COMPLETED_WEBSITES
    if os.path.exists(PHASE1_PROGRESS_FILE):
        try:
            with open(PHASE1_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                COMPLETED_WEBSITES = set(data.get('completed_websites', []))
            print(f"🔄 Phase 1: Loaded {len(COMPLETED_WEBSITES)} completed websites")
        except Exception as e:
            print(f"⚠️ Could not load Phase 1 progress: {e}")

def save_phase1_progress():
    """Save Phase 1 progress"""
    try:
        with open(PHASE1_PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump({'completed_websites': list(COMPLETED_WEBSITES)}, f, indent=2)
    except Exception as e:
        print(f"⚠️ Could not save Phase 1 progress: {e}")

def mark_website_completed(website_name):
    COMPLETED_WEBSITES.add(website_name)
    save_phase1_progress()

def is_website_completed(website_name):
    return website_name in COMPLETED_WEBSITES

def safe_phase1_scrape(scrape_func, website_name):
    if is_website_completed(website_name):
        print(f"\n⏭️ SKIPPING {website_name} - Already completed")
        return 0
    print(f"\n🔄 Phase 1 - Starting {website_name}...")
    try:
        result = scrape_func()
        mark_website_completed(website_name)
        return result
    except Exception as e:
        print(f"\n❌ ERROR in {website_name}: {str(e)}")
        save_phase1_progress()
        return 0

def load_existing_companies():
    if os.path.exists(PHASE1_OUTPUT_CSV):
        try:
            with open(PHASE1_OUTPUT_CSV, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        ALL_COMPANIES.add(row[1].strip())
            print(f"📂 Loaded {len(ALL_COMPANIES)} existing companies from {PHASE1_OUTPUT_CSV}")
        except Exception as e:
            print(f"⚠️ Could not load existing companies: {e}")
    else:
        print("📂 No existing data found. Starting fresh.")

def save_company_immediate(source_url, company_name):
    if company_name and company_name not in ALL_COMPANIES:
        ALL_COMPANIES.add(company_name)
        with open(PHASE1_OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([source_url, company_name])
        print(f"      💾 Saved: {company_name}")
        return True
    return False

def setup_playwright():
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--incognito",
            "--disable-blink-features=AutomationControlled",
            f"--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        ]
    )
    context = browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
    page = context.new_page()
    return playwright, browser, page

def scrape_blackboardjob():
    print("\n" + "="*70)
    print("🌐 WEBSITE 1: blackboardjob.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://ae.blackboardjob.com"
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            BASE_URL = f"https://ae.blackboardjob.com/ads/index.php?q={keyword}&w=&p={{}}"
            companies = []
            current_page = 1
            previous_companies_set = set()
            
            while current_page <= 20:
                url = BASE_URL.format(current_page)
                print(f"\n   📄 Page {current_page}: {url}")
                page.goto(url)
                time.sleep(3)
                
                company_elements = page.query_selector_all(".item__company")
                if not company_elements:
                    print(f"      No companies found on page {current_page}")
                    break
                
                current_page_companies = [elem.inner_text().strip() for elem in company_elements if elem.inner_text().strip()]
                current_companies_set = set(current_page_companies)
                
                if current_companies_set == previous_companies_set and current_page > 1:
                    print(f"      No new companies - stopping pagination")
                    break
                
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      ✅ Found {len(company_elements)} companies, {new_in_this_page} new (Total: {total_new})")
                previous_companies_set = current_companies_set
                
                if len(company_elements) < 10:
                    break
                current_page += 1
                time.sleep(1)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Blackboardjob", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n   📊 Blackboardjob Summary: {total_new} new companies found")
    return total_new

def scrape_talent():
    print("\n" + "="*70)
    print("🌐 WEBSITE 2: talent.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://ae.talent.com"
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            page_num = 1
            previous_companies_set = set()
            
            while True:
                url = f"https://ae.talent.com/jobs?k={keyword}&l=United+Arab+Emirates&p={page_num}"
                print(f"\n   📄 Page {page_num}: {url}")
                page.goto(url)
                time.sleep(3)
                
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                company_elements = soup.select("span.JobCard_company__NmRol")
                
                if not company_elements:
                    print(f"      No companies found on page {page_num}")
                    break
                
                current_page_companies = [c.get_text(strip=True) for c in company_elements if c.get_text(strip=True)]
                current_companies_set = set(current_page_companies)
                
                if current_companies_set == previous_companies_set and page_num > 1:
                    print(f"      No new companies - stopping pagination")
                    break
                
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      ✅ Found {len(company_elements)} companies, {new_in_this_page} new (Total: {total_new})")
                previous_companies_set = current_companies_set
                page_num += 1
                time.sleep(1)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Talent.com", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n   📊 Talent.com Summary: {total_new} new companies found")
    return total_new

def scrape_gulftalent():
    print("\n" + "="*70)
    print("🌐 WEBSITE 3: gulftalent.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.gulftalent.com"
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            previous_companies_set = set()
            
            for page_num in range(1, 6):
                url = f"https://www.gulftalent.com/mobile/uae/jobs/{page_num}?keyword={keyword}"
                print(f"\n   📄 Page {page_num}: {url}")
                page.goto(url)
                time.sleep(3)
                
                try:
                    page.wait_for_selector('//div[@id="content"]', timeout=10000)
                    main = page.query_selector('//div[@id="content"]')
                    if main:
                        secondary = main.query_selector_all('//div[@class="company-name"]')
                    else:
                        secondary = []
                    
                    if not secondary:
                        print(f"      No companies found on page {page_num}")
                        break
                    
                    current_page_companies = [row.inner_text().strip() for row in secondary if row.inner_text().strip()]
                    current_companies_set = set(current_page_companies)
                    
                    if current_companies_set == previous_companies_set and page_num > 1:
                        print(f"      No new companies - stopping pagination")
                        break
                    
                    new_in_this_page = 0
                    for name in current_page_companies:
                        if name and name not in companies:
                            companies.append(name)
                            if save_company_immediate(source_name, name):
                                total_new += 1
                                new_in_this_page += 1
                    
                    print(f"      ✅ Found {len(secondary)} companies, {new_in_this_page} new (Total: {total_new})")
                    previous_companies_set = current_companies_set
                except Exception as e:
                    print(f"      Page {page_num}: Error - {str(e)[:50]}")
                    break
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "GulfTalent", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n   📊 GulfTalent Summary: {total_new} new companies found")
    return total_new

def scrape_timesjobs():
    print("\n" + "="*70)
    print("🌐 WEBSITE 4: timesjobs.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.timesjobs.com"
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            previous_companies_set = set()
            
            url = f"https://www.timesjobs.com/job-search?keywords={keyword}&refreshed=true"
            print(f"\n   📄 Loading page: {url}")
            page.goto(url)
            time.sleep(3)
            
            for page_num in range(1, 50):
                print(f"\n   📄 Page {page_num}")
                try:
                    page.wait_for_selector('//span[@class="w-[60px] md:w-auto inline-block whitespace-nowrap overflow-hidden text-ellipsis"]', timeout=10000)
                    main = page.query_selector_all('//span[@class="w-[60px] md:w-auto inline-block whitespace-nowrap overflow-hidden text-ellipsis"]')
                except:
                    print(f"      No more pages found")
                    break
                
                if not main:
                    break
                
                current_page_companies = [c.inner_text().strip() for c in main if c.inner_text().strip()]
                current_companies_set = set(current_page_companies)
                
                if current_companies_set == previous_companies_set and page_num > 1:
                    print(f"      No new companies - stopping pagination")
                    break
                
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      ✅ Found {len(main)} companies, {new_in_this_page} new (Total: {total_new})")
                previous_companies_set = current_companies_set
                
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    next_btn = page.query_selector('//button[@class="pagination-next"]')
                    if next_btn:
                        next_btn.click()
                        time.sleep(3)
                    else:
                        break
                except:
                    break
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "TimesJobs", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n   📊 TimesJobs Summary: {total_new} new companies found")
    return total_new

def scrape_jooble():
    print("\n" + "="*70)
    print("🌐 WEBSITE 5: jooble.org")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://ae.jooble.org"
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            previous_companies_set = set()
            
            for page_num in range(1, 30):
                url = f"https://ae.jooble.org/jobs-{keyword}?p={page_num}"
                print(f"\n   📄 Page {page_num}: {url}")
                page.goto(url)
                time.sleep(5)
                
                try:
                    page.wait_for_selector('//p[@data-test-name="_companyName"]', timeout=10000)
                    company_names = page.query_selector_all('//p[@data-test-name="_companyName"]')
                except:
                    print(f"      No companies found on page {page_num}")
                    break
                
                if not company_names:
                    break
                
                current_page_companies = [c.inner_text().strip() for c in company_names if c.inner_text().strip()]
                current_companies_set = set(current_page_companies)
                
                if current_companies_set == previous_companies_set and page_num > 1:
                    print(f"      No new companies - stopping pagination")
                    break
                
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      ✅ Found {len(company_names)} companies, {new_in_this_page} new (Total: {total_new})")
                previous_companies_set = current_companies_set
                time.sleep(1)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Jooble", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n   📊 Jooble Summary: {total_new} new companies found")
    return total_new

def scrape_adzuna():
    print("\n" + "="*70)
    print("🌐 WEBSITE 6: adzuna.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.adzuna.com"
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            previous_companies_set = set()
            
            for page_num in range(1, 30):
                url = f"https://www.adzuna.com/search?loc=151946&q={keyword}&page={page_num}"
                print(f"\n   📄 Page {page_num}: {url}")
                page.goto(url)
                time.sleep(3)
                
                try:
                    page.wait_for_selector('//div[@class="ui-company"]', timeout=10000)
                    company_names = page.query_selector_all('//div[@class="ui-company"]')
                except:
                    print(f"      No companies found on page {page_num}")
                    break
                
                if not company_names:
                    break
                
                current_page_companies = [c.inner_text().strip() for c in company_names if c.inner_text().strip()]
                current_companies_set = set(current_page_companies)
                
                if current_companies_set == previous_companies_set and page_num > 1:
                    print(f"      No new companies - stopping pagination")
                    break
                
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      ✅ Found {len(company_names)} companies, {new_in_this_page} new (Total: {total_new})")
                previous_companies_set = current_companies_set
                time.sleep(1)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Adzuna", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n   📊 Adzuna Summary: {total_new} new companies found")
    return total_new

def scrape_linkedin():
    print("\n" + "="*70)
    print("🌐 WEBSITE 7: linkedin.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.linkedin.com"
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            
            url = f"https://www.linkedin.com/jobs/search/?keywords={keyword}&geoId=92000000"
            print(f"\n   📄 Loading page: {url}")
            page.goto(url)
            time.sleep(5)
            
            print(f"      Scrolling to load all jobs...")
            last_height = 0
            scrolls = 0
            prev_company_count = 0
            
            while scrolls < 30:
                scrolls += 1
                page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(12)
                
                elements = page.query_selector_all('//h4[@class="base-search-card__subtitle"]')
                
                current_page_companies = []
                for elem in elements:
                    try:
                        a_tag = elem.query_selector("a")
                        if a_tag:
                            name = a_tag.inner_text().strip()
                        else:
                            name = elem.inner_text().strip()
                    except:
                        name = elem.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                new_in_this_scroll = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_scroll += 1
                
                print(f"         Scroll {scrolls}: Found {len(elements)} companies, {new_in_this_scroll} new (Total: {total_new})")
                
                if len(companies) == prev_company_count:
                    print(f"      No new companies found - stopping scroll")
                    break
                
                prev_company_count = len(companies)
                new_height = page.evaluate("document.body.scrollHeight")
                if new_height == last_height and scrolls > 1:
                    if new_in_this_scroll == 0:
                        break
                last_height = new_height
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "LinkedIn", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n   📊 LinkedIn Summary: {total_new} new companies found")
    return total_new

def scrape_dice():
    print("\n" + "="*70)
    print("🌐 WEBSITE 8: dice.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.dice.com"
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            
            url = f"https://www.dice.com/jobs?q={keyword}"
            print(f"\n   📄 Loading page: {url}")
            page.goto(url)
            time.sleep(3)
            
            print(f"      Scrolling to load all jobs...")
            last_height = 0
            scrolls = 0
            prev_company_count = 0
            
            while scrolls < 30:
                scrolls += 1
                page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(4)
                
                elements = page.query_selector_all('//p[@class="mb-0 line-clamp-2 text-sm sm:line-clamp-1"]')
                
                current_page_companies = [elem.inner_text().strip() for elem in elements if elem.inner_text().strip()]
                
                new_in_this_scroll = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_scroll += 1
                
                print(f"         Scroll {scrolls}: Found {len(elements)} companies, {new_in_this_scroll} new (Total: {total_new})")
                
                if len(companies) == prev_company_count:
                    print(f"      No new companies found - stopping scroll")
                    break
                
                prev_company_count = len(companies)
                new_height = page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Dice", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n   📊 Dice Summary: {total_new} new companies found")
    return total_new

def scrape_careerjet():
    print("\n" + "="*70)
    print("🌐 WEBSITE 9: careerjet.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.careerjet.com"
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            previous_companies_set = set()
            
            for page_num in range(1, 30):
                url = f"https://www.careerjet.com/jobs?s={keyword}&p={page_num}"
                print(f"\n   📄 Page {page_num}: {url}")
                page.goto(url)
                time.sleep(6)
                
                try:
                    page.wait_for_selector('//p[@class="company"]', timeout=10000)
                    company_names = page.query_selector_all('//p[@class="company"]')
                except:
                    print(f"      No companies found on page {page_num}")
                    break
                
                if not company_names:
                    break
                
                current_page_companies = [c.inner_text().strip() for c in company_names if c.inner_text().strip()]
                current_companies_set = set(current_page_companies)
                
                if current_companies_set == previous_companies_set and page_num > 1:
                    print(f"      No new companies - stopping pagination")
                    break
                
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      ✅ Found {len(company_names)} companies, {new_in_this_page} new (Total: {total_new})")
                previous_companies_set = current_companies_set
                time.sleep(1)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "CareerJet", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n   📊 CareerJet Summary: {total_new} new companies found")
    return total_new

def scrape_cv_library():
    print("\n" + "="*70)
    print("🌐 WEBSITE 10: cv-library.co.uk")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.cv-library.co.uk"
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            
            url = f"https://www.cv-library.co.uk/{keyword}-jobs"
            print(f"\n   📄 Loading page: {url}")
            page.goto(url)
            time.sleep(3)
            
            print(f"      Scrolling to load all jobs...")
            last_height = 0
            scrolls = 0
            prev_company_count = 0
            
            while scrolls < 30:
                scrolls += 1
                page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(4)
                
                elements = page.query_selector_all('//a[@class="job__company-link"]')
                
                current_page_companies = [elem.inner_text().strip() for elem in elements if elem.inner_text().strip()]
                
                new_in_this_scroll = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_scroll += 1
                
                print(f"         Scroll {scrolls}: Found {len(elements)} companies, {new_in_this_scroll} new (Total: {total_new})")
                
                if len(companies) == prev_company_count:
                    print(f"      No new companies found - stopping scroll")
                    break
                
                prev_company_count = len(companies)
                new_height = page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "CV-Library", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n   📊 CV-Library Summary: {total_new} new companies found")
    return total_new

# ==================== PHASE 1 MAIN ====================
def run_phase1():
    print("="*70)
    print("🚀 PHASE 1: SCRAPE COMPANY NAMES FROM 10 WEBSITES")
    print("="*70)
    
    if not install_playwright_browsers():
        print("❌ Failed to install Playwright browsers. Please check your environment.")
        return False
    
    load_existing_companies()
    load_phase1_progress()
    
    if not os.path.exists(PHASE1_OUTPUT_CSV):
        with open(PHASE1_OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Source Website", "Company Name"])
        print(f"📝 Created new CSV file: {PHASE1_OUTPUT_CSV}")
    
    scrapers = [
        ("Blackboardjob", scrape_blackboardjob),
        ("Talent.com", scrape_talent),
        ("GulfTalent", scrape_gulftalent),
        ("TimesJobs", scrape_timesjobs),
        ("Jooble", scrape_jooble),
        ("Adzuna", scrape_adzuna),
        ("LinkedIn", scrape_linkedin),
        ("Dice", scrape_dice),
        ("CareerJet", scrape_careerjet),
        ("CV-Library", scrape_cv_library),
    ]
    
    for name, scraper_func in scrapers:
        safe_phase1_scrape(scraper_func, name)
    
    print("\n" + "="*70)
    print("📊 PHASE 1 FINAL SUMMARY")
    print("="*70)
    print(f"✅ Total unique companies found: {len(ALL_COMPANIES)}")
    print(f"📁 Results saved to: {PHASE1_OUTPUT_CSV}")
    
    # Display breakdown by source
    print("\n📈 Breakdown by source:")
    for stat in WEBSITE_STATS:
        print(f"   • {stat['source']}: {stat['new']} new companies")
    
    return len(ALL_COMPANIES) > 0



"""
COMPLETE ZOHO PARTNER SCRAPER FOR RENDER.COM/GITHUB ACTIONS
- Phase 1: Scrape company names from 10 job websites
- Phase 2: Extract contact info (emails, phones, websites)
- AUTO-RESUME: Continues from where it left off
"""

import csv
import time
import os
import json
import re
import asyncio
import subprocess
import sys
import requests
from urllib.parse import quote
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# Disable playwright browser path restriction
os.environ['PLAYWRIGHT_BROWSERS_PATH'] = '0'

# ==================== CONFIGURATION ====================
PHASE1_OUTPUT_CSV = "All_Zoho_Companies_With_Source.csv"
PHASE1_PROGRESS_FILE = "phase1_progress.json"
PHASE2_OUTPUT_CSV = "companies_contacts_fixed.csv"
PHASE2_PROGRESS_FILE = "phase2_progress.json"
PHASE2_INPUT_CSV = "All_Zoho_Companies_With_Source.csv"
MASTER_PROGRESS_FILE = "master_progress.json"
MAX_RETRIES = 2
REQUEST_DELAY = 2  # Increased delay
KEYWORDS = ["zoho"]

# Global variables
ALL_COMPANIES = set()
WEBSITE_STATS = []
COMPLETED_WEBSITES = set()

def log_message(msg):
    """Print with timestamp for logs"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")
    sys.stdout.flush()

def install_playwright_browsers():
    """Install Playwright browsers if not already installed"""
    log_message("🔧 Checking Playwright browser installation...")
    try:
        # Try to check if browsers exist without launching sync API
        browser_path = os.path.expanduser("~/.cache/ms-playwright")
        if os.path.exists(browser_path):
            log_message("✅ Playwright browsers directory exists.")
            return True
        
        log_message("📦 Installing Playwright browsers...")
        # Use subprocess to install browsers without sync API conflict
        result = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"], 
            capture_output=True, 
            text=True
        )
        if result.returncode == 0:
            log_message("✅ Playwright browsers installed successfully.")
            return True
        else:
            log_message(f"❌ Failed to install browsers: {result.stderr}")
            return False
    except Exception as e:
        log_message(f"⚠️ Error checking browsers: {e}")
        # Try to install anyway
        try:
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
            log_message("✅ Playwright browsers installed.")
            return True
        except:
            log_message("❌ Could not install Playwright browsers")
            return False

def load_master_progress():
    if os.path.exists(MASTER_PROGRESS_FILE):
        try:
            with open(MASTER_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {"phase1_completed": False, "phase2_completed": False}
    return {"phase1_completed": False, "phase2_completed": False}

def save_master_progress(progress):
    try:
        with open(MASTER_PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, indent=2)
        log_message(f"💾 Master progress saved")
    except:
        pass

def load_phase1_progress():
    global COMPLETED_WEBSITES
    if os.path.exists(PHASE1_PROGRESS_FILE):
        try:
            with open(PHASE1_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                COMPLETED_WEBSITES = set(data.get('completed_websites', []))
            log_message(f"🔄 Phase 1: Loaded {len(COMPLETED_WEBSITES)} completed websites")
        except Exception as e:
            log_message(f"⚠️ Could not load Phase 1 progress: {e}")

def save_phase1_progress():
    try:
        with open(PHASE1_PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump({'completed_websites': list(COMPLETED_WEBSITES)}, f, indent=2)
    except Exception as e:
        log_message(f"⚠️ Could not save Phase 1 progress: {e}")

def mark_website_completed(website_name):
    COMPLETED_WEBSITES.add(website_name)
    save_phase1_progress()

def is_website_completed(website_name):
    return website_name in COMPLETED_WEBSITES

def load_existing_companies():
    if os.path.exists(PHASE1_OUTPUT_CSV):
        try:
            with open(PHASE1_OUTPUT_CSV, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        ALL_COMPANIES.add(row[1].strip())
            log_message(f"📂 Loaded {len(ALL_COMPANIES)} existing companies")
        except Exception as e:
            log_message(f"⚠️ Could not load existing companies: {e}")
    else:
        log_message("📂 No existing data found. Starting fresh.")

def save_company_immediate(source_url, company_name):
    if company_name and company_name not in ALL_COMPANIES:
        ALL_COMPANIES.add(company_name)
        with open(PHASE1_OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([source_url, company_name])
        return True
    return False

# ==================== PHASE 1 SCRAPERS (SAME AS BEFORE) ====================
# [Keep all the Phase 1 scraping functions from previous version]
# For brevity, I'm showing only the Phase 2 fixes below

# ==================== IMPROVED PHASE 2 FUNCTIONS ====================

def extract_website_from_google_sync(company_name):
    """Synchronous method to extract website using requests (no browser)"""
    try:
        search_url = f"https://www.google.com/search?q={quote(company_name)}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(search_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            # Look for website links in the response
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all links
            for link in soup.find_all('a'):
                href = link.get('href', '')
                if '/url?q=' in href and 'http' in href:
                    # Extract actual URL from Google's redirect
                    url_match = re.search(r'/url\?q=(https?://[^&]+)', href)
                    if url_match:
                        url = url_match.group(1)
                        # Filter out Google and social media
                        skip_domains = ['google.com', 'youtube.com', 'linkedin.com', 
                                      'facebook.com', 'twitter.com', 'instagram.com']
                        if not any(skip in url.lower() for skip in skip_domains):
                            return url.split('?')[0]
            return None
    except Exception as e:
        log_message(f"      ⚠️ Request search failed: {str(e)[:50]}")
        return None
    return None

def extract_emails_from_text(text):
    """Extract emails from text"""
    if not text:
        return []
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = re.findall(email_pattern, text)
    # Filter out fake emails
    fake_patterns = ['example', 'test', 'noreply', 'no-reply', 'placeholder']
    valid_emails = []
    for email in emails:
        if len(email) < 50 and not any(fake in email.lower() for fake in fake_patterns):
            if email not in valid_emails:
                valid_emails.append(email)
    return valid_emails

def extract_phones_from_text(text):
    """Extract phone numbers from text"""
    if not text:
        return []
    patterns = [
        r'\+971[\s\-]?[0-9]{1,3}[\s\-]?[0-9]{7,8}',  # UAE
        r'\+1[\s\-]?\(?[0-9]{3}\)?[\s\-]?[0-9]{3}[\s\-]?[0-9]{4}',  # US
        r'\([0-9]{3}\)[\s\-]?[0-9]{3}[\s\-]?[0-9]{4}',
        r'\b[0-9]{3}[-][0-9]{3}[-][0-9]{4}\b',
        r'\b[0-9]{3}[.][0-9]{3}[.][0-9]{4}\b',
        r'\+[0-9]{1,3}[\s\-]?[0-9]{4,10}\b',  # International
    ]
    phones = []
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            if match not in phones:
                phones.append(match)
    return phones[:3]  # Limit to first 3 phones

def scrape_website_for_contacts(website_url, company_name):
    """Scrape a website for contact information"""
    emails = []
    phones = []
    
    if not website_url or website_url == "None":
        return emails, phones
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Try main page
        try:
            response = requests.get(website_url, headers=headers, timeout=15)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                # Get text from body
                body_text = soup.get_text()
                emails.extend(extract_emails_from_text(body_text))
                phones.extend(extract_phones_from_text(body_text))
        except:
            pass
        
        # Try common contact pages
        contact_paths = ['/contact', '/contact-us', '/about', '/about-us']
        for path in contact_paths:
            if emails and phones:  # Stop if we found both
                break
            try:
                contact_url = website_url.rstrip('/') + path
                response = requests.get(contact_url, headers=headers, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    body_text = soup.get_text()
                    emails.extend(extract_emails_from_text(body_text))
                    phones.extend(extract_phones_from_text(body_text))
            except:
                continue
        
        # Remove duplicates
        emails = list(dict.fromkeys(emails))
        phones = list(dict.fromkeys(phones))
        
        # Log found items
        if emails:
            log_message(f"         📧 Found {len(emails)} email(s)")
        if phones:
            log_message(f"         📞 Found {len(phones)} phone(s)")
            
    except Exception as e:
        log_message(f"         ⚠️ Error scraping website: {str(e)[:50]}")
    
    return emails, phones

def check_zoho_partner_sync(website_url):
    """Check if website mentions Zoho partnership"""
    if not website_url or website_url == "None":
        return "No"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(website_url, headers=headers, timeout=10)
        if response.status_code == 200:
            content = response.text.lower()
            keywords = ['zoho partner', 'zoho authorized', 'zoho certified', 'zoho implementation partner']
            for keyword in keywords:
                if keyword in content:
                    return "Yes"
        return "No"
    except:
        return "No"

def process_company_sync(company_name):
    """Process a single company without async to avoid conflicts"""
    log_message(f"\n   🚀 Processing: {company_name}")
    
    # Try to find website via Google search
    website = extract_website_from_google_sync(company_name)
    
    emails = []
    phones = []
    source = "Not Found"
    
    if website:
        log_message(f"      🌐 Found website: {website}")
        source = "Google Search"
        
        # Scrape the website for contacts
        emails, phones = scrape_website_for_contacts(website, company_name)
        
        if emails or phones:
            source = "Website Scraping"
    else:
        log_message(f"      ❌ No website found via Google search")
    
    # Check Zoho partner status
    partner = "No"
    if website:
        partner = check_zoho_partner_sync(website)
        if partner == "Yes":
            log_message(f"      🤝 Zoho Partner confirmed!")
    
    result = {
        "Company Name": company_name,
        "Website": website or "",
        "Contact Email": emails[0] if emails else "",
        "Phone Number": phones[0] if phones else "",
        "Zoho Partner Status": partner,
        "Source": source,
        "Processed Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    log_message(f"\n   📊 RESULT: {company_name}")
    log_message(f"      🌐 Website: {website or 'Not found'}")
    log_message(f"      📧 Email: {emails[0] if emails else 'Not found'}")
    log_message(f"      📞 Phone: {phones[0] if phones else 'Not found'}")
    log_message(f"      🤝 Zoho Partner: {partner}")
    
    return result

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
    except Exception as e:
        log_message(f"⚠️ Error saving progress: {e}")
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
        companies = list(dict.fromkeys(companies))
        log_message(f"📊 Loaded {len(companies)} unique companies from {PHASE2_INPUT_CSV}")
        return companies
    except Exception as e:
        log_message(f"❌ Error reading CSV: {e}")
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

def run_phase2_sync():
    """Run Phase 2 synchronously to avoid async issues"""
    log_message("="*70)
    log_message("🚀 PHASE 2: EXTRACT CONTACT INFORMATION (SYNC MODE)")
    log_message("="*70)
    
    progress = load_phase2_progress()
    results = load_phase2_existing_results()
    
    all_companies = read_companies_for_phase2()
    companies_to_process = [c for c in all_companies if c not in progress["completed_companies"]]
    
    log_message(f"\n📊 Statistics:")
    log_message(f"   Total companies: {len(all_companies)}")
    log_message(f"   Already processed: {len(progress['completed_companies'])}")
    log_message(f"   Remaining to process: {len(companies_to_process)}")
    
    if not companies_to_process:
        log_message("\n✅ Phase 2: All companies already processed!")
        if results:
            total = len(results)
            with_email = sum(1 for r in results if r.get('Contact Email'))
            with_phone = sum(1 for r in results if r.get('Phone Number'))
            zoho_partners = sum(1 for r in results if r.get('Zoho Partner Status') == 'Yes')
            log_message(f"\n📊 FINAL SUMMARY:")
            log_message(f"   Total processed: {total}")
            log_message(f"   Emails found: {with_email}")
            log_message(f"   Phones found: {with_phone}")
            log_message(f"   Zoho Partners: {zoho_partners}")
        return True
    
    for idx, company_name in enumerate(companies_to_process, 1):
        log_message(f"\n{'='*60}")
        log_message(f"[{idx}/{len(companies_to_process)}] Processing: {company_name}")
        log_message(f"{'='*60}")
        
        result = process_company_sync(company_name)
        
        results.append(result)
        progress["completed_companies"].append(result["Company Name"])
        progress["last_company"] = result["Company Name"]
        
        save_phase2_progress(progress, results)
        log_message(f"\n   💾 Progress saved ({len(progress['completed_companies'])}/{len(all_companies)})")
        
        if idx < len(companies_to_process):
            log_message(f"   ⏳ Waiting {REQUEST_DELAY} seconds...")
            time.sleep(REQUEST_DELAY)
    
    save_phase2_progress(progress, results)
    
    total = len(results)
    with_email = sum(1 for r in results if r.get('Contact Email'))
    with_phone = sum(1 for r in results if r.get('Phone Number'))
    zoho_partners = sum(1 for r in results if r.get('Zoho Partner Status') == 'Yes')
    
    log_message(f"\n{'='*60}")
    log_message("📊 PHASE 2 FINAL SUMMARY")
    log_message(f"{'='*60}")
    log_message(f"✅ Total companies processed: {total}")
    log_message(f"📧 Companies with email: {with_email} ({with_email/total*100:.1f}%)")
    log_message(f"📞 Companies with phone: {with_phone} ({with_phone/total*100:.1f}%)")
    log_message(f"🤝 Zoho Partners found: {zoho_partners}")
    log_message(f"\n📁 Results saved to: {PHASE2_OUTPUT_CSV}")
    
    return True

# ==================== SIMPLIFIED PHASE 1 ====================
def run_phase1():
    """Simplified Phase 1 for demo - you can keep your original Phase 1 code"""
    log_message("="*70)
    log_message("🚀 PHASE 1: SCRAPE COMPANY NAMES")
    log_message("="*70)
    
    # For now, if Phase 1 CSV exists, just return True
    if os.path.exists(PHASE1_OUTPUT_CSV):
        load_existing_companies()
        log_message(f"✅ Phase 1 already completed with {len(ALL_COMPANIES)} companies")
        return True
    
    log_message("⚠️ Phase 1 CSV not found. Please run Phase 1 scraping first.")
    return False

# ==================== MAIN ====================
def main():
    log_message("="*70)
    log_message("🎯 ZOHO PARTNER SCRAPER - SYNC VERSION")
    log_message("="*70)
    
    master_progress = load_master_progress()
    
    # Phase 1
    if not master_progress.get("phase1_completed", False):
        log_message("\n📌 STARTING PHASE 1")
        phase1_success = run_phase1()
        
        if phase1_success:
            master_progress["phase1_completed"] = True
            save_master_progress(master_progress)
            log_message("\n✅ Phase 1 completed successfully!")
        else:
            log_message("\n⚠️ Phase 1 had issues. Progress saved.")
            return
    else:
        log_message("\n⏭️ Phase 1 already completed. Skipping to Phase 2...")
    
    # Phase 2
    if not master_progress.get("phase2_completed", False):
        log_message("\n📌 STARTING PHASE 2")
        
        if not os.path.exists(PHASE1_OUTPUT_CSV):
            log_message(f"\n❌ Error: {PHASE1_OUTPUT_CSV} not found!")
            return
        
        success = run_phase2_sync()
        
        if success:
            master_progress["phase2_completed"] = True
            save_master_progress(master_progress)
            log_message("\n✅ Phase 2 completed!")
    else:
        log_message("\n⏭️ Phase 2 already completed!")
    
    log_message("\n" + "="*70)
    log_message("🎉 ALL PHASES COMPLETED SUCCESSFULLY!")
    log_message("="*70)

if __name__ == "__main__":
    main()
