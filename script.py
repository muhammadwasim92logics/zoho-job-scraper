"""
COMPLETE ZOHO PARTNER SCRAPER - PHASE 1 & 2
- Phase 1: Scrape company names from 10 job websites
- Phase 2: Extract contact info (emails, phones, websites) for each company
- AUTO-RESUME: If stopped anywhere, continues from where it left off
- No XPaths or structure changed
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

# Install browsers if not present
def install_playwright_browsers():
    """Install Playwright browsers if not already installed"""
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
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
            print("✅ Playwright browsers installed successfully.")
            return True
        else:
            print(f"⚠️ Unexpected error: {e}")
            return False

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
        return True
    return False

def setup_playwright():
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",  # Required for some environments
            "--disable-setuid-sandbox",
            "--incognito",
            "--disable-blink-features=AutomationControlled",
            f"--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        ]
    )
    context = browser.new_context(
        viewport={'width': 1920, 'height': 1080'},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
    page = context.new_page()
    return playwright, browser, page

# Phase 1 Scrapers
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
                    break
                
                current_page_companies = [elem.inner_text().strip() for elem in company_elements if elem.inner_text().strip()]
                current_companies_set = set(current_page_companies)
                
                if current_companies_set == previous_companies_set and current_page > 1:
                    break
                
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Found {len(company_elements)} companies, {new_in_this_page} new")
                previous_companies_set = current_companies_set
                
                if len(company_elements) < 10:
                    break
                current_page += 1
                time.sleep(1)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Blackboardjob", "new": total_new, "total": len(ALL_COMPANIES)})
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
                page.goto(url)
                time.sleep(3)
                
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                company_elements = soup.select("span.JobCard_company__NmRol")
                
                if not company_elements:
                    break
                
                current_page_companies = [c.get_text(strip=True) for c in company_elements if c.get_text(strip=True)]
                current_companies_set = set(current_page_companies)
                
                if current_companies_set == previous_companies_set and page_num > 1:
                    break
                
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Page {page_num}: Found {len(company_elements)} companies, {new_in_this_page} new")
                previous_companies_set = current_companies_set
                page_num += 1
                time.sleep(1)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Talent.com", "new": total_new, "total": len(ALL_COMPANIES)})
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
                        break
                    
                    current_page_companies = [row.inner_text().strip() for row in secondary if row.inner_text().strip()]
                    current_companies_set = set(current_page_companies)
                    
                    if current_companies_set == previous_companies_set and page_num > 1:
                        break
                    
                    new_in_this_page = 0
                    for name in current_page_companies:
                        if name and name not in companies:
                            companies.append(name)
                            if save_company_immediate(source_name, name):
                                total_new += 1
                                new_in_this_page += 1
                    
                    print(f"      Page {page_num}: Found {len(secondary)} companies, {new_in_this_page} new")
                    previous_companies_set = current_companies_set
                except Exception as e:
                    print(f"      Page {page_num}: Error - {str(e)[:50]}")
                    break
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "GulfTalent", "new": total_new, "total": len(ALL_COMPANIES)})
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
            page.goto(url)
            time.sleep(3)
            
            for page_num in range(1, 50):
                print(f"      Page {page_num}")
                try:
                        # wait up to 10 seconds for elements to load
                        page.wait_for_selector('//span[@class="w-[60px] md:w-auto inline-block whitespace-nowrap overflow-hidden text-ellipsis"]', timeout=10000)

                        main = page.query_selector_all('//span[@class="w-[60px] md:w-auto inline-block whitespace-nowrap overflow-hidden text-ellipsis"]')
                except:
                        break
                
                if not main:
                    break
                
                current_page_companies = [c.inner_text().strip() for c in main if c.inner_text().strip()]
                current_companies_set = set(current_page_companies)
                
                if current_companies_set == previous_companies_set and page_num > 1:
                    break
                
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"         Found {len(main)} companies, {new_in_this_page} new")
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
                page.goto(url)
                time.sleep(5)
                
                try:
                    # wait for elements to appear (max 10s)
                    page.wait_for_selector('//p[@data-test-name="_companyName"]', timeout=10000)

                    company_names = page.query_selector_all('//p[@data-test-name="_companyName"]')
                except:
                    break
                
                if not company_names:
                    break
                
                current_page_companies = [c.inner_text().strip() for c in company_names if c.inner_text().strip()]
                current_companies_set = set(current_page_companies)
                
                if current_companies_set == previous_companies_set and page_num > 1:
                    break
                
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Page {page_num}: Found {len(company_names)} companies, {new_in_this_page} new")
                previous_companies_set = current_companies_set
                time.sleep(1)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Jooble", "new": total_new, "total": len(ALL_COMPANIES)})
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
                page.goto(url)
                time.sleep(3)
                
                try:
                    # wait up to 10 seconds for elements to appear
                    page.wait_for_selector('//div[@class="ui-company"]', timeout=10000)

                    company_names = page.query_selector_all('//div[@class="ui-company"]')
                except:
                    break
                
                if not company_names:
                    break
                
                current_page_companies = [c.inner_text().strip() for c in company_names if c.inner_text().strip()]
                current_companies_set = set(current_page_companies)
                
                if current_companies_set == previous_companies_set and page_num > 1:
                    break
                
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Page {page_num}: Found {len(company_names)} companies, {new_in_this_page} new")
                previous_companies_set = current_companies_set
                time.sleep(1)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Adzuna", "new": total_new, "total": len(ALL_COMPANIES)})
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
                
                print(f"         Scroll {scrolls}: Found {len(elements)} companies, {new_in_this_scroll} new")
                
                if len(companies) == prev_company_count:
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
                
                print(f"         Scroll {scrolls}: Found {len(elements)} companies, {new_in_this_scroll} new")
                
                if len(companies) == prev_company_count:
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
                page.goto(url)
                time.sleep(6)
                
                try:
                    # wait up to 10 seconds
                    page.wait_for_selector('//p[@class="company"]', timeout=10000)

                    company_names = page.query_selector_all('//p[@class="company"]')
                except:
                    break
                
                if not company_names:
                    break
                
                current_page_companies = [c.inner_text().strip() for c in company_names if c.inner_text().strip()]
                current_companies_set = set(current_page_companies)
                
                if current_companies_set == previous_companies_set and page_num > 1:
                    break
                
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Page {page_num}: Found {len(company_names)} companies, {new_in_this_page} new")
                previous_companies_set = current_companies_set
                time.sleep(1)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "CareerJet", "new": total_new, "total": len(ALL_COMPANIES)})
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
                
                print(f"         Scroll {scrolls}: Found {len(elements)} companies, {new_in_this_scroll} new")
                
                if len(companies) == prev_company_count:
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
    return total_new

# ==================== PHASE 1 MAIN ====================
def run_phase1():
    print("="*70)
    print("🚀 PHASE 1: SCRAPE COMPANY NAMES FROM 10 WEBSITES")
    print("="*70)
    
    # Install browsers first
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
    print("📊 PHASE 1 SUMMARY")
    print("="*70)
    print(f"Total unique companies found: {len(ALL_COMPANIES)}")
    print(f"Results saved to: {PHASE1_OUTPUT_CSV}")
    
    return len(ALL_COMPANIES) > 0

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
            return phones, emails, website        except:
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
    
    # Install browsers for async as well
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
    
    async with async_playwright_lib() as p:
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

# ==================== MAIN ====================
def main():
    print("="*70)
    print("🎯 COMPLETE ZOHO PARTNER SCRAPER")
    print("   Phase 1: Scrape company names from 10 job websites")
    print("   Phase 2: Extract contact info (emails, phones, websites)")
    print("   AUTO-RESUME: Continues from where it left off if stopped")
    print("="*70)
    
    # Install browsers before anything else
    install_playwright_browsers()
    
    master_progress = load_master_progress()
    
    # Phase 1
    if not master_progress.get("phase1_completed", False):
        print("\n" + "="*70)
        print("📌 STARTING PHASE 1")
        print("="*70)
        
        phase1_success = run_phase1()
        
        if phase1_success:
            master_progress["phase1_completed"] = True
            save_master_progress(master_progress)
            print("\n✅ Phase 1 completed successfully!")
        else:
            print("\n⚠️ Phase 1 had issues. Progress saved. Run again to continue.")
            return
    else:
        print("\n⏭️ Phase 1 already completed. Skipping to Phase 2...")
    
    # Phase 2
    if not master_progress.get("phase2_completed", False):
        print("\n" + "="*70)
        print("📌 STARTING PHASE 2")
        print("="*70)
        
        # Check if Phase 1 output exists
        if not os.path.exists(PHASE1_OUTPUT_CSV):
            print(f"\n❌ Error: {PHASE1_OUTPUT_CSV} not found!")
            print("   Please run Phase 1 first.")
            return
        
        # Run Phase 2
        asyncio.run(run_phase2())
        
        master_progress["phase2_completed"] = True
        save_master_progress(master_progress)
        print("\n✅ Phase 2 completed!")
    else:
        print("\n⏭️ Phase 2 already completed!")
    
    print("\n" + "="*70)
    print("🎉 ALL PHASES COMPLETED SUCCESSFULLY!")
    print("="*70)
    print(f"📁 Phase 1 output: {PHASE1_OUTPUT_CSV}")
    print(f"📁 Phase 2 output: {PHASE2_OUTPUT_CSV}")

if __name__ == "__main__":
    main()
