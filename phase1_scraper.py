"""
COMPLETE ZOHO PARTNER SCRAPER - PHASE 1 ONLY
- Scrape company names from 10 job websites
- AUTO-RESUME: If stopped anywhere, continues from where it left off
"""

import csv
import time
import os
import json
import re
import subprocess
import sys
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

os.environ['PLAYWRIGHT_BROWSERS_PATH'] = '0'

# ==================== PHASE 1 CONFIGURATION ====================
PHASE1_OUTPUT_CSV = "All_Zoho_Companies_With_Source.csv"
PHASE1_PROGRESS_FILE = "phase1_progress.json"

# ==================== FIX: INSTALL PLAYWRIGHT BROWSERS ====================
def install_playwright_browsers():
    """Install Playwright browsers if not already installed"""
    print("🔧 Checking Playwright browser installation...")
    try:
        # Try to use playwright with sync API
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
            "--no-sandbox",  # FIX: Added for Linux environments
            "--disable-setuid-sandbox",  # FIX: Added for security
            "--incognito",
            "--disable-blink-features=AutomationControlled",
            "--start-maximized",
            f"--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        ]
    )
    context = browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
    page = context.new_page()
    return playwright, browser, page

# Phase 1 Scrapers (YOUR ORIGINAL CODE - NO CHANGES)
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

if __name__ == "__main__":
    run_phase1()
