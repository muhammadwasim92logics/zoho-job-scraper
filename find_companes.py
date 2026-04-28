# Full Cord With PlayWrite

"""
ENHANCED UNIFIED ZOHO JOB SCRAPER - 10 WEBSITES WITH MULTIPLE KEYWORDS
- Runs all scrapers one by one
- Searches multiple keywords per website (zoho, zoho developer, zoho consultant, etc.)
- Saves to single CSV with columns: Source Website, Company Name
- No duplicate companies across all websites
- Preserves all original XPaths
- RESUME CAPABLE: Continues from where it left off if stopped
"""

import csv
import time
import os
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# ==================== OUTPUT FILE ====================
OUTPUT_CSV = "All_Zoho_Companies_With_Source.csv"
PROGRESS_FILE = "scraper_progress.txt"  # Track progress across runs

# ==================== KEYWORDS FOR EACH WEBSITE ====================
# Each website will be searched with multiple keywords to get more results
KEYWORDS = [
    "zoho",
]

# Master set to track all companies (prevents duplicates)
ALL_COMPANIES = set()
WEBSITE_STATS = []

def load_existing_companies():
    """Load existing companies from CSV file to prevent duplicates and resume properly"""
    if os.path.exists(OUTPUT_CSV):
        try:
            with open(OUTPUT_CSV, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)  # Skip header
                for row in reader:
                    if len(row) >= 2:
                        ALL_COMPANIES.add(row[1].strip())
            print(f"📂 Loaded {len(ALL_COMPANIES)} existing companies from {OUTPUT_CSV}")
        except Exception as e:
            print(f"⚠️ Could not load existing companies: {e}")
    else:
        print("📂 No existing data found. Starting fresh.")

def save_company_immediate(source_url, company_name):
    """Save a single company immediately to CSV (called as soon as found)"""
    if company_name and company_name not in ALL_COMPANIES:
        ALL_COMPANIES.add(company_name)
        with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([source_url, company_name])
        return True
    return False

def save_batch_companies(source_url, companies):
    """Save multiple companies from same source"""
    new_count = 0
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for company in companies:
            if company and company.strip() and company not in ALL_COMPANIES:
                ALL_COMPANIES.add(company)
                writer.writerow([source_url, company.strip()])
                new_count += 1
    return new_count

# ==================== SHARED FUNCTIONS ====================

def setup_playwright():
    """Setup Playwright browser"""
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(
        headless=False,
        args=[
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

# ==================== WEBSITE 1: blackboardjob.com ====================
def scrape_blackboardjob():
    print("\n" + "="*70)
    print("🌐 WEBSITE 1: blackboardjob.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://ae.blackboardjob.com"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            BASE_URL = f"https://ae.blackboardjob.com/ads/index.php?q={keyword}&w=&p={{}}"
            companies = []
            current_page = 1
            previous_companies_set = set()  # Track companies from previous page
            
            while current_page <= 20:
                url = BASE_URL.format(current_page)
                print(f"\n   📄 Page {current_page}: {url}")
                page.goto(url)
                time.sleep(5)
                
                company_elements = page.query_selector_all(".item__company")
                
                if not company_elements:
                    print(f"      No companies found - stopping this keyword")
                    break
                
                # Get current page companies
                current_page_companies = []
                for elem in company_elements:
                    name = elem.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                # Convert to set for comparison
                current_companies_set = set(current_page_companies)
                
                # Check if we're getting the same companies as previous page
                if current_companies_set == previous_companies_set and current_page > 1:
                    print(f"      Same companies as previous page - no new data, stopping this keyword")
                    break
                
                # Save each new company immediately
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Found {len(company_elements)} companies, {new_in_this_page} new, {len(companies)} total for this keyword")
                
                # Update previous companies set
                previous_companies_set = current_companies_set
                
                # Check if the page has less than 10 companies (last page indicator)
                if len(company_elements) < 10:
                    print(f"      Last page detected (only {len(company_elements)} companies).")
                    break
                
                current_page += 1
                time.sleep(1)
            
            all_site_companies.extend(companies)
        
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Blackboardjob", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ Blackboardjob: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 2: talent.com ====================
def scrape_talent():
    print("\n" + "="*70)
    print("🌐 WEBSITE 2: talent.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    
    source_name = "https://ae.talent.com"
    all_site_companies = []
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
                time.sleep(5)
                
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                
                company_elements = soup.select("span.JobCard_company__NmRol")
                
                if not company_elements:
                    print(f"      Page {page_num}: No companies found - stopping")
                    break
                
                # Get current page companies
                current_page_companies = []
                for c in company_elements:
                    name = c.get_text(strip=True)
                    if name:
                        current_page_companies.append(name)
                
                current_companies_set = set(current_page_companies)
                
                # Check if we're getting the same companies as previous page
                if current_companies_set == previous_companies_set and page_num > 1:
                    print(f"      Same companies as previous page - no new data, stopping this keyword")
                    break
                
                # Save each new company immediately
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Page {page_num}: Found {len(company_elements)} companies, {new_in_this_page} new, {len(companies)} total for this keyword")
                
                previous_companies_set = current_companies_set
                page_num += 1
                time.sleep(1)
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Talent.com", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ Talent.com: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 3: gulftalent.com ====================
def scrape_gulftalent():
    print("\n" + "="*70)
    print("🌐 WEBSITE 3: gulftalent.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.gulftalent.com"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            previous_companies_set = set()
            
            for page_num in range(1, 6):
                url = f"https://www.gulftalent.com/mobile/uae/jobs/{page_num}?keyword={keyword}"
                page.goto(url)
                time.sleep(5)
                
                try:
                    main = page.query_selector('//div[@id="content"]')
                    if main:
                        secondary = main.query_selector_all('//div[@class="company-name"]')
                    else:
                        secondary = []
                    
                    if not secondary:
                        print(f"      Page {page_num}: No companies found - stopping")
                        break
                    
                    # Get current page companies
                    current_page_companies = []
                    for row in secondary:
                        name = row.inner_text().strip()
                        if name:
                            current_page_companies.append(name)
                    
                    current_companies_set = set(current_page_companies)
                    
                    # Check if we're getting the same companies as previous page
                    if current_companies_set == previous_companies_set and page_num > 1:
                        print(f"      Same companies as previous page - no new data, stopping this keyword")
                        break
                    
                    # Save each new company immediately
                    new_in_this_page = 0
                    for name in current_page_companies:
                        if name and name not in companies:
                            companies.append(name)
                            if save_company_immediate(source_name, name):
                                total_new += 1
                                new_in_this_page += 1
                    
                    print(f"      Page {page_num}: Found {len(secondary)} companies, {new_in_this_page} new, {len(companies)} total for this keyword")
                    
                    previous_companies_set = current_companies_set
                    
                except Exception as e:
                    print(f"      Page {page_num}: Error - {str(e)[:50]}")
                    break
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "GulfTalent", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ GulfTalent: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 4: timesjobs.com ====================
def scrape_timesjobs():
    print("\n" + "="*70)
    print("🌐 WEBSITE 4: timesjobs.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.timesjobs.com"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            previous_companies_set = set()
            
            url = f"https://www.timesjobs.com/job-search?keywords={keyword}&refreshed=true"
            page.goto(url)
            time.sleep(5)
            
            for page_num in range(1, 50):
                print(f"      Page {page_num}")
                
                try:
                    main = page.query_selector_all('//span[@class="w-[60px] md:w-auto inline-block whitespace-nowrap overflow-hidden text-ellipsis"]')
                except:
                    print(f"      No elements found - stopping this keyword")
                    break
                
                if not main:
                    print(f"      No companies found - stopping")
                    break
                
                # Get current page companies
                current_page_companies = []
                for c in main:
                    name = c.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                current_companies_set = set(current_page_companies)
                
                # Check if we're getting the same companies as previous page
                if current_companies_set == previous_companies_set and page_num > 1:
                    print("         Same companies as previous page - no new data, stopping this keyword")
                    break
                
                # Save each new company immediately
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"         Found {len(main)} companies, {new_in_this_page} new, {len(companies)} total for this keyword")
                
                previous_companies_set = current_companies_set
                
                # Try to go to next page
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    next_btn = page.query_selector('//button[@class="pagination-next"]')
                    if next_btn:
                        next_btn.click()
                        time.sleep(5)
                    else:
                        print("         No more pages")
                        break
                except:
                    print("         No more pages")
                    break
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "TimesJobs", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ TimesJobs: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 5: jooble.org ====================
def scrape_jooble():
    print("\n" + "="*70)
    print("🌐 WEBSITE 5: jooble.org")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://ae.jooble.org"
    all_site_companies = []
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
                    company_names = page.query_selector_all('//p[@data-test-name="_companyName"]')
                except:
                    print(f"      Page {page_num}: No companies found - stopping")
                    break
                
                if not company_names:
                    print(f"      Page {page_num}: No companies found - stopping")
                    break
                
                # Get current page companies
                current_page_companies = []
                for c in company_names:
                    name = c.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                current_companies_set = set(current_page_companies)
                
                # Check if we're getting the same companies as previous page
                if current_companies_set == previous_companies_set and page_num > 1:
                    print(f"      Same companies as previous page - no new data, stopping this keyword")
                    break
                
                # Save each new company immediately
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Page {page_num}: Found {len(company_names)} companies, {new_in_this_page} new, {len(companies)} total for this keyword")
                
                previous_companies_set = current_companies_set
                time.sleep(1)
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Jooble", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ Jooble: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 6: adzuna.com ====================
def scrape_adzuna():
    print("\n" + "="*70)
    print("🌐 WEBSITE 6: adzuna.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.adzuna.com"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            previous_companies_set = set()
            
            for page_num in range(1, 30):
                url = f"https://www.adzuna.com/search?loc=151946&q={keyword}&page={page_num}"
                page.goto(url)
                time.sleep(5)
                
                try:
                    company_names = page.query_selector_all('//div[@class="ui-company"]')
                except:
                    print(f"      Page {page_num}: No companies found - stopping")
                    break
                
                if not company_names:
                    print(f"      Page {page_num}: No companies found - stopping")
                    break
                
                # Get current page companies
                current_page_companies = []
                for c in company_names:
                    name = c.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                current_companies_set = set(current_page_companies)
                
                # Check if we're getting the same companies as previous page
                if current_companies_set == previous_companies_set and page_num > 1:
                    print(f"      Same companies as previous page - no new data, stopping this keyword")
                    break
                
                # Save each new company immediately
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Page {page_num}: Found {len(company_names)} companies, {new_in_this_page} new, {len(companies)} total for this keyword")
                
                previous_companies_set = current_companies_set
                time.sleep(1)
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Adzuna", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ Adzuna: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 7: linkedin.com ====================
def scrape_linkedin():
    print("\n" + "="*70)
    print("🌐 WEBSITE 7: linkedin.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.linkedin.com"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            
            url = f"https://www.linkedin.com/jobs/search/?keywords={keyword}&geoId=92000000"
            page.goto(url)
            time.sleep(5)
            
            print(f"      Scrolling to load all jobs for '{keyword}'...")
            last_height = 0
            scrolls = 0
            prev_company_count = 0
            
            while scrolls < 30:
                scrolls += 1
                page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(12)
                
                elements = page.query_selector_all('//h4[@class="base-search-card__subtitle"]')
                
                # Get current companies
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
                
                # Save new companies
                new_in_this_scroll = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_scroll += 1
                
                print(f"         Scroll {scrolls}: Found {len(elements)} companies, {new_in_this_scroll} new, {len(companies)} total for this keyword")
                
                # Check if we're getting new data
                if len(companies) == prev_company_count:
                    print(f"         No new companies found - stopping scroll for this keyword")
                    break
                
                prev_company_count = len(companies)
                
                new_height = page.evaluate("return document.body.scrollHeight")
                if new_height == last_height and scrolls > 1:
                    if new_in_this_scroll == 0:
                        print(f"         No more new companies - stopping")
                        break
                last_height = new_height
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "LinkedIn", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ LinkedIn: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 8: dice.com ====================
def scrape_dice():
    print("\n" + "="*70)
    print("🌐 WEBSITE 8: dice.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.dice.com"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            
            url = f"https://www.dice.com/jobs?q={keyword}"
            page.goto(url)
            time.sleep(5)
            
            print(f"      Scrolling to load all jobs for '{keyword}'...")
            last_height = 0
            scrolls = 0
            prev_company_count = 0
            
            while scrolls < 30:
                scrolls += 1
                page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(4)
                
                elements = page.query_selector_all('//p[@class="mb-0 line-clamp-2 text-sm sm:line-clamp-1"]')
                
                # Get current companies
                current_page_companies = []
                for elem in elements:
                    name = elem.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                # Save new companies
                new_in_this_scroll = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_scroll += 1
                
                print(f"         Scroll {scrolls}: Found {len(elements)} companies, {new_in_this_scroll} new, {len(companies)} total for this keyword")
                
                # Check if we're getting new data
                if len(companies) == prev_company_count:
                    print(f"         No new companies found - stopping scroll for this keyword")
                    break
                
                prev_company_count = len(companies)
                
                new_height = page.evaluate("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Dice", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ Dice: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 9: careerjet.com ====================
def scrape_careerjet():
    print("\n" + "="*70)
    print("🌐 WEBSITE 9: careerjet.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.careerjet.com"
    all_site_companies = []
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
                    company_names = page.query_selector_all('//p[@class="company"]')
                except:
                    print(f"      Page {page_num}: No companies found - stopping")
                    break
                
                if not company_names:
                    print(f"      Page {page_num}: No companies found - stopping")
                    break
                
                # Get current page companies
                current_page_companies = []
                for c in company_names:
                    name = c.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                current_companies_set = set(current_page_companies)
                
                # Check if we're getting the same companies as previous page
                if current_companies_set == previous_companies_set and page_num > 1:
                    print(f"      Same companies as previous page - no new data, stopping this keyword")
                    break
                
                # Save each new company immediately
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Page {page_num}: Found {len(company_names)} companies, {new_in_this_page} new, {len(companies)} total for this keyword")
                
                previous_companies_set = current_companies_set
                time.sleep(1)
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "CareerJet", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ CareerJet: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 10: cv-library.co.uk ====================
def scrape_cv_library():
    print("\n" + "="*70)
    print("🌐 WEBSITE 10: cv-library.co.uk")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.cv-library.co.uk"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            
            url = f"https://www.cv-library.co.uk/{keyword}-jobs"
            page.goto(url)
            time.sleep(5)
            
            print(f"      Scrolling to load all jobs for '{keyword}'...")
            last_height = 0
            scrolls = 0
            prev_company_count = 0
            
            while scrolls < 30:
                scrolls += 1
                page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(4)
                
                elements = page.query_selector_all('//a[@class="job__company-link"]')
                
                # Get current companies
                current_page_companies = []
                for elem in elements:
                    name = elem.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                # Save new companies
                new_in_this_scroll = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_scroll += 1
                
                print(f"         Scroll {scrolls}: Found {len(elements)} companies, {new_in_this_scroll} new, {len(companies)} total for this keyword")
                
                # Check if we're getting new data
                if len(companies) == prev_company_count:
                    print(f"         No new companies found - stopping scroll for this keyword")
                    break
                
                prev_company_count = len(companies)
                
                new_height = page.evaluate("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "CV-Library", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ CV-Library: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== MAIN FUNCTION ====================
def main():
    print("="*70)
    print("🚀 ENHANCED UNIFIED ZOHO JOB SCRAPER - 10 WEBSITES")
    print("   Features:")
    print("   - Multiple keywords per website (zoho, developer, consultant, etc.)")
    print("   - Source tracking (which website each company came from)")
    print("   - No duplicate companies across all websites")
    print("   - RESUME CAPABLE: Continues from where it left off")
    print("   - Immediate saving per company found")
    print("   - Detects duplicate companies across pages to avoid redundant scraping")
    print("="*70)
    
    # Load existing companies from CSV (prevents data loss on restart)
    load_existing_companies()
    
    # Create CSV file with headers only if it doesn't exist
    if not os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Source Website", "Company Name"])
        print(f"📝 Created new CSV file: {OUTPUT_CSV}")
    
    # Run all scrapers
    scrape_blackboardjob()
    scrape_talent()
    scrape_gulftalent()
    scrape_timesjobs()
    scrape_jooble()
    scrape_adzuna()
    scrape_linkedin()
    scrape_dice()
    scrape_careerjet()
    scrape_cv_library()
    
    # Print final summary
    print("\n" + "="*70)
    print("📊 FINAL SUMMARY")
    print("="*70)
    print(f"{'Source':<25} {'New Companies':<15} {'Running Total':<15}")
    print("-"*55)
    for stat in WEBSITE_STATS:
        print(f"{stat['source']:<25} {stat['new']:<15} {stat['total']:<15}")
    print("-"*55)
    print(f"{'TOTAL UNIQUE COMPANIES':<25} {len(ALL_COMPANIES):<15}")
    print("="*70)
    print(f"\n✅ All results saved to: {OUTPUT_CSV}")
    print(f"   Format: [Source Website] [Company Name]")
    print(f"   Total unique companies: {len(ALL_COMPANIES)}")

if __name__ == "__main__":
    main()
    
    
    
    
    
    
    
    
    
    # Find Contect Info
    
    
    
    
    
    """
ENHANCED UNIFIED ZOHO JOB SCRAPER - 10 WEBSITES WITH MULTIPLE KEYWORDS
- Runs all scrapers one by one
- Searches multiple keywords per website (zoho, zoho developer, zoho consultant, etc.)
- Saves to single CSV with columns: Source Website, Company Name
- No duplicate companies across all websites
- Preserves all original XPaths
- RESUME CAPABLE: Continues from where it left off if stopped
"""

import csv
import time
import os
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# ==================== OUTPUT FILE ====================
OUTPUT_CSV = "All_Zoho_Companies_With_Source.csv"
PROGRESS_FILE = "scraper_progress.txt"  # Track progress across runs

# ==================== KEYWORDS FOR EACH WEBSITE ====================
# Each website will be searched with multiple keywords to get more results
KEYWORDS = [
    "zoho",
]

# Master set to track all companies (prevents duplicates)
ALL_COMPANIES = set()
WEBSITE_STATS = []

def load_existing_companies():
    """Load existing companies from CSV file to prevent duplicates and resume properly"""
    if os.path.exists(OUTPUT_CSV):
        try:
            with open(OUTPUT_CSV, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)  # Skip header
                for row in reader:
                    if len(row) >= 2:
                        ALL_COMPANIES.add(row[1].strip())
            print(f"📂 Loaded {len(ALL_COMPANIES)} existing companies from {OUTPUT_CSV}")
        except Exception as e:
            print(f"⚠️ Could not load existing companies: {e}")
    else:
        print("📂 No existing data found. Starting fresh.")

def save_company_immediate(source_url, company_name):
    """Save a single company immediately to CSV (called as soon as found)"""
    if company_name and company_name not in ALL_COMPANIES:
        ALL_COMPANIES.add(company_name)
        with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([source_url, company_name])
        return True
    return False

def save_batch_companies(source_url, companies):
    """Save multiple companies from same source"""
    new_count = 0
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for company in companies:
            if company and company.strip() and company not in ALL_COMPANIES:
                ALL_COMPANIES.add(company)
                writer.writerow([source_url, company.strip()])
                new_count += 1
    return new_count

# ==================== SHARED FUNCTIONS ====================

def setup_playwright():
    """Setup Playwright browser with better stealth settings"""
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(
        headless=False,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-accelerated-2d-canvas",
            "--disable-gpu"
        ]
    )
    context = browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        extra_http_headers={
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
    )
    page = context.new_page()
    
    # Remove webdriver property
    page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    """)
    
    return playwright, browser, page

# ==================== WEBSITE 1: blackboardjob.com ====================
def scrape_blackboardjob():
    print("\n" + "="*70)
    print("🌐 WEBSITE 1: blackboardjob.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://ae.blackboardjob.com"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            BASE_URL = f"https://ae.blackboardjob.com/ads/index.php?q={keyword}&w=&p={{}}"
            companies = []
            current_page = 1
            previous_companies_set = set()  # Track companies from previous page
            
            while current_page <= 20:
                url = BASE_URL.format(current_page)
                print(f"\n   📄 Page {current_page}: {url}")
                try:
                    page.goto(url, timeout=30000)
                except Exception as e:
                    print(f"      Timeout error: {e}")
                    break
                time.sleep(3)
                
                company_elements = page.query_selector_all(".item__company")
                
                if not company_elements:
                    print(f"      No companies found - stopping this keyword")
                    break
                
                # Get current page companies
                current_page_companies = []
                for elem in company_elements:
                    name = elem.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                # Convert to set for comparison
                current_companies_set = set(current_page_companies)
                
                # Check if we're getting the same companies as previous page
                if current_companies_set == previous_companies_set and current_page > 1:
                    print(f"      Same companies as previous page - no new data, stopping this keyword")
                    break
                
                # Save each new company immediately
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Found {len(company_elements)} companies, {new_in_this_page} new, {len(companies)} total for this keyword")
                
                # Update previous companies set
                previous_companies_set = current_companies_set
                
                # Check if the page has less than 10 companies (last page indicator)
                if len(company_elements) < 10:
                    print(f"      Last page detected (only {len(company_elements)} companies).")
                    break
                
                current_page += 1
                time.sleep(1)
            
            all_site_companies.extend(companies)
        
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Blackboardjob", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ Blackboardjob: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 2: talent.com ====================
def scrape_talent():
    print("\n" + "="*70)
    print("🌐 WEBSITE 2: talent.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    
    source_name = "https://ae.talent.com"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            page_num = 1
            previous_companies_set = set()
            
            while True:
                url = f"https://ae.talent.com/jobs?k={keyword}&l=United+Arab+Emirates&p={page_num}"
                try:
                    page.goto(url, timeout=30000)
                except Exception as e:
                    print(f"      Timeout error: {e}")
                    break
                time.sleep(3)
                
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                
                company_elements = soup.select("span.JobCard_company__NmRol")
                
                if not company_elements:
                    print(f"      Page {page_num}: No companies found - stopping")
                    break
                
                # Get current page companies
                current_page_companies = []
                for c in company_elements:
                    name = c.get_text(strip=True)
                    if name:
                        current_page_companies.append(name)
                
                current_companies_set = set(current_page_companies)
                
                # Check if we're getting the same companies as previous page
                if current_companies_set == previous_companies_set and page_num > 1:
                    print(f"      Same companies as previous page - no new data, stopping this keyword")
                    break
                
                # Save each new company immediately
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Page {page_num}: Found {len(company_elements)} companies, {new_in_this_page} new, {len(companies)} total for this keyword")
                
                previous_companies_set = current_companies_set
                page_num += 1
                time.sleep(1)
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Talent.com", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ Talent.com: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 3: gulftalent.com ====================
def scrape_gulftalent():
    print("\n" + "="*70)
    print("🌐 WEBSITE 3: gulftalent.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.gulftalent.com"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            previous_companies_set = set()
            
            for page_num in range(1, 6):
                url = f"https://www.gulftalent.com/mobile/uae/jobs/{page_num}?keyword={keyword}"
                try:
                    page.goto(url, timeout=60000, wait_until='domcontentloaded')
                    time.sleep(5)
                except Exception as e:
                    print(f"      Page {page_num}: Error loading - {str(e)[:100]}")
                    continue
                
                try:
                    # Try multiple selectors to find company names
                    secondary = page.query_selector_all('//div[@class="company-name"]')
                    
                    if not secondary:
                        secondary = page.query_selector_all('.company-name')
                    
                    if not secondary:
                        print(f"      Page {page_num}: No companies found - stopping")
                        break
                    
                    # Get current page companies
                    current_page_companies = []
                    for row in secondary:
                        name = row.inner_text().strip()
                        if name:
                            current_page_companies.append(name)
                    
                    current_companies_set = set(current_page_companies)
                    
                    # Check if we're getting the same companies as previous page
                    if current_companies_set == previous_companies_set and page_num > 1:
                        print(f"      Same companies as previous page - no new data, stopping this keyword")
                        break
                    
                    # Save each new company immediately
                    new_in_this_page = 0
                    for name in current_page_companies:
                        if name and name not in companies:
                            companies.append(name)
                            if save_company_immediate(source_name, name):
                                total_new += 1
                                new_in_this_page += 1
                    
                    print(f"      Page {page_num}: Found {len(secondary)} companies, {new_in_this_page} new, {len(companies)} total for this keyword")
                    
                    previous_companies_set = current_companies_set
                    
                except Exception as e:
                    print(f"      Page {page_num}: Error - {str(e)[:50]}")
                    break
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "GulfTalent", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ GulfTalent: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 4: timesjobs.com ====================
def scrape_timesjobs():
    print("\n" + "="*70)
    print("🌐 WEBSITE 4: timesjobs.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.timesjobs.com"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            previous_companies_set = set()
            
            url = f"https://www.timesjobs.com/job-search?keywords={keyword}&refreshed=true"
            try:
                page.goto(url, timeout=30000)
                time.sleep(3)
            except Exception as e:
                print(f"      Error loading: {e}")
                continue
            
            for page_num in range(1, 50):
                print(f"      Page {page_num}")
                
                try:
                    main = page.query_selector_all('//span[@class="w-[60px] md:w-auto inline-block whitespace-nowrap overflow-hidden text-ellipsis"]')
                except:
                    print(f"      No elements found - stopping this keyword")
                    break
                
                if not main:
                    print(f"      No companies found - stopping")
                    break
                
                # Get current page companies
                current_page_companies = []
                for c in main:
                    name = c.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                current_companies_set = set(current_page_companies)
                
                # Check if we're getting the same companies as previous page
                if current_companies_set == previous_companies_set and page_num > 1:
                    print("         Same companies as previous page - no new data, stopping this keyword")
                    break
                
                # Save each new company immediately
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"         Found {len(main)} companies, {new_in_this_page} new, {len(companies)} total for this keyword")
                
                previous_companies_set = current_companies_set
                
                # Try to go to next page
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    next_btn = page.query_selector('//button[@class="pagination-next"]')
                    if next_btn:
                        next_btn.click()
                        time.sleep(3)
                    else:
                        print("         No more pages")
                        break
                except:
                    print("         No more pages")
                    break
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "TimesJobs", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ TimesJobs: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 5: jooble.org ====================
def scrape_jooble():
    print("\n" + "="*70)
    print("🌐 WEBSITE 5: jooble.org")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://ae.jooble.org"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            previous_companies_set = set()
            
            for page_num in range(1, 30):
                url = f"https://ae.jooble.org/jobs-{keyword}?p={page_num}"
                try:
                    page.goto(url, timeout=30000)
                    time.sleep(3)
                except Exception as e:
                    print(f"      Page {page_num}: Error loading - {e}")
                    break
                
                try:
                    company_names = page.query_selector_all('//p[@data-test-name="_companyName"]')
                except:
                    print(f"      Page {page_num}: No companies found - stopping")
                    break
                
                if not company_names:
                    print(f"      Page {page_num}: No companies found - stopping")
                    break
                
                # Get current page companies
                current_page_companies = []
                for c in company_names:
                    name = c.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                current_companies_set = set(current_page_companies)
                
                # Check if we're getting the same companies as previous page
                if current_companies_set == previous_companies_set and page_num > 1:
                    print(f"      Same companies as previous page - no new data, stopping this keyword")
                    break
                
                # Save each new company immediately
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Page {page_num}: Found {len(company_names)} companies, {new_in_this_page} new, {len(companies)} total for this keyword")
                
                previous_companies_set = current_companies_set
                time.sleep(1)
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Jooble", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ Jooble: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 6: adzuna.com ====================
def scrape_adzuna():
    print("\n" + "="*70)
    print("🌐 WEBSITE 6: adzuna.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.adzuna.com"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            previous_companies_set = set()
            
            for page_num in range(1, 30):
                url = f"https://www.adzuna.com/search?loc=151946&q={keyword}&page={page_num}"
                try:
                    page.goto(url, timeout=30000)
                    time.sleep(3)
                except Exception as e:
                    print(f"      Page {page_num}: Error loading - {e}")
                    break
                
                try:
                    company_names = page.query_selector_all('//div[@class="ui-company"]')
                except:
                    print(f"      Page {page_num}: No companies found - stopping")
                    break
                
                if not company_names:
                    print(f"      Page {page_num}: No companies found - stopping")
                    break
                
                # Get current page companies
                current_page_companies = []
                for c in company_names:
                    name = c.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                current_companies_set = set(current_page_companies)
                
                # Check if we're getting the same companies as previous page
                if current_companies_set == previous_companies_set and page_num > 1:
                    print(f"      Same companies as previous page - no new data, stopping this keyword")
                    break
                
                # Save each new company immediately
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Page {page_num}: Found {len(company_names)} companies, {new_in_this_page} new, {len(companies)} total for this keyword")
                
                previous_companies_set = current_companies_set
                time.sleep(1)
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Adzuna", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ Adzuna: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 7: linkedin.com ====================
def scrape_linkedin():
    print("\n" + "="*70)
    print("🌐 WEBSITE 7: linkedin.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.linkedin.com"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            
            url = f"https://www.linkedin.com/jobs/search/?keywords={keyword}&geoId=92000000"
            try:
                page.goto(url, timeout=30000)
                time.sleep(5)
            except Exception as e:
                print(f"      Error loading: {e}")
                continue
            
            print(f"      Scrolling to load all jobs for '{keyword}'...")
            last_height = 0
            scrolls = 0
            prev_company_count = 0
            
            while scrolls < 30:
                scrolls += 1
                page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(12)
                
                elements = page.query_selector_all('//h4[@class="base-search-card__subtitle"]')
                
                # Get current companies
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
                
                # Save new companies
                new_in_this_scroll = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_scroll += 1
                
                print(f"         Scroll {scrolls}: Found {len(elements)} companies, {new_in_this_scroll} new, {len(companies)} total for this keyword")
                
                # Check if we're getting new data
                if len(companies) == prev_company_count:
                    print(f"         No new companies found - stopping scroll for this keyword")
                    break
                
                prev_company_count = len(companies)
                
                new_height = page.evaluate("return document.body.scrollHeight")
                if new_height == last_height and scrolls > 1:
                    if new_in_this_scroll == 0:
                        print(f"         No more new companies - stopping")
                        break
                last_height = new_height
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "LinkedIn", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ LinkedIn: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 8: dice.com ====================
def scrape_dice():
    print("\n" + "="*70)
    print("🌐 WEBSITE 8: dice.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.dice.com"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            
            url = f"https://www.dice.com/jobs?q={keyword}"
            try:
                page.goto(url, timeout=30000)
                time.sleep(3)
            except Exception as e:
                print(f"      Error loading: {e}")
                continue
            
            print(f"      Scrolling to load all jobs for '{keyword}'...")
            last_height = 0
            scrolls = 0
            prev_company_count = 0
            
            while scrolls < 30:
                scrolls += 1
                page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(4)
                
                elements = page.query_selector_all('//p[@class="mb-0 line-clamp-2 text-sm sm:line-clamp-1"]')
                
                # Get current companies
                current_page_companies = []
                for elem in elements:
                    name = elem.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                # Save new companies
                new_in_this_scroll = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_scroll += 1
                
                print(f"         Scroll {scrolls}: Found {len(elements)} companies, {new_in_this_scroll} new, {len(companies)} total for this keyword")
                
                # Check if we're getting new data
                if len(companies) == prev_company_count:
                    print(f"         No new companies found - stopping scroll for this keyword")
                    break
                
                prev_company_count = len(companies)
                
                new_height = page.evaluate("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "Dice", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ Dice: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 9: careerjet.com ====================
def scrape_careerjet():
    print("\n" + "="*70)
    print("🌐 WEBSITE 9: careerjet.com")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.careerjet.com"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            previous_companies_set = set()
            
            for page_num in range(1, 30):
                url = f"https://www.careerjet.com/jobs?s={keyword}&p={page_num}"
                try:
                    page.goto(url, timeout=60000)
                    time.sleep(6)
                except Exception as e:
                    print(f"      Page {page_num}: Error loading - {e}")
                    break
                
                try:
                    company_names = page.query_selector_all('//p[@class="company"]')
                except:
                    print(f"      Page {page_num}: No companies found - stopping")
                    break
                
                if not company_names:
                    print(f"      Page {page_num}: No companies found - stopping")
                    break
                
                # Get current page companies
                current_page_companies = []
                for c in company_names:
                    name = c.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                current_companies_set = set(current_page_companies)
                
                # Check if we're getting the same companies as previous page
                if current_companies_set == previous_companies_set and page_num > 1:
                    print(f"      Same companies as previous page - no new data, stopping this keyword")
                    break
                
                # Save each new company immediately
                new_in_this_page = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_page += 1
                
                print(f"      Page {page_num}: Found {len(company_names)} companies, {new_in_this_page} new, {len(companies)} total for this keyword")
                
                previous_companies_set = current_companies_set
                time.sleep(1)
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "CareerJet", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ CareerJet: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== WEBSITE 10: cv-library.co.uk ====================
def scrape_cv_library():
    print("\n" + "="*70)
    print("🌐 WEBSITE 10: cv-library.co.uk")
    print("="*70)
    
    playwright, browser, page = setup_playwright()
    source_name = "https://www.cv-library.co.uk"
    all_site_companies = []
    total_new = 0
    
    try:
        for keyword in KEYWORDS:
            print(f"\n   🔍 Searching keyword: {keyword}")
            companies = []
            
            url = f"https://www.cv-library.co.uk/{keyword}-jobs"
            try:
                page.goto(url, timeout=30000)
                time.sleep(3)
            except Exception as e:
                print(f"      Error loading: {e}")
                continue
            
            print(f"      Scrolling to load all jobs for '{keyword}'...")
            last_height = 0
            scrolls = 0
            prev_company_count = 0
            
            while scrolls < 30:
                scrolls += 1
                page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(4)
                
                elements = page.query_selector_all('//a[@class="job__company-link"]')
                
                # Get current companies
                current_page_companies = []
                for elem in elements:
                    name = elem.inner_text().strip()
                    if name:
                        current_page_companies.append(name)
                
                # Save new companies
                new_in_this_scroll = 0
                for name in current_page_companies:
                    if name and name not in companies:
                        companies.append(name)
                        if save_company_immediate(source_name, name):
                            total_new += 1
                            new_in_this_scroll += 1
                
                print(f"         Scroll {scrolls}: Found {len(elements)} companies, {new_in_this_scroll} new, {len(companies)} total for this keyword")
                
                # Check if we're getting new data
                if len(companies) == prev_company_count:
                    print(f"         No new companies found - stopping scroll for this keyword")
                    break
                
                prev_company_count = len(companies)
                
                new_height = page.evaluate("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            all_site_companies.extend(companies)
    finally:
        browser.close()
        playwright.stop()
    
    WEBSITE_STATS.append({"source": "CV-Library", "new": total_new, "total": len(ALL_COMPANIES)})
    print(f"\n✅ CV-Library: Added {total_new} new companies (Total: {len(ALL_COMPANIES)})")
    return total_new

# ==================== MAIN FUNCTION ====================
def main():
    print("="*70)
    print("🚀 ENHANCED UNIFIED ZOHO JOB SCRAPER - 10 WEBSITES")
    print("   Features:")
    print("   - Multiple keywords per website (zoho, developer, consultant, etc.)")
    print("   - Source tracking (which website each company came from)")
    print("   - No duplicate companies across all websites")
    print("   - RESUME CAPABLE: Continues from where it left off")
    print("   - Immediate saving per company found")
    print("   - Detects duplicate companies across pages to avoid redundant scraping")
    print("="*70)
    
    # Load existing companies from CSV (prevents data loss on restart)
    load_existing_companies()
    
    # Create CSV file with headers only if it doesn't exist
    if not os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Source Website", "Company Name"])
        print(f"📝 Created new CSV file: {OUTPUT_CSV}")
    
    # Run all scrapers
    scrape_blackboardjob()
    scrape_talent()
    scrape_gulftalent()
    scrape_timesjobs()
    scrape_jooble()
    scrape_adzuna()
    scrape_linkedin()
    scrape_dice()
    scrape_careerjet()
    scrape_cv_library()
    
    # Print final summary
    print("\n" + "="*70)
    print("📊 FINAL SUMMARY")
    print("="*70)
    print(f"{'Source':<25} {'New Companies':<15} {'Running Total':<15}")
    print("-"*55)
    for stat in WEBSITE_STATS:
        print(f"{stat['source']:<25} {stat['new']:<15} {stat['total']:<15}")
    print("-"*55)
    print(f"{'TOTAL UNIQUE COMPANIES':<25} {len(ALL_COMPANIES):<15}")
    print("="*70)
    print(f"\n✅ All results saved to: {OUTPUT_CSV}")
    print(f"   Format: [Source Website] [Company Name]")
    print(f"   Total unique companies: {len(ALL_COMPANIES)}")

if __name__ == "__main__":
    main()
    
    