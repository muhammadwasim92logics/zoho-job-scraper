"""
PHASE 2: EXTRACT CONTACT INFO - GitHub Actions Compatible
- Uses DuckDuckGo HTML search (no JavaScript)
- Falls back to direct website checking
- No Playwright needed - uses requests only
- Resume capable
"""

import csv
import os
import json
import re
import time
import requests
from urllib.parse import quote
from datetime import datetime
from bs4 import BeautifulSoup

os.environ['PYTHONUNBUFFERED'] = '1'

# ==================== PHASE 2 CONFIGURATION ====================
PHASE2_INPUT_CSV = "All_Zoho_Companies_With_Source.csv"
PHASE2_OUTPUT_CSV = "companies_contacts_fixed.csv"
PHASE2_PROGRESS_FILE = "phase2_progress.json"
REQUEST_DELAY = 2

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

# ==================== PHONE/EMAIL PATTERNS (YOUR ORIGINAL) ====================
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

# ==================== SEARCH USING DUCKDUCKGO (No JavaScript) ====================
def search_duckduckgo(company_name):
    """Search DuckDuckGo HTML version - works without JavaScript"""
    all_emails = []
    all_phones = []
    website = None
    
    encoded_name = quote(f"{company_name}")
    url = f"https://html.duckduckgo.com/html/?q={encoded_name}"
    
    try:
        print(f"   🔍 Searching: {company_name[:40]}...")
        response = requests.get(url, headers=HEADERS, timeout=20)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        text = soup.get_text()
        
        # Extract emails and phones
        all_emails = extract_real_emails_from_text(text)
        all_phones = extract_real_phones_from_text(text)
        
        # Extract website from results
        for result in soup.find_all('a', class_='result__a'):
            href = result.get('href', '')
            if href and 'http' in href:
                # Clean URL
                match = re.search(r'(https?://[^/]+)', href)
                if match:
                    potential = match.group(1)
                    # Skip social media
                    skip = ['duckduckgo', 'facebook', 'twitter', 'linkedin', 'instagram', 'youtube', 'wikipedia']
                    if not any(s in potential.lower() for s in skip):
                        website = potential
                        print(f"   🌐 Found: {website}")
                        break
        
        # If no website from first page, try searching with "website"
        if not website:
            url2 = f"https://html.duckduckgo.com/html/?q={encoded_name}+website"
            response2 = requests.get(url2, headers=HEADERS, timeout=20)
            soup2 = BeautifulSoup(response2.text, 'html.parser')
            text2 = soup2.get_text()
            
            all_emails.extend(extract_real_emails_from_text(text2))
            all_phones.extend(extract_real_phones_from_text(text2))
            
            for result in soup2.find_all('a', class_='result__a'):
                href = result.get('href', '')
                if href and 'http' in href:
                    match = re.search(r'(https?://[^/]+)', href)
                    if match:
                        potential = match.group(1)
                        skip = ['duckduckgo', 'facebook', 'twitter', 'linkedin', 'instagram', 'youtube']
                        if not any(s in potential.lower() for s in skip):
                            website = potential
                            print(f"   🌐 Found: {website}")
                            break
        
        # Deduplicate
        all_emails = list(dict.fromkeys(all_emails))
        all_phones = list(dict.fromkeys(all_phones))
        
        return all_phones, all_emails, website
        
    except Exception as e:
        print(f"   ⚠️ Search error: {str(e)[:50]}")
        return [], [], None

# ==================== DIRECT WEBSITE CHECKING ====================
def try_company_website(company_name):
    """Try common domain patterns directly"""
    # Clean company name
    name_clean = re.sub(r'[^\w\s]', '', company_name.lower())
    name_clean = re.sub(r'\s+', '', name_clean)
    name_clean = re.sub(r'(pvt|ltd|llc|inc|corp|co|technologies|solutions)$', '', name_clean)
    name_clean = name_clean.strip()
    
    domains = [
        f"https://{name_clean}.com",
        f"https://www.{name_clean}.com",
        f"https://{name_clean}.io",
        f"https://{name_clean}.co",
        f"https://{name_clean}.org",
        f"https://{name_clean}.net",
    ]
    
    for domain in domains:
        try:
            response = requests.get(domain, headers=HEADERS, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                text = soup.get_text()
                emails = extract_real_emails_from_text(text)
                phones = extract_real_phones_from_text(text)
                print(f"   ✅ Direct website: {domain}")
                return domain, emails, phones
        except:
            continue
    
    return None, [], []

def visit_website_for_contact(website):
    """Visit website to extract contact info"""
    if not website:
        return [], []
    
    all_emails = []
    all_phones = []
    
    try:
        url = website if website.startswith('http') else f"https://{website}"
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.get_text()
            all_emails = extract_real_emails_from_text(text)
            all_phones = extract_real_phones_from_text(text)
            
            # Also check contact page
            contact_urls = [f"{url.rstrip('/')}/contact", f"{url.rstrip('/')}/contact-us", f"{url.rstrip('/')}/about"]
            for contact_url in contact_urls[:2]:
                try:
                    resp = requests.get(contact_url, headers=HEADERS, timeout=10)
                    if resp.status_code == 200:
                        soup2 = BeautifulSoup(resp.text, 'html.parser')
                        text2 = soup2.get_text()
                        all_emails.extend(extract_real_emails_from_text(text2))
                        all_phones.extend(extract_real_phones_from_text(text2))
                except:
                    pass
    
            all_emails = list(dict.fromkeys(all_emails))
            all_phones = list(dict.fromkeys(all_phones))
            
    except:
        pass
    
    return all_emails, all_phones

def check_zoho_partner(website, company_name, page_text=""):
    """Check if company is Zoho partner"""
    if 'zoho' in company_name.lower():
        return "Yes"
    
    if website:
        try:
            url = website if website.startswith('http') else f"https://{website}"
            response = requests.get(url, headers=HEADERS, timeout=10)
            text = response.text.lower()
            keywords = ['zoho partner', 'zoho authorized', 'zoho certified', 'zoho implementation', 
                       'zoho consultant', 'zoho expert', 'zoho solution', 'zoho alliance']
            for keyword in keywords:
                if keyword in text:
                    return "Yes"
        except:
            pass
    
    return "No"

# ==================== PROGRESS TRACKING ====================
def load_progress():
    if os.path.exists(PHASE2_PROGRESS_FILE):
        try:
            with open(PHASE2_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {"completed_companies": [], "last_company": None}
    return {"completed_companies": [], "last_company": None}

def save_progress(progress, results):
    try:
        with open(PHASE2_PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, indent=2)
        with open(PHASE2_OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["Company Name", "Website", "Contact Email", "Phone Number", "Zoho Partner Status", "Source", "Processed Date"])
            writer.writeheader()
            writer.writerows(results)
        return True
    except Exception as e:
        print(f"⚠️ Error saving: {e}")
        return False

def read_companies():
    companies = []
    try:
        with open(PHASE2_INPUT_CSV, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) >= 2 and row[1].strip():
                    companies.append(row[1].strip())
        # Remove duplicates
        return list(dict.fromkeys(companies))
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return []

def load_existing_results():
    if os.path.exists(PHASE2_OUTPUT_CSV):
        try:
            with open(PHASE2_OUTPUT_CSV, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                return list(reader)
        except:
            pass
    return []

# ==================== MAIN ====================
def main():
    print("=" * 70)
    print("🚀 PHASE 2: EXTRACT CONTACT INFO (GitHub Actions Compatible)")
    print("   Using DuckDuckGo + Direct Website Checking")
    print("=" * 70)
    
    # Load data
    all_companies = read_companies()
    progress = load_progress()
    results = load_existing_results()
    
    companies_to_process = [c for c in all_companies if c not in progress["completed_companies"]]
    
    print(f"📊 Total companies: {len(all_companies)}")
    print(f"📊 Already processed: {len(progress['completed_companies'])}")
    print(f"📊 Remaining: {len(companies_to_process)}")
    
    if not companies_to_process:
        print("\n✅ All companies processed!")
        if results:
            total = len(results)
            with_email = sum(1 for r in results if r.get('Contact Email'))
            with_phone = sum(1 for r in results if r.get('Phone Number'))
            print(f"\n📊 FINAL SUMMARY:")
            print(f"   Total processed: {total}")
            print(f"   Emails found: {with_email}")
            print(f"   Phones found: {with_phone}")
        return
    
    for idx, company in enumerate(companies_to_process, 1):
        print(f"\n{'='*60}")
        print(f"[{idx}/{len(companies_to_process)}] Processing: {company[:60]}")
        print(f"{'='*60}")
        
        phones = []
        emails = []
        website = ""
        source = "Not Found"
        
        try:
            # Method 1: Search DuckDuckGo (works on GitHub Actions)
            phones, emails, website = search_duckduckgo(company)
            
            if website:
                source = "DuckDuckGo"
                print(f"   🌐 Website: {website}")
                
                # Visit website for more contact info
                web_emails, web_phones = visit_website_for_contact(website)
                emails.extend([e for e in web_emails if e not in emails])
                phones.extend([p for p in web_phones if p not in phones])
                if web_emails or web_phones:
                    source = "DuckDuckGo + Website"
            
            # Method 2: If no website found, try direct domain
            if not website:
                print(f"   🔍 Trying direct domain...")
                website, direct_emails, direct_phones = try_company_website(company)
                if website:
                    emails.extend(direct_emails)
                    phones.extend(direct_phones)
                    source = "Direct Domain"
                    print(f"   🌐 Direct: {website}")
                else:
                    print(f"   ❌ No website found")
            
            # Deduplicate
            emails = list(dict.fromkeys(emails))
            phones = list(dict.fromkeys(phones))
            
            # Check Zoho partner status
            partner = check_zoho_partner(website, company)
            
            result = {
                "Company Name": company,
                "Website": website if website else "",
                "Contact Email": emails[0] if emails else "",
                "Phone Number": phones[0] if phones else "",
                "Zoho Partner Status": partner,
                "Source": source,
                "Processed Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            print(f"\n   📊 RESULT:")
            print(f"      Website: {website if website else '❌ Not found'}")
            print(f"      Email: {emails[0] if emails else '❌ Not found'}")
            print(f"      Phone: {phones[0] if phones else '❌ Not found'}")
            print(f"      Zoho Partner: {partner}")
            
        except Exception as e:
            print(f"   ❌ Error: {str(e)[:100]}")
            result = {
                "Company Name": company,
                "Website": "",
                "Contact Email": "",
                "Phone Number": "",
                "Zoho Partner Status": "Error",
                "Source": "Error",
                "Processed Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        
        # Save progress
        results.append(result)
        progress["completed_companies"].append(company)
        progress["last_company"] = company
        save_progress(progress, results)
        print(f"\n   💾 Progress: {len(progress['completed_companies'])}/{len(all_companies)}")
        
        # Delay
        if idx < len(companies_to_process):
            time.sleep(REQUEST_DELAY)
    
    # Final summary
    print(f"\n{'='*60}")
    print("📊 PHASE 2 FINAL SUMMARY")
    print(f"{'='*60}")
    total = len(results)
    with_email = sum(1 for r in results if r.get('Contact Email'))
    with_phone = sum(1 for r in results if r.get('Phone Number'))
    with_website = sum(1 for r in results if r.get('Website'))
    zoho_partners = sum(1 for r in results if r.get('Zoho Partner Status') == 'Yes')
    
    print(f"Total companies: {total}")
    print(f"With website: {with_website}")
    print(f"With email: {with_email}")
    print(f"With phone: {with_phone}")
    print(f"Zoho Partners: {zoho_partners}")
    print(f"\n✅ Results: {PHASE2_OUTPUT_CSV}")

if __name__ == "__main__":
    main()
