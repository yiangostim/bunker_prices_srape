#!/usr/bin/env python3
"""
Ship & Bunker Price Scraper
Scrapes fuel prices from shipandbunker.com and saves to CSV files
"""

import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import pytz
import os
import re
import time


def get_utc_timestamp():
    """Get current UTC timestamp in European format (DD/MM/YYYY HH:MM)"""
    utc_now = datetime.now(pytz.UTC)
    return utc_now.strftime("%d/%m/%Y %H:%M")


def clean_numeric_value(text):
    """Clean and convert text to float, handling various formats"""
    if not text:
        return 0.0
    
    # Remove HTML tags and extra whitespace
    cleaned = re.sub(r'<[^>]+>', '', str(text)).strip()
    
    # Remove currency symbols and extract numeric value
    # Handle negative values with + or - prefix
    numeric_match = re.search(r'[+-]?\d+\.?\d*', cleaned)
    if numeric_match:
        return float(numeric_match.group())
    return 0.0


def scrape_fuel_prices(url, fuel_type, max_retries=3):
    """Scrape prices for a specific fuel type with retry logic"""
    
    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the price table for the specific fuel type
            price_table = soup.find('table', class_=f'price-table {fuel_type}')
            
            if not price_table:
                print(f"Warning: Could not find price table for {fuel_type}")
                return []
            
            prices = []
            tbody = price_table.find('tbody')
            
            if tbody:
                rows = tbody.find_all('tr')
                
                for row in rows:
                    try:
                        # Extract port name
                        port_cell = row.find('th', class_='port')
                        if port_cell:
                            port_link = port_cell.find('a')
                            port_name = port_link.text.strip() if port_link else port_cell.text.strip()
                        else:
                            continue
                        
                        # Extract price data from table cells
                        cells = row.find_all('td')
                        if len(cells) >= 5:
                            # Price (remove any indicator spans)
                            price_cell = cells[0]
                            price_text = price_cell.get_text().strip()
                            price = clean_numeric_value(price_text)
                            
                            # Change
                            change = clean_numeric_value(cells[1].get_text().strip())
                            
                            # High
                            high = clean_numeric_value(cells[2].get_text().strip())
                            
                            # Low  
                            low = clean_numeric_value(cells[3].get_text().strip())
                            
                            # Spread
                            spread = clean_numeric_value(cells[4].get_text().strip())
                            
                            prices.append({
                                'fuel_type': fuel_type,
                                'port': port_name,
                                'price_usd_mt': price,
                                'change': change,
                                'high': high,
                                'low': low,
                                'spread': spread
                            })
                            
                    except (ValueError, AttributeError, IndexError) as e:
                        print(f"Error parsing row for {fuel_type}: {e}")
                        continue
            
            print(f"Successfully scraped {len(prices)} prices for {fuel_type}")
            return prices
            
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {fuel_type}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)  # Wait before retry
            else:
                print(f"Failed to scrape {fuel_type} after {max_retries} attempts")
                return []


def scrape_methanol_prices(soup):
    """Scrape methanol bunker prices from the main page"""
    try:
        methanol_prices = []
        
        # Find the methanol price block
        methanol_block = soup.find('div', id='block_1053')
        if not methanol_block:
            print("Warning: Could not find methanol price block")
            return []
        
        table = methanol_block.find('table', class_='price-table sm')
        if not table:
            print("Warning: Could not find methanol price table")
            return []
            
        tbody = table.find('tbody')
        if tbody:
            rows = tbody.find_all('tr')
            
            for row in rows:
                try:
                    # Get port name
                    port_cell = row.find('th', class_='port')
                    if port_cell:
                        port_link = port_cell.find('a')
                        port_name = port_link.text.strip() if port_link else port_cell.text.strip()
                    else:
                        continue
                    
                    # Get all price cells
                    price_cells = row.find_all('td', class_='price')
                    if len(price_cells) >= 3:
                        # The order might vary, so let's be more specific about extraction
                        # Looking at the HTML structure: Gray Methanol, MEOH-VLSFOe, MEOH-MGOe
                        gray_methanol = clean_numeric_value(price_cells[2].get_text()) if len(price_cells) > 2 else 0.0
                        meoh_vlsfoe = clean_numeric_value(price_cells[0].get_text()) if len(price_cells) > 0 else 0.0  
                        meoh_mgoe = clean_numeric_value(price_cells[1].get_text()) if len(price_cells) > 1 else 0.0
                        
                        methanol_prices.append({
                            'port': port_name,
                            'gray_methanol_usd_mt': gray_methanol,
                            'meoh_vlsfoe_usd_mte': meoh_vlsfoe,
                            'meoh_mgoe_usd_mte': meoh_mgoe
                        })
                        
                except (ValueError, AttributeError, IndexError) as e:
                    print(f"Error parsing methanol row: {e}")
                    continue
        
        print(f"Successfully scraped {len(methanol_prices)} methanol prices")
        return methanol_prices
        
    except Exception as e:
        print(f"Error scraping methanol prices: {e}")
        return []


def scrape_eua_prices(soup):
    """Scrape EUA - EU ETS Compliance Costs from the main page"""
    try:
        eua_prices = []
        
        # Find the EUA price block
        eua_block = soup.find('div', id='block_1070')
        if not eua_block:
            print("Warning: Could not find EUA price block")
            return []
        
        table = eua_block.find('table', class_='price-table sm')
        if not table:
            print("Warning: Could not find EUA price table")
            return []
            
        tbody = table.find('tbody')
        if tbody:
            row = tbody.find('tr')  # Should be only one row for EUA
            if row:
                try:
                    price_cells = row.find_all('td', class_='price')
                    if len(price_cells) >= 5:
                        eua_eur = clean_numeric_value(price_cells[0].get_text())
                        eua_usd = clean_numeric_value(price_cells[1].get_text())
                        eua_vlsfo = clean_numeric_value(price_cells[2].get_text())
                        eua_mgo = clean_numeric_value(price_cells[3].get_text())
                        eua_hfo = clean_numeric_value(price_cells[4].get_text())
                        
                        eua_prices.append({
                            'eua_eur': eua_eur,
                            'eua_usd': eua_usd,
                            'eua_vlsfo_usd_mt': eua_vlsfo,
                            'eua_mgo_usd_mt': eua_mgo,
                            'eua_hfo_usd_mt': eua_hfo
                        })
                        
                except (ValueError, AttributeError, IndexError) as e:
                    print(f"Error parsing EUA row: {e}")
        
        print(f"Successfully scraped {len(eua_prices)} EUA prices")
        return eua_prices
        
    except Exception as e:
        print(f"Error scraping EUA prices: {e}")
        return []


def append_to_csv(filename, data, fieldnames):
    """Append data to CSV file, creating file with headers if it doesn't exist"""
    file_exists = os.path.exists(filename)
    
    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header if file is new
        if not file_exists:
            writer.writeheader()
            print(f"Created new file: {filename}")
        
        # Write data
        for item in data:
            writer.writerow(item)
        
        print(f"Appended {len(data)} rows to {filename}")


def main():
    """Main scraping function"""
    timestamp = get_utc_timestamp()
    print(f"=== Starting Ship & Bunker price scrape at {timestamp} UTC ===")
    
    # URLs and fuel types to scrape
    base_url = "https://shipandbunker.com/prices"
    fuel_types = ['VLSFO', 'MGO', 'IFO380']
    
    # Initialize data containers
    all_fuel_prices = []
    all_methanol_prices = []
    all_eua_prices = []
    
    # First, get the main page for methanol and EUA prices
    print("Fetching main page for methanol and EUA prices...")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(base_url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Scrape methanol and EUA prices
        methanol_prices = scrape_methanol_prices(soup)
        eua_prices = scrape_eua_prices(soup)
        
        # Add timestamp to each record
        for price in methanol_prices:
            price['timestamp'] = timestamp
        all_methanol_prices.extend(methanol_prices)
        
        for price in eua_prices:
            price['timestamp'] = timestamp
        all_eua_prices.extend(eua_prices)
        
    except Exception as e:
        print(f"Error fetching main page: {e}")
    
    # Scrape each fuel type
    for fuel_type in fuel_types:
        url = f"{base_url}#{fuel_type}"
        print(f"\nScraping {fuel_type} prices from {url}")
        
        fuel_prices = scrape_fuel_prices(url, fuel_type)
        
        # Add timestamp to each price record
        for price in fuel_prices:
            price['timestamp'] = timestamp
        
        all_fuel_prices.extend(fuel_prices)
        print(f"Added {len(fuel_prices)} {fuel_type} prices")
    
    # Save all data to master CSV files
    print(f"\n=== Saving data to CSV files ===")
    
    # Fuel prices CSV
    if all_fuel_prices:
        fuel_fieldnames = ['timestamp', 'fuel_type', 'port', 'price_usd_mt', 'change', 'high', 'low', 'spread']
        append_to_csv('master_fuel_prices.csv', all_fuel_prices, fuel_fieldnames)
    else:
        print("No fuel prices to save")
    
    # Methanol prices CSV  
    if all_methanol_prices:
        methanol_fieldnames = ['timestamp', 'port', 'gray_methanol_usd_mt', 'meoh_vlsfoe_usd_mte', 'meoh_mgoe_usd_mte']
        append_to_csv('master_methanol_prices.csv', all_methanol_prices, methanol_fieldnames)
    else:
        print("No methanol prices to save")
    
    # EUA prices CSV
    if all_eua_prices:
        eua_fieldnames = ['timestamp', 'eua_eur', 'eua_usd', 'eua_vlsfo_usd_mt', 'eua_mgo_usd_mt', 'eua_hfo_usd_mt']
        append_to_csv('master_eua_prices.csv', all_eua_prices, eua_fieldnames)
    else:
        print("No EUA prices to save")
    
    # Summary
    total_records = len(all_fuel_prices) + len(all_methanol_prices) + len(all_eua_prices)
    print(f"\n=== Scraping Summary ===")
    print(f"Timestamp: {timestamp}")
    print(f"Fuel prices: {len(all_fuel_prices)} records")
    print(f"Methanol prices: {len(all_methanol_prices)} records") 
    print(f"EUA prices: {len(all_eua_prices)} records")
    print(f"Total records: {total_records}")
    print("=== Scraping completed successfully ===")


if __name__ == "__main__":
    main()
