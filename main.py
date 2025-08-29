import asyncio
import random
import time
import os
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich import box

# Brightdata credentials
AUTH = "brd-customer-hl_a3093bff-zone-scraping_browser1-country-ca:3yzkexu9yb7l"
SBR_WS_CDP = f'wss://{AUTH}@brd.superproxy.io:9222'

# Information
# Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): 2025-08-28 19:00:43
# Current User's Login: aamanbhagat

# Global statistics and progress tracking
stats = {
    'total_urls_processed': 0,
    'instances': {}
}

# Console for rich output
console = Console()

# Custom exception for element not found
class ElementNotFoundException(Exception):
    pass

# Read random URL from file
def get_random_url():
    with open("/workspaces/codespaces-blank/urls.txt", "r") as file:
        urls = file.readlines()
    return random.choice(urls).strip()

# Update progress display
def update_progress_display():
    table = Table(title="URL Processing Progress", box=box.ROUNDED)
    
    # Add columns
    table.add_column("Instance", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Current URL", style="blue")
    table.add_column("Current Step", style="magenta")
    table.add_column("URLs Processed", style="yellow")
    table.add_column("Runtime", style="red")
    
    # Add instance rows
    for instance_id, instance_data in sorted(stats['instances'].items()):
        status = instance_data.get('status', 'Unknown')
        status_style = "green" if status == "Running" else "red"
        
        table.add_row(
            f"#{instance_id}",
            f"[{status_style}]{status}[/{status_style}]",
            instance_data.get('current_url', 'N/A'),
            f"Step {instance_data.get('current_step', 'N/A')}",
            str(instance_data.get('urls_processed', 0)),
            f"{int(time.time() - instance_data.get('start_time', time.time()))}s"
        )
    
    # Add total row
    table.add_row(
        "[bold]TOTAL[/bold]", 
        "", 
        "", 
        "", 
        f"[bold]{stats['total_urls_processed']}[/bold]",
        "",
        end_section=True
    )
    
    return table

# Robust element finder and clicker
async def find_and_click(page, css_selector, xpath_selector, step_num, instance_id, retry_count=600):
    for attempt in range(retry_count):
        try:
            # Try multiple methods to find and click the element
            element = None
            
            # Method 1: Try CSS selector with visibility check
            element = await page.query_selector(css_selector)
            if element and await element.is_visible():
                stats['instances'][instance_id]['log'] = f"Found element using CSS selector: {css_selector}"
                
                # Try different click methods
                try:
                    await element.click(force=True, timeout=1000)
                    stats['instances'][instance_id]['log'] = f"Clicked element using force click"
                except Exception:
                    try:
                        await page.evaluate(f"document.querySelector('{css_selector}').click();")
                        stats['instances'][instance_id]['log'] = f"Clicked element using JavaScript"
                    except Exception:
                        try:
                            # Get element position and click center
                            box = await element.bounding_box()
                            if box:
                                await page.mouse.click(box["x"] + box["width"]/2, box["y"] + box["height"]/2)
                                stats['instances'][instance_id]['log'] = f"Clicked element using mouse coordinates"
                        except Exception as e:
                            stats['instances'][instance_id]['log'] = f"All click methods failed: {e}"
                            continue
                
                return True
            
            # Method 2: Try XPath selector
            if not element or not await element.is_visible():
                element = await page.query_selector(f"xpath={xpath_selector}")
                if element and await element.is_visible():
                    stats['instances'][instance_id]['log'] = f"Found element using XPath selector: {xpath_selector}"
                    
                    try:
                        await element.click(force=True, timeout=1000)
                        stats['instances'][instance_id]['log'] = f"Clicked element using force click"
                    except Exception:
                        try:
                            await page.evaluate(f"document.evaluate('{xpath_selector}', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.click();")
                            stats['instances'][instance_id]['log'] = f"Clicked element using JavaScript with XPath"
                        except Exception:
                            try:
                                # Get element position and click center
                                box = await element.bounding_box()
                                if box:
                                    await page.mouse.click(box["x"] + box["width"]/2, box["y"] + box["height"]/2)
                                    stats['instances'][instance_id]['log'] = f"Clicked element using mouse coordinates"
                            except Exception as e:
                                stats['instances'][instance_id]['log'] = f"All click methods failed: {e}"
                                continue
                    
                    return True
            
            # Wait a bit before retrying
            await asyncio.sleep(0.1)
            
            # Check if we need to handle a Google vignette ad
            await handle_google_vignette(page, instance_id)
            
            # Check if we've reached the timeout (60 seconds)
            if attempt > 0 and attempt % 600 == 0:  # 600 * 0.1 = 60 seconds
                stats['instances'][instance_id]['log'] = f"Element not found after 60 seconds, relaunching instance..."
                # Throw a specific exception to trigger instance restart
                raise ElementNotFoundException(f"Element {css_selector} not found after 60 seconds")
            
        except ElementNotFoundException:
            # Re-raise to be caught by the caller
            raise
        except Exception as e:
            stats['instances'][instance_id]['log'] = f"Attempt {attempt+1} failed: {str(e)}"
            await asyncio.sleep(0.1)
    
    # If we reach here, we've exhausted all retries
    stats['instances'][instance_id]['log'] = f"Failed to find and click element after {retry_count} attempts"
    raise ElementNotFoundException(f"Element {css_selector} not found after all attempts")

# Handle Google vignette ads
async def handle_google_vignette(page, instance_id):
    try:
        # Look for Google vignette ad using different selectors
        vignette_selectors = [
            "#google_vignette", 
            "div[id*='google_vignette']",
            "iframe[id*='google_ads']"
        ]
        
        for selector in vignette_selectors:
            vignette = await page.query_selector(selector)
            if vignette and await vignette.is_visible():
                # Try different methods to close the ad
                close_selectors = [
                    "div.close-button", 
                    "button.close", 
                    "div.dismiss-button",
                    "[aria-label='Close']",
                    ".close-ad-button"
                ]
                
                for close_selector in close_selectors:
                    try:
                        close_button = await page.query_selector(close_selector)
                        if close_button and await close_button.is_visible():
                            await close_button.click(force=True)
                            stats['instances'][instance_id]['log'] = f"Closed ad using selector: {close_selector}"
                            return True
                    except Exception:
                        continue
                
                # If no close button found, try to click outside the ad
                try:
                    await page.mouse.click(10, 10)
                    return True
                except Exception:
                    pass
                
                # Try to dismiss using JavaScript
                try:
                    await page.evaluate("""() => {
                        const adElements = document.querySelectorAll('#google_vignette, div[id*="google_vignette"], iframe[id*="google_ads"]');
                        for(const ad of adElements) {
                            ad.remove();
                        }
                    }""")
                    stats['instances'][instance_id]['log'] = "Removed ad using JavaScript"
                    return True
                except Exception:
                    pass
    except Exception as e:
        stats['instances'][instance_id]['log'] = f"Error handling Google vignette: {str(e)}"
    
    return False

# Process a single URL
async def process_url(instance_id, playwright):
    # Initialize or reset instance stats
    if instance_id not in stats['instances']:
        stats['instances'][instance_id] = {
            'start_time': time.time(),
            'urls_processed': 0,
            'status': 'Initializing',
            'current_url': 'N/A',
            'current_step': 'N/A',
            'log': 'Starting instance'
        }
    
    browser = None
    
    try:
        stats['instances'][instance_id]['status'] = 'Connecting'
        browser = await playwright.chromium.connect_over_cdp(SBR_WS_CDP)
        context = await browser.new_context()
        page = await context.new_page()
        
        # Set default timeout to 8 seconds
        page.set_default_timeout(8000)
        
        # Get random URL from file
        url = get_random_url()
        stats['instances'][instance_id]['current_url'] = url
        stats['instances'][instance_id]['status'] = 'Running'
        stats['instances'][instance_id]['log'] = f'Navigating to {url}'
        
        try:
            stats['instances'][instance_id]['current_step'] = 'Page Load'
            await page.goto(url, wait_until="domcontentloaded", timeout=8000)
        except TimeoutError:
            stats['instances'][instance_id]['log'] = "Page load timed out after 8 seconds, continuing anyway..."
        except Exception as e:
            stats['instances'][instance_id]['log'] = f"Error during navigation: {str(e)}, continuing anyway..."
        
        # Wait for network to be idle (or timeout after 8 seconds)
        try:
            await page.wait_for_load_state("networkidle", timeout=8000)
        except TimeoutError:
            stats['instances'][instance_id]['log'] = "Network idle timeout after 8 seconds, continuing anyway..."
        except Exception as e:
            stats['instances'][instance_id]['log'] = f"Error waiting for network idle: {str(e)}, continuing anyway..."
        
        # Step 1: Click on div.start_btn
        stats['instances'][instance_id]['current_step'] = '1'
        await find_and_click(page, "div.start_btn", "//BODY/MAIN[1]/DIV[1]/DIV[3]", 1, instance_id)
        await asyncio.sleep(1)
        
        # Step 2: Click on div.btn:nth-child(1)
        stats['instances'][instance_id]['current_step'] = '2'
        await find_and_click(page, "div.btn:nth-child(1)", "//BODY/MAIN[1]/DIV[3]/DIV[1]", 2, instance_id)
        await asyncio.sleep(1)
        
        # Step 3: Click on a.btn:nth-child(1)
        stats['instances'][instance_id]['current_step'] = '3'
        await find_and_click(page, "a.btn:nth-child(1)", "//BODY/MAIN[1]/DIV[3]/A[1]", 3, instance_id)
        await asyncio.sleep(2)  # Wait for potential ad and redirect
        
        # Check for Google vignette ad
        await handle_google_vignette(page, instance_id)
        
        # Wait for new page to load (with timeout)
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=8000)
        except TimeoutError:
            stats['instances'][instance_id]['log'] = "Page load timed out after 8 seconds, continuing anyway..."
        except Exception as e:
            stats['instances'][instance_id]['log'] = f"Error waiting for page load: {str(e)}, continuing anyway..."
        
        # Step 4: Click on div.start_btn on new page
        stats['instances'][instance_id]['current_step'] = '4'
        await find_and_click(page, "div.start_btn", "//BODY/MAIN[1]/DIV[1]/DIV[3]", 4, instance_id)
        await asyncio.sleep(1)
        
        # Step 5: Click on div.btn:nth-child(1)
        stats['instances'][instance_id]['current_step'] = '5'
        await find_and_click(page, "div.btn:nth-child(1)", "//BODY/MAIN[1]/DIV[3]/DIV[1]", 5, instance_id)
        await asyncio.sleep(1)
        
        # Step 6: Click on a.btn:nth-child(1)
        stats['instances'][instance_id]['current_step'] = '6'
        await find_and_click(page, "a.btn:nth-child(1)", "//BODY/MAIN[1]/DIV[3]/A[1]", 6, instance_id)
        await asyncio.sleep(2)  # Wait for potential ad and redirect
        
        # Check for Google vignette ad
        await handle_google_vignette(page, instance_id)
        
        # Wait for new page to load (with timeout)
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=8000)
        except TimeoutError:
            stats['instances'][instance_id]['log'] = "Page load timed out after 8 seconds, continuing anyway..."
        except Exception as e:
            stats['instances'][instance_id]['log'] = f"Error waiting for page load: {str(e)}, continuing anyway..."
        
        # Step 7: Click on div.start_btn on new page
        stats['instances'][instance_id]['current_step'] = '7'
        await find_and_click(page, "div.start_btn", "//BODY/MAIN[1]/DIV[1]/DIV[3]", 7, instance_id)
        await asyncio.sleep(1)
        
        # Step 8: Click on div.btn:nth-child(2)
        stats['instances'][instance_id]['current_step'] = '8'
        await find_and_click(page, "div.btn:nth-child(2)", "//BODY/MAIN[1]/DIV[3]/DIV[2]", 8, instance_id)
        await asyncio.sleep(1)
        
        # Step 9: Click on a.btn:nth-child(2)
        stats['instances'][instance_id]['current_step'] = '9'
        await find_and_click(page, "a.btn:nth-child(2)", "//BODY/MAIN[1]/DIV[3]/A[1]", 9, instance_id)
        await asyncio.sleep(2)  # Wait for potential ad and redirect
        
        # Check for Google vignette ad
        await handle_google_vignette(page, instance_id)
        
        # Wait for new page to load (with timeout)
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=8000)
        except TimeoutError:
            stats['instances'][instance_id]['log'] = "Page load timed out after 8 seconds, continuing anyway..."
        except Exception as e:
            stats['instances'][instance_id]['log'] = f"Error waiting for page load: {str(e)}, continuing anyway..."
        
        # Step 10: Wait 9 seconds and then click on a.get-link
        stats['instances'][instance_id]['current_step'] = '10'
        stats['instances'][instance_id]['log'] = "Waiting 9 seconds before final step..."
        await asyncio.sleep(9)
        await find_and_click(page, "a.get-link", "//BODY/DIV[1]/DIV[1]/DIV[3]/A[1]", 10, instance_id)
        
        # Step 11: Wait 2 seconds and then close the browser
        stats['instances'][instance_id]['current_step'] = '11'
        stats['instances'][instance_id]['log'] = "Final step completed! Waiting 2 seconds before closing..."
        await asyncio.sleep(2)
        
        # Update statistics
        stats['instances'][instance_id]['urls_processed'] += 1
        stats['total_urls_processed'] += 1
        stats['instances'][instance_id]['log'] = "URL processed successfully"
        
        return True
        
    except ElementNotFoundException as e:
        # This is expected when element not found after 60 seconds
        stats['instances'][instance_id]['status'] = 'Relaunching'
        stats['instances'][instance_id]['log'] = f"Relaunching instance due to: {str(e)}"
        return False
    except Exception as e:
        stats['instances'][instance_id]['status'] = 'Error'
        stats['instances'][instance_id]['log'] = f"Error: {str(e)}"
        return False
    finally:
        if browser:
            try:
                await browser.close()
                stats['instances'][instance_id]['log'] = "Browser closed"
            except Exception as e:
                stats['instances'][instance_id]['log'] = f"Error closing browser: {str(e)}"

# Instance runner - runs continuously processing URLs
async def instance_runner(instance_id, playwright):
    while True:
        try:
            result = await process_url(instance_id, playwright)
            if not result:
                # If process_url returns False, it means we need to relaunch with a new URL
                stats['instances'][instance_id]['log'] = "Relaunching with new URL..."
            else:
                # Normal completion, small delay before starting next URL
                await asyncio.sleep(1)
        except Exception as e:
            # Just log the error and continue with next URL
            if instance_id in stats['instances']:
                stats['instances'][instance_id]['status'] = 'Error'
                stats['instances'][instance_id]['log'] = f"Unexpected error: {str(e)}"
            
            # Short delay before retrying
            await asyncio.sleep(1)

# Update display task
async def update_display():
    with Live(update_progress_display(), refresh_per_second=1) as live:
        while True:
            live.update(update_progress_display())
            await asyncio.sleep(0.5)

# Main function
async def main():
    # Clear screen
    os.system('cls' if os.name == 'nt' else 'clear')
    
    # Display header
    console.print(f"[bold green]URL Processing Script[/bold green]", justify="center")
    console.print(f"Current Date and Time (UTC): [cyan]{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}[/cyan]", justify="center")
    console.print(f"Current User's Login: [yellow]aamanbhagat[/yellow]", justify="center")
    console.print()
    
    # Ask for number of instances
    num_instances = 0
    while num_instances <= 0:
        try:
            num_instances = int(console.input("[bold yellow]How many instances would you like to run? [/bold yellow]"))
            if num_instances <= 0:
                console.print("[bold red]Please enter a positive number.[/bold red]")
        except ValueError:
            console.print("[bold red]Please enter a valid number.[/bold red]")
    
    console.print(f"[bold green]Starting {num_instances} instances...[/bold green]")
    console.print(f"[bold blue]Note: Instances will automatically relaunch if an element isn't found within 60 seconds[/bold blue]")
    
    # Start display update task
    display_task = asyncio.create_task(update_display())
    
    # Run instances
    async with async_playwright() as playwright:
        # Create tasks for each instance
        instance_tasks = [
            instance_runner(i+1, playwright) for i in range(num_instances)
        ]
        
        # Wait for all tasks (which will run indefinitely)
        await asyncio.gather(
            *instance_tasks,
            return_exceptions=True
        )
    
    # Cancel the display task if we somehow exit the infinite loop
    display_task.cancel()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("[bold red]Script terminated by user.[/bold red]")
    except Exception as e:
        console.print(f"[bold red]Unexpected error: {str(e)}[/bold red]")
