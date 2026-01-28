"""
Email notification system for voter data updates.
Sends summary emails when new voter data is loaded.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging
import json
from sqlalchemy import text
from config.settings import EMAIL_CONFIG, PROJECT_ROOT
from src.database.connection import get_engine

logger = logging.getLogger(__name__)

SNAPSHOT_FILE = PROJECT_ROOT / "data" / "last_snapshot.json"

def get_current_statistics():
    """
    Query current voter registration statistics.
    Only includes voters with ACTIVE or INACTIVE status.
    
    Returns:
        dict: Current statistics including totals by party, race, gender, and age group
    """
    engine = get_engine()
    
    stats = {}
    
    with engine.connect() as conn:
        # Total registrations (ACTIVE or INACTIVE only)
        result = conn.execute(text("""
            SELECT COUNT(*) as total 
            FROM raw.raw_voters
            WHERE status_cd IN ('A', 'I')
        """))
        stats['total_voters'] = result.scalar()
        
        # By party
        result = conn.execute(text("""
            SELECT party_cd, COUNT(*) as count 
            FROM raw.raw_voters 
            WHERE status_cd IN ('A', 'I')
              AND party_cd IS NOT NULL
            GROUP BY party_cd
            ORDER BY count DESC
        """))
        stats['by_party'] = {row[0]: row[1] for row in result}
        
        # By race
        result = conn.execute(text("""
            SELECT race_code, COUNT(*) as count 
            FROM raw.raw_voters 
            WHERE status_cd IN ('A', 'I')
              AND race_code IS NOT NULL
            GROUP BY race_code
            ORDER BY count DESC
        """))
        stats['by_race'] = {row[0]: row[1] for row in result}
        
        # By gender
        result = conn.execute(text("""
            SELECT gender_code, COUNT(*) as count 
            FROM raw.raw_voters 
            WHERE status_cd IN ('A', 'I')
              AND gender_code IS NOT NULL
            GROUP BY gender_code
            ORDER BY count DESC
        """))
        stats['by_gender'] = {row[0]: row[1] for row in result}
        
        # By age group (uses age_group column)
        result = conn.execute(text("""
            SELECT age_group, COUNT(*) as count
            FROM raw.raw_voters
            WHERE status_cd IN ('A', 'I')
              AND age_group IS NOT NULL
            GROUP BY age_group
            ORDER BY 
                CASE age_group
                    WHEN '18-25' THEN 1
                    WHEN '26-35' THEN 2
                    WHEN '36-50' THEN 3
                    WHEN '51-65' THEN 4
                    WHEN '65+' THEN 5
                    ELSE 6
                END
        """))
        stats['by_age_group'] = {row[0]: row[1] for row in result}
        
        # By county (top 10)
        result = conn.execute(text("""
            SELECT county_desc, COUNT(*) as count 
            FROM raw.raw_voters 
            WHERE status_cd IN ('A', 'I')
            GROUP BY county_desc
            ORDER BY count DESC
            LIMIT 10
        """))
        stats['top_counties'] = {row[0]: row[1] for row in result}
    
    return stats

def load_previous_snapshot():
    """
    Load the previous statistics snapshot.
    
    Returns:
        dict: Previous statistics or None if no snapshot exists
    """
    if not SNAPSHOT_FILE.exists():
        logger.info("No previous snapshot found")
        return None
    
    try:
        with open(SNAPSHOT_FILE, 'r') as f:
            snapshot = json.load(f)
        logger.info("Loaded previous snapshot")
        return snapshot
    except Exception as e:
        logger.error(f"Failed to load snapshot: {e}")
        return None

def save_snapshot(stats):
    """
    Save current statistics as snapshot.
    
    Args:
        stats: Statistics dictionary to save
    """
    try:
        SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(SNAPSHOT_FILE, 'w') as f:
            json.dump({
                'stats': stats,
                'timestamp': datetime.utcnow().isoformat()
            }, f, indent=2)
        logger.info("Snapshot saved")
    except Exception as e:
        logger.error(f"Failed to save snapshot: {e}")

def calculate_changes(current, previous):
    """
    Calculate changes between current and previous statistics.
    
    Args:
        current: Current statistics
        previous: Previous statistics
        
    Returns:
        dict: Dictionary of changes
    """
    if previous is None:
        return {
            'is_first_run': True,
            'total_change': current['total_voters']
        }
    
    prev_stats = previous['stats']
    
    changes = {
        'is_first_run': False,
        'total_change': current['total_voters'] - prev_stats['total_voters'],
        'party_changes': {},
        'race_changes': {},
        'gender_changes': {},
        'age_group_changes': {},
        'county_changes': {}
    }
    
    # Party changes
    for party, count in current['by_party'].items():
        prev_count = prev_stats['by_party'].get(party, 0)
        changes['party_changes'][party] = count - prev_count
    
    # Race changes
    for race, count in current['by_race'].items():
        prev_count = prev_stats['by_race'].get(race, 0)
        changes['race_changes'][race] = count - prev_count
    
    # Gender changes
    for gender, count in current['by_gender'].items():
        prev_count = prev_stats['by_gender'].get(gender, 0)
        changes['gender_changes'][gender] = count - prev_count
    
    # Age group changes
    for age_group, count in current['by_age_group'].items():
        prev_count = prev_stats['by_age_group'].get(age_group, 0)
        changes['age_group_changes'][age_group] = count - prev_count
    
    # County changes (top counties)
    for county, count in current['top_counties'].items():
        prev_count = prev_stats['top_counties'].get(county, 0)
        changes['county_changes'][county] = count - prev_count
    
    return changes

def format_email_body(current_stats, changes):
    """
    Format the email body with statistics and changes.
    
    Args:
        current_stats: Current statistics
        changes: Calculated changes
        
    Returns:
        str: Formatted HTML email body
    """
    if changes['is_first_run']:
        body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333; max-width: 800px;">
            <h2 style="color: #001f3f;">NC Votes - Initial Data Load</h2>
            <p>The NC Votes database has been initialized with voter registration data (ACTIVE and INACTIVE voters only).</p>
            
            <h3>Overview</h3>
            <p style="font-size: 20px;"><strong>Total Registered Voters:</strong> {current_stats['total_voters']:,}</p>
            
            <div style="display: inline-block; width: 100%;">
                <div style="float: left; width: 48%; margin-right: 2%;">
                    <h3>By Party</h3>
                    <table style="border-collapse: collapse; width: 100%;">
                        <tr style="background-color: #f4f4f4;">
                            <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Party</th>
                            <th style="padding: 8px; text-align: right; border: 1px solid #ddd;">Count</th>
                        </tr>
        """
        for party, count in sorted(current_stats['by_party'].items(), 
                                   key=lambda x: x[1], reverse=True):
            body += f"""
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;">{party}</td>
                            <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{count:,}</td>
                        </tr>
            """
        
        body += """
                    </table>
                </div>
                
                <div style="float: right; width: 48%;">
                    <h3>By Race</h3>
                    <table style="border-collapse: collapse; width: 100%;">
                        <tr style="background-color: #f4f4f4;">
                            <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Race</th>
                            <th style="padding: 8px; text-align: right; border: 1px solid #ddd;">Count</th>
                        </tr>
        """
        
        for race, count in sorted(current_stats['by_race'].items(), 
                                  key=lambda x: x[1], reverse=True):
            body += f"""
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;">{race}</td>
                            <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{count:,}</td>
                        </tr>
            """
        
        body += """
                    </table>
                </div>
            </div>
            
            <div style="clear: both; margin-top: 30px;"></div>
            
            <div style="display: inline-block; width: 100%;">
                <div style="float: left; width: 48%; margin-right: 2%;">
                    <h3>By Gender</h3>
                    <table style="border-collapse: collapse; width: 100%;">
                        <tr style="background-color: #f4f4f4;">
                            <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Gender</th>
                            <th style="padding: 8px; text-align: right; border: 1px solid #ddd;">Count</th>
                        </tr>
        """
        
        for gender, count in sorted(current_stats['by_gender'].items(), 
                                    key=lambda x: x[1], reverse=True):
            body += f"""
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;">{gender}</td>
                            <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{count:,}</td>
                        </tr>
            """
        
        body += """
                    </table>
                </div>
                
                <div style="float: right; width: 48%;">
                    <h3>By Age Group</h3>
                    <table style="border-collapse: collapse; width: 100%;">
                        <tr style="background-color: #f4f4f4;">
                            <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Age Group</th>
                            <th style="padding: 8px; text-align: right; border: 1px solid #ddd;">Count</th>
                        </tr>
        """
        
        # Define age group order
        age_order = ['18-25', '26-35', '36-50', '51-65', '65+', 'Unknown']
        for age_group in age_order:
            if age_group in current_stats['by_age_group']:
                count = current_stats['by_age_group'][age_group]
                body += f"""
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;">{age_group}</td>
                            <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{count:,}</td>
                        </tr>
                """
        
        body += f"""
                    </table>
                </div>
            </div>
            
            <div style="clear: both; margin-top: 30px;"></div>
            
            <p style="margin-top: 20px; color: #666; font-size: 14px;">
                Data snapshot saved to: data/last_snapshot.json<br>
                Future emails will show changes since this baseline.
            </p>
        </body>
        </html>
        """
    else:
        total_change = changes['total_change']
        change_sign = "+" if total_change >= 0 else ""
        change_color = "#109618" if total_change >= 0 else "#dc3912"
        
        body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333; max-width: 900px;">
            <h2 style="color: #001f3f;">NC Votes - Data Update</h2>
            <p>New voter registration data has been loaded (ACTIVE and INACTIVE voters only).</p>
            
            <h3>Overall Change</h3>
            <p style="font-size: 24px; margin: 10px 0;">
                <strong style="color: {change_color};">{change_sign}{total_change:,}</strong> 
                total registrations
            </p>
            <p><strong>Current Total:</strong> {current_stats['total_voters']:,}</p>
            
            <h3>Changes by Party</h3>
            <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
                <tr style="background-color: #f4f4f4;">
                    <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Party</th>
                    <th style="padding: 8px; text-align: right; border: 1px solid #ddd;">Change</th>
                    <th style="padding: 8px; text-align: right; border: 1px solid #ddd;">Total</th>
                </tr>
        """
        
        for party in sorted(changes['party_changes'].keys()):
            change = changes['party_changes'][party]
            total = current_stats['by_party'].get(party, 0)
            change_sign = "+" if change >= 0 else ""
            row_color = "#e8f5e9" if change > 0 else "#ffebee" if change < 0 else "#fff"
            
            body += f"""
                <tr style="background-color: {row_color};">
                    <td style="padding: 8px; border: 1px solid #ddd;">{party}</td>
                    <td style="padding: 8px; text-align: right; border: 1px solid #ddd; font-weight: bold;">
                        {change_sign}{change:,}
                    </td>
                    <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{total:,}</td>
                </tr>
            """
        
        body += """
            </table>
            
            <h3 style="margin-top: 30px;">Changes by Race</h3>
            <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
                <tr style="background-color: #f4f4f4;">
                    <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Race</th>
                    <th style="padding: 8px; text-align: right; border: 1px solid #ddd;">Change</th>
                    <th style="padding: 8px; text-align: right; border: 1px solid #ddd;">Total</th>
                </tr>
        """
        
        for race in sorted(changes['race_changes'].keys(), 
                          key=lambda x: abs(changes['race_changes'][x]), reverse=True):
            change = changes['race_changes'][race]
            total = current_stats['by_race'].get(race, 0)
            change_sign = "+" if change >= 0 else ""
            row_color = "#e8f5e9" if change > 0 else "#ffebee" if change < 0 else "#fff"
            
            body += f"""
                <tr style="background-color: {row_color};">
                    <td style="padding: 8px; border: 1px solid #ddd;">{race}</td>
                    <td style="padding: 8px; text-align: right; border: 1px solid #ddd; font-weight: bold;">
                        {change_sign}{change:,}
                    </td>
                    <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{total:,}</td>
                </tr>
            """
        
        body += """
            </table>
            
            <h3 style="margin-top: 30px;">Changes by Gender</h3>
            <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
                <tr style="background-color: #f4f4f4;">
                    <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Gender</th>
                    <th style="padding: 8px; text-align: right; border: 1px solid #ddd;">Change</th>
                    <th style="padding: 8px; text-align: right; border: 1px solid #ddd;">Total</th>
                </tr>
        """
        
        for gender in sorted(changes['gender_changes'].keys()):
            change = changes['gender_changes'][gender]
            total = current_stats['by_gender'].get(gender, 0)
            change_sign = "+" if change >= 0 else ""
            row_color = "#e8f5e9" if change > 0 else "#ffebee" if change < 0 else "#fff"
            
            body += f"""
                <tr style="background-color: {row_color};">
                    <td style="padding: 8px; border: 1px solid #ddd;">{gender}</td>
                    <td style="padding: 8px; text-align: right; border: 1px solid #ddd; font-weight: bold;">
                        {change_sign}{change:,}
                    </td>
                    <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{total:,}</td>
                </tr>
            """
        
        body += """
            </table>
            
            <h3 style="margin-top: 30px;">Changes by Age Group</h3>
            <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
                <tr style="background-color: #f4f4f4;">
                    <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Age Group</th>
                    <th style="padding: 8px; text-align: right; border: 1px solid #ddd;">Change</th>
                    <th style="padding: 8px; text-align: right; border: 1px solid #ddd;">Total</th>
                </tr>
        """
        
        # Define age group order
        age_order = ['18-25', '26-35', '36-50', '51-65', '65+', 'Unknown']
        for age_group in age_order:
            if age_group in changes['age_group_changes']:
                change = changes['age_group_changes'][age_group]
                total = current_stats['by_age_group'].get(age_group, 0)
                change_sign = "+" if change >= 0 else ""
                row_color = "#e8f5e9" if change > 0 else "#ffebee" if change < 0 else "#fff"
                
                body += f"""
                <tr style="background-color: {row_color};">
                    <td style="padding: 8px; border: 1px solid #ddd;">{age_group}</td>
                    <td style="padding: 8px; text-align: right; border: 1px solid #ddd; font-weight: bold;">
                        {change_sign}{change:,}
                    </td>
                    <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{total:,}</td>
                </tr>
                """
        
        body += """
            </table>
            
            <h3 style="margin-top: 30px;">Top Counties by Change</h3>
            <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
                <tr style="background-color: #f4f4f4;">
                    <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">County</th>
                    <th style="padding: 8px; text-align: right; border: 1px solid #ddd;">Change</th>
                    <th style="padding: 8px; text-align: right; border: 1px solid #ddd;">Total</th>
                </tr>
        """
        
        # Sort counties by absolute change
        sorted_counties = sorted(changes['county_changes'].items(), 
                                key=lambda x: abs(x[1]), reverse=True)[:5]
        
        for county, change in sorted_counties:
            total = current_stats['top_counties'].get(county, 0)
            change_sign = "+" if change >= 0 else ""
            row_color = "#e8f5e9" if change > 0 else "#ffebee" if change < 0 else "#fff"
            
            body += f"""
                <tr style="background-color: {row_color};">
                    <td style="padding: 8px; border: 1px solid #ddd;">{county}</td>
                    <td style="padding: 8px; text-align: right; border: 1px solid #ddd; font-weight: bold;">
                        {change_sign}{change:,}
                    </td>
                    <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{total:,}</td>
                </tr>
            """
        
        body += f"""
            </table>
            
            <p style="margin-top: 30px; color: #666; font-size: 14px;">
                Data updated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}<br>
                Snapshot saved to: data/last_snapshot.json
            </p>
        </body>
        </html>
        """
    
    return body

def send_update_email():
    """
    Main function to send voter data update email.
    Calculates changes and sends formatted email.
    
    Returns:
        bool: True if email sent successfully, False otherwise
    """
    try:
        # Check if email is configured
        if not EMAIL_CONFIG.get('enabled', False):
            logger.info("Email notifications are disabled")
            return False
        
        if not EMAIL_CONFIG.get('smtp_user') or not EMAIL_CONFIG.get('smtp_password'):
            logger.warning("Email credentials not configured")
            return False
        
        # Get current statistics
        logger.info("Gathering current statistics...")
        current_stats = get_current_statistics()
        
        # Load previous snapshot
        previous_snapshot = load_previous_snapshot()
        
        # Calculate changes
        changes = calculate_changes(current_stats, previous_snapshot)
        
        # Format email
        subject = "NC Votes - " + ("Initial Data Load" if changes['is_first_run'] 
                                   else f"Data Update ({changes['total_change']:+,} registrations)")
        body = format_email_body(current_stats, changes)
        
        # Send email
        logger.info("Sending email notification...")
        send_email(
            to_email=EMAIL_CONFIG['to_email'],
            subject=subject,
            body=body
        )
        
        # Save current stats as new snapshot
        save_snapshot(current_stats)
        
        logger.info("Email notification sent successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send update email: {e}", exc_info=True)
        return False

def send_email(to_email, subject, body):
    """
    Send an email using Gmail SMTP.
    
    Args:
        to_email: Recipient email address
        subject: Email subject
        body: HTML email body
    """
    msg = MIMEMultipart('alternative')
    msg['From'] = EMAIL_CONFIG['smtp_user']
    msg['To'] = to_email
    msg['Subject'] = subject
    
    # Attach HTML body
    html_part = MIMEText(body, 'html')
    msg.attach(html_part)
    
    # Send via Gmail SMTP
    with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
        server.starttls()
        server.login(EMAIL_CONFIG['smtp_user'], EMAIL_CONFIG['smtp_password'])
        server.send_message(msg)
        logger.info(f"Email sent to {to_email}")

def main():
    """Entry point for command-line testing."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    success = send_update_email()
    if success:
        print("Email sent successfully!")
    else:
        print("Failed to send email. Check logs for details.")
        exit(1)

if __name__ == "__main__":
    main()