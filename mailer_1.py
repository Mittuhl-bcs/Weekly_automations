     
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import json
import os
from datetime import datetime
import zipfile
import shutil  # Import shutil to create a zip file


def send_email(attachment_filename, attachment_display_name, orders_df, quote_df):
    # Get the credentials stored:
    with open("D:\\Item_replenishment_report_automation\\Credentials.json", "r+") as crednt:
        data = json.load(crednt)
        password = data["password"]

    try:
        # Credentials for usage
        sender_email = "Bcs.notifications@building-controls.com"  # Outlook email address
        sender_password = password  
        receiver_emails = ["mithul.murugaadev@building-controls.com"]  # List of recipient email addresses , "brian.ackerman@building-controls.com"
        subject = 'Wednesday Automated reports'

        # Set up the MIME
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = ', '.join(receiver_emails)
        message['Subject'] = subject

        # Attach the body with HTML content
        body = f"""
            <html>
            <head>
                <style>
                    /* General styling for the newsletter container */
                    body {{
                        font-family: Arial, sans-serif;
                        margin: 0;
                        padding: 0;
                        background-color: #f4f4f4;
                    }}

                    .newsletter-container {{
                        width: 100%;
                        max-width: 600px;
                        margin: 0 auto;
                        background-color: #ffffff;
                        padding: 20px;
                        box-shadow: 0 4px 10px rgba(0, 0, 0, 0.1);
                        border-radius: 8px;
                    }}

                    h1, h2 {{
                        color: #333333;
                        font-size: 24px;
                        font-weight: bold;
                        margin-bottom: 10px;
                    }}

                    p {{
                        color: #555555;
                        font-size: 16px;
                        line-height: 1.5;
                        margin-bottom: 20px;
                    }}

                    .table-container {{
                        width: 100%;
                        overflow-x: auto;  /* Enables horizontal scroll if the table is too wide */
                        margin-bottom: 20px;
                    }}

                    table {{
                        width: 100%;
                        max-width: 100%;  /* Ensures the table fits within the container */
                        border-collapse: collapse;
                        margin-top: 10px;
                        font-size: 12px;  /* Decreased font size for the table */
                    }}

                    th, td {{
                        padding: 8px;  /* Reduced padding for a more compact table */
                        text-align: left;
                        border: 1px solid #ddd;
                        word-wrap: break-word;  /* Wraps text inside table cells */
                        white-space: normal;   /* Allows content to break and wrap properly */
                    }}

                    th {{
                        background-color: #f2f2f2;
                        color: #333333;
                    }}

                    tr:nth-child(even) {{
                        background-color: #fafafa;
                    }}

                    tr:hover {{
                        background-color: #f1f1f1;
                    }}

                    .footer {{
                        margin-top: 30px;
                        text-align: center;
                        font-size: 14px;
                        color: #888888;
                    }}

                    .footer a {{
                        color: #007bff;
                        text-decoration: none;
                    }}

                    .footer a:hover {{
                        text-decoration: underline;
                    }}

                    /* Responsive design */
                    @media (max-width: 600px) {{
                        .newsletter-container {{
                            padding: 15px;
                        }}

                        table {{
                            font-size: 12px;  /* Font size adjustment for smaller screens */
                        }}

                        th, td {{
                            padding: 6px;  /* Adjust padding on mobile devices */
                        }}

                        .table-container {{
                            overflow-x: auto;  /* Horizontal scroll for large tables */
                        }}
                    }}
                </style>
            </head>
            <body>
                <div class="newsletter-container">
                    <h1>Wednesday Automated Reports</h1>
                    <p>Hi Team,</p>
                    <p>A sample report data is generated and shared through this automated mail. Please find the Excel file attached for further review.</p>

                    <!-- Orders Table -->
                    <h3>Orders Table</h3>
                    <div class="table-container">
                        <p>The below given is the transfer table:</p>
                        <table>
                            {orders_df}
                        </table>
                    </div>
                    <br>

                    <!-- Quote Table -->
                    <h3>Quote Table</h3>
                    <div class="table-container">
                        <p>The below given is the customer table:</p>
                        <table>
                            {quote_df}
                        </table>
                    </div>
                    <br>

                    <p>Regards,</p>
                    <p>Mithul</p>

                    <div class="footer">
                        <p>&copy; Building Controls. All rights reserved.</p>
                        <p><a href="mailto:bcs.notifications@building-controls.com">Contact Us</a></p>
                    </div>
                </div>
            </body>
            </html>
"""

        message.attach(MIMEText(body, 'html'))

        # Open the zip file to be sent
        with open(attachment_filename, 'rb') as attachment:
            # Add file as application/zip for .zip files
            part = MIMEBase('application', 'zip')
            part.set_payload(attachment.read())

        # Encode file in ASCII characters to send by email
        encoders.encode_base64(part)

        # Add header as key/value pair to attachment part
        part.add_header('Content-Disposition', f'attachment; filename="{attachment_display_name}"')

        # Add attachment to message and convert message to string
        message.attach(part)
        text = message.as_string()

        # Log in to SMTP server (for Outlook)
        server = smtplib.SMTP('smtp.office365.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)

        # Send email
        server.sendmail(sender_email, receiver_emails, text)

        # Close the SMTP server
        server.quit()

        return True

    except Exception as e:
        raise ValueError(f'Failed to send email: {e}')


def create_zip_from_folders(folder_path_1, folder_path_2, zip_filename):
    # Create a zip file that will contain both folders
    with zipfile.ZipFile(f'{zip_filename}.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add all files from the first folder
        for root, dirs, files in os.walk(folder_path_1):
            for file in files:
                file_path = os.path.join(root, file)
                # Add file to the zip with a relative path to the folder
                zipf.write(file_path, os.path.relpath(file_path, folder_path_1))

        # Add all files from the second folder
        for root, dirs, files in os.walk(folder_path_2):
            for file in files:
                file_path = os.path.join(root, file)
                # Add file to the zip with a relative path to the folder
                zipf.write(file_path, os.path.relpath(file_path, folder_path_2))


def sender(folder_path_1, folder_path_2, orders_df, quote_df):
    current_time = datetime.now()
    day = current_time.day
    month = current_time.strftime("%b")
    year = current_time.year

    # Define zip filename and create the zip file
    zip_filename = f"Weekly_OrdersnQuotes_report_{day}_{month}_{year}"
    create_zip_from_folders(folder_path_1, folder_path_2, zip_filename)

    # Define display name for the zip file
    attachment_display_name = f"Wednesday_OrdersnQuotes_report_{day}_{month}_{year}.zip"

    # Send the email with the zip file attached
    send_email(f"{zip_filename}.zip", attachment_display_name, orders_df, quote_df)

    # Clean up the zip file after sending email (optional)
    os.remove(f"{zip_filename}.zip")