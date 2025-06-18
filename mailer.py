import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import json
import os
from datetime import datetime
import pandas as pd



def send_email(transfer_table, rma_table, ir_table, partial, attachment_filename, attachment_display_name):
    # Get the credentials stored:
    with open("D:\\Item_replenishment_report_automation\\Credentials.json", "r+") as crednt:
        data = json.load(crednt)
        password = data["password"]

    try:
        # Credentials for usage
        sender_email = "Bcs.notifications@building-controls.com"  # Outlook email address
        sender_password = password  
        receiver_emails = ["mithul.murugaadev@building-controls.com", "brian.ackerman@building-controls.com"]  # List of recipient email addresses 
        subject = 'Wednesday Automated reports'

        # Generate the HTML table to include in the email body
        transfer_html_table = transfer_table

        # Modify the body to include the HTML table
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
                    <h1>Weekly Automated Reports</h1>
                    <p>Hi Team,</p>
                    <p>A sample report data is generated and shared through this automated mail. Please find the Excel file attached for further review.</p>

                    <!-- Transfer Table -->
                    <h3>Transfer Report</h3>
                    <div class="table-container">
                        <p>The below given is the transfer table:</p>
                        <table>
                            {transfer_html_table}
                        </table>
                    </div>
                    <br>

                    <!-- Partial Data -->
                    <h3>Partial Report</h3>
                    <div class="table-container">
                        <p>The below given is the partial transfers table:</p>
                        <table>
                            {partial}
                        </table>
                    </div>
                    <p><br></p>
                    
                    <!-- RMA Table -->
                    <h3>RMA Report</h3>
                    <div class="table-container">
                        <p>The below given is the RMA table:</p>
                        <table>
                            {rma_table}
                        </table>
                    </div>
                    <br>
                    <!-- IR Table -->
                    <h3>IR Report</h3>
                    <div class="table-container">
                        <p>The below given is the Inventory returns table:</p>
                        <table>
                            {ir_table}
                        </table>
                    </div>

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

        # Set up the MIME
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = ', '.join(receiver_emails)
        message['Subject'] = subject

        # Attach the body with HTML content
        message.attach(MIMEText(body, 'html'))

        # Open the Excel file to be sent
        with open(attachment_filename, 'rb') as attachment:
            # Add file as application/vnd.openxmlformats-officedocument.spreadsheetml.sheet for .xlsx files
            part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
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


def sender(excel_file_path, transfer_table, rma_table, ir_table, partial):
    current_time = datetime.now()
    day = current_time.day
    month = current_time.strftime("%b")
    year = current_time.year

    # Set the file to attach
    attachment_display_name = f"Wednesday_report_{day}_{month}_{year}.xlsx"
    send_email(transfer_table, rma_table, ir_table, partial, excel_file_path, attachment_display_name)
