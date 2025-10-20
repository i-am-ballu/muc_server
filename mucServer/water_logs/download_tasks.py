# app/tasks.py
import os
import uuid
import calendar
from datetime import datetime
from pathlib import Path
from threading import Thread
from openpyxl import Workbook
from django.conf import settings
from user_register.models import MucUser
from water_logs.models import MucWaterLogs


def get_month_start_end_bigint(month_name):
    month_name, year = month_name.split()
    year = int(year)
    month = datetime.strptime(month_name, "%B").month  # Convert month name → number

    # Get first and last day of month
    start_date = datetime(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = datetime(year, month, last_day, 23, 59, 59, 999000)

    # Convert to milliseconds (as BigInt)
    start_bigint = int(start_date.timestamp() * 1000)
    end_bigint = int(end_date.timestamp() * 1000)

    return {
        "start_date": start_bigint,
        "end_date": end_bigint
    }

def create_monthly_template(body):
    try:
        company_id = body.get("company_id");
        user_id = body.get("user_id", 0);
        month_name = body.get("month_name", '');
        result_dict = {"status": "running"}
        start_end_date_obj = get_month_start_end_bigint(month_name);
        start_date = start_end_date_obj["start_date"] if start_end_date_obj and start_end_date_obj["start_date"] else datetime.today();
        end_date = start_end_date_obj["end_date"] if start_end_date_obj and start_end_date_obj["end_date"] else datetime.today();
        # Prepare folder
        downloads_root = Path(settings.MEDIA_ROOT) / 'download_templates' / str(company_id) / str(user_id)
        downloads_root.mkdir(parents=True, exist_ok=True)

        # Unique filename
        filename = f"{month_name}-water-template-{uuid.uuid4().hex}.xlsx"
        full_path = downloads_root / filename

        # Create workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Water Template"

        # Headers
        headers = ['User Name', 'Water Ids', 'User Ids', 'Month', 'Water Cane']
        ws.append(headers)

        user = MucUser.objects.filter(pk=user_id).first();
        username = (user.full_name).strip() or user.username if user else ''

        try:

            existing_rows = MucWaterLogs.objects.filter(
                user_id = user_id,
                created_on__range=(start_date, end_date)
            )[:50]
            if existing_rows.exists():
                for r in existing_rows:
                    ws.append([
                        username,
                        getattr(r, 'water_id', 0),
                        getattr(r, 'user_id', 0),
                        month_name,
                        getattr(r, 'water_cane', 0)
                    ])
            else:
                # add an empty sample row to show import format
                ws.append([username, 0, user_id, month_name, 0])
        except Exception:
            # fallback: just add single blank sample row
            ws.append([username, 0, user_id, month_name, 0])

        # Save workbook
        wb.save(full_path)

        # Return a result dict (Celery result backend stores it)
        download_url = os.path.join(settings.MEDIA_URL, 'downloads', str(user_id), filename)

        result_dict["status"] = "success"
        result_dict["filename"] = filename
        result_dict["path"] = str(full_path)
        result_dict["download_url"] = f"{settings.MEDIA_URL}downloads/{user_id}/{filename}"

        return result_dict;

    except Exception as e:
        result_dict["status"] = "error"
        result_dict["error"] = str(e)

def generate_excel_in_background(body):
    """Run Excel generation in a background thread."""
    response = {}

    def run():
        nonlocal response
        response = create_monthly_template(body);

    thread = Thread(target=run);
    thread.start();
    thread.join();
    return response;

def download_file_view_end_point(body):
    company_id = body.get("company_id");
    user_id = body.get("user_id", 0);
    filename = body.get("filename", '');

    downloads_root = Path(settings.MEDIA_ROOT) / 'download_templates' / str(company_id) / str(user_id)
    file_path = downloads_root / filename

    if not file_path.exists():
        return None

    return file_path
