from django.shortcuts import render
from rest_framework import status
from django.db import connection, DatabaseError, transaction
from django.http import JsonResponse
from rest_framework.decorators import api_view
import water_logs.views as water_logs
import time
import calendar
import datetime
import json
import logging
logger = logging.getLogger(__name__)

# Create your views here.

# ✅ Common response function
def api_response(status=True, message='', data=None, http_code=200):
    if data is None:
        data = {}
    return JsonResponse({
        "status": status,
        "http_code": http_code,
        "message": message,
        "data": data
    }, status=http_code, safe=False);

# ✅ Common to get current month start and end date
def get_current_month_start_date(epoch_in_ms=True):
    now = datetime.datetime.now()

    # Start of month
    month_start = datetime.datetime(now.year, now.month, 1)

    if epoch_in_ms:
        start_ts = int(month_start.timestamp() * 1000)  # convert to ms
    else:
        start_ts = int(month_start.timestamp())  # seconds

    return start_ts;

def get_current_month_end_date(epoch_in_ms=True):
    now = datetime.datetime.now();
    # End of month (last day, 23:59:59)
    last_day = calendar.monthrange(now.year, now.month)[1]
    month_end = datetime.datetime(now.year, now.month, last_day, 23, 59, 59);

    if epoch_in_ms:
        end_ts   = int(month_end.timestamp() * 1000)
    else:
        end_ts   = int(month_end.timestamp())

    return end_ts

def processToGetDistributionBasedOnUserId(body):
    company_id = body.get('company_id');

    select_query = " SELECT ";
    select_query += " user_id, SUM(distributed_amount) AS total_distribution_amount ";
    select_query += " FROM muc_user_payment_distribution";
    select_query += " WHERE company_id = %s ";
    select_query += " GROUP BY user_id ";
    select_params = [company_id]

    with connection.cursor() as cursor:
        try:
            cursor.execute(select_query, select_params);
            result_rows = cursor.fetchall();
            columns = [col[0] for col in cursor.description];
            distributed_list = [dict(zip(columns, row)) for row in result_rows];
            return { "status": True, "message": "Data successfully found.", "distributed_list": distributed_list};

        except Exception as e:
            logger.error(f"Error#01 activity stream views.py | processToGetDistributionBasedOnUserId | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
            return { "status": False, "message": "Error#01 in activity stream log views.pay."};

@api_view(["GET"])
def getDistributionBasedOnUserId(request):
    try:
        company_id = request.query_params.get("company_id");

        if not company_id:
            logger.error(f"Error#02 in activity stream views.py | getDistributionBasedOnUserId | Missing required fields | company_id: {company_id} ");
            return api_response(False, "Error#02 Missing required fields", {}, status.HTTP_400_BAD_REQUEST);

        distributed_list_response = processToGetDistributionBasedOnUserId({"company_id": company_id});
        distributed_list = distributed_list_response['distributed_list'] if distributed_list_response and distributed_list_response['status'] and distributed_list_response['distributed_list'] and len(distributed_list_response['distributed_list']) > 0 else [];
        return api_response(True, "Data successfully found.", distributed_list, status.HTTP_200_OK);

    except DatabaseError as e:
        logger.error(f"Error#03 in activity stream views.py | getDistributionBasedOnUserId | Database error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#03 Database error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        logger.error(f"Error#04 in activity stream views.py | getDistributionBasedOnUserId | Unexpected error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#04 Unexpected error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

def processToGetAdminUserCountBasedOnCompany(body):
    company_id = body.get('company_id');

    select_query = " SELECT count(*) as total_users";
    select_query += " FROM muc_user";
    select_query += " WHERE company_id = %s ";

    select_params = [company_id]

    with connection.cursor() as cursor:
        try:
            cursor.execute(select_query, select_params);
            result_rows = cursor.fetchall();
            columns = [col[0] for col in cursor.description];
            user_count_list = [dict(zip(columns, row)) for row in result_rows];
            return { "status": True, "message": "Data successfully found.", "user_count_list": user_count_list};

        except Exception as e:
            logger.error(f"Error#05 activity stream views.py | processToGetAdminUserCountBasedOnCompany | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
            return { "status": False, "message": "Error#05 in activity stream views.pay."};

def processToGetCollectionCountBasedOnCompany(body):
    company_id = body.get('company_id');

    select_query = " SELECT ";
    select_query += " COUNT(DISTINCT mup.user_id) AS total_cu_count, ";
    select_query += " (COUNT(DISTINCT mu.user_id) - COUNT(DISTINCT mup.user_id)) AS total_cru_count ";
    select_query += " FROM muc_user mu ";
    select_query += " LEFT JOIN muc_user_payment mup ";
    select_query += " ON mu.company_id = mup.company_id and mu.user_id = mup.user_id ";
    select_query += " WHERE mu.company_id = %s ";

    select_params = [company_id]

    with connection.cursor() as cursor:
        try:
            cursor.execute(select_query, select_params);
            result_rows = cursor.fetchall();
            columns = [col[0] for col in cursor.description];
            collection_count_list = [dict(zip(columns, row)) for row in result_rows];
            return { "status": True, "message": "Data successfully found.", "collection_count_list": collection_count_list};

        except Exception as e:
            logger.error(f"Error#06 activity stream logs views.py | processToGetCollectionCountBasedOnCompany | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
            return { "status": False, "message": "Error#06 in activity stream log views.pay."};

def processToGetWaterTakenCountBasedOnCompany(body):
    company_id = body.get('company_id');

    superadmin_response = water_logs.get_superadmin_details({"company_id": company_id, "user_id": 0});
    superadmin_details = superadmin_response['superadmin_data'][0] if superadmin_response and len(superadmin_response['superadmin_data']) > 0 else {}
    column_key = 'water_cane' if superadmin_details['water_department'] and superadmin_details['water_department'] == 1 else 'liters';

    select_query = " SELECT ";
    select_query += f" COALESCE(SUM({column_key}), 0) AS total_water_cane, ";
    select_query += " COUNT(DISTINCT mu.user_id) AS total_water_users ";
    select_query += " FROM muc_water_logs mu ";
    select_query += " WHERE mu.company_id = %s ";
    select_query += f" and {column_key} > 0";

    select_params = [company_id]

    with connection.cursor() as cursor:
        try:
            cursor.execute(select_query, select_params);
            result_rows = cursor.fetchall();
            columns = [col[0] for col in cursor.description];
            water_taken_count_list = [dict(zip(columns, row)) for row in result_rows];
            return { "status": True, "message": "Data successfully found.", "water_taken_count_list": water_taken_count_list};

        except Exception as e:
            logger.error(f"Error#07 activity stream views.py | processToGetWaterTakenCountBasedOnCompany | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
            return { "status": False, "message": "Error#07 in activity stream views.py."};


@api_view(["GET"])
def getSuperAdminSupportDetailsBasedOnCompany(request):
    try:
        company_id = request.query_params.get("company_id");
        final_response = {};

        if not company_id:
            logger.error(f"Error#08 in activity stream views.py | getSuperAdminSupportDetailsBasedOnCompany | Missing required fields | company_id: {company_id} ");
            return api_response(False, "Error#08 Missing required fields", {}, status.HTTP_400_BAD_REQUEST);

        collection_count_list_response = processToGetCollectionCountBasedOnCompany({"company_id": company_id});
        final_response['collection_count_list'] = collection_count_list_response['collection_count_list'] if collection_count_list_response and collection_count_list_response['status'] and collection_count_list_response['collection_count_list'] and len(collection_count_list_response['collection_count_list']) > 0 else [];

        user_count_list_response = processToGetAdminUserCountBasedOnCompany({"company_id": company_id});
        final_response['admin_count_list'] = user_count_list_response['user_count_list'] if user_count_list_response and user_count_list_response['status'] and user_count_list_response['user_count_list'] and len(user_count_list_response['user_count_list']) > 0 else [];

        water_taken_count_list_response = processToGetWaterTakenCountBasedOnCompany({"company_id": company_id});
        final_response['water_taken_count_list'] = water_taken_count_list_response['water_taken_count_list'] if water_taken_count_list_response and water_taken_count_list_response['status'] and water_taken_count_list_response['water_taken_count_list'] and len(water_taken_count_list_response['water_taken_count_list']) > 0 else [];

        return api_response(True, "Data successfully found.", final_response, status.HTTP_200_OK);

    except DatabaseError as e:
        logger.error(f"Error#09 in activity stream views.py | getSuperAdminSupportDetailsBasedOnCompany | Database error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#09 Database error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        logger.error(f"Error#010 in activity stream views.py | getSuperAdminSupportDetailsBasedOnCompany | Unexpected error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#010 Unexpected error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

@api_view(["GET"])
def getSuperAdminActivityStreamBasedOnCompany(request):
    try:
        company_id = request.query_params.get("company_id");

        if not company_id:
            logger.error(f"Error#011 in activity stream log views.pay. | Missing required fields | company_id: {company_id} ");
            return api_response(False, "Error#011 Missing required fields", {}, status.HTTP_400_BAD_REQUEST);

        select_query = " SELECT ";
        select_query += " u.user_id, u.full_name AS user_fn, u.email AS user_email, u.mobile_number AS user_mobn,";
        select_query += " COALESCE(SUM(up.amount), 0) AS total_payment,";
        select_query += " COALESCE(DATEDIFF(CURDATE(), FROM_UNIXTIME(MAX(up.created_on) / 1000)), 0) AS payment_db_days,"
        select_query += " (CASE ";
        select_query += " WHEN MAX(up.payment_status) = 'success' THEN 'Paid' ";
        select_query += " WHEN MAX(up.payment_status) = 'error' THEN 'Error' ";
        select_query += " ELSE 'Not Paid' "
        select_query += " END) AS payment_status ";
        select_query += " FROM muc_user u ";
        select_query += " LEFT JOIN muc_user_payment up ON u.company_id = up.company_id and u.user_id = up.user_id ";
        select_query += " WHERE u.company_id = %s ";
        select_query += " GROUP BY u.user_id, u.full_name, u.email, u.mobile_number ";
        select_query += " ORDER BY u.full_name ";

        select_params = [company_id]

        with connection.cursor() as cursor:
            try:
                cursor.execute(select_query, select_params);
                result_rows = cursor.fetchall();
                columns = [col[0] for col in cursor.description];
                activity_stream_list = [dict(zip(columns, row)) for row in result_rows];
                return api_response(True, "Data successfully found.", activity_stream_list, status.HTTP_200_OK);

            except Exception as e:
                logger.error(f"Error#012 activity stream views.pay | getActivityStreamBasedOnCompany | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
                return { "status": False, "message": "Error#012 in activity stream log views.pay."};

    except DatabaseError as e:
        logger.error(f"Error#013 in activity stream views.pay | getActivityStreamBasedOnCompany | Database error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#013 Database error : {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        logger.error(f"Error#014 in activity stream views.pay | getActivityStreamBasedOnCompany | Unexpected error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#014 Unexpected error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);


def processToGetUserMonthlyCollectionCountBasedOnCompany(body):
    company_id = body.get('company_id');
    user_id = body.get('user_id');
    start_date = body.get('start_date', 0);
    end_date = body.get('end_date', 0);

    start_ts = start_date if start_date else get_current_month_start_date(epoch_in_ms=True);
    end_ts = end_date if end_date else get_current_month_end_date(epoch_in_ms=True);
    superadmin_response = water_logs.get_superadmin_details({"company_id": company_id, "user_id": user_id});
    superadmin_details = superadmin_response['superadmin_data'][0] if superadmin_response and len(superadmin_response['superadmin_data']) > 0 else {};
    user_response = water_logs.get_user_details({"company_id": company_id, "user_id": user_id});
    user_details = user_response['user_data'][0] if user_response and len(user_response['user_data']) > 0 else {}
    rate_per_key = 'rate_per_cane' if superadmin_details['water_department'] and superadmin_details['water_department'] == 1 else 'rate_per_liter';
    column_key = 'water_cane' if superadmin_details['water_department'] and superadmin_details['water_department'] == 1 else 'liters';
    value_rate_per = user_details[rate_per_key] if user_details else 20 # default value of rate_per_cane

    select_query = " SELECT ";
    select_query += " COALESCE(SUM(COALESCE(up.amount, 0)), 0) AS total_paid_amount, ";
    select_query += f" COALESCE(SUM(((COALESCE(wl.{column_key}, 0) * {value_rate_per})) - COALESCE(up.amount, 0)), 0) AS total_pending_amount ";
    select_query += " FROM muc_water_logs wl ";
    select_query += " LEFT JOIN muc_user_payment up ";
    select_query += " ON wl.company_id = up.company_id AND wl.user_id = up.user_id AND wl.water_id = up.water_id ";
    select_query += " WHERE wl.company_id = %s AND wl.user_id = %s ";
    select_query += " AND wl.created_on BETWEEN %s AND %s ";

    select_params = [company_id, user_id, start_ts, end_ts]

    with connection.cursor() as cursor:
        try:
            cursor.execute(select_query, select_params);
            result_rows = cursor.fetchall();
            columns = [col[0] for col in cursor.description];
            collection_count_list = [dict(zip(columns, row)) for row in result_rows];
            return { "status": True, "message": "Data successfully found.", "collection_count_list": collection_count_list};

        except Exception as e:
            logger.error(f"Error#06 activity stream logs views.py | processToGetCollectionCountBasedOnCompany | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
            return { "status": False, "message": "Error#06 in activity stream log views.pay."};

def processToGetUserMonthlyWaterTakenCountBasedOnCompany(body):
    company_id = body.get('company_id');
    user_id = body.get('user_id');
    start_date = body.get('start_date', 0);
    end_date = body.get('end_date', 0);

    start_ts = start_date if start_date else get_current_month_start_date(epoch_in_ms=True);
    end_ts = end_date if end_date else get_current_month_end_date(epoch_in_ms=True);
    superadmin_response = water_logs.get_superadmin_details({"company_id": company_id, "user_id": user_id});
    superadmin_details = superadmin_response['superadmin_data'][0] if superadmin_response and len(superadmin_response['superadmin_data']) > 0 else {}
    column_key = 'water_cane' if superadmin_details['water_department'] and superadmin_details['water_department'] == 1 else 'liters';

    select_query = " SELECT ";
    select_query += f" COALESCE(SUM({column_key}), 0) AS total_water_cane, ";
    select_query += " COUNT(DISTINCT mu.user_id) AS total_water_users, ";
    select_query += " COUNT(DISTINCT DATE(FROM_UNIXTIME(mu.created_on / 1000))) as total_days, ";
    select_query += " DAY(LAST_DAY(CURDATE())) - COUNT(DISTINCT DATE(FROM_UNIXTIME(mu.created_on / 1000))) AS remaining_days ";
    select_query += " FROM muc_water_logs mu ";
    select_query += " WHERE mu.company_id = %s and mu.user_id = %s";
    select_query += " AND mu.created_on BETWEEN %s AND %s ";

    select_params = [company_id, user_id, start_ts, end_ts]

    print(select_params, select_query)

    with connection.cursor() as cursor:
        try:
            cursor.execute(select_query, select_params);
            result_rows = cursor.fetchall();
            columns = [col[0] for col in cursor.description];
            water_taken_count_list = [dict(zip(columns, row)) for row in result_rows];
            return { "status": True, "message": "Data successfully found.", "water_taken_count_list": water_taken_count_list};

        except Exception as e:
            logger.error(f"Error#07 activity stream views.py | processToGetWaterTakenCountBasedOnCompany | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
            return { "status": False, "message": "Error#07 in activity stream views.py."};

@api_view(["GET"])
def getAdminSupportDetailsBasedOnCompany(request):
    try:
        company_id = request.query_params.get("company_id");
        user_id = request.query_params.get("user_id");
        start_date = request.query_params.get("start_date", 0);
        end_date = request.query_params.get("end_date", 0);
        final_response = {};

        if not company_id or not user_id:
            logger.error(f"Error#015 in activity stream views.py | getAdminSupportDetailsBasedOnCompany | Missing required fields | company_id: {company_id} | user_id: {user_id}");
            return api_response(False, "Error#015 Missing required fields", {}, status.HTTP_400_BAD_REQUEST);

        collection_count_list_response = processToGetUserMonthlyCollectionCountBasedOnCompany({"company_id": company_id, "user_id": user_id, "start_date": start_date, "end_date": end_date});
        final_response['collection_count_list'] = collection_count_list_response['collection_count_list'] if collection_count_list_response and collection_count_list_response['status'] and collection_count_list_response['collection_count_list'] and len(collection_count_list_response['collection_count_list']) > 0 else [];

        # user_count_list_response = processToGetAdminUserCountBasedOnCompany({"company_id": company_id, "user_id": user_id});
        # final_response['admin_count_list'] = user_count_list_response['user_count_list'] if user_count_list_response and user_count_list_response['status'] and user_count_list_response['user_count_list'] and len(user_count_list_response['user_count_list']) > 0 else [];
        #
        water_taken_count_list_response = processToGetUserMonthlyWaterTakenCountBasedOnCompany({"company_id": company_id, "user_id": user_id, "start_date": start_date, "end_date": end_date});
        final_response['water_taken_count_list'] = water_taken_count_list_response['water_taken_count_list'] if water_taken_count_list_response and water_taken_count_list_response['status'] and water_taken_count_list_response['water_taken_count_list'] and len(water_taken_count_list_response['water_taken_count_list']) > 0 else [];

        return api_response(True, "Data successfully found.", final_response, status.HTTP_200_OK);

    except DatabaseError as e:
        logger.error(f"Error#016 in activity stream views.py | getAdminSupportDetailsBasedOnCompany | Database error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#016 Database error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        logger.error(f"Error#017 in activity stream views.py | getAdminSupportDetailsBasedOnCompany | Unexpected error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#017 Unexpected error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

@api_view(["GET"])
def getAdminActivityStreamBasedOnCompany(request):
    try:
        company_id = request.query_params.get("company_id");
        user_id = request.query_params.get("user_id");

        if not company_id or not user_id:
            logger.error(f"Error#011 in activity stream log views.pay. | Missing required fields | company_id: {company_id} | user_id: {user_id}");
            return api_response(False, "Error#011 Missing required fields", {}, status.HTTP_400_BAD_REQUEST);

        select_query = " SELECT ";
        select_query += " u.user_id, u.full_name AS user_fn, u.email AS user_email, u.mobile_number AS user_mobn,";
        select_query += " DATE_FORMAT(FROM_UNIXTIME(up.created_on / 1000), '%%d %%b %%Y') AS payment_month, ";
        select_query += " COALESCE(SUM(up.amount), 0) AS total_payment,";
        select_query += " COALESCE(DATEDIFF(CURDATE(), FROM_UNIXTIME(MAX(up.created_on) / 1000)), 0) AS payment_db_days,"
        select_query += " (CASE ";
        select_query += " WHEN MAX(up.payment_status) = 'success' THEN 'Paid' ";
        select_query += " WHEN MAX(up.payment_status) = 'error' THEN 'Error' ";
        select_query += " ELSE 'Not Paid' "
        select_query += " END) AS payment_status ";
        select_query += " FROM muc_user u ";
        select_query += " LEFT JOIN muc_user_payment up ON u.company_id = up.company_id and u.user_id = up.user_id ";
        select_query += " WHERE u.company_id = %s and u.user_id = %s ";
        select_query += " GROUP BY u.user_id, u.full_name, u.email, u.mobile_number, ";
        select_query += " DATE_FORMAT(FROM_UNIXTIME(up.created_on / 1000), '%%d %%b %%Y') ";
        select_query += " ORDER BY u.full_name, payment_month DESC ";

        select_params = [company_id, user_id]

        with connection.cursor() as cursor:
            try:
                cursor.execute(select_query, select_params);
                result_rows = cursor.fetchall();
                columns = [col[0] for col in cursor.description];
                activity_stream_list = [dict(zip(columns, row)) for row in result_rows];
                return api_response(True, "Data successfully found.", activity_stream_list, status.HTTP_200_OK);

            except Exception as e:
                logger.error(f"Error#012 activity stream views.pay | getActivityStreamBasedOnCompany | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
                return { "status": False, "message": "Error#012 in activity stream log views.pay."};

    except DatabaseError as e:
        logger.error(f"Error#013 in activity stream views.pay | getActivityStreamBasedOnCompany | Database error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#013 Database error : {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        logger.error(f"Error#014 in activity stream views.pay | getActivityStreamBasedOnCompany | Unexpected error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#014 Unexpected error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

@api_view(["GET"])
def getInsightsWaterPayment(request):
    try:
        company_id = request.query_params.get("company_id");
        is_superadmin = request.query_params.get("is_superadmin", 0);
        is_range_between = request.query_params.get("is_range_between", 0);
        user_id = request.query_params.get("user_id", 0);
        start_date = request.query_params.get("start_date", 0);
        end_date = request.query_params.get("end_date", 0);

        if not company_id:
            logger.error(f"Error#011 in activity stream log views.pay. | Missing required fields | company_id: {company_id}");
            return api_response(False, "Error#011 Missing required fields", {}, status.HTTP_400_BAD_REQUEST);

        start_ts = start_date if start_date else get_current_month_start_date(epoch_in_ms=True);
        end_ts = end_date if end_date else get_current_month_end_date(epoch_in_ms=True);
        superadmin_response = water_logs.get_superadmin_details({"company_id": company_id, "user_id": user_id});
        superadmin_details = superadmin_response['superadmin_data'][0] if superadmin_response and len(superadmin_response['superadmin_data']) > 0 else {}
        column_key = 'water_cane' if superadmin_details['water_department'] and superadmin_details['water_department'] == 1 else 'liters';
        rate_per_key = 'rate_per_cane' if superadmin_details['water_department'] and superadmin_details['water_department'] == 1 else 'rate_per_liter';

        select_query = " SELECT ";
        select_query += " u.user_id,u.full_name AS user_name,wl.water_id, ";
        select_query += " wl.created_on as water_log_c_date, ";
        select_query += " up.created_on as payment_c_date, ";
        select_query += " COALESCE(wl.liters, 0) AS liters, ";
        select_query += " COALESCE(wl.water_cane, 0) AS water_cane, ";
        select_query += " COALESCE(up.amount, 0) AS paid_amount, ";
        select_query += f" (COALESCE(wl.{column_key}, 0) * u.{rate_per_key}) - COALESCE(up.amount, 0) AS remaining_amount, ";
        select_query += " (CASE WHEN up.amount IS NOT NULL AND up.amount > 0 THEN 'Paid' ELSE 'Not Paid' END) AS payment_status ";
        select_query += " FROM muc_user u ";
        select_query += " LEFT JOIN muc_water_logs wl ";
        select_query += " ON u.company_id = wl.company_id AND u.user_id = wl.user_id ";
        select_query += " LEFT JOIN muc_user_payment up ";
        select_query += " ON wl.company_id = up.company_id AND wl.user_id = up.user_id AND wl.water_id = up.water_id ";
        select_query += " WHERE u.company_id = %s ";

        select_params = [company_id]

        if not is_superadmin and user_id:
            select_query += " AND u.user_id = %s ";
            select_params.append(user_id);

        if is_range_between:
            select_query += " AND wl.created_on BETWEEN %s AND %s ";
            select_params.extend([start_ts, end_ts]);

        select_query += " ORDER BY u.user_id, wl.created_on ";

        with connection.cursor() as cursor:
            try:
                cursor.execute(select_query, select_params);
                result_rows = cursor.fetchall();
                columns = [col[0] for col in cursor.description];
                activity_stream_list = [dict(zip(columns, row)) for row in result_rows];
                return api_response(True, "Data successfully found.", activity_stream_list, status.HTTP_200_OK);

            except Exception as e:
                logger.error(f"Error#012 activity stream views.pay | getInsightsWaterPayment | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
                return { "status": False, "message": "Error#012 in activity stream log views.pay."};

    except DatabaseError as e:
        logger.error(f"Error#013 in activity stream views.pay | getInsightsWaterPayment | Database error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#013 Database error : {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        logger.error(f"Error#014 in activity stream views.pay | getInsightsWaterPayment | Unexpected error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#014 Unexpected error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);
