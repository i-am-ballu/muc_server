from django.shortcuts import render
from rest_framework import status
from django.db import connection, DatabaseError, transaction
from django.http import JsonResponse
from rest_framework.decorators import api_view
import water_logs.views as water_logs
import time
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
def getSupportDetailsBasedOnCompany(request):
    try:
        company_id = request.query_params.get("company_id");
        final_response = {};

        if not company_id:
            logger.error(f"Error#08 in activity stream views.py | getSupportDetailsBasedOnCompany | Missing required fields | company_id: {company_id} ");
            return api_response(False, "Error#08 Missing required fields", {}, status.HTTP_400_BAD_REQUEST);

        collection_count_list_response = processToGetCollectionCountBasedOnCompany({"company_id": company_id});
        final_response['collection_count_list'] = collection_count_list_response['collection_count_list'] if collection_count_list_response and collection_count_list_response['status'] and collection_count_list_response['collection_count_list'] and len(collection_count_list_response['collection_count_list']) > 0 else [];

        user_count_list_response = processToGetAdminUserCountBasedOnCompany({"company_id": company_id});
        final_response['admin_count_list'] = user_count_list_response['user_count_list'] if user_count_list_response and user_count_list_response['status'] and user_count_list_response['user_count_list'] and len(user_count_list_response['user_count_list']) > 0 else [];

        water_taken_count_list_response = processToGetWaterTakenCountBasedOnCompany({"company_id": company_id});
        final_response['water_taken_count_list'] = water_taken_count_list_response['water_taken_count_list'] if water_taken_count_list_response and water_taken_count_list_response['status'] and water_taken_count_list_response['water_taken_count_list'] and len(water_taken_count_list_response['water_taken_count_list']) > 0 else [];

        return api_response(True, "Data successfully found.", final_response, status.HTTP_200_OK);

    except DatabaseError as e:
        logger.error(f"Error#09 in activity stream views.py | getSupportDetailsBasedOnCompany | Database error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#09 Database error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        logger.error(f"Error#010 in activity stream views.py | getSupportDetailsBasedOnCompany | Unexpected error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#010 Unexpected error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

@api_view(["GET"])
def getActivityStreamBasedOnCompany(request):
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
