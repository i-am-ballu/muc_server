from django.shortcuts import render
from rest_framework import status
from django.db import connection, DatabaseError, transaction
from django.http import JsonResponse
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from user_register.authentication import CustomJWTAuthentication
from django.http import FileResponse, HttpResponseNotFound
import activity_stream.views as activity_stream
import water_logs.download_tasks as download_tasks
import water_logs.upload_tasks as upload_tasks
import time
import json
import logging
logger = logging.getLogger(__name__)

# Create your views here.

ALLOWED_EXTENSIONS = ['xlsx', 'xls']


def allowed_file(filename):
    """Check if file has allowed extension"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

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

@api_view(["POST"])
@authentication_classes([CustomJWTAuthentication])
@permission_classes([IsAuthenticated])
def getUserPaymentStatusDetails(request):

    body = json.loads(request.body.decode("utf-8"))

    company_id = request.auth.payload.get("company_id")
    user_id = body.get("user_id", 0);
    is_range_between = body.get("is_range_between", 0);
    start_date = body.get("start_date", 0);
    end_date = body.get("end_date", 0);

    if not company_id or not user_id:
        logger.error(f"Error#01 in water log views.pay | User Not Found | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, "Error#01 User Not Found", {}, status.HTTP_400_BAD_REQUEST);
    else:
        try:
            start_ts = start_date if start_date else activity_stream.get_current_month_start_date(epoch_in_ms=True);
            end_ts = end_date if end_date else activity_stream.get_current_month_end_date(epoch_in_ms=True);

            obj = {
                "company_id" : company_id,
                "user_id" : user_id,
                "start_ts" : start_ts,
                "end_ts" : end_ts,
                "is_range_between" : is_range_between
            };

            data = get_user_payment_status_method(obj);
            return api_response(True, "Payment status fetched successfully", data, status.HTTP_201_CREATED);
        except DatabaseError as e:
            # Catch DB errors and return as API error
            logger.error(f"Error#02 in water log views.pay. | Database error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
            return api_response(False,f"Error#02 Database error: {str(e)}",None,status.HTTP_500_INTERNAL_SERVER_ERROR);
        except Exception as e:
            # Catch any other unexpected errors
            logger.error(f"Error#03 in water log views.pay. | Unexpected error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
            return api_response(False,f"Error#03 Unexpected error: {str(e)}",None,status.HTTP_500_INTERNAL_SERVER_ERROR);


def get_user_payment_status_method(request_data):
     # Case 2: query params (new style)
    company_id = request_data["company_id"]
    user_id = request_data["user_id"]
    start_ts = request_data["start_ts"]
    end_ts = request_data["end_ts"]
    is_range_between = request_data["is_range_between"]

    if not company_id or not user_id:
        logger.error(f"Error#04 in water log views.pay | User Not Found | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, "Error#04 User Not Found", {}, status.HTTP_400_BAD_REQUEST);
    else:
        select_query = " SELECT ";
        select_query += " mu.first_name, mu.last_name, mu.full_name as user_name, mu.company_id, mu.user_id, mu.rate_per_cane, mul.water_id, mul.liters, ";
        select_query += " mul.water_cane, IFNULL(mup.payment_id, 0) AS payment_id, IFNULL(mupd.payment_id, 0) AS distribution_id, ";
        select_query += " IFNULL(SUM(mupd.distributed_amount), 0) AS paid_amount, "
        select_query += " (CASE ";
        select_query += " WHEN IFNULL(SUM(mupd.distributed_amount), 0) = 0 THEN 'Not Paid' ";
        select_query += " WHEN IFNULL(SUM(mupd.distributed_amount), 0) < (mul.liters * 2) THEN 'Partially Paid' ";
        select_query += " ELSE 'Paid' ";
        select_query += " END) AS payment_status, ";
        select_query += " mul.created_on as log_created_on,mup.modified_on as last_payment_date, mupd.created_on as distribution_created_date ";
        select_query += " FROM muc_user mu ";
        select_query += " LEFT JOIN muc_water_logs mul ";
        select_query += " ON mu.company_id = mul.company_id AND mu.user_id = mul.user_id ";
        select_query += " LEFT JOIN muc_user_payment mup ";
        select_query += " ON mul.company_id = mup.company_id AND mul.user_id = mup.user_id AND mul.water_id = mup.water_id ";
        select_query += " LEFT JOIN muc_user_payment_distribution mupd ";
        select_query += " ON mup.company_id = mupd.company_id AND mup.payment_id = mupd.payment_id AND mul.water_id = mupd.water_id AND mul.user_id = mupd.user_id ";
        select_query += " WHERE mu.user_id = %s AND mu.company_id = %s ";

        params = [user_id, company_id]

        if is_range_between:
            select_query += " AND mul.created_on BETWEEN %s AND %s ";
            params.extend([start_ts, end_ts]);

        select_query += " GROUP BY mul.water_id, mup.payment_id ";

        print('select_query ------- ', select_query, params)

        with connection.cursor() as cursor:
            try:
                cursor.execute(select_query, params)
                results = cursor.fetchall()
                columns = [col[0] for col in cursor.description]
                data = [dict(zip(columns, row)) for row in results]
                return data;
            except Exception as e:
                logger.error(f"Error#05 water logs views.pay | SQL Error: {e} | Query: {select_query} | Params: {params}");
                return [];

def calculate_water_cane(liters_taken, liters_per_cane):
    safe_liters = liters_taken or 0
    return safe_liters / liters_per_cane

def calculate_water_liters(water_cane_taken, liters_per_cane):
    safe_water_cane_taken = water_cane_taken or 0
    return safe_water_cane_taken * liters_per_cane

@api_view(["POST"])
@authentication_classes([CustomJWTAuthentication])
@permission_classes([IsAuthenticated])
def upsert_water_log_details(request):
    try:
        body = json.loads(request.body.decode("utf-8"))
        company_id = request.auth.payload.get("company_id");
        users_details = body.get("users_details", [])

        if isinstance(users_details, dict):
            users_details = [users_details]

        if not company_id or not isinstance(users_details, list):
            logger.error(f"Error#06 in water log views.pay | Invalid payload format | company_id: {company_id}");
            return api_response(False, "Error#06 Invalid payload format", {}, status.HTTP_400_BAD_REQUEST);

        select_query = """ SELECT superadmin_id, water_department FROM `superadmin` WHERE superadmin_id = %s """
        select_params = [company_id]

        with connection.cursor() as cursor:
            try:
                cursor.execute(select_query, select_params);
                result_rows = cursor.fetchall();
                columns = [col[0] for col in cursor.description];
                superadmin_data = [dict(zip(columns, row)) for row in result_rows];

                if not superadmin_data or not isinstance(superadmin_data, list):
                    logger.error(f"Error#07 in water log views.pay | Invalid payload format | company_id: {company_id}");
                    return api_response(False, "Error#07 Invalid payload format", {}, status.HTTP_400_BAD_REQUEST);

                superadmin_details = superadmin_data[0] if superadmin_data and len(superadmin_data) > 0 else {}

                liters_per_cane = 20;
                modified_on = int(time.time()* 1000);
                results = []

                for entry in users_details:
                    company_id = entry.get("company_id")
                    user_id = entry.get("user_id")
                    water_id = entry.get("water_id")
                    liters = entry.get("liters")
                    water_cane = entry.get("water_cane")

                    if not company_id or not user_id or not water_id:
                        results.append({
                            "company_id": company_id,
                            "user_id": user_id,
                            "water_id": water_id,
                            "status": "failed",
                            "message": "Missing required fields"
                        })
                        continue

                    if liters > 0 and superadmin_details and superadmin_details.get('water_department') == 0:
                        liters = float(liters)
                        water_cane = calculate_water_cane(liters, liters_per_cane)

                    elif water_cane > 0 and superadmin_details and superadmin_details.get('water_department') == 1:
                        water_cane = float(water_cane)
                        liters = calculate_water_liters(water_cane, liters_per_cane)

                    else:
                        results.append({
                            "company_id": company_id,
                            "user_id": user_id,
                            "water_id": water_id,
                            "status": "failed",
                            "message": "Either liters or water_cane must be provided"
                        })
                        continue

                    query = """
                    UPDATE muc_water_logs
                    SET liters = %s,
                        water_cane = %s,
                        modified_on = %s
                    WHERE company_id = %s
                      AND user_id = %s
                      AND water_id = %s
                    """

                    params = [liters, water_cane, modified_on, company_id, user_id, water_id]

                    try:
                        cursor.execute(query, params)
                        results.append({
                            "company_id": company_id,
                            "user_id": user_id,
                            "water_id": water_id,
                            "status": "success",
                            "liters": liters,
                            "water_cane": water_cane
                        });
                    except Exception as e:
                        logger.error(f"Error#08 water logs views.pay | SQL Error: {e} | Query: {query} | Params: {params}");
                        return { "status": False, "message": "Error#08 in water log views.pay.", "payment_id": 0};
                return api_response(True, "Processed all water logs", results, status.HTTP_200_OK);
            except Exception as e:
                logger.error(f"Error#09 water logs views.pay | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
                return { "status": False, "message": "Error#09 in water log views.pay.", "payment_id": 0};

    except DatabaseError as e:
        # Catch DB errors and return as API error
        logger.error(f"Error#010 in water log views.pay. | Database error: {str(e)} | company_id: {company_id}");
        return api_response(False,f"Error#010 Database error: {str(e)}",None,status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        # Catch any other unexpected errors
        logger.error(f"Error#011 in water log views.pay. | Unexpected error: {str(e)} | company_id: {company_id}");
        return api_response(False,f"Error#011 Unexpected error: {str(e)}",None,status.HTTP_500_INTERNAL_SERVER_ERROR);

def insert_user_payment(request_data):

    cursor = request_data["cursor"];
    company_id = request_data["company_id"];
    user_id = request_data["user_id"];
    water_id = request_data["water_id"];
    payment_id = request_data["payment_id"];
    pending_amount = request_data["pending_amount"];
    required_amount = request_data["required_amount"];
    total_paid_amount_by_user = request_data["total_paid_amount_by_user"];
    payment_status = request_data["payment_status"];
    created_on = int(time.time() * 1000)
    modified_on = created_on

    # 🔎 Step 1: Check if user already paid
    where_condition = " company_id = %s AND user_id = %s AND water_id = %s ";
    where_condition += " AND payment_id = %s " if payment_id else "";
    select_query = " SELECT * ";
    select_query += " FROM muc_user_payment ";
    select_query += " WHERE " + where_condition
    select_query += " LIMIT 1"
    select_params = [company_id, user_id, water_id, payment_id] if payment_id else [company_id, user_id, water_id]

    try:
        cursor.execute(select_query, select_params)
        existing = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        data = [dict(zip(columns, row)) for row in existing]

        if data:
            already_payment_id = data[0].get('payment_id', 0);
            already_paid_amount = data[0].get('amount',0);
            user_id = data[0].get("user_id", 0) if data else 0;
            payment_id = data[0].get("payment_id", 0) if data else 0;

            total_paid_amount = already_paid_amount + pending_amount;

            update_query = " UPDATE muc_user_payment ";
            update_query += " set amount = %s, modified_on = %s";
            update_query += " where company_id = %s and payment_id = %s";
            update_query += " and user_id = %s and water_id = %s ";
            update_params = [total_paid_amount, modified_on, company_id, already_payment_id, user_id, water_id,]
            print('update payment query ', update_query, update_params)
            try:
                cursor.execute(update_query, update_params)
                return { "status": True, "message": "Payment inserted successfully", "payment_id": already_payment_id};
            except Exception as e:
                logger.error(f"Error#012 water logs views.pay | SQL Error: {e} | Query: {update_query} | Params: {update_params}");
                return { "status": False, "message": "Error#012 in water log views.pay.", "payment_id": 0};

        else:
            query = " INSERT INTO muc_user_payment ";
            query += " (company_id, user_id, water_id, amount, payment_status, created_on, modified_on) ";
            query += " VALUES (%s, %s, %s, %s, %s, %s, %s)";

            params = [company_id, user_id, water_id, pending_amount, payment_status, created_on, modified_on]
            try:
                cursor.execute(query, params)
                return { "status": True, "message": "Payment inserted successfully", "payment_id": cursor.lastrowid};
            except Exception as e:
                logger.error(f"Error#013 water logs views.pay | SQL Error: {e} | Query: {query} | Params: {params}");
                return { "status": False, "message": "Error#013 in water log views.pay.", "payment_id": 0};

    except Exception as e:
        logger.error(f"Error#014234 water logs views.pay | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
        return { "status": False, "message": "Error#014 in water log views.pay.", "payment_id": 0};

def insert_payment_distribution(request_data):

    cursor = request_data["cursor"];
    company_id = request_data["company_id"];
    payment_id = request_data["payment_id"];
    water_id = request_data["water_id"];
    user_id = request_data["user_id"];
    distributed_amount = request_data["pending_amount"];
    total_paid_amount_by_user = request_data["total_paid_amount_by_user"];
    created_on = int(time.time() * 1000)
    modified_on = created_on

    # 🔎 Step 1: Check if user already paid
    select_query = """
        SELECT *
        FROM muc_user_payment_distribution
        WHERE company_id = %s AND payment_id = %s AND water_id = %s AND user_id = %s
        LIMIT 1
    """
    select_params = [company_id, payment_id, water_id, user_id]

    try:
        cursor.execute(select_query, select_params)
        existing = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        data = [dict(zip(columns, row)) for row in existing]

        if data:
            user_id = data[0].get("user_id", 0) if data else 0
            already_distribution_id = data[0].get("distribution_id", 0) if data else 0;
            already_payment_id = data[0].get("payment_id",0) if data else 0;
            already_paid_distributed_amount = data[0].get('distributed_amount',0);
            total_paid_distributed_amount = already_paid_distributed_amount + distributed_amount;

            update_query = " UPDATE muc_user_payment_distribution ";
            update_query += " set distributed_amount = %s, modified_on = %s";
            update_query += " where company_id = %s and distribution_id = %s and payment_id = %s";
            update_query += " and user_id = %s and water_id = %s ";
            update_params = [total_paid_distributed_amount, modified_on, company_id, already_distribution_id, already_payment_id, user_id, water_id]
            print('update distribution query ', update_query, update_params)
            try:
                cursor.execute(update_query, update_params)
                return { "status": True, "message": "Payment Distribution update successfully.", "payment_id": already_payment_id};
            except Exception as e:
                logger.error(f"Error#012 water logs views.pay | SQL Error: {e} | Query: {update_query} | Params: {update_params}");
                return { "status": False, "message": "Error#012 in water log views.pay.", "payment_id": 0};

        else:

            query = """
                    INSERT INTO muc_user_payment_distribution (company_id, payment_id, water_id, user_id, distributed_amount, created_on, modified_on)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
            params = [company_id, payment_id, water_id, user_id, distributed_amount, created_on, modified_on]

            try:
                cursor.execute(query, params)
                return { "status": True, "message": "Payment distribution successfully", "distribution_id": cursor.lastrowid};
            except Exception as e:
                logger.error(f"Error#0141 water logs views.pay | SQL Error: {e} | Query: {query} | Params: {params}");
                return { "status": False, "message": "Error#012 in water log views.pay.", "distribution_id": 0};

    except Exception as e:
        logger.error(f"Error#0141 water logs views.pay | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
        return { "status": False, "message": "Error#012 in water log views.pay.", "distribution_id": 0};

@api_view(["POST"])
@authentication_classes([CustomJWTAuthentication])
@permission_classes([IsAuthenticated])
def insert_payments(request):
    try:
        body = json.loads(request.body.decode("utf-8"))
        company_id = request.auth.payload.get("company_id");
        user_id = request.auth.payload.get("user_id");
        total_pending_amount = body.get("total_pending_amount")

        if not company_id or not user_id:
            logger.error(f"Error#015 in water log views.pay. | Missing required fields | company_id: {company_id} | user_id: {user_id}");
            return api_response(False, "Error#015 Missing required fields", {}, status.HTTP_400_BAD_REQUEST);

        results = []

        with transaction.atomic():

            pending_pay_list_response = get_user_wise_pending_payment_list({"company_id": company_id, "user_id": user_id});
            pending_pay_list = pending_pay_list_response['pending_pay_list'] if pending_pay_list_response and pending_pay_list_response['status'] and pending_pay_list_response['pending_pay_list'] and len(pending_pay_list_response['pending_pay_list']) > 0 else [];

            if pending_pay_list and len(pending_pay_list) > 0:
                # ✅ Rollback all if any error
                with connection.cursor() as cursor:
                    for p in pending_pay_list:
                        water_id = p.get('water_id', 0);
                        user_id = p.get('user_id', 0);
                        payment_id = p.get('payment_id',0);
                        required_amount = p.get('required_amount', 0);
                        water_cane = p.get('water_cane', 0);
                        liters = p.get('liters', 0);

                        if water_id and required_amount > 0 and (liters or water_cane):
                            pending_amount = p.get('pending_amount', 0);
                            pending_amount = int(pending_amount);

                            if(pending_amount > 0 and total_pending_amount > 0):
                                pay_amount = min(total_pending_amount, pending_amount)
                                obj = {
                                    "cursor" : cursor,
                                    "company_id" : company_id,
                                    "user_id" : user_id,
                                    "water_id" : water_id,
                                    "payment_id" : payment_id,
                                    "pending_amount" : pay_amount,
                                    "required_amount" : required_amount,
                                    "total_paid_amount_by_user" : total_pending_amount,
                                    "payment_status" : 'success',
                                }

                                payment_response = insert_user_payment(obj);
                                obj["payment_id"] = payment_id if obj and obj['payment_id'] else payment_response["payment_id"];
                                if payment_response["status"]:
                                    payment_distribution_response = insert_payment_distribution(obj);
                                    results.append(payment_distribution_response);
                                    total_pending_amount = total_pending_amount - pay_amount;
                                else:
                                    results.append(payment_response);
                                    total_pending_amount = total_pending_amount - pay_amount;

                            else:
                                results.append({"status": False,"message": "No Pending Amount Remaing"});
                        else:
                            results.append({"status": False,"message": "No record found","payment_id": None});

            else:
                logger.error(f"Error#016 in water log views.pay. | Pending payment list not found. | company_id: {company_id} | user_id: {user_id}");
                return api_response(False, "Error#016 Pending payment list not found.", {}, status.HTTP_400_BAD_REQUEST);
        # ✅ Decide final response
        if any(r["status"] for r in results):
            return api_response(True, "Data successfully saved.", results, status.HTTP_200_OK);
        else:
            logger.error(f"Error#017 in water log views.pay. | company_id: {company_id} | user_id: {user_id}");
            return api_response(False, "Error#017 " + results[0]["message"], results, status.HTTP_400_BAD_REQUEST);

    except DatabaseError as e:
        logger.error(f"Error#10 in water log views.pay. | Database error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, f"Error#010 Database error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        logger.error(f"Error#018 in water log views.pay. | Unexpected error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, f"Error#018 Unexpected error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);


        # api for get the required payment based on user id and company_id
def get_superadmin_details(body):
    company_id = body.get("company_id");
    user_id = body.get("user_id");

    if not company_id:
        logger.error(f"Error#019 in water log views.pay. | Missing required fields | company_id: {company_id}");
        return api_response(False, "Error#019 Missing required fields", {}, status.HTTP_400_BAD_REQUEST);

    select_query = """ SELECT superadmin_id, water_department FROM `superadmin` WHERE superadmin_id = %s """
    select_params = [company_id]
    with connection.cursor() as cursor:
        try:
            cursor.execute(select_query, select_params);
            result_rows = cursor.fetchall();
            columns = [col[0] for col in cursor.description];
            superadmin_data = [dict(zip(columns, row)) for row in result_rows];
            return { "status": True, "message": "Data successfully found.", "superadmin_data": superadmin_data};

        except Exception as e:
            logger.error(f"Error#020 water logs views.pay | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
            return { "status": False, "message": "Error#020 in water log views.pay."};

def get_user_details(body):
    company_id = body.get("company_id");
    user_id = body.get("user_id");

    if not company_id:
        logger.error(f"Error#021 in water log views.pay. | Missing required fields | company_id: {company_id}");
        return api_response(False, "Error#021 Missing required fields", {}, status.HTTP_400_BAD_REQUEST);

    select_query = """ SELECT user_id, company_id, rate_per_cane  FROM muc_user WHERE company_id = %s AND user_id = %s"""
    select_params = [company_id,user_id]
    with connection.cursor() as cursor:
        try:
            cursor.execute(select_query, select_params);
            result_rows = cursor.fetchall();
            columns = [col[0] for col in cursor.description];
            user_data = [dict(zip(columns, row)) for row in result_rows];
            return { "status": True, "message": "Data successfully found.", "user_data": user_data};

        except Exception as e:
            logger.error(f"Error#022 water logs views.pay | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
            return { "status": False, "message": "Error#022 in water log views.pay."};

def get_user_wise_pending_payment_list(body):
    try:
        company_id = body.get('company_id');
        user_id = body.get('user_id');

        if not company_id:
            logger.error(f"Error#023 in water log views.pay. | Missing required fields | company_id: {company_id}");
            return api_response(False, "Error#023 Missing required fields", {}, status.HTTP_400_BAD_REQUEST);

        superadmin_response = get_superadmin_details({"company_id": company_id, "user_id": user_id});
        superadmin_details = superadmin_response['superadmin_data'][0] if superadmin_response and len(superadmin_response['superadmin_data']) > 0 else {}
        user_response = get_user_details({"company_id": company_id, "user_id": user_id});
        user_details = user_response['user_data'][0] if user_response and len(user_response['user_data']) > 0 else {}
        rate_per_key = 'rate_per_cane' if superadmin_details['water_department'] and superadmin_details['water_department'] == 1 else 'rate_per_liter';
        column_key = 'water_cane' if superadmin_details['water_department'] and superadmin_details['water_department'] == 1 else 'liters';
        value_rate_per = user_details[rate_per_key] if user_details else 20 # default value of rate_per_cane

        final_query = " SELECT ";
        final_query += " wl.water_id, wl.company_id, wl.user_id, wl.created_on, wl.liters, wl.water_cane, up.payment_id, ";
        final_query += f" ((COALESCE(wl.{column_key}, 0) * {value_rate_per})) AS required_amount, ";
        final_query += " COALESCE(SUM(up.amount), 0) AS paid_amount, ";
        final_query += f" ((COALESCE(wl.{column_key}, 0) * {value_rate_per})) - COALESCE(SUM(up.amount), 0) AS pending_amount ";
        final_query += " FROM muc_water_logs wl ";
        final_query += " LEFT JOIN muc_user_payment up ";
        final_query += " ON wl.company_id = up.company_id AND wl.user_id   = up.user_id AND wl.water_id  = up.water_id ";
        final_query += " WHERE wl.company_id = %s and wl.user_id = %s ";
        final_query += " GROUP BY wl.water_id, up.payment_id ";
        final_query += " HAVING pending_amount > 0 "
        final_query += " ORDER BY wl.created_on ";

        select_params = [company_id,user_id]

        with connection.cursor() as cursor:
            try:
                cursor.execute(final_query, select_params);
                result_rows = cursor.fetchall();
                columns = [col[0] for col in cursor.description];
                pending_pay_list = [dict(zip(columns, row)) for row in result_rows];
                return { "status": True, "message": "Data successfully found.", "pending_pay_list": pending_pay_list};

            except Exception as e:
                logger.error(f"Error#024 water logs views.pay | SQL Error: {e} | Query: {final_query} | Params: {select_params}");
                return { "status": False, "message": "Error#024 in water log views.pay."};

    except DatabaseError as e:
        logger.error(f"Error#025 in water log views.pay. | Database error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, f"Error#025 Database error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        logger.error(f"Error#026 in water log views.pay. | Unexpected error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, f"Error#026 Unexpected error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

def get_user_wise_total_pending_amount_details(body):
    try:
        company_id = body.get('company_id');
        user_id = body.get('user_id');

        if not company_id:
            logger.error(f"Error#027 in water log views.pay. | Missing required fields | company_id: {company_id}");
            return api_response(False, "Error#027 Missing required fields", {}, status.HTTP_400_BAD_REQUEST);

        superadmin_response = get_superadmin_details({"company_id": company_id, "user_id": user_id});
        superadmin_details = superadmin_response['superadmin_data'][0] if superadmin_response and len(superadmin_response['superadmin_data']) > 0 else {}
        user_response = get_user_details({"company_id": company_id, "user_id": user_id});
        user_details = user_response['user_data'][0] if user_response and len(user_response['user_data']) > 0 else {}
        rate_per_key = 'rate_per_cane' if superadmin_details['water_department'] and superadmin_details['water_department'] == 1 else 'rate_per_liter';
        column_key = 'water_cane' if superadmin_details['water_department'] and superadmin_details['water_department'] == 1 else 'liters';
        value_rate_per = user_details[rate_per_key] if user_details else 20 # default value of rate_per_cane

        select_query = " SELECT ";
        select_query += " SUM(COALESCE(up.amount, 0)) AS total_paid_amount, ";
        select_query += f" SUM(((COALESCE(wl.{column_key}, 0) * {value_rate_per})) - COALESCE(up.amount, 0)) AS total_pending_amount ";
        select_query += " FROM muc_water_logs wl ";
        select_query += " LEFT JOIN muc_user_payment up ";
        select_query += " ON wl.company_id = up.company_id AND wl.user_id = up.user_id AND wl.water_id = up.water_id ";
        select_query += " WHERE wl.company_id = %s AND wl.user_id = %s ";

        select_params = [company_id,user_id]

        with connection.cursor() as cursor:
            try:
                cursor.execute(select_query, select_params);
                total_result = cursor.fetchall();
                columns = [col[0] for col in cursor.description];
                total_result_data = [dict(zip(columns, row)) for row in total_result];
                return { "status": True, "message": "Data successfully found.", "total_result_data": total_result_data};

            except Exception as e:
                logger.error(f"Error#028 water logs views.pay | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
                return { "status": False, "message": "Error#028 in water log views.pay."};


    except DatabaseError as e:
        logger.error(f"Error#029 in water log views.pay. | Database error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, f"Error#029 Database error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        logger.error(f"Error#030 in water log views.pay. | Unexpected error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, f"Error#030 Unexpected error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

@api_view(["GET"])
@authentication_classes([CustomJWTAuthentication])
@permission_classes([IsAuthenticated])
def get_pending_payments(request):
    try:
        company_id = request.auth.payload.get("company_id");
        user_id = request.auth.payload.get("user_id");

        final_response = {};

        if not company_id or not user_id:
            logger.error(f"Error#031 in water log views.pay. | Missing required fields | company_id: {company_id} | user_id: {user_id}");
            return api_response(False, "Error#031 Missing required fields", {}, status.HTTP_400_BAD_REQUEST);

        pending_pay_list_response = get_user_wise_pending_payment_list({"company_id": company_id, "user_id": user_id});
        final_response['pending_pay_list'] = pending_pay_list_response['pending_pay_list'] if pending_pay_list_response and pending_pay_list_response['status'] and pending_pay_list_response['pending_pay_list'] and len(pending_pay_list_response['pending_pay_list']) > 0 else [];

        total_result_data_response = get_user_wise_total_pending_amount_details({"company_id": company_id, "user_id": user_id});
        total_result_data = total_result_data_response['total_result_data'][0] if total_result_data_response and total_result_data_response['status'] and total_result_data_response['total_result_data'] and len(total_result_data_response['total_result_data']) > 0 else {};
        final_response['total_paid_amount'] = total_result_data['total_paid_amount'] if total_result_data and total_result_data['total_paid_amount'] else 0;
        final_response['total_pending_amount'] = total_result_data['total_pending_amount'] if total_result_data and total_result_data['total_pending_amount'] else 0;

        return api_response(True, "Data successfully found.", final_response, status.HTTP_200_OK);

    except DatabaseError as e:
        logger.error(f"Error#032 in water log views.pay. | Database error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, f"Error#032 Database error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        logger.error(f"Error#033 in water log views.pay. | Unexpected error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, f"Error#033 Unexpected error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

@api_view(["POST"])
@authentication_classes([CustomJWTAuthentication])
@permission_classes([IsAuthenticated])
def downloadMonthlyTemplateView(request):
    body = json.loads(request.body.decode("utf-8"));

    company_id = request.auth.payload.get("company_id");
    user_id = body.get("user_id", 0);
    month_name = body.get("month_name", '');


    if not company_id or not user_id:
        logger.error(f"Error#034 in water log views.pay | User Not Found | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, "Error#034 User Not Found", {}, status.HTTP_400_BAD_REQUEST);
    else:
        if not month_name:
            logger.error(f"Error#035 in water log views.pay | Month is required | company_id: {company_id} | user_id: {user_id}");
            return api_response(False, "Error#035 Month is required", {}, status.HTTP_400_BAD_REQUEST);
        else:
            try:
                obj = {
                    "company_id" : company_id,
                    "user_id" : user_id,
                    "month_name" : month_name,
                };
                task = download_tasks.generate_excel_in_background(obj);
                return api_response(True, "Month template successfully download.", task, status.HTTP_201_CREATED);
            except DatabaseError as e:
                # Catch DB errors and return as API error
                logger.error(f"Error#036 in water log views.pay. | Database error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
                return api_response(False,f"Error#036 Database error: {str(e)}",None,status.HTTP_500_INTERNAL_SERVER_ERROR);
            except Exception as e:
                # Catch any other unexpected errors
                logger.error(f"Error#037 in water log views.pay. | Unexpected error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
                return api_response(False,f"Error#037 Unexpected error: {str(e)}",None,status.HTTP_500_INTERNAL_SERVER_ERROR);

@api_view(['GET'])
@authentication_classes([CustomJWTAuthentication])
@permission_classes([IsAuthenticated])
def downloadFileViewEndPoint(request, filename):

    company_id = request.auth.payload.get("company_id");
    user_id = request.query_params.get("user_id", 0);

    if not company_id or not user_id:
        logger.error(f"Error#038 in water log views.pay | User Not Found | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, "Error#038 User Not Found", {}, status.HTTP_400_BAD_REQUEST);
    else:
        try:
            user_id = int(user_id)
            obj = {
                "company_id" : company_id,
                "user_id" : user_id,
                "filename" : filename,
            }
            file_path = download_tasks.download_file_view_end_point(obj);

            if not file_path:
                return HttpResponseNotFound("File not found")

            response = FileResponse(open(file_path, 'rb'))
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            return response

        except Exception as e:
            # Catch any other unexpected errors
            logger.error(f"Error#039 in water log views.pay. | Unexpected error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
            return api_response(False,f"Error#039 Unexpected error: {str(e)}",None,status.HTTP_500_INTERNAL_SERVER_ERROR);

@api_view(['POST'])
@authentication_classes([CustomJWTAuthentication])
@permission_classes([IsAuthenticated])
def uploadExcelFile(request):
    try:
        company_id = request.auth.payload.get("company_id")
        file = request.FILES.get("file")

        print("company_id:", company_id)
        print("FILES:", request.FILES)

        if not company_id:
            logger.error(f"Error#040 in water log views.pay | User Not Found | company_id: {company_id}");
            return api_response(False, "Error#040 User Not Found", {}, status.HTTP_400_BAD_REQUEST);
        else:
            if not file:
                logger.error(f"Error#041 in water log views.pay | No file uploaded | company_id: {company_id}");
                return api_response(False, "Error#041 No file uploaded", {}, status.HTTP_400_BAD_REQUEST);
            else:
                 if not allowed_file(file.name):
                     logger.error(f"Error#042 in water log views.pay | Wrong extension type | company_id: {company_id}");
                     return api_response(False, "Error#042 Wrong extension type. Allowed extension types are .xlsx and .xls.", {}, status.HTTP_400_BAD_REQUEST);
                 else:
                     task = upload_tasks.process_upload_excel_file(request);
                     return api_response(True, "uploaded successfully.", task, status.HTTP_201_CREATED);

    except DatabaseError as e:
        logger.error(f"Error#032 in water log views.pay. | Database error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#032 Database error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        logger.error(f"Error#033 in water log views.pay. | Unexpected error: {str(e)} | company_id: {company_id}");
        return api_response(False, f"Error#033 Unexpected error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);
