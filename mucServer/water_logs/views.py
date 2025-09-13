from django.shortcuts import render
from rest_framework import status
from django.db import connection, DatabaseError, transaction
from django.http import JsonResponse
from rest_framework.decorators import api_view
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

def get_user_payment_status(request):
    company_id = request.GET.get("company_id");
    user_id = request.GET.get("user_id");

    if not company_id or not user_id:
        logger.error(f"Error#01 in water log views.pay | User Not Found | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, "Error#01 User Not Found", {}, status.HTTP_400_BAD_REQUEST);
    else:
        try:
            obj = {
                "company_id" : company_id,
                "user_id" : user_id
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

    if not company_id or not user_id:
        logger.error(f"Error#04 in water log views.pay | User Not Found | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, "Error#04 User Not Found", {}, status.HTTP_400_BAD_REQUEST);
    else:
        query = """
        SELECT
            mu.first_name,
            mu.last_name,
            mu.full_name as user_name,
            mu.company_id,
            mu.user_id,
            mul.water_id,
            mul.liters,
            mul.water_cane,
            IFNULL(mup.payment_id, 0) AS payment_id,
            IFNULL(mupd.payment_id, 0) AS distribution_id,
            IFNULL(SUM(mupd.distributed_amount), 0) AS paid_amount,
            CASE
                WHEN IFNULL(SUM(mupd.distributed_amount), 0) = 0 THEN 'Not Paid'
                WHEN IFNULL(SUM(mupd.distributed_amount), 0) < (mul.liters * 2) THEN 'Partially Paid'
                ELSE 'Paid'
            END AS payment_status,
            mul.created_on as log_created_on,
            mup.modified_on as last_payment_date,
            mupd.created_on as distribution_created_date
        FROM muc_user mu
        LEFT JOIN muc_water_logs mul
            ON mu.company_id = mul.company_id
           AND mu.user_id = mul.user_id
        LEFT JOIN muc_user_payment mup
            ON mul.company_id = mup.company_id
           AND mul.user_id = mup.user_id
           AND mul.water_id = mup.water_id
        LEFT JOIN muc_user_payment_distribution mupd
            ON mup.company_id = mupd.company_id
           AND mup.payment_id = mupd.payment_id
           AND mul.water_id = mupd.water_id
           AND mul.user_id = mupd.user_id
        WHERE mu.user_id = %s
          AND mu.company_id = %s
        GROUP BY mul.water_id, mup.payment_id
        """

        params = [user_id, company_id]
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

        data = [dict(zip(columns, row)) for row in results]
        return data

def calculate_water_cane(liters_taken, liters_per_cane):
    safe_liters = liters_taken or 0
    return safe_liters / liters_per_cane

def calculate_water_liters(water_cane_taken, liters_per_cane):
    safe_water_cane_taken = water_cane_taken or 0
    return safe_water_cane_taken * liters_per_cane

@api_view(["POST"])
def upsert_water_log_details(request):
    try:
        body = json.loads(request.body.decode("utf-8"))
        if isinstance(body, dict):
            body = [body]

        if not isinstance(body, list):
            logger.error(f"Error#05 in water log views.pay | Invalid payload format | company_id: {company_id} | user_id: {user_id}");
            return api_response(False, "Error#05 Invalid payload format", {}, status.HTTP_400_BAD_REQUEST);

        liters_per_cane = 20;
        modified_on = int(time.time()* 1000);
        results = []

        with connection.cursor() as cursor:
            for entry in body:
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

                if liters is not None:
                    liters = float(liters)
                    water_cane = calculate_water_cane(liters, liters_per_cane)

                elif water_cane is not None:
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

                cursor.execute(query, params)

                results.append({
                    "company_id": company_id,
                    "user_id": user_id,
                    "water_id": water_id,
                    "status": "success",
                    "liters": liters,
                    "water_cane": water_cane
                })

        return api_response(True, "Processed all water logs", results, status.HTTP_200_OK);

    except DatabaseError as e:
        # Catch DB errors and return as API error
        logger.error(f"Error#06 in water log views.pay. | Database error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
        return api_response(False,f"Error#06 Database error: {str(e)}",None,status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        # Catch any other unexpected errors
        logger.error(f"Error#07 in water log views.pay. | Unexpected error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
        return api_response(False,f"Error#07 Unexpected error: {str(e)}",None,status.HTTP_500_INTERNAL_SERVER_ERROR);

def insert_user_payment(request_data):

    cursor = request_data["cursor"];
    company_id = request_data["company_id"];
    user_id = request_data["user_id"];
    water_id = request_data["water_id"];
    amount = request_data["amount"];
    payment_status = request_data["payment_status"];
    created_on = int(time.time() * 1000)
    modified_on = created_on

    # 🔎 Step 1: Check if user already paid
    select_query = """
        SELECT *
        FROM muc_user_payment
        WHERE company_id = %s AND user_id = %s AND water_id = %s
        LIMIT 1
    """
    select_params = [company_id, user_id, water_id]

    try:
        cursor.execute(select_query, select_params)
        existing = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        data = [dict(zip(columns, row)) for row in existing]

        if data:
            user_id = data[0].get("user_id", 0) if data else 0
            payment_id = data[0].get("payment_id", 0) if data else 0
            # Already paid
            return {
                "status": False,
                "message": f"User has already paid for this user_id {user_id}, {payment_id}",
                "payment_id": payment_id,
            }

        else:

            query = """
                INSERT INTO muc_user_payment (company_id, user_id, water_id, amount, payment_status, created_on, modified_on)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            params = [company_id, user_id, water_id, amount, payment_status, created_on, modified_on]
            try:
                cursor.execute(query, params)
                return { "status": True, "message": "Payment inserted successfully", "payment_id": cursor.lastrowid};
            except Exception as e:
                logger.error(f"Error#012 water logs views.pay | SQL Error: {e} | Query: {query} | Params: {params}");
                return { "status": False, "message": "Error#012 in water log views.pay.", "payment_id": 0};

    except Exception as e:
        logger.error(f"Error#013 water logs views.pay | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
        return { "status": False, "message": "Error#012 in water log views.pay.", "payment_id": 0};

def insert_payment_distribution(request_data):

    cursor = request_data["cursor"];
    company_id = request_data["company_id"];
    payment_id = request_data["payment_id"];
    water_id = request_data["water_id"];
    user_id = request_data["user_id"];
    distributed_amount = request_data["amount"];
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
            distribution_id = data[0].get("distribution_id", 0) if data else 0
            # Already paid
            logger.error(f"Error#013 in water logs views.pay | SQL Error: {e} | Query: {query} | Params: {params}");
            return {"status": False, "message": f"User has already payment distribution for this user_id {user_id}, {distribution_id}", "distribution_id": distribution_id}

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
                logger.error(f"Error#014 water logs views.pay | SQL Error: {e} | Query: {query} | Params: {params}");
                return { "status": False, "message": "Error#012 in water log views.pay.", "distribution_id": 0};

    except Exception as e:
        logger.error(f"Error#014 water logs views.pay | SQL Error: {e} | Query: {select_query} | Params: {select_params}");
        return { "status": False, "message": "Error#012 in water log views.pay.", "distribution_id": 0};

@api_view(["POST"])
def insert_payments(request):
    try:
        body = json.loads(request.body.decode("utf-8"))
        company_id = body.get("company_id")
        user_id = body.get("user_id")
        payments = body.get("payments", [])

        obj = {
            "company_id" : company_id,
            "user_id" : user_id
        }
        user_payment_details = get_user_payment_status_method(obj);

        if not company_id or not user_id or not isinstance(user_payment_details, list):
            logger.error(f"Error#08 in water log views.pay. | Missing required fields | company_id: {company_id} | user_id: {user_id}");
            return api_response(False, "Error#08 Missing required fields", {}, status.HTTP_400_BAD_REQUEST);

        results = []

        with transaction.atomic():  # ✅ Rollback all if any error
            with connection.cursor() as cursor:
                for p in payments:
                    water_id = p.get('water_id', 0)
                    amount = p.get("amount", 0)
                    payment_date = p.get("log_created_on")
                    payment_status = "success" if amount > 0 else "none"

                    # 💡 You need logic to decide water_id for that date
                    # for now we fetch if exists, else 0
                    cursor.execute("""
                        SELECT * FROM muc_water_logs
                        WHERE company_id=%s AND user_id=%s AND water_id=%s AND created_on=%s
                        LIMIT 1
                    """, [company_id, user_id, water_id, payment_date])

                    rows = cursor.fetchall()
                    columns = [col[0] for col in cursor.description]
                    data = [dict(zip(columns, row)) for row in rows]

                    water_id = data[0].get("water_id", 0) if data else 0
                    liters = data[0].get("liters", 0) if data else 0
                    water_cane = data[0].get("water_cane", 0) if data else 0

                    if water_id and (liters or water_cane):

                        obj = {
                            "cursor" : cursor,
                            "company_id" : company_id,
                            "user_id" : user_id,
                            "water_id" : water_id,
                            "amount" : amount,
                            "payment_status" : payment_status
                        }

                        payment_response = insert_user_payment(obj);
                        obj["payment_id"] = payment_response["payment_id"];
                        if payment_response["status"]:
                            payment_distribution_response = insert_payment_distribution(obj);
                            results.append(payment_distribution_response);
                        else:
                            # payment_distribution_response =  insert_payment_distribution(obj)
                            results.append(payment_response);
                    else:
                        results.append({"status": False,"message": "No record found","payment_id": None});

        # ✅ Decide final response
        if any(r["status"] for r in results):
            return api_response(True, "Data successfully saved.", results, status.HTTP_200_OK);
        else:
            logger.error(f"Error#09 in water log views.pay. | company_id: {company_id} | user_id: {user_id}");
            return api_response(False, "Error#09 " + results[0]["message"], results, status.HTTP_400_BAD_REQUEST);

    except DatabaseError as e:
        logger.error(f"Error#10 in water log views.pay. | Database error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, f"Error#010 Database error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);

    except Exception as e:
        logger.error(f"Error#011 in water log views.pay. | Unexpected error: {str(e)} | company_id: {company_id} | user_id: {user_id}");
        return api_response(False, f"Error#011 Unexpected error: {str(e)}", None, status.HTTP_500_INTERNAL_SERVER_ERROR);
