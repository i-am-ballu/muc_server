from django.shortcuts import render
from rest_framework import status
from django.db import connection, DatabaseError
from django.http import JsonResponse
from rest_framework.decorators import api_view
import time
import json

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

def user_payment_status(request):
     # Case 2: query params (new style)
    company_id = request.GET.get("company_id")
    user_id = request.GET.get("user_id")

    if not company_id or not user_id:
        return api_response(False, "User Not Found", {}, status.HTTP_400_BAD_REQUEST)
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

        try:
            params = [user_id, company_id]
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                columns = [col[0] for col in cursor.description]

            data = [dict(zip(columns, row)) for row in results]
            return api_response(True, "Payment status fetched successfully", data, status.HTTP_201_CREATED)

        except DatabaseError as e:
            # Catch DB errors and return as API error
            return api_response(
                False,
                f"Database error: {str(e)}",
                None,
                status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        except Exception as e:
            # Catch any other unexpected errors
            return api_response(
                False,
                f"Unexpected error: {str(e)}",
                None,
                status.HTTP_500_INTERNAL_SERVER_ERROR
            )

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
            return api_response(False, "Invalid payload format", {}, status.HTTP_400_BAD_REQUEST)

        liters_per_cane = 20;
        modified_on = int(time.time());
        results = []

        with connection.cursor() as cursor:
            for entry in body:
                company_id = entry.get("company_id")
                user_id = entry.get("user_id")
                water_id = entry.get("water_id")
                liters = entry.get("liters")
                water_cane = entry.get("water_cane")

                print('company_id ------- ', company_id, water_id)

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


                print('water_cane ------- ', water_cane, liters)

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

        return api_response(True, "Processed all water logs", results, status.HTTP_200_OK)

    except DatabaseError as e:
        # Catch DB errors and return as API error
        return api_response(
            False,
            f"Database error: {str(e)}",
            None,
            status.HTTP_500_INTERNAL_SERVER_ERROR
        )

    except Exception as e:
        # Catch any other unexpected errors
        return api_response(
            False,
            f"Unexpected error: {str(e)}",
            None,
            status.HTTP_500_INTERNAL_SERVER_ERROR
        )
