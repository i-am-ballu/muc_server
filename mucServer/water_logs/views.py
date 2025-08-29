from django.shortcuts import render
from rest_framework import status
from django.db import connection, DatabaseError
from django.http import JsonResponse

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

def user_payment_status(request, company_id, user_id):
    query = """
    SELECT
        mu.first_name,
        mu.last_name,
        mul.water_id,
        mul.liters,
        mul.water_cane,
        IFNULL(SUM(mupd.distributed_amount), 0) AS paid_amount,
        CASE
            WHEN IFNULL(SUM(mupd.distributed_amount), 0) = 0 THEN 'Not Paid'
            WHEN IFNULL(SUM(mupd.distributed_amount), 0) < (mul.liters * 2) THEN 'Partially Paid'
            ELSE 'Paid'
        END AS payment_status
    FROM muc_user mu
    LEFT JOIN muc_water_logs mul
        ON mu.company_id = mul.company_id
       AND mu.user_id = mul.user_id
    LEFT JOIN muc_user_payment mup
        ON mul.company_id = mup.company_id
       AND mul.user_id = mup.user_id
    LEFT JOIN muc_user_payment_distribution mupd
        ON mup.company_id = mupd.company_id
       AND mup.payment_id = mupd.payment_id
       AND mul.water_id = mupd.water_id
       AND mul.user_id = mupd.user_id
    WHERE mu.user_id = %s
      AND mu.company_id = %s
    GROUP BY mul.water_id
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
