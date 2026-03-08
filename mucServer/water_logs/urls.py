from django.urls import path
from . import views

urlpatterns = [
    path('getUserPaymentStatusDetails/', views.getUserPaymentStatusDetails),
    path('upsert-log/', views.upsert_water_log_details),
    path('insert_payments/', views.insert_payments),
    path('get_pending_payments/', views.get_pending_payments),
    path('downloadMonthlyTemplate/', views.downloadMonthlyTemplateView),
    path('csv/<str:filename>', views.downloadFileViewEndPoint, name='download_file'),
    path('uploadExcelFile/', views.uploadExcelFile, name='upload_excel_file'),
    path('importComponentWaterLogsData/', views.importComponentWaterLogsData, name='import_data'),
    # Razorpay Payment URLs
    path('payment/create-order/', views.create_payment_order, name='create_payment_order'),
    path('payment/verify/', views.verify_and_process_payment, name='verify_payment'),
    path('payment/webhook/', views.razorpay_webhook, name='razorpay_webhook'),
]
