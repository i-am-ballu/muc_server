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
]
