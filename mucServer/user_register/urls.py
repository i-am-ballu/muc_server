from django.urls import path
from .views import UserCreateView, UserListView, UserDetailView, LoginView

urlpatterns = [
    path('login/', LoginView.as_view(), name="login"),
    path('users/', UserListView.as_view(), name='user-list'),
    path('users/add/', UserCreateView.as_view(), name='user-create'),
    path('users/<int:pk>/', UserDetailView.as_view(), name='user-detail'),  # GET/PUT/PATCH/DELETE single
]
