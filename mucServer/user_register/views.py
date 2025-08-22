from django.shortcuts import render
from rest_framework import generics
from .models import MucUser
from .serializers import MucUserSerializer
from django.http import JsonResponse
from rest_framework import status
from rest_framework.response import Response

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

# POST: create a user
class UserCreateView(generics.CreateAPIView):
    queryset = MucUser.objects.all()
    serializer_class = MucUserSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        serializer.save()
        return api_response(True, "User created successfully", serializer.data, status.HTTP_201_CREATED)
        return api_response(False, "Validation error", serializer.errors, status.HTTP_400_BAD_REQUEST)

# GET: list all users
class UserListView(generics.ListAPIView):
    queryset = MucUser.objects.all()
    serializer_class = MucUserSerializer

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        serializer = self.get_serializer(queryset, many=True)
        return api_response(True, "Users fetched successfully", serializer.data, 200)

# GET/PUT/PATCH/DELETE: single user
class UserDetailView(generics.RetrieveUpdateDestroyAPIView):

    def get_object(self, pk):
        try:
            return MucUser.objects.get(pk=pk)
        except MucUser.DoesNotExist:
            return None

    def get(self, request, pk):
        user = self.get_object(pk)
        if not user:
            return api_response(False, status.HTTP_404_NOT_FOUND, "User not found")
        serializer = MucUserSerializer(user)
        return api_response(True, "User created successfully", serializer.data, status.HTTP_201_CREATED)

    def put(self, request, pk):
        user = self.get_object(pk)
        if not user:
            return api_response(False, "User not found", {}, status.HTTP_404_NOT_FOUND)

        serializer = MucUserSerializer(user, data=request.data, partial=True)  # partial update allowed
        if serializer.is_valid():
            serializer.save()
            return api_response(True, "User updated successfully", serializer.data, status.HTTP_200_OK)
        return api_response(False, "Validation error", serializer.errors, status.HTTP_400_BAD_REQUEST)

    def delete(self, request, pk):
        user = self.get_object(pk)
        if not user:
            return api_response(False, "User not found", {}, status.HTTP_404_NOT_FOUND)
        user.delete()
        return api_response(True, "User deleted successfully", {}, status.HTTP_204_NO_CONTENT)
