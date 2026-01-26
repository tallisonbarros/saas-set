from django.urls import path

from . import views

urlpatterns = [
    path("", views.dashboard, name="app_milhao_bla_dashboard"),
]
