from django.urls import path

from . import views

urlpatterns = [
    path("", views.dashboard, name="app_milhao_bla_dashboard"),
    path("cards-data/", views.dashboard_cards_data, name="app_milhao_bla_cards_data"),
    path("export-excel/", views.export_excel, name="app_milhao_bla_export_excel"),
]
