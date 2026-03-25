from django.urls import path

from . import views

urlpatterns = [
    path("", views.dashboard, name="app_milhao_bla_dashboard"),
    path("cards-data/", views.dashboard_cards_data, name="app_milhao_bla_cards_data"),
    path("mural-day/access/", views.mural_day_access, name="app_milhao_bla_mural_day_access"),
    path("mural-day/live/", views.mural_day_live, name="app_milhao_bla_mural_day_live"),
    path("mural-day/create/", views.mural_day_create, name="app_milhao_bla_mural_day_create"),
    path("mural-day/<int:note_id>/delete/", views.mural_day_delete, name="app_milhao_bla_mural_day_delete"),
    path("mural-day/mark-viewed/", views.mural_day_mark_viewed, name="app_milhao_bla_mural_day_mark_viewed"),
    path("export-excel/access/", views.export_excel_access, name="app_milhao_bla_export_excel_access"),
    path("export-excel/", views.export_excel, name="app_milhao_bla_export_excel"),
]
