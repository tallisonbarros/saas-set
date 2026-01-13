from django.contrib import admin

from .forms import ClienteAdminForm
from .models import Cliente, Proposta


@admin.register(Cliente)
class ClienteAdmin(admin.ModelAdmin):
    form = ClienteAdminForm
    list_display = ("nome", "email", "ativo")
    search_fields = ("nome", "email")


@admin.register(Proposta)
class PropostaAdmin(admin.ModelAdmin):
    list_display = ("nome", "codigo", "cliente", "status", "prioridade", "valor", "criado_em", "decidido_em")
    list_filter = ("status",)
    search_fields = ("nome", "codigo", "cliente__nome")
