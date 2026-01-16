from django.contrib import admin
from django.http import JsonResponse
from django.urls import path
from django.utils.translation import gettext_lazy as _

from .forms import PerfilUsuarioAdminForm
from .models import (
    CategoriaCompra,
    Caderno,
    CentroCusto,
    PerfilUsuario,
    Compra,
    Proposta,
    StatusCompra,
    TipoCompra,
    TipoPerfil,
)

admin.site.site_header = "SET Admin"
admin.site.site_title = "SET Admin"
admin.site.index_title = "Painel administrativo"


@admin.register(PerfilUsuario)
class PerfilUsuarioAdmin(admin.ModelAdmin):
    form = PerfilUsuarioAdminForm
    list_display = ("nome", "email", "tipos_display", "ativo")
    search_fields = ("nome", "email")

    def tipos_display(self, obj):
        return ", ".join(obj.tipos.values_list("nome", flat=True))
    tipos_display.short_description = "Tipos"


@admin.register(Proposta)
class PropostaAdmin(admin.ModelAdmin):
    change_form_template = "admin/core/proposta/change_form.html"
    list_display = ("nome", "codigo", "cliente", "status", "prioridade", "valor", "criado_em", "decidido_em")
    list_filter = ("status",)
    search_fields = ("nome", "codigo", "cliente__nome")

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                "gerar-codigo/",
                self.admin_site.admin_view(self.gerar_codigo),
                name="core_proposta_gerar_codigo",
            )
        ]
        return custom_urls + urls

    def gerar_codigo(self, request):
        cliente_id = request.GET.get("cliente_id")
        if not cliente_id:
            return JsonResponse({"error": _("Selecione um cliente.")}, status=400)
        try:
            cliente = PerfilUsuario.objects.get(pk=cliente_id)
        except PerfilUsuario.DoesNotExist:
            return JsonResponse({"error": _("Cliente invalido.")}, status=400)
        proposta = Proposta(cliente=cliente)
        return JsonResponse({"codigo": proposta._proximo_codigo()})


@admin.register(TipoPerfil)
class TipoPerfilAdmin(admin.ModelAdmin):
    list_display = ("nome",)
    search_fields = ("nome",)


@admin.register(Caderno)
class CadernoAdmin(admin.ModelAdmin):
    list_display = ("nome", "clientes_display", "ativo")
    search_fields = ("nome", "cliente__nome")
    list_filter = ("ativo",)

    def clientes_display(self, obj):
        return ", ".join(obj.clientes.values_list("nome", flat=True))
    clientes_display.short_description = "Clientes"


@admin.register(CategoriaCompra)
class CategoriaCompraAdmin(admin.ModelAdmin):
    list_display = ("nome",)
    search_fields = ("nome",)


@admin.register(TipoCompra)
class TipoCompraAdmin(admin.ModelAdmin):
    list_display = ("nome",)
    search_fields = ("nome",)


@admin.register(CentroCusto)
class CentroCustoAdmin(admin.ModelAdmin):
    list_display = ("nome",)
    search_fields = ("nome",)


@admin.register(StatusCompra)
class StatusCompraAdmin(admin.ModelAdmin):
    list_display = ("nome", "ativo")
    search_fields = ("nome",)
    list_filter = ("ativo",)


@admin.register(Compra)
class CompraAdmin(admin.ModelAdmin):
    list_display = ("descricao", "caderno", "valor", "data", "status", "pago", "data_pagamento")
    list_filter = ("status", "tipo", "categoria")
    search_fields = ("descricao", "caderno__nome")
