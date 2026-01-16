from datetime import datetime
from decimal import Decimal, InvalidOperation

from django.contrib.auth import logout
from django.contrib.auth.decorators import login_required
from django.http import Http404, HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST

from django.contrib.auth.models import User
from django.db.models import Case, IntegerField, Sum, Value, When

from .forms import TipoPerfilCreateForm, UserCreateForm
from .models import (
    CategoriaCompra,
    Caderno,
    CentroCusto,
    Cliente,
    Compra,
    Proposta,
    StatusCompra,
    TipoCompra,
    TipoPerfil,
)


def _get_cliente(user):
    try:
        return user.cliente
    except Cliente.DoesNotExist:
        return None


def _user_role(user):
    if user.is_superuser or user.is_staff:
        return "ADMIN"
    cliente = _get_cliente(user)
    if not cliente:
        return "CLIENTE"
    has_cliente = cliente.tipos.filter(nome__iexact="Contratante").exists() or cliente.tipos.filter(
        nome__iexact="Cliente"
    ).exists()
    has_financeiro = cliente.tipos.filter(nome__iexact="Financeiro").exists()
    if has_cliente:
        return "CLIENTE"
    if has_financeiro:
        return "FINANCEIRO"
    return "CLIENTE"


def _has_tipo(user, nome):
    cliente = _get_cliente(user)
    if not cliente:
        return False
    return cliente.tipos.filter(nome__iexact=nome).exists()


def _has_tipo_any(user, nomes):
    cliente = _get_cliente(user)
    if not cliente:
        return False
    return cliente.tipos.filter(nome__in=nomes).exists()


def home(request):
    if request.user.is_authenticated:
        logout(request)
    return render(request, "core/home.html")


@login_required
def painel(request):
    return render(
        request,
        "core/painel.html",
        {
            "role": _user_role(request.user),
            "is_financeiro": _has_tipo(request.user, "Financeiro") or request.user.is_staff,
            "is_cliente": _has_tipo_any(request.user, ["Contratante", "Cliente"]),
        },
    )


IO_CHANNEL_TYPES = ["DI", "DO", "AI", "AO", "RTD", "TC", "Pulso"]
IO_MODULES_SAMPLE = [
    {
        "id": 1,
        "nome": "Modulo de Entradas Digitais",
        "modelo": "DI-16X",
        "marca": "SET",
        "canais": 16,
        "tipo": "DI",
    },
    {
        "id": 2,
        "nome": "Modulo de Saidas Digitais",
        "modelo": "DO-16R",
        "marca": "SET",
        "canais": 16,
        "tipo": "DO",
    },
    {
        "id": 3,
        "nome": "Modulo de Entradas Analogicas",
        "modelo": "AI-08H",
        "marca": "Festo",
        "canais": 8,
        "tipo": "AI",
    },
    {
        "id": 4,
        "nome": "Modulo de Saidas Analogicas",
        "modelo": "AO-04P",
        "marca": "Siemens",
        "canais": 4,
        "tipo": "AO",
    },
]
IO_RACKS_SAMPLE = [
    {
        "id": 1,
        "nome": "Rack Principal",
        "descricao": "Linha 01 - Esteira e empacotamento",
        "slots": 10,
        "ocupados": 6,
    },
    {
        "id": 2,
        "nome": "Rack Remoto",
        "descricao": "Sala de bombas e utilidades",
        "slots": 8,
        "ocupados": 3,
    },
]


def _get_module(module_id):
    for module in IO_MODULES_SAMPLE:
        if module["id"] == module_id:
            return module
    return None


@login_required
def ios_list(request):
    return render(
        request,
        "core/ios_list.html",
        {
            "racks": IO_RACKS_SAMPLE,
            "modules": IO_MODULES_SAMPLE,
            "channel_types": IO_CHANNEL_TYPES,
        },
    )


@login_required
def ios_rack_detail(request, pk):
    rack = next((item for item in IO_RACKS_SAMPLE if item["id"] == pk), None)
    if not rack:
        raise Http404("Rack nao encontrado.")
    layout = [
        {"slot": "S1", "module_id": 1},
        {"slot": "S2", "module_id": 2},
        {"slot": "S3", "module_id": None},
        {"slot": "S4", "module_id": 3},
        {"slot": "S5", "module_id": None},
        {"slot": "S6", "module_id": 4},
        {"slot": "S7", "module_id": None},
        {"slot": "S8", "module_id": None},
        {"slot": "S9", "module_id": None},
        {"slot": "S10", "module_id": None},
    ]
    slots = []
    for item in layout:
        module = _get_module(item["module_id"]) if item["module_id"] else None
        slots.append(
            {
                "slot": item["slot"],
                "module": module,
            }
        )
    return render(
        request,
        "core/ios_rack_detail.html",
        {
            "rack": rack,
            "slots": slots,
            "modules": IO_MODULES_SAMPLE,
        },
    )


@login_required
def ios_modulos(request):
    return render(
        request,
        "core/ios_modulos.html",
        {
            "modules": IO_MODULES_SAMPLE,
            "channel_types": IO_CHANNEL_TYPES,
        },
    )


@login_required
def ios_modulo_detail(request, pk):
    module = _get_module(pk)
    if not module:
        raise Http404("Modulo nao encontrado.")
    max_channels = min(module["canais"], 12)
    channels = []
    for index in range(1, max_channels + 1):
        channels.append(
            {
                "index": index,
                "nome": "Canal %02d" % index,
                "tipo": module["tipo"],
            }
        )
    return render(
        request,
        "core/ios_modulo_detail.html",
        {
            "module": module,
            "channels": channels,
            "channel_types": IO_CHANNEL_TYPES,
            "remaining_channels": module["canais"] - max_channels,
        },
    )


@login_required
def proposta_list(request):
    if _user_role(request.user) == "FINANCEIRO":
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    propostas = Proposta.objects.none()
    if cliente:
        propostas = Proposta.objects.filter(cliente=cliente).order_by("-criado_em")
    status = request.GET.get("status")
    if status in Proposta.Status.values:
        propostas = propostas.filter(status=status)
        if status == Proposta.Status.APROVADA:
            propostas = propostas.order_by("prioridade", "-criado_em")
    else:
        propostas = propostas.exclude(status__in=[Proposta.Status.REPROVADA, Proposta.Status.FINALIZADO])
        propostas = propostas.annotate(
            status_order=Case(
                When(status=Proposta.Status.PENDENTE, then=Value(1)),
                When(status=Proposta.Status.EXECUTANDO, then=Value(2)),
                When(status=Proposta.Status.APROVADA, then=Value(3)),
                When(status=Proposta.Status.LEVANTAMENTO, then=Value(4)),
                default=Value(5),
                output_field=IntegerField(),
            )
        ).order_by("status_order", "-criado_em")
    return render(
        request,
        "core/proposta_list.html",
        {"cliente": cliente, "propostas": propostas, "status_filter": status},
    )


@login_required
def proposta_detail(request, pk):
    if _user_role(request.user) == "FINANCEIRO":
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    proposta = get_object_or_404(Proposta, pk=pk, cliente=cliente)
    return render(request, "core/proposta_detail.html", {"cliente": cliente, "proposta": proposta})


@login_required
@require_POST
def aprovar_proposta(request, pk):
    if _user_role(request.user) == "FINANCEIRO":
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    proposta = get_object_or_404(Proposta, pk=pk, cliente=cliente)
    if proposta.status == Proposta.Status.PENDENTE:
        proposta.status = Proposta.Status.APROVADA
        proposta.decidido_em = timezone.now()
        proposta.aprovado_por = request.user
        proposta.save(update_fields=["status", "decidido_em", "aprovado_por"])
    return redirect("propostas")


@login_required
@require_POST
def reprovar_proposta(request, pk):
    if _user_role(request.user) == "FINANCEIRO":
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    proposta = get_object_or_404(Proposta, pk=pk, cliente=cliente)
    if proposta.status == Proposta.Status.PENDENTE:
        proposta.status = Proposta.Status.REPROVADA
        proposta.decidido_em = timezone.now()
        proposta.aprovado_por = request.user
        proposta.save(update_fields=["status", "decidido_em", "aprovado_por"])
    return redirect("propostas")


@login_required
@require_POST
def salvar_observacao(request, pk):
    if _user_role(request.user) == "FINANCEIRO":
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    proposta = get_object_or_404(Proposta, pk=pk, cliente=cliente)
    observacao = request.POST.get("observacao", "").strip()
    prioridade_raw = request.POST.get("prioridade", "").strip()
    proposta.observacao_cliente = observacao
    if prioridade_raw:
        try:
            prioridade = int(prioridade_raw)
        except ValueError:
            prioridade = None
        if prioridade is not None:
            prioridade = max(1, min(99, prioridade))
            proposta.prioridade = prioridade
    proposta.save(update_fields=["observacao_cliente", "prioridade"])
    return redirect("proposta_detail", pk=proposta.pk)


@login_required
def user_management(request):
    if not request.user.is_staff:
        return HttpResponseForbidden("Sem permissao.")
    message = None
    form = UserCreateForm()
    tipo_form = TipoPerfilCreateForm()
    if request.method == "POST":
        if request.POST.get("create_tipo") == "1":
            tipo_form = TipoPerfilCreateForm(request.POST)
            if tipo_form.is_valid():
                tipo_form.save()
                return redirect("usuarios")
        elif request.POST.get("update_tipo") == "1":
            tipo_id = request.POST.get("tipo_id")
            novo_nome = request.POST.get("novo_nome", "").strip()
            tipo = get_object_or_404(TipoPerfil, pk=tipo_id)
            if novo_nome:
                if TipoPerfil.objects.exclude(pk=tipo.id).filter(nome__iexact=novo_nome).exists():
                    message = "Tipo ja existe."
                else:
                    tipo.nome = novo_nome
                    tipo.save(update_fields=["nome"])
                    return redirect("usuarios")
            else:
                message = "Informe um nome valido."
        elif request.POST.get("delete_tipo") == "1":
            tipo_id = request.POST.get("tipo_id")
            tipo = get_object_or_404(TipoPerfil, pk=tipo_id)
            tipo.delete()
            return redirect("usuarios")
        if request.POST.get("create_user") == "1":
            form = UserCreateForm(request.POST)
            if form.is_valid():
                user = form.save()
                tipo_ids = request.POST.getlist("tipos")
                if tipo_ids:
                    tipos = TipoPerfil.objects.filter(id__in=tipo_ids)
                    nome = user.username.split("@")[0]
                    cliente = Cliente.objects.create(
                        nome=nome,
                        email=user.username,
                        usuario=user,
                        ativo=True,
                    )
                    cliente.tipos.set(tipos)
                return redirect("usuarios")
        else:
            user_id = request.POST.get("user_id")
            action = request.POST.get("action")
            user = get_object_or_404(User, pk=user_id)
            if action == "toggle_active":
                user.is_active = not user.is_active
                user.save(update_fields=["is_active"])
                return redirect("usuarios")
            if action == "set_password":
                new_password = request.POST.get("new_password", "").strip()
                if new_password:
                    user.set_password(new_password)
                    user.save(update_fields=["password"])
                    message = "Senha atualizada."
                else:
                    message = "Informe uma senha valida."
            if action == "set_tipos":
                tipo_ids = request.POST.getlist("tipos")
                cliente = _get_cliente(user)
                if not cliente:
                    message = "Usuario sem cadastro de cliente."
                else:
                    tipos = TipoPerfil.objects.filter(id__in=tipo_ids)
                    cliente.tipos.set(tipos)
                    return redirect("usuarios?user_id=%s" % user.id)
    else:
        form = UserCreateForm()
    users = User.objects.order_by("username")
    selected_user_id = request.GET.get("user_id")
    selected_user = None
    if selected_user_id:
        selected_user = get_object_or_404(User, pk=selected_user_id)
    return render(
        request,
        "core/usuarios.html",
        {
            "form": form,
            "users": users,
            "selected_user": selected_user,
            "tipos": TipoPerfil.objects.order_by("nome"),
            "tipo_form": tipo_form,
            "message": message,
        },
    )


@login_required
def financeiro_overview(request):
    if not (request.user.is_staff or _has_tipo(request.user, "Financeiro")):
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    cadernos = Caderno.objects.filter(clientes=cliente) if cliente else Caderno.objects.none()
    cadernos = cadernos.annotate(total=Sum("compras__valor")).order_by("nome")
    total_geral = Compra.objects.filter(caderno__clientes=cliente).aggregate(total=Sum("valor")).get("total")
    ultimas_compras = Compra.objects.filter(caderno__clientes=cliente).order_by("-data")[:6]

    caderno_id = request.GET.get("caderno_id")
    compras = Compra.objects.none()
    if cliente and caderno_id:
        compras = Compra.objects.filter(caderno_id=caderno_id, caderno__clientes=cliente).order_by("-data")

    return render(
        request,
        "core/financeiro_overview.html",
        {
            "cliente": cliente,
            "cadernos": cadernos,
            "total_geral": total_geral or 0,
            "compras": compras,
            "caderno_id": caderno_id,
            "ultimas_compras": ultimas_compras,
        },
    )


@login_required
def financeiro_nova(request):
    if not (request.user.is_staff or _has_tipo(request.user, "Financeiro")):
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_status":
            nome = request.POST.get("nome", "").strip()
            if nome:
                StatusCompra.objects.get_or_create(nome=nome, defaults={"ativo": True})
            return redirect("financeiro_nova")
        if action == "create_compra":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            caderno_id = request.POST.get("caderno")
            descricao = request.POST.get("descricao", "").strip()
            valor_raw = request.POST.get("valor", "").replace(",", ".").strip()
            data_raw = request.POST.get("data", "").strip()
            categoria_id = request.POST.get("categoria")
            tipo_id = request.POST.get("tipo")
            centro_id = request.POST.get("centro_custo")
            status_id = request.POST.get("status")
            pago = request.POST.get("pago") == "on"
            data_pagamento_raw = request.POST.get("data_pagamento", "").strip()
            try:
                valor = Decimal(valor_raw)
            except (InvalidOperation, ValueError):
                valor = None
            try:
                data = datetime.strptime(data_raw, "%Y-%m-%d").date()
            except ValueError:
                data = None
            try:
                data_pagamento = datetime.strptime(data_pagamento_raw, "%Y-%m-%d").date()
            except ValueError:
                data_pagamento = None
            if pago and not data_pagamento:
                return redirect("financeiro_nova")
            if all([caderno_id, descricao, valor, data, categoria_id, tipo_id, centro_id, status_id]):
                Compra.objects.create(
                    caderno_id=caderno_id,
                    descricao=descricao,
                    valor=valor,
                    data=data,
                    categoria_id=categoria_id,
                    tipo_id=tipo_id,
                    centro_custo_id=centro_id,
                    status_id=status_id,
                    pago=pago,
                    data_pagamento=data_pagamento if pago else None,
                )
            return redirect("financeiro")

    cadernos = Caderno.objects.filter(clientes=cliente) if cliente else Caderno.objects.none()
    categorias = CategoriaCompra.objects.order_by("nome")
    tipos = TipoCompra.objects.order_by("nome")
    centros = CentroCusto.objects.order_by("nome")
    status_list = StatusCompra.objects.filter(ativo=True).order_by("nome")

    return render(
        request,
        "core/financeiro_nova.html",
        {
            "cliente": cliente,
            "cadernos": cadernos,
            "categorias": categorias,
            "tipos": tipos,
            "centros": centros,
            "status_list": status_list,
        },
    )


@login_required
def financeiro_cadernos(request):
    if not (request.user.is_staff or _has_tipo(request.user, "Financeiro")):
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_caderno":
            nome = request.POST.get("nome", "").strip()
            if nome and cliente:
                caderno = Caderno.objects.create(nome=nome, ativo=True)
                caderno.clientes.add(cliente)
            return redirect("financeiro_cadernos")
        if action == "toggle_caderno":
            caderno_id = request.POST.get("caderno_id")
            caderno = get_object_or_404(Caderno, pk=caderno_id, clientes=cliente)
            caderno.ativo = not caderno.ativo
            caderno.save(update_fields=["ativo"])
            return redirect("financeiro_cadernos")

    cadernos = Caderno.objects.filter(clientes=cliente).annotate(total=Sum("compras__valor")).order_by("nome")
    return render(
        request,
        "core/financeiro_cadernos.html",
        {"cadernos": cadernos},
    )


@login_required
def financeiro_caderno_detail(request, pk):
    if not (request.user.is_staff or _has_tipo(request.user, "Financeiro")):
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    caderno = get_object_or_404(Caderno, pk=pk, clientes=cliente)
    compras = Compra.objects.filter(caderno=caderno).order_by("-data")
    return render(
        request,
        "core/financeiro_caderno_detail.html",
        {"caderno": caderno, "compras": compras},
    )


@login_required
def financeiro_compra_detail(request, pk):
    if not (request.user.is_staff or _has_tipo(request.user, "Financeiro")):
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    compra = get_object_or_404(Compra, pk=pk, caderno__clientes=cliente)
    return render(
        request,
        "core/financeiro_compra_detail.html",
        {"compra": compra},
    )


@login_required
def admin_explorar(request):
    if not request.user.is_staff:
        return HttpResponseForbidden("Sem permissao.")
    cliente_id = request.GET.get("cliente_id")
    cliente_q = request.GET.get("cliente_q", "").strip()
    cliente_sort = request.GET.get("cliente_sort", "nome")
    proposta_status = request.GET.get("proposta_status", "").strip()
    proposta_sort = request.GET.get("proposta_sort", "-criado_em")

    clientes = Cliente.objects.all()
    if cliente_q:
        clientes = clientes.filter(nome__icontains=cliente_q)
    if cliente_sort == "empresa":
        clientes = clientes.order_by("empresa", "nome")
    elif cliente_sort == "email":
        clientes = clientes.order_by("email", "nome")
    else:
        clientes = clientes.order_by("nome")

    cliente = None
    propostas = Proposta.objects.none()
    if cliente_id:
        cliente = get_object_or_404(Cliente, pk=cliente_id)
        propostas = Proposta.objects.filter(cliente=cliente)
        if proposta_status in Proposta.Status.values:
            propostas = propostas.filter(status=proposta_status)
        if proposta_sort == "prioridade":
            propostas = propostas.order_by("prioridade", "-criado_em")
        elif proposta_sort == "valor":
            propostas = propostas.order_by("-valor", "-criado_em")
        else:
            propostas = propostas.order_by("-criado_em")
    return render(
        request,
        "admin/explorar.html",
        {
            "clientes": clientes,
            "cliente": cliente,
            "propostas": propostas,
            "cliente_q": cliente_q,
            "cliente_sort": cliente_sort,
            "proposta_status": proposta_status,
            "proposta_sort": proposta_sort,
        },
    )
