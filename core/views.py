from django.contrib.auth import logout
from django.contrib.auth.decorators import login_required
from django.http import HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST

from django.contrib.auth.models import User
from django.db.models import Case, IntegerField, Value, When

from .forms import TipoPerfilCreateForm, UserCreateForm
from .models import Cliente, Proposta, TipoPerfil


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
    has_cliente = cliente.tipos.filter(nome__iexact="Cliente").exists()
    has_financeiro = cliente.tipos.filter(nome__iexact="Financeiro").exists()
    if has_cliente:
        return "CLIENTE"
    if has_financeiro:
        return "FINANCEIRO"
    return "CLIENTE"


def home(request):
    if request.user.is_authenticated:
        logout(request)
    return render(request, "core/home.html")


@login_required
def painel(request):
    return render(request, "core/painel.html", {"role": _user_role(request.user)})


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
                form.save()
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
