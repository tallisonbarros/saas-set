from django.contrib.auth.decorators import login_required
from django.http import HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST

from django.contrib.auth.models import User
from django.db.models import Case, IntegerField, Value, When

from .forms import UserCreateForm
from .models import Cliente, Proposta


def _get_cliente(user):
    try:
        return user.cliente
    except Cliente.DoesNotExist:
        return None


def home(request):
    return render(request, "core/home.html")


@login_required
def painel(request):
    return render(request, "core/painel.html")


@login_required
def proposta_list(request):
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
    cliente = _get_cliente(request.user)
    proposta = get_object_or_404(Proposta, pk=pk, cliente=cliente)
    return render(request, "core/proposta_detail.html", {"cliente": cliente, "proposta": proposta})


@login_required
@require_POST
def aprovar_proposta(request, pk):
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
    if request.method == "POST":
        form = UserCreateForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect("usuarios")
    else:
        form = UserCreateForm()
    users = User.objects.order_by("username")
    return render(request, "core/usuarios.html", {"form": form, "users": users})


def admin_explorar(request):
    if not request.user.is_staff:
        return HttpResponseForbidden("Sem permissao.")
    cliente_id = request.GET.get("cliente_id")
    clientes = Cliente.objects.order_by("nome")
    cliente = None
    propostas = Proposta.objects.none()
    if cliente_id:
        cliente = get_object_or_404(Cliente, pk=cliente_id)
        propostas = Proposta.objects.filter(cliente=cliente).order_by("-criado_em")
    return render(
        request,
        "admin/explorar.html",
        {"clientes": clientes, "cliente": cliente, "propostas": propostas},
    )
