from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST

from User.views import login_required
from getkpi.views import get_structure_data, _find_subordinates

from . import embeddings


@require_POST
@login_required
def search_departments(request):
    query = request.GET.get("q", "").strip()
    if not query:
        return JsonResponse({"error": "Parameter 'q' is required"}, status=400)

    top_k = request.GET.get("top_k", "5")
    try:
        top_k = max(1, min(int(top_k), 20))
    except (ValueError, TypeError):
        top_k = 5

    department = request.current_user.department
    structure = get_structure_data()

    allowed = _find_subordinates(structure, department)
    if allowed is None:
        allowed = {department}

    results = embeddings.search(query, allowed, top_k=top_k)

    return JsonResponse({"query": query, "results": results})
