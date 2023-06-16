# trials/views
from django.views.generic import ListView, DetailView
from slick_reporting.views import SlickReportView
from slick_reporting.fields import SlickReportField
from django.shortcuts import render, redirect
from django.http import HttpResponse
from django.views.generic.base import TemplateView
from .models import Ctgov1, Contacts
from .forms import TrialForm1
from .filters import Ctgov1Filter
from django.views.generic import FormView


class Index(ListView):
	model = Ctgov1
	template_name = 'trials/index.html'
	context_object_name = 'trial_list'
	queryset = Ctgov1.objects.filter(drank_final__gte=150)
	paginate_by = 40

class View1(ListView):
	model = Ctgov1
	paginate_by = 40

	def get_queryset(self):
		return Ctgov1.objects.filter(drank_final__gte = 400)

#ctgov1_detail.html trial_detail
class TrialDetailView(DetailView):
	model = Ctgov1

	def get_context_data(self, **kwargs):
		context = super().get_context_data(**kwargs)
		context['contacts'] = Contacts.objects.filter(nct_id = self.kwargs['pk'])
		print([dom[0].split('@')[1] for dom in context['contacts'].values_list('email')])
		return context

class CTView1(SlickReportView):
	report_model = Ctgov1
	date_field = 'updated'
	columns = ['nct_id', 'updated', 'brief_title', 'drank_final', 'alloc']

class Search1(Ctgov1Filter):
	model = Ctgov1

'''
class TrialView(View):

    def get(self, request, *args, **kwargs):
        view = TrialDetailView.as_view()
        return view(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        view = TrialInterestFormView.as_view()
        return view(request, *args, **kwargs)
        
class ViewTrial1(FormView):
	template_name = 'trials/nct.html'
	form_class = TrialForm1

	def form_valid(self, form):
		return super().form_valid()

#input form
def View2(request):
	print('view2')
	if request.method == 'POST':
		form = TrialForm1(request.POST)
		if form.is_valid():
			data = form.cleaned_data['data']
			print(data)
			return redirect('trials:trial_detail', pk=data)
	else:
		print('else view2')
		form = TrialForm1()
		context ={'form':form}
		return render(request,'trials/trialform1.html', context)


class TrialInterestFormView(SingleObjectMixin, FormView):
    template_name = 'trials/ctgov_detail.html'
    form_class = TrialForm1
    model = Ctgov1

    def post(self, request, *args, **kwargs):
        if not request.user.is_authenticated:
            return HttpResponseForbidden()
        self.object = self.get_object()
        return super().post(request, *args, **kwargs)

    def get_success_url(self):
        return reverse('ctgov1_detail', kwargs={'pk': self.object.nct_id})

'''
