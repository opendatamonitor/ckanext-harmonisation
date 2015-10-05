import logging

import ckan.model as model
import ckan.plugins as p
import ckan.plugins.toolkit as tk
import ckan.logic as logic

log = logging.getLogger(__name__)
assert not log.disabled

DATASET_TYPE_NAME = 'harmonisation'


class Harmonisation(p.SingletonPlugin, tk.DefaultDatasetForm):

    p.implements(p.IConfigurable)
    p.implements(p.IRoutes, inherit=True)
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IDatasetForm)
    p.implements(p.IPackageController, inherit=True)
    p.implements(p.ITemplateHelpers)
   # p.implements(p.IFacets, inherit=True)

    # IDatasetForm

    def is_fallback(self):
        return False

    def package_types(self):
        return [DATASET_TYPE_NAME]

    def package_form(self):
        return 'sources/new_source_form.html'

    def search_template(self):
        return 'dashboard.html'

    def read_template(self):
        return 'sources/read.html'

    def new_template(self):
        return 'sources/new.html'

    # def edit_template(self):
        # return 'source/edit.html'

    def setup_template_variables(self, context, data_dict):

        #p.toolkit.c.harvest_source = p.toolkit.c.pkg_dict

        p.toolkit.c.dataset_type = DATASET_TYPE_NAME

    def update_config(self, config):
        # check if new templates
        p.toolkit.add_template_directory(config, 'templates')
        p.toolkit.add_public_directory(config, 'public')
#        p.toolkit.add_resource('fanstatic_library', 'ckanext-harvest')
#        p.toolkit.add_resource('public/ckanext/harvest/javascript', 'harvest-extra-field')

     # ITemplateHelpers

    def get_helpers(self):
        from ckanext.harmonisation import helpers as harmonisation_helpers
        return {
            #'package_list_for_source': harvest_helpers.package_list_for_source,
            'harmonisation_sources_list': harmonisation_helpers.harmonisation_sources_list,
            'harmonisation_sources_rules_list': harmonisation_helpers.harmonisation_sources_rules_list
            #'harvesters_info': harvest_helpers.harvesters_info,
            #'harvester_types': harvest_helpers.harvester_types,
            #'harvest_frequencies': harvest_helpers.harvest_frequencies,
            #'link_for_harvest_object': harvest_helpers.link_for_harvest_object,
            #'harvest_source_extra_fields': harvest_helpers.harvest_source_extra_fields,
        }

    ## IConfigurable interface ##

    def configure(self, config):
        ''' Apply configuration options to this plugin '''
        pass

    # IPackageController

    def after_create(self, context, data_dict):
        if 'type' in data_dict and data_dict[
                'type'] == DATASET_TYPE_NAME and not self.startup:
            log.info('Harmonisation_form: Nothing important')

    def before_map(self, m):
        ''' Called before routes map is setup. '''
        controller = 'ckanext.harmonisation.controllers.package:CustomHarmonisationController'

        m.connect('view_rules', '/' + DATASET_TYPE_NAME + '/view_rules',
                  controller=controller, action='view_rules')

        m.connect('main_dashboard', '/' + DATASET_TYPE_NAME,
                  controller=controller, action='main_dashboard')

        m.connect('edit_rules', '/' + DATASET_TYPE_NAME + '/edit_rules',
                  controller=controller, action='edit_rules')
        # m.connect('/' + DATASET_TYPE_NAME + '/{controller}/{action}',
        # controller = controller, action = 'read_data')

        m.connect('/' + DATASET_TYPE_NAME + '/{action}',
                  controller=controller,
                  requirements=dict(action='|'.join([
                      'read_data',
                      'main_dashboard',
                      'new_metadata',
                      'new_resource',
                      'history',
                      'read_ajax',
                      'history_ajax',
                      'follow',
                      'activity',
                      'unfollow',
                      'delete',
                      'api_data',
                  ])))

        return m
