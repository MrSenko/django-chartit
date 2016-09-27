import copy

from django.db.models.aggregates import Aggregate
from django.db.models.query import QuerySet
from django.utils import six

from .exceptions import APIInputError


def get_all_field_names(meta):
    """
        Taken from Django 1.9.8 b/c this is unofficial API
        which has been deprecated in 1.10.
    """
    names = set()
    fields = meta.get_fields()
    for field in fields:
        # For backwards compatibility GenericForeignKey should not be
        # included in the results.
        if field.is_relation and field.many_to_one and \
           field.related_model is None:
            continue
        # Relations to child proxy models should not be included.
        if (field.model != meta.model and
                field.model._meta.concrete_model == meta.concrete_model):
            continue

        names.add(field.name)
        if hasattr(field, 'attname'):
            names.add(field.attname)
    return list(names)


def _validate_field_lookup_term(model, term, query):
    """Checks whether the term is a valid field_lookup for the model.

    **Args**:

    - **model** (**required**) - a django model for which to check whether
      the term is a valid field_lookup.
    - **term** (**required**) - the term to check whether it is a valid
      field lookup for the model supplied.
    - **query** - the source query so we can check for aggregate or extra
      fields.

    **Returns**:

    -  The verbose name of the field if the supplied term is a valid field.

    **Raises**:

    - APIInputError: If the term supplied is not a valid field lookup
      parameter for the model.
    """
    # if this is an extra or annotated field then return
    if term in query.annotations.keys() or term in query.extra.keys():
        return term

    # TODO: Memoization for speed enchancements?
    terms = term.split('__')
    model_fields = get_all_field_names(model._meta)
    if terms[0] not in model_fields:
        raise APIInputError("Field %r does not exist. Valid lookups are %s."
                            % (terms[0], ', '.join(model_fields)))
    if len(terms) == 1:
        return model._meta.get_field(terms[0]).verbose_name
    else:
        field = model._meta.get_field(terms[0])
        # if the field is direct field
        if not field.auto_created or field.concrete:
            m = field.related_model
        else:
            m = model

        return _validate_field_lookup_term(m, '__'.join(terms[1:]), query)


def _validate_source(source):
    """
        Used to validate the source parameter passed to
        DataPool and PivotDataPool. It must be a QuerySet!
    """
    if isinstance(source, QuerySet):
        return source
    raise APIInputError("'source' must be a QuerySet. Got "
                        "%s of type %s instead." % (source, type(source)))


def _validate_func(func):
    """
        Used to validate aggregate functions for PivotDataPool
        terms.
    """
    if not isinstance(func, Aggregate):
        raise APIInputError("'func' must an instance of django Aggregate. "
                            "Got %s of type %s instead" % (func, type(func)))


def _clean_categories(categories, source):
    if isinstance(categories, six.string_types):
        categories = [categories]
    elif isinstance(categories, (tuple, list)):
        if not categories:
            raise APIInputError("'categories' tuple or list must contain at "
                                "least one valid model field. Got %s."
                                % categories)
    else:
        raise APIInputError("'categories' must be one of the following "
                            "types: basestring, tuple or list. Got %s of "
                            "type %s instead."
                            % (categories, type(categories)))
    field_aliases = {}
    for c in categories:
        field_aliases[c] = _validate_field_lookup_term(source.model, c,
                                                       source.query)
    return categories, field_aliases


def _validate_legend_by(legend_by, source):
    if not legend_by:
        legend_by = []

    if not isinstance(legend_by, list):
        raise APIInputError("'legend_by' must be a list")

    field_aliases = {}
    for lg in legend_by:
        field_aliases[lg] = _validate_field_lookup_term(source.model, lg,
                                                        source.query)
    return legend_by, field_aliases


def _validate_top_n_per_cat(top_n_per_cat):
    """
        Validates parameter used in PivotDataPool.
    """
    if not isinstance(top_n_per_cat,  int):
        raise APIInputError("'top_n_per_cat' must be an int. Got %s of type "
                            "%s instead."
                            % (top_n_per_cat, type(top_n_per_cat)))


def _merge_field_aliases(fa_actual, fa_cat, fa_lgby):
    """
        Merges dicts containing field aliases data.
        Used in PivotDataPool validation
    """
    fa = copy.copy(fa_lgby)
    fa.update(fa_cat)
    fa.update(fa_actual)
    return fa


def _convert_pdps_to_dict(series_list):
    series_dict = {}
    for sd in series_list:
        for _key in ['options', 'terms']:
            if _key not in sd.keys():
                raise APIInputError("%s is missing the '%s' key." % (sd, _key))

        options = sd['options']
        if not isinstance(options, dict):
            raise APIInputError("Expecting a dict in place of: %s" % options)

        terms = sd['terms']
        # see PivotDataPool.__init__ for the format of terms
        if not isinstance(terms, dict):
            raise APIInputError("Expecting a dict in place of: %s" % terms)

        if not terms:
            raise APIInputError("'terms' cannot be empty.")

        for tk, tv in terms.items():
            if isinstance(tv, Aggregate):
                tv = {'func': tv}
            elif isinstance(tv, dict):
                pass
            else:
                raise APIInputError("Expecting a dict or django Aggregate "
                                    "in place of: %s" % tv)

            opts = copy.deepcopy(options)
            opts.update(tv)

            # make some more validations
            for _key in ['source', 'func', 'categories']:
                if _key not in opts.keys():
                    raise APIInputError("%s is missing the '%s' key."
                                        % (opts, _key))

            opts['source'] = _validate_source(opts['source'])
            _validate_func(opts['func'])
            opts['categories'], fa_cat = _clean_categories(opts['categories'],
                                                           opts['source'])
            if 'legend_by' in opts.keys():
                opts['legend_by'], fa_lgby = _validate_legend_by(
                                                opts['legend_by'],
                                                opts['source'])
            else:
                opts['legend_by'], fa_lgby = (), {}

            if 'top_n_per_cat' in opts.keys():
                _validate_top_n_per_cat(opts['top_n_per_cat'])
            else:
                opts['top_n_per_cat'] = 0

            if 'field_aliases' in opts.keys():
                fa_actual = opts['field_aliases']
            else:
                opts['field_aliases'] = fa_actual = {}
            opts['field_aliases'] = _merge_field_aliases(fa_actual, fa_cat,
                                                         fa_lgby)

            series_dict.update({tk: opts})

    return series_dict


def clean_pdps(series):
    """Clean the PivotDataPool series input from the user.
    """
    if not series:
        raise APIInputError("'series' cannot be empty.")

    if not isinstance(series, list):
        raise APIInputError("Expecting a list in place of: %s" % series)

    return _convert_pdps_to_dict(series)


def _convert_dps_to_dict(series_list):
    """
        Converts a list of options/source/terms items into
        a dictionary containing the same source for all terms.

        @series_list is a list of dicts. See DataPool.__init__
        for more information.
    """
    series_dict = {}
    for sd in series_list:
        for _key in ['options', 'terms']:
            if _key not in sd.keys():
                raise APIInputError("%s is missing the '%s' key." % (sd, _key))

        options = sd['options']
        if not isinstance(options, dict):
            raise APIInputError("Expecting a dict in place of: %s" % options)

        terms = sd['terms']
        # see DataPool.__init__ for the format of terms
        if not isinstance(terms, list):
            raise APIInputError("Expecting a list in place of: %s" % terms)

        if not terms:
            raise APIInputError("'terms' cannot be empty.")

        for term in terms:
            _new_name = ''
            sd_term = {}
            if isinstance(term, six.string_types):
                _new_name = term
                sd_term = series_dict[_new_name] = copy.deepcopy(options)
            elif isinstance(term, dict):
                _new_name = term['_new_name']
                del term['_new_name']

                # note: use 'func' to specify a lambda func for this field
                opts = copy.deepcopy(options)
                opts.update(term)
                sd_term = series_dict[_new_name] = opts
            else:
                raise APIInputError("Expecting a basestring or dict "
                                    "in place of: %s" % str(term))

            # make some more validations
            if 'source' not in sd_term.keys():
                raise APIInputError("%s is missing the 'source' key."
                                    % sd_term)
            sd_term['source'] = _validate_source(sd_term['source'])

            sd_term.setdefault('field', _new_name)
            fa = _validate_field_lookup_term(sd_term['source'].model,
                                             sd_term['field'],
                                             sd_term['source'].query)
            # If the user supplied term is not a field name, use it as an alias
            if _new_name != sd_term['field']:
                fa = _new_name
            sd_term.setdefault('field_alias', fa)

    return series_dict


def clean_dps(series):
    """Clean the DataPool series input from the user.
    """
    if not series:
        raise APIInputError("'series' cannot be empty.")

    if not isinstance(series, list):
        raise APIInputError("Expecting a list in place of: %s" % series)

    return _convert_dps_to_dict(series)


def _convert_pcso_to_dict(series_options):
    series_options_dict = {}
    for stod in series_options:
        try:
            options = stod['options']
        except KeyError:
            raise APIInputError("%s is missing the 'options' key." % stod)
        if not isinstance(options, dict):
            raise APIInputError("Expecting a dict in place of: %s" % options)

        try:
            terms = stod['terms']
        except KeyError:
            raise APIInputError("%s is missing the 'terms' key." % stod)
        if isinstance(terms, list):
            for term in terms:
                if isinstance(term, six.string_types):
                    opts = copy.deepcopy(options)
                    series_options_dict.update({term: opts})
                elif isinstance(term, dict):
                    for tk, tv in term.items():
                        if not isinstance(tv, dict):
                            raise APIInputError("Expecting a dict in place "
                                                "of: %s" % tv)
                        opts = copy.deepcopy(options)
                        opts.update(tv)
                        series_options_dict.update({tk: opts})
        else:
            raise APIInputError("Expecting a list in place of: %s" % terms)
    return series_options_dict


def clean_pcso(series_options, ds):
    """Clean the PivotChart series_options input from the user.
    """
    # todlist = term option dict list
    if isinstance(series_options, dict):
        for sok, sod in series_options.items():
            if sok not in ds.series.keys():
                raise APIInputError("All the series terms must be present "
                                    "in the series dict of the "
                                    "datasource. Got %s. Allowed values "
                                    "are: %s"
                                    % (sok, ', '.join(ds.series.keys())))
            if not isinstance(sod, dict):
                raise APIInputError("All the series options must be of the "
                                    "type dict. Got %s of type %s instead."
                                    % (sod, type(sod)))
    elif isinstance(series_options, list):
        series_options = _convert_pcso_to_dict(series_options)
        clean_pcso(series_options, ds)
    else:
        raise APIInputError("Expecting a dict or list in place of: %s."
                            % series_options)
    return series_options


def _convert_cso_to_dict(series_options):
    series_options_dict = {}
    # stod: series term and option dict
    for stod in series_options:
        try:
            options = stod['options']
        except KeyError:
            raise APIInputError("%s is missing the 'options' key." % stod)
        if not isinstance(options, dict):
            raise APIInputError("Expecting a dict in place of: %s" % options)

        try:
            terms = stod['terms']
        except KeyError:
            raise APIInputError("%s is missing the 'terms' key." % stod)

        if isinstance(terms, dict):
            if not terms:
                raise APIInputError("'terms' dict cannot be empty.")
            for tk, td in terms.items():
                if isinstance(td, list):
                    for yterm in td:
                        if isinstance(yterm, six.string_types):
                            opts = copy.deepcopy(options)
                            opts['_x_axis_term'] = tk
                            series_options_dict[yterm] = opts
                        elif isinstance(yterm, dict):
                            opts = copy.deepcopy(options)
                            opts.update(list(yterm.values())[0])
                            opts['_x_axis_term'] = tk
                            series_options_dict[list(yterm.keys())[0]] = opts
                        else:
                            raise APIInputError("Expecting a basestring or "
                                                "dict in place of: %s." %
                                                yterm)
                else:
                    raise APIInputError("Expecting a list instead of: %s"
                                        % td)
        else:
            raise APIInputError("Expecting a dict in place of: %s."
                                % terms)
    return series_options_dict


def clean_cso(series_options, ds):
    """Clean the Chart series_options input from the user.
    """
    if isinstance(series_options, dict):
        for sok, sod in series_options.items():
            if sok not in ds.series.keys():
                raise APIInputError("%s is not one of the keys of the "
                                    "datasource series. Allowed values "
                                    "are: %s"
                                    % (sok, ', '.join(ds.series.keys())))
            if not isinstance(sod, dict):
                raise APIInputError("%s is of type: %s. Expecting a dict."
                                    % (sod, type(sod)))
            try:
                _x_axis_term = sod['_x_axis_term']
                if _x_axis_term not in ds.series.keys():
                    raise APIInputError("%s is not one of the keys of the "
                                        "datasource series. Allowed values "
                                        "are: %s" %
                                        (_x_axis_term,
                                         ', '.join(ds.series.keys())))
            except KeyError:
                raise APIInputError("Expecting a '_x_axis_term' for %s." % sod)
            if ds.series[sok]['_data'] != ds.series[_x_axis_term]['_data']:
                raise APIInputError("%s and %s do not belong to the same "
                                    "table." % (sok, _x_axis_term))
    elif isinstance(series_options, list):
        series_options = _convert_cso_to_dict(series_options)
        clean_cso(series_options, ds)
    else:
        raise APIInputError("'series_options' must either be a dict or a "
                            "list. Got %s of type %s instead."
                            % (series_options, type(series_options)))
    return series_options


def clean_sortf_mapf_mts(sortf_mapf_mts):
    """
    **sortf_mapf_mts** is a ``tuple`` with three elements of the form
    ``(sort_func, map_func, map_then_sort_bool)``. It is used in PivotDataPool
    and as helper method for ``clean_x_sortf_mapf_mts()``.
    """

    if not sortf_mapf_mts:
        return (None, None, False)

    if not isinstance(sortf_mapf_mts, tuple):
        raise APIInputError("sortf_mapf_mts must be a tuple!")

    if len(sortf_mapf_mts) != 3:
        raise APIInputError("%r must have exactly three elements."
                            % sortf_mapf_mts)

    sortf, mapf, mts = sortf_mapf_mts
    if sortf and not callable(sortf):
        raise APIInputError("sortf must be callable or None.")

    if mapf and not callable(mapf):
        raise APIInputError("mapf must be callable or None.")

    mts = bool(mts)

    return (sortf, mapf, mts)


def clean_x_sortf_mapf_mts(x_sortf_mapf_mts):
    """
        Similar to ``clean_sortf_mapf_mts`` but input parameter
        can be either a tuple of a list of tuples and the return value is a
        list of tuples.
    """
    if not x_sortf_mapf_mts:
        return [(None, None, False)]

    cleaned_x_s_m_mts = []

    if isinstance(x_sortf_mapf_mts, tuple):
        x_sortf_mapf_mts = [x_sortf_mapf_mts]
    elif not isinstance(x_sortf_mapf_mts, list):
        raise APIInputError("x_sortf_mapf_mts must be a list of tuples!")

    for x_s_m_mts in x_sortf_mapf_mts:
        cleaned_x_s_m_mts.append(clean_sortf_mapf_mts(x_s_m_mts))

    return cleaned_x_s_m_mts
