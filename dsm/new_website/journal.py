from opac_schema.v1.models import (
    Journal,
    Timeline,
    SocialNetwork,
    OtherTitle,
    Mission,
    JounalMetrics,
    LastIssue,
)

class DataNotFoundError(Exception):
    ...


JOURNAL_ATTRIBUTES = (
    'title',
    'title_iso',
    'next_title',
    'short_title',
    'title_slug',
    'acronym',
    'scielo_issn',
    'print_issn',
    'eletronic_issn',
    'copyrighter',
    'online_submission_url',
    'logo_url',
    'previous_journal_ref',
    'publisher_name',
    'publisher_country',
    'publisher_state',
    'publisher_city',
    'publisher_address',
    'publisher_telephone',
    'current_status',
    'editor_email',
    'unpublish_reason',
    'url_segment',
    'scimago_id',
    'enable_contact',
    'is_public',
    'issue_count',
)
JOURNAL_ATTRIBUTES_STR_LIST = (
    'subject_categories',
    'study_areas',
    'index_at',
    'sponsors',
    'subject_descriptors',
)

JOURNAL_ATTRIBUTES_DICT_LIST = (
    ('timeline', Timeline),
    ('social_networks', SocialNetwork),
    ('other_titles', OtherTitle),
    ('mission', Mission),
)


def update_journal(journal, data):
    # FIXME errors
    errors = []
    for attr_name in JOURNAL_ATTRIBUTES:
        try:
            setattr(journal, attr_name, data[attr_name])
        except KeyError:
            errors.append(attr_name)

    for attr_name in JOURNAL_ATTRIBUTES_STR_LIST:
        try:
            setattr(journal, attr_name, data[attr_name])
        except KeyError:
            errors.append(attr_name)

    for attr_name, class_name in JOURNAL_ATTRIBUTES_DICT_LIST:
        try:
            add_list_attribute(journal, attr_name, data, class_name)
        except DataNotFoundError as e:
            errors.append(attr_name)

    try:
        add_dict_attribute(journal, 'last_issue', data, LastIssue)
    except DataNotFoundError as e:
        errors.append(attr_name)
    try:
        add_dict_attribute(journal, 'metrics', data, JounalMetrics)
    except DataNotFoundError as e:
        errors.append(attr_name)
    journal.save()


def add_list_attribute(journal, attr_name, data, class_name):
    try:
        items = data[attr_name]
    except KeyError as e:
        raise DataNotFoundError(e)
    values = []
    for item in items:
        obj = class_name()
        for k, v in items.items():
            setattr(obj, k, v)
        values.append(obj)
    setattr(journal, attr_name, values)


def add_dict_attribute(journal, attr_name, data, class_name):
    try:
        item = data[attr_name]
    except KeyError as e:
        raise DataNotFoundError(e)
    obj = class_name()
    for k, v in item.items():
        setattr(obj, k, v)
    setattr(journal, attr_name, obj)
