"""
Not defined;coordinator;inventor;publisher;organizer;translator
nd;coord;inventor;ed;org;tr
"""
CONTRIB_ROLES = {
    "ND": "author",
    "nd": "author",
    "coord": "coordinator",
    "inventor": "inventor",
    "tr": "translator",
    "ed": "editor",
    "org": "organizer",
}


def _get_value(data, tag):
    """
    Returns first value of field `tag`
    """
    # data['v880'][0]['_']
    try:
        return data[tag][0]['_']
    except (KeyError, IndexError):
        return None


def _get_items(data, tag):
    """
    Returns first value of field `tag`
    """
    # data['v880'][0]['_']
    try:
        for item in data[tag]:
            if len(item) > 1:
                yield item
            else:
                yield item['_']
    except KeyError:
        return []


class Document:

    def __init__(self, _id, records):
        self._id = _id
        self._records = records

    def _get_article_meta_item_(self, tag, formatted=False):
        if formatted:
            # record = f
            return _get_value(self._records[2], tag)
        return _get_value(self._records[1], tag)

    def _get_article_meta_items_(self, tag, formatted=False):
        if formatted:
            # record = f
            return _get_items(self._records[2], tag)
        return _get_items(self._records[1], tag)

    @property
    def records(self):
        return self._records

    @property
    def data(self):
        _data = {}
        _data["article"] = self._records
        return _data

    @property
    def doi(self):
        return self._get_article_meta_item_("v237")

    @property
    def language(self):
        return self._get_article_meta_item_("v040")

    @property
    def document_type(self):
        return self._get_article_meta_item_("v071")

    @property
    def isis_dates(self):
        dates = (
            _get_value(self._records[0], "v091"),
            _get_value(self._records[0], "v093")
        )
        return [d[:8] for d in dates if d]

    @property
    def isis_updated_date(self):
        return max(self.isis_dates)

    @property
    def isis_created_date(self):
        return min(self.isis_dates)

    @property
    def html_body_items(self):
        _html_body_items = self.html_body
        _html_body_items.update(self.translated_html_body_items)
        return _html_body_items

    @property
    def html_body(self):
        _paragraphs = []
        for rec in self._records:
            p = _get_value(rec, "v704")
            if p:
                _paragraphs.append(p)
        return {self.language: "".join(_paragraphs)}

    @property
    def translated_html_body_items(self):
        # TODO
        return {}

    @property
    def _mixed_citations(self):
        # TODO
        mixed_citations = {}
        for rec in self._records:
            p = _get_value(rec, "v704")
            ref_number = _get_value(rec, "v118")
            if p and ref_number:
                mixed_citations.update(
                    {ref_number: p}
                )
        return mixed_citations

    @property
    def pdfs(self):
        # TODO
        pass

    @property
    def images(self):
        # TODO
        pass

    @property
    def translated_body_items(self):
        # TODO
        pass

    @property
    def material_supplementar(self):
        # TODO
        pass

    @property
    def journal(self):
        return self._journal

    @journal.setter
    def journal(self, value):
        self._journal = value

    @property
    def issue(self):
        return self._issue

    @issue.setter
    def issue(self, value):
        self._issue = value

    @property
    def original_section(self):
        code = self._get_article_meta_item_("v049")
        return self.issue.get_section(code, self.language)

    @property
    def scielo_pid_v1(self):
        return self._get_article_meta_item_("v002")

    @property
    def scielo_pid_v2(self):
        return self._get_article_meta_item_("v880")

    @property
    def scielo_pid_v3(self):
        return self._get_article_meta_item_("v885")

    @property
    def ahead_of_print_pid(self):
        return self._get_article_meta_item_("v881")

    @property
    def order(self):
        return self._get_article_meta_item_("v121")

    @property
    def original_title(self):
        return self.titles.get(self.language)

    @property
    def titles(self):
        # TODO manter formatação itálico e maths
        return {
            item['l']: item['_']
            for item in self._get_article_meta_items_("v012", True)
        }

    @property
    def translated_titles(self):
        return {
            lang: title
            for lang, title in self.titles.items()
            if lang != self.language
        }

    @property
    def titles(self):
        # TODO manter formatação itálico e maths
        return {
            item['l']: item['_']
            for item in self._get_article_meta_items_("v012")
        }

    def translated_htmls(self, iso_format=None):
        if not self.body:
            return None

        fmt = iso_format or self._iso_format

        translated_bodies = {}
        for language, body in self.data.get('body', {}).items():
            if language != self.original_language(iso_format=fmt):
                translated_bodies[language] = body

        if len(translated_bodies) == 0:
            return None

    @property
    def contrib_group(self):
        for item in self._get_article_meta_items_("v010"):
            yield (
                {
                    "surname": item.get("s"),
                    "given_names": item.get("n"),
                    "role": CONTRIB_ROLES.get(item.get("r")),
                    "xref": contrib_xref(item.get("1")),
                    "orcid": item.get("k"),
                }
            )


def contrib_xref(xrefs):
    for xref in xrefs.split():
        if xref.startswith("aff") or xref.startswith("a0"):
            xref_type = "aff"
        elif xref.startswith("fn"):
            xref_type = "fn"
        else:
            xref_type = "author-notes"
        yield (xref_type, xref)


def complete_uri(text, website_url):
    return text.replace("/img/revistas", f"{website_url}/img/revistas")


class Journal:

    """
    """
    def __init__(self, _id, record):
        self._id = _id
        self._record = record
        self._issns = get_issns(self._record) or {}

    def _get_items_(self, tag):
        return _get_items(self._record, tag)

    def _get_item_(self, tag):
        return _get_value(self._record, tag)

    @property
    def record(self):
        return self._record

    @property
    def acronym(self):
        # TODO
        return self._get_item_("v068").lower()

    @property
    def title(self):
        return self._get_item_("v100")

    @property
    def iso_abbreviated_title(self):
        return self._get_item_("v151")

    @property
    def abbreviated_title(self):
        return self._get_item_("v150")

    @property
    def print_issn(self):
        return self._issns.get("PRINT")

    @property
    def electronic_issn(self):
        return self._issns.get("ONLIN")

    @property
    def raw_publisher_names(self):
        return self._get_items_('v480')

    def get_publisher_names(self, sep="; "):
        return sep.join(self.raw_publisher_names)

    @property
    def publisher_city(self):
        return self._get_item_('v490')

    @property
    def publisher_state(self):
        return self._get_item_('v320')

    def get_publisher_loc(self, sep=", "):
        loc = [item
               for item in [self.publisher_city, self.publisher_state]
               if item]
        return sep.join(loc)

    @property
    def new_title(self):
        return self._get_item_("v710")

    @property
    def old_title(self):
        return self._get_item_("v610")

    @property
    def isis_created_date(self):
        return self._get_item_("v940")

    @property
    def isis_updated_date(self):
        return self._get_item_("v941")


class Issue:
    """
    """
    def __init__(self, _id, record):
        self._id = _id
        self._record = record
        self._sections = None

    @property
    def record(self):
        return self._record

    def _get_items_(self, tag):
        return _get_items(self._record, tag)

    def _get_item_(self, tag):
        return _get_value(self._record, tag)

    @property
    def sections(self):
        if self._sections is None:
            self._sections = {}
            for item in _get_items(self._record, "v049"):
                self._sections.setdefault(item["c"], {})
                self._sections[item["c"]][item["l"]] = item["t"]
        return self._sections

    def get_section(self, code, lang):
        try:
            return self.sections[code][lang]
        except KeyError:
            return None

    @property
    def isis_created_date(self):
        return self._get_item_("NotAvailable")

    @property
    def isis_updated_date(self):
        return self._get_item_("NotAvailable")

    @property
    def iid(self):
        return self._id

    @property
    def journal(self):
        return self._get_item_("v035")

    @property
    def volume(self):
        return self._get_item_("v031")

    @property
    def number(self):
        return self._get_item_("v032")

    @property
    def suppl(self):
        return self._get_item_("v131") or self._get_item_("v132")

    @property
    def start_month(self):
        return self._get_item_("v065")[4:6]

    @property
    def end_month(self):
        return self._get_item_("v065")[4:6]

    @property
    def year(self):
        return self._get_item_("v065")[:4]

    @property
    def label(self):
        if self.number == "ahead":
            return self.year + "nahead"

        pairs = (
            ("v", remove_leading_zeros(self.volume)),
            ("n", remove_leading_zeros(self.number)),
            ("s", remove_leading_zeros(self.suppl)),
        )
        return "".join([prefix + value for prefix, value in pairs if value])

    @property
    def order(self):
        _order = self._get_item_("v036")
        return _order[:4] + _order[4:].zfill(4)

    @property
    def is_public(self):
        return self._get_item_("v042") == 1

    @property
    def pid(self):
        return f"{self.journal}{self.order}"

    @property
    def unpublish_reason(self):
        return self._get_item_("NotAvailable")

    @property
    def url_segment(self):
        return self._get_item_("NotAvailable")

    @property
    def assets_code(self):
        return self._get_item_("NotAvailable")

    @property
    def type(self):
        return self._get_item_("NotAvailable")

    @property
    def suppl_text(self):
        return self._get_item_("NotAvailable")

    @property
    def spe_text(self):
        return self._get_item_("NotAvailable")

    @property
    def cover_url(self):
        return self._get_item_("NotAvailable")


def remove_leading_zeros(data):
    try:
        return str(int(data))
    except:
        return data


def get_issns(data):
    _issns = {}
    for item in data.get("v435") or []:
        _issns[item.get("t")] = item['_']
    if _issns:
        return _issns

    issn_type = _get_value(data, "v035")
    if not issn_type:
        return None

    if 'v935' in data:
        _issns[issn_type] = _get_value(data, "v935")
        v400 = _get_value(data, "v400")
        if _issns[issn_type] != v400:
            _issns["PRINT" if issn_type != "PRINT" else "ONLIN"] = v400
        return _issns

    # ISSN and Other Complex Stuffs from the old version
    issn_type = _get_value(data, "v035")
    _issns[issn_type] = _get_value(data, "v400")
    return _issns